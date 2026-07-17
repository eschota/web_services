"""Human review for Animation Fitting videos sent through Telegram.

This module deliberately contains no bot token.  Telegram authenticates callback
updates by delivering them through the already authenticated Bot API polling
connection; this layer additionally restricts decisions to one configured private
owner chat/user pair.

The JSONL ledger is append-only.  An approval is a *reference/fitted-preview
review decision*, never automatic catalog/library admission.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Mapping


SCHEMA = "autorig.animation-fitting-telegram-review.v1"
CALLBACK_PREFIX = "af1"
APPROVE = "approve"
REJECT = "reject"
_ACTION_TO_CODE = {APPROVE: "a", REJECT: "r"}
_CODE_TO_ACTION = {value: key for key, value in _ACTION_TO_CODE.items()}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CALLBACK_RE = re.compile(r"^af1:([ar]):([0-9a-f]{24})$")
_LEDGER_LOCK = threading.RLock()


class ReviewError(RuntimeError):
    """Base error for fail-closed review processing."""


class UnauthorizedReviewer(ReviewError):
    pass


class UnknownCandidate(ReviewError):

    pass


@dataclass(frozen=True)
class CandidateBinding:
    candidate_id: str
    video_sha256: str
    callback_key: str
    media_kind: str
    machine_qa_sha256: str | None


@dataclass(frozen=True)
class DecisionResult:
    record: Mapping[str, Any]
    changed: bool
    duplicate: bool


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validated_sha256(value: str) -> str:
    value = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(value):
        raise ValueError("video_sha256 must be lowercase 64-hex")
    return value


def make_candidate_binding(
    candidate_id: str,
    video_sha256: str,
    *,
    media_kind: str = "raw_reference",
    machine_qa_sha256: str | None = None,
) -> CandidateBinding:
    candidate_id = str(candidate_id or "").strip()
    if not candidate_id or len(candidate_id) > 512:
        raise ValueError("candidate_id must contain 1..512 characters")
    video_sha256 = _validated_sha256(video_sha256)
    media_kind = str(media_kind or "").strip().lower()
    if media_kind not in {"raw_reference", "fitted_preview"}:
        raise ValueError("media_kind must be raw_reference or fitted_preview")
    if media_kind == "fitted_preview":
        machine_qa_sha256 = _validated_sha256(machine_qa_sha256 or "")
    elif machine_qa_sha256 is not None:
        raise ValueError("raw_reference must not claim a machine QA binding")
    # 96 bits keeps callback_data compact while the ledger retains and verifies
    # the complete immutable candidate id + video digest.  Collisions fail closed.
    callback_key = hashlib.sha256(
        f"{SCHEMA}\0{candidate_id}\0{video_sha256}\0{media_kind}\0{machine_qa_sha256 or ''}".encode("utf-8")
    ).hexdigest()[:24]
    return CandidateBinding(candidate_id, video_sha256, callback_key, media_kind, machine_qa_sha256)


def callback_data(binding: CandidateBinding, decision: str) -> str:
    try:
        code = _ACTION_TO_CODE[decision]
    except KeyError as exc:
        raise ValueError("decision must be approve or reject") from exc
    value = f"{CALLBACK_PREFIX}:{code}:{binding.callback_key}"
    if len(value.encode("utf-8")) > 64:
        raise AssertionError("Telegram callback_data exceeds 64 bytes")
    return value


def inline_keyboard_payload(binding: CandidateBinding) -> dict[str, Any]:
    return {
        "inline_keyboard": [[
            {"text": "✅ Утвердить", "callback_data": callback_data(binding, APPROVE)},
            {"text": "❌ Отклонить", "callback_data": callback_data(binding, REJECT)},
        ]]
    }


def parse_callback_data(value: str) -> tuple[str, str]:
    match = _CALLBACK_RE.fullmatch(str(value or ""))
    if not match:
        raise ValueError("not an Animation Fitting review callback")
    return _CODE_TO_ACTION[match.group(1)], match.group(2)


def configured_owner() -> tuple[int, int]:
    chat_raw = os.getenv("ANIMATION_FITTING_TELEGRAM_OWNER_CHAT_ID", "").strip()
    user_raw = os.getenv("ANIMATION_FITTING_TELEGRAM_OWNER_USER_ID", "").strip()
    if not chat_raw or not user_raw:
        raise ReviewError("Animation Fitting Telegram owner chat/user are not configured")
    try:
        return int(chat_raw), int(user_raw)
    except ValueError as exc:
        raise ReviewError("Animation Fitting Telegram owner ids must be integers") from exc


def assert_authorized_owner(
    *,
    chat_id: int,
    user_id: int,
    chat_type: str,
    owner_chat_id: int,
    owner_user_id: int,
) -> None:
    if (
        str(chat_type or "").lower() != "private"
        or int(chat_id) != int(owner_chat_id)
        or int(user_id) != int(owner_user_id)
    ):
        raise UnauthorizedReviewer("review is restricted to the configured private owner")


def default_ledger_path() -> Path:
    raw = os.getenv("ANIMATION_FITTING_TELEGRAM_REVIEW_LEDGER", "").strip()
    return Path(raw) if raw else Path("/var/lib/autorig-animation-fitting/telegram-review.jsonl")


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ReviewError(f"invalid review ledger JSON at line {line_no}") from exc
            if not isinstance(event, dict) or event.get("schema") != SCHEMA:
                raise ReviewError(f"invalid review ledger event at line {line_no}")
            events.append(event)
    return events


def _append_event(path: Path, event: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    fd = os.open(path, flags, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while appending Telegram review ledger")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def _locked_ledger(path: Path):
    """Serialize read/append decisions across polling and sender processes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with _LEDGER_LOCK, lock_path.open("a+b") as lock_handle:
        lock_handle.seek(0, os.SEEK_END)
        if lock_handle.tell() == 0:
            lock_handle.write(b"0")
            lock_handle.flush()
        lock_handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(lock_handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            lock_handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def register_sent_candidate(
    ledger_path: str | os.PathLike[str],
    binding: CandidateBinding,
    *,
    chat_id: int,
    message_id: int,
    sent_at: str | None = None,
) -> Mapping[str, Any]:
    path = Path(ledger_path)
    with _locked_ledger(path):
        events = _read_events(path)
        matches = [e for e in events if e.get("event") == "candidate_sent" and e.get("callbackKey") == binding.callback_key]
        for existing in matches:
            immutable = (
                existing.get("candidateId"),
                existing.get("videoSha256"),
                existing.get("mediaKind"),
                existing.get("machineQaSha256"),
            )
            expected = (
                binding.candidate_id,
                binding.video_sha256,
                binding.media_kind,
                binding.machine_qa_sha256,
            )
            if immutable != expected:
                raise ReviewError("callback key collision or immutable candidate binding mismatch")
            if int(existing.get("chatId")) == int(chat_id) and int(existing.get("messageId")) == int(message_id):
                return existing
        record = {
            "schema": SCHEMA,
            "event": "candidate_sent",
            "sentAt": sent_at or utc_now_iso(),
            "candidateId": binding.candidate_id,
            "videoSha256": binding.video_sha256,
            "mediaKind": binding.media_kind,
            "reviewStage": "reference_selection" if binding.media_kind == "raw_reference" else "fitted_visual_qa",
            "machineQaSha256": binding.machine_qa_sha256,
            "callbackKey": binding.callback_key,
            "chatId": int(chat_id),
            "messageId": int(message_id),
            "automaticLibraryApproval": False,
        }
        _append_event(path, record)
        return record


def record_candidate_markup_failure(
    ledger_path: str | os.PathLike[str],
    binding: CandidateBinding,
    *,
    chat_id: int,
    message_id: int,
    error_type: str,
    failed_at: str | None = None,
) -> Mapping[str, Any]:
    """Append a non-secret audit event when attaching buttons fails."""
    path = Path(ledger_path)
    safe_error_type = re.sub(r"[^A-Za-z0-9_.-]", "_", str(error_type or "Error"))[:80]
    with _locked_ledger(path):
        events = _read_events(path)
        registered = any(
            event.get("event") == "candidate_sent"
            and event.get("callbackKey") == binding.callback_key
            and event.get("candidateId") == binding.candidate_id
            and event.get("videoSha256") == binding.video_sha256
            and int(event.get("chatId")) == int(chat_id)
            and int(event.get("messageId")) == int(message_id)
            for event in events
        )
        if not registered:
            raise UnknownCandidate("cannot record markup failure before immutable candidate registration")
        record = {
            "schema": SCHEMA,
            "event": "candidate_markup_failed",
            "failedAt": failed_at or utc_now_iso(),
            "candidateId": binding.candidate_id,
            "videoSha256": binding.video_sha256,
            "mediaKind": binding.media_kind,
            "machineQaSha256": binding.machine_qa_sha256,
            "callbackKey": binding.callback_key,
            "chatId": int(chat_id),
            "messageId": int(message_id),
            # Persist only the exception class, never Telegram URLs/token text.
            "errorType": safe_error_type,
            "automaticLibraryApproval": False,
        }
        _append_event(path, record)
        return record


def record_decision(
    ledger_path: str | os.PathLike[str],
    *,
    callback_data_value: str,
    callback_query_id: str,
    chat_id: int,
    user_id: int,
    message_id: int,
    chat_type: str,
    owner_chat_id: int,
    owner_user_id: int,
    decided_at: str | None = None,
) -> DecisionResult:
    assert_authorized_owner(
        chat_id=chat_id,
        user_id=user_id,
        chat_type=chat_type,
        owner_chat_id=owner_chat_id,
        owner_user_id=owner_user_id,
    )
    decision, key = parse_callback_data(callback_data_value)
    callback_query_id = str(callback_query_id or "").strip()
    if not callback_query_id or len(callback_query_id) > 256:
        raise ValueError("callback_query_id must contain 1..256 characters")
    path = Path(ledger_path)

    with _locked_ledger(path):
        events = _read_events(path)
        for event in events:
            if event.get("event") == "decision" and event.get("callbackQueryId") == callback_query_id:
                replay_identity = (
                    event.get("callbackKey"),
                    event.get("decision"),
                    int(event.get("chatId")),
                    int(event.get("userId")),
                    int(event.get("messageId")),
                )
                current_identity = (key, decision, int(chat_id), int(user_id), int(message_id))
                if replay_identity != current_identity:
                    raise ReviewError("callback query id replay does not match the original decision")
                return DecisionResult(event, changed=False, duplicate=True)

        bindings = [e for e in events if e.get("event") == "candidate_sent" and e.get("callbackKey") == key]
        if not bindings:
            raise UnknownCandidate("candidate callback key is not registered")
        immutable = {
            (
                e.get("candidateId"),
                e.get("videoSha256"),
                e.get("mediaKind"),
                e.get("machineQaSha256"),
            )
            for e in bindings
        }
        if len(immutable) != 1:
            raise ReviewError("ambiguous immutable candidate binding")
        sent = next((e for e in reversed(bindings) if int(e.get("chatId")) == int(chat_id) and int(e.get("messageId")) == int(message_id)), None)
        if sent is None:
            raise UnauthorizedReviewer("callback message is not the registered candidate message")

        prior = next(
            (
                e for e in reversed(events)
                if e.get("event") == "decision"
                and e.get("callbackKey") == key
                and int(e.get("chatId")) == int(chat_id)
                and int(e.get("messageId")) == int(message_id)
            ),
            None,
        )
        if prior is not None and prior.get("decision") == decision:
            return DecisionResult(prior, changed=False, duplicate=True)

        candidate_id, video_sha256, media_kind, machine_qa_sha256 = next(iter(immutable))
        review_stage = "reference_selection" if media_kind == "raw_reference" else "fitted_visual_qa"
        if decision == APPROVE:
            decision_meaning = (
                "approved_as_fitting_reference"
                if media_kind == "raw_reference"
                else "human_visual_pass"
            )
            next_pipeline_action = (
                "fit_candidate"
                if media_kind == "raw_reference"
                else "run_release_qa_and_export"
            )
        else:
            decision_meaning = (
                "rejected_as_fitting_reference"
                if media_kind == "raw_reference"
                else "human_visual_fail"
            )
            next_pipeline_action = "stop_candidate"
        decision_id = hashlib.sha256(
            f"{SCHEMA}\0{callback_query_id}\0{key}\0{decision}".encode("utf-8")
        ).hexdigest()
        record = {
            "schema": SCHEMA,
            "event": "decision",
            "decisionId": decision_id,
            "decidedAt": decided_at or utc_now_iso(),
            "decision": decision,
            "decisionMeaning": decision_meaning,
            "nextPipelineAction": next_pipeline_action,
            "candidateId": candidate_id,
            "videoSha256": video_sha256,
            "mediaKind": media_kind,
            "reviewStage": review_stage,
            "machineQaSha256": machine_qa_sha256,
            "callbackKey": key,
            "callbackQueryId": callback_query_id,
            "chatId": int(chat_id),
            "userId": int(user_id),
            "messageId": int(message_id),
            "supersedesDecisionId": prior.get("decisionId") if prior else None,
            # Explicit safety boundary: this human reference choice may unlock a
            # fitting/export stage, but cannot itself publish a library action.
            "automaticLibraryApproval": False,
            "catalogAdmission": False,
            "requiresFittingAndReleaseQa": decision == APPROVE,
        }
        _append_event(path, record)
        return DecisionResult(record, changed=True, duplicate=False)


def decision_caption(original_caption: str | None, record: Mapping[str, Any]) -> str:
    base = re.sub(
        r"\n\n(?:✅ Утверждено как референс для фиттинга|❌ Отклонено как референс для фиттинга|✅ Ручной visual QA: PASS|❌ Ручной visual QA: FAIL).*(?:\Z)",
        "",
        str(original_caption or ""),
    ).rstrip()
    fitted = record.get("reviewStage") == "fitted_visual_qa"
    approved = record.get("decision") == APPROVE
    if fitted:
        marker = "✅ Ручной visual QA: PASS" if approved else "❌ Ручной visual QA: FAIL"
    else:
        marker = "✅ Утверждено как референс для фиттинга" if approved else "❌ Отклонено как референс для фиттинга"
    stamp = str(record.get("decidedAt") or "")
    suffix = f"\n\n{marker} · {stamp}" if stamp else f"\n\n{marker}"
    # Video captions are capped at 1024 characters by Telegram.
    return (base[: max(0, 1024 - len(suffix))].rstrip() + suffix)[:1024]
