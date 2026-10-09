"""Authenticated, replay-safe receiver for Motion Transfer V3 callbacks.

The router is dependency-injected so importing it cannot touch the production
database or Telegram.  Wiring supplies an explicit V3 dispatch binding, an
atomic callback ledger, the credential resolver, and the existing idempotent
task-completion broadcaster.
"""
from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol

from fastapi import APIRouter, HTTPException, Request

from v3_pipeline_adapter import V3ProtocolError, V3State, V3Submission, parse_status


CALLBACK_SCHEMA = "autorig.v3.callback/1"
RECEIPT_SCHEMA = "autorig.v3.callback-receipt/1"
MAX_CALLBACK_BYTES = 2 * 1024 * 1024
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EVENT_ID = re.compile(r"^v3cb-[0-9a-f]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_CREDENTIAL_ID = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


class CallbackReceiverError(ValueError):
    pass


class CallbackConflict(CallbackReceiverError):
    pass


@dataclass(frozen=True)
class CallbackBinding:
    task_id: str
    run_id: str
    source_sha256: str
    attempt: int
    idempotency_key: str
    credential_id: str
    pipeline_kind: str = "v3"


@dataclass(frozen=True)
class CallbackCommit:
    receipt_id: str
    duplicate: bool
    notification_required: bool


class CallbackTransaction(Protocol):
    async def accept(
        self,
        binding: CallbackBinding,
        event_id: str,
        payload: Mapping[str, Any],
        terminal_status: Any | None,
    ) -> CallbackCommit: ...

    async def mark_notified(self, binding: CallbackBinding, event_id: str) -> None: ...


SecretProvider = Callable[[str], str | bytes | Awaitable[str | bytes]]
BindingStore = Callable[[str], CallbackBinding | None | Awaitable[CallbackBinding | None]]
DoneNotifier = Callable[[str], Any | Awaitable[Any]]


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def callback_event_id(payload: Mapping[str, Any]) -> str:
    return "v3cb-" + hashlib.sha256(_canonical(payload)).hexdigest()


def _validated_payload(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_CALLBACK_BYTES:
        raise CallbackReceiverError("callback body size is invalid")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CallbackReceiverError("callback body is not JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != CALLBACK_SCHEMA:
        raise CallbackReceiverError("unsupported callback schema")
    if raw != _canonical(payload):
        raise CallbackReceiverError("callback body must use canonical JSON")
    if payload.get("event") not in {"progress", "terminal"}:
        raise CallbackReceiverError("callback event is invalid")
    sequence = payload.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise CallbackReceiverError("callback sequence is invalid")
    return payload


def _verify_signature(raw: bytes, signature: str, secret: str | bytes) -> None:
    if not isinstance(secret, (str, bytes)) or not secret:
        raise CallbackReceiverError("callback credential is unavailable")
    if not re.fullmatch(r"v1=[0-9a-f]{64}", signature or ""):
        raise CallbackReceiverError("callback signature is invalid")
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    expected = hmac.new(key, raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature[3:], expected):
        raise CallbackReceiverError("callback signature is invalid")


def _match_binding(payload: Mapping[str, Any], binding: CallbackBinding, credential_id: str) -> None:
    try:
        task_id = str(uuid.UUID(str(payload.get("task_id"))))
    except (ValueError, TypeError, AttributeError) as exc:
        raise CallbackReceiverError("task_id is invalid") from exc
    if task_id != payload.get("task_id"):
        raise CallbackReceiverError("task_id must be canonical")
    if binding.pipeline_kind != "v3":
        raise CallbackConflict("task is not bound to the V3 pipeline")
    expected = {
        "task_id": binding.task_id,
        "run_id": binding.run_id,
        "source_sha256": binding.source_sha256,
        "attempt": binding.attempt,
        "idempotency_key": binding.idempotency_key,
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise CallbackConflict(f"callback {field} does not match the dispatch binding")
    if credential_id != binding.credential_id:
        raise CallbackConflict("callback credential does not match the dispatch binding")
    if not _RUN_ID.fullmatch(binding.run_id):
        raise CallbackConflict("stored run_id is invalid")
    if not _SHA256.fullmatch(binding.source_sha256):
        raise CallbackConflict("stored source_sha256 is invalid")


def _validate_event(payload: Mapping[str, Any], binding: CallbackBinding) -> Any | None:
    if payload["event"] == "progress":
        stage = str(payload.get("stage") or "").strip()
        status = str(payload.get("status") or "").strip().lower()
        progress = payload.get("progress")
        if not stage or len(stage) > 100 or status not in {"queued", "running", "needs_review"}:
            raise CallbackReceiverError("progress stage or status is invalid")
        if isinstance(progress, bool) or not isinstance(progress, (int, float)) or not 0 <= progress <= 1:
            raise CallbackReceiverError("progress value is invalid")
        return None

    status = str(payload.get("status") or "").strip().lower()
    if status == "partial":
        status = "needs_review"
    if status not in {"done", "needs_review", "failed"}:
        raise CallbackReceiverError("terminal status is invalid")
    if status != "done":
        return None

    submission = V3Submission(
        binding.task_id,
        binding.source_sha256,
        binding.attempt,
        binding.idempotency_key,
    )
    dispatch_status = {
        "schema": "autorig.v3.dispatch/1",
        "task_id": binding.task_id,
        "run_id": binding.run_id,
        "source_sha256": binding.source_sha256,
        "attempt": binding.attempt,
        "idempotency_key": binding.idempotency_key,
        "status": "done",
        "qa": payload.get("qa"),
        "artifact_manifest": payload.get("artifact_manifest"),
        "artifact_manifest_sha256": payload.get("artifact_manifest_sha256"),
    }
    result = parse_status(dispatch_status, submission, _allow_unverified=True)
    if result.state is not V3State.DONE or result.qa_status != "accepted":
        raise CallbackReceiverError("done callback has not passed V3 QA")
    return result


def create_v3_callback_router(
    *,
    secret_provider: SecretProvider,
    binding_store: BindingStore,
    transaction: CallbackTransaction,
    notify_done: DoneNotifier,
) -> APIRouter:
    """Build the internal receiver without importing production globals."""
    router = APIRouter()

    @router.post("/internal/v3/task-callbacks")
    async def receive_v3_task_callback(request: Request) -> dict[str, Any]:
        raw = await request.body()
        credential_id = request.headers.get("X-AutoRig-V3-Credential", "")
        signature = request.headers.get("X-AutoRig-V3-Signature", "")
        supplied_event_id = request.headers.get("Idempotency-Key", "")
        if not _CREDENTIAL_ID.fullmatch(credential_id) or not _EVENT_ID.fullmatch(supplied_event_id):
            raise HTTPException(status_code=401, detail="Invalid V3 callback authentication")
        try:
            secret = await _resolve(secret_provider(credential_id))
            _verify_signature(raw, signature, secret)
        except CallbackReceiverError as exc:
            raise HTTPException(status_code=401, detail="Invalid V3 callback authentication") from exc

        try:
            payload = _validated_payload(raw)
            event_id = callback_event_id(payload)
            if supplied_event_id != event_id:
                raise CallbackConflict("Idempotency-Key does not match callback body")
            task_id = str(payload.get("task_id") or "")
            binding = await _resolve(binding_store(task_id))
            if not isinstance(binding, CallbackBinding):
                raise CallbackConflict("task has no active V3 dispatch binding")
            _match_binding(payload, binding, credential_id)
            terminal_status = _validate_event(payload, binding)
            commit = await transaction.accept(binding, event_id, payload, terminal_status)
            if not isinstance(commit, CallbackCommit) or not commit.receipt_id:
                raise RuntimeError("callback transaction returned no durable receipt")
        except (CallbackConflict, V3ProtocolError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except CallbackReceiverError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if commit.notification_required:
            try:
                await _resolve(notify_done(binding.task_id))
                await transaction.mark_notified(binding, event_id)
            except Exception as exc:
                # The terminal state is already committed.  A retry resumes the
                # still-pending notification through the same event receipt.
                raise HTTPException(status_code=503, detail="Task completion notification is pending") from exc

        return {
            "schema": RECEIPT_SCHEMA,
            "event_id": event_id,
            "accepted": True,
            "receipt_id": commit.receipt_id,
            "duplicate": commit.duplicate,
        }

    return router

