"""Durable Telegram delivery for character-generation jobs.

Once a user presses 🎨 the result must arrive no matter what: bot restarts,
renderfin restarts, VPS reboots, network blips. Delivery therefore lives in the
renderfin service next to the persisted job state, not in a bot coroutine.

A reconciler loop walks the jobs and sends whatever has not been delivered yet.
Delivery is keyed by CONTENT (the image/video url, the error text), so a
regenerated image or a second failure is delivered again, while a restart mid
loop never double-sends the same artifact.

The bot process still owns the callback buttons; the callback_data prefixes here
must stay in sync with backend/telegram_bot.py:
    rfa/rfr/rfd  image review     (approve / regenerate / discard)
    rfs/rfd      model review     (submit / discard)
    rfe/rfr/rfd  failure recovery (resume 3D / regenerate image / discard)
"""
from __future__ import annotations

import asyncio
import html
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlsplit

import httpx

from . import config
from .models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_SUBMITTED,
    CHARGEN_STAGE_TURNTABLE,
    CharacterGenJob,
    SentMessage,
)

DELIVERY_IMAGE = "image"
DELIVERY_MODEL = "model"
DELIVERY_FAILED = "failed"
DELIVERY_RETRY = "retry"
DELIVERY_PROGRESS = "progress"

_MAX_ATTEMPTS = 6


def is_private_chat(chat_id: int) -> bool:
    """Bot API ids: a user DM is positive, groups and channels are negative.

    Cleanup is DM-only. A group is a shared log nobody asked us to rewrite.
    """
    return int(chat_id or 0) > 0

# A bot may only delete its own message while it is under 48h old. Filtering
# locally keeps every sweep from re-attempting ids the API will never accept.
DELETE_WINDOW_SECONDS = 47 * 3600
# Stages whose cards are finished business and may be taken back out of the
# chat. Deliberately a whitelist, never "not active": `ready` and `failed` are
# not active either, and their cards carry the only buttons that can move the
# job on. `submitted` is excluded too - the conversion is still running and
# that message is the reply anchor for its completion notice.
CLEANABLE_STAGES = (CHARGEN_STAGE_DISCARDED,)
SWEEP_EXEMPT_STAGES = (
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_SUBMITTED,
)


def is_configured() -> bool:
    return bool(config.TELEGRAM_BOT_TOKEN)


def _api_url(method: str) -> str:
    return f"{config.TELEGRAM_API_BASE}/bot{config.TELEGRAM_BOT_TOKEN}/{method}"


def _image_markup(job_id: str, two_variants: bool = False) -> str:
    """One button per rendered variant: whichever the user picks becomes the
    base for the 3D model."""
    if two_variants:
        rows = [
            [
                {"text": "1️⃣ 3D из первого", "callback_data": f"rfa:{job_id}:a"},
                {"text": "2️⃣ 3D из второго", "callback_data": f"rfa:{job_id}:b"},
            ],
            [
                {"text": "🔁 Перегенерировать", "callback_data": f"rfr:{job_id}"},
                {"text": "🗑 Отмена", "callback_data": f"rfd:{job_id}"},
            ],
        ]
    else:
        rows = [
            [
                {"text": "✅ Сделать 3D-модель", "callback_data": f"rfa:{job_id}:a"},
                {"text": "🔁 Перегенерировать", "callback_data": f"rfr:{job_id}"},
            ],
            [{"text": "🗑 Отмена", "callback_data": f"rfd:{job_id}"}],
        ]
    return json.dumps({"inline_keyboard": rows})


# No keyboard after the variant is chosen. Picking a style is the one decision
# asked of the operator; everything downstream runs to completion on its own,
# and a dead button in a finished chat is exactly the clutter being removed.


def _image_marker(job: CharacterGenJob) -> str:
    """Identity of the rendered pair: a regenerated variant re-delivers.

    Single-variant jobs keep the bare image url as their marker, so reviews
    already delivered before two-variant rendering existed are not re-sent.
    """
    if not job.image_url_b:
        return job.image_url
    return f"{job.image_url}|{job.image_url_b}"


def _prompt_preview(job: CharacterGenJob, limit: int = 300) -> str:
    return html.escape((job.prompt or "")[:limit])


def _stats_line(stats: Optional[Dict[str, int]]) -> str:
    """"24ч 7 | 🟢↗ +2" - the throughput half of the notification header."""
    if not stats:
        return ""
    current = int(stats.get("current_24h") or 0)
    delta = int(stats.get("delta_24h") or 0)
    if delta >= 10:
        trend = "🟢⇈"
    elif delta > 0:
        trend = "🟢↗"
    elif delta <= -10:
        trend = "🔴⇊"
    elif delta < 0:
        trend = "🔴↘"
    else:
        trend = "⚪️→"
    return f"24ч {current} | {trend} {delta:+d}"


def format_stats(job: CharacterGenJob, stats: Optional[Dict[str, int]]) -> str:
    """"#12 | 24ч 7 | 🟢↗ +2" - same shape as the site's task notifications."""
    seq = int(job.seq or 0)
    if not stats:
        return f"#{seq}" if seq else ""
    current = int(stats.get("current_24h") or 0)
    delta = int(stats.get("delta_24h") or 0)
    if delta >= 10:
        trend = "🟢⇈"
    elif delta > 0:
        trend = "🟢↗"
    elif delta <= -10:
        trend = "🔴⇊"
    elif delta < 0:
        trend = "🔴↘"
    else:
        trend = "⚪→"
    delta_str = f"+{delta}" if delta > 0 else str(delta)
    return f"#{seq} | 24ч {current} | {trend} {delta_str}"


async def _call(
    client: httpx.AsyncClient,
    method: str,
    payload: Dict[str, Any],
    *,
    files: Optional[Any] = None,
) -> Optional[dict]:
    resp = await client.post(
        _api_url(method), data=payload, files=files, timeout=90.0
    )
    if resp.status_code != 200:
        raise RuntimeError(f"{method} failed: HTTP {resp.status_code} {resp.text[:200]}")
    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"{method} rejected: {json.dumps(body)[:200]}")
    return body.get("result")


def _local_render_file(url: str) -> Optional[Path]:
    """Map one of our public Renderfin URLs back to its durable local file.

    Telegram occasionally fails to fetch a perfectly healthy HTTPS image with
    WEBPAGE_MEDIA_EMPTY. Uploading the owned file as multipart avoids that
    external fetch, but the mapping must remain fail-closed against traversal.
    """
    prefix = f"{config.PUBLIC_BASE_URL.rstrip('/')}/render/"
    if not (url or "").startswith(prefix):
        return None
    relative = unquote(urlsplit(url).path)
    public_prefix = urlsplit(prefix).path
    if not relative.startswith(public_prefix):
        return None
    relative = relative[len(public_prefix):].lstrip("/")
    root = config.RENDER_DIR.resolve()
    candidate = (root / relative).resolve()
    if candidate != root and root not in candidate.parents:
        return None
    if not candidate.is_file():
        return None
    return candidate


def _upload_part(path: Path) -> tuple[str, bytes, str]:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return path.name, path.read_bytes(), content_type


async def _delete_message(client: httpx.AsyncClient, chat_id: int, message_id: int) -> None:
    if not message_id:
        return
    try:
        await _call(client, "deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        pass  # the status message may already be gone


def _message_ids(result: Any) -> List[int]:
    """Ids out of a Bot API result.

    sendMediaGroup answers with a LIST of messages while every other method
    answers with one; reading .get("message_id") off the list raises.
    """
    if isinstance(result, list):
        return [int(m.get("message_id") or 0) for m in result if isinstance(m, dict)]
    if isinstance(result, dict):
        return [int(result.get("message_id") or 0)]
    return []


async def deliver_image_review(
    client: httpx.AsyncClient, job: CharacterGenJob, stats: Optional[Dict[str, int]] = None
) -> List[int]:
    """Send the rendered variants and ask which one becomes the 3D model."""
    header = (
        f"🖼 <b>T-поза готова</b>\n"
        f"<code>{format_stats(job, stats)}</code>\n"
        f"<i>{_prompt_preview(job)}</i>"
    )
    if job.warning:
        header += f"\n⚠️ {html.escape(job.warning[:150])}"

    if job.image_url_b:
        # both styles in one album, then the choice under it
        local_a = _local_render_file(job.image_url)
        local_b = _local_render_file(job.image_url_b)
        use_upload = local_a is not None and local_b is not None
        media = [
            {
                "type": "photo",
                "media": "attach://variant_a" if use_upload else job.image_url,
                "caption": header + "\n1️⃣ базовый стиль",
                "parse_mode": "HTML",
            },
            {
                "type": "photo",
                "media": "attach://variant_b" if use_upload else job.image_url_b,
                "caption": "2️⃣ low-poly cartoon PBR",
                "parse_mode": "HTML",
            },
        ]
        files = None
        if use_upload:
            files = {
                "variant_a": _upload_part(local_a),
                "variant_b": _upload_part(local_b),
            }
        album = await _call(client, "sendMediaGroup", {
            "chat_id": job.telegram_chat_id,
            "media": json.dumps(media),
        }, files=files)
        result = await _call(client, "sendMessage", {
            "chat_id": job.telegram_chat_id,
            "text": (
                "Какой вариант отправляем в 3D?\n"
                f'✂️ <a href="{html.escape(job.isolated_url)}">альфа 1</a> · '
                f'<a href="{html.escape(job.isolated_url_b)}">альфа 2</a>'
            ),
            "parse_mode": "HTML",
            "reply_markup": _image_markup(job.id, two_variants=True),
        })
        return _message_ids(album) + _message_ids(result)

    payload = {
        "chat_id": job.telegram_chat_id,
        "photo": job.image_url,
        "caption": header + f'\n✂️ <a href="{html.escape(job.isolated_url)}">PNG с альфой</a>',
        "parse_mode": "HTML",
        "reply_markup": _image_markup(job.id),
    }
    local_image = _local_render_file(job.image_url)
    files = None
    if local_image is not None:
        payload.pop("photo")
        files = {"photo": _upload_part(local_image)}
    result = await _call(client, "sendPhoto", payload, files=files)
    return _message_ids(result)


async def deliver_model_review(
    client: httpx.AsyncClient, job: CharacterGenJob, stats: Optional[Dict[str, int]] = None
) -> List[int]:
    collection_line = ""
    if job.collection_guid:
        collection_line = (
            f"🧩 <b>{html.escape(job.collection_title[:120])}</b> · "
            f"{int(job.collection_index or 0)}/{int(job.collection_size or 0)}\n"
            f"<b>{html.escape(job.collection_member_title[:120])}</b>\n"
        )
    caption = (
        f"🎨 <b>3D-модель готова</b>\n"
        f"{collection_line}"
        f"<code>{format_stats(job, stats)}</code>\n"
        f"<i>{_prompt_preview(job)}</i>\n"
        f'🧊 <a href="{html.escape(job.glb_url)}">GLB</a>'
    )
    result = await _call(client, "sendVideo", {
        "chat_id": job.telegram_chat_id,
        "video": job.video_url,
        "caption": caption,
        "parse_mode": "HTML",
        "supports_streaming": "true",
    })
    return _message_ids(result)


DIGEST_STATE_PATH = config.DB_DIR / "digest_messages.json"
DIGEST_MAX_ROWS = int(os.getenv("RENDERFIN_DIGEST_MAX_ROWS", "25"))


# The operator asked for a restart button "to clean up and refresh the
# statuses". That is exactly what the startup sweep does, so the button runs
# the sweep rather than bouncing a production service from a chat message -
# same outcome, without handing a Telegram button the power to take the
# pipeline down mid-generation.
DIGEST_REFRESH_CALLBACK = "rfq:refresh"


def _digest_markup() -> str:
    return json.dumps({
        "inline_keyboard": [[
            {"text": "🔄 Очистить и обновить", "callback_data": DIGEST_REFRESH_CALLBACK}
        ]]
    })


def _digest_state() -> Dict[str, Any]:
    try:
        return json.loads(DIGEST_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_digest_state(state: Dict[str, Any]) -> None:
    try:
        DIGEST_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = DIGEST_STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        tmp.replace(DIGEST_STATE_PATH)
    except Exception as exc:
        print(f"[Renderfin][Delivery] could not persist digest state: {exc}")


def digest_text(jobs: List[CharacterGenJob], stats: Optional[Dict[str, int]] = None) -> str:
    """The whole queue in one message: what is running and how far along.

    One message per job was still a wall of text once thirty were in flight.
    What the operator asked to see is the queue, so that is what this is.
    """
    if not jobs:
        return ""
    lines = [f"🧩 <b>В работе: {len(jobs)}</b>"]
    if stats:
        summary = f"✅ {int(stats.get('done') or 0)} готово"
        if stats.get("done_24h"):
            summary += f" · {int(stats['done_24h'])} за 24ч"
        if stats.get("failed"):
            summary += f" · ❌ {int(stats['failed'])}"
        summary += f" · {_stats_line(stats)}"
        lines.append(f"<code>{summary}</code>")
    # Telegram caps a message at 4096 characters, so a queue long enough to
    # reach that would silently fail to send. The tail is summarised instead.
    ordered = sorted(jobs, key=lambda j: int(j.seq or 0))
    shown, hidden = ordered[:DIGEST_MAX_ROWS], ordered[DIGEST_MAX_ROWS:]
    for job in shown:
        stage = _STAGE_LABELS.get(job.stage, job.stage)
        row = f"#{int(job.seq or 0)} · {html.escape(stage)}"
        attempt = int((job.attempts or {}).get(job.stage, 0))
        if attempt:
            row += f" · попытка {attempt + 1}"
        if job.retry_at and job.last_error:
            row += " · жду воркер" if "worker" in job.last_error.lower() else " · повтор"
        if job.collection_guid:
            subject = (
                f"{int(job.collection_index or 0)}/{int(job.collection_size or 0)} "
                f"{job.collection_member_title}"
            )[:64]
        else:
            subject = (job.prompt or "").split(",")[0].strip()[:48]
        if subject:
            row += f" — <i>{html.escape(subject)}</i>"
        lines.append(row)
    if hidden:
        by_stage: Dict[str, int] = {}
        for job in hidden:
            label = _STAGE_LABELS.get(job.stage, job.stage)
            by_stage[label] = by_stage.get(label, 0) + 1
        tail = ", ".join(f"{n}× {html.escape(k)}" for k, n in sorted(by_stage.items()))
        lines.append(f"… и ещё {len(hidden)}: {tail}")
    return "\n".join(lines)


def progress_text(job: CharacterGenJob, stats: Optional[Dict[str, int]] = None) -> str:
    """What this job is doing right now, in one line plus its subject.

    Every stage change rewrites this same message instead of sending a new
    one, so a job occupies exactly one line of the chat from start to finish.
    """
    stage = _STAGE_LABELS.get(job.stage, job.stage)
    attempt = int((job.attempts or {}).get(job.stage, 0))
    line = f"⏳ <b>{html.escape(stage)}</b>"
    if attempt:
        line += f" · попытка {attempt + 1}"
    if job.retry_at and job.last_error:
        line += " · жду воркер" if "worker" in job.last_error.lower() else " · повтор"
    return (
        f"{line}\n"
        f"<code>{format_stats(job, stats)}</code>\n"
        f"<i>{_prompt_preview(job, 200)}</i>"
    )


async def deliver_progress(
    client: httpx.AsyncClient, job: CharacterGenJob, stats: Optional[Dict[str, int]] = None
) -> List[int]:
    """Create or update this job's single progress message."""
    text = progress_text(job, stats)
    if job.telegram_status_message_id:
        try:
            await _call(client, "editMessageText", {
                "chat_id": job.telegram_chat_id,
                "message_id": job.telegram_status_message_id,
                "text": text,
                "parse_mode": "HTML",
            })
            return []
        except Exception as exc:
            # "message is not modified" is normal and means the state matched
            if "not modified" in str(exc).lower():
                return []
            # the message is gone: fall through and make a new one
    result = await _call(client, "sendMessage", {
        "chat_id": job.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
    })
    return _message_ids(result)


async def deliver_retry_notice(client: httpx.AsyncClient, job: CharacterGenJob) -> List[int]:
    """Tell the owner a stage is being retried, so a long wait is not silence."""
    stage = _STAGE_LABELS.get(job.stage, job.stage)
    attempt = int((job.attempts or {}).get(job.stage, 0))
    text = (
        f"🔁 <b>Повторяю: {html.escape(stage)}</b> (попытка {attempt + 1})\n"
        f"<i>{_prompt_preview(job, 160)}</i>\n"
        f"{html.escape((job.last_error or '')[:200])}"
    )
    result = await _call(client, "sendMessage", {
        "chat_id": job.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
    })
    return _message_ids(result)


async def deliver_failure(client: httpx.AsyncClient, job: CharacterGenJob) -> List[int]:
    collection_line = ""
    if job.collection_guid:
        collection_line = (
            f"🧩 {html.escape(job.collection_title[:100])} · "
            f"{int(job.collection_index or 0)}/{int(job.collection_size or 0)} · "
            f"{html.escape(job.collection_member_title[:100])}\n"
        )
    text = (
        f"❌ <b>Генерация не удалась</b>\n"
        f"{collection_line}"
        f"<code>#{int(job.seq or 0)}</code>\n"
        f"<i>{_prompt_preview(job, 200)}</i>\n"
        f"{html.escape((job.error or '')[:250])}\n"
        "«Повторить 3D» продолжит с этого места, «Перегенерировать» — новая картинка."
    )
    result = await _call(client, "sendMessage", {
        "chat_id": job.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
    })
    return _message_ids(result)


_STAGE_LABELS = {
    "flux_render": "рендер T-позы",
    "awaiting_image_approval": "ждёт вашего выбора",
    "ready": "готова к отправке",
    "hunyuan": "3D-модель",
    "turntable": "видео-облёт",
    "submitted": "полный пайплайн",
}


# Stages that keep a live progress line. `submitted` is included: the
# conversion is the longest part of the job and silence there is what made the
# chat impossible to read.
# What the operator asked to see in the chat: work that is not finished.
# awaiting_image_approval is included because it is waiting on THEM.
UNFINISHED_STAGES = (
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_TURNTABLE,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_SUBMITTED,
)

PROGRESS_STAGES = (
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_TURNTABLE,
    CHARGEN_STAGE_SUBMITTED,
)


def stale_kinds(job: CharacterGenJob) -> set:
    """Delivery kinds whose cards no longer describe this job.

    The two renders and their buttons are meaningless once a variant has been
    picked; a failure notice is meaningless once the stage is running again.
    Removing them is what keeps the chat to one line per live job.
    """
    stale = set()
    if job.stage != CHARGEN_STAGE_AWAITING_IMAGE:
        stale.add(DELIVERY_IMAGE)
    if job.stage != CHARGEN_STAGE_FAILED:
        stale.add(DELIVERY_FAILED)
    return stale


def _progress_marker(job: CharacterGenJob) -> str:
    """Identity of the progress text, so it is rewritten only when it changed."""
    return (
        f"{job.stage}:{(job.attempts or {}).get(job.stage, 0)}:"
        f"{1 if job.retry_at else 0}"
    )


def pending_delivery(job: CharacterGenJob) -> Optional[str]:
    """Which delivery (if any) this job still owes its chat."""
    if not job.telegram_chat_id:
        return None
    delivered = job.delivered or {}
    # A running job keeps ONE line in the chat that is rewritten as it moves.
    # Retries used to arrive as their own message each time, which is most of
    # what made the chat unreadable.
    if job.stage == CHARGEN_STAGE_AWAITING_IMAGE and job.image_url:
        if delivered.get(DELIVERY_IMAGE) != _image_marker(job):
            return DELIVERY_IMAGE
    if job.stage == CHARGEN_STAGE_READY and job.video_url:
        if delivered.get(DELIVERY_MODEL) != job.video_url:
            return DELIVERY_MODEL
    if job.stage == CHARGEN_STAGE_FAILED and job.error:
        if delivered.get(DELIVERY_FAILED) != job.error:
            return DELIVERY_FAILED
    return None


class TelegramDeliveryService:
    """Reconciler that keeps sending until every job has been delivered."""

    def __init__(self, manager, *, client: Optional[httpx.AsyncClient] = None):
        self.manager = manager
        self._client = client
        self._own_client = client is None
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._attempts: Dict[str, int] = {}
        self._next_try: Dict[str, float] = {}

    async def start(self) -> None:
        if not is_configured():
            print("[Renderfin][Delivery] TELEGRAM_BOT_TOKEN not set — delivery disabled")
            return
        if self._client is None:
            self._client = httpx.AsyncClient(follow_redirects=True)
        self._stopped.clear()
        try:
            await self.sweep_private_chats()
        except Exception as exc:
            print(f"[Renderfin][Delivery] startup sweep failed: {exc}")
        self._task = asyncio.create_task(self._loop())
        print("[Renderfin][Delivery] telegram delivery reconciler started")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._own_client and self._client:
            await self._client.aclose()
            self._client = None

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            for pass_name, run_pass in (
                ("delivery", self.tick),
                ("queue", self.digest_tick),
                ("cleanup", self.cleanup_tick),
            ):
                try:
                    await run_pass()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"[Renderfin][Delivery] {pass_name} pass error: {exc}")
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=config.DELIVERY_TICK_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    async def _forget(self, job: CharacterGenJob, deleted: List[int], dead: List[int]) -> None:
        keep = [m for m in job.telegram_messages if m.id not in set(deleted) | set(dead)]
        undeletable = list(dict.fromkeys(list(job.telegram_undeletable) + dead))
        await self.manager.set_telegram_messages(
            job.id, keep, undeletable=undeletable
        )

    async def cleanup_chat(
        self, job: CharacterGenJob, only: Optional[set] = None, sweep_all: bool = False
    ) -> int:
        """Take this job's own messages back out of a private chat.

        `only` limits the sweep to certain delivery kinds, which is how a card
        whose moment has passed is removed while the job is still running.
        Returns how many were removed. Never raises: cleanup is hygiene, and a
        chat that refuses a delete must not stop anything else from happening.
        """
        if self._client is None or not job.telegram_messages:
            return 0
        if not is_private_chat(job.telegram_chat_id):
            return 0
        if only is not None and not only:
            return 0
        now = time.time()
        refused = set(job.telegram_undeletable or [])
        deleted: List[int] = []
        dead: List[int] = []
        for message in list(job.telegram_messages):
            if message.id in refused:
                continue
            if not sweep_all and only is not None and message.kind not in only:
                continue
            if now - message.at > DELETE_WINDOW_SECONDS:
                # past the window Telegram allows; stop asking
                dead.append(message.id)
                continue
            try:
                await _call(
                    self._client,
                    "deleteMessage",
                    {"chat_id": job.telegram_chat_id, "message_id": message.id},
                )
                deleted.append(message.id)
            except Exception as exc:
                # already gone, too old, or otherwise permanently refused:
                # retrying every tick forever would only flood the API
                dead.append(message.id)
                print(
                    f"[Renderfin][Delivery] cannot delete {message.id} in "
                    f"{job.telegram_chat_id}: {exc}"
                )
        if deleted or dead:
            await self._forget(job, deleted, dead)
        if sweep_all:
            await self.manager.mark_delivered(job.id, DELIVERY_IMAGE, "")
            await self.manager.mark_delivered(job.id, DELIVERY_MODEL, "")
            await self.manager.mark_delivered(job.id, DELIVERY_FAILED, "")
        if deleted:
            print(
                f"[Renderfin][Delivery] cleaned {len(deleted)} message(s) for "
                f"job {job.id} ({job.stage})"
            )
        return len(deleted)

    async def sweep_private_chats(self) -> int:
        """Empty the private chats of everything this pipeline put there.

        Run once at startup. The operator asked for a chat that shows the
        current queue and nothing else, so a restart wipes what is there and
        the next pass re-posts only the unfinished work. Cards recorded before
        delivery kinds existed carry no kind at all, which is why a rule based
        on kinds could never reach them and they accumulated.
        """
        if self._client is None:
            return 0
        removed = 0
        for job in list(self.manager.all_jobs()):
            if not job.telegram_messages or not is_private_chat(job.telegram_chat_id):
                continue
            if job.stage in SWEEP_EXEMPT_STAGES:
                continue
            try:
                removed += await self.cleanup_chat(job, sweep_all=True)
            except Exception as exc:
                print(f"[Renderfin][Delivery] sweep failed for {job.id}: {exc}")

        # drop the previous queue message too: it is re-posted below the clean slate
        state = _digest_state()
        for chat_id, entry in list(state.items()):
            message_id = int((entry or {}).get("id") or 0)
            if message_id:
                await _delete_message(self._client, int(chat_id), message_id)
        _save_digest_state({})
        print(f"[Renderfin][Delivery] startup sweep removed {removed} message(s)")
        return removed

    async def digest_tick(self) -> None:
        """Keep one message per chat listing the unfinished queue."""
        if self._client is None:
            return
        by_chat: Dict[int, List[CharacterGenJob]] = {}
        for job in self.manager.all_jobs():
            if job.stage in UNFINISHED_STAGES and is_private_chat(job.telegram_chat_id):
                by_chat.setdefault(int(job.telegram_chat_id), []).append(job)

        stats = None
        try:
            stats = self.manager.stats()
        except Exception:
            stats = None

        state = _digest_state()
        for chat_id in set(list(by_chat) + [int(c) for c in state]):
            jobs = by_chat.get(chat_id) or []
            entry = state.get(str(chat_id)) or {}
            message_id = int(entry.get("id") or 0)
            text = digest_text(jobs, stats)

            if not text:
                # nothing left to show: an empty queue means an empty chat
                if message_id:
                    await _delete_message(self._client, chat_id, message_id)
                    state.pop(str(chat_id), None)
                continue
            if entry.get("text") == text:
                continue
            if message_id:
                try:
                    await _call(self._client, "editMessageText", {
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "reply_markup": _digest_markup(),
                    })
                    state[str(chat_id)] = {"id": message_id, "text": text}
                    continue
                except Exception as exc:
                    if "not modified" in str(exc).lower():
                        state[str(chat_id)] = {"id": message_id, "text": text}
                        continue
                    # the message is gone; fall through and post a new one
            result = await _call(self._client, "sendMessage", {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": _digest_markup(),
            })
            ids = _message_ids(result)
            if ids:
                state[str(chat_id)] = {"id": ids[-1], "text": text}
        _save_digest_state(state)

    async def cleanup_tick(self) -> None:
        """Sweep finished jobs and stale cards. Isolated so it cannot wedge delivery."""
        if self._client is None:
            return
        for job in list(self.manager.all_jobs()):
            if not job.telegram_messages:
                continue
            try:
                if job.stage in CLEANABLE_STAGES:
                    await self.cleanup_chat(job)
                else:
                    await self.cleanup_chat(job, only=stale_kinds(job))
            except Exception as exc:
                print(f"[Renderfin][Delivery] cleanup failed for {job.id}: {exc}")

    async def tick(self) -> None:
        """One reconciliation pass (kept separate for tests)."""
        if self._client is None:
            return
        now = time.time()
        for job in list(self.manager.all_jobs()):
            kind = pending_delivery(job)
            if kind is None:
                continue
            key = f"{job.id}:{kind}"
            if self._next_try.get(key, 0) > now:
                continue
            try:
                await self._deliver(job, kind)
                self._attempts.pop(key, None)
                self._next_try.pop(key, None)
            except Exception as exc:
                attempts = self._attempts.get(key, 0) + 1
                self._attempts[key] = attempts
                # exponential backoff, but never give up on a delivery the user
                # is waiting for: cap the delay instead of dropping the job
                delay = min(300.0, config.DELIVERY_TICK_SECONDS * (2 ** attempts))
                self._next_try[key] = now + delay
                if attempts <= _MAX_ATTEMPTS or attempts % 20 == 0:
                    print(
                        f"[Renderfin][Delivery] {kind} for job {job.id} failed "
                        f"(attempt {attempts}, retry in {int(delay)}s): {exc}"
                    )

    async def _deliver(self, job: CharacterGenJob, kind: str) -> None:
        assert self._client is not None
        if kind == DELIVERY_PROGRESS:
            stats = None
            try:
                stats = self.manager.stats()
            except Exception:
                stats = None
            sent = await deliver_progress(self._client, job, stats)
            if sent:
                # a brand new progress line; an edit returns nothing
                await self.manager.record_messages(job.id, sent, kind=DELIVERY_PROGRESS)
                await self.manager.set_status_message(job.id, sent[-1])
            await self.manager.mark_delivered(
                job.id, kind, _progress_marker(job), message_id=0
            )
            return
        stats = None
        try:
            stats = self.manager.stats()
        except Exception:
            stats = None
        if kind == DELIVERY_IMAGE:
            sent = await deliver_image_review(self._client, job, stats)
            marker = _image_marker(job)
        elif kind == DELIVERY_MODEL:
            sent = await deliver_model_review(self._client, job, stats)
            marker = job.video_url
        else:
            sent = await deliver_failure(self._client, job)
            marker = job.error
        # record before anything else can fail: an id we sent but did not write
        # down is a card that can never be cleaned up
        await self.manager.record_messages(job.id, sent, kind=kind)
        # the progress line is NOT deleted here: it is the job's one line in
        # the chat and it keeps reporting until the job is finished
        await self.manager.mark_delivered(
            job.id,
            kind,
            marker,
            message_id=(sent[-1] if sent else 0),
        )
        print(f"[Renderfin][Delivery] {kind} delivered for job {job.id}")
