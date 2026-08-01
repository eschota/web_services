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
import time
from typing import Any, Dict, List, Optional

import httpx

from . import config
from .models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_READY,
    CharacterGenJob,
    SentMessage,
)

DELIVERY_IMAGE = "image"
DELIVERY_MODEL = "model"
DELIVERY_FAILED = "failed"
DELIVERY_RETRY = "retry"

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


def _model_markup(job_id: str) -> str:
    return json.dumps({
        "inline_keyboard": [
            [
                {"text": "✅ Сабмитить", "callback_data": f"rfs:{job_id}"},
                {"text": "🗑 Удалить", "callback_data": f"rfd:{job_id}"},
            ]
        ]
    })


def _retry_markup(job_id: str) -> str:
    return json.dumps({
        "inline_keyboard": [
            [
                {"text": "♻️ Повторить 3D", "callback_data": f"rfe:{job_id}"},
                {"text": "🔁 Перегенерировать", "callback_data": f"rfr:{job_id}"},
            ],
            [{"text": "🗑 Отмена", "callback_data": f"rfd:{job_id}"}],
        ]
    })


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


async def _call(client: httpx.AsyncClient, method: str, payload: Dict[str, Any]) -> Optional[dict]:
    resp = await client.post(_api_url(method), data=payload, timeout=90.0)
    if resp.status_code != 200:
        raise RuntimeError(f"{method} failed: HTTP {resp.status_code} {resp.text[:200]}")
    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"{method} rejected: {json.dumps(body)[:200]}")
    return body.get("result")


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
        media = [
            {
                "type": "photo",
                "media": job.image_url,
                "caption": header + "\n1️⃣ базовый стиль",
                "parse_mode": "HTML",
            },
            {
                "type": "photo",
                "media": job.image_url_b,
                "caption": "2️⃣ low-poly cartoon PBR",
                "parse_mode": "HTML",
            },
        ]
        album = await _call(client, "sendMediaGroup", {
            "chat_id": job.telegram_chat_id,
            "media": json.dumps(media),
        })
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

    result = await _call(client, "sendPhoto", {
        "chat_id": job.telegram_chat_id,
        "photo": job.image_url,
        "caption": header + f'\n✂️ <a href="{html.escape(job.isolated_url)}">PNG с альфой</a>',
        "parse_mode": "HTML",
        "reply_markup": _image_markup(job.id),
    })
    return _message_ids(result)


async def deliver_model_review(
    client: httpx.AsyncClient, job: CharacterGenJob, stats: Optional[Dict[str, int]] = None
) -> List[int]:
    caption = (
        f"🎨 <b>3D-модель готова</b>\n"
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
        "reply_markup": _model_markup(job.id),
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
    text = (
        f"❌ <b>Генерация не удалась</b>\n"
        f"<code>#{int(job.seq or 0)}</code>\n"
        f"<i>{_prompt_preview(job, 200)}</i>\n"
        f"{html.escape((job.error or '')[:250])}\n"
        "«Повторить 3D» продолжит с этого места, «Перегенерировать» — новая картинка."
    )
    result = await _call(client, "sendMessage", {
        "chat_id": job.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": _retry_markup(job.id),
    })
    return _message_ids(result)


_STAGE_LABELS = {
    "flux_render": "рендер T-позы",
    "hunyuan": "3D-модель",
    "turntable": "видео-облёт",
}


def pending_delivery(job: CharacterGenJob) -> Optional[str]:
    """Which delivery (if any) this job still owes its chat."""
    if not job.telegram_chat_id:
        return None
    delivered = job.delivered or {}
    # an automatic retry is progress worth reporting, not silence
    if job.retry_at and job.last_error:
        marker = f"{job.stage}:{(job.attempts or {}).get(job.stage, 0)}"
        if delivered.get(DELIVERY_RETRY) != marker:
            return DELIVERY_RETRY
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

    async def cleanup_chat(self, job: CharacterGenJob) -> int:
        """Take this job's own messages back out of a private chat.

        Returns how many were removed. Never raises: cleanup is hygiene, and a
        chat that refuses a delete must not stop anything else from happening.
        """
        if self._client is None or not job.telegram_messages:
            return 0
        if not is_private_chat(job.telegram_chat_id):
            return 0
        now = time.time()
        refused = set(job.telegram_undeletable or [])
        deleted: List[int] = []
        dead: List[int] = []
        for message in list(job.telegram_messages):
            if message.id in refused:
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
        if deleted:
            print(
                f"[Renderfin][Delivery] cleaned {len(deleted)} message(s) for "
                f"job {job.id} ({job.stage})"
            )
        return len(deleted)

    async def cleanup_tick(self) -> None:
        """Sweep finished jobs. Isolated from delivery so it cannot wedge it."""
        if self._client is None:
            return
        for job in list(self.manager.all_jobs()):
            if job.stage not in CLEANABLE_STAGES or not job.telegram_messages:
                continue
            try:
                await self.cleanup_chat(job)
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
        if kind == DELIVERY_RETRY:
            sent = await deliver_retry_notice(self._client, job)
            await self.manager.record_messages(job.id, sent)
            marker = f"{job.stage}:{(job.attempts or {}).get(job.stage, 0)}"
            await self.manager.mark_delivered(job.id, kind, marker, message_id=0)
            print(f"[Renderfin][Delivery] retry notice sent for job {job.id}")
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
        await self.manager.record_messages(job.id, sent)
        # the interim "⏳ …" message has served its purpose
        if job.telegram_status_message_id:
            await _delete_message(
                self._client, job.telegram_chat_id, job.telegram_status_message_id
            )
        await self.manager.mark_delivered(
            job.id,
            kind,
            marker,
            message_id=(sent[-1] if sent else 0),
            clear_status_message=True,
        )
        print(f"[Renderfin][Delivery] {kind} delivered for job {job.id}")
