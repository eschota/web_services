"""Telegram bot integration for AutoRig Online.

- Polling bot (python-telegram-bot) with /start to subscribe a chat.
- Broadcast helpers for task events.
- Server startup notifications with statistics.

Token is read via config.TELEGRAM_BOT_TOKEN (env + optional /etc/autorig-*.env loaded in config.py).
"""

from __future__ import annotations

import os
import asyncio
import hashlib
import html
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from sqlalchemy import select, func, update, case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, TelegramChat, TelegramNotification, Task, SupportChatSession, SupportChatMessage
from config import (
    APP_URL,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_NOTIFICATION_CHAT_ID,
)
from workers import get_worker_base_url


def _get_token() -> str | None:
    # Prefer live os.environ (systemd merges EnvironmentFile before exec); fallback to cached config.
    tok = (os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or (TELEGRAM_BOT_TOKEN or "").strip())
    return tok or None


def _task_url(task_id: str) -> str:
    """Task URL with cache-busting parameter for fresh Telegram previews."""
    import time
    base = (APP_URL or "").rstrip("/")
    ts = int(time.time())
    return f"{base}/task?id={task_id}&t={ts}"


def _sanitize_error_for_telegram(message: str | None) -> str:
    """Keep operator error notifications useful without leaking raw paths or tokens."""
    text = (message or "Task failed").replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"https?://\S+", "<url>", text)
    text = re.sub(r"[A-Za-z]:\\[^\s]+", "<path>", text)
    text = re.sub(r"(?<!\w)/(?:[^\s]+/)+[^\s]+", "<path>", text)
    text = re.sub(r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b", "<token>", text)
    text = re.sub(r"\s+", " ", text).strip()
    return (text or "Task failed")[:500]


def _format_content_rating_line(rating: str | None) -> str:
    """HTML line for server-side NSFW poster rating (Task.content_rating)."""
    r = (rating or "unknown").strip().lower()
    emoji = {"safe": "🟢", "suggestive": "🟡", "adult": "🔴", "unknown": "⚪"}.get(r, "⚪")
    return f"{emoji} Content rating: <code>{html.escape(r)}</code>"


def _task_summary(input_url: str | None, input_type: str | None) -> str:
    parts: list[str] = []

    if input_type:
        parts.append(input_type.lower())

    ext = None
    if input_url:
        try:
            path = urlparse(input_url).path or ""
            if "." in path:
                ext = path.rsplit(".", 1)[-1].lower()
        except Exception:
            ext = None

    if ext:
        # Avoid duplicate if input_type is same as ext
        if not input_type or input_type.lower() != ext:
            parts.append(ext)

    return " | ".join(parts) if parts else ""


def _format_input_url(input_url: str | None) -> str:
    """Format input_url for display in Telegram message."""
    if not input_url:
        return ""
    
    # For Free3D URLs, show a compact link
    if "free3d.online" in input_url:
        return f'📦 <a href="{html.escape(input_url)}">Free3D Model</a>'
    
    # For other URLs, show domain + path as a link
    try:
        parsed = urlparse(input_url)
        domain = parsed.netloc or ""
        path = parsed.path or ""
        # Truncate very long paths
        if len(path) > 40:
            path = path[:20] + "..." + path[-15:]
        return f'📦 <a href="{html.escape(input_url)}">{html.escape(domain + path)}</a>'
    except Exception:
        return f'📦 <a href="{html.escape(input_url)}">Source</a>'


def _is_http_url(url: str | None) -> bool:
    try:
        parsed = urlparse((url or "").strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def _extract_meta_image_url(page_html: str, base_url: str) -> str | None:
    for tag in re.findall(r"<meta\b[^>]*>", page_html or "", flags=re.IGNORECASE):
        if not re.search(
            r"""(?:property|name)\s*=\s*["'](?:og:image|twitter:image)["']""",
            tag,
            flags=re.IGNORECASE,
        ):
            continue
        match = re.search(r"""content\s*=\s*["']([^"']+)["']""", tag, flags=re.IGNORECASE)
        if not match:
            continue
        image_url = html.unescape(match.group(1)).strip()
        if image_url:
            return urljoin(base_url, image_url)
    return None


def _image_suffix_from_response(content_type: str, image_bytes: bytes) -> str | None:
    ct = (content_type or "").lower()
    if "jpeg" in ct or "jpg" in ct or image_bytes.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if "png" in ct or image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if "webp" in ct or image_bytes.startswith(b"RIFF"):
        return ".webp"
    return None


async def _resolve_source_preview_url(input_url: str | None, source_preview_url: str | None) -> str | None:
    explicit = (source_preview_url or "").strip()
    if _is_http_url(explicit):
        return explicit

    source = (input_url or "").strip()
    if not _is_http_url(source):
        return None

    path = (urlparse(source).path or "").lower()
    if path.endswith((".glb", ".fbx", ".obj", ".zip", ".blend", ".mp4", ".png", ".jpg", ".jpeg", ".webp")):
        return None

    try:
        async with httpx.AsyncClient(headers={"User-Agent": "AutoRigBot/1.0 (+https://autorig.online)"}) as client:
            resp = await client.get(source, timeout=10.0, follow_redirects=True)
        if resp.status_code != 200:
            print(f"[Telegram] Source preview page fetch failed for {source}: HTTP {resp.status_code}")
            return None
        ctype = (resp.headers.get("content-type") or "").lower()
        if "html" not in ctype and "<html" not in resp.text[:500].lower():
            return None
        return _extract_meta_image_url(resp.text, str(resp.url))
    except Exception as e:
        print(f"[Telegram] Source preview page fetch failed for {source}: {type(e).__name__}: {e}")
        return None


async def _download_source_preview_for_telegram(
    task_id: str,
    input_url: str | None,
    source_preview_url: str | None = None,
) -> Path | None:
    image_url = await _resolve_source_preview_url(input_url, source_preview_url)
    if not image_url:
        return None
    try:
        async with httpx.AsyncClient(headers={"User-Agent": "AutoRigBot/1.0 (+https://autorig.online)"}) as client:
            resp = await client.get(image_url, timeout=20.0, follow_redirects=True)
        if resp.status_code != 200:
            print(f"[Telegram] Source preview image fetch failed for {task_id}: HTTP {resp.status_code} {image_url}")
            return None
        image_bytes = resp.content or b""
        if not image_bytes:
            print(f"[Telegram] Source preview image empty for {task_id}: {image_url}")
            return None
        if len(image_bytes) > 6 * 1024 * 1024:
            print(f"[Telegram] Source preview image too large for {task_id}: {len(image_bytes)} bytes")
            return None
        suffix = _image_suffix_from_response(resp.headers.get("content-type") or "", image_bytes)
        if not suffix:
            print(f"[Telegram] Source preview image unsupported for {task_id}: {resp.headers.get('content-type')}")
            return None
        cache_dir = Path("/var/autorig/preflight-renders")
        cache_dir.mkdir(parents=True, exist_ok=True)
        final_path = cache_dir / f"{task_id}_telegram_source{suffix}"
        tmp_path = cache_dir / f"{task_id}_telegram_source.tmp"
        tmp_path.write_bytes(image_bytes)
        tmp_path.replace(final_path)
        print(f"[Telegram] Source preview image cached for task {task_id}: {final_path} ({len(image_bytes)} bytes)")
        return final_path
    except Exception as e:
        print(f"[Telegram] Source preview image fetch failed for {task_id}: {type(e).__name__}: {e}")
        return None


def _normalize_telegram_chat_type(raw) -> str | None:
    """PTB Chat.type may be Enum or str — store stable lowercase for SQL filters."""
    if raw is None:
        return None
    v = getattr(raw, "value", None)
    if v is None:
        v = raw
    s = str(v).strip().lower()
    if not s:
        return None
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    return s


async def upsert_chat(chat_id: int, chat_type, title: str | None) -> None:
    ctype = _normalize_telegram_chat_type(chat_type)
    async with AsyncSessionLocal() as db:
        rs = await db.execute(select(TelegramChat).where(TelegramChat.chat_id == chat_id))
        rec = rs.scalar_one_or_none()
        now = datetime.utcnow()
        if rec:
            rec.chat_type = ctype
            rec.title = title
            rec.is_active = True
            rec.last_seen_at = now
        else:
            rec = TelegramChat(
                chat_id=chat_id,
                chat_type=ctype,
                title=title,
                is_active=True,
                created_at=now,
                last_seen_at=now,
            )
            db.add(rec)
        await db.commit()


async def get_active_chat_ids(*, include_private: bool = True) -> list[int]:
    """Subscribed chats.

    Private chats are reserved for the 3D generation pipeline (validation image,
    model video, their buttons and failures). General site traffic - task
    notifications, disk pressure, startup stats - is group-only, so callers that
    broadcast such messages pass include_private=False.
    """
    chat_ids: list[int] = []
    if TELEGRAM_NOTIFICATION_CHAT_ID is not None and int(TELEGRAM_NOTIFICATION_CHAT_ID) != 0:
        chat_ids.append(int(TELEGRAM_NOTIFICATION_CHAT_ID))

    async with AsyncSessionLocal() as db:
        query = select(TelegramChat.chat_id, TelegramChat.chat_type).where(
            TelegramChat.is_active.is_(True)
        )
        rs = await db.execute(query)
        for chat_id, chat_type in rs.all():
            chat_id = int(chat_id)
            if not include_private and str(chat_type or "").lower() == "private":
                continue
            if chat_id not in chat_ids:
                chat_ids.append(chat_id)
    return chat_ids


async def get_broadcast_chat_ids() -> list[int]:
    """Chats that receive general site notifications (never private DMs)."""
    return await get_active_chat_ids(include_private=False)


async def private_chats_awaiting_task(task_id: str) -> list[int]:
    """Private chats that asked for this task: they pressed Submit on a
    generated model, so its completion belongs to the generation pipeline."""
    if not task_id:
        return []
    async with AsyncSessionLocal() as db:
        rs = await db.execute(
            select(TelegramNotification.chat_id)
            .where(TelegramNotification.event_type == "task_reply_to")
            .where(TelegramNotification.event_key == task_id)
        )
        return [int(row[0]) for row in rs.all()]


# =============================================================================
# Site support chat (forum topic per session)
# =============================================================================
async def resolve_support_forum_chat_id(db: AsyncSession) -> int | None:
    """
    Target supergroup for support topics:
    TELEGRAM_NOTIFICATION_CHAT_ID if set,
    else earliest active subscriber preferring Bot-API-negative ids (forums/groups),
    then any active chat (matches notification fan-out ordering when only positives exist).
    """
    if TELEGRAM_NOTIFICATION_CHAT_ID is not None and int(TELEGRAM_NOTIFICATION_CHAT_ID) != 0:
        return int(TELEGRAM_NOTIFICATION_CHAT_ID)
    r = await db.execute(
        select(TelegramChat.chat_id)
        .where(TelegramChat.is_active.is_(True))
        .order_by(
            case((TelegramChat.chat_id < 0, 0), else_=1),
            TelegramChat.created_at.asc(),
        )
        .limit(1)
    )
    row = r.scalar_one_or_none()
    return int(row) if row is not None else None


async def support_forum_readiness_error(db: AsyncSession) -> str | None:
    """Return None only when the bot can create support forum topics in the target chat."""
    if not (_get_token() or ""):
        return "TELEGRAM_BOT_TOKEN is not set"
    cid = await resolve_support_forum_chat_id(db)
    if cid is None or int(cid) == 0:
        return "Support forum chat_id not resolved"

    from telegram import Bot

    bot = Bot(token=_get_token())
    try:
        chat = await bot.get_chat(chat_id=int(cid))
        chat_type = _normalize_telegram_chat_type(getattr(chat, "type", None))
        if chat_type != "supergroup":
            return f"resolved support chat must be a supergroup, got {chat_type or 'unknown'}"
        if not bool(getattr(chat, "is_forum", False)):
            return "resolved support supergroup does not have forum topics enabled"

        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=int(cid), user_id=int(me.id))
        status = _normalize_telegram_chat_type(getattr(member, "status", None))
        if status not in ("administrator", "creator", "owner"):
            return f"support bot must be an admin in the forum supergroup, got {status or 'unknown'}"
        if status not in ("creator", "owner") and getattr(member, "can_manage_topics", None) is not True:
            return "support bot admin is missing the Telegram right to manage topics"
    except Exception as exc:
        return f"Telegram support forum check failed: {type(exc).__name__}: {exc}"
    return None


async def support_forum_configured_bool(db: AsyncSession) -> bool:
    """True only when token, target forum, and bot topic-management rights are valid."""
    return await support_forum_readiness_error(db) is None


async def telegram_create_support_topic(db: AsyncSession, topic_name: str) -> tuple[int, int]:
    token = _get_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    readiness_error = await support_forum_readiness_error(db)
    if readiness_error is not None:
        raise RuntimeError(readiness_error)
    cid = await resolve_support_forum_chat_id(db)
    if cid is None:
        raise RuntimeError(
            "Support forum chat_id not resolved (set TELEGRAM_NOTIFICATION_CHAT_ID "
            "or subscribe the target group with /start so a row exists in telegram_chats)"
        )

    from telegram import Bot

    bot = Bot(token=token)
    name = (topic_name or "").strip()[:128] or "Support"

    forum_t = await _send_with_retry(lambda: bot.create_forum_topic(chat_id=int(cid), name=name))
    if not forum_t:
        raise RuntimeError("create_forum_topic failed")
    mtid = getattr(forum_t, "message_thread_id", None)
    if mtid is None:
        raise RuntimeError("create_forum_topic returned no message_thread_id")
    return int(cid), int(mtid)


async def telegram_send_support_message_html(
    *,
    forum_chat_id: int,
    message_thread_id: int,
    html: str,
) -> int:
    token = _get_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    msg = await _send_with_retry(
        lambda: bot.send_message(
            chat_id=int(forum_chat_id),
            message_thread_id=int(message_thread_id),
            text=html,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        ),
        raise_last=True,
    )
    if not msg:
        raise RuntimeError("send_support_message failed")
    return int(getattr(msg, "message_id"))


async def ingest_support_reply_from_forum_message(
    *,
    forum_chat_id: int,
    message_thread_id: int,
    text: str,
    telegram_message_id: int | None,
    from_bot: bool,
) -> None:
    if from_bot:
        return
    t = (text or "").strip()
    if not t:
        return
    async with AsyncSessionLocal() as db:
        r = await db.execute(
            select(SupportChatSession).where(
                SupportChatSession.telegram_chat_id == int(forum_chat_id),
                SupportChatSession.telegram_thread_id == int(message_thread_id),
                SupportChatSession.status == "open",
            )
        )
        sess = r.scalar_one_or_none()
        if sess is None:
            return

        if telegram_message_id is not None:
            dup_chk = await db.execute(
                select(SupportChatMessage).where(
                    SupportChatMessage.session_id == sess.id,
                    SupportChatMessage.telegram_message_id == int(telegram_message_id),
                )
            )
            if dup_chk.scalar_one_or_none() is not None:
                return

        db.add(
            SupportChatMessage(
                session_id=sess.id,
                direction="admin",
                body_text=t,
                telegram_message_id=(
                    int(telegram_message_id) if telegram_message_id is not None else None
                ),
            )
        )
        await db.commit()


# sqlite is opened with a StaticPool (database.py), so every AsyncSessionLocal in
# this process shares ONE connection and therefore one transaction. Two chats
# reserving concurrently would then interleave: a sibling's rollback on
# IntegrityError discards the other's pending INSERT, the reservation silently
# vanishes and the "once per hour" guard turns into one message per run. Every
# reservation write is serialized here so that cannot happen.
_notification_write_lock = asyncio.Lock()


async def reserve_notification(chat_id: int, event_type: str, event_key: str) -> bool:
    """
    Reserve a per-chat notification key atomically.
    Returns True if reserved now, False if it was already reserved/sent earlier.
    """
    async with _notification_write_lock:
        return await _reserve_notification_locked(chat_id, event_type, event_key)


async def _reserve_notification_locked(chat_id: int, event_type: str, event_key: str) -> bool:
    async with AsyncSessionLocal() as db:
        rec = TelegramNotification(
            chat_id=chat_id,
            event_type=event_type,
            event_key=event_key,
            created_at=datetime.utcnow(),
        )
        db.add(rec)
        try:
            await db.commit()
            return True
        except IntegrityError:
            await db.rollback()
            return False
        except Exception:
            await db.rollback()
            raise


async def attach_notification_message_id(chat_id: int, event_type: str, event_key: str, message_id: int | None) -> None:
    if not message_id:
        return
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(TelegramNotification)
            .where(TelegramNotification.chat_id == chat_id)
            .where(TelegramNotification.event_type == event_type)
            .where(TelegramNotification.event_key == event_key)
            .values(message_id=int(message_id))
        )
        await db.commit()


async def pop_notification_message_id(chat_id: int, event_type: str, event_key: str) -> int | None:
    async with AsyncSessionLocal() as db:
        rs = await db.execute(
            select(TelegramNotification)
            .where(TelegramNotification.chat_id == chat_id)
            .where(TelegramNotification.event_type == event_type)
            .where(TelegramNotification.event_key == event_key)
        )
        rec = rs.scalar_one_or_none()
        if not rec or not rec.message_id:
            return None
        message_id = int(rec.message_id)
        rec.deleted_at = datetime.utcnow()
        await db.commit()
        return message_id


async def peek_notification_message_id(chat_id: int, event_type: str, event_key: str) -> int | None:
    """Read a stored message id without consuming the reservation."""
    async with AsyncSessionLocal() as db:
        rs = await db.execute(
            select(TelegramNotification)
            .where(TelegramNotification.chat_id == chat_id)
            .where(TelegramNotification.event_type == event_type)
            .where(TelegramNotification.event_key == event_key)
        )
        rec = rs.scalar_one_or_none()
        if not rec or not rec.message_id:
            return None
        return int(rec.message_id)


async def _cleanup_generation_chat(task_id: str) -> None:
    """Take a finished generation's cards out of the private chat.

    The completion notice has already been sent by this point, so the image
    review, the turntable card and their now-dead buttons are just clutter.
    Groups are never touched - renderfin only cleans private chats.
    """
    try:
        import render_prompting

        cleaned = await render_prompting.cleanup_character_gen_chat(task_id)
        if cleaned:
            print(f"[Telegram] cleaned {cleaned} generation message(s) for task {task_id}")
    except Exception as exc:
        print(f"[Telegram] generation chat cleanup failed for {task_id}: {exc}")


async def remember_task_reply_target(chat_id: int, task_id: str, message_id: int) -> None:
    """Thread the eventual 'Task completed' notice under the message the user
    acted on, so it is not lost in the chat flow."""
    if not (chat_id and task_id and message_id):
        return
    try:
        await reserve_notification(chat_id, "task_reply_to", task_id)
        await attach_notification_message_id(chat_id, "task_reply_to", task_id, message_id)
    except Exception as e:
        print(f"[Telegram] reply target store failed for {task_id}: {e}")


async def release_notification(chat_id: int, event_type: str, event_key: str) -> None:
    """Drop a reservation so the action can be retried (e.g. after a failed generation)."""
    from sqlalchemy import delete as sa_delete

    async with AsyncSessionLocal() as db:
        await db.execute(
            sa_delete(TelegramNotification)
            .where(TelegramNotification.chat_id == chat_id)
            .where(TelegramNotification.event_type == event_type)
            .where(TelegramNotification.event_key == event_key)
        )
        await db.commit()


async def _task_telegram_metrics(task_id: str) -> dict[str, int]:
    now = datetime.utcnow()
    current_from = now - timedelta(hours=24)
    previous_from = now - timedelta(hours=48)
    async with AsyncSessionLocal() as db:
        task_rs = await db.execute(select(Task).where(Task.id == task_id))
        task = task_rs.scalar_one_or_none()
        if not task:
            return {"ordinal": 0, "current_24h": 0, "delta_24h": 0}

        ordinal_rs = await db.execute(
            select(func.count(Task.id))
            .where(Task.created_at <= task.created_at)
        )
        ordinal = int(ordinal_rs.scalar() or 0)

        current_rs = await db.execute(
            select(func.count(Task.id))
            .where(Task.created_at >= current_from)
            .where(Task.created_at <= now)
        )
        current_24h = int(current_rs.scalar() or 0)

        previous_rs = await db.execute(
            select(func.count(Task.id))
            .where(Task.created_at >= previous_from)
            .where(Task.created_at < current_from)
        )
        previous_24h = int(previous_rs.scalar() or 0)

    return {
        "ordinal": ordinal,
        "current_24h": current_24h,
        "delta_24h": current_24h - previous_24h,
    }


def _format_task_metrics(metrics: dict[str, int]) -> str:
    ordinal = int(metrics.get("ordinal") or 0)
    current_24h = int(metrics.get("current_24h") or 0)
    delta = int(metrics.get("delta_24h") or 0)
    delta_str = f"+{delta}" if delta > 0 else str(delta)
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
    return f"#{ordinal} | 24h {current_24h} | {trend} {delta_str}"


async def _send_with_retry(
    coro_factory,
    *,
    max_retries: int = 2,
    retry_network: bool = True,
    raise_last: bool = False,
):
    """Best-effort retry for Telegram rate limits/transient errors."""
    from telegram.error import RetryAfter, TimedOut, NetworkError

    attempt = 0
    while True:
        try:
            return await coro_factory()
        except RetryAfter as e:
            attempt += 1
            print(f"[Telegram] Rate limited, retry {attempt}/{max_retries}")
            if attempt > max_retries:
                print("[Telegram] Max retries exceeded (rate limit)")
                if raise_last:
                    raise
                return None
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.5)
        except (TimedOut, NetworkError) as e:
            if not retry_network:
                print(f"[Telegram] Network error (no retry mode): {e}")
                if raise_last:
                    raise
                return None
            attempt += 1
            print(f"[Telegram] Network error: {e}, retry {attempt}/{max_retries}")
            if attempt > max_retries:
                print("[Telegram] Max retries exceeded (network)")
                if raise_last:
                    raise
                return None
            await asyncio.sleep(1.0 * attempt)
        except Exception as e:
            # Log unexpected API errors
            print(f"[Telegram] API Error: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            if raise_last:
                raise
            return None


async def broadcast_new_task(
    task_id: str,
    input_url: str | None,
    input_type: str | None,
    progress_page: str | None = None,
    via_api: bool = False,
    title: str | None = None,
    theme_name: str | None = None,
    poster_path: str | None = None,
    detector_text: str | None = None,
    source_preview_url: str | None = None,
) -> None:
    print(f"[Telegram] broadcast_new_task called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping new task notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    summary = _task_summary(input_url, input_type)
    source_line = _format_input_url(input_url)
    metrics_line = _format_task_metrics(await _task_telegram_metrics(task_id))
    
    # Compact 2-line format using HTML
    header = "🟢 <b>New task started</b>"
    if via_api:
        header += " · 🔌 <b>API</b>"
    new_parts = [f'🔗 <a href="{html.escape(url)}">View Result</a>']
    if title:
        new_parts.append(f"🖼️ <b>{html.escape(title)}</b>")
    elif theme_name:
        new_parts.append(f"🖼️ {html.escape(theme_name)}")
    if summary:
        new_parts.append(f"📄 {html.escape(summary)}")
    if detector_text:
        new_parts.append(f"🧠 {html.escape(detector_text)}")
    new_parts.append(html.escape(metrics_line))
    text = header + "\n" + " | ".join(new_parts)
    if progress_page:
        text += f' | 🔧 <a href="{html.escape(progress_page)}">Worker</a>'
    if source_line:
        text += f"\n{source_line}"

    notification_photo_file = Path(poster_path) if poster_path else None
    if len(text) <= 1000:
        if notification_photo_file and not notification_photo_file.is_file():
            print(f"[Telegram] Poster file missing for task {task_id}: {notification_photo_file}")
            notification_photo_file = None
        if not notification_photo_file:
            notification_photo_file = await _download_source_preview_for_telegram(
                task_id,
                input_url,
                source_preview_url,
            )
    else:
        print(f"[Telegram] Caption too long for photo task {task_id}: {len(text)} chars")

    chat_ids = await get_broadcast_chat_ids()
    print(f"[Telegram] Sending new task notification to {len(chat_ids)} chat(s)")
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "task_new", task_id)
            if not reserved:
                print(f"[Telegram] Skip duplicate new-task notification for chat={chat_id}, task={task_id}")
                return
            photo_file = notification_photo_file
            result = None
            sent_method = "text"
            if photo_file and photo_file.is_file() and len(text) <= 1000:
                async def _send_photo(cid=chat_id, p=photo_file):
                    with p.open("rb") as f:
                        return await bot.send_photo(
                            chat_id=cid,
                            photo=f,
                            caption=text,
                            parse_mode=ParseMode.HTML,
                        )
                result = await _send_with_retry(_send_photo, retry_network=False)
                if result:
                    sent_method = "photo"
                else:
                    print(f"[Telegram] Photo send failed for task {task_id}, chat {chat_id}; falling back to text")
            if not result:
                result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                ), retry_network=False)
            if result:
                await attach_notification_message_id(chat_id, "task_new", task_id, getattr(result, "message_id", None))
                print(f"[Telegram] New task notification sent to chat {chat_id} via {sent_method}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_purchase_intent(
    task_id: str,
    user_email: str | None = None,
    anon_id: str | None = None,
    source: str | None = None,
    animation_id: str | None = None,
    animation_name: str | None = None
) -> None:
    """Notify when user clicks download-to-purchase."""
    print(f"[Telegram] broadcast_purchase_intent called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping purchase intent notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anon")
    source_label = source or "download_all"
    text = f'💳 <b>Purchase intent</b>\n🔗 <a href="{html.escape(url)}">Task</a> | 👤 {html.escape(actor)} | 📍 {html.escape(source_label)}'

    anim_name = (animation_name or "").strip()
    anim_id = (animation_id or "").strip()
    if anim_name or anim_id:
        if anim_name and anim_id and anim_name.lower() != anim_id.lower():
            text += f"\n🎬 <b>Animation:</b> {html.escape(anim_name)} ({html.escape(anim_id)})"
        else:
            text += f"\n🎬 <b>Animation:</b> {html.escape(anim_name or anim_id)}"

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            ))
            if result:
                print(f"[Telegram] Purchase intent sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_ltx_video_generation_started(
    *,
    task_id: str,
    user_email: str | None = None,
    theme_name: str | None = None,
    background_hint: str | None = None,
    variant_count: int = 4,
) -> None:
    """Notify admins when a user starts LTX motion-reference video generation."""
    print(f"[Telegram] broadcast_ltx_video_generation_started task={task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping LTX generation notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    actor = user_email or "anon"
    parts = [
        f'🔗 <a href="{html.escape(url)}">Task</a>',
        f"👤 {html.escape(actor)}",
        f"🎞️ {int(variant_count or 4)} refs",
    ]
    if theme_name:
        parts.append(f"🖼️ {html.escape(theme_name)}")
    if background_hint:
        parts.append(f"🌄 {html.escape(background_hint[:120])}")
    text = "🎬 <b>LTX reference generation started</b>\n" + " | ".join(parts)

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return
    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            ), retry_network=False)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_animation_fitting_started(
    *,
    task_id: str,
    variant_name: str | None = None,
    video_url: str | None = None,
    user_email: str | None = None,
) -> None:
    """Notify admins when a user starts fitting a skeletal animation from a reference video."""
    print(f"[Telegram] broadcast_animation_fitting_started task={task_id} variant={variant_name}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping animation fitting notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    actor = user_email or "anon"
    variant = (variant_name or "selected video").strip()
    parts = [
        f'🔗 <a href="{html.escape(url)}">Task</a>',
        f"👤 {html.escape(actor)}",
        f"🦴 {html.escape(variant)}",
    ]
    if video_url:
        parts.append(f'🎥 <a href="{html.escape(video_url)}">Reference video</a>')
    text = "🧬 <b>Animation fitting started</b>\n" + " | ".join(parts)

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return
    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            ), retry_network=False)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_full_bundle_download(task_id: str, user_email: str | None = None) -> None:
    """Notify admins when a user downloads the full-task ZIP bundle (archive endpoint)."""
    print(f"[Telegram] broadcast_full_bundle_download task={task_id} user={user_email}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping full bundle download notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    actor = user_email or "unknown"
    text = (
        f'📦 <b>Full bundle download</b>\n'
        f'🔗 <a href="{html.escape(url)}">Task</a> | 👤 {html.escape(actor)}'
    )

    hour_bucket = datetime.utcnow().strftime("%Y-%m-%d-%H")
    # event_key max 128 chars (DB); hash task + user + hour for dedupe
    event_key = hashlib.sha256(
        f"{task_id}\0{(user_email or '')}\0{hour_bucket}".encode()
    ).hexdigest()[:48]

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "bundle_download", event_key)
            if not reserved:
                print(f"[Telegram] Skip duplicate bundle download notice chat={chat_id} key={event_key}")
                return
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            ))
            if result:
                print(f"[Telegram] Full bundle download notice sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_credits_purchase_click(
    package: str,
    price: str,
    user_email: str | None = None,
    anon_id: str | None = None,
    product_kind: str = "credits",
    permalink: str = "",
    source: str = "",
    page_url: str = "",
) -> None:
    """Notify when user clicks a Gumroad purchase button."""
    print(f"[Telegram] broadcast_credits_purchase_click: kind={product_kind}, package={package}, price={price}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping credits purchase notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anonymous")
    details = [
        f"Kind: {html.escape(product_kind or 'unknown')}",
        f"Package: {html.escape(package)}",
        f"Price: {html.escape(price)}",
        f"User: {html.escape(actor)}",
    ]
    if permalink:
        details.append(f"Permalink: <code>{html.escape(permalink)}</code>")
    if source:
        details.append(f"Source: <code>{html.escape(source)}</code>")
    text = "💰 <b>Purchase click</b>\n" + " | ".join(details)
    if page_url:
        text += f'\nPage: <a href="{html.escape(page_url)}">open</a>'

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            ))
            if result:
                print(f"[Telegram] Credits purchase click sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_youtube_token_refresh_needed(detail: str = "") -> None:
    """
    Notify admins that YouTube OAuth refresh token must be renewed (invalid_grant / revoked).
    At most once per calendar day per chat (reserve_notification).
    """
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping YouTube OAuth refresh notice")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    day = datetime.utcnow().strftime("%Y-%m-%d")
    oauth_url = f"{APP_URL.rstrip('/')}/api/admin/youtube/oauth/start"
    detail_line = ""
    if detail:
        d = detail.strip().replace("\n", " ")
        if len(d) > 400:
            d = d[:400] + "…"
        detail_line = f"\n<code>{html.escape(d)}</code>"

    text = (
        "⚠️ <b>YouTube: нужно обновить токен</b>\n"
        "Refresh-токен недействителен или отозван — автозагрузка роликов на канал не работает."
        f"{detail_line}\n"
        f'→ <a href="{html.escape(oauth_url)}">Подключить канал заново</a>'
    )

    bot = Bot(token=token)
    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "youtube_token", f"refresh_{day}")
            if not reserved:
                print(f"[Telegram] Skip duplicate YouTube token notice chat={chat_id} day={day}")
                return
            result = await _send_with_retry(
                lambda cid=chat_id: bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
            )
            if result:
                print(f"[Telegram] YouTube token refresh notice sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_disk_space_low(
    *,
    free_gb: float,
    target_gb: float,
    zips_deleted: int,
    tasks_purged: int,
) -> None:
    """
    Alert admins: root filesystem still below target after new-task cleanup.
    Throttled: at most once per UTC hour per chat (reserve_notification).
    """
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping disk space low notice")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    hour_bucket = datetime.utcnow().strftime("%Y-%m-%d-%H")
    tgt = f"{float(target_gb):.1f}".replace(".", "_")
    event_key = f"below_{tgt}g_{hour_bucket}"

    text = (
        "🚨 <b>Мало места на диске</b>\n"
        f"Свободно на <code>/</code>: <b>{free_gb:.2f} GB</b> "
        f"(цель при создании задачи: <b>{float(target_gb):.1f} GB</b>)\n"
        f"Очистка при создании задачи: удалено ZIP: <code>{zips_deleted}</code>, "
        f"задач (done/error): <code>{tasks_purged}</code>"
    )

    bot = Bot(token=token)
    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "disk_low", event_key)
            if not reserved:
                print(f"[Telegram] Skip duplicate disk-low notice chat={chat_id} hour={hour_bucket}")
                return
            result = await _send_with_retry(
                lambda cid=chat_id: bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            )
            if result:
                print(f"[Telegram] Disk space low notice sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_disk_usage_warning(
    *,
    free_gb: float,
    total_gb: float,
    used_percent: float,
    target_free_gb: float,
    task_cache_gb: float,
    glb_cache_gb: float,
    periodic_task_cache_cap_gb: float,
    glb_cache_cap_gb: float,
) -> None:
    """
    Send a live disk-pressure warning at most once per UTC hour per chat while
    the root filesystem remains above the configured used-percent threshold.
    """
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping disk usage warning")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    text = (
        "🚨 <b>AutoRig disk pressure</b>\n"
        f"Root usage: <b>{used_percent:.1f}%</b> of <code>/</code>\n"
        f"Free: <b>{free_gb:.2f} GB</b> / <b>{total_gb:.2f} GB</b>\n"
        f"Cleanup target: <b>{target_free_gb:.2f} GB free</b>\n"
        f"Task cache: <code>{task_cache_gb:.2f} GB</code> (cap <code>{periodic_task_cache_cap_gb:.2f} GB</code>)\n"
        f"GLB cache: <code>{glb_cache_gb:.2f} GB</code> (cap <code>{glb_cache_cap_gb:.2f} GB</code>)"
    )

    bot = Bot(token=token)
    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    hour_bucket = datetime.utcnow().strftime("%Y-%m-%d-%H")
    event_key = f"pressure_{hour_bucket}"

    # Process-independent guard: the cleanup timer fires every minute, and a
    # single failed DB reservation (sqlite write contention) used to turn that
    # into a per-minute alert storm. The stamp file makes the hourly cadence
    # hold even if the DB write races or rolls back.
    stamp = Path("/var/autorig/disk_pressure_alert.stamp")
    try:
        stamp.parent.mkdir(parents=True, exist_ok=True)
        if stamp.is_file() and stamp.read_text(encoding="utf-8").strip() == event_key:
            print(f"[Telegram] Disk-pressure warning already sent this hour ({hour_bucket})")
            return
        stamp.write_text(event_key, encoding="utf-8")
    except Exception as e:
        print(f"[Telegram] Disk-pressure stamp unavailable ({e}); falling back to DB guard")

    sem = asyncio.Semaphore(1)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "disk_pressure", event_key)
            if not reserved:
                print(f"[Telegram] Skip duplicate disk-pressure warning chat={chat_id} hour={hour_bucket}")
                return
            result = await _send_with_retry(
                lambda cid=chat_id: bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            )
            if result:
                print(f"[Telegram] Disk usage warning sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_feedback_submitted(
    user_email: str,
    text_content: str
) -> None:
    """Notify when user submits feedback."""
    print(f"[Telegram] broadcast_feedback_submitted: user={user_email}")
    token = _get_token()
    if not token:
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    text = f"📝 <b>New Feedback Submitted!</b>\n👤 User: {html.escape(user_email)}\n💬 Text: {html.escape(text_content[:500])}"

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML))
        for cid in chat_ids
    ])


async def broadcast_crypto_payment_submitted(
    report_id: int,
    tier: str,
    network_id: str,
    tx_id: str,
    user_email: str | None,
    agent_anon_id: str | None,
    contact_note: str | None,
) -> None:
    """Notify admins: crypto payment report pending manual credit."""
    print(
        f"[Telegram] broadcast_crypto_payment_submitted id={report_id} tier={tier} net={network_id} tx={tx_id[:32]}..."
    )
    token = _get_token()
    if not token:
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    who_parts: list[str] = []
    if user_email:
        who_parts.append(f"👤 User: {html.escape(user_email)}")
    if agent_anon_id:
        who_parts.append(f"🤖 Agent id: <code>{html.escape(agent_anon_id)}</code>")
    who = "\n".join(who_parts) if who_parts else "👤 Anonymous (see note)"
    note_line = ""
    if contact_note:
        note_line = f"\n📝 Note: {html.escape(contact_note[:500])}"
    text = (
        f"₿ <b>Crypto payment report</b> #{report_id} <i>pending</i>\n"
        f"📦 Tier: <code>{html.escape(tier)}</code> | 🌐 Network: <code>{html.escape(network_id)}</code>\n"
        f"🔗 Tx: <code>{html.escape(tx_id[:200])}</code>\n"
        f"{who}{note_line}"
    )

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML))
        for cid in chat_ids
    ])


async def broadcast_credits_purchased(
    credits: int,
    price: str,
    user_email: str,
    product: str,
    sale_id: str,
    is_test: bool = False,
    is_recurring_charge: bool = False,
    refunded: bool = False,
    product_kind: str = "credits",
    package: str = "",
) -> None:
    """Notify when credits are successfully purchased via Gumroad."""
    print(
        f"[Telegram] broadcast_credits_purchased: kind={product_kind} credits={credits} "
        f"user={user_email} (test={is_test})"
    )
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping credits purchased notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    test_label = " [TEST]" if is_test else ""
    text = (
        f"✅ <b>Credits purchased!</b>{test_label}\n"
        f"💰 Amount: {credits} credits | 💵 Price: {html.escape(price)}\n"
        f"👤 User: {html.escape(user_email)} | 📦 Product: {html.escape(product)}\n"
        f"🆔 Sale: {html.escape(sale_id)}"
    )

    if (product_kind or "").strip().lower() == "plugin":
        package_label = package or "Blender Plugin"
        text = (
            f"<b>Plugin purchased!</b>{test_label}\n"
            f"Package: {html.escape(package_label)} | Price: {html.escape(price)}\n"
            f"User: {html.escape(user_email)} | Product: {html.escape(product)}\n"
            f"Sale: {html.escape(sale_id)}"
        )

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            reserved = await reserve_notification(chat_id, "gumroad_sale", sale_id)
            if not reserved:
                print(f"[Telegram] Skip duplicate gumroad-sale notification for chat={chat_id}, sale={sale_id}")
                return
            flags = []
            if is_recurring_charge:
                flags.append("recurring")
            if refunded:
                flags.append("refunded")
            extra = f" ({', '.join(flags)})" if flags else ""
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text + extra,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            ))
            if result:
                print(f"[Telegram] Credits purchased sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


def _format_duration(seconds: int | None) -> str:
    if seconds is None:
        return ""
    s = max(0, int(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}h {m}m {sec}s"
    if m > 0:
        return f"{m}m {sec}s"
    return f"{sec}s"


def _task_duration_parts(task: Task, now: datetime) -> tuple[int | None, int | None, int | None]:
    if not task.created_at:
        return None, None, None
    end = task.updated_at if task.status in {"done", "error"} and task.updated_at else now
    total = max(0, int((end - task.created_at).total_seconds()))
    processing_started_at = getattr(task, "processing_started_at", None)
    if not processing_started_at:
        return total, 0, total
    queued = max(0, int((processing_started_at - task.created_at).total_seconds()))
    processing = max(0, int((end - processing_started_at).total_seconds()))
    return queued, processing, total


async def _task_video_candidate_urls(task_id: str) -> list[str]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        if not task:
            print(f"[Telegram] Cannot download video: task {task_id} not found")
            return []

        video_urls: list[str] = []

        def add_url(url: str | None) -> None:
            u = (url or "").strip()
            if u and u not in video_urls:
                video_urls.append(u)

        is_animal = str(getattr(task, "input_type", "") or "").strip().lower() == "animal"
        source_urls = list(getattr(task, "ready_urls", None) or []) + list(getattr(task, "output_urls", None) or [])
        if is_animal:
            for url in source_urls:
                if str(url or "").lower().endswith("_rig_preview.mp4"):
                    add_url(str(url))

        task_video_url = str(task.video_url or "").strip()
        if is_animal and task.guid and task.worker_api:
            worker_base = get_worker_base_url(task.worker_api)
            if worker_base:
                add_url(f"{worker_base.rstrip('/')}/converter/glb/{task.guid}/{task.guid}_rig_preview.mp4")

        add_url(task_video_url)
        if "_video_small.mp4" in task_video_url:
            add_url(task_video_url.replace("_video_small.mp4", "_video.mp4"))

        if task.guid and task.worker_api:
            worker_base = get_worker_base_url(task.worker_api)
            if worker_base:
                if is_animal:
                    add_url(f"{worker_base.rstrip('/')}/converter/glb/{task.guid}/{task.guid}_rig_preview.mp4")
                add_url(f"{worker_base}/converter/glb/{task.guid}/{task.guid}_video_small.mp4")
                add_url(f"{worker_base}/converter/glb/{task.guid}/{task.guid}_video.mp4")

        if not video_urls:
            print(f"[Telegram] Cannot download video: task {task_id} has no video URL, guid, or worker_api")

        return video_urls


async def _download_video_from_worker(
    task_id: str,
    *,
    wait_timeout_seconds: int = 180,
    poll_interval_seconds: int = 5,
) -> tuple[str | None, int, str | None]:
    """Wait for a worker video, download it, and cache it locally."""
    cache_dir = "/var/autorig/videos"
    cache_path = f"{cache_dir}/{task_id}.mp4"
    tmp_path = f"{cache_path}.tmp"
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + max(0, int(wait_timeout_seconds))
    last_status: str | None = None
    attempt = 0

    def waited_seconds() -> int:
        return int(max(0, loop.time() - started))

    def cached_video_exists() -> bool:
        return os.path.exists(cache_path) and os.path.getsize(cache_path) > 0

    if cached_video_exists():
        return cache_path, waited_seconds(), "cached"

    try:
        async with httpx.AsyncClient() as client:
            while True:
                if cached_video_exists():
                    return cache_path, waited_seconds(), last_status or "cached"

                video_urls = await _task_video_candidate_urls(task_id)
                if not video_urls:
                    last_status = "no video candidates"

                for video_url in video_urls:
                    attempt += 1
                    print(
                        f"[Telegram] Downloading video attempt={attempt} "
                        f"waited={waited_seconds()}s from {video_url}"
                    )
                    try:
                        remaining = max(1.0, deadline - loop.time())
                        resp = await client.get(
                            video_url,
                            timeout=min(15.0, remaining),
                            follow_redirects=True,
                        )
                        last_status = f"HTTP {resp.status_code}"
                        if resp.status_code == 200 and resp.content:
                            os.makedirs(cache_dir, exist_ok=True)
                            try:
                                with open(tmp_path, "wb") as f:
                                    f.write(resp.content)
                                os.replace(tmp_path, cache_path)
                            except Exception:
                                try:
                                    if os.path.exists(tmp_path):
                                        os.remove(tmp_path)
                                except Exception:
                                    pass
                                raise
                            print(
                                f"[Telegram] Video cached at {cache_path} "
                                f"({len(resp.content)} bytes, wait={waited_seconds()}s)"
                            )
                            return cache_path, waited_seconds(), last_status
                        print(f"[Telegram] Failed to download video from {video_url}: {last_status}")
                    except Exception as e:
                        last_status = f"{type(e).__name__}: {e}"
                        print(f"[Telegram] Failed to download video from {video_url}: {last_status}")

                if loop.time() >= deadline:
                    break

                sleep_for = min(float(poll_interval_seconds), max(0.0, deadline - loop.time()))
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
    except Exception as e:
        last_status = f"{type(e).__name__}: {e}"
        print(f"[Telegram] Failed to download video: {last_status}")

    print(
        f"[Telegram] Video wait exhausted for task {task_id}: "
        f"video_wait_seconds={waited_seconds()} last_video_status={last_status}"
    )
    return None, waited_seconds(), last_status


async def broadcast_task_restarted(task_id: str, reason: str = "manual", admin_email: str | None = None) -> None:
    """Notify about task restart."""
    print(f"[Telegram] broadcast_task_restarted called for task {task_id}, reason={reason}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping restart notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    
    # Get task details
    input_info = ""
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                summary = _task_summary(task.input_url, task.input_type)
                if summary:
                    input_info = f" | 📄 {html.escape(summary)}"
    except Exception as e:
        print(f"[Telegram] Failed to get task details: {e}")
    
    admin_line = f" | 👤 Admin: {html.escape(admin_email)}" if admin_email else ""
    text = f'🔄 <b>Task restarted</b> ({html.escape(reason)})\n🔗 <a href="{html.escape(url)}">Task</a>{input_info}{admin_line}'

    chat_ids = await get_broadcast_chat_ids()
    print(f"[Telegram] Sending restart notification to {len(chat_ids)} chat(s)")
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid, 
                text=text, 
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            ))

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_worker_stalled(
    worker_url: str,
    stalled_tasks: int,
    oldest_stalled_minutes: int,
    sample_task_ids: list[str] | None = None,
) -> None:
    """Notify about stalled worker state (throttled by caller)."""
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping worker stalled notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    from worker_labels import format_worker_stalled_telegram_html

    worker_block = format_worker_stalled_telegram_html(worker_url)
    sample = ", ".join((sample_task_ids or [])[:3])
    sample_line = f"\n🧩 Tasks: <code>{html.escape(sample)}</code>" if sample else ""
    link_lines: list[str] = []
    if sample_task_ids:
        try:
            async with AsyncSessionLocal() as db:
                rows = await db.execute(select(Task).where(Task.id.in_(sample_task_ids[:3])))
                by_id = {task.id: task for task in rows.scalars().all()}
            for task_id in sample_task_ids[:3]:
                task = by_id.get(task_id)
                if not task:
                    continue
                task_url = _task_url(task_id)
                progress_url = task.progress_page
                if not progress_url and task.guid and task.worker_api:
                    worker_base = get_worker_base_url(task.worker_api)
                    if worker_base:
                        progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
                parts = [f'<a href="{html.escape(task_url)}">Task {html.escape(task_id[:8])}</a>']
                if progress_url:
                    parts.append(f'<a href="{html.escape(progress_url)}">Progress</a>')
                link_lines.append(" · ".join(parts))
        except Exception as e:
            print(f"[Telegram] Failed to build stalled task links: {e}")
    links_line = ("\n🔎 " + "\n🔎 ".join(link_lines)) if link_lines else ""
    text = (
        f"🚨 <b>Worker stalled</b>\n"
        f"{worker_block}\n"
        f"📌 stalled: {int(stalled_tasks)} | ⏱ oldest: {int(oldest_stalled_minutes)}m"
        f"{sample_line}"
        f"{links_line}"
    )

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            ), retry_network=False)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_bulk_restart_summary(total: int, restarted: int, errors: list, admin_email: str) -> None:
    """Notify about bulk restart completion."""
    print(f"[Telegram] broadcast_bulk_restart_summary: {restarted}/{total}")
    token = _get_token()
    if not token:
        return

    from telegram import Bot

    bot = Bot(token=token)
    
    error_line = ""
    if errors:
        error_line = f"\n❌ Errors: {len(errors)}"
        if len(errors) <= 5:
            error_line += f"\n{chr(10).join(errors)}"
    
    text = (
        f"🔄 Bulk restart completed\n"
        f"👤 Admin: {admin_email}\n"
        f"✅ Restarted: {restarted}/{total}{error_line}"
    )

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text, disable_web_page_preview=True))
        for cid in chat_ids
    ])


async def reserve_and_broadcast_task_done(task_id: str) -> None:
    """
    Atomically reserve telegram_done_notified_at and enqueue the Telegram "task completed"
    message. Call only after Task.content_rating / content_classified_at are committed so
    the notification always reflects server-side classification.
    """
    async with AsyncSessionLocal() as db:
        now = datetime.utcnow()
        stmt = (
            update(Task)
            .where(Task.id == task_id)
            .where(Task.telegram_done_notified_at.is_(None))
            .values(telegram_done_notified_at=now)
        )
        res = await db.execute(stmt)
        await db.commit()

        if res.rowcount != 1:
            return

        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task:
            return

        queue_wait, processing_time, duration = _task_duration_parts(task, datetime.utcnow())
        progress_url = None
        if task.guid and task.worker_api:
            worker_base = get_worker_base_url(task.worker_api)
            progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"

        print(f"[Telegram] Scheduling done notification for task {task_id} (after content rating)")
        asyncio.create_task(
            broadcast_task_done(
                task_id,
                duration_seconds=duration,
                queue_wait_seconds=queue_wait,
                processing_time_seconds=processing_time,
                progress_page=progress_url,
            )
        )


async def reserve_and_broadcast_task_error(task_id: str) -> None:
    """
    Atomically reserve telegram_done_notified_at and enqueue a terminal error
    notification. This shares the task-level terminal-notified flag with done
    notifications so each terminal state emits at most one operator alert.
    """
    async with AsyncSessionLocal() as db:
        now = datetime.utcnow()
        stmt = (
            update(Task)
            .where(Task.id == task_id)
            .where(Task.telegram_done_notified_at.is_(None))
            .values(telegram_done_notified_at=now)
        )
        res = await db.execute(stmt)
        await db.commit()

        if res.rowcount != 1:
            return

        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task:
            return

        queue_wait, processing_time, duration = _task_duration_parts(task, datetime.utcnow())
        progress_url = None
        if task.guid and task.worker_api:
            worker_base = get_worker_base_url(task.worker_api)
            progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"

        print(f"[Telegram] Scheduling error notification for task {task_id}")
        asyncio.create_task(
            broadcast_task_error(
                task_id,
                duration_seconds=duration,
                queue_wait_seconds=queue_wait,
                processing_time_seconds=processing_time,
                progress_page=progress_url,
            )
        )


async def mark_task_done_notification_sent(task_id: str) -> None:
    """Backstop task-level idempotency after at least one Telegram done send succeeds."""
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .where(Task.telegram_done_notified_at.is_(None))
            .values(telegram_done_notified_at=datetime.utcnow())
        )
        await db.commit()


async def broadcast_task_error(
    task_id: str,
    *,
    duration_seconds: int | None = None,
    queue_wait_seconds: int | None = None,
    processing_time_seconds: int | None = None,
    progress_page: str | None = None,
) -> None:
    print(f"[Telegram] broadcast_task_error called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping error notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    metrics_line = _format_task_metrics(await _task_telegram_metrics(task_id))
    resolved_progress = progress_page
    error_message = "Task failed"

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                error_message = _sanitize_error_for_telegram(getattr(task, "error_message", None))
                if not resolved_progress and task.guid and task.worker_api:
                    parsed = urlparse(task.worker_api)
                    worker_base = f"{parsed.scheme}://{parsed.netloc}"
                    resolved_progress = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
    except Exception as e:
        print(f"[Telegram] Failed to get task details for error notification: {e}")

    dur = _format_duration(duration_seconds)
    queue_dur = _format_duration(queue_wait_seconds)
    processing_dur = _format_duration(processing_time_seconds)
    parts = [f'<a href="{html.escape(url)}">View Result</a>']
    if queue_dur:
        parts.append(f"queue {html.escape(queue_dur)}")
    if processing_dur:
        parts.append(f"processing {html.escape(processing_dur)}")
    if dur:
        parts.append(f"total {html.escape(dur)}")
    parts.append(html.escape(metrics_line))

    text = "ERROR <b>Task failed</b>\n" + " | ".join(parts)
    if resolved_progress:
        text += f'\nWorker logs: <a href="{html.escape(resolved_progress)}">open</a>'
    text += f"\n<code>{html.escape(error_message)}</code>"

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        print("[Telegram] No active chats, skipping error notification")
        return

    async def _one_text(chat_id: int):
        old_message_id = await pop_notification_message_id(chat_id, "task_new", task_id)
        if old_message_id:
            await _send_with_retry(
                lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid),
                retry_network=False,
            )
        reserved = await reserve_notification(chat_id, "task_error", task_id)
        if not reserved:
            print(f"[Telegram] Skip duplicate error notification for chat={chat_id}, task={task_id}")
            return None
        return await _send_with_retry(lambda cid=chat_id: bot.send_message(
            chat_id=cid,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False,
        ), retry_network=False)

    results = await asyncio.gather(*[_one_text(cid) for cid in chat_ids])
    sent_count = sum(1 for r in results if r is not None)
    print(f"[Telegram] Error notification sent to {sent_count}/{len(chat_ids)} chat(s)")
    if sent_count > 0:
        await mark_task_done_notification_sent(task_id)


async def broadcast_task_done(
    task_id: str,
    *,
    duration_seconds: int | None = None,
    queue_wait_seconds: int | None = None,
    processing_time_seconds: int | None = None,
    progress_page: str | None = None,
) -> None:
    print(f"[Telegram] broadcast_task_done called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping done notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    metrics_line = _format_task_metrics(await _task_telegram_metrics(task_id))

    owner_email = None
    content_rating = "unknown"
    resolved_progress = progress_page
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                if task.owner_type == "user":
                    owner_email = task.owner_id
                cr = getattr(task, "content_rating", None)
                if cr:
                    content_rating = str(cr).strip().lower()
                if not resolved_progress and task.guid and task.worker_api:
                    parsed = urlparse(task.worker_api)
                    worker_base = f"{parsed.scheme}://{parsed.netloc}"
                    resolved_progress = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
    except Exception as e:
        print(f"[Telegram] Failed to get task details for done notification: {e}")

    rating_line = _format_content_rating_line(content_rating)

    dur = _format_duration(duration_seconds)
    queue_dur = _format_duration(queue_wait_seconds)
    processing_dur = _format_duration(processing_time_seconds)
    timing_parts = []
    if queue_dur:
        timing_parts.append(f"queue {html.escape(queue_dur)}")
    if processing_dur:
        timing_parts.append(f"processing {html.escape(processing_dur)}")
    done_parts = [f'🔗 <a href="{html.escape(url)}">View Result</a>']
    if owner_email:
        done_parts.append(f"👤 {html.escape(owner_email)}")
    if dur:
        done_parts.append(f"⏱ {html.escape(dur)}")
    done_parts.extend(timing_parts)
    done_parts.append(html.escape(metrics_line))

    text = f"✅ <b>Task completed</b>\n{rating_line}\n" + " | ".join(done_parts)
    if resolved_progress:
        text += f'\n🔧 <a href="{html.escape(resolved_progress)}">Worker Logs</a>'

    # Try to find cached video
    mp4_path = f"/var/autorig/videos/{task_id}.mp4"
    video_path = mp4_path if (os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0) else None
    video_wait_seconds = 0
    last_video_status = "cached" if video_path else None

    # If not cached, try to download from worker
    if not video_path:
        video_path, video_wait_seconds, last_video_status = await _download_video_from_worker(task_id)

    chat_ids = await get_broadcast_chat_ids()
    # a model submitted from the generation flow belongs to that private chat
    for private_id in await private_chats_awaiting_task(task_id):
        if private_id not in chat_ids:
            chat_ids.append(private_id)
    if not chat_ids:
        print("[Telegram] No active chats, skipping done notification")
        return

    print(
        f"[Telegram] Sending done notification to {len(chat_ids)} chat(s), "
        f"video={video_path is not None} video_wait_seconds={video_wait_seconds} "
        f"video_path={video_path} last_video_status={last_video_status}"
    )

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    generate_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎨 Сгенерировать", callback_data=f"rfg:{task_id}")]]
    )

    if not video_path:
        # Fallback: at least notify completion
        async def _one_text(chat_id: int):
            reply_to = await peek_notification_message_id(chat_id, "task_reply_to", task_id)
            old_message_id = await pop_notification_message_id(chat_id, "task_new", task_id)
            if old_message_id:
                await _send_with_retry(
                    lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid),
                    retry_network=False,
                )
            reserved = await reserve_notification(chat_id, "task_done", task_id)
            if not reserved:
                print(f"[Telegram] Skip duplicate done notification for chat={chat_id}, task={task_id}")
                return None
            return await _send_with_retry(lambda cid=chat_id, rt=reply_to: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
                reply_markup=generate_markup,
                reply_to_message_id=rt,
                allow_sending_without_reply=True,
            ), retry_network=False)

        results = await asyncio.gather(*[_one_text(cid) for cid in chat_ids])
        sent_count = sum(1 for r in results if r is not None)
        print(f"[Telegram] Done notification sent to {sent_count}/{len(chat_ids)} chat(s)")
        if sent_count > 0:
            await mark_task_done_notification_sent(task_id)
            await _cleanup_generation_chat(task_id)
        return

    sem = asyncio.Semaphore(2)
    caption = text

    async def _one(chat_id: int):
        async with sem:
            reply_to = await peek_notification_message_id(chat_id, "task_reply_to", task_id)
            old_message_id = await pop_notification_message_id(chat_id, "task_new", task_id)
            if old_message_id:
                await _send_with_retry(
                    lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid),
                    retry_network=False,
                )
            reserved = await reserve_notification(chat_id, "task_done", task_id)
            if not reserved:
                print(f"[Telegram] Skip duplicate done notification for chat={chat_id}, task={task_id}")
                return
            # Telegram expects a file-like object
            def _send():
                f = open(video_path, "rb")
                # bot.send_video will close the file? not guaranteed; close in finally
                async def _inner():
                    try:
                        return await bot.send_video(
                            chat_id=chat_id,
                            video=f,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                            reply_markup=generate_markup,
                            reply_to_message_id=reply_to,
                            allow_sending_without_reply=True,
                        )
                    finally:
                        try:
                            f.close()
                        except Exception:
                            pass
                return _inner()

            result = await _send_with_retry(_send, retry_network=False)
            if result is None:
                print(f"[Telegram] send_video failed for chat={chat_id}, task={task_id}; sending text fallback")
                return await _send_with_retry(lambda cid=chat_id, rt=reply_to: bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                    reply_markup=generate_markup,
                    reply_to_message_id=rt,
                allow_sending_without_reply=True,
                ), retry_network=False)
            return result

    results = await asyncio.gather(*[_one(cid) for cid in chat_ids])
    sent_count = sum(1 for r in results if r is not None)
    print(f"[Telegram] Done video notification sent to {sent_count}/{len(chat_ids)} chat(s)")
    if sent_count > 0:
        await _cleanup_generation_chat(task_id)
    if sent_count > 0:
        await mark_task_done_notification_sent(task_id)


async def broadcast_server_startup() -> None:
    """Send server startup notification with task statistics."""
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping startup notification")
        return

    from telegram import Bot

    bot = Bot(token=token)
    
    # Gather statistics
    try:
        async with AsyncSessionLocal() as db:
            # Count tasks by status
            result = await db.execute(
                select(Task.status, func.count(Task.id)).group_by(Task.status)
            )
            status_counts = dict(result.all())
            
            done_count = status_counts.get("done", 0)
            processing_count = status_counts.get("processing", 0)
            created_count = status_counts.get("created", 0)
            error_count = status_counts.get("error", 0)
            total_count = sum(status_counts.values())
            
            # Count active chats
            chat_result = await db.execute(
                select(func.count(TelegramChat.chat_id)).where(TelegramChat.is_active.is_(True))
            )
            active_chats = chat_result.scalar() or 0
    except Exception as e:
        print(f"[Telegram] Failed to gather stats: {e}")
        done_count = processing_count = created_count = error_count = total_count = 0
        active_chats = 0
    
    # Format message
    start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    base_url = (APP_URL or "").rstrip("/")
    
    text = (
        f"🚀 Server started\n"
        f"📅 {start_time}\n"
        f"🌐 {base_url}\n"
        f"\n"
        f"📊 Task Statistics:\n"
        f"  ✅ Done: {done_count}\n"
        f"  ⏳ Processing: {processing_count}\n"
        f"  📝 Queued: {created_count}\n"
        f"  ❌ Errors: {error_count}\n"
        f"  📦 Total: {total_count}\n"
        f"\n"
        f"📱 Active chats: {active_chats}"
    )

    chat_ids = await get_broadcast_chat_ids()
    if not chat_ids:
        print("[Telegram] No active chats for startup notification")
        return

    print(f"[Telegram] Sending startup notification to {len(chat_ids)} chat(s)")
    
    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda: bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True))

    await asyncio.gather(*[_one(cid) for cid in chat_ids])
    print("[Telegram] Startup notification sent")


# =============================================================================
# Bot runner (polling)
# =============================================================================
async def _support_forum_message_handler(update, context):
    msg = update.effective_message
    if not msg:
        return
    async with AsyncSessionLocal() as db:
        forum_cid = await resolve_support_forum_chat_id(db)
    if forum_cid is None:
        return
    if int(msg.chat_id) != int(forum_cid):
        return
    mtid = getattr(msg, "message_thread_id", None)
    if mtid is None:
        return
    user = msg.from_user
    from_bot = bool(user is not None and getattr(user, "is_bot", False))
    txt = getattr(msg, "text", None) or ""
    await ingest_support_reply_from_forum_message(
        forum_chat_id=int(msg.chat_id),
        message_thread_id=int(mtid),
        text=str(txt),
        telegram_message_id=getattr(msg, "message_id", None),
        from_bot=from_bot,
    )


async def _start_cmd(update, context):
    chat = update.effective_chat
    if not chat:
        return
    async with AsyncSessionLocal() as db:
        forum_cid = await resolve_support_forum_chat_id(db)
    if forum_cid is not None and int(chat.id) == int(forum_cid):
        if update.message:
            await update.message.reply_text(
                "This forum is for support threads. Task notifications cannot be subscribed via /start here; use the site chat bubble."
            )
        return
    title = getattr(chat, "title", None) or getattr(chat, "username", None) or getattr(chat, "full_name", None)
    print(f"[Telegram] /start command from chat_id={chat.id}, type={getattr(chat, 'type', None)}, title={title}")
    await upsert_chat(chat.id, getattr(chat, "type", None), title)
    # Get current subscriber count
    active_chats = await get_active_chat_ids()
    print(f"[Telegram] New subscriber added. Total active chats: {len(active_chats)}")
    if str(getattr(chat, "type", "")).lower() == "private":
        await update.message.reply_text(
            "✅ Подписка оформлена.\n"
            "Сюда приходят только генерации 3D-моделей: "
            "картинка на валидацию, готовая модель и кнопки. "
            "Остальные уведомления сайта остаются в группе."
        )
    else:
        await update.message.reply_text("✅ Subscribed. You will receive task notifications here.")


# ---------------------------------------------------------------------------
# Renderfin character generation (🎨 Сгенерировать button on done notifications)
# ---------------------------------------------------------------------------

CHARGEN_POLL_INTERVAL_SECONDS = 10
# Must exceed the server-side budget (hunyuan stage + turntable + queue wait),
# otherwise the bot declares a failure the server would still complete.
CHARGEN_TOTAL_TIMEOUT_SECONDS = 7800

# Approve callback carries the chosen render: "rfa:{job}:a" | "rfa:{job}:b".
# The trailing variant is optional so buttons sent before two-variant rendering
# still resolve (they mean variant "a").
_APPROVE_PATTERN = r"^rfa:([0-9a-fA-F-]{8,64})(?::([ab]))?$"

_CHARGEN_STAGE_LABELS = {
    "flux_render": "рендерим T-позу (Flux)",
    "awaiting_image_approval": "ждём подтверждения изображения",
    "hunyuan": "генерируем 3D-модель (Hunyuan3D)",
    "turntable": "рендерим видео-облёт",
}


def _chargen_retry_markup(job_id: str):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("♻️ Повторить 3D", callback_data=f"rfe:{job_id}"),
            InlineKeyboardButton("🔁 Перегенерировать", callback_data=f"rfr:{job_id}"),
        ],
        [InlineKeyboardButton("🗑 Отмена", callback_data=f"rfd:{job_id}")],
    ])


async def _download_bytes(url: str, timeout: float = 120.0) -> bytes | None:
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.content
    except Exception as e:
        print(f"[Telegram][Renderfin] download failed {url}: {e}")
    return None


async def _chargen_edit_status(bot, chat_id: int, message_id: int, text: str) -> None:
    from telegram.constants import ParseMode

    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text,
                                    parse_mode=ParseMode.HTML)
    except Exception as e:
        # "message is not modified" and similar are harmless
        if "not modified" not in str(e).lower():
            print(f"[Telegram][Renderfin] status edit failed: {e}")


async def _run_generation(bot, chat_id: int, task_id: str, reply_to_message_id: int | None,
                          status_message_id: int) -> None:
    """🎨 button: build the prompt and hand the job to renderfin.

    Delivery of every result (image review, model video, failures) is done by
    the renderfin service itself, so it survives a bot restart. This coroutine
    only has to get the job created.
    """
    import render_prompting

    try:
        plan = await render_prompting.build_render_request(task_id)
        prompt_preview = html.escape(plan.prompt[:300])
        await _chargen_edit_status(
            bot, chat_id, status_message_id,
            f"⏳ Промпт готов ({plan.source}, телосложение: {plan.body_type}), рендерим T-позу…\n"
            f"<i>{prompt_preview}</i>",
        )
        job_id = await render_prompting.start_character_gen(
            plan, source_task_id=task_id, telegram_chat_id=chat_id
        )
        await render_prompting.set_character_gen_telegram_context(
            job_id, chat_id=chat_id, status_message_id=status_message_id
        )
        print(f"[Telegram][Renderfin] job {job_id} started for task {task_id}")
    except Exception as e:
        print(f"[Telegram][Renderfin] generation failed for task {task_id}: {e}")
        await _chargen_edit_status(
            bot, chat_id, status_message_id,
            f"❌ Ошибка генерации: {html.escape(str(e)[:300])}\nКнопку можно нажать ещё раз.",
        )
        await release_notification(chat_id, "renderfin_gen", task_id)


async def _handle_approve_callback(update, context) -> None:
    import render_prompting

    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(_APPROVE_PATTERN, query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    job_id, variant = match.group(1), (match.group(2) or "a")
    try:
        payload = await render_prompting.approve_character_gen_image(job_id, variant=variant)
    except Exception as e:
        await query.answer(f"Ошибка: {str(e)[:150]}")
        return
    if not payload.get("transitioned"):
        await query.answer(f"Уже в работе (стадия: {payload.get('stage')})")
        return
    label = "второго" if variant == "b" else "первого"
    await query.answer(f"Делаю 3D из {label} варианта…")
    chat_id = int(query.message.chat.id)
    text = f"⏳ Генерируем 3D-модель из {label} варианта (обычно 10-60 минут)…"
    for edit in (context.bot.edit_message_text, context.bot.edit_message_caption):
        try:
            kwargs = {"chat_id": chat_id, "message_id": query.message.message_id}
            kwargs["text" if edit is context.bot.edit_message_text else "caption"] = text
            await edit(**kwargs)
            break
        except Exception:
            continue


async def _handle_regen_callback(update, context) -> None:
    import render_prompting

    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(r"^rfr:([0-9a-fA-F-]{8,64})$", query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    job_id = match.group(1)
    try:
        payload = await render_prompting.regenerate_character_gen_image(job_id)
    except Exception as e:
        await query.answer(f"Ошибка: {str(e)[:150]}")
        return
    if not payload.get("transitioned"):
        await query.answer(f"Сейчас нельзя (стадия: {payload.get('stage')})")
        return
    await query.answer("Перегенерируем изображение…")
    chat_id = int(query.message.chat.id)
    try:
        await context.bot.edit_message_caption(
            chat_id=chat_id, message_id=query.message.message_id,
            caption="🔁 Рендерим новый вариант T-позы…",
        )
    except Exception:
        pass


async def _handle_generate_callback(update, context) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(r"^rfg:([0-9a-fA-F-]{8,64})$", query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    task_id = match.group(1)
    chat = query.message.chat if query.message else None
    if chat is None:
        await query.answer("Нет чата")
        return
    origin_chat_id = int(chat.id)
    user = getattr(query, "from_user", None)
    user_id = int(getattr(user, "id", 0) or 0)

    reserved = await reserve_notification(origin_chat_id, "renderfin_gen", task_id)
    if not reserved:
        await query.answer("Генерация уже запущена для этой задачи")
        return

    bot = context.bot
    # Deliver the validation image and the model to the person who asked for
    # them: their DM if the bot may write there, otherwise the origin chat.
    chat_id, status_message_id, reply_to = origin_chat_id, 0, query.message.message_id
    if user_id and user_id != origin_chat_id:
        try:
            dm = await bot.send_message(
                chat_id=user_id,
                text="⏳ Генерация запущена: строим промпт…",
            )
            chat_id, status_message_id, reply_to = user_id, dm.message_id, None
            await query.answer("Запускаю генерацию — результат пришлю в личку")
        except Exception as e:
            print(f"[Telegram][Renderfin] DM to {user_id} unavailable ({e}); using chat {origin_chat_id}")

    if not status_message_id:
        await query.answer("Запускаю генерацию…")
        status_message = await bot.send_message(
            chat_id=chat_id,
            text="⏳ Генерация запущена: строим промпт…",
            reply_to_message_id=reply_to,
        )
        status_message_id = status_message.message_id

    asyncio.create_task(
        _run_generation(bot, chat_id, task_id, reply_to, status_message_id)
    )


async def _submit_generated_model(glb_url: str) -> tuple[str | None, str | None]:
    """Create the FULL conversion task from the generated GLB.

    pipeline_kind="convert" with type="t_pose" runs the complete worker
    scenario — retopology (1k/10k/100k), bake, autorig, animation retarget and
    every export format — whereas "rig" is only_rig and skips retopology.
    Dispatch is handled by the main backend's background loop, which picks up
    status=created tasks within seconds.
    """
    from tasks import create_conversion_task

    async with AsyncSessionLocal() as db:
        task, error = await create_conversion_task(
            db,
            input_url=glb_url,
            task_type="t_pose",
            owner_type="anon",
            owner_id="telegram-bot",
            created_via_api=True,
            pipeline_kind="convert",
        )
        if task is None:
            return None, error or "task creation failed"
        return task.id, None


async def _auto_submit_ready_jobs() -> None:
    """Push every finished 3D model into the full pipeline by itself.

    Choosing one of the two renders is the only decision asked of the owner.
    Everything after it - retopology, bake, rig, animations, every format -
    used to wait behind a second button that added nothing but a place for the
    job to get stuck when nobody was looking.
    """
    import render_prompting

    try:
        jobs = await render_prompting.list_active_character_gen_jobs()
    except Exception as exc:
        print(f"[Telegram][Renderfin] cannot list jobs for auto-submit: {exc}")
        return
    for job in jobs or []:
        if str(job.get("stage") or "") != "ready":
            continue
        job_id = str(job.get("job_id") or "")
        glb_url = str(job.get("glb_url") or "")
        chat_id = int(job.get("telegram_chat_id") or 0)
        if not job_id or not glb_url:
            continue
        # one submit per job, even across bot restarts
        if not await reserve_notification(chat_id or 0, "renderfin_submit", job_id):
            continue
        try:
            task_id, error = await _submit_generated_model(glb_url)
            if task_id is None:
                raise RuntimeError(error or "не удалось создать задачу")
            await render_prompting.mark_character_gen_submitted(job_id, task_id)
            if chat_id:
                await remember_task_reply_target(
                    chat_id, task_id, int(job.get("telegram_message_id") or 0)
                )
            print(f"[Telegram][Renderfin] job {job_id} auto-submitted as task {task_id}")
        except Exception as exc:
            # the reservation would otherwise block the next attempt forever
            await release_notification(chat_id or 0, "renderfin_submit", job_id)
            print(f"[Telegram][Renderfin] auto-submit failed for {job_id}: {exc}")


async def _auto_submit_loop() -> None:
    while True:
        try:
            await _auto_submit_ready_jobs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[Telegram][Renderfin] auto-submit loop error: {exc}")
        await asyncio.sleep(20)


async def _handle_queue_refresh_callback(update, context) -> None:
    """The queue message's own button: wipe the chat and re-post what is live.

    This is the startup sweep on demand. It deliberately does not restart the
    service: the outcome the operator wants is a clean chat showing current
    statuses, and a button that can take the pipeline down mid-generation is a
    worse way to get it.
    """
    import render_prompting

    query = update.callback_query
    if not query:
        return
    await query.answer("Чищу чат…")
    try:
        removed = await render_prompting.sweep_character_gen_chats()
        print(f"[Telegram][Renderfin] queue refresh removed {removed} message(s)")
    except Exception as exc:
        await query.answer(f"Не вышло: {str(exc)[:150]}", show_alert=True)


async def _handle_submit_callback(update, context) -> None:
    import render_prompting
    from telegram.constants import ParseMode

    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(r"^rfs:([0-9a-fA-F-]{8,64})$", query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    job_id = match.group(1)
    chat_id = int(query.message.chat.id) if query.message else 0
    if not chat_id:
        await query.answer("Нет чата")
        return

    reserved = await reserve_notification(chat_id, "renderfin_submit", job_id)
    if not reserved:
        await query.answer("Уже отправлено в пайплайн")
        return
    await query.answer("Отправляю в пайплайн…")

    try:
        status = await render_prompting.poll_character_gen(job_id)
        glb_url = str(status.get("glb_url") or "")
        if not glb_url:
            raise RuntimeError("у джоба нет GLB")
        task_id, error = await _submit_generated_model(glb_url)
        if task_id is None:
            raise RuntimeError(error or "не удалось создать задачу")
        await render_prompting.mark_character_gen_submitted(job_id, task_id)
        # thread the completion notice under this very message
        await remember_task_reply_target(chat_id, task_id, query.message.message_id)
        url = _task_url(task_id)
        new_caption = (
            f"🚀 <b>Отправлено в полный пайплайн</b>\n"
            f'🔗 <a href="{html.escape(url)}">Task {html.escape(task_id[:8])}…</a> — '
            f"ретопология 1k/10k/100k, запечка, риг, анимации и все форматы; "
            f"по готовности придёт обычное уведомление."
        )
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id, message_id=query.message.message_id,
                caption=new_caption, parse_mode=ParseMode.HTML, reply_markup=None,
            )
        except Exception:
            await context.bot.send_message(
                chat_id=chat_id, text=new_caption, parse_mode=ParseMode.HTML,
                reply_to_message_id=query.message.message_id,
            )
        print(f"[Telegram][Renderfin] job {job_id} submitted as task {task_id}")
    except Exception as e:
        print(f"[Telegram][Renderfin] submit failed for job {job_id}: {e}")
        await release_notification(chat_id, "renderfin_submit", job_id)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Сабмит не удался: {html.escape(str(e)[:300])}",
                reply_to_message_id=query.message.message_id,
            )
        except Exception:
            pass


async def _handle_resume_callback(update, context) -> None:
    """♻️ retry the 3D stage of a failed job without re-rendering the image."""
    import render_prompting

    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(r"^rfe:([0-9a-fA-F-]{8,64})$", query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    job_id = match.group(1)
    try:
        payload = await render_prompting.resume_character_gen(job_id)
    except Exception as e:
        await query.answer(f"Ошибка: {str(e)[:150]}")
        return
    if not payload.get("transitioned"):
        await query.answer(f"Сейчас нельзя (стадия: {payload.get('stage')})")
        return
    await query.answer("Продолжаем…")
    chat_id = int(query.message.chat.id)
    try:
        await context.bot.edit_message_caption(
            chat_id=chat_id, message_id=query.message.message_id,
            caption="♻️ Повторяем 3D-генерацию…",
        )
    except Exception:
        pass


async def _handle_delete_callback(update, context) -> None:
    import render_prompting

    query = update.callback_query
    if not query or not query.data:
        return
    match = re.match(r"^rfd:([0-9a-fA-F-]{8,64})$", query.data)
    if not match:
        await query.answer("Некорректные данные кнопки")
        return
    job_id = match.group(1)
    chat_id = int(query.message.chat.id) if query.message else 0
    await query.answer("Удаляю…")
    try:
        payload = await render_prompting.discard_character_gen(job_id)
        # let the 🎨 button work again for this task
        source_task_id = str(payload.get("source_task_id") or "")
        if chat_id and source_task_id:
            await release_notification(chat_id, "renderfin_gen", source_task_id)
        if chat_id:
            await release_notification(chat_id, "renderfin_submit", job_id)
    except Exception as e:
        print(f"[Telegram][Renderfin] discard failed for job {job_id}: {e}")
    try:
        await context.bot.delete_message(
            chat_id=query.message.chat.id, message_id=query.message.message_id
        )
    except Exception as e:
        print(f"[Telegram][Renderfin] review message delete failed: {e}")


async def _reattach_chargen_watchers(bot) -> None:
    """Delivery of in-flight jobs is owned by the renderfin service, which
    persists them and keeps retrying until Telegram accepts each result. This
    only reports what is still pending so a restart is visible in the log."""
    import render_prompting

    for attempt in range(6):
        try:
            jobs = await render_prompting.list_active_character_gen_jobs()
            if jobs:
                stages = ", ".join(
                    f"{str(j.get('job_id'))[:8]}={j.get('stage')}" for j in jobs
                )
                print(f"[Telegram][Renderfin] {len(jobs)} job(s) in flight: {stages}")
            return
        except Exception as e:
            print(f"[Telegram][Renderfin] job list attempt {attempt + 1}: {e}")
        await asyncio.sleep(5)


async def run_polling() -> None:
    token = _get_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, MessageHandler, filters

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", _start_cmd))
    app.add_handler(CallbackQueryHandler(_handle_generate_callback, pattern=r"^rfg:[0-9a-fA-F-]{8,64}$"))
    app.add_handler(CallbackQueryHandler(_handle_approve_callback, pattern=_APPROVE_PATTERN))
    app.add_handler(CallbackQueryHandler(_handle_regen_callback, pattern=r"^rfr:[0-9a-fA-F-]{8,64}$"))
    app.add_handler(CallbackQueryHandler(_handle_resume_callback, pattern=r"^rfe:[0-9a-fA-F-]{8,64}$"))
    app.add_handler(CallbackQueryHandler(_handle_queue_refresh_callback, pattern=r"^rfq:refresh$"))
    app.add_handler(CallbackQueryHandler(_handle_submit_callback, pattern=r"^rfs:[0-9a-fA-F-]{8,64}$"))
    app.add_handler(CallbackQueryHandler(_handle_delete_callback, pattern=r"^rfd:[0-9a-fA-F-]{8,64}$"))

    group_filter = filters.ChatType.GROUP | filters.ChatType.SUPERGROUP
    print("[Telegram] Support forum reply handler (resolved chat_id from env or telegram_chats)")
    app.add_handler(
        MessageHandler(
            group_filter & filters.TEXT & (~filters.COMMAND),
            _support_forum_message_handler,
        )
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )
    
    # Log startup info
    await _reattach_chargen_watchers(app.bot)
    # a finished model no longer waits behind a button
    asyncio.create_task(_auto_submit_loop())

    active_chats = await get_active_chat_ids()
    print(f"[Telegram] Bot started. Active subscribers: {len(active_chats)}")
    if len(active_chats) == 0:
        print("[Telegram] WARNING: No subscribers! Send /start to @autorigbot to subscribe.")

    # Keep alive
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def main():
    asyncio.run(run_polling())


if __name__ == "__main__":
    main()
