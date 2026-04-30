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
from datetime import datetime, timedelta
from urllib.parse import urlparse

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


async def get_active_chat_ids() -> list[int]:
    async with AsyncSessionLocal() as db:
        rs = await db.execute(select(TelegramChat.chat_id).where(TelegramChat.is_active.is_(True)))
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
        )
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


async def reserve_notification(chat_id: int, event_type: str, event_key: str) -> bool:
    """
    Reserve a per-chat notification key atomically.
    Returns True if reserved now, False if it was already reserved/sent earlier.
    """
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


async def _send_with_retry(coro_factory, *, max_retries: int = 2, retry_network: bool = True):
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
                return None
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.5)
        except (TimedOut, NetworkError) as e:
            if not retry_network:
                print(f"[Telegram] Network error (no retry mode): {e}")
                return None
            attempt += 1
            print(f"[Telegram] Network error: {e}, retry {attempt}/{max_retries}")
            if attempt > max_retries:
                print("[Telegram] Max retries exceeded (network)")
                return None
            await asyncio.sleep(1.0 * attempt)
        except Exception as e:
            # Log unexpected API errors
            print(f"[Telegram] API Error: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None


async def broadcast_new_task(
    task_id: str,
    input_url: str | None,
    input_type: str | None,
    progress_page: str | None = None,
    via_api: bool = False,
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
    if summary:
        new_parts.append(f"📄 {html.escape(summary)}")
    new_parts.append(html.escape(metrics_line))
    text = header + "\n" + " | ".join(new_parts)
    if progress_page:
        text += f' | 🔧 <a href="{html.escape(progress_page)}">Worker</a>'
    if source_line:
        text += f"\n{source_line}"

    chat_ids = await get_active_chat_ids()
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
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid, 
                text=text, 
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            ), retry_network=False)
            if result:
                await attach_notification_message_id(chat_id, "task_new", task_id, getattr(result, "message_id", None))
                print(f"[Telegram] New task notification sent to chat {chat_id}")

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

    chat_ids = await get_active_chat_ids()
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

    chat_ids = await get_active_chat_ids()
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
    anon_id: str | None = None
) -> None:
    """Notify when user clicks buy credits button."""
    print(f"[Telegram] broadcast_credits_purchase_click: package={package}, price={price}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping credits purchase notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anonymous")
    text = f"💰 <b>Credits purchase click</b>\nPackage: {html.escape(package)} | Price: {html.escape(price)} | User: {html.escape(actor)}"

    chat_ids = await get_active_chat_ids()
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
    chat_ids = await get_active_chat_ids()
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
    chat_ids = await get_active_chat_ids()
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


async def broadcast_youtube_bonus_click(
    user_email: str
) -> None:
    """Notify when user clicks YouTube bonus link."""
    print(f"[Telegram] broadcast_youtube_bonus_click: user={user_email}")
    token = _get_token()
    if not token:
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    text = f"🎁 <b>YouTube Bonus Clicked!</b>\n👤 User: {html.escape(user_email)} | 💰 +10 credits granted"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML))
        for cid in chat_ids
    ])


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

    chat_ids = await get_active_chat_ids()
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

    chat_ids = await get_active_chat_ids()
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
) -> None:
    """Notify when credits are successfully purchased via Gumroad."""
    print(f"[Telegram] broadcast_credits_purchased: {credits} credits for {user_email} (test={is_test})")
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

    chat_ids = await get_active_chat_ids()
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


async def _download_video_from_worker(task_id: str) -> str | None:
    """Download video from worker API and cache locally."""
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if not task or not task.guid or not task.worker_api:
                print(f"[Telegram] Cannot download video: task {task_id} has no guid or worker_api")
                return None
            
            parsed = urlparse(task.worker_api)
            worker_base = f"{parsed.scheme}://{parsed.netloc}"
            video_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_video.mp4"
        
        print(f"[Telegram] Downloading video from {video_url}")
        
        # Download video
        async with httpx.AsyncClient() as client:
            resp = await client.get(video_url, timeout=60.0, follow_redirects=True)
            if resp.status_code == 200:
                cache_dir = "/var/autorig/videos"
                os.makedirs(cache_dir, exist_ok=True)
                cache_path = f"{cache_dir}/{task_id}.mp4"
                with open(cache_path, "wb") as f:
                    f.write(resp.content)
                print(f"[Telegram] Video cached at {cache_path} ({len(resp.content)} bytes)")
                return cache_path
            else:
                print(f"[Telegram] Failed to download video: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[Telegram] Failed to download video: {e}")
    return None


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

    chat_ids = await get_active_chat_ids()
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
    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    from worker_labels import format_worker_stalled_telegram_html

    worker_block = format_worker_stalled_telegram_html(worker_url)
    sample = ", ".join((sample_task_ids or [])[:3])
    sample_line = f"\n🧩 Tasks: <code>{html.escape(sample)}</code>" if sample else ""
    text = (
        f"🚨 <b>Worker stalled</b>\n"
        f"{worker_block}\n"
        f"📌 stalled: {int(stalled_tasks)} | ⏱ oldest: {int(oldest_stalled_minutes)}m"
        f"{sample_line}"
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

    chat_ids = await get_active_chat_ids()
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

        duration = None
        if task.created_at:
            duration = int((datetime.utcnow() - task.created_at).total_seconds())
        progress_url = None
        if task.guid and task.worker_api:
            worker_base = get_worker_base_url(task.worker_api)
            progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"

        print(f"[Telegram] Scheduling done notification for task {task_id} (after content rating)")
        asyncio.create_task(
            broadcast_task_done(task_id, duration_seconds=duration, progress_page=progress_url)
        )


async def broadcast_task_done(task_id: str, *, duration_seconds: int | None = None, progress_page: str | None = None) -> None:
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
    done_parts = [f'🔗 <a href="{html.escape(url)}">View Result</a>']
    if owner_email:
        done_parts.append(f"👤 {html.escape(owner_email)}")
    if dur:
        done_parts.append(f"⏱ {html.escape(dur)}")
    done_parts.append(html.escape(metrics_line))

    text = f"✅ <b>Task completed</b>\n{rating_line}\n" + " | ".join(done_parts)
    if resolved_progress:
        text += f'\n🔧 <a href="{html.escape(resolved_progress)}">Worker Logs</a>'

    # Try to find cached video
    mp4_path = f"/var/autorig/videos/{task_id}.mp4"
    video_path = mp4_path if (os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0) else None

    # If not cached, try to download from worker
    if not video_path:
        video_path = await _download_video_from_worker(task_id)

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        print("[Telegram] No active chats, skipping done notification")
        return

    print(f"[Telegram] Sending done notification to {len(chat_ids)} chat(s), video={video_path is not None}")

    if not video_path:
        # Fallback: at least notify completion
        async def _one_text(chat_id: int):
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
            return await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            ), retry_network=False)

        results = await asyncio.gather(*[_one_text(cid) for cid in chat_ids])
        sent_count = sum(1 for r in results if r is not None)
        print(f"[Telegram] Done notification sent to {sent_count}/{len(chat_ids)} chat(s)")
        return

    sem = asyncio.Semaphore(2)
    caption = text

    async def _one(chat_id: int):
        async with sem:
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
                        )
                    finally:
                        try:
                            f.close()
                        except Exception:
                            pass
                return _inner()

            await _send_with_retry(_send, retry_network=False)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


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

    chat_ids = await get_active_chat_ids()
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
    await update.message.reply_text("✅ Subscribed. You will receive task notifications here.")


async def run_polling() -> None:
    token = _get_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", _start_cmd))

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
    await app.updater.start_polling(drop_pending_updates=True)
    
    # Log startup info
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
