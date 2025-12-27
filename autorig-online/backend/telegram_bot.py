"""Telegram bot integration for AutoRig Online.

- Polling bot (python-telegram-bot) with /start to subscribe a chat.
- Broadcast helpers for task events.

Token is read from environment: TELEGRAM_BOT_TOKEN
"""

from __future__ import annotations

import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy import select

from database import AsyncSessionLocal, TelegramChat
from config import APP_URL


def _get_token() -> str | None:
    tok = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    return tok or None


def _task_url(task_id: str) -> str:
    base = (APP_URL or "").rstrip("/")
    return f"{base}/task?id={task_id}"


def _task_summary(input_url: str | None, input_type: str | None) -> str:
    parts: list[str] = []

    if input_type:
        parts.append(f"type={input_type}")

    ext = None
    if input_url:
        try:
            path = urlparse(input_url).path or ""
            if "." in path:
                ext = path.rsplit(".", 1)[-1].lower()
        except Exception:
            ext = None

    if ext:
        parts.append(f"format=.{ext}")

    return ", ".join(parts) if parts else ""


async def upsert_chat(chat_id: int, chat_type: str | None, title: str | None) -> None:
    async with AsyncSessionLocal() as db:
        rs = await db.execute(select(TelegramChat).where(TelegramChat.chat_id == chat_id))
        rec = rs.scalar_one_or_none()
        now = datetime.utcnow()
        if rec:
            rec.chat_type = chat_type
            rec.title = title
            rec.is_active = True
            rec.last_seen_at = now
        else:
            rec = TelegramChat(
                chat_id=chat_id,
                chat_type=chat_type,
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
        return [row[0] for row in rs.all()]


async def _send_with_retry(coro_factory, *, max_retries: int = 2):
    """Best-effort retry for Telegram rate limits/transient errors."""
    from telegram.error import RetryAfter, TimedOut, NetworkError

    attempt = 0
    while True:
        try:
            return await coro_factory()
        except RetryAfter as e:
            attempt += 1
            if attempt > max_retries:
                return None
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.5)
        except (TimedOut, NetworkError):
            attempt += 1
            if attempt > max_retries:
                return None
            await asyncio.sleep(1.0 * attempt)
        except Exception:
            # Don't crash on unexpected API errors
            return None


async def broadcast_new_task(task_id: str, input_url: str | None, input_type: str | None) -> None:
    token = _get_token()
    if not token:
        return

    from telegram import Bot

    bot = Bot(token=token)
    url = _task_url(task_id)
    summary = _task_summary(input_url, input_type)
    text = f"🟢 New task started\n{url}"
    if summary:
        text += f"\n{summary}"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(lambda: bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False))

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


async def broadcast_task_done(task_id: str, *, duration_seconds: int | None = None) -> None:
    token = _get_token()
    if not token:
        return

    from telegram import Bot

    bot = Bot(token=token)
    url = _task_url(task_id)

    # Ensure local cache exists (final format: mp4)
    try:
        from tasks import cache_task_video_by_id
        await cache_task_video_by_id(task_id)
    except Exception:
        pass

    dur = _format_duration(duration_seconds)
    stats_line = f"\n⏱ {dur}" if dur else ""

    mp4_path = f"/var/autorig/videos/{task_id}.mp4"
    video_path = mp4_path if (os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0) else None

    if not video_path:
        # Fallback: at least notify completion
        text = f"✅ Task completed\n{url}{stats_line}"
        chat_ids = await get_active_chat_ids()
        if not chat_ids:
            return
        await asyncio.gather(*[
            _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text, disable_web_page_preview=False))
            for cid in chat_ids
        ])
        return

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(2)

    async def _one(chat_id: int):
        async with sem:
            # Telegram expects a file-like object
            def _send():
                f = open(video_path, "rb")
                # bot.send_video will close the file? not guaranteed; close in finally
                async def _inner():
                    try:
                        return await bot.send_video(
                            chat_id=chat_id,
                            video=f,
                            caption=f"✅ Task completed\n{url}{stats_line}",
                            supports_streaming=True,
                        )
                    finally:
                        try:
                            f.close()
                        except Exception:
                            pass
                return _inner()

            await _send_with_retry(_send)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


# =============================================================================
# Bot runner (polling)
# =============================================================================
async def _start_cmd(update, context):
    chat = update.effective_chat
    if not chat:
        return
    title = getattr(chat, "title", None) or getattr(chat, "username", None) or getattr(chat, "full_name", None)
    await upsert_chat(chat.id, getattr(chat, "type", None), title)
    await update.message.reply_text("✅ Subscribed. You will receive task notifications here.")


async def run_polling() -> None:
    token = _get_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    from telegram.ext import ApplicationBuilder, CommandHandler

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", _start_cmd))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

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
