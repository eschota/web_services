"""Telegram bot integration for AutoRig Online.

- Polling bot (python-telegram-bot) with /start to subscribe a chat.
- Broadcast helpers for task events.
- Server startup notifications with statistics.

Token is read from environment: TELEGRAM_BOT_TOKEN
"""

from __future__ import annotations

import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse

import httpx
from sqlalchemy import select, func

from database import AsyncSessionLocal, TelegramChat, Task
from config import APP_URL


def _get_token() -> str | None:
    tok = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    return tok or None


def _task_url(task_id: str) -> str:
    """Task URL with cache-busting parameter for fresh Telegram previews."""
    import time
    base = (APP_URL or "").rstrip("/")
    ts = int(time.time())
    return f"{base}/task?id={task_id}&t={ts}"


def _webapp_url(task_id: str) -> str:
    """URL for Telegram WebApp mode (minimal UI) with cache-busting."""
    import time
    base = (APP_URL or "").rstrip("/")
    # Add timestamp to bypass Telegram's link preview cache
    ts = int(time.time())
    return f"{base}/task?id={task_id}&mode=webapp&t={ts}"


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


def _format_input_url(input_url: str | None) -> str:
    """Format input_url for display in Telegram message."""
    if not input_url:
        return ""
    
    # For Free3D URLs, show a compact markdown link
    if "free3d.online" in input_url:
        return f"📦 [Free3D Model]({input_url})"
    
    # For other URLs, show domain + path as a markdown link
    try:
        parsed = urlparse(input_url)
        domain = parsed.netloc or ""
        path = parsed.path or ""
        # Truncate very long paths
        if len(path) > 40:
            path = path[:20] + "..." + path[-15:]
        return f"📦 [{domain}{path}]({input_url})"
    except Exception:
        return f"📦 [Source]({input_url})"


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
            print(f"[Telegram] Rate limited, retry {attempt}/{max_retries}")
            if attempt > max_retries:
                print("[Telegram] Max retries exceeded (rate limit)")
                return None
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)) + 0.5)
        except (TimedOut, NetworkError) as e:
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


async def broadcast_new_task(task_id: str, input_url: str | None, input_type: str | None, progress_page: str | None = None) -> None:
    print(f"[Telegram] broadcast_new_task called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping new task notification")
        return

    from telegram import Bot
    from telegram.constants import ParseMode

    bot = Bot(token=token)
    url = _task_url(task_id)
    webapp_url = _webapp_url(task_id)
    summary = _task_summary(input_url, input_type)
    source_line = _format_input_url(input_url)
    
    text = f"🟢 New task started\n🔗 [View Task]({url})"
    if summary:
        text += f"\n📄 {summary}"
    if source_line:
        text += f"\n{source_line}"
    if progress_page:
        text += f"\n🔧 [Worker Page]({progress_page})"

    chat_ids = await get_active_chat_ids()
    print(f"[Telegram] Sending new task notification to {len(chat_ids)} chat(s)")
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            # Use appropriate button type for chat
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid, 
                text=text, 
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=False
            ))
            if result:
                print(f"[Telegram] New task notification sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_purchase_intent(
    task_id: str,
    user_email: str | None = None,
    anon_id: str | None = None,
    source: str | None = None
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
    text = f"💳 Purchase intent\n🔗 [Task]({url})\n👤 {actor} | 📍 {source_label}"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=False
            ))
            if result:
                print(f"[Telegram] Purchase intent sent to chat {chat_id}")

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

    bot = Bot(token=token)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anonymous")
    text = f"💰 Credits purchase click\nPackage: {package}\nPrice: {price}\nUser: {actor}"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                disable_web_page_preview=True
            ))
            if result:
                print(f"[Telegram] Credits purchase click sent to chat {chat_id}")

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
    bot = Bot(token=token)
    text = f"🎁 YouTube Bonus Clicked!\n👤 User: {user_email}\n💰 +10 credits granted"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text))
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
    bot = Bot(token=token)
    text = f"📝 New Feedback Submitted!\n👤 User: {user_email}\n💬 Text: {text_content[:500]}"

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(lambda cid=cid: bot.send_message(chat_id=cid, text=text))
        for cid in chat_ids
    ])


async def broadcast_credits_purchased(
    credits: int,
    price: str,
    user_email: str,
    product: str,
    sale_id: str,
    is_test: bool = False
) -> None:
    """Notify when credits are successfully purchased via Gumroad."""
    print(f"[Telegram] broadcast_credits_purchased: {credits} credits for {user_email} (test={is_test})")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping credits purchased notification")
        return

    from telegram import Bot

    bot = Bot(token=token)
    test_label = " [TEST]" if is_test else ""
    text = (
        f"✅ Credits purchased!{test_label}\n"
        f"💰 Amount: {credits} credits\n"
        f"💵 Price: {price}\n"
        f"👤 User: {user_email}\n"
        f"📦 Product: {product}\n"
        f"🆔 Sale: {sale_id}"
    )

    chat_ids = await get_active_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
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

    bot = Bot(token=token)
    url = _task_url(task_id)
    webapp_url = _webapp_url(task_id)
    
    # Get task details
    input_info = ""
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                summary = _task_summary(task.input_url, task.input_type)
                if summary:
                    input_info = f"\n{summary}"
    except Exception as e:
        print(f"[Telegram] Failed to get task details: {e}")
    
    admin_line = f"\n👤 Admin: {admin_email}" if admin_email else ""
    text = f"🔄 Task restarted ({reason})\n{url}{input_info}{admin_line}"

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
                disable_web_page_preview=False
            ))

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
    webapp_url = _webapp_url(task_id)

    # Get task details including owner for author line
    owner_email = None
    if not progress_page:
        try:
            async with AsyncSessionLocal() as db:
                from sqlalchemy import select
                result = await db.execute(select(Task).where(Task.id == task_id))
                task = result.scalar_one_or_none()
                if task:
                    # Get author email if owner is a user
                    if task.owner_type == "user":
                        owner_email = task.owner_id
                    # Construct progress_page URL from worker_api and guid
                    if task.guid and task.worker_api:
                        from urllib.parse import urlparse
                        parsed = urlparse(task.worker_api)
                        worker_base = f"{parsed.scheme}://{parsed.netloc}"
                        progress_page = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
        except Exception as e:
            print(f"[Telegram] Failed to get task details: {e}")
    else:
        # Still need to fetch owner_email even if progress_page is passed
        try:
            async with AsyncSessionLocal() as db:
                from sqlalchemy import select
                result = await db.execute(select(Task).where(Task.id == task_id))
                task = result.scalar_one_or_none()
                if task and task.owner_type == "user":
                    owner_email = task.owner_id
        except Exception as e:
            print(f"[Telegram] Failed to get owner_email: {e}")

    dur = _format_duration(duration_seconds)
    stats_line = f"\n⏱ {dur}" if dur else ""
    author_line = f"\n👤 Author: {owner_email}" if owner_email else ""
    
    text = f"✅ Task completed\n🔗 [View Result]({url}){author_line}{stats_line}"
    if progress_page:
        text += f"\n🔧 [Worker Logs]({progress_page})"

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
        results = await asyncio.gather(*[
            _send_with_retry(lambda cid=cid: bot.send_message(
                chat_id=cid, 
                text=text, 
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=False
            ))
            for cid in chat_ids
        ])
        sent_count = sum(1 for r in results if r is not None)
        print(f"[Telegram] Done notification sent to {sent_count}/{len(chat_ids)} chat(s)")
        return

    sem = asyncio.Semaphore(2)
    caption = text

    async def _one(chat_id: int):
        async with sem:
            # Use appropriate button type for chat
            
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
                            parse_mode=ParseMode.MARKDOWN,
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
async def _start_cmd(update, context):
    chat = update.effective_chat
    if not chat:
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

    from telegram.ext import ApplicationBuilder, CommandHandler

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", _start_cmd))

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
