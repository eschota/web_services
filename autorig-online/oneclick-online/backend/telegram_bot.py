"""Telegram bot integration for OneClick Online.

- Polling bot (python-telegram-bot) with /start to subscribe a chat.
- Broadcast helpers for task events.
- Server startup notifications with statistics.

Token is read from environment: TELEGRAM_BOT_TOKEN
"""

from __future__ import annotations

import os
import asyncio
import re
from datetime import datetime
from urllib.parse import urlparse

import httpx
from sqlalchemy import select, func

from database import AsyncSessionLocal, TelegramChat, Task
from config import APP_URL


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


TELEGRAM_MAX_RETRIES = _env_int("TELEGRAM_MAX_RETRIES", 5)
TELEGRAM_RETRY_BASE_DELAY_SEC = float((os.environ.get("TELEGRAM_RETRY_BASE_DELAY_SEC") or "1.5").strip() or "1.5")
TELEGRAM_POOL_SIZE = _env_int("TELEGRAM_POOL_SIZE", 32)
TELEGRAM_POOL_TIMEOUT_SEC = float((os.environ.get("TELEGRAM_POOL_TIMEOUT_SEC") or "45").strip() or "45")
TELEGRAM_CONNECT_TIMEOUT_SEC = float((os.environ.get("TELEGRAM_CONNECT_TIMEOUT_SEC") or "20").strip() or "20")
TELEGRAM_READ_TIMEOUT_SEC = float((os.environ.get("TELEGRAM_READ_TIMEOUT_SEC") or "90").strip() or "90")
TELEGRAM_WRITE_TIMEOUT_SEC = float((os.environ.get("TELEGRAM_WRITE_TIMEOUT_SEC") or "90").strip() or "90")


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
    
    # For Free3D URLs, show the full URL
    if "free3d.online" in input_url:
        return f"📦 Source: {input_url}"
    
    # For other URLs, show domain + path
    try:
        parsed = urlparse(input_url)
        domain = parsed.netloc or ""
        path = parsed.path or ""
        # Truncate very long paths
        if len(path) > 50:
            path = path[:25] + "..." + path[-22:]
        return f"📦 Source: {domain}{path}"
    except Exception:
        return f"📦 Source: {input_url[:80]}..." if len(input_url) > 80 else f"📦 Source: {input_url}"


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


def _get_env_chat_ids() -> list[int]:
    """Optional static chat IDs from env, comma/space separated."""
    raw = (os.environ.get("TELEGRAM_CHAT_IDS") or "").strip()
    if not raw:
        return []

    normalized = raw.replace(";", ",").replace(" ", ",")
    chat_ids: list[int] = []
    for token in normalized.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            chat_ids.append(int(token))
        except Exception:
            print(f"[Telegram] Skipping invalid TELEGRAM_CHAT_IDS value: {token!r}")
    return chat_ids


async def get_notification_chat_ids() -> list[int]:
    """Merge DB subscriptions and optional static env recipients."""
    db_chat_ids = await get_active_chat_ids()
    env_chat_ids = _get_env_chat_ids()

    merged: list[int] = []
    seen: set[int] = set()
    for cid in db_chat_ids + env_chat_ids:
        if cid in seen:
            continue
        seen.add(cid)
        merged.append(cid)
    return merged


async def deactivate_chat(chat_id: int, *, reason: str | None = None) -> None:
    """Mark chat inactive for permanent send failures."""
    async with AsyncSessionLocal() as db:
        rs = await db.execute(select(TelegramChat).where(TelegramChat.chat_id == chat_id))
        rec = rs.scalar_one_or_none()
        if not rec or not rec.is_active:
            return
        rec.is_active = False
        rec.last_seen_at = datetime.utcnow()
        await db.commit()
    if reason:
        print(f"[Telegram] Deactivated chat {chat_id}: {reason}")
    else:
        print(f"[Telegram] Deactivated chat {chat_id}")


def _is_permanent_chat_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    permanent_markers = (
        "bot was blocked by the user",
        "user_is_blocked",
        "chat not found",
        "have no rights to send a message",
        "bot is not a member",
        "group chat was upgraded",
        "user is deactivated",
    )
    return any(marker in msg for marker in permanent_markers)


def _make_bot(token: str):
    from telegram import Bot
    from telegram.request import HTTPXRequest

    request = HTTPXRequest(
        connection_pool_size=TELEGRAM_POOL_SIZE,
        pool_timeout=TELEGRAM_POOL_TIMEOUT_SEC,
        connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SEC,
        read_timeout=TELEGRAM_READ_TIMEOUT_SEC,
        write_timeout=TELEGRAM_WRITE_TIMEOUT_SEC,
    )
    return Bot(token=token, request=request)


async def _send_with_retry(coro_factory, *, max_retries: int | None = None, chat_id: int | None = None):
    """Best-effort retry for Telegram rate limits/transient errors."""
    from telegram.error import RetryAfter, TimedOut, NetworkError, Forbidden, BadRequest

    if max_retries is None:
        max_retries = TELEGRAM_MAX_RETRIES

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
        except (Forbidden, BadRequest) as e:
            # Permanent permission/chat errors should not be retried.
            print(f"[Telegram] API permission/chat error: {e}")
            if chat_id is not None and _is_permanent_chat_error(e):
                await deactivate_chat(chat_id, reason=str(e))
            return None
        except (TimedOut, NetworkError) as e:
            if _is_permanent_chat_error(e):
                print(f"[Telegram] Permanent chat error: {e}")
                if chat_id is not None:
                    await deactivate_chat(chat_id, reason=str(e))
                return None
            attempt += 1
            print(f"[Telegram] Network error: {e}, retry {attempt}/{max_retries}")
            if attempt > max_retries:
                print("[Telegram] Max retries exceeded (network)")
                return None
            backoff = TELEGRAM_RETRY_BASE_DELAY_SEC * attempt
            await asyncio.sleep(min(backoff, 15.0))
        except Exception as e:
            # Log unexpected API errors
            print(f"[Telegram] API Error: {type(e).__name__}: {e}")
            if chat_id is not None and _is_permanent_chat_error(e):
                await deactivate_chat(chat_id, reason=str(e))
            import traceback
            traceback.print_exc()
            return None


def _make_viewer_keyboard(chat_id: int, webapp_url: str):
    """Create keyboard with appropriate button type for chat.
    
    - Private chats (positive ID): web_app button (opens in Telegram WebApp)
    - Groups/channels (negative ID): url button (opens in browser)
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from telegram._webappinfo import WebAppInfo
    
    if chat_id > 0:
        # Private chat - web_app button works here
        button = InlineKeyboardButton("🎮 Open Viewer", web_app=WebAppInfo(url=webapp_url))
    else:
        # Group/channel - use regular URL button
        button = InlineKeyboardButton("🎮 Open Viewer", url=webapp_url)
    
    return InlineKeyboardMarkup([[button]])


async def broadcast_new_task(task_id: str, input_url: str | None, input_type: str | None, progress_page: str | None = None) -> None:
    print(f"[Telegram] broadcast_new_task called for task {task_id}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping new task notification")
        return

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    bot = _make_bot(token)
    url = _task_url(task_id)
    summary = _task_summary(input_url, input_type)
    source_line = _format_input_url(input_url)
    scene_size_bytes = await _fetch_content_length(input_url, timeout=6.0)
    details_lines: list[str] = [f"🆔 Task: {task_id}"]
    if source_line:
        details_lines.append(source_line)
    if scene_size_bytes is not None:
        details_lines.append(f"📦 Scene size: {_format_bytes(scene_size_bytes)}")
    if summary:
        details_lines.append(f"🧩 Input: {summary}")
    if progress_page:
        details_lines.append(f"🔧 Worker: {progress_page}")

    text = "\n".join([
        "🟢 New task started",
        "🔗 Open Scene",
        *details_lines,
        "👇 Use button below",
    ])
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Open Scene", url=url)]])

    chat_ids = await get_notification_chat_ids()
    print(f"[Telegram] Sending new task notification to {len(chat_ids)} chat(s)")
    if not chat_ids:
        print("[Telegram] No recipients. Subscribe with /start or set TELEGRAM_CHAT_IDS in env.")
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid, 
                text=text, 
                reply_markup=keyboard,
                disable_web_page_preview=True
            ), chat_id=chat_id)
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

    bot = _make_bot(token)
    url = _task_url(task_id)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anon")
    source_label = source or "download_all"
    text = f"💳 Purchase intent\n{url}\nUser: {actor}\nSource: {source_label}"

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                disable_web_page_preview=False
            ), chat_id=chat_id)
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

    bot = _make_bot(token)
    actor = user_email or (f"anon:{anon_id}" if anon_id else "anonymous")
    text = f"💰 Credits purchase click\nPackage: {package}\nPrice: {price}\nUser: {actor}"

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                disable_web_page_preview=True
            ), chat_id=chat_id)
            if result:
                print(f"[Telegram] Credits purchase click sent to chat {chat_id}")

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


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

    bot = _make_bot(token)
    test_label = " [TEST]" if is_test else ""
    text = (
        f"✅ Credits purchased!{test_label}\n"
        f"💰 Amount: {credits} credits\n"
        f"💵 Price: {price}\n"
        f"👤 User: {user_email}\n"
        f"📦 Product: {product}\n"
        f"🆔 Sale: {sale_id}"
    )

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        return

    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            result = await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid,
                text=text,
                disable_web_page_preview=True
            ), chat_id=chat_id)
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


def _format_bytes(size_bytes: int | None) -> str:
    if size_bytes is None or size_bytes < 0:
        return ""
    value = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_idx = 0
    while value >= 1024.0 and unit_idx < len(units) - 1:
        value /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(value)} {units[unit_idx]}"
    return f"{value:.2f} {units[unit_idx]}"


async def _fetch_content_length(url: str | None, *, timeout: float = 8.0) -> int | None:
    if not url:
        return None
    try:
        async with httpx.AsyncClient() as client:
            head = await client.head(url, timeout=timeout, follow_redirects=True)
            cl = (head.headers.get("content-length") or "").strip()
            if cl.isdigit():
                size = int(cl)
                if size > 0:
                    return size

            # Fallback for backends that don't expose size on HEAD.
            probe = await client.get(
                url,
                headers={"Range": "bytes=0-0"},
                timeout=timeout,
                follow_redirects=True,
            )
            content_range = (probe.headers.get("content-range") or "").strip()
            m = re.search(r"/(\d+)$", content_range)
            if m:
                total = int(m.group(1))
                if total > 0:
                    return total
            cl_probe = (probe.headers.get("content-length") or "").strip()
            if cl_probe.isdigit():
                size = int(cl_probe)
                if size > 0:
                    return size
    except Exception:
        return None
    return None


def _extract_camera_count_from_summary(export_summary: str | None) -> int | None:
    if not export_summary:
        return None
    patterns = (
        r"\bprocessed\s+cameras?\s*[:=]?\s*(\d+)\b",
        r"\bcameras?\s*[:=]\s*(\d+)\b",
        r"\bcamera\s+count\s*[:=]\s*(\d+)\b",
    )
    text = str(export_summary)
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            value = int(match.group(1))
            if value >= 0:
                return value
        except Exception:
            continue
    return None


def _estimate_camera_count_from_entries(entries: list[dict]) -> int | None:
    if not entries:
        return None
    camera_keys: set[str] = set()
    render_seen = False
    for entry in entries:
        if entry.get("category") != "renders":
            continue
        render_seen = True
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        base = os.path.basename(name).lower()
        if "." in base:
            base = base.rsplit(".", 1)[0]
        # Common render suffixes: physcamera001_vp, cam1_wire, etc.
        base = re.sub(r"_(vp|wire|ao|depth|mask|max|preview|thumb)$", "", base)
        base = re.sub(r"[^a-z0-9_\-]+", "", base)
        if base:
            camera_keys.add(base)
    if camera_keys:
        return len(camera_keys)
    if render_seen:
        # Renders are present but naming is unknown; at least one camera exists.
        return 1
    return None


async def _download_file(url: str, dest_path: str, *, timeout: float = 60.0) -> str | None:
    """Download a file from *url* and save to *dest_path*. Returns path on success."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=timeout, follow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 0:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                with open(dest_path, "wb") as f:
                    f.write(resp.content)
                print(f"[Telegram] Downloaded {url} -> {dest_path} ({len(resp.content)} bytes)")
                return dest_path
            else:
                print(f"[Telegram] Download failed {url}: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[Telegram] Download error {url}: {e}")
    return None


PLATFORM_ORDER = [
    ("renders", "3ds Max"),
    ("unity_renders", "URP"),
    ("unity_renders_oc_android_apk", "Android"),
    ("unity_renders_oc_vr", "Quest VR"),
    ("unity_renders_oc_webbuild", "WebGL"),
    ("unity_renders_oc_hdrp", "HDRP"),
]


async def broadcast_task_restarted(task_id: str, reason: str = "manual", admin_email: str | None = None) -> None:
    """Notify about task restart."""
    print(f"[Telegram] broadcast_task_restarted called for task {task_id}, reason={reason}")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping restart notification")
        return

    bot = _make_bot(token)
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
    text = f"🔄 Task restarted ({reason})\n{url}{admin_line}"

    chat_ids = await get_notification_chat_ids()
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
            ), chat_id=chat_id)

    await asyncio.gather(*[_one(cid) for cid in chat_ids])


async def broadcast_bulk_restart_summary(total: int, restarted: int, errors: list, admin_email: str) -> None:
    """Notify about bulk restart completion."""
    print(f"[Telegram] broadcast_bulk_restart_summary: {restarted}/{total}")
    token = _get_token()
    if not token:
        return

    bot = _make_bot(token)
    
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

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        return

    await asyncio.gather(*[
        _send_with_retry(
            lambda cid=cid: bot.send_message(chat_id=cid, text=text, disable_web_page_preview=True),
            chat_id=cid
        )
        for cid in chat_ids
    ])


async def broadcast_task_done(
    task_id: str,
    *,
    duration_seconds: int | None = None,
    product_entries: list | None = None,
    failure_reason: str | None = None,
) -> None:
    """Send task completion/failure notification to all subscribers.

    Sends at most 2 messages per chat:
    1. Media group (HDRP video, URP video, one cam1 image per platform)
       with caption on the first element.
    2. Short text message with inline keyboard buttons (Open Scene / Open Web Build).
    """
    print(f"[Telegram] broadcast_task_done called for task {task_id} (failure={failure_reason})")
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping done notification")
        return

    from telegram import (
        InputMediaPhoto, InputMediaVideo,
        InlineKeyboardButton, InlineKeyboardMarkup,
    )

    bot = _make_bot(token)
    task_url = _task_url(task_id)

    # --- Fetch owner email ---
    owner_email = None
    task_input_url = None
    task_export_summary = None
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                if task.owner_type == "user":
                    owner_email = task.owner_id
                if not product_entries and hasattr(task, "product_entries"):
                    product_entries = task.product_entries
                task_input_url = getattr(task, "input_url", None)
                task_export_summary = getattr(task, "export_summary", None)
    except Exception as e:
        print(f"[Telegram] Failed to get task details: {e}")

    entries = product_entries or []
    dur = _format_duration(duration_seconds)
    source_line = _format_input_url(task_input_url)
    scene_size_bytes = await _fetch_content_length(task_input_url)
    camera_count = _extract_camera_count_from_summary(task_export_summary)
    if camera_count is None:
        camera_count = _estimate_camera_count_from_entries(entries)
    max_render_count = sum(1 for e in entries if e.get("category") == "renders" and e.get("url"))

    # --- Build caption (HTML) ---
    if failure_reason:
        short_reason = str(failure_reason).strip()
        if len(short_reason) > 500:
            short_reason = short_reason[:497].rstrip() + "..."
        status_line = f"❌ Task failed: {short_reason}"
    else:
        status_line = "✅ Task completed"
    caption_parts = [status_line]
    if owner_email:
        caption_parts.append(f"👤 {owner_email}")
    if dur:
        caption_parts.append(f"⏱ {dur}")
    caption_html = "\n".join(caption_parts)

    # --- Download media into a temp dir ---
    cache_dir = f"/var/oneclick/media/{task_id}"
    os.makedirs(cache_dir, exist_ok=True)

    # media_items: list of (file_path, label, "video"|"photo")
    media_items: list[tuple[str, str, str]] = []

    # Videos: HDRP first, then URP
    for vid_cat, label in [("unity_hdrp_video", "HDRP Video"), ("unity_video", "URP Video")]:
        vid_entry = next((e for e in entries if e.get("category") == vid_cat and e.get("url")), None)
        if vid_entry:
            suffix = "hdrp" if "hdrp" in vid_cat else "urp"
            p = await _download_file(vid_entry["url"], os.path.join(cache_dir, f"video_{suffix}.mp4"))
            if p:
                media_items.append((p, label, "video"))

    # Images: one cam1 per platform (first entry in each category)
    for cat, label in PLATFORM_ORDER:
        first = next((e for e in entries if e.get("category") == cat and e.get("url")), None)
        if first:
            safe_name = f"{cat}_cam1.png".replace("/", "_")
            p = await _download_file(first["url"], os.path.join(cache_dir, safe_name))
            if p:
                media_items.append((p, label, "photo"))

    # WebGL build URL
    webgl_entry = next((e for e in entries if e.get("category") == "unity_web_build_folder" and e.get("url")), None)
    webgl_url = webgl_entry["url"] if webgl_entry else None
    available_video_labels = []
    if any(e.get("category") == "unity_video" and e.get("url") for e in entries):
        available_video_labels.append("URP")
    if any(e.get("category") == "unity_hdrp_video" and e.get("url") for e in entries):
        available_video_labels.append("HDRP")

    details_lines: list[str] = [f"🆔 Task: {task_id}"]
    if source_line:
        details_lines.append(source_line)
    if scene_size_bytes is not None:
        details_lines.append(f"📦 Scene size: {_format_bytes(scene_size_bytes)}")
    if camera_count is not None:
        details_lines.append(f"📷 Cameras: {camera_count}")
    if max_render_count > 0:
        details_lines.append(f"🖼 3ds Max renders: {max_render_count}")
    if available_video_labels:
        details_lines.append(f"🎬 Video output: {', '.join(available_video_labels)}")
    if webgl_url:
        details_lines.append("🌐 WebGL: ready")
    if dur:
        details_lines.append(f"⏱ Conversion time: {dur}")

    link_text_title = "❌ Task failed" if failure_reason else "✅ Task completed"
    link_message_text = "\n".join([
        f"{link_text_title}",
        "🔗 Open Scene",
        *details_lines,
        "👇 Use buttons below",
    ])

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        print("[Telegram] No active chats, skipping done notification")
        return

    print(f"[Telegram] Sending done notification to {len(chat_ids)} chat(s), "
          f"media_items={len(media_items)}, webgl={webgl_url is not None}")

    # --- Build inline keyboard ---
    buttons: list[InlineKeyboardButton] = [
        InlineKeyboardButton("Open Scene", url=task_url),
    ]
    if webgl_url:
        buttons.append(InlineKeyboardButton("🌐 Open Web Build", url=webgl_url))
    keyboard = InlineKeyboardMarkup([buttons])

    sem = asyncio.Semaphore(2)

    async def _send_to_chat(chat_id: int):
        async with sem:
            # 1. Media group (videos + images, max 10)
            if media_items:
                def _build_and_send(cid=chat_id):
                    files = []
                    media = []
                    try:
                        for idx, (fpath, label, mtype) in enumerate(media_items[:10]):
                            fobj = open(fpath, "rb")
                            files.append(fobj)
                            cap = caption_html if idx == 0 else label
                            parse = "HTML" if idx == 0 else None
                            if mtype == "video":
                                media.append(InputMediaVideo(
                                    media=fobj, caption=cap, parse_mode=parse,
                                    supports_streaming=True,
                                ))
                            else:
                                media.append(InputMediaPhoto(
                                    media=fobj, caption=cap, parse_mode=parse,
                                ))
                    except Exception:
                        for f in files:
                            try: f.close()
                            except: pass
                        raise

                    async def _inner():
                        try:
                            return await bot.send_media_group(chat_id=cid, media=media)
                        finally:
                            for f in files:
                                try: f.close()
                                except: pass
                    return _inner()

                await _send_with_retry(_build_and_send, chat_id=chat_id)
            else:
                await _send_with_retry(lambda cid=chat_id: bot.send_message(
                    chat_id=cid, text=caption_html,
                    parse_mode="HTML", disable_web_page_preview=True,
                ), chat_id=chat_id)

            # 2. Inline keyboard message
            await _send_with_retry(lambda cid=chat_id: bot.send_message(
                chat_id=cid, text=link_message_text,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            ), chat_id=chat_id)

    await asyncio.gather(*[_send_to_chat(cid) for cid in chat_ids])


async def broadcast_server_startup() -> None:
    """Send server startup notification with task statistics."""
    token = _get_token()
    if not token:
        print("[Telegram] No token, skipping startup notification")
        return

    bot = _make_bot(token)
    
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

    chat_ids = await get_notification_chat_ids()
    if not chat_ids:
        print("[Telegram] No active chats for startup notification")
        print("[Telegram] Subscribe with /start or set TELEGRAM_CHAT_IDS in env.")
        return

    print(f"[Telegram] Sending startup notification to {len(chat_ids)} chat(s)")
    
    sem = asyncio.Semaphore(3)

    async def _one(chat_id: int):
        async with sem:
            await _send_with_retry(
                lambda: bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True),
                chat_id=chat_id
            )

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
        print("[Telegram] WARNING: No subscribers! Send /start to @oneclickbot to subscribe.")

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
