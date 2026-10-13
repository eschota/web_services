"""Terminal task notifications, isolated from shared Telegram bot edits.

Completion retains the deployed video-only policy. QA needs_review stays
visible in V3 extra_html and does not become an approved rig.
"""
import asyncio, html, os
from urllib.parse import urlparse
from sqlalchemy import select
from database import AsyncSessionLocal, Task
import telegram_bot as _api
GALLERY_POSTER_SCRIPT = os.getenv("AUTORIG_GALLERY_POSTER_SCRIPT", "/srv/autorig/gallery-poster/gallery_poster.py")
V3_PREVIEW_VIDEO_DIR = os.getenv("AUTORIG_V3_PREVIEW_VIDEO_DIR", "/srv/autorig/data/static/previews-v3")
V3_POSTER_DIR_TG = os.getenv("AUTORIG_V3_POSTER_DIR", "/srv/autorig/data/static/posters-v3")
_format_duration = _api._format_duration
_format_content_rating_line = _api._format_content_rating_line

def _tg_preview_wait_seconds() -> int:
    """Initial wait for the actual farm viewer video; no poster substitutes."""
    try:
        return max(0, min(1800, int(os.getenv('AUTORIG_TG_DONE_PREVIEW_WAIT_SEC', '600'))))
    except ValueError:
        return 600

async def _render_v3_preview_now(task_id: str) -> None:
    """Event-driven poster + clip for one finished task (owner, 2026-10-11: «уникальные результаты рига, а не
    заглушка»): the poster service's own `once --task`, at normal priority for this one job, so the poster lands
    in ~10-30 s and the clip right after. Fire and forget; the crash guard and the light view GLB of
    gallery_poster.py apply."""
    import sys as _sys
    if not os.path.isfile(GALLERY_POSTER_SCRIPT):
        return
    try:
        proc = await asyncio.create_subprocess_exec('timeout', '600', _sys.executable, '-P', GALLERY_POSTER_SCRIPT, 'once', '--task', task_id, '--pause', '0', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, env=dict(os.environ, GP_NICE='0', GP_CLIP_NICE='5'))
        (out, _) = await proc.communicate()
        tail = (out or b'').decode('utf-8', 'replace').strip().splitlines()[-1:]
        print(f'[Telegram] V3 preview render for {task_id}: rc={proc.returncode} {tail}')
    except Exception as exc:
        print(f'[Telegram] V3 preview render failed to start for {task_id}: {exc}')

def _v3_preview_files(task_id: str) -> tuple:
    video = os.path.join(V3_PREVIEW_VIDEO_DIR, f'{task_id}.mp4')
    poster = os.path.join(V3_POSTER_DIR_TG, f'{task_id}.jpg')
    ok = lambda p, n: os.path.isfile(p) and n < os.path.getsize(p) < 48 * 1024 * 1024
    (video_ok, poster_ok) = (ok(video, 20000), ok(poster, 8000))
    if video_ok and poster_ok and (os.path.getmtime(video) < os.path.getmtime(poster) - 1):
        video_ok = False
    return (video if video_ok else None, poster if poster_ok else None)

async def _wait_v3_preview(task_id: str, wait_seconds: int) -> tuple:
    """Wait for the task's real V3 clip; a poster never satisfies completion."""
    deadline = asyncio.get_running_loop().time() + max(0, wait_seconds)
    while True:
        (video, poster) = _v3_preview_files(task_id)
        if video or asyncio.get_running_loop().time() >= deadline:
            if not video:
                print(f'[Telegram] V3 video not ready in {wait_seconds}s for {task_id}: poster={bool(poster)}; video-only completion remains pending')
            return (video, poster)
        await asyncio.sleep(3)

async def _fallback_v3_photo(task_id: str):
    """No V3 capture and no video: any real picture of this model, never the site card. The classic /thumb poster,
    else an image of the V3 run (weights preview, projection sheet, QA renders). Written to a temp file."""
    import glob
    import tempfile
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f'http://127.0.0.1:8200/thumb/{task_id}', follow_redirects=True)
        if r.status_code == 200 and len(r.content) > 8000 and r.headers.get('content-type', '').startswith('image/'):
            (fd, p) = tempfile.mkstemp(suffix='.jpg', prefix='tg-fallback-')
            with os.fdopen(fd, 'wb') as fh:
                fh.write(r.content)
            return p
    except Exception as exc:
        print(f'[Telegram] fallback thumb failed for {task_id}: {exc}')
    try:
        async with AsyncSessionLocal() as db:
            task = await db.scalar(select(Task).where(Task.id == task_id))
        url = str(getattr(task, 'viewer_prepared_glb_url', '') or '')
        import re as _re
        m = _re.search('/api/mt/files/([0-9a-f]{20})/', url)
        if m:
            run = f'/srv/autorig/data/motion_transfer/runs/{m.group(1)}'
            for pattern in ('rig/weights_preview.png', 'proj*/*.png', 'proj*/*.jpg', 'analysis/*.png', 'card/*.png', 'card/*.jpg', '*/*.png'):
                for cand in sorted(glob.glob(f'{run}/{pattern}')):
                    if 20000 < os.path.getsize(cand) < 9 * 1024 * 1024:
                        return cand
    except Exception as exc:
        print(f'[Telegram] fallback run image failed for {task_id}: {exc}')
    return None

async def _late_v3_media(bot, chat_id: int, reply_to: int, task_id: str, caption: str, markup, wait_seconds: int=900) -> None:
    """The notice went out as text (no poster in time): the task's own poster follows as a reply when it lands,
    and becomes the clip afterwards."""
    from telegram.constants import ParseMode
    deadline = asyncio.get_running_loop().time() + wait_seconds
    while asyncio.get_running_loop().time() < deadline:
        (_video, poster) = _v3_preview_files(task_id)
        if poster:
            try:
                with open(poster, 'rb') as fh:
                    sent = await bot.send_photo(chat_id=chat_id, photo=fh, caption=caption[:1024] if len(caption) <= 1024 else None, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to, allow_sending_without_reply=True, disable_notification=True)
                print(f'[Telegram] V3 poster sent late for chat={chat_id}, task={task_id}')
                if len(caption) <= 1024:
                    await _upgrade_to_v3_clip(bot, chat_id, sent.message_id, task_id, caption, markup)
            except Exception as exc:
                print(f'[Telegram] late V3 poster failed for chat={chat_id}, task={task_id}: {exc}')
            return
        await asyncio.sleep(10)

async def _upgrade_to_v3_clip(bot, chat_id: int, message_id: int, task_id: str, caption: str, markup, wait_seconds: int=900) -> None:
    """A «Task completed» sent with the poster becomes the V3 clip once it exists (editMessageMedia)."""
    from telegram import InputMediaVideo
    from telegram.constants import ParseMode
    deadline = asyncio.get_running_loop().time() + wait_seconds
    while asyncio.get_running_loop().time() < deadline:
        (video, _poster) = _v3_preview_files(task_id)
        if video:
            try:
                with open(video, 'rb') as fh:
                    await bot.edit_message_media(chat_id=chat_id, message_id=message_id, reply_markup=markup, media=InputMediaVideo(media=fh, caption=caption, parse_mode=ParseMode.HTML, supports_streaming=True, width=540, height=960))
                print(f'[Telegram] V3 clip swapped in for chat={chat_id}, task={task_id}')
            except Exception as exc:
                print(f'[Telegram] V3 clip edit failed for chat={chat_id}, task={task_id}: {exc}')
            return
        await asyncio.sleep(15)

async def broadcast_task_error(task_id: str, *, duration_seconds: int | None=None, queue_wait_seconds: int | None=None, processing_time_seconds: int | None=None, progress_page: str | None=None) -> None:
    print(f'[Telegram] broadcast_task_error called for task {task_id}')
    token = _api._get_token()
    if not token:
        print('[Telegram] No token, skipping error notification')
        return
    from telegram import Bot
    from telegram.constants import ParseMode
    bot = Bot(token=token)
    url = _api._task_url(task_id)
    metrics_line = _api._format_task_metrics(await _api._task_telegram_metrics(task_id))
    resolved_progress = progress_page
    error_message = 'Task failed'
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                error_message = _api._sanitize_error_for_telegram(getattr(task, 'error_message', None))
                if not resolved_progress and task.guid and task.worker_api:
                    parsed = urlparse(task.worker_api)
                    worker_base = f'{parsed.scheme}://{parsed.netloc}'
                    resolved_progress = f'{worker_base}/converter/glb/{task.guid}/{task.guid}.html'
    except Exception as e:
        print(f'[Telegram] Failed to get task details for error notification: {e}')
    dur = _format_duration(duration_seconds)
    queue_dur = _format_duration(queue_wait_seconds)
    processing_dur = _format_duration(processing_time_seconds)
    parts = [f'<a href="{html.escape(url)}">View Result</a>']
    if queue_dur:
        parts.append(f'queue {html.escape(queue_dur)}')
    if processing_dur:
        parts.append(f'processing {html.escape(processing_dur)}')
    if dur:
        parts.append(f'total {html.escape(dur)}')
    parts.append(html.escape(metrics_line))
    text = 'ERROR <b>Task failed</b>\n' + ' | '.join(parts)
    if resolved_progress:
        text += f'\nWorker logs: <a href="{html.escape(resolved_progress)}">open</a>'
    text += f'\n<code>{html.escape(error_message)}</code>'
    chat_ids = await _api.get_broadcast_chat_ids()
    if not chat_ids:
        print('[Telegram] No active chats, skipping error notification')
        return

    async def _one_text(chat_id: int):
        old_message_id = await _api.pop_notification_message_id(chat_id, 'task_new', task_id)
        if old_message_id:
            await _api._send_with_retry(lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid), retry_network=False)
        reserved = await _api.reserve_notification(chat_id, 'task_error', task_id)
        if not reserved:
            print(f'[Telegram] Skip duplicate error notification for chat={chat_id}, task={task_id}')
            return None
        return await _api._send_with_retry(lambda cid=chat_id: bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False), retry_network=False)
    results = await asyncio.gather(*[_one_text(cid) for cid in chat_ids])
    sent_count = sum((1 for r in results if r is not None))
    print(f'[Telegram] Error notification sent to {sent_count}/{len(chat_ids)} chat(s)')
    if sent_count > 0:
        await _api.mark_task_done_notification_sent(task_id)

async def broadcast_task_done(task_id: str, *, duration_seconds: int | None=None, queue_wait_seconds: int | None=None, processing_time_seconds: int | None=None, progress_page: str | None=None, extra_html: str | None=None, video_wait_seconds: int=180) -> None:
    """``extra_html``: lines the caller built (V3: viewer link, phase timings); ``video_wait_seconds=0`` when the
    task has no worker video to wait for (V3)."""
    print(f'[Telegram] broadcast_task_done called for task {task_id}')
    token = _api._get_token()
    if not token:
        print('[Telegram] No token, skipping done notification')
        return
    from telegram import Bot
    from telegram.constants import ParseMode
    bot = Bot(token=token)
    url = _api._task_url(task_id)
    metrics_line = _api._format_task_metrics(await _api._task_telegram_metrics(task_id))
    owner_email = None
    content_rating = 'unknown'
    task_state = None
    collection_title = None
    collection_member_title = None
    collection_index = None
    collection_size = None
    resolved_progress = progress_page
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if task:
                task_state = getattr(task, 'status', None)
                if task.owner_type == 'user':
                    owner_email = task.owner_id
                cr = getattr(task, 'content_rating', None)
                if cr:
                    content_rating = str(cr).strip().lower()
                collection_title = getattr(task, 'collection_title', None)
                collection_member_title = getattr(task, 'collection_member_title', None)
                collection_index = getattr(task, 'collection_index', None)
                collection_size = getattr(task, 'collection_size', None)
                if not resolved_progress and task.guid and task.worker_api:
                    parsed = urlparse(task.worker_api)
                    worker_base = f'{parsed.scheme}://{parsed.netloc}'
                    resolved_progress = f'{worker_base}/converter/glb/{task.guid}/{task.guid}.html'
    except Exception as e:
        print(f'[Telegram] Failed to get task details for done notification: {e}')
    rating_line = _format_content_rating_line(content_rating)
    dur = _format_duration(duration_seconds)
    queue_dur = _format_duration(queue_wait_seconds)
    processing_dur = _format_duration(processing_time_seconds)
    timing_parts = []
    if queue_dur:
        timing_parts.append(f'queue {html.escape(queue_dur)}')
    if processing_dur:
        timing_parts.append(f'processing {html.escape(processing_dur)}')
    done_parts = [f'🔗 <a href="{html.escape(url)}">View Result</a>']
    if owner_email:
        done_parts.append(f'👤 {html.escape(owner_email)}')
    if dur:
        done_parts.append(f'⏱ {html.escape(dur)}')
    done_parts.extend(timing_parts)
    done_parts.append(html.escape(metrics_line))
    heading = '🟡 <b>Task needs review</b>' if task_state == 'needs_review' else '✅ <b>Task completed</b>'
    text = heading + f'\n{rating_line}\n' + ' | '.join(done_parts)
    if collection_title:
        text += f'\n🧩 <b>{html.escape(str(collection_title)[:160])}</b> · {int(collection_index or 0)}/{int(collection_size or 0)}'
        if collection_member_title:
            text += f' · {html.escape(str(collection_member_title)[:120])}'
    if extra_html:
        text += '\n' + extra_html
    mp4_path = f'/var/autorig/videos/{task_id}.mp4'
    video_path = mp4_path if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0 else None
    video_wait_seconds = 0
    last_video_status = 'cached' if video_path else None
    if not video_path:
        (video_path, video_wait_seconds, last_video_status) = await _api._download_video_from_worker(task_id, wait_timeout_seconds=video_wait_seconds)
    v3_photo = None
    if not video_path:
        asyncio.create_task(_render_v3_preview_now(task_id))
        (v3_video, v3_photo) = await _wait_v3_preview(task_id, _tg_preview_wait_seconds())
        if not v3_video:
            try:
                hard = max(0, min(7200, int(os.getenv('AUTORIG_TG_DONE_VIDEO_HARD_WAIT_SEC', '1800'))))
            except ValueError:
                hard = 1800
            (v3_video, v3_photo) = await _wait_v3_preview(task_id, hard)
        v3_photo = None
        if v3_video:
            (video_path, last_video_status) = (v3_video, 'v3_preview')
        else:
            print(f'[Telegram] no V3 video for {task_id}: the completion notice is not sent (video only)')
            return
        print(f'[Telegram] V3 preview for {task_id}: video={bool(v3_video)}')
    chat_ids = await _api.get_broadcast_chat_ids()
    for private_id in await _api.private_chats_awaiting_task(task_id):
        if private_id not in chat_ids:
            chat_ids.append(private_id)
    if not chat_ids:
        print('[Telegram] No active chats, skipping done notification')
        return
    print(f'[Telegram] Sending done notification to {len(chat_ids)} chat(s), video={video_path is not None} video_wait_seconds={video_wait_seconds} video_path={video_path} last_video_status={last_video_status}')
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    generate_markup = InlineKeyboardMarkup([[InlineKeyboardButton('🎨 Коллекция ×15', callback_data=f'rfg:{task_id}'), InlineKeyboardButton('📦 Сабмитить', callback_data=f'rfc:{task_id}')]])
    if not video_path:

        async def _one_text(chat_id: int):
            reply_to = await _api.peek_notification_message_id(chat_id, 'task_reply_to', task_id)
            old_message_id = await _api.pop_notification_message_id(chat_id, 'task_new', task_id)
            if old_message_id:
                await _api._send_with_retry(lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid), retry_network=False)
            reserved = await _api.reserve_notification(chat_id, 'task_done', task_id)
            if not reserved:
                print(f'[Telegram] Skip duplicate done notification for chat={chat_id}, task={task_id}')
                return None
            if v3_photo and len(text) <= 1024:

                async def _photo(cid=chat_id, rt=reply_to):
                    with open(v3_photo, 'rb') as fh:
                        return await bot.send_photo(chat_id=cid, photo=fh, caption=text, parse_mode=ParseMode.HTML, reply_markup=generate_markup, reply_to_message_id=rt, allow_sending_without_reply=True)
                sent = await _api._send_with_retry(_photo, retry_network=False)
                if sent is not None:
                    asyncio.create_task(_upgrade_to_v3_clip(bot, chat_id, sent.message_id, task_id, text, generate_markup))
                    return sent
                print(f'[Telegram] V3 poster send failed for chat={chat_id}, task={task_id}; text fallback')
            sent_text = await _api._send_with_retry(lambda cid=chat_id, rt=reply_to: bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=generate_markup, reply_to_message_id=rt, allow_sending_without_reply=True), retry_network=False)
            if sent_text is not None and (not v3_photo):
                asyncio.create_task(_late_v3_media(bot, chat_id, sent_text.message_id, task_id, text, generate_markup))
            return sent_text
        results = await asyncio.gather(*[_one_text(cid) for cid in chat_ids])
        sent_count = sum((1 for r in results if r is not None))
        print(f'[Telegram] Done notification sent to {sent_count}/{len(chat_ids)} chat(s)')
        if sent_count > 0:
            await _api.mark_task_done_notification_sent(task_id)
            await _api._cleanup_generation_chat(task_id)
        return
    sem = asyncio.Semaphore(2)
    caption = text

    async def _one(chat_id: int):
        async with sem:
            reply_to = await _api.peek_notification_message_id(chat_id, 'task_reply_to', task_id)
            old_message_id = await _api.pop_notification_message_id(chat_id, 'task_new', task_id)
            if old_message_id:
                await _api._send_with_retry(lambda cid=chat_id, mid=old_message_id: bot.delete_message(chat_id=cid, message_id=mid), retry_network=False)
            reserved = await _api.reserve_notification(chat_id, 'task_done', task_id)
            if not reserved:
                print(f'[Telegram] Skip duplicate done notification for chat={chat_id}, task={task_id}')
                return

            def _send():
                f = open(video_path, 'rb')

                async def _inner():
                    try:
                        return await bot.send_video(chat_id=chat_id, video=f, caption=caption, parse_mode=ParseMode.HTML, supports_streaming=True, reply_markup=generate_markup, reply_to_message_id=reply_to, allow_sending_without_reply=True)
                    finally:
                        try:
                            f.close()
                        except Exception:
                            pass
                return _inner()
            result = await _api._send_with_retry(_send, retry_network=False)
            if result is None:
                print(f'[Telegram] send_video failed for chat={chat_id}, task={task_id}; video-only delivery remains pending')
                return None
            return result
    results = await asyncio.gather(*[_one(cid) for cid in chat_ids])
    sent_count = sum((1 for r in results if r is not None))
    print(f'[Telegram] Done video notification sent to {sent_count}/{len(chat_ids)} chat(s)')
    if sent_count > 0:
        await _api._cleanup_generation_chat(task_id)
    if sent_count > 0:
        await _api.mark_task_done_notification_sent(task_id)
