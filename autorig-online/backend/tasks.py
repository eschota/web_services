"""
Task management for AutoRig Online
"""
import asyncio
import uuid
from datetime import datetime
from typing import Optional, Tuple, List

from sqlalchemy import select, desc, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task, User, AnonSession, AsyncSessionLocal
from config import WORKERS
from workers import (
    select_best_worker,
    send_task_to_worker,
    check_urls_batch,
    check_video_availability,
    get_worker_base_url
)

from telegram_bot import broadcast_task_done


# =============================================================================
# Local Media Cache (Runtime)
# =============================================================================
_VIDEO_CACHE_DIR = "/var/autorig/videos"
_video_cache_locks: dict[str, asyncio.Lock] = {}


def _get_video_cache_lock(task_id: str) -> asyncio.Lock:
    # Best-effort in-process lock to avoid double-downloads
    lock = _video_cache_locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _video_cache_locks[task_id] = lock
    return lock


async def cache_task_video_by_id(task_id: str) -> None:
    """
    Cache task video to local disk:
      /var/autorig/videos/{task_id}.{ext} (mp4/mov)
    Safe to call multiple times; uses in-process lock and file existence check.
    """
    import os
    import httpx

    os.makedirs(_VIDEO_CACHE_DIR, exist_ok=True)
    # Prefer mp4 if already cached; otherwise allow mov.
    cached_mp4 = os.path.join(_VIDEO_CACHE_DIR, f"{task_id}.mp4")
    cached_mov = os.path.join(_VIDEO_CACHE_DIR, f"{task_id}.mov")
    if os.path.exists(cached_mp4) and os.path.getsize(cached_mp4) > 0:
        return
    if os.path.exists(cached_mov) and os.path.getsize(cached_mov) > 0:
        return

    lock = _get_video_cache_lock(task_id)
    async with lock:
        if os.path.exists(cached_mp4) and os.path.getsize(cached_mp4) > 0:
            return
        if os.path.exists(cached_mov) and os.path.getsize(cached_mov) > 0:
            return

        async with AsyncSessionLocal() as db:
            task = await get_task_by_id(db, task_id)
            if not task or not task.video_url:
                return

            # Determine extension from remote URL
            ext = ".mp4"
            try:
                url = (task.video_url or "").lower()
                if url.endswith(".mov"):
                    ext = ".mov"
                elif url.endswith(".mp4"):
                    ext = ".mp4"
            except Exception:
                ext = ".mp4"

            final_path = os.path.join(_VIDEO_CACHE_DIR, f"{task_id}{ext}")
            tmp_path = final_path + ".tmp"

            try:
                async with httpx.AsyncClient() as client:
                    async with client.stream("GET", task.video_url, timeout=120.0, follow_redirects=True) as r:
                        if r.status_code != 200:
                            return
                        with open(tmp_path, "wb") as f:
                            async for chunk in r.aiter_bytes(chunk_size=1024 * 256):
                                if chunk:
                                    f.write(chunk)
                if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
                    os.replace(tmp_path, final_path)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass


def _pick_video_from_urls(urls: List[str]) -> Optional[str]:
    """Pick best video URL from a list of URLs (final format: prefer *_video.mp4)."""
    if not urls:
        return None
    for u in urls:
        if "_video.mp4" in u:
            return u
    # Minimal fallback: any mp4
    for u in urls:
        if u.lower().endswith(".mp4"):
            return u
    return None


# =============================================================================
# Helper Functions
# =============================================================================
def find_file_by_pattern(ready_urls: List[str], pattern: str, quality: str = "100k") -> Optional[str]:
    """
    Find a file in ready_urls matching the pattern in the specified quality folder.
    
    Args:
        ready_urls: List of ready file URLs
        pattern: File extension or pattern to match (e.g., ".html", ".max", ".ma")
        quality: Quality folder to search in ("100k", "10k", "1k")
    
    Returns:
        First matching URL or None
    """
    quality_folder = f"_{quality}/"
    
    for url in ready_urls:
        # Check if URL contains the quality folder and matches the pattern
        if quality_folder in url and pattern in url:
            return url
    
    # Fallback: try other qualities if 100k not found
    if quality == "100k":
        for fallback_quality in ["10k", "1k"]:
            fallback_folder = f"_{fallback_quality}/"
            for url in ready_urls:
                if fallback_folder in url and pattern in url:
                    return url
    
    return None


# =============================================================================
# Task Creation
# =============================================================================
async def create_conversion_task(
    db: AsyncSession,
    input_url: str,
    task_type: str,
    owner_type: str,
    owner_id: str
) -> Tuple[Optional[Task], Optional[str]]:
    """
    Create a new conversion task.
    Returns: (task, error_message)
    """
    # Create task record
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id,
        owner_type=owner_type,
        owner_id=owner_id,
        input_url=input_url,
        input_type=task_type,
        status="created"
    )

    db.add(task)
    await db.commit()
    await db.refresh(task)

    
    return task, None


async def start_task_on_worker(db: AsyncSession, task: Task, worker_url: str) -> Tuple[Task, Optional[str]]:
    """
    Start a queued (status=created) task on a specific worker.
    Unified worker pipeline for all input formats.
    Returns: (task, error_message)
    """
    candidate_workers = [worker_url] + [w for w in WORKERS if w != worker_url]

    last_error: Optional[str] = None
    for candidate in candidate_workers:
        result = await send_task_to_worker(candidate, task.input_url, task.input_type or "t_pose")
        if result.success:
            # Some workers return wildcard URL patterns (e.g. "*") as placeholders.
            # They are not real files and will never become HEAD/GET=200, so we must
            # exclude them from progress tracking to allow tasks to complete.
            filtered_output_urls = [u for u in (result.output_urls or []) if "*" not in u]
            task.worker_api = candidate
            task.worker_task_id = result.task_id
            task.progress_page = result.progress_page
            task.guid = result.guid
            task.output_urls = filtered_output_urls
            task.total_count = len(filtered_output_urls)
            task.status = "processing"
            task.started_at = datetime.utcnow()  # Track when processing started
            task.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(task)
            return task, None

        last_error = result.error
        # If endpoint missing on this worker, try another.
        if last_error and "HTTP 404" in last_error:
            continue
        # For other errors (timeouts, 5xx), still try other workers.
        continue

    task.status = "error"
    task.error_message = last_error
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)
    return task, last_error

    # Unreachable; return happens inside loop above.


# =============================================================================
# Progress Checking
# =============================================================================
async def update_task_progress(db: AsyncSession, task: Task) -> Task:
    """
    Check and update task progress.
    Checks a batch of URLs and updates ready count.
    """
    # Track if task just completed
    was_processing = task.status == "processing"

    # Get already ready URLs
    already_ready = set(task.ready_urls)
    
    # Check new URLs (only for processing tasks)
    if task.status not in ("done", "error") and task.output_urls:
        # Do not track wildcard patterns like "..._*.png" as real outputs.
        check_urls = [u for u in (task.output_urls or []) if "*" not in u]
        # Keep total_count consistent with what we actually check.
        if task.total_count != len(check_urls):
            task.total_count = len(check_urls)
        newly_ready, total_ready = await check_urls_batch(
            check_urls,
            already_ready
        )
        
        # Update task
        if newly_ready:
            current_ready = task.ready_urls
            current_ready.extend(newly_ready)
            task.ready_urls = current_ready
        
        task.ready_count = total_ready
        task.updated_at = datetime.utcnow()
        
        # Check if all URLs are ready
        if task.total_count > 0 and task.ready_count >= task.total_count:
            task.status = "done"
    
    # Check video availability (for both processing and done tasks)
    prev_video_ready = bool(task.video_ready)
    if task.guid and not task.video_ready:
        # Prefer a known URL from ready_urls/output_urls first.
        v = _pick_video_from_urls(task.ready_urls or [])
        if v:
            task.video_ready = True
            task.video_url = v
            task.updated_at = datetime.utcnow()
        else:
            v2 = _pick_video_from_urls(task.output_urls or [])
            if v2:
                # If it's only in output_urls, it might not exist yet; verify via HEAD.
                try:
                    import httpx
                    async with httpx.AsyncClient() as client:
                        h = await client.head(v2, timeout=5.0, follow_redirects=True)
                        if h.status_code == 200:
                            task.video_ready = True
                            task.video_url = v2
                            task.updated_at = datetime.utcnow()
                except Exception:
                    pass

        # Fallback: old-style known naming on worker base (mp4/mov variants)
        if not task.video_ready:
            worker_base = get_worker_base_url(task.worker_api)
            video_ready, video_url = await check_video_availability(task.guid, worker_base)
            if video_ready:
                task.video_ready = True
                task.video_url = video_url
                task.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(task)

    # Warm up local MP4 cache asynchronously when video becomes ready.
    # IMPORTANT: do NOT notify Telegram here, иначе будет дубль с notify-on-done ниже.
    try:
        if (not prev_video_ready) and task.video_ready and task.video_url:
            asyncio.create_task(cache_task_video_by_id(task.id))
    except Exception:
        pass

    # Telegram notify on completion (even if video is still being cached).
    if was_processing and task.status == "done":
        try:
            duration_seconds = int((datetime.utcnow() - task.created_at).total_seconds())
        except Exception:
            duration_seconds = None
        try:
            asyncio.create_task(broadcast_task_done(task.id, duration_seconds=duration_seconds))
        except Exception:
            pass
    
    # Send email notification if task just completed (100%)
    if was_processing and task.status == "done" and task.owner_type == "user":
        try:
            from email_service import send_task_completed_email
            worker_base = get_worker_base_url(task.worker_api)
            await send_task_completed_email(
                to_email=task.owner_id,  # owner_id contains user email
                task_id=task.id,
                guid=task.guid,
                worker_base=worker_base
            )
        except Exception as e:
            print(f"[Tasks] Failed to send completion email for task {task.id}: {e}")
    
    return task


# =============================================================================
# Task Retrieval
# =============================================================================
async def get_task_by_id(db: AsyncSession, task_id: str) -> Optional[Task]:
    """Get task by ID"""
    result = await db.execute(
        select(Task).where(Task.id == task_id)
    )
    return result.scalar_one_or_none()


async def get_user_tasks(
    db: AsyncSession,
    owner_type: str,
    owner_id: str,
    page: int = 1,
    per_page: int = 10
) -> Tuple[list, int]:
    """
    Get tasks for a user/anon with pagination.
    Returns: (tasks, total_count)
    """
    # Count total
    count_result = await db.execute(
        select(Task).where(
            Task.owner_type == owner_type,
            Task.owner_id == owner_id
        )
    )
    total = len(count_result.scalars().all())
    
    # Get paginated
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(
            Task.owner_type == owner_type,
            Task.owner_id == owner_id
        )
        .order_by(desc(Task.created_at))
        .offset(offset)
        .limit(per_page)
    )
    tasks = result.scalars().all()
    
    return list(tasks), total


# =============================================================================
# Admin Functions
# =============================================================================
async def get_all_users(
    db: AsyncSession,
    search: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20
) -> Tuple[list, int]:
    """
    Get all users with search and pagination (admin).
    Returns: (users, total_count)
    """
    query = select(User)
    
    if search:
        query = query.where(User.email.ilike(f"%{search}%"))
    
    # Count total
    count_result = await db.execute(query)
    total = len(count_result.scalars().all())
    
    # Sort
    sort_column = getattr(User, sort_by, User.created_at)
    if sort_desc:
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(sort_column)
    
    # Paginate
    offset = (page - 1) * per_page
    result = await db.execute(
        query.offset(offset).limit(per_page)
    )
    users = result.scalars().all()
    
    return list(users), total


async def get_anon_sessions(
    db: AsyncSession,
    search: Optional[str] = None,
    sort_by: str = "last_seen_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20
) -> Tuple[list[tuple[AnonSession, int]], int]:
    """
    Get anon sessions with task counts (admin).
    Returns: ([(anon_session, total_tasks)], total_sessions)
    """
    # Total sessions count
    count_q = select(func.count(AnonSession.anon_id))
    if search:
        count_q = count_q.where(AnonSession.anon_id.ilike(f"%{search}%"))
    total_res = await db.execute(count_q)
    total = int(total_res.scalar() or 0)

    # Main query
    task_count = func.count(Task.id).label("total_tasks")
    q = (
        select(AnonSession, task_count)
        .select_from(AnonSession)
        .outerjoin(
            Task,
            and_(Task.owner_type == "anon", Task.owner_id == AnonSession.anon_id),
        )
        .group_by(AnonSession.anon_id)
    )

    if search:
        q = q.where(AnonSession.anon_id.ilike(f"%{search}%"))

    # Sorting
    if sort_by == "total_tasks":
        sort_col = task_count
    else:
        sort_col = getattr(AnonSession, sort_by, AnonSession.last_seen_at)

    q = q.order_by(desc(sort_col) if sort_desc else sort_col)

    # Pagination
    offset = (page - 1) * per_page
    q = q.offset(offset).limit(per_page)

    res = await db.execute(q)
    rows = res.all()  # list[(AnonSession, int)]
    return [(r[0], int(r[1] or 0)) for r in rows], total


async def update_user_balance(
    db: AsyncSession,
    user_id: int,
    delta: Optional[int] = None,
    set_to: Optional[int] = None
) -> Tuple[Optional[User], int, int]:
    """
    Update user balance.
    Returns: (user, old_balance, new_balance)
    """
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()
    
    if not user:
        return None, 0, 0
    
    old_balance = user.balance_credits
    
    if set_to is not None:
        user.balance_credits = max(0, set_to)
    elif delta is not None:
        user.balance_credits = max(0, user.balance_credits + delta)
    
    await db.commit()
    await db.refresh(user)
    
    return user, old_balance, user.balance_credits


# =============================================================================
# Gallery Functions
# =============================================================================
async def get_gallery_items(
    db: AsyncSession,
    page: int = 1,
    per_page: int = 12
) -> Tuple[list, int]:
    """
    Get completed tasks with videos for public gallery.
    Returns: (tasks, total_count)
    """
    from sqlalchemy import func
    
    # Count total completed tasks with video
    count_result = await db.execute(
        select(func.count(Task.id)).where(
            Task.status == "done",
            Task.video_ready == True
        )
    )
    total = count_result.scalar() or 0
    
    # Get paginated results, newest first
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(
            Task.status == "done",
            Task.video_ready == True
        )
        .order_by(desc(Task.created_at))
        .offset(offset)
        .limit(per_page)
    )
    tasks = result.scalars().all()
    
    return list(tasks), total


def format_time_ago(dt: datetime) -> str:
    """Format datetime as human-readable time ago string"""
    now = datetime.utcnow()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        mins = int(seconds / 60)
        return f"{mins}m ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h ago"
    elif seconds < 604800:
        days = int(seconds / 86400)
        return f"{days}d ago"
    else:
        weeks = int(seconds / 604800)
        return f"{weeks}w ago"


# =============================================================================
# Gallery Functions
# =============================================================================
async def get_gallery_items(
    db: AsyncSession,
    page: int = 1,
    per_page: int = 12
) -> Tuple[list, int]:
    """
    Get completed tasks with videos for public gallery.
    Returns: (tasks, total_count)
    """
    from sqlalchemy import func
    
    # Count total completed tasks with video
    count_result = await db.execute(
        select(func.count(Task.id)).where(
            Task.status == "done",
            Task.video_ready == True
        )
    )
    total = count_result.scalar() or 0
    
    # Get paginated results, newest first
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(
            Task.status == "done",
            Task.video_ready == True
        )
        .order_by(desc(Task.created_at))
        .offset(offset)
        .limit(per_page)
    )
    tasks = result.scalars().all()
    
    return list(tasks), total


def format_time_ago(dt: datetime) -> str:
    """Format datetime as human-readable time ago string"""
    now = datetime.utcnow()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        mins = int(seconds / 60)
        return f"{mins}m ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h ago"
    elif seconds < 604800:
        days = int(seconds / 86400)
        return f"{days}d ago"
    elif seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks}w ago"
    else:
        months = int(seconds / 2592000)
        return f"{months}mo ago"

