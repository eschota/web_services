"""
Task management for AutoRig Online
"""
import asyncio
import uuid
from datetime import datetime
from typing import Optional, Tuple, List
from urllib.parse import urlparse

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task, User, AnonSession, AsyncSessionLocal
from config import WORKERS
from workers import (
    select_best_worker,
    send_task_to_worker,
    send_fbx_to_glb,
    check_urls_batch,
    check_video_availability,
    get_worker_base_url
)


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


def _is_fbx_url(input_url: str) -> bool:
    """Return True if input_url path ends with .fbx (case-insensitive), ignoring query/fragment."""
    try:
        path = urlparse(input_url).path or ""
    except Exception:
        path = input_url or ""
    return path.lower().endswith(".fbx")


async def _head_is_ready(url: str) -> bool:
    """Lightweight availability check for a single URL (HEAD 200)."""
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.head(url, timeout=5.0, follow_redirects=True)
            return resp.status_code == 200
    except Exception:
        return False


async def _start_fbx_preconvert_async(task_id: str, first_worker_url: str, input_url: str) -> None:
    """
    Run FBX->GLB pre-conversion asynchronously after task creation/restart.
    Writes fbx_glb_* fields into the task once the worker responds.
    """
    last_error = None
    candidate_workers = [first_worker_url] + [w for w in WORKERS if w != first_worker_url]

    async with AsyncSessionLocal() as db:
        task = await get_task_by_id(db, task_id)
        if not task:
            return

        # If task already has output_url or is terminal, don't redo
        if task.status in ("done", "error") or task.fbx_glb_output_url:
            return

        for candidate in candidate_workers:
            res = await send_fbx_to_glb(candidate, input_url)
            if res.success:
                task.worker_api = candidate
                task.fbx_glb_model_name = res.model_name
                task.fbx_glb_output_url = res.output_url
                # If worker returns output_url, assume file is ready (no HEAD/GET checks).
                task.fbx_glb_ready = True
                task.fbx_glb_error = None
                task.updated_at = datetime.utcnow()
                await db.commit()
                await db.refresh(task)

                # Start main pipeline immediately (do not wait for next poll).
                if not task.worker_task_id and task.fbx_glb_output_url:
                    result = await send_task_to_worker(
                        task.worker_api,
                        task.fbx_glb_output_url,
                        task.input_type or "t_pose"
                    )
                    if not result.success:
                        task.status = "error"
                        task.error_message = result.error
                        task.updated_at = datetime.utcnow()
                        await db.commit()
                        return

                    task.worker_task_id = result.task_id
                    task.progress_page = result.progress_page
                    task.guid = result.guid
                    task.output_urls = result.output_urls
                    task.total_count = len(result.output_urls)
                    task.status = "processing"
                    task.updated_at = datetime.utcnow()
                    await db.commit()
                return

            last_error = res.error

            # Endpoint missing? try next worker
            if last_error and "HTTP 404" in last_error:
                continue

            # For other errors (timeouts, 5xx), still try other workers
            continue

        # No worker succeeded
        task.status = "error"
        task.fbx_glb_error = last_error or "FBX->GLB conversion failed"
        task.error_message = task.fbx_glb_error
        task.updated_at = datetime.utcnow()
        await db.commit()


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
    
    # Note: Telegram notification moved to start_task_on_worker (when we have progress_page)
    
    return task, None


async def start_task_on_worker(db: AsyncSession, task: Task, worker_url: str) -> Tuple[Task, Optional[str]]:
    """
    Start a queued (status=created) task on a specific worker.
    Workers accept GLB, FBX, OBJ directly via input_url.
    Returns: (task, error_message)
    """
    task.worker_api = worker_url
    task.status = "processing"
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)

    # Send task directly to worker (workers handle GLB, FBX, OBJ natively)
    result = await send_task_to_worker(worker_url, task.input_url, task.input_type or "t_pose")
    if not result.success:
        task.status = "error"
        task.error_message = result.error
        task.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(task)
        return task, result.error

    task.worker_task_id = result.task_id
    task.progress_page = result.progress_page
    task.guid = result.guid
    task.output_urls = result.output_urls
    task.total_count = len(result.output_urls)
    task.status = "processing"
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)
    
    # Telegram notification (fire-and-forget) - now we have progress_page
    try:
        from telegram_bot import broadcast_new_task
        # Construct progress_page URL from worker_api and guid
        worker_base = get_worker_base_url(worker_url)
        progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
        print(f"[Tasks] Scheduling Telegram notification for new task {task.id}")
        asyncio.create_task(broadcast_new_task(task.id, task.input_url, task.input_type, progress_url))
    except Exception as e:
        print(f"[Telegram] Failed to notify new task: {e}")
        import traceback
        traceback.print_exc()
    
    return task, None


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
    previous_ready_count = task.ready_count

    # Get already ready URLs
    already_ready = set(task.ready_urls)
    
    # Check new URLs (only for processing tasks)
    if task.status not in ("done", "error") and task.output_urls:
        newly_ready, total_ready = await check_urls_batch(
            task.output_urls, 
            already_ready
        )
        
        # Update task
        if newly_ready:
            current_ready = task.ready_urls
            current_ready.extend(newly_ready)
            task.ready_urls = current_ready
        
        task.ready_count = total_ready
        task.updated_at = datetime.utcnow()
        
        # Track last progress time (when ready_count actually increased)
        if total_ready > previous_ready_count:
            task.last_progress_at = datetime.utcnow()
        
        # Check if all URLs are ready
        if task.total_count > 0 and task.ready_count >= task.total_count:
            task.status = "done"
    
    # Check video availability (for both processing and done tasks)
    if task.guid and not task.video_ready:
        worker_base = get_worker_base_url(task.worker_api)
        video_ready, video_url = await check_video_availability(task.guid, worker_base)
        if video_ready:
            task.video_ready = True
            task.video_url = video_url
            task.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(task)
    
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
    
    # Cache task files to static directory when task completes (replaces ZIP)
    if was_processing and task.status == "done" and task.ready_urls:
        try:
            from main import cache_task_files
            print(f"[Tasks] Starting file caching for completed task {task.id}")
            asyncio.create_task(cache_task_files(task.id, task.ready_urls, task.guid))
        except Exception as e:
            print(f"[Tasks] Failed to cache files for task {task.id}: {e}")
    
    # Telegram notification if task just completed
    if was_processing and task.status == "done":
        try:
            from telegram_bot import broadcast_task_done
            duration = None
            if task.created_at:
                duration = int((datetime.utcnow() - task.created_at).total_seconds())
            # Construct progress_page URL
            progress_url = None
            if task.guid and task.worker_api:
                worker_base = get_worker_base_url(task.worker_api)
                progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
            print(f"[Tasks] Scheduling Telegram done notification for task {task.id}")
            asyncio.create_task(broadcast_task_done(task.id, duration_seconds=duration, progress_page=progress_url))
            
            # GA4 rig_completed event
            if task.ga_client_id:
                from main import send_ga4_event
                asyncio.create_task(send_ga4_event(
                    task.ga_client_id, 
                    "rig_completed", 
                    {"duration": duration, "task_id": task.id}
                ))
        except Exception as e:
            print(f"[Telegram] Failed to notify done: {e}")
            import traceback
            traceback.print_exc()
    
    return task


# =============================================================================
# Stale Task Detection & Auto-Restart
# =============================================================================
async def reset_stale_task(db: AsyncSession, task: Task) -> bool:
    """
    Reset a stale task for re-processing.
    Returns True if task was reset, False if max restarts exceeded.
    """
    from config import MAX_TASK_RESTARTS
    
    # Check if we've exceeded max restarts
    current_restarts = task.restart_count or 0
    if current_restarts >= MAX_TASK_RESTARTS:
        # Mark as error - too many restarts
        task.status = "error"
        task.error_message = f"Task failed after {current_restarts} automatic restart attempts. Worker may be unavailable."
        task.updated_at = datetime.utcnow()
        await db.commit()
        print(f"[Stale Task] Task {task.id} marked as error after {current_restarts} restarts")
        return False
    
    # Reset task for re-processing
    task.status = "created"
    task.ready_count = 0
    task.ready_urls = []
    task.output_urls = []
    task.total_count = 0
    task.worker_api = None
    task.worker_task_id = None
    task.progress_page = None
    task.guid = None
    task.video_ready = False
    task.video_url = None
    task.error_message = None
    task.restart_count = current_restarts + 1
    task.last_progress_at = None
    task.updated_at = datetime.utcnow()
    
    await db.commit()
    print(f"[Stale Task] Task {task.id} reset for re-processing (restart #{task.restart_count})")
    return True


async def find_and_reset_stale_tasks(db: AsyncSession) -> int:
    """
    Find all stale processing tasks and reset them or mark as error.
    Returns number of tasks reset/marked as error.
    """
    from config import STALE_TASK_TIMEOUT_MINUTES, GLOBAL_TASK_TIMEOUT_MINUTES
    from datetime import timedelta
    
    now = datetime.utcnow()
    stale_cutoff = now - timedelta(minutes=STALE_TASK_TIMEOUT_MINUTES)
    global_cutoff = now - timedelta(minutes=GLOBAL_TASK_TIMEOUT_MINUTES)
    
    # Find all non-terminal tasks
    result = await db.execute(
        select(Task).where(
            Task.status.notin_(["done", "error"]),
        )
    )
    active_tasks = result.scalars().all()
    
    action_count = 0
    for task in active_tasks:
        # 1. Check for global hard timeout (3 hours)
        if task.created_at and task.created_at < global_cutoff:
            task.status = "error"
            task.error_message = f"Task timed out after {GLOBAL_TASK_TIMEOUT_MINUTES} minutes."
            task.updated_at = now
            print(f"[Timeout] Task {task.id} marked as error (global timeout)")
            action_count += 1
            continue

        # 2. Check for staleness (no progress for 10 minutes)
        # Only for tasks that are actually in 'processing'
        if task.status == "processing":
            reference_time = task.last_progress_at or task.updated_at or task.created_at
            
            if reference_time and reference_time < stale_cutoff:
                # Additional check: if it has URLs but no progress, or just no progress at all
                if task.ready_count == 0:
                    print(f"[Stale Task] Detected stale task {task.id}: no progress since {reference_time}")
                    if await reset_stale_task(db, task):
                        action_count += 1
    
    if action_count > 0:
        await db.commit()
        
    return action_count


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
    elif seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks}w ago"
    else:
        months = int(seconds / 2592000)
        return f"{months}mo ago"

