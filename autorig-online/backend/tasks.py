"""
Task management for AutoRig Online
"""
import uuid
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task, User, AnonSession
from workers import (
    select_best_worker,
    send_task_to_worker,
    check_urls_batch,
    check_video_availability,
    get_worker_base_url
)


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
    # Select best worker
    worker_url = await select_best_worker()
    if not worker_url:
        return None, "No workers available"
    
    # Create task record
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id,
        owner_type=owner_type,
        owner_id=owner_id,
        input_url=input_url,
        input_type=task_type,
        worker_api=worker_url,
        status="created"
    )
    
    # Send to worker
    result = await send_task_to_worker(worker_url, input_url, task_type)
    
    if not result.success:
        task.status = "error"
        task.error_message = result.error
        db.add(task)
        await db.commit()
        return task, result.error
    
    # Update task with worker response
    task.worker_task_id = result.task_id
    task.progress_page = result.progress_page
    task.guid = result.guid
    task.output_urls = result.output_urls
    task.total_count = len(result.output_urls)
    task.status = "processing"
    
    db.add(task)
    await db.commit()
    await db.refresh(task)
    
    return task, None


# =============================================================================
# Progress Checking
# =============================================================================
async def update_task_progress(db: AsyncSession, task: Task) -> Task:
    """
    Check and update task progress.
    Checks a batch of URLs and updates ready count.
    """
    if task.status in ("done", "error"):
        return task
    
    # Get already ready URLs
    already_ready = set(task.ready_urls)
    
    # Check new URLs
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
    
    # Check if all URLs are ready
    if task.total_count > 0 and task.ready_count >= task.total_count:
        task.status = "done"
    
    # Check video availability
    if task.guid and not task.video_ready:
        worker_base = get_worker_base_url(task.worker_api)
        video_ready, video_url = await check_video_availability(task.guid, worker_base)
        if video_ready:
            task.video_ready = True
            task.video_url = video_url
    
    await db.commit()
    await db.refresh(task)
    
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

