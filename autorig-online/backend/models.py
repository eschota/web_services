"""
Pydantic models (schemas) for API request/response
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


# =============================================================================
# Task Schemas
# =============================================================================
class TaskCreateRequest(BaseModel):
    """Request to create a new conversion task"""
    source: str = Field(..., description="Source type: 'link' or 'upload'")
    input_url: Optional[str] = Field(None, description="URL of the model (for link source)")
    type: str = Field(default="t_pose", description="Conversion type")


class TaskCreateResponse(BaseModel):
    """Response after creating a task"""
    task_id: str
    status: str
    message: Optional[str] = None


class TaskStatusResponse(BaseModel):
    """Task status and progress"""
    task_id: str
    status: str
    progress: int
    ready_count: int
    total_count: int
    ready_urls: List[str]
    video_ready: bool
    video_url: Optional[str]
    # Debug / worker assignment (needed for interactive progress)
    worker_api: Optional[str] = None
    worker_task_id: Optional[str] = None
    guid: Optional[str] = None
    progress_page: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class TaskHistoryItem(BaseModel):
    """Task item for history list"""
    task_id: str
    status: str
    progress: int
    created_at: datetime
    input_url: Optional[str]
    video_ready: bool


class TaskHistoryResponse(BaseModel):
    """Response for task history"""
    tasks: List[TaskHistoryItem]
    total: int
    page: int
    per_page: int


# =============================================================================
# Task Owner List (public, by task_id)
# =============================================================================
class OwnerTaskListItem(BaseModel):
    """Task item for 'owner tasks' list"""
    task_id: str
    status: str
    progress: int
    created_at: datetime
    video_ready: bool
    thumbnail_url: str


class OwnerTaskListResponse(BaseModel):
    """Response: list of tasks for the owner of a given task_id"""
    owner_type: str
    owner_id: str
    tasks: List[OwnerTaskListItem]
    total: int
    page: int
    per_page: int


# =============================================================================
# User Schemas
# =============================================================================
class UserInfo(BaseModel):
    """Current user info"""
    id: int
    email: str
    name: Optional[str]
    picture: Optional[str]
    balance_credits: int
    total_tasks: int
    is_admin: bool


class AnonInfo(BaseModel):
    """Anonymous user info"""
    anon_id: str
    free_used: int
    free_remaining: int


class AuthStatusResponse(BaseModel):
    """Authentication status response"""
    authenticated: bool
    user: Optional[UserInfo] = None
    anon: Optional[AnonInfo] = None
    credits_remaining: int
    login_required: bool


# =============================================================================
# API Key Schemas
# =============================================================================
class ApiKeyItem(BaseModel):
    """API key metadata (masked)."""
    id: int
    key_prefix: str
    created_at: datetime
    revoked_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None


class ApiKeyListResponse(BaseModel):
    keys: List[ApiKeyItem]


class ApiKeyCreateResponse(BaseModel):
    """Returned only once at creation/regeneration."""
    api_key: str
    key: ApiKeyItem


# =============================================================================
# Admin Schemas
# =============================================================================
class AdminUserListItem(BaseModel):
    """User item for admin list"""
    id: int
    email: str
    name: Optional[str]
    balance_credits: int
    total_tasks: int
    created_at: datetime
    last_login_at: datetime


class AdminUserListResponse(BaseModel):
    """Response for admin user list"""
    users: List[AdminUserListItem]
    total: int
    page: int
    per_page: int


class AdminBalanceUpdate(BaseModel):
    """Request to update user balance"""
    delta: Optional[int] = Field(None, description="Add/subtract from balance")
    set_to: Optional[int] = Field(None, description="Set balance to exact value")


class AdminBalanceResponse(BaseModel):
    """Response after balance update"""
    user_id: int
    email: str
    old_balance: int
    new_balance: int


class AdminUserTaskItem(BaseModel):
    """Task item for admin user tasks list"""
    task_id: str
    status: str
    progress: int
    ready_count: int
    total_count: int
    created_at: datetime
    updated_at: datetime
    input_url: Optional[str] = None


class AdminUserTasksResponse(BaseModel):
    """Response for admin user tasks list"""
    tasks: List[AdminUserTaskItem]
    total: int
    page: int
    per_page: int


# =============================================================================
# Admin (Anon Sessions)
# =============================================================================
class AdminAnonSessionListItem(BaseModel):
    """Anon session item for admin list"""
    anon_id: str
    free_used: int
    created_at: datetime
    last_seen_at: datetime
    total_tasks: int


class AdminAnonSessionListResponse(BaseModel):
    """Response for admin anon sessions list"""
    sessions: List[AdminAnonSessionListItem]
    total: int
    page: int
    per_page: int


class AdminAnonSessionTaskItem(BaseModel):
    """Task item for admin anon session tasks list"""
    task_id: str
    status: str
    progress: int
    ready_count: int
    total_count: int
    created_at: datetime
    updated_at: datetime
    input_url: Optional[str] = None


class AdminAnonSessionTasksResponse(BaseModel):
    """Response for admin anon session tasks list"""
    tasks: List[AdminAnonSessionTaskItem]
    total: int
    page: int
    per_page: int


class AdminTaskOwnerResponse(BaseModel):
    """Owner info for a task (admin)."""
    task_id: str
    owner_type: str
    owner_id: str
    user_id: Optional[int] = None
    created_at: datetime
    status: str


class AdminTaskListItem(BaseModel):
    """Task item for admin tasks list"""
    task_id: str
    status: str
    progress: int
    ready_count: int
    total_count: int
    created_at: datetime
    updated_at: datetime
    worker_api: Optional[str] = None
    guid: Optional[str] = None
    owner_type: str
    owner_id: str
    owner_name: Optional[str] = None  # User name or "Anonymous"
    input_url: Optional[str] = None
    retry_count: int = 0
    started_at: Optional[datetime] = None


class AdminTaskStatusCounts(BaseModel):
    """Status counts for admin tasks"""
    processing: int = 0
    created: int = 0
    done: int = 0
    error: int = 0


class AdminTaskListResponse(BaseModel):
    """Response for admin tasks list"""
    tasks: List[AdminTaskListItem]
    total: int
    page: int
    per_page: int
    status_counts: AdminTaskStatusCounts


# =============================================================================
# Gallery Schemas
# =============================================================================
class GalleryItem(BaseModel):
    """Gallery item for public gallery"""
    task_id: str
    video_url: str
    thumbnail_url: str
    created_at: datetime
    time_ago: str


class GalleryResponse(BaseModel):
    """Response for gallery"""
    items: List[GalleryItem]
    total: int
    page: int
    per_page: int
    has_more: bool


# =============================================================================
# Worker Schemas
# =============================================================================
class WorkerStatus(BaseModel):
    """Worker status info"""
    url: str
    available: bool
    load: Optional[float] = None
    error: Optional[str] = None


class WorkerTaskResponse(BaseModel):
    """Response from worker after task creation"""
    task_id: str
    output_urls: List[str]
    progress_page: Optional[str]
    status: str


class WorkerQueueInfo(BaseModel):
    """Single worker queue info"""
    port: str
    available: bool
    active: int
    pending: int
    queue_size: int
    error: Optional[str] = None


class QueueStatusResponse(BaseModel):
    """Global queue status response"""
    workers: List[WorkerQueueInfo]
    total_active: int
    total_pending: int
    total_queue: int
    available_workers: int
    total_workers: int
    estimated_wait_seconds: int
    estimated_wait_formatted: str

