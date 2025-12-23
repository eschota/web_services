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
    # FBX -> GLB pre-conversion (only when input was .fbx)
    fbx_glb_output_url: Optional[str] = None
    fbx_glb_model_name: Optional[str] = None
    fbx_glb_ready: Optional[bool] = None
    fbx_glb_error: Optional[str] = None
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
# Gallery Schemas
# =============================================================================
class GalleryItem(BaseModel):
    """Gallery item for public gallery"""
    task_id: str
    video_url: str
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

