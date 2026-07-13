"""
Database models and setup for AutoRig Online
"""
from datetime import datetime, timedelta
from typing import Optional
import json

from sqlalchemy import (
    Column, String, Integer, BigInteger, Boolean, DateTime, Text, Float,
    UniqueConstraint, ForeignKey, create_engine, event, Index, text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool

from config import DATABASE_URL

# =============================================================================
# Engine and Session Setup
# =============================================================================
# For SQLite, we want to enable WAL mode for better concurrency
@event.listens_for(create_engine(DATABASE_URL.replace("sqlite+aiosqlite", "sqlite")), "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if "sqlite" in DATABASE_URL:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    poolclass=StaticPool if "sqlite" in DATABASE_URL else None,
)

AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()


# =============================================================================
# Models
# =============================================================================
class User(Base):
    """Registered user (via Google OAuth)"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    nickname = Column(String(100), nullable=True)  # Public display name (preferred over email)
    picture = Column(String(512), nullable=True)
    gumroad_email = Column(String(255), nullable=True)
    balance_credits = Column(Integer, default=0)
    total_tasks = Column(Integer, default=0)
    youtube_bonus_received = Column(Boolean, default=False)
    email_task_completed = Column(Boolean, default=True, nullable=False)  # task-ready emails; False = unsubscribed
    email_marketing_unsubscribed_at = Column(DateTime, nullable=True)
    # Global suppression: hard bounces/complaints must not receive marketing or transactional email.
    email_invalid_at = Column(DateTime, nullable=True)
    email_invalid_reason = Column(Text, nullable=True)
    email_invalid_source = Column(String(64), nullable=True)
    email_last_bounce_at = Column(DateTime, nullable=True)
    email_last_bounce_type = Column(String(32), nullable=True)
    email_transient_bounce_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime, default=datetime.utcnow)
    
    @property
    def is_admin(self) -> bool:
        from config import is_admin_email
        return is_admin_email(self.email)


class EmailCampaignSend(Base):
    """Per-recipient campaign send log for resumable marketing email sends."""
    __tablename__ = "email_campaign_sends"
    __table_args__ = (
        UniqueConstraint("campaign_key", "email_hash", name="uq_email_campaign_key_hash"),
        Index("ix_email_campaign_sends_campaign_status", "campaign_key", "status"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_key = Column(String(128), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    email_hash = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="pending", index=True)
    provider_message_id = Column(String(128), nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    sent_at = Column(DateTime, nullable=True)


class EmailCampaignClick(Base):
    """Click events for tracked marketing campaign links."""
    __tablename__ = "email_campaign_clicks"
    __table_args__ = (
        Index("ix_email_campaign_clicks_campaign_link", "campaign_key", "link_key"),
        Index("ix_email_campaign_clicks_email_hash", "email_hash"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_key = Column(String(128), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    email_hash = Column(String(64), nullable=False)
    link_key = Column(String(64), nullable=False, index=True)
    destination_url = Column(String(1024), nullable=False)
    ip_hash = Column(String(64), nullable=True)
    user_agent = Column(String(512), nullable=True)
    clicked_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class EmailDeliveryEvent(Base):
    """Resend delivery/bounce/complaint webhook event log."""
    __tablename__ = "email_delivery_events"
    __table_args__ = (
        Index("ix_email_delivery_events_message", "provider_message_id"),
        Index("ix_email_delivery_events_email_hash", "email_hash"),
        Index("ix_email_delivery_events_event_type", "event_type"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    svix_id = Column(String(128), unique=True, nullable=True)
    provider_message_id = Column(String(128), nullable=True)
    campaign_key = Column(String(128), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    email_hash = Column(String(64), nullable=True)
    event_type = Column(String(64), nullable=False)
    bounce_type = Column(String(32), nullable=True)
    bounce_subtype = Column(String(64), nullable=True)
    error_message = Column(Text, nullable=True)
    raw_event_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

class AnonSession(Base):
    """Anonymous user session (tracked by cookie)"""
    __tablename__ = "anon_sessions"
    
    anon_id = Column(String(36), primary_key=True)  # UUID
    free_used = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)
    agent_name = Column(String(255), nullable=True)
    agent_description = Column(Text, nullable=True)
    registered_as_agent = Column(Boolean, default=False)


class TaskLike(Base):
    """Like on a task (by registered user)"""
    __tablename__ = "task_likes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class TaskFilePurchase(Base):
    """Purchase of task files (by registered user)"""
    __tablename__ = "task_file_purchases"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    file_index = Column(Integer, nullable=True)  # NULL means "all files"
    credits_spent = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)


class TaskAnimationPurchase(Base):
    """Purchase of a single custom animation for a task."""
    __tablename__ = "task_animation_purchases"
    __table_args__ = (
        UniqueConstraint("task_id", "user_email", "animation_id", name="uq_task_animation_purchase"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    animation_id = Column(String(128), nullable=False, index=True)
    credits_spent = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)


class TaskAnimationBundlePurchase(Base):
    """Unlock-all custom animations purchase for a task."""
    __tablename__ = "task_animation_bundle_purchases"
    __table_args__ = (
        UniqueConstraint("task_id", "user_email", name="uq_task_animation_bundle_purchase"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    credits_spent = Column(Integer, nullable=False, default=10)
    created_at = Column(DateTime, default=datetime.utcnow)


class TaskAnimalAnimationPackPurchase(Base):
    """Purchase of one animal variant animation pack for a task."""
    __tablename__ = "task_animal_animation_pack_purchases"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "user_email",
            "animal_type",
            "orientation",
            name="uq_task_animal_animation_pack_purchase",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    animal_type = Column(String(32), nullable=False, index=True)
    orientation = Column(String(16), nullable=False, index=True)
    credits_spent = Column(Integer, nullable=False, default=10)
    created_at = Column(DateTime, default=datetime.utcnow)


class Task(Base):
    """Conversion task"""
    __tablename__ = "tasks"
    
    id = Column(String(36), primary_key=True)  # UUID
    owner_type = Column(String(10), nullable=False)  # 'anon' or 'user'
    owner_id = Column(String(255), nullable=False)  # anon_id or user email
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Worker info
    worker_api = Column(String(255), nullable=True)
    worker_task_id = Column(String(255), nullable=True)
    progress_page = Column(String(512), nullable=True)
    guid = Column(String(36), nullable=True)
    
    # Input
    input_url = Column(String(1024), nullable=True)
    input_type = Column(String(50), default="t_pose")
    
    # Output URLs (JSON array)
    _output_urls = Column("output_urls", Text, default="[]")
    
    # Progress tracking
    ready_count = Column(Integer, default=0)
    total_count = Column(Integer, default=0)
    _ready_urls = Column("ready_urls", Text, default="[]")  # Cache of ready URLs
    
    # Status
    status = Column(String(20), default="created")  # created, processing, done, error
    error_message = Column(Text, nullable=True)
    
    # Auto-restart tracking for stale tasks
    restart_count = Column(Integer, default=0)  # Number of times task was auto-restarted
    # Stuck-hour policy: auto requeues (admin-style) before full delete; not cleared by admin_requeue
    stuck_hour_requeue_count = Column(Integer, default=0)
    last_progress_at = Column(DateTime, nullable=True)  # Last time progress changed
    
    # Video
    video_url = Column(String(512), nullable=True)
    video_ready = Column(Boolean, default=False)

    # FBX -> GLB pre-conversion (optional)
    fbx_glb_output_url = Column(String(1024), nullable=True)
    fbx_glb_model_name = Column(String(64), nullable=True)
    fbx_glb_ready = Column(Boolean, default=False)
    fbx_glb_error = Column(Text, nullable=True)

    # Telegram notification tracking
    telegram_new_notified_at = Column(DateTime, nullable=True)
    telegram_done_notified_at = Column(DateTime, nullable=True)

    # Viewer settings (JSON string). Used by task.html to persist viewer state per-task.
    viewer_settings = Column(Text, nullable=True)
    face_rig_analysis = Column(Text, nullable=True)
    face_rig_analysis_updated_at = Column(DateTime, nullable=True)
    ga_client_id = Column(String(100), nullable=True)
    created_via_api = Column(Boolean, default=False)  # True if POST /api/task/create used API key auth

    # rig: Auto Rig worker payload with mode=only_rig; convert: minimal {input_url, type} only
    pipeline_kind = Column(String(20), nullable=False, default="rig")

    # Source size (bytes) when known from upload; optional for link-only tasks
    input_bytes = Column(BigInteger, nullable=True)

    # Server-side NSFW classification from task poster URL (ready_urls); not client-controlled.
    content_rating = Column(String(20), nullable=False, default="unknown")  # safe | suggestive | adult | unknown
    content_score = Column(Float, nullable=True)  # 0..1 composite from detector
    content_classified_at = Column(DateTime, nullable=True)
    content_classifier_version = Column(String(64), nullable=True)

    # OpenAI vision metadata from poster (same image as NudeNet); JSON array in poster_llm_keywords
    poster_llm_title = Column(String(256), nullable=True)
    poster_llm_description = Column(Text, nullable=True)
    poster_llm_keywords = Column(Text, nullable=True)
    poster_llm_at = Column(DateTime, nullable=True)

    # YouTube auto-upload (server uses OAuth refresh token; see youtube_upload.py)
    youtube_video_id = Column(String(64), nullable=True)
    youtube_upload_status = Column(String(32), nullable=True)  # uploaded | skipped | failed | deferred
    youtube_upload_error = Column(Text, nullable=True)
    youtube_uploaded_at = Column(DateTime, nullable=True)
    # SHA-256 hex of uploaded video bytes (dedupe + audit)
    youtube_source_sha256 = Column(String(64), nullable=True)

    @property
    def output_urls(self) -> list:
        return json.loads(self._output_urls) if self._output_urls else []
    
    @output_urls.setter
    def output_urls(self, value: list):
        self._output_urls = json.dumps(value)
    
    @property
    def ready_urls(self) -> list:
        return json.loads(self._ready_urls) if self._ready_urls else []
    
    @ready_urls.setter
    def ready_urls(self, value: list):
        self._ready_urls = json.dumps(value)

    @property
    def progress(self) -> int:
        if self.total_count == 0:
            return 0
        value = int((self.ready_count / self.total_count) * 100)
        if self.status == "processing":
            return min(value, 99)
        return value


class TaskAnimationCorrection(Base):
    """Draft/published realtime bone corrections for one task viewer."""

    __tablename__ = "task_animation_corrections"

    task_id = Column(
        String(36),
        ForeignKey("tasks.id", ondelete="CASCADE"),
        primary_key=True,
    )
    draft_json = Column(Text, nullable=True)
    published_json = Column(Text, nullable=True)
    published_revision = Column(Integer, nullable=False, default=0)
    export_status = Column(String(32), nullable=False, default="idle")
    export_error = Column(Text, nullable=True)
    source_sha256_json = Column(Text, nullable=True)
    corrected_glb_url = Column(String(1024), nullable=True)
    corrected_fbx_zip_url = Column(String(1024), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    published_at = Column(DateTime, nullable=True)


class AnimalAnimationLibraryVersion(Base):
    """One immutable draft/published revision of a canonical species library."""

    __tablename__ = "animal_animation_library_versions"
    __table_args__ = (
        UniqueConstraint("rig_type", "revision", name="uq_animal_animation_library_rig_revision"),
        Index("ix_animal_animation_library_rig_status", "rig_type", "status"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    rig_type = Column(String(32), nullable=False, index=True)
    revision = Column(String(128), nullable=False, index=True)
    status = Column(String(24), nullable=False, default="draft", index=True)  # draft | published | retired
    template_skeleton_sha256 = Column(String(64), nullable=False)
    qa_profile_revision = Column(String(128), nullable=False)
    notes = Column(Text, nullable=True)
    created_by = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    published_at = Column(DateTime, nullable=True)


class AnimalAnimationLibraryArtifact(Base):
    """Orientation-specific canonical manifest and multi-clip GLB for a revision."""

    __tablename__ = "animal_animation_library_artifacts"
    __table_args__ = (
        UniqueConstraint("library_version_id", "orientation", name="uq_animal_animation_artifact_orientation"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    library_version_id = Column(
        Integer,
        ForeignKey("animal_animation_library_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    orientation = Column(String(16), nullable=False)
    manifest_json = Column(Text, nullable=False)
    manifest_sha256 = Column(String(64), nullable=False)
    animation_glb_url = Column(String(2048), nullable=True)
    animation_glb_path = Column(String(2048), nullable=True)
    artifact_sha256 = Column(String(64), nullable=False)
    animation_clip_count = Column(Integer, nullable=False)
    package_zip_url = Column(String(2048), nullable=True)
    package_zip_path = Column(String(2048), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AnimalAnimationLibraryActivation(Base):
    """History interval used to bind a task to the revision active when it was created."""

    __tablename__ = "animal_animation_library_activations"
    __table_args__ = (
        Index("ix_animal_animation_activation_lookup", "rig_type", "activated_at", "deactivated_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    rig_type = Column(String(32), nullable=False, index=True)
    library_version_id = Column(
        Integer,
        ForeignKey("animal_animation_library_versions.id"),
        nullable=False,
        index=True,
    )
    activated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    deactivated_at = Column(DateTime, nullable=True, index=True)
    activated_by = Column(String(255), nullable=False)
    reason = Column(String(32), nullable=False, default="activate")  # activate | rollback


class AnimalAnimationFittingJob(Base):
    """Durable orchestration state for one species/semantic action fitting run."""

    __tablename__ = "animal_animation_fitting_jobs"
    __table_args__ = (
        Index("ix_animal_fitting_job_library_action", "library_version_id", "semantic_id"),
        Index("ix_animal_fitting_job_status_created", "status", "created_at"),
    )

    id = Column(String(36), primary_key=True)
    library_version_id = Column(
        Integer,
        ForeignKey("animal_animation_library_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    rig_type = Column(String(32), nullable=False, index=True)
    semantic_id = Column(String(128), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="queued", index=True)
    workflow_name = Column(String(128), nullable=False)
    workflow_fingerprint = Column(String(128), nullable=False)
    worker_url = Column(String(2048), nullable=False)
    prompt_id = Column(String(128), nullable=False, unique=True, index=True)
    prompt = Column(Text, nullable=False)
    candidate_target = Column(Integer, nullable=False, default=8)
    candidate_limit = Column(Integer, nullable=False, default=16)
    candidates_attempted = Column(Integer, nullable=False, default=0)
    config_json = Column(Text, nullable=False, default="{}")
    error = Column(Text, nullable=True)
    created_by = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)


class AnimalAnimationCandidate(Base):
    """Generated LTX video plus fitted clip and machine QA for a fitting job."""

    __tablename__ = "animal_animation_candidates"
    __table_args__ = (
        UniqueConstraint("job_id", "seed", name="uq_animal_animation_candidate_job_seed"),
        UniqueConstraint("job_id", "rank", name="uq_animal_animation_candidate_job_rank"),
        Index("ix_animal_animation_candidate_job_rank", "job_id", "rank"),
    )

    id = Column(String(36), primary_key=True)
    job_id = Column(
        String(36),
        ForeignKey("animal_animation_fitting_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    seed = Column(BigInteger, nullable=False)
    status = Column(String(32), nullable=False, default="generated", index=True)
    raw_video_url = Column(String(2048), nullable=True)
    raw_video_path = Column(String(2048), nullable=True)
    decoded_frames_path = Column(String(2048), nullable=True)
    fitted_clip_url = Column(String(2048), nullable=True)
    fitted_clip_path = Column(String(2048), nullable=True)
    fitted_clip_sha256 = Column(String(64), nullable=True)
    duration = Column(Float, nullable=True)
    fps = Column(Float, nullable=True)
    root_motion_available = Column(Boolean, nullable=False, default=False)
    metrics_json = Column(Text, nullable=False, default="{}")
    provenance_json = Column(Text, nullable=False, default="{}")
    rank_score = Column(Float, nullable=True)
    rank = Column(Integer, nullable=True)
    qa_passed = Column(Boolean, nullable=False, default=False)
    decision = Column(String(16), nullable=True)  # approved | rejected
    decision_reason = Column(Text, nullable=True)
    reviewed_by = Column(String(255), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AnimalAnimationApprovedClip(Base):
    """The one admin-approved candidate for a semantic ID in a library revision."""

    __tablename__ = "animal_animation_approved_clips"
    __table_args__ = (
        UniqueConstraint("library_version_id", "semantic_id", name="uq_animal_approved_clip_semantic"),
        UniqueConstraint("candidate_id", name="uq_animal_approved_clip_candidate"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    library_version_id = Column(
        Integer,
        ForeignKey("animal_animation_library_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    candidate_id = Column(
        String(36),
        ForeignKey("animal_animation_candidates.id"),
        nullable=False,
        index=True,
    )
    semantic_id = Column(String(128), nullable=False, index=True)
    category = Column(String(32), nullable=False)
    clip_order = Column(Integer, nullable=False)
    loop = Column(Boolean, nullable=False)
    duration = Column(Float, nullable=False)
    fps = Column(Float, nullable=False)
    start_pose_id = Column(String(128), nullable=False)
    end_pose_id = Column(String(128), nullable=False)
    root_motion_available = Column(Boolean, nullable=False, default=False)
    qa_profile_revision = Column(String(128), nullable=False)
    fbx_url = Column(String(2048), nullable=True)
    fbx_path = Column(String(2048), nullable=True)
    fbx_sha256 = Column(String(64), nullable=False)
    metrics_json = Column(Text, nullable=False, default="{}")
    provenance_json = Column(Text, nullable=False, default="{}")
    approved_by = Column(String(255), nullable=False)
    approved_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class AdminOverlayCounters(Base):
    """Singleton (id=1): периодные счётчики для админ-оверлея; сброс вручную."""

    __tablename__ = "admin_overlay_counters"

    id = Column(Integer, primary_key=True, default=1)
    completed_count = Column(Integer, nullable=False, default=0)
    total_duration_seconds = Column(Float, nullable=False, default=0.0)
    # Public all-time completed rig count; not reset by admin overlay metrics.
    public_completed_total = Column(Integer, nullable=False, default=7124)
    # Upper bound for total size of static/tasks (GB); evict oldest cache dirs when exceeded
    task_cache_max_gb = Column(Float, nullable=False, default=22.0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RigCompletionEvent(Base):
    """Durable public completion event, independent from purged task rows/cache."""
    __tablename__ = "rig_completion_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), nullable=False, unique=True, index=True)
    completed_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class YoutubeCredentials(Base):
    """Single-row store for YouTube channel OAuth (refresh token for uploads)."""
    __tablename__ = "youtube_credentials"

    id = Column(Integer, primary_key=True)  # always 1
    refresh_token = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class YoutubeUploadedHash(Base):
    """SHA-256 of video file bytes already uploaded to YouTube (dedupe by content)."""
    __tablename__ = "youtube_uploaded_hashes"

    sha256_hex = Column(String(64), primary_key=True)
    youtube_video_id = Column(String(64), nullable=False)
    first_task_id = Column(String(36), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class WorkerEndpoint(Base):
    """Conversion worker endpoint (admin-managed)."""
    __tablename__ = "worker_endpoints"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(255), unique=True, nullable=False, index=True)
    enabled = Column(Boolean, default=True)
    weight = Column(Integer, default=0)  # Higher means higher priority
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Session(Base):
    """User session for authentication"""
    __tablename__ = "sessions"
    
    token = Column(String(64), primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)


class GumroadSale(Base):
    """
    Gumroad ping record (idempotency + audit).
    sale_id is unique and used to ignore duplicate webhook deliveries.
    """
    __tablename__ = "gumroad_sales"

    sale_id = Column(String(255), primary_key=True)
    user_email = Column(String(255), nullable=True, index=True)
    product_permalink = Column(String(255), nullable=True)
    gumroad_email = Column(String(255), nullable=True)
    price = Column(String(64), nullable=True)
    quantity = Column(Integer, nullable=True)
    refunded = Column(Boolean, default=False)
    test = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class GumroadPurchase(Base):
    """
    Full Gumroad webhook audit log + idempotency by sale_id.
    """
    __tablename__ = "gumroad_purchases"

    sale_id = Column(String(255), primary_key=True)
    email = Column(String(255), nullable=False, index=True)
    product_permalink = Column(String(255), nullable=False, index=True)
    product_name = Column(String(255), nullable=True)
    price = Column(Integer, nullable=True)
    refunded = Column(Boolean, default=False)
    is_recurring_charge = Column(Boolean, default=False)
    subscription_id = Column(String(255), nullable=True)
    license_key = Column(String(255), nullable=True)
    test = Column(Boolean, default=False)
    raw_payload = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    credited = Column(Boolean, default=False)
    credits_added = Column(Integer, default=0)


class PurchaseCheckoutIntent(Base):
    """Server-side checkout click/pending task-unlock intent."""
    __tablename__ = "purchase_checkout_intents"
    __table_args__ = (
        Index("ix_purchase_checkout_intents_user_created", "user_email", "created_at"),
        Index("ix_purchase_checkout_intents_task", "task_id"),
        Index("ix_purchase_checkout_intents_sale", "gumroad_sale_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(255), nullable=False, index=True)
    product_permalink = Column(String(255), nullable=False, index=True)
    product_kind = Column(String(40), nullable=False, default="credits")
    source = Column(String(80), nullable=True)
    task_id = Column(String(36), nullable=True, index=True)
    required_credits = Column(Integer, nullable=True)
    page_url = Column(String(1024), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    used_at = Column(DateTime, nullable=True)
    gumroad_sale_id = Column(String(255), nullable=True)
    auto_unlock_status = Column(String(64), nullable=True)


class ApiKey(Base):
    """API keys (stored hashed): either bound to a registered user or an anonymous session."""
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True, index=True)
    anon_id = Column(String(36), nullable=True, index=True)
    key_prefix = Column(String(16), nullable=False, index=True)
    key_hash = Column(String(64), nullable=False, index=True)  # sha256 hex
    created_at = Column(DateTime, default=datetime.utcnow)
    revoked_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)


class TelegramChat(Base):
    """Telegram chat subscribed to notifications"""
    __tablename__ = "telegram_chats"
    
    chat_id = Column(BigInteger, primary_key=True)
    chat_type = Column(String(50), nullable=True)
    title = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)


class TelegramNotification(Base):
    """Idempotency guard for Telegram sends per chat/event."""
    __tablename__ = "telegram_notifications"
    __table_args__ = (
        UniqueConstraint("chat_id", "event_type", "event_key", name="uq_tg_chat_event_key"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, nullable=False, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    event_key = Column(String(128), nullable=False, index=True)
    message_id = Column(BigInteger, nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SupportChatSession(Base):
    """Site support chat session; maps to one Telegram forum topic after first outbound message."""

    __tablename__ = "support_chat_sessions"
    __table_args__ = (
        Index("ix_support_sessions_forum_thread", "telegram_chat_id", "telegram_thread_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    visitor_id = Column(String(96), nullable=False, index=True)
    user_email = Column(String(255), nullable=True, index=True)
    page_url = Column(Text, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    telegram_thread_id = Column(Integer, nullable=True)
    topic_name = Column(String(512), nullable=True)
    status = Column(String(32), nullable=False, default="open", index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SupportChatMessage(Base):
    """Single support chat line (website user / Telegram admin / system)."""

    __tablename__ = "support_chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("support_chat_sessions.id"), nullable=False, index=True)
    direction = Column(String(16), nullable=False, index=True)  # user | admin | system
    body_text = Column(Text, nullable=False)
    telegram_message_id = Column(BigInteger, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Feedback(Base):
    """User feedback/comments"""
    __tablename__ = "feedback"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(255), nullable=False, index=True)
    user_name = Column(String(255), nullable=True)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    parent_id = Column(Integer, ForeignKey("feedback.id"), nullable=True, index=True)


class RoadmapVote(Base):
    """One roadmap priority vote per registered user (choice can be updated)."""
    __tablename__ = "roadmap_votes"
    __table_args__ = ()

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    choice = Column(String(64), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CryptoPaymentReport(Base):
    """User/agent-reported crypto payment pending manual credit."""

    __tablename__ = "crypto_payment_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    tier = Column(String(64), nullable=False)
    network_id = Column(String(32), nullable=False)
    tx_id = Column(String(256), nullable=False)
    contact_note = Column(Text, nullable=True)
    user_email = Column(String(255), nullable=True, index=True)
    agent_anon_id = Column(String(36), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="pending", index=True)


class Scene(Base):
    """Scene combining multiple GLB models with transforms"""
    __tablename__ = "scenes"
    
    id = Column(String(36), primary_key=True)  # UUID
    owner_type = Column(String(10), nullable=False)  # 'anon' or 'user'
    owner_id = Column(String(255), nullable=False)  # anon_id or user email
    
    name = Column(String(255), nullable=True)
    
    # JSON array of task_ids that make up this scene
    _task_ids = Column("task_ids", Text, default="[]")
    
    # JSON object with transform data for each task
    # Format: {"task_id": {"position": [x,y,z], "rotation": [x,y,z], "scale": [x,y,z]}}
    _transforms = Column("transforms", Text, default="{}")
    
    # Hierarchy structure (which objects are grouped)
    # Format: {"groups": [{"name": "...", "task_ids": [...]}]}
    _hierarchy = Column("hierarchy", Text, default="{}")
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Gallery integration
    like_count = Column(Integer, default=0)
    is_public = Column(Boolean, default=False)
    
    @property
    def task_ids(self) -> list:
        return json.loads(self._task_ids) if self._task_ids else []
    
    @task_ids.setter
    def task_ids(self, value: list):
        self._task_ids = json.dumps(value)
    
    @property
    def transforms(self) -> dict:
        return json.loads(self._transforms) if self._transforms else {}
    
    @transforms.setter
    def transforms(self, value: dict):
        self._transforms = json.dumps(value)
    
    @property
    def hierarchy(self) -> dict:
        return json.loads(self._hierarchy) if self._hierarchy else {}
    
    @hierarchy.setter
    def hierarchy(self, value: dict):
        self._hierarchy = json.dumps(value)


class SceneLike(Base):
    """Like on a scene (by registered user)"""
    __tablename__ = "scene_likes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    scene_id = Column(String(36), nullable=False, index=True)
    user_email = Column(String(255), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# Database Initialization
# =============================================================================
def _migrate_api_keys_sqlite_for_anon(sync_conn):
    """Rebuild api_keys so user_id is nullable and anon_id exists (SQLite)."""
    from sqlalchemy import text

    try:
        r = sync_conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='api_keys'")
        )
        if r.scalar() is None:
            return
    except Exception:
        return
    r = sync_conn.execute(text("PRAGMA table_info(api_keys)"))
    cols = [row[1] for row in r.fetchall()]
    if "anon_id" in cols:
        return

    sync_conn.execute(text("PRAGMA foreign_keys=OFF"))
    sync_conn.execute(
        text(
            """
            CREATE TABLE api_keys_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                anon_id VARCHAR(36),
                key_prefix VARCHAR(16) NOT NULL,
                key_hash VARCHAR(64) NOT NULL,
                created_at DATETIME,
                revoked_at DATETIME,
                last_used_at DATETIME
            )
            """
        )
    )
    sync_conn.execute(
        text(
            """
            INSERT INTO api_keys_new (id, user_id, anon_id, key_prefix, key_hash, created_at, revoked_at, last_used_at)
            SELECT id, user_id, NULL, key_prefix, key_hash, created_at, revoked_at, last_used_at FROM api_keys
            """
        )
    )
    sync_conn.execute(text("DROP TABLE api_keys"))
    sync_conn.execute(text("ALTER TABLE api_keys_new RENAME TO api_keys"))
    sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_api_keys_user_id ON api_keys (user_id)"))
    sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_api_keys_anon_id ON api_keys (anon_id)"))
    sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_api_keys_key_prefix ON api_keys (key_prefix)"))
    sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash)"))
    sync_conn.execute(text("PRAGMA foreign_keys=ON"))


_ADMIN_OVERLAY_ROW_ID = 1


async def get_or_create_admin_overlay_counters(db: AsyncSession) -> AdminOverlayCounters:
    from sqlalchemy import select
    from config import PUBLIC_COMPLETED_RIG_BASELINE, TASK_CACHE_MAX_GB

    result = await db.execute(
        select(AdminOverlayCounters).where(AdminOverlayCounters.id == _ADMIN_OVERLAY_ROW_ID)
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = AdminOverlayCounters(
            id=_ADMIN_OVERLAY_ROW_ID,
            completed_count=0,
            total_duration_seconds=0.0,
            public_completed_total=int(PUBLIC_COMPLETED_RIG_BASELINE),
            task_cache_max_gb=float(TASK_CACHE_MAX_GB),
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
    elif int(row.public_completed_total or 0) < int(PUBLIC_COMPLETED_RIG_BASELINE):
        row.public_completed_total = int(PUBLIC_COMPLETED_RIG_BASELINE)
        await db.commit()
        await db.refresh(row)
    return row


async def bump_admin_overlay_task_completed(db: AsyncSession, task: Task) -> None:
    """Счётчик периода: +1 done и сумма длительностей (created→updated)."""
    from sqlalchemy import update

    await get_or_create_admin_overlay_counters(db)
    completed_at = datetime.utcnow()
    if "sqlite" in DATABASE_URL:
        insert_result = await db.execute(
            text(
                """
                INSERT OR IGNORE INTO rig_completion_events (task_id, completed_at)
                VALUES (:task_id, :completed_at)
                """
            ),
            {"task_id": task.id, "completed_at": completed_at},
        )
    else:
        insert_result = await db.execute(
            text(
                """
                INSERT INTO rig_completion_events (task_id, completed_at)
                VALUES (:task_id, :completed_at)
                ON CONFLICT (task_id) DO NOTHING
                """
            ),
            {"task_id": task.id, "completed_at": completed_at},
        )
    if insert_result.rowcount == 0:
        return

    dur = 0.0
    if task.created_at and task.updated_at:
        dur = max(0.0, (task.updated_at - task.created_at).total_seconds())
    values = {
        "completed_count": AdminOverlayCounters.completed_count + 1,
        "total_duration_seconds": AdminOverlayCounters.total_duration_seconds + dur,
        "updated_at": datetime.utcnow(),
        "public_completed_total": AdminOverlayCounters.public_completed_total + 1,
    }
    await db.execute(
        update(AdminOverlayCounters)
        .where(AdminOverlayCounters.id == _ADMIN_OVERLAY_ROW_ID)
        .values(**values)
    )
    await db.commit()


async def get_public_gallery_stats(db: AsyncSession) -> dict:
    """Public counters for the homepage/gallery, independent from visible gallery rows."""
    from sqlalchemy import distinct, func, select
    from config import PUBLIC_COMPLETED_RIG_BASELINE

    row = await get_or_create_admin_overlay_counters(db)
    completed_total = max(
        int(PUBLIC_COMPLETED_RIG_BASELINE),
        int(getattr(row, "public_completed_total", 0) or 0),
    )
    since = datetime.utcnow() - timedelta(hours=24)

    event_result = await db.execute(
        select(func.count(distinct(RigCompletionEvent.task_id))).where(
            RigCompletionEvent.completed_at >= since
        )
    )
    event_last_24h = int(event_result.scalar() or 0)

    task_result = await db.execute(
        select(func.count(Task.id)).where(
            Task.status == "done",
            Task.updated_at >= since,
        )
    )
    task_last_24h = int(task_result.scalar() or 0)

    telegram_last_24h = 0
    try:
        telegram_result = await db.execute(
            select(func.count(distinct(TelegramNotification.event_key))).where(
                TelegramNotification.event_type == "task_done",
                TelegramNotification.created_at >= since,
            )
        )
        telegram_last_24h = int(telegram_result.scalar() or 0)
    except Exception:
        telegram_last_24h = 0

    return {
        "completed_total": completed_total,
        "completed_last_24h": max(event_last_24h, task_last_24h, telegram_last_24h),
    }


async def reset_admin_overlay_counters(db: AsyncSession) -> None:
    from sqlalchemy import update

    await get_or_create_admin_overlay_counters(db)
    await db.execute(
        update(AdminOverlayCounters)
        .where(AdminOverlayCounters.id == _ADMIN_OVERLAY_ROW_ID)
        .values(
            completed_count=0,
            total_duration_seconds=0.0,
            updated_at=datetime.utcnow(),
        )
    )
    await db.commit()


async def init_db():
    """Create all tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Lightweight sqlite "migration" to add new columns without a migration framework.
        # Safe to run repeatedly (errors are ignored when column already exists).
        if "sqlite" in DATABASE_URL:
            await conn.run_sync(_migrate_api_keys_sqlite_for_anon)
            async def _try_add_column(sql: str):
                try:
                    await conn.exec_driver_sql(sql)
                except Exception:
                    # Column likely already exists, or DB doesn't support the statement.
                    pass
            await _try_add_column("ALTER TABLE users ADD COLUMN gumroad_email VARCHAR(255)")
            await _try_add_column("ALTER TABLE users ADD COLUMN nickname VARCHAR(100)")
            await _try_add_column("ALTER TABLE users ADD COLUMN youtube_bonus_received BOOLEAN DEFAULT 0")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_task_completed BOOLEAN DEFAULT 1")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_marketing_unsubscribed_at DATETIME")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_invalid_at DATETIME")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_invalid_reason TEXT")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_invalid_source VARCHAR(64)")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_last_bounce_at DATETIME")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_last_bounce_type VARCHAR(32)")
            await _try_add_column("ALTER TABLE users ADD COLUMN email_transient_bounce_count INTEGER DEFAULT 0")
            await _try_add_column("ALTER TABLE feedback ADD COLUMN parent_id INTEGER")

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS email_campaign_sends (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        campaign_key VARCHAR(128) NOT NULL,
                        user_id INTEGER,
                        email_hash VARCHAR(64) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        provider_message_id VARCHAR(128),
                        error TEXT,
                        created_at DATETIME,
                        updated_at DATETIME,
                        sent_at DATETIME,
                        FOREIGN KEY(user_id) REFERENCES users(id)
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_email_campaign_key_hash
                    ON email_campaign_sends (campaign_key, email_hash)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_sends_campaign_status
                    ON email_campaign_sends (campaign_key, status)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_sends_email_hash
                    ON email_campaign_sends (email_hash)
                    """
                )
            except Exception:
                pass

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS email_campaign_clicks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        campaign_key VARCHAR(128) NOT NULL,
                        user_id INTEGER,
                        email_hash VARCHAR(64) NOT NULL,
                        link_key VARCHAR(64) NOT NULL,
                        destination_url VARCHAR(1024) NOT NULL,
                        ip_hash VARCHAR(64),
                        user_agent VARCHAR(512),
                        clicked_at DATETIME NOT NULL,
                        FOREIGN KEY(user_id) REFERENCES users(id)
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_clicks_campaign_link
                    ON email_campaign_clicks (campaign_key, link_key)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_clicks_email_hash
                    ON email_campaign_clicks (email_hash)
                    """
                )
            except Exception:
                pass

            await _try_add_column("ALTER TABLE anon_sessions ADD COLUMN agent_name VARCHAR(255)")
            await _try_add_column("ALTER TABLE anon_sessions ADD COLUMN agent_description TEXT")
            await _try_add_column("ALTER TABLE anon_sessions ADD COLUMN registered_as_agent BOOLEAN DEFAULT 0")

            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_output_url VARCHAR(1024)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_model_name VARCHAR(64)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_ready BOOLEAN DEFAULT 0")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_error TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN viewer_settings TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN face_rig_analysis TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN face_rig_analysis_updated_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN ga_client_id VARCHAR(100)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN created_via_api BOOLEAN DEFAULT 0")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN telegram_new_notified_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN telegram_done_notified_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN content_rating VARCHAR(20) DEFAULT 'unknown'")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN content_score REAL")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN content_classified_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN content_classifier_version VARCHAR(64)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN youtube_video_id VARCHAR(64)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN youtube_upload_status VARCHAR(32)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN youtube_upload_error TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN youtube_uploaded_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN youtube_source_sha256 VARCHAR(64)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN pipeline_kind VARCHAR(20) DEFAULT 'rig'")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN input_bytes BIGINT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN poster_llm_title VARCHAR(256)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN poster_llm_description TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN poster_llm_keywords TEXT")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN poster_llm_at DATETIME")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN stuck_hour_requeue_count INTEGER DEFAULT 0")
            await _try_add_column(
                "ALTER TABLE admin_overlay_counters ADD COLUMN task_cache_max_gb REAL DEFAULT 22"
            )
            await _try_add_column(
                "ALTER TABLE admin_overlay_counters ADD COLUMN public_completed_total INTEGER DEFAULT 7124"
            )
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS rig_completion_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id VARCHAR(36) NOT NULL UNIQUE,
                        completed_at DATETIME NOT NULL
                    )
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_rig_completion_events_completed_at ON rig_completion_events (completed_at)"
                )
            except Exception:
                pass
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS youtube_uploaded_hashes (
                        sha256_hex VARCHAR(64) PRIMARY KEY,
                        youtube_video_id VARCHAR(64) NOT NULL,
                        first_task_id VARCHAR(36),
                        created_at DATETIME
                    )
                    """
                )
            except Exception:
                pass
            
            # Scene table migrations
            await _try_add_column("ALTER TABLE scenes ADD COLUMN is_public BOOLEAN DEFAULT 0")

            # Telegram idempotency table/index (safe to run repeatedly)
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS telegram_notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id BIGINT NOT NULL,
                        event_type VARCHAR(64) NOT NULL,
                        event_key VARCHAR(128) NOT NULL,
                        message_id BIGINT,
                        deleted_at DATETIME,
                        created_at DATETIME
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tg_chat_event_key
                    ON telegram_notifications (chat_id, event_type, event_key)
                    """
                )
            except Exception:
                pass
            await _try_add_column("ALTER TABLE telegram_notifications ADD COLUMN message_id BIGINT")
            await _try_add_column("ALTER TABLE telegram_notifications ADD COLUMN deleted_at DATETIME")

            # Worker endpoints table/index (safe to run repeatedly)
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS worker_endpoints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url VARCHAR(255) NOT NULL,
                        enabled BOOLEAN DEFAULT 1,
                        weight INTEGER DEFAULT 0,
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_endpoints_url
                    ON worker_endpoints (url)
                    """
                )

                # Seed defaults from config.WORKERS (safe + idempotent).
                # This makes the DB the source of truth without manual setup.
                try:
                    from config import WORKERS as DEFAULT_WORKERS
                except Exception:
                    DEFAULT_WORKERS = []

                for raw_url in (DEFAULT_WORKERS or []):
                    url = (raw_url or "").strip()
                    while url.endswith("/"):
                        url = url[:-1]
                    if not url:
                        continue
                    try:
                        await conn.exec_driver_sql(
                            "INSERT OR IGNORE INTO worker_endpoints (url, enabled, weight, created_at, updated_at) VALUES (?, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                            (url,)
                        )
                    except Exception:
                        pass
            except Exception:
                pass

            # Gumroad purchases audit table/index (safe to run repeatedly)
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS gumroad_purchases (
                        sale_id VARCHAR(255) PRIMARY KEY,
                        email VARCHAR(255) NOT NULL,
                        product_permalink VARCHAR(255) NOT NULL,
                        product_name VARCHAR(255),
                        price INTEGER,
                        refunded BOOLEAN DEFAULT 0,
                        is_recurring_charge BOOLEAN DEFAULT 0,
                        subscription_id VARCHAR(255),
                        license_key VARCHAR(255),
                        test BOOLEAN DEFAULT 0,
                        raw_payload TEXT,
                        created_at DATETIME,
                        credited BOOLEAN DEFAULT 0,
                        credits_added INTEGER DEFAULT 0
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS idx_gumroad_purchases_email
                    ON gumroad_purchases (email)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS idx_gumroad_purchases_product
                    ON gumroad_purchases (product_permalink)
                    """
                )
            except Exception:
                pass

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS purchase_checkout_intents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_email VARCHAR(255) NOT NULL,
                        product_permalink VARCHAR(255) NOT NULL,
                        product_kind VARCHAR(40) NOT NULL DEFAULT 'credits',
                        source VARCHAR(80),
                        task_id VARCHAR(36),
                        required_credits INTEGER,
                        page_url VARCHAR(1024),
                        created_at DATETIME NOT NULL,
                        used_at DATETIME,
                        gumroad_sale_id VARCHAR(255),
                        auto_unlock_status VARCHAR(64)
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_purchase_checkout_intents_user_created
                    ON purchase_checkout_intents (user_email, created_at)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_purchase_checkout_intents_product
                    ON purchase_checkout_intents (product_permalink)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_purchase_checkout_intents_task
                    ON purchase_checkout_intents (task_id)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_purchase_checkout_intents_sale
                    ON purchase_checkout_intents (gumroad_sale_id)
                    """
                )
            except Exception:
                pass

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS roadmap_votes (
                        user_id INTEGER NOT NULL PRIMARY KEY,
                        choice VARCHAR(64) NOT NULL,
                        updated_at DATETIME,
                        FOREIGN KEY(user_id) REFERENCES users(id)
                    )
                    """
                )
            except Exception:
                pass

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS crypto_payment_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at DATETIME,
                        tier VARCHAR(64) NOT NULL,
                        network_id VARCHAR(32) NOT NULL,
                        tx_id VARCHAR(256) NOT NULL,
                        contact_note TEXT,
                        user_email VARCHAR(255),
                        agent_anon_id VARCHAR(36),
                        status VARCHAR(32) NOT NULL DEFAULT 'pending'
                    )
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_crypto_reports_created ON crypto_payment_reports (created_at)"
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_crypto_reports_status ON crypto_payment_reports (status)"
                )
            except Exception:
                pass

            # Custom animation purchase tables/indexes (safe to run repeatedly)
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS task_animation_purchases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id VARCHAR(36) NOT NULL,
                        user_email VARCHAR(255) NOT NULL,
                        animation_id VARCHAR(128) NOT NULL,
                        credits_spent INTEGER NOT NULL DEFAULT 1,
                        created_at DATETIME
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_task_animation_purchase
                    ON task_animation_purchases (task_id, user_email, animation_id)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS task_animation_bundle_purchases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id VARCHAR(36) NOT NULL,
                        user_email VARCHAR(255) NOT NULL,
                        credits_spent INTEGER NOT NULL DEFAULT 10,
                        created_at DATETIME
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_task_animation_bundle_purchase
                    ON task_animation_bundle_purchases (task_id, user_email)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS task_animal_animation_pack_purchases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id VARCHAR(36) NOT NULL,
                        user_email VARCHAR(255) NOT NULL,
                        animal_type VARCHAR(32) NOT NULL,
                        orientation VARCHAR(16) NOT NULL,
                        credits_spent INTEGER NOT NULL DEFAULT 10,
                        created_at DATETIME
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_task_animal_animation_pack_purchase
                    ON task_animal_animation_pack_purchases (task_id, user_email, animal_type, orientation)
                    """
                )
            except Exception:
                pass

        else:
            # PostgreSQL (and others): create_all does not ALTER existing tables.
            async def _try_add_column_any(sql: str):
                try:
                    await conn.exec_driver_sql(sql)
                except Exception:
                    pass

            await _try_add_column_any(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_task_completed BOOLEAN DEFAULT TRUE NOT NULL"
            )
            await _try_add_column_any(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_marketing_unsubscribed_at TIMESTAMP"
            )
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_invalid_at TIMESTAMP")
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_invalid_reason TEXT")
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_invalid_source VARCHAR(64)")
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_last_bounce_at TIMESTAMP")
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_last_bounce_type VARCHAR(32)")
            await _try_add_column_any("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_transient_bounce_count INTEGER DEFAULT 0")
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS email_campaign_sends (
                        id SERIAL PRIMARY KEY,
                        campaign_key VARCHAR(128) NOT NULL,
                        user_id INTEGER,
                        email_hash VARCHAR(64) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        provider_message_id VARCHAR(128),
                        error TEXT,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        sent_at TIMESTAMP
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_email_campaign_key_hash
                    ON email_campaign_sends (campaign_key, email_hash)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_sends_campaign_status
                    ON email_campaign_sends (campaign_key, status)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_sends_email_hash
                    ON email_campaign_sends (email_hash)
                    """
                )
            except Exception:
                pass

            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS email_campaign_clicks (
                        id SERIAL PRIMARY KEY,
                        campaign_key VARCHAR(128) NOT NULL,
                        user_id INTEGER,
                        email_hash VARCHAR(64) NOT NULL,
                        link_key VARCHAR(64) NOT NULL,
                        destination_url VARCHAR(1024) NOT NULL,
                        ip_hash VARCHAR(64),
                        user_agent VARCHAR(512),
                        clicked_at TIMESTAMP NOT NULL
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_clicks_campaign_link
                    ON email_campaign_clicks (campaign_key, link_key)
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE INDEX IF NOT EXISTS ix_email_campaign_clicks_email_hash
                    ON email_campaign_clicks (email_hash)
                    """
                )
            except Exception:
                pass
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS content_rating VARCHAR(20) NOT NULL DEFAULT 'unknown'"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS content_score DOUBLE PRECISION"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS content_classified_at TIMESTAMP"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS content_classifier_version VARCHAR(64)"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS youtube_video_id VARCHAR(64)"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS youtube_upload_status VARCHAR(32)"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS youtube_upload_error TEXT"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS youtube_uploaded_at TIMESTAMP"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS youtube_source_sha256 VARCHAR(64)"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS pipeline_kind VARCHAR(20) NOT NULL DEFAULT 'rig'"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS input_bytes BIGINT"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS poster_llm_title VARCHAR(256)"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS poster_llm_description TEXT"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS poster_llm_keywords TEXT"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS poster_llm_at TIMESTAMP"
            )
            await _try_add_column_any(
                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS stuck_hour_requeue_count INTEGER NOT NULL DEFAULT 0"
            )
            await _try_add_column_any(
                "ALTER TABLE admin_overlay_counters ADD COLUMN IF NOT EXISTS task_cache_max_gb DOUBLE PRECISION NOT NULL DEFAULT 22"
            )
            await _try_add_column_any(
                "ALTER TABLE admin_overlay_counters ADD COLUMN IF NOT EXISTS public_completed_total INTEGER NOT NULL DEFAULT 7124"
            )
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS rig_completion_events (
                        id SERIAL PRIMARY KEY,
                        task_id VARCHAR(36) NOT NULL UNIQUE,
                        completed_at TIMESTAMP NOT NULL
                    )
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_rig_completion_events_completed_at ON rig_completion_events (completed_at)"
                )
            except Exception:
                pass
            await _try_add_column_any(
                "ALTER TABLE feedback ADD COLUMN IF NOT EXISTS parent_id INTEGER"
            )
            await _try_add_column_any(
                "ALTER TABLE anon_sessions ADD COLUMN IF NOT EXISTS agent_name VARCHAR(255)"
            )
            await _try_add_column_any(
                "ALTER TABLE anon_sessions ADD COLUMN IF NOT EXISTS agent_description TEXT"
            )
            await _try_add_column_any(
                "ALTER TABLE anon_sessions ADD COLUMN IF NOT EXISTS registered_as_agent BOOLEAN DEFAULT FALSE"
            )
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS roadmap_votes (
                        user_id INTEGER NOT NULL PRIMARY KEY,
                        choice VARCHAR(64) NOT NULL,
                        updated_at TIMESTAMP
                    )
                    """
                )
            except Exception:
                pass
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS youtube_uploaded_hashes (
                        sha256_hex VARCHAR(64) PRIMARY KEY,
                        youtube_video_id VARCHAR(64) NOT NULL,
                        first_task_id VARCHAR(36),
                        created_at TIMESTAMP
                    )
                    """
                )
            except Exception:
                pass
            try:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE IF NOT EXISTS crypto_payment_reports (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP,
                        tier VARCHAR(64) NOT NULL,
                        network_id VARCHAR(32) NOT NULL,
                        tx_id VARCHAR(256) NOT NULL,
                        contact_note TEXT,
                        user_email VARCHAR(255),
                        agent_anon_id VARCHAR(36),
                        status VARCHAR(32) NOT NULL DEFAULT 'pending'
                    )
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_crypto_reports_created ON crypto_payment_reports (created_at)"
                )
                await conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_crypto_reports_status ON crypto_payment_reports (status)"
                )
            except Exception:
                pass


async def get_db():
    """Dependency for getting database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
