"""
Database models and setup for AutoRig Online
"""
from datetime import datetime
from typing import Optional
import json

from sqlalchemy import (
    Column, String, Integer, BigInteger, Boolean, DateTime, Text, Float,
    UniqueConstraint, ForeignKey, create_engine, event
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
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime, default=datetime.utcnow)
    
    @property
    def is_admin(self) -> bool:
        from config import is_admin_email
        return is_admin_email(self.email)


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
    youtube_upload_status = Column(String(32), nullable=True)  # uploaded | skipped | failed
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
        return int((self.ready_count / self.total_count) * 100)


class AdminOverlayCounters(Base):
    """Singleton (id=1): периодные счётчики для админ-оверлея; сброс вручную."""

    __tablename__ = "admin_overlay_counters"

    id = Column(Integer, primary_key=True, default=1)
    completed_count = Column(Integer, nullable=False, default=0)
    total_duration_seconds = Column(Float, nullable=False, default=0.0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


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

    result = await db.execute(
        select(AdminOverlayCounters).where(AdminOverlayCounters.id == _ADMIN_OVERLAY_ROW_ID)
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = AdminOverlayCounters(
            id=_ADMIN_OVERLAY_ROW_ID,
            completed_count=0,
            total_duration_seconds=0.0,
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
    return row


async def bump_admin_overlay_task_completed(db: AsyncSession, task: Task) -> None:
    """Счётчик периода: +1 done и сумма длительностей (created→updated)."""
    from sqlalchemy import update

    await get_or_create_admin_overlay_counters(db)
    dur = 0.0
    if task.created_at and task.updated_at:
        dur = max(0.0, (task.updated_at - task.created_at).total_seconds())
    await db.execute(
        update(AdminOverlayCounters)
        .where(AdminOverlayCounters.id == _ADMIN_OVERLAY_ROW_ID)
        .values(
            completed_count=AdminOverlayCounters.completed_count + 1,
            total_duration_seconds=AdminOverlayCounters.total_duration_seconds + dur,
            updated_at=datetime.utcnow(),
        )
    )
    await db.commit()


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
            await _try_add_column("ALTER TABLE feedback ADD COLUMN parent_id INTEGER")

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

