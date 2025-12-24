"""
Database models and setup for AutoRig Online
"""
from datetime import datetime
from typing import Optional
import json

from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Text, 
    create_engine, event
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool

from config import DATABASE_URL

# =============================================================================
# Engine and Session Setup
# =============================================================================
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
    picture = Column(String(512), nullable=True)
    gumroad_email = Column(String(255), nullable=True)
    balance_credits = Column(Integer, default=0)
    total_tasks = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime, default=datetime.utcnow)
    
    @property
    def is_admin(self) -> bool:
        from config import ADMIN_EMAIL
        return self.email == ADMIN_EMAIL


class AnonSession(Base):
    """Anonymous user session (tracked by cookie)"""
    __tablename__ = "anon_sessions"
    
    anon_id = Column(String(36), primary_key=True)  # UUID
    free_used = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)


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
    
    # Video
    video_url = Column(String(512), nullable=True)
    video_ready = Column(Boolean, default=False)

    # FBX -> GLB pre-conversion (optional)
    fbx_glb_output_url = Column(String(1024), nullable=True)
    fbx_glb_model_name = Column(String(64), nullable=True)
    fbx_glb_ready = Column(Boolean, default=False)
    fbx_glb_error = Column(Text, nullable=True)
    
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


class ApiKey(Base):
    """User API keys (stored hashed)."""
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    key_prefix = Column(String(16), nullable=False, index=True)
    key_hash = Column(String(64), nullable=False, index=True)  # sha256 hex
    created_at = Column(DateTime, default=datetime.utcnow)
    revoked_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)


# =============================================================================
# Database Initialization
# =============================================================================
async def init_db():
    """Create all tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Lightweight sqlite "migration" to add new columns without a migration framework.
        # Safe to run repeatedly (errors are ignored when column already exists).
        if "sqlite" in DATABASE_URL:
            async def _try_add_column(sql: str):
                try:
                    await conn.exec_driver_sql(sql)
                except Exception:
                    # Column likely already exists, or DB doesn't support the statement.
                    pass
            await _try_add_column("ALTER TABLE users ADD COLUMN gumroad_email VARCHAR(255)")

            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_output_url VARCHAR(1024)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_model_name VARCHAR(64)")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_ready BOOLEAN DEFAULT 0")
            await _try_add_column("ALTER TABLE tasks ADD COLUMN fbx_glb_error TEXT")


async def get_db():
    """Dependency for getting database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

