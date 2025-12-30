"""
AutoRig Online - Main FastAPI Application
=========================================
API for automatic 3D model rigging service.
"""
import os
import uuid
import shutil
import asyncio
from datetime import datetime
from typing import Optional
import hashlib
import hmac
import secrets
import json
import tempfile
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse, HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import (
    APP_NAME, APP_URL, DEBUG, SECRET_KEY,
    UPLOAD_DIR, MAX_UPLOAD_SIZE_MB,
    RATE_LIMIT_TASKS_PER_MINUTE, ADMIN_EMAILS,
    ANON_FREE_LIMIT,
    GUMROAD_PRODUCT_CREDITS,
    TELEGRAM_BOT_TOKEN, TELEGRAM_BOT_USERNAME,
    VIEWER_DEFAULT_SETTINGS_PATH
)
from database import init_db, get_db, User, AnonSession, GumroadSale, ApiKey, TaskLike, TaskFilePurchase
from models import (
    TaskCreateRequest, TaskCreateResponse, TaskStatusResponse,
    TaskHistoryItem, TaskHistoryResponse,
    UserInfo, AnonInfo, AuthStatusResponse,
    ApiKeyItem, ApiKeyListResponse, ApiKeyCreateResponse,
    AdminUserListItem, AdminUserListResponse,
    AdminBalanceUpdate, AdminBalanceResponse,
    AdminUserTaskItem, AdminUserTasksResponse,
    AdminStatsResponse, AdminTaskListItem, AdminTaskListResponse,
    WorkerQueueInfo, QueueStatusResponse,
    GalleryItem, GalleryResponse, LikeResponse,
    PurchaseStateResponse, PurchaseRequest, PurchaseResponse
)
from workers import get_global_queue_status
from auth import (
    get_google_auth_url, exchange_code_for_tokens, get_google_user_info,
    create_session, get_user_by_session, delete_session,
    get_or_create_user, get_or_create_anon_session,
    increment_anon_usage, can_create_task_anon, can_create_task_user,
    get_remaining_credits_anon, decrement_user_credits
)
from tasks import (
    create_conversion_task, update_task_progress, start_task_on_worker,
    get_task_by_id, get_user_tasks,
    get_all_users, update_user_balance,
    get_gallery_items, format_time_ago,
    find_and_reset_stale_tasks,
    find_file_by_pattern
)


# =============================================================================
# Viewer Settings Defaults (global file + fallback)
# =============================================================================
DEFAULT_VIEWER_SETTINGS: dict = {
    "mainLightIntensity": 3.0,
    "envIntensity": 1.0,
    "reflectionIntensity": 3.0,
    "modelRotation": "z180",
    "bgColor": "#000000",
    "groundColor": "#222222",
    "groundSize": 1.0,
    "shadowIntensity": 0.5,
    "shadowRadius": 1.0,
    "sunRotation": 45.0,
    "sunInclination": 45.0,
    "timeOfDay": 12.0,
    "ambientColor": "#ffffff",
    "ambientIntensity": 0.3,
    "fogColor": "#000000",
    "fogDensity": 0.0,
    "lightingPreset": "day",
    "camera": {
        "position": {"x": 0, "y": 1.6, "z": 3.5},
        "target": {"x": 0, "y": 1.0, "z": 0},
    },
    "syncAdjChannel": False,
    "bloom": {"strength": 0.0, "threshold": 0.8, "radius": 0.4},
    "adjustments": {
        "albedo": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
        "ao": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
        "normal": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
        "roughness": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
        "metalness": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
        "emissive": {
            "brightness": 1.0, "contrast": 1.0, "saturation": 1.0, "mode": 0,
            "maskColor": "#ffffff", "softness": 0.5, "emissiveMult": 2.0,
            "blendColor": "#ffffff", "invert": False
        },
    },
    "aoSettings": {"samples": 32, "radius": 0.15, "intensity": 1.5},
}


def _read_json_file(path: str) -> Optional[dict]:
    try:
        p = Path(path)
        if not p.exists():
            return None
        raw = p.read_text(encoding="utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        print(f"[ViewerDefaults] Failed to read json file {path}: {e}")
        return None


def _atomic_write_json_file(path: str, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_fd = None
    tmp_path = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        tmp_fd = None
        os.replace(tmp_path, str(p))
        tmp_path = None
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except Exception:
                pass
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


# =============================================================================
# Background Task Worker
# =============================================================================
background_task_running = False
background_worker_cycle_count = 0  # Track cycles for periodic stale task checks

async def background_task_updater():
    """Background worker that updates all processing tasks periodically"""
    from database import AsyncSessionLocal, Task
    from config import STALE_CHECK_INTERVAL_CYCLES
    
    global background_task_running, background_worker_cycle_count
    background_task_running = True
    background_worker_cycle_count = 0
    
    print("[Background Worker] Started task updater")
    
    while background_task_running:
        try:
            background_worker_cycle_count += 1
            
            async with AsyncSessionLocal() as db:
                # =============================================================
                # 1. Dispatch queued tasks (status=created) to free workers
                # =============================================================
                try:
                    queue_status = await get_global_queue_status()
                    free_workers = [
                        w for w in queue_status.workers
                        if w.available and (w.total_active < w.max_concurrent) and (w.queue_size <= 0)
                    ]

                    if free_workers:
                        # Pull up to N queued tasks
                        queued_result = await db.execute(
                            select(Task)
                            .where(Task.status == "created")
                            .order_by(Task.created_at)
                            .limit(len(free_workers))
                        )
                        queued_tasks = queued_result.scalars().all()

                        for task, worker in zip(queued_tasks, free_workers):
                            try:
                                await start_task_on_worker(db, task, worker.url)
                            except Exception as e:
                                print(f"[Background Worker] Error dispatching task {task.id}: {e}")
                except Exception as e:
                    print(f"[Background Worker] Queue dispatch error: {e}")

                # =============================================================
                # 2. Check for stale tasks periodically
                # =============================================================
                if background_worker_cycle_count % STALE_CHECK_INTERVAL_CYCLES == 0:
                    try:
                        reset_count = await find_and_reset_stale_tasks(db)
                        if reset_count > 0:
                            print(f"[Background Worker] Auto-reset {reset_count} stale task(s)")
                    except Exception as e:
                        print(f"[Background Worker] Stale task check error: {e}")

                # =============================================================
                # 3. Update progress for all processing tasks
                # =============================================================
                result = await db.execute(
                    select(Task).where(Task.status == "processing")
                )
                processing_tasks = result.scalars().all()
                
                if processing_tasks:
                    print(f"[Background Worker] Updating {len(processing_tasks)} processing tasks")

                    # Update tasks concurrently (bounded) so the loop doesn't take minutes when many tasks are processing
                    # IMPORTANT: Each task gets its own DB session to avoid SQLAlchemy transaction conflicts
                    semaphore = asyncio.Semaphore(8)

                    async def _update_one(task_id: str):
                        async with semaphore:
                            try:
                                async with AsyncSessionLocal() as task_db:
                                    task = await get_task_by_id(task_db, task_id)
                                    if task and task.status == "processing":
                                        await update_task_progress(task_db, task)
                            except Exception as e:
                                print(f"[Background Worker] Error updating task {task_id}: {e}")

                    # Pass task IDs, not task objects (to get fresh data in each session)
                    task_ids = [t.id for t in processing_tasks]
                    await asyncio.gather(*[_update_one(tid) for tid in task_ids])
                
        except Exception as e:
            print(f"[Background Worker] Error: {e}")
        
        # Wait 30 seconds between checks
        await asyncio.sleep(30)
    
    print("[Background Worker] Stopped")


async def restart_background_worker(app: FastAPI):
    """Restart the in-process background worker task updater."""
    global background_task_running

    # Stop current loop and cancel current task (if any)
    background_task_running = False
    existing = getattr(app.state, "background_worker", None)
    if existing:
        existing.cancel()
        try:
            await existing
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[Background Worker] Failed to stop: {e}")

    # Start a new worker (it will set background_task_running = True)
    app.state.background_worker = asyncio.create_task(background_task_updater())


# =============================================================================
# App Setup
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""
    global background_task_running
    
    # Startup
    await init_db()
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    
    # Start background worker
    app.state.background_worker = asyncio.create_task(background_task_updater())
    
    # Send Telegram startup notification (fire-and-forget)
    try:
        from telegram_bot import broadcast_server_startup
        asyncio.create_task(broadcast_server_startup())
    except Exception as e:
        print(f"[Telegram] Failed to send startup notification: {e}")
    
    yield
    
    # Shutdown
    background_task_running = False
    background_worker = getattr(app.state, "background_worker", None)
    if background_worker:
        background_worker.cancel()
    try:
        if background_worker:
            await background_worker
    except asyncio.CancelledError:
        pass


limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title=APP_NAME,
    description="Automatic 3D model rigging service",
    version="1.0.0",
    lifespan=lifespan
)

app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Rate limit exceeded. Please try again later."}
    )


# =============================================================================
# Dependencies
# =============================================================================
ANON_COOKIE = "anon_id"
SESSION_COOKIE = "session"


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db)
) -> Optional[User]:
    """Get current authenticated user from session cookie"""
    session_token = request.cookies.get(SESSION_COOKIE)
    if not session_token:
        # API Key auth fallback (REST API usage)
        api_key = request.headers.get("x-api-key")
        auth = request.headers.get("authorization") or ""
        if not api_key and auth.lower().startswith("bearer "):
            api_key = auth.split(" ", 1)[1].strip()

        if not api_key:
            return None

        # Expected format: ar_<prefix>_<secret>
        prefix = None
        if api_key.startswith("ar_") and api_key.count("_") >= 2:
            try:
                prefix = api_key.split("_", 2)[1]
            except Exception:
                prefix = None
        if not prefix:
            return None

        key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
        krs = await db.execute(
            select(ApiKey).where(
                ApiKey.key_prefix == prefix,
                ApiKey.revoked_at.is_(None)
            )
        )
        key_rec = krs.scalar_one_or_none()
        if not key_rec:
            return None
        if not hmac.compare_digest(key_rec.key_hash, key_hash):
            return None

        urs = await db.execute(select(User).where(User.id == key_rec.user_id))
        user = urs.scalar_one_or_none()
        if user:
            key_rec.last_used_at = datetime.utcnow()
            await db.commit()
        return user

    return await get_user_by_session(db, session_token)


async def get_anon_session(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db)
) -> AnonSession:
    """Get or create anonymous session"""
    anon_id = request.cookies.get(ANON_COOKIE)
    
    if not anon_id:
        anon_id = str(uuid.uuid4())
        response.set_cookie(
            ANON_COOKIE, 
            anon_id, 
            max_age=365*24*60*60,  # 1 year
            httponly=True,
            samesite="lax"
        )
    
    return await get_or_create_anon_session(db, anon_id)


async def require_admin(
    user: Optional[User] = Depends(get_current_user)
) -> User:
    """Require admin access"""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    if user.email not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _validate_viewer_settings_payload(body_bytes: bytes) -> dict:
    # Basic safety: limit payload size so we can't be spammed with huge JSON
    max_bytes = 256 * 1024
    if len(body_bytes) > max_bytes:
        raise HTTPException(status_code=413, detail="Viewer settings payload too large")
    try:
        data = json.loads(body_bytes.decode("utf-8") if isinstance(body_bytes, (bytes, bytearray)) else body_bytes)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Viewer settings must be a JSON object")
    return data


def _is_task_owner_or_admin(*, task, user: Optional[User], anon_session: Optional[AnonSession]) -> bool:
    if user and user.email in ADMIN_EMAILS:
        return True
    if user and task.owner_type == "user" and task.owner_id == user.email:
        return True
    if anon_session and task.owner_type == "anon" and task.owner_id == anon_session.anon_id:
        return True
    return False


# =============================================================================
# Authentication Endpoints
# =============================================================================
@app.get("/auth/login")
async def auth_login(request: Request):
    """Redirect to Google OAuth"""
    state = str(uuid.uuid4())
    # Could store state in session for verification
    auth_url = get_google_auth_url(state)
    return RedirectResponse(url=auth_url)


@app.get("/auth/callback")
async def auth_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Google OAuth callback"""
    if error:
        return RedirectResponse(url=f"/?error={error}")
    
    if not code:
        return RedirectResponse(url="/?error=no_code")
    
    # Exchange code for tokens
    tokens = await exchange_code_for_tokens(code)
    if not tokens:
        return RedirectResponse(url="/?error=token_exchange_failed")
    
    # Get user info
    access_token = tokens.get("access_token")
    user_info = await get_google_user_info(access_token)
    if not user_info:
        return RedirectResponse(url="/?error=user_info_failed")
    
    email = user_info.get("email")
    if not email:
        return RedirectResponse(url="/?error=no_email")
    
    # Get anon session for credit transfer
    anon_id = request.cookies.get(ANON_COOKIE)
    anon_session = None
    if anon_id:
        anon_session = await get_or_create_anon_session(db, anon_id)
    
    # Get or create user
    user = await get_or_create_user(
        db,
        email=email,
        name=user_info.get("name"),
        picture=user_info.get("picture"),
        anon_session=anon_session
    )
    
    # Create session
    session_token = await create_session(db, user.id)
    
    # Set session cookie
    redirect = RedirectResponse(url="/", status_code=302)
    redirect.set_cookie(
        SESSION_COOKIE,
        session_token,
        max_age=30*24*60*60,  # 30 days
        httponly=True,
        secure=True,
        samesite="lax"
    )
    
    return redirect


@app.get("/auth/logout")
async def auth_logout(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Logout user"""
    session_token = request.cookies.get(SESSION_COOKIE)
    if session_token:
        await delete_session(db, session_token)
    
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/auth/me", response_model=AuthStatusResponse)
async def auth_me(
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current auth status"""
    if user:
        return AuthStatusResponse(
            authenticated=True,
            user=UserInfo(
                id=user.id,
                email=user.email,
                name=user.name,
                picture=user.picture,
                balance_credits=user.balance_credits,
                total_tasks=user.total_tasks,
                is_admin=user.is_admin
            ),
            credits_remaining=user.balance_credits,
            login_required=False
        )
    
    # Anonymous user
    anon_session = await get_anon_session(request, response, db)
    remaining = get_remaining_credits_anon(anon_session)
    
    return AuthStatusResponse(
        authenticated=False,
        anon=AnonInfo(
            anon_id=anon_session.anon_id,
            free_used=anon_session.free_used,
            free_remaining=remaining
        ),
        credits_remaining=remaining,
        login_required=remaining <= 0
    )


# =============================================================================
# API Keys (User)
# =============================================================================
def _make_api_key() -> tuple[str, str, str]:
    """
    Returns (api_key_plain, prefix, sha256_hex_hash).
    api_key format: ar_<prefix>_<secret>
    """
    secret = secrets.token_urlsafe(32)
    prefix = secret[:8]
    api_key = f"ar_{prefix}_{secret}"
    key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    return api_key, prefix, key_hash


@app.get("/api/user/api-keys", response_model=ApiKeyListResponse)
async def api_list_api_keys(
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    rs = await db.execute(select(ApiKey).where(ApiKey.user_id == user.id).order_by(ApiKey.created_at.desc()))
    keys = rs.scalars().all()
    return ApiKeyListResponse(
        keys=[
            ApiKeyItem(
                id=k.id,
                key_prefix=k.key_prefix,
                created_at=k.created_at,
                revoked_at=k.revoked_at,
                last_used_at=k.last_used_at
            )
            for k in keys
        ]
    )


@app.post("/api/user/api-keys", response_model=ApiKeyCreateResponse)
async def api_create_api_key(
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    # Revoke existing active keys (keep history)
    rs = await db.execute(
        select(ApiKey).where(ApiKey.user_id == user.id, ApiKey.revoked_at.is_(None))
    )
    active = rs.scalars().all()
    now = datetime.utcnow()
    for k in active:
        k.revoked_at = now

    api_key, prefix, key_hash = _make_api_key()
    rec = ApiKey(user_id=user.id, key_prefix=prefix, key_hash=key_hash)
    db.add(rec)
    await db.commit()
    await db.refresh(rec)

    return ApiKeyCreateResponse(
        api_key=api_key,
        key=ApiKeyItem(
            id=rec.id,
            key_prefix=rec.key_prefix,
            created_at=rec.created_at,
            revoked_at=rec.revoked_at,
            last_used_at=rec.last_used_at
        )
    )


@app.post("/api/user/api-keys/{key_id}/revoke", status_code=200)
async def api_revoke_api_key(
    key_id: int,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    rs = await db.execute(select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user.id))
    rec = rs.scalar_one_or_none()
    if not rec:
        raise HTTPException(status_code=404, detail="API key not found")
    if rec.revoked_at is None:
        rec.revoked_at = datetime.utcnow()
        await db.commit()
    return {"ok": True, "key_id": key_id}


# =============================================================================
# Task Endpoints
# =============================================================================
@app.post("/api/task/create", response_model=TaskCreateResponse)
@limiter.limit(f"{RATE_LIMIT_TASKS_PER_MINUTE}/minute")
async def api_create_task(
    request: Request,
    response: Response,
    source: str = Form(...),
    input_url: Optional[str] = Form(None),
    type: str = Form("t_pose"),
    file: Optional[UploadFile] = File(None),
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new conversion task"""
    # Get anon session
    anon_session = await get_anon_session(request, response, db)
    
    # Check limits
    if user:
        if not can_create_task_user(user):
            raise HTTPException(
                status_code=402,
                detail="No credits remaining. Payment required."
            )
        owner_type = "user"
        owner_id = user.email
    else:
        if not can_create_task_anon(anon_session):
            raise HTTPException(
                status_code=401,
                detail="Free limit reached. Please sign in with Google to continue."
            )
        owner_type = "anon"
        owner_id = anon_session.anon_id
    
    # Handle file upload
    final_url = input_url
    if source == "upload" and file:
        # Save uploaded file
        upload_token = str(uuid.uuid4())
        upload_dir = os.path.join(UPLOAD_DIR, upload_token)
        os.makedirs(upload_dir, exist_ok=True)
        
        filename = file.filename or "model.glb"
        filepath = os.path.join(upload_dir, filename)
        
        # Check file size
        content = await file.read()
        if len(content) > MAX_UPLOAD_SIZE_MB * 1024 * 1024:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE_MB}MB."
            )
        
        with open(filepath, "wb") as f:
            f.write(content)
        
        # Generate public URL (URL-encode filename for special chars, spaces, cyrillic)
        from urllib.parse import quote
        final_url = f"{APP_URL}/u/{upload_token}/{quote(filename)}"
    
    if not final_url:
        raise HTTPException(status_code=400, detail="No input URL provided")
    
    # Create task
    task, error = await create_conversion_task(
        db, final_url, type, owner_type, owner_id
    )
    
    if error and not task:
        raise HTTPException(status_code=500, detail=error)
    
    # Deduct credit
    if user:
        await decrement_user_credits(db, user)
    else:
        await increment_anon_usage(db, anon_session.anon_id)
    
    # Try to dispatch immediately to a free worker (don't wait for background cycle)
    try:
        queue_status = await get_global_queue_status()
        free_worker = next(
            (w for w in queue_status.workers 
             if w.available and (w.total_active < w.max_concurrent) and (w.queue_size <= 0)),
            None
        )
        if free_worker:
            # Refresh task from DB and dispatch
            await db.refresh(task)
            if task.status == "created":
                await start_task_on_worker(db, task, free_worker.url)
                print(f"[Immediate Dispatch] Task {task.id} sent to {free_worker.url}")
    except Exception as e:
        # Don't fail task creation if immediate dispatch fails - background worker will pick it up
        print(f"[Immediate Dispatch] Failed for task {task.id}: {e}")
    
    return TaskCreateResponse(
        task_id=task.id,
        status=task.status,
        message=error
    )


# =============================================================================
# Gumroad (Payments)
# =============================================================================
@app.post("/api-gumroad")
async def api_gumroad_ping(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Gumroad Ping webhook (application/x-www-form-urlencoded).
    Credits are granted ONLY from this endpoint (idempotent via sale_id).
    """
    form = await request.form()
    sale_id = (form.get("sale_id") or "").strip()
    product_permalink = (form.get("product_permalink") or "").strip()
    gumroad_email = (form.get("email") or "").strip()
    refunded = str(form.get("refunded") or "").lower() == "true"
    test = str(form.get("test") or "").lower() == "true"
    # per spec: url_params[userid] binds purchase to the user; you confirmed it is user email
    user_identifier = (form.get("url_params[userid]") or "").strip()
    price = (form.get("price") or "").strip()
    quantity_raw = (form.get("quantity") or "").strip()
    try:
        quantity = int(quantity_raw) if quantity_raw else None
    except Exception:
        quantity = None

    if not sale_id:
        raise HTTPException(status_code=400, detail="Missing sale_id")

    # Idempotency: save the ping; duplicates must not re-credit
    sale = GumroadSale(
        sale_id=sale_id,
        user_email=user_identifier or None,
        product_permalink=product_permalink or None,
        gumroad_email=gumroad_email or None,
        price=price or None,
        quantity=quantity,
        refunded=refunded,
        test=test
    )
    db.add(sale)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return {"ok": True, "duplicate": True}

    # Validate userid
    if not user_identifier:
        print(f"[Gumroad] Missing url_params[userid] for sale_id={sale_id}")
        return {"ok": False, "error": "missing_userid"}

    # Ignore test/refunded
    if test or refunded:
        return {"ok": True, "credited": False}

    credits = GUMROAD_PRODUCT_CREDITS.get(product_permalink)
    if not credits:
        print(f"[Gumroad] Unknown product_permalink={product_permalink} sale_id={sale_id}")
        return {"ok": False, "error": "unknown_product"}

    # userid = user email
    urs = await db.execute(select(User).where(User.email == user_identifier))
    user = urs.scalar_one_or_none()
    if not user:
        print(f"[Gumroad] User not found for userid(email)={user_identifier} sale_id={sale_id}")
        return {"ok": False, "error": "user_not_found"}

    user.balance_credits += credits
    if gumroad_email:
        user.gumroad_email = gumroad_email
    await db.commit()

    return {"ok": True, "credited": True, "credits_added": credits, "user_email": user.email}


@app.get("/api/task/{task_id}", response_model=TaskStatusResponse)
async def api_get_task(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get task status and progress"""
    task = await get_task_by_id(db, task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Update progress if still processing, or check video for done tasks
    if task.status == "processing":
        task = await update_task_progress(db, task)
    elif task.status == "done" and not task.video_ready:
        # Check video availability for completed tasks
        task = await update_task_progress(db, task)
    
    # Find viewer HTML file (_100k .html)
    viewer_html_url = None
    if task.ready_urls:
        viewer_html_url = find_file_by_pattern(task.ready_urls, ".html", "100k")
    
    # Find quick download files
    quick_downloads = {}
    if task.ready_urls:
        # 3ds Max
        max_url = find_file_by_pattern(task.ready_urls, ".max", "100k")
        if max_url:
            quick_downloads["max"] = max_url
        
        # Maya
        maya_url = find_file_by_pattern(task.ready_urls, ".ma", "100k")
        if maya_url:
            quick_downloads["maya"] = maya_url
        
        # Cinema 4D
        c4d_url = find_file_by_pattern(task.ready_urls, ".c4d", "100k")
        if c4d_url:
            quick_downloads["cinema4d"] = c4d_url
        
        # Unity HDRP
        unity_hdrp_url = find_file_by_pattern(task.ready_urls, ".hdrp.unitypackage", "100k")
        if unity_hdrp_url:
            quick_downloads["unity_hdrp"] = unity_hdrp_url
        
        # Unity Standard (if different from HDRP)
        unity_url = find_file_by_pattern(task.ready_urls, ".unitypackage", "100k")
        if unity_url and unity_url != unity_hdrp_url:
            quick_downloads["unity"] = unity_url
        
        # Unreal Engine (FBX)
        unreal_url = find_file_by_pattern(task.ready_urls, ".fbx", "100k")
        if unreal_url:
            quick_downloads["unreal"] = unreal_url
    
    # prepared.glb ready if:
    # - _model_prepared.glb exists in ready_urls (worker uploaded it)
    # - OR for FBX tasks: fbx_glb_ready == True
    # - OR fallback: guid is not None and model uploaded to worker
    prepared_glb_ready = (
        any('_model_prepared.glb' in url.lower() for url in (task.ready_urls or [])) or
        task.fbx_glb_ready or 
        (task.guid is not None and task.status != 'created')
    )
    
    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        progress=task.progress,
        ready_count=task.ready_count,
        total_count=task.total_count,
        output_urls=task.output_urls,
        ready_urls=task.ready_urls,
        video_ready=task.video_ready,
        video_url=task.video_url,
        input_url=task.input_url,
        fbx_glb_output_url=task.fbx_glb_output_url,
        fbx_glb_model_name=task.fbx_glb_model_name,
        fbx_glb_ready=task.fbx_glb_ready,
        fbx_glb_error=task.fbx_glb_error,
        progress_page=task.progress_page,
        viewer_html_url=viewer_html_url,
        quick_downloads=quick_downloads if quick_downloads else None,
        prepared_glb_ready=prepared_glb_ready,
        error_message=task.error_message,
        created_at=task.created_at,
        updated_at=task.updated_at
    )


@app.get("/api/task/{task_id}/progress_log")
async def api_task_progress_log(
    task_id: str,
    full: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get task progress log from worker"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Need guid and worker_api to fetch log
    if not task.guid or not task.worker_api:
        return {"available": False, "state": task.status}
    
    # Construct log URL on worker
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    log_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_progress.txt"
    
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get(log_url, timeout=5.0)
            
            if resp.status_code == 404:
                # Log not created yet
                return {"available": False, "state": task.status}
            
            if resp.status_code != 200:
                return {"available": False, "state": task.status, "error": f"HTTP {resp.status_code}"}
            
            # Normalize line endings (Windows -> Unix)
            full_text = resp.text.replace('\r\n', '\n').replace('\r', '\n')
            lines = full_text.strip().split('\n') if full_text.strip() else []
            
            # Return last N lines as tail, full text if requested
            tail_count = 10
            tail_lines = lines[-tail_count:] if len(lines) > tail_count else lines
            
            return {
                "available": True,
                "state": task.status,
                "full_text": full_text if full else None,
                "tail_lines": tail_lines,
                "total_lines": len(lines),
                "truncated": len(lines) > tail_count and not full
            }
    except Exception as e:
        print(f"[Progress Log] Error fetching log for task {task_id}: {e}")
        return {"available": False, "state": task.status, "error": str(e)}


@app.get("/api/task/{task_id}/worker_files")
async def api_task_worker_files(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get list of files from worker for this task"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.guid or not task.worker_api:
        return {"available": False, "files": []}
    
    # Fetch from worker's model-files API
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    # worker_base is like http://x.x.x.x:port/converter/glb, API is at /api-converter-glb
    api_base = worker_base.replace('/converter/glb', '')
    files_url = f"{api_base}/api-converter-glb/model-files/{task.guid}"
    
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get(files_url, timeout=5.0)
            
            if resp.status_code != 200:
                return {"available": False, "files": [], "error": f"HTTP {resp.status_code}"}
            
            data = resp.json()
            
            # Flatten files from all folders
            all_files = []
            for folder_name, folder_data in data.get('folders', {}).items():
                for f in folder_data.get('files', []):
                    rel_path = f.get('rel_path', '')
                    all_files.append({
                        "name": f.get('name'),
                        "folder": folder_name,
                        "type": f.get('type'),
                        "size": f.get('size'),
                        "url": f"{worker_base}/converter/glb/{task.guid}/{rel_path}"
                    })
            
            return {
                "available": True,
                "exists": data.get('exists', False),
                "files": all_files,
                "totals": data.get('totals', {})
            }
    except Exception as e:
        print(f"[Worker Files] Error fetching files for task {task_id}: {e}")
        return {"available": False, "files": [], "error": str(e)}


@app.post("/api/task/{task_id}/retry", response_model=TaskCreateResponse)
async def api_retry_task(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Retry a stuck task (only if older than 2 hours and not done)"""
    from datetime import timedelta
    
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Check if task is owned by current user
    anon_session = await get_anon_session(request, response, db)
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
    )
    
    if not is_owner:
        raise HTTPException(status_code=403, detail="Not authorized to retry this task")
    
    # Check if task is eligible for retry
    if task.status == "done":
        raise HTTPException(status_code=400, detail="Task already completed")
    
    task_age = datetime.utcnow() - task.created_at
    if task_age < timedelta(hours=2):
        remaining = timedelta(hours=2) - task_age
        minutes = int(remaining.total_seconds() / 60)
        raise HTTPException(
            status_code=400, 
            detail=f"Task is too recent. Retry available in {minutes} minutes."
        )
    
    # Re-send to worker
    if not task.input_url:
        raise HTTPException(status_code=400, detail="No input URL to retry")
    
    # Create new task (don't deduct credits - it's a retry)
    new_task, error = await create_conversion_task(
        db, task.input_url, task.input_type or "t_pose", 
        task.owner_type, task.owner_id
    )
    
    if error and not new_task:
        raise HTTPException(status_code=500, detail=error)
    
    # Mark old task as error
    task.status = "error"
    task.error_message = f"Retried as task {new_task.id}"
    await db.commit()
    
    return TaskCreateResponse(
        task_id=new_task.id,
        status=new_task.status,
        message="Task resubmitted successfully"
    )


@app.post("/api/task/{task_id}/restart", response_model=TaskCreateResponse)
async def api_restart_task(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Restart task with the same task_id (available if older than 3 hours)"""
    from datetime import timedelta
    from workers import select_best_worker, send_task_to_worker

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Check ownership
    anon_session = await get_anon_session(request, response, db)
    is_admin = bool(user and user.email in ADMIN_EMAILS)
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
    )
    if not (is_owner or is_admin):
        raise HTTPException(status_code=403, detail="Not authorized to restart this task")

    # Age gate: 3 hours
    task_age = datetime.utcnow() - task.created_at
    min_age = timedelta(minutes=1) if task.status == "error" else timedelta(hours=3)
    if task_age < min_age:
        remaining = min_age - task_age
        minutes = int(remaining.total_seconds() / 60)
        raise HTTPException(
            status_code=400,
            detail=f"Task is too recent. Restart available in {minutes} minutes."
        )

    if not task.input_url:
        raise HTTPException(status_code=400, detail="No input URL to restart")

    # Reset fields (keep id/owner/input)
    task.worker_api = None
    task.worker_task_id = None
    task.progress_page = None
    task.guid = None
    task.output_urls = []
    task.ready_urls = []
    task.ready_count = 0
    task.total_count = 0
    task.status = "created"
    task.error_message = None
    task.video_url = None
    task.video_ready = False

    # Reset FBX->GLB state
    task.fbx_glb_output_url = None
    task.fbx_glb_model_name = None
    task.fbx_glb_ready = False
    task.fbx_glb_error = None

    await db.commit()
    await db.refresh(task)

    # Start pipeline for the same task_id without blocking on FBX pre-conversion.
    worker_url = await select_best_worker()
    if not worker_url:
        raise HTTPException(status_code=500, detail="No workers available")

    task.worker_api = worker_url
    task.status = "processing"

    # Send directly to worker - workers handle GLB/FBX/OBJ natively
    result = await send_task_to_worker(worker_url, task.input_url, task.input_type or "t_pose")
    if not result.success:
        task.status = "error"
        task.error_message = result.error
    else:
        task.worker_task_id = result.task_id
        task.progress_page = result.progress_page
        task.guid = result.guid
        task.output_urls = result.output_urls
        task.total_count = len(result.output_urls)
        task.status = "processing"
    await db.commit()
    await db.refresh(task)

    return TaskCreateResponse(
        task_id=task.id,
        status=task.status,
        message="Task restarted successfully"
    )


# =============================================================================
# File Purchase Endpoints
# =============================================================================
@app.get("/api/task/{task_id}/purchases", response_model=PurchaseStateResponse)
async def api_get_purchase_state(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get purchase state for a task"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = await get_anon_session(request, response, db)
    
    # Check if user is owner
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
    )
    
    # Check if user is admin
    is_admin = bool(user and user.email in ADMIN_EMAILS)

    # Only admin or owner has free access to all files
    if is_admin or is_owner:
        return PurchaseStateResponse(
            purchased_all=True,
            purchased_files=[],
            is_owner=True,
            login_required=False,
            user_credits=user.balance_credits if user else 0
        )
    
    # For non-owners, check purchases
    if not user:
        return PurchaseStateResponse(
            purchased_all=False,
            purchased_files=[],
            is_owner=False,
            login_required=True,
            user_credits=0
        )
    
    # Get user's purchases for this task
    result = await db.execute(
        select(TaskFilePurchase).where(
            TaskFilePurchase.task_id == task_id,
            TaskFilePurchase.user_email == user.email
        )
    )
    purchases = result.scalars().all()
    
    # Check if "all files" was purchased (file_index is NULL)
    purchased_all = any(p.file_index is None for p in purchases)
    purchased_indices = [p.file_index for p in purchases if p.file_index is not None]
    
    return PurchaseStateResponse(
        purchased_all=purchased_all,
        purchased_files=purchased_indices,
        is_owner=False,
        login_required=False,
        user_credits=user.balance_credits
    )


@app.post("/api/task/{task_id}/purchases", response_model=PurchaseResponse)
async def api_purchase_files(
    task_id: str,
    purchase_req: PurchaseRequest,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Purchase files for a task"""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = await get_anon_session(request, response, db)
    
    # Check if user is owner - they already have access
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
    )
    
    if is_owner:
        return PurchaseResponse(
            success=True,
            purchased_files=[],
            purchased_all=True,
            credits_remaining=user.balance_credits
        )
    
    # Check existing purchases
    result = await db.execute(
        select(TaskFilePurchase).where(
            TaskFilePurchase.task_id == task_id,
            TaskFilePurchase.user_email == user.email
        )
    )
    existing = result.scalars().all()
    already_all = any(p.file_index is None for p in existing)
    already_indices = {p.file_index for p in existing if p.file_index is not None}
    
    if already_all:
        return PurchaseResponse(
            success=True,
            purchased_files=list(already_indices),
            purchased_all=True,
            credits_remaining=user.balance_credits
        )
    
    # Handle "buy all" request (1 credit for full task access)
    if purchase_req.all:
        cost = 1
        if user.balance_credits < cost:
            raise HTTPException(status_code=402, detail="Insufficient credits")
        
        # Deduct credits
        user.balance_credits -= cost
        
        # Create purchase record for "all files"
        purchase = TaskFilePurchase(
            task_id=task_id,
            user_email=user.email,
            file_index=None,  # NULL means all files
            credits_spent=cost
        )
        db.add(purchase)
        await db.commit()
        
        return PurchaseResponse(
            success=True,
            purchased_files=list(already_indices),
            purchased_all=True,
            credits_remaining=user.balance_credits
        )
    
    # Handle individual file purchase
    if purchase_req.file_indices:
        new_indices = [i for i in purchase_req.file_indices if i not in already_indices]
        
        if not new_indices:
            return PurchaseResponse(
                success=True,
                purchased_files=list(already_indices),
                purchased_all=False,
                credits_remaining=user.balance_credits
            )
        
        cost = len(new_indices)  # 1 credit per file
        if user.balance_credits < cost:
            raise HTTPException(status_code=402, detail="Insufficient credits")
        
        # Deduct credits
        user.balance_credits -= cost
        
        # Create purchase records
        for idx in new_indices:
            purchase = TaskFilePurchase(
                task_id=task_id,
                user_email=user.email,
                file_index=idx,
                credits_spent=1
            )
            db.add(purchase)
        
        await db.commit()
        
        return PurchaseResponse(
            success=True,
            purchased_files=list(already_indices | set(new_indices)),
            purchased_all=False,
            credits_remaining=user.balance_credits
        )
    
    raise HTTPException(status_code=400, detail="Must specify file_indices or all=true")


@app.get("/api/history", response_model=TaskHistoryResponse)
async def api_get_history(
    request: Request,
    response: Response,
    page: int = 1,
    per_page: int = 10,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get task history for current user"""
    if user:
        owner_type = "user"
        owner_id = user.email
    else:
        anon_session = await get_anon_session(request, response, db)
        owner_type = "anon"
        owner_id = anon_session.anon_id
    
    tasks, total = await get_user_tasks(db, owner_type, owner_id, page, per_page)
    
    return TaskHistoryResponse(
        tasks=[
            TaskHistoryItem(
                task_id=t.id,
                status=t.status,
                progress=t.progress,
                created_at=t.created_at,
                input_url=t.input_url,
                video_ready=t.video_ready
            )
            for t in tasks
        ],
        total=total,
        page=page,
        per_page=per_page
    )


@app.get("/api/gallery", response_model=GalleryResponse)
async def api_get_gallery(
    request: Request,
    page: int = 1,
    per_page: int = 12,
    sort: str = "likes",
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get public gallery of completed tasks with videos"""
    from sqlalchemy import func, desc
    from database import Task
    
    # Get current user email for liked_by_me check
    user_email = user.email if user else None
    
    # Count total completed tasks with video
    count_result = await db.execute(
        select(func.count(Task.id)).where(
            Task.status == "done",
            Task.video_ready == True
        )
    )
    total = count_result.scalar() or 0
    
    # Get task IDs with like counts
    offset = (page - 1) * per_page
    
    if sort == "likes":
        # Sort by like count (descending), then by date
        result = await db.execute(
            select(
                Task,
                func.count(TaskLike.id).label('like_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .where(Task.status == "done", Task.video_ready == True)
            .group_by(Task.id)
            .order_by(desc('like_count'), desc(Task.created_at))
            .offset(offset)
            .limit(per_page)
        )
    else:
        # Sort by date (newest first)
        result = await db.execute(
            select(
                Task,
                func.count(TaskLike.id).label('like_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .where(Task.status == "done", Task.video_ready == True)
            .group_by(Task.id)
            .order_by(desc(Task.created_at))
            .offset(offset)
            .limit(per_page)
        )
    
    rows = result.all()
    
    # Get user's likes if logged in
    user_likes = set()
    if user_email:
        task_ids = [row[0].id for row in rows]
        if task_ids:
            likes_result = await db.execute(
                select(TaskLike.task_id).where(
                    TaskLike.user_email == user_email,
                    TaskLike.task_id.in_(task_ids)
                )
            )
            user_likes = set(r[0] for r in likes_result.all())
    
    items = [
        GalleryItem(
            task_id=t.id,
            video_url=f"/api/video/{t.id}",
            thumbnail_url=f"/api/thumb/{t.id}",
            created_at=t.created_at,
            time_ago=format_time_ago(t.created_at),
            like_count=like_count,
            liked_by_me=t.id in user_likes
        )
        for t, like_count in rows
    ]
    
    has_more = (page * per_page) < total
    
    return GalleryResponse(
        items=items,
        total=total,
        page=page,
        per_page=per_page,
        has_more=has_more
    )


@app.post("/api/gallery/{task_id}/like", response_model=LikeResponse)
async def api_toggle_like(
    task_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Toggle like on a task (requires authentication)"""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Check if task exists
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Check if already liked
    existing = await db.execute(
        select(TaskLike).where(
            TaskLike.task_id == task_id,
            TaskLike.user_email == user.email
        )
    )
    existing_like = existing.scalar_one_or_none()
    
    if existing_like:
        # Unlike
        await db.delete(existing_like)
        await db.commit()
        liked_by_me = False
    else:
        # Like
        new_like = TaskLike(task_id=task_id, user_email=user.email)
        db.add(new_like)
        await db.commit()
        liked_by_me = True
    
    # Get updated like count
    from sqlalchemy import func
    count_result = await db.execute(
        select(func.count(TaskLike.id)).where(TaskLike.task_id == task_id)
    )
    like_count = count_result.scalar() or 0
    
    return LikeResponse(
        task_id=task_id,
        like_count=like_count,
        liked_by_me=liked_by_me
    )


# =============================================================================
# Queue Status Endpoint
# =============================================================================
@app.get("/api/queue/status", response_model=QueueStatusResponse)
async def api_queue_status():
    """Get global queue status across all workers"""
    status = await get_global_queue_status()
    
    return QueueStatusResponse(
        workers=[
            WorkerQueueInfo(
                port=w.port,
                available=w.available,
                active=w.total_active,
                pending=w.total_pending,
                queue_size=w.queue_size,
                error=w.error
            )
            for w in status.workers
        ],
        total_active=status.total_active,
        total_pending=status.total_pending,
        total_queue=status.total_queue,
        available_workers=status.available_workers,
        total_workers=status.total_workers,
        estimated_wait_seconds=status.estimated_wait_seconds,
        estimated_wait_formatted=status.estimated_wait_formatted
    )


# =============================================================================
# Admin Endpoints
# =============================================================================
@app.get("/api/admin/stats", response_model=AdminStatsResponse)
async def api_admin_stats(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get admin dashboard stats (admin only)"""
    from sqlalchemy import func
    from database import Task
    
    # Count total users
    users_count = await db.execute(select(func.count(User.id)))
    total_users = users_count.scalar() or 0
    
    # Count total credits across all users
    credits_sum = await db.execute(select(func.sum(User.balance_credits)))
    total_credits = credits_sum.scalar() or 0
    
    # Count tasks by status
    tasks_by_status = {}
    for status in ["created", "processing", "done", "error"]:
        count_result = await db.execute(
            select(func.count(Task.id)).where(Task.status == status)
        )
        tasks_by_status[status] = count_result.scalar() or 0
    
    total_tasks = sum(tasks_by_status.values())
    
    return AdminStatsResponse(
        total_users=total_users,
        total_tasks=total_tasks,
        tasks_by_status=tasks_by_status,
        total_credits=total_credits
    )


@app.get("/api/admin/users", response_model=AdminUserListResponse)
async def api_admin_users(
    query: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get list of users (admin only)"""
    users, total = await get_all_users(
        db, search=query, sort_by=sort_by, 
        sort_desc=sort_desc, page=page, per_page=per_page
    )
    
    return AdminUserListResponse(
        users=[
            AdminUserListItem(
                id=u.id,
                email=u.email,
                name=u.name,
                balance_credits=u.balance_credits,
                total_tasks=u.total_tasks,
                created_at=u.created_at,
                last_login_at=u.last_login_at
            )
            for u in users
        ],
        total=total,
        page=page,
        per_page=per_page
    )


@app.post("/api/admin/user/{user_id}/balance", response_model=AdminBalanceResponse)
async def api_admin_update_balance(
    user_id: int,
    data: AdminBalanceUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update user balance (admin only)"""
    user, old_balance, new_balance = await update_user_balance(
        db, user_id, delta=data.delta, set_to=data.set_to
    )
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return AdminBalanceResponse(
        user_id=user.id,
        email=user.email,
        old_balance=old_balance,
        new_balance=new_balance
    )


@app.get("/api/admin/user/{user_id}/tasks", response_model=AdminUserTasksResponse)
async def api_admin_user_tasks(
    user_id: int,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get tasks for a specific user (admin only)"""
    # Get user by ID
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get user tasks
    tasks, total = await get_user_tasks(
        db, owner_type="user", owner_id=user.email, page=page, per_page=per_page
    )
    
    return AdminUserTasksResponse(
        tasks=[
            AdminUserTaskItem(
                task_id=task.id,
                status=task.status,
                progress=task.progress,
                ready_count=task.ready_count,
                total_count=task.total_count,
                created_at=task.created_at,
                updated_at=task.updated_at,
                input_url=task.input_url
            )
            for task in tasks
        ],
        total=total,
        page=page,
        per_page=per_page
    )


@app.get("/api/admin/tasks", response_model=AdminTaskListResponse)
async def api_admin_all_tasks(
    status: Optional[str] = None,
    query: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get all tasks with filtering, sorting, and pagination (admin only)"""
    from sqlalchemy import func, or_, desc
    from database import Task
    
    # Base query
    base_query = select(Task)
    
    # Filter by status
    if status and status in ["created", "processing", "done", "error"]:
        base_query = base_query.where(Task.status == status)
    
    # Search by task_id or owner_id
    if query:
        search_pattern = f"%{query}%"
        base_query = base_query.where(
            or_(
                Task.id.ilike(search_pattern),
                Task.owner_id.ilike(search_pattern)
            )
        )
    
    # Count total
    count_query = select(func.count()).select_from(base_query.subquery())
    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0
    
    # Sort
    sort_column = getattr(Task, sort_by, Task.created_at)
    if sort_desc:
        base_query = base_query.order_by(desc(sort_column))
    else:
        base_query = base_query.order_by(sort_column)
    
    # Paginate
    offset = (page - 1) * per_page
    result = await db.execute(base_query.offset(offset).limit(per_page))
    tasks = result.scalars().all()
    
    return AdminTaskListResponse(
        tasks=[
            AdminTaskListItem(
                task_id=t.id,
                owner_type=t.owner_type,
                owner_id=t.owner_id,
                status=t.status,
                progress=t.progress,
                ready_count=t.ready_count,
                total_count=t.total_count,
                input_url=t.input_url,
                worker_api=t.worker_api,
                video_ready=t.video_ready,
                created_at=t.created_at,
                updated_at=t.updated_at
            )
            for t in tasks
        ],
        total=total,
        page=page,
        per_page=per_page
    )


@app.delete("/api/admin/task/{task_id}")
async def api_admin_delete_task(
    task_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Delete a task by id (admin only)."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    await db.delete(task)
    await db.commit()
    return {"ok": True, "task_id": task_id}


@app.post("/api/admin/service/restart")
async def api_admin_restart_service(
    request: Request,
    admin: User = Depends(require_admin),
):
    """
    Restart backend service (admin only).
    Implementation: restart background worker in-process, then terminate the process.
    With systemd Restart=always, the service will come back up automatically.
    """
    import os
    import signal

    # Best-effort: restart background worker now (useful if process doesn't restart immediately)
    try:
        await restart_background_worker(request.app)
    except Exception as e:
        print(f"[Admin] Failed to restart background worker: {e}")

    async def _terminate_soon():
        await asyncio.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.create_task(_terminate_soon())
    return {"ok": True, "message": "Service restart scheduled"}


@app.delete("/api/admin/tasks/all")
async def api_admin_delete_all_tasks(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete ALL tasks from database and restart service (admin only).
    DANGEROUS: This action cannot be undone!
    """
    import os
    import signal
    from database import Task
    from sqlalchemy import delete
    
    # Count tasks before deletion
    from sqlalchemy import func
    count_result = await db.execute(select(func.count(Task.id)))
    total_deleted = count_result.scalar() or 0
    
    # Delete all tasks
    await db.execute(delete(Task))
    await db.commit()
    
    print(f"[Admin] Deleted ALL {total_deleted} tasks by {admin.email}")
    
    # Schedule service restart
    async def _terminate_soon():
        await asyncio.sleep(1.0)
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.create_task(_terminate_soon())
    
    return {"ok": True, "deleted_count": total_deleted, "message": "All tasks deleted. Service restarting..."}


# =============================================================================
# Upload Serving
# =============================================================================
@app.get("/u/{token}/{filename}")
async def serve_upload(token: str, filename: str):
    """Serve uploaded files"""
    filepath = os.path.join(UPLOAD_DIR, token, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath)


# =============================================================================
# File & Video Proxy (to avoid Mixed Content issues)
# =============================================================================
import httpx
from fastapi.responses import StreamingResponse

@app.get("/api/video/{task_id}")
async def proxy_video(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy video from worker to serve over HTTPS"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.video_url:
        raise HTTPException(status_code=404, detail="Video not available")
    
    async def stream_video():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", task.video_url, timeout=60.0) as response:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    return StreamingResponse(
        stream_video(),
        media_type="video/mp4",
        headers={
            "Content-Disposition": f"inline; filename={task_id}_video.mp4",
            "Cache-Control": "public, max-age=86400"
        }
    )


@app.get("/api/file/{task_id}/{file_index}")
async def proxy_file(
    task_id: str,
    file_index: int,
    db: AsyncSession = Depends(get_db)
):
    """Proxy file from worker to serve over HTTPS"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    ready_urls = task.ready_urls
    if file_index < 0 or file_index >= len(ready_urls):
        raise HTTPException(status_code=404, detail="File not found")
    
    file_url = ready_urls[file_index]
    filename = file_url.split("/")[-1]
    
    # Determine content type
    ext = filename.split(".")[-1].lower()
    content_types = {
        "glb": "model/gltf-binary",
        "fbx": "application/octet-stream",
        "blend": "application/x-blender",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    async def stream_file():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", file_url, timeout=120.0) as response:
                if response.status_code != 200:
                    return
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Cache-Control": "public, max-age=86400"
        }
    )


@app.get("/api/file/{task_id}/download/{filename:path}")
async def proxy_file_by_name(
    task_id: str,
    filename: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy file from worker by filename to serve over HTTPS"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Search in output_urls first, then ready_urls
    file_url = None
    all_urls = (task.output_urls or []) + (task.ready_urls or [])
    
    for url in all_urls:
        url_clean = url.strip()
        if url_clean.endswith(filename) or filename in url_clean.split('/')[-1]:
            file_url = url_clean
            break
    
    if not file_url:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Clean filename for download (remove GUID)
    clean_filename = filename
    import re
    clean_filename = re.sub(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_', '', filename, flags=re.IGNORECASE)
    
    # Determine content type
    ext = clean_filename.split(".")[-1].lower()
    content_types = {
        "glb": "model/gltf-binary",
        "gltf": "model/gltf+json",
        "fbx": "application/octet-stream",
        "blend": "application/x-blender",
        "unitypackage": "application/octet-stream",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    async def stream_file():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", file_url, timeout=120.0) as response:
                if response.status_code != 200:
                    return
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers={
            "Content-Disposition": f"attachment; filename={clean_filename}",
            "Cache-Control": "public, max-age=86400"
        }
    )


@app.get("/api/task/{task_id}/viewer")
async def api_proxy_viewer(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy 3D viewer HTML file from worker to avoid mixed content issues"""
    import re
    from urllib.parse import urlparse, urljoin
    
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Find viewer HTML file
    viewer_url = None
    if task.ready_urls:
        viewer_url = find_file_by_pattern(task.ready_urls, ".html", "100k")
    
    if not viewer_url:
        raise HTTPException(status_code=404, detail="Viewer file not found")
    
    # Get base URL from viewer_url (e.g., http://5.129.157.224:5132)
    parsed = urlparse(viewer_url)
    worker_base = f"{parsed.scheme}://{parsed.netloc}"
    
    # Proxy the HTML file
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(viewer_url, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            
            # Get HTML content
            html_content = response.text
            
            # Replace relative paths with proxy URLs
            # Extract GUID from viewer URL
            guid_match = re.search(r'/converter/glb/([a-f0-9\-]+)/', viewer_url)
            if guid_match:
                guid = guid_match.group(1)
            else:
                guid = task.guid or task_id
            
            converter_base = f"/converter/glb/{guid}"
            from urllib.parse import quote
            
            # Strategy: Replace paths in two passes to avoid recursion
            # First pass: Find and replace paths that are NOT inside viewer-resource URLs
            # Use negative lookahead to ensure we don't touch already replaced paths
            
            # Replace absolute paths: "/converter/glb/..." 
            # Only match if NOT followed by viewer-resource in the same attribute/string
            def replace_absolute_path(match):
                quote_char = match.group(1)
                path = match.group(2)
                # Double check - if path somehow contains our proxy, skip
                if '/api/task/' in path or 'viewer-resource' in path:
                    return match.group(0)
                encoded_path = quote(path, safe='/')
                return f'{quote_char}/api/task/{task_id}/viewer-resource?path={encoded_path}{quote_char}'
            
            # Use negative lookahead to avoid matching inside already replaced URLs
            html_content = re.sub(
                r'(["\'])(/converter/glb/[^"\']+)(["\'])(?![^"\']*viewer-resource)',
                replace_absolute_path,
                html_content
            )
            
            # Replace relative paths: "./file.mview", "../file.mview"
            def replace_relative_path(match):
                quote_char = match.group(1)
                rel_path = match.group(2).lstrip('./')
                if '/api/task/' in rel_path or 'viewer-resource' in rel_path:
                    return match.group(0)
                if not rel_path.startswith(guid):
                    full_path = f"{converter_base}/{guid}_100k/{rel_path}"
                else:
                    full_path = f"{converter_base}/{rel_path}"
                encoded_path = quote(full_path, safe='/')
                return f'{quote_char}/api/task/{task_id}/viewer-resource?path={encoded_path}{quote_char}'
            
            html_content = re.sub(
                r'(["\'])(\.?\.?/[^"\']+\.(mview|json|png|jpg|jpeg|webp)[^"\']*)(["\'])(?![^"\']*viewer-resource)',
                replace_relative_path,
                html_content
            )
            
            # Handle bare filenames (e.g., "model.mview")
            def replace_bare_filename(match):
                quote_char = match.group(1)
                filename = match.group(2)
                closing_quote = match.group(3)
                if '/api/task/' in filename or 'viewer-resource' in filename:
                    return match.group(0)
                full_path = f"{converter_base}/{guid}_100k/{filename}"
                encoded_path = quote(full_path, safe='/')
                return f'{quote_char}/api/task/{task_id}/viewer-resource?path={encoded_path}{closing_quote}'
            
            html_content = re.sub(
                r'(["\'])([^/"\']+\.(mview|json|png|jpg|jpeg|webp))(["\'])(?![^"\']*viewer-resource)',
                replace_bare_filename,
                html_content
            )
            
            # Handle paths in JavaScript (src=, href=, etc.) - be more careful
            def replace_js_path(match):
                attr = match.group(1)
                path = match.group(2)
                if '/api/task/' in path or 'viewer-resource' in path:
                    return match.group(0)
                encoded_path = quote(path, safe='/')
                return f'{attr}="/api/task/{task_id}/viewer-resource?path={encoded_path}"'
            
            html_content = re.sub(
                r'(src|href|url|load)\s*[:=]\s*["\']?(/converter/glb/[^"\'\s\)]+)["\']?(?![^"\']*viewer-resource)',
                replace_js_path,
                html_content
            )
            
            # Return as HTML with proper headers
            return Response(
                content=html_content,
                media_type="text/html",
                headers={
                    "X-Frame-Options": "SAMEORIGIN",
                    "Cache-Control": "public, max-age=3600"
                }
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Failed to fetch viewer: {str(e)}")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail="Viewer not available")


@app.get("/api/task/{task_id}/viewer-resource")
async def api_proxy_viewer_resource(
    task_id: str,
    path: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy resources (like .mview files) for the 3D viewer"""
    from urllib.parse import unquote
    
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Get worker base URL
    if not task.worker_api:
        raise HTTPException(status_code=404, detail="Worker info not found")
    
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    
    # Decode path (in case it was double-encoded or has quotes)
    path = unquote(path)
    # Remove any leading/trailing quotes that might have been included
    path = path.strip("'\"")
    
    # Construct full URL
    resource_url = f"{worker_base}{path}"
    
    # Proxy the resource
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(resource_url, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            
            # Determine content type
            content_type = response.headers.get("content-type", "application/octet-stream")
            if not content_type or content_type == "application/octet-stream":
                if path.endswith(".mview"):
                    content_type = "application/octet-stream"
                elif path.endswith(".png"):
                    content_type = "image/png"
                elif path.endswith(".jpg") or path.endswith(".jpeg"):
                    content_type = "image/jpeg"
                elif path.endswith(".json"):
                    content_type = "application/json"
            
            return Response(
                content=response.content,
                media_type=content_type,
                headers={
                    "Cache-Control": "public, max-age=3600",
                    "Access-Control-Allow-Origin": "*"
                }
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Failed to fetch resource: {str(e)}")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail="Resource not available")


# =============================================================================
# Model File Proxy Endpoints (for 3D viewer)
# =============================================================================
def _find_file_in_ready_urls(ready_urls: list, pattern: str, extension: str = None) -> Optional[str]:
    """Find a file in ready_urls matching the pattern (case-insensitive).
    
    Args:
        ready_urls: List of URLs to search
        pattern: Pattern to match (case-insensitive)
        extension: Optional extension filter (e.g., ".glb") - must match exactly
    
    Returns:
        First matching URL (trimmed) or None
    """
    pattern_lower = pattern.lower()
    for url in ready_urls:
        url_clean = url.strip()  # Remove trailing whitespace
        if pattern_lower in url_clean.lower():
            if extension:
                if url_clean.lower().endswith(extension.lower()):
                    return url_clean
            else:
                return url_clean
    return None


async def _proxy_model_file(url: str, filename: str) -> StreamingResponse:
    """Proxy a model file from worker with streaming and Content-Length.
    
    Uses streaming to avoid timeout on slow workers, but forwards Content-Length
    from upstream so clients know the file size.
    """
    # Determine content type
    ext = filename.split(".")[-1].lower()
    content_types = {
        "glb": "model/gltf-binary",
        "fbx": "application/octet-stream",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    # Create client that lives through the response
    client = httpx.AsyncClient()
    
    async def stream_file():
        try:
            async with client.stream("GET", url, timeout=300.0, follow_redirects=True) as response:
                if response.status_code != 200:
                    return
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
        finally:
            await client.aclose()
    
    # Get Content-Length from HEAD request (fast)
    content_length = None
    try:
        async with httpx.AsyncClient() as head_client:
            head_resp = await head_client.head(url, timeout=10.0, follow_redirects=True)
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get("content-length")
    except Exception:
        pass  # Content-Length is optional, continue without it
    
    headers = {
        "Content-Disposition": f"attachment; filename={filename}",
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*"
    }
    if content_length:
        headers["Content-Length"] = content_length

    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers=headers
    )


@app.get("/api/task/{task_id}/model.glb")
async def api_proxy_model_glb(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy the main model GLB file from worker"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.guid or not task.worker_api:
        raise HTTPException(status_code=404, detail="Model not available yet")
    
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    model_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.glb"
    
    return await _proxy_model_file(model_url, f"{task_id}_model.glb")


@app.get("/api/task/{task_id}/animations.glb")
async def api_proxy_animations_glb(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy animations GLB file from worker (searches ready_urls or fallback to model.glb)"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.guid or not task.worker_api:
        raise HTTPException(status_code=404, detail="Model not available yet")
    
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    
    # Try to find animations GLB in ready_urls (must end with .glb, not .blend)
    animations_url = _find_file_in_ready_urls(task.ready_urls, "_all_animations", ".glb")
    if animations_url:
        return await _proxy_model_file(animations_url, f"{task_id}_animations.glb")
    
    # Fallback: try main model GLB
    model_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.glb"
    return await _proxy_model_file(model_url, f"{task_id}_animations.glb")


@app.get("/api/task/{task_id}/animations.fbx")
async def api_proxy_animations_fbx(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy animations FBX file from worker (searches ready_urls)"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.guid or not task.worker_api:
        raise HTTPException(status_code=404, detail="Model not available yet")
    
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    
    # Try to find animations FBX in ready_urls (_all_animations_unity.fbx)
    animations_url = _find_file_in_ready_urls(task.ready_urls, "_all_animations_unity.fbx")
    if animations_url:
        return await _proxy_model_file(animations_url, f"{task_id}_animations.fbx")
    
    # Try alternative pattern (_all_animations.fbx)
    animations_url = _find_file_in_ready_urls(task.ready_urls, "_all_animations.fbx")
    if animations_url:
        return await _proxy_model_file(animations_url, f"{task_id}_animations.fbx")
    
    # Try to construct URL based on GUID pattern
    fbx_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_100k/{task.guid}_all_animations_unity.fbx"
    return await _proxy_model_file(fbx_url, f"{task_id}_animations.fbx")


@app.get("/api/task/{task_id}/prepared.glb")
async def api_proxy_prepared_glb(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy prepared GLB file (_model_prepared.glb from worker or fallbacks)"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    from workers import get_worker_base_url
    
    # 1. Try to find _model_prepared.glb in ready_urls (best option for preview)
    prepared_url = _find_file_in_ready_urls(task.ready_urls or [], "_model_prepared.glb")
    if prepared_url:
        return await _proxy_model_file(prepared_url, f"{task_id}_prepared.glb")
    
    # 2. Fallback: try direct URL to _model_prepared.glb on worker
    if task.guid and task.worker_api:
        worker_base = get_worker_base_url(task.worker_api)
        direct_prepared_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_model_prepared.glb"
        result = await _proxy_model_file(direct_prepared_url, f"{task_id}_prepared.glb")
        if result.status_code == 200:
            return result
    
    # 3. For FBX tasks, use fbx_glb_output_url
    if task.fbx_glb_output_url and task.fbx_glb_ready:
        return await _proxy_model_file(task.fbx_glb_output_url, f"{task_id}_prepared.glb")
    
    # 4. Final fallback: original model GLB (uploaded model before processing)
    if not task.guid or not task.worker_api:
        raise HTTPException(status_code=404, detail="Model not available yet")
    
    worker_base = get_worker_base_url(task.worker_api)
    model_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.glb"
    
    return await _proxy_model_file(model_url, f"{task_id}_prepared.glb")


@app.get("/api/thumb/{task_id}")
async def api_proxy_thumb(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy video poster/thumbnail image from worker"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Try to find _video_poster.jpg in ready_urls
    poster_url = _find_file_in_ready_urls(task.ready_urls or [], "_video_poster.jpg")
    if not poster_url:
        # Try alternative pattern
        poster_url = _find_file_in_ready_urls(task.ready_urls or [], "_poster.jpg")
    
    if not poster_url:
        raise HTTPException(status_code=404, detail="Thumbnail not available")
    
    # Download and return the image (not streaming - more compatible with HTTP/2)
    async with httpx.AsyncClient() as client:
        response = await client.get(poster_url, timeout=30.0, follow_redirects=True)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="Thumbnail not available")
        
        return Response(
            content=response.content,
            media_type="image/jpeg",
            headers={
                "Cache-Control": "public, max-age=86400",
                "Access-Control-Allow-Origin": "*"
            }
        )


# =============================================================================
# Free3D Model Search Proxy
# =============================================================================

FREE3D_BASE_URL = "https://free3d.online"

@app.get("/api/free3d/search")
async def api_free3d_search(
    q: str = "",
    query: str = "",
    topK: int = 20,
    type: Optional[int] = None,
    mode: str = "clip"
):
    """Proxy search requests to free3d.online API"""
    search_query = q or query
    if not search_query:
        return {"results": []}
    
    params = {
        "q": search_query,
        "topK": min(topK, 100)
    }
    if type is not None:
        params["type"] = type
    if mode:
        params["mode"] = mode
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{FREE3D_BASE_URL}/api-embeddings/", params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        print(f"[Free3D] Search error: {e}")
        return {"results": [], "error": str(e)}


@app.get("/api/free3d/image/{guid}/{filename}")
async def api_free3d_image(guid: str, filename: str):
    """Proxy image files from free3d.online"""
    url = f"{FREE3D_BASE_URL}/data/{guid}/{filename}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            
            content_type = resp.headers.get("content-type", "image/jpeg")
            return Response(
                content=resp.content,
                media_type=content_type,
                headers={
                    "Cache-Control": "public, max-age=86400",
                    "Access-Control-Allow-Origin": "*"
                }
            )
    except Exception as e:
        print(f"[Free3D] Image proxy error: {e}")
        raise HTTPException(status_code=404, detail="Image not found")


@app.get("/api/free3d/glb/{guid}/{filename}")
async def api_free3d_glb(guid: str, filename: str):
    """Proxy GLB model files from free3d.online"""
    url = f"{FREE3D_BASE_URL}/data/{guid}/{filename}"
    
    async def stream_file():
        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream("GET", url) as resp:
                if resp.status_code != 200:
                    return
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    return StreamingResponse(
        stream_file(),
        media_type="model/gltf-binary",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "public, max-age=86400",
            "Access-Control-Allow-Origin": "*"
        }
    )


# =============================================================================
# Telegram Web App
# =============================================================================
def validate_telegram_init_data(init_data: str) -> Optional[dict]:
    """Validate Telegram Web App init data and return user info if valid"""
    if not TELEGRAM_BOT_TOKEN:
        return None
    
    from urllib.parse import parse_qs, unquote
    import json
    
    try:
        # Parse init_data
        parsed = dict(parse_qs(init_data, keep_blank_values=True))
        parsed = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        
        # Get hash from data
        received_hash = parsed.pop('hash', None)
        if not received_hash:
            return None
        
        # Create data check string (sorted alphabetically)
        data_check_string = '\n'.join(
            f"{k}={v}" for k, v in sorted(parsed.items())
        )
        
        # Create secret key
        secret_key = hmac.new(
            b"WebAppData",
            TELEGRAM_BOT_TOKEN.encode(),
            hashlib.sha256
        ).digest()
        
        # Calculate hash
        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        # Validate
        if calculated_hash != received_hash:
            return None
        
        # Parse user data
        user_data = parsed.get('user')
        if user_data:
            return json.loads(unquote(user_data))
        
        return parsed
    except Exception as e:
        print(f"[Telegram] Validation error: {e}")
        return None


@app.post("/api/telegram/auth")
async def telegram_auth(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Authenticate user via Telegram Web App"""
    try:
        body = await request.json()
        init_data = body.get('initData', '')
        
        user_data = validate_telegram_init_data(init_data)
        if not user_data:
            return JSONResponse(
                status_code=401,
                content={"error": "Invalid Telegram data"}
            )
        
        telegram_id = user_data.get('id')
        first_name = user_data.get('first_name', '')
        last_name = user_data.get('last_name', '')
        username = user_data.get('username', '')
        
        return {
            "success": True,
            "telegram_id": telegram_id,
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "full_name": f"{first_name} {last_name}".strip()
        }
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": str(e)}
        )


@app.get("/api/telegram/config")
async def telegram_config():
    """Get Telegram bot configuration for frontend"""
    return {
        "bot_username": TELEGRAM_BOT_USERNAME,
        "webapp_url": APP_URL
    }


# =============================================================================
# Viewer Settings (per-task + global defaults)
# =============================================================================
@app.get("/api/viewer-default-settings")
async def api_get_viewer_default_settings():
    """Public: get global default viewer settings JSON."""
    data = _read_json_file(VIEWER_DEFAULT_SETTINGS_PATH)
    if not data:
        data = DEFAULT_VIEWER_SETTINGS
    return data


@app.post("/api/admin/viewer-default-settings")
async def api_set_viewer_default_settings(
    request: Request,
    admin: User = Depends(require_admin),
):
    """Admin-only: overwrite global default viewer settings JSON file."""
    body = await request.body()
    settings = _validate_viewer_settings_payload(body)
    try:
        _atomic_write_json_file(VIEWER_DEFAULT_SETTINGS_PATH, settings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save default settings: {e}")
    return {"ok": True}


@app.get("/api/task/{task_id}/viewer-settings")
async def api_get_task_viewer_settings(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get viewer settings for a task.

    - If requester is owner/admin: return per-task settings if present, otherwise global defaults.
    - If requester is not owner/admin: return ONLY global defaults (do not leak per-task settings).
    """
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = None
    try:
        anon_session = await get_anon_session(request, response, db)
    except Exception:
        anon_session = None

    is_owner_or_admin = _is_task_owner_or_admin(task=task, user=user, anon_session=anon_session)
    if is_owner_or_admin and getattr(task, "viewer_settings", None):
        try:
            data = json.loads(task.viewer_settings)
            if isinstance(data, dict):
                return data
        except Exception:
            # Corrupt JSON in DB: ignore and fallback to defaults.
            pass

    data = _read_json_file(VIEWER_DEFAULT_SETTINGS_PATH)
    if not data:
        data = DEFAULT_VIEWER_SETTINGS
    return data


@app.post("/api/task/{task_id}/viewer-settings")
async def api_set_task_viewer_settings(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Owner/admin: save per-task viewer settings JSON."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = None
    try:
        anon_session = await get_anon_session(request, response, db)
    except Exception:
        anon_session = None

    if not _is_task_owner_or_admin(task=task, user=user, anon_session=anon_session):
        raise HTTPException(status_code=403, detail="Not authorized to update viewer settings")

    body = await request.body()
    settings = _validate_viewer_settings_payload(body)
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    await db.commit()
    return {"ok": True}


# =============================================================================
# Static Files & Pages
# =============================================================================
# Get the directory containing this file
import pathlib
BASE_DIR = pathlib.Path(__file__).parent.parent
STATIC_DIR = BASE_DIR / "static"

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    """Serve main page"""
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/task")
async def task_page(
    id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Serve task page with dynamic OG meta tags for Telegram/social sharing"""
    
    # Read base template
    task_html_path = STATIC_DIR / "task.html"
    html_content = task_html_path.read_text(encoding="utf-8")
    
    # If no task_id, return default page
    if not id:
        return HTMLResponse(content=html_content)
    
    task_id = id
    base_url = APP_URL or "https://autorig.online"
    
    # Try to get task info for better OG tags
    task_title = "Rigged 3D Model"
    task_description = "View this rigged 3D character with 50+ animations"
    has_video = False
    has_thumb = False
    
    try:
        from database import Task
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        
        if task:
            if task.status == "done":
                task_title = "✅ Rigged 3D Model Ready"
                task_description = "3D character rigged with skeleton and 50+ animations. Download in GLB, FBX, OBJ formats."
            elif task.status == "processing":
                task_title = "⏳ Rigging in Progress..."
                task_description = "3D model is being rigged with AI. View live progress."
            elif task.status == "error":
                task_title = "❌ Rigging Failed"
                task_description = "There was an error processing this model."
            
            # Check if video exists
            video_path = f"/var/autorig/videos/{task_id}.mp4"
            has_video = os.path.exists(video_path) and os.path.getsize(video_path) > 0
            
            # Assume thumb exists if task has ready_urls
            has_thumb = bool(task.ready_urls)
    except Exception as e:
        print(f"[Task Page] Error getting task info: {e}")
    
    # Build OG meta tags
    og_tags = f'''
    <!-- Open Graph / Telegram / Social -->
    <meta property="og:type" content="{'video.other' if has_video else 'website'}">
    <meta property="og:url" content="{base_url}/task?id={task_id}">
    <meta property="og:title" content="{task_title} | AutoRig.online">
    <meta property="og:description" content="{task_description}">
    <meta property="og:site_name" content="AutoRig.online">'''
    
    # Add image/video tags
    if has_thumb:
        og_tags += f'''
    <meta property="og:image" content="{base_url}/api/thumb/{task_id}">
    <meta property="og:image:width" content="640">
    <meta property="og:image:height" content="360">'''
    
    if has_video:
        og_tags += f'''
    <meta property="og:video" content="{base_url}/api/video/{task_id}">
    <meta property="og:video:secure_url" content="{base_url}/api/video/{task_id}">
    <meta property="og:video:type" content="video/mp4">
    <meta property="og:video:width" content="640">
    <meta property="og:video:height" content="360">'''
    
    # Twitter Card tags
    if has_video:
        og_tags += f'''
    <meta name="twitter:card" content="player">
    <meta name="twitter:player" content="{base_url}/api/video/{task_id}">
    <meta name="twitter:player:width" content="640">
    <meta name="twitter:player:height" content="360">'''
    elif has_thumb:
        og_tags += f'''
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:image" content="{base_url}/api/thumb/{task_id}">'''
    else:
        og_tags += '''
    <meta name="twitter:card" content="summary">'''
    
    og_tags += f'''
    <meta name="twitter:title" content="{task_title} | AutoRig.online">
    <meta name="twitter:description" content="{task_description}">
    '''
    
    # Inject OG tags after <meta name="robots">
    html_content = html_content.replace(
        '<meta name="robots" content="noindex, nofollow">',
        f'<meta name="robots" content="noindex, nofollow">{og_tags}'
    )
    
    # Update <title> tag to be dynamic
    html_content = html_content.replace(
        '<title>Task Progress | AutoRig.online</title>',
        f'<title>{task_title} | AutoRig.online</title>'
    )
    
    return HTMLResponse(content=html_content)


@app.get("/admin")
async def admin_page(user: Optional[User] = Depends(get_current_user)):
    """Serve admin page"""
    if not user or user.email not in ADMIN_EMAILS:
        return RedirectResponse(url="/auth/login")
    return FileResponse(str(STATIC_DIR / "admin.html"))


@app.get("/gallery")
async def gallery_page():
    """Serve Gallery page"""
    return FileResponse(str(STATIC_DIR / "gallery.html"))


@app.get("/guides")
async def guides_page():
    """Serve Guides page"""
    return FileResponse(str(STATIC_DIR / "guides.html"))


@app.get("/buy-credits")
async def buy_credits_page():
    """Serve Buy Credits page"""
    return FileResponse(str(STATIC_DIR / "buy-credits.html"))


@app.get("/payment/success")
async def payment_success_page():
    """Serve payment success info page (no credit logic here)."""
    return FileResponse(str(STATIC_DIR / "payment-success.html"))


# SEO pages
@app.get("/glb-auto-rig")
@app.get("/fbx-auto-rig")
@app.get("/obj-auto-rig")
@app.get("/how-it-works")
@app.get("/faq")
async def seo_pages():
    """Serve SEO landing pages (redirect to main for now)"""
    return FileResponse(str(STATIC_DIR / "index.html"))


# Sitemap and robots.txt
@app.get("/sitemap.xml")
async def sitemap():
    """Serve sitemap.xml for SEO"""
    return FileResponse(
        str(STATIC_DIR / "sitemap.xml"),
        media_type="application/xml"
    )


@app.get("/robots.txt")
async def robots():
    """Serve robots.txt for crawlers"""
    return FileResponse(
        str(STATIC_DIR / "robots.txt"),
        media_type="text/plain"
    )


# =============================================================================
# Health Check
# =============================================================================
@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# =============================================================================
# Run
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=DEBUG
    )

