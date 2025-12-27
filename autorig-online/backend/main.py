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
from contextlib import asynccontextmanager
import httpx

from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
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
    GUMROAD_PRODUCT_CREDITS
)
from database import init_db, get_db, User, AnonSession, GumroadSale, ApiKey
from models import (
    TaskCreateRequest, TaskCreateResponse, TaskStatusResponse,
    TaskHistoryItem, TaskHistoryResponse,
    OwnerTaskListItem, OwnerTaskListResponse,
    UserInfo, AnonInfo, AuthStatusResponse,
    ApiKeyItem, ApiKeyListResponse, ApiKeyCreateResponse,
    AdminUserListItem, AdminUserListResponse,
    AdminBalanceUpdate, AdminBalanceResponse,
    AdminUserTaskItem, AdminUserTasksResponse,
    AdminAnonSessionListItem, AdminAnonSessionListResponse,
    AdminAnonSessionTaskItem, AdminAnonSessionTasksResponse,
    AdminTaskOwnerResponse,
    AdminTaskListItem, AdminTaskListResponse, AdminTaskStatusCounts,
    WorkerQueueInfo, QueueStatusResponse,
    GalleryItem, GalleryResponse
)
from workers import get_global_queue_status, get_worker_base_url
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
    get_all_users, update_user_balance, get_anon_sessions,
    get_gallery_items, format_time_ago,
    find_file_by_pattern
)

from telegram_bot import broadcast_new_task, broadcast_task_done


# =============================================================================
# Background Task Worker
# =============================================================================
background_task_running = False


async def check_task_on_worker(worker_api: str, worker_task_id: str) -> bool:
    """
    Check if a task still exists on its assigned worker.
    Returns True if worker knows about this task, False otherwise.
    """
    if not worker_api or not worker_task_id:
        return False
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(worker_api, timeout=5.0)
            if r.status_code != 200:
                return False  # worker unreachable
            data = r.json()
            # Combine all task dicts from worker
            all_tasks = {
                **data.get("active_tasks", {}),
                **data.get("pending_tasks", {}),
                **data.get("processing_tasks", {}),
            }
            return worker_task_id in all_tasks
    except Exception:
        return False


async def background_task_updater():
    """Background worker that updates all processing tasks periodically"""
    from database import AsyncSessionLocal, Task
    
    global background_task_running
    background_task_running = True
    iteration = 0
    
    print("[Background Worker] Started task updater")
    
    while background_task_running:
        iteration += 1
        try:
            async with AsyncSessionLocal() as db:
                # Dispatch queued tasks (status=created) to actually free workers
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

                # Get all processing tasks
                result = await db.execute(
                    select(Task).where(Task.status == "processing")
                )
                processing_tasks = result.scalars().all()
                
                if processing_tasks:
                    print(f"[Background Worker] Updating {len(processing_tasks)} processing tasks")

                    # Update tasks concurrently (bounded). IMPORTANT: each update uses its own DB session
                    # (AsyncSession is not safe to share across concurrent tasks).
                    semaphore = asyncio.Semaphore(8)
                    task_ids = [t.id for t in processing_tasks]

                    async def _update_one(task_id: str):
                        async with semaphore:
                            try:
                                async with AsyncSessionLocal() as db2:
                                    rs = await db2.execute(select(Task).where(Task.id == task_id))
                                    t2 = rs.scalar_one_or_none()
                                    if not t2:
                                        return
                                    await update_task_progress(db2, t2)
                            except Exception as e:
                                print(f"[Background Worker] Error updating task {task_id}: {e}")

                    await asyncio.gather(*[_update_one(tid) for tid in task_ids])

                # ----------------------------------------------------------
                # Zombie + Timeout detection: every 5 minutes (10 iterations * 30s)
                # 1. Check if "processing" tasks are actually on their workers (zombie).
                # 2. Check if tasks have been processing for > 2 hours (timeout).
                # With retry logic: max 1 retry, then permanent error.
                # ----------------------------------------------------------
                TASK_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours
                MAX_RETRIES = 1

                if iteration % 10 == 0 and processing_tasks:
                    print(f"[Health Check] Checking {len(processing_tasks)} processing tasks")
                    now = datetime.utcnow()

                    for t in processing_tasks:
                        should_retry = False
                        fail_reason = None

                        # Check 1: Timeout (started_at > 2 hours ago)
                        if t.started_at:
                            elapsed = (now - t.started_at).total_seconds()
                            if elapsed > TASK_TIMEOUT_SECONDS:
                                should_retry = True
                                fail_reason = f"Timeout after {int(elapsed // 60)} minutes"

                        # Check 2: Zombie (worker doesn't know about task)
                        if not should_retry and t.worker_api and t.worker_task_id:
                            on_worker = await check_task_on_worker(t.worker_api, t.worker_task_id)
                            if not on_worker:
                                should_retry = True
                                fail_reason = "Worker lost task"

                        if should_retry:
                            async with AsyncSessionLocal() as db_health:
                                rs = await db_health.execute(select(Task).where(Task.id == t.id))
                                tz = rs.scalar_one_or_none()
                                if tz and tz.status == "processing":
                                    current_retries = tz.retry_count or 0
                                    if current_retries < MAX_RETRIES:
                                        # Retry: reset to created
                                        tz.status = "created"
                                        tz.worker_api = None
                                        tz.worker_task_id = None
                                        tz.guid = None
                                        tz.output_urls = None
                                        tz.ready_urls = []
                                        tz.ready_count = 0
                                        tz.total_count = 0
                                        tz.video_ready = False
                                        tz.video_url = None
                                        tz.started_at = None
                                        tz.retry_count = current_retries + 1
                                        await db_health.commit()
                                        print(f"[Health Check] Retry task {t.id} ({fail_reason}), attempt {current_retries + 1}")
                                    else:
                                        # Max retries exceeded - permanent error
                                        tz.status = "error"
                                        tz.error_message = f"{fail_reason} (max retries exceeded)"
                                        await db_health.commit()
                                        print(f"[Health Check] Failed task {t.id} permanently ({fail_reason})")

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
    os.makedirs("/var/autorig/videos", exist_ok=True)
    os.makedirs("/var/autorig/thumbnails", exist_ok=True)

    
    # Start background worker
    app.state.background_worker = asyncio.create_task(background_task_updater())
    
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
    if (user.email or "").strip().lower() not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


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
        
        # Generate public URL
        final_url = f"{APP_URL}/u/{upload_token}/{filename}"
    
    if not final_url:
        raise HTTPException(status_code=400, detail="No input URL provided")
    
    # Create task
    task, error = await create_conversion_task(
        db, final_url, type, owner_type, owner_id
    )
    
    if error and not task:
        raise HTTPException(status_code=500, detail=error)
    
    # Telegram notify (best-effort, async)
    try:
        asyncio.create_task(broadcast_new_task(task.id, final_url, type))
    except Exception:
        pass

    # Deduct credit
    if user:
        await decrement_user_credits(db, user)
    else:
        await increment_anon_usage(db, anon_session.anon_id)
    
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
    
    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        progress=task.progress,
        ready_count=task.ready_count,
        total_count=task.total_count,
        ready_urls=task.ready_urls,
        video_ready=task.video_ready,
        video_url=task.video_url,
        worker_api=task.worker_api,
        worker_task_id=task.worker_task_id,
        guid=task.guid,
        progress_page=task.progress_page,
        viewer_html_url=viewer_html_url,
        quick_downloads=quick_downloads if quick_downloads else None,
        error_message=task.error_message,
        created_at=task.created_at,
        updated_at=task.updated_at
    )


@app.get("/api/task/{task_id}/owner_tasks", response_model=OwnerTaskListResponse)
async def api_task_owner_tasks(
    task_id: str,
    page: int = 1,
    per_page: int = 12,
    db: AsyncSession = Depends(get_db)
):
    """
    Public: list all tasks for the owner of a given task_id (sorted by created_at desc).
    Used by task page to show 'all tasks of this user' even for anon sessions.
    """
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    owner_type = task.owner_type
    owner_id = task.owner_id

    tasks, total = await get_user_tasks(db, owner_type, owner_id, page, per_page)

    placeholder = "/static/images/placeholder-thumb.svg"
    items = []
    for t in tasks:
        thumb = f"/api/thumb/{t.id}" if t.status == "done" else placeholder
        items.append(
            OwnerTaskListItem(
                task_id=t.id,
                status=t.status,
                progress=t.progress,
                created_at=t.created_at,
                video_ready=bool(t.video_ready),
                thumbnail_url=thumb,
            )
        )

    return OwnerTaskListResponse(
        owner_type=owner_type,
        owner_id=owner_id,
        tasks=items,
        total=total,
        page=page,
        per_page=per_page,
    )


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
    
    # Telegram notify (best-effort, async)
    try:
        asyncio.create_task(broadcast_new_task(new_task.id, new_task.input_url, new_task.input_type))
    except Exception:
        pass

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
    is_admin = bool(user and (user.email or "").strip().lower() in ADMIN_EMAILS)
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

    await db.commit()
    await db.refresh(task)

    # Start pipeline for the same task_id without blocking on FBX pre-conversion.
    worker_url = await select_best_worker()
    if not worker_url:
        raise HTTPException(status_code=500, detail="No workers available")

    task.worker_api = worker_url
    task.status = "processing"

    # Some worker endpoints may respond OK to GET status but miss POST route (HTTP 404).
    # Retry across workers in that case to avoid hard-failing the restart.
    from config import WORKERS
    candidate_workers = [worker_url] + [w for w in WORKERS if w != worker_url]

    last_error = None
    for candidate in candidate_workers:
        result = await send_task_to_worker(candidate, task.input_url, task.input_type or "t_pose")
        if result.success:
            filtered_output_urls = [u for u in (result.output_urls or []) if "*" not in u]
            task.worker_api = candidate
            task.worker_task_id = result.task_id
            task.progress_page = result.progress_page
            task.guid = result.guid
            task.output_urls = filtered_output_urls
            task.total_count = len(filtered_output_urls)
            task.status = "processing"
            last_error = None
            break

        last_error = result.error
        if last_error and "HTTP 404" in last_error:
            continue
        continue

    if last_error:
        task.status = "error"
        task.error_message = last_error

    await db.commit()
    await db.refresh(task)

    return TaskCreateResponse(
        task_id=task.id,
        status=task.status,
        message="Task restarted successfully"
    )


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
    page: int = 1,
    per_page: int = 12,
    db: AsyncSession = Depends(get_db)
):
    """Get public gallery of completed tasks with videos"""
    tasks, total = await get_gallery_items(db, page, per_page)
    
    items = [
        GalleryItem(
            task_id=t.id,
            video_url=f"/api/video/{t.id}",  # Use proxy URL
            thumbnail_url=f"/api/thumb/{t.id}",
            created_at=t.created_at,
            time_ago=format_time_ago(t.created_at)
        )
        for t in tasks
    ]
    
    has_more = (page * per_page) < total
    
    return GalleryResponse(
        items=items,
        total=total,
        page=page,
        per_page=per_page,
        has_more=has_more
    )


@app.api_route("/api/thumb/{task_id}", methods=["GET", "HEAD"])
async def api_task_thumbnail(task_id: str, db: AsyncSession = Depends(get_db)):
    """Return cached JPEG thumbnail for a task (gallery preview)."""
    import os
    import httpx

    task = await get_task_by_id(db, task_id)
    if not task or not task.guid or not task.worker_api:
        raise HTTPException(status_code=404, detail="Thumbnail not available")

    # Local cache (runtime)
    cache_dir = "/var/autorig/thumbnails"
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{task_id}.jpg")
    tmp_path = cache_path + ".tmp"

    # Final worker format prefers a dedicated poster:
    # - {guid}_video_poster.jpg
    poster_name = f"{task.guid}_video_poster.jpg"
    # Prefer HDRP preview render from task output URLs (fallback)
    target_name = "Unity_HDRP_Render_2_view.jpg"
    remote_url = None

    # 1) Try poster first (ready_urls then output_urls)
    for u in (task.ready_urls or []):
        if poster_name in u:
            remote_url = u
            break

    if not remote_url:
        for u in (task.output_urls or []):
            if poster_name in u:
                remote_url = u
                break

    # 2) HDRP fallback
    if not remote_url:
        for u in (task.ready_urls or []):
            if target_name in u:
                remote_url = u
                break

    if not remote_url:
        for u in (task.output_urls or []):
            if target_name in u:
                remote_url = u
                break

    # Fallback (legacy) if HDRP preview not present
    if not remote_url:
        worker_base = get_worker_base_url(task.worker_api)
        guid = task.guid
        remote_url = f"{worker_base}/converter/glb/{guid}/{guid}_100k/{guid}_VRayCam001_view.jpg"

    # If cached file exists, usually serve immediately.
    # BUT: if HDRP preview exists, and our cache was created earlier from a fallback image,
    # refresh once when the remote Content-Length differs from local size.
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        try:
            local_size = os.path.getsize(cache_path)
            if remote_url and ((poster_name in remote_url) or (target_name in remote_url)):
                async with httpx.AsyncClient() as client:
                    h = await client.head(remote_url, timeout=10.0, follow_redirects=True)
                    if h.status_code == 200:
                        cl = h.headers.get('content-length')
                        if cl and cl.isdigit() and int(cl) != local_size:
                            # Refresh cache with the preferred thumbnail (poster/HDRP)
                            r2 = await client.get(remote_url, timeout=30.0, follow_redirects=True)
                            if r2.status_code == 200 and r2.content:
                                with open(tmp_path, 'wb') as f:
                                    f.write(r2.content)
                                os.replace(tmp_path, cache_path)
        except Exception:
            pass

        return FileResponse(cache_path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})

    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(remote_url, timeout=30.0, follow_redirects=True)
            if r.status_code != 200 or not r.content:
                raise HTTPException(status_code=404, detail="Thumbnail not available")
            with open(tmp_path, "wb") as f:
                f.write(r.content)
        os.replace(tmp_path, cache_path)
        return FileResponse(cache_path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# =============================================================================
# Queue Status Endpoint
# =============================================================================
@app.get("/api/queue/status", response_model=QueueStatusResponse)
async def api_queue_status(db: AsyncSession = Depends(get_db)):
    """Get global queue status across all workers"""
    from database import Task
    from sqlalchemy import func

    status = await get_global_queue_status()

    # Count tasks waiting in our DB (status=created) — these are queued locally
    db_pending_result = await db.execute(
        select(func.count()).select_from(Task).where(Task.status == "created")
    )
    db_pending_count = db_pending_result.scalar() or 0

    # total_pending = workers' pending + our local queue
    combined_pending = status.total_pending + db_pending_count

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
        total_pending=combined_pending,  # includes DB queue
        total_queue=status.total_queue,
        available_workers=status.available_workers,
        total_workers=status.total_workers,
        estimated_wait_seconds=status.estimated_wait_seconds,
        estimated_wait_formatted=status.estimated_wait_formatted
    )


# =============================================================================
# Admin Endpoints
# =============================================================================
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


@app.get("/api/admin/anon-sessions", response_model=AdminAnonSessionListResponse)
async def api_admin_anon_sessions(
    query: Optional[str] = None,
    sort_by: str = "last_seen_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get list of anon sessions (admin only)"""
    sessions, total = await get_anon_sessions(
        db, search=query, sort_by=sort_by, sort_desc=sort_desc, page=page, per_page=per_page
    )

    return AdminAnonSessionListResponse(
        sessions=[
            AdminAnonSessionListItem(
                anon_id=s.anon_id,
                free_used=s.free_used,
                created_at=s.created_at,
                last_seen_at=s.last_seen_at,
                total_tasks=total_tasks,
            )
            for (s, total_tasks) in sessions
        ],
        total=total,
        page=page,
        per_page=per_page,
    )


@app.get("/api/admin/anon-session/{anon_id}/tasks", response_model=AdminAnonSessionTasksResponse)
async def api_admin_anon_session_tasks(
    anon_id: str,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get tasks for a specific anon session (admin only)"""
    tasks, total = await get_user_tasks(
        db, owner_type="anon", owner_id=anon_id, page=page, per_page=per_page
    )

    return AdminAnonSessionTasksResponse(
        tasks=[
            AdminAnonSessionTaskItem(
                task_id=task.id,
                status=task.status,
                progress=task.progress,
                ready_count=task.ready_count,
                total_count=task.total_count,
                created_at=task.created_at,
                updated_at=task.updated_at,
                input_url=task.input_url,
            )
            for task in tasks
        ],
        total=total,
        page=page,
        per_page=per_page,
    )


@app.get("/api/admin/task/{task_id}/owner", response_model=AdminTaskOwnerResponse)
async def api_admin_task_owner(
    task_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get task owner (admin only)."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    user_id: Optional[int] = None
    if task.owner_type == "user":
        res = await db.execute(select(User).where(User.email == task.owner_id))
        u = res.scalar_one_or_none()
        if u:
            user_id = u.id

    return AdminTaskOwnerResponse(
        task_id=task.id,
        owner_type=task.owner_type,
        owner_id=task.owner_id,
        user_id=user_id,
        created_at=task.created_at,
        status=task.status,
    )


@app.get("/api/admin/tasks")
async def api_admin_tasks(
    status: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of all tasks with optional status filter (admin only).
    Status: processing, created, done, error, or None for all.
    """
    from sqlalchemy import func, desc, asc
    from database import Task

    # Build base query
    query = select(Task)
    count_query = select(func.count()).select_from(Task)

    # Apply status filter
    if status:
        query = query.where(Task.status == status)
        count_query = count_query.where(Task.status == status)

    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply sorting
    sort_col = getattr(Task, sort_by, Task.created_at)
    query = query.order_by(desc(sort_col) if sort_desc else asc(sort_col))

    # Apply pagination
    offset = (page - 1) * per_page
    query = query.offset(offset).limit(per_page)

    # Execute
    result = await db.execute(query)
    tasks = result.scalars().all()

    # Get status counts (for filter badges)
    status_counts_result = await db.execute(
        select(Task.status, func.count()).group_by(Task.status)
    )
    counts_raw = {row[0]: row[1] for row in status_counts_result.fetchall()}
    status_counts = AdminTaskStatusCounts(
        processing=counts_raw.get("processing", 0),
        created=counts_raw.get("created", 0),
        done=counts_raw.get("done", 0),
        error=counts_raw.get("error", 0),
    )

    # Get owner names for users
    user_emails = [t.owner_id for t in tasks if t.owner_type == "user"]
    user_names = {}
    if user_emails:
        users_result = await db.execute(
            select(User.email, User.name).where(User.email.in_(user_emails))
        )
        user_names = {row[0]: row[1] for row in users_result.fetchall()}

    # Build response
    items = []
    for t in tasks:
        owner_name = None
        if t.owner_type == "user":
            owner_name = user_names.get(t.owner_id) or t.owner_id
        elif t.owner_type == "anon":
            owner_name = "Anonymous"

        items.append(AdminTaskListItem(
            task_id=t.id,
            status=t.status,
            progress=t.progress,
            ready_count=t.ready_count,
            total_count=t.total_count,
            created_at=t.created_at,
            updated_at=t.updated_at,
            worker_api=t.worker_api,
            guid=t.guid,
            owner_type=t.owner_type,
            owner_id=t.owner_id,
            owner_name=owner_name,
            input_url=t.input_url,
            retry_count=t.retry_count or 0,
            started_at=t.started_at,
        ))

    return AdminTaskListResponse(
        tasks=items,
        total=total,
        page=page,
        per_page=per_page,
        status_counts=status_counts,
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

@app.api_route("/api/video/{task_id}", methods=["GET", "HEAD"])
async def proxy_video(
    task_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Serve task video.

    Priority:
    1) If cached locally: /var/autorig/videos/{task_id}.mp4 (supports Range for Chrome/Edge)
    2) Else download from worker to cache, then serve from local cache
    """
    import os
    import re
    import httpx
    from fastapi.responses import StreamingResponse

    cache_dir = "/var/autorig/videos"
    os.makedirs(cache_dir, exist_ok=True)
    cache_mp4 = os.path.join(cache_dir, f"{task_id}.mp4")
    cache_mov = os.path.join(cache_dir, f"{task_id}.mov")

    def _serve_cached(path: str, *, media_type: str, download_name: str) -> Response:
        file_size = os.path.getsize(path)
        range_header = request.headers.get("range")

        base_headers = {
            "Content-Disposition": f"inline; filename={download_name}",
            "Cache-Control": "public, max-age=86400",
            "Accept-Ranges": "bytes",
        }

        # HEAD without Range
        if request.method == "HEAD" and not range_header:
            return Response(status_code=200, headers={**base_headers, "Content-Length": str(file_size)})

        if not range_header:
            return FileResponse(path, media_type=media_type, headers={**base_headers, "Content-Length": str(file_size)})

        m = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if not m:
            return FileResponse(path, media_type=media_type, headers={**base_headers, "Content-Length": str(file_size)})

        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else (file_size - 1)
        if start >= file_size:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}", **base_headers})

        end = min(end, file_size - 1)
        length = end - start + 1

        headers = {
            **base_headers,
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(length),
        }

        if request.method == "HEAD":
            return Response(status_code=206, headers=headers)

        def iterfile():
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(1024 * 256, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return StreamingResponse(iterfile(), status_code=206, media_type=media_type, headers=headers)

    # Serve cached if present
    if os.path.exists(cache_mp4) and os.path.getsize(cache_mp4) > 0:
        return _serve_cached(cache_mp4, media_type="video/mp4", download_name=f"{task_id}_video.mp4")
    if os.path.exists(cache_mov) and os.path.getsize(cache_mov) > 0:
        return _serve_cached(cache_mov, media_type="video/quicktime", download_name=f"{task_id}_video.mov")

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if not task.video_url:
        raise HTTPException(status_code=404, detail="Video not available")

    # Ensure local cache exists (mp4 or mov)
    try:
        from tasks import cache_task_video_by_id
        await cache_task_video_by_id(task_id)
    except Exception:
        pass

    if os.path.exists(cache_mp4) and os.path.getsize(cache_mp4) > 0:
        return _serve_cached(cache_mp4, media_type="video/mp4", download_name=f"{task_id}_video.mp4")
    if os.path.exists(cache_mov) and os.path.getsize(cache_mov) > 0:
        return _serve_cached(cache_mov, media_type="video/quicktime", download_name=f"{task_id}_video.mov")

    raise HTTPException(status_code=404, detail="Video not available")


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
    
    is_image = ext in {"png", "jpg", "jpeg", "webp", "gif"}
    disposition = "inline" if is_image else "attachment"

    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers={
            "Content-Disposition": f'{disposition}; filename="{filename}"',
            "Cache-Control": "public, max-age=86400",
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
# Progress Log (Worker)
# =============================================================================
@app.get("/api/task/{task_id}/progress_log")
async def api_task_progress_log(
    task_id: str,
    full: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch worker-side {GUID}_progress.txt (no caching) and return last 2 lines + full text.
    Worker path (final): /converter/glb/{GUID}/{GUID}_progress.txt
    """
    import httpx

    from urllib.parse import urlparse
    import re

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Derive guid/worker_base even if task.guid / task.worker_api are missing (older records)
    guid = task.guid
    if not guid:
        # Try infer GUID from any artifact filename prefix: {GUID}_something.ext
        candidates = []
        if getattr(task, "ready_urls", None):
            candidates.extend(task.ready_urls or [])
        if getattr(task, "output_urls", None):
            candidates.extend(task.output_urls or [])
        if getattr(task, "video_url", None):
            candidates.append(task.video_url)
        for u in candidates:
            fn = (u or "").split("/")[-1]
            m = re.match(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})_", fn)
            if m:
                guid = m.group(1)
                break
    if not guid:
        # Not started / no artifacts yet => treat as queued
        return {
            "available": False,
            "state": "queued",
            "tail_lines": [],
            "full_text": None,
            "truncated": False,
        }

    worker_base = None
    if getattr(task, "worker_api", None):
        worker_base = get_worker_base_url(task.worker_api)
    else:
        # Infer worker base from any URL (ready/output/video)
        candidates = []
        if getattr(task, "ready_urls", None):
            candidates.extend(task.ready_urls or [])
        if getattr(task, "output_urls", None):
            candidates.extend(task.output_urls or [])
        if getattr(task, "video_url", None):
            candidates.append(task.video_url)
        for u in candidates:
            try:
                p = urlparse(u)
                if p.scheme and p.netloc:
                    worker_base = f"{p.scheme}://{p.netloc}"
                    break
            except Exception:
                continue
    if not worker_base:
        # Worker not assigned yet => queued
        return {
            "available": False,
            "state": "queued",
            "tail_lines": [],
            "full_text": None,
            "truncated": False,
        }

    candidate_urls = [
        # Preferred (as requested)
        f"{worker_base}/converter/glb/{guid}/{guid}_progress.txt",
        # Common alternates (older worker variants)
        f"{worker_base}/converter/glb/{guid}/{guid}_progress.log",
        f"{worker_base}/converter/glb/{guid}/progress.txt",
        f"{worker_base}/converter/glb/{guid}/progress.log",
        f"{worker_base}/converter/glb/{guid}_progress.txt",
        f"{worker_base}/converter/glb/{guid}_progress.log",
    ]

    async def _fetch_text(url: str) -> tuple[str, bool] | None:
        MAX_BYTES = 1_000_000  # 1MB safety cap
        buf = bytearray()
        truncated = False
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", url, timeout=15.0, follow_redirects=True) as resp:
                if resp.status_code == 404:
                    return None
                if resp.status_code != 200:
                    # treat other statuses as a hard error
                    raise HTTPException(status_code=502, detail=f"Worker returned {resp.status_code} for progress log")
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    if not chunk:
                        continue
                    if len(buf) + len(chunk) > MAX_BYTES:
                        take = MAX_BYTES - len(buf)
                        if take > 0:
                            buf.extend(chunk[:take])
                        truncated = True
                        break
                    buf.extend(chunk)
        text = bytes(buf).decode("utf-8", errors="replace")
        return text, truncated

    text = None
    truncated = False
    try:
        for u in candidate_urls:
            got = await _fetch_text(u)
            if got is None:
                continue
            text, truncated = got
            break
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch progress log: {e}")

    if text is None:
        # File is not published yet (or missing) => return 200 with availability flag
        # status=created => queued; processing/done => missing (worker running but log not available)
        state = "queued" if getattr(task, "status", None) == "created" else "missing"
        return {
            "available": False,
            "state": state,
            "tail_lines": [],
            "full_text": None,
            "truncated": False,
        }

    lines = text.splitlines()
    tail = lines[-2:] if len(lines) >= 2 else lines

    return {
        "available": True,
        "tail_lines": tail,
        "full_text": (text if full else None),
        "truncated": truncated,
    }


# =============================================================================
# Model Proxy (GLB)
# =============================================================================
@app.get("/api/task/{task_id}/model.glb")
async def api_task_model_glb(
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Proxy {GUID}.glb from worker (no caching) to serve over HTTPS.
    Worker path: /converter/glb/{GUID}/{GUID}.glb
    """
    import httpx
    import re
    from urllib.parse import urlparse
    from fastapi.responses import StreamingResponse

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Infer GUID
    guid = getattr(task, "guid", None)
    if not guid:
        candidates = []
        if getattr(task, "ready_urls", None):
            candidates.extend(task.ready_urls or [])
        if getattr(task, "output_urls", None):
            candidates.extend(task.output_urls or [])
        if getattr(task, "video_url", None):
            candidates.append(task.video_url)
        for u in candidates:
            fn = (u or "").split("/")[-1]
            m = re.match(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})_", fn)
            if m:
                guid = m.group(1)
                break
    if not guid:
        raise HTTPException(status_code=404, detail="Task GUID not found")

    # Infer worker base
    worker_base = None
    if getattr(task, "worker_api", None):
        worker_base = get_worker_base_url(task.worker_api)
    else:
        candidates = []
        if getattr(task, "ready_urls", None):
            candidates.extend(task.ready_urls or [])
        if getattr(task, "output_urls", None):
            candidates.extend(task.output_urls or [])
        if getattr(task, "video_url", None):
            candidates.append(task.video_url)
        for u in candidates:
            try:
                p = urlparse(u)
                if p.scheme and p.netloc:
                    worker_base = f"{p.scheme}://{p.netloc}"
                    break
            except Exception:
                continue
    if not worker_base:
        raise HTTPException(status_code=404, detail="Worker info not found")

    glb_url = f"{worker_base}/converter/glb/{guid}/{guid}.glb"

    async def stream_glb():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", glb_url, timeout=60.0, follow_redirects=True) as resp:
                if resp.status_code != 200:
                    raise HTTPException(status_code=404, detail="Model not found")
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        stream_glb(),
        media_type="model/gltf-binary",
        headers={
            "Cache-Control": "public, max-age=86400",
            "Content-Disposition": f'inline; filename="{guid}.glb"',
        },
    )


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
async def task_page():
    """Serve task page"""
    return FileResponse(str(STATIC_DIR / "task.html"))


@app.get("/admin")
async def admin_page(user: Optional[User] = Depends(get_current_user)):
    """Serve admin page"""
    if not user or (user.email or "").strip().lower() not in ADMIN_EMAILS:
        return RedirectResponse(url="/auth/login")
    return FileResponse(str(STATIC_DIR / "admin.html"))






@app.get("/developers")
async def developers_page():
    """Serve Developers / API info page"""
    return FileResponse(str(STATIC_DIR / "developers.html"))
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

