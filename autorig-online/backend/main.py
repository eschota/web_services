"""
AutoRig Online - Main FastAPI Application
=========================================
API for automatic 3D model rigging service.
"""
import os
import uuid
import shutil
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import (
    APP_NAME, APP_URL, DEBUG, SECRET_KEY,
    UPLOAD_DIR, MAX_UPLOAD_SIZE_MB,
    RATE_LIMIT_TASKS_PER_MINUTE, ADMIN_EMAIL,
    ANON_FREE_LIMIT
)
from database import init_db, get_db, User, AnonSession
from models import (
    TaskCreateRequest, TaskCreateResponse, TaskStatusResponse,
    TaskHistoryItem, TaskHistoryResponse,
    UserInfo, AnonInfo, AuthStatusResponse,
    AdminUserListItem, AdminUserListResponse,
    AdminBalanceUpdate, AdminBalanceResponse,
    WorkerQueueInfo, QueueStatusResponse
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
    create_conversion_task, update_task_progress,
    get_task_by_id, get_user_tasks,
    get_all_users, update_user_balance
)


# =============================================================================
# App Setup
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown"""
    # Startup
    await init_db()
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    yield
    # Shutdown
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
        return None
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
    if user.email != ADMIN_EMAIL:
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
    
    # Update progress if still processing
    if task.status == "processing":
        task = await update_task_progress(db, task)
    
    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        progress=task.progress,
        ready_count=task.ready_count,
        total_count=task.total_count,
        ready_urls=task.ready_urls,
        video_ready=task.video_ready,
        video_url=task.video_url,
        error_message=task.error_message,
        created_at=task.created_at,
        updated_at=task.updated_at
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
# Video Proxy (to avoid Mixed Content issues)
# =============================================================================
@app.get("/api/video/{task_id}")
async def proxy_video(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy video from worker to serve over HTTPS"""
    import httpx
    from fastapi.responses import StreamingResponse
    
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
    if not user or user.email != ADMIN_EMAIL:
        return RedirectResponse(url="/auth/login")
    return FileResponse(str(STATIC_DIR / "admin.html"))


# SEO pages
@app.get("/glb-auto-rig")
async def glb_auto_rig():
    """Serve GLB auto rig landing page"""
    return FileResponse(str(STATIC_DIR / "glb-auto-rig.html"))

@app.get("/fbx-auto-rig")
async def fbx_auto_rig():
    """Serve FBX auto rig landing page"""
    return FileResponse(str(STATIC_DIR / "fbx-auto-rig.html"))

@app.get("/obj-auto-rig")
async def obj_auto_rig():
    """Serve OBJ auto rig landing page"""
    return FileResponse(str(STATIC_DIR / "obj-auto-rig.html"))

@app.get("/t-pose-rig")
async def t_pose_rig():
    """Serve T-pose rig landing page"""
    return FileResponse(str(STATIC_DIR / "t-pose-rig.html"))

@app.get("/how-it-works")
async def how_it_works():
    """Serve how it works page"""
    return FileResponse(str(STATIC_DIR / "how-it-works.html"))

@app.get("/faq")
async def faq():
    """Serve FAQ page"""
    return FileResponse(str(STATIC_DIR / "faq.html"))

@app.get("/gallery")
async def gallery():
    """Serve gallery page"""
    return FileResponse(str(STATIC_DIR / "gallery.html"))

@app.get("/g/{item_id}")
async def gallery_item(item_id: str):
    """Serve gallery item pages"""
    return FileResponse(str(STATIC_DIR / "g-template.html"))

@app.get("/guides")
async def guides():
    """Serve guides page"""
    return FileResponse(str(STATIC_DIR / "guides.html"))

# Guide pages
@app.get("/mixamo-alternative")
async def mixamo_alternative():
    """Serve mixamo-alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative.html"))

@app.get("/rig-glb-unity")
async def rig_glb_unity():
    """Serve rig-glb-unity page"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity.html"))

@app.get("/rig-fbx-unreal")
async def rig_fbx_unreal():
    """Serve rig-fbx-unreal page"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal.html"))

@app.get("/t-pose-vs-a-pose")
async def t_pose_vs_a_pose():
    """Serve t-pose-vs-a-pose page"""
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose.html"))

@app.get("/glb-vs-fbx")
async def glb_vs_fbx():
    """Serve glb-vs-fbx page"""
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx.html"))

@app.get("/auto-rig-obj")
async def auto_rig_obj():
    """Serve auto-rig-obj page"""
    return FileResponse(str(STATIC_DIR / "auto-rig-obj.html"))

@app.get("/animation-retargeting")
async def animation_retargeting():
    """Serve animation-retargeting page"""
    return FileResponse(str(STATIC_DIR / "animation-retargeting.html"))

@app.get("/mixamo-alternative-ru")
async def mixamo_alternative_ru():
    """Serve RU mixamo-alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-ru.html"))

@app.get("/mixamo-alternative-zh")
async def mixamo_alternative_zh():
    """Serve ZH mixamo-alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-zh.html"))

@app.get("/mixamo-alternative-hi")
async def mixamo_alternative_hi():
    """Serve HI mixamo-alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-hi.html"))

@app.get("/rig-glb-unity-ru")
async def rig_glb_unity_ru():
    """Serve RU rig-glb-unity page"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-ru.html"))

@app.get("/rig-glb-unity-zh")
async def rig_glb_unity_zh():
    """Serve ZH rig-glb-unity page"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-zh.html"))

@app.get("/rig-glb-unity-hi")
async def rig_glb_unity_hi():
    """Serve HI rig-glb-unity page"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-hi.html"))

@app.get("/rig-fbx-unreal-ru")
async def rig_fbx_unreal_ru():
    """Serve RU rig-fbx-unreal page"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-ru.html"))

@app.get("/rig-fbx-unreal-zh")
async def rig_fbx_unreal_zh():
    """Serve ZH rig-fbx-unreal page"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-zh.html"))

@app.get("/rig-fbx-unreal-hi")
async def rig_fbx_unreal_hi():
    """Serve HI rig-fbx-unreal page"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-hi.html"))

@app.get("/t-pose-vs-a-pose-ru")
async def t_pose_vs_a_pose_ru():
    """Serve RU t-pose-vs-a-pose page"""
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-ru.html"))

@app.get("/t-pose-vs-a-pose-zh")
async def t_pose_vs_a_pose_zh():
    """Serve ZH t-pose-vs-a-pose page"""
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-zh.html"))

@app.get("/t-pose-vs-a-pose-hi")
async def t_pose_vs_a_pose_hi():
    """Serve HI t-pose-vs-a-pose page"""
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-hi.html"))

@app.get("/glb-vs-fbx-ru")
async def glb_vs_fbx_ru():
    """Serve RU glb-vs-fbx page"""
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-ru.html"))

@app.get("/glb-vs-fbx-zh")
async def glb_vs_fbx_zh():
    """Serve ZH glb-vs-fbx page"""
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-zh.html"))

@app.get("/glb-vs-fbx-hi")
async def glb_vs_fbx_hi():
    """Serve HI glb-vs-fbx page"""
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-hi.html"))

@app.get("/auto-rig-obj-ru")
async def auto_rig_obj_ru():
    """Serve RU auto-rig-obj page"""
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-ru.html"))

@app.get("/auto-rig-obj-zh")
async def auto_rig_obj_zh():
    """Serve ZH auto-rig-obj page"""
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-zh.html"))

@app.get("/auto-rig-obj-hi")
async def auto_rig_obj_hi():
    """Serve HI auto-rig-obj page"""
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-hi.html"))

@app.get("/animation-retargeting-ru")
async def animation_retargeting_ru():
    """Serve RU animation-retargeting page"""
    return FileResponse(str(STATIC_DIR / "animation-retargeting-ru.html"))

@app.get("/animation-retargeting-zh")
async def animation_retargeting_zh():
    """Serve ZH animation-retargeting page"""
    return FileResponse(str(STATIC_DIR / "animation-retargeting-zh.html"))

@app.get("/animation-retargeting-hi")
async def animation_retargeting_hi():
    """Serve HI animation-retargeting page"""
    return FileResponse(str(STATIC_DIR / "animation-retargeting-hi.html"))
@app.get("/mixamo-alternative")
async def mixamo_alternative():
    """Serve Mixamo alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative.html"))

@app.get("/mixamo-alternative-ru")
async def mixamo_alternative_ru():
    """Serve Russian Mixamo alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-ru.html"))

@app.get("/mixamo-alternative-zh")
async def mixamo_alternative_zh():
    """Serve Chinese Mixamo alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-zh.html"))

@app.get("/mixamo-alternative-hi")
async def mixamo_alternative_hi():
    """Serve Hindi Mixamo alternative page"""
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-hi.html"))

@app.get("/rig-glb-unity-ru")
async def rig_glb_unity_ru():
    """Serve Russian GLB Unity guide"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-ru.html"))

@app.get("/rig-fbx-unreal-ru")
async def rig_fbx_unreal_ru():
    """Serve Russian FBX Unreal guide"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-ru.html"))

@app.get("/rig-glb-unity")
async def rig_glb_unity():
    """Serve GLB Unity rigging guide"""
    return FileResponse(str(STATIC_DIR / "rig-glb-unity.html"))

@app.get("/rig-fbx-unreal")
async def rig_fbx_unreal():
    """Serve FBX Unreal rigging guide"""
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal.html"))

@app.get("/t-pose-vs-a-pose")
async def t_pose_vs_a_pose():
    """Serve T-pose vs A-pose guide"""
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose.html"))

@app.get("/glb-vs-fbx")
async def glb_vs_fbx():
    """Serve GLB vs FBX comparison"""
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx.html"))

@app.get("/auto-rig-obj")
async def auto_rig_obj():
    """Serve OBJ auto rigging guide"""
    return FileResponse(str(STATIC_DIR / "auto-rig-obj.html"))

@app.get("/animation-retargeting")
async def animation_retargeting():
    """Serve animation retargeting guide"""
    return FileResponse(str(STATIC_DIR / "animation-retargeting.html"))


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

