"""
AutoRig Online - Main FastAPI Application
=========================================
API for automatic 3D model rigging service.
"""
import os
import uuid
import shutil
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
import hashlib
import hmac
import secrets
import json
import tempfile
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import urlparse, quote, unquote, parse_qsl
from starlette.background import BackgroundTask

from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse, HTMLResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, or_, update
from sqlalchemy.exc import IntegrityError
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import (
    APP_NAME, APP_URL, DEBUG, SECRET_KEY,
    UPLOAD_DIR, MAX_UPLOAD_SIZE_MB,
    RATE_LIMIT_TASKS_PER_MINUTE, RATE_LIMIT_AGENT_REGISTER, is_admin_email,
    ANON_FREE_LIMIT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_BOT_USERNAME,
    VIEWER_DEFAULT_SETTINGS_PATH,
    MIN_FREE_SPACE_GB, CLEANUP_CHECK_INTERVAL_CYCLES, CLEANUP_MIN_AGE_HOURS,
    NEW_TASK_MIN_FREE_GB, NEW_TASK_PURGE_TASKS_MAX_FREED_GB,
    AUTOMATIC_TASK_DB_DELETION,
    STUCK_HOUR_MINUTES,
    STUCK_HOUR_MAX_REQUEUES,
    GALLERY_DB_PURGE_INTERVAL_CYCLES,
    GALLERY_UPSTREAM_PURGE_BATCH,
    GALLERY_UPSTREAM_PURGE_ROUNDS,
    GA_MEASUREMENT_ID, GA_API_SECRET,
    GUMROAD_PRODUCT_CREDITS,
    AUTORIG_DONATION_PRODUCT_KEYS,
    DONATION_GOAL_USD,
    DONATION_BASELINE_USD,
    AUTORIG_CRYPTO_TIERS,
    CRYPTO_RECEIVE_NETWORKS,
    CRYPTO_ALLOWED_TIER_KEYS,
    CRYPTO_ALLOWED_NETWORK_IDS,
    CRYPTO_DISCOUNT_FRACTION,
    CRYPTO_BTC_USD_RATE,
    RATE_LIMIT_CRYPTO_SUBMIT,
    YOUTUBE_REFRESH_TOKEN,
)
from database import (
    init_db, get_db, AsyncSessionLocal, User, AnonSession, ApiKey, Task, TaskLike, TaskFilePurchase,
    Scene, SceneLike, Feedback, WorkerEndpoint, YoutubeCredentials,
    TaskAnimationPurchase, TaskAnimationBundlePurchase, GumroadPurchase, RoadmapVote,
    CryptoPaymentReport,
    reset_admin_overlay_counters,
    get_or_create_admin_overlay_counters,
)
from models import (
    TaskCreateResponse, TaskStatusResponse,
    TaskHistoryItem, TaskHistoryResponse,
    UserInfo, UserNotificationSettingsUpdate, AnonInfo, AuthStatusResponse,
    ApiKeyItem, ApiKeyListResponse, ApiKeyCreateResponse,
    AdminUserListItem, AdminUserListResponse,
    AdminBalanceUpdate, AdminBalanceResponse,
    AdminUserTaskItem, AdminUserTasksResponse,
    AdminStatsResponse, AdminOverlayMetricsResponse, AdminTaskListItem, AdminTaskListResponse,
    AdminTaskInspectResponse, AdminBulkTaskIdsRequest, AdminBulkRestartCountRecentRequest,
    AdminBulkAffectedResponse,
    AdminWorkerItem, AdminWorkerListResponse, AdminWorkerCreate, AdminWorkerUpdate,
    WorkerQueueInfo, QueueStatusResponse,
    GalleryItem, GalleryResponse, LikeResponse, TaskCardInfo,
    PurchaseStateResponse, PurchaseRequest, PurchaseResponse,
    AnimationCatalogItem, AnimationCatalogResponse,
    AnimationPurchaseRequest, AnimationPurchaseResponse,
    # Scene models
    SceneCreateRequest, SceneAddModelRequest, SceneUpdateRequest,
    SceneResponse, SceneModelInfo, TransformData,
    # Feedback models
    FeedbackCreateRequest, FeedbackItem, FeedbackListResponse,
    DonationStatsResponse, RoadmapVotesResponse, RoadmapVoteRequest,
    CryptoBuyConfigResponse, CryptoNetworkItem, CryptoTierItem,
    CryptoPaymentSubmitRequest, CryptoPaymentSubmitResponse,
    # Scene list models
    SceneListItem, SceneListResponse, SceneLikeResponse,
    AgentRegisterRequest, AgentRegisterResponse, AgentMeResponse,
)
from workers import (
    get_global_queue_status,
    quarantine_worker,
    clear_worker_quarantine,
    is_worker_quarantined,
    normalize_task_type,
)
from content_moderation import build_free3d_similar_query, schedule_task_poster_classification
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
    find_file_by_pattern,
    get_stalled_processing_tasks_by_worker,
    get_task_no_progress_minutes,
    resolve_prepared_glb_source_url,
    admin_requeue_task_to_created,
)


import re
import httpx

# Throttle poster-classification recovery triggers from GET /api/task (per task_id).
_poster_recovery_throttle: Dict[str, float] = {}
POSTER_RECOVERY_THROTTLE_SEC = 20.0


def _task_needs_poster_classification(task) -> bool:
    if getattr(task, "status", None) != "done":
        return False
    if getattr(task, "content_classified_at", None) is None:
        return True
    cv = getattr(task, "content_classifier_version", None) or ""
    return ":pipeline_error" in cv


def _schedule_poster_recovery_throttled(task_id: str) -> None:
    now = time.monotonic()
    last = _poster_recovery_throttle.get(task_id, 0.0)
    if now - last < POSTER_RECOVERY_THROTTLE_SEC:
        return
    _poster_recovery_throttle[task_id] = now
    if len(_poster_recovery_throttle) > 5000:
        for k in list(_poster_recovery_throttle.keys())[:2500]:
            del _poster_recovery_throttle[k]
    schedule_task_poster_classification(task_id)


def _url_path_endswith_glb(url: str) -> bool:
    """True if URL path ends with .glb (query/fragment ignored)."""
    from urllib.parse import urlparse

    try:
        path = urlparse(url or "").path or ""
    except Exception:
        path = url or ""
    return path.lower().endswith(".glb")


FACE_RIG_ANALYZE_HEAD_PROXY_URL = os.getenv(
    "FACE_RIG_ANALYZE_HEAD_PROXY_URL",
    "https://worker-0001.free3d.online/api/face-rig/analyze-head",
)
_face_rig_analysis_locks: Dict[str, asyncio.Lock] = {}


def _get_face_rig_analysis_lock(task_id: str) -> asyncio.Lock:
    lock = _face_rig_analysis_locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _face_rig_analysis_locks[task_id] = lock
    return lock


def _load_face_rig_analysis(task: Task) -> Optional[dict]:
    raw = getattr(task, "face_rig_analysis", None)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    return data if isinstance(data, dict) else None

# =============================================================================
# Google Analytics 4 Helper
# =============================================================================
async def send_ga4_event(client_id: str, event_name: str, params: dict = None):
    """
    Send event to GA4 via Measurement Protocol.
    """
    if not GA_MEASUREMENT_ID or not GA_API_SECRET or not client_id:
        if DEBUG:
            print(f"[GA4] Skipping event '{event_name}' (missing config or client_id)")
        return
    
    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA_MEASUREMENT_ID}&api_secret={GA_API_SECRET}"
    payload = {
        "client_id": client_id,
        "events": [{
            "name": event_name,
            "params": params or {}
        }]
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if DEBUG:
                print(f"[GA4] Event '{event_name}' sent. Status: {resp.status_code}")
    except Exception as e:
        print(f"[GA4] Failed to send event '{event_name}': {e}")


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


def _try_acquire_gallery_purge_lock():
    """
    Exclusive non-blocking lock so only one uvicorn worker runs gallery DB purge at a time
    (--workers N would otherwise execute N purges in parallel).
    """
    import fcntl

    path = os.getenv("GALLERY_PURGE_LOCK_PATH", "/var/autorig/locks/gallery_purge.lock")
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception:
        path = "/tmp/autorig_gallery_purge.lock"
    f = None
    try:
        f = open(path, "a+")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except (BlockingIOError, OSError):
        if f is not None:
            try:
                f.close()
            except Exception:
                pass
        return None


def _release_gallery_purge_lock(lock_file) -> None:
    if lock_file is None:
        return
    import fcntl

    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()
    except Exception:
        pass


STALLED_ALERT_REPEAT_SECONDS = int(os.getenv("STALLED_ALERT_REPEAT_SECONDS", "3600"))
STALLED_TASK_THRESHOLD = int(os.getenv("STALLED_TASK_THRESHOLD", "1"))
STALLED_RECOVERY_HEALTHY_CYCLES = int(os.getenv("STALLED_RECOVERY_HEALTHY_CYCLES", "3"))
_stalled_worker_state: dict[str, dict] = {}


async def _monitor_stalled_workers(db: AsyncSession, queue_status=None) -> bool:
    """
    Detect workers with stale processing tasks, quarantine them and send periodic alerts.
    Returns True when at least one worker is currently stalled.
    """
    from config import STALE_TASK_TIMEOUT_MINUTES
    from telegram_bot import broadcast_worker_stalled

    now = datetime.utcnow()
    stalled_grouped = await get_stalled_processing_tasks_by_worker(
        db,
        min_stalled_minutes=STALE_TASK_TIMEOUT_MINUTES,
        queue_status=queue_status,
    )
    active_stalled_workers: set[str] = set()
    queue_by_url = {w.url: w for w in (queue_status.workers if queue_status else [])}

    for worker_url, tasks in stalled_grouped.items():
        if len(tasks) < STALLED_TASK_THRESHOLD:
            continue
        active_stalled_workers.add(worker_url)
        oldest_minutes = int(max(get_task_no_progress_minutes(t, now=now) for t in tasks) or 0)
        reason = f"stalled_tasks={len(tasks)}, oldest={oldest_minutes}m"
        quarantine_worker(worker_url, reason=reason)

        state = _stalled_worker_state.get(worker_url) or {}
        first_seen = state.get("first_seen_at") or now
        last_alert_at = state.get("last_alert_at")
        should_alert = (
            last_alert_at is None
            or (now - last_alert_at).total_seconds() >= STALLED_ALERT_REPEAT_SECONDS
        )
        if should_alert:
            sample_ids = [t.id for t in tasks[:3]]
            asyncio.create_task(
                broadcast_worker_stalled(
                    worker_url=worker_url,
                    stalled_tasks=len(tasks),
                    oldest_stalled_minutes=oldest_minutes,
                    sample_task_ids=sample_ids,
                )
            )
            last_alert_at = now

        _stalled_worker_state[worker_url] = {
            "first_seen_at": first_seen,
            "last_alert_at": last_alert_at,
            "healthy_cycles": 0,
        }

    # Recovery tracking for previously stalled workers.
    for worker_url in list(_stalled_worker_state.keys()):
        if worker_url in active_stalled_workers:
            continue
        state = _stalled_worker_state.get(worker_url) or {}
        healthy_cycles = int(state.get("healthy_cycles") or 0)
        worker_q = queue_by_url.get(worker_url)
        # Consider worker healthy when it responds and has no queue pressure.
        is_healthy_now = bool(
            worker_q
            and worker_q.available
            and worker_q.total_active == 0
            and worker_q.total_pending == 0
            and worker_q.queue_size == 0
        )
        healthy_cycles = healthy_cycles + 1 if is_healthy_now else 0
        if healthy_cycles >= STALLED_RECOVERY_HEALTHY_CYCLES:
            clear_worker_quarantine(worker_url)
            _stalled_worker_state.pop(worker_url, None)
            print(f"[Stalled Monitor] Worker recovered: {worker_url}")
        else:
            state["healthy_cycles"] = healthy_cycles
            _stalled_worker_state[worker_url] = state

    return len(active_stalled_workers) > 0

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
                queue_status = None
                force_stale_reset = False
                # =============================================================
                # 1. Queue snapshot + stall monitor, then stale reset, then dispatch
                #    (reset must run before dispatch so tasks moved to "created" post in the same tick)
                # =============================================================
                try:
                    queue_status = await get_global_queue_status(db=db)
                    try:
                        stalled_detected = await _monitor_stalled_workers(db, queue_status=queue_status)
                        if stalled_detected:
                            force_stale_reset = True
                    except Exception as e:
                        print(f"[Background Worker] Stalled monitor error: {e}")

                    if force_stale_reset or (background_worker_cycle_count % STALE_CHECK_INTERVAL_CYCLES == 0):
                        try:
                            reset_count = await find_and_reset_stale_tasks(db, queue_status=queue_status)
                            if reset_count > 0:
                                reason = "forced" if force_stale_reset else "periodic"
                                print(f"[Background Worker] Auto-reset {reset_count} stale task(s) [{reason}]")
                        except Exception as e:
                            print(f"[Background Worker] Stale task check error: {e}")

                        try:
                            sh = await process_stuck_hour_tasks(db)
                            if sh > 0:
                                print(f"[Background Worker] Stuck-hour policy: {sh} action(s)")
                        except Exception as e:
                            print(f"[Background Worker] Stuck-hour policy error: {e}")

                    free_workers = [
                        w for w in queue_status.workers
                        if (
                            w.available
                            and (w.total_active < w.max_concurrent)
                            and (w.queue_size <= 0)
                            and not is_worker_quarantined(w.url)
                        )
                    ]
                    if not free_workers:
                        fallback_workers = [
                            w for w in queue_status.workers
                            if w.available and (w.total_active < w.max_concurrent) and (w.queue_size <= 0)
                        ]
                        if fallback_workers:
                            free_workers = fallback_workers
                            print("[Background Worker] All free workers are quarantined, using degraded dispatch fallback")

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
                    else:
                        c_q = await db.execute(
                            select(func.count()).select_from(Task).where(Task.status == "created")
                        )
                        n_created = int(c_q.scalar() or 0)
                        if n_created > 0:
                            for w in queue_status.workers:
                                print(
                                    f"[Background Worker] No free worker: url={w.url} "
                                    f"available={w.available} err={w.error!r} "
                                    f"active={w.total_active} max={w.max_concurrent} "
                                    f"queue_size={w.queue_size} quarantined={is_worker_quarantined(w.url)}"
                                )
                            print(
                                f"[Background Worker] {n_created} task(s) in status=created but "
                                f"no worker passed filters (available, capacity, queue_size<=0)"
                            )
                except Exception as e:
                    print(f"[Background Worker] Queue dispatch error: {e}")

                # =============================================================
                # 2.5. Disk space cleanup (every CLEANUP_CHECK_INTERVAL_CYCLES cycles)
                # =============================================================
                if background_worker_cycle_count % CLEANUP_CHECK_INTERVAL_CYCLES == 0:
                    try:
                        from main import cleanup_disk_space
                        result = await cleanup_disk_space(
                            min_free_gb=MIN_FREE_SPACE_GB,
                            db=db,
                            delete_task_rows=AUTOMATIC_TASK_DB_DELETION,
                        )
                        if result["deleted_count"] > 0:
                            print(f"[Background Worker] Disk cleanup: freed {result['freed_gb']:.2f} GB, deleted {result['deleted_count']} items")
                    except Exception as e:
                        print(f"[Background Worker] Disk cleanup error: {e}")

                # =============================================================
                # 2.6–2.7 Gallery DB cleanup (single-worker lock; multi-round upstream)
                # Default: once per week (GALLERY_DB_PURGE_INTERVAL_CYCLES) — not every worker tick.
                # =============================================================
                if AUTOMATIC_TASK_DB_DELETION and background_worker_cycle_count % GALLERY_DB_PURGE_INTERVAL_CYCLES == 0:
                    lock_f = _try_acquire_gallery_purge_lock()
                    if lock_f is not None:
                        try:
                            upstream_deleted = 0
                            upstream_off = 0
                            for _round in range(GALLERY_UPSTREAM_PURGE_ROUNDS):
                                try:
                                    ur = await purge_gallery_upstream_dead_tasks(
                                        db,
                                        batch=GALLERY_UPSTREAM_PURGE_BATCH,
                                        offset=upstream_off,
                                    )
                                except Exception as e:
                                    print(f"[Background Worker] Gallery upstream purge error: {e}")
                                    break
                                upstream_deleted += ur["deleted"]
                                if ur["scanned"] == 0:
                                    break
                                if ur["deleted"] > 0:
                                    upstream_off = 0
                                else:
                                    upstream_off += ur["scanned"]
                            if upstream_deleted > 0:
                                print(
                                    f"[Background Worker] Gallery upstream purge: deleted {upstream_deleted} "
                                    f"task(s) in up to {GALLERY_UPSTREAM_PURGE_ROUNDS} round(s)"
                                )

                            try:
                                pr = await purge_tasks_without_poster_and_video(db)
                                if pr["deleted"] > 0:
                                    print(
                                        f"[Background Worker] Purged {pr['deleted']} stale task(s) "
                                        f"(no poster paths in JSON); scanned {pr['scanned']}"
                                    )
                            except Exception as e:
                                print(f"[Background Worker] No-assets purge error: {e}")
                        finally:
                            _release_gallery_purge_lock(lock_f)

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

                # =============================================================
                # 4. Poster classification recovery (done but content_classified_at never set)
                # =============================================================
                try:
                    async with AsyncSessionLocal() as db:
                        r2 = await db.execute(
                            select(Task.id).where(
                                Task.status == "done",
                                Task.content_classified_at.is_(None),
                            ).limit(25)
                        )
                        pending_poster = list(r2.scalars().all())
                    for tid in pending_poster:
                        try:
                            schedule_task_poster_classification(tid)
                        except Exception as e:
                            print(f"[Background Worker] Poster recovery schedule failed {tid}: {e}")
                except Exception as e:
                    print(f"[Background Worker] Poster recovery query error: {e}")
                
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

# Add GZip compression for responses > 500 bytes.
# GLB task artifact responses set Content-Encoding: identity to avoid streaming gzip + HTTP/2 issues.
app.add_middleware(GZipMiddleware, minimum_size=500)

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

_API_KEY_IDENTITY_SENTINEL = object()


async def resolve_api_key_identity(
    request: Request, db: AsyncSession
) -> Tuple[Optional[User], Optional[str]]:
    """
    Parse X-Api-Key / Authorization Bearer, validate against ApiKey rows.
    Cached per request. Updates last_used_at and commits on success.
    Returns (user, anon_id) where at most one of anon_id / user is set for a valid key.
    """
    cached = getattr(request.state, "_api_key_identity_result", _API_KEY_IDENTITY_SENTINEL)
    if cached is not _API_KEY_IDENTITY_SENTINEL:
        return cached  # type: ignore[return-value]

    api_key = request.headers.get("x-api-key")
    auth = request.headers.get("authorization") or ""
    if not api_key and auth.lower().startswith("bearer "):
        api_key = auth.split(" ", 1)[1].strip()

    if not api_key:
        out: Tuple[Optional[User], Optional[str]] = (None, None)
        request.state._api_key_identity_result = out
        return out

    prefix = None
    if api_key.startswith("ar_") and api_key.count("_") >= 2:
        try:
            prefix = api_key.split("_", 2)[1]
        except Exception:
            prefix = None
    if not prefix:
        out = (None, None)
        request.state._api_key_identity_result = out
        return out

    key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    krs = await db.execute(
        select(ApiKey).where(
            ApiKey.key_prefix == prefix,
            ApiKey.revoked_at.is_(None),
        )
    )
    key_rec = krs.scalar_one_or_none()
    if not key_rec or not hmac.compare_digest(key_rec.key_hash, key_hash):
        out = (None, None)
        request.state._api_key_identity_result = out
        return out

    key_rec.last_used_at = datetime.utcnow()
    if key_rec.user_id is not None:
        urs = await db.execute(select(User).where(User.id == key_rec.user_id))
        user = urs.scalar_one_or_none()
        await db.commit()
        out = (user, None)
        request.state._api_key_identity_result = out
        return out

    if key_rec.anon_id:
        await db.commit()
        out = (None, key_rec.anon_id)
        request.state._api_key_identity_result = out
        return out

    await db.commit()
    out = (None, None)
    request.state._api_key_identity_result = out
    return out


def _effective_anon_id(request: Request) -> Optional[str]:
    """Cookie anon session and/or API key bound to anon (Bearer without browser cookie)."""
    return getattr(request.state, "api_key_anon_id", None) or request.cookies.get(ANON_COOKIE)


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db)
) -> Optional[User]:
    """Session cookie user, or user resolved from API key; anon API keys set request.state.api_key_anon_id."""
    request.state.api_key_anon_id = None
    request.state.auth_via_api_key = False
    session_token = request.cookies.get(SESSION_COOKIE)
    if session_token:
        user = await get_user_by_session(db, session_token)
        if user:
            return user

    user, anon_id = await resolve_api_key_identity(request, db)
    request.state.api_key_anon_id = anon_id
    request.state.auth_via_api_key = bool(user is not None or anon_id is not None)
    return user


async def get_anon_session(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db)
) -> AnonSession:
    """Browser cookie session, or anon id from API key (same identity as task owner for Bearer agents)."""
    _user_k, anon_from_key = await resolve_api_key_identity(request, db)
    if anon_from_key:
        return await get_or_create_anon_session(db, anon_from_key)

    anon_id = request.cookies.get(ANON_COOKIE)

    if not anon_id:
        anon_id = str(uuid.uuid4())
        response.set_cookie(
            ANON_COOKIE,
            anon_id,
            max_age=365 * 24 * 60 * 60,  # 1 year
            httponly=True,
            samesite="lax",
        )

    return await get_or_create_anon_session(db, anon_id)


async def require_admin(
    user: Optional[User] = Depends(get_current_user)
) -> User:
    """Require admin access"""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not is_admin_email(user.email):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


async def require_login_user(
    user: Optional[User] = Depends(get_current_user),
) -> User:
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


ROADMAP_CHOICE_KEYS: Tuple[str, ...] = (
    "face_rig_animation",
    "animals_rig_animation",
    "avatar_speech_lipsync",
)


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
    if user and is_admin_email(user.email):
        return True
    if user and task.owner_type == "user" and task.owner_id == user.email:
        return True
    if anon_session and task.owner_type == "anon" and task.owner_id == anon_session.anon_id:
        return True
    return False


def _normalize_worker_url(url: str) -> str:
    url = (url or "").strip()
    # Avoid duplicate records due to trailing slash differences.
    while url.endswith("/"):
        url = url[:-1]
    return url


def _validate_worker_url(url: str) -> str:
    url = _normalize_worker_url(url)
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Invalid worker url")
    return url


# =============================================================================
# Custom Animations Catalog / Pricing
# =============================================================================
ANIMATION_SINGLE_CREDITS = 1
ANIMATION_BUNDLE_CREDITS = 10
DOWNLOAD_ALL_FILES_CREDITS = 10

ANIMATIONS_DIR = Path(__file__).resolve().parent.parent / "static" / "all_animations"
ANIMATIONS_MANIFEST_PATH = ANIMATIONS_DIR / "manifest.json"
GUID_PREFIX_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_',
    re.IGNORECASE
)
GUID_ANY_RE = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
    re.IGNORECASE
)

_ANIM_CATALOG_CACHE: Dict[str, Any] = {
    "mtime_ns": None,
    "data": None,
}


def _normalize_animation_key(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r'[^a-z0-9]+', '_', value)
    value = re.sub(r'_+', '_', value).strip('_')
    return value


def _strip_guid_prefix(filename: str) -> str:
    return GUID_PREFIX_RE.sub('', filename or '')


def _extract_guid_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = GUID_ANY_RE.search(text)
    return m.group(0) if m else None


def _infer_worker_root_and_guid(task) -> tuple[Optional[str], Optional[str]]:
    """
    Infer worker root (http://host:port/converter/glb) and GUID for a task.
    Uses task.worker_api/task.guid first, then falls back to progress_page and URLs.
    """
    worker_root: Optional[str] = None
    guid: Optional[str] = (task.guid or "").strip() or None

    worker_api = (task.worker_api or "").strip()
    if worker_api:
        if "/api-converter-glb" in worker_api:
            worker_root = worker_api.split("/api-converter-glb", 1)[0].rstrip("/") + "/converter/glb"
        elif "/converter/glb" in worker_api:
            worker_root = worker_api.split("/converter/glb", 1)[0].rstrip("/") + "/converter/glb"
        else:
            parsed = urlparse(worker_api)
            if parsed.scheme and parsed.netloc:
                worker_root = f"{parsed.scheme}://{parsed.netloc}/converter/glb"
        if not guid:
            guid = _extract_guid_from_text(worker_api)

    progress_page = (task.progress_page or "").strip()
    if progress_page:
        m = re.search(r'^(https?://[^/]+)/converter/glb/([0-9a-fA-F\-]{36})/', progress_page)
        if not m:
            m = re.search(
                r'^(https?://[^/]+)/converter/glb/([0-9a-fA-F\-]{36})\.zip',
                progress_page,
            )
        if m:
            if not worker_root:
                worker_root = f"{m.group(1)}/converter/glb"
            if not guid:
                guid = m.group(2)
        if not guid:
            guid = _extract_guid_from_text(progress_page)

    if not worker_root or not guid:
        for u in list(task.output_urls or []) + list(task.ready_urls or []):
            if not isinstance(u, str):
                continue
            url = u.strip()
            if not url:
                continue
            m = re.search(r'^(https?://[^/]+)/converter/glb/([0-9a-fA-F\-]{36})/', url)
            if not m:
                m = re.search(
                    r'^(https?://[^/]+)/converter/glb/([0-9a-fA-F\-]{36})\.zip',
                    url,
                )
            if m:
                if not worker_root:
                    worker_root = f"{m.group(1)}/converter/glb"
                if not guid:
                    guid = m.group(2)
                if worker_root and guid:
                    break
            if not guid:
                guid = _extract_guid_from_text(url)

    return worker_root, guid


def _task_urls_indicate_animation_bundle(combined_urls: List[str]) -> bool:
    """
    True if task output/ready URLs include a combined animations deliverable (GLB or FBX pack).
    Used to allow synthetic per-animation FBX URLs when the worker model-files API is empty.
    Previously only _all_animations_unity.fbx was detected; many pipelines only expose *_all_animations.glb.
    """
    for u in combined_urls:
        if not u or not isinstance(u, str):
            continue
        ul = u.lower()
        if "_all_animations" not in ul:
            continue
        if ".glb" in ul or ul.endswith(".glb"):
            return True
        if ".fbx" in ul or ul.endswith(".fbx"):
            return True
    return False


def _task_urls_suggest_100k_animation_layout(combined_urls: List[str]) -> bool:
    """True if any task URL points at the common .../{guid}_100k/... layout for FBX outputs."""
    for u in combined_urls:
        if not u or not isinstance(u, str):
            continue
        if "_100k" in u.lower():
            return True
    return False


def _upsert_animation_file_map(result: Dict[str, dict], key: str, rec: dict) -> None:
    """
    Merge records with priority:
    1) ready real files
    2) non-ready real files
    3) synthetic files
    """
    existing = result.get(key)
    if not existing:
        result[key] = rec
        return

    existing_score = (2 if not existing.get("synthetic") else 0) + (1 if existing.get("ready") else 0)
    rec_score = (2 if not rec.get("synthetic") else 0) + (1 if rec.get("ready") else 0)
    if rec_score > existing_score:
        result[key] = rec


def _add_animation_file_url_to_map(
    *,
    result: Dict[str, dict],
    file_url: str,
    ready: bool,
    index: Optional[int] = None,
    synthetic: bool = False
) -> None:
    raw_name = unquote((file_url or "").split("/")[-1])
    if not raw_name.lower().endswith(".fbx"):
        return

    clean_name = _strip_guid_prefix(raw_name)
    stem = clean_name[:-4] if clean_name.lower().endswith(".fbx") else clean_name
    stem_l = stem.lower()
    # Exclude package-level animation files; keep only single animation files.
    if "_all_animations" in stem_l:
        return

    key = _normalize_animation_key(stem)
    if not key:
        return

    rec = {
        "url": file_url,
        "raw_filename": raw_name,
        "clean_filename": clean_name,
        "index": index,
        "ready": bool(ready),
        "synthetic": bool(synthetic),
    }
    _upsert_animation_file_map(result, key, rec)


async def _fetch_worker_animation_file_urls(worker_root: str, guid: str) -> List[str]:
    """
    Fetch worker file listing via /api-converter-glb/model-files/{guid}
    and return URLs for all .fbx files discovered.
    """
    if not worker_root or not guid:
        return []
    api_base = worker_root.replace("/converter/glb", "")
    files_url = f"{api_base}/api-converter-glb/model-files/{guid}"
    out: List[str] = []
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(files_url, timeout=5.0)
        if resp.status_code != 200:
            return []
        data = resp.json()
        folders = data.get("folders", {})
        if not isinstance(folders, dict):
            return []
        for folder_data in folders.values():
            if not isinstance(folder_data, dict):
                continue
            for f in (folder_data.get("files") or []):
                if not isinstance(f, dict):
                    continue
                rel_path = str(f.get("rel_path") or "").strip()
                if not rel_path:
                    continue
                if not rel_path.lower().endswith(".fbx"):
                    continue
                # Keep slashes in rel_path (e.g. nested folders), encode only unsafe chars.
                out.append(f"{worker_root}/{guid}/{quote(rel_path, safe='/')}")
    except Exception:
        return []
    return out


def _load_animation_manifest() -> dict:
    """Load custom animation manifest from static/all_animations with lightweight cache."""
    try:
        if not ANIMATIONS_MANIFEST_PATH.exists():
            return {
                "version": 1,
                "types": [],
                "pricing": {
                    "single_animation_credits": ANIMATION_SINGLE_CREDITS,
                    "all_animations_credits": ANIMATION_BUNDLE_CREDITS,
                    "download_format": "fbx",
                },
                "animations": [],
            }

        mtime_ns = ANIMATIONS_MANIFEST_PATH.stat().st_mtime_ns
        if _ANIM_CATALOG_CACHE["data"] is not None and _ANIM_CATALOG_CACHE["mtime_ns"] == mtime_ns:
            return _ANIM_CATALOG_CACHE["data"]

        raw = ANIMATIONS_MANIFEST_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Manifest must be an object")

        data.setdefault("types", [])
        data.setdefault("animations", [])
        data.setdefault("pricing", {
            "single_animation_credits": ANIMATION_SINGLE_CREDITS,
            "all_animations_credits": ANIMATION_BUNDLE_CREDITS,
            "download_format": "fbx",
        })

        _ANIM_CATALOG_CACHE["mtime_ns"] = mtime_ns
        _ANIM_CATALOG_CACHE["data"] = data
        return data
    except Exception as e:
        print(f"[Animations] Failed to load manifest: {e}")
        return {
            "version": 1,
            "types": [],
            "pricing": {
                "single_animation_credits": ANIMATION_SINGLE_CREDITS,
                "all_animations_credits": ANIMATION_BUNDLE_CREDITS,
                "download_format": "fbx",
            },
            "animations": [],
        }


async def _build_task_animation_file_map(
    task,
    manifest_items: Optional[List[dict]] = None
) -> Dict[str, dict]:
    """
    Build map of normalized animation key -> worker file metadata for individual FBX animations.
    Sources:
    1) task.output_urls/task.ready_urls
    2) worker model-files API
    3) synthesized GUID-based URLs from manifest (fallback)
    """
    result: Dict[str, dict] = {}

    output_urls = [u.strip() for u in (task.output_urls or []) if isinstance(u, str)]
    ready_urls = [u.strip() for u in (task.ready_urls or []) if isinstance(u, str)]
    ready_url_set = set(ready_urls)
    output_url_set = set(output_urls)
    terminal = getattr(task, "status", None) == "done"

    # Use both output and ready URLs, preserving order and uniqueness.
    combined_urls: List[str] = []
    seen = set()
    for u in output_urls + ready_urls:
        if u and u not in seen:
            seen.add(u)
            combined_urls.append(u)

    for idx, file_url in enumerate(combined_urls):
        url_ready = file_url in ready_url_set or (terminal and file_url in output_url_set)
        _add_animation_file_url_to_map(
            result=result,
            file_url=file_url,
            ready=url_ready,
            index=idx,
            synthetic=False
        )

    worker_root, guid = _infer_worker_root_and_guid(task)
    worker_file_urls: List[str] = []
    if worker_root and guid:
        worker_file_urls = await _fetch_worker_animation_file_urls(worker_root, guid)
        for idx, file_url in enumerate(worker_file_urls):
            _add_animation_file_url_to_map(
                result=result,
                file_url=file_url,
                ready=True,
                index=None,
                synthetic=False
            )

    # Synthesize URLs by convention: /{guid}/{guid}_{animation_name}.fbx or under {guid}_100k/
    # Only enabled when task likely contains generated animation set.
    has_animation_bundle = _task_urls_indicate_animation_bundle(combined_urls)
    use_100k = _task_urls_suggest_100k_animation_layout(combined_urls)
    # Important: if worker model-files API returns concrete list, trust it and avoid
    # marking missing animations as available via synthetic guesses.
    can_synthesize = bool(worker_root and guid and manifest_items and has_animation_bundle and not worker_file_urls)
    if can_synthesize:
        for item in (manifest_items or []):
            if not isinstance(item, dict):
                continue
            if item.get("enabled", True) is False:
                continue
            anim_name = None
            src = item.get("source_file")
            if isinstance(src, str) and src.strip():
                anim_name = Path(src).stem
            if not anim_name:
                raw_name = str(item.get("name") or "").strip()
                anim_name = raw_name
            if not anim_name:
                continue

            key = _normalize_animation_key(str(item.get("id") or anim_name))
            if not key:
                continue
            file_name = f"{guid}_{anim_name}.fbx"
            if use_100k:
                file_url = f"{worker_root}/{guid}/{guid}_100k/{quote(file_name)}"
            else:
                file_url = f"{worker_root}/{guid}/{quote(file_name)}"
            _add_animation_file_url_to_map(
                result=result,
                file_url=file_url,
                ready=(task.status == "done"),
                index=None,
                synthetic=True
            )

    return result


def _resolve_animation_file(item: dict, file_map: Dict[str, dict]) -> Optional[dict]:
    """Resolve animation metadata item to the best matching task file."""
    candidates: List[str] = []

    for field in ("id", "name"):
        if isinstance(item.get(field), str):
            candidates.append(_normalize_animation_key(item[field]))

    aliases = item.get("aliases") or []
    if isinstance(aliases, list):
        for alias in aliases:
            if isinstance(alias, str):
                candidates.append(_normalize_animation_key(alias))

    src = item.get("source_file")
    if isinstance(src, str):
        src_stem = Path(src).stem
        candidates.append(_normalize_animation_key(src_stem))

    # Unique, keep order
    uniq = []
    for c in candidates:
        if c and c not in uniq:
            uniq.append(c)

    for c in uniq:
        if c in file_map:
            return file_map[c]

    # Fuzzy fallback (useful if worker adds prefixes/suffixes)
    for c in uniq:
        for k, v in file_map.items():
            if k == c or k.endswith(f"_{c}") or c.endswith(f"_{k}") or c in k:
                return v

    return None


def _resolve_worker_files_api_context(task) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve context for worker files API:
    returns (api_base, guid), where api_base is like http://host:port
    """
    worker_root, guid = _infer_worker_root_and_guid(task)
    if not worker_root or not guid:
        return None, None
    api_base = worker_root.replace("/converter/glb", "")
    return api_base, guid


async def _get_animation_purchase_state(db: AsyncSession, user: Optional[User], task_id: str) -> tuple[set, bool]:
    """Return (purchased_ids, purchased_all) for custom animations."""
    if not user:
        return set(), False

    purchased_ids = set()
    try:
        rows = await db.execute(
            select(TaskAnimationPurchase.animation_id).where(
                TaskAnimationPurchase.task_id == task_id,
                TaskAnimationPurchase.user_email == user.email
            )
        )
        purchased_ids = {r[0] for r in rows.all() if r and r[0]}
    except Exception:
        purchased_ids = set()

    purchased_all = False
    try:
        bundle = await db.execute(
            select(TaskAnimationBundlePurchase.id).where(
                TaskAnimationBundlePurchase.task_id == task_id,
                TaskAnimationBundlePurchase.user_email == user.email
            )
        )
        purchased_all = bundle.scalar_one_or_none() is not None
    except Exception:
        purchased_all = False

    # Backward compatibility: old "buy all files" purchase unlocks animation downloads too.
    if not purchased_all:
        legacy = await db.execute(
            select(TaskFilePurchase.id).where(
                TaskFilePurchase.task_id == task_id,
                TaskFilePurchase.user_email == user.email,
                TaskFilePurchase.file_index.is_(None)
            )
        )
        purchased_all = legacy.scalar_one_or_none() is not None

    return purchased_ids, purchased_all


async def _has_full_task_download_purchase(db: AsyncSession, user: Optional[User], task_id: str) -> bool:
    """True if user bought full-task download (TaskFilePurchase with file_index NULL)."""
    if not user:
        return False
    row = await db.execute(
        select(TaskFilePurchase.id).where(
            TaskFilePurchase.task_id == task_id,
            TaskFilePurchase.user_email == user.email,
            TaskFilePurchase.file_index.is_(None),
        ).limit(1)
    )
    return row.scalar_one_or_none() is not None


def resolve_worker_full_bundle_zip_url(task: Task) -> Optional[str]:
    """
    Absolute URL to the worker-built full bundle: {worker_root}/{guid}.zip
    (worker_root is http://host/converter/glb — same inference as gallery / artifacts).
    """
    worker_root, inferred_guid = _infer_worker_root_and_guid(task)
    guid = ((getattr(task, "guid", None) or "") or "").strip() or inferred_guid
    if not worker_root or not guid:
        return None
    return f"{worker_root.rstrip('/')}/{guid}.zip"


async def _credit_task_owner_for_sale(db: AsyncSession, task, buyer_user: User, amount: int) -> None:
    """Credit task owner when a paid download/purchase happens."""
    if amount <= 0:
        return
    if task.owner_type != "user" or not task.owner_id:
        return
    owner_result = await db.execute(select(User).where(User.email == task.owner_id))
    task_owner = owner_result.scalar_one_or_none()
    if task_owner and task_owner.id != buyer_user.id:
        task_owner.balance_credits += amount


# =============================================================================
# Authentication Endpoints
# =============================================================================
@app.get("/auth/login")
async def auth_login(request: Request, next: Optional[str] = None):
    """Redirect to Google OAuth"""
    state = str(uuid.uuid4())
    auth_url = get_google_auth_url(state)
    
    response = RedirectResponse(url=auth_url)
    # Save return URL in cookie (max 5 minutes) for redirect after OAuth
    if next and next.startswith("/"):  # Security: only allow relative URLs
        response.set_cookie("auth_next", next, max_age=300, httponly=True, samesite="lax")
    return response


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
    
    # Get return URL from cookie (saved during /auth/login)
    next_url = request.cookies.get("auth_next", "/")
    # Security: ensure it's a relative URL
    if not next_url.startswith("/"):
        next_url = "/"
    
    # Set session cookie and redirect to original page
    redirect = RedirectResponse(url=next_url, status_code=302)
    redirect.set_cookie(
        SESSION_COOKIE,
        session_token,
        max_age=30*24*60*60,  # 30 days
        httponly=True,
        secure=True,
        samesite="lax"
    )
    # Clean up auth_next cookie
    redirect.delete_cookie("auth_next")
    
    return redirect


@app.get("/api/admin/youtube/oauth/start")
async def admin_youtube_oauth_start(
    request: Request,
    admin: User = Depends(require_admin),
):
    """Redirect to Google OAuth (youtube.upload scope) to connect the channel for auto-uploads."""
    from youtube_upload import build_youtube_authorize_url

    state = secrets.token_urlsafe(32)
    response = RedirectResponse(url=build_youtube_authorize_url(state))
    response.set_cookie(
        "yt_oauth_state",
        state,
        max_age=600,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@app.get("/api/oauth/youtube/callback")
async def admin_youtube_oauth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_current_user),
):
    """OAuth callback: stores refresh token for YouTube uploads."""
    from youtube_upload import exchange_youtube_code_for_tokens, save_youtube_refresh_token

    if error:
        return RedirectResponse(url=f"/?youtube_error={quote(error)}")
    if not user or not is_admin_email(user.email):
        return RedirectResponse(url="/?youtube_error=not_admin")
    cookie_state = request.cookies.get("yt_oauth_state")
    if not state or not cookie_state or state != cookie_state:
        return RedirectResponse(url="/?youtube_error=state")
    if not code:
        return RedirectResponse(url="/?youtube_error=no_code")
    tokens = await exchange_youtube_code_for_tokens(code)
    if not tokens:
        return RedirectResponse(url="/?youtube_error=token_exchange")
    refresh = tokens.get("refresh_token")
    if not refresh:
        return RedirectResponse(url="/?youtube_error=no_refresh_token_reauthorize_with_prompt")
    await save_youtube_refresh_token(db, refresh)
    response = RedirectResponse(url="/?youtube_connected=1")
    response.delete_cookie("yt_oauth_state")
    return response


@app.get("/api/admin/youtube/status")
async def admin_youtube_status(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Whether YouTube channel OAuth is stored."""
    row = await db.get(YoutubeCredentials, 1)
    connected = bool((row and row.refresh_token) or YOUTUBE_REFRESH_TOKEN)
    updated_at = row.updated_at.isoformat() + "Z" if row and row.updated_at else None
    return {"connected": connected, "updated_at": updated_at}


@app.post("/api/admin/youtube/upload-task/{task_id}")
async def admin_youtube_upload_task(
    task_id: str,
    admin: User = Depends(require_admin),
):
    """Queue a YouTube upload for this task (must be done, safe rating, video ready)."""
    from youtube_upload import run_youtube_upload_for_task

    asyncio.create_task(run_youtube_upload_for_task(task_id))
    return {"ok": True, "task_id": task_id, "message": "upload_scheduled"}


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
                youtube_bonus_received=user.youtube_bonus_received,
                is_admin=user.is_admin,
                email_task_completed=user.email_task_completed,
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
        login_required=False
    )


@app.patch("/api/user/notification-settings", response_model=UserInfo)
async def api_user_notification_settings(
    body: UserNotificationSettingsUpdate,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update email notification preferences (signed-in users only)."""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    user.email_task_completed = body.email_task_completed
    await db.commit()
    await db.refresh(user)
    return UserInfo(
        id=user.id,
        email=user.email,
        name=user.name,
        picture=user.picture,
        balance_credits=user.balance_credits,
        total_tasks=user.total_tasks,
        youtube_bonus_received=user.youtube_bonus_received,
        is_admin=user.is_admin,
        email_task_completed=user.email_task_completed,
    )


@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe_email(
    token: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """One-click unsubscribe from task-ready emails (signed token)."""
    from unsubscribe_tokens import verify_unsubscribe_token

    if not token:
        raise HTTPException(status_code=400, detail="Missing token")
    email = verify_unsubscribe_token(token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid token")
    rs = await db.execute(select(User).where(User.email == email))
    row = rs.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=400, detail="Invalid token")
    row.email_task_completed = False
    await db.commit()
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Unsubscribed — AutoRig.online</title>
  <link rel="stylesheet" href="/static/css/styles.css">
</head>
<body style="margin:0;padding:2rem;font-family:system-ui,sans-serif;background:var(--bg,#0a0a0f);color:var(--text,#f0f0f5);">
  <div style="max-width:520px;margin:0 auto;">
    <h1 style="font-size:1.35rem;margin-top:0;">You are unsubscribed</h1>
    <p style="color:var(--text-secondary,#a0a0b0);line-height:1.5;">
      You will no longer receive emails when your rigging tasks are ready.
      You can turn this back on anytime in your dashboard.
    </p>
    <p style="margin-top:1.5rem;">
      <a href="{APP_URL}/dashboard" style="color:#6366f1;">Notification settings</a>
      &nbsp;·&nbsp;
      <a href="{APP_URL}" style="color:#6366f1;">Home</a>
    </p>
    <p style="color:#606070;font-size:0.85rem;margin-top:2rem;">
      Отписка оформлена. Письма о готовности задач больше не будут отправляться.
    </p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)


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
    request: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if user:
        rs = await db.execute(select(ApiKey).where(ApiKey.user_id == user.id).order_by(ApiKey.created_at.desc()))
    else:
        anon_id = request.cookies.get(ANON_COOKIE)
        if not anon_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        rs = await db.execute(select(ApiKey).where(ApiKey.anon_id == anon_id).order_by(ApiKey.created_at.desc()))
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
    request: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if user:
        rs = await db.execute(
            select(ApiKey).where(ApiKey.user_id == user.id, ApiKey.revoked_at.is_(None))
        )
    else:
        anon_id = request.cookies.get(ANON_COOKIE)
        if not anon_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        rs = await db.execute(
            select(ApiKey).where(ApiKey.anon_id == anon_id, ApiKey.revoked_at.is_(None))
        )
    active = rs.scalars().all()
    now = datetime.utcnow()
    for k in active:
        k.revoked_at = now

    api_key, prefix, key_hash = _make_api_key()
    if user:
        rec = ApiKey(user_id=user.id, anon_id=None, key_prefix=prefix, key_hash=key_hash)
    else:
        rec = ApiKey(user_id=None, anon_id=anon_id, key_prefix=prefix, key_hash=key_hash)
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
    request: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if user:
        rs = await db.execute(select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user.id))
    else:
        anon_id = request.cookies.get(ANON_COOKIE)
        if not anon_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        rs = await db.execute(select(ApiKey).where(ApiKey.id == key_id, ApiKey.anon_id == anon_id))
    rec = rs.scalar_one_or_none()
    if not rec:
        raise HTTPException(status_code=404, detail="API key not found")
    if rec.revoked_at is None:
        rec.revoked_at = datetime.utcnow()
        await db.commit()
    return {"ok": True, "key_id": key_id}


# =============================================================================
# AI agents (register without Google; Bearer API key)
# =============================================================================
def _skill_md_candidate_paths() -> List[Path]:
    """Primary: autorig-online/skill.md next to backend/; fallback: parent of repo root."""
    here = Path(__file__).resolve().parent
    return [here.parent.parent / "skill.md", here.parent.parent.parent / "skill.md"]


@app.post("/api/agents/register", response_model=AgentRegisterResponse)
@limiter.limit(RATE_LIMIT_AGENT_REGISTER)
async def api_agents_register(
    request: Request,
    body: AgentRegisterRequest,
    db: AsyncSession = Depends(get_db),
):
    anon_id = str(uuid.uuid4())
    name = (body.name or "").strip() or None
    desc = (body.description or "").strip() or None
    sess = AnonSession(
        anon_id=anon_id,
        agent_name=name,
        agent_description=desc,
        registered_as_agent=True,
    )
    db.add(sess)
    api_key, prefix, key_hash = _make_api_key()
    db.add(ApiKey(user_id=None, anon_id=anon_id, key_prefix=prefix, key_hash=key_hash))
    await db.commit()
    return AgentRegisterResponse(api_key=api_key, agent_id=anon_id)


@app.get("/api/agents/me", response_model=AgentMeResponse)
async def api_agents_me(
    request: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if user:
        raise HTTPException(
            status_code=400,
            detail="Use GET /auth/me for Google accounts; this endpoint is for agent API keys only.",
        )
    anon_id = getattr(request.state, "api_key_anon_id", None)
    if not anon_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required (Bearer agent API key or X-Api-Key)",
        )
    rs = await db.execute(select(AnonSession).where(AnonSession.anon_id == anon_id))
    sess = rs.scalar_one_or_none()
    if not sess:
        raise HTTPException(status_code=404, detail="Agent session not found")
    remaining = get_remaining_credits_anon(sess)
    note = (
        "Credit balance and paid file purchases are tied to Google sign-in on the website. "
        "This agent id uses the anonymous free tier only (see free_remaining)."
    )
    return AgentMeResponse(
        agent_id=anon_id,
        name=sess.agent_name,
        description=sess.agent_description,
        registered_as_agent=bool(sess.registered_as_agent),
        free_used=int(sess.free_used or 0),
        free_remaining=remaining,
        account_note=note,
    )


# =============================================================================
# Buy-credits: donations (Gumroad) & roadmap votes
# =============================================================================
@app.get("/api/buy-credits/donation-stats", response_model=DonationStatsResponse)
async def api_buy_credits_donation_stats(db: AsyncSession = Depends(get_db)):
    """Sum successful AutoRig credit-pack sales (gumroad_purchases) in USD + optional baseline."""
    lowered = [k.lower() for k in AUTORIG_DONATION_PRODUCT_KEYS]
    row = (
        await db.execute(
            select(
                func.coalesce(func.sum(GumroadPurchase.price), 0),
                func.count(),
            ).where(
                GumroadPurchase.refunded.is_(False),
                GumroadPurchase.test.is_(False),
                func.lower(GumroadPurchase.product_permalink).in_(lowered),
            )
        )
    ).one()
    sum_cents = int(row[0] or 0)
    purchase_count = int(row[1] or 0)
    raised = DONATION_BASELINE_USD + (sum_cents / 100.0)
    return DonationStatsResponse(
        raised_usd=round(raised, 2),
        goal_usd=DONATION_GOAL_USD,
        currency="USD",
        purchase_count=purchase_count,
    )


def _build_crypto_buy_config_response() -> CryptoBuyConfigResponse:
    disc = CRYPTO_DISCOUNT_FRACTION
    rate = CRYPTO_BTC_USD_RATE
    if rate <= 0:
        rate = 95000.0
    tiers_out: List[CryptoTierItem] = []
    for key, credits, usd_std in AUTORIG_CRYPTO_TIERS:
        usd_disc = round(usd_std * (1.0 - disc), 2)
        usdt_amt = usd_disc
        btc_approx = round(usd_disc / rate, 8)
        per = round(usd_disc / max(credits, 1), 4)
        tiers_out.append(
            CryptoTierItem(
                tier_key=key,
                credits=credits,
                usd_standard=usd_std,
                usd_discounted=usd_disc,
                usdt_amount=usdt_amt,
                btc_amount_approx=btc_approx,
                usd_per_credit_discounted=per,
            )
        )
    nets = [
        CryptoNetworkItem(
            id=n["id"],
            label=n["label"],
            asset=n["asset"],
            address=n["address"],
            warning=n["warning"],
        )
        for n in CRYPTO_RECEIVE_NETWORKS
    ]
    return CryptoBuyConfigResponse(
        discount_fraction=disc,
        btc_usd_rate=rate,
        networks=nets,
        tiers=tiers_out,
    )


@app.get("/api/buy-credits/crypto-config", response_model=CryptoBuyConfigResponse)
async def api_buy_credits_crypto_config():
    """Public tiers, discount math, and receive addresses for UI and AI agents."""
    return _build_crypto_buy_config_response()


@app.post("/api/buy-credits/crypto-submit", response_model=CryptoPaymentSubmitResponse)
@limiter.limit(RATE_LIMIT_CRYPTO_SUBMIT)
async def api_buy_credits_crypto_submit(
    request: Request,
    body: CryptoPaymentSubmitRequest,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Report a crypto payment tx for manual verification and crediting."""
    t = body.tier.strip().lower()
    if t not in CRYPTO_ALLOWED_TIER_KEYS:
        raise HTTPException(status_code=400, detail="Invalid tier")
    nid = body.network_id.strip().lower()
    if nid not in CRYPTO_ALLOWED_NETWORK_IDS:
        raise HTTPException(status_code=400, detail="Invalid network_id")
    tx = body.tx_id.strip()
    if len(tx) < 8 or len(tx) > 256:
        raise HTTPException(status_code=400, detail="Invalid tx_id")

    agent_anon = getattr(request.state, "api_key_anon_id", None)
    uemail = user.email if user else None
    if not uemail and not agent_anon:
        note = (body.contact_note or "").strip()
        if len(note) < 5:
            raise HTTPException(
                status_code=400,
                detail="contact_note required when not authenticated (include email or agent id for crediting)",
            )

    row = CryptoPaymentReport(
        tier=t,
        network_id=nid,
        tx_id=tx[:256],
        contact_note=(body.contact_note or "").strip() or None,
        user_email=uemail,
        agent_anon_id=agent_anon,
        status="pending",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)

    from telegram_bot import broadcast_crypto_payment_submitted

    asyncio.create_task(
        broadcast_crypto_payment_submitted(
            report_id=row.id,
            tier=t,
            network_id=nid,
            tx_id=tx,
            user_email=uemail,
            agent_anon_id=agent_anon,
            contact_note=row.contact_note,
        )
    )

    return CryptoPaymentSubmitResponse(id=row.id)


@app.get("/api/roadmap/votes", response_model=RoadmapVotesResponse)
async def api_roadmap_votes_get(
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_current_user),
):
    counts = {k: 0 for k in ROADMAP_CHOICE_KEYS}
    agg = await db.execute(
        select(RoadmapVote.choice, func.count())
        .where(RoadmapVote.choice.in_(ROADMAP_CHOICE_KEYS))
        .group_by(RoadmapVote.choice)
    )
    for choice, n in agg.all():
        if choice in counts:
            counts[choice] = int(n)
    my_choice: Optional[str] = None
    if user:
        rv = await db.execute(select(RoadmapVote).where(RoadmapVote.user_id == user.id))
        rec = rv.scalar_one_or_none()
        if rec and rec.choice in counts:
            my_choice = rec.choice
    return RoadmapVotesResponse(
        counts=counts,
        choice_order=list(ROADMAP_CHOICE_KEYS),
        my_choice=my_choice,
    )


@app.post("/api/roadmap/vote", response_model=RoadmapVotesResponse)
async def api_roadmap_vote_post(
    body: RoadmapVoteRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_login_user),
):
    choice = body.choice.strip().lower()
    if choice not in ROADMAP_CHOICE_KEYS:
        raise HTTPException(status_code=400, detail="Invalid choice")
    rs = await db.execute(select(RoadmapVote).where(RoadmapVote.user_id == user.id))
    rec = rs.scalar_one_or_none()
    now = datetime.utcnow()
    if rec:
        rec.choice = choice
        rec.updated_at = now
    else:
        db.add(RoadmapVote(user_id=user.id, choice=choice, updated_at=now))
    await db.commit()
    return await api_roadmap_votes_get(db=db, user=user)


# =============================================================================
# YouTube Bonus & Feedback
# =============================================================================
@app.post("/api/user/grant-youtube-bonus")
async def grant_youtube_bonus(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Grant 10 credits for YouTube link click (once per user)"""
    if user.youtube_bonus_received:
        return {"ok": False, "already_received": True}
    
    user.balance_credits += 10
    user.youtube_bonus_received = True
    await db.commit()
    
    from telegram_bot import broadcast_youtube_bonus_click
    asyncio.create_task(broadcast_youtube_bonus_click(user.email))
    
    return {"ok": True, "new_balance": user.balance_credits}


@app.post("/api/user/feedback")
async def submit_feedback(
    req: FeedbackCreateRequest,
    user: User = Depends(require_login_user),
    db: AsyncSession = Depends(get_db)
):
    """Submit user feedback"""
    parent_id = req.parent_id
    if parent_id is not None:
        chk = await db.execute(select(Feedback).where(Feedback.id == parent_id))
        if chk.scalar_one_or_none() is None:
            raise HTTPException(status_code=400, detail="Invalid parent_id")
    fb = Feedback(
        user_email=user.email,
        user_name=user.name or user.email,
        text=req.text,
        parent_id=parent_id,
    )
    db.add(fb)
    await db.commit()

    from telegram_bot import broadcast_feedback_submitted
    asyncio.create_task(broadcast_feedback_submitted(user.email, req.text))

    return {"ok": True}


def _feedback_avatar_url(email: str, oauth_picture: Optional[str]) -> str:
    if oauth_picture and str(oauth_picture).strip():
        return str(oauth_picture).strip()
    h = hashlib.md5((email or "").strip().lower().encode("utf-8")).hexdigest()
    return f"https://www.gravatar.com/avatar/{h}?d=identicon&s=96"


def _feedback_parent_preview(text: str, max_len: int = 100) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


@app.get("/api/feedback", response_model=FeedbackListResponse)
async def get_feedback_list(
    db: AsyncSession = Depends(get_db)
):
    """Get all feedback items (with user avatars when linked to a Google OAuth user)."""
    stmt = (
        select(Feedback, User.picture)
        .outerjoin(User, func.lower(User.email) == func.lower(Feedback.user_email))
        .order_by(Feedback.created_at.desc())
    )
    rows = (await db.execute(stmt)).all()
    parent_ids = {fb.parent_id for fb, _pic in rows if getattr(fb, "parent_id", None)}
    parent_map: Dict[int, Feedback] = {}
    if parent_ids:
        pr = await db.execute(select(Feedback).where(Feedback.id.in_(parent_ids)))
        for p in pr.scalars().all():
            parent_map[p.id] = p

    out: List[FeedbackItem] = []
    for fb, user_picture in rows:
        parent_user_name = None
        parent_preview = None
        pid = getattr(fb, "parent_id", None)
        if pid and pid in parent_map:
            par = parent_map[pid]
            parent_user_name = par.user_name or par.user_email
            parent_preview = _feedback_parent_preview(par.text)
        out.append(
            FeedbackItem(
                id=fb.id,
                user_email=fb.user_email,
                user_name=fb.user_name,
                text=fb.text,
                created_at=fb.created_at,
                parent_id=pid,
                user_picture=_feedback_avatar_url(fb.user_email, user_picture),
                parent_user_name=parent_user_name,
                parent_preview=parent_preview,
            )
        )
    return FeedbackListResponse(items=out)


@app.delete("/api/feedback/{feedback_id}")
async def delete_feedback(
    feedback_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete feedback (admin only); replies referencing this row are removed first."""
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")

    result = await db.execute(select(Feedback).where(Feedback.id == feedback_id))
    fb = result.scalar_one_or_none()
    if not fb:
        raise HTTPException(status_code=404, detail="Feedback not found")

    await db.execute(delete(Feedback).where(Feedback.parent_id == feedback_id))
    await db.delete(fb)
    await db.commit()
    return {"ok": True}


# =============================================================================
# Task Endpoints
# =============================================================================
@app.post("/api/task/create", response_model=TaskCreateResponse)
@limiter.limit(f"{RATE_LIMIT_TASKS_PER_MINUTE}/minute")
async def api_create_task(
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new conversion task.

    - ``application/json``: ``{"input_url": "https://..."}`` is enough; ``source`` defaults to ``link``,
      ``type`` to ``t_pose``. Optional: ``source``, ``type``, ``ga_client_id``.
    - ``multipart/form-data`` / ``application/x-www-form-urlencoded``: same fields as before;
      ``source`` defaults to ``link`` if omitted. Use ``source=upload`` + ``file`` for uploads.
    """
    via_api = bool(getattr(request.state, "auth_via_api_key", False))
    api_key_anon = getattr(request.state, "api_key_anon_id", None)
    if user:
        owner_type = "user"
        owner_id = user.email
    elif api_key_anon:
        owner_type = "anon"
        owner_id = api_key_anon
    else:
        anon_session = await get_anon_session(request, response, db)
        owner_type = "anon"
        owner_id = anon_session.anon_id

    content_type = (request.headers.get("content-type") or "").lower()
    source = "link"
    input_url: Optional[str] = None
    input_type = "t_pose"
    ga_client_id: Optional[str] = None
    file: Optional[UploadFile] = None
    pipeline = "rig"
    uploaded_bytes: Optional[int] = None

    if "application/json" in content_type:
        try:
            data = await request.json()
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="JSON body must be an object")
        raw_source = data.get("source")
        source = str(raw_source).strip() if raw_source not in (None, "") else "link"
        raw_url = data.get("input_url")
        if raw_url is not None and str(raw_url).strip():
            input_url = str(raw_url).strip()
        raw_t = data.get("type")
        if raw_t is None or not str(raw_t).strip():
            raw_t = data.get("input_type")
        if raw_t is not None and str(raw_t).strip():
            input_type = str(raw_t).strip()
        raw_ga = data.get("ga_client_id")
        if raw_ga is not None:
            ga_client_id = str(raw_ga)
        raw_pipeline = data.get("pipeline")
        if raw_pipeline is not None and str(raw_pipeline).strip():
            pipeline = str(raw_pipeline).strip().lower()
    else:
        form = await request.form()
        raw_source = form.get("source")
        source = str(raw_source).strip() if raw_source not in (None, "") else "link"
        raw_url = form.get("input_url")
        if raw_url is not None and str(raw_url).strip():
            input_url = str(raw_url).strip()
        raw_t = form.get("type")
        if raw_t is None or not str(raw_t).strip():
            raw_t = form.get("input_type")
        if raw_t is not None and str(raw_t).strip():
            input_type = str(raw_t).strip()
        raw_ga = form.get("ga_client_id")
        if raw_ga is not None:
            ga_client_id = str(raw_ga)
        raw_pipeline = form.get("pipeline")
        if raw_pipeline is not None and str(raw_pipeline).strip():
            pipeline = str(raw_pipeline).strip().lower()
        fu = form.get("file")
        # Accept any Starlette/FastAPI upload object (isinstance can fail across re-exports).
        if fu is not None and hasattr(fu, "read") and hasattr(fu, "filename"):
            file = fu

    # Handle file upload
    final_url = input_url
    if file is not None:
        source = "upload"
    if source == "upload" and file:
        # Save uploaded file
        upload_token = str(uuid.uuid4())
        upload_dir = os.path.join(UPLOAD_DIR, upload_token)
        os.makedirs(upload_dir, exist_ok=True)
        
        filename = file.filename or "model.glb"
        filepath = os.path.join(upload_dir, filename)
        
        # Stream upload to disk so large files do not spike RAM.
        max_upload_bytes = MAX_UPLOAD_SIZE_MB * 1024 * 1024
        uploaded_bytes = 0
        try:
            with open(filepath, "wb") as f:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    uploaded_bytes += len(chunk)
                    if uploaded_bytes > max_upload_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE_MB}MB."
                        )
                    f.write(chunk)
        except Exception:
            try:
                os.unlink(filepath)
            except OSError:
                pass
            raise
        
        # Generate public URL (URL-encode filename for special chars, spaces, cyrillic)
        from urllib.parse import quote
        final_url = f"{APP_URL}/u/{upload_token}/{quote(filename)}"
    
    if not final_url:
        raise HTTPException(status_code=400, detail="No input URL provided")

    if pipeline not in ("rig", "convert"):
        pipeline = "rig"

    if pipeline == "convert" and not _url_path_endswith_glb(final_url):
        raise HTTPException(
            status_code=400,
            detail="pipeline=convert requires a .glb input URL or .glb upload filename.",
        )

    input_type = normalize_task_type(input_type)

    # Create task
    task, error = await create_conversion_task(
        db,
        final_url,
        input_type,
        owner_type,
        owner_id,
        created_via_api=via_api,
        pipeline_kind=pipeline,
        input_bytes=uploaded_bytes,
    )
    
    if error and not task:
        raise HTTPException(status_code=500, detail=error)
    
    # Store GA client ID if provided
    if ga_client_id:
        task.ga_client_id = ga_client_id
        await db.commit()
        
        # Send GA4 event
        asyncio.create_task(send_ga4_event(
            ga_client_id,
            "task_created",
            {"source": source, "type": input_type, "pipeline": pipeline},
        ))
    
    # Try to dispatch immediately to a free worker (don't wait for background cycle)
    try:
        queue_status = await get_global_queue_status(db=db)
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

    await db.refresh(task)

    return TaskCreateResponse(
        task_id=task.id,
        status=task.status,
        message=error,
    )


@app.post("/api/task/{parent_task_id}/create-convert", response_model=TaskCreateResponse)
@limiter.limit(f"{RATE_LIMIT_TASKS_PER_MINUTE}/minute")
async def api_create_convert_from_rig_task(
    parent_task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Start an Auto Convert task from a **completed** rig task.
    The server resolves the prepared GLB URL; clients must not pass arbitrary URLs.
    """
    via_api = bool(getattr(request.state, "auth_via_api_key", False))
    api_key_anon = getattr(request.state, "api_key_anon_id", None)
    if user:
        owner_type = "user"
        owner_id = user.email
    elif api_key_anon:
        owner_type = "anon"
        owner_id = api_key_anon
    else:
        anon_session = await get_anon_session(request, response, db)
        owner_type = "anon"
        owner_id = anon_session.anon_id

    parent = await get_task_by_id(db, parent_task_id)
    if not parent:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = await get_anon_session(request, response, db)
    is_admin = bool(user and is_admin_email(user.email))
    is_owner = (
        (user and parent.owner_type == "user" and parent.owner_id == user.email)
        or (parent.owner_type == "anon" and parent.owner_id == anon_session.anon_id)
    )
    if not (is_owner or is_admin):
        raise HTTPException(status_code=403, detail="Not authorized to use this task")

    pk_parent = getattr(parent, "pipeline_kind", None) or "rig"
    if pk_parent == "convert":
        raise HTTPException(
            status_code=400,
            detail="Auto Convert can only be started from a rig task",
        )

    if parent.status != "done":
        raise HTTPException(
            status_code=400,
            detail="Rig task must be completed before starting Auto Convert",
        )

    prepared_url = resolve_prepared_glb_source_url(parent)
    if not prepared_url:
        raise HTTPException(
            status_code=400,
            detail="Prepared GLB is not available for this task",
        )

    task, error = await create_conversion_task(
        db,
        prepared_url,
        "t_pose",
        owner_type,
        owner_id,
        created_via_api=via_api,
        pipeline_kind="convert",
        input_bytes=getattr(parent, "input_bytes", None),
    )

    if error and not task:
        raise HTTPException(status_code=500, detail=error)

    try:
        queue_status = await get_global_queue_status(db=db)
        free_worker = next(
            (
                w
                for w in queue_status.workers
                if w.available
                and (w.total_active < w.max_concurrent)
                and (w.queue_size <= 0)
            ),
            None,
        )
        if free_worker:
            await db.refresh(task)
            if task.status == "created":
                await start_task_on_worker(db, task, free_worker.url)
                print(f"[Immediate Dispatch] Convert task {task.id} sent to {free_worker.url}")
    except Exception as e:
        print(f"[Immediate Dispatch] Failed for convert task {task.id}: {e}")

    await db.refresh(task)

    return TaskCreateResponse(
        task_id=task.id,
        status=task.status,
        message=error,
    )


# =============================================================================
# Gumroad (Payments)
# =============================================================================
@app.api_route("/api-gumroad", methods=["GET", "HEAD", "OPTIONS"])
@app.api_route("/webhook/gumroad", methods=["GET", "HEAD", "OPTIONS"])
@app.api_route("/gumroad", methods=["GET", "HEAD", "OPTIONS"])
@app.api_route("/gumroad/ping", methods=["GET", "HEAD", "OPTIONS"])
async def api_gumroad_ping_check():
    """Gumroad URL validation check (responds to GET/HEAD/OPTIONS for URL verification)"""
    return {"ok": True, "message": "Gumroad webhook endpoint ready"}


GUMROAD_PROXY_TARGET = "https://free3d.online/gumroad/ping"


def _normalize_gumroad_product_key(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    try:
        parsed = urlparse(value)
        if parsed.scheme or parsed.netloc:
            path = (parsed.path or "").rstrip("/")
            if path:
                value = path.rsplit("/", 1)[-1]
    except Exception:
        pass
    return value.strip().lower()


@app.post("/api-gumroad")
@app.post("/webhook/gumroad")
@app.post("/gumroad")
@app.post("/gumroad/ping")
async def api_gumroad_ping(
    request: Request
):
    """
    Proxy-tunnel for Gumroad webhook.
    Accepts payload from Gumroad and forwards it AS-IS to free3d.online.
    Always returns 200 "ok" (fallback mode) to avoid Gumroad retries.
    """
    raw_body = await request.body()
    content_type = request.headers.get("content-type", "application/x-www-form-urlencoded")
    parsed_form = dict(parse_qsl(raw_body.decode("utf-8", errors="ignore"), keep_blank_values=True))

    sale_id = (parsed_form.get("sale_id") or "").strip()
    email = (parsed_form.get("email") or "").strip() or "unknown"
    product = (parsed_form.get("product_permalink") or "").strip() or "unknown"
    product_name = (parsed_form.get("product_name") or "").strip() or None
    price_raw = (parsed_form.get("price") or "").strip() or "0"
    is_test = str(parsed_form.get("test") or "").strip().lower() in {"1", "true", "yes", "on"}
    is_recurring_charge = str(parsed_form.get("is_recurring_charge") or "").strip().lower() in {"1", "true", "yes", "on"}
    refunded = str(parsed_form.get("refunded") or "").strip().lower() in {"1", "true", "yes", "on"}
    subscription_id = (parsed_form.get("subscription_id") or "").strip() or None
    license_key = (parsed_form.get("license_key") or "").strip() or None
    if not sale_id:
        sale_id = f"proxy-{hashlib.sha256(raw_body).hexdigest()[:16]}"

    try:
        price_cents = int(price_raw)
    except Exception:
        price_cents = 0

    product_key = _normalize_gumroad_product_key(product)
    local_credits_added = 0
    known_product = product_key in {str(k).strip().lower() for k in GUMROAD_PRODUCT_CREDITS.keys()}
    should_notify_purchase = False

    if product_key.startswith("autorig-") and email and email != "unknown":
        try:
            async with AsyncSessionLocal() as db:
                purchase = GumroadPurchase(
                    sale_id=sale_id,
                    email=email,
                    product_permalink=product_key,
                    product_name=product_name,
                    price=price_cents,
                    refunded=refunded,
                    is_recurring_charge=is_recurring_charge,
                    subscription_id=subscription_id,
                    license_key=license_key,
                    test=is_test,
                    raw_payload=raw_body.decode("utf-8", errors="ignore"),
                    credited=False,
                    credits_added=0,
                )
                db.add(purchase)
                try:
                    await db.flush()
                except IntegrityError:
                    await db.rollback()
                    purchase = None

                if purchase is not None:
                    credits_to_add = 0 if refunded else int(GUMROAD_PRODUCT_CREDITS.get(product_key, max(price_cents, 0)))
                    user_result = await db.execute(
                        select(User).where(func.lower(User.email) == email.lower())
                    )
                    user = user_result.scalar_one_or_none()
                    if user and credits_to_add > 0:
                        user.balance_credits = max(0, int(user.balance_credits or 0) + credits_to_add)
                        user.gumroad_email = email
                        purchase.credited = True
                        purchase.credits_added = credits_to_add
                        local_credits_added = credits_to_add
                    await db.commit()
                    should_notify_purchase = True
        except Exception as e:
            print(f"[Gumroad] Local autorig crediting failed for {sale_id}: {e}", flush=True)

    if not should_notify_purchase:
        if known_product or price_cents > 0 or refunded or is_test:
            should_notify_purchase = True
        else:
            print(
                f"[GumroadProxy] Ignoring unknown zero-price webhook sale={sale_id} product={product!r}",
                flush=True,
            )

    from telegram_bot import broadcast_credits_purchased
    if should_notify_purchase:
        asyncio.create_task(
            broadcast_credits_purchased(
                credits=local_credits_added if local_credits_added > 0 else max(price_cents, 0),
                price=str(price_raw),
                user_email=email,
                product=product,
                sale_id=sale_id,
                is_test=is_test,
                is_recurring_charge=is_recurring_charge,
                refunded=refunded,
            )
        )

    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            upstream = await client.post(
                GUMROAD_PROXY_TARGET,
                content=raw_body,
                headers={"Content-Type": content_type},
            )
        if upstream.status_code != 200:
            print(f"[GumroadProxy] Upstream non-200: {upstream.status_code}", flush=True)
    except Exception as e:
        print(f"[GumroadProxy] Upstream request failed: {e}", flush=True)

    return Response(content="ok", media_type="text/plain")


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

    if _task_needs_poster_classification(task):
        _schedule_poster_recovery_throttled(task.id)
    
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
    # NOTE: Removed fallback (guid is not None and status != 'created') as it
    # caused false positives - returned True before _model_prepared.glb actually exists
    prepared_glb_ready = (
        any('_model_prepared.glb' in url.lower() for url in (task.ready_urls or [])) or
        task.fbx_glb_ready
    )
    
    def _poster_llm_keywords_list() -> Optional[list]:
        raw = getattr(task, "poster_llm_keywords", None)
        if not raw:
            return None
        try:
            import json
            data = json.loads(raw)
            if isinstance(data, list):
                out = [str(x).strip() for x in data if str(x).strip()]
                return out or None
        except Exception:
            pass
        return None

    kw_list = _poster_llm_keywords_list()
    poster_free3d_query = build_free3d_similar_query(
        getattr(task, "poster_llm_title", None),
        kw_list,
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
        guid=task.guid,
        content_rating=getattr(task, "content_rating", None),
        content_score=getattr(task, "content_score", None),
        content_classified_at=getattr(task, "content_classified_at", None),
        content_classifier_version=getattr(task, "content_classifier_version", None),
        poster_llm_title=getattr(task, "poster_llm_title", None),
        poster_llm_description=getattr(task, "poster_llm_description", None),
        poster_llm_keywords=kw_list,
        poster_llm_at=getattr(task, "poster_llm_at", None),
        poster_free3d_query=poster_free3d_query,
        created_at=task.created_at,
        updated_at=task.updated_at,
        pipeline=getattr(task, "pipeline_kind", None) or "rig",
        youtube_video_id=getattr(task, "youtube_video_id", None),
        youtube_upload_status=getattr(task, "youtube_upload_status", None),
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
    
    api_base, guid = _resolve_worker_files_api_context(task)
    if not api_base or not guid:
        return {"available": False, "files": []}

    worker_root = f"{api_base}/converter/glb"
    files_url = f"{api_base}/api-converter-glb/model-files/{guid}"
    
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
                        "url": f"{worker_root}/{guid}/{rel_path}"
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
        db,
        task.input_url,
        task.input_type or "t_pose",
        task.owner_type,
        task.owner_id,
        pipeline_kind=getattr(task, "pipeline_kind", None) or "rig",
        input_bytes=getattr(task, "input_bytes", None),
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
    """Restart task with the same task_id (available after 1 minute)"""
    from datetime import timedelta
    from workers import select_best_worker, send_task_to_worker

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Check ownership
    anon_session = await get_anon_session(request, response, db)
    is_admin = bool(user and is_admin_email(user.email))
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
    )
    if not (is_owner or is_admin):
        raise HTTPException(status_code=403, detail="Not authorized to restart this task")

    # Age gate: 1 minute
    task_age = datetime.utcnow() - task.created_at
    min_age = timedelta(minutes=1)
    if task_age < min_age:
        remaining = min_age - task_age
        minutes = int(remaining.total_seconds() / 60)
        raise HTTPException(
            status_code=400,
            detail=f"Task is too recent. Restart available in {minutes} minutes."
        )

    if not task.input_url:
        raise HTTPException(status_code=400, detail="No input URL to restart")

    # Increment version (restart_count)
    task.restart_count = (task.restart_count or 0) + 1

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
    
    # Clear ALL local caches for this task (so fresh files are downloaded)
    try:
        import pathlib
        import shutil
        static_dir = pathlib.Path(__file__).parent.parent / "static"
        
        # 1. Clear GLB cache (prepared.glb, animations.glb)
        glb_cache = static_dir / "glb_cache"
        for cache_file in glb_cache.glob(f"{task_id}_*.glb"):
            cache_file.unlink()
            print(f"[Restart] Deleted cached GLB: {cache_file.name}")
        
        # 2. Clear task files cache (downloads: videos, zips, individual files)
        task_cache = static_dir / "tasks" / task_id
        if task_cache.exists():
            shutil.rmtree(task_cache)
            print(f"[Restart] Deleted task cache folder: {task_cache.name}")
            
    except Exception as e:
        print(f"[Restart] Failed to clear caches: {e}")

    await db.commit()
    await db.refresh(task)

    # Start pipeline for the same task_id without blocking on FBX pre-conversion.
    worker_url = await select_best_worker(db=db)
    if not worker_url:
        raise HTTPException(status_code=500, detail="No workers available")

    task.worker_api = worker_url
    task.status = "processing"

    pk_restart = getattr(task, "pipeline_kind", None) or "rig"
    if pk_restart not in ("rig", "convert"):
        pk_restart = "rig"

    # Parse transform params from request body (rig pipeline only)
    transform_params = None
    if pk_restart == "rig":
        try:
            body = await request.body()
            if body:
                import json as json_module
                body_data = json_module.loads(body)
                if isinstance(body_data, dict):
                    # Extract transform params if present
                    if any(k in body_data for k in ("local_position", "local_rotation", "local_scale")):
                        transform_params = {
                            "local_position": body_data.get("local_position"),
                            "local_rotation": body_data.get("local_rotation"),
                            "local_scale": body_data.get("local_scale")
                        }
                        print(f"[Restart] Transform params from request: {transform_params}")
        except Exception as e:
            print(f"[Restart] Could not parse body: {e}")

        # Fallback: read from saved viewer_settings if no transforms in request
        if not transform_params and task.viewer_settings:
            try:
                settings = json.loads(task.viewer_settings)
                mt = settings.get("modelTransform")
                if mt and isinstance(mt, dict):
                    pos = mt.get("position", {})
                    rot = mt.get("rotation", {})
                    scale = mt.get("scale", {})
                    # Only use if any value is non-default
                    has_transform = (
                        any(v != 0 for v in [pos.get("x", 0), pos.get("y", 0), pos.get("z", 0)]) or
                        any(v != 0 for v in [rot.get("x", 0), rot.get("y", 0), rot.get("z", 0)]) or
                        any(v != 1 for v in [scale.get("x", 1), scale.get("y", 1), scale.get("z", 1)])
                    )
                    if has_transform:
                        rad_to_deg = 180 / 3.14159265359
                        transform_params = {
                            "local_position": [pos.get("x", 0), pos.get("y", 0), pos.get("z", 0)],
                            "local_rotation": [
                                rot.get("x", 0) * rad_to_deg,
                                rot.get("y", 0) * rad_to_deg,
                                rot.get("z", 0) * rad_to_deg
                            ],
                            "local_scale": [scale.get("x", 1), scale.get("y", 1), scale.get("z", 1)]
                        }
                        print(f"[Restart] Transform params from viewer_settings: {transform_params}")
            except Exception as e:
                print(f"[Restart] Could not read viewer_settings: {e}")

    # Send directly to worker - workers handle GLB/FBX/OBJ natively
    result = await send_task_to_worker(
        worker_url,
        task.input_url,
        task.input_type or "t_pose",
        transform_params=transform_params,
        pipeline_kind=pk_restart,
    )
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
        is_owner=is_owner,
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
    
    # Check if user is owner (owners must still purchase to download)
    is_owner = (
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and task.owner_id == anon_session.anon_id)
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
    
    # Handle "buy all" request (full-task access)
    if purchase_req.all:
        cost = DOWNLOAD_ALL_FILES_CREDITS
        if user.balance_credits < cost:
            raise HTTPException(status_code=402, detail="Insufficient credits")
        
        # Deduct credits from buyer
        user.balance_credits -= cost
        
        # Credit task owner (if they have a user account)
        await _credit_task_owner_for_sale(db, task, user, cost)
        
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
        
        # Deduct credits from buyer
        user.balance_credits -= cost
        
        # Credit task owner (if they have a user account)
        if task.owner_type == "user" and task.owner_id:
            owner_result = await db.execute(
                select(User).where(User.email == task.owner_id)
            )
            task_owner = owner_result.scalar_one_or_none()
            if task_owner and task_owner.id != user.id:  # Don't credit yourself
                task_owner.balance_credits += cost  # Same amount as buyer spent
        
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


@app.get("/api/task/{task_id}/animations/catalog", response_model=AnimationCatalogResponse)
async def api_get_animation_catalog(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Return custom animation catalog + availability + purchase state for a task."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    manifest = _load_animation_manifest()
    raw_items = manifest.get("animations") or []
    if not isinstance(raw_items, list):
        raw_items = []

    file_map = await _build_task_animation_file_map(task, raw_items)
    purchased_ids, purchased_all = await _get_animation_purchase_state(db, user, task_id)

    items: List[AnimationCatalogItem] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        if item.get("enabled", True) is False:
            continue

        anim_id = _normalize_animation_key(str(item.get("id") or item.get("name") or ""))
        if not anim_id:
            continue

        matched = _resolve_animation_file(item, file_map)
        available = matched is not None
        ready = bool(matched and matched.get("ready"))
        file_name = matched.get("clean_filename") if matched else None
        preview_url = f"/api/task/{task_id}/animations/preview/{quote(anim_id)}" if available else None

        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        items.append(AnimationCatalogItem(
            id=anim_id,
            name=str(item.get("name") or anim_id),
            type=str(item.get("type") or "other"),
            type_label=str(item.get("type_label") or str(item.get("type") or "other").title()),
            tags=[str(t) for t in tags if isinstance(t, str)],
            credits=int(item.get("credits", ANIMATION_SINGLE_CREDITS) or ANIMATION_SINGLE_CREDITS),
            format=str(item.get("format") or "fbx"),
            preview_gif=item.get("preview_gif"),
            available=available,
            ready=ready,
            purchased=(purchased_all or (anim_id in purchased_ids)),
            file_name=file_name,
            preview_url=preview_url,
        ))

    items.sort(key=lambda x: (x.type, x.name.lower()))

    types = manifest.get("types")
    if not isinstance(types, list):
        type_counts: Dict[str, int] = {}
        for it in items:
            type_counts[it.type] = type_counts.get(it.type, 0) + 1
        types = [
            {"id": k, "label": k.title(), "count": v}
            for k, v in sorted(type_counts.items(), key=lambda x: x[0])
        ]

    return AnimationCatalogResponse(
        types=types,
        animations=items,
        purchased_all=purchased_all,
        purchased_ids=sorted(purchased_ids),
        login_required=(user is None),
        user_credits=(user.balance_credits if user else 0),
        pricing={
            "single_animation_credits": ANIMATION_SINGLE_CREDITS,
            "all_animations_credits": ANIMATION_BUNDLE_CREDITS,
            "download_format": "fbx"
        }
    )


@app.post("/api/task/{task_id}/animations/purchase", response_model=AnimationPurchaseResponse)
async def api_purchase_animation(
    task_id: str,
    purchase_req: AnimationPurchaseRequest,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Purchase one custom animation (1 credit) or unlock all custom animations (10 credits)."""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    manifest = _load_animation_manifest()
    raw_items = manifest.get("animations") or []
    catalog_by_id: Dict[str, dict] = {}
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        if item.get("enabled", True) is False:
            continue
        anim_id = _normalize_animation_key(str(item.get("id") or item.get("name") or ""))
        if anim_id:
            catalog_by_id[anim_id] = item

    purchased_ids, purchased_all = await _get_animation_purchase_state(db, user, task_id)

    if purchase_req.all:
        if purchased_all:
            return AnimationPurchaseResponse(
                success=True,
                purchased_animation_ids=sorted(purchased_ids),
                purchased_all=True,
                credits_remaining=user.balance_credits
            )

        cost = ANIMATION_BUNDLE_CREDITS
        if user.balance_credits < cost:
            raise HTTPException(status_code=402, detail="Insufficient credits")

        user.balance_credits -= cost
        await _credit_task_owner_for_sale(db, task, user, cost)
        db.add(TaskAnimationBundlePurchase(
            task_id=task_id,
            user_email=user.email,
            credits_spent=cost,
        ))

        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()

        purchased_ids, purchased_all = await _get_animation_purchase_state(db, user, task_id)
        return AnimationPurchaseResponse(
            success=True,
            purchased_animation_ids=sorted(purchased_ids),
            purchased_all=purchased_all,
            credits_remaining=user.balance_credits
        )

    animation_id = _normalize_animation_key(str(purchase_req.animation_id or ""))
    if not animation_id:
        raise HTTPException(status_code=400, detail="animation_id is required")
    if animation_id not in catalog_by_id:
        raise HTTPException(status_code=404, detail="Animation not found")

    if purchased_all or animation_id in purchased_ids:
        return AnimationPurchaseResponse(
            success=True,
            purchased_animation_ids=sorted(purchased_ids),
            purchased_all=purchased_all,
            credits_remaining=user.balance_credits
        )

    # Guard: animation should exist in this task outputs before purchase.
    file_map = await _build_task_animation_file_map(task, raw_items)
    resolved = _resolve_animation_file(catalog_by_id[animation_id], file_map)
    if not resolved:
        raise HTTPException(status_code=409, detail="Animation is not available for this task yet")

    cost = ANIMATION_SINGLE_CREDITS
    if user.balance_credits < cost:
        raise HTTPException(status_code=402, detail="Insufficient credits")

    user.balance_credits -= cost
    await _credit_task_owner_for_sale(db, task, user, cost)
    db.add(TaskAnimationPurchase(
        task_id=task_id,
        user_email=user.email,
        animation_id=animation_id,
        credits_spent=cost
    ))

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()

    purchased_ids, purchased_all = await _get_animation_purchase_state(db, user, task_id)
    return AnimationPurchaseResponse(
        success=True,
        purchased_animation_ids=sorted(purchased_ids),
        purchased_all=purchased_all,
        credits_remaining=user.balance_credits
    )


@app.get("/api/task/{task_id}/animations/preview/{animation_id}")
async def api_preview_animation(
    task_id: str,
    animation_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Stream selected custom animation FBX for in-viewer preview.

    Unlike download endpoint, preview does not require purchase.
    For completed tasks we allow public preview access so animation playback works
    across browsers/sessions (e.g. when owner cookies are missing in another browser).
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
    # Keep strict access for in-progress tasks, but allow preview on completed tasks.
    if not is_owner_or_admin and task.status != "done":
        raise HTTPException(status_code=403, detail="Not authorized to preview this animation")

    anim_id = _normalize_animation_key(animation_id)
    manifest = _load_animation_manifest()
    raw_items = manifest.get("animations") or []
    catalog_by_id: Dict[str, dict] = {}
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        if item.get("enabled", True) is False:
            continue
        key = _normalize_animation_key(str(item.get("id") or item.get("name") or ""))
        if key:
            catalog_by_id[key] = item

    if anim_id not in catalog_by_id:
        raise HTTPException(status_code=404, detail="Animation not found")

    file_map = await _build_task_animation_file_map(task, raw_items)
    resolved = _resolve_animation_file(catalog_by_id[anim_id], file_map)
    if not resolved:
        raise HTTPException(status_code=404, detail="Animation file not ready")

    file_url = resolved["url"]
    client = httpx.AsyncClient(timeout=120.0)
    try:
        req = client.build_request("GET", file_url)
        worker_resp = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        raise HTTPException(status_code=404, detail="Animation file is unavailable")

    if worker_resp.status_code != 200:
        await worker_resp.aclose()
        await client.aclose()
        raise HTTPException(status_code=404, detail="Animation file is unavailable")

    async def _close_stream_resources():
        try:
            await worker_resp.aclose()
        finally:
            await client.aclose()

    return StreamingResponse(
        worker_resp.aiter_bytes(chunk_size=65536),
        media_type="application/octet-stream",
        headers={"Cache-Control": "no-store, max-age=0"},
        background=BackgroundTask(_close_stream_resources)
    )


@app.get("/api/task/{task_id}/animations/download/{animation_id}")
async def api_download_animation(
    task_id: str,
    animation_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Download selected custom animation FBX if purchased."""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anim_id = _normalize_animation_key(animation_id)
    manifest = _load_animation_manifest()
    raw_items = manifest.get("animations") or []
    catalog_by_id: Dict[str, dict] = {}
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        if item.get("enabled", True) is False:
            continue
        key = _normalize_animation_key(str(item.get("id") or item.get("name") or ""))
        if key:
            catalog_by_id[key] = item

    if anim_id not in catalog_by_id:
        raise HTTPException(status_code=404, detail="Animation not found")

    file_map = await _build_task_animation_file_map(task, raw_items)
    resolved = _resolve_animation_file(catalog_by_id[anim_id], file_map)
    if not resolved:
        raise HTTPException(status_code=404, detail="Animation file not ready")

    purchased_ids, purchased_all = await _get_animation_purchase_state(db, user, task_id)
    if not (purchased_all or anim_id in purchased_ids):
        raise HTTPException(status_code=402, detail="Payment required to download this animation")

    file_url = resolved["url"]
    clean_filename = resolved["clean_filename"]

    # Track download event for paid custom animation
    if task.ga_client_id:
        asyncio.create_task(send_ga4_event(
            task.ga_client_id,
            "custom_animation_downloaded",
            {"animation_id": anim_id, "task_id": task_id, "filename": clean_filename}
        ))

    # Serve from local cache if present
    cached_path = TASK_CACHE_DIR / task_id / clean_filename
    if cached_path.exists():
        return FileResponse(
            cached_path,
            media_type="application/octet-stream",
            filename=clean_filename,
            headers={"Cache-Control": "public, max-age=86400"}
        )

    async def stream_file():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", file_url, timeout=120.0) as response:
                if response.status_code != 200:
                    raise HTTPException(status_code=404, detail="Animation file is unavailable")
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        stream_file(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={clean_filename}",
            "Cache-Control": "public, max-age=86400"
        }
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
                video_ready=t.video_ready,
                thumbnail_url=f"/api/thumb/{t.id}" if t.status == "done" and t.video_ready else None,
                content_rating=getattr(t, "content_rating", None),
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
    author: Optional[str] = None,  # Filter by author email
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get public gallery of completed tasks with videos"""
    from sqlalchemy import func, desc, distinct
    from database import Task, TaskFilePurchase

    # Get current user email for liked_by_me check
    user_email = user.email if user else None

    # Build base conditions (must have poster URLs in DB or /api/thumb 404s in the grid)
    base_conditions = [
        Task.status == "done",
        Task.video_ready == True,
        _gallery_task_has_poster_sql(),
    ]
    if author:
        base_conditions.append(Task.owner_type == "user")
        base_conditions.append(Task.owner_id == author)

    # Count total completed tasks with video (with author filter if present)
    count_result = await db.execute(
        select(func.count(distinct(func.coalesce(Task.input_url, Task.id)))).where(*base_conditions)
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
            .where(*base_conditions)
            .group_by(func.coalesce(Task.input_url, Task.id))
            .order_by(desc('like_count'), desc(Task.created_at))
            .offset(offset)
            .limit(per_page)
        )
    elif sort == "sales":
        # Sort by sales count (descending), then by date
        result = await db.execute(
            select(
                Task,
                func.count(TaskLike.id).label('like_count'),
                func.count(distinct(TaskFilePurchase.user_email)).label('sales_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .outerjoin(TaskFilePurchase, Task.id == TaskFilePurchase.task_id)
            .where(*base_conditions)
            .group_by(func.coalesce(Task.input_url, Task.id))
            .order_by(desc('sales_count'), desc(Task.created_at))
            .offset(offset)
            .limit(per_page)
        )
    else:
        # Sort by date (newest first) - default
        result = await db.execute(
            select(
                Task,
                func.count(TaskLike.id).label('like_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .where(*base_conditions)
            .group_by(func.coalesce(Task.input_url, Task.id))
            .order_by(desc(Task.created_at))
            .offset(offset)
            .limit(per_page)
        )
    
    rows = result.all()
    task_ids = [row[0].id for row in rows]
    
    # Get user's likes if logged in
    user_likes = set()
    if user_email and task_ids:
        likes_result = await db.execute(
            select(TaskLike.task_id).where(
                TaskLike.user_email == user_email,
                TaskLike.task_id.in_(task_ids)
            )
        )
        user_likes = set(r[0] for r in likes_result.all())
    
    # Get sales counts per task (count unique buyers, not individual file purchases)
    sales_counts = {}
    if task_ids:
        sales_result = await db.execute(
            select(
                TaskFilePurchase.task_id,
                func.count(distinct(TaskFilePurchase.user_email)).label('sales_count')
            )
            .where(TaskFilePurchase.task_id.in_(task_ids))
            .group_by(TaskFilePurchase.task_id)
        )
        sales_counts = {r[0]: r[1] for r in sales_result.all()}
    
    # Get author nicknames for user-owned tasks
    author_nicknames = {}
    owner_emails = [row[0].owner_id for row in rows if row[0].owner_type == "user"]
    if owner_emails:
        users_result = await db.execute(
            select(User.email, User.nickname).where(User.email.in_(owner_emails))
        )
        author_nicknames = {r[0]: r[1] for r in users_result.all()}
    
    items = [
        GalleryItem(
            task_id=t.id,
            video_url=f"/api/video/{t.id}",
            thumbnail_url=f"/api/thumb/{t.id}",
            created_at=t.created_at,
            time_ago=format_time_ago(t.created_at),
            like_count=like_count,
            liked_by_me=t.id in user_likes,
            sales_count=sales_counts.get(t.id, 0),
            author_email=t.owner_id if t.owner_type == "user" else None,
            author_nickname=author_nicknames.get(t.owner_id) if t.owner_type == "user" else None,
            content_rating=getattr(t, "content_rating", None),
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


@app.get("/api/task/{task_id}/card", response_model=TaskCardInfo)
async def api_get_task_card(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get task card info (likes, sales, author) for display"""
    from sqlalchemy import func, distinct
    from database import Task, TaskFilePurchase
    
    # Get task
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Get like count
    like_result = await db.execute(
        select(func.count(TaskLike.id)).where(TaskLike.task_id == task_id)
    )
    like_count = like_result.scalar() or 0
    
    # Check if current user liked
    liked_by_me = False
    if user:
        user_like = await db.execute(
            select(TaskLike).where(
                TaskLike.task_id == task_id,
                TaskLike.user_email == user.email
            )
        )
        liked_by_me = user_like.scalar_one_or_none() is not None
    
    # Get sales count (unique buyers)
    sales_result = await db.execute(
        select(func.count(distinct(TaskFilePurchase.user_email))).where(
            TaskFilePurchase.task_id == task_id
        )
    )
    sales_count = sales_result.scalar() or 0
    
    # Get author info
    author_email = None
    author_nickname = None
    if task.owner_type == "user":
        author_email = task.owner_id
        # Get nickname from User
        user_result = await db.execute(
            select(User.nickname).where(User.email == task.owner_id)
        )
        row = user_result.first()
        if row:
            author_nickname = row[0]
    
    return TaskCardInfo(
        task_id=task_id,
        like_count=like_count,
        liked_by_me=liked_by_me,
        sales_count=sales_count,
        author_email=author_email,
        author_nickname=author_nickname,
        time_ago=format_time_ago(task.created_at),
        version=(task.restart_count or 0) + 1,
        content_rating=getattr(task, "content_rating", None),
    )


@app.get("/api/task/{task_id}/owner_tasks")
async def api_get_owner_tasks(
    task_id: str,
    page: int = 1,
    per_page: int = 12,
    db: AsyncSession = Depends(get_db)
):
    """Get all tasks from the same owner as the specified task"""
    from tasks import get_user_tasks
    
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    tasks, total = await get_user_tasks(db, task.owner_type, task.owner_id, page, per_page)
    
    # Get author nicknames if needed
    author_nicknames = {}
    if task.owner_type == "user":
        user_result = await db.execute(
            select(User.email, User.nickname).where(User.email == task.owner_id)
        )
        author_nicknames = {r[0]: r[1] for r in user_result.all()}

    return {
        "tasks": [
            {
                "task_id": t.id,
                "status": t.status,
                "progress": t.progress,
                "created_at": t.created_at,
                "thumbnail_url": f"/api/thumb/{t.id}" if t.status == "done" else None,
                "content_rating": getattr(t, "content_rating", None),
                "owner_type": t.owner_type,
                "owner_id": t.owner_id if t.owner_type == "user" else "anon"
            }
            for t in tasks
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "owner_type": task.owner_type
    }


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
async def api_queue_status(db: AsyncSession = Depends(get_db)):
    """Get global queue status across all workers"""
    status = await get_global_queue_status(db=db)
    
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
@app.get("/api/admin/workers", response_model=AdminWorkerListResponse)
async def api_admin_workers(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """List configured conversion workers (admin only)."""
    from sqlalchemy import desc, func, case
    from database import Task

    res = await db.execute(
        select(WorkerEndpoint).order_by(
            desc(WorkerEndpoint.enabled),
            desc(WorkerEndpoint.weight),
            WorkerEndpoint.id
        )
    )
    workers = res.scalars().all()

    # Stats: count how many tasks each worker actually processed.
    # We focus on done_tasks for "who does most of the work".
    stats_res = await db.execute(
        select(
            Task.worker_api.label("worker_api"),
            func.count(Task.id).label("total_tasks"),
            func.sum(case((Task.status == "done", 1), else_=0)).label("done_tasks"),
        )
        .where(Task.worker_api.is_not(None))
        .group_by(Task.worker_api)
    )
    stats_rows = stats_res.all()

    def _norm(url: str) -> str:
        url = (url or "").strip()
        while url.endswith("/"):
            url = url[:-1]
        return url

    stats_by_url = {}
    for row in stats_rows:
        url = _norm(row.worker_api)
        stats_by_url[url] = {
            "total_tasks": int(row.total_tasks or 0),
            "done_tasks": int(row.done_tasks or 0),
        }

    total_done_all = 0
    for w in workers:
        s = stats_by_url.get(_norm(w.url))
        if s:
            total_done_all += int(s.get("done_tasks", 0))

    return AdminWorkerListResponse(
        workers=[
            AdminWorkerItem(
                id=w.id,
                url=w.url,
                enabled=bool(w.enabled),
                weight=int(w.weight or 0),
                created_at=w.created_at,
                updated_at=w.updated_at,
                done_tasks=int(stats_by_url.get(_norm(w.url), {}).get("done_tasks", 0)),
                total_tasks=int(stats_by_url.get(_norm(w.url), {}).get("total_tasks", 0)),
                done_share_pct=(
                    (float(stats_by_url.get(_norm(w.url), {}).get("done_tasks", 0)) / float(total_done_all)) * 100.0
                    if total_done_all > 0 else 0.0
                )
            )
            for w in workers
        ]
    )


@app.post("/api/admin/workers", response_model=AdminWorkerItem)
async def api_admin_create_worker(
    data: AdminWorkerCreate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Create a new worker endpoint (admin only)."""
    url = _validate_worker_url(data.url)
    worker = WorkerEndpoint(
        url=url,
        enabled=bool(data.enabled),
        weight=int(data.weight or 0)
    )
    db.add(worker)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Worker url already exists")

    await db.refresh(worker)
    return AdminWorkerItem(
        id=worker.id,
        url=worker.url,
        enabled=bool(worker.enabled),
        weight=int(worker.weight or 0),
        created_at=worker.created_at,
        updated_at=worker.updated_at,
        done_tasks=0,
        total_tasks=0,
        done_share_pct=0.0
    )


@app.put("/api/admin/workers/{worker_id}", response_model=AdminWorkerItem)
async def api_admin_update_worker(
    worker_id: int,
    data: AdminWorkerUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update worker endpoint (admin only)."""
    res = await db.execute(select(WorkerEndpoint).where(WorkerEndpoint.id == worker_id))
    worker = res.scalar_one_or_none()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")

    if data.url is not None:
        worker.url = _validate_worker_url(data.url)
    if data.enabled is not None:
        worker.enabled = bool(data.enabled)
    if data.weight is not None:
        worker.weight = int(data.weight)

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Worker url already exists")

    await db.refresh(worker)
    return AdminWorkerItem(
        id=worker.id,
        url=worker.url,
        enabled=bool(worker.enabled),
        weight=int(worker.weight or 0),
        created_at=worker.created_at,
        updated_at=worker.updated_at,
        done_tasks=0,
        total_tasks=0,
        done_share_pct=0.0
    )


@app.delete("/api/admin/workers/{worker_id}")
async def api_admin_delete_worker(
    worker_id: int,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Delete worker endpoint (admin only)."""
    res = await db.execute(select(WorkerEndpoint).where(WorkerEndpoint.id == worker_id))
    worker = res.scalar_one_or_none()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    await db.delete(worker)
    await db.commit()
    return {"ok": True, "id": worker_id}


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


@app.get("/api/admin/overlay-metrics", response_model=AdminOverlayMetricsResponse)
async def api_admin_overlay_metrics(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Статистика для панели оверлея: текущие статусы по БД + периодные счётчики завершений."""
    tasks_by_status: dict = {}
    for status in ["created", "processing", "done", "error"]:
        count_result = await db.execute(
            select(func.count(Task.id)).where(Task.status == status)
        )
        tasks_by_status[status] = count_result.scalar() or 0
    total_tasks = sum(tasks_by_status.values())
    done_n = tasks_by_status.get("done", 0) or 0
    err_n = tasks_by_status.get("error", 0) or 0
    rating_percent = None
    if done_n + err_n > 0:
        rating_percent = round(100.0 * float(done_n) / float(done_n + err_n), 1)

    row = await get_or_create_admin_overlay_counters(db)
    sc = int(row.completed_count or 0)
    st = float(row.total_duration_seconds or 0.0)
    session_avg = None
    if sc > 0:
        session_avg = round(st / float(sc), 1)

    return AdminOverlayMetricsResponse(
        tasks_by_status=tasks_by_status,
        total_tasks=total_tasks,
        rating_percent=rating_percent,
        session_completed=sc,
        session_total_duration_seconds=st,
        session_avg_seconds=session_avg,
    )


@app.post("/api/admin/overlay-metrics/reset", response_model=AdminBulkAffectedResponse)
async def api_admin_overlay_metrics_reset(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Обнулить периодные счётчики (завершения и сумму длительностей) в БД."""
    await reset_admin_overlay_counters(db)
    return AdminBulkAffectedResponse(affected=1)


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
                input_url=task.input_url,
                content_rating=getattr(task, "content_rating", None),
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
    pipeline_kind: Optional[str] = None,
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

    page = max(1, page)
    per_page = min(max(1, per_page), 200)

    _VALID_STATUS = frozenset({"created", "processing", "done", "error"})
    _ALLOWED_SORT = frozenset({"created_at", "updated_at", "pipeline_kind", "status", "progress", "id"})

    # Base query
    base_query = select(Task)

    # Filter by status: omit / "all" = any; one value; or comma-separated (e.g. created,processing)
    if status and status.strip() and status.strip().lower() != "all":
        parts = [s.strip().lower() for s in status.split(",") if s.strip()]
        parts = [p for p in parts if p in _VALID_STATUS]
        if len(parts) == 1:
            base_query = base_query.where(Task.status == parts[0])
        elif len(parts) > 1:
            base_query = base_query.where(Task.status.in_(parts))

    if pipeline_kind and pipeline_kind in ("rig", "convert"):
        base_query = base_query.where(Task.pipeline_kind == pipeline_kind)
    
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

    # Sort (whitelist column names)
    if sort_by not in _ALLOWED_SORT:
        sort_by = "created_at"
    sort_column = getattr(Task, sort_by, Task.created_at)
    if sort_desc:
        base_query = base_query.order_by(desc(sort_column))
    else:
        base_query = base_query.order_by(sort_column)
    
    # Paginate
    offset = (page - 1) * per_page
    result = await db.execute(base_query.offset(offset).limit(per_page))
    tasks = result.scalars().all()
    
    def _err_preview(msg: Optional[str]) -> Optional[str]:
        if not msg:
            return None
        s = msg.strip()
        if len(s) <= 280:
            return s
        return s[:279] + "…"

    def _admin_input_bytes(t) -> Optional[int]:
        ib = getattr(t, "input_bytes", None)
        if ib is not None:
            return int(ib)
        return _task_cache_dir_size_bytes(t.id)

    def _admin_poster_url(t) -> Optional[str]:
        if t.status != "done":
            return None
        if not _task_has_poster(t):
            return None
        return f"/api/thumb/{t.id}"

    return AdminTaskListResponse(
        tasks=[
            AdminTaskListItem(
                task_id=t.id,
                owner_type=t.owner_type,
                owner_id=t.owner_id,
                owner_email=(t.owner_id if t.owner_type == "user" else None),
                status=t.status,
                progress=t.progress,
                ready_count=t.ready_count,
                total_count=t.total_count,
                input_url=t.input_url,
                worker_api=t.worker_api,
                worker_task_id=t.worker_task_id,
                guid=t.guid,
                restart_count=t.restart_count or 0,
                pipeline_kind=(getattr(t, "pipeline_kind", None) or "rig"),
                error_message=_err_preview(t.error_message),
                video_ready=t.video_ready,
                content_rating=getattr(t, "content_rating", None),
                content_score=getattr(t, "content_score", None),
                content_classifier_version=getattr(t, "content_classifier_version", None),
                input_bytes=_admin_input_bytes(t),
                poster_url=_admin_poster_url(t),
                created_at=t.created_at,
                updated_at=t.updated_at,
                age_seconds=_task_age_seconds(t.created_at),
            )
            for t in tasks
        ],
        total=total,
        page=page,
        per_page=per_page
    )


@app.get("/api/admin/task/{task_id}/inspect", response_model=AdminTaskInspectResponse)
async def api_admin_task_inspect(
    task_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Extended task fields for admin overlay (admin only)."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    _ib = getattr(task, "input_bytes", None)
    if _ib is not None:
        _ib = int(_ib)
    else:
        _ib = _task_cache_dir_size_bytes(task.id)

    _poster = None
    if task.status == "done" and _task_has_poster(task):
        _poster = f"/api/thumb/{task.id}"

    return AdminTaskInspectResponse(
        task_id=task.id,
        owner_type=task.owner_type,
        owner_id=task.owner_id,
        owner_email=(task.owner_id if task.owner_type == "user" else None),
        status=task.status,
        progress=task.progress,
        ready_count=task.ready_count,
        total_count=task.total_count,
        restart_count=task.restart_count or 0,
        input_url=task.input_url,
        input_type=task.input_type,
        pipeline_kind=(getattr(task, "pipeline_kind", None) or "rig"),
        input_bytes=_ib,
        poster_url=_poster,
        worker_api=task.worker_api,
        worker_task_id=task.worker_task_id,
        progress_page=task.progress_page,
        guid=task.guid,
        error_message=task.error_message,
        last_progress_at=task.last_progress_at,
        fbx_glb_output_url=task.fbx_glb_output_url,
        fbx_glb_model_name=task.fbx_glb_model_name,
        fbx_glb_ready=task.fbx_glb_ready,
        fbx_glb_error=task.fbx_glb_error,
        video_ready=task.video_ready,
        video_url=task.video_url,
        created_at=task.created_at,
        updated_at=task.updated_at,
        age_seconds=_task_age_seconds(task.created_at),
    )


@app.post("/api/admin/tasks/bulk-restart-count", response_model=AdminBulkAffectedResponse)
async def api_admin_bulk_restart_count(
    body: AdminBulkTaskIdsRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin only. Для задач в status=error — полный requeue в created (как в карточке).
    Для остальных — только restart_count=0.
    """
    ids = list(dict.fromkeys([x for x in (body.task_ids or []) if x]))
    if not ids:
        return AdminBulkAffectedResponse(affected=0)
    n = 0
    for tid in ids:
        task = await get_task_by_id(db, tid)
        if not task:
            continue
        if task.status == "error":
            await admin_requeue_task_to_created(db, task)
        else:
            task.restart_count = 0
            task.updated_at = datetime.utcnow()
        n += 1
    if n:
        await db.commit()
    return AdminBulkAffectedResponse(affected=n)


@app.post("/api/admin/tasks/bulk-restart-count-recent", response_model=AdminBulkAffectedResponse)
async def api_admin_bulk_restart_count_recent(
    body: AdminBulkRestartCountRecentRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Set restart_count=0 for all tasks created within the last `hours` (admin only)."""
    hours = max(0.01, float(body.hours))
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    r = await db.execute(
        update(Task)
        .where(Task.created_at >= cutoff)
        .values(restart_count=0, updated_at=datetime.utcnow())
    )
    await db.commit()
    return AdminBulkAffectedResponse(affected=int(r.rowcount or 0))


@app.post("/api/admin/tasks/bulk-requeue", response_model=AdminBulkAffectedResponse)
async def api_admin_bulk_requeue(
    body: AdminBulkTaskIdsRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Requeue tasks to status=created with cleared worker state (admin only)."""
    ids = list(dict.fromkeys([x for x in (body.task_ids or []) if x]))
    n = 0
    for tid in ids:
        task = await get_task_by_id(db, tid)
        if task:
            await admin_requeue_task_to_created(db, task)
            n += 1
    if n:
        await db.commit()
    return AdminBulkAffectedResponse(affected=n)


@app.post("/api/admin/tasks/bulk-delete", response_model=AdminBulkAffectedResponse)
async def api_admin_bulk_delete_tasks(
    body: AdminBulkTaskIdsRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Delete tasks and on-disk artifacts (admin only)."""
    ids = list(dict.fromkeys([x for x in (body.task_ids or []) if x]))
    n = 0
    for tid in ids:
        if await admin_delete_task_full(db, tid):
            n += 1
    return AdminBulkAffectedResponse(affected=n)


@app.delete("/api/admin/task/{task_id}")
async def api_admin_delete_task(
    task_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """Delete a task by id including artifacts (admin only)."""
    ok = await admin_delete_task_full(db, task_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Task not found")
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


@app.post("/api/admin/tasks/purge-no-poster-video")
async def api_admin_purge_no_poster_video(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    1) Purge terminal rows with no poster filename in JSON.
    2) Purge gallery-eligible rows whose poster/video URLs 404 on workers (files TTL-expired).
    Admin only — same as background jobs.
    """
    if not AUTOMATIC_TASK_DB_DELETION:
        raise HTTPException(
            status_code=403,
            detail="Automatic task DB deletion is disabled (set AUTOMATIC_TASK_DB_DELETION=1 to enable).",
        )
    a = await purge_tasks_without_poster_and_video(db)
    upstream_total = 0
    upstream_off = 0
    rounds = 0
    last_b: dict = {}
    batch = max(GALLERY_UPSTREAM_PURGE_BATCH, 200)
    for _ in range(500):
        b = await purge_gallery_upstream_dead_tasks(db, batch=batch, offset=upstream_off)
        last_b = b
        upstream_total += b["deleted"]
        rounds += 1
        if b["scanned"] == 0:
            break
        if b["deleted"] > 0:
            upstream_off = 0
        else:
            upstream_off += b["scanned"]
    print(
        f"[Admin] purge-no-poster-video by {admin.email}: string={a} "
        f"upstream_deleted={upstream_total} rounds={rounds} last={last_b}"
    )
    return {
        "ok": True,
        "string_purge": a,
        "upstream_purge": last_b,
        "upstream_deleted_total": upstream_total,
        "upstream_rounds": rounds,
    }


@app.post("/api/admin/cleanup")
async def api_admin_cleanup(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger disk cleanup (admin only).
    Deletes oldest task files until MIN_FREE_SPACE_GB is available.
    """
    import shutil
    
    # Get current disk stats
    disk_usage = shutil.disk_usage("/")
    initial_free_gb = disk_usage.free / (1024**3)
    
    # Run cleanup (task rows only if AUTOMATIC_TASK_DB_DELETION)
    from main import cleanup_disk_space
    result = await cleanup_disk_space(
        min_free_gb=MIN_FREE_SPACE_GB,
        db=db,
        delete_task_rows=AUTOMATIC_TASK_DB_DELETION,
    )
    
    # Get final disk stats
    disk_usage = shutil.disk_usage("/")
    final_free_gb = disk_usage.free / (1024**3)
    
    print(f"[Admin] Manual disk cleanup by {admin.email}: deleted {result['deleted_count']} items, freed {result['freed_gb']:.2f} GB")
    
    return {
        "ok": True,
        "initial_free_gb": round(initial_free_gb, 2),
        "final_free_gb": round(final_free_gb, 2),
        "freed_gb": round(result["freed_gb"], 2),
        "deleted_count": result["deleted_count"],
        "target_free_gb": MIN_FREE_SPACE_GB,
        "deleted_items": result.get("deleted_items", [])[:20]  # Limit to first 20 items
    }


@app.get("/api/admin/disk-stats")
async def api_admin_disk_stats(
    request: Request,
    admin: User = Depends(require_admin),
):
    """
    Get disk usage statistics (admin only).
    """
    import shutil
    
    disk_usage = shutil.disk_usage("/")
    
    # Count items in each cleanable directory
    task_cache_count = len(list(TASK_CACHE_DIR.iterdir())) if TASK_CACHE_DIR.exists() else 0
    glb_cache_count = len(list(GLB_CACHE_DIR.iterdir())) if GLB_CACHE_DIR.exists() else 0
    
    upload_dir = pathlib.Path(UPLOAD_DIR)
    upload_count = len(list(upload_dir.iterdir())) if upload_dir.exists() else 0
    
    videos_dir = pathlib.Path("/var/autorig/videos")
    videos_count = len(list(videos_dir.iterdir())) if videos_dir.exists() else 0
    
    return {
        "ok": True,
        "disk": {
            "total_gb": round(disk_usage.total / (1024**3), 2),
            "used_gb": round(disk_usage.used / (1024**3), 2),
            "free_gb": round(disk_usage.free / (1024**3), 2),
            "percent_used": round(disk_usage.used / disk_usage.total * 100, 1)
        },
        "cleanable_items": {
            "task_cache": task_cache_count,
            "glb_cache": glb_cache_count,
            "uploads": upload_count,
            "videos": videos_count
        },
        "settings": {
            "min_free_space_gb": MIN_FREE_SPACE_GB,
            "cleanup_interval_cycles": CLEANUP_CHECK_INTERVAL_CYCLES,
            "min_age_hours": CLEANUP_MIN_AGE_HOURS,
            "gallery_db_purge_interval_cycles": GALLERY_DB_PURGE_INTERVAL_CYCLES,
            "automatic_task_db_deletion": AUTOMATIC_TASK_DB_DELETION,
        }
    }


@app.post("/api/admin/tasks/restart-incomplete")
async def api_admin_restart_incomplete_tasks(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Restart all incomplete tasks (status: created, processing, error).
    Admin only. No age gate. Runs in background to avoid timeout.
    """
    from database import Task
    
    # Count incomplete tasks
    result = await db.execute(
        select(Task).where(Task.status.in_(["created", "processing", "error"]))
    )
    incomplete_tasks = result.scalars().all()
    task_count = len(incomplete_tasks)
    
    if task_count == 0:
        return {"ok": True, "restarted_count": 0, "message": "No incomplete tasks found"}
    
    # Get task IDs to restart
    task_ids = [t.id for t in incomplete_tasks]
    admin_email = admin.email
    
    # Run restart in background
    async def restart_tasks_background():
        from database import AsyncSessionLocal, Task
        from workers import select_best_worker, send_task_to_worker
        from telegram_bot import broadcast_task_restarted, broadcast_bulk_restart_summary
        
        async with AsyncSessionLocal() as bg_db:
            restarted = 0
            errors = []
            
            for task_id in task_ids:
                try:
                    # Re-fetch task in this session
                    result = await bg_db.execute(select(Task).where(Task.id == task_id))
                    task = result.scalar_one_or_none()
                    if not task:
                        continue
                    
                    if not task.input_url:
                        errors.append(f"{task_id[:8]}: no input URL")
                        continue
                    
                    # Reset task fields
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
                    task.fbx_glb_output_url = None
                    task.fbx_glb_model_name = None
                    task.fbx_glb_ready = False
                    task.fbx_glb_error = None
                    
                    # Select worker and send task
                    worker_url = await select_best_worker(db=bg_db)
                    if not worker_url:
                        task.status = "error"
                        task.error_message = "No workers available"
                        errors.append(f"{task_id[:8]}: no workers")
                        await bg_db.commit()
                        continue
                    
                    task.worker_api = worker_url
                    task.status = "processing"
                    
                    pk_ad = getattr(task, "pipeline_kind", None) or "rig"
                    if pk_ad not in ("rig", "convert"):
                        pk_ad = "rig"
                    send_result = await send_task_to_worker(
                        worker_url,
                        task.input_url,
                        task.input_type or "t_pose",
                        pipeline_kind=pk_ad,
                    )
                    if not send_result.success:
                        task.status = "error"
                        task.error_message = send_result.error
                        errors.append(f"{task_id[:8]}: {send_result.error}")
                    else:
                        task.worker_task_id = send_result.task_id
                        task.progress_page = send_result.progress_page
                        task.guid = send_result.guid
                        task.output_urls = send_result.output_urls
                        task.total_count = len(send_result.output_urls)
                        task.status = "processing"
                        restarted += 1
                        
                        # Send Telegram notification for each restarted task
                        try:
                            await broadcast_task_restarted(task_id, reason="admin bulk restart", admin_email=admin_email)
                        except Exception as e:
                            print(f"[Admin] Failed to send restart notification: {e}")
                    
                    await bg_db.commit()
                    
                except Exception as e:
                    errors.append(f"{task_id[:8]}: {str(e)}")
            
            print(f"[Admin] Background restart complete: {restarted}/{len(task_ids)} tasks by {admin_email}")
            if errors:
                print(f"[Admin] Restart errors: {errors}")
            
            # Send summary notification
            try:
                await broadcast_bulk_restart_summary(len(task_ids), restarted, errors, admin_email)
            except Exception as e:
                print(f"[Admin] Failed to send bulk restart summary: {e}")
    
    # Start background task
    asyncio.create_task(restart_tasks_background())
    
    return {
        "ok": True,
        "total_incomplete": task_count,
        "message": f"Restarting {task_count} incomplete tasks in background..."
    }


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
import asyncio
from fastapi.responses import StreamingResponse

@app.get("/api/video/{task_id}")
async def proxy_video(
    task_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Proxy video from worker to serve over HTTPS"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.video_url:
        raise HTTPException(status_code=404, detail="Video not available")

    # Forward range headers so browser can seek to arbitrary frames.
    # Without 206 + Content-Range, timeline scrubbing in <video> is broken.
    upstream_headers: Dict[str, str] = {}
    range_header = request.headers.get("range")
    if range_header:
        upstream_headers["Range"] = range_header
    if_range = request.headers.get("if-range")
    if if_range:
        upstream_headers["If-Range"] = if_range

    client = httpx.AsyncClient(timeout=120.0)
    try:
        req = client.build_request("GET", task.video_url, headers=upstream_headers)
        worker_resp = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Video source unavailable")

    if worker_resp.status_code not in (200, 206):
        code = worker_resp.status_code if worker_resp.status_code in (401, 403, 404) else 502
        try:
            await worker_resp.aclose()
        finally:
            await client.aclose()
        raise HTTPException(status_code=code, detail="Video is unavailable")

    async def _close_stream_resources():
        try:
            await worker_resp.aclose()
        finally:
            await client.aclose()

    _vname = (
        f"{task_id}_video_small.mp4"
        if "_video_small.mp4" in (task.video_url or "")
        else f"{task_id}_video.mp4"
    )
    response_headers: Dict[str, str] = {
        "Content-Disposition": f'inline; filename="{_vname}"',
        "Cache-Control": "public, max-age=86400",
        # Prevent GZip middleware from wrapping video stream and breaking ranges.
        "Content-Encoding": "identity",
        "Accept-Ranges": "bytes",
    }

    content_length = worker_resp.headers.get("content-length")
    content_range = worker_resp.headers.get("content-range")
    etag = worker_resp.headers.get("etag")
    last_modified = worker_resp.headers.get("last-modified")
    if content_length:
        response_headers["Content-Length"] = content_length
    if content_range:
        response_headers["Content-Range"] = content_range
    if etag:
        response_headers["ETag"] = etag
    if last_modified:
        response_headers["Last-Modified"] = last_modified

    media_type = worker_resp.headers.get("content-type") or "video/mp4"
    return StreamingResponse(
        worker_resp.aiter_bytes(chunk_size=65536),
        status_code=worker_resp.status_code,
        media_type=media_type,
        headers=response_headers,
        background=BackgroundTask(_close_stream_resources),
    )


def _is_preview_asset(filename: str) -> bool:
    name = (filename or "").lower()
    if name.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4", ".mov", ".html", ".mview", ".json")):
        return True
    if name.endswith(".glb") and ("model_prepared" in name or "prepared" in name):
        return True
    return False


async def _has_paid_access(
    db: AsyncSession,
    user: Optional[User],
    task_id: str,
    file_index: Optional[int]
) -> bool:
    if not user:
        return False
    result = await db.execute(
        select(TaskFilePurchase).where(
            TaskFilePurchase.task_id == task_id,
            TaskFilePurchase.user_email == user.email
        )
    )
    purchases = result.scalars().all()
    if any(p.file_index is None for p in purchases):
        return True
    if file_index is None:
        return False
    purchased_indices = {p.file_index for p in purchases if p.file_index is not None}
    return file_index in purchased_indices


@app.get("/api/file/{task_id}/{file_index}")
async def proxy_file(
    task_id: str,
    file_index: int,
    user: Optional[User] = Depends(get_current_user),
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
        "webp": "image/webp",
        "gif": "image/gif",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "json": "application/json",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    # Allow preview assets without purchase; require purchase for downloads
    if not _is_preview_asset(clean_filename):
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required to download files")
        has_access = await _has_paid_access(db, user, task_id, file_index)
        if not has_access:
            raise HTTPException(status_code=402, detail="Payment required to download files")
    
    # Serve from local cache if present
    cached_path = TASK_CACHE_DIR / task_id / clean_filename
    if cached_path.exists():
        file_headers: Dict[str, str] = {"Cache-Control": "public, max-age=86400"}
        if content_type == "model/gltf-binary":
            file_headers["Content-Encoding"] = "identity"
        return FileResponse(
            cached_path,
            media_type=content_type,
            filename=clean_filename,
            headers=file_headers,
        )
    
    async def stream_file():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", file_url, timeout=120.0) as response:
                if response.status_code != 200:
                    return
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    stream_headers: Dict[str, str] = {
        "Content-Disposition": f"attachment; filename={clean_filename}",
        "Cache-Control": "public, max-age=86400",
    }
    if content_type == "model/gltf-binary":
        stream_headers["Content-Encoding"] = "identity"
    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers=stream_headers,
    )


@app.get("/api/file/{task_id}/download/{filename:path}")
async def proxy_file_by_name(
    task_id: str,
    filename: str,
    user: Optional[User] = Depends(get_current_user),
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
    
    # Determine index in download list (for purchase checks)
    download_urls = (task.output_urls or []) if (task.output_urls and len(task.output_urls) > 0) else (task.ready_urls or [])
    file_index = None
    for idx, url in enumerate(download_urls):
        url_clean = (url or "").strip()
        if url_clean.endswith(filename) or filename in url_clean.split('/')[-1]:
            file_index = idx
            break
    
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
        "webp": "image/webp",
        "gif": "image/gif",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "json": "application/json",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    # Allow preview assets without purchase; require purchase for downloads
    if not _is_preview_asset(clean_filename):
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required to download files")
        has_access = await _has_paid_access(db, user, task_id, file_index)
        if not has_access:
            raise HTTPException(status_code=402, detail="Payment required to download files")
        
        # Track download event for paid files
        if task.ga_client_id:
            asyncio.create_task(send_ga4_event(
                task.ga_client_id, 
                "rig_downloaded", 
                {"filename": clean_filename, "task_id": task_id}
            ))
    
    # Serve from local cache if present
    cached_path = TASK_CACHE_DIR / task_id / clean_filename
    if cached_path.exists():
        file_headers_dn: Dict[str, str] = {"Cache-Control": "public, max-age=86400"}
        if content_type == "model/gltf-binary":
            file_headers_dn["Content-Encoding"] = "identity"
        return FileResponse(
            cached_path,
            media_type=content_type,
            filename=clean_filename,
            headers=file_headers_dn,
        )
    
    async def stream_file():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", file_url, timeout=120.0) as response:
                if response.status_code != 200:
                    return
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
    
    stream_headers_dn: Dict[str, str] = {
        "Content-Disposition": f"attachment; filename={clean_filename}",
        "Cache-Control": "public, max-age=86400",
    }
    if content_type == "model/gltf-binary":
        stream_headers_dn["Content-Encoding"] = "identity"
    return StreamingResponse(
        stream_file(),
        media_type=content_type,
        headers=stream_headers_dn,
    )


@app.get("/api/task/{task_id}/cached-files")
async def api_task_cached_files(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Get list of cached files for a task.
    Returns files from /static/tasks/{task_id}/ if cached,
    otherwise triggers caching and returns status.
    """
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    cache_dir = TASK_CACHE_DIR / task_id
    
    # If files are already cached, return them
    if cache_dir.exists():
        files = []
        total_size = 0
        from urllib.parse import quote
        for f in sorted(cache_dir.iterdir()):
            if f.is_file() and not f.name.endswith('.tmp'):
                size = f.stat().st_size
                total_size += size
                files.append({
                    "name": f.name,
                    "size": size,
                    "url": f"/api/file/{task_id}/download/{quote(f.name)}"
                })
        
        if files:
            return {
                "cached": True,
                "task_id": task_id,
                "files": files,
                "total_size": total_size,
                "file_count": len(files)
            }
    
    # If task is done but not cached yet, trigger caching
    if task.status == "done" and (task.ready_urls or task.output_urls):
        urls_to_cache = []
        if task.ready_urls:
            urls_to_cache.extend(task.ready_urls)
        if task.output_urls:
            urls_to_cache.extend(task.output_urls)
        # Preserve order and remove duplicates
        urls_to_cache = list(dict.fromkeys(urls_to_cache))
        # Start caching in background
        result = await cache_task_files(task_id, urls_to_cache, task.guid)
        return {
            "cached": result["cached"],
            "task_id": task_id,
            "files": result["files"],
            "total_size": sum(f["size"] for f in result["files"]),
            "file_count": len(result["files"]),
            "errors": result.get("errors", [])
        }
    
    # Task not ready yet
    return {
        "cached": False,
        "task_id": task_id,
        "files": [],
        "total_size": 0,
        "file_count": 0,
        "message": "Task not completed yet" if task.status != "done" else "No files to cache"
    }


async def _ensure_purchased_worker_bundle_zip_url(
    db: AsyncSession,
    user: Optional[User],
    task_id: str,
    *,
    verify_worker_byte: bool,
) -> Tuple[Task, str]:
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != "done":
        raise HTTPException(status_code=400, detail="Task is not completed yet")
    if not await _has_full_task_download_purchase(db, user, task_id):
        raise HTTPException(status_code=402, detail="Full download purchase required")

    zip_url = resolve_worker_full_bundle_zip_url(task)
    if not zip_url:
        raise HTTPException(status_code=404, detail="Worker bundle URL could not be resolved")

    if verify_worker_byte:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream(
                    "GET",
                    zip_url,
                    follow_redirects=True,
                    headers={"Range": "bytes=0-0"},
                ) as r:
                    if r.status_code not in (200, 206):
                        raise HTTPException(
                            status_code=404,
                            detail=f"Worker bundle not available (HTTP {r.status_code})",
                        )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Worker bundle check failed: {e}") from e

    return task, zip_url


@app.get("/api/task/{task_id}/downloads/bundle-url")
async def api_task_worker_bundle_url(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Return absolute URL to the worker-hosted full bundle ZIP under /converter/glb/.
    Requires full-task download purchase. Verifies presence via GET + Range (HEAD often 404s on static).
    Browsers should download via GET /downloads/bundle (same-origin HTTPS) to avoid mixed-content warnings.
    """
    _, zip_url = await _ensure_purchased_worker_bundle_zip_url(
        db, user, task_id, verify_worker_byte=True
    )

    return {"task_id": task_id, "url": zip_url}


async def _stream_purchased_task_bundle_zip(
    task_id: str,
    user: Optional[User],
    db: AsyncSession,
) -> StreamingResponse:
    """Stream full-task ZIP from worker over HTTPS (same checks as bundle-url except byte probe)."""
    task, zip_url = await _ensure_purchased_worker_bundle_zip_url(
        db, user, task_id, verify_worker_byte=False
    )
    from telegram_bot import broadcast_full_bundle_download

    asyncio.create_task(broadcast_full_bundle_download(task_id, user.email))

    safe_guid = (task.guid or task_id).strip()
    filename = f"{safe_guid}.zip"
    return await _proxy_model_file(zip_url, filename, as_attachment=True)


@app.get("/api/task/{task_id}/downloads/bundle")
async def api_task_download_bundle(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Stream worker ZIP over HTTPS (proxied). Same purchase checks as bundle-url; avoids http:// worker in the browser.
    """
    return await _stream_purchased_task_bundle_zip(task_id, user, db)


@app.get("/api/task/{task_id}/bundle.zip")
async def api_task_download_bundle_zip(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Same as /downloads/bundle — stable path next to model.glb / prepared.glb (avoids missing route on some deploys)."""
    return await _stream_purchased_task_bundle_zip(task_id, user, db)


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


def _task_has_poster(task: Task) -> bool:
    """True if ready_urls/output_urls contain a file usable as /api/thumb source (same rules as api_proxy_thumb)."""
    urls = list(task.ready_urls or []) + list(task.output_urls or [])
    if not urls:
        return False
    if _find_file_in_ready_urls(urls, "_video_poster.jpg"):
        return True
    if _find_file_in_ready_urls(urls, "_poster.jpg"):
        return True
    if _find_file_in_ready_urls(urls, "icon.png"):
        return True
    if _find_file_in_ready_urls(urls, "Render_1_view.jpg"):
        return True
    return False


def resolve_poster_url_for_task(task: Task) -> Optional[str]:
    """First poster/thumb URL for /api/thumb — searches ready_urls and output_urls (same order as thumb proxy)."""
    urls = list(task.ready_urls or []) + list(task.output_urls or [])
    if not urls:
        return None
    for pattern in ("_video_poster.jpg", "_poster.jpg", "icon.png", "Render_1_view.jpg"):
        u = _find_file_in_ready_urls(urls, pattern)
        if u:
            return u.strip()
    return None


def _gallery_task_has_poster_sql():
    """
    SQL condition aligned with _task_has_poster(): JSON URL text must contain a thumb filename.
    Used so /api/gallery does not list tasks whose /api/thumb would 404.
    """
    pats = ("_video_poster.jpg", "_poster.jpg", "icon.png", "Render_1_view.jpg")
    cols = (Task._ready_urls, Task._output_urls)
    return or_(*[func.instr(col, p) > 0 for col in cols for p in pats])


def _resolve_worker_base_from_task(task) -> Optional[str]:
    """Resolve worker base URL from task metadata without requiring worker_api."""
    from workers import get_worker_base_url

    # 1) Best source: normalized worker_api.
    if getattr(task, "worker_api", None):
        try:
            return get_worker_base_url(task.worker_api)
        except Exception:
            pass

    # 2) Progress page can contain the worker host.
    progress_page = str(getattr(task, "progress_page", "") or "").strip()
    if progress_page:
        try:
            parsed = urlparse(progress_page)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            pass

    # 3) Fallback to any known ready/output URL host.
    for url in (list(getattr(task, "ready_urls", []) or []) + list(getattr(task, "output_urls", []) or [])):
        try:
            parsed = urlparse(str(url).strip())
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            continue

    return None


# Skip GZipMiddleware for GLB: browsers send Accept-Encoding: gzip; streaming gzip
# over HTTP/2 has caused ERR_HTTP2_PROTOCOL_ERROR in the wild (same idea as video proxy).
_GLB_FILE_HTTP_HEADERS: Dict[str, str] = {
    "Cache-Control": "public, max-age=86400",
    "Access-Control-Allow-Origin": "*",
    "Content-Encoding": "identity",
}


async def _proxy_model_file(
    url: str,
    filename: str,
    *,
    as_attachment: bool = False,
) -> StreamingResponse:
    """Proxy a model file from worker with streaming.

    Uses streaming to avoid timeout on slow workers.
    Content-Encoding identity skips app-level gzip on binary streams.
    """
    # Determine content type
    ext = filename.split(".")[-1].lower()
    content_types = {
        "glb": "model/gltf-binary",
        "fbx": "application/octet-stream",
        "zip": "application/zip",
    }
    content_type = content_types.get(ext, "application/octet-stream")
    
    # Large ZIP bundles: allow long reads from worker (nginx should use long proxy_read_timeout too).
    client = httpx.AsyncClient(timeout=httpx.Timeout(connect=60.0, read=600.0, write=60.0, pool=60.0), follow_redirects=True)
    try:
        req = client.build_request("GET", url)
        upstream = await client.send(req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Model source unavailable: {e}")

    if upstream.status_code != 200:
        status = 404 if upstream.status_code == 404 else 502
        try:
            await upstream.aclose()
        finally:
            await client.aclose()
        raise HTTPException(status_code=status, detail=f"Model source returned HTTP {upstream.status_code}")

    async def _close_stream_resources():
        try:
            await upstream.aclose()
        finally:
            await client.aclose()

    disp = "attachment" if as_attachment else "inline"
    headers = {
        "Content-Disposition": f'{disp}; filename="{filename}"',
        "Cache-Control": "private, max-age=0" if as_attachment else "public, max-age=86400",
        "Access-Control-Allow-Origin": "*",
        "X-Content-Type-Options": "nosniff",
        "Content-Encoding": "identity",
    }
    content_length = upstream.headers.get("content-length")
    last_modified = upstream.headers.get("last-modified")
    etag = upstream.headers.get("etag")
    if content_length:
        headers["Content-Length"] = content_length
    if last_modified:
        headers["Last-Modified"] = last_modified
    if etag:
        headers["ETag"] = etag

    media_type = upstream.headers.get("content-type") or content_type
    return StreamingResponse(
        upstream.aiter_bytes(chunk_size=131072),  # 128KB chunks
        media_type=media_type,
        headers=headers,
        background=BackgroundTask(_close_stream_resources),
    )


def _validate_glb(data: bytes) -> bool:
    """Validate GLB file is complete and not corrupted.
    
    GLB header format (12 bytes):
    - bytes 0-3: magic "glTF" (0x46546C67)
    - bytes 4-7: version (should be 2)
    - bytes 8-11: total file length (little endian)
    """
    if len(data) < 12:
        return False
    # Check magic header
    if data[:4] != b'glTF':
        return False
    # Check version (should be 1 or 2)
    version = int.from_bytes(data[4:8], 'little')
    if version not in (1, 2):
        return False
    # Check file length matches header
    expected_length = int.from_bytes(data[8:12], 'little')
    if len(data) != expected_length:
        print(f"[GLB Validate] Length mismatch: header says {expected_length}, actual {len(data)}")
        return False
    return True


def _validate_glb_file(path: Path) -> bool:
    """Validate GLB header and declared length without loading the whole file into RAM."""
    try:
        actual_size = path.stat().st_size
        if actual_size < 12:
            return False
        with path.open("rb") as f:
            header = f.read(12)
    except Exception as e:
        print(f"[GLB Validate] Failed to read {path.name}: {e}")
        return False

    if header[:4] != b"glTF":
        return False
    version = int.from_bytes(header[4:8], "little")
    if version not in (1, 2):
        return False
    expected_length = int.from_bytes(header[8:12], "little")
    if actual_size != expected_length:
        print(f"[GLB Validate] Length mismatch for {path.name}: header says {expected_length}, actual {actual_size}")
        return False
    return True


async def _stream_httpx_response_to_file(response: httpx.Response, destination: Path) -> int:
    """Stream an upstream response to disk to avoid buffering large files in memory."""
    bytes_written = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("wb") as f:
            async for chunk in response.aiter_bytes(1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                bytes_written += len(chunk)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
            try:
                if hasattr(os, "posix_fadvise") and hasattr(os, "POSIX_FADV_DONTNEED"):
                    os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            except OSError:
                pass
        return bytes_written
    except Exception:
        try:
            destination.unlink()
        except OSError:
            pass
        raise


def _extract_upload_token_from_input_url(input_url: Optional[str]) -> Optional[str]:
    if not input_url:
        return None
    try:
        parsed = urlparse(input_url)
        path_parts = [part for part in (parsed.path or "").split("/") if part]
    except Exception:
        return None
    if len(path_parts) >= 2 and path_parts[0] == "u":
        return path_parts[1]
    return None


def _estimate_path_size(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        if path.is_file():
            return path.stat().st_size
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    except Exception:
        return 0


def _iter_task_artifact_paths(task: Task) -> List[Path]:
    paths: List[Path] = [
        TASK_CACHE_DIR / task.id,
        Path("/var/autorig/videos") / f"{task.id}.mp4",
    ]
    paths.extend(GLB_CACHE_DIR.glob(f"{task.id}_*.glb"))
    upload_token = _extract_upload_token_from_input_url(getattr(task, "input_url", None))
    if upload_token:
        paths.append(Path(UPLOAD_DIR) / upload_token)

    unique_paths: List[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(path)
    return unique_paths


def _delete_task_artifacts(task: Task) -> tuple[int, int]:
    deleted_items = 0
    freed_bytes = 0
    for path in _iter_task_artifact_paths(task):
        if not path.exists():
            continue
        item_size = _estimate_path_size(path)
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            deleted_items += 1
            freed_bytes += item_size
        except Exception as e:
            print(f"[Disk Cleanup] Failed to delete task artifact {path}: {e}")
    return deleted_items, freed_bytes


async def _delete_task_record_and_related(db: AsyncSession, task_id: str) -> None:
    await db.execute(delete(TaskLike).where(TaskLike.task_id == task_id))
    await db.execute(delete(TaskFilePurchase).where(TaskFilePurchase.task_id == task_id))
    await db.execute(delete(TaskAnimationPurchase).where(TaskAnimationPurchase.task_id == task_id))
    await db.execute(delete(TaskAnimationBundlePurchase).where(TaskAnimationBundlePurchase.task_id == task_id))
    await db.execute(delete(Task).where(Task.id == task_id))
    await db.commit()


async def admin_delete_task_full(db: AsyncSession, task_id: str) -> bool:
    """Remove task files on disk and DB row + related rows. Returns True if the task existed."""
    task = await get_task_by_id(db, task_id)
    if not task:
        return False
    _delete_task_artifacts(task)
    await _delete_task_record_and_related(db, task_id)
    return True


async def process_stuck_hour_tasks(db: AsyncSession) -> int:
    """
    processing + 0 ready, no real progress for >= STUCK_HOUR_MINUTES:
    requeue (admin-style) up to STUCK_HOUR_MAX_REQUEUES times, then delete task + artifacts.
    """
    now = datetime.utcnow()
    r = await db.execute(select(Task).where(Task.status == "processing"))
    processing = list(r.scalars().all())
    to_requeue: List[Task] = []
    to_delete_ids: List[str] = []
    for task in processing:
        if (task.ready_count or 0) != 0:
            continue
        if get_task_no_progress_minutes(task, now=now) < STUCK_HOUR_MINUTES:
            continue
        cnt = task.stuck_hour_requeue_count or 0
        if cnt >= STUCK_HOUR_MAX_REQUEUES:
            to_delete_ids.append(task.id)
        else:
            to_requeue.append(task)
    actions = 0
    for task in to_requeue:
        try:
            await admin_requeue_task_to_created(db, task)
            task.stuck_hour_requeue_count = (task.stuck_hour_requeue_count or 0) + 1
            actions += 1
            print(
                f"[StuckHour] Requeued {task.id} (stuck_hour_requeue_count={task.stuck_hour_requeue_count})"
            )
        except Exception as e:
            print(f"[StuckHour] Requeue failed {task.id}: {e}")
    if to_requeue:
        await db.commit()
    for tid in to_delete_ids:
        try:
            t = await get_task_by_id(db, tid)
            if not t:
                continue
            _delete_task_artifacts(t)
            await _delete_task_record_and_related(db, tid)
            actions += 1
            print(f"[StuckHour] Deleted {tid} (stuck_hour requeues exhausted)")
        except Exception as e:
            print(f"[StuckHour] Delete failed {tid}: {e}")
    return actions


def purge_task_cache_bundle_zips(
    record_items: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[int, int]:
    """
    Remove regenerable **/*.zip under TASK_CACHE_DIR (full-task download bundles).
    If record_items is a list, append the same entries cleanup_disk_space uses for admin summaries.
    Returns (deleted_file_count, freed_bytes).
    """
    deleted = 0
    freed = 0
    if not TASK_CACHE_DIR.exists():
        return deleted, freed
    for zp in TASK_CACHE_DIR.rglob("*.zip"):
        if not zp.is_file():
            continue
        try:
            sz = zp.stat().st_size
            zp.unlink()
            deleted += 1
            freed += sz
            print(f"[Disk] Removed bundle zip {zp} ({sz / (1024**2):.1f} MB)")
            if record_items is not None:
                try:
                    rel = str(zp.relative_to(TASK_CACHE_DIR))
                except ValueError:
                    rel = zp.name
                record_items.append(
                    {"path": rel, "type": "bundle_zip", "size_mb": sz / (1024**2)}
                )
        except Exception as e:
            print(f"[Disk] Failed to remove zip {zp}: {e}")
    return deleted, freed


async def ensure_disk_headroom_for_new_task(db: AsyncSession) -> dict:
    """
    Run when creating a new task: if free space on / is below NEW_TASK_MIN_FREE_GB,
    first delete bundle ZIPs; if still low, delete oldest done/error tasks (DB + artifacts)
    until free space target is met or NEW_TASK_PURGE_TASKS_MAX_FREED_GB of data was removed
    in that second phase (whichever comes first).
    """
    target_bytes = NEW_TASK_MIN_FREE_GB * 1024 * 1024 * 1024
    max_task_phase_bytes = NEW_TASK_PURGE_TASKS_MAX_FREED_GB * 1024 * 1024 * 1024

    free_bytes = shutil.disk_usage("/").free
    summary: Dict[str, Any] = {
        "initial_free_gb": free_bytes / (1024**3),
        "zips_deleted": 0,
        "zip_freed_bytes": 0,
        "tasks_purged": 0,
        "task_phase_freed_bytes": 0,
        "final_free_gb": free_bytes / (1024**3),
    }

    if free_bytes >= target_bytes:
        return summary

    zd, zb = purge_task_cache_bundle_zips()
    summary["zips_deleted"] = zd
    summary["zip_freed_bytes"] = zb

    free_bytes = shutil.disk_usage("/").free
    summary["final_free_gb"] = free_bytes / (1024**3)
    if free_bytes >= target_bytes:
        if zd:
            print(
                f"[NewTask Disk] ZIP purge: removed {zd} file(s), "
                f"{free_bytes / (1024**3):.2f} GB free (target {NEW_TASK_MIN_FREE_GB} GB)"
            )
        return summary

    task_phase_freed = 0
    res = await db.execute(
        select(Task)
        .where(Task.status.in_(["done", "error"]))
        .order_by(Task.created_at.asc().nulls_last())
    )
    candidates: List[Task] = list(res.scalars().all())

    for task in candidates:
        free_bytes = shutil.disk_usage("/").free
        if free_bytes >= target_bytes:
            break
        if task_phase_freed >= max_task_phase_bytes:
            print(
                f"[NewTask Disk] Task purge cap reached "
                f"({task_phase_freed / (1024**3):.2f} / {NEW_TASK_PURGE_TASKS_MAX_FREED_GB} GB); "
                f"free {free_bytes / (1024**3):.2f} GB"
            )
            break

        _del_n, f_bytes = _delete_task_artifacts(task)
        try:
            await _delete_task_record_and_related(db, task.id)
            summary["tasks_purged"] += 1
            task_phase_freed += f_bytes
            summary["task_phase_freed_bytes"] = task_phase_freed
            print(
                f"[NewTask Disk] Purged oldest terminal task {task.id} ({task.status}), "
                f"~{f_bytes / (1024**2):.1f} MB artifacts"
            )
        except Exception as e:
            await db.rollback()
            print(f"[NewTask Disk] Failed to purge task {task.id}: {e}")

    free_bytes = shutil.disk_usage("/").free
    summary["final_free_gb"] = free_bytes / (1024**3)
    if summary["zips_deleted"] or summary["tasks_purged"]:
        print(
            f"[NewTask Disk] Headroom pass done: zips={summary['zips_deleted']}, "
            f"tasks={summary['tasks_purged']}, free now {summary['final_free_gb']:.2f} GB "
            f"(target {NEW_TASK_MIN_FREE_GB} GB)"
        )
    if free_bytes < target_bytes:
        try:
            from telegram_bot import broadcast_disk_space_low

            await broadcast_disk_space_low(
                free_gb=summary["final_free_gb"],
                target_gb=NEW_TASK_MIN_FREE_GB,
                zips_deleted=summary["zips_deleted"],
                tasks_purged=summary["tasks_purged"],
            )
        except Exception as e:
            print(f"[NewTask Disk] Telegram disk-low notify failed: {e}")
    return summary


async def purge_tasks_without_poster_and_video(db: AsyncSession) -> dict:
    """
    Delete terminal tasks (done/error) with no poster-like URL in ready_urls/output_urls.
    Covers: no video + no poster; and video_ready=true in DB but thumb paths missing (broken gallery).

    Keeps task when a poster path exists but video_ready is false (may still have file downloads).
    """
    result = await db.execute(select(Task).where(Task.status.in_(["done", "error"])))
    rows = list(result.scalars().all())
    deleted = 0
    for task in rows:
        if _task_has_poster(task):
            continue
        _delete_task_artifacts(task)
        try:
            await _delete_task_record_and_related(db, task.id)
            deleted += 1
        except Exception as e:
            await db.rollback()
            print(f"[Purge no-assets tasks] Failed {task.id}: {e}")
    return {"deleted": deleted, "scanned": len(rows)}


async def _probe_http_asset_reachable(client, url: str) -> bool:
    """True if worker still serves this URL (HEAD or tiny ranged GET)."""
    if not url or not str(url).strip():
        return False
    url = str(url).strip()
    try:
        r = await client.head(url, timeout=15.0, follow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code in (404, 410):
            return False
    except Exception:
        pass
    try:
        r = await client.get(
            url,
            timeout=15.0,
            follow_redirects=True,
            headers={"Range": "bytes=0-0"},
        )
        return r.status_code in (200, 206)
    except Exception:
        return False


async def purge_gallery_upstream_dead_tasks(
    db: AsyncSession, batch: Optional[int] = None, offset: int = 0
) -> dict:
    """
    Remove done+video_ready tasks that still match gallery SQL but poster or video 404 on worker.
    Workers delete files over time; DB strings become stale — this aligns DB with reality.

    Use ``offset`` to skip a block of rows that already passed HEAD checks (no deletes in prior batch).
    """
    import httpx

    limit = batch if batch is not None else GALLERY_UPSTREAM_PURGE_BATCH
    result = await db.execute(
        select(Task)
        .where(
            Task.status == "done",
            Task.video_ready.is_(True),
            _gallery_task_has_poster_sql(),
        )
        .order_by(Task.created_at.asc())
        .offset(max(0, offset))
        .limit(limit)
    )
    rows = list(result.scalars().all())
    if not rows:
        return {"deleted": 0, "scanned": 0, "upstream": True, "offset": offset}

    deleted = 0
    async with httpx.AsyncClient() as client:
        for task in rows:
            poster_url = resolve_poster_url_for_task(task)
            if not poster_url:
                _delete_task_artifacts(task)
                try:
                    await _delete_task_record_and_related(db, task.id)
                    deleted += 1
                except Exception as e:
                    await db.rollback()
                    print(f"[Gallery upstream purge] Failed {task.id} (no poster url): {e}")
                continue
            if not await _probe_http_asset_reachable(client, poster_url):
                _delete_task_artifacts(task)
                try:
                    await _delete_task_record_and_related(db, task.id)
                    deleted += 1
                except Exception as e:
                    await db.rollback()
                    print(f"[Gallery upstream purge] Failed {task.id} (dead poster): {e}")
                continue
            if task.video_url and not await _probe_http_asset_reachable(client, task.video_url):
                _delete_task_artifacts(task)
                try:
                    await _delete_task_record_and_related(db, task.id)
                    deleted += 1
                except Exception as e:
                    await db.rollback()
                    print(f"[Gallery upstream purge] Failed {task.id} (dead video): {e}")
    return {
        "deleted": deleted,
        "scanned": len(rows),
        "upstream": True,
        "offset": offset,
    }


async def _get_cached_glb(task_id: str, url: str, cache_name: str) -> Optional[FileResponse]:
    """
    Get GLB file from local cache, or download and cache it.
    Returns FileResponse for cached file, or None if download failed.
    Validates GLB integrity before caching and when serving.
    """
    cache_path = GLB_CACHE_DIR / f"{task_id}_{cache_name}.glb"
    
    # Check if already cached
    if cache_path.exists():
        # Validate cached file is not corrupted
        try:
            if not _validate_glb_file(cache_path):
                print(f"[GLB Cache] Cached file corrupted, deleting: {cache_path.name}")
                cache_path.unlink()
            else:
                return FileResponse(
                    path=str(cache_path),
                    media_type="model/gltf-binary",
                    filename=f"{task_id}_{cache_name}.glb",
                    headers=dict(_GLB_FILE_HTTP_HEADERS),
                )
        except Exception as e:
            print(f"[GLB Cache] Error validating cached file: {e}")
            try:
                cache_path.unlink()
            except:
                pass
    
    # Download and cache
    try:
        async with httpx.AsyncClient() as client:
            temp_path = cache_path.with_suffix(".tmp")
            async with client.stream("GET", url, timeout=120.0, follow_redirects=True) as response:
                if response.status_code != 200:
                    return None
                size_bytes = await _stream_httpx_response_to_file(response, temp_path)

            if not _validate_glb_file(temp_path):
                print(f"[GLB Cache] Downloaded file is invalid/incomplete for {task_id}_{cache_name}")
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                return None

            temp_path.replace(cache_path)
            print(f"[GLB Cache] Cached valid GLB: {cache_path.name} ({size_bytes} bytes)")

            return FileResponse(
                path=str(cache_path),
                media_type="model/gltf-binary",
                filename=f"{task_id}_{cache_name}.glb",
                headers=dict(_GLB_FILE_HTTP_HEADERS),
            )
    except Exception as e:
        print(f"[GLB Cache] Failed to cache {cache_name} for {task_id}: {e}")
        return None


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
    """Get animations GLB file with server-side caching"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.guid:
        raise HTTPException(status_code=404, detail="Model not available yet")
    
    # Check cache first (fastest path)
    cache_path = GLB_CACHE_DIR / f"{task_id}_animations.glb"
    if cache_path.exists():
        return FileResponse(
            path=str(cache_path),
            media_type="model/gltf-binary",
            filename=f"{task_id}_animations.glb",
            headers=dict(_GLB_FILE_HTTP_HEADERS),
        )
    
    # Try to find animations GLB in ready_urls (must end with .glb, not .blend)
    animations_url = _find_file_in_ready_urls(task.ready_urls or [], "_all_animations", ".glb")
    if animations_url:
        result = await _get_cached_glb(task_id, animations_url, "animations")
        if result:
            return result

    # Fallback: synthesize canonical all_animations path from worker root.
    worker_base = _resolve_worker_base_from_task(task)
    if worker_base:
        synthesized_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_all_animations.glb"
        result = await _get_cached_glb(task_id, synthesized_url, "animations")
        if result:
            return result

    raise HTTPException(status_code=404, detail="Animations GLB not available yet")


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
    """Get prepared GLB file with server-side caching for fast loading"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    from workers import get_worker_base_url
    
    # Check cache first (fastest path)
    cache_path = GLB_CACHE_DIR / f"{task_id}_prepared.glb"
    if cache_path.exists():
        return FileResponse(
            path=str(cache_path),
            media_type="model/gltf-binary",
            filename=f"{task_id}_prepared.glb",
            headers=dict(_GLB_FILE_HTTP_HEADERS),
        )
    
    # 1. Try to find _model_prepared.glb in ready_urls (best option for preview)
    prepared_url = _find_file_in_ready_urls(task.ready_urls or [], "_model_prepared.glb")
    if prepared_url:
        result = await _get_cached_glb(task_id, prepared_url, "prepared")
        if result:
            return result
    
    # 2. Fallback: try direct URL to _model_prepared.glb on worker
    if task.guid and task.worker_api:
        worker_base = get_worker_base_url(task.worker_api)
        direct_prepared_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}_model_prepared.glb"
        result = await _get_cached_glb(task_id, direct_prepared_url, "prepared")
        if result:
            return result
    
    # 3. For FBX tasks, use fbx_glb_output_url
    if task.fbx_glb_output_url and task.fbx_glb_ready:
        result = await _get_cached_glb(task_id, task.fbx_glb_output_url, "prepared")
        if result:
            return result
    
    # 4. Prepared model not available yet - return 404
    # NOTE: Don't fall back to original model ({guid}.glb) as it's not "prepared" 
    # and would cause viewer to set preparedLoaded=true, skipping the actual prepared version
    raise HTTPException(status_code=404, detail="Prepared model not available yet")


@app.get("/api/thumb/{task_id}")
async def api_proxy_thumb(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy video poster/thumbnail image from worker"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    poster_url = resolve_poster_url_for_task(task)
    if not poster_url:
        raise HTTPException(status_code=404, detail="Thumbnail not available")
    
    # Download and return the image (not streaming - more compatible with HTTP/2)
    try:
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
    except httpx.TimeoutException as error:
        raise HTTPException(status_code=504, detail="Thumbnail upstream timed out") from error
    except httpx.RequestError as error:
        raise HTTPException(status_code=502, detail=f"Thumbnail upstream request failed: {error}") from error


# =============================================================================
# Free3D Model Search Proxy
# =============================================================================

FREE3D_BASE_URL = "https://free3d.online"


def _normalize_free3d_item(item: dict) -> Optional[dict]:
    """Normalize Free3D API item into stable frontend shape."""
    if not isinstance(item, dict):
        return None

    guid = (item.get("guid") or "").strip()
    if not guid:
        return None

    title = (item.get("title") or item.get("name") or "Untitled").strip() or "Untitled"
    model_page_url = item.get("modelPageUrl") or f"/models/{guid}"

    preview_small = item.get("previewSmallUrl")
    preview_medium = item.get("previewMediumUrl")
    if not preview_small and preview_medium:
        preview_small = preview_medium
    if not preview_medium and preview_small:
        preview_medium = preview_small

    def _to_abs(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"{FREE3D_BASE_URL}{url}"

    viewer_asset_base = f"{FREE3D_BASE_URL}/viewer-asset/{guid}"
    return {
        "guid": guid,
        "title": title,
        "type": item.get("type"),
        "typeLabel": item.get("typeLabel"),
        "category": item.get("category"),
        "modelPageUrl": _to_abs(model_page_url),
        "previewSmallUrl": preview_small,
        "previewMediumUrl": preview_medium,
        "previewSmallAbsUrl": _to_abs(preview_small),
        "previewMediumAbsUrl": _to_abs(preview_medium),
        # Stable public asset URLs (no auth required)
        "glb_url": f"{viewer_asset_base}/glb100k",
        "glb1k_url": f"{viewer_asset_base}/glb1k",
        "glb10k_url": f"{viewer_asset_base}/glb10k",
        "glb100k_url": f"{viewer_asset_base}/glb100k",
        "glb_base_url": f"{viewer_asset_base}/glb",
        "animations_url": f"{viewer_asset_base}/animations100k",
    }

@app.get("/api/free3d/search")
async def api_free3d_search(
    q: str = "",
    query: str = "",
    topK: int = 20,
    offset: int = 0,
    sort: str = "relevance",
    type: Optional[int] = None,
    category: Optional[str] = None,
    categories: Optional[str] = None,
    mode: str = "semantic",
):
    """Proxy search requests to Free3D API with normalized fail-safe response."""
    search_query = q or query
    safe_topk = max(1, min(topK, 100))
    safe_offset = max(0, offset)
    normalized_mode = (mode or "semantic").strip().lower()
    use_browse = normalized_mode == "browse" or not search_query

    params: Dict[str, Any] = {
        "topK": safe_topk,
        "offset": safe_offset,
        "sort": sort or ("popular" if use_browse else "relevance"),
    }
    if not use_browse and search_query:
        params["q"] = search_query
    if type is not None:
        params["type"] = type
    # Free3D semantic endpoint can return empty results when legacy clients
    # pass category filters (e.g. category=characters). Keep category filters
    # only for browse mode and ignore them for semantic search.
    if use_browse and category:
        params["category"] = category
    if use_browse and categories:
        params["categories"] = categories

    endpoint = "/api-embeddings/browse" if use_browse else "/api-embeddings/"

    last_error: Optional[str] = None
    for attempt in range(2):
        timeout = 8.0 if attempt == 0 else 12.0
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(f"{FREE3D_BASE_URL}{endpoint}", params=params)
                resp.raise_for_status()
                payload = resp.json()

            raw_results = []
            if isinstance(payload, dict):
                raw_results = payload.get("results") or payload.get("items") or []
            normalized_results = []
            for item in raw_results:
                normalized = _normalize_free3d_item(item)
                if normalized:
                    normalized_results.append(normalized)

            return {
                "ok": True,
                "degraded": False,
                "source": "browse" if use_browse else "semantic",
                "query": search_query,
                "total": payload.get("total", len(normalized_results)) if isinstance(payload, dict) else len(normalized_results),
                "hasMore": bool(payload.get("hasMore")) if isinstance(payload, dict) else False,
                "offset": safe_offset,
                "results": normalized_results,
            }
        except Exception as e:
            last_error = str(e)
            if attempt == 0:
                continue

            print(f"[Free3D] Search error ({endpoint}): {e}")
            return {
                "ok": False,
                "degraded": True,
                "source": "browse" if use_browse else "semantic",
                "query": search_query,
                "total": 0,
                "hasMore": False,
                "offset": safe_offset,
                "results": [],
                "error": last_error or "free3d_upstream_unavailable",
            }

    try:
        # Defensive fallback; loop above should always return.
        return {
            "ok": False,
            "degraded": True,
            "source": "browse" if use_browse else "semantic",
            "query": search_query,
            "total": 0,
            "hasMore": False,
            "offset": safe_offset,
            "results": [],
            "error": last_error or "free3d_unknown_error",
        }
    except Exception:
        return {"ok": False, "degraded": True, "results": []}


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
            "Access-Control-Allow-Origin": "*",
            "Content-Encoding": "identity",
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


@app.post("/api/notify/credits-click")
async def notify_credits_click(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Notify Telegram about credits purchase click"""
    try:
        body = await request.json()
        package = body.get('package', 'unknown')
        price = body.get('price', 'unknown')
        
        # Get user info from session
        user_email = None
        anon_id = None
        
        session_id = request.cookies.get("session_id")
        if session_id:
            result = await db.execute(
                select(UserSession).where(UserSession.session_id == session_id)
            )
            sess = result.scalar_one_or_none()
            if sess:
                user_result = await db.execute(
                    select(User).where(User.id == sess.user_id)
                )
                user = user_result.scalar_one_or_none()
                if user:
                    user_email = user.email
        
        if not user_email:
            anon_id = request.cookies.get("anon_id")
        
        # Fire-and-forget notification
        from telegram_bot import broadcast_credits_purchase_click
        asyncio.create_task(broadcast_credits_purchase_click(
            package=package,
            price=price,
            user_email=user_email,
            anon_id=anon_id
        ))
        
        return {"ok": True}
    except Exception as e:
        print(f"[Notify] Credits click error: {e}")
        return {"ok": False, "error": str(e)}


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


@app.get("/api/task/{task_id}/face-rig/analysis")
async def api_get_face_rig_analysis(
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    analysis = _load_face_rig_analysis(task)
    return {
        "ready": bool(analysis),
        "analysis": analysis,
        "updated_at": task.face_rig_analysis_updated_at.isoformat() if getattr(task, "face_rig_analysis_updated_at", None) else None,
    }


@app.post("/api/task/{task_id}/face-rig/analyze-head")
async def api_proxy_face_rig_analyze_head(
    task_id: str,
    metadata: str = Form(...),
    force: str = Form("0"),
    front_rgb_pbr: UploadFile = File(...),
    front_depth: UploadFile = File(...),
    front_alpha: UploadFile = File(...),
    front_albedo: Optional[UploadFile] = File(None),
    front_normal: Optional[UploadFile] = File(None),
    left_3q_rgb_pbr: Optional[UploadFile] = File(None),
    right_3q_rgb_pbr: Optional[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db),
):
    """Proxy face-rig head analysis requests to the external HTTPS worker."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    force_refresh = str(force).strip().lower() in {"1", "true", "yes", "on"}

    if not force_refresh:
        cached = _load_face_rig_analysis(task)
        if cached:
            return JSONResponse(content=cached, headers={"Cache-Control": "no-store", "X-Face-Rig-Cache": "hit"})

    uploads = {
        "front_rgb_pbr": front_rgb_pbr,
        "front_depth": front_depth,
        "front_alpha": front_alpha,
        "front_albedo": front_albedo,
        "front_normal": front_normal,
        "left_3q_rgb_pbr": left_3q_rgb_pbr,
        "right_3q_rgb_pbr": right_3q_rgb_pbr,
    }

    multipart_files = []
    lock = _get_face_rig_analysis_lock(task_id)
    async with lock:
        await db.refresh(task)
        if not force_refresh:
            cached = _load_face_rig_analysis(task)
            if cached:
                for upload in uploads.values():
                    if upload:
                        await upload.close()
                return JSONResponse(content=cached, headers={"Cache-Control": "no-store", "X-Face-Rig-Cache": "hit"})

        try:
            for field_name, upload in uploads.items():
                if not upload:
                    continue
                content = await upload.read()
                multipart_files.append((
                    field_name,
                    (
                        upload.filename or f"{field_name}.png",
                        content,
                        upload.content_type or "application/octet-stream",
                    ),
                ))

            timeout = httpx.Timeout(connect=20.0, read=240.0, write=240.0, pool=20.0)
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                upstream = await client.post(
                    FACE_RIG_ANALYZE_HEAD_PROXY_URL,
                    data={"metadata": metadata},
                    files=multipart_files,
                )
        except httpx.RequestError as error:
            raise HTTPException(status_code=502, detail=f"Face rig worker request failed: {error}") from error
        finally:
            for upload in uploads.values():
                if upload:
                    await upload.close()

        content_type = upstream.headers.get("content-type", "application/json")
        if upstream.status_code == 200 and "application/json" in content_type:
            try:
                cached_payload = upstream.json()
                task.face_rig_analysis = json.dumps(cached_payload, ensure_ascii=False)
                task.face_rig_analysis_updated_at = datetime.utcnow()
                await db.commit()
                return JSONResponse(
                    content=cached_payload,
                    headers={"Cache-Control": "no-store", "X-Face-Rig-Cache": "miss"},
                )
            except Exception as error:
                print(f"[FaceRig] Failed to cache analysis for task {task_id}: {error}")

        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers={
                "Content-Type": content_type,
                "Cache-Control": "no-store",
                "X-Face-Rig-Cache": "bypass" if force_refresh else "miss",
            },
        )


# =============================================================================
# Static Files & Pages
# =============================================================================
# Get the directory containing this file
import pathlib
BASE_DIR = pathlib.Path(__file__).parent.parent
STATIC_DIR = BASE_DIR / "static"
TASK_CACHE_DIR = STATIC_DIR / "tasks"  # Cached task files for download
GLB_CACHE_DIR = STATIC_DIR / "glb_cache"  # Cached GLB files for fast loading

# Ensure cache directories exist
TASK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
GLB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _task_cache_dir_size_bytes(task_id: str) -> Optional[int]:
    """Sum of file sizes under static/tasks/{task_id}/ when cache exists."""
    d = TASK_CACHE_DIR / task_id
    if not d.is_dir():
        return None
    total = 0
    try:
        for p in d.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
    except OSError:
        return None
    return total if total > 0 else None


def _task_age_seconds(created_at: Optional[datetime]) -> int:
    """Elapsed whole seconds from task creation to current UTC time (server clock)."""
    if created_at is None:
        return 0
    try:
        now = datetime.now(timezone.utc)
        ca = created_at
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=timezone.utc)
        else:
            ca = ca.astimezone(timezone.utc)
        return max(0, int((now - ca).total_seconds()))
    except Exception:
        return 0


# =============================================================================
# Task File Caching (replaces ZIP downloads)
# =============================================================================
import re as _re_module

def _clean_filename_for_cache(url: str, guid: str = None) -> str:
    """
    Extract and clean filename from URL for caching.
    Removes GUID prefix from filename for cleaner downloads.
    """
    # Get filename from URL path
    from urllib.parse import urlparse, unquote
    path = urlparse(url).path
    filename = unquote(path.split('/')[-1])
    
    # Remove GUID prefix if present (e.g., "abc123-def456_model.glb" -> "model.glb")
    if guid:
        # Pattern: {guid}_ at the start of filename
        guid_pattern = _re_module.compile(rf'^{_re_module.escape(guid)}_', _re_module.IGNORECASE)
        filename = guid_pattern.sub('', filename)
    
    # Also remove any UUID-like prefix
    uuid_pattern = _re_module.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_', _re_module.IGNORECASE)
    filename = uuid_pattern.sub('', filename)
    
    return filename


async def cache_task_files(task_id: str, ready_urls: list, guid: str = None) -> dict:
    """
    Download ready files from worker and cache them in static directory.
    Returns dict with cached file info.
    
    Args:
        task_id: Task ID for cache directory
        ready_urls: List of URLs to download
        guid: Optional GUID to strip from filenames
    
    Returns:
        {"cached": True/False, "files": [...], "errors": [...]}
    """
    cache_dir = TASK_CACHE_DIR / task_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    cached_files = []
    errors = []
    
    from urllib.parse import quote
    async with httpx.AsyncClient(timeout=120.0) as client:
        for url in ready_urls:
            try:
                filename = _clean_filename_for_cache(url, guid)
                filepath = cache_dir / filename
                
                # Skip if already cached
                if filepath.exists():
                    cached_files.append({
                        "name": filename,
                        "size": filepath.stat().st_size,
                        "url": f"/api/file/{task_id}/download/{quote(filename)}"
                    })
                    continue
                
                # Download file
                async with client.stream("GET", url, follow_redirects=True) as response:
                    if response.status_code == 200:
                        # Write to temp file first, then rename (atomic)
                        temp_path = filepath.with_suffix('.tmp')
                        size_bytes = await _stream_httpx_response_to_file(response, temp_path)
                        temp_path.rename(filepath)
                        
                        cached_files.append({
                            "name": filename,
                            "size": size_bytes,
                            "url": f"/api/file/{task_id}/download/{quote(filename)}"
                        })
                        print(f"[Cache] Cached {filename} for task {task_id} ({size_bytes} bytes)")
                    else:
                        errors.append(f"HTTP {response.status_code} for {filename}")
                    
            except Exception as e:
                errors.append(f"Error caching {url}: {str(e)}")
                print(f"[Cache] Error caching file for task {task_id}: {e}")
    
    return {
        "cached": len(cached_files) > 0,
        "files": cached_files,
        "errors": errors
    }


async def cleanup_old_cached_files(max_age_days: int = 30):
    """
    Remove cached task files older than max_age_days.
    Called periodically by background worker.
    """
    from datetime import timedelta
    
    if not TASK_CACHE_DIR.exists():
        return 0
    
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    cutoff_timestamp = cutoff.timestamp()
    removed_count = 0
    
    for task_dir in TASK_CACHE_DIR.iterdir():
        if not task_dir.is_dir():
            continue
        
        try:
            # Check directory modification time
            if task_dir.stat().st_mtime < cutoff_timestamp:
                shutil.rmtree(task_dir)
                removed_count += 1
                print(f"[Cache Cleanup] Removed old cache for task {task_dir.name}")
        except Exception as e:
            print(f"[Cache Cleanup] Error removing {task_dir}: {e}")
    
    return removed_count


async def cleanup_disk_space(
    min_free_gb: int = 10,
    db: Optional[AsyncSession] = None,
    *,
    delete_task_rows: Optional[bool] = None,
) -> dict:
    """
    Clean up old files when disk space is low.

    Priority:
    1. Remove the oldest completed/error tasks physically and delete their DB rows (if delete_task_rows).
    2. Remove orphaned cache/upload/video files not referenced by any remaining task.
    """
    if delete_task_rows is None:
        delete_task_rows = AUTOMATIC_TASK_DB_DELETION
    from datetime import timedelta
    import shutil

    min_free_bytes = min_free_gb * 1024 * 1024 * 1024
    min_age_hours = CLEANUP_MIN_AGE_HOURS

    disk_usage = shutil.disk_usage("/")
    free_bytes = disk_usage.free

    result = {
        "deleted_count": 0,
        "deleted_task_rows": 0,
        "freed_bytes": 0,
        "freed_gb": 0.0,
        "initial_free_gb": free_bytes / (1024**3),
        "target_free_gb": min_free_gb,
        "deleted_items": []
    }

    if free_bytes >= min_free_bytes:
        return result

    print(f"[Disk Cleanup] Free space {free_bytes / (1024**3):.2f} GB < {min_free_gb} GB target. Starting cleanup...")

    # Step 0: "Download all" bundle ZIPs under task cache are regenerable and can accumulate
    # to many GB; remove them before orphan/task DB cleanup.
    zd, zb = purge_task_cache_bundle_zips(record_items=result["deleted_items"])
    result["deleted_count"] += zd
    result["freed_bytes"] += zb

    disk_usage = shutil.disk_usage("/")
    free_bytes = disk_usage.free
    if free_bytes >= min_free_bytes:
        result["freed_gb"] = result["freed_bytes"] / (1024**3)
        result["final_free_gb"] = free_bytes / (1024**3)
        if result["deleted_count"] > 0:
            print(
                f"[Disk Cleanup] Target reached after bundle ZIP purge: "
                f"freed {result['freed_gb']:.2f} GB, {result['final_free_gb']:.2f} GB free now"
            )
        return result

    age_cutoff = datetime.utcnow() - timedelta(hours=min_age_hours)
    age_cutoff_timestamp = age_cutoff.timestamp()
    cleanable_items = []

    cleanup_db: Optional[AsyncSession] = db
    owns_db_session = cleanup_db is None
    if owns_db_session:
        cleanup_db = AsyncSessionLocal()

    existing_task_ids: set[str] = set()
    upload_tokens_in_use: set[str] = set()

    try:
        if cleanup_db is not None:
            task_rows = (
                await cleanup_db.execute(
                    select(Task).order_by(Task.created_at)
                )
            ).scalars().all()

            task_cleanup_candidates: List[Task] = []
            for task in task_rows:
                existing_task_ids.add(task.id)
                upload_token = _extract_upload_token_from_input_url(task.input_url)
                if upload_token:
                    upload_tokens_in_use.add(upload_token)
                if (
                    task.status in ("done", "error")
                    and task.created_at
                    and task.created_at < age_cutoff
                ):
                    task_cleanup_candidates.append(task)

            if delete_task_rows:
                for task in task_cleanup_candidates:
                    disk_usage = shutil.disk_usage("/")
                    free_bytes = disk_usage.free
                    if free_bytes >= min_free_bytes:
                        break

                    deleted_items, freed_bytes = _delete_task_artifacts(task)
                    if deleted_items <= 0 and freed_bytes <= 0:
                        continue

                    try:
                        await _delete_task_record_and_related(cleanup_db, task.id)
                    except Exception as e:
                        await cleanup_db.rollback()
                        print(f"[Disk Cleanup] Failed to delete task {task.id} from DB: {e}")
                        continue

                    result["deleted_count"] += deleted_items
                    result["deleted_task_rows"] += 1
                    result["freed_bytes"] += freed_bytes
                    result["deleted_items"].append({
                        "path": task.id,
                        "type": "task",
                        "size_mb": freed_bytes / (1024**2)
                    })
                    existing_task_ids.discard(task.id)

                    upload_token = _extract_upload_token_from_input_url(task.input_url)
                    if upload_token:
                        upload_tokens_in_use.discard(upload_token)

                    print(f"[Disk Cleanup] Deleted task {task.id} with {deleted_items} local item(s), freed {freed_bytes / (1024**2):.1f} MB")

        if TASK_CACHE_DIR.exists():
            for item in TASK_CACHE_DIR.iterdir():
                if not item.is_dir() or item.name in existing_task_ids:
                    continue
                try:
                    mtime = item.stat().st_mtime
                    if mtime < age_cutoff_timestamp:
                        cleanable_items.append({
                            "path": item,
                            "mtime": mtime,
                            "size": _estimate_path_size(item),
                            "type": "task_cache"
                        })
                except Exception as e:
                    print(f"[Disk Cleanup] Error scanning {item}: {e}")

        if GLB_CACHE_DIR.exists():
            for item in GLB_CACHE_DIR.iterdir():
                task_prefix = item.name[:36]
                if task_prefix in existing_task_ids:
                    continue
                try:
                    mtime = item.stat().st_mtime
                    if mtime < age_cutoff_timestamp:
                        cleanable_items.append({
                            "path": item,
                            "mtime": mtime,
                            "size": _estimate_path_size(item),
                            "type": "glb_cache"
                        })
                except Exception as e:
                    print(f"[Disk Cleanup] Error scanning {item}: {e}")

        upload_dir = pathlib.Path(UPLOAD_DIR)
        if upload_dir.exists():
            for item in upload_dir.iterdir():
                if not item.is_dir() or item.name in upload_tokens_in_use:
                    continue
                try:
                    mtime = item.stat().st_mtime
                    if mtime < age_cutoff_timestamp:
                        cleanable_items.append({
                            "path": item,
                            "mtime": mtime,
                            "size": _estimate_path_size(item),
                            "type": "upload"
                        })
                except Exception as e:
                    print(f"[Disk Cleanup] Error scanning {item}: {e}")

        videos_dir = pathlib.Path("/var/autorig/videos")
        if videos_dir.exists():
            for item in videos_dir.iterdir():
                if not item.is_file() or item.stem in existing_task_ids:
                    continue
                try:
                    mtime = item.stat().st_mtime
                    if mtime < age_cutoff_timestamp:
                        cleanable_items.append({
                            "path": item,
                            "mtime": mtime,
                            "size": item.stat().st_size,
                            "type": "video"
                        })
                except Exception as e:
                    print(f"[Disk Cleanup] Error scanning {item}: {e}")

        cleanable_items.sort(key=lambda x: x["mtime"])
        print(f"[Disk Cleanup] Found {len(cleanable_items)} orphan cleanable items")

        for item in cleanable_items:
            disk_usage = shutil.disk_usage("/")
            free_bytes = disk_usage.free

            if free_bytes >= min_free_bytes:
                print(f"[Disk Cleanup] Target reached: {free_bytes / (1024**3):.2f} GB free")
                break

            try:
                item_path = item["path"]
                item_size = item["size"]
                item_type = item["type"]

                if item_path.is_dir():
                    shutil.rmtree(item_path)
                else:
                    item_path.unlink()

                result["deleted_count"] += 1
                result["freed_bytes"] += item_size
                result["deleted_items"].append({
                    "path": str(item_path.name),
                    "type": item_type,
                    "size_mb": item_size / (1024**2)
                })

                print(f"[Disk Cleanup] Deleted {item_type}: {item_path.name} ({item_size / (1024**2):.1f} MB)")

            except Exception as e:
                print(f"[Disk Cleanup] Error deleting {item['path']}: {e}")
    finally:
        if owns_db_session and cleanup_db is not None:
            await cleanup_db.close()

    result["freed_gb"] = result["freed_bytes"] / (1024**3)

    disk_usage = shutil.disk_usage("/")
    result["final_free_gb"] = disk_usage.free / (1024**3)

    if result["deleted_count"] > 0 or result["deleted_task_rows"] > 0:
        print(
            f"[Disk Cleanup] Complete: deleted {result['deleted_count']} item(s), "
            f"deleted {result['deleted_task_rows']} task row(s), freed {result['freed_gb']:.2f} GB"
        )
        print(f"[Disk Cleanup] Free space now: {result['final_free_gb']:.2f} GB")

    return result


@app.get("/skill.md")
async def serve_skill_md():
    for p in _skill_md_candidate_paths():
        if p.is_file():
            return FileResponse(p, media_type="text/markdown; charset=utf-8")
    raise HTTPException(status_code=404, detail="skill.md not found")


@app.get("/llm.txt")
async def serve_llm_txt():
    """Root llm.txt for crawlers and LLM tooling (plain-text site summary + links)."""
    path = STATIC_DIR / "llm.txt"
    if path.is_file():
        return FileResponse(str(path), media_type="text/plain; charset=utf-8")
    raise HTTPException(status_code=404, detail="llm.txt not found")


# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

FAVICON_SVG = STATIC_DIR / "images" / "logo" / "favicon.svg"


@app.get("/favicon.ico")
async def favicon_ico():
    """Browsers request /favicon.ico by default; serve existing SVG (no separate .ico asset)."""
    if not FAVICON_SVG.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(str(FAVICON_SVG), media_type="image/svg+xml")


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


@app.post("/api/task/{task_id}/purchase-intent")
async def api_purchase_intent(
    task_id: str,
    request: Request,
    response: Response,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Notify when user clicks download-to-purchase."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anon_session = await get_anon_session(request, response, db)
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    source = None
    animation_id = None
    animation_name = None
    if isinstance(payload, dict):
        source_raw = payload.get("source")
        source = str(source_raw).strip() if source_raw is not None else None

        anim_id_raw = payload.get("animation_id")
        anim_id_normalized = _normalize_animation_key(str(anim_id_raw or ""))
        animation_id = anim_id_normalized or None

        anim_name_raw = payload.get("animation_name")
        if anim_name_raw is not None:
            anim_name_clean = str(anim_name_raw).strip()
            if anim_name_clean:
                animation_name = anim_name_clean

    # Prefer canonical animation name from manifest for stable Telegram logs.
    if animation_id:
        try:
            manifest = _load_animation_manifest()
            raw_items = manifest.get("animations") or []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                if item.get("enabled", True) is False:
                    continue
                key = _normalize_animation_key(str(item.get("id") or item.get("name") or ""))
                if key == animation_id:
                    resolved_name = str(item.get("name") or "").strip()
                    if resolved_name:
                        animation_name = resolved_name
                    break
        except Exception:
            # Best-effort enrichment only.
            pass

    from telegram_bot import broadcast_purchase_intent
    asyncio.create_task(broadcast_purchase_intent(
        task_id=task_id,
        user_email=user.email if user else None,
        anon_id=anon_session.anon_id if not user else None,
        source=source,
        animation_id=animation_id,
        animation_name=animation_name
    ))

    return {"ok": True}


@app.get("/admin")
async def admin_page(user: Optional[User] = Depends(get_current_user)):
    """Serve admin page"""
    if not user or not is_admin_email(user.email):
        return RedirectResponse(url="/auth/login")
    return FileResponse(str(STATIC_DIR / "admin.html"))


@app.get("/admin/workers")
async def admin_workers_page(user: Optional[User] = Depends(get_current_user)):
    """Serve admin workers page (dedicated)."""
    if not user or not is_admin_email(user.email):
        return RedirectResponse(url="/auth/login")
    return FileResponse(str(STATIC_DIR / "admin-workers.html"))


@app.get("/gallery")
async def gallery_page():
    """Serve Gallery page"""
    return FileResponse(str(STATIC_DIR / "gallery.html"))


@app.get("/dashboard")
async def dashboard_page():
    """User dashboard: notification settings."""
    return FileResponse(str(STATIC_DIR / "dashboard.html"))


@app.get("/guides")
async def guides_page():
    """Serve Guides page"""
    return FileResponse(str(STATIC_DIR / "guides.html"))


@app.get("/buy-credits")
async def buy_credits_page():
    """Serve Buy Credits page"""
    return FileResponse(str(STATIC_DIR / "buy-credits.html"))


@app.get("/developers")
async def developers_page():
    """API documentation and key management for developers."""
    return FileResponse(str(STATIC_DIR / "developers.html"))


@app.get("/payment/success")
async def payment_success_page():
    """Serve payment success info page (no credit logic here)."""
    return FileResponse(str(STATIC_DIR / "payment-success.html"))


# =============================================================================
# SEO Landing Pages
# =============================================================================

# Format-specific pages
@app.get("/glb-auto-rig")
async def glb_auto_rig_page():
    """GLB auto-rigging landing page"""
    return FileResponse(str(STATIC_DIR / "glb-auto-rig.html"))


@app.get("/fbx-auto-rig")
async def fbx_auto_rig_page():
    """FBX auto-rigging landing page"""
    return FileResponse(str(STATIC_DIR / "fbx-auto-rig.html"))


@app.get("/obj-auto-rig")
async def obj_auto_rig_page():
    """OBJ auto-rigging landing page"""
    return FileResponse(str(STATIC_DIR / "obj-auto-rig.html"))


# Info pages
@app.get("/how-it-works")
async def how_it_works_page():
    """How it works page"""
    return FileResponse(str(STATIC_DIR / "how-it-works.html"))


@app.get("/faq")
async def faq_page():
    """FAQ page"""
    return FileResponse(str(STATIC_DIR / "faq.html"))


@app.get("/terms")
async def terms_of_use_page():
    """Terms of Use (website)."""
    return FileResponse(str(STATIC_DIR / "terms-of-use.html"))


@app.get("/user-agreement")
async def user_agreement_page():
    """User Agreement (content license, previews, promotional use)."""
    return FileResponse(str(STATIC_DIR / "user-agreement.html"))


@app.get("/guides")
async def guides_page():
    """Guides page"""
    return FileResponse(str(STATIC_DIR / "guides.html"))


@app.get("/t-pose-rig")
async def t_pose_rig_page():
    """T-pose rig page"""
    return FileResponse(str(STATIC_DIR / "t-pose-rig.html"))


# Mixamo alternative pages (4 languages)
@app.get("/mixamo-alternative")
async def mixamo_alternative_page():
    return FileResponse(str(STATIC_DIR / "mixamo-alternative.html"))


@app.get("/mixamo-alternative-ru")
async def mixamo_alternative_ru_page():
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-ru.html"))


@app.get("/mixamo-alternative-zh")
async def mixamo_alternative_zh_page():
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-zh.html"))


@app.get("/mixamo-alternative-hi")
async def mixamo_alternative_hi_page():
    return FileResponse(str(STATIC_DIR / "mixamo-alternative-hi.html"))


# Rig GLB for Unity pages (4 languages)
@app.get("/rig-glb-unity")
async def rig_glb_unity_page():
    return FileResponse(str(STATIC_DIR / "rig-glb-unity.html"))


@app.get("/rig-glb-unity-ru")
async def rig_glb_unity_ru_page():
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-ru.html"))


@app.get("/rig-glb-unity-zh")
async def rig_glb_unity_zh_page():
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-zh.html"))


@app.get("/rig-glb-unity-hi")
async def rig_glb_unity_hi_page():
    return FileResponse(str(STATIC_DIR / "rig-glb-unity-hi.html"))


# Rig FBX for Unreal pages (4 languages)
@app.get("/rig-fbx-unreal")
async def rig_fbx_unreal_page():
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal.html"))


@app.get("/rig-fbx-unreal-ru")
async def rig_fbx_unreal_ru_page():
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-ru.html"))


@app.get("/rig-fbx-unreal-zh")
async def rig_fbx_unreal_zh_page():
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-zh.html"))


@app.get("/rig-fbx-unreal-hi")
async def rig_fbx_unreal_hi_page():
    return FileResponse(str(STATIC_DIR / "rig-fbx-unreal-hi.html"))


# GLB vs FBX comparison pages (4 languages)
@app.get("/glb-vs-fbx")
async def glb_vs_fbx_page():
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx.html"))


@app.get("/glb-vs-fbx-ru")
async def glb_vs_fbx_ru_page():
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-ru.html"))


@app.get("/glb-vs-fbx-zh")
async def glb_vs_fbx_zh_page():
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-zh.html"))


@app.get("/glb-vs-fbx-hi")
async def glb_vs_fbx_hi_page():
    return FileResponse(str(STATIC_DIR / "glb-vs-fbx-hi.html"))


# T-pose vs A-pose comparison pages (4 languages)
@app.get("/t-pose-vs-a-pose")
async def t_pose_vs_a_pose_page():
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose.html"))


@app.get("/t-pose-vs-a-pose-ru")
async def t_pose_vs_a_pose_ru_page():
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-ru.html"))


@app.get("/t-pose-vs-a-pose-zh")
async def t_pose_vs_a_pose_zh_page():
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-zh.html"))


@app.get("/t-pose-vs-a-pose-hi")
async def t_pose_vs_a_pose_hi_page():
    return FileResponse(str(STATIC_DIR / "t-pose-vs-a-pose-hi.html"))


# Animation retargeting pages (4 languages)
@app.get("/animation-retargeting")
async def animation_retargeting_page():
    return FileResponse(str(STATIC_DIR / "animation-retargeting.html"))


@app.get("/animation-retargeting-ru")
async def animation_retargeting_ru_page():
    return FileResponse(str(STATIC_DIR / "animation-retargeting-ru.html"))


@app.get("/animation-retargeting-zh")
async def animation_retargeting_zh_page():
    return FileResponse(str(STATIC_DIR / "animation-retargeting-zh.html"))


@app.get("/animation-retargeting-hi")
async def animation_retargeting_hi_page():
    return FileResponse(str(STATIC_DIR / "animation-retargeting-hi.html"))


@app.get("/face-rig-animation")
async def face_rig_animation_page():
    return FileResponse(str(STATIC_DIR / "face-rig-animation.html"))


@app.get("/face-rig-animation-ru")
async def face_rig_animation_ru_page():
    return FileResponse(str(STATIC_DIR / "face-rig-animation-ru.html"))


@app.get("/face-rig-animation-zh")
async def face_rig_animation_zh_page():
    return FileResponse(str(STATIC_DIR / "face-rig-animation-zh.html"))


@app.get("/face-rig-animation-hi")
async def face_rig_animation_hi_page():
    return FileResponse(str(STATIC_DIR / "face-rig-animation-hi.html"))


# Auto-rig OBJ pages (4 languages)
@app.get("/auto-rig-obj")
async def auto_rig_obj_page():
    return FileResponse(str(STATIC_DIR / "auto-rig-obj.html"))


@app.get("/auto-rig-obj-ru")
async def auto_rig_obj_ru_page():
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-ru.html"))


@app.get("/auto-rig-obj-zh")
async def auto_rig_obj_zh_page():
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-zh.html"))


@app.get("/auto-rig-obj-hi")
async def auto_rig_obj_hi_page():
    return FileResponse(str(STATIC_DIR / "auto-rig-obj-hi.html"))


# Sitemap and robots.txt
@app.get("/sitemap.xml")
async def sitemap_index(db: AsyncSession = Depends(get_db)):
    """
    Sitemap index: static marketing pages + gallery /m/{id} urlsets by day (max 50 URLs per part).
    Regenerated on each request so lastmod stays current without a separate cron.
    """
    from seo_gallery import build_sitemap_index_xml, gallery_sitemap_day_parts

    base = (APP_URL or "https://autorig.online").rstrip("/")
    child_locs: List[Tuple[str, Optional[datetime]]] = [(f"{base}/sitemap/pages.xml", None)]
    for day_str, parts in await gallery_sitemap_day_parts(db):
        for p in range(parts):
            child_locs.append((f"{base}/sitemap/gallery/{day_str}/{p}.xml", None))
    xml = build_sitemap_index_xml(base, child_locs)
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.get("/sitemap/pages.xml")
async def sitemap_pages():
    """Marketing / guide urlset (was static/sitemap.xml)."""
    path = STATIC_DIR / "sitemap-pages.xml"
    if not path.is_file():
        return FileResponse(
            str(STATIC_DIR / "sitemap.xml"),
            media_type="application/xml",
        )
    return FileResponse(str(path), media_type="application/xml")


@app.get("/sitemap/gallery/{day}/{part}.xml")
async def sitemap_gallery_chunk(day: str, part: int, db: AsyncSession = Depends(get_db)):
    """One chunk (max 50) of /m/{task_id} URLs for tasks created on ``day`` (UTC date)."""
    from seo_gallery import GALLERY_SEO_URLS_PER_SITEMAP, build_urlset_xml, gallery_sitemap_urls_for_chunk

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", day) or part < 0:
        raise HTTPException(status_code=404, detail="Invalid sitemap chunk")
    base = (APP_URL or "https://autorig.online").rstrip("/")
    urls = await gallery_sitemap_urls_for_chunk(db, day, part)
    if not urls:
        raise HTTPException(status_code=404, detail="Empty sitemap chunk")
    xml = build_urlset_xml(base, urls)
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.get("/m/{task_id}", response_class=HTMLResponse)
async def public_model_seo_page(task_id: str, db: AsyncSession = Depends(get_db)):
    """
    Lightweight indexable landing: poster + LLM metadata + link to full /task viewer.
    Gallery-eligible tasks only; adult-rated tasks excluded (same as sitemap).
    """
    from seo_gallery import build_public_model_page_html, load_task_for_public_model_page

    task = await load_task_for_public_model_page(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Not found")
    base = APP_URL or "https://autorig.online"
    title = (getattr(task, "poster_llm_title", None) or "").strip() or "Rigged 3D character"
    desc = (getattr(task, "poster_llm_description", None) or "").strip()
    if not desc:
        desc = (
            "Rigged 3D model with skeleton and animations. Open the viewer to preview and download "
            "GLB, FBX, OBJ, and engine packages."
        )
    keywords: List[str] = []
    raw_kw = getattr(task, "poster_llm_keywords", None)
    if raw_kw:
        try:
            data = json.loads(raw_kw)
            if isinstance(data, list):
                keywords = [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            pass
    html_page = build_public_model_page_html(base, task_id, title, desc, keywords)
    return HTMLResponse(content=html_page)


@app.get("/robots.txt")
async def robots():
    """Serve robots.txt for crawlers"""
    return FileResponse(
        str(STATIC_DIR / "robots.txt"),
        media_type="text/plain"
    )


# Search engine verification files
@app.get("/yandex_7bb48a0ce446816a.html")
async def yandex_verification():
    """Yandex Webmaster verification file"""
    return FileResponse(str(STATIC_DIR / "yandex_7bb48a0ce446816a.html"))


@app.get("/BingSiteAuth.xml")
async def bing_verification():
    """Bing Webmaster verification file"""
    return FileResponse(
        str(STATIC_DIR / "BingSiteAuth.xml"),
        media_type="application/xml"
    )


# =============================================================================
# Scene API (Multi-model 3D scenes)
# =============================================================================

@app.post("/api/scene", response_model=SceneResponse)
async def create_scene(
    request: SceneCreateRequest,
    req: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new scene from one or two tasks"""
    anon_id = _effective_anon_id(req)
    if user:
        owner_type = "user"
        owner_id = user.email
    elif anon_id:
        owner_type = "anon"
        owner_id = anon_id
    else:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Verify base task exists and get its data
    from database import Task
    base_task = await db.execute(select(Task).where(Task.id == request.base_task_id))
    base_task = base_task.scalar_one_or_none()
    if not base_task:
        raise HTTPException(status_code=404, detail="Base task not found")
    
    # Build task list
    task_ids = [request.base_task_id]
    transforms = {
        request.base_task_id: {"position": [0, 0, 0], "rotation": [0, 0, 0], "scale": [1, 1, 1]}
    }
    
    # Add second task if provided
    if request.add_task_id:
        add_task = await db.execute(select(Task).where(Task.id == request.add_task_id))
        add_task = add_task.scalar_one_or_none()
        if not add_task:
            raise HTTPException(status_code=404, detail="Additional task not found")
        task_ids.append(request.add_task_id)
        transforms[request.add_task_id] = {"position": [2, 0, 0], "rotation": [0, 0, 0], "scale": [1, 1, 1]}
    
    # Create scene
    scene_id = str(uuid.uuid4())
    scene = Scene(
        id=scene_id,
        owner_type=owner_type,
        owner_id=owner_id,
        name=request.name,
    )
    scene.task_ids = task_ids
    scene.transforms = transforms
    scene.hierarchy = {}
    
    db.add(scene)
    await db.commit()
    await db.refresh(scene)
    
    # Build response with model info
    models = []
    for tid in task_ids:
        task = await db.execute(select(Task).where(Task.id == tid))
        task = task.scalar_one_or_none()
        if task:
            models.append(SceneModelInfo(
                task_id=tid,
                input_url=task.input_url,
                glb_url=f"/api/task/{tid}/prepared.glb" if task.status == "done" else None,
                transform=TransformData(**transforms.get(tid, {}))
            ))
    
    return SceneResponse(
        scene_id=scene.id,
        name=scene.name,
        task_ids=scene.task_ids,
        transforms=scene.transforms,
        hierarchy=scene.hierarchy,
        models=models,
        is_public=scene.is_public,
        like_count=scene.like_count,
        liked_by_me=False,
        owner_type=scene.owner_type,
        owner_id=scene.owner_id,
        created_at=scene.created_at,
        updated_at=scene.updated_at
    )


@app.get("/api/scene/{scene_id}", response_model=SceneResponse)
async def get_scene(
    scene_id: str,
    req: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get scene data"""
    scene = await db.execute(select(Scene).where(Scene.id == scene_id))
    scene = scene.scalar_one_or_none()
    if not scene:
        raise HTTPException(status_code=404, detail="Scene not found")
    
    # Check ownership for private scenes
    anon_id = _effective_anon_id(req)
    
    is_owner = False
    if scene.owner_type == "user" and user and user.email == scene.owner_id:
        is_owner = True
    elif scene.owner_type == "anon" and anon_id == scene.owner_id:
        is_owner = True
    
    if not scene.is_public and not is_owner:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Check if user liked this scene
    liked_by_me = False
    if user:
        like = await db.execute(
            select(SceneLike).where(
                SceneLike.scene_id == scene_id,
                SceneLike.user_email == user.email
            )
        )
        liked_by_me = like.scalar_one_or_none() is not None
    
    # Build model info
    from database import Task
    models = []
    transforms = scene.transforms
    for tid in scene.task_ids:
        task = await db.execute(select(Task).where(Task.id == tid))
        task = task.scalar_one_or_none()
        if task:
            models.append(SceneModelInfo(
                task_id=tid,
                input_url=task.input_url,
                glb_url=f"/api/task/{tid}/prepared.glb" if task.status == "done" else None,
                transform=TransformData(**transforms.get(tid, {}))
            ))
    
    return SceneResponse(
        scene_id=scene.id,
        name=scene.name,
        task_ids=scene.task_ids,
        transforms=scene.transforms,
        hierarchy=scene.hierarchy,
        models=models,
        is_public=scene.is_public,
        like_count=scene.like_count,
        liked_by_me=liked_by_me,
        owner_type=scene.owner_type,
        owner_id=scene.owner_id,
        created_at=scene.created_at,
        updated_at=scene.updated_at
    )


@app.put("/api/scene/{scene_id}", response_model=SceneResponse)
async def update_scene(
    scene_id: str,
    request: SceneUpdateRequest,
    req: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update scene transforms, hierarchy, or metadata"""
    scene = await db.execute(select(Scene).where(Scene.id == scene_id))
    scene = scene.scalar_one_or_none()
    if not scene:
        raise HTTPException(status_code=404, detail="Scene not found")
    
    # Check ownership
    anon_id = _effective_anon_id(req)
    
    is_owner = False
    if scene.owner_type == "user" and user and user.email == scene.owner_id:
        is_owner = True
    elif scene.owner_type == "anon" and anon_id == scene.owner_id:
        is_owner = True
    
    if not is_owner:
        raise HTTPException(status_code=403, detail="Only scene owner can update")
    
    # Update fields
    if request.transforms is not None:
        scene.transforms = request.transforms
    if request.hierarchy is not None:
        scene.hierarchy = request.hierarchy
    if request.name is not None:
        scene.name = request.name
    if request.is_public is not None:
        scene.is_public = request.is_public
    
    scene.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(scene)
    
    # Build model info
    from database import Task
    models = []
    transforms = scene.transforms
    for tid in scene.task_ids:
        task = await db.execute(select(Task).where(Task.id == tid))
        task = task.scalar_one_or_none()
        if task:
            models.append(SceneModelInfo(
                task_id=tid,
                input_url=task.input_url,
                glb_url=f"/api/task/{tid}/prepared.glb" if task.status == "done" else None,
                transform=TransformData(**transforms.get(tid, {}))
            ))
    
    return SceneResponse(
        scene_id=scene.id,
        name=scene.name,
        task_ids=scene.task_ids,
        transforms=scene.transforms,
        hierarchy=scene.hierarchy,
        models=models,
        is_public=scene.is_public,
        like_count=scene.like_count,
        liked_by_me=False,
        owner_type=scene.owner_type,
        owner_id=scene.owner_id,
        created_at=scene.created_at,
        updated_at=scene.updated_at
    )


@app.post("/api/scene/{scene_id}/add", response_model=SceneResponse)
async def add_model_to_scene(
    scene_id: str,
    request: SceneAddModelRequest,
    req: Request,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Add a model to an existing scene"""
    scene = await db.execute(select(Scene).where(Scene.id == scene_id))
    scene = scene.scalar_one_or_none()
    if not scene:
        raise HTTPException(status_code=404, detail="Scene not found")
    
    # Check ownership
    anon_id = _effective_anon_id(req)
    
    is_owner = False
    if scene.owner_type == "user" and user and user.email == scene.owner_id:
        is_owner = True
    elif scene.owner_type == "anon" and anon_id == scene.owner_id:
        is_owner = True
    
    if not is_owner:
        raise HTTPException(status_code=403, detail="Only scene owner can add models")
    
    # Verify task exists
    from database import Task
    task = await db.execute(select(Task).where(Task.id == request.task_id))
    task = task.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Add to scene
    task_ids = scene.task_ids
    if request.task_id in task_ids:
        raise HTTPException(status_code=400, detail="Task already in scene")
    
    task_ids.append(request.task_id)
    scene.task_ids = task_ids
    
    # Add transform