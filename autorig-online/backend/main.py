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
import math
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple, Set
import hashlib
import hmac
import secrets
import json
import base64
import tempfile
import zipfile
import re
import html
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import urlparse, quote, unquote, parse_qsl, urlencode
from starlette.background import BackgroundTask

from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse, HTMLResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, or_, update, text
from sqlalchemy.exc import IntegrityError
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import (
    APP_NAME, APP_URL, DEBUG, SECRET_KEY,
    DATABASE_URL,
    UPLOAD_DIR, MAX_UPLOAD_SIZE_MB,
    RATE_LIMIT_TASKS_PER_MINUTE, RATE_LIMIT_AGENT_REGISTER, is_admin_email,
    ANON_FREE_LIMIT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_BOT_USERNAME,
    VIEWER_DEFAULT_SETTINGS_PATH,
    MIN_FREE_SPACE_GB, CLEANUP_CHECK_INTERVAL_CYCLES, CLEANUP_MIN_AGE_HOURS,
    UPLOAD_PRESSURE_CLEANUP_MIN_AGE_HOURS,
    NEW_TASK_MIN_FREE_GB, NEW_TASK_PURGE_TASKS_MAX_FREED_GB,
    AUTOMATIC_TASK_DB_DELETION,
    STUCK_HOUR_MINUTES,
    STUCK_HOUR_MAX_REQUEUES,
    GALLERY_DB_PURGE_INTERVAL_CYCLES,
    GALLERY_UPSTREAM_PURGE_BATCH,
    GALLERY_UPSTREAM_PURGE_ROUNDS,
    TASK_CACHE_MAX_GB,
    GA_MEASUREMENT_ID, GA_API_SECRET,
    GUMROAD_PRODUCT_CREDITS,
    BLENDER_PLUGIN_AB_VARIANTS,
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
    SUPPORT_CHAT_MESSAGE_MAX_CHARS,
    RESEND_WEBHOOK_SECRET,
    RATE_LIMIT_SUPPORT_CHAT_SESSION,
    RATE_LIMIT_SUPPORT_CHAT_MESSAGE,
    RATE_LIMIT_SUPPORT_CHAT_MESSAGES_POLL,
)
from viewer_environment import build_viewer_environment_from_settings
from database import (
    init_db, get_db, AsyncSessionLocal, User, AnonSession, ApiKey, Task, TaskLike, TaskFilePurchase,
    Scene, SceneLike, Feedback, WorkerEndpoint, YoutubeCredentials,
    TaskAnimationPurchase, TaskAnimationBundlePurchase, TaskAnimalAnimationPackPurchase,
    GumroadPurchase, PurchaseCheckoutIntent, RoadmapVote,
    CryptoPaymentReport, EmailCampaignClick, EmailCampaignSend, EmailDeliveryEvent,
    SupportChatSession,
    SupportChatMessage,
    reset_admin_overlay_counters,
    get_or_create_admin_overlay_counters,
    get_public_gallery_stats,
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
    AdminTaskCacheMaxUpdate,
    WorkerQueueInfo, QueueStatusResponse,
    GalleryItem, GalleryResponse, LikeResponse, TaskCardInfo,
    PurchaseStateResponse, PurchaseRequest, PurchaseResponse,
    AnimalRigVariantsResponse, AnimalRigVariantItem, AnimalVariantFileState,
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
    SupportChatSessionPostRequest,
    SupportChatSessionPostResponse,
    SupportChatMessagePostRequest,
    SupportChatMessagePostResponse,
    SupportChatMessageItem,
    SupportChatMessagesPollResponse,
)
from workers import (
    get_global_queue_status,
    quarantine_worker,
    clear_worker_quarantine,
    is_worker_quarantined,
    normalize_task_type,
    get_backend_worker_processing_counts,
    get_worker_effective_active,
)
from content_moderation import build_free3d_similar_query, schedule_task_poster_classification
from viewer_theme_vision import analyze_backdrop_theme_with_openai
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

from namecheap_remote_api import router as namecheap_remote_router
from idle_ltx_routes import register_idle_ltx_routes

# Throttle poster-classification recovery triggers from GET /api/task (per task_id).
_poster_recovery_throttle: Dict[str, float] = {}
POSTER_RECOVERY_THROTTLE_SEC = 20.0
PREFLIGHT_RENDER_DIR = Path("/var/autorig/preflight-renders")
PREFLIGHT_RENDER_MAX_BYTES = 6 * 1024 * 1024


async def get_dispatchable_workers(db: AsyncSession, queue_status, *, allow_quarantined: bool = False) -> List[Any]:
    """
    Return workers that are free according to both worker API and backend DB.
    The DB overlay avoids burst dispatch races where several tasks pick the same
    worker before its live /api-converter-glb counters update.
    """
    backend_processing = await get_backend_worker_processing_counts(db)
    return [
        w
        for w in (queue_status.workers if queue_status else [])
        if (
            w.available
            and (get_worker_effective_active(w, backend_processing) < w.max_concurrent)
            and ((w.total_pending or 0) <= 0)
            and (w.queue_size <= 0)
            and (allow_quarantined or not is_worker_quarantined(w.url))
        )
    ]


def _pop_preflight_render_image_from_meta(meta: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(meta, dict):
        return None
    for key in (
        "preflight_render_jpg_base64_string",
        "preflight_render_image_jpg_base64_string",
        "preview_image_jpg_base64_string",
    ):
        value = meta.pop(key, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _decode_preflight_render_image(data_url_or_b64: str) -> bytes:
    raw = (data_url_or_b64 or "").strip()
    if "," in raw and raw.lower().startswith("data:image/"):
        raw = raw.split(",", 1)[1]
    if not raw:
        raise ValueError("empty preflight render image")

    def _decode_candidate(candidate: str) -> bytes:
        return base64.b64decode(candidate, validate=True)

    compact = re.sub(r"\s+", "", raw)
    try:
        data = _decode_candidate(compact)
    except Exception:
        # Multipart form parsing can turn literal '+' characters in a data URL
        # into spaces. Try that repair only after strict decoding fails.
        repaired = re.sub(r"\s+", "+", raw)
        data = _decode_candidate(repaired)

    if len(data) > PREFLIGHT_RENDER_MAX_BYTES:
        raise ValueError("preflight render image too large")
    if not (data.startswith(b"\xff\xd8\xff") or data.startswith(b"\x89PNG\r\n\x1a\n") or data.startswith(b"RIFF")):
        raise ValueError("unsupported preflight render image")
    return data


def _save_preflight_render_image(task_id: str, data_url_or_b64: Optional[str]) -> None:
    if not data_url_or_b64:
        return
    try:
        image_bytes = _decode_preflight_render_image(data_url_or_b64)
        PREFLIGHT_RENDER_DIR.mkdir(parents=True, exist_ok=True)
        tmp_path = PREFLIGHT_RENDER_DIR / f"{task_id}.tmp"
        final_path = PREFLIGHT_RENDER_DIR / f"{task_id}.jpg"
        tmp_path.write_bytes(image_bytes)
        tmp_path.replace(final_path)
        print(f"[PreflightRender] Saved render image for task {task_id}: {final_path} ({len(image_bytes)} bytes)")
    except Exception as e:
        print(f"[PreflightRender] Failed to save render image for task {task_id}: {e}")



def _task_needs_poster_classification(task) -> bool:
    if getattr(task, "status", None) != "done":
        return False
    if getattr(task, "content_classified_at", None) is None:
        return True
    cv = getattr(task, "content_classifier_version", None) or ""
    return ":pipeline_error" in cv or ":fetch_error" in cv or ":poster_pending" in cv


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

    async def _sync_processing_tasks(db):
        # Keep backend task rows aligned with terminal worker state before stall checks
        # and before dispatch can hand the same worker another queued task.
        result = await db.execute(
            select(Task).where(Task.status == "processing")
        )
        processing_tasks = result.scalars().all()

        if not processing_tasks:
            return

        print(f"[Background Worker] Updating {len(processing_tasks)} processing tasks")

        # Update tasks concurrently (bounded) so the loop doesn't take minutes when many tasks are processing.
        # IMPORTANT: Each task gets its own DB session to avoid SQLAlchemy transaction conflicts.
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

        # Pass task IDs, not task objects (to get fresh data in each session).
        task_ids = [t.id for t in processing_tasks]
        await asyncio.gather(*[_update_one(tid) for tid in task_ids])
    
    while background_task_running:
        try:
            background_worker_cycle_count += 1
            
            async with AsyncSessionLocal() as db:
                queue_status = None
                force_stale_reset = False
                # =============================================================
                # 1. Worker state sync, queue snapshot + stall monitor, then stale reset, then dispatch
                #    (reset must run before dispatch so tasks moved to "created" post in the same tick)
                # =============================================================
                try:
                    await _sync_processing_tasks(db)

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

                    free_workers = await get_dispatchable_workers(db, queue_status)
                    if not free_workers:
                        fallback_workers = await get_dispatchable_workers(db, queue_status, allow_quarantined=True)
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
                            backend_processing = await get_backend_worker_processing_counts(db)
                            for w in queue_status.workers:
                                print(
                                    f"[Background Worker] No free worker: url={w.url} "
                                    f"available={w.available} err={w.error!r} "
                                    f"active={w.total_active} "
                                    f"backend_active={backend_processing.get(w.url.rstrip('/'), 0)} "
                                    f"effective_active={get_worker_effective_active(w, backend_processing)} "
                                    f"max={w.max_concurrent} "
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
                        await enforce_task_cache_max_size(db)
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
                # 4. Poster classification recovery (pending or transient poster fetch errors)
                # =============================================================
                try:
                    async with AsyncSessionLocal() as db:
                        r2 = await db.execute(
                            select(Task.id).where(
                                Task.status == "done",
                                or_(
                                    Task.content_classified_at.is_(None),
                                    Task.content_classifier_version.like("%:fetch_error%"),
                                    Task.content_classifier_version.like("%:poster_pending%"),
                                ),
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

    try:
        async with AsyncSessionLocal() as startup_db:
            await enforce_task_cache_max_size(startup_db)
            startup_cleanup = await cleanup_disk_space(
                min_free_gb=MIN_FREE_SPACE_GB,
                db=startup_db,
                delete_task_rows=AUTOMATIC_TASK_DB_DELETION,
            )
            if startup_cleanup.get("deleted_count", 0) > 0:
                print(
                    f"[Startup Disk] Cleanup freed {startup_cleanup.get('freed_gb', 0):.2f} GB, "
                    f"deleted {startup_cleanup.get('deleted_count', 0)} item(s)"
                )
    except Exception as e:
        print(f"[Startup Disk] Cleanup failed: {e}")
    
    # Start background worker
    app.state.background_worker = asyncio.create_task(background_task_updater())
    
    # Send Telegram startup notification (fire-and-forget)
    try:
        from telegram_bot import broadcast_server_startup
        asyncio.create_task(broadcast_server_startup())
    except Exception as e:
        print(f"[Telegram] Failed to send startup notification: {e}")

    # Namecheap: facerig.autorig.online A record (if registrar API env set)
    try:
        from namecheap_remote_api import ensure_facerig_on_startup

        await asyncio.to_thread(ensure_facerig_on_startup)
    except Exception as e:
        print(f"[Namecheap DNS] Startup: {e}")

    try:
        await _sync_viewer_backdrop_themes_async()
    except Exception as e:
        print(f"[ViewerThemes] startup sync: {e}")

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

app.include_router(namecheap_remote_router)


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


register_idle_ltx_routes(
    app,
    limiter,
    get_db=get_db,
    get_current_user=get_current_user,
    get_task_by_id=get_task_by_id,
    app_url=APP_URL,
)


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


def _validate_viewer_camera_vector(value: Any, field_name: str) -> Dict[str, float]:
    if not isinstance(value, dict):
        raise HTTPException(status_code=400, detail=f"camera.{field_name} must be an object")
    out: Dict[str, float] = {}
    for axis in ("x", "y", "z"):
        try:
            n = float(value[axis])
        except Exception:
            raise HTTPException(status_code=400, detail=f"camera.{field_name}.{axis} must be a number")
        if not math.isfinite(n) or abs(n) > 1_000_000:
            raise HTTPException(status_code=400, detail=f"camera.{field_name}.{axis} is out of range")
        out[axis] = n
    return out


def _validate_viewer_default_camera_payload(body_bytes: bytes, *, saved_by: str) -> Dict[str, Any]:
    data = _validate_viewer_settings_payload(body_bytes)
    source = data.get("camera") if isinstance(data.get("camera"), dict) else data
    if not isinstance(source, dict):
        raise HTTPException(status_code=400, detail="camera payload must be an object")

    try:
        fov = float(source["fov"])
    except Exception:
        raise HTTPException(status_code=400, detail="camera.fov must be a number")
    if not math.isfinite(fov) or fov < 1 or fov > 120:
        raise HTTPException(status_code=400, detail="camera.fov is out of range")

    camera_settings: Dict[str, Any] = {
        "position": _validate_viewer_camera_vector(source.get("position"), "position"),
        "target": _validate_viewer_camera_vector(source.get("target"), "target"),
        "fov": fov,
        "global_camera_preset": True,
        "bounds_policy": "ignore",
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "saved_by": saved_by,
    }
    if isinstance(source.get("up"), dict):
        camera_settings["up"] = _validate_viewer_camera_vector(source.get("up"), "up")
    return camera_settings


def _read_global_viewer_camera_preset() -> Optional[Dict[str, Any]]:
    data = _read_json_file(VIEWER_DEFAULT_SETTINGS_PATH)
    camera_settings = data.get("camera") if isinstance(data, dict) else None
    if not isinstance(camera_settings, dict):
        return None
    bounds_policy = str(camera_settings.get("bounds_policy") or "").strip().lower()
    if camera_settings.get("global_camera_preset") is True or bounds_policy == "ignore":
        return camera_settings
    return None


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
TASK_UNLOCK_CREDITS = 10
ANIMATION_SINGLE_CREDITS = 1
ANIMATION_BUNDLE_CREDITS = TASK_UNLOCK_CREDITS
ANIMAL_ANIMATION_PACK_CREDITS = TASK_UNLOCK_CREDITS
DOWNLOAD_ALL_FILES_CREDITS = TASK_UNLOCK_CREDITS
ANIMAL_RIG_DOWNLOAD_CREDITS = TASK_UNLOCK_CREDITS
PURCHASE_CHECKOUT_INTENT_MAX_AGE = timedelta(hours=72)
ANIMAL_VARIANT_TYPES = [
    "dog",
    "bear",
    "cat",
    "cow",
    "deer",
    "elephant",
    "giraffe",
    "horse",
    "mouse",
    "pig",
    "rabbit",
    "turtle",
]
ANIMAL_VARIANT_ORIENTATIONS = ("front", "back")


def _is_animal_download_task(task: Task) -> bool:
    return str(getattr(task, "input_type", "") or "").strip().lower() == "animal"


def _download_all_files_cost(task: Task) -> int:
    return ANIMAL_RIG_DOWNLOAD_CREDITS if _is_animal_download_task(task) else DOWNLOAD_ALL_FILES_CREDITS


def _clamp_text(value: Any, max_len: int, *, default: str = "") -> str:
    text_value = str(value or "").strip()
    return (text_value or default)[:max_len]


def _safe_checkout_task_id(value: Any) -> Optional[str]:
    task_id = _clamp_text(value, 64)
    if re.fullmatch(r"[A-Za-z0-9_-]{8,64}", task_id):
        return task_id
    return None


def _checkout_pack_price_label(product_key: str) -> str:
    key = _normalize_gumroad_product_key(product_key)
    for tier_key, _credits, usd in AUTORIG_CRYPTO_TIERS:
        if _normalize_gumroad_product_key(tier_key) == key:
            if float(usd).is_integer():
                return f"${int(usd)}"
            return f"${usd:.2f}".rstrip("0").rstrip(".")
    return "unknown"


def _checkout_pack_label(product_key: str) -> str:
    credits = int(GUMROAD_PRODUCT_CREDITS.get(_normalize_gumroad_product_key(product_key), 0) or 0)
    return f"{credits} credits" if credits > 0 else "credits"


def _format_usd_price(value: int | float) -> str:
    amount = float(value)
    if amount.is_integer():
        return f"${int(amount)}"
    return f"${amount:.2f}".rstrip("0").rstrip(".")


def _plugin_variant_items() -> List[Tuple[str, int]]:
    return [
        (key, int(price))
        for key, price in BLENDER_PLUGIN_AB_VARIANTS.items()
        if _normalize_gumroad_product_key(key) and int(price) > 0
    ]


def _select_blender_plugin_variant(user: Optional[User]) -> Tuple[str, int]:
    variants = _plugin_variant_items()
    if not variants:
        return ("blender-plugin", 100)
    if user and getattr(user, "id", None):
        index = max(0, int(user.id) - 1) % len(variants)
    else:
        email = (getattr(user, "email", "") or "").strip().lower()
        digest = hashlib.sha256(email.encode("utf-8")).hexdigest() if email else "0"
        index = int(digest[:8], 16) % len(variants)
    return variants[index]


def _is_blender_plugin_product(product_key: str, product_name: Optional[str] = None) -> bool:
    key = _normalize_gumroad_product_key(product_key)
    if key in {_normalize_gumroad_product_key(k) for k in BLENDER_PLUGIN_AB_VARIANTS}:
        return True
    name = (product_name or "").strip().lower()
    return "blender" in name and "plugin" in name and "auto" in name and "rig" in name


def _blender_plugin_price_label(product_key: str, price_cents: int = 0) -> str:
    key = _normalize_gumroad_product_key(product_key)
    if key in BLENDER_PLUGIN_AB_VARIANTS:
        return _format_usd_price(int(BLENDER_PLUGIN_AB_VARIANTS[key]))
    if price_cents > 0:
        return _format_usd_price(price_cents / 100.0)
    return "unknown"


def _gumroad_checkout_url(product_key: str, user_email: str) -> str:
    base = f"https://u3d.gumroad.com/l/{quote(_normalize_gumroad_product_key(product_key))}"
    return f"{base}?{urlencode({'userid': user_email})}"


def _task_animal_type_from_settings(task: Task) -> Optional[str]:
    """Return the selected/detected animal slug stored in viewer_settings."""
    try:
        settings = json.loads(task.viewer_settings or "{}")
    except Exception:
        settings = {}
    detection = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None
    if not isinstance(detection, dict):
        return None
    for key in (
        "animal_type",
        "animal_type_string",
        "selected_type_string",
        "candidate_animal_type_string",
        "selected_animal_type",
        "selected_animal_type_string",
    ):
        value = str(detection.get(key) or "").strip().lower()
        if value in ANIMAL_VARIANT_TYPES:
            return value
    first_result = detection.get("first_result")
    if isinstance(first_result, dict):
        for key in ("animal_type", "animal_type_string"):
            value = str(first_result.get(key) or "").strip().lower()
            if value in ANIMAL_VARIANT_TYPES:
                return value
    return None

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


def _resolve_all_animations_fbx_url(task) -> tuple[Optional[str], Optional[str]]:
    """
    Return the package-level FBX animation file for a task.
    Prefer real ready_urls, then synthesize the standard worker path used by current workers.
    """
    ready_urls = task.ready_urls or []

    animations_url = _find_file_in_ready_urls(ready_urls, "_all_animations_unity.fbx")
    if animations_url:
        return animations_url, unquote(animations_url.split("/")[-1]) or f"{task.id}_all_animations.fbx"

    animations_url = _find_file_in_ready_urls(ready_urls, "_all_animations.fbx")
    if animations_url:
        return animations_url, unquote(animations_url.split("/")[-1]) or f"{task.id}_all_animations.fbx"

    if not task.guid or not task.worker_api:
        return None, None

    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    filename = f"{task.guid}_all_animations_unity.fbx"
    return (
        f"{worker_base}/converter/glb/{task.guid}/{task.guid}_100k/{filename}",
        filename,
    )


async def _worker_file_available(url: str) -> bool:
    """Lightweight existence probe for worker-hosted files."""
    if not url:
        return False
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.head(url, timeout=15.0)
            if resp.status_code == 200:
                return True
            if resp.status_code not in (405, 403):
                return False
            resp = await client.get(url, headers={"Range": "bytes=0-0"}, timeout=20.0)
            return resp.status_code in (200, 206)
    except Exception:
        return False


async def _download_worker_file_bytes(url: str, label: str, *, max_bytes: int = 80 * 1024 * 1024) -> bytes:
    """Download a worker file into memory for small on-demand bundles."""
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, timeout=120.0)
            if resp.status_code != 200:
                raise HTTPException(status_code=404, detail=f"{label} is unavailable")
            data = resp.content
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{label} source unavailable: {e}")
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail=f"{label} is too large for this bundle")
    return data


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
                    "single_animation_credits": TASK_UNLOCK_CREDITS,
                    "all_animations_credits": TASK_UNLOCK_CREDITS,
                    "task_unlock_credits": TASK_UNLOCK_CREDITS,
                    "purchase_scope": "task",
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
            "single_animation_credits": TASK_UNLOCK_CREDITS,
            "all_animations_credits": TASK_UNLOCK_CREDITS,
            "task_unlock_credits": TASK_UNLOCK_CREDITS,
            "purchase_scope": "task",
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
                "single_animation_credits": TASK_UNLOCK_CREDITS,
                "all_animations_credits": TASK_UNLOCK_CREDITS,
                "task_unlock_credits": TASK_UNLOCK_CREDITS,
                "purchase_scope": "task",
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


async def _fetch_worker_model_files(task: Task) -> Tuple[bool, List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """Return flattened worker model-files entries for a task."""
    api_base, guid = _resolve_worker_files_api_context(task)
    if not api_base or not guid:
        return False, [], {}, None

    worker_root = f"{api_base}/converter/glb"
    files_url = f"{api_base}/api-converter-glb/model-files/{guid}"

    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
            resp = await client.get(files_url)
        if resp.status_code != 200:
            return False, [], {}, f"HTTP {resp.status_code}"
        data = resp.json() if resp.content else {}
    except Exception as e:
        return False, [], {}, str(e)

    all_files: List[Dict[str, Any]] = []
    for folder_name, folder_data in (data.get("folders") or {}).items():
        if not isinstance(folder_data, dict):
            continue
        for f in folder_data.get("files") or []:
            if not isinstance(f, dict):
                continue
            rel_path = str(f.get("rel_path") or "")
            name = str(f.get("name") or "")
            if not rel_path or not name:
                continue
            all_files.append({
                "name": name,
                "folder": folder_name,
                "type": f.get("type"),
                "size": f.get("size"),
                "rel_path": rel_path,
                "url": f"{worker_root}/{guid}/{rel_path}",
            })

    return True, all_files, data, None


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


async def _ensure_full_task_unlock(
    db: AsyncSession,
    task: Task,
    buyer_user: User,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Create the single paid entitlement for a task, charging credits only once."""
    if not task or not buyer_user:
        return {"status": "invalid", "credits_spent": 0, "cost": 0}

    task_id = str(task.id)
    existing = await db.execute(
        select(TaskFilePurchase.id).where(
            TaskFilePurchase.task_id == task_id,
            TaskFilePurchase.user_email == buyer_user.email,
            TaskFilePurchase.file_index.is_(None),
        ).limit(1)
    )
    if existing.scalar_one_or_none() is not None:
        return {"status": "already_unlocked", "task_id": task_id, "credits_spent": 0, "cost": 0}

    cost = _download_all_files_cost(task)
    if int(buyer_user.balance_credits or 0) < cost:
        return {"status": "insufficient_credits", "task_id": task_id, "credits_spent": 0, "cost": cost}

    created_at = now or datetime.utcnow()
    insert_result = await db.execute(
        text(
            """
            INSERT INTO task_file_purchases
                (task_id, user_email, file_index, credits_spent, created_at)
            SELECT
                :task_id, :user_email, NULL, :credits_spent, :created_at
            WHERE NOT EXISTS (
                SELECT 1
                FROM task_file_purchases
                WHERE task_id = :task_id
                  AND user_email = :user_email
                  AND file_index IS NULL
            )
            """
        ),
        {
            "task_id": task_id,
            "user_email": buyer_user.email,
            "credits_spent": cost,
            "created_at": created_at,
        },
    )
    if insert_result.rowcount == 0:
        return {"status": "already_unlocked", "task_id": task_id, "credits_spent": 0, "cost": 0}

    buyer_user.balance_credits = int(buyer_user.balance_credits or 0) - cost
    await _credit_task_owner_for_sale(db, task, buyer_user, cost)
    await db.flush()
    return {"status": "unlocked", "task_id": task_id, "credits_spent": cost, "cost": cost}


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


async def _try_auto_unlock_pending_checkout(
    db: AsyncSession,
    buyer_user: User,
    sale_id: str,
) -> Optional[Dict[str, Any]]:
    """Convert the newest recent task-paywall checkout intent into a full-task unlock."""
    if not buyer_user or not sale_id:
        return None

    email = _gumroad_clean_email(getattr(buyer_user, "email", None))
    if not email:
        return None

    cutoff = datetime.utcnow() - PURCHASE_CHECKOUT_INTENT_MAX_AGE
    result = await db.execute(
        select(PurchaseCheckoutIntent)
        .where(
            func.lower(PurchaseCheckoutIntent.user_email) == email.lower(),
            PurchaseCheckoutIntent.product_kind == "credits",
            PurchaseCheckoutIntent.task_id.is_not(None),
            PurchaseCheckoutIntent.used_at.is_(None),
            PurchaseCheckoutIntent.created_at >= cutoff,
        )
        .order_by(PurchaseCheckoutIntent.created_at.desc(), PurchaseCheckoutIntent.id.desc())
        .limit(5)
    )

    now = datetime.utcnow()
    for intent in result.scalars().all():
        task_id = _safe_checkout_task_id(intent.task_id)
        if not task_id:
            intent.auto_unlock_status = "invalid_task_id"
            continue

        task = await get_task_by_id(db, task_id)
        if not task:
            intent.auto_unlock_status = "task_not_found"
            continue

        unlock = await _ensure_full_task_unlock(db, task, buyer_user, now=now)
        status = unlock.get("status")
        if status == "already_unlocked":
            intent.used_at = now
            intent.gumroad_sale_id = sale_id
            intent.auto_unlock_status = "already_unlocked"
            await db.flush()
            print(
                f"[Checkout] Pending intent already unlocked task={task_id} sale={sale_id}",
                flush=True,
            )
            return {"status": "already_unlocked", "task_id": task_id, "credits_spent": 0}

        if status == "insufficient_credits":
            intent.auto_unlock_status = "insufficient_credits"
            await db.flush()
            print(
                f"[Checkout] Pending intent has insufficient credits task={task_id} "
                f"sale={sale_id} balance={buyer_user.balance_credits} cost={unlock.get('cost')}",
                flush=True,
            )
            continue

        intent.used_at = now
        intent.gumroad_sale_id = sale_id
        intent.auto_unlock_status = str(status or "unlocked")
        await db.flush()
        credits_spent = int(unlock.get("credits_spent") or 0)
        print(
            f"[Checkout] Auto-unlocked task={task_id} sale={sale_id} "
            f"user={buyer_user.email} credits_spent={credits_spent}",
            flush=True,
        )
        return {"status": status or "unlocked", "task_id": task_id, "credits_spent": credits_spent}

    return None


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


@app.post("/api/email/marketing-unsubscribe")
async def api_marketing_unsubscribe(
    token: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """RFC 8058 one-click unsubscribe for marketing emails."""
    from unsubscribe_tokens import verify_marketing_unsubscribe_token

    if not token:
        raise HTTPException(status_code=400, detail="Missing token")
    email = verify_marketing_unsubscribe_token(token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid token")

    rs = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    row = rs.scalar_one_or_none()
    if row and not row.email_marketing_unsubscribed_at:
        row.email_marketing_unsubscribed_at = datetime.utcnow()
        await db.commit()
    return Response(status_code=204)


@app.get("/unsubscribe/marketing", response_class=HTMLResponse)
async def marketing_unsubscribe_page(
    token: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Visible unsubscribe page for marketing emails."""
    from unsubscribe_tokens import verify_marketing_unsubscribe_token

    if not token:
        raise HTTPException(status_code=400, detail="Missing token")
    email = verify_marketing_unsubscribe_token(token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid token")

    rs = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    row = rs.scalar_one_or_none()
    if row and not row.email_marketing_unsubscribed_at:
        row.email_marketing_unsubscribed_at = datetime.utcnow()
        await db.commit()

    safe_email = html.escape(email)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Marketing emails unsubscribed - AutoRig.online</title>
  <link rel="stylesheet" href="/static/css/styles.css">
</head>
<body style="margin:0;padding:2rem;font-family:system-ui,sans-serif;background:var(--bg,#0a0a0f);color:var(--text,#f0f0f5);">
  <div style="max-width:560px;margin:0 auto;">
    <h1 style="font-size:1.35rem;margin-top:0;">You are unsubscribed</h1>
    <p style="color:var(--text-secondary,#a0a0b0);line-height:1.5;">
      Marketing emails for {safe_email} have been turned off. Task-ready notifications are unchanged.
    </p>
    <p style="margin-top:1.5rem;">
      <a href="{APP_URL}/dashboard" style="color:#6366f1;">Notification settings</a>
      &nbsp;-&nbsp;
      <a href="{APP_URL}" style="color:#6366f1;">Home</a>
    </p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html_content)


def _verify_resend_webhook_signature(payload: bytes, request: Request) -> None:
    """Verify Resend/Svix webhook signature using RESEND_WEBHOOK_SECRET."""
    if not RESEND_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="RESEND_WEBHOOK_SECRET is not configured")
    svix_id = request.headers.get("svix-id") or ""
    svix_timestamp = request.headers.get("svix-timestamp") or ""
    svix_signature = request.headers.get("svix-signature") or ""
    if not svix_id or not svix_timestamp or not svix_signature:
        raise HTTPException(status_code=400, detail="Missing webhook signature headers")
    secret = RESEND_WEBHOOK_SECRET
    if secret.startswith("whsec_"):
        secret = secret.split("_", 1)[1]
    try:
        key = base64.b64decode(secret)
    except Exception:
        key = RESEND_WEBHOOK_SECRET.encode("utf-8")
    signed_payload = f"{svix_id}.{svix_timestamp}.".encode("utf-8") + payload
    expected = base64.b64encode(hmac.new(key, signed_payload, hashlib.sha256).digest()).decode("ascii")
    candidates = []
    for part in svix_signature.split():
        if part.startswith("v1,"):
            candidates.append(part.split(",", 1)[1])
    if not any(hmac.compare_digest(expected, candidate) for candidate in candidates):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")


def _event_email_hash(email: str) -> str:
    return hashlib.sha256((email or "").strip().lower().encode("utf-8")).hexdigest()


@app.post("/api/webhooks/resend")
async def api_resend_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Persist Resend delivery events and suppress hard-bounced/complained users."""
    raw_body = await request.body()
    _verify_resend_webhook_signature(raw_body, request)
    try:
        event = json.loads(raw_body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    svix_id = request.headers.get("svix-id") or None
    if svix_id:
        existing = await db.execute(select(EmailDeliveryEvent.id).where(EmailDeliveryEvent.svix_id == svix_id))
        if existing.scalar_one_or_none() is not None:
            return {"ok": True, "duplicate": True}

    event_type = str(event.get("type") or "unknown")[:64]
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    provider_message_id = data.get("email_id") or data.get("id")
    recipients = data.get("to") if isinstance(data.get("to"), list) else []
    recipient_email = str(recipients[0]).strip().lower() if recipients else ""
    bounce = data.get("bounce") if isinstance(data.get("bounce"), dict) else {}
    bounce_type = str(bounce.get("type") or "")[:32] or None
    bounce_subtype = str(bounce.get("subType") or bounce.get("subtype") or "")[:64] or None
    error_message = str(bounce.get("message") or data.get("error") or "")[:4000] or None

    send_row = None
    if provider_message_id:
        send_rs = await db.execute(
            select(EmailCampaignSend).where(EmailCampaignSend.provider_message_id == str(provider_message_id)).limit(1)
        )
        send_row = send_rs.scalar_one_or_none()

    user = None
    if recipient_email:
        user_rs = await db.execute(select(User).where(func.lower(func.trim(User.email)) == recipient_email).limit(1))
        user = user_rs.scalar_one_or_none()
    if user is None and send_row and send_row.user_id:
        user = await db.get(User, send_row.user_id)

    email_hash = send_row.email_hash if send_row else (_event_email_hash(recipient_email) if recipient_email else None)
    now = datetime.utcnow()
    db.add(
        EmailDeliveryEvent(
            svix_id=svix_id,
            provider_message_id=str(provider_message_id)[:128] if provider_message_id else None,
            campaign_key=send_row.campaign_key if send_row else None,
            user_id=user.id if user else (send_row.user_id if send_row else None),
            email_hash=email_hash,
            event_type=event_type,
            bounce_type=bounce_type,
            bounce_subtype=bounce_subtype,
            error_message=error_message,
            raw_event_json=json.dumps(event, ensure_ascii=False)[:12000],
            created_at=now,
        )
    )

    suppress = False
    reason = None
    if event_type == "email.complained":
        suppress = True
        reason = "complaint"
    elif event_type == "email.suppressed":
        suppress = True
        reason = f"suppressed:{bounce_subtype or bounce_type or 'unknown'}"
    elif event_type == "email.bounced":
        # Resend's email.bounced event is for permanent rejection; keep Transient/Undetermined as retryable.
        if not bounce_type or bounce_type.lower() == "permanent":
            suppress = True
            reason = f"permanent_bounce:{bounce_subtype or 'unknown'}"

    if user is not None:
        if event_type in {"email.bounced", "email.delivery_delayed", "email.suppressed", "email.failed"}:
            user.email_last_bounce_at = now
            user.email_last_bounce_type = bounce_type or event_type
        if event_type == "email.delivery_delayed" or (bounce_type and bounce_type.lower() in {"transient", "undetermined"}):
            user.email_transient_bounce_count = int(user.email_transient_bounce_count or 0) + 1
        if suppress and not user.email_invalid_at:
            user.email_invalid_at = now
            user.email_invalid_reason = reason or event_type
            user.email_invalid_source = "resend_webhook"
            user.email_task_completed = False
            if not user.email_marketing_unsubscribed_at:
                user.email_marketing_unsubscribed_at = now

    await db.commit()
    return {"ok": True, "event_type": event_type, "suppressed": bool(suppress and user is not None)}


def _campaign_click_destination(campaign_key: str, link_key: str) -> Optional[str]:
    from urllib.parse import urlencode

    base = (APP_URL or "https://autorig.online").rstrip("/")
    campaign = (campaign_key or "email-campaign")[:128]
    content = (link_key or "link")[:64]
    utm = urlencode(
        {
            "utm_source": "email",
            "utm_medium": "campaign",
            "utm_campaign": campaign,
            "utm_content": content,
        }
    )
    if link_key == "animal_rig":
        return f"{base}/animal-rig?{utm}"
    if link_key == "home":
        return f"{base}/?{utm}"
    if link_key == "youtube_short":
        return f"https://www.youtube.com/shorts/vEn7laZijOI?{utm}"
    return None


@app.get("/email/click")
async def email_campaign_click(
    request: Request,
    token: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Tracked redirect for signed marketing campaign links."""
    from unsubscribe_tokens import verify_campaign_click_token

    payload = verify_campaign_click_token(token or "")
    if not payload:
        raise HTTPException(status_code=400, detail="Invalid token")

    campaign_key = payload["campaign_key"]
    email = payload["email"]
    link_key = payload["link_key"]
    destination_url = _campaign_click_destination(campaign_key, link_key)
    if not destination_url:
        raise HTTPException(status_code=400, detail="Unknown link")

    rs = await db.execute(select(User).where(func.lower(User.email) == email.lower()))
    user = rs.scalar_one_or_none()
    email_hash = hashlib.sha256(email.encode("utf-8")).hexdigest()
    client_ip = request.client.host if request.client else ""
    ip_hash = (
        hmac.new(SECRET_KEY.encode("utf-8"), client_ip.encode("utf-8"), hashlib.sha256).hexdigest()
        if client_ip else None
    )
    user_agent = (request.headers.get("user-agent") or "")[:512] or None
    db.add(
        EmailCampaignClick(
            campaign_key=campaign_key,
            user_id=user.id if user else None,
            email_hash=email_hash,
            link_key=link_key,
            destination_url=destination_url,
            ip_hash=ip_hash,
            user_agent=user_agent,
            clicked_at=datetime.utcnow(),
        )
    )
    await db.commit()
    return RedirectResponse(url=destination_url, status_code=302)


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
    return [here.parent / "skill.md", here.parent.parent / "skill.md", here.parent.parent.parent / "skill.md"]


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


@app.get("/buy-credits/checkout/{permalink}")
async def buy_credits_checkout(
    permalink: str,
    request: Request,
    source: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    required_credits: Optional[int] = Query(None),
    page_url: Optional[str] = Query(None),
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Server-owned checkout redirect so payment clicks do not depend on JS fetches."""
    product_key = _normalize_gumroad_product_key(permalink)
    if product_key not in AUTORIG_DONATION_PRODUCT_KEYS:
        raise HTTPException(status_code=404, detail="Unknown AutoRig credit product")

    if not user:
        next_path = request.url.path
        if request.url.query:
            next_path += f"?{request.url.query}"
        login_url = f"/auth/login?next={quote(next_path, safe='')}"
        return RedirectResponse(url=login_url, status_code=303)

    source_clean = _clamp_text(source, 80, default="buy_credits_checkout")
    task_id_clean = _safe_checkout_task_id(task_id)
    if required_credits is not None and required_credits <= 0:
        required_credits = None
    page_url_clean = _clamp_text(page_url or request.headers.get("referer") or str(request.url), 1024)
    package_label = _checkout_pack_label(product_key)
    price_label = _checkout_pack_price_label(product_key)
    intent_id = None

    try:
        intent = PurchaseCheckoutIntent(
            user_email=user.email,
            product_permalink=product_key,
            product_kind="credits",
            source=source_clean,
            task_id=task_id_clean,
            required_credits=required_credits,
            page_url=page_url_clean,
            created_at=datetime.utcnow(),
        )
        db.add(intent)
        await db.flush()
        intent_id = intent.id
        await db.commit()
        print(
            f"[Checkout] Credit checkout intent id={intent_id} product={product_key} "
            f"source={source_clean} task_id={task_id_clean or '-'}",
            flush=True,
        )
    except Exception as e:
        await db.rollback()
        print(f"[Checkout] Failed to persist checkout intent product={product_key}: {e}", flush=True)

    try:
        from telegram_bot import broadcast_credits_purchase_click
        asyncio.create_task(
            broadcast_credits_purchase_click(
                package=package_label,
                price=price_label,
                user_email=user.email,
                anon_id=None,
                product_kind="credits",
                permalink=product_key,
                source=source_clean,
                page_url=page_url_clean,
            )
        )
    except Exception as e:
        print(f"[Checkout] Failed to schedule checkout notification id={intent_id}: {e}", flush=True)

    return RedirectResponse(url=_gumroad_checkout_url(product_key, user.email), status_code=303)


@app.get("/api/blender-plugin/offer")
async def blender_plugin_offer(
    user: Optional[User] = Depends(get_current_user),
):
    if not user:
        return {
            "product_kind": "plugin",
            "authenticated": False,
            "checkout_url": "/blender-plugin/checkout",
        }

    product_key, price_usd = _select_blender_plugin_variant(user)
    price_label = _format_usd_price(price_usd)
    return {
        "product_kind": "plugin",
        "authenticated": True,
        "permalink": product_key,
        "price_usd": price_usd,
        "price_label": price_label,
        "display_price": f"{price_label}+",
        "package": f"Blender Plugin ABCD {price_label}",
        "checkout_url": "/blender-plugin/checkout",
    }


@app.get("/blender-plugin/checkout")
async def blender_plugin_checkout(
    request: Request,
    source: Optional[str] = Query(None),
    page_url: Optional[str] = Query(None),
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Server-owned Blender plugin checkout with ABCD price assignment and click telemetry."""
    if not user:
        next_path = request.url.path
        if request.url.query:
            next_path += f"?{request.url.query}"
        login_url = f"/auth/login?next={quote(next_path, safe='')}"
        return RedirectResponse(url=login_url, status_code=303)

    product_key, price_usd = _select_blender_plugin_variant(user)
    price_label = _format_usd_price(price_usd)
    source_clean = _clamp_text(source, 80, default="blender_plugin_checkout")
    page_url_clean = _clamp_text(page_url or request.headers.get("referer") or str(request.url), 1024)
    package_label = f"Blender Plugin ABCD {price_label}"
    intent_id = None

    try:
        intent = PurchaseCheckoutIntent(
            user_email=user.email,
            product_permalink=product_key,
            product_kind="plugin",
            source=source_clean,
            task_id=None,
            required_credits=None,
            page_url=page_url_clean,
            created_at=datetime.utcnow(),
        )
        db.add(intent)
        await db.flush()
        intent_id = intent.id
        await db.commit()
        print(
            f"[Checkout] Plugin checkout intent id={intent_id} product={product_key} "
            f"price={price_label} source={source_clean}",
            flush=True,
        )
    except Exception as e:
        await db.rollback()
        print(f"[Checkout] Failed to persist plugin checkout intent product={product_key}: {e}", flush=True)

    try:
        from telegram_bot import broadcast_credits_purchase_click
        asyncio.create_task(
            broadcast_credits_purchase_click(
                package=package_label,
                price=price_label,
                user_email=user.email,
                anon_id=None,
                product_kind="plugin",
                permalink=product_key,
                source=source_clean,
                page_url=page_url_clean,
            )
        )
    except Exception as e:
        print(f"[Checkout] Failed to schedule plugin checkout notification id={intent_id}: {e}", flush=True)

    return RedirectResponse(url=_gumroad_checkout_url(product_key, user.email), status_code=303)


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
    """Compatibility endpoint for the retired YouTube credit bonus."""
    return {
        "ok": False,
        "disabled": True,
        "detail": "Free credit bonuses are no longer available.",
        "new_balance": user.balance_credits if user else 0,
    }


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
# Support chat (website widget ↔ Telegram forum topic)
# =============================================================================
_SUPPORT_VISITOR_RE = re.compile(r"^[A-Za-z0-9_-]{8,96}$")


def _support_validate_visitor_id_string(visitor_id_string: str) -> str:
    v = (visitor_id_string or "").strip()
    if not _SUPPORT_VISITOR_RE.fullmatch(v):
        raise HTTPException(status_code=400, detail="Invalid visitor_id_string")
    return v


def _support_sanitize_page_url_string(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if len(s) > 4096:
        s = s[:4096]
    return s


@app.get("/api/support-chat/health")
async def api_support_chat_health():
    """Cheap GET to verify reverse-proxy routes /api/support-chat/* to FastAPI."""
    return {"support_api_ok_bool": True}


@app.get("/api/support-chat/session")
async def api_support_chat_session_get(db: AsyncSession = Depends(get_db)):
    """Mirror GET: usage for ``POST /api/support-chat/session``."""
    from telegram_bot import support_forum_configured_bool

    configured = bool(await support_forum_configured_bool(db))
    return {
        "purpose_string": "Describe how to obtain or reuse an active support-chat session.",
        "support_enabled_bool": True,
        "support_configured_bool": configured,
        "post_endpoint_string": "/api/support-chat/session",
        "required_fields_json": {
            "visitor_id_string": "string — browser-held id (8–96 [A-Za-z0-9_-])",
            "page_url_string": "optional string — current page URL for operator context",
        },
        "optional_auth_string": "If the visitor is logged in, include session cookie; server may persist user_email_string on the row.",
        "example_request_json": {
            "visitor_id_string": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
            "page_url_string": "https://example.com/task?id=demo_task",
        },
        "answer_example_json": {
            "session_id_int": 101,
            "visitor_id_string": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
            "topic_ready_bool": False,
            "support_enabled_bool": True,
            "support_configured_bool": True,
            "page_url_string": "https://example.com/task?id=demo_task",
            "user_email_string": "user@example.com",
        },
    }


@app.post("/api/support-chat/session", response_model=SupportChatSessionPostResponse)
@limiter.limit(RATE_LIMIT_SUPPORT_CHAT_SESSION)
async def api_support_chat_session_post(
    request: Request,
    body: SupportChatSessionPostRequest,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from telegram_bot import support_forum_configured_bool

    visitor = _support_validate_visitor_id_string(body.visitor_id_string)
    page = _support_sanitize_page_url_string(body.page_url_string)
    email = getattr(user, "email", None) if user else None

    stmt = (
        select(SupportChatSession)
        .where(
            SupportChatSession.visitor_id == visitor,
            SupportChatSession.status == "open",
        )
        .order_by(SupportChatSession.id.desc())
        .limit(1)
    )
    row = (await db.execute(stmt)).scalar_one_or_none()

    topic_ready_bool = False
    if row is None:
        row = SupportChatSession(
            visitor_id=visitor,
            user_email=email,
            page_url=page,
            status="open",
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
    else:
        if email:
            row.user_email = email
        if page:
            row.page_url = page
        await db.commit()
        await db.refresh(row)

    topic_ready_bool = row.telegram_thread_id is not None
    configured = bool(await support_forum_configured_bool(db))
    return SupportChatSessionPostResponse(
        session_id_int=int(row.id),
        visitor_id_string=visitor,
        topic_ready_bool=topic_ready_bool,
        support_enabled_bool=True,
        support_configured_bool=configured,
        page_url_string=row.page_url,
        user_email_string=row.user_email,
    )


@app.get("/api/support-chat/message")
async def api_support_chat_message_get():
    """Mirror GET: usage for ``POST /api/support-chat/message``."""
    return {
        "purpose_string": "Append a plaintext user message; first message allocates a Telegram forum topic when support is configured.",
        "rate_limit_hint_string": RATE_LIMIT_SUPPORT_CHAT_MESSAGE,
        "max_chars_int": SUPPORT_CHAT_MESSAGE_MAX_CHARS,
        "required_fields_json": {
            "visitor_id_string": "string — must match the session visitor",
            "session_id_int": "int — SupportChatSession id from POST /session",
            "message_text_string": "string — plaintext only",
        },
        "example_request_json": {
            "visitor_id_string": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
            "session_id_int": 101,
            "message_text_string": "Hello — I cannot download my GLB.",
        },
        "answer_example_json": {
            "ok_bool": True,
            "telegram_message_id_int": 555,
        },
    }


@app.post("/api/support-chat/message", response_model=SupportChatMessagePostResponse)
@limiter.limit(RATE_LIMIT_SUPPORT_CHAT_MESSAGE)
async def api_support_chat_message_post(
    request: Request,
    body: SupportChatMessagePostRequest,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from telegram_bot import (
        support_forum_configured_bool,
        telegram_create_support_topic,
        telegram_send_support_message_html,
    )

    visitor = _support_validate_visitor_id_string(body.visitor_id_string)

    sess = (
        (
            await db.execute(
                select(SupportChatSession).where(SupportChatSession.id == body.session_id_int)
            )
        ).scalar_one_or_none()
    )
    if sess is None or sess.visitor_id != visitor:
        raise HTTPException(status_code=404, detail="support session not found")
    if sess.status != "open":
        raise HTTPException(status_code=400, detail="session is not open")

    email = getattr(user, "email", None) if user else None
    if email:
        sess.user_email = email

    txt = body.message_text_string.strip()
    if not txt:
        raise HTTPException(status_code=400, detail="Empty message_text_string")
    if len(txt) > SUPPORT_CHAT_MESSAGE_MAX_CHARS:
        raise HTTPException(status_code=400, detail="message_text_string too long")

    if not await support_forum_configured_bool(db):
        raise HTTPException(
            status_code=503,
            detail="Support chat is temporarily unavailable. Please try again later.",
        )

    if sess.telegram_thread_id is None or sess.telegram_chat_id is None:
        label = sess.user_email or sess.visitor_id[:12]
        topic = (f"Support #{sess.id} · {label}")[:128]
        try:
            fcid, mtid = await telegram_create_support_topic(db, topic)
        except Exception as exc:
            print(f"[SupportChat] telegram topic error: {type(exc).__name__}: {exc}")
            raise HTTPException(
                status_code=503,
                detail="Support chat is temporarily unavailable. Please try again later.",
            ) from exc
        sess.telegram_chat_id = int(fcid)
        sess.telegram_thread_id = int(mtid)
        sess.topic_name = topic
        await db.commit()
        await db.refresh(sess)

    who = html.escape(sess.user_email or sess.visitor_id)
    escaped_text = html.escape(txt)
    page_snip = ""
    if sess.page_url:
        p = html.escape((sess.page_url or "")[:500])
        page_snip = f"\n🌐 {p}"

    tg_html = (
        f"💬 <b>Support session</b> <code>{int(sess.id)}</code>\n"
        f"👤 <b>Visitor</b> {who}{page_snip}\n\n{escaped_text}"
    )

    try:
        telegram_message_id_int = await telegram_send_support_message_html(
            forum_chat_id=int(sess.telegram_chat_id),
            message_thread_id=int(sess.telegram_thread_id),
            html=tg_html,
        )
    except Exception as exc:
        if "message thread not found" not in str(exc).lower():
            print(f"[SupportChat] telegram send failed: {type(exc).__name__}: {exc}")
            raise HTTPException(
                status_code=503,
                detail="Support chat is temporarily unavailable. Please try again later.",
            ) from exc

        print(f"[SupportChat] stale telegram thread, recreating topic: {type(exc).__name__}: {exc}")
        label = sess.user_email or sess.visitor_id[:12]
        topic = (f"Support #{sess.id} · {label}")[:128]
        try:
            fcid, mtid = await telegram_create_support_topic(db, topic)
            sess.telegram_chat_id = int(fcid)
            sess.telegram_thread_id = int(mtid)
            sess.topic_name = topic
            await db.commit()
            await db.refresh(sess)
            telegram_message_id_int = await telegram_send_support_message_html(
                forum_chat_id=int(sess.telegram_chat_id),
                message_thread_id=int(sess.telegram_thread_id),
                html=tg_html,
            )
        except Exception as retry_exc:
            print(f"[SupportChat] telegram send retry failed: {type(retry_exc).__name__}: {retry_exc}")
            raise HTTPException(
                status_code=503,
                detail="Support chat is temporarily unavailable. Please try again later.",
            ) from retry_exc

    msg_row = SupportChatMessage(
        session_id=sess.id,
        direction="user",
        body_text=txt,
        telegram_message_id=int(telegram_message_id_int),
    )
    db.add(msg_row)
    await db.commit()

    return SupportChatMessagePostResponse(
        ok_bool=True,
        telegram_message_id_int=int(telegram_message_id_int),
    )


@app.get("/api/support-chat/messages")
@limiter.limit(RATE_LIMIT_SUPPORT_CHAT_MESSAGES_POLL)
async def api_support_chat_messages_poll(
    request: Request,
    visitor_id_string: Optional[str] = Query(None),
    session_id_int: Optional[int] = Query(None),
    after_id_int: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Poll support messages; omit query params to receive GET usage instructions."""

    instr = {
        "purpose_string": "Return chronological messages newer than ``after_id_int`` for a session.",
        "post_mirror_string": "(none) polling only; authenticated by visitor id + numeric session.",
        "query_params_json": {
            "visitor_id_string": "required query string — same as session creation",
            "session_id_int": "required query int — session id",
            "after_id_int": "optional query int — return rows with id > after_id_int",
        },
        "example_answer_json": {
            "messages": [
                {
                    "id_int": 1,
                    "direction_string": "user",
                    "body_text_string": "hello",
                    "created_at_string": "2026-04-30T12:34:56.000000",
                }
            ]
        },
        "mirrors_related_string": "POST /api/support-chat/message mirrors usage is documented at GET /api/support-chat/message",
    }

    if not visitor_id_string or session_id_int is None:
        return JSONResponse(content=instr)

    visitor = _support_validate_visitor_id_string(visitor_id_string)
    sess = (
        (
            await db.execute(
                select(SupportChatSession).where(SupportChatSession.id == int(session_id_int))
            )
        ).scalar_one_or_none()
    )
    if sess is None or sess.visitor_id != visitor:
        raise HTTPException(status_code=404, detail="support session not found")

    mq = (
        await db.execute(
            select(SupportChatMessage)
            .where(SupportChatMessage.session_id == sess.id, SupportChatMessage.id > int(after_id_int))
            .order_by(SupportChatMessage.id.asc())
        )
    ).scalars().all()

    items: List[SupportChatMessageItem] = []
    for m in mq:
        created = m.created_at.isoformat()
        items.append(
            SupportChatMessageItem(
                id_int=int(m.id),
                direction_string=str(m.direction),
                body_text_string=str(m.body_text),
                created_at_string=created,
            )
        )
    return SupportChatMessagesPollResponse(messages=items)


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
    animal_type: Optional[str] = None
    rig_mode: Optional[str] = None
    rig_v2_detection_meta: Optional[Dict[str, Any]] = None
    rig_v2_manual_selection = False
    local_rotation: Optional[List[float]] = None
    animal_semantic_markers: Optional[Dict[str, List[float]]] = None
    preflight_render_image_data_url: Optional[str] = None
    source_preview_url: Optional[str] = None

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
        raw_animal_type = data.get("animal_type")
        if raw_animal_type is not None and str(raw_animal_type).strip():
            animal_type = str(raw_animal_type).strip().lower()
        raw_mode = data.get("mode")
        if raw_mode is not None and str(raw_mode).strip():
            rig_mode = str(raw_mode).strip()
        local_rotation = _coerce_float_vec3(data.get("local_rotation"), "local_rotation")
        animal_semantic_markers = _coerce_animal_semantic_markers(data.get("animal_semantic_markers"))
        rig_v2_manual_selection = bool(data.get("rig_v2_manual_selection"))
        raw_detection = data.get("rig_v2_animal_detection")
        if isinstance(raw_detection, dict):
            rig_v2_detection_meta = raw_detection
        elif data.get("rig_v2_animal_detection_json"):
            try:
                parsed_detection = json.loads(str(data.get("rig_v2_animal_detection_json") or "{}"))
                if isinstance(parsed_detection, dict):
                    rig_v2_detection_meta = parsed_detection
            except Exception:
                rig_v2_detection_meta = None
        raw_preflight_render = data.get("preflight_render_jpg_base64_string")
        if isinstance(raw_preflight_render, str) and raw_preflight_render.strip():
            preflight_render_image_data_url = raw_preflight_render.strip()
        raw_source_preview = data.get("source_preview_url")
        if isinstance(raw_source_preview, str) and raw_source_preview.strip():
            source_preview_url = raw_source_preview.strip()
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
        raw_animal_type = form.get("animal_type")
        if raw_animal_type is not None and str(raw_animal_type).strip():
            animal_type = str(raw_animal_type).strip().lower()
        raw_mode = form.get("mode")
        if raw_mode is not None and str(raw_mode).strip():
            rig_mode = str(raw_mode).strip()
        local_rotation = _coerce_float_vec3(
            form.get("local_rotation_json") or form.get("local_rotation"),
            "local_rotation",
        )
        animal_semantic_markers = _coerce_animal_semantic_markers(
            form.get("animal_semantic_markers_json") or form.get("animal_semantic_markers")
        )
        rig_v2_manual_selection = str(form.get("rig_v2_manual_selection") or "").strip().lower() in ("1", "true", "yes", "on")
        raw_detection = form.get("rig_v2_animal_detection_json")
        if raw_detection is not None and str(raw_detection).strip():
            try:
                parsed_detection = json.loads(str(raw_detection))
                if isinstance(parsed_detection, dict):
                    rig_v2_detection_meta = parsed_detection
            except Exception:
                rig_v2_detection_meta = None
        raw_preflight_render = form.get("preflight_render_jpg_base64_string")
        if raw_preflight_render is not None and str(raw_preflight_render).strip():
            preflight_render_image_data_url = str(raw_preflight_render).strip()
        raw_source_preview = form.get("source_preview_url")
        if raw_source_preview is not None and str(raw_source_preview).strip():
            source_preview_url = str(raw_source_preview).strip()
        fu = form.get("file")
        # Accept any Starlette/FastAPI upload object (isinstance can fail across re-exports).
        if fu is not None and hasattr(fu, "read") and hasattr(fu, "filename"):
            file = fu

    if pipeline not in ("rig", "convert"):
        pipeline = "rig"
    input_type = normalize_task_type(input_type)
    disk_headroom_checked = False

    # Handle file upload
    final_url = input_url
    if file is not None:
        source = "upload"
    if source == "upload" and file:
        await ensure_request_disk_headroom(db, context="task_create_upload")
        disk_headroom_checked = True

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

    if not disk_headroom_checked:
        await ensure_request_disk_headroom(db, context="task_create")

    preflight_render_image_data_url = (
        preflight_render_image_data_url
        or _pop_preflight_render_image_from_meta(rig_v2_detection_meta)
    )

    if pipeline == "convert" and not _url_path_endswith_glb(final_url):
        raise HTTPException(
            status_code=400,
            detail="pipeline=convert requires a .glb input URL or .glb upload filename.",
        )

    animal_allowed = [x for x in RIG_V2_ALLOWED_ANIMAL_TYPES if x != "humanoid"]
    if input_type == "animal":
        if animal_type not in animal_allowed and isinstance(rig_v2_detection_meta, dict):
            candidate = str(rig_v2_detection_meta.get("animal_type") or "").strip().lower()
            if candidate in animal_allowed:
                animal_type = candidate
        if animal_type not in animal_allowed:
            raise HTTPException(status_code=400, detail="animal_type is required for animal rig tasks")
        rig_mode = rig_mode or "only_rig"
        if rig_v2_detection_meta is None:
            rig_v2_detection_meta = {}
        manual_animal_selection = bool(
            rig_v2_manual_selection
            or rig_v2_detection_meta.get("manual_selection")
            or rig_v2_detection_meta.get("user_selected_bool")
            or not any(k in rig_v2_detection_meta for k in ("animal_decision_accepted_bool", "animal_decision_weight_float", "results", "scores"))
        )
        rig_v2_detection_meta = {
            **rig_v2_detection_meta,
            "type": "animal",
            "animal_type": animal_type,
            "animal_type_string": animal_type,
            "mode": rig_mode,
        }
        if local_rotation is not None:
            rig_v2_detection_meta["local_rotation"] = local_rotation
        if animal_semantic_markers:
            rig_v2_detection_meta["animal_semantic_markers"] = animal_semantic_markers
            rig_v2_detection_meta["source"] = "blueprint_retarget"
        if manual_animal_selection:
            rig_v2_detection_meta.update({
                "source": rig_v2_detection_meta.get("source") or "manual_task_create",
                "accepted": True,
                "manual_selection": True,
                "user_selected_bool": True,
                "animal_decision_accepted_bool": True,
            })

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

    _save_preflight_render_image(task.id, preflight_render_image_data_url)

    try:
        settings = json.loads(task.viewer_settings or "{}")
        if not isinstance(settings, dict):
            settings = {}
    except Exception:
        settings = {}

    if rig_v2_detection_meta:
        settings["rig_v2_animal_detection"] = rig_v2_detection_meta

    if source_preview_url:
        parsed_source_preview = urlparse(source_preview_url)
        if parsed_source_preview.scheme in ("http", "https") and parsed_source_preview.netloc:
            settings["source_preview_url"] = source_preview_url

    if "viewer_theme_selection" not in settings:
        selected_theme = _select_viewer_theme_from_metadata(
            input_url=final_url,
            input_type=input_type,
            rig_v2_detection_meta=rig_v2_detection_meta if isinstance(rig_v2_detection_meta, dict) else None,
        )
        if selected_theme:
            settings["viewer_theme_selection"] = selected_theme

    if settings:
        task.viewer_settings = json.dumps(settings, ensure_ascii=False)
        await db.commit()
    
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
        free_workers = await get_dispatchable_workers(db, queue_status)
        free_worker = free_workers[0] if free_workers else None
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

RIG_V2_ALLOWED_EXTS = {".fbx", ".glb", ".obj"}
RIG_V2_ALLOWED_SUPPORT_EXTS = {".mtl"}
RIG_V2_PREVIEW_STATUS = "rig_v2_preview"
RIG_V2_VISION_CONFIG_PATH = Path("/root/autorig/ai_vision_animal_type_detect.json")
RIG_V2_ALLOWED_ANIMAL_TYPES = [
    "humanoid",
    "dog",
    "bear",
    "cat",
    "cow",
    "deer",
    "elephant",
    "giraffe",
    "horse",
    "mouse",
    "pig",
    "rabbit",
    "turtle",
]
RIG_V2_DISCOVERED_MODELS_CACHE: Dict[str, Any] = {"expires_at": 0.0, "models": []}
RIG_V2_OPENAI_MODELS_CACHE: Dict[str, Any] = {"expires_at": 0.0, "models": []}


def _coerce_float_vec3(value: Any, field_name: str) -> Optional[List[float]]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} must be a JSON array [x,y,z]") from exc
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise HTTPException(status_code=400, detail=f"{field_name} must be an array of 3 numbers")
    out: List[float] = []
    for item in value:
        try:
            number = float(item)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} must contain only numbers") from exc
        if number != number or abs(number) > 1_000_000:
            raise HTTPException(status_code=400, detail=f"{field_name} contains an invalid number")
        out.append(number)
    return out


def _coerce_animal_semantic_markers(value: Any) -> Optional[Dict[str, List[float]]]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail="animal_semantic_markers must be a JSON object") from exc
    if not isinstance(value, dict):
        raise HTTPException(status_code=400, detail="animal_semantic_markers must be an object")

    markers: Dict[str, List[float]] = {}
    for raw_key, raw_vec in value.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if len(key) > 96 or not re.match(r"^[A-Za-z0-9_.:-]+$", key):
            raise HTTPException(status_code=400, detail=f"Invalid semantic marker key: {key[:96]}")
        markers[key] = _coerce_float_vec3(raw_vec, f"animal_semantic_markers.{key}") or [0.0, 0.0, 0.0]
        if len(markers) > 256:
            raise HTTPException(status_code=400, detail="Too many semantic markers")
    return markers or None


def _rig_v2_server_time() -> int:
    return int(time.time())


def _rig_v2_load_vision_config() -> Dict[str, Any]:
    try:
        raw = RIG_V2_VISION_CONFIG_PATH.read_text(encoding="utf-8")
        cfg = json.loads(raw)
        if not isinstance(cfg, dict):
            cfg = {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Rig V2 vision config error: {e}")

    cfg.setdefault("open_router_api_url_string", "https://openrouter.ai/api/v1/chat/completions")
    cfg.setdefault("open_router_models_url_string", "https://openrouter.ai/api/v1/models")
    cfg.setdefault("open_ai_api_url_string", "https://api.openai.com/v1/chat/completions")
    cfg.setdefault("open_ai_vision_model_string", "gpt-4o-mini")
    cfg.setdefault("open_ai_strong_vision_model_string", "gpt-5.4-nano")
    cfg.setdefault("image_size_int", 512)
    cfg.setdefault("allowed_animal_types_array", RIG_V2_ALLOWED_ANIMAL_TYPES)
    cfg.setdefault(
        "prompt",
        (
            "Analyze this 3D model render. Choose exactly one animal_type from: "
            + ", ".join(RIG_V2_ALLOWED_ANIMAL_TYPES)
            + '. Return only valid JSON: {"animal_type":"<one_allowed_value>","confidence_float":0.0}. '
            "confidence_float must be between 0 and 1 and should reflect visual certainty for this single view. "
            "Choose humanoid only for a clearly upright human-like biped with a head, torso, two arms, and two legs. "
            "Do not choose humanoid for robots, spider/mech robots, vehicles, drones, or low multi-legged bodies. "
            "For spider-like or multi-legged mechanical models, choose the closest non-humanoid quadruped/low-body animal type, often turtle, dog, cat, or mouse depending on silhouette. "
            "If uncertain, choose the closest visual body type and lower confidence."
        ),
    )
    cfg.setdefault(
        "open_router_free_vision_models_array",
        [
            "qwen/qwen2.5-vl-72b-instruct:free",
            "qwen/qwen2.5-vl-32b-instruct:free",
            "google/gemini-2.0-flash-exp:free",
            "meta-llama/llama-3.2-11b-vision-instruct:free",
        ],
    )
    return cfg


def _rig_v2_normalize_image_data_url(image_value: str) -> str:
    value = (image_value or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="image_jpg_base64_string is required")
    if value.startswith("data:image/"):
        header, _, b64 = value.partition(",")
        if not b64:
            raise HTTPException(status_code=400, detail="Invalid image data URL")
        mime = "image/jpeg" if "jpeg" in header or "jpg" in header else "image/png"
    else:
        b64 = value
        mime = "image/jpeg"
    try:
        decoded = base64.b64decode(b64, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="image_jpg_base64_string must be valid base64")
    if not decoded:
        raise HTTPException(status_code=400, detail="image_jpg_base64_string is empty")
    if len(decoded) > 2_500_000:
        raise HTTPException(status_code=413, detail="Vision image is too large")
    return f"data:{mime};base64,{base64.b64encode(decoded).decode('ascii')}"


def _rig_v2_prompt_from_config(cfg: Dict[str, Any], prompt_override: str = "") -> str:
    prompt = (prompt_override or "").strip()
    if not prompt:
        prompt = str(cfg.get("prompt") or "").strip()
    if len(prompt) > 6000:
        raise HTTPException(status_code=400, detail="prompt_override_string is too long")
    return prompt


def _rig_v2_is_openai_vision_model_id(model_id: str) -> bool:
    model = (model_id or "").strip().lower()
    if not model:
        return False
    non_vision_markers = (
        "audio",
        "codex",
        "dall-e",
        "embedding",
        "image",
        "moderation",
        "realtime",
        "search",
        "speech",
        "transcribe",
        "tts",
        "whisper",
    )
    if any(marker in model for marker in non_vision_markers):
        return False
    return model.startswith(("gpt-5", "gpt-4.1", "gpt-4o", "o3", "o4"))


def _rig_v2_sort_openai_model_ids(model_ids: List[str], preferred_model: str = "") -> List[str]:
    preferred = (preferred_model or "").strip()

    def score(model_id: str) -> Tuple[int, str]:
        if preferred and model_id == preferred:
            return (0, model_id)
        if model_id.startswith("gpt-5.5"):
            return (1, model_id)
        if model_id.startswith("gpt-5.4"):
            return (2, model_id)
        if model_id.startswith("gpt-5"):
            return (3, model_id)
        if model_id.startswith("gpt-4.1"):
            return (4, model_id)
        if model_id.startswith("gpt-4o"):
            return (5, model_id)
        if model_id.startswith(("o4", "o3")):
            return (6, model_id)
        return (20, model_id)

    return sorted(model_ids, key=score)


async def _rig_v2_list_openai_models(cfg: Dict[str, Any]) -> List[str]:
    now = time.time()
    cached_until = float(RIG_V2_OPENAI_MODELS_CACHE.get("expires_at") or 0.0)
    cached_models = RIG_V2_OPENAI_MODELS_CACHE.get("models") or []
    if now < cached_until and isinstance(cached_models, list):
        return [str(model) for model in cached_models if str(model).strip()]

    api_key = str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip()
    if not api_key:
        return []
    models_url = str(cfg.get("open_ai_models_url_string") or "https://api.openai.com/v1/models").strip()
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(models_url, headers={"Authorization": f"Bearer {api_key}"})
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []
    model_ids = [
        str(item.get("id") or "").strip()
        for item in data.get("data", []) if isinstance(item, dict)
        if _rig_v2_is_openai_vision_model_id(str(item.get("id") or ""))
    ]
    model_ids = _rig_v2_sort_openai_model_ids(
        list(dict.fromkeys(model_ids)),
        str(cfg.get("open_ai_strong_vision_model_string") or ""),
    )
    RIG_V2_OPENAI_MODELS_CACHE["models"] = model_ids
    RIG_V2_OPENAI_MODELS_CACHE["expires_at"] = now + 300
    return model_ids


def _rig_v2_extract_vision_result(text: str, allowed: List[str]) -> Tuple[Optional[str], float]:
    raw = (text or "").strip()
    candidates = [raw]
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        candidates.insert(0, m.group(0))
    for candidate in candidates:
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        if isinstance(data, dict):
            animal = str(data.get("animal_type") or data.get("animal_type_string") or "").strip().lower()
            if animal in allowed:
                confidence_value = data.get("confidence_float")
                if confidence_value is None:
                    confidence_value = data.get("confidence")
                if confidence_value is None:
                    confidence_value = data.get("weight_float")
                if confidence_value is None:
                    confidence_value = 1.0
                try:
                    confidence = float(confidence_value)
                except Exception:
                    confidence = 1.0
                return animal, max(0.0, min(1.0, confidence))
    lowered = raw.lower()
    for animal in allowed:
        if re.search(rf"\b{re.escape(animal)}\b", lowered):
            return animal, 1.0
    return None, 0.0


async def _rig_v2_discover_free_vision_models(cfg: Dict[str, Any], api_key: str) -> List[str]:
    now = time.time()
    cached_until = float(RIG_V2_DISCOVERED_MODELS_CACHE.get("expires_at") or 0.0)
    cached_models = RIG_V2_DISCOVERED_MODELS_CACHE.get("models") or []
    if now < cached_until and isinstance(cached_models, list):
        return [str(model) for model in cached_models if str(model).strip()]

    models_url = str(cfg.get("open_router_models_url_string") or "").strip()
    if not models_url:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(
                models_url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "HTTP-Referer": (APP_URL or "https://autorig.online").rstrip("/"),
                    "X-Title": "AutoRig Rig V2",
                },
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []
    out: List[str] = []
    for item in data.get("data", []) if isinstance(data, dict) else []:
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or "").strip()
        if not model_id or not model_id.endswith(":free"):
            continue
        haystack = json.dumps(item, ensure_ascii=False).lower()
        if "image" not in haystack and "vision" not in haystack:
            continue
        out.append(model_id)
    out = out[:24]
    RIG_V2_DISCOVERED_MODELS_CACHE["models"] = out
    RIG_V2_DISCOVERED_MODELS_CACHE["expires_at"] = now + 300
    return out


async def _rig_v2_call_openrouter_vision(
    *,
    cfg: Dict[str, Any],
    image_data_url: str,
    prompt_override: str = "",
) -> Dict[str, Any]:
    api_key = str(cfg.get("open_router_api_key") or "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="OpenRouter API key is not configured")
    api_url = str(cfg.get("open_router_api_url_string") or "").strip()
    if not api_url:
        raise HTTPException(status_code=500, detail="OpenRouter API URL is not configured")

    allowed = [
        str(x).strip().lower()
        for x in (cfg.get("allowed_animal_types_array") or RIG_V2_ALLOWED_ANIMAL_TYPES)
        if str(x).strip()
    ]
    prompt = _rig_v2_prompt_from_config(cfg, prompt_override)
    configured_models = [
        str(x).strip()
        for x in (cfg.get("open_router_free_vision_models_array") or [])
        if str(x).strip()
    ]
    discovered_models = await _rig_v2_discover_free_vision_models(cfg, api_key)
    models: List[str] = []
    for model in configured_models + discovered_models:
        if model not in models:
            models.append(model)
    if not models:
        raise HTTPException(status_code=500, detail="No OpenRouter vision models configured or discovered")

    last_error = ""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": (APP_URL or "https://autorig.online").rstrip("/"),
        "X-Title": "AutoRig Rig V2",
    }
    for model in models:
        payload = {
            "model": model,
            "temperature": 0,
            "max_tokens": 64,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_data_url}},
                    ],
                }
            ],
        }
        try:
            async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
                resp = await client.post(api_url, headers=headers, json=payload)
            if resp.status_code != 200:
                last_error = f"{model}: HTTP {resp.status_code} {resp.text[:240]}"
                if resp.status_code == 429:
                    return {
                        "success_bool": False,
                        "status_string": "vision_rate_limited",
                        "animal_type_string": "",
                        "model_used_string": model,
                        "error_string": last_error[:500],
                        "server_time_unix_int": _rig_v2_server_time(),
                    }
                continue
            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
            if isinstance(content, list):
                content = " ".join(
                    str(part.get("text") or part) if isinstance(part, dict) else str(part)
                    for part in content
                )
            animal_type, confidence_float = _rig_v2_extract_vision_result(str(content), allowed)
            if animal_type:
                return {
                    "success_bool": True,
                    "status_string": "ok",
                    "animal_type_string": animal_type,
                    "confidence_float": confidence_float,
                    "model_used_string": model,
                    "server_time_unix_int": _rig_v2_server_time(),
                }
            last_error = f"{model}: response did not contain allowed animal_type"
        except Exception as e:
            last_error = f"{model}: {e}"
            continue
    return {
        "success_bool": False,
        "status_string": "vision_failed",
        "animal_type_string": "",
        "model_used_string": "",
        "error_string": last_error[:500],
        "server_time_unix_int": _rig_v2_server_time(),
    }


async def _rig_v2_call_openai_vision(
    *,
    cfg: Dict[str, Any],
    image_data_url: str,
    fallback_reason: str,
    view_id: str = "",
    prompt_override: str = "",
    model_override: str = "",
) -> Dict[str, Any]:
    api_key = str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip()
    if not api_key:
        return {
            "success_bool": False,
            "status_string": "openai_fallback_not_configured",
            "animal_type_string": "",
            "model_used_string": "",
            "error_string": fallback_reason[:500],
            "server_time_unix_int": _rig_v2_server_time(),
        }
    api_url = str(cfg.get("open_ai_api_url_string") or "").strip()
    default_model = str(cfg.get("open_ai_vision_model_string") or "gpt-4o-mini").strip()
    strong_model = str(cfg.get("open_ai_strong_vision_model_string") or default_model).strip()
    model = (model_override or "").strip() or (strong_model if view_id == "top_side_45" else default_model)
    if len(model) > 160 or not re.match(r"^[A-Za-z0-9._:/-]+$", model):
        raise HTTPException(status_code=400, detail="Invalid open_ai_model_override_string")
    allowed = [
        str(x).strip().lower()
        for x in (cfg.get("allowed_animal_types_array") or RIG_V2_ALLOWED_ANIMAL_TYPES)
        if str(x).strip()
    ]
    prompt = _rig_v2_prompt_from_config(cfg, prompt_override)
    payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_data_url, "detail": "low"}},
                ],
            }
        ],
    }
    if model.startswith(("gpt-5", "o3", "o4")):
        payload["max_completion_tokens"] = 256
    else:
        payload["temperature"] = 0
        payload["max_tokens"] = 80
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            resp = await client.post(api_url, headers=headers, json=payload)
        if resp.status_code != 200:
            return {
                "success_bool": False,
                "status_string": "openai_fallback_failed",
                "animal_type_string": "",
                "model_used_string": model,
                "error_string": f"OpenRouter fallback reason: {fallback_reason[:220]}; OpenAI HTTP {resp.status_code} {resp.text[:220]}",
                "server_time_unix_int": _rig_v2_server_time(),
            }
        data = resp.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        if isinstance(content, list):
            content = " ".join(
                str(part.get("text") or part) if isinstance(part, dict) else str(part)
                for part in content
            )
        animal_type, confidence_float = _rig_v2_extract_vision_result(str(content), allowed)
        if animal_type:
            return {
                "success_bool": True,
                "status_string": "ok_openai_fallback",
                "animal_type_string": animal_type,
                "confidence_float": confidence_float,
                "model_used_string": f"openai/{model}",
                "view_id_string": view_id,
                "fallback_reason_string": fallback_reason[:500],
                "server_time_unix_int": _rig_v2_server_time(),
            }
        return {
            "success_bool": False,
            "status_string": "openai_fallback_invalid_response",
            "animal_type_string": "",
            "model_used_string": model,
            "error_string": f"OpenAI response did not contain allowed animal_type. OpenRouter fallback reason: {fallback_reason[:300]}",
            "server_time_unix_int": _rig_v2_server_time(),
        }
    except Exception as e:
        return {
            "success_bool": False,
            "status_string": "openai_fallback_failed",
            "animal_type_string": "",
            "model_used_string": model,
            "error_string": f"OpenRouter fallback reason: {fallback_reason[:220]}; OpenAI error: {e}",
            "server_time_unix_int": _rig_v2_server_time(),
        }




@app.get("/api/rig-v2/vision/animal-type")
async def api_rig_v2_vision_animal_type_docs():
    """GET mirror documenting the POST vision endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/rig-v2/vision/animal-type",
        "required_fields_array": ["image_jpg_base64_string"],
        "optional_fields_array": [
            "task_id_string",
            "view_id_string",
            "prompt_override_string",
            "force_openai_bool",
            "open_ai_model_override_string",
        ],
        "allowed_animal_types_array": RIG_V2_ALLOWED_ANIMAL_TYPES,
        "example_request_object": {
            "image_jpg_base64_string": "data:image/jpeg;base64,/9j/...",
            "task_id_string": "optional-task-id",
        },
        "example_response_object": {
            "success_bool": True,
            "status_string": "ok",
            "animal_type_string": "rabbit",
            "confidence_float": 0.86,
            "model_used_string": "provider/model:free or openai/model",
            "server_time_unix_int": 0,
        },
        "server_time_unix_int": _rig_v2_server_time(),
    }


@app.get("/api/rig-v2/vision/config")
async def api_rig_v2_vision_config():
    """Expose editable prototype prompt and available OpenAI model IDs without exposing API keys."""
    cfg = _rig_v2_load_vision_config()
    models = await _rig_v2_list_openai_models(cfg)
    strong_model = str(cfg.get("open_ai_strong_vision_model_string") or "").strip()
    default_model = str(cfg.get("open_ai_vision_model_string") or "").strip()
    return {
        "success_bool": True,
        "status_string": "ok",
        "prompt_string": _rig_v2_prompt_from_config(cfg),
        "open_ai_models_array": models,
        "open_ai_default_model_string": default_model,
        "open_ai_strong_model_string": strong_model,
        "server_time_unix_int": _rig_v2_server_time(),
    }


@app.post("/api/rig-v2/vision/animal-type")
@limiter.limit("30/minute")
async def api_rig_v2_vision_animal_type(request: Request):
    """Classify a 512x512 JPEG render via OpenRouter, with OpenAI only as rate-limit fallback."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")
    image_data_url = _rig_v2_normalize_image_data_url(
        str(body.get("image_jpg_base64_string") or "")
    )
    view_id = str(body.get("view_id_string") or "").strip()
    prompt_override = str(body.get("prompt_override_string") or "").strip()
    model_override = str(body.get("open_ai_model_override_string") or "").strip()
    force_openai = bool(body.get("force_openai_bool"))
    cfg = _rig_v2_load_vision_config()
    if force_openai:
        return await _rig_v2_call_openai_vision(
            cfg=cfg,
            image_data_url=image_data_url,
            fallback_reason="forced_openai_retry",
            view_id=view_id,
            prompt_override=prompt_override,
            model_override=model_override,
        )
    openrouter_result = await _rig_v2_call_openrouter_vision(
        cfg=cfg,
        image_data_url=image_data_url,
        prompt_override=prompt_override,
    )
    if openrouter_result.get("status_string") == "vision_rate_limited":
        fallback_reason = str(openrouter_result.get("error_string") or openrouter_result.get("status_string") or "")
        return await _rig_v2_call_openai_vision(
            cfg=cfg,
            image_data_url=image_data_url,
            fallback_reason=fallback_reason,
            view_id=view_id,
            prompt_override=prompt_override,
            model_override=model_override,
        )
    return openrouter_result

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

    await ensure_request_disk_headroom(db, context="convert_from_rig")

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
        free_workers = await get_dispatchable_workers(db, queue_status)
        free_worker = free_workers[0] if free_workers else None
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


def _gumroad_product_key_from_payload(product: str | None, product_name: str | None) -> str:
    product_key = _normalize_gumroad_product_key(product)
    if product_key in GUMROAD_PRODUCT_CREDITS:
        return product_key

    name = (product_name or "").strip().lower()
    if "autorig" not in name or "credit" not in name:
        return product_key
    if re.search(r"\b1000\b", name):
        return "autorig-1000"
    if re.search(r"\b100\b", name):
        return "autorig-100"
    if re.search(r"\b30\b", name):
        return "oneclick-30-credits"
    return product_key


def _gumroad_clean_email(value: Optional[str]) -> str:
    email = (str(value or "").strip()).lower()
    if not email or len(email) > 255:
        return ""
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return ""
    return email


def _gumroad_credit_target_email(parsed_form: Dict[str, Any]) -> str:
    for key in (
        "url_params[userid]",
        "userid",
        "user_id",
        "custom_fields[userid]",
        "custom_fields[user_id]",
    ):
        email = _gumroad_clean_email(parsed_form.get(key))
        if email:
            return email
    return _gumroad_clean_email(parsed_form.get("email")) or "unknown"


def _is_autorig_credit_product(product_key: str) -> bool:
    return (product_key or "").strip().lower() in AUTORIG_DONATION_PRODUCT_KEYS


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
    checkout_email = _gumroad_clean_email(parsed_form.get("email")) or "unknown"
    email = _gumroad_credit_target_email(parsed_form)
    product = (parsed_form.get("product_permalink") or parsed_form.get("permalink") or "").strip() or "unknown"
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

    product_key = _gumroad_product_key_from_payload(product, product_name)
    local_credits_added = 0
    is_plugin_product = _is_blender_plugin_product(product_key, product_name)
    known_product = (
        product_key in {str(k).strip().lower() for k in GUMROAD_PRODUCT_CREDITS.keys()}
        or is_plugin_product
    )
    should_notify_purchase = False

    if _is_autorig_credit_product(product_key) and email and email != "unknown":
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
                        user.gumroad_email = checkout_email if checkout_email != "unknown" else email
                        purchase.credited = True
                        purchase.credits_added = credits_to_add
                        local_credits_added = credits_to_add
                        auto_unlock = await _try_auto_unlock_pending_checkout(db, user, sale_id)
                        if auto_unlock:
                            print(
                                f"[Gumroad] Checkout auto-unlock result sale={sale_id} "
                                f"status={auto_unlock.get('status')} task={auto_unlock.get('task_id')} "
                                f"credits_spent={auto_unlock.get('credits_spent')}",
                                flush=True,
                            )
                    await db.commit()
                    should_notify_purchase = True
        except Exception as e:
            print(f"[Gumroad] Local autorig crediting failed for {sale_id}: {e}", flush=True)

    if is_plugin_product:
        try:
            async with AsyncSessionLocal() as db:
                purchase = GumroadPurchase(
                    sale_id=sale_id,
                    email=email or "unknown",
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
                    await db.commit()
                    should_notify_purchase = True
                except IntegrityError:
                    await db.rollback()
        except Exception as e:
            print(f"[Gumroad] Plugin purchase audit failed for {sale_id}: {e}", flush=True)

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
        notice_kind = "plugin" if is_plugin_product else "credits"
        notice_price = _blender_plugin_price_label(product_key, price_cents) if is_plugin_product else str(price_raw)
        notice_package = (
            f"Blender Plugin ABCD {notice_price}" if is_plugin_product else _checkout_pack_label(product_key)
        )
        asyncio.create_task(
            broadcast_credits_purchased(
                credits=0 if is_plugin_product else (local_credits_added if local_credits_added > 0 else max(price_cents, 0)),
                price=notice_price,
                user_email=email,
                product=product_key or product,
                sale_id=sale_id,
                is_test=is_test,
                is_recurring_charge=is_recurring_charge,
                refunded=refunded,
                product_kind=notice_kind,
                package=notice_package,
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

    rig_v2_animal_detection = None
    viewer_theme_selection = None
    try:
        settings = json.loads(task.viewer_settings or "{}")
        if isinstance(settings, dict):
            detection = settings.get("rig_v2_animal_detection")
            if isinstance(detection, dict):
                rig_v2_animal_detection = detection
            theme = settings.get("viewer_theme_selection")
            if isinstance(theme, dict):
                viewer_theme_selection = theme
    except Exception:
        rig_v2_animal_detection = None
        viewer_theme_selection = None

    def _task_response_animal_type(detection: Optional[dict]) -> Optional[str]:
        if not isinstance(detection, dict):
            return None
        for key in (
            "animal_type",
            "animal_type_string",
            "selected_type_string",
            "candidate_animal_type_string",
            "selected_animal_type",
            "selected_animal_type_string",
        ):
            value = str(detection.get(key) or "").strip().lower()
            if value:
                return value
        first_result = detection.get("first_result")
        if isinstance(first_result, dict):
            for key in ("animal_type", "animal_type_string"):
                value = str(first_result.get(key) or "").strip().lower()
                if value:
                    return value
        return None

    is_admin_viewer = bool(user and is_admin_email(user.email))
    worker_api_for_response = (task.worker_api or None) if is_admin_viewer else None
    blueprint_skeleton_url, blueprint_rig_preview_url = await _resolve_task_blueprint_urls(task)
    response_video_ready = bool(task.video_ready or blueprint_rig_preview_url)
    response_video_url = blueprint_rig_preview_url or task.video_url
    response_animal_type = _task_response_animal_type(rig_v2_animal_detection)

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        progress=task.progress,
        ready_count=task.ready_count,
        total_count=task.total_count,
        output_urls=task.output_urls,
        ready_urls=task.ready_urls,
        video_ready=response_video_ready,
        video_url=response_video_url,
        blueprint_skeleton_ready=bool(blueprint_skeleton_url),
        blueprint_skeleton_url=blueprint_skeleton_url,
        blueprint_rig_preview_ready=bool(blueprint_rig_preview_url),
        blueprint_rig_preview_url=blueprint_rig_preview_url,
        input_url=task.input_url,
        input_type=task.input_type,
        animal_type=response_animal_type,
        rig_type=response_animal_type,
        rig_v2_animal_detection=rig_v2_animal_detection,
        viewer_theme_selection=viewer_theme_selection,
        fbx_glb_output_url=task.fbx_glb_output_url,
        fbx_glb_model_name=task.fbx_glb_model_name,
        fbx_glb_ready=task.fbx_glb_ready,
        fbx_glb_error=task.fbx_glb_error,
        progress_page=task.progress_page,
        worker_api=worker_api_for_response,
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

    available, all_files, data, error = await _fetch_worker_model_files(task)
    if not available:
        return {"available": False, "files": [], "error": error} if error else {"available": False, "files": []}

    return {
        "available": True,
        "exists": data.get("exists", False),
        "files": all_files,
        "totals": data.get("totals", {}),
    }


def _animal_variant_filename(guid: str, animal_type: str, orientation: str, kind: str, *, is_primary: bool) -> str:
    if is_primary:
        if kind == "blend":
            return f"{guid}_rigged.blend"
        if kind == "fbx":
            return f"{guid}_all_animations_unity.fbx"
        if kind == "skeleton":
            return f"{guid}_skeleton.json"
    suffix = f"{animal_type}_{orientation}"
    if kind == "blend":
        return f"{guid}_{suffix}_rigged.blend"
    if kind == "fbx":
        return f"{guid}_{suffix}_all_animations_unity.fbx"
    if kind == "skeleton":
        return f"{guid}_{suffix}_skeleton.json"
    raise ValueError(f"Unknown animal variant kind: {kind}")


def _animal_variant_candidate_filenames(guid: str, animal_type: str, orientation: str, kind: str, *, is_primary: bool) -> List[str]:
    primary = _animal_variant_filename(guid, animal_type, orientation, kind, is_primary=is_primary)
    if not is_primary:
        return [primary]
    if kind == "blend":
        return [primary, f"{guid}_model_prepared_rigged.blend"]
    if kind == "fbx":
        return [primary, f"{guid}_all_animations.fbx"]
    return [primary]


def _animal_variant_public_url(task_id: str, animal_type: str, orientation: str, kind: str, *, preview: bool = False) -> str:
    a = quote(animal_type)
    o = quote(orientation)
    if preview:
        return f"/api/task/{task_id}/animal-variants/{a}/{o}/preview.fbx"
    if kind == "skeleton":
        return f"/api/task/{task_id}/animal-variants/{a}/{o}/skeleton.json"
    return f"/api/task/{task_id}/animal-variants/{a}/{o}/download/{quote(kind)}"


def _animal_variant_file_state(
    item: Optional[Dict[str, Any]],
    task_id: str,
    animal_type: str,
    orientation: str,
    kind: str,
) -> AnimalVariantFileState:
    if not item:
        return AnimalVariantFileState(ready=False)
    return AnimalVariantFileState(
        ready=True,
        url=_animal_variant_public_url(task_id, animal_type, orientation, kind),
        size=item.get("size") if isinstance(item.get("size"), int) else None,
        filename=str(item.get("name") or "") or None,
    )


async def _fetch_animal_variant_matrix(task: Task, file_map: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    matrix_item = file_map.get("animal_variant_matrix.json")
    candidate_urls: List[str] = []
    if matrix_item and matrix_item.get("url"):
        candidate_urls.append(str(matrix_item["url"]))

    worker_root, guid = _infer_worker_root_and_guid(task)
    if worker_root and guid:
        candidate_urls.append(f"{worker_root}/{guid}/logs/animal_variant_matrix.json")

    for url in dict.fromkeys(candidate_urls):
        try:
            async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
                resp = await client.get(url)
            if resp.status_code != 200:
                continue
            payload = resp.json() if resp.content else {}
        except Exception:
            continue

        rows = payload.get("variants") if isinstance(payload, dict) else None
        if rows is None and isinstance(payload, list):
            rows = payload
        if not isinstance(rows, list):
            continue

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            animal = str(row.get("animal_slug") or row.get("animal_type") or row.get("animal") or "").strip().lower()
            orientation = str(row.get("orientation") or row.get("direction") or "").strip().lower()
            suffix = str(row.get("suffix") or "").strip().lower()
            if (not animal or not orientation) and suffix:
                for known in ANIMAL_VARIANT_TYPES:
                    if suffix.startswith(f"{known}_"):
                        animal = known
                        orientation = suffix.rsplit("_", 1)[-1]
                        break
            if animal in ANIMAL_VARIANT_TYPES and orientation in ANIMAL_VARIANT_ORIENTATIONS:
                out[f"{animal}:{orientation}"] = row
        return out
    return {}


def _animal_animation_id(animal_type: str, orientation: str, action_name: str) -> str:
    action_key = _normalize_animation_key(action_name)
    return f"animal_{animal_type}_{orientation}_{action_key}"


def _parse_animal_animation_id(animation_id: str) -> Optional[Tuple[str, str, str]]:
    anim_id = _normalize_animation_key(animation_id)
    if not anim_id.startswith("animal_"):
        return None
    for animal_type in ANIMAL_VARIANT_TYPES:
        for orientation in ANIMAL_VARIANT_ORIENTATIONS:
            prefix = f"animal_{animal_type}_{orientation}_"
            if anim_id.startswith(prefix):
                action_key = anim_id[len(prefix):].strip("_")
                if action_key:
                    return animal_type, orientation, action_key
    return None


def _clean_animal_action_name(value: Any) -> Optional[str]:
    name = str(value or "").strip()
    if not name:
        return None
    if name.endswith("__LD_STAGE4_TUNED"):
        return None
    return name[:128]


def _extract_animal_variant_actions(row: Dict[str, Any], animal_type: str) -> List[str]:
    candidates: List[Any] = []
    if isinstance(row, dict):
        stage4 = row.get("stage4_finalize")
        if isinstance(stage4, dict):
            for key in ("after_actions", "actions_after", "actions"):
                raw = stage4.get(key)
                if isinstance(raw, list):
                    candidates.extend(raw)
                    break
            if not candidates:
                raw_before = stage4.get("before_actions")
                if isinstance(raw_before, list):
                    candidates.extend(raw_before)
        for key in ("after_actions", "actions_after", "actions"):
            raw = row.get(key)
            if isinstance(raw, list):
                candidates.extend(raw)
                break

    actions: List[str] = []
    seen: Set[str] = set()
    for raw in candidates:
        clean = _clean_animal_action_name(raw)
        if not clean:
            continue
        key = _normalize_animation_key(clean)
        if not key or key in seen:
            continue
        seen.add(key)
        actions.append(clean)

    if not actions:
        label = animal_type.capitalize()
        actions.append(f"{label}_default")
    return actions


async def _has_animal_animation_pack_purchase(
    db: AsyncSession,
    user: Optional[User],
    task_id: str,
    animal_type: str,
    orientation: str,
) -> bool:
    if not user:
        return False
    if await _has_full_task_download_purchase(db, user, task_id):
        return True
    result = await db.execute(
        select(TaskAnimalAnimationPackPurchase).where(
            TaskAnimalAnimationPackPurchase.task_id == task_id,
            TaskAnimalAnimationPackPurchase.user_email == user.email,
            TaskAnimalAnimationPackPurchase.animal_type == animal_type,
            TaskAnimalAnimationPackPurchase.orientation == orientation,
        )
    )
    return result.scalar_one_or_none() is not None


async def _build_animal_animation_catalog_response(
    task: Task,
    db: AsyncSession,
    user: Optional[User],
    animal_type: Optional[str] = None,
    orientation: Optional[str] = None,
) -> AnimationCatalogResponse:
    selected_animal = _task_animal_type_from_settings(task)
    normalized_animal = str(animal_type or selected_animal or "").strip().lower()
    normalized_orientation = str(orientation or "front").strip().lower()
    if normalized_animal not in ANIMAL_VARIANT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid animal type")
    if normalized_orientation not in ANIMAL_VARIANT_ORIENTATIONS:
        raise HTTPException(status_code=400, detail="Invalid animal orientation")

    file_name: Optional[str] = None
    ready = False
    try:
        _source_url, file_name = await _resolve_animal_variant_source(
            task,
            normalized_animal,
            normalized_orientation,
            "fbx",
        )
        ready = True
    except HTTPException as exc:
        if exc.status_code not in (404,):
            raise

    available, files, _data, _error = await _fetch_worker_model_files(task)
    file_map: Dict[str, Dict[str, Any]] = {}
    if available:
        for item in files:
            name = str(item.get("name") or "").strip()
            if name:
                file_map[name.lower()] = item
    matrix = await _fetch_animal_variant_matrix(task, file_map)
    row = matrix.get(f"{normalized_animal}:{normalized_orientation}") or {}
    actions = _extract_animal_variant_actions(row, normalized_animal)
    pack_purchased = await _has_animal_animation_pack_purchase(
        db,
        user,
        task.id,
        normalized_animal,
        normalized_orientation,
    )

    items: List[AnimationCatalogItem] = []
    purchased_ids: List[str] = []
    for action_name in actions:
        item_id = _animal_animation_id(normalized_animal, normalized_orientation, action_name)
        if pack_purchased:
            purchased_ids.append(item_id)
        items.append(AnimationCatalogItem(
            id=item_id,
            name=action_name,
            type="animal",
            type_label="Animal actions",
            tags=["animal", normalized_animal, normalized_orientation],
            credits=TASK_UNLOCK_CREDITS,
            format="fbx",
            available=ready,
            ready=ready,
            purchased=pack_purchased,
            file_name=file_name,
            preview_url=f"/api/task/{task.id}/animations/preview/{quote(item_id)}" if ready else None,
            source_kind="animal_variant_pack",
            animal_type=normalized_animal,
            orientation=normalized_orientation,
            action_name=action_name,
            download_scope="variant_pack",
            pack_credits=TASK_UNLOCK_CREDITS,
            pack_purchased=pack_purchased,
        ))

    return AnimationCatalogResponse(
        types=[{"id": "animal", "label": "Animal actions", "count": len(items)}],
        animations=items,
        purchased_all=pack_purchased,
        purchased_ids=sorted(purchased_ids),
        login_required=(user is None),
        user_credits=(user.balance_credits if user else 0),
        pricing={
            "single_animation_credits": TASK_UNLOCK_CREDITS,
            "all_animations_credits": TASK_UNLOCK_CREDITS,
            "animal_animation_pack_credits": TASK_UNLOCK_CREDITS,
            "task_unlock_credits": TASK_UNLOCK_CREDITS,
            "purchase_scope": "task",
            "animal_animation_pack_purchased": pack_purchased,
            "download_format": "fbx",
            "download_scope": "variant_pack",
            "animal_type": normalized_animal,
            "orientation": normalized_orientation,
            "animal_animation_pack_download_url": (
                f"/api/task/{task.id}/animations/download-pack"
                f"?animal_type={quote(normalized_animal)}&orientation={quote(normalized_orientation)}"
            ),
        },
    )


async def _purchase_animal_animation_pack(
    task: Task,
    db: AsyncSession,
    user: User,
    animal_type: Optional[str],
    orientation: Optional[str],
) -> AnimationPurchaseResponse:
    selected_animal = _task_animal_type_from_settings(task)
    normalized_animal = str(animal_type or selected_animal or "").strip().lower()
    normalized_orientation = str(orientation or "front").strip().lower()
    if normalized_animal not in ANIMAL_VARIANT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid animal type")
    if normalized_orientation not in ANIMAL_VARIANT_ORIENTATIONS:
        raise HTTPException(status_code=400, detail="Invalid animal orientation")

    await _resolve_animal_variant_source(task, normalized_animal, normalized_orientation, "fbx")
    already = await _has_animal_animation_pack_purchase(
        db,
        user,
        task.id,
        normalized_animal,
        normalized_orientation,
    )
    if already:
        catalog = await _build_animal_animation_catalog_response(
            task,
            db,
            user,
            animal_type=normalized_animal,
            orientation=normalized_orientation,
        )
        return AnimationPurchaseResponse(
            success=True,
            purchased_animation_ids=sorted(catalog.purchased_ids),
            purchased_all=catalog.purchased_all,
            credits_remaining=user.balance_credits,
        )

    unlock = await _ensure_full_task_unlock(db, task, user)
    if unlock.get("status") == "insufficient_credits":
        raise HTTPException(status_code=402, detail="Insufficient credits")

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()

    catalog = await _build_animal_animation_catalog_response(
        task,
        db,
        user,
        animal_type=normalized_animal,
        orientation=normalized_orientation,
    )
    return AnimationPurchaseResponse(
        success=True,
        purchased_animation_ids=sorted(catalog.purchased_ids),
        purchased_all=catalog.purchased_all,
        credits_remaining=user.balance_credits,
    )


async def _fetch_animal_variant_progress_line(task: Task) -> Optional[str]:
    if not task.guid or not task.worker_api:
        return None
    from workers import get_worker_base_url
    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return None
    log_url = f"{worker_base.rstrip('/')}/converter/glb/{task.guid}/{task.guid}_progress.txt"
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            resp = await client.get(log_url)
        if resp.status_code != 200:
            return None
        lines = [ln.strip() for ln in resp.text.replace("\r\n", "\n").replace("\r", "\n").split("\n") if ln.strip()]
    except Exception:
        return None
    for line in reversed(lines):
        if "Animal rigging variants" in line:
            return line[:500]
    return None


async def _build_animal_variants_response(
    task: Task,
    db: AsyncSession,
    user: Optional[User],
) -> AnimalRigVariantsResponse:
    selected_animal = _task_animal_type_from_settings(task)
    if not _is_animal_task(task) or not task.guid or not selected_animal:
        return AnimalRigVariantsResponse(
            available=False,
            task_id=task.id,
            current_animal_type=selected_animal,
            selected_animal_type=selected_animal,
            login_required=not bool(user),
            all_files_credits=_download_all_files_cost(task),
        )

    available, files, _data, _error = await _fetch_worker_model_files(task)
    file_map: Dict[str, Dict[str, Any]] = {}
    if available:
        for item in files:
            name = str(item.get("name") or "").strip()
            if name:
                file_map[name.lower()] = item

    matrix = await _fetch_animal_variant_matrix(task, file_map) if file_map else {}
    progress_text = await _fetch_animal_variant_progress_line(task)
    purchased_all = await _has_full_task_download_purchase(db, user, task.id) if user else False

    variants: List[AnimalRigVariantItem] = []
    for animal_type in ANIMAL_VARIANT_TYPES:
        for orientation in ANIMAL_VARIANT_ORIENTATIONS:
            is_primary = animal_type == selected_animal and orientation == "front"
            blend_item = next((
                file_map.get(name.lower())
                for name in _animal_variant_candidate_filenames(task.guid, animal_type, orientation, "blend", is_primary=is_primary)
                if file_map.get(name.lower())
            ), None)
            fbx_item = next((
                file_map.get(name.lower())
                for name in _animal_variant_candidate_filenames(task.guid, animal_type, orientation, "fbx", is_primary=is_primary)
                if file_map.get(name.lower())
            ), None)
            skeleton_item = next((
                file_map.get(name.lower())
                for name in _animal_variant_candidate_filenames(task.guid, animal_type, orientation, "skeleton", is_primary=is_primary)
                if file_map.get(name.lower())
            ), None)

            row = matrix.get(f"{animal_type}:{orientation}") or {}
            matrix_status = str(row.get("status") or row.get("state") or "").strip().lower()
            error = str(row.get("error") or row.get("message") or "").strip() or None

            if blend_item and fbx_item:
                status = "ready"
            elif matrix_status in ("failed", "error", "skipped"):
                status = "skipped" if matrix_status == "skipped" else "failed"
            elif task.status == "done":
                status = "missing"
            else:
                status = "pending"

            variants.append(AnimalRigVariantItem(
                animal_type=animal_type,
                orientation=orientation,
                label=f"{animal_type} {orientation}",
                is_primary=is_primary,
                status=status,
                error=error,
                preview_url=_animal_variant_public_url(task.id, animal_type, orientation, "fbx", preview=True) if fbx_item else None,
                blend=_animal_variant_file_state(blend_item, task.id, animal_type, orientation, "blend"),
                fbx=_animal_variant_file_state(fbx_item, task.id, animal_type, orientation, "fbx"),
                skeleton=_animal_variant_file_state(skeleton_item, task.id, animal_type, orientation, "skeleton"),
            ))

    return AnimalRigVariantsResponse(
        available=True,
        task_id=task.id,
        current_animal_type=selected_animal,
        selected_animal_type=selected_animal,
        selected_orientation="front",
        progress_text=progress_text,
        purchased_all=purchased_all,
        login_required=not bool(user),
        all_files_credits=_download_all_files_cost(task),
        variants=variants,
    )


@app.get("/api/task/{task_id}/animal-variants", response_model=AnimalRigVariantsResponse)
async def api_task_animal_variants(
    task_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return await _build_animal_variants_response(task, db, user)


async def _resolve_animal_variant_source(
    task: Task,
    animal_type: str,
    orientation: str,
    kind: str,
) -> Tuple[str, str]:
    animal_type = str(animal_type or "").strip().lower()
    orientation = str(orientation or "").strip().lower()
    kind = str(kind or "").strip().lower()
    if not _is_animal_task(task):
        raise HTTPException(status_code=404, detail="Animal variants are available for animal rig tasks only")
    if animal_type not in ANIMAL_VARIANT_TYPES or orientation not in ANIMAL_VARIANT_ORIENTATIONS or kind not in ("blend", "fbx", "skeleton"):
        raise HTTPException(status_code=400, detail="Invalid animal variant request")
    if not task.guid:
        raise HTTPException(status_code=404, detail="Variant files are not available yet")

    selected_animal = _task_animal_type_from_settings(task)
    is_primary = animal_type == selected_animal and orientation == "front"
    filenames = _animal_variant_candidate_filenames(task.guid, animal_type, orientation, kind, is_primary=is_primary)

    available, files, _data, _error = await _fetch_worker_model_files(task)
    if available:
        for item in files:
            item_name = str(item.get("name") or "").strip().lower()
            for filename in filenames:
                if item_name == filename.lower() and item.get("url"):
                    return str(item["url"]), filename

    worker_root, guid = _infer_worker_root_and_guid(task)
    if worker_root and guid:
        for filename in filenames:
            url = f"{worker_root}/{guid}/{filename}"
            if await _remote_file_exists(url):
                return url, filename

    raise HTTPException(status_code=404, detail="Variant file not available yet")


@app.get("/api/task/{task_id}/animal-variants/{animal_type}/{orientation}/preview.fbx")
async def api_animal_variant_preview_fbx(
    task_id: str,
    animal_type: str,
    orientation: str,
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    source_url, filename = await _resolve_animal_variant_source(task, animal_type, orientation, "fbx")
    return await _proxy_model_file(source_url, filename, as_attachment=False)


@app.get("/api/task/{task_id}/animal-variants/{animal_type}/{orientation}/skeleton.json")
async def api_animal_variant_skeleton(
    task_id: str,
    animal_type: str,
    orientation: str,
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    source_url, filename = await _resolve_animal_variant_source(task, animal_type, orientation, "skeleton")
    return await _proxy_model_file(source_url, filename, as_attachment=False)


@app.get("/api/task/{task_id}/animal-variants/{animal_type}/{orientation}/download/{kind}")
async def api_animal_variant_download(
    task_id: str,
    animal_type: str,
    orientation: str,
    kind: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    kind = str(kind or "").strip().lower()
    if kind not in ("blend", "fbx"):
        raise HTTPException(status_code=400, detail="Unsupported variant download kind")
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required to download files")
    if not await _has_full_task_download_purchase(db, user, task_id):
        raise HTTPException(status_code=402, detail="Full download purchase required")
    source_url, filename = await _resolve_animal_variant_source(task, animal_type, orientation, kind)
    return await _proxy_model_file(source_url, filename, as_attachment=True)


async def _animal_animation_pack_download_response(
    task: Task,
    user: Optional[User],
    db: AsyncSession,
    animal_type: str,
    orientation: str,
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    animal_type = str(animal_type or "").strip().lower()
    orientation = str(orientation or "").strip().lower()
    if animal_type not in ANIMAL_VARIANT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid animal type")
    if orientation not in ANIMAL_VARIANT_ORIENTATIONS:
        raise HTTPException(status_code=400, detail="Invalid animal orientation")

    if not await _has_animal_animation_pack_purchase(db, user, task.id, animal_type, orientation):
        raise HTTPException(status_code=402, detail="Payment required to download this animal animation pack")

    source_url, filename = await _resolve_animal_variant_source(task, animal_type, orientation, "fbx")
    fbx_bytes = await _download_worker_file_bytes(
        source_url,
        "Animal animation pack FBX",
        max_bytes=250 * 1024 * 1024,
    )

    safe_variant = re.sub(r"[^a-zA-Z0-9_.-]+", "_", f"{animal_type}_{orientation}").strip("_")
    zip_path = Path(tempfile.gettempdir()) / f"autorig_{task.id}_{safe_variant}_animal_animations_{uuid.uuid4().hex}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, fbx_bytes)
        zf.writestr(
            "README.txt",
            (
                "This animal animation pack contains one rigged FBX file.\n\n"
                "The FBX includes the selected animal variant rig and all animation clips generated by AutoRig.online for this variant.\n"
                "Use the animation/action list inside your DCC or game engine to select the clip you need.\n"
                "Blend files and full task archives remain separate downloads.\n"
            ),
        )

    if task.ga_client_id:
        asyncio.create_task(send_ga4_event(
            task.ga_client_id,
            "animal_animation_pack_downloaded",
            {
                "task_id": task.id,
                "animal_type": animal_type,
                "orientation": orientation,
                "filename": filename,
            },
        ))

    bundle_name = f"{task.id}_{safe_variant}_animal_animations.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=bundle_name,
        headers={"Cache-Control": "private, max-age=0"},
        background=BackgroundTask(lambda: zip_path.unlink(missing_ok=True)),
    )


@app.get("/api/task/{task_id}/animations/download-pack")
async def api_download_animal_animation_pack(
    task_id: str,
    animal_type: str,
    orientation: str = "front",
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if not _is_animal_task(task):
        raise HTTPException(status_code=404, detail="Animal animation packs are available for animal rig tasks only")
    return await _animal_animation_pack_download_response(task, user, db, animal_type, orientation)


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

    restart_body_data: Dict[str, Any] = {}
    try:
        body = await request.body()
        if body:
            import json as json_module
            parsed_body = json_module.loads(body)
            if isinstance(parsed_body, dict):
                restart_body_data = parsed_body
    except Exception as e:
        print(f"[Restart] Could not parse body: {e}")

    animal_allowed = [x for x in RIG_V2_ALLOWED_ANIMAL_TYPES if x != "humanoid"]
    requested_rig_key = str(
        restart_body_data.get("rig_type")
        or restart_body_data.get("animal_type")
        or restart_body_data.get("input_type")
        or ""
    ).strip().lower()
    if requested_rig_key in ("human", "character", "humanoid", "t_pose", "t-pose"):
        task.input_type = "t_pose"
        restart_animal_type = None
        restart_worker_mode = None
    elif requested_rig_key in animal_allowed:
        task.input_type = "animal"
        restart_animal_type = requested_rig_key
        restart_worker_mode = str(restart_body_data.get("mode") or "only_rig").strip() or "only_rig"
    elif str(restart_body_data.get("input_type") or "").strip().lower() == "animal":
        restart_animal_type = str(restart_body_data.get("animal_type") or "").strip().lower()
        if restart_animal_type not in animal_allowed:
            raise HTTPException(status_code=400, detail="animal_type is required for animal rig restart")
        task.input_type = "animal"
        restart_worker_mode = str(restart_body_data.get("mode") or "only_rig").strip() or "only_rig"
    else:
        restart_animal_type = None
        restart_worker_mode = None

    if not restart_animal_type and str(task.input_type or "").strip().lower() == "animal":
        try:
            settings_for_animal = json.loads(task.viewer_settings or "{}")
            detection_for_animal = settings_for_animal.get("rig_v2_animal_detection") if isinstance(settings_for_animal, dict) else None
            if isinstance(detection_for_animal, dict):
                candidate = str(
                    detection_for_animal.get("animal_type")
                    or detection_for_animal.get("animal_type_string")
                    or detection_for_animal.get("candidate_animal_type_string")
                    or ""
                ).strip().lower()
                if candidate in animal_allowed:
                    restart_animal_type = candidate
                    restart_worker_mode = str(detection_for_animal.get("mode") or "only_rig").strip() or "only_rig"
        except Exception as e:
            print(f"[Restart] Could not read existing animal metadata: {e}")
        if not restart_animal_type:
            raise HTTPException(status_code=400, detail="animal_type is required for animal rig restart")

    if restart_body_data.get("rig_v2_manual_selection") or restart_animal_type:
        try:
            settings = json.loads(task.viewer_settings or "{}")
            if not isinstance(settings, dict):
                settings = {}
        except Exception:
            settings = {}
        if restart_animal_type:
            existing_detection = settings.get("rig_v2_animal_detection")
            if not isinstance(existing_detection, dict):
                existing_detection = {}
            settings["rig_v2_animal_detection"] = {
                **existing_detection,
                "type": "animal",
                "animal_type": restart_animal_type,
                "animal_type_string": restart_animal_type,
                "mode": restart_worker_mode or "only_rig",
                "source": "manual_task_restart",
                "accepted": True,
                "manual_selection": True,
                "user_selected_bool": True,
                "animal_decision_accepted_bool": True,
            }
        else:
            existing_detection = settings.get("rig_v2_animal_detection")
            if isinstance(existing_detection, dict):
                settings["rig_v2_animal_detection"] = {
                    **existing_detection,
                    "type": "humanoid",
                    "animal_type": "",
                    "animal_type_string": "",
                    "mode": "t_pose",
                    "source": "manual_task_restart",
                    "accepted": True,
                    "manual_selection": True,
                    "user_selected_bool": True,
                }
        task.viewer_settings = json.dumps(settings, ensure_ascii=False)

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
        if any(k in restart_body_data for k in ("local_position", "local_rotation", "local_scale")):
            transform_params = {
                "local_position": restart_body_data.get("local_position"),
                "local_rotation": restart_body_data.get("local_rotation"),
                "local_scale": restart_body_data.get("local_scale")
            }
            print(f"[Restart] Transform params from request: {transform_params}")

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
        animal_type=restart_animal_type,
        mode=restart_worker_mode,
        viewer_environment=(
            build_viewer_environment_from_settings(task.viewer_settings, app_url=APP_URL)
            if pk_restart == "rig"
            else None
        ),
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

    anon_id = (
        getattr(request.state, "api_key_anon_id", None)
        or request.cookies.get(ANON_COOKIE)
    )
    
    # Check if user is owner
    is_owner = bool(
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and anon_id and task.owner_id == anon_id)
    )
    
    # For non-owners, check purchases
    if not user:
        return PurchaseStateResponse(
            purchased_all=False,
            purchased_files=[],
            is_owner=False,
            login_required=True,
            user_credits=0,
            all_files_credits=_download_all_files_cost(task),
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
        user_credits=user.balance_credits,
        all_files_credits=_download_all_files_cost(task),
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

    anon_id = (
        getattr(request.state, "api_key_anon_id", None)
        or request.cookies.get(ANON_COOKIE)
    )
    
    # Check if user is owner (owners must still purchase to download)
    is_owner = bool(
        (user and task.owner_type == "user" and task.owner_id == user.email) or
        (task.owner_type == "anon" and anon_id and task.owner_id == anon_id)
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
    
    if not purchase_req.all and not purchase_req.file_indices:
        raise HTTPException(status_code=400, detail="Must specify file_indices or all=true")

    unlock = await _ensure_full_task_unlock(db, task, user)
    if unlock.get("status") == "insufficient_credits":
        raise HTTPException(status_code=402, detail="Insufficient credits")

    await db.commit()

    return PurchaseResponse(
        success=True,
        purchased_files=list(already_indices),
        purchased_all=True,
        credits_remaining=user.balance_credits
    )


@app.get("/api/task/{task_id}/animations/catalog", response_model=AnimationCatalogResponse)
async def api_get_animation_catalog(
    task_id: str,
    animal_type: Optional[str] = None,
    orientation: Optional[str] = None,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Return custom animation catalog + availability + purchase state for a task."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if _is_animal_task(task):
        return await _build_animal_animation_catalog_response(
            task,
            db,
            user,
            animal_type=animal_type,
            orientation=orientation,
        )

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
            credits=TASK_UNLOCK_CREDITS,
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
            "single_animation_credits": TASK_UNLOCK_CREDITS,
            "all_animations_credits": TASK_UNLOCK_CREDITS,
            "task_unlock_credits": TASK_UNLOCK_CREDITS,
            "purchase_scope": "task",
            "download_format": "fbx",
            "all_animations_fbx_url": f"/api/task/{task_id}/animations.fbx",
        }
    )


@app.post("/api/task/{task_id}/animations/purchase", response_model=AnimationPurchaseResponse)
async def api_purchase_animation(
    task_id: str,
    purchase_req: AnimationPurchaseRequest,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Legacy animation purchase endpoint; new purchases unlock the whole task."""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if _is_animal_task(task):
        parsed_animal = _parse_animal_animation_id(str(purchase_req.animation_id or ""))
        if parsed_animal:
            animal_type, orientation, _action_key = parsed_animal
        else:
            animal_type = purchase_req.animal_type
            orientation = purchase_req.orientation
        if purchase_req.all or parsed_animal or animal_type or orientation:
            return await _purchase_animal_animation_pack(
                task,
                db,
                user,
                animal_type=animal_type,
                orientation=orientation,
            )

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

        unlock = await _ensure_full_task_unlock(db, task, user)
        if unlock.get("status") == "insufficient_credits":
            raise HTTPException(status_code=402, detail="Insufficient credits")
        await db.commit()

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

    unlock = await _ensure_full_task_unlock(db, task, user)
    if unlock.get("status") == "insufficient_credits":
        raise HTTPException(status_code=402, detail="Insufficient credits")
    await db.commit()

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
    parsed_animal = _parse_animal_animation_id(anim_id)
    if parsed_animal and _is_animal_task(task):
        animal_type, orientation, _action_key = parsed_animal
        source_url, filename = await _resolve_animal_variant_source(task, animal_type, orientation, "fbx")
        return await _proxy_model_file(source_url, filename, as_attachment=False)

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
    parsed_animal = _parse_animal_animation_id(anim_id)
    if parsed_animal and _is_animal_task(task):
        animal_type, orientation, _action_key = parsed_animal
        return await _animal_animation_pack_download_response(task, user, db, animal_type, orientation)

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


@app.get("/api/task/{task_id}/animations/download-with-base/{animation_id}")
async def api_download_animation_with_base(
    task_id: str,
    animation_id: str,
    user: Optional[User] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Download a ZIP containing the package-level _all_animations FBX and the selected custom animation FBX.
    This avoids browser blocking of multiple automatic downloads from one click.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    anim_id = _normalize_animation_key(animation_id)
    parsed_animal = _parse_animal_animation_id(anim_id)
    if parsed_animal and _is_animal_task(task):
        animal_type, orientation, _action_key = parsed_animal
        return await _animal_animation_pack_download_response(task, user, db, animal_type, orientation)

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

    base_url, base_filename = _resolve_all_animations_fbx_url(task)
    if not base_url or not base_filename:
        raise HTTPException(status_code=404, detail="Animations FBX not available yet")

    custom_url = resolved["url"]
    custom_filename = resolved["clean_filename"]
    base_bytes, custom_bytes = await asyncio.gather(
        _download_worker_file_bytes(base_url, "_all_animations.fbx"),
        _download_worker_file_bytes(custom_url, "Custom animation FBX"),
    )

    safe_anim = re.sub(r"[^a-zA-Z0-9_.-]+", "_", anim_id).strip("_") or "custom_animation"
    zip_path = Path(tempfile.gettempdir()) / f"autorig_{task_id}_{safe_anim}_with_base_{uuid.uuid4().hex}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(base_filename, base_bytes)
        zf.writestr(custom_filename, custom_bytes)
        zf.writestr(
            "README.txt",
            (
                "This bundle contains two FBX files.\n\n"
                "1. Use the _all_animations FBX as the rigged character/base animation file.\n"
                "2. The custom animation FBX contains the selected skeleton animation clip only.\n"
                "3. Apply the selected custom clip to the rigged character in Unity, Unreal Engine, Blender, or another DCC/engine.\n"
            ),
        )

    if task.ga_client_id:
        asyncio.create_task(send_ga4_event(
            task.ga_client_id,
            "custom_animation_bundle_downloaded",
            {"animation_id": anim_id, "task_id": task_id, "filename": custom_filename}
        ))

    bundle_name = f"{task_id}_{safe_anim}_with_all_animations.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=bundle_name,
        headers={"Cache-Control": "private, max-age=0"},
        background=BackgroundTask(lambda: zip_path.unlink(missing_ok=True)),
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
                thumbnail_url=f"/thumb/{t.id}" if t.status == "done" and t.video_ready else None,
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
    rig_type: str = "all",
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

    sort = (sort or "date").strip().lower()
    if sort not in {"date", "likes", "sales"}:
        sort = "date"
    rig_filter = (rig_type or "all").strip().lower()
    if rig_filter not in GALLERY_RIG_TYPES:
        rig_filter = "all"
    should_filter_rig = rig_filter != "all"

    # Get task IDs with like counts
    offset = (page - 1) * per_page
    page_offset = 0 if should_filter_rig else offset
    page_limit = None if should_filter_rig else per_page

    if sort == "likes":
        # Sort by like count (descending), then by date
        query = (
            select(
                Task,
                func.count(TaskLike.id).label('like_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .where(*base_conditions)
            .group_by(func.coalesce(Task.input_url, Task.id))
            .order_by(desc('like_count'), desc(Task.created_at))
        )
    elif sort == "sales":
        # Sort by sales count (descending), then by date
        query = (
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
        )
    else:
        # Sort by date (newest first) - default
        query = (
            select(
                Task,
                func.count(TaskLike.id).label('like_count')
            )
            .outerjoin(TaskLike, Task.id == TaskLike.task_id)
            .where(*base_conditions)
            .group_by(func.coalesce(Task.input_url, Task.id))
            .order_by(desc(Task.created_at))
        )

    if page_offset:
        query = query.offset(page_offset)
    if page_limit is not None:
        query = query.limit(page_limit)
    
    result = await db.execute(query)
    rows = result.all()
    if should_filter_rig:
        rows = [row for row in rows if _gallery_rig_icon_key(row[0]) == rig_filter]
        total = len(rows)
        rows = rows[offset:offset + per_page]
    else:
        count_result = await db.execute(
            select(func.count(distinct(func.coalesce(Task.input_url, Task.id)))).where(*base_conditions)
        )
        total = count_result.scalar() or 0
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
    
    items = []
    for row in rows:
        t = row[0]
        like_count = row[1] if len(row) > 1 else 0
        items.append(GalleryItem(
            task_id=t.id,
            video_url=f"/api/video/{t.id}",
            thumbnail_url=f"/thumb/{t.id}",
            created_at=t.created_at,
            time_ago=format_time_ago(t.created_at),
            like_count=like_count,
            liked_by_me=t.id in user_likes,
            sales_count=sales_counts.get(t.id, 0),
            author_email=t.owner_id if t.owner_type == "user" else None,
            author_nickname=author_nicknames.get(t.owner_id) if t.owner_type == "user" else None,
            content_rating=getattr(t, "content_rating", None),
            rig_icon_key=_gallery_rig_icon_key(t),
        ))
    
    has_more = (page * per_page) < total
    
    return GalleryResponse(
        items=items,
        total=total,
        page=page,
        per_page=per_page,
        has_more=has_more,
        stats=await get_public_gallery_stats(db),
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
                "thumbnail_url": f"/thumb/{t.id}" if t.status == "done" else None,
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
        return f"/thumb/{t.id}"

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
        _poster = f"/thumb/{task.id}"

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
    Runs pressure cleanup until MIN_FREE_SPACE_GB is available.
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


def _dir_size_bytes(path: Path) -> int:
    """Sum file sizes under path (file or directory)."""
    if not path.exists():
        return 0
    total = 0
    try:
        if path.is_file():
            return path.stat().st_size
        for root, _dirs, files in os.walk(str(path)):
            for fn in files:
                fp = os.path.join(root, fn)
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
    except OSError:
        return 0
    return total


def _sqlite_db_path_and_bytes() -> Tuple[Optional[str], int]:
    """If DATABASE_URL is SQLite, return (path string, file size) or (None, 0)."""
    try:
        from sqlalchemy.engine.url import make_url

        u = make_url(DATABASE_URL)
    except Exception:
        return None, 0
    if not u.drivername.startswith("sqlite") or not u.database:
        return None, 0
    p = Path(u.database)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    if not p.is_file():
        return str(p), 0
    try:
        return str(p), p.stat().st_size
    except OSError:
        return str(p), 0


@app.get("/api/admin/disk-stats")
async def api_admin_disk_stats(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Get disk usage statistics (admin only).
    Includes per-directory size breakdown (GB) for main data categories.
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

    task_b = _dir_size_bytes(TASK_CACHE_DIR)
    task_cache_max_gb = await get_effective_task_cache_max_gb(db)
    glb_b = _dir_size_bytes(GLB_CACHE_DIR)
    upload_b = _dir_size_bytes(upload_dir)
    videos_b = _dir_size_bytes(videos_dir)
    static_total_b = _dir_size_bytes(STATIC_DIR)
    static_assets_b = max(0, static_total_b - task_b - glb_b)
    db_path, db_b = _sqlite_db_path_and_bytes()

    used_b = disk_usage.used
    tracked_b = static_total_b + upload_b + videos_b + db_b
    other_b = max(0, used_b - tracked_b)

    def _gb(x: int) -> float:
        return round(x / (1024**3), 2)

    breakdown_gb = {
        "task_cache": _gb(task_b),
        "glb_cache": _gb(glb_b),
        "static_assets": _gb(static_assets_b),
        "uploads": _gb(upload_b),
        "videos": _gb(videos_b),
        "database_sqlite": _gb(db_b) if db_b > 0 else None,
        "other_on_disk": _gb(other_b),
    }

    return {
        "ok": True,
        "disk": {
            "total_gb": round(disk_usage.total / (1024**3), 2),
            "used_gb": round(disk_usage.used / (1024**3), 2),
            "free_gb": round(disk_usage.free / (1024**3), 2),
            "percent_used": round(disk_usage.used / disk_usage.total * 100, 1),
        },
        "breakdown_gb": breakdown_gb,
        "database_path": db_path,
        "cleanable_items": {
            "task_cache": task_cache_count,
            "glb_cache": glb_cache_count,
            "uploads": upload_count,
            "videos": videos_count,
        },
        "task_cache_bytes": task_b,
        "task_cache_max_gb": round(float(task_cache_max_gb), 4),
        "task_cache_max_gb_default": round(float(TASK_CACHE_MAX_GB), 4),
        "settings": {
            "min_free_space_gb": round(float(MIN_FREE_SPACE_GB), 2),
            "new_task_min_free_gb": round(float(NEW_TASK_MIN_FREE_GB), 2),
            "new_task_purge_max_freed_gb": round(float(NEW_TASK_PURGE_TASKS_MAX_FREED_GB), 2),
            "cleanup_interval_cycles": CLEANUP_CHECK_INTERVAL_CYCLES,
            "min_age_hours": CLEANUP_MIN_AGE_HOURS,
            "upload_pressure_cleanup_min_age_hours": round(float(UPLOAD_PRESSURE_CLEANUP_MIN_AGE_HOURS), 2),
            "gallery_db_purge_interval_cycles": GALLERY_DB_PURGE_INTERVAL_CYCLES,
            "automatic_task_db_deletion": AUTOMATIC_TASK_DB_DELETION,
            "task_cache_max_gb": round(float(task_cache_max_gb), 4),
            "task_cache_max_gb_default": round(float(TASK_CACHE_MAX_GB), 4),
        },
    }


@app.patch("/api/admin/settings/task-cache-max")
async def api_admin_settings_task_cache_max(
    body: AdminTaskCacheMaxUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    row = await get_or_create_admin_overlay_counters(db)
    row.task_cache_max_gb = float(body.task_cache_max_gb)
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "task_cache_max_gb": float(row.task_cache_max_gb)}


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
                        viewer_environment=(
                            build_viewer_environment_from_settings(task.viewer_settings, app_url=APP_URL)
                            if pk_ad == "rig"
                            else None
                        ),
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
    
    source_video_url = await _resolve_task_video_source_url(task)
    if not source_video_url:
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
        req = client.build_request("GET", source_video_url, headers=upstream_headers)
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

    _source_lower = source_video_url.lower()
    if "_rig_preview.mp4" in _source_lower:
        _vname = f"{task_id}_rig_preview.mp4"
    elif "_video_small.mp4" in _source_lower:
        _vname = f"{task_id}_video_small.mp4"
    else:
        _vname = f"{task_id}_video.mp4"
    response_headers: Dict[str, str] = {
        "Content-Disposition": f'inline; filename="{_vname}"',
        "Cache-Control": "public, max-age=0, must-revalidate",
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
    task = await get_task_by_id(db, task_id)
    if task and _is_animal_download_task(task):
        return False
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


def _task_bundle_meta_cache_path(task_id: str) -> Path:
    return TASK_CACHE_DIR / task_id / ".meta" / "bundle.json"


def _bundle_meta_response(
    *,
    ready: bool,
    source: str,
    file_count: Optional[int] = None,
    total_size: Optional[int] = None,
    generated_at: Optional[str] = None,
    converter_version: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "bundle_file_count": file_count if ready else None,
        "bundle_file_count_ready": bool(ready),
        "bundle_file_count_source": source,
        "bundle_total_size": total_size,
        "bundle_meta_generated_at": generated_at,
        "bundle_converter_version": converter_version,
    }


def _normalize_bundle_meta(raw: Any, *, source: str, worker_zip_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    try:
        file_count = int(raw.get("bundle_file_count", raw.get("file_count", 0)) or 0)
    except Exception:
        file_count = 0
    if file_count <= 0:
        return None

    total_size_raw = raw.get("bundle_total_size", raw.get("zip_size"))
    try:
        total_size = int(total_size_raw) if total_size_raw is not None else None
    except Exception:
        total_size = None

    meta_source = str(raw.get("bundle_file_count_source") or raw.get("source") or source or "unknown")
    return _bundle_meta_response(
        ready=True,
        source=meta_source,
        file_count=file_count,
        total_size=total_size,
        generated_at=raw.get("bundle_meta_generated_at") or raw.get("generated_at"),
        converter_version=raw.get("bundle_converter_version") or raw.get("converter_version"),
    )


def _read_cached_task_bundle_meta(task_id: str, worker_zip_url: Optional[str]) -> Optional[Dict[str, Any]]:
    path = _task_bundle_meta_cache_path(task_id)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    cached_zip_url = raw.get("bundle_worker_zip_url") or raw.get("worker_zip_url")
    if cached_zip_url and worker_zip_url and cached_zip_url != worker_zip_url:
        return None
    return _normalize_bundle_meta(raw, source="metadata_cache", worker_zip_url=worker_zip_url)


def _write_cached_task_bundle_meta(task_id: str, meta: Dict[str, Any]) -> None:
    try:
        path = _task_bundle_meta_cache_path(task_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    except Exception as e:
        print(f"[BundleMeta] Failed to write metadata cache for task {task_id}: {e}")


async def _worker_bundle_zip_available(zip_url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
            async with client.stream("GET", zip_url, headers={"Range": "bytes=0-0"}) as response:
                return response.status_code in (200, 206)
    except Exception:
        return False


async def _load_task_bundle_meta(
    task: Task,
    *,
    fallback_file_count: int = 0,
    fallback_total_size: int = 0,
) -> Dict[str, Any]:
    zip_url = resolve_worker_full_bundle_zip_url(task)
    cached_meta = _read_cached_task_bundle_meta(task.id, zip_url)
    if cached_meta and cached_meta.get("bundle_file_count_source") != "fallback_cache":
        return cached_meta

    if zip_url:
        meta_url = f"{zip_url}.meta.json"
        try:
            async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
                response = await client.get(meta_url, headers={"Accept": "application/json"})
            if response.status_code == 200:
                worker_meta = _normalize_bundle_meta(
                    response.json(),
                    source="worker_meta",
                    worker_zip_url=zip_url,
                )
                if worker_meta:
                    worker_meta["bundle_file_count_source"] = "worker_meta"
                    worker_meta_cache = dict(worker_meta)
                    worker_meta_cache["worker_zip_url"] = zip_url
                    _write_cached_task_bundle_meta(task.id, worker_meta_cache)
                    return worker_meta
        except Exception as e:
            print(f"[BundleMeta] Worker metadata unavailable for task {task.id}: {e}")

        if await _worker_bundle_zip_available(zip_url):
            return _bundle_meta_response(
                ready=False,
                source="worker_meta_missing",
            )

    if cached_meta and cached_meta.get("bundle_file_count_source") == "fallback_cache":
        return cached_meta

    if fallback_file_count > 0:
        return _bundle_meta_response(
            ready=True,
            source="fallback_cache",
            file_count=int(fallback_file_count),
            total_size=int(fallback_total_size or 0),
        )

    return _bundle_meta_response(
        ready=False,
        source="unknown" if task.status == "done" else "task_not_done",
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
            bundle_meta = await _load_task_bundle_meta(
                task,
                fallback_file_count=len(files),
                fallback_total_size=total_size,
            )
            return {
                "cached": True,
                "task_id": task_id,
                "files": files,
                "total_size": total_size,
                "file_count": len(files),
                **bundle_meta,
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
        total_size = sum(f["size"] for f in result["files"])
        bundle_meta = await _load_task_bundle_meta(
            task,
            fallback_file_count=len(result["files"]),
            fallback_total_size=total_size,
        )
        return {
            "cached": result["cached"],
            "task_id": task_id,
            "files": result["files"],
            "total_size": total_size,
            "file_count": len(result["files"]),
            "errors": result.get("errors", []),
            **bundle_meta,
        }
    
    # Task not ready yet
    bundle_meta = (
        await _load_task_bundle_meta(task)
        if task.status == "done"
        else _bundle_meta_response(ready=False, source="task_not_done")
    )
    return {
        "cached": False,
        "task_id": task_id,
        "files": [],
        "total_size": 0,
        "file_count": 0,
        "message": "Task not completed yet" if task.status != "done" else "No files to cache",
        **bundle_meta,
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


async def _build_task_bundle_zip_from_cache(task: Task) -> FileResponse:
    urls_to_cache = list(dict.fromkeys((task.ready_urls or []) + (task.output_urls or [])))
    cache_dir = TASK_CACHE_DIR / task.id
    if urls_to_cache:
        await cache_task_files(task.id, urls_to_cache, task.guid)

    files = [
        p for p in sorted(cache_dir.iterdir())
        if p.is_file() and not p.name.endswith(".tmp") and not p.name.startswith(".")
    ] if cache_dir.exists() else []
    if not files:
        raise HTTPException(status_code=404, detail="No downloadable task files are available")

    safe_guid = (task.guid or task.id).strip()
    zip_path = Path(tempfile.gettempdir()) / f"autorig_{task.id}_{uuid.uuid4().hex}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in files:
            zf.write(file_path, arcname=file_path.name)

    fallback_meta = _bundle_meta_response(
        ready=True,
        source="fallback_cache",
        file_count=len(files),
        total_size=zip_path.stat().st_size if zip_path.exists() else None,
        generated_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
    )
    fallback_meta_cache = dict(fallback_meta)
    fallback_meta_cache["worker_zip_url"] = resolve_worker_full_bundle_zip_url(task)
    _write_cached_task_bundle_meta(task.id, fallback_meta_cache)

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{safe_guid}.zip",
        headers={"Cache-Control": "private, max-age=0"},
        background=BackgroundTask(lambda: zip_path.unlink(missing_ok=True)),
    )


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
    try:
        return await _proxy_model_file(zip_url, filename, as_attachment=True)
    except HTTPException as exc:
        print(f"[Bundle] Worker ZIP unavailable for task {task_id}: {exc.detail}; building fallback")
        return await _build_task_bundle_zip_from_cache(task)


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


BLUEPRINT_SKELETON_SUFFIX = "_skeleton.json"
BLUEPRINT_RIG_PREVIEW_SUFFIX = "_rig_preview.mp4"


def _is_animal_task(task: Task) -> bool:
    return str(getattr(task, "input_type", "") or "").strip().lower() == "animal"


def _find_cached_blueprint_file(task_id: str, guid: Optional[str], suffix: str) -> Optional[Path]:
    cache_dir = TASK_CACHE_DIR / task_id
    if not cache_dir.exists():
        return None
    if suffix == BLUEPRINT_SKELETON_SUFFIX:
        names = ["skeleton.json"]
    elif suffix == BLUEPRINT_RIG_PREVIEW_SUFFIX:
        names = ["rig_preview.mp4"]
    else:
        names = []
    if guid:
        names.append(f"{guid}{suffix}")
    for name in names:
        path = cache_dir / name
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            return path
    matches = sorted(cache_dir.glob(f"*{suffix}"))
    for path in matches:
        if path.is_file() and path.stat().st_size > 0:
            return path
    return None


async def _remote_file_exists(url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True) as client:
            resp = await client.head(url)
            if 200 <= resp.status_code < 400:
                return True
            if resp.status_code not in (405, 501):
                return False
            resp = await client.get(url, headers={"Range": "bytes=0-0"})
            return resp.status_code in (200, 206)
    except Exception:
        return False


async def _resolve_task_worker_file_url(task: Task, suffix: str) -> Optional[str]:
    urls = list(task.ready_urls or []) + list(task.output_urls or [])
    existing = _find_file_in_ready_urls(urls, suffix)
    if existing:
        return existing
    if not task.guid or not task.worker_api:
        return None

    from workers import get_worker_base_url

    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return None
    worker_base = worker_base.rstrip("/")
    worker_root = f"{worker_base}/converter/glb"

    direct_url = f"{worker_root}/{task.guid}/{task.guid}{suffix}"
    if await _remote_file_exists(direct_url):
        return direct_url

    files_url = f"{worker_base}/api-converter-glb/model-files/{task.guid}"
    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
            resp = await client.get(files_url)
        if resp.status_code != 200:
            return None
        data = resp.json() if resp.content else {}
    except Exception:
        return None

    for folder_data in (data.get("folders") or {}).values():
        if not isinstance(folder_data, dict):
            continue
        for item in folder_data.get("files") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            rel_path = str(item.get("rel_path") or "")
            if rel_path and name.lower().endswith(suffix):
                return f"{worker_root}/{task.guid}/{rel_path}"
    return None


async def _resolve_task_blueprint_urls(task: Task) -> Tuple[Optional[str], Optional[str]]:
    if not _is_animal_task(task):
        return None, None

    skeleton_cached = _find_cached_blueprint_file(task.id, task.guid, BLUEPRINT_SKELETON_SUFFIX)
    rig_preview_cached = _find_cached_blueprint_file(task.id, task.guid, BLUEPRINT_RIG_PREVIEW_SUFFIX)

    skeleton_url = (
        f"/api/task/{task.id}/blueprint/skeleton.json"
        if skeleton_cached or await _resolve_task_worker_file_url(task, BLUEPRINT_SKELETON_SUFFIX)
        else None
    )
    rig_preview_url = (
        f"/api/task/{task.id}/blueprint/rig-preview.mp4"
        if rig_preview_cached or await _resolve_task_worker_file_url(task, BLUEPRINT_RIG_PREVIEW_SUFFIX)
        else None
    )
    return skeleton_url, rig_preview_url


async def _resolve_task_video_source_url(task: Task) -> Optional[str]:
    """Return the preferred upstream video source for public playback/upload flows."""
    if _is_animal_task(task):
        rig_preview_url = await _resolve_task_worker_file_url(task, BLUEPRINT_RIG_PREVIEW_SUFFIX)
        if rig_preview_url:
            return rig_preview_url
    return (task.video_url or "").strip() or None


async def _resolve_task_blueprint_model_url(task: Task) -> Optional[str]:
    urls = list(task.ready_urls or []) + list(task.output_urls or [])
    for pattern in ("_model_prepared.glb", "_model_prepared_temp.glb"):
        existing = _find_file_in_ready_urls(urls, pattern, ".glb")
        if existing:
            return existing
    if not task.guid or not task.worker_api:
        return None

    from workers import get_worker_base_url

    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return None
    worker_base = worker_base.rstrip("/")
    worker_root = f"{worker_base}/converter/glb"
    for filename in (
        f"{task.guid}_model_prepared.glb",
        f"{task.guid}_model_prepared_temp.glb",
        f"{task.guid}.glb",
    ):
        direct_url = f"{worker_root}/{task.guid}/{filename}"
        if await _remote_file_exists(direct_url):
            return direct_url

    files_url = f"{worker_base}/api-converter-glb/model-files/{task.guid}"
    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
            resp = await client.get(files_url)
        if resp.status_code != 200:
            return None
        data = resp.json() if resp.content else {}
    except Exception:
        return None

    priority = (
        f"{task.guid}_model_prepared.glb",
        f"{task.guid}_model_prepared_temp.glb",
        f"{task.guid}.glb",
    )
    candidates: Dict[str, str] = {}
    for folder_data in (data.get("folders") or {}).values():
        if not isinstance(folder_data, dict):
            continue
        for item in folder_data.get("files") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            rel_path = str(item.get("rel_path") or "")
            if rel_path and name in priority:
                candidates[name] = f"{worker_root}/{task.guid}/{rel_path}"
    for name in priority:
        if name in candidates:
            return candidates[name]
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


GALLERY_RIG_TYPES = (
    "humanoid",
    "dog",
    "bear",
    "cat",
    "cow",
    "deer",
    "elephant",
    "giraffe",
    "horse",
    "mouse",
    "pig",
    "rabbit",
    "turtle",
)


def _gallery_rig_icon_key(task: Task) -> str:
    """Humanoid vs animal rig key for gallery UI icons and filters."""
    input_type = str(getattr(task, "input_type", "") or "").strip().lower()
    if input_type != "animal":
        return "humanoid"
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except Exception:
        settings = {}
    det = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None
    if not isinstance(det, dict):
        return "humanoid"
    animal = str(
        det.get("animal_type")
        or det.get("animal_type_string")
        or ""
    ).strip().lower()
    return animal if animal in GALLERY_RIG_TYPES and animal != "humanoid" else "humanoid"


RIG_ARTICLE_LANGS: Dict[str, Dict[str, str]] = {
    "en": {
        "suffix": "",
        "html_lang": "en",
        "guides": "Guides",
        "kicker": "Rig type SEO guide",
        "overview": "What this rig type is for",
        "workflow": "Online workflow",
        "prep": "Model preparation tips",
        "examples": "Real AutoRig examples",
        "faq": "FAQ",
        "cta_upload": "Start auto rigging online",
        "cta_gallery": "Browse real examples",
        "empty": "Real public examples for this rig type will appear here automatically after matching completed tasks are published in the gallery.",
        "open_example": "Open rig preview",
        "examples_intro": "These examples are pulled from completed public AutoRig tasks and refreshed from the live gallery data.",
    },
    "ru": {
        "suffix": "-ru",
        "html_lang": "ru",
        "guides": "Руководства",
        "kicker": "SEO-статья по типу рига",
        "overview": "Для чего нужен этот тип рига",
        "workflow": "Онлайн workflow",
        "prep": "Как подготовить модель",
        "examples": "Реальные примеры AutoRig",
        "faq": "FAQ",
        "cta_upload": "Запустить auto rigging online",
        "cta_gallery": "Смотреть реальные примеры",
        "empty": "Реальные публичные примеры для этого типа появятся здесь автоматически после публикации подходящих задач в галерее.",
        "open_example": "Открыть preview рига",
        "examples_intro": "Эти примеры подтягиваются из завершённых публичных задач AutoRig и обновляются по live gallery data.",
    },
    "zh": {
        "suffix": "-zh",
        "html_lang": "zh",
        "guides": "指南",
        "kicker": "绑定类型 SEO 指南",
        "overview": "这个绑定类型适合什么",
        "workflow": "在线工作流",
        "prep": "模型准备建议",
        "examples": "真实 AutoRig 示例",
        "faq": "FAQ",
        "cta_upload": "开始在线自动绑定",
        "cta_gallery": "浏览真实示例",
        "empty": "当图库中出现匹配的已完成公开任务后，此处会自动显示该绑定类型的真实示例。",
        "open_example": "打开绑定预览",
        "examples_intro": "这些示例来自已完成的公开 AutoRig 任务，并从实时图库数据刷新。",
    },
    "hi": {
        "suffix": "-hi",
        "html_lang": "hi",
        "guides": "Guides",
        "kicker": "Rig type SEO guide",
        "overview": "यह rig type किसके लिए है",
        "workflow": "Online workflow",
        "prep": "Model preparation tips",
        "examples": "Real AutoRig examples",
        "faq": "FAQ",
        "cta_upload": "Online auto rigging शुरू करें",
        "cta_gallery": "Real examples देखें",
        "empty": "इस rig type के real public examples matching completed tasks gallery में publish होते ही यहां automatically दिखेंगे।",
        "open_example": "Rig preview खोलें",
        "examples_intro": "ये examples completed public AutoRig tasks से आते हैं और live gallery data से refresh होते हैं।",
    },
}

# Generated rig-type pages are currently indexed in English first. The localized
# variants stay routable, but are not advertised in sitemap/hreflang until their
# copy is fully reviewed.
RIG_ARTICLE_INDEXED_LANGS: Tuple[str, ...] = ("en",)

RIG_ARTICLE_EXAMPLE_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "dog": ("dog", "dogs", "wolf", "wolves", "canine", "puppy", "fox"),
    "bear": ("bear", "bears", "cub", "grizzly", "panda"),
    "cat": ("cat", "cats", "kitten", "feline", "panther", "tiger", "lion"),
    "cow": ("cow", "cows", "bull", "cattle", "calf", "ox"),
    "deer": ("deer", "stag", "elk", "antler", "antlers", "doe"),
    "elephant": ("elephant", "elephants", "mammoth", "trunk"),
    "giraffe": ("giraffe", "giraffes", "long neck", "long-necked"),
    "horse": ("horse", "horses", "pony", "ponies", "equine", "unicorn", "mount"),
    "mouse": ("mouse", "mice", "rat", "rats", "rodent", "hamster"),
    "pig": ("pig", "pigs", "piglet", "boar", "hog", "swine"),
    "rabbit": ("rabbit", "rabbits", "bunny", "bunnies", "hare"),
    "turtle": ("turtle", "turtles", "tortoise", "tortoises", "shell", "reptile"),
}

RIG_ARTICLE_STATIC_EXAMPLES: Dict[str, Dict[str, str]] = {
    "cat": {
        "title": "Cat V2 animal rig animation example",
        "image": "/static/videos/animal-rig/cat-v2-rig-poster-20260516.png",
        "url": "/animal-rig#examples",
    },
    "horse": {
        "title": "Quadruped animal rig animation example",
        "image": "/static/videos/animal-rig/alpaca-v2-rig-poster-20260516.png",
        "url": "/animal-rig#examples",
    },
    "rabbit": {
        "title": "Rabbit V2 animal rig animation example",
        "image": "/static/videos/animal-rig/rabbit-v2-rig-poster-20260516.png",
        "url": "/animal-rig#examples",
    },
    "turtle": {
        "title": "Turtle V2 low-body rig animation example",
        "image": "/static/videos/animal-rig/turtle-v2-rig-poster-20260516.png",
        "url": "/animal-rig#examples",
    },
}

DEFAULT_RIG_ARTICLE_STATIC_EXAMPLE: Dict[str, str] = {
    "title": "AutoRig V2 animal rig presentation",
    "image": "/static/videos/animal-rig/after-poster-20260516.png",
    "url": "/animal-rig#presentation",
}


RIG_ARTICLE_TYPES: Dict[str, Dict[str, Any]] = {
    "humanoid": {
        "labels": {"en": "humanoid character", "ru": "humanoid-персонаж", "zh": "人形角色", "hi": "humanoid character"},
        "shape": {"en": "biped characters with arms, legs, hands, head, and animation-ready proportions", "ru": "двуногие персонажи с руками, ногами, кистями, головой и animation-ready пропорциями", "zh": "带有手臂、腿部、手、头部和动画比例的人形角色", "hi": "arms, legs, hands, head और animation-ready proportions वाले biped characters"},
        "use": {"en": "game heroes, NPCs, stylized people, robots, and human-like creatures", "ru": "игровых героев, NPC, stylized people, роботов и human-like creatures", "zh": "游戏主角、NPC、风格化人物、机器人和类人生物", "hi": "game heroes, NPCs, stylized people, robots और human-like creatures"},
        "tip": {"en": "Use a clear T-pose or relaxed A-pose, keep arms away from the torso, and avoid fused fingers if hand motion matters.", "ru": "Используйте чистую T-pose или relaxed A-pose, держите руки отдельно от тела и не склеивайте пальцы, если важна анимация кистей.", "zh": "使用清晰的 T-pose 或放松的 A-pose，让手臂离开身体；如果需要手部动画，避免手指粘连。", "hi": "Clear T-pose या relaxed A-pose रखें, arms को torso से अलग रखें, और hand motion चाहिए तो fused fingers से बचें।"},
    },
    "dog": {
        "labels": {"en": "dog and wolf", "ru": "собака и wolf", "zh": "狗和狼", "hi": "dog और wolf"},
        "shape": {"en": "quadruped bodies with paws, tail motion, shoulder/hip deformation, and low head posture", "ru": "quadruped body с лапами, tail motion, shoulder/hip deformation и низким положением головы", "zh": "具有爪子、尾巴运动、肩/髋变形和较低头部姿态的四足身体", "hi": "paws, tail motion, shoulder/hip deformation और low head posture वाले quadruped bodies"},
        "use": {"en": "dogs, wolves, stylized pets, fantasy companions, and canine game creatures", "ru": "собак, волков, stylized pets, fantasy companions и canine game creatures", "zh": "狗、狼、风格化宠物、奇幻伙伴和犬科游戏生物", "hi": "dogs, wolves, stylized pets, fantasy companions और canine game creatures"},
        "tip": {"en": "Keep legs separated, make paws visible, and leave enough mesh loops around shoulders, hips, neck, and tail.", "ru": "Разведите лапы, сделайте paws читаемыми и оставьте достаточно mesh loops у плеч, таза, шеи и хвоста.", "zh": "让四肢分开、爪子清晰，并在肩部、髋部、颈部和尾巴周围保留足够网格环。", "hi": "Legs अलग रखें, paws visible रखें, और shoulders, hips, neck तथा tail के आसपास पर्याप्त mesh loops रखें।"},
    },
    "bear": {
        "labels": {"en": "bear", "ru": "медведь", "zh": "熊", "hi": "bear"},
        "shape": {"en": "heavy quadrupeds with broad shoulders, thick paws, short tail, and powerful torso motion", "ru": "тяжёлые quadrupeds с широкими плечами, мощными лапами, коротким хвостом и массивным корпусом", "zh": "宽肩、厚爪、短尾和强壮躯干运动的大型四足动物", "hi": "broad shoulders, thick paws, short tail और powerful torso motion वाले heavy quadrupeds"},
        "use": {"en": "bears, stylized cubs, fantasy beasts, and bulky animal characters", "ru": "медведей, stylized cubs, fantasy beasts и bulky animal characters", "zh": "熊、风格化幼熊、奇幻野兽和大型动物角色", "hi": "bears, stylized cubs, fantasy beasts और bulky animal characters"},
        "tip": {"en": "Avoid merged front legs and give the shoulder area enough geometry for weight transfer during walking animations.", "ru": "Не склеивайте передние лапы и добавьте геометрию в зоне плеч для корректного переноса веса при walk animations.", "zh": "避免前腿粘连，并为肩部区域提供足够几何，以支持行走动画中的重心转移。", "hi": "Front legs merge न करें और walking animations में weight transfer के लिए shoulder area में enough geometry रखें।"},
    },
    "cat": {
        "labels": {"en": "cat", "ru": "кошка", "zh": "猫", "hi": "cat"},
        "shape": {"en": "flexible quadrupeds with arched backs, small paws, long tails, and agile spine motion", "ru": "гибкие quadrupeds с арочной спиной, маленькими лапами, длинным хвостом и agile spine motion", "zh": "具有弓背、小爪、长尾和灵活脊柱运动的四足动物", "hi": "arched backs, small paws, long tails और agile spine motion वाले flexible quadrupeds"},
        "use": {"en": "cats, kittens, cartoon pets, feline companions, and stylized fantasy animals", "ru": "кошек, kittens, cartoon pets, feline companions и stylized fantasy animals", "zh": "猫、小猫、卡通宠物、猫科伙伴和风格化奇幻动物", "hi": "cats, kittens, cartoon pets, feline companions और stylized fantasy animals"},
        "tip": {"en": "Keep the tail detached from the body silhouette and make the spine topology clean enough for curved poses.", "ru": "Держите хвост отдельным от силуэта тела и сделайте topology спины достаточно чистой для curved poses.", "zh": "让尾巴在轮廓上与身体分离，并保持脊柱拓扑干净，以支持弯曲姿态。", "hi": "Tail को body silhouette से अलग रखें और curved poses के लिए spine topology clean रखें।"},
    },
    "cow": {
        "labels": {"en": "cow", "ru": "корова", "zh": "牛", "hi": "cow"},
        "shape": {"en": "large farm quadrupeds with hooves, broad torso, neck motion, and optional horns or udder details", "ru": "крупные farm quadrupeds с копытами, широким корпусом, neck motion и optional horns/udder details", "zh": "带蹄、宽大躯干、颈部运动以及可选角/乳房细节的大型农场四足动物", "hi": "hooves, broad torso, neck motion और optional horns/udder details वाले large farm quadrupeds"},
        "use": {"en": "cows, bulls, calves, farm animals, and stylized livestock for games or animation", "ru": "коров, быков, телят, farm animals и stylized livestock для игр или animation", "zh": "奶牛、公牛、小牛、农场动物和游戏/动画中的风格化家畜", "hi": "cows, bulls, calves, farm animals और games या animation के stylized livestock"},
        "tip": {"en": "Separate hooves clearly, avoid a single fused underside mesh, and keep horns as readable geometry if they should move with the head.", "ru": "Чётко разделите копыта, не делайте слитную нижнюю часть mesh и оставьте horns читаемой геометрией, если они должны двигаться с головой.", "zh": "清晰分离蹄子，避免底部网格完全粘连；如果角要随头部移动，应保持角的几何清晰。", "hi": "Hooves clearly separate रखें, underside mesh fused न रखें, और horns को readable geometry रखें यदि वे head के साथ move करने चाहिए।"},
    },
    "deer": {
        "labels": {"en": "deer", "ru": "олень", "zh": "鹿", "hi": "deer"},
        "shape": {"en": "slender quadrupeds with thin legs, light torso, neck motion, and optional antlers", "ru": "стройные quadrupeds с тонкими ногами, лёгким корпусом, neck motion и optional antlers", "zh": "细腿、轻盈躯干、颈部运动和可选鹿角的纤细四足动物", "hi": "thin legs, light torso, neck motion और optional antlers वाले slender quadrupeds"},
        "use": {"en": "deer, elk-like creatures, forest animals, fantasy mounts, and elegant wildlife models", "ru": "оленей, elk-like creatures, forest animals, fantasy mounts и elegant wildlife models", "zh": "鹿、麋鹿类生物、森林动物、奇幻坐骑和优雅野生动物模型", "hi": "deer, elk-like creatures, forest animals, fantasy mounts और elegant wildlife models"},
        "tip": {"en": "Give thin legs enough thickness for skin weights and keep antlers separate enough to avoid confusing them with ears.", "ru": "Сделайте тонкие ноги достаточно толстыми для skin weights и отделите antlers от ушей, чтобы модель читалась правильно.", "zh": "让细腿有足够厚度以便蒙皮权重稳定，并让鹿角与耳朵足够分离。", "hi": "Thin legs में skin weights के लिए enough thickness रखें और antlers को ears से clear separation दें।"},
    },
    "elephant": {
        "labels": {"en": "elephant", "ru": "слон", "zh": "大象", "hi": "elephant"},
        "shape": {"en": "large quadrupeds with heavy legs, big ears, trunk silhouette, and slow weight-shift motion", "ru": "крупные quadrupeds с массивными ногами, большими ушами, trunk silhouette и slow weight-shift motion", "zh": "具有粗壮腿、大耳朵、象鼻轮廓和缓慢重心移动的大型四足动物", "hi": "heavy legs, big ears, trunk silhouette और slow weight-shift motion वाले large quadrupeds"},
        "use": {"en": "elephants, mammoths, fantasy giants, stylized zoo animals, and large creature rigs", "ru": "слонов, мамонтов, fantasy giants, stylized zoo animals и large creature rigs", "zh": "大象、猛犸、奇幻巨兽、风格化动物园动物和大型生物绑定", "hi": "elephants, mammoths, fantasy giants, stylized zoo animals और large creature rigs"},
        "tip": {"en": "Keep the trunk visible and separated from the legs, and avoid very thin ear geometry that cannot deform cleanly.", "ru": "Держите trunk видимым и отделённым от ног, избегайте слишком тонкой геометрии ушей, которая плохо деформируется.", "zh": "保持象鼻清晰并与腿部分离，避免耳朵几何过薄导致变形不干净。", "hi": "Trunk को visible और legs से separate रखें, और बहुत thin ear geometry से बचें जो clean deform नहीं कर सके।"},
    },
    "giraffe": {
        "labels": {"en": "giraffe", "ru": "жираф", "zh": "长颈鹿", "hi": "giraffe"},
        "shape": {"en": "tall quadrupeds with long necks, long legs, small horns, and high center-of-mass motion", "ru": "высокие quadrupeds с длинной шеей, длинными ногами, маленькими horns и высоким center-of-mass motion", "zh": "长颈、长腿、小角和高重心运动的高大四足动物", "hi": "long necks, long legs, small horns और high center-of-mass motion वाले tall quadrupeds"},
        "use": {"en": "giraffes, tall fantasy herbivores, stylized safari animals, and long-necked creatures", "ru": "жирафов, tall fantasy herbivores, stylized safari animals и long-necked creatures", "zh": "长颈鹿、高大奇幻食草动物、风格化 safari 动物和长颈生物", "hi": "giraffes, tall fantasy herbivores, stylized safari animals और long-necked creatures"},
        "tip": {"en": "Keep the neck segmented with enough topology and avoid merging thin legs into the body silhouette.", "ru": "Добавьте достаточно topology по шее и не сливайте тонкие ноги с body silhouette.", "zh": "颈部需要足够分段拓扑，并避免细腿与身体轮廓粘连。", "hi": "Neck में enough segmented topology रखें और thin legs को body silhouette में merge न करें।"},
    },
    "horse": {
        "labels": {"en": "horse", "ru": "лошадь", "zh": "马", "hi": "horse"},
        "shape": {"en": "equine quadrupeds with long legs, hooves, mane, tail, and gait-focused deformation", "ru": "equine quadrupeds с длинными ногами, копытами, гривой, хвостом и gait-focused deformation", "zh": "长腿、蹄、鬃毛、尾巴和步态变形重点的马类四足动物", "hi": "long legs, hooves, mane, tail और gait-focused deformation वाले equine quadrupeds"},
        "use": {"en": "horses, ponies, mounts, unicorns, and stylized riding animals", "ru": "лошадей, пони, mounts, unicorns и stylized riding animals", "zh": "马、小马、坐骑、独角兽和风格化骑乘动物", "hi": "horses, ponies, mounts, unicorns और stylized riding animals"},
        "tip": {"en": "Make hooves distinct, keep the tail separate, and add clean loops around shoulders and hips for walk and run cycles.", "ru": "Сделайте копыта различимыми, отделите хвост и добавьте clean loops вокруг плеч и таза для walk/run cycles.", "zh": "让蹄子清晰、尾巴分离，并在肩部和髋部添加干净网格环以支持走/跑循环。", "hi": "Hooves distinct रखें, tail separate रखें, और walk/run cycles के लिए shoulders और hips के आसपास clean loops रखें।"},
    },
    "mouse": {
        "labels": {"en": "mouse", "ru": "мышь", "zh": "老鼠", "hi": "mouse"},
        "shape": {"en": "small rodent bodies with short legs, round torso, large ears, and a thin tail", "ru": "маленькие rodent bodies с короткими ногами, круглым корпусом, большими ушами и тонким хвостом", "zh": "短腿、圆身体、大耳朵和细尾巴的小型啮齿动物身体", "hi": "short legs, round torso, large ears और thin tail वाले small rodent bodies"},
        "use": {"en": "mice, rats, cartoon rodents, tiny game companions, and stylized mascot animals", "ru": "мышей, крыс, cartoon rodents, tiny game companions и stylized mascot animals", "zh": "老鼠、鼠类、卡通啮齿动物、小型游戏伙伴和风格化吉祥物动物", "hi": "mice, rats, cartoon rodents, tiny game companions और stylized mascot animals"},
        "tip": {"en": "Keep the tail and ears readable, and avoid paws that are too small to deform in preview animations.", "ru": "Держите хвост и уши читаемыми, избегайте лап, слишком маленьких для деформации в preview animations.", "zh": "保持尾巴和耳朵清晰，避免爪子过小导致预览动画无法良好变形。", "hi": "Tail और ears readable रखें, और paws इतने छोटे न हों कि preview animations में deform न हो सकें।"},
    },
    "pig": {
        "labels": {"en": "pig", "ru": "свинья", "zh": "猪", "hi": "pig"},
        "shape": {"en": "compact quadrupeds with short legs, rounded torso, snout, ears, and small tail motion", "ru": "compact quadrupeds с короткими ногами, круглым корпусом, snout, ушами и small tail motion", "zh": "短腿、圆躯干、猪鼻、耳朵和小尾巴运动的紧凑四足动物", "hi": "short legs, rounded torso, snout, ears और small tail motion वाले compact quadrupeds"},
        "use": {"en": "pigs, boars, farm animals, cartoon mascots, and stylized livestock characters", "ru": "свиней, кабанов, farm animals, cartoon mascots и stylized livestock characters", "zh": "猪、野猪、农场动物、卡通吉祥物和风格化家畜角色", "hi": "pigs, boars, farm animals, cartoon mascots और stylized livestock characters"},
        "tip": {"en": "Leave space under the belly, separate the legs clearly, and keep the snout geometry attached cleanly to the head.", "ru": "Оставьте пространство под животом, чётко разделите ноги и аккуратно соедините snout geometry с головой.", "zh": "腹部下方留出空间，腿部清晰分离，并让猪鼻几何干净连接到头部。", "hi": "Belly के नीचे space रखें, legs clear separate रखें, और snout geometry को head से clean attach करें।"},
    },
    "rabbit": {
        "labels": {"en": "rabbit", "ru": "кролик", "zh": "兔子", "hi": "rabbit"},
        "shape": {"en": "small quadrupeds with long ears, strong back legs, compact torso, and short tail", "ru": "small quadrupeds с длинными ушами, сильными задними ногами, compact torso и short tail", "zh": "长耳、强后腿、紧凑躯干和短尾的小型四足动物", "hi": "long ears, strong back legs, compact torso और short tail वाले small quadrupeds"},
        "use": {"en": "rabbits, hares, cartoon pets, fantasy familiars, and cute game creatures", "ru": "кроликов, зайцев, cartoon pets, fantasy familiars и cute game creatures", "zh": "兔子、野兔、卡通宠物、奇幻伙伴和可爱游戏生物", "hi": "rabbits, hares, cartoon pets, fantasy familiars और cute game creatures"},
        "tip": {"en": "Keep long ears separate from the body and give the rear legs enough topology for crouch and hop poses.", "ru": "Держите длинные уши отдельно от тела и добавьте topology задним ногам для crouch/hop poses.", "zh": "长耳要与身体分离，并为后腿提供足够拓扑以支持蹲伏和跳跃姿态。", "hi": "Long ears को body से separate रखें और crouch/hop poses के लिए rear legs में enough topology दें।"},
    },
    "turtle": {
        "labels": {"en": "turtle", "ru": "черепаха", "zh": "乌龟", "hi": "turtle"},
        "shape": {"en": "shell-based non-humanoid bodies with short legs, neck motion, tail, and rigid shell areas", "ru": "shell-based non-humanoid bodies с короткими ногами, neck motion, хвостом и rigid shell areas", "zh": "基于龟壳的非人形身体，带短腿、颈部运动、尾巴和刚性龟壳区域", "hi": "short legs, neck motion, tail और rigid shell areas वाले shell-based non-humanoid bodies"},
        "use": {"en": "turtles, tortoises, armored creatures, stylized reptiles, and shell-backed game characters", "ru": "черепах, tortoises, armored creatures, stylized reptiles и shell-backed game characters", "zh": "乌龟、陆龟、装甲生物、风格化爬行动物和带壳游戏角色", "hi": "turtles, tortoises, armored creatures, stylized reptiles और shell-backed game characters"},
        "tip": {"en": "Keep the shell as a clear rigid volume and separate the neck and legs enough for visible motion.", "ru": "Держите панцирь как clear rigid volume и отделите шею и ноги достаточно для заметного motion.", "zh": "龟壳应保持清晰刚性体积，颈部和腿部要足够分离以显示运动。", "hi": "Shell को clear rigid volume रखें और visible motion के लिए neck तथा legs को enough separate रखें।"},
    },
}


def _rig_article_path(rig_key: str, lang: str = "en") -> str:
    suffix = RIG_ARTICLE_LANGS.get(lang, RIG_ARTICLE_LANGS["en"])["suffix"]
    return f"/{rig_key}-auto-rig{suffix}"


def _rig_article_localized_urls(base: str, rig_key: str) -> Dict[str, str]:
    return {
        lang: f"{base}{_rig_article_path(rig_key, lang)}"
        for lang in RIG_ARTICLE_LANGS
    }


def _rig_article_text(rig_key: str, lang: str) -> Dict[str, Any]:
    ui = RIG_ARTICLE_LANGS.get(lang, RIG_ARTICLE_LANGS["en"])
    info = RIG_ARTICLE_TYPES[rig_key]
    label = info["labels"].get(lang, info["labels"]["en"])
    shape = info["shape"].get(lang, info["shape"]["en"])
    use = info["use"].get(lang, info["use"]["en"])
    tip = info["tip"].get(lang, info["tip"]["en"])
    if lang == "ru":
        title = f"AI auto rigging online: {label} | AutoRig.online"
        description = f"Онлайн AI-риггинг для типа {label}: загрузите GLB, FBX или OBJ и получите rigged модель с animation-ready preview для Blender, Unity и Unreal Engine."
        h1 = f"AI auto rigging online: {label}"
        lead = f"AutoRig.online автоматически строит rig для {label}: {shape}. Страница сфокусирована на задачах, где пользователю нужно быстро получить готовый rig онлайн без ручной расстановки костей."
        workflow = f"Загрузите GLB, FBX или OBJ, дождитесь обработки, откройте task preview и скачайте результат для Blender, Unity, Unreal Engine или web viewers. Такой workflow подходит для {use}."
        prep = tip
        faq = [
            (f"Можно ли сделать auto rig для {label} онлайн?", f"Да. AutoRig.online принимает GLB, FBX и OBJ модели и запускает AI-assisted rigging workflow для {label}, если геометрия подходит для выбранного типа."),
            (f"Какие файлы подходят для {label} rigging?", "Лучше всего подходят чистые GLB, FBX или OBJ модели с разделёнными конечностями, читаемым силуэтом и достаточной topology в местах сгиба."),
            (f"Можно ли использовать результат в Blender, Unity и Unreal Engine?", "Да. После обработки можно открыть preview, скачать rigged файлы и использовать их в Blender, Unity, Unreal Engine и других 3D pipelines."),
        ]
    elif lang == "zh":
        title = f"{label} AI 在线自动绑定 | AutoRig.online"
        description = f"面向 {label} 的在线 AI 自动绑定。上传 GLB、FBX 或 OBJ，生成适合 Blender、Unity、Unreal Engine 和网页预览的可动画 rigged 模型。"
        h1 = f"{label} AI 在线自动绑定"
        lead = f"AutoRig.online 为 {label} 自动生成 rig：{shape}。此页面面向需要在线自动绑定特定动物或模型类型的创作者。"
        workflow = f"上传 GLB、FBX 或 OBJ，等待处理，打开 task preview，并导出到 Blender、Unity、Unreal Engine 或网页查看器。该流程适合 {use}。"
        prep = tip
        faq = [
            (f"可以在线自动绑定 {label} 吗？", f"可以。AutoRig.online 支持上传 GLB、FBX 或 OBJ，并为 {label} 运行 AI-assisted rigging workflow。"),
            (f"{label} 绑定前模型应该怎样准备？", "保持轮廓清晰、四肢分离，并在关节和弯曲区域保留足够拓扑。"),
            ("结果可以用于 Blender、Unity 和 Unreal Engine 吗？", "可以。处理完成后可打开 preview，下载 rigged 文件，并用于常见 3D 和游戏开发流程。"),
        ]
    elif lang == "hi":
        title = f"{label} AI auto rigging online | AutoRig.online"
        description = f"{label} के लिए online AI auto rigging. GLB, FBX या OBJ upload करें और Blender, Unity, Unreal Engine तथा web previews के लिए rigged animation-ready model export करें."
        h1 = f"{label} AI auto rigging online"
        lead = f"AutoRig.online {label} के लिए automatically rig बनाता है: {shape}. यह page उन creators के लिए है जिन्हें specific rig type के लिए online automatic rigging चाहिए."
        workflow = f"GLB, FBX या OBJ upload करें, processing complete होने दें, task preview खोलें, और Blender, Unity, Unreal Engine या web viewers के लिए result download करें. यह workflow {use} के लिए useful है."
        prep = tip
        faq = [
            (f"क्या {label} को online auto rig किया जा सकता है?", f"हाँ. AutoRig.online GLB, FBX और OBJ uploads लेकर {label} के लिए AI-assisted rigging workflow चलाता है."),
            (f"{label} rigging के लिए model कैसे prepare करें?", "Clear silhouette, separated limbs और bending areas में enough topology रखें ताकि animation preview साफ रहे।"),
            ("क्या result Blender, Unity और Unreal Engine में काम करेगा?", "हाँ. Processing के बाद preview खोलें, rigged files download करें, और उन्हें common 3D/game pipelines में use करें।"),
        ]
    else:
        title = f"{label.title()} AI auto rigging online | AutoRig.online"
        description = f"Online AI auto rigging for {label} 3D models. Upload GLB, FBX, or OBJ and export a rigged, animation-ready result for Blender, Unity, Unreal Engine, and web previews."
        h1 = f"{label.title()} AI auto rigging online"
        lead = f"AutoRig.online automatically builds rigs for {label} models: {shape}. This page targets creators who need a fast online workflow for a specific rig type, not only generic humanoid character rigging."
        workflow = f"Upload a GLB, FBX, or OBJ file, wait for the AI-assisted rigging task, open the task preview, and export the rigged result for Blender, Unity, Unreal Engine, or web viewers. The workflow is designed for {use}."
        prep = tip
        faq = [
            (f"Can I auto rig a {label} model online?", f"Yes. AutoRig.online accepts GLB, FBX, and OBJ uploads and runs an AI-assisted rigging workflow for {label} models when the geometry matches the supported rig type."),
            (f"What should I prepare before {label} auto rigging?", "Use a clean silhouette, separated limbs, and enough topology around bending areas so the generated rig and animation preview can deform cleanly."),
            ("Can I use the result in Blender, Unity, and Unreal Engine?", "Yes. After processing, open the preview, download the rigged files, and use them in common 3D and game development pipelines."),
        ]
    return {
        "title": title,
        "description": description,
        "h1": h1,
        "lead": lead,
        "workflow": workflow,
        "prep": prep,
        "label": label,
        "shape": shape,
        "use": use,
        "faq": faq,
        "ui": ui,
        "keywords": [
            f"{label} auto rig",
            f"{label} rigging",
            f"{label} 3D model rig",
            "AI auto rigging",
            "animal rigging online",
            "GLB FBX OBJ rigging",
            "Blender rigging",
            "Unity Unreal rigging",
        ],
    }


async def _rig_article_examples(db: AsyncSession, rig_key: str, limit: int = 6) -> List[Task]:
    base_conditions = [
        Task.status == "done",
        Task.video_ready == True,
        _gallery_task_has_poster_sql(),
        or_(Task.content_rating.is_(None), Task.content_rating != "adult"),
    ]
    result = await db.execute(
        select(Task)
        .where(*base_conditions)
        .order_by(func.coalesce(Task.updated_at, Task.created_at).desc())
        .limit(700)
    )
    out: List[Task] = []
    for task in result.scalars().all():
        if _gallery_rig_icon_key(task) == rig_key and _rig_article_example_matches(task, rig_key):
            out.append(task)
            if len(out) >= limit:
                break
    return out


def _rig_article_example_matches(task: Task, rig_key: str) -> bool:
    if rig_key == "humanoid":
        return True
    keywords = RIG_ARTICLE_EXAMPLE_KEYWORDS.get(rig_key)
    if not keywords:
        return False
    haystack = _rig_article_example_text(task)
    return any(
        re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", haystack)
        for keyword in keywords
    )


def _rig_article_example_text(task: Task) -> str:
    parts: List[str] = []
    for attr in ("poster_llm_title", "poster_llm_description"):
        value = str(getattr(task, attr, "") or "").strip()
        if value:
            parts.append(value)
    raw_keywords = getattr(task, "poster_llm_keywords", None)
    if raw_keywords:
        try:
            parsed = json.loads(raw_keywords)
            if isinstance(parsed, list):
                parts.extend(str(item) for item in parsed if item)
            else:
                parts.append(str(raw_keywords))
        except Exception:
            parts.append(str(raw_keywords))
    return " ".join(parts).lower()


def _task_public_title(task: Task, fallback: str) -> str:
    title = str(getattr(task, "poster_llm_title", "") or "").strip()
    return title[:96] if title else fallback


def _rig_article_static_example(rig_key: str) -> Dict[str, str]:
    item = dict(DEFAULT_RIG_ARTICLE_STATIC_EXAMPLE)
    item.update(RIG_ARTICLE_STATIC_EXAMPLES.get(rig_key, {}))
    return item


def _build_rig_article_html(rig_key: str, lang: str, examples: List[Task]) -> str:
    base = (APP_URL or "https://autorig.online").rstrip("/")
    lang = lang if lang in RIG_ARTICLE_LANGS else "en"
    text = _rig_article_text(rig_key, lang)
    ui = text["ui"]
    is_indexed_lang = lang in RIG_ARTICLE_INDEXED_LANGS
    canonical_lang = lang if is_indexed_lang else RIG_ARTICLE_INDEXED_LANGS[0]
    canonical = f"{base}{_rig_article_path(rig_key, canonical_lang)}"
    localized_urls = _rig_article_localized_urls(base, rig_key)
    advertised_localized_urls = {
        hreflang: localized_urls[hreflang]
        for hreflang in RIG_ARTICLE_INDEXED_LANGS
        if hreflang in localized_urls
    }
    static_example = _rig_article_static_example(rig_key)
    static_example_image = f"{base}{static_example['image']}"
    static_example_url = f"{base}{static_example['url']}"
    first_image = f"{base}/thumb/{examples[0].id}" if examples else static_example_image
    robots_content = "index,follow" if is_indexed_lang else "noindex,follow"
    article_json = {
        "@context": "https://schema.org",
        "@type": "TechArticle",
        "headline": text["title"],
        "description": text["description"],
        "inLanguage": ui["html_lang"],
        "url": canonical,
        "mainEntityOfPage": canonical,
        "image": first_image,
        "keywords": ", ".join(text["keywords"]),
        "publisher": {"@type": "Organization", "name": "AutoRig.online", "url": base},
        "about": [
            text["label"],
            "AI auto rigging",
            "animal rigging",
            "GLB FBX OBJ workflow",
            "Blender Unity Unreal export",
        ],
    }
    faq_json = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {"@type": "Answer", "text": a},
            }
            for q, a in text["faq"]
        ],
    }
    breadcrumb_json = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "AutoRig.online", "item": base},
            {"@type": "ListItem", "position": 2, "name": ui["guides"], "item": f"{base}/guides"},
            {"@type": "ListItem", "position": 3, "name": text["h1"], "item": canonical},
        ],
    }
    item_list_json = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": f"{text['label']} AutoRig examples",
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": idx + 1,
                "url": f"{base}/task?id={task.id}",
                "name": _task_public_title(task, text["h1"]),
                "image": f"{base}/thumb/{task.id}",
            }
            for idx, task in enumerate(examples)
        ] if examples else [
            {
                "@type": "ListItem",
                "position": 1,
                "url": static_example_url,
                "name": static_example["title"],
                "image": static_example_image,
            }
        ],
    }
    alt_links = "\n".join(
        f'    <link rel="alternate" hreflang="{hreflang}" href="{html.escape(url, quote=True)}">'
        for hreflang, url in advertised_localized_urls.items()
    )
    alt_links += f'\n    <link rel="alternate" hreflang="x-default" href="{html.escape(localized_urls["en"], quote=True)}">'
    examples_html = ""
    if examples:
        cards: List[str] = []
        for task in examples:
            task_title = html.escape(_task_public_title(task, text["h1"]), quote=True)
            cards.append(f"""
                    <a class="rig-article-example" href="/task?id={html.escape(task.id, quote=True)}">
                        <span class="rig-article-example-media">
                            <img src="/thumb/{html.escape(task.id, quote=True)}" alt="{task_title}" loading="lazy" width="360" height="640">
                        </span>
                        <span class="rig-article-example-title">{task_title}</span>
                        <span class="rig-article-example-link">{html.escape(ui["open_example"])}</span>
                    </a>""")
        examples_html = "\n".join(cards)
    else:
        static_title = html.escape(static_example["title"], quote=True)
        examples_html = f"""
                    <a class="rig-article-example" href="{html.escape(static_example["url"], quote=True)}">
                        <span class="rig-article-example-media">
                            <img src="{html.escape(static_example["image"], quote=True)}" alt="{static_title}" loading="lazy" width="640" height="360">
                        </span>
                        <span class="rig-article-example-title">{static_title}</span>
                        <span class="rig-article-example-link">Open V2 animal rig examples</span>
                    </a>"""

    faq_html = "\n".join(
        f"""
                    <article class="rig-article-faq-item">
                        <h3>{html.escape(q)}</h3>
                        <p>{html.escape(a)}</p>
                    </article>"""
        for q, a in text["faq"]
    )

    return f"""<!DOCTYPE html>
<html lang="{html.escape(ui["html_lang"], quote=True)}">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html.escape(text["title"])}</title>
    <meta name="description" content="{html.escape(text["description"], quote=True)}">
    <meta name="keywords" content="{html.escape(", ".join(text["keywords"]), quote=True)}">
    <meta name="robots" content="{robots_content}">
    <link rel="canonical" href="{html.escape(canonical, quote=True)}">
{alt_links}
    <meta property="og:type" content="article">
    <meta property="og:url" content="{html.escape(canonical, quote=True)}">
    <meta property="og:title" content="{html.escape(text["title"], quote=True)}">
    <meta property="og:description" content="{html.escape(text["description"], quote=True)}">
    <meta property="og:image" content="{html.escape(first_image, quote=True)}">
    <meta property="og:site_name" content="AutoRig.online">
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:title" content="{html.escape(text["title"], quote=True)}">
    <meta name="twitter:description" content="{html.escape(text["description"], quote=True)}">
    <meta name="twitter:image" content="{html.escape(first_image, quote=True)}">
    <script type="application/ld+json">{json.dumps(article_json, ensure_ascii=False)}</script>
    <script type="application/ld+json">{json.dumps(faq_json, ensure_ascii=False)}</script>
    <script type="application/ld+json">{json.dumps(breadcrumb_json, ensure_ascii=False)}</script>
    <script type="application/ld+json">{json.dumps(item_list_json, ensure_ascii=False)}</script>
    <link rel="stylesheet" href="/static/css/styles.css?v=20260516-rig-type-seo">
</head>
<body data-layout-free3d-init="none">
    <div id="site-header"></div>
    <main class="rig-article-page">
        <section class="rig-article-hero">
            <div class="container rig-article-hero-inner">
                <p class="rig-article-kicker">{html.escape(ui["kicker"])}</p>
                <h1>{html.escape(text["h1"])}</h1>
                <p class="rig-article-lead">{html.escape(text["lead"])}</p>
                <div class="rig-article-actions">
                    <a class="btn btn-primary" href="/">{html.escape(ui["cta_upload"])}</a>
                    <a class="btn btn-secondary" href="/gallery?rig_type={html.escape(rig_key, quote=True)}">{html.escape(ui["cta_gallery"])}</a>
                </div>
            </div>
        </section>
        <section class="container rig-article-section">
            <div class="rig-article-content-grid">
                <article class="rig-article-panel">
                    <h2>{html.escape(ui["overview"])}</h2>
                    <p>{html.escape(text["shape"])}</p>
                    <p>{html.escape(text["use"])}</p>
                </article>
                <article class="rig-article-panel">
                    <h2>{html.escape(ui["workflow"])}</h2>
                    <p>{html.escape(text["workflow"])}</p>
                </article>
                <article class="rig-article-panel">
                    <h2>{html.escape(ui["prep"])}</h2>
                    <p>{html.escape(text["prep"])}</p>
                </article>
            </div>
        </section>
        <section class="container rig-article-section">
            <div class="rig-article-section-header">
                <h2>{html.escape(ui["examples"])}</h2>
                <p>{html.escape(ui["examples_intro"])}</p>
            </div>
            <div class="rig-article-examples">
{examples_html}
            </div>
        </section>
        <section class="container rig-article-section">
            <div class="rig-article-section-header">
                <h2>{html.escape(ui["faq"])}</h2>
            </div>
            <div class="rig-article-faq">
{faq_html}
            </div>
        </section>
    </main>
    <div id="site-footer"></div>
    <script src="/static/js/header.js?v=20260516-mobile-nav"></script>
    <script src="/static/js/footer.js?v=20260516-seo-layout"></script>
    <script src="/static/js/site-layout.js?v=20260516-seo-layout"></script>
    <script src="/static/js/i18n.js"></script>
</body>
</html>
"""


async def _rig_article_response(rig_key: str, lang: str, db: AsyncSession) -> HTMLResponse:
    if rig_key not in RIG_ARTICLE_TYPES or lang not in RIG_ARTICLE_LANGS:
        raise HTTPException(status_code=404, detail="Not found")
    examples = await _rig_article_examples(db, rig_key)
    return HTMLResponse(content=_inject_static_layout(_build_rig_article_html(rig_key, lang, examples)))


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
        "blend": "application/x-blender",
        "json": "application/json",
        "mp4": "video/mp4",
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


def _upload_path_for_token(upload_token: Optional[str]) -> Optional[Path]:
    if not upload_token or upload_token in (".", "..") or "/" in upload_token or "\\" in upload_token:
        return None
    try:
        base = Path(UPLOAD_DIR).resolve()
        path = (base / upload_token).resolve()
        if path == base or os.path.commonpath([str(base), str(path)]) != str(base):
            return None
        return path
    except Exception:
        return None


def _iter_task_artifact_paths(task: Task) -> List[Path]:
    paths: List[Path] = [
        TASK_CACHE_DIR / task.id,
        Path("/var/autorig/videos") / f"{task.id}.mp4",
    ]
    paths.extend(GLB_CACHE_DIR.glob(f"{task.id}_*.glb"))
    upload_token = _extract_upload_token_from_input_url(getattr(task, "input_url", None))
    upload_path = _upload_path_for_token(upload_token)
    if upload_path:
        paths.append(upload_path)

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


async def purge_terminal_upload_dirs(
    db: AsyncSession,
    *,
    target_free_bytes: Optional[int] = None,
    min_age_hours: float = UPLOAD_PRESSURE_CLEANUP_MIN_AGE_HOURS,
    record_items: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[int, int]:
    """
    Under disk pressure, remove original upload folders for completed/failed tasks.

    This preserves public gallery rows, cached task downloads, GLB cache, videos, and
    poster assets. Upload folders for created/processing tasks are always protected.
    """
    upload_base = Path(UPLOAD_DIR)
    if not upload_base.exists():
        return 0, 0

    cutoff_ts = time.time() - max(0.0, float(min_age_hours)) * 3600
    protected_tokens: set[str] = set()
    candidate_paths: Dict[str, Path] = {}

    rows = (
        await db.execute(
            select(Task.id, Task.status, Task.input_url)
            .where(Task.input_url.isnot(None))
        )
    ).all()

    for _task_id, status, input_url in rows:
        token = _extract_upload_token_from_input_url(input_url)
        if not token:
            continue
        path = _upload_path_for_token(token)
        if path is None or not path.is_dir():
            continue
        if status not in ("done", "error"):
            protected_tokens.add(token)
            candidate_paths.pop(token, None)
            continue
        if token not in protected_tokens:
            candidate_paths[token] = path

    cleanable: List[Tuple[float, Path]] = []
    for token, path in candidate_paths.items():
        if token in protected_tokens:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if mtime < cutoff_ts:
            cleanable.append((mtime, path))

    cleanable.sort(key=lambda x: x[0])
    deleted = 0
    freed = 0

    for _mtime, path in cleanable:
        if target_free_bytes is not None and shutil.disk_usage("/").free >= target_free_bytes:
            break
        if not path.is_dir():
            continue
        size = _estimate_path_size(path)
        try:
            shutil.rmtree(path)
            deleted += 1
            freed += size
            print(f"[Disk] Removed terminal upload {path.name} ({size / (1024**2):.1f} MB)")
            if record_items is not None:
                record_items.append(
                    {
                        "path": path.name,
                        "type": "terminal_upload",
                        "size_mb": size / (1024**2),
                    }
                )
        except Exception as e:
            print(f"[Disk] Failed to remove terminal upload {path}: {e}")

    return deleted, freed


async def ensure_disk_headroom_for_new_task(
    db: AsyncSession,
    *,
    delete_task_rows: bool = AUTOMATIC_TASK_DB_DELETION,
) -> dict:
    """
    Run when creating a new task: if free space on / is below NEW_TASK_MIN_FREE_GB,
    first delete bundle ZIPs, then old terminal-task upload originals; if still low,
    delete oldest done/error tasks (DB + artifacts) until free space target is met or
    NEW_TASK_PURGE_TASKS_MAX_FREED_GB of data was removed in that final phase.
    """
    target_bytes = NEW_TASK_MIN_FREE_GB * 1024 * 1024 * 1024
    max_task_phase_bytes = NEW_TASK_PURGE_TASKS_MAX_FREED_GB * 1024 * 1024 * 1024

    free_bytes = shutil.disk_usage("/").free
    summary: Dict[str, Any] = {
        "initial_free_gb": free_bytes / (1024**3),
        "zips_deleted": 0,
        "zip_freed_bytes": 0,
        "terminal_uploads_deleted": 0,
        "terminal_upload_freed_bytes": 0,
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

    ud, ub = await purge_terminal_upload_dirs(db, target_free_bytes=target_bytes)
    summary["terminal_uploads_deleted"] = ud
    summary["terminal_upload_freed_bytes"] = ub

    free_bytes = shutil.disk_usage("/").free
    summary["final_free_gb"] = free_bytes / (1024**3)
    if free_bytes >= target_bytes:
        if ud:
            print(
                f"[NewTask Disk] Upload purge: removed {ud} folder(s), "
                f"{free_bytes / (1024**3):.2f} GB free (target {NEW_TASK_MIN_FREE_GB} GB)"
            )
        return summary

    if not delete_task_rows:
        print(
            f"[NewTask Disk] Still low after non-DB cleanup: "
            f"{free_bytes / (1024**3):.2f} GB free (target {NEW_TASK_MIN_FREE_GB} GB); "
            "skipping task DB row purge"
        )
        try:
            from telegram_bot import broadcast_disk_space_low

            await broadcast_disk_space_low(
                free_gb=summary["final_free_gb"],
                target_gb=NEW_TASK_MIN_FREE_GB,
                zips_deleted=summary["zips_deleted"],
                tasks_purged=0,
            )
        except Exception as e:
            print(f"[NewTask Disk] Telegram disk-low notify failed: {e}")
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
    if summary["zips_deleted"] or summary["terminal_uploads_deleted"] or summary["tasks_purged"]:
        print(
            f"[NewTask Disk] Headroom pass done: zips={summary['zips_deleted']}, "
            f"uploads={summary['terminal_uploads_deleted']}, "
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


async def ensure_request_disk_headroom(db: AsyncSession, *, context: str) -> Dict[str, Any]:
    """
    Request-path disk guard. It may remove regenerable bundle ZIPs, stale terminal
    upload originals, and orphan cache files, but it never deletes task DB rows.
    """
    try:
        await enforce_task_cache_max_size(db)
        result = await cleanup_disk_space(
            min_free_gb=NEW_TASK_MIN_FREE_GB,
            db=db,
            delete_task_rows=False,
        )
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[Request Disk] Cleanup failed before {context}: {exc}")
        result = {}

    free_gb = shutil.disk_usage("/").free / (1024**3)
    if free_gb < NEW_TASK_MIN_FREE_GB:
        raise HTTPException(
            status_code=503,
            detail=(
                "Server disk is under pressure. "
                f"{free_gb:.2f}GB free, target is {NEW_TASK_MIN_FREE_GB:.2f}GB. "
                "Please try again later."
            ),
        )
    return result


def _parse_uuid_dirname(name: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(name)
    except ValueError:
        return None


async def get_effective_task_cache_max_gb(db: AsyncSession) -> float:
    row = await get_or_create_admin_overlay_counters(db)
    v = getattr(row, "task_cache_max_gb", None)
    if v is None or (isinstance(v, (int, float)) and float(v) <= 0):
        return float(TASK_CACHE_MAX_GB)
    return float(v)


async def _task_cache_eviction_candidates(
    db: AsyncSession,
) -> List[Tuple[float, str]]:
    """
    Directories under TASK_CACHE_DIR that may be removed for cache cap enforcement.
    Skips created/processing. Oldest-first sort key (unix timestamp).
    """
    if not TASK_CACHE_DIR.exists():
        return []
    subs = [p for p in TASK_CACHE_DIR.iterdir() if p.is_dir()]
    uuid_names: List[str] = []
    for p in subs:
        u = _parse_uuid_dirname(p.name)
        if u is not None:
            uuid_names.append(p.name)
    task_map: Dict[str, Task] = {}
    if uuid_names:
        res = await db.execute(select(Task).where(Task.id.in_(uuid_names)))
        for t in res.scalars().all():
            task_map[t.id] = t

    out: List[Tuple[float, str]] = []
    for p in subs:
        u = _parse_uuid_dirname(p.name)
        if u is None:
            try:
                out.append((p.stat().st_mtime, p.name))
            except OSError:
                pass
            continue
        tid = p.name
        task = task_map.get(tid)
        if task is None:
            try:
                out.append((p.stat().st_mtime, p.name))
            except OSError:
                pass
            continue
        if task.status in ("created", "processing"):
            continue
        tdt = task.updated_at or task.created_at
        if tdt is None:
            try:
                out.append((p.stat().st_mtime, p.name))
            except OSError:
                pass
        else:
            ca = tdt
            if ca.tzinfo is not None:
                ca = ca.astimezone(timezone.utc).replace(tzinfo=None)
            out.append((ca.timestamp(), p.name))
    out.sort(key=lambda x: x[0])
    return out


async def enforce_task_cache_max_size(db: AsyncSession) -> Dict[str, Any]:
    """
    Before a new task: if static/tasks total exceeds the configured cap, remove whole
    task directories oldest-first. Never deletes cache for tasks in created/processing.
    If still over cap after dirs, strips regenerable bundle zips (same as headroom pass).
    """
    max_gb = await get_effective_task_cache_max_gb(db)
    if max_gb <= 0:
        return {"skipped": True, "reason": "cap_disabled"}
    cap_bytes = int(max_gb * 1024 * 1024 * 1024)
    total = _dir_size_bytes(TASK_CACHE_DIR)
    summary: Dict[str, Any] = {
        "cap_gb": round(max_gb, 4),
        "initial_bytes": total,
        "dirs_removed": 0,
        "bytes_freed_dirs": 0,
        "zips_deleted": 0,
        "zip_freed_bytes": 0,
        "final_bytes": total,
    }
    if total <= cap_bytes:
        return summary

    safety = 0
    while _dir_size_bytes(TASK_CACHE_DIR) > cap_bytes:
        safety += 1
        if safety > 50000:
            print("[TaskCacheCap] Safety stop: too many iterations")
            break
        candidates = await _task_cache_eviction_candidates(db)
        if not candidates:
            break
        _ts, dirname = candidates[0]
        target = TASK_CACHE_DIR / dirname
        if not target.is_dir():
            continue
        try:
            before = _dir_size_bytes(target)
            shutil.rmtree(target)
            summary["dirs_removed"] += 1
            summary["bytes_freed_dirs"] += before
            print(
                f"[TaskCacheCap] Removed {dirname} (~{before / (1024**2):.1f} MB), "
                f"task_cache now ~{_dir_size_bytes(TASK_CACHE_DIR) / (1024**3):.2f} GB (cap {max_gb} GB)"
            )
        except OSError as e:
            print(f"[TaskCacheCap] Failed to remove {target}: {e}")
            break

    total = _dir_size_bytes(TASK_CACHE_DIR)
    summary["final_bytes"] = total
    if total > cap_bytes:
        zd, zb = purge_task_cache_bundle_zips()
        summary["zips_deleted"] = zd
        summary["zip_freed_bytes"] = zb
        summary["final_bytes"] = _dir_size_bytes(TASK_CACHE_DIR)
    return summary


async def evict_task_cache_until_free_space(
    db: AsyncSession,
    *,
    min_free_gb: float,
    record_items: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Pressure-only task cache eviction.

    This removes oldest static/tasks/<task_id> directories until root free space
    reaches min_free_gb. It does not delete task DB rows and skips active tasks.
    """
    target_bytes = int(float(min_free_gb) * 1024 * 1024 * 1024)
    free_bytes = shutil.disk_usage("/").free
    summary: Dict[str, Any] = {
        "target_free_gb": float(min_free_gb),
        "initial_free_gb": free_bytes / (1024**3),
        "initial_task_cache_gb": _dir_size_bytes(TASK_CACHE_DIR) / (1024**3),
        "dirs_removed": 0,
        "bytes_freed": 0,
        "final_free_gb": free_bytes / (1024**3),
        "final_task_cache_gb": _dir_size_bytes(TASK_CACHE_DIR) / (1024**3),
    }
    if free_bytes >= target_bytes:
        return summary

    safety = 0
    while shutil.disk_usage("/").free < target_bytes:
        safety += 1
        if safety > 50000:
            print("[TaskCachePressure] Safety stop: too many iterations")
            break

        candidates = await _task_cache_eviction_candidates(db)
        if not candidates:
            break

        _ts, dirname = candidates[0]
        target = TASK_CACHE_DIR / dirname
        if not target.is_dir():
            continue

        try:
            before = _dir_size_bytes(target)
            shutil.rmtree(target)
            summary["dirs_removed"] += 1
            summary["bytes_freed"] += before
            if record_items is not None:
                record_items.append(
                    {
                        "path": dirname,
                        "type": "task_cache_pressure",
                        "size_mb": before / (1024**2),
                    }
                )
            print(
                f"[TaskCachePressure] Removed {dirname} (~{before / (1024**2):.1f} MB), "
                f"free now {shutil.disk_usage('/').free / (1024**3):.2f} GB "
                f"(target {min_free_gb} GB)"
            )
        except OSError as e:
            print(f"[TaskCachePressure] Failed to remove {target}: {e}")
            break

    final_free = shutil.disk_usage("/").free
    summary["final_free_gb"] = final_free / (1024**3)
    summary["final_task_cache_gb"] = _dir_size_bytes(TASK_CACHE_DIR) / (1024**3)
    if summary["dirs_removed"] > 0:
        print(
            f"[TaskCachePressure] Complete: removed {summary['dirs_removed']} dir(s), "
            f"freed {summary['bytes_freed'] / (1024**3):.2f} GB, "
            f"free now {summary['final_free_gb']:.2f} GB"
        )
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
            video_url = await _resolve_task_video_source_url(task)
            if video_url and not await _probe_http_asset_reachable(client, video_url):
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


@app.head("/api/task/{task_id}/animations.fbx")
async def api_head_animations_fbx(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Check whether the package-level FBX animation file is available."""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    animations_url, filename = _resolve_all_animations_fbx_url(task)
    if not animations_url or not filename:
        raise HTTPException(status_code=404, detail="Animations FBX not available yet")
    if not await _worker_file_available(animations_url):
        raise HTTPException(status_code=404, detail="Animations FBX not available yet")

    return Response(
        status_code=200,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "public, max-age=86400",
            "Access-Control-Allow-Origin": "*",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.get("/api/task/{task_id}/animations.fbx")
async def api_proxy_animations_fbx(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Proxy animations FBX file from worker (searches ready_urls)"""
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    animations_url, filename = _resolve_all_animations_fbx_url(task)
    if not animations_url or not filename:
        raise HTTPException(status_code=404, detail="Animations FBX not available yet")
    return await _proxy_model_file(animations_url, filename, as_attachment=True)


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


async def _blueprint_file_response(
    task_id: str,
    suffix: str,
    public_filename: str,
    media_type: str,
    db: AsyncSession,
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if str(task.input_type or "").strip().lower() != "animal":
        raise HTTPException(status_code=404, detail="Blueprint files are available for animal rig tasks only")

    cached = _find_cached_blueprint_file(task_id, task.guid, suffix)
    if cached:
        return FileResponse(
            path=str(cached),
            media_type=media_type,
            filename=public_filename,
            headers={
                "Cache-Control": "public, max-age=86400",
                "Access-Control-Allow-Origin": "*",
                "Content-Encoding": "identity",
            },
        )

    source_url = await _resolve_task_worker_file_url(task, suffix)
    if not source_url:
        raise HTTPException(status_code=404, detail="Blueprint file not available yet")
    return await _proxy_model_file(source_url, public_filename, as_attachment=False)


@app.head("/api/task/{task_id}/blueprint/skeleton.json")
@app.get("/api/task/{task_id}/blueprint/skeleton.json")
async def api_proxy_blueprint_skeleton(
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    return await _blueprint_file_response(
        task_id,
        BLUEPRINT_SKELETON_SUFFIX,
        "skeleton.json",
        "application/json",
        db,
    )


@app.head("/api/task/{task_id}/blueprint/rig-preview.mp4")
@app.get("/api/task/{task_id}/blueprint/rig-preview.mp4")
async def api_proxy_blueprint_rig_preview(
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    return await _blueprint_file_response(
        task_id,
        BLUEPRINT_RIG_PREVIEW_SUFFIX,
        "rig_preview.mp4",
        "video/mp4",
        db,
    )


@app.head("/api/task/{task_id}/blueprint/model.glb")
@app.get("/api/task/{task_id}/blueprint/model.glb")
async def api_proxy_blueprint_model_glb(
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if not _is_animal_task(task):
        raise HTTPException(status_code=404, detail="Blueprint model is available for animal rig tasks only")

    cache_path = GLB_CACHE_DIR / f"{task_id}_blueprint.glb"
    if cache_path.exists() and _validate_glb_file(cache_path):
        return FileResponse(
            path=str(cache_path),
            media_type="model/gltf-binary",
            filename=f"{task_id}_blueprint.glb",
            headers=dict(_GLB_FILE_HTTP_HEADERS),
        )

    source_url = await _resolve_task_blueprint_model_url(task)
    if not source_url:
        raise HTTPException(status_code=404, detail="Blueprint model not available yet")
    result = await _get_cached_glb(task_id, source_url, "blueprint")
    if result:
        return result
    raise HTTPException(status_code=404, detail="Blueprint model not available yet")


@app.head("/thumb/{task_id}")
@app.get("/thumb/{task_id}")
@app.head("/api/thumb/{task_id}")
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
                    "Cache-Control": "public, max-age=0, must-revalidate",
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
    user: Optional[User] = Depends(get_current_user),
):
    """Notify Telegram about a purchase button click."""
    try:
        body = await request.json()
        package = str(body.get('package') or 'unknown')[:120]
        price = str(body.get('price') or 'unknown')[:80]
        product_kind = str(body.get('product_kind') or 'credits')[:40]
        permalink = str(body.get('permalink') or '')[:120]
        source = str(body.get('source') or '')[:80]
        page_url = str(body.get('page_url') or '')[:500]
        
        user_email = user.email if user else None
        anon_id = None if user_email else _effective_anon_id(request)
        
        # Fire-and-forget notification
        from telegram_bot import broadcast_credits_purchase_click
        asyncio.create_task(broadcast_credits_purchase_click(
            package=package,
            price=price,
            user_email=user_email,
            anon_id=anon_id,
            product_kind=product_kind,
            permalink=permalink,
            source=source,
            page_url=page_url,
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
VIEWER_BACKDROP_SUFFIXES = frozenset({".jpg", ".jpeg"})
VIEWER_THEME_JSON_VERSION = 1
VIEWER_THEME_ROOT_DIR = Path(__file__).resolve().parent.parent / "static" / "env" / "backdrops"
VIEWER_THEME_SOURCE_DIR = VIEWER_THEME_ROOT_DIR / "source"
VIEWER_THEME_VIEWER_DIR = VIEWER_THEME_ROOT_DIR / "viewer"
VIEWER_THEME_THUMB_DIR = VIEWER_THEME_ROOT_DIR / "thumbs"
VIEWER_THEME_ASSET_VERSION = "20260516-16x9"





def _slugify_viewer_theme(value: str) -> str:

    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())

    s = re.sub(r"_+", "_", s).strip("_")

    return s or "viewer_theme"





def _viewer_theme_json_path_for_image(image_path: Path) -> Path:
    return VIEWER_THEME_ROOT_DIR / f"{_slugify_viewer_theme(image_path.stem)}.json"

def _load_viewer_theme_json(image_path: Path) -> Dict[str, Any]:
    theme_id = _slugify_viewer_theme(image_path.stem)
    json_path = _viewer_theme_json_path_for_image(image_path)
    if not json_path.is_file():
        raise RuntimeError(f"Viewer theme JSON missing for {theme_id}: {json_path}")
    data = _read_json_file(str(json_path))
    if not isinstance(data, dict):
        raise RuntimeError(f"Viewer theme JSON is invalid for {theme_id}: {json_path}")
    theme_id = _slugify_viewer_theme(str(data.get("theme_id") or theme_id))
    viewer_path = VIEWER_THEME_VIEWER_DIR / f"{theme_id}.jpg"
    thumb_path = VIEWER_THEME_THUMB_DIR / f"{theme_id}.jpg"
    if not viewer_path.is_file():
        raise RuntimeError(f"Viewer theme derivative missing for {theme_id}: {viewer_path}")
    if not thumb_path.is_file():
        raise RuntimeError(f"Viewer theme thumbnail missing for {theme_id}: {thumb_path}")
    merged = dict(data)
    merged["theme_id"] = theme_id
    merged["id"] = theme_id
    merged["image_filename"] = image_path.name
    merged["source_src"] = f"/static/env/backdrops/source/{quote(image_path.name)}"
    merged["src"] = f"/static/env/backdrops/viewer/{quote(theme_id)}.jpg?v={VIEWER_THEME_ASSET_VERSION}"
    merged["thumb_src"] = f"/static/env/backdrops/thumbs/{quote(theme_id)}.jpg?v={VIEWER_THEME_ASSET_VERSION}"
    return merged

def _viewer_theme_source_images() -> List[Path]:
    if not VIEWER_THEME_SOURCE_DIR.is_dir():
        raise RuntimeError(f"Viewer theme source directory missing: {VIEWER_THEME_SOURCE_DIR}")
    return sorted(
        [p for p in VIEWER_THEME_SOURCE_DIR.iterdir() if p.is_file() and p.suffix.lower() in VIEWER_BACKDROP_SUFFIXES],
        key=lambda p: p.name.lower(),
    )


def _ensure_viewer_theme_json_files() -> None:
    missing = []
    for p in _viewer_theme_source_images():
        if not _viewer_theme_json_path_for_image(p).is_file():
            missing.append(_slugify_viewer_theme(p.stem))
    if missing:
        raise RuntimeError(f"Viewer theme JSON missing for: {', '.join(missing)}")

async def _sync_viewer_backdrop_themes_async() -> None:
    await asyncio.to_thread(_ensure_viewer_theme_json_files)

def _list_viewer_theme_items() -> List[Dict[str, Any]]:
    """Theme manifest for the 3D viewer: optimized viewer JPG plus tiny JPG strip thumbnail."""
    _ensure_viewer_theme_json_files()
    files = _viewer_theme_source_images()
    seen = set()
    out: List[Dict[str, Any]] = []
    for p in files:
        item = _load_viewer_theme_json(p)
        tid = _slugify_viewer_theme(str(item.get("theme_id") or p.stem))
        if tid in seen:
            continue
        seen.add(tid)
        item["id"] = tid
        item["theme_id"] = tid
        out.append(item)
    return out

@app.get("/api/viewer-themes")

async def api_viewer_themes():

    """Auto-discovered 3D viewer themes (image + companion JSON settings, name order)."""

    return _list_viewer_theme_items()





@app.get("/api/viewer-backdrops")

async def api_viewer_backdrops():

    """Backward-compatible alias for theme-aware viewer backgrounds."""

    return _list_viewer_theme_items()





@app.put("/api/admin/viewer-themes/{theme_id}")

async def api_admin_save_viewer_theme(

    theme_id: str,

    request: Request,

    admin: User = Depends(require_admin),

):

    body = await request.body()

    try:

        payload = json.loads(body.decode("utf-8"))

    except Exception:

        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(payload, dict):

        raise HTTPException(status_code=400, detail="Expected JSON object")

    themes = _list_viewer_theme_items()

    theme = next((x for x in themes if str(x.get("theme_id")) == str(theme_id)), None)

    if not theme:

        raise HTTPException(status_code=404, detail="Theme not found")

    image_filename = str(theme.get("image_filename") or "")

    image_path = VIEWER_THEME_SOURCE_DIR / image_filename

    if not image_path.is_file():

        raise HTTPException(status_code=404, detail="Theme image not found")

    existing = _load_viewer_theme_json(image_path)

    allowed = {

        "theme_name",

        "theme_short_description",

        "semantic_tags",

        "camera_transform",

        "plane_color",

        "shadow_settings",

        "environment_settings",

        "sun_settings",

    }

    clean = {k: payload[k] for k in allowed if k in payload}

    clean["schema_version"] = VIEWER_THEME_JSON_VERSION

    clean["theme_id"] = str(theme_id)

    clean["image_filename"] = image_filename

    clean["admin_edited_bool"] = True

    merged = {**existing, **clean}

    _atomic_write_json_file(str(_viewer_theme_json_path_for_image(image_path)), merged)

    return {"ok": True, "theme": _load_viewer_theme_json(image_path)}





def _viewer_theme_score_text(theme: Dict[str, Any], text: str) -> float:

    hay = f" {text.lower()} "

    score = 0.0

    tid = str(theme.get("theme_id") or theme.get("id") or "").lower()

    alias_tags: Dict[str, List[str]] = {

        "dog_park_yard": ["cat", "cats", "kitten", "kitty", "feline", "dog", "puppy", "pet", "pets"],

        "ranch_farmyard": ["horse", "cow", "bull", "pony", "deer", "goat", "sheep", "pig", "piglet", "hog", "boar", "swine"],

        "alien_planet": ["astronaut", "space", "alien", "planet", "ufo"],

        "sci_fi_hangar": ["robot", "mech", "android", "cyborg", "spaceship", "vehicle"],

        "jungle_temple_ruins": ["dinosaur", "dinosaurs", "dino", "reptile", "prehistoric", "lizard", "turtle"],

        "studio_white_softbox": ["product", "toy", "neutral", "studio"],

    }

    for alias in alias_tags.get(tid, []):

        if f" {alias} " in hay:

            score += 3.0

    for tag in theme.get("semantic_tags") or []:

        tag_s = str(tag).strip().lower()

        if not tag_s:

            continue

        if tag_s in hay:

            score += 2.5

        for part in re.split(r"[^a-z0-9]+", tag_s):

            if len(part) >= 4 and f" {part} " in hay:

                score += 1.0

    name = str(theme.get("theme_name") or "").lower()

    if name and any(part and part in hay for part in re.split(r"[^a-z0-9]+", name) if len(part) >= 4):

        score += 0.75

    return score





def _viewer_theme_text_has_any(text: str, terms: List[str]) -> bool:

    tokens = set(re.findall(r"[a-z0-9]+", str(text or "").lower()))

    return any(str(term or "").lower() in tokens for term in terms)





def _viewer_theme_rule_based_selection(

    themes: List[Dict[str, Any]],

    text: str,

    *,

    provider_string: str,

) -> Optional[Dict[str, Any]]:

    rules = [

        (

            ["pig", "piglet", "hog", "boar", "swine", "cow", "bull", "horse", "pony", "goat", "sheep"],

            "ranch_farmyard",

            "farm/livestock animal keyword match",

        ),

        (

            ["dinosaur", "dinosaurs", "dino", "prehistoric", "reptile"],

            "jungle_temple_ruins",

            "dinosaur/reptile keyword match",

        ),

        (

            ["dragon", "wyvern"],

            "crystal_cavern",

            "dragon/fantasy creature keyword match",

        ),

    ]

    for terms, theme_id, reason in rules:

        if not _viewer_theme_text_has_any(text, terms):

            continue

        if not any(str(t.get("theme_id")) == theme_id for t in themes):

            continue

        return {

            "theme_id": theme_id,

            "confidence_float": 0.95,

            "reason_string": reason,

            "provider_string": provider_string,

        }

    return None





def _select_viewer_theme_from_metadata(

    *,

    input_url: Optional[str],

    input_type: Optional[str],

    rig_v2_detection_meta: Optional[Dict[str, Any]],

) -> Optional[Dict[str, Any]]:

    try:

        themes = _list_viewer_theme_items()

    except Exception as e:

        print(f"[ViewerThemes] metadata select failed to list themes: {e}")

        return None

    if not themes:

        return None

    pieces: List[str] = [str(input_type or "")]

    if input_url:

        try:

            pieces.append(Path(urlparse(input_url).path or "").name)

        except Exception:

            pieces.append(str(input_url))

    if isinstance(rig_v2_detection_meta, dict):

        pieces.extend(str(v) for v in rig_v2_detection_meta.values() if isinstance(v, (str, int, float)))

        first = rig_v2_detection_meta.get("first_result")

        if isinstance(first, dict):

            pieces.extend(str(v) for v in first.values() if isinstance(v, (str, int, float)))

    text = " ".join(pieces)

    rule_selected = _viewer_theme_rule_based_selection(themes, text, provider_string="heuristic:create-rule")

    if rule_selected:

        return rule_selected

    scored = sorted(((t, _viewer_theme_score_text(t, text)) for t in themes), key=lambda x: x[1], reverse=True)

    best, score = scored[0]

    if score <= 0:

        return None

    return {

        "theme_id": str(best.get("theme_id")),

        "confidence_float": min(1.0, max(0.2, score / 8.0)),

        "reason_string": "create metadata match",

        "provider_string": "heuristic:create",

    }





async def _viewer_theme_select_with_openai(image_data_url: str, themes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:

    if not image_data_url:

        return None

    try:

        cfg = _rig_v2_load_vision_config()

    except Exception:

        return None

    api_key = str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip()

    api_url = str(cfg.get("open_ai_api_url_string") or "https://api.openai.com/v1/chat/completions").strip()

    if not api_key or not api_url:

        return None

    compact = [

        {

            "theme_id": str(t.get("theme_id") or t.get("id") or ""),

            "theme_name": str(t.get("theme_name") or ""),

            "description": str(t.get("theme_short_description") or ""),

            "tags": t.get("semantic_tags") or [],

        }

        for t in themes

    ]

    prompt = (

        "Analyze the rendered 3D model image and choose the most semantically appropriate viewer theme for the model subject. "

        "Ignore the current background/backdrop in the screenshot; it may be a wrong temporary default. "

        "Return only JSON: {\"theme_id\":\"<one theme_id>\",\"confidence_float\":0.0,\"reason_string\":\"short\"}. "

        "Prefer exact concepts: cat/kitten/dog/pet -> pet park; astronaut/alien/planet -> space; robot/mech -> sci-fi hangar; horse/cow/deer -> ranch/stable; "

        "wild animal -> forest/savanna; fantasy creature -> crystal/ruins/jungle; neutral product -> studio.\n\n"

        f"Available themes JSON:\n{json.dumps(compact, ensure_ascii=False)}"

    )

    model = str(cfg.get("open_ai_vision_model_string") or "gpt-4o-mini").strip()

    payload: Dict[str, Any] = {

        "model": model,

        "response_format": {"type": "json_object"},

        "messages": [

            {

                "role": "user",

                "content": [

                    {"type": "text", "text": prompt},

                    {"type": "image_url", "image_url": {"url": image_data_url, "detail": "low"}},

                ],

            }

        ],

    }

    if model.startswith(("gpt-5", "o3", "o4")):

        payload["max_completion_tokens"] = 900

    else:

        payload["temperature"] = 0.0

        payload["max_tokens"] = 900

    try:

        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:

            resp = await client.post(api_url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload)

        if resp.status_code != 200:

            print(f"[ViewerThemes] OpenAI auto-select HTTP {resp.status_code}: {resp.text[:240]}")

            return None

        data = resp.json()

        content = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")

        parsed = json.loads(str(content))

        if not isinstance(parsed, dict):

            return None

        tid = str(parsed.get("theme_id") or "").strip()

        if not tid:

            return None

        match = next((t for t in themes if str(t.get("theme_id")) == tid), None)

        if not match:

            return None

        return {

            "theme_id": tid,

            "confidence_float": max(0.0, min(1.0, float(parsed.get("confidence_float") or 0.5))),

            "reason_string": str(parsed.get("reason_string") or "vision selected theme")[:240],

            "provider_string": f"openai:{model}",

        }

    except Exception as e:

        print(f"[ViewerThemes] OpenAI auto-select failed: {e}")

        return None





@app.post("/api/task/{task_id}/viewer-theme/auto-select")

async def api_task_viewer_theme_auto_select(

    task_id: str,

    request: Request,

    response: Response,

    user: Optional[User] = Depends(get_current_user),

    db: AsyncSession = Depends(get_db),

):

    task = await get_task_by_id(db, task_id)

    if not task:

        raise HTTPException(status_code=404, detail="Task not found")

    try:

        body = await request.json()

        if not isinstance(body, dict):

            body = {}

    except Exception:

        body = {}

    themes = _list_viewer_theme_items()

    if not themes:

        return {"ok": False, "detail": "No viewer themes"}

    anon_session = None

    try:

        anon_session = await get_anon_session(request, response, db)

    except Exception:

        anon_session = None

    can_persist = _is_task_owner_or_admin(task=task, user=user, anon_session=anon_session)

    image_data = str(body.get("image_data_url_string") or "").strip()

    hint_pieces: List[str] = [

        str(getattr(task, "input_type", "") or ""),

        str(getattr(task, "poster_llm_title", "") or ""),

        str(getattr(task, "poster_llm_description", "") or ""),

    ]

    input_url_text = str(getattr(task, "input_url", "") or "")

    if input_url_text:

        hint_pieces.append(input_url_text)

        try:

            hint_pieces.append(Path(urlparse(input_url_text).path or "").name)

        except Exception:

            pass

    try:

        kws = json.loads(getattr(task, "poster_llm_keywords", "") or "[]")

        if isinstance(kws, list):

            hint_pieces.extend(str(x) for x in kws)

    except Exception:

        pass

    try:

        settings = json.loads(task.viewer_settings or "{}")

        det = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None

        if isinstance(det, dict):

            hint_pieces.extend(str(v) for v in det.values() if isinstance(v, (str, int, float)))

            for result in det.get("results") or []:

                if isinstance(result, dict):

                    hint_pieces.extend(str(v) for v in result.values() if isinstance(v, (str, int, float)))

    except Exception:

        pass

    hint_pieces.append(str(body.get("model_hint_string") or ""))

    hint_text = " ".join(hint_pieces)

    selected = _viewer_theme_rule_based_selection(themes, hint_text, provider_string="heuristic:auto-rule")

    if not selected and can_persist:

        selected = await _viewer_theme_select_with_openai(image_data, themes)

    provider = selected.get("provider_string") if selected else "heuristic"

    if not selected:

        scored = sorted(((t, _viewer_theme_score_text(t, hint_text)) for t in themes), key=lambda x: x[1], reverse=True)

        best, score = scored[0]

        if score <= 0:

            return {"ok": False, "detail": "No matching viewer theme"}

        selected = {

            "theme_id": str(best.get("theme_id")),

            "confidence_float": min(1.0, max(0.2, score / 8.0)),

            "reason_string": "keyword match",

            "provider_string": provider,

        }

    if can_persist:

        try:

            existing = json.loads(task.viewer_settings or "{}")

            if not isinstance(existing, dict):

                existing = {}

        except Exception:

            existing = {}

        existing["viewer_theme_selection"] = selected

        task.viewer_settings = json.dumps(existing, ensure_ascii=False)

        await db.commit()

    theme = next((t for t in themes if str(t.get("theme_id")) == selected["theme_id"]), None)

    return {"ok": True, "selection": selected, "theme": theme, "persisted": bool(can_persist)}


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


@app.post("/api/admin/viewer-default-camera")
async def api_set_viewer_default_camera(
    request: Request,
    admin: User = Depends(require_admin),
):
    """Admin-only: merge the live 3D viewer camera into global default settings."""
    body = await request.body()
    camera_settings = _validate_viewer_default_camera_payload(
        body,
        saved_by=str(getattr(admin, "email", "") or ""),
    )
    try:
        existing = _read_json_file(VIEWER_DEFAULT_SETTINGS_PATH)
        if not existing:
            existing = json.loads(json.dumps(DEFAULT_VIEWER_SETTINGS))
        existing["camera"] = camera_settings
        _atomic_write_json_file(VIEWER_DEFAULT_SETTINGS_PATH, existing)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save default camera: {e}")
    return {"ok": True, "camera": camera_settings}


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
                global_camera = _read_global_viewer_camera_preset()
                if global_camera:
                    data = {**data, "camera": global_camera}
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
    try:
        existing = json.loads(task.viewer_settings or "{}")
        if not isinstance(existing, dict):
            existing = {}
    except Exception:
        existing = {}
    for key in ("rig_v2_animal_detection", "viewer_theme_selection"):
        if isinstance(existing.get(key), dict) and key not in settings:
            settings[key] = existing[key]
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

STATIC_PAGE_CANONICAL_PATHS: Dict[str, str] = {
    "index.html": "/",
    "gallery.html": "/gallery",
    "guides.html": "/guides",
    "buy-credits.html": "/buy-credits",
    "blender-plugin.html": "/blender-plugin",
    "animal-rig.html": "/animal-rig",
    "developers.html": "/developers",
    "payment-success.html": "/payment/success",
    "glb-auto-rig.html": "/glb-auto-rig",
    "fbx-auto-rig.html": "/fbx-auto-rig",
    "obj-auto-rig.html": "/obj-auto-rig",
    "how-it-works.html": "/how-it-works",
    "faq.html": "/faq",
    "terms-of-use.html": "/terms",
    "user-agreement.html": "/user-agreement",
    "t-pose-rig.html": "/t-pose-rig",
    "mixamo-alternative.html": "/mixamo-alternative",
    "mixamo-alternative-ru.html": "/mixamo-alternative-ru",
    "mixamo-alternative-zh.html": "/mixamo-alternative-zh",
    "mixamo-alternative-hi.html": "/mixamo-alternative-hi",
    "rig-glb-unity.html": "/rig-glb-unity",
    "rig-glb-unity-ru.html": "/rig-glb-unity-ru",
    "rig-glb-unity-zh.html": "/rig-glb-unity-zh",
    "rig-glb-unity-hi.html": "/rig-glb-unity-hi",
    "rig-fbx-unreal.html": "/rig-fbx-unreal",
    "rig-fbx-unreal-ru.html": "/rig-fbx-unreal-ru",
    "rig-fbx-unreal-zh.html": "/rig-fbx-unreal-zh",
    "rig-fbx-unreal-hi.html": "/rig-fbx-unreal-hi",
    "glb-vs-fbx.html": "/glb-vs-fbx",
    "glb-vs-fbx-ru.html": "/glb-vs-fbx-ru",
    "glb-vs-fbx-zh.html": "/glb-vs-fbx-zh",
    "glb-vs-fbx-hi.html": "/glb-vs-fbx-hi",
    "t-pose-vs-a-pose.html": "/t-pose-vs-a-pose",
    "t-pose-vs-a-pose-ru.html": "/t-pose-vs-a-pose-ru",
    "t-pose-vs-a-pose-zh.html": "/t-pose-vs-a-pose-zh",
    "t-pose-vs-a-pose-hi.html": "/t-pose-vs-a-pose-hi",
    "animation-retargeting.html": "/animation-retargeting",
    "animation-retargeting-ru.html": "/animation-retargeting-ru",
    "animation-retargeting-zh.html": "/animation-retargeting-zh",
    "animation-retargeting-hi.html": "/animation-retargeting-hi",
    "face-rig-animation.html": "/face-rig-animation",
    "face-rig-animation-ru.html": "/face-rig-animation-ru",
    "face-rig-animation-zh.html": "/face-rig-animation-zh",
    "face-rig-animation-hi.html": "/face-rig-animation-hi",
    "auto-rig-obj.html": "/auto-rig-obj",
    "auto-rig-obj-ru.html": "/auto-rig-obj-ru",
    "auto-rig-obj-zh.html": "/auto-rig-obj-zh",
    "auto-rig-obj-hi.html": "/auto-rig-obj-hi",
}

PUBLIC_QUERY_NOINDEX_PATHS = {"/", "/gallery"}


def _response_without_body(response: Response) -> Response:
    headers = dict(response.headers)
    headers.pop("content-length", None)
    return Response(
        content=b"",
        status_code=response.status_code,
        headers=headers,
        media_type=getattr(response, "media_type", None),
    )


def _should_fallback_head_to_get(path: str) -> bool:
    if path in {"/", "/task", "/dashboard", "/admin", "/admin/workers"}:
        return True
    return path in set(STATIC_PAGE_CANONICAL_PATHS.values())


@app.middleware("http")
async def seo_http_headers_and_head_fallback(request: Request, call_next):
    original_method = request.scope.get("method", "").upper()
    is_head_fallback = original_method == "HEAD" and _should_fallback_head_to_get(request.url.path)

    if is_head_fallback:
        request.scope["method"] = "GET"

    response = await call_next(request)

    path = request.url.path
    if path == "/dashboard":
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
    elif path.startswith("/m/"):
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
    elif path in PUBLIC_QUERY_NOINDEX_PATHS and request.url.query:
        response.headers["X-Robots-Tag"] = "noindex, follow"

    if is_head_fallback:
        request.scope["method"] = original_method
        return _response_without_body(response)

    return response


def _read_static_partial(name: str) -> str:
    path = STATIC_DIR / "partials" / name
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def _inject_static_layout(html_content: str, canonical_path: Optional[str] = None) -> str:
    body_match = re.search(r"<body\b[^>]*>", html_content, flags=re.IGNORECASE)
    body_tag = body_match.group(0) if body_match else ""
    show_search = (
        'data-layout-free3d-ribbon="1"' in body_tag
        or "data-layout-free3d-ribbon='1'" in body_tag
        or 'data-layout-free3d-ribbon="true"' in body_tag
        or "data-layout-free3d-ribbon='true'" in body_tag
    )

    if '<div id="site-header"></div>' in html_content:
        header_markup = _read_static_partial("site-header.html")
        if show_search:
            header_markup = f"{header_markup}\n{_read_static_partial('site-free3d-search.html')}"
        html_content = html_content.replace(
            '<div id="site-header"></div>',
            f'<div id="site-header" data-server-rendered="1">\n{header_markup}\n</div>',
            1,
        )

    if '<div id="site-footer"></div>' in html_content:
        footer_markup = _read_static_partial("site-footer.html")
        html_content = html_content.replace(
            '<div id="site-footer"></div>',
            f'<div id="site-footer" data-server-rendered="1">\n{footer_markup}\n</div>',
            1,
        )

    if canonical_path and not re.search(r"<link\b[^>]+rel=[\"']canonical[\"']", html_content, flags=re.IGNORECASE):
        base_url = (APP_URL or "https://autorig.online").rstrip("/")
        canonical_url = f"{base_url}{canonical_path}"
        canonical_tag = f'    <link rel="canonical" href="{html.escape(canonical_url, quote=True)}">\n'
        html_content = re.sub(
            r"</head\s*>",
            f"{canonical_tag}</head>",
            html_content,
            count=1,
            flags=re.IGNORECASE,
        )

    return html_content


def _static_html_response(filename: str) -> HTMLResponse:
    path = STATIC_DIR / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return HTMLResponse(
        content=_inject_static_layout(
            path.read_text(encoding="utf-8"),
            canonical_path=STATIC_PAGE_CANONICAL_PATHS.get(filename),
        )
    )


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
                        temp_path = filepath.with_name(f".{filename}.{uuid.uuid4().hex}.tmp")
                        size_bytes = await _stream_httpx_response_to_file(response, temp_path)
                        os.replace(temp_path, filepath)
                        
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
    1. Remove regenerable bundle ZIPs.
    2. Remove old upload originals for terminal tasks, preserving gallery/cache/video/poster data.
    3. Remove the oldest completed/error tasks physically and delete their DB rows (only if delete_task_rows).
    4. Remove orphaned cache/upload/video files not referenced by any remaining task.
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

            ud, ub = await purge_terminal_upload_dirs(
                cleanup_db,
                target_free_bytes=min_free_bytes,
                record_items=result["deleted_items"],
            )
            result["deleted_count"] += ud
            result["freed_bytes"] += ub

            disk_usage = shutil.disk_usage("/")
            free_bytes = disk_usage.free
            if free_bytes >= min_free_bytes:
                result["freed_gb"] = result["freed_bytes"] / (1024**3)
                result["final_free_gb"] = free_bytes / (1024**3)
                if ud > 0:
                    print(
                        f"[Disk Cleanup] Target reached after terminal upload purge: "
                        f"freed {result['freed_gb']:.2f} GB, {result['final_free_gb']:.2f} GB free now"
                    )
                return result

            cd = await evict_task_cache_until_free_space(
                cleanup_db,
                min_free_gb=min_free_gb,
                record_items=result["deleted_items"],
            )
            result["deleted_count"] += int(cd.get("dirs_removed") or 0)
            result["freed_bytes"] += int(cd.get("bytes_freed") or 0)

            disk_usage = shutil.disk_usage("/")
            free_bytes = disk_usage.free
            if free_bytes >= min_free_bytes:
                result["freed_gb"] = result["freed_bytes"] / (1024**3)
                result["final_free_gb"] = free_bytes / (1024**3)
                if cd.get("dirs_removed"):
                    print(
                        f"[Disk Cleanup] Target reached after task cache pressure eviction: "
                        f"freed {result['freed_gb']:.2f} GB, {result['final_free_gb']:.2f} GB free now"
                    )
                return result

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


@app.get("/llms.txt")
async def serve_llms_txt():
    """De-facto LLM discovery file; keep /llm.txt as backward-compatible alias."""
    path = STATIC_DIR / "llms.txt"
    if path.is_file():
        return FileResponse(str(path), media_type="text/plain; charset=utf-8")
    fallback = STATIC_DIR / "llm.txt"
    if fallback.is_file():
        return FileResponse(str(fallback), media_type="text/plain; charset=utf-8")
    raise HTTPException(status_code=404, detail="llms.txt not found")


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
    return _static_html_response("index.html")


@app.get("/task")
async def task_page(
    id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Serve task page with dynamic OG meta tags for Telegram/social sharing"""
    
    # Read base template
    task_html_path = STATIC_DIR / "task.html"
    html_content = task_html_path.read_text(encoding="utf-8")
    
    # If no task_id, return default page (non-indexable)
    if not id:
        html_content = html_content.replace(
            "<!-- TASK_SEO_PLACEHOLDER -->",
            f'<link rel="canonical" href="{(APP_URL or "https://autorig.online").rstrip("/")}/task">',
        )
        return HTMLResponse(content=_inject_static_layout(html_content))
    
    task_id = id.strip()
    if not task_id:
        html_content = html_content.replace(
            "<!-- TASK_SEO_PLACEHOLDER -->",
            f'<link rel="canonical" href="{(APP_URL or "https://autorig.online").rstrip("/")}/task">',
        )
        return HTMLResponse(content=_inject_static_layout(html_content))

    base_url = (APP_URL or "https://autorig.online").rstrip("/")
    task_url = f"{base_url}/task?id={task_id}"
    
    # Try to get task info for better OG tags
    task_title = "Rigged 3D Model"
    task_description = "View this rigged 3D character with 50+ animations"
    has_video = False
    has_thumb = False
    task = None
    task_keywords: List[str] = []
    
    try:
        from database import Task
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()
        
        if task:
            if task.status == "done":
                task_title = "✅ Rigged 3D Model Ready"
                task_description = "3D character rigged with skeleton and 50+ animations. Download in GLB, FBX, OBJ formats."
                try:
                    from seo_gallery import enrich_seo_metadata

                    seo_title, seo_desc, seo_keywords, _seo_semantic = enrich_seo_metadata(task)
                    if seo_title:
                        task_title = seo_title
                    if seo_desc:
                        task_description = seo_desc[:500]
                    task_keywords = seo_keywords
                except Exception as seo_error:
                    print(f"[Task Page] Error enriching SEO metadata: {seo_error}")
            elif task.status == "processing":
                task_title = "⏳ Rigging in Progress..."
                task_description = "3D model is being rigged with AI. View live progress."
            elif task.status == "error":
                task_title = "❌ Rigging Failed"
                task_description = "There was an error processing this model."
            
            # Check if video exists. Prefer DB truth; filesystem check is a legacy fallback.
            video_path = f"/var/autorig/videos/{task_id}.mp4"
            has_video = bool(getattr(task, "video_ready", False)) or (
                os.path.exists(video_path) and os.path.getsize(video_path) > 0
            )
            
            # Assume thumb exists if task has ready_urls
            has_thumb = bool(task.ready_urls)
    except Exception as e:
        print(f"[Task Page] Error getting task info: {e}")

    if not task:
        html_content = html_content.replace(
            "<!-- TASK_SEO_PLACEHOLDER -->",
            f'<link rel="canonical" href="{base_url}/task">',
        )
        return HTMLResponse(content=_inject_static_layout(html_content))
    
    title_suffix = f" | AutoRig task {task_id[:8]}"
    compact_task_title = re.sub(r"\s+", " ", task_title).strip() or "Rigged 3D model"
    max_task_title_len = max(24, 70 - len(title_suffix))
    if len(compact_task_title) > max_task_title_len:
        compact_task_title = compact_task_title[: max_task_title_len - 3].rstrip() + "..."
    task_page_title = f"{compact_task_title}{title_suffix}"
    safe_task_page_title = html.escape(task_page_title, quote=True)
    safe_task_heading = html.escape(compact_task_title)
    task_meta_description = re.sub(r"\s+", " ", task_description).strip()
    if len(task_meta_description) > 170:
        task_meta_description = task_meta_description[:167].rsplit(" ", 1)[0].rstrip(".,;:-") + "..."
    safe_task_meta_description = html.escape(task_meta_description, quote=True)
    keywords_meta = ""
    if task_keywords:
        safe_keywords = html.escape(", ".join(task_keywords[:24]), quote=True)
        keywords_meta = f'\n    <meta name="keywords" content="{safe_keywords}">'
    json_ld = ""
    if task.status == "done":
        creative_work = {
            "@context": "https://schema.org",
            "@type": "CreativeWork",
            "name": task_page_title[:200],
            "description": task_description[:2000],
            "url": task_url,
            "image": f"{base_url}/api/thumb/{task_id}",
            "thumbnailUrl": f"{base_url}/api/thumb/{task_id}",
            "mainEntityOfPage": task_url,
            "creator": {"@type": "Organization", "name": "AutoRig.online"},
            "isFamilyFriendly": True,
        }
        if task.created_at:
            creative_work["dateCreated"] = task.created_at.isoformat()
        if task.updated_at:
            creative_work["dateModified"] = task.updated_at.isoformat()
        if task_keywords:
            creative_work["keywords"] = ", ".join(task_keywords[:24])
        if has_video:
            creative_work["associatedMedia"] = {
                "@type": "VideoObject",
                "name": task_page_title[:200],
                "description": task_description[:2000],
                "thumbnailUrl": f"{base_url}/api/thumb/{task_id}",
                "contentUrl": f"{base_url}/api/video/{task_id}",
                "uploadDate": (task.updated_at or task.created_at or datetime.utcnow()).isoformat(),
            }
        json_ld = f'\n    <script type="application/ld+json">{json.dumps(creative_work, ensure_ascii=False)}</script>'
    standard_seo_tags = f'<meta name="description" content="{safe_task_meta_description}">{keywords_meta}{json_ld}'

    # Build OG meta tags
    og_tags = f'''
    <!-- Open Graph / Telegram / Social -->
    <meta property="og:type" content="{'video.other' if has_video else 'website'}">
    <meta property="og:url" content="{task_url}">
    <meta property="og:title" content="{safe_task_page_title}">
    <meta property="og:description" content="{safe_task_meta_description}">
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
    <meta name="twitter:title" content="{safe_task_page_title}">
    <meta name="twitter:description" content="{safe_task_meta_description}">
    '''
    
    # Mark as indexable, inject canonical and OG/Twitter tags for valid tasks
    html_content = html_content.replace(
        '<meta name="robots" content="noindex, nofollow">',
        '<meta name="robots" content="index, follow, max-image-preview:large, max-video-preview:-1">',
    )
    html_content = html_content.replace(
        "<!-- TASK_SEO_PLACEHOLDER -->",
        f'{standard_seo_tags}\n    <link rel="canonical" href="{task_url}">\n    {og_tags}',
    )
    
    # Update <title> tag to be dynamic
    html_content = html_content.replace(
        '<title>Task Progress | AutoRig.online</title>',
        f'<title>{safe_task_page_title}</title>'
    )
    html_content = html_content.replace(
        '<h2 data-i18n="task_title" class="task-status-header-title">AutoRig task</h2>',
        f'<h1 class="task-status-header-title" id="task-seo-heading">{safe_task_heading}</h1>',
    )
    
    return HTMLResponse(content=_inject_static_layout(html_content))


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
    return _static_html_response("admin.html")


@app.get("/admin/workers")
async def admin_workers_page(user: Optional[User] = Depends(get_current_user)):
    """Serve admin workers page (dedicated)."""
    if not user or not is_admin_email(user.email):
        return RedirectResponse(url="/auth/login")
    return _static_html_response("admin-workers.html")


@app.get("/gallery")
async def gallery_page():
    """Serve Gallery page"""
    return _static_html_response("gallery.html")


@app.get("/dashboard")
async def dashboard_page():
    """User dashboard: notification settings."""
    return _static_html_response("dashboard.html")


@app.get("/guides")
async def guides_page():
    """Serve Guides page"""
    return _static_html_response("guides.html")


@app.get("/buy-credits")
async def buy_credits_page():
    """Serve Buy Credits page"""
    return _static_html_response("buy-credits.html")


@app.get("/blender-plugin")
async def blender_plugin_page():
    """Native Blender plugin landing page."""
    return _static_html_response("blender-plugin.html")


@app.get("/animal-rig")
async def animal_rig_page():
    """AutoRig V2 animal and non-humanoid rigging landing page."""
    return _static_html_response("animal-rig.html")


@app.get("/rig-animals", include_in_schema=False)
async def redirect_rig_animals_page():
    return RedirectResponse(url="/animal-rig", status_code=301)


@app.get("/animal-rig-animation", include_in_schema=False)
async def redirect_animal_rig_animation_page():
    return RedirectResponse(url="/animal-rig", status_code=301)


@app.get("/developers")
async def developers_page():
    """API documentation and key management for developers."""
    return _static_html_response("developers.html")


@app.get("/payment/success")
async def payment_success_page():
    """Serve payment success info page (no credit logic here)."""
    return _static_html_response("payment-success.html")


# =============================================================================
# SEO Landing Pages
# =============================================================================

# Format-specific pages
@app.get("/glb-auto-rig")
async def glb_auto_rig_page():
    """GLB auto-rigging landing page"""
    return _static_html_response("glb-auto-rig.html")


@app.get("/fbx-auto-rig")
async def fbx_auto_rig_page():
    """FBX auto-rigging landing page"""
    return _static_html_response("fbx-auto-rig.html")


@app.get("/obj-auto-rig")
async def obj_auto_rig_page():
    """OBJ auto-rigging landing page"""
    return _static_html_response("obj-auto-rig.html")


# Info pages
@app.get("/how-it-works")
async def how_it_works_page():
    """How it works page"""
    return _static_html_response("how-it-works.html")


@app.get("/faq")
async def faq_page():
    """FAQ page"""
    return _static_html_response("faq.html")


@app.get("/terms")
async def terms_of_use_page():
    """Terms of Use (website)."""
    return _static_html_response("terms-of-use.html")


@app.get("/user-agreement")
async def user_agreement_page():
    """User Agreement (content license, previews, promotional use)."""
    return _static_html_response("user-agreement.html")


@app.get("/guides")
async def guides_page():
    """Guides page"""
    return _static_html_response("guides.html")


@app.get("/t-pose-rig")
async def t_pose_rig_page():
    """T-pose rig page"""
    return _static_html_response("t-pose-rig.html")


# Mixamo alternative pages (4 languages)
@app.get("/mixamo-alternative")
async def mixamo_alternative_page():
    return _static_html_response("mixamo-alternative.html")


@app.get("/mixamo-alternative-ru")
async def mixamo_alternative_ru_page():
    return _static_html_response("mixamo-alternative-ru.html")


@app.get("/mixamo-alternative-zh")
async def mixamo_alternative_zh_page():
    return _static_html_response("mixamo-alternative-zh.html")


@app.get("/mixamo-alternative-hi")
async def mixamo_alternative_hi_page():
    return _static_html_response("mixamo-alternative-hi.html")


# Rig GLB for Unity pages (4 languages)
@app.get("/rig-glb-unity")
async def rig_glb_unity_page():
    return _static_html_response("rig-glb-unity.html")


@app.get("/rig-glb-unity-ru")
async def rig_glb_unity_ru_page():
    return _static_html_response("rig-glb-unity-ru.html")


@app.get("/rig-glb-unity-zh")
async def rig_glb_unity_zh_page():
    return _static_html_response("rig-glb-unity-zh.html")


@app.get("/rig-glb-unity-hi")
async def rig_glb_unity_hi_page():
    return _static_html_response("rig-glb-unity-hi.html")


# Rig FBX for Unreal pages (4 languages)
@app.get("/rig-fbx-unreal")
async def rig_fbx_unreal_page():
    return _static_html_response("rig-fbx-unreal.html")


@app.get("/rig-fbx-unreal-ru")
async def rig_fbx_unreal_ru_page():
    return _static_html_response("rig-fbx-unreal-ru.html")


@app.get("/rig-fbx-unreal-zh")
async def rig_fbx_unreal_zh_page():
    return _static_html_response("rig-fbx-unreal-zh.html")


@app.get("/rig-fbx-unreal-hi")
async def rig_fbx_unreal_hi_page():
    return _static_html_response("rig-fbx-unreal-hi.html")


# GLB vs FBX comparison pages (4 languages)
@app.get("/glb-vs-fbx")
async def glb_vs_fbx_page():
    return _static_html_response("glb-vs-fbx.html")


@app.get("/glb-vs-fbx-ru")
async def glb_vs_fbx_ru_page():
    return _static_html_response("glb-vs-fbx-ru.html")


@app.get("/glb-vs-fbx-zh")
async def glb_vs_fbx_zh_page():
    return _static_html_response("glb-vs-fbx-zh.html")


@app.get("/glb-vs-fbx-hi")
async def glb_vs_fbx_hi_page():
    return _static_html_response("glb-vs-fbx-hi.html")


# T-pose vs A-pose comparison pages (4 languages)
@app.get("/t-pose-vs-a-pose")
async def t_pose_vs_a_pose_page():
    return _static_html_response("t-pose-vs-a-pose.html")


@app.get("/t-pose-vs-a-pose-ru")
async def t_pose_vs_a_pose_ru_page():
    return _static_html_response("t-pose-vs-a-pose-ru.html")


@app.get("/t-pose-vs-a-pose-zh")
async def t_pose_vs_a_pose_zh_page():
    return _static_html_response("t-pose-vs-a-pose-zh.html")


@app.get("/t-pose-vs-a-pose-hi")
async def t_pose_vs_a_pose_hi_page():
    return _static_html_response("t-pose-vs-a-pose-hi.html")


# Animation retargeting pages (4 languages)
@app.get("/animation-retargeting")
async def animation_retargeting_page():
    return _static_html_response("animation-retargeting.html")


@app.get("/animation-retargeting-ru")
async def animation_retargeting_ru_page():
    return _static_html_response("animation-retargeting-ru.html")


@app.get("/animation-retargeting-zh")
async def animation_retargeting_zh_page():
    return _static_html_response("animation-retargeting-zh.html")


@app.get("/animation-retargeting-hi")
async def animation_retargeting_hi_page():
    return _static_html_response("animation-retargeting-hi.html")


@app.get("/face-rig-animation")
async def face_rig_animation_page():
    return _static_html_response("face-rig-animation.html")


@app.get("/face-rig-animation-ru")
async def face_rig_animation_ru_page():
    return _static_html_response("face-rig-animation-ru.html")


@app.get("/face-rig-animation-zh")
async def face_rig_animation_zh_page():
    return _static_html_response("face-rig-animation-zh.html")


@app.get("/face-rig-animation-hi")
async def face_rig_animation_hi_page():
    return _static_html_response("face-rig-animation-hi.html")


# Auto-rig OBJ pages (4 languages)
@app.get("/auto-rig-obj")
async def auto_rig_obj_page():
    return _static_html_response("auto-rig-obj.html")


@app.get("/auto-rig-obj-ru")
async def auto_rig_obj_ru_page():
    return _static_html_response("auto-rig-obj-ru.html")


@app.get("/auto-rig-obj-zh")
async def auto_rig_obj_zh_page():
    return _static_html_response("auto-rig-obj-zh.html")


@app.get("/auto-rig-obj-hi")
async def auto_rig_obj_hi_page():
    return _static_html_response("auto-rig-obj-hi.html")


def _make_rig_article_endpoint(rig_key: str, lang: str):
    async def rig_article_endpoint(db: AsyncSession = Depends(get_db)):
        return await _rig_article_response(rig_key, lang, db)

    rig_article_endpoint.__name__ = f"{rig_key}_auto_rig_{lang}_page"
    return rig_article_endpoint


for _rig_key in GALLERY_RIG_TYPES:
    for _lang in RIG_ARTICLE_LANGS:
        app.add_api_route(
            _rig_article_path(_rig_key, _lang),
            _make_rig_article_endpoint(_rig_key, _lang),
            methods=["GET", "HEAD"],
            response_class=HTMLResponse,
            include_in_schema=False,
        )


# Sitemap and robots.txt
@app.head("/sitemap.xml")
@app.get("/sitemap.xml")
async def sitemap_index(db: AsyncSession = Depends(get_db)):
    """
    Sitemap index: static marketing pages + SEO-gated public task urlsets by part.
    """
    from seo_gallery import (
        build_sitemap_index_xml,
        gallery_sitemap_index_part_count,
        video_sitemap_entry_count,
    )

    base = (APP_URL or "https://autorig.online").rstrip("/")
    child_locs: List[Tuple[str, Optional[datetime]]] = [(f"{base}/sitemap/pages.xml", None)]
    n_parts = await gallery_sitemap_index_part_count(db)
    for p in range(n_parts):
        child_locs.append((f"{base}/sitemap/gallery/part/{p}.xml", None))
    n_video_entries = await video_sitemap_entry_count(db)
    if n_video_entries > 0:
        child_locs.append((f"{base}/sitemap/videos.xml", None))
    xml = build_sitemap_index_xml(base, child_locs)
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.head("/sitemap/pages.xml")
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


@app.head("/sitemap-mirror.xml")
@app.get("/sitemap-mirror.xml")
async def sitemap_mirror(db: AsyncSession = Depends(get_db)):
    """
    Mirror sitemap index from the latest daily refresh (`backend/scripts/daily_sitemap_refresh.py`).
    Falls back to live /sitemap.xml when mirror file is absent.
    """
    mirror_path = BASE_DIR / "backend" / "data" / "sitemap_generated" / "sitemap.xml"
    if mirror_path.is_file():
        return FileResponse(str(mirror_path), media_type="application/xml; charset=utf-8")
    return await sitemap_index(db)


@app.head("/sitemap/gallery/part/{part}.xml")
@app.get("/sitemap/gallery/part/{part}.xml")
async def sitemap_gallery_indexing_part(part: int, db: AsyncSession = Depends(get_db)):
    """Chunk (max 50) of SEO-gated public /task?id={task_id} URLs."""
    from seo_gallery import build_urlset_xml, gallery_sitemap_urls_for_indexing_part

    if part < 0:
        raise HTTPException(status_code=404, detail="Invalid sitemap chunk")
    base = (APP_URL or "https://autorig.online").rstrip("/")
    urls = await gallery_sitemap_urls_for_indexing_part(db, part)
    if not urls:
        raise HTTPException(status_code=404, detail="Empty sitemap chunk")
    xml = build_urlset_xml(base, urls, changefreq="daily", priority="0.75")
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.head("/sitemap/gallery/all/part/{part}.xml")
@app.get("/sitemap/gallery/all/part/{part}.xml")
async def sitemap_gallery_public_part(part: int, db: AsyncSession = Depends(get_db)):
    """Diagnostic full public chunk (max 50) of /task?id={task_id} URLs, without SEO gate."""
    from seo_gallery import build_urlset_xml, gallery_sitemap_urls_for_all_part

    if part < 0:
        raise HTTPException(status_code=404, detail="Invalid sitemap chunk")
    base = (APP_URL or "https://autorig.online").rstrip("/")
    urls = await gallery_sitemap_urls_for_all_part(db, part)
    if not urls:
        raise HTTPException(status_code=404, detail="Empty sitemap chunk")
    xml = build_urlset_xml(base, urls, changefreq="daily", priority="0.45")
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.head("/sitemap/gallery/indexing/part/{part}.xml")
@app.get("/sitemap/gallery/indexing/part/{part}.xml")
async def sitemap_gallery_indexing_part_only(part: int, db: AsyncSession = Depends(get_db)):
    """SEO-gated chunk (max 50) of /task?id={task_id} URLs."""
    from seo_gallery import build_urlset_xml, gallery_sitemap_urls_for_indexing_part

    if part < 0:
        raise HTTPException(status_code=404, detail="Invalid sitemap chunk")
    base = (APP_URL or "https://autorig.online").rstrip("/")
    urls = await gallery_sitemap_urls_for_indexing_part(db, part)
    if not urls:
        raise HTTPException(status_code=404, detail="Empty sitemap chunk")
    xml = build_urlset_xml(base, urls, changefreq="daily", priority="0.75")
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.head("/sitemap/videos.xml")
@app.get("/sitemap/videos.xml")
async def sitemap_videos(db: AsyncSession = Depends(get_db)):
    """Google Video sitemap for public model pages with uploaded YouTube previews."""
    from seo_gallery import build_video_sitemap_xml, video_sitemap_entries

    base = (APP_URL or "https://autorig.online").rstrip("/")
    entries = await video_sitemap_entries(db)
    if not entries:
        raise HTTPException(status_code=404, detail="Empty video sitemap")
    xml = build_video_sitemap_xml(base, entries)
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@app.get("/robots.txt")
async def robots():
    """Serve robots.txt for crawlers"""
    return FileResponse(
        str(STATIC_DIR / "robots.txt"),
        media_type="text/plain"
    )


# Search engine verification files
INDEXNOW_KEY = "793f81f63218433f87e43c0afd353c14"
INDEXNOW_KEY_FILE = f"{INDEXNOW_KEY}.txt"


@app.head(f"/{INDEXNOW_KEY_FILE}")
@app.get(f"/{INDEXNOW_KEY_FILE}")
async def indexnow_key_file():
    """Serve the IndexNow API key from the site root."""
    return FileResponse(
        str(STATIC_DIR / INDEXNOW_KEY_FILE),
        media_type="text/plain",
    )


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
