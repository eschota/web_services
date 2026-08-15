"""
Task management for AutoRig Online
"""
import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import parse_qs, quote, urlparse
import httpx

from sqlalchemy import select, desc, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task, User, AnonSession, AsyncSessionLocal
from config import APP_URL
from viewer_environment import build_viewer_environment_from_settings
from worker_progress_contract import latest_terminal_failure_reason
from task_timeout_contract import task_hard_timeout_reference
from animal_submission_policy import animal_detection_accepted
from worker_artifact_urls import (
    canonical_worker_artifact_url,
    viewer_artifact_kind,
)
from workers import (
    select_best_worker,
    send_task_to_worker,
    send_fbx_to_glb,
    check_urls_batch,
    check_video_availability,
    get_worker_base_url,
    get_configured_workers,
    normalize_task_type,
    GlobalQueueStatus,
    get_worker_active_lookup,
    lookup_worker_queue_entry,
    task_visible_on_worker_refs,
    find_worker_queue_status_for_task,
    quarantine_worker,
)


# =============================================================================
# Helper Functions
# =============================================================================
PREFLIGHT_RENDER_DIR = Path("/var/autorig/preflight-renders")
RIG_V2_WORKER_ANIMAL_TYPES = {
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
}
RIG_V2_ANIMAL_DECISION_THRESHOLD = 0.62
SOURCE_PREFLIGHT_TIMEOUT_SECONDS = 8.0
SOURCE_PREFLIGHT_MAX_ATTEMPTS = 3
SOURCE_PREFLIGHT_BACKOFF_SECONDS = (60, 300)
VIEWER_ARTIFACT_PROBE_TIMEOUT_SECONDS = 4.0
VIEWER_RECONCILE_BACKOFF_SECONDS = 300.0
_viewer_reconcile_last_attempt: Dict[str, float] = {}


def _source_format_error(input_url: str, prefix: bytes, content_type: str) -> Optional[str]:
    path = urlparse(input_url or "").path.lower()
    probe = prefix.lstrip()
    content_type_lc = (content_type or "").lower()
    if "text/html" in content_type_lc or probe.startswith((b"<!doctype html", b"<html")):
        return "source returned HTML instead of a 3D asset"
    if path.endswith(".glb") and not prefix.startswith(b"glTF"):
        return "source is not a valid binary glTF file"
    if path.endswith(".fbx") and not (
        prefix.startswith(b"Kaydara FBX Binary") or probe.startswith(b"; FBX")
    ):
        return "source is not a valid FBX file"
    if not Path(path).suffix and not (
        prefix.startswith(b"glTF")
        or prefix.startswith(b"Kaydara FBX Binary")
        or probe.startswith((b"; FBX", b"#", b"mtllib ", b"o ", b"v "))
    ):
        return "source format could not be recognized as GLB, FBX, or OBJ"
    return None


async def preflight_task_source(input_url: Optional[str]) -> Tuple[bool, str, bool]:
    """
    Read only the first bytes of the source before reserving a worker.

    Returns (available, detail, permanent_error). Network/HTTP availability
    failures are retryable; an invalid payload/format is terminal immediately.
    """
    url = (input_url or "").strip()
    if not url:
        return False, "source URL is missing", True
    try:
        timeout = httpx.Timeout(
            SOURCE_PREFLIGHT_TIMEOUT_SECONDS,
            connect=SOURCE_PREFLIGHT_TIMEOUT_SECONDS,
        )
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            async with client.stream(
                "GET",
                url,
                headers={"Range": "bytes=0-63", "Accept-Encoding": "identity"},
            ) as response:
                if response.status_code not in (200, 206):
                    return False, f"source returned HTTP {response.status_code}", False
                prefix = b""
                async for chunk in response.aiter_bytes():
                    prefix += chunk
                    if len(prefix) >= 64:
                        break
                prefix = prefix[:64]
                if not prefix:
                    return False, "source returned an empty response", False
                size = None
                content_range = response.headers.get("content-range") or ""
                match = re.search(r"/(\d+)\s*$", content_range)
                if match:
                    size = int(match.group(1))
                elif response.status_code == 200:
                    raw_length = response.headers.get("content-length")
                    if raw_length and raw_length.isdigit():
                        size = int(raw_length)
                if size == 0:
                    return False, "source file is empty", True
                format_error = _source_format_error(
                    url,
                    prefix,
                    response.headers.get("content-type") or "",
                )
                if format_error:
                    return False, format_error, True
                return True, "", False
    except httpx.TimeoutException:
        return False, f"source did not respond within {SOURCE_PREFLIGHT_TIMEOUT_SECONDS:g}s", False
    except httpx.HTTPError as exc:
        return False, f"source request failed: {exc.__class__.__name__}", False
    except Exception as exc:
        return False, f"source check failed: {exc.__class__.__name__}", False


async def _apply_source_preflight_failure(
    db: AsyncSession,
    task: Task,
    detail: str,
    *,
    permanent: bool,
) -> str:
    now = datetime.utcnow()
    attempts = int(getattr(task, "source_attempt_count", 0) or 0) + 1
    task.source_attempt_count = attempts
    task.updated_at = now
    if permanent or attempts >= SOURCE_PREFLIGHT_MAX_ATTEMPTS:
        task.status = "error"
        task.source_next_retry_at = None
        task.error_message = f"Source asset unavailable: {detail}."
        await db.commit()
        await db.refresh(task)
        _schedule_task_error_notification(task.id)
        print(
            f"[Source Preflight] Task {task.id} failed after {attempts} attempt(s): {detail}"
        )
        return task.error_message

    backoff_index = min(attempts - 1, len(SOURCE_PREFLIGHT_BACKOFF_SECONDS) - 1)
    delay_seconds = SOURCE_PREFLIGHT_BACKOFF_SECONDS[backoff_index]
    task.status = "created"
    task.source_next_retry_at = now + timedelta(seconds=delay_seconds)
    task.error_message = None
    await db.commit()
    await db.refresh(task)
    print(
        f"[Source Preflight] Task {task.id} retry {attempts}/"
        f"{SOURCE_PREFLIGHT_MAX_ATTEMPTS} in {delay_seconds}s: {detail}"
    )
    return f"Source preflight retry scheduled: {detail}"


def _is_transient_worker_dispatch_error(error: Optional[str]) -> bool:
    msg = (error or "").strip().lower()
    if not msg:
        return False
    if re.search(r"http\s+(429|5\d\d)\b", msg):
        return True
    return any(
        marker in msg
        for marker in (
            "timeout",
            "timed out",
            "connection",
            "connect",
            "network",
            "temporar",
            "read error",
            "server disconnected",
        )
    )


def _animal_detection_confident_enough(detection: Any) -> bool:
    return animal_detection_accepted(
        detection,
        default_threshold=RIG_V2_ANIMAL_DECISION_THRESHOLD,
    )


def _viewer_environment_for_task(task: Task) -> Optional[Dict[str, Any]]:
    return build_viewer_environment_from_settings(
        getattr(task, "viewer_settings", None),
        app_url=APP_URL,
    )


def _animal_type_from_detection_meta(detection: Any) -> Optional[str]:
    if not isinstance(detection, dict):
        return None
    for key in ("animal_type", "animal_type_string", "selected_animal_type", "selected_animal_type_string"):
        animal = str(detection.get(key) or "").strip().lower()
        if animal in RIG_V2_WORKER_ANIMAL_TYPES:
            return animal

    best_type = ""
    best_score = 0.0
    scores = detection.get("scores")
    if isinstance(scores, dict):
        for key, raw_score in scores.items():
            animal = str(key or "").strip().lower()
            if animal not in RIG_V2_WORKER_ANIMAL_TYPES:
                continue
            try:
                score = float(raw_score)
            except Exception:
                score = 0.0
            if score > best_score:
                best_type = animal
                best_score = score

    results = detection.get("results")
    if isinstance(results, list):
        tally: Dict[str, float] = {}
        for result in results:
            if not isinstance(result, dict):
                continue
            animal = str(result.get("animal_type_string") or result.get("animal_type") or "").strip().lower()
            if animal not in RIG_V2_WORKER_ANIMAL_TYPES:
                continue
            try:
                confidence = float(result.get("confidence_float") or result.get("confidence") or 0.5)
            except Exception:
                confidence = 0.5
            tally[animal] = tally.get(animal, 0.0) + max(0.05, min(1.0, confidence))
        for animal, score in tally.items():
            if score > best_score:
                best_type = animal
                best_score = score

    return best_type or None


def _task_notification_theme_meta(task: Task) -> dict:
    try:
        settings = json.loads(task.viewer_settings or "{}")
        if not isinstance(settings, dict):
            settings = {}
    except Exception:
        settings = {}
    detection = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None
    theme = settings.get("viewer_theme_selection") if isinstance(settings, dict) else None
    source_preview_url = str(settings.get("source_preview_url") or "").strip() if isinstance(settings, dict) else ""
    animal_type = ""
    detector_text = ""
    if isinstance(detection, dict):
        accepted = _animal_detection_confident_enough(detection)
        candidate = str(
            detection.get("candidate_animal_type_string")
            or detection.get("selected_type_string")
            or detection.get("animal_type")
            or detection.get("animal_type_string")
            or (detection.get("first_result") or {}).get("animal_type_string")
            or ""
        ).strip().lower()
        try:
            weight = float(detection.get("animal_decision_weight_float") or 0.0)
        except Exception:
            weight = 0.0
        try:
            threshold = float(detection.get("animal_decision_threshold_float") or RIG_V2_ANIMAL_DECISION_THRESHOLD)
        except Exception:
            threshold = RIG_V2_ANIMAL_DECISION_THRESHOLD
        votes = detection.get("selected_votes_int")
        views = detection.get("view_count_int")
        if candidate or "animal_decision_weight_float" in detection:
            vote_suffix = ""
            try:
                if votes is not None and views is not None:
                    vote_suffix = f" v={int(votes)}/{int(views)}"
            except Exception:
                vote_suffix = ""
            verdict = "accepted" if accepted else "rejected"
            detector_text = f"AI {candidate or '?'} w={weight:.2f}/{threshold:.2f}{vote_suffix} {verdict}"
        if accepted:
            animal_type = str(
                detection.get("animal_type")
                or detection.get("animal_type_string")
                or ""
            ).strip().lower()
    theme_id = str(theme.get("theme_id") or "").strip() if isinstance(theme, dict) else ""
    theme_names = {
        "dog_park_yard": "Pet Park Yard",
        "studio_white_softbox": "White Photo Studio",
        "alien_planet": "Alien Planet",
        "sci_fi_hangar": "Sci-Fi Hangar",
        "ranch_farmyard": "Ranch Farmyard",
        "pine_forest_trail": "Pine Forest Trail",
        "savanna_acacia_plain": "Savanna Acacia Plain",
        "crystal_cavern": "Crystal Cavern",
        "ancient_ruins": "Ancient Marble Ruins",
        "jungle_temple_ruins": "Jungle Temple Ruins",
    }
    title_bits = []
    if animal_type:
        title_bits.append(animal_type.replace("_", " ").title())
    elif task.input_type:
        title_bits.append(str(task.input_type).replace("_", " ").title())
    if theme_id:
        title_bits.append(theme_names.get(theme_id, theme_id.replace("_", " ").title()))
    poster_path = PREFLIGHT_RENDER_DIR / f"{task.id}.jpg"
    return {
        "title": " · ".join(title_bits),
        "theme_id": theme_id,
        "theme_name": theme_names.get(theme_id, theme_id.replace("_", " ").title()) if theme_id else "",
        "poster_path": str(poster_path) if poster_path.is_file() else "",
        "detector_text": detector_text,
        "source_preview_url": source_preview_url,
    }


def get_task_progress_reference_time(task: Task) -> Optional[datetime]:
    """
    Return last *real* progress timestamp for stale detection.
    Do not use updated_at, because it is bumped by periodic polling.
    """
    return task.last_progress_at or task.created_at


def get_task_no_progress_minutes(task: Task, now: Optional[datetime] = None) -> float:
    """Minutes since real progress start/reference."""
    ref = get_task_progress_reference_time(task)
    if not ref:
        return 0.0
    now_ts = now or datetime.utcnow()
    return max(0.0, (now_ts - ref).total_seconds() / 60.0)


def find_file_by_pattern(ready_urls: List[str], pattern: str, quality: str = "100k") -> Optional[str]:
    """
    Find a file in ready_urls matching the pattern in the specified quality folder.
    
    Args:
        ready_urls: List of ready file URLs
        pattern: File extension or pattern to match (e.g., ".html", ".max", ".ma")
        quality: Quality folder to search in ("100k", "10k", "1k")
    
    Returns:
        First matching URL or None
    """
    quality_folder = f"_{quality}/"
    
    for url in ready_urls:
        # Check if URL contains the quality folder and matches the pattern
        if quality_folder in url and pattern in url:
            return url
    
    # Fallback: try other qualities if 100k not found
    if quality == "100k":
        for fallback_quality in ["10k", "1k"]:
            fallback_folder = f"_{fallback_quality}/"
            for url in ready_urls:
                if fallback_folder in url and pattern in url:
                    return url
    
    return None


def resolve_prepared_glb_source_url(task: Task) -> Optional[str]:
    """
    Best URL for Auto Convert input: rigged prepared GLB (same sources as /api/task/.../prepared.glb).
    """
    for url in task.ready_urls or []:
        u = (url or "").strip()
        if "_model_prepared.glb" in u.lower():
            return u
    if task.guid and task.worker_api:
        wb = get_worker_base_url(task.worker_api)
        return f"{wb}/converter/glb/{task.guid}/{task.guid}_model_prepared.glb"
    if task.fbx_glb_output_url and task.fbx_glb_ready:
        return (task.fbx_glb_output_url or "").strip() or None
    return None


def _is_fbx_url(input_url: str) -> bool:
    """Return True if input_url path ends with .fbx (case-insensitive), ignoring query/fragment."""
    try:
        path = urlparse(input_url).path or ""
    except Exception:
        path = input_url or ""
    return path.lower().endswith(".fbx")


async def _head_is_ready(url: str) -> bool:
    """Lightweight availability check for a single URL (HEAD 200)."""
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.head(url, timeout=5.0, follow_redirects=True)
            return resp.status_code == 200
    except Exception:
        return False


async def _start_fbx_preconvert_async(task_id: str, first_worker_url: str, input_url: str) -> None:
    """
    Run FBX->GLB pre-conversion asynchronously after task creation/restart.
    Writes fbx_glb_* fields into the task once the worker responds.
    """
    last_error = None

    async with AsyncSessionLocal() as db:
        configured_workers = await get_configured_workers(db)
        candidate_workers = []
        if first_worker_url:
            candidate_workers.append(first_worker_url)
        candidate_workers += [w for w in configured_workers if w and w != first_worker_url]

        task = await get_task_by_id(db, task_id)
        if not task:
            return

        # If task already has output_url or is terminal, don't redo
        if task.status in ("done", "error") or task.fbx_glb_output_url:
            return

        for candidate in candidate_workers:
            res = await send_fbx_to_glb(candidate, input_url)
            if res.success:
                task.worker_api = candidate
                task.fbx_glb_model_name = res.model_name
                task.fbx_glb_output_url = res.output_url
                # If worker returns output_url, assume file is ready (no HEAD/GET checks).
                task.fbx_glb_ready = True
                task.fbx_glb_error = None
                task.updated_at = datetime.utcnow()
                await db.commit()
                await db.refresh(task)

                # Start main pipeline immediately (do not wait for next poll).
                if not task.worker_task_id and task.fbx_glb_output_url:
                    result = await send_task_to_worker(
                        task.worker_api,
                        task.fbx_glb_output_url,
                        task.input_type or "t_pose",
                        pipeline_kind="rig",
                        viewer_environment=_viewer_environment_for_task(task),
                    )
                    if not result.success:
                        task.status = "error"
                        task.error_message = result.error
                        task.updated_at = datetime.utcnow()
                        await db.commit()
                        _schedule_task_error_notification(task.id)
                        return

                    task.worker_task_id = result.task_id
                    task.progress_page = result.progress_page
                    task.guid = result.guid
                    task.output_urls = result.output_urls
                    await persist_validated_worker_viewer_artifacts(task, result)
                    task.total_count = len(result.output_urls)
                    task.status = "processing"
                    task.last_progress_at = datetime.utcnow()
                    task.updated_at = datetime.utcnow()
                    await db.commit()
                return

            last_error = res.error

            # Endpoint missing? try next worker
            if last_error and "HTTP 404" in last_error:
                continue

            # For other errors (timeouts, 5xx), still try other workers
            continue

        # No worker succeeded
        task.status = "error"
        task.fbx_glb_error = last_error or "FBX->GLB conversion failed"
        task.error_message = task.fbx_glb_error
        task.updated_at = datetime.utcnow()
        await db.commit()
        _schedule_task_error_notification(task.id)


# =============================================================================
# Task Creation
# =============================================================================
async def create_conversion_task(
    db: AsyncSession,
    input_url: str,
    task_type: str,
    owner_type: str,
    owner_id: str,
    *,
    created_via_api: bool = False,
    pipeline_kind: str = "rig",
    input_bytes: Optional[int] = None,
) -> Tuple[Optional[Task], Optional[str]]:
    """
    Create a new conversion task.
    Returns: (task, error_message)
    """
    task_type = normalize_task_type(task_type)
    pk = (pipeline_kind or "rig").strip().lower()
    if pk not in ("rig", "convert", "generate"):
        pk = "rig"

    from main import ensure_disk_headroom_for_new_task, enforce_task_cache_max_size

    await enforce_task_cache_max_size(db)
    await ensure_disk_headroom_for_new_task(db)

    # Create task record
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id,
        owner_type=owner_type,
        owner_id=owner_id,
        input_url=input_url,
        input_type=task_type,
        status="created",
        created_via_api=created_via_api,
        pipeline_kind=pk,
        input_bytes=input_bytes,
    )

    db.add(task)
    await db.commit()
    await db.refresh(task)
    
    # Note: Telegram notification moved to start_task_on_worker (when we have progress_page)
    
    return task, None


async def start_task_on_worker(db: AsyncSession, task: Task, worker_url: str) -> Tuple[Task, Optional[str]]:
    """
    Start a queued (status=created) task on a specific worker.
    Workers accept GLB, FBX, OBJ directly via input_url.
    Returns: (task, error_message)
    """
    pk = getattr(task, "pipeline_kind", None) or "rig"
    if pk not in ("rig", "convert"):
        pk = "rig"
    task_type_for_worker = task.input_type or "t_pose"
    animal_type = None
    body_topology = None
    mode = None
    transform_params = None
    animal_semantic_markers = None
    if str(task_type_for_worker).strip().lower() == "animal":
        try:
            settings = json.loads(task.viewer_settings or "{}")
            detection = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None
            if isinstance(detection, dict):
                if _animal_detection_confident_enough(detection):
                    animal_type = _animal_type_from_detection_meta(detection)
                    body_topology = str(detection.get("body_topology") or "").strip() or None
                    mode = str(detection.get("mode") or "").strip() or None
                    local_rotation = detection.get("local_rotation")
                    if isinstance(local_rotation, list) and len(local_rotation) == 3:
                        transform_params = {"local_rotation": local_rotation}
                    markers = detection.get("animal_semantic_markers")
                    if isinstance(markers, dict):
                        animal_semantic_markers = markers
                else:
                    animal_type = None
                    mode = None
        except Exception:
            animal_type = None
            mode = None
        if not animal_type:
            task.status = "error"
            task.worker_api = None
            task.error_message = (
                "Animal rig task is missing animal_type metadata. "
                "Please retry the upload so AI animal detection can finish."
            )
            task.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(task)
            _schedule_task_error_notification(task.id)
            return task, task.error_message

    source_ok, source_detail, source_permanent = await preflight_task_source(task.input_url)
    if not source_ok:
        error = await _apply_source_preflight_failure(
            db,
            task,
            source_detail,
            permanent=source_permanent,
        )
        return task, error

    # Reserve the worker only after all deterministic task metadata is valid.
    # Otherwise malformed metadata or an unreachable source looks like a
    # worker-side failure and can quarantine healthy capacity.
    task.worker_api = worker_url
    task.status = "processing"
    task.source_next_retry_at = None
    task.processing_started_at = datetime.utcnow()
    task.updated_at = task.processing_started_at
    await db.commit()
    await db.refresh(task)

    # Best-effort: generate LLM poster metadata from the pre-convert preview
    # (browser preflight render, else the renderfin turntable frame) so the
    # converter receives title/description/keywords/category. The worker writes
    # the full request into <model>_rig.json, so this drives animation selection
    # and the listing. Fully non-fatal and time-bounded: a slow/absent preview
    # or OpenAI outage must never delay or fail dispatch.
    poster_metadata = None
    try:
        from content_moderation import build_pre_convert_metadata_sync

        poster_metadata = await asyncio.wait_for(
            asyncio.to_thread(
                build_pre_convert_metadata_sync,
                task.id,
                task.input_url,
                task_type_for_worker,
            ),
            timeout=45,
        )
    except Exception as _meta_err:
        print(f"[PreConvertMeta] skipped for task {task.id}: {_meta_err}")
        poster_metadata = None
    if poster_metadata:
        print(
            f"[PreConvertMeta] task {task.id} ATTACHED pk={pk} "
            f"subcategory={poster_metadata.get('subcategory')} title={poster_metadata.get('title')!r}"
        )
    else:
        print(f"[PreConvertMeta] task {task.id} NO-METADATA pk={pk}")

    # Send task directly to worker (workers handle GLB, FBX, OBJ natively)
    result = await send_task_to_worker(
        worker_url,
        task.input_url,
        task_type_for_worker,
        transform_params=transform_params,
        pipeline_kind=pk,
        animal_type=animal_type,
        body_topology=body_topology,
        mode=mode,
        animal_semantic_markers=animal_semantic_markers,
        viewer_environment=_viewer_environment_for_task(task) if pk == "rig" else None,
        metadata=poster_metadata,
    )
    if not result.success:
        error = result.error or "Worker dispatch failed"
        if _is_transient_worker_dispatch_error(error):
            quarantine_worker(worker_url, reason=f"dispatch_failed:{error[:120]}")
            task.status = "created"
            task.worker_api = None
            task.worker_task_id = None
            task.progress_page = None
            task.guid = None
            task.output_urls = []
            task.ready_urls = []
            task.ready_count = 0
            task.total_count = 0
            task.video_ready = False
            task.video_url = None
            task.error_message = None
            task.processing_started_at = None
            task.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(task)
            print(f"[Tasks] Requeued {task.id} after transient worker dispatch failure on {worker_url}: {error}")
            return task, error

        task.status = "error"
        task.error_message = error
        task.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(task)
        _schedule_task_error_notification(task.id)
        return task, error

    # Persist the worker binding atomically with the successful dispatch
    # metadata. A concurrent stale-task reset can otherwise clear worker_api
    # after the reservation commit while the worker has already accepted the
    # job, leaving a processing task with a GUID but no worker URL.
    task.worker_api = worker_url
    task.worker_task_id = result.task_id
    task.progress_page = result.progress_page
    task.guid = result.guid
    task.output_urls = result.output_urls
    await persist_validated_worker_viewer_artifacts(task, result)
    task.total_count = len(result.output_urls)
    task.status = "processing"
    # Start stale timer from (re)dispatch moment, not from original task creation time.
    task.last_progress_at = datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)
    
    # Telegram notification (fire-and-forget) - now we have progress_page
    try:
        from telegram_bot import broadcast_new_task
        from sqlalchemy import update
        
        # Atomic check-and-set to prevent duplicate notifications
        now = datetime.utcnow()
        stmt = (
            update(Task)
            .where(Task.id == task.id)
            .where(Task.telegram_new_notified_at.is_(None))
            .values(telegram_new_notified_at=now)
        )
        res = await db.execute(stmt)
        await db.commit()
        
        if res.rowcount == 1:
            # Construct progress_page URL from worker_api and guid
            worker_base = get_worker_base_url(worker_url)
            progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
            notify_meta = _task_notification_theme_meta(task)
            print(f"[Tasks] Scheduling Telegram notification for new task {task.id}")
            asyncio.create_task(
                broadcast_new_task(
                    task.id,
                    task.input_url,
                    task.input_type,
                    progress_url,
                    via_api=bool(getattr(task, "created_via_api", False)),
                    title=notify_meta.get("title") or None,
                    theme_name=notify_meta.get("theme_name") or None,
                    poster_path=notify_meta.get("poster_path") or None,
                    detector_text=notify_meta.get("detector_text") or None,
                    source_preview_url=notify_meta.get("source_preview_url") or None,
                )
            )
        else:
            print(f"[Tasks] New task notification already sent for {task.id}, skipping")
            
    except Exception as e:
        print(f"[Telegram] Failed to notify new task: {e}")
        import traceback
        traceback.print_exc()
    
    return task, None


# =============================================================================
# Progress Checking
# =============================================================================
def _is_primary_worker_output(name: str) -> bool:
    """Files that make a task downloadable/finalizable across worker layouts."""
    n = (name or "").strip().lower()
    if not n or "_temp" in n or "initial_temp" in n:
        return False
    return (
        n.endswith("_model_prepared.glb")
        or n.endswith("_model_prepared_rigged.blend")
        or n.endswith("_rigged.blend")
        or n.endswith(".zip")
        or n.endswith("_video.mp4")
        or n.endswith("_video_small.mp4")
        or n.endswith("_video_poster.jpg")
        or n.endswith("_rig_preview.mp4")
        or n.endswith("_skeleton.json")
        or n.endswith("_all_animations.blend")
        or n.endswith("_all_animations_unity.fbx")
        or n.endswith("_hdrp.unitypackage")
    )


def _is_animal_task(task: Task) -> bool:
    return str(getattr(task, "input_type", "") or "").strip().lower() == "animal"


def _restore_worker_api_from_progress_page(task: Task) -> bool:
    """
    Recover worker_api for already-dispatched tasks when the DB row kept only
    progress_page. This lets existing progress/failure sync code use the worker.
    """
    if (getattr(task, "worker_api", None) or "").strip():
        return False
    progress_page = (getattr(task, "progress_page", None) or "").strip()
    if not progress_page:
        return False
    try:
        parsed = urlparse(progress_page)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return False
    if "/converter/glb/" not in parsed.path:
        return False

    task.worker_api = f"{parsed.scheme}://{parsed.netloc}/api-converter-glb"
    if not (getattr(task, "guid", None) or "").strip():
        match = re.search(r"/converter/glb/([0-9a-fA-F-]{36})(?:/|$)", parsed.path)
        if match:
            task.guid = match.group(1)
    return True


def _worker_outputs_look_complete(urls: List[str]) -> bool:
    names = [urlparse(u).path.rsplit("/", 1)[-1].lower() for u in urls or []]
    has_video = any(
        n.endswith("_rig_preview.mp4")
        or n.endswith("_video.mp4")
        or n.endswith("_video_small.mp4")
        for n in names
    )
    has_poster = any(n.endswith("_video_poster.jpg") for n in names)
    has_download = any(
        n.endswith("_all_animations_unity.fbx")
        or n.endswith("_hdrp.unitypackage")
        or n.endswith(".zip")
        for n in names
    )
    return has_video and has_poster and has_download


async def _fetch_concrete_worker_artifacts(
    task: Task,
) -> Tuple[List[str], Optional[str], Optional[str]]:
    """Recover downloadable and private viewer artifacts from model-files."""
    if not task.guid or not task.worker_api:
        return [], None, None
    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return [], None, None

    files_url = f"{worker_base.rstrip('/')}/api-converter-glb/model-files/{task.guid}"
    worker_root = f"{worker_base.rstrip('/')}/converter/glb"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(files_url, timeout=8.0)
        if resp.status_code != 200:
            return [], None, None
        data = resp.json() if resp.content else {}
    except Exception:
        return [], None, None

    urls: List[str] = []
    seen = set()
    viewer_prepared: Optional[str] = None
    viewer_animations: Optional[str] = None
    for folder_data in (data.get("folders") or {}).values():
        if not isinstance(folder_data, dict):
            continue
        for item in folder_data.get("files") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            rel_path = str(item.get("rel_path") or "")
            if not rel_path:
                continue
            url = f"{worker_root}/{task.guid}/{rel_path}"
            kind = viewer_artifact_kind(url)
            if kind == "prepared":
                viewer_prepared = viewer_prepared or canonical_worker_artifact_url(url)
                continue
            if kind == "animations":
                viewer_animations = viewer_animations or canonical_worker_artifact_url(url)
                continue
            if not _is_primary_worker_output(name):
                continue
            if url not in seen:
                seen.add(url)
                urls.append(url)
    return urls, viewer_prepared, viewer_animations


async def _fetch_concrete_worker_output_urls(task: Task) -> List[str]:
    """Backward-compatible downloadable-only model-files helper."""
    urls, _viewer_prepared, _viewer_animations = await _fetch_concrete_worker_artifacts(task)
    return urls


async def _fetch_worker_status_viewer_artifacts(
    task: Task,
) -> Tuple[Optional[str], Optional[str]]:
    """Read dedicated optional viewer URLs from the worker task-status contract."""
    worker_api = str(getattr(task, "worker_api", None) or "").strip()
    worker_task_id = str(getattr(task, "worker_task_id", None) or "").strip()
    if not worker_api or not worker_task_id:
        return None, None
    status_url = (
        f"{worker_api.rstrip('/')}/status/"
        f"{quote(worker_task_id, safe='')}"
    )
    try:
        async with httpx.AsyncClient(
            timeout=VIEWER_ARTIFACT_PROBE_TIMEOUT_SECONDS,
            follow_redirects=True,
        ) as client:
            response = await client.get(status_url)
        if response.status_code != 200:
            return None, None
        payload = response.json() if response.content else {}
        if not isinstance(payload, dict):
            return None, None
    except Exception:
        return None, None

    prepared = (
        payload.get("viewer_prepared_glb_url")
        or payload.get("viewerPreparedGlbUrl")
    )
    animations = (
        payload.get("viewer_animations_glb_url")
        or payload.get("viewerAnimationsGlbUrl")
    )
    return (
        canonical_worker_artifact_url(str(prepared)) if prepared else None,
        canonical_worker_artifact_url(str(animations)) if animations else None,
    )


async def _fetch_worker_completion_contract(task: Task) -> Optional[dict]:
    """Fetch the additive worker completion contract, if the worker exposes it."""
    worker_api = str(getattr(task, "worker_api", None) or "").strip()
    worker_task_id = str(getattr(task, "worker_task_id", None) or "").strip()
    if not worker_api or not worker_task_id:
        return None
    status_url = f"{worker_api.rstrip('/')}/status/{quote(worker_task_id, safe='')}"
    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            response = await client.get(status_url)
        if response.status_code != 200:
            return None
        payload = response.json() if response.content else {}
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _completion_contract_v2_state(payload: Optional[dict]) -> Tuple[bool, bool, Optional[str]]:
    """Return (is_v2, finalized_successfully, terminal_failure)."""
    if not isinstance(payload, dict):
        return False, False, None
    try:
        version = int(payload.get("completion_contract_version") or 0)
    except (TypeError, ValueError):
        version = 0
    if version < 2:
        return False, False, None
    errors = payload.get("finalization_errors")
    if isinstance(errors, list):
        error_text = "; ".join(str(item).strip() for item in errors if str(item).strip())
        if error_text:
            return True, False, error_text
    worker_status = str(payload.get("status") or "").strip().lower()
    if worker_status in {"failed", "error"}:
        error_text = str(payload.get("error") or "worker finalization failed").strip()
        return True, False, error_text
    finalized = bool(payload.get("finalized")) and worker_status == "completed"
    return True, finalized, None


def _task_declares_completion_v2(task: Task) -> bool:
    """Read the persisted, migration-free v2 declaration from progress_page."""
    try:
        query = parse_qs(urlparse(str(getattr(task, "progress_page", None) or "")).query)
        values = query.get("completion_contract_version", [])
        return any(int(value) >= 2 for value in values)
    except (TypeError, ValueError):
        return False


async def _probe_remote_glb_artifact(url: Optional[str]) -> bool:
    """Validate HTTP reachability plus GLB magic/version/declared total length."""
    candidate = str(url or "").strip()
    if not candidate:
        return False
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream(
                "GET",
                candidate,
                headers={"Range": "bytes=0-11", "Accept-Encoding": "identity"},
                timeout=VIEWER_ARTIFACT_PROBE_TIMEOUT_SECONDS,
            ) as response:
                if response.status_code not in (200, 206):
                    return False
                total_size: Optional[int] = None
                content_range = str(response.headers.get("content-range") or "")
                match = re.fullmatch(r"bytes\s+\d+-\d+/(\d+)", content_range, re.IGNORECASE)
                if match:
                    total_size = int(match.group(1))
                elif response.status_code == 200:
                    content_length = str(response.headers.get("content-length") or "")
                    if content_length.isdigit():
                        total_size = int(content_length)

                header = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=12):
                    if chunk:
                        header.extend(chunk[: 12 - len(header)])
                    if len(header) >= 12:
                        break
    except Exception:
        return False

    if len(header) != 12 or header[:4] != b"glTF":
        return False
    version = int.from_bytes(header[4:8], "little")
    declared_size = int.from_bytes(header[8:12], "little")
    return version in (1, 2) and declared_size >= 12 and total_size == declared_size


async def _validated_viewer_artifact_urls(
    prepared_url: Optional[str],
    animations_url: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    candidates = (prepared_url, animations_url)
    checks = await asyncio.gather(
        *(_probe_remote_glb_artifact(url) for url in candidates),
        return_exceptions=True,
    )
    return tuple(
        canonical_worker_artifact_url(str(url))
        if url and check is True
        else None
        for url, check in zip(candidates, checks)
    )  # type: ignore[return-value]


async def persist_validated_worker_viewer_artifacts(task: Task, result: Any) -> None:
    """Persist worker-declared viewer URLs only when their GLB headers are live."""
    prepared, animations = await _validated_viewer_artifact_urls(
        getattr(result, "viewer_prepared_glb_url", None),
        getattr(result, "viewer_animations_glb_url", None),
    )
    if prepared:
        task.viewer_prepared_glb_url = prepared
    if animations:
        task.viewer_animations_glb_url = animations


async def reconcile_task_viewer_artifacts(
    db: AsyncSession,
    task: Task,
    *,
    force: bool = False,
) -> Task:
    """Bounded late discovery for completed tasks whose viewer export arrived later."""
    expected_status = task.status
    expected_guid = task.guid
    expected_worker_api = task.worker_api
    if not task.guid or not task.worker_api:
        return task
    if task.viewer_prepared_glb_url and task.viewer_animations_glb_url:
        return task

    now = time.monotonic()
    last_attempt = _viewer_reconcile_last_attempt.get(task.id, 0.0)
    if not force and now - last_attempt < VIEWER_RECONCILE_BACKOFF_SECONDS:
        return task
    _viewer_reconcile_last_attempt[task.id] = now
    if len(_viewer_reconcile_last_attempt) > 4096:
        cutoff = now - VIEWER_RECONCILE_BACKOFF_SECONDS
        for key, attempted_at in list(_viewer_reconcile_last_attempt.items()):
            if attempted_at < cutoff:
                _viewer_reconcile_last_attempt.pop(key, None)

    status_prepared, status_animations = await _fetch_worker_status_viewer_artifacts(task)
    if status_prepared and status_animations:
        prepared_candidate, animations_candidate = status_prepared, status_animations
    else:
        _downloads, model_files_prepared, model_files_animations = (
            await _fetch_concrete_worker_artifacts(task)
        )
        prepared_candidate = status_prepared or model_files_prepared
        animations_candidate = status_animations or model_files_animations
    prepared, animations = await _validated_viewer_artifact_urls(
        None if task.viewer_prepared_glb_url else prepared_candidate,
        None if task.viewer_animations_glb_url else animations_candidate,
    )
    updates: Dict[str, Any] = {}
    if prepared:
        updates["viewer_prepared_glb_url"] = prepared
    if animations:
        updates["viewer_animations_glb_url"] = animations
    if updates:
        # Viewer metadata can be generated long after a task completed. Keep the
        # terminal timestamp stable because it drives duration and gallery order.
        if expected_status in ("done", "error"):
            updates["updated_at"] = Task.updated_at
        result = await db.execute(
            update(Task)
            .where(
                Task.id == task.id,
                Task.status == expected_status,
                Task.guid == expected_guid,
                Task.worker_api == expected_worker_api,
            )
            .values(**updates)
        )
        if int(getattr(result, "rowcount", 0) or 0) != 1:
            # The task was restarted/reassigned while the worker was being
            # probed. Never attach stale viewer artifacts to the new run.
            await db.rollback()
            await db.refresh(task)
            return task
        await db.commit()
        await db.refresh(task)
        try:
            from artifact_cache import enqueue_artifact_cache

            await enqueue_artifact_cache(db, task, force_refresh=True)
            await db.commit()
            await db.refresh(task)
        except Exception as exc:
            await db.rollback()
            print(f"[ArtifactCache] late viewer refresh enqueue failed for {task.id}: {exc}")
    return task

async def _fetch_worker_failure_message(task: Task) -> Optional[str]:
    """Return terminal worker failure text from {guid}_progress.txt, if present."""
    if not task.guid or not task.worker_api:
        return None
    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return None
    log_url = f"{worker_base.rstrip('/')}/converter/glb/{task.guid}/{task.guid}_progress.txt"
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            resp = await client.get(log_url)
        if resp.status_code != 200:
            return None
        text = resp.text
    except Exception:
        return None
    return latest_terminal_failure_reason(text)


async def _worker_conversion_completed(task: Task) -> bool:
    """True when worker progress log says final collection/verification finished."""
    if not task.guid or not task.worker_api:
        return False
    worker_base = get_worker_base_url(task.worker_api)
    if not worker_base:
        return False
    log_url = f"{worker_base.rstrip('/')}/converter/glb/{task.guid}/{task.guid}_progress.txt"
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            resp = await client.get(log_url)
        if resp.status_code != 200:
            return False
        text = resp.text.replace("\r\n", "\n").replace("\r", "\n")
    except Exception:
        return False
    # Animal-only-rig workers continue scheduling optional cross-species
    # variants after the requested rig, skeleton, and preview have been
    # exported.  Their primary completion marker is therefore distinct from
    # the legacy generic converter marker.
    return (
        "Conversion completed" in text
        or "Animal primary rigging completed" in text
    )


def _schedule_task_error_notification(task_id: str) -> None:
    """Fire-and-forget operator alert when a task reaches terminal error."""
    try:
        from telegram_bot import reserve_and_broadcast_task_error

        print(f"[Tasks] Scheduling Telegram error notification for task {task_id}")
        asyncio.create_task(reserve_and_broadcast_task_error(task_id))
    except Exception as e:
        print(f"[Telegram] Failed to schedule error notification for task {task_id}: {e}")


async def _mark_task_worker_failed_if_reported(db: AsyncSession, task: Task) -> bool:
    failure = await _fetch_worker_failure_message(task)
    if not failure:
        return False
    task.status = "error"
    task.error_message = f"Worker failed: {failure}"
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(f"[Tasks] Worker reported terminal failure for {task.id}: {failure}")
    _schedule_task_error_notification(task.id)
    return True


def _preferred_video_url_from_outputs(urls: List[str], *, prefer_rig_preview: bool = False) -> Optional[str]:
    if prefer_rig_preview:
        for url in urls or []:
            if url.lower().endswith("_rig_preview.mp4"):
                return url
    for url in urls or []:
        if url.lower().endswith("_video_small.mp4"):
            return url
    for url in urls or []:
        if url.lower().endswith("_video.mp4"):
            return url
    if not prefer_rig_preview:
        for url in urls or []:
            if url.lower().endswith("_rig_preview.mp4"):
                return url
    return None


# Accounts that always want the full convert scenario (retopology, bake, all
# formats) after a rig finishes, without pressing Submit in Telegram.
AUTO_FULL_CONVERT_OWNERS = {
    owner.strip().lower()
    for owner in os.getenv("AUTORIG_AUTO_FULL_CONVERT_OWNERS", "eschota@gmail.com").split(",")
    if owner.strip()
}


async def update_task_progress(db: AsyncSession, task: Task) -> Task:
    """
    Check and update task progress.
    Checks a batch of URLs and updates ready count.
    """
    if _restore_worker_api_from_progress_page(task):
        task.updated_at = datetime.utcnow()
        print(f"[Tasks] Restored worker_api from progress_page for task {task.id}: {task.worker_api}")

    # Track if task just completed
    was_processing = task.status == "processing"
    previous_ready_count = task.ready_count
    video_was_ready = task.video_ready
    previous_video_url = task.video_url

    completion_payload = await _fetch_worker_completion_contract(task)
    contract_v2, worker_finalized, finalization_failure = _completion_contract_v2_state(
        completion_payload
    )
    expects_v2 = contract_v2 or _task_declares_completion_v2(task)
    completion_probe_unavailable = expects_v2 and completion_payload is None
    if contract_v2 and finalization_failure:
        task.status = "error"
        task.error_message = f"Worker failed: {finalization_failure}"
        task.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(task)
        _schedule_task_error_notification(task.id)
        return task

    # Get already ready URLs
    already_ready = set(task.ready_urls)
    
    # Check new URLs (only for processing tasks)
    if task.status not in ("done", "error") and task.output_urls:
        newly_ready, total_ready = await check_urls_batch(
            task.output_urls, 
            already_ready
        )
        
        # Update task
        if newly_ready:
            current_ready = task.ready_urls
            current_ready.extend(newly_ready)
            task.ready_urls = current_ready
        
        task.ready_count = total_ready
        task.updated_at = datetime.utcnow()
        
        # Track last progress time (when ready_count actually increased)
        if total_ready > previous_ready_count:
            task.last_progress_at = datetime.utcnow()
        
        # Check if all URLs are ready
        if task.total_count > 0 and task.ready_count >= task.total_count:
            if completion_probe_unavailable:
                task.status = "processing"
            elif contract_v2 and not worker_finalized:
                task.status = "processing"
            elif _is_animal_task(task) and not await _worker_conversion_completed(task):
                task.status = "processing"
            else:
                task.status = "done"

    # Some worker modes (notably animal-only-rig and newer exporters) write the
    # final files into concrete folders such as {guid}_100k or root, while the
    # initial task response may contain legacy placeholder URLs. Reconcile from
    # model-files once the actual downloadable set is present.
    if task.status not in ("done", "error") and task.guid and task.worker_api:
        (
            concrete_urls,
            viewer_prepared_glb_url,
            viewer_animations_glb_url,
        ) = await _fetch_concrete_worker_artifacts(task)
        validated_prepared_url, validated_animations_url = await _validated_viewer_artifact_urls(
            None if task.viewer_prepared_glb_url else viewer_prepared_glb_url,
            None if task.viewer_animations_glb_url else viewer_animations_glb_url,
        )
        if validated_prepared_url:
            task.viewer_prepared_glb_url = validated_prepared_url
            task.updated_at = datetime.utcnow()
        if validated_animations_url:
            task.viewer_animations_glb_url = validated_animations_url
            task.updated_at = datetime.utcnow()
        if (
            task.status not in ("done", "error")
            and concrete_urls
            and _worker_outputs_look_complete(concrete_urls)
            and not completion_probe_unavailable
            and (not contract_v2 or worker_finalized)
        ):
            task.output_urls = concrete_urls
            task.ready_urls = concrete_urls
            task.total_count = len(concrete_urls)
            task.ready_count = len(concrete_urls)
            conversion_completed = worker_finalized if contract_v2 else (
                (not _is_animal_task(task)) or await _worker_conversion_completed(task)
            )
            task.status = "done" if conversion_completed else "processing"
            task.last_progress_at = datetime.utcnow()
            preferred_video_url = _preferred_video_url_from_outputs(
                concrete_urls,
                prefer_rig_preview=_is_animal_task(task),
            )
            if preferred_video_url:
                task.video_ready = True
                task.video_url = preferred_video_url
            task.updated_at = datetime.utcnow()

    if task.status not in ("done", "error") and task.guid and task.worker_api:
        if await _mark_task_worker_failed_if_reported(db, task):
            await db.refresh(task)
            return task
    
    # Video: animal tasks use the rig preview; other tasks use the lightweight site preview.
    if task.guid and task.worker_api:
        worker_base = get_worker_base_url(task.worker_api)
        if worker_base:
            preferred_current = "_rig_preview.mp4" if _is_animal_task(task) else "_video_small.mp4"
            if task.video_url and preferred_current in task.video_url:
                if not task.video_ready:
                    task.video_ready = True
                    task.updated_at = datetime.utcnow()
            else:
                video_ready, video_url = await check_video_availability(
                    task.guid,
                    worker_base,
                    prefer_rig_preview=_is_animal_task(task),
                )
                if video_ready and video_url:
                    changed = (not task.video_ready) or (task.video_url != video_url)
                    if changed:
                        task.video_ready = True
                        task.video_url = video_url
                        task.updated_at = datetime.utcnow()

    if task.status == "done":
        from artifact_cache import enqueue_artifact_cache

        # This flush shares the task-completion transaction. A periodic
        # backfill remains as recovery for rows completed before this release.
        await enqueue_artifact_cache(
            db,
            task,
            force_refresh=bool(
                task.video_ready
                and (not video_was_ready or previous_video_url != task.video_url)
            ),
        )

    await db.commit()
    await db.refresh(task)

    if (
        task.status == "done"
        and task.video_ready
        and (
            not video_was_ready
            or (
                _is_animal_task(task)
                and previous_video_url != task.video_url
                and "_rig_preview.mp4" in str(task.video_url or "").lower()
                and not getattr(task, "youtube_video_id", None)
            )
        )
    ):
        try:
            from youtube_upload import schedule_youtube_upload_if_eligible

            schedule_youtube_upload_if_eligible(task.id)
        except Exception as e:
            print(f"[Tasks] Failed to schedule YouTube upload for task {task.id}: {e}")
    
    # Send email notification if task just completed (100%)
    if was_processing and task.status == "done" and task.owner_type == "user":
        try:
            from email_service import send_task_completed_email

            rs_owner = await db.execute(select(User).where(User.email == task.owner_id))
            owner_user = rs_owner.scalar_one_or_none()
            if owner_user is not None and owner_user.email_invalid_at:
                print(
                    f"[Tasks] Skipping completion email for task {task.id}: email marked invalid after bounce/complaint"
                )
            elif owner_user is not None and not owner_user.email_task_completed:
                print(
                    f"[Tasks] Skipping completion email for task {task.id}: user opted out of task-ready emails"
                )
            else:
                worker_base = get_worker_base_url(task.worker_api)
                await send_task_completed_email(
                    to_email=task.owner_id,  # owner_id contains user email
                    task_id=task.id,
                    guid=task.guid,
                    worker_base=worker_base,
                )
        except Exception as e:
            print(f"[Tasks] Failed to send completion email for task {task.id}: {e}")

    # Owners who always want the full convert scenario get it without pressing
    # the button. Guarded three ways, because this runs inside the completion
    # path and a mistake here would loop: only the original rig task qualifies
    # (the convert task it creates has pipeline_kind="convert" and can never
    # re-trigger), the source url must exist, and an identical convert task must
    # not already be present - otherwise a re-run of this function would submit
    # the same model twice.
    if (
        task.status == "done"
        and task.owner_type == "user"
        and str(task.owner_id or "").strip().lower() in AUTO_FULL_CONVERT_OWNERS
        and (task.pipeline_kind or "rig") == "rig"
        and task.input_url
    ):
        try:
            existing = await db.scalar(
                select(Task.id).where(
                    Task.input_url == task.input_url,
                    Task.pipeline_kind == "convert",
                )
            )
            if existing:
                print(f"[AutoSubmit] task {task.id}: convert already exists ({str(existing)[:8]}), skipping")
            else:
                auto_task, auto_error = await create_conversion_task(
                    db,
                    input_url=task.input_url,
                    task_type=task.input_type or "t_pose",
                    owner_type=task.owner_type,
                    owner_id=task.owner_id,
                    created_via_api=True,
                    pipeline_kind="convert",
                )
                if auto_task is not None:
                    print(f"[AutoSubmit] task {task.id} -> full convert {auto_task.id}")
                else:
                    print(f"[AutoSubmit] task {task.id} failed: {auto_error}")
        except Exception as auto_exc:
            print(f"[AutoSubmit] task {task.id} raised: {type(auto_exc).__name__}: {auto_exc}")
    
    if was_processing and task.status == "done":
        try:
            from content_moderation import schedule_task_poster_classification

            schedule_task_poster_classification(task.id)
        except Exception as e:
            print(f"[Tasks] Failed to schedule poster classification for task {task.id}: {e}")

    # GA4 rig_completed (fires when task reaches done; Telegram done waits on poster classification)
    if was_processing and task.status == "done":
        try:
            duration = None
            if task.created_at:
                duration = int((datetime.utcnow() - task.created_at).total_seconds())
            if task.ga_client_id:
                from main import send_ga4_event

                asyncio.create_task(
                    send_ga4_event(
                        task.ga_client_id,
                        "rig_completed",
                        {"duration": duration, "task_id": task.id},
                    )
                )
        except Exception as e:
            print(f"[Tasks] Failed to send GA4 rig_completed for task {task.id}: {e}")

    if was_processing and task.status == "done":
        try:
            from database import bump_admin_overlay_task_completed

            await bump_admin_overlay_task_completed(db, task)
        except Exception as e:
            print(f"[Tasks] Admin overlay metrics bump: {e}")

    # VIEWER_RECONCILE_ON_DONE: every reconcile pass above is gated on the task
    # NOT being done, but the viewer export lands together with the last
    # artifacts - so at the exact moment those urls become discoverable, nothing
    # looks for them again. They were then only picked up if somebody happened
    # to open the task page, which is why finished tasks sat with an empty 3D
    # preview. One late pass here closes that window; it is throttled and
    # scheduled, so it never delays the completion path.
    if (
        task.status == "done"
        and task.guid
        and task.worker_api
        and not (task.viewer_prepared_glb_url and task.viewer_animations_glb_url)
    ):
        try:
            from main import _schedule_viewer_artifact_reconciliation

            _schedule_viewer_artifact_reconciliation(task.id)
        except Exception as e:
            print(f"[ViewerArtifacts] could not schedule reconcile for {task.id}: {e}")

    return task


# =============================================================================
# Stale Task Detection & Auto-Restart
# =============================================================================
async def admin_requeue_task_to_created(db: AsyncSession, task: Task) -> None:
    """
    Operator recovery: move task back to queue like stale reset but restart_count := 0
    (does not increment). Caller should commit.
    """
    task.status = "created"
    task.ready_count = 0
    task.ready_urls = []
    task.output_urls = []
    task.total_count = 0
    task.worker_api = None
    task.worker_task_id = None
    task.progress_page = None
    task.guid = None
    task.video_ready = False
    task.video_url = None
    task.error_message = None
    task.restart_count = 0
    task.last_progress_at = None
    task.updated_at = datetime.utcnow()
    task.fbx_glb_output_url = None
    task.fbx_glb_model_name = None
    task.fbx_glb_ready = False
    task.fbx_glb_error = None
    task.viewer_prepared_glb_url = None
    task.viewer_animations_glb_url = None
    task.telegram_new_notified_at = None
    task.telegram_done_notified_at = None
    task.processing_started_at = None
    task.source_attempt_count = 0
    task.source_next_retry_at = None


async def reset_stale_task(db: AsyncSession, task: Task) -> bool:
    """
    Reset a stale task for re-processing.
    Returns True if task was reset, False if max restarts exceeded.
    """
    from config import MAX_TASK_RESTARTS
    
    # Check if we've exceeded max restarts
    current_restarts = task.restart_count or 0
    if current_restarts >= MAX_TASK_RESTARTS:
        # Mark as error - too many restarts
        task.status = "error"
        task.error_message = f"Task made no progress after {current_restarts} automatic restart attempts."
        task.updated_at = datetime.utcnow()
        await db.commit()
        print(f"[Stale Task] Task {task.id} marked as error after {current_restarts} restarts")
        _schedule_task_error_notification(task.id)
        return False
    
    # Reset task for re-processing
    task.status = "created"
    task.ready_count = 0
    task.ready_urls = []
    task.output_urls = []
    task.total_count = 0
    task.worker_api = None
    task.worker_task_id = None
    task.progress_page = None
    task.guid = None
    task.video_ready = False
    task.video_url = None
    task.error_message = None
    task.restart_count = current_restarts + 1
    task.last_progress_at = None
    task.processing_started_at = None
    task.source_next_retry_at = None
    task.viewer_prepared_glb_url = None
    task.viewer_animations_glb_url = None
    task.updated_at = datetime.utcnow()
    
    await db.commit()
    print(f"[Stale Task] Task {task.id} reset for re-processing (restart #{task.restart_count})")
    return True


async def find_and_reset_stale_tasks(
    db: AsyncSession,
    queue_status: Optional[GlobalQueueStatus] = None,
) -> int:
    """
    Find all stale processing tasks and reset them or mark as error.
    Returns number of tasks reset/marked as error.

    When queue_status is provided, GET responses include optional active_tasks JSON; if our
    worker_task_id/guid/output URLs do not appear while the worker lists active jobs (or lists
    none), we requeue after STALE_TASK_TIMEOUT_MINUTES with no real progress.

    Prod diagnostics for one stuck task_id (adjust table name for your DB):
    SELECT id, status, worker_api, worker_task_id, guid, output_urls, total_count, ready_count,
           last_progress_at, restart_count, created_at, updated_at FROM tasks WHERE id = ?;
    """
    from config import (
        STALE_TASK_TIMEOUT_MINUTES,
        GLOBAL_TASK_TIMEOUT_MINUTES,
        PARTIAL_PROGRESS_STALE_MINUTES,
        WORKER_IDLE_STALE_MINUTES,
    )

    now = datetime.utcnow()
    stale_cutoff = now - timedelta(minutes=STALE_TASK_TIMEOUT_MINUTES)
    worker_idle_cutoff = now - timedelta(minutes=WORKER_IDLE_STALE_MINUTES)
    global_cutoff = now - timedelta(minutes=GLOBAL_TASK_TIMEOUT_MINUTES)
    partial_cutoff = now - timedelta(minutes=PARTIAL_PROGRESS_STALE_MINUTES)
    lookup = get_worker_active_lookup(queue_status)

    # Find all non-terminal tasks
    result = await db.execute(
        select(Task).where(
            Task.status.notin_(["done", "error"]),
        )
    )
    active_tasks = result.scalars().all()

    action_count = 0
    terminal_error_task_ids: list[str] = []
    for task in active_tasks:
        # 1. Hard timeout from the current dispatch/progress epoch. Using the
        # original creation time here made every redispatch of an old task
        # immediately stale again, producing misleading multi-worker failures.
        hard_timeout_reference = task_hard_timeout_reference(
            status=task.status,
            created_at=task.created_at,
            updated_at=task.updated_at,
            last_progress_at=task.last_progress_at,
        )
        if hard_timeout_reference and hard_timeout_reference < global_cutoff:
            task.status = "error"
            task.error_message = (
                f"Task had no dispatch/progress activity for "
                f"{GLOBAL_TASK_TIMEOUT_MINUTES} minutes."
            )
            task.updated_at = now
            print(f"[Timeout] Task {task.id} marked as error (global timeout)")
            action_count += 1
            terminal_error_task_ids.append(task.id)
            continue

        if task.status != "processing":
            continue

        reference_time = get_task_progress_reference_time(task)
        if not reference_time:
            continue

        entry = lookup_worker_queue_entry(task.worker_api, lookup)
        lost_on_worker = False
        if entry:
            refs, has_payload = entry
            if has_payload and not task_visible_on_worker_refs(
                task.worker_task_id,
                task.guid,
                task.output_urls,
                refs,
                has_payload,
            ):
                lost_on_worker = True

        ws = find_worker_queue_status_for_task(task.worker_api, queue_status)
        worker_reports_idle = (
            ws is not None
            and ws.available
            and (ws.total_active or 0) == 0
            and (ws.queue_size or 0) <= 0
            and (task.ready_count or 0) == 0
        )

        should_reset = False
        reason = ""
        if lost_on_worker and reference_time < stale_cutoff:
            should_reset = True
            reason = "lost_on_worker"
        elif worker_reports_idle and reference_time < worker_idle_cutoff:
            should_reset = True
            reason = "worker_reports_idle"
        elif reference_time < stale_cutoff and (task.ready_count or 0) == 0:
            should_reset = True
            reason = "no_ready_yet"
        elif (
            (task.total_count or 0) > 0
            and (task.ready_count or 0) < (task.total_count or 0)
            and reference_time < partial_cutoff
        ):
            should_reset = True
            reason = "partial_progress_stale"

        if should_reset:
            if await _mark_task_worker_failed_if_reported(db, task):
                action_count += 1
                continue
            no_progress_min = get_task_no_progress_minutes(task, now=now)
            print(
                f"[Stale Task] Detected stale task {task.id} ({reason}): "
                f"worker={task.worker_api}, no_progress={no_progress_min:.1f}m, "
                f"since={reference_time}"
            )
            if await reset_stale_task(db, task):
                action_count += 1

    if action_count > 0:
        await db.commit()
        for task_id in terminal_error_task_ids:
            _schedule_task_error_notification(task_id)

    return action_count


async def get_stalled_processing_tasks_by_worker(
    db: AsyncSession,
    *,
    min_stalled_minutes: int,
    queue_status: Optional[GlobalQueueStatus] = None,
) -> dict[str, list[Task]]:
    """
    Return processing tasks that look stalled (no progress, lost on worker per active_tasks JSON,
    or partial progress beyond PARTIAL_PROGRESS_STALE_MINUTES), grouped by worker_api.
    """
    from config import PARTIAL_PROGRESS_STALE_MINUTES

    now = datetime.utcnow()
    stale_cutoff = now - timedelta(minutes=min_stalled_minutes)
    partial_cutoff = now - timedelta(minutes=PARTIAL_PROGRESS_STALE_MINUTES)
    lookup = get_worker_active_lookup(queue_status)

    result = await db.execute(
        select(Task).where(Task.status == "processing",
            # a generation task has no worker progress to go stale on;
            # its own pump owns the lifecycle until the mesh exists
            Task.pipeline_kind != "generate",
        )
    )
    processing_tasks = result.scalars().all()

    grouped: dict[str, list[Task]] = {}
    for task in processing_tasks:
        ref = get_task_progress_reference_time(task)
        if not ref:
            continue
        worker = (task.worker_api or "").strip()
        if not worker:
            continue

        entry = lookup_worker_queue_entry(task.worker_api, lookup)
        lost_on_worker = False
        if entry:
            refs, has_payload = entry
            if has_payload and not task_visible_on_worker_refs(
                task.worker_task_id,
                task.guid,
                task.output_urls,
                refs,
                has_payload,
            ):
                lost_on_worker = True

        stalled = False
        if lost_on_worker and ref < stale_cutoff:
            stalled = True
        elif ref < stale_cutoff and (task.ready_count or 0) == 0:
            stalled = True
        elif (
            (task.total_count or 0) > 0
            and (task.ready_count or 0) < (task.total_count or 0)
            and ref < partial_cutoff
        ):
            stalled = True

        if stalled:
            grouped.setdefault(worker, []).append(task)
    return grouped


# =============================================================================
# Task Retrieval
# =============================================================================
async def get_task_by_id(db: AsyncSession, task_id: str) -> Optional[Task]:
    """Get task by ID"""
    result = await db.execute(
        select(Task).where(Task.id == task_id)
    )
    return result.scalar_one_or_none()


async def get_user_tasks(
    db: AsyncSession,
    owner_type: str,
    owner_id: str,
    page: int = 1,
    per_page: int = 10
) -> Tuple[list, int]:
    """
    Get tasks for a user/anon with pagination.
    Returns: (tasks, total_count)
    """
    # Count total
    count_result = await db.execute(
        select(Task).where(
            Task.owner_type == owner_type,
            Task.owner_id == owner_id
        )
    )
    total = len(count_result.scalars().all())
    
    # Get paginated
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(
            Task.owner_type == owner_type,
            Task.owner_id == owner_id
        )
        .order_by(desc(Task.created_at))
        .offset(offset)
        .limit(per_page)
    )
    tasks = result.scalars().all()
    
    return list(tasks), total


# =============================================================================
# Admin Functions
# =============================================================================
async def get_all_users(
    db: AsyncSession,
    search: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    page: int = 1,
    per_page: int = 20
) -> Tuple[list, int]:
    """
    Get all users with search and pagination (admin).
    Returns: (users, total_count)
    """
    query = select(User)
    
    if search:
        query = query.where(User.email.ilike(f"%{search}%"))
    
    # Count total
    count_result = await db.execute(query)
    total = len(count_result.scalars().all())
    
    # Sort
    sort_column = getattr(User, sort_by, User.created_at)
    if sort_desc:
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(sort_column)
    
    # Paginate
    offset = (page - 1) * per_page
    result = await db.execute(
        query.offset(offset).limit(per_page)
    )
    users = result.scalars().all()
    
    return list(users), total


async def update_user_balance(
    db: AsyncSession,
    user_id: int,
    delta: Optional[int] = None,
    set_to: Optional[int] = None
) -> Tuple[Optional[User], int, int]:
    """
    Update user balance.
    Returns: (user, old_balance, new_balance)
    """
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()
    
    if not user:
        return None, 0, 0
    
    old_balance = user.balance_credits
    
    if set_to is not None:
        user.balance_credits = max(0, set_to)
    elif delta is not None:
        user.balance_credits = max(0, user.balance_credits + delta)
    
    await db.commit()
    await db.refresh(user)
    
    return user, old_balance, user.balance_credits


# =============================================================================
# Gallery Functions
# =============================================================================
def _gallery_task_has_poster_sql():
    """Same thumb-path rule as main._gallery_task_has_poster_sql (avoid importing main)."""
    from sqlalchemy import or_, func

    pats = ("_video_poster.jpg", "_poster.jpg", "icon.png", "Render_1_view.jpg")
    return or_(*[func.instr(c, p) > 0 for c in (Task._ready_urls, Task._output_urls) for p in pats])


async def get_gallery_items(
    db: AsyncSession,
    page: int = 1,
    per_page: int = 12
) -> Tuple[list, int]:
    """
    Get completed tasks with videos for public gallery.
    Returns: (tasks, total_count)
    """
    from sqlalchemy import func

    base = (
        Task.status == "done",
        Task.video_ready == True,
        _gallery_task_has_poster_sql(),
    )

    count_result = await db.execute(select(func.count(Task.id)).where(*base))
    total = count_result.scalar() or 0

    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(*base)
        .order_by(desc(Task.created_at))
        .offset(offset)
        .limit(per_page)
    )
    tasks = result.scalars().all()

    return list(tasks), total


def format_time_ago(dt: datetime) -> str:
    """Format datetime as human-readable time ago string"""
    now = datetime.utcnow()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        mins = int(seconds / 60)
        return f"{mins}m ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h ago"
    elif seconds < 604800:
        days = int(seconds / 86400)
        return f"{days}d ago"
    elif seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks}w ago"
    else:
        months = int(seconds / 2592000)
        return f"{months}mo ago"
