"""
Task management for OneClick Online
"""
import asyncio
import uuid
import re
import httpx
from datetime import datetime
from typing import Optional, Tuple, List
from urllib.parse import urlparse

from sqlalchemy import select, desc, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task, User, AnonSession, AsyncSessionLocal
from config import WORKERS
from workers import (
    select_best_worker,
    send_task_to_worker,
    send_fbx_to_glb,
    check_urls_batch,
    check_video_availability,
    get_worker_base_url
)


# =============================================================================
# Helper Functions
# =============================================================================
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
    candidate_workers = [first_worker_url] + [w for w in WORKERS if w != first_worker_url]

    async with AsyncSessionLocal() as db:
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
                        task.input_type or "t_pose"
                    )
                    if not result.success:
                        task.status = "error"
                        task.error_message = result.error
                        task.updated_at = datetime.utcnow()
                        await db.commit()
                        return

                    task.worker_task_id = result.task_id
                    task.progress_page = result.progress_page
                    task.guid = result.guid
                    task.output_urls = result.output_urls
                    task.total_count = len(result.output_urls)
                    task.status = "processing"
                    task.started_at = datetime.utcnow()
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


# =============================================================================
# Task Creation
# =============================================================================
async def create_conversion_task(
    db: AsyncSession,
    input_url: str,
    task_type: str,
    owner_type: str,
    owner_id: str
) -> Tuple[Optional[Task], Optional[str]]:
    """
    Create a new conversion task.
    Returns: (task, error_message)
    """
    # Create task record
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id,
        owner_type=owner_type,
        owner_id=owner_id,
        input_url=input_url,
        input_type=task_type,
        status="created"
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
    task.worker_api = worker_url
    task.status = "processing"
    task.started_at = datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)

    # Send task directly to worker (workers handle GLB, FBX, OBJ natively)
    result = await send_task_to_worker(worker_url, task.input_url, task.input_type or "t_pose", backend_task_id=task.id)
    if not result.success:
        task.status = "error"
        task.error_message = result.error
        task.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(task)
        return task, result.error

    task.worker_task_id = result.task_id
    task.progress_page = result.progress_page
    task.output_log_url = result.output_log_url
    task.output_json_url = result.output_json_url
    task.guid = result.guid
    task.output_urls = result.output_urls
    task.total_count = 100
    task.ready_count = 2
    task.status = "processing"
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)
    
    # Telegram notification (fire-and-forget) - now we have progress_page
    try:
        from telegram_bot import broadcast_new_task
        # Construct progress_page URL from worker_api and guid
        worker_base = get_worker_base_url(worker_url)
        progress_url = f"{worker_base}/converter/glb/{task.guid}/{task.guid}.html"
        print(f"[Tasks] Scheduling Telegram notification for new task {task.id}")
        asyncio.create_task(broadcast_new_task(task.id, task.input_url, task.input_type, progress_url))
    except Exception as e:
        print(f"[Telegram] Failed to notify new task: {e}")
        import traceback
        traceback.print_exc()
    
    return task, None


# =============================================================================
# Progress Checking
# =============================================================================
def _parse_product_lines(log_text: str) -> list:
    """Parse all PRODUCT\\t... lines from log text.

    Returns list of dicts: [{category, name, url, extra?}, ...]
    """
    entries = []
    for line in log_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("PRODUCT\t"):
            continue
        parts = stripped.split("\t")
        if len(parts) < 4:
            continue
        entry = {
            "category": parts[1],
            "name": parts[2],
            "url": parts[3],
        }
        if len(parts) >= 5 and parts[4]:
            entry["extra"] = parts[4]
        # Skip the schema marker row (informational only)
        if entry["category"] == "schema":
            continue
        entries.append(entry)

    # Deduplicate: same category+name can appear under different URL paths (e.g. export/raptor/renders/ vs export/renders/).
    # Keep the last occurrence so the most recent/canonical path wins.
    seen: dict[tuple, dict] = {}
    for e in entries:
        key = (e["category"], e["name"].lower())
        seen[key] = e
    return list(seen.values())


_GENERIC_FAILURE_MARKERS = (
    "processing failed. check logs for details",
    "exporting was not completed",
    "check logs for details",
)


def _looks_generic_failure(reason: Optional[str]) -> bool:
    text = (reason or "").strip().lower()
    if not text:
        return True
    return any(marker in text for marker in _GENERIC_FAILURE_MARKERS)


def _extract_reason_from_occonvert_log(log_text: str) -> Optional[str]:
    """Extract human-readable root cause from verbose OCConvert log."""
    if not log_text:
        return None

    lines = [ln.strip() for ln in log_text.splitlines() if ln.strip()]

    # Priority 1: quiet-mode modal message usually contains the real exception text.
    for line in reversed(lines):
        if "modal messagebox" in line and "with message '" in line:
            msg_match = re.search(r"with message '(.+?)' automatically closed", line, re.IGNORECASE)
            if msg_match:
                msg = msg_match.group(1).strip()
                msg = msg.replace("? ERROR! ?", "").strip()
                msg = msg.replace("-- Runtime error:", "Runtime error:").strip()
                msg = re.sub(r"\s+", " ", msg).strip(" .")
                if msg:
                    return msg

    # Priority 2: common explicit error patterns.
    explicit_patterns = (
        r"Server connection error:\s*(.+)",
        r"Runtime error:\s*(.+)",
        r"\b(ERROR|Error|Exception)\b[:\s-]+(.+)",
        r"remote certificate is invalid according to the validation procedure",
    )
    for line in reversed(lines):
        for pattern in explicit_patterns:
            m = re.search(pattern, line, re.IGNORECASE)
            if not m:
                continue
            if m.lastindex and m.lastindex >= 2:
                raw = m.group(2)
            elif m.lastindex == 1:
                raw = m.group(1)
            else:
                raw = m.group(0)
            cleaned = re.sub(r"\s+", " ", (raw or "").strip()).strip(" .")
            if cleaned:
                return cleaned

    return None


def _collect_worker_files(payload: dict) -> list[dict]:
    """Flatten worker model-files payload into a plain file list."""
    if not isinstance(payload, dict):
        return []
    files = payload.get("files")
    if isinstance(files, list) and files:
        return [f for f in files if isinstance(f, dict)]

    collected: list[dict] = []
    folders = payload.get("folders")
    if not isinstance(folders, dict):
        return collected

    for folder_name, folder_meta in folders.items():
        if not isinstance(folder_meta, dict):
            continue
        folder_files = folder_meta.get("files")
        if not isinstance(folder_files, list):
            continue
        for file_meta in folder_files:
            if not isinstance(file_meta, dict):
                continue
            file_copy = dict(file_meta)
            # New worker payload stores files grouped by folder and usually omits "folder" on item level.
            file_copy.setdefault("folder", folder_name)
            collected.append(file_copy)
    return collected


async def _resolve_detailed_failure_reason(
    client: httpx.AsyncClient,
    task: Task,
    fail_reason: Optional[str],
) -> tuple[str, Optional[str]]:
    """
    Return (final_reason, exporter_error_log_url).
    Uses exporter log only when FAILURE reason is generic.
    """
    fallback = (fail_reason or "").strip() or "Task failed. Check logs for details."
    if not _looks_generic_failure(fallback):
        return fallback, None

    if not task.guid or not task.worker_api:
        return fallback, None

    try:
        worker_base = get_worker_base_url(task.worker_api).rstrip("/")
        files_url = f"{worker_base}/api-converter-glb/model-files/{task.guid}"
        files_resp = await client.get(files_url, timeout=5.0)
        if files_resp.status_code != 200:
            return fallback, None

        data = files_resp.json() if files_resp.content else {}
        files = _collect_worker_files(data)
        occonvert_logs = [
            f for f in files
            if str(f.get("name", "")).lower().startswith("occonvert_")
            and str(f.get("name", "")).lower().endswith(".log")
            and "logs" in str(f.get("folder", "")).lower()
            and f.get("url")
        ]
        if not occonvert_logs:
            return fallback, None

        # Name includes timestamp, lexicographic sort gives latest.
        occonvert_logs.sort(key=lambda x: str(x.get("name", "")))
        latest = occonvert_logs[-1]
        exporter_log_url = str(latest.get("url"))

        log_resp = await client.get(exporter_log_url, timeout=10.0)
        if log_resp.status_code != 200:
            return fallback, exporter_log_url

        detailed = _extract_reason_from_occonvert_log(log_resp.text)
        if detailed:
            return detailed, exporter_log_url
        return fallback, exporter_log_url
    except Exception:
        return fallback, None


async def update_task_progress(db: AsyncSession, task: Task) -> Task:
    """
    Check and update task progress by parsing worker logs.
    Supports both legacy ``*_url = "..."`` lines and the new
    ``PRODUCT\\t<category>\\t<name>\\t<url>`` format, as well as
    ``TASK_COMPLETE`` / ``FAILURE: <reason>`` completion markers.
    """
    if task.status in ("done", "error") or not task.output_log_url:
        return task

    was_processing = task.status == "processing"
    failure_reason = None

    try:
        async with httpx.AsyncClient() as client:
            # 1. Fetch and parse log
            log_resp = await client.get(task.output_log_url, timeout=10.0)
            if log_resp.status_code == 200:
                log_text = log_resp.text

                # ----- Legacy URL extraction -----
                legacy_pattern = re.compile(
                    r'^(renders|textures|meshes)_url\s*=\s*"(https?://[^"]+)"',
                    re.MULTILINE,
                )
                legacy_matches = legacy_pattern.findall(log_text)

                new_urls = []
                render_count = 0
                for type_group, url in legacy_matches:
                    if url not in task.ready_urls:
                        new_urls.append(url)
                    if type_group == "renders":
                        render_count += 1

                # ----- PRODUCT line extraction -----
                product_entries = _parse_product_lines(log_text)
                task.product_entries = product_entries

                # Collect URLs from PRODUCT entries into ready_urls
                product_url_categories = {
                    "renders", "textures", "meshes",
                    # Phase 1: URP
                    "unity_renders", "unity_video",
                    "unity_package", "unity_build_zip",
                    # Phase 2: Android
                    "unity_android_apk", "unity_renders_oc_android_apk",
                    # Phase 3: Quest VR
                    "unity_quest_apk", "unity_renders_oc_vr",
                    # Phase 4: WebGL
                    "unity_web_build_folder", "unity_renders_oc_webbuild",
                    # Phase 5: HDRP
                    "unity_renders_oc_hdrp", "unity_hdrp_video",
                    "unity_hdrp_build_zip", "unity_hdrp_package",
                }
                phase_counts = {
                    "unity_renders": 0,
                    "unity_video": False,
                    "unity_android_apk": False,
                    "unity_quest_apk": False,
                    "unity_web_build_folder": False,
                    "unity_renders_oc_hdrp": 0,
                    "unity_hdrp_video": False,
                }
                for pe in product_entries:
                    cat = pe.get("category", "")
                    url = pe.get("url", "")
                    if cat in product_url_categories and url and url not in task.ready_urls and url not in new_urls:
                        new_urls.append(url)
                    if cat == "renders":
                        render_count += 1
                    if cat == "unity_renders":
                        phase_counts["unity_renders"] += 1
                    if cat == "unity_renders_oc_hdrp":
                        phase_counts["unity_renders_oc_hdrp"] += 1
                    if cat in ("unity_video", "unity_android_apk", "unity_quest_apk",
                               "unity_web_build_folder", "unity_hdrp_video"):
                        phase_counts[cat] = True

                if new_urls:
                    current_ready = list(task.ready_urls)
                    current_ready.extend(new_urls)
                    task.ready_urls = current_ready
                    task.last_progress_at = datetime.utcnow()

                # ----- Progress calculation -----
                # 3ds Max stage: 0-50%   (10% per render, capped at 50)
                # Unity stage:   51-99%  (60 base + 5 per unity render, capped at 95)
                progress_pct = 2
                if render_count > 0:
                    progress_pct = min(2 + render_count * 10, 50)

                # JSON availability pushes 3ds Max stage to 50%
                if task.output_json_url:
                    try:
                        json_check = await client.head(task.output_json_url, timeout=5.0)
                        if json_check.status_code == 200:
                            progress_pct = max(progress_pct, 50)
                    except Exception:
                        pass

                # "Export completed" = 3ds Max done → 50%
                if "Export completed" in log_text:
                    progress_pct = max(progress_pct, 50)
                    # Parse Summary
                    summary_match = re.search(
                        r'--- EXPORT SUMMARY.*?---(.*?)------------------------------',
                        log_text,
                        re.DOTALL,
                    )
                    if summary_match:
                        task.export_summary = summary_match.group(1).strip()
                    else:
                        lines = log_text.splitlines()
                        summary_lines = []
                        for line in reversed(lines):
                            if "Export completed" in line:
                                continue
                            if "Time:" in line or "Processed" in line or "Total" in line or "Unwrapped" in line:
                                summary_lines.append(line)
                            if len(summary_lines) >= 5:
                                break
                        task.export_summary = "\n".join(reversed(summary_lines))

                # Multi-phase Unity progress (5 phases)
                # Phase 1 URP: 20-40%
                urp_renders = phase_counts["unity_renders"]
                if urp_renders > 0:
                    progress_pct = max(progress_pct, min(20 + urp_renders * 3, 35))
                if phase_counts["unity_video"]:
                    progress_pct = max(progress_pct, 40)
                # Phase 2 Android: 40-55%
                if phase_counts["unity_android_apk"]:
                    progress_pct = max(progress_pct, 55)
                # Phase 3 Quest VR: 55-70%
                if phase_counts["unity_quest_apk"]:
                    progress_pct = max(progress_pct, 70)
                # Phase 4 WebGL: 70-80%
                if phase_counts["unity_web_build_folder"]:
                    progress_pct = max(progress_pct, 80)
                # Phase 5 HDRP: 80-98%
                hdrp_renders = phase_counts["unity_renders_oc_hdrp"]
                if hdrp_renders > 0:
                    progress_pct = max(progress_pct, min(80 + hdrp_renders * 3, 95))
                if phase_counts["unity_hdrp_video"]:
                    progress_pct = max(progress_pct, 98)

                # ----- Completion markers -----
                # FAILURE takes priority: if any stage failed the task is an error,
                # even if TASK_COMPLETE also appears in the log.
                fail_match = re.search(r'^FAILURE:\s*(.+)', log_text, re.MULTILINE)
                if fail_match:
                    raw_fail_reason = fail_match.group(1).strip()
                    failure_reason, exporter_error_log_url = await _resolve_detailed_failure_reason(
                        client,
                        task,
                        raw_fail_reason,
                    )
                    if exporter_error_log_url:
                        has_error_log_entry = any(
                            e.get("category") == "error_log" for e in product_entries
                        )
                        if not has_error_log_entry:
                            product_entries.append({
                                "category": "error_log",
                                "name": "occonvert.log",
                                "url": exporter_error_log_url,
                            })
                            task.product_entries = product_entries
                    task.status = "error"
                    task.error_message = failure_reason
                    # Still extract videos for partial results
                    for cat in ("unity_hdrp_video", "unity_video"):
                        for pe in product_entries:
                            if pe.get("category") == cat and pe.get("url"):
                                task.video_url = pe["url"]
                                task.video_ready = True
                                break
                        if task.video_ready:
                            break
                elif "TASK_COMPLETE" in log_text:
                    task.status = "done"
                    progress_pct = 100
                    for cat in ("unity_hdrp_video", "unity_video"):
                        for pe in product_entries:
                            if pe.get("category") == cat and pe.get("url"):
                                task.video_url = pe["url"]
                                task.video_ready = True
                                break
                        if task.video_ready:
                            break
                elif "Export completed" in log_text and not product_entries:
                    task.status = "done"
                    progress_pct = max(progress_pct, 50)

                task.total_count = 100
                task.ready_count = progress_pct
                task.updated_at = datetime.utcnow()

    except Exception as e:
        print(f"[Tasks] Error updating progress for task {task.id}: {e}")

    await db.commit()
    await db.refresh(task)

    # Telegram notification if task just completed (success or failure).
    # Atomic UPDATE prevents duplicate sends when two processes (oneclick +
    # autorig) race to detect the same completion on a shared SQLite DB.
    if was_processing and task.status in ("done", "error"):
        result = await db.execute(
            update(Task)
            .where(Task.id == task.id, Task.telegram_notified == False)
            .values(telegram_notified=True)
        )
        await db.commit()
        await db.refresh(task)

        if result.rowcount > 0:
            try:
                from telegram_bot import broadcast_task_done
                duration = None
                duration_start = task.started_at or task.created_at
                if duration_start:
                    duration = int((datetime.utcnow() - duration_start).total_seconds())

                asyncio.create_task(broadcast_task_done(
                    task.id,
                    duration_seconds=duration,
                    product_entries=task.product_entries,
                    failure_reason=failure_reason,
                ))
            except Exception as e:
                print(f"[Telegram] Failed to notify done: {e}")

    return task


# =============================================================================
# Stale Task Detection & Auto-Restart
# =============================================================================
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
        task.error_message = f"Task failed after {current_restarts} automatic restart attempts. Worker may be unavailable."
        task.updated_at = datetime.utcnow()
        await db.commit()
        print(f"[Stale Task] Task {task.id} marked as error after {current_restarts} restarts")
        return False
    
    # Reset task for re-processing
    task.status = "created"
    task.ready_count = 0
    task.ready_urls = []
    task.output_urls = []
    task.product_entries = []
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
    task.updated_at = datetime.utcnow()
    
    await db.commit()
    print(f"[Stale Task] Task {task.id} reset for re-processing (restart #{task.restart_count})")
    return True


async def find_and_reset_stale_tasks(db: AsyncSession) -> int:
    """
    Find all stale processing tasks and reset them.
    Returns number of tasks reset.
    """
    from config import STALE_TASK_TIMEOUT_MINUTES
    from datetime import timedelta
    
    cutoff_time = datetime.utcnow() - timedelta(minutes=STALE_TASK_TIMEOUT_MINUTES)
    
    # Find processing tasks that haven't made progress
    result = await db.execute(
        select(Task).where(
            Task.status == "processing",
        )
    )
    processing_tasks = result.scalars().all()
    
    reset_count = 0
    for task in processing_tasks:
        # Determine the reference time for staleness
        # Use last_progress_at if available, otherwise use updated_at or created_at
        reference_time = task.last_progress_at or task.updated_at or task.created_at
        
        # Check if task is stale
        if reference_time and reference_time < cutoff_time:
            # Additional check: verify worker files are actually not accessible
            if task.output_urls and task.ready_count == 0:
                # Task has URLs but none are ready - likely worker lost the task
                print(f"[Stale Task] Detected stale task {task.id}: "
                      f"no progress since {reference_time}, "
                      f"ready_count={task.ready_count}/{task.total_count}")
                
                if await reset_stale_task(db, task):
                    reset_count += 1
    
    return reset_count


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
    
    # Count total completed tasks with video
    count_result = await db.execute(
        select(func.count(Task.id)).where(
            Task.status == "done",
            Task.video_ready == True
        )
    )
    total = count_result.scalar() or 0
    
    # Get paginated results, newest first
    offset = (page - 1) * per_page
    result = await db.execute(
        select(Task)
        .where(
            Task.status == "done",
            Task.video_ready == True
        )
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

