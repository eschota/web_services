"""
Worker integration for AutoRig Online
Handles communication with conversion workers
"""
import re
import asyncio
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field
import random
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from database import WorkerEndpoint
from config import (
    WORKERS, 
    PROGRESS_BATCH_SIZE, 
    PROGRESS_CONCURRENCY,
    PROGRESS_CHECK_TIMEOUT
)


def normalize_task_type(value: Optional[str]) -> str:
    """Rigging mode sent to workers; empty/whitespace must default to t_pose."""
    s = (value or "").strip()
    return s if s else "t_pose"


# =============================================================================
# Data Classes
# =============================================================================
@dataclass
class WorkerInfo:
    """Worker status information"""
    url: str
    available: bool
    load: float = 1.0
    error: Optional[str] = None


@dataclass
class WorkerTaskResult:
    """Result from creating a task on worker"""
    success: bool
    task_id: Optional[str] = None
    output_urls: List[str] = None
    progress_page: Optional[str] = None
    guid: Optional[str] = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.output_urls is None:
            self.output_urls = []


@dataclass
class FbxToGlbResult:
    """Result from FBX -> GLB converter endpoint"""
    success: bool
    model_name: Optional[str] = None
    output_url: Optional[str] = None
    error: Optional[str] = None



# =============================================================================
# GUID Extraction
# =============================================================================
GUID_PATTERN = re.compile(
    r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
)


def extract_guid(text: str) -> Optional[str]:
    """Extract GUID from text (URL or progress_page)"""
    if not text:
        return None
    match = GUID_PATTERN.search(text)
    return match.group(0) if match else None


# =============================================================================
# Worker Communication
# =============================================================================
async def get_configured_workers_with_weight(db: Optional[AsyncSession] = None) -> List[Tuple[str, int]]:
    """
    Return enabled worker URLs ordered by weight desc (priority), id asc.
    Fallback to config.WORKERS (weight=0) when DB has no rows or db is not provided.
    """
    if not db:
        return [(u, 0) for u in WORKERS]

    try:
        res = await db.execute(
            select(WorkerEndpoint.url, WorkerEndpoint.weight)
            .where(WorkerEndpoint.enabled.is_(True))
            .order_by(desc(WorkerEndpoint.weight), WorkerEndpoint.id)
        )
        rows = res.all()
        workers = [(url, int(weight or 0)) for (url, weight) in rows if url]
    except Exception:
        workers = []

    return workers or [(u, 0) for u in WORKERS]


async def get_configured_workers(db: Optional[AsyncSession] = None) -> List[str]:
    """Convenience wrapper returning only URLs."""
    return [url for (url, _w) in await get_configured_workers_with_weight(db)]


WORKER_QUARANTINE_SECONDS = int(os.getenv("WORKER_QUARANTINE_SECONDS", "900"))
_worker_quarantine_until: Dict[str, datetime] = {}
_worker_quarantine_reason: Dict[str, str] = {}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _purge_expired_quarantine(now: Optional[datetime] = None) -> None:
    ts = now or _utcnow()
    expired = [url for (url, until) in _worker_quarantine_until.items() if until <= ts]
    for url in expired:
        _worker_quarantine_until.pop(url, None)
        _worker_quarantine_reason.pop(url, None)


def quarantine_worker(worker_url: str, reason: Optional[str] = None, ttl_seconds: Optional[int] = None) -> None:
    """Temporarily exclude worker from selection."""
    if not worker_url:
        return
    ttl = int(ttl_seconds or WORKER_QUARANTINE_SECONDS)
    now = _utcnow()
    until = now + timedelta(seconds=max(60, ttl))
    prev_until = _worker_quarantine_until.get(worker_url)
    if not prev_until or prev_until < until:
        _worker_quarantine_until[worker_url] = until
    if reason:
        _worker_quarantine_reason[worker_url] = reason
    print(
        f"[Workers] Quarantine enabled for {worker_url} "
        f"until {until.isoformat()} reason={reason or '-'}"
    )


def clear_worker_quarantine(worker_url: str) -> None:
    """Clear worker quarantine manually or on recovery."""
    removed = _worker_quarantine_until.pop(worker_url, None)
    _worker_quarantine_reason.pop(worker_url, None)
    if removed:
        print(f"[Workers] Quarantine cleared for {worker_url}")


def is_worker_quarantined(worker_url: str) -> bool:
    _purge_expired_quarantine()
    return worker_url in _worker_quarantine_until


def get_quarantined_workers() -> Dict[str, dict]:
    """Snapshot of currently quarantined workers."""
    _purge_expired_quarantine()
    snapshot: Dict[str, dict] = {}
    for url, until in _worker_quarantine_until.items():
        snapshot[url] = {
            "until": until.isoformat(),
            "reason": _worker_quarantine_reason.get(url)
        }
    return snapshot


async def get_worker_load(worker_url: str, client: httpx.AsyncClient) -> WorkerInfo:
    """Get load/status from a single worker"""
    try:
        response = await client.get(worker_url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            # Worker may return load info in different formats
            load = data.get("load", data.get("queue_size", 0))
            if isinstance(load, (int, float)):
                return WorkerInfo(url=worker_url, available=True, load=float(load))
            return WorkerInfo(url=worker_url, available=True, load=0.0)
        return WorkerInfo(url=worker_url, available=False, error=f"HTTP {response.status_code}")
    except Exception as e:
        return WorkerInfo(url=worker_url, available=False, error=str(e))


async def get_all_workers_status(worker_urls: List[str]) -> List[WorkerInfo]:
    """Get status of provided workers"""
    async with httpx.AsyncClient() as client:
        tasks = [get_worker_load(url, client) for url in worker_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        workers = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                workers.append(WorkerInfo(
                    url=worker_urls[i], 
                    available=False, 
                    error=str(result)
                ))
            else:
                workers.append(result)
        return workers


async def select_best_worker(db: Optional[AsyncSession] = None) -> Optional[str]:
    """
    Select best worker for a new task.
    Strategy:
    - Prefer higher weight (priority).
    - Within the highest-weight available group, pick least busy (min load).
    - If none respond, fallback to first configured URL (still weight-ordered).
    """
    workers_with_weight = await get_configured_workers_with_weight(db)
    worker_urls = [u for (u, _w) in workers_with_weight]
    if not worker_urls:
        return None

    statuses = await get_all_workers_status(worker_urls)
    available = [w for w in statuses if w.available]
    quarantine_safe_available = [w for w in available if not is_worker_quarantined(w.url)]

    if quarantine_safe_available:
        candidates_pool = quarantine_safe_available
    elif available:
        # Degraded mode: all available workers are quarantined.
        # Keep service alive by falling back to available workers anyway.
        candidates_pool = available
        print("[Workers] All available workers are quarantined, using degraded fallback")
    else:
        # No worker responded as available. Prefer non-quarantined URL for optimistic dispatch.
        non_quarantined_urls = [u for u in worker_urls if not is_worker_quarantined(u)]
        return (non_quarantined_urls or worker_urls)[0]

    weight_by_url: Dict[str, int] = {u: w for (u, w) in workers_with_weight}
    max_weight = max(weight_by_url.get(w.url, 0) for w in candidates_pool)
    candidates = [w for w in candidates_pool if weight_by_url.get(w.url, 0) == max_weight]
    candidates.sort(key=lambda w: w.load)
    return candidates[0].url


async def send_task_to_worker(
    worker_url: str,
    input_url: str,
    task_type: str = "t_pose",
    transform_params: dict = None,
    *,
    pipeline_kind: str = "rig",
) -> WorkerTaskResult:
    """Send task to worker.

    - pipeline_kind ``rig`` (default): ``mode: only_rig`` and optional transform params (Auto Rig).
    - pipeline_kind ``convert``: only ``input_url`` and ``type`` (retopo / format conversion).

    Args:
        worker_url: Worker API endpoint
        input_url: URL to input model file
        task_type: Type of task (t_pose, etc.)
        transform_params: Optional dict with local_position, local_rotation, local_scale arrays (rig only)
        pipeline_kind: ``rig`` or ``convert``
    """
    task_type = normalize_task_type(task_type)
    pk = (pipeline_kind or "rig").strip().lower()
    if pk not in ("rig", "convert"):
        pk = "rig"

    async with httpx.AsyncClient() as client:
        try:
            if pk == "convert":
                payload = {
                    "input_url": input_url,
                    "type": task_type,
                }
            else:
                payload = {
                    "input_url": input_url,
                    "type": task_type,
                    "mode": "only_rig",
                }
                if transform_params:
                    if transform_params.get("local_position"):
                        payload["local_position"] = transform_params["local_position"]
                    if transform_params.get("local_rotation"):
                        payload["local_rotation"] = transform_params["local_rotation"]
                    if transform_params.get("local_scale"):
                        payload["local_scale"] = transform_params["local_scale"]
            
            response = await client.post(
                worker_url,
                json=payload,
                timeout=30.0
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract data from response
                task_id = data.get("task_id", data.get("id"))
                output_urls = data.get("output_urls", [])
                progress_page = data.get("progress_page", data.get("progress_url"))
                
                # Try to extract GUID
                guid = None
                if progress_page:
                    guid = extract_guid(progress_page)
                if not guid and output_urls:
                    guid = extract_guid(output_urls[0])
                
                return WorkerTaskResult(
                    success=True,
                    task_id=task_id,
                    output_urls=output_urls,
                    progress_page=progress_page,
                    guid=guid
                )
            else:
                return WorkerTaskResult(
                    success=False,
                    error=f"Worker returned HTTP {response.status_code}: {response.text[:200]}"
                )
                
        except httpx.TimeoutException:
            return WorkerTaskResult(success=False, error="Worker timeout")
        except Exception as e:
            return WorkerTaskResult(success=False, error=str(e))


async def send_fbx_to_glb(worker_api_url: str, input_url: str) -> FbxToGlbResult:
    """
    Convert FBX to GLB using the same worker host, but different endpoint:
    {worker_base}/api-converter-glb-to-fbx
    Payload: { "input_url": "<fbx_url>" }
    Response: { "model_name": "...", "output_url": "..." }
    """
    worker_base = get_worker_base_url(worker_api_url)
    endpoint = f"{worker_base}/api-converter-glb-to-fbx"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                endpoint,
                json={"input_url": input_url},
                timeout=90.0
            )

            if response.status_code != 200:
                return FbxToGlbResult(
                    success=False,
                    error=f"Worker returned HTTP {response.status_code}: {response.text[:200]}"
                )

            data = response.json() if response.content else {}
            model_name = data.get("model_name")
            output_url = data.get("output_url")

            if not output_url:
                return FbxToGlbResult(success=False, error="Worker response missing output_url")

            return FbxToGlbResult(success=True, model_name=model_name, output_url=output_url)
        except httpx.TimeoutException:
            return FbxToGlbResult(success=False, error="Worker timeout")
        except Exception as e:
            return FbxToGlbResult(success=False, error=str(e))


# =============================================================================
# Progress Checking
# =============================================================================
async def probe_resource_available(url: str, client: httpx.AsyncClient) -> bool:
    """
    True if the URL looks fetchable (artifact exists on worker/CDN).

    Uses HEAD first. Many reverse proxies return 403/405 for HEAD on static files while GET works;
    in those cases we fall back to a 1-byte Range GET (cheap existence check).
    """
    try:
        r = await client.head(url, timeout=PROGRESS_CHECK_TIMEOUT, follow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        if r.status_code in (403, 405, 501) or r.status_code >= 500:
            g = await client.get(
                url,
                timeout=PROGRESS_CHECK_TIMEOUT,
                follow_redirects=True,
                headers={"Range": "bytes=0-0"},
            )
            return g.status_code in (200, 206)
        return False
    except Exception:
        try:
            g = await client.get(
                url,
                timeout=PROGRESS_CHECK_TIMEOUT,
                follow_redirects=True,
                headers={"Range": "bytes=0-0"},
            )
            return g.status_code in (200, 206)
        except Exception:
            return False


async def check_url_availability(url: str, client: httpx.AsyncClient) -> bool:
    """Check if a single output URL is available (HEAD or Range GET fallback)."""
    return await probe_resource_available(url, client)


async def check_urls_batch(
    urls: List[str], 
    already_ready: set = None
) -> Tuple[List[str], int]:
    """
    Check availability of URLs in batches.
    Returns: (list of newly ready URLs, total ready count)
    """
    if already_ready is None:
        already_ready = set()
    
    # Filter out already confirmed ready URLs
    urls_to_check = [u for u in urls if u not in already_ready]
    
    if not urls_to_check:
        return [], len(already_ready)

    # Shuffle so we don't get stuck checking the same early URLs that may be generated last.
    # This allows progress to advance as soon as *any* outputs become available.
    random.shuffle(urls_to_check)
    
    newly_ready = []
    
    async with httpx.AsyncClient() as client:
        # Process in batches with concurrency limit
        semaphore = asyncio.Semaphore(PROGRESS_CONCURRENCY)
        
        async def check_with_semaphore(url: str) -> Tuple[str, bool]:
            async with semaphore:
                is_ready = await check_url_availability(url, client)
                return url, is_ready
        
        # Check batch
        batch = urls_to_check[:PROGRESS_BATCH_SIZE]
        tasks = [check_with_semaphore(url) for url in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                url, is_ready = result
                if is_ready:
                    newly_ready.append(url)
                    already_ready.add(url)
    
    return newly_ready, len(already_ready)


async def check_video_availability(guid: str, worker_base_url: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a preview or full video is available on the worker.
    Returns: (is_ready, video_url)

    Prefers ``{guid}_video_small.mp4`` for the site /api/video proxy; falls back to
    ``{guid}_video.mp4`` for older tasks or before the small encode exists.

    worker_base_url is already without /api-converter-glb (e.g., http://5.129.157.224:5267)
    """
    if not guid or not (worker_base_url or "").strip():
        return False, None

    base = worker_base_url.rstrip("/")
    small_url = f"{base}/converter/glb/{guid}/{guid}_video_small.mp4"
    large_url = f"{base}/converter/glb/{guid}/{guid}_video.mp4"

    try:
        async with httpx.AsyncClient() as client:
            if await probe_resource_available(small_url, client):
                return True, small_url
            if await probe_resource_available(large_url, client):
                return True, large_url
    except Exception:
        pass

    return False, None


def get_worker_base_url(worker_api_url: str) -> str:
    """
    HTTP origin (scheme://host:port) for paths like /converter/glb/{guid}/...

    worker_api is often either:
    - http://host:port/api-converter-glb
    - http://host:port/converter/glb/{guid}/  (task folder URL)

    Stripping only /api-converter-glb leaves the second form broken and produces
    duplicate /converter/glb/... segments in derived URLs (404 on bundle zip).
    """
    raw = (worker_api_url or "").strip()
    if not raw:
        return ""
    if "/api-converter-glb" in raw:
        raw = raw.split("/api-converter-glb", 1)[0].rstrip("/")
    elif "/converter/glb" in raw:
        raw = raw.split("/converter/glb", 1)[0].rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return raw.rstrip("/")


# =============================================================================
# Queue Status
# =============================================================================
@dataclass
class WorkerQueueStatus:
    """Detailed worker queue status"""
    url: str
    available: bool
    total_active: int = 0
    total_pending: int = 0
    queue_size: int = 0
    max_concurrent: int = 1
    avg_task_time: float = 900.0  # Default 15 min in seconds
    error: Optional[str] = None
    # Flattened strings from worker JSON (active_tasks / similar) for matching Task.worker_task_id / guid / URLs
    active_refs: List[str] = field(default_factory=list)
    # True if JSON contained a non-null active_tasks (or alias) key — enables "lost on worker" detection
    has_active_tasks_payload: bool = False

    @property
    def port(self) -> str:
        """Extract port from URL for display"""
        try:
            return self.url.split(':')[-1].split('/')[0]
        except:
            return "?"


# Expected GET worker JSON (api-converter-glb root): total_active, queue_size, max_concurrent,
# average_time_converting_task, plus optional active_tasks (or activeTasks / running_tasks) — list or dict
# of jobs; each job may include task_id, id, guid, output_urls, progress_page strings for correlation.


def _walk_json_strings(obj: Any, depth: int = 0, max_depth: int = 10) -> List[str]:
    """Collect string leaves from nested JSON for substring matching against our Task fields."""
    if depth > max_depth or obj is None:
        return []
    if isinstance(obj, str):
        s = obj.strip()
        return [s] if s else []
    if isinstance(obj, (int, float)):
        return [str(obj)]
    if isinstance(obj, dict):
        out: List[str] = []
        for k, v in obj.items():
            # api-converter-glb often maps task_id / guid -> object; ids live in dict keys
            if isinstance(k, str):
                ks = k.strip()
                if ks:
                    out.append(ks)
            out.extend(_walk_json_strings(v, depth + 1, max_depth))
        return out
    if isinstance(obj, (list, tuple)):
        out = []
        for x in obj:
            out.extend(_walk_json_strings(x, depth + 1, max_depth))
        return out
    return []


def parse_worker_active_tasks_from_json(data: dict) -> Tuple[List[str], bool]:
    """
    Extract flattened reference strings and whether the payload included explicit task-bucket keys.
    Merge active_tasks + processing_tasks + pending_tasks — workers may use different buckets.
    has_payload True + empty refs means the worker reports zero running jobs (idle).
    """
    if not isinstance(data, dict):
        return [], False
    keys = (
        "active_tasks",
        "activeTasks",
        "running_tasks",
        "runningTasks",
        "current_tasks",
        "currentTasks",
        "processing_tasks",
        "processingTasks",
        "pending_tasks",
        "pendingTasks",
    )
    has_payload = False
    all_refs: List[str] = []
    for key in keys:
        if key not in data:
            continue
        has_payload = True
        val = data[key]
        if val is None:
            continue
        if isinstance(val, (list, dict)):
            all_refs.extend(_walk_json_strings(val))
        elif isinstance(val, (str, int, float)):
            s = str(val).strip()
            if s:
                all_refs.append(s)
    # Dedupe while keeping order
    seen = set()
    deduped: List[str] = []
    for s in all_refs:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped, has_payload


def task_visible_on_worker_refs(
    worker_task_id: Optional[str],
    guid: Optional[str],
    output_urls: Optional[List[str]],
    active_refs: List[str],
    has_active_tasks_payload: bool,
) -> bool:
    """
    True if we cannot conclude the job was dropped, or the task matches worker-reported activity.
    False when the worker explicitly lists active tasks but none reference our ids/urls/guid.
    """
    if not has_active_tasks_payload:
        return True
    if not active_refs:
        return False
    blob_lc = "\n".join(active_refs).lower()
    tid = (worker_task_id or "").strip().lower()
    if len(tid) >= 4 and tid in blob_lc:
        return True
    g = (guid or "").strip().lower()
    if len(g) >= 8 and g in blob_lc:
        return True
    for u in output_urls or []:
        if not isinstance(u, str):
            continue
        s = (u.strip().lower())
        if len(s) >= 16 and s in blob_lc:
            return True
        for segment in s.split("/"):
            seg = segment.strip().lower()
            if len(seg) >= 8 and seg in blob_lc:
                return True
    return False


def normalize_worker_url_key(url: str) -> str:
    return (url or "").strip().rstrip("/")


def get_worker_active_lookup(queue_status: Optional["GlobalQueueStatus"]) -> Dict[str, Tuple[List[str], bool]]:
    """Map normalized worker root URL -> (active_refs, has_active_tasks_payload)."""
    if queue_status is None:
        return {}
    out: Dict[str, Tuple[List[str], bool]] = {}
    for w in queue_status.workers:
        if not w.available:
            continue
        key = normalize_worker_url_key(w.url)
        out[key] = (list(w.active_refs), bool(w.has_active_tasks_payload))
    return out


def lookup_worker_queue_entry(
    task_worker_api: Optional[str],
    lookup: Dict[str, Tuple[List[str], bool]],
) -> Optional[Tuple[List[str], bool]]:
    if not lookup:
        return None
    w = normalize_worker_url_key(task_worker_api or "")
    if not w:
        return None
    if w in lookup:
        return lookup[w]
    for k, v in lookup.items():
        if not k:
            continue
        if w.startswith(k) or k.startswith(w):
            return v
    return None


def find_worker_queue_status_for_task(
    task_worker_api: Optional[str],
    queue_status: Optional["GlobalQueueStatus"],
) -> Optional[WorkerQueueStatus]:
    """
    Match task.worker_api (api-converter-glb root or similar) to a row from get_global_queue_status.
    """
    if not queue_status or not (task_worker_api or "").strip():
        return None
    wk = normalize_worker_url_key(task_worker_api)
    for w in queue_status.workers:
        ku = normalize_worker_url_key(w.url)
        if ku == wk:
            return w
        if wk.startswith(ku) or ku.startswith(wk):
            return w
    return None


@dataclass
class GlobalQueueStatus:
    """Global queue status across all workers"""
    workers: List[WorkerQueueStatus]
    total_active: int
    total_pending: int
    total_queue: int
    available_workers: int
    total_workers: int
    estimated_wait_seconds: int
    estimated_wait_formatted: str


def _safe_worker_int(value: Any, default: int = 0) -> int:
    """Coerce worker JSON counters; bad types must not break dispatch (queue_size <= 0) filters."""
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_worker_float(value: Any, default: float = 900.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


async def get_worker_queue_status(worker_url: str, client: httpx.AsyncClient) -> WorkerQueueStatus:
    """Get detailed queue status from a single worker"""
    try:
        response = await client.get(worker_url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            if not isinstance(data, dict):
                data = {}
            active_refs, has_active_payload = parse_worker_active_tasks_from_json(data)
            max_c = max(1, _safe_worker_int(data.get("max_concurrent"), 1))
            return WorkerQueueStatus(
                url=worker_url,
                available=True,
                total_active=_safe_worker_int(data.get("total_active"), 0),
                total_pending=_safe_worker_int(data.get("total_pending"), 0),
                queue_size=_safe_worker_int(data.get("queue_size"), 0),
                max_concurrent=max_c,
                avg_task_time=_safe_worker_float(data.get("average_time_converting_task"), 900.0),
                active_refs=active_refs,
                has_active_tasks_payload=has_active_payload,
            )
        return WorkerQueueStatus(
            url=worker_url,
            available=False,
            error=f"HTTP {response.status_code}"
        )
    except Exception as e:
        return WorkerQueueStatus(
            url=worker_url,
            available=False,
            error=str(e)
        )


async def get_global_queue_status(db: Optional[AsyncSession] = None) -> GlobalQueueStatus:
    """Get queue status from all workers and calculate wait time"""
    worker_urls = await get_configured_workers(db)
    async with httpx.AsyncClient() as client:
        tasks = [get_worker_queue_status(url, client) for url in worker_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        workers = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                workers.append(WorkerQueueStatus(
                    url=worker_urls[i],
                    available=False,
                    error=str(result)
                ))
            else:
                workers.append(result)
        
        # Calculate totals
        available_workers = [w for w in workers if w.available]
        total_active = sum(w.total_active for w in available_workers)
        total_pending = sum(w.total_pending for w in available_workers)
        total_queue = sum(w.queue_size for w in available_workers)
        
        # Calculate estimated wait time
        # Formula: (pending + active tasks) * avg_time / num_available_workers
        avg_task_time = 900  # 15 minutes default
        if available_workers:
            # Use average from workers that have data
            times = [w.avg_task_time for w in available_workers if w.avg_task_time > 0]
            if times:
                avg_task_time = sum(times) / len(times)
        
        num_workers = len(available_workers) if available_workers else 1
        tasks_ahead = total_pending + total_active
        
        # Each worker can process 1 task at a time (max_concurrent=1)
        # So wait time = (tasks_ahead / num_workers) * avg_task_time
        estimated_wait_seconds = int((tasks_ahead / num_workers) * avg_task_time) if num_workers > 0 else 0
        
        # Format wait time
        if estimated_wait_seconds < 60:
            wait_formatted = "< 1 мин"
        elif estimated_wait_seconds < 3600:
            minutes = estimated_wait_seconds // 60
            wait_formatted = f"~{minutes} мин"
        else:
            hours = estimated_wait_seconds // 3600
            minutes = (estimated_wait_seconds % 3600) // 60
            wait_formatted = f"~{hours}ч {minutes}мин"
        
        return GlobalQueueStatus(
            workers=workers,
            total_active=total_active,
            total_pending=total_pending,
            total_queue=total_queue,
            available_workers=len(available_workers),
            total_workers=len(workers),
            estimated_wait_seconds=estimated_wait_seconds,
            estimated_wait_formatted=wait_formatted
        )

