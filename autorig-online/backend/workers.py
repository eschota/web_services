"""
Worker integration for AutoRig Online
Handles communication with conversion workers
"""
import re
import asyncio
from typing import Optional, List, Tuple
from dataclasses import dataclass
import random

import httpx

from config import (
    WORKERS, 
    PROGRESS_BATCH_SIZE, 
    PROGRESS_CONCURRENCY,
    PROGRESS_CHECK_TIMEOUT
)


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


async def get_all_workers_status() -> List[WorkerInfo]:
    """Get status of all workers"""
    async with httpx.AsyncClient() as client:
        tasks = [get_worker_load(url, client) for url in WORKERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        workers = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                workers.append(WorkerInfo(
                    url=WORKERS[i], 
                    available=False, 
                    error=str(result)
                ))
            else:
                workers.append(result)
        return workers


async def select_best_worker() -> Optional[str]:
    """Select the least busy available worker"""
    workers = await get_all_workers_status()
    available = [w for w in workers if w.available]
    
    if not available:
        # If no workers respond to GET, try them anyway
        return WORKERS[0] if WORKERS else None
    
    # Sort by load, return least busy
    available.sort(key=lambda w: w.load)
    return available[0].url


async def send_task_to_worker(
    worker_url: str, 
    input_url: str, 
    task_type: str = "t_pose"
) -> WorkerTaskResult:
    """Send conversion task to worker"""
    async with httpx.AsyncClient() as client:
        try:
            payload = {
                "input_url": input_url,
                "type": task_type
            }
            
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
async def check_url_availability(url: str, client: httpx.AsyncClient) -> bool:
    """Check if a single URL is available (returns 200)"""
    try:
        response = await client.head(url, timeout=PROGRESS_CHECK_TIMEOUT, follow_redirects=True)
        return response.status_code == 200
    except:
        return False


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
    Check if video file is available.
    Returns: (is_ready, video_url)
    
    Video is located at: {worker_base}/converter/glb/{guid}/{guid}_video.mp4
    worker_base_url is already without /api-converter-glb (e.g., http://5.129.157.224:5267)
    """
    if not guid:
        return False, None
    
    try:
        # worker_base_url is already the base (e.g., http://5.129.157.224:5267)
        video_url = f"{worker_base_url}/converter/glb/{guid}/{guid}_video.mp4"
        
        async with httpx.AsyncClient() as client:
            response = await client.head(video_url, timeout=5.0, follow_redirects=True)
            if response.status_code == 200:
                return True, video_url
    except Exception as e:
        pass
    
    return False, None


def get_worker_base_url(worker_api_url: str) -> str:
    """Extract base URL from worker API URL"""
    # http://5.129.157.224:5267/api-converter-glb -> http://5.129.157.224:5267
    return worker_api_url.replace('/api-converter-glb', '')


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
    
    @property
    def port(self) -> str:
        """Extract port from URL for display"""
        try:
            return self.url.split(':')[-1].split('/')[0]
        except:
            return "?"


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


async def get_worker_queue_status(worker_url: str, client: httpx.AsyncClient) -> WorkerQueueStatus:
    """Get detailed queue status from a single worker"""
    try:
        response = await client.get(worker_url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            return WorkerQueueStatus(
                url=worker_url,
                available=True,
                total_active=data.get("total_active", 0),
                total_pending=data.get("total_pending", 0),
                queue_size=data.get("queue_size", 0),
                max_concurrent=data.get("max_concurrent", 1),
                avg_task_time=data.get("average_time_converting_task", 900.0)
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


async def get_global_queue_status() -> GlobalQueueStatus:
    """Get queue status from all workers and calculate wait time"""
    async with httpx.AsyncClient() as client:
        tasks = [get_worker_queue_status(url, client) for url in WORKERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        workers = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                workers.append(WorkerQueueStatus(
                    url=WORKERS[i],
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

