"""Async worker for background image generation."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

# Support both package and direct execution
try:
    from renderfin_mcp.logger import logger, log_success, log_error, log_queued
    from renderfin_mcp.task_queue import TaskQueue
except ImportError:
    from logger import logger, log_success, log_error, log_queued
    from task_queue import TaskQueue

# RenderFin API endpoint
RENDERFIN_API_URL = "https://renderfin.com/api-render"

# Worker configuration
POLL_INTERVAL = 3  # seconds to wait when queue is empty
REQUEST_TIMEOUT = 300  # 5 minutes timeout for requests


def update_metadata_json(output_path: str, data: dict) -> None:
    """Write/update metadata JSON file next to image.
    
    Args:
        output_path: Path to the image file (JSON will be output_path + .json)
        data: Metadata dictionary to write
    """
    json_path = Path(f"{output_path}.json")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


class AsyncWorker:
    """Singleton async worker that processes image generation tasks."""
    
    _running: bool = False
    _task: asyncio.Task | None = None
    
    @classmethod
    async def ensure_running(cls, queue: TaskQueue) -> None:
        """Start the worker if not already running.
        
        Args:
            queue: TaskQueue instance to poll for tasks.
        """
        if not cls._running:
            cls._running = True
            cls._task = asyncio.create_task(cls._worker_loop(queue))
            logger.debug("AsyncWorker started", extra={"event": "WORKER_START"})
    
    @classmethod
    async def _worker_loop(cls, queue: TaskQueue) -> None:
        """Main worker loop - polls queue and processes tasks.
        
        Args:
            queue: TaskQueue instance to poll.
        """
        try:
            while True:
                task = queue.pop_task()
                if task:
                    logger.info(
                        f"Processing task {task['id']}: {task['prompt'][:30]}...",
                        extra={"event": "TASK_START"}
                    )
                    await cls._process_task(task, queue)
                else:
                    # No pending tasks, wait before polling again
                    await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.debug("AsyncWorker shutting down", extra={"event": "WORKER_STOP"})
            # Reset state before re-raising
            cls._running = False
            cls._task = None
            raise  # Re-raise to signal proper cancellation
        except Exception as e:
            logger.error(f"Worker loop error: {e}", extra={"event": "WORKER_ERROR"})
        finally:
            cls._running = False
            cls._task = None
    
    @classmethod
    async def _process_task(cls, task: dict, queue: TaskQueue) -> None:
        """Process a single image generation task.
        
        Args:
            task: Task dict with prompt, output_path, aspect_ratio.
            queue: TaskQueue for updating task status.
        """
        task_id = task["id"]
        output_path_str = task["output_path"]
        output_path = Path(output_path_str)
        
        # Base metadata from task
        metadata = {
            "task_id": task_id,
            "prompt": task["prompt"],
            "output_path": output_path_str,
            "aspect_ratio": task["aspect_ratio"],
            "queued_at": task.get("created_at"),
        }
        
        try:
            # Update JSON: processing
            started_at = datetime.now().isoformat()
            metadata.update({
                "status": "processing",
                "started_at": started_at,
            })
            update_metadata_json(output_path_str, metadata)
            
            # Prepare request payload
            payload = {
                "prompt": task["prompt"],
                "aspect_ratio": task["aspect_ratio"],
            }
            
            # Update JSON: sending request
            request_sent_at = datetime.now().isoformat()
            metadata.update({
                "status": "requesting",
                "request_payload": payload,
                "request_url": RENDERFIN_API_URL,
                "request_sent_at": request_sent_at,
            })
            update_metadata_json(output_path_str, metadata)
            
            logger.debug(f"POST to {RENDERFIN_API_URL} with payload: {payload}", extra={"event": "HTTP_REQUEST"})
            
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                # Request image generation
                response = await client.post(RENDERFIN_API_URL, json=payload)
                response_received_at = datetime.now().isoformat()
                logger.debug(f"Response status: {response.status_code}", extra={"event": "HTTP_RESPONSE"})
                response.raise_for_status()
                
                result = response.json()
                output_url = result["output_url"]
                
                # Update JSON: downloading (got URL, waiting for image)
                metadata.update({
                    "status": "downloading",
                    "output_url": output_url,
                    "render_response": result,
                    "response_received_at": response_received_at,
                    "response_status_code": response.status_code,
                })
                update_metadata_json(output_path_str, metadata)
                
                # Download the generated image (with retry - image may not be ready immediately)
                # Rendering can take 2-5 minutes
                max_retries = 60  # 60 x 5 = 300 seconds = 5 minutes max wait
                retry_delay = 5  # seconds between download attempts
                image_response = None
                
                for attempt in range(max_retries):
                    image_response = await client.get(output_url)
                    if image_response.status_code == 200:
                        break
                    elif image_response.status_code == 404:
                        # Image not ready yet, wait and retry
                        logger.debug(
                            f"Image not ready (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s",
                            extra={"event": "DOWNLOAD_RETRY"}
                        )
                        metadata.update({
                            "download_attempt": attempt + 1,
                            "last_download_attempt_at": datetime.now().isoformat(),
                        })
                        update_metadata_json(output_path_str, metadata)
                        await asyncio.sleep(retry_delay)
                    else:
                        image_response.raise_for_status()
                
                if image_response is None or image_response.status_code != 200:
                    raise RuntimeError(f"Failed to download image after {max_retries} attempts")
                
                # Ensure output directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Save image to file
                image_bytes = image_response.content
                output_path.write_bytes(image_bytes)
            
            # Update JSON: completed
            completed_at = datetime.now().isoformat()
            metadata.update({
                "status": "completed",
                "completed_at": completed_at,
                "file_size": len(image_bytes),
            })
            update_metadata_json(output_path_str, metadata)
            
            # Mark task as completed in queue
            queue.complete_task(task_id)
            log_success(task_id, output_url, str(output_path))
            
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            metadata.update({
                "status": "failed",
                "error": error_msg,
                "failed_at": datetime.now().isoformat(),
            })
            update_metadata_json(output_path_str, metadata)
            queue.fail_task(task_id, error_msg)
            log_error(task_id, error_msg)
            
        except Exception as e:
            error_msg = str(e)
            metadata.update({
                "status": "failed",
                "error": error_msg,
                "failed_at": datetime.now().isoformat(),
            })
            update_metadata_json(output_path_str, metadata)
            queue.fail_task(task_id, error_msg)
            log_error(task_id, error_msg)
    
    @classmethod
    def is_running(cls) -> bool:
        """Check if worker is currently running."""
        return cls._running


# Convenience function for starting worker
async def start_worker(queue: TaskQueue) -> None:
    """Start the async worker if not already running.
    
    Args:
        queue: TaskQueue to process.
    """
    await AsyncWorker.ensure_running(queue)
