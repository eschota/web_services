"""RenderFin MCP Server - generates images via renderfin.com API."""

import logging
from datetime import datetime
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

# Support both package and direct execution modes
try:
    from renderfin_mcp.logger import logger, log_request, log_success, log_error, log_queued
    from renderfin_mcp.task_queue import TaskQueue, is_absolute_path
    from renderfin_mcp.async_worker import AsyncWorker, update_metadata_json
except ImportError:
    from logger import logger, log_request, log_success, log_error, log_queued
    from task_queue import TaskQueue, is_absolute_path
    from async_worker import AsyncWorker, update_metadata_json

# RenderFin API endpoint
RENDERFIN_API_URL = "https://renderfin.com/api-render"

# Task queue for scheduled renders (file-based with locking)
task_queue = TaskQueue("./vars/tasks.json")

# Disable duplicate logging from MCP internals
logging.getLogger("mcp").setLevel(logging.WARNING)
logging.getLogger("mcp.server").setLevel(logging.WARNING)

# Initialize MCP server
mcp = FastMCP("RenderFin")


@mcp.tool()
async def generate_image(
    prompt: str,
    output_file: str,
    aspect_ratio: float = 1.0,
) -> dict:
    """Generate an image using RenderFin API and save it to a local file.

    Args:
        prompt: Text description of the image to generate.
        output_file: Path where the PNG image will be saved.
        aspect_ratio: Width/height ratio (1.0 = square, 1.777 = 16:9, 0.5625 = 9:16).

    Returns:
        Dictionary with output_url (remote URL) and local_path (saved file path).
    """
    log_request("generate_image", {
        "prompt": prompt[:50] + "..." if len(prompt) > 50 else prompt,
        "output_file": output_file,
        "aspect_ratio": aspect_ratio,
    })
    
    try:
        # Prepare request payload
        payload = {
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            # Send generation request to RenderFin
            response = await client.post(RENDERFIN_API_URL, json=payload)
            response.raise_for_status()

            result = response.json()
            output_url = result["output_url"]

            # Download the generated image
            image_response = await client.get(output_url)
            image_response.raise_for_status()

            # Ensure output directory exists
            output_path = Path(output_file).resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save image to file
            output_path.write_bytes(image_response.content)

        log_success("sync", output_url, str(output_path))
        
        return {
            "output_url": output_url,
            "local_path": str(output_path),
        }
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
        log_error("sync", error_msg)
        raise RuntimeError(error_msg) from e
        
    except Exception as e:
        log_error("sync", str(e))
        raise


@mcp.tool()
async def schedule_render(
    prompt: str,
    absolute_output_path: str,
    aspect_ratio: float = 1.0,
) -> dict:
    """Schedule an image generation task (fire-and-forget).
    
    The task runs in background. Image will be saved when ready.
    Returns immediately after scheduling.

    Args:
        prompt: Text description of the image to generate.
        absolute_output_path: ABSOLUTE path where PNG will be saved (e.g. C:/images/out.png or /home/user/img.png).
        aspect_ratio: Width/height ratio (1.0 = square, 1.777 = 16:9, 0.5625 = 9:16).

    Returns:
        Dictionary with status message and output path.
    """
    log_request("schedule_render", {
        "prompt": prompt[:50] + "..." if len(prompt) > 50 else prompt,
        "absolute_output_path": absolute_output_path,
        "aspect_ratio": aspect_ratio,
    })
    
    # Validate absolute path
    if not is_absolute_path(absolute_output_path):
        error_msg = f"Path must be absolute (e.g. C:/images/out.png or /home/user/img.png), got: {absolute_output_path}"
        log_error("schedule", error_msg)
        return {
            "status": "error",
            "message": error_msg,
        }
    
    # Add task to queue
    task_id = task_queue.add_task(
        prompt=prompt,
        output_path=absolute_output_path,
        aspect_ratio=aspect_ratio,
    )
    
    # Create initial metadata JSON with status: queued
    queued_at = datetime.now().isoformat()
    metadata = {
        "task_id": task_id,
        "status": "queued",
        "prompt": prompt,
        "output_path": absolute_output_path,
        "aspect_ratio": aspect_ratio,
        "queued_at": queued_at,
    }
    update_metadata_json(absolute_output_path, metadata)
    
    log_queued(task_id, absolute_output_path)
    
    # Ensure worker is running
    await AsyncWorker.ensure_running(task_queue)
    
    return {
        "status": "scheduled",
        "task_id": task_id,
        "message": f"Task accepted. Image will be saved to: {absolute_output_path}",
        "output_path": absolute_output_path,
        "metadata_json": f"{absolute_output_path}.json",
    }


def main():
    """Entry point for the MCP server."""
    logger.info("Starting RenderFin MCP Server", extra={"event": "STARTUP"})
    mcp.run()


if __name__ == "__main__":
    main()
