"""TDD tests for AsyncWorker."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

# Support both package and direct execution
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from renderfin_mcp.task_queue import TaskQueue
from renderfin_mcp.async_worker import AsyncWorker


@pytest.fixture
def temp_queue(tmp_path):
    """Create a TaskQueue with temporary storage."""
    queue_file = tmp_path / "tasks.json"
    return TaskQueue(str(queue_file))


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


class TestAsyncWorker:
    """Test AsyncWorker functionality."""
    
    @pytest.fixture(autouse=True)
    def reset_worker(self):
        """Reset worker state before each test."""
        AsyncWorker._running = False
        AsyncWorker._task = None
        yield
        # Cleanup: cancel any running task
        if AsyncWorker._task and not AsyncWorker._task.done():
            AsyncWorker._task.cancel()
            try:
                asyncio.get_event_loop().run_until_complete(AsyncWorker._task)
            except (asyncio.CancelledError, RuntimeError):
                pass
        AsyncWorker._running = False
        AsyncWorker._task = None
    
    @pytest.mark.asyncio
    async def test_worker_singleton(self, temp_queue):
        """Test that only one worker instance runs at a time."""
        # Start worker twice
        await AsyncWorker.ensure_running(temp_queue)
        task1 = AsyncWorker._task
        
        await AsyncWorker.ensure_running(temp_queue)
        task2 = AsyncWorker._task
        
        # Should be the same task
        assert task1 is task2
        assert AsyncWorker._running is True
        
        # Cleanup
        if task1:
            task1.cancel()
            try:
                await task1
            except asyncio.CancelledError:
                pass
    
    @pytest.mark.asyncio
    async def test_worker_processes_task(self, temp_queue, temp_output_dir):
        """Test worker processes a task and saves image."""
        output_file = str(temp_output_dir / "test_image.png")
        
        # Add task
        task_id = temp_queue.add_task(
            prompt="A test image",
            output_path=output_file,
            aspect_ratio=1.0
        )
        
        # Mock HTTP responses
        mock_render_response = MagicMock()
        mock_render_response.json.return_value = {"output_url": "https://example.com/image.png"}
        mock_render_response.raise_for_status = MagicMock()
        
        mock_image_response = MagicMock()
        mock_image_response.content = b"fake png data"
        mock_image_response.raise_for_status = MagicMock()
        
        # Create a mock client that returns different responses for different calls
        async def mock_post(*args, **kwargs):
            return mock_render_response
        
        async def mock_get(*args, **kwargs):
            return mock_image_response
        
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        
        with patch("renderfin_mcp.async_worker.httpx.AsyncClient", return_value=mock_client):
            # Process the task directly
            task = temp_queue.pop_task()
            await AsyncWorker._process_task(task, temp_queue)
        
        # Verify task completed
        tasks = temp_queue._load_tasks()
        assert tasks[0]["status"] == "completed"
        
        # Verify file was created
        assert Path(output_file).exists()
        assert Path(output_file).read_bytes() == b"fake png data"
    
    @pytest.mark.asyncio
    async def test_worker_handles_failure(self, temp_queue, temp_output_dir):
        """Test worker marks task as failed on error."""
        output_file = str(temp_output_dir / "test_fail.png")
        
        # Add task
        task_id = temp_queue.add_task(
            prompt="A failing task",
            output_path=output_file,
            aspect_ratio=1.0
        )
        
        # Mock HTTP to raise exception
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Network error"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        
        with patch("renderfin_mcp.async_worker.httpx.AsyncClient", return_value=mock_client):
            task = temp_queue.pop_task()
            await AsyncWorker._process_task(task, temp_queue)
        
        # Verify task failed
        tasks = temp_queue._load_tasks()
        assert tasks[0]["status"] == "failed"
        assert "Network error" in tasks[0]["error"]
    
    @pytest.mark.asyncio
    async def test_worker_loop_polls_queue(self, temp_queue):
        """Test worker loop continues to poll when queue is empty."""
        # Start worker with empty queue
        await AsyncWorker.ensure_running(temp_queue)
        
        # Give it a moment to run
        await asyncio.sleep(0.1)
        
        # Worker should still be running (waiting for tasks)
        assert AsyncWorker._running is True
        
        # Cleanup
        AsyncWorker._task.cancel()
        try:
            await AsyncWorker._task
        except asyncio.CancelledError:
            pass
    
    @pytest.mark.asyncio
    async def test_worker_graceful_shutdown(self, temp_queue):
        """Test worker shuts down gracefully on cancel."""
        await AsyncWorker.ensure_running(temp_queue)
        
        assert AsyncWorker._running is True
        
        # Give worker time to enter sleep state
        await asyncio.sleep(0.1)
        
        # Cancel the worker
        AsyncWorker._task.cancel()
        
        try:
            await AsyncWorker._task
        except asyncio.CancelledError:
            pass
        
        # Give event loop a moment to process finally block
        await asyncio.sleep(0.05)
        
        # Worker should have reset _running flag
        assert AsyncWorker._running is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
