"""Integration tests for the full schedule_render flow."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.fixture(autouse=True)
def reset_worker():
    """Reset worker state before each test."""
    AsyncWorker._running = False
    AsyncWorker._task = None
    yield
    # Cleanup: cancel any running task
    if AsyncWorker._task and not AsyncWorker._task.done():
        AsyncWorker._task.cancel()
    AsyncWorker._running = False
    AsyncWorker._task = None


class TestScheduleRenderIntegration:
    """Integration tests for the full render scheduling flow."""
    
    @pytest.mark.asyncio
    async def test_schedule_and_complete_full_flow(self, temp_queue, temp_output_dir):
        """Test full flow: schedule -> worker processes -> file saved."""
        output_file = str(temp_output_dir / "integration_test.png")
        
        # 1. Add task to queue (simulating schedule_render tool)
        task_id = temp_queue.add_task(
            prompt="A beautiful mountain landscape at sunset",
            output_path=output_file,
            aspect_ratio=1.5
        )
        
        assert task_id is not None
        assert temp_queue.pending_count() == 1
        
        # 2. Mock HTTP responses
        mock_render_response = MagicMock()
        mock_render_response.json.return_value = {"output_url": "https://renderfin.com/images/abc123.png"}
        mock_render_response.raise_for_status = MagicMock()
        
        mock_image_response = MagicMock()
        mock_image_response.content = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100  # Fake PNG header
        mock_image_response.raise_for_status = MagicMock()
        
        async def mock_post(*args, **kwargs):
            return mock_render_response
        
        async def mock_get(*args, **kwargs):
            return mock_image_response
        
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        
        # 3. Start worker with mocked HTTP
        with patch("renderfin_mcp.async_worker.httpx.AsyncClient", return_value=mock_client):
            await AsyncWorker.ensure_running(temp_queue)
            
            # Give worker time to process
            await asyncio.sleep(0.5)
        
        # 4. Verify task completed
        tasks = temp_queue._load_tasks()
        assert len(tasks) == 1
        assert tasks[0]["status"] == "completed"
        assert tasks[0]["id"] == task_id
        
        # 5. Verify file was created
        assert Path(output_file).exists()
        content = Path(output_file).read_bytes()
        assert content.startswith(b"\x89PNG")  # PNG magic bytes
        
        # Cleanup worker
        if AsyncWorker._task:
            AsyncWorker._task.cancel()
            try:
                await AsyncWorker._task
            except asyncio.CancelledError:
                pass
    
    @pytest.mark.asyncio
    async def test_multiple_tasks_processed_in_order(self, temp_queue, temp_output_dir):
        """Test that multiple scheduled tasks are processed in FIFO order."""
        output_file1 = str(temp_output_dir / "task1.png")
        output_file2 = str(temp_output_dir / "task2.png")
        output_file3 = str(temp_output_dir / "task3.png")
        
        # Schedule 3 tasks
        task_id1 = temp_queue.add_task("First task", output_file1, 1.0)
        task_id2 = temp_queue.add_task("Second task", output_file2, 1.0)
        task_id3 = temp_queue.add_task("Third task", output_file3, 1.0)
        
        assert temp_queue.pending_count() == 3
        
        # Mock HTTP
        call_order = []
        
        async def mock_post(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            call_order.append(prompt)
            mock_response = MagicMock()
            mock_response.json.return_value = {"output_url": f"https://example.com/{prompt}.png"}
            mock_response.raise_for_status = MagicMock()
            return mock_response
        
        async def mock_get(*args, **kwargs):
            mock_response = MagicMock()
            mock_response.content = b"fake png"
            mock_response.raise_for_status = MagicMock()
            return mock_response
        
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        
        with patch("renderfin_mcp.async_worker.httpx.AsyncClient", return_value=mock_client):
            await AsyncWorker.ensure_running(temp_queue)
            
            # Give worker time to process all tasks
            await asyncio.sleep(1.0)
        
        # Verify processing order
        assert call_order == ["First task", "Second task", "Third task"]
        
        # Verify all files created
        assert Path(output_file1).exists()
        assert Path(output_file2).exists()
        assert Path(output_file3).exists()
        
        # Verify all tasks completed
        tasks = temp_queue._load_tasks()
        assert all(t["status"] == "completed" for t in tasks)
        
        # Cleanup
        if AsyncWorker._task:
            AsyncWorker._task.cancel()
            try:
                await AsyncWorker._task
            except asyncio.CancelledError:
                pass
    
    @pytest.mark.asyncio
    async def test_failed_task_does_not_block_others(self, temp_queue, temp_output_dir):
        """Test that a failed task doesn't block subsequent tasks."""
        output_file1 = str(temp_output_dir / "failing.png")
        output_file2 = str(temp_output_dir / "success.png")
        
        # Schedule tasks - first will fail, second should succeed
        task_id1 = temp_queue.add_task("Failing task", output_file1, 1.0)
        task_id2 = temp_queue.add_task("Successful task", output_file2, 1.0)
        
        call_count = [0]
        
        async def mock_post(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call fails
                raise Exception("Network timeout")
            # Second call succeeds
            mock_response = MagicMock()
            mock_response.json.return_value = {"output_url": "https://example.com/ok.png"}
            mock_response.raise_for_status = MagicMock()
            return mock_response
        
        async def mock_get(*args, **kwargs):
            mock_response = MagicMock()
            mock_response.content = b"success image"
            mock_response.raise_for_status = MagicMock()
            return mock_response
        
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.get = mock_get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        
        with patch("renderfin_mcp.async_worker.httpx.AsyncClient", return_value=mock_client):
            await AsyncWorker.ensure_running(temp_queue)
            await asyncio.sleep(1.0)
        
        # First task should be failed, second completed
        tasks = temp_queue._load_tasks()
        task1 = next(t for t in tasks if t["id"] == task_id1)
        task2 = next(t for t in tasks if t["id"] == task_id2)
        
        assert task1["status"] == "failed"
        assert "Network timeout" in task1["error"]
        
        assert task2["status"] == "completed"
        assert Path(output_file2).exists()
        
        # Cleanup
        if AsyncWorker._task:
            AsyncWorker._task.cancel()
            try:
                await AsyncWorker._task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
