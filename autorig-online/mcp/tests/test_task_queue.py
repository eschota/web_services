"""TDD tests for TaskQueue (JSON + filelock)."""

import json
import tempfile
from pathlib import Path

import pytest

# Support both package and direct execution
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from renderfin_mcp.task_queue import TaskQueue, is_absolute_path


class TestIsAbsolutePath:
    """Test absolute path validation."""
    
    def test_windows_path_with_drive(self):
        """Windows paths with drive letter are absolute."""
        assert is_absolute_path("C:/images/test.png") is True
        assert is_absolute_path("D:\\output\\file.png") is True
        assert is_absolute_path("R:/work/renderfin-mcp/test.png") is True
    
    def test_unix_path(self):
        """Unix paths starting with / are absolute."""
        assert is_absolute_path("/home/user/images/test.png") is True
        assert is_absolute_path("/var/data/output.png") is True
    
    def test_relative_paths_rejected(self):
        """Relative paths should be rejected."""
        assert is_absolute_path("images/test.png") is False
        assert is_absolute_path("./output/file.png") is False
        assert is_absolute_path("../parent/file.png") is False
        assert is_absolute_path("test.png") is False


class TestTaskQueue:
    """Test TaskQueue operations."""
    
    @pytest.fixture
    def temp_queue(self, tmp_path):
        """Create a TaskQueue with temporary storage."""
        queue_file = tmp_path / "tasks.json"
        return TaskQueue(str(queue_file))
    
    def test_add_task(self, temp_queue):
        """Test adding a task to the queue."""
        task_id = temp_queue.add_task(
            prompt="A beautiful sunset",
            output_path="C:/images/sunset.png",
            aspect_ratio=1.5
        )
        
        assert task_id is not None
        assert len(task_id) > 0
        
        # Verify task was added
        tasks = temp_queue._load_tasks()
        assert len(tasks) == 1
        assert tasks[0]["prompt"] == "A beautiful sunset"
        assert tasks[0]["output_path"] == "C:/images/sunset.png"
        assert tasks[0]["aspect_ratio"] == 1.5
        assert tasks[0]["status"] == "pending"
    
    def test_pop_task_returns_pending(self, temp_queue):
        """Test popping returns first pending task and marks as processing."""
        task_id = temp_queue.add_task(
            prompt="Test image",
            output_path="D:/test.png",
            aspect_ratio=1.0
        )
        
        task = temp_queue.pop_task()
        
        assert task is not None
        assert task["id"] == task_id
        assert task["prompt"] == "Test image"
        
        # Verify status changed to processing
        tasks = temp_queue._load_tasks()
        assert tasks[0]["status"] == "processing"
    
    def test_pop_task_empty_queue(self, temp_queue):
        """Test popping from empty queue returns None."""
        task = temp_queue.pop_task()
        assert task is None
    
    def test_pop_task_skips_processing(self, temp_queue):
        """Test pop skips tasks already being processed."""
        temp_queue.add_task("Task 1", "C:/a.png", 1.0)
        
        # Pop first task (now processing)
        temp_queue.pop_task()
        
        # Add second task
        task_id_2 = temp_queue.add_task("Task 2", "C:/b.png", 1.0)
        
        # Pop should return second task, not first
        task = temp_queue.pop_task()
        assert task is not None
        assert task["id"] == task_id_2
    
    def test_complete_task(self, temp_queue):
        """Test marking task as completed."""
        task_id = temp_queue.add_task("Test", "C:/test.png", 1.0)
        temp_queue.pop_task()  # Mark as processing
        
        temp_queue.complete_task(task_id)
        
        tasks = temp_queue._load_tasks()
        assert tasks[0]["status"] == "completed"
    
    def test_fail_task(self, temp_queue):
        """Test marking task as failed with error."""
        task_id = temp_queue.add_task("Test", "C:/test.png", 1.0)
        temp_queue.pop_task()  # Mark as processing
        
        temp_queue.fail_task(task_id, "Network timeout")
        
        tasks = temp_queue._load_tasks()
        assert tasks[0]["status"] == "failed"
        assert tasks[0]["error"] == "Network timeout"
    
    def test_concurrent_safety(self, tmp_path):
        """Test that file locking prevents race conditions."""
        queue_file = tmp_path / "tasks.json"
        queue1 = TaskQueue(str(queue_file))
        queue2 = TaskQueue(str(queue_file))
        
        # Both queues add tasks
        id1 = queue1.add_task("Task 1", "C:/1.png", 1.0)
        id2 = queue2.add_task("Task 2", "C:/2.png", 1.0)
        
        # Both tasks should exist
        tasks = queue1._load_tasks()
        assert len(tasks) == 2
        
        # Each queue should pop a different task
        task1 = queue1.pop_task()
        task2 = queue2.pop_task()
        
        assert task1["id"] != task2["id"]
    
    def test_pending_count(self, temp_queue):
        """Test counting pending tasks."""
        assert temp_queue.pending_count() == 0
        
        temp_queue.add_task("Task 1", "C:/1.png", 1.0)
        assert temp_queue.pending_count() == 1
        
        temp_queue.add_task("Task 2", "C:/2.png", 1.0)
        assert temp_queue.pending_count() == 2
        
        temp_queue.pop_task()  # Mark one as processing
        assert temp_queue.pending_count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
