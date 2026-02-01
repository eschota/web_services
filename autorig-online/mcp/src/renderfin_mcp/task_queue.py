"""JSON file-based task queue with file locking."""

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from filelock import FileLock


def is_absolute_path(path: str) -> bool:
    """Check if path is absolute (Windows or Unix).
    
    Args:
        path: Path string to check.
        
    Returns:
        True if path is absolute, False otherwise.
    """
    # Windows: C:/ or C:\
    # Linux/Mac: /
    return bool(re.match(r'^[A-Za-z]:[/\\]|^/', path))


class TaskQueue:
    """JSON file-based task queue with file locking for concurrency safety."""
    
    def __init__(self, queue_file: str = "./vars/tasks.json"):
        """Initialize the task queue.
        
        Args:
            queue_file: Path to the JSON file storing tasks.
        """
        self.queue_file = Path(queue_file)
        self.lock_file = Path(f"{queue_file}.lock")
        self.lock = FileLock(str(self.lock_file), timeout=10)
        
        # Ensure directory exists
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize file if not exists
        if not self.queue_file.exists():
            self._save_tasks([])
    
    def _load_tasks(self) -> list[dict]:
        """Load tasks from JSON file (no locking - internal use)."""
        if not self.queue_file.exists():
            return []
        try:
            with open(self.queue_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("tasks", [])
        except (json.JSONDecodeError, FileNotFoundError):
            return []
    
    def _save_tasks(self, tasks: list[dict]) -> None:
        """Save tasks to JSON file (no locking - internal use)."""
        with open(self.queue_file, "w", encoding="utf-8") as f:
            json.dump({"tasks": tasks}, f, indent=2, ensure_ascii=False)
    
    def add_task(
        self,
        prompt: str,
        output_path: str,
        aspect_ratio: float = 1.0,
    ) -> str:
        """Add a new task to the queue.
        
        Args:
            prompt: Image generation prompt.
            output_path: Absolute path for output file.
            aspect_ratio: Width/height ratio.
            
        Returns:
            Task ID (UUID string).
        """
        task_id = str(uuid.uuid4())[:8]
        task = {
            "id": task_id,
            "prompt": prompt,
            "output_path": output_path,
            "aspect_ratio": aspect_ratio,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "error": None,
        }
        
        with self.lock:
            tasks = self._load_tasks()
            tasks.append(task)
            self._save_tasks(tasks)
        
        return task_id
    
    def pop_task(self) -> Optional[dict]:
        """Get and mark the first pending task as processing.
        
        Returns:
            Task dict if found, None if queue is empty.
        """
        with self.lock:
            tasks = self._load_tasks()
            
            for task in tasks:
                if task["status"] == "pending":
                    task["status"] = "processing"
                    self._save_tasks(tasks)
                    return task
        
        return None
    
    def complete_task(self, task_id: str) -> bool:
        """Mark a task as completed.
        
        Args:
            task_id: ID of the task to complete.
            
        Returns:
            True if task was found and updated, False otherwise.
        """
        with self.lock:
            tasks = self._load_tasks()
            
            for task in tasks:
                if task["id"] == task_id:
                    task["status"] = "completed"
                    self._save_tasks(tasks)
                    return True
        
        return False
    
    def fail_task(self, task_id: str, error: str) -> bool:
        """Mark a task as failed with error message.
        
        Args:
            task_id: ID of the task to mark as failed.
            error: Error message describing the failure.
            
        Returns:
            True if task was found and updated, False otherwise.
        """
        with self.lock:
            tasks = self._load_tasks()
            
            for task in tasks:
                if task["id"] == task_id:
                    task["status"] = "failed"
                    task["error"] = error
                    self._save_tasks(tasks)
                    return True
        
        return False
    
    def pending_count(self) -> int:
        """Get count of pending tasks.
        
        Returns:
            Number of tasks with status 'pending'.
        """
        with self.lock:
            tasks = self._load_tasks()
            return sum(1 for t in tasks if t["status"] == "pending")
    
    def has_pending(self) -> bool:
        """Check if there are pending tasks.
        
        Returns:
            True if there are pending tasks.
        """
        return self.pending_count() > 0


# Default queue instance (relative to workspace)
task_queue = TaskQueue("./vars/tasks.json")
