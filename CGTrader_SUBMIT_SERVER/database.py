"""
Database module for CGTrader Submit Server.
SQLite-based persistent task queue with checkpoint support.
"""
import sqlite3
import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

from config import DB_PATH

# =============================================================================
# Task Status Constants
# =============================================================================
STATUS_CREATED = "created"
STATUS_DOWNLOADING = "downloading"
STATUS_EXTRACTING = "extracting"
STATUS_PREPARING = "preparing"
STATUS_ANALYZING = "analyzing"
STATUS_UPLOADING = "uploading"
STATUS_FILLING_FORM = "filling_form"
STATUS_PUBLISHING = "publishing"
STATUS_DONE = "done"
STATUS_ERROR = "error"

# Order of steps for recovery
STEP_ORDER = [
    STATUS_CREATED,
    STATUS_DOWNLOADING,
    STATUS_EXTRACTING,
    STATUS_PREPARING,
    STATUS_ANALYZING,
    STATUS_UPLOADING,
    STATUS_FILLING_FORM,
    STATUS_PUBLISHING,
    STATUS_DONE,
]


def init_db():
    """Initialize the database schema."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                input_url TEXT NOT NULL,
                status TEXT DEFAULT 'created',
                step TEXT DEFAULT NULL,
                error_message TEXT,
                
                -- Checkpoint data
                download_path TEXT,
                extract_path TEXT,
                prepared_path TEXT,
                metadata_json TEXT,
                cgtrader_draft_id TEXT,
                cgtrader_product_url TEXT,
                
                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                
                -- Retry tracking
                attempts INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 3,
                last_error TEXT
            )
        """)
        
        # Index for querying pending tasks
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status 
            ON tasks(status)
        """)
        
        conn.commit()


@contextmanager
def get_connection():
    """Get a database connection with context manager."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def create_task(input_url: str) -> str:
    """Create a new task and return its ID."""
    task_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO tasks (id, input_url, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (task_id, input_url, STATUS_CREATED, now, now))
        conn.commit()
    
    return task_id


def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    """Get a task by ID."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        
        if row:
            return dict(row)
        return None


def update_task_status(
    task_id: str,
    status: str,
    step: Optional[str] = None,
    error_message: Optional[str] = None,
    **checkpoint_data
) -> bool:
    """Update task status and checkpoint data."""
    now = datetime.utcnow().isoformat()
    
    updates = ["status = ?", "updated_at = ?"]
    values = [status, now]
    
    if step is not None:
        updates.append("step = ?")
        values.append(step)
    
    if error_message is not None:
        updates.append("error_message = ?")
        updates.append("last_error = ?")
        values.extend([error_message, error_message])
    
    # Handle checkpoint data
    if "download_path" in checkpoint_data:
        updates.append("download_path = ?")
        values.append(checkpoint_data["download_path"])
    
    if "extract_path" in checkpoint_data:
        updates.append("extract_path = ?")
        values.append(checkpoint_data["extract_path"])
    
    if "prepared_path" in checkpoint_data:
        updates.append("prepared_path = ?")
        values.append(checkpoint_data["prepared_path"])
    
    if "metadata_json" in checkpoint_data:
        updates.append("metadata_json = ?")
        # Serialize if dict
        val = checkpoint_data["metadata_json"]
        if isinstance(val, dict):
            val = json.dumps(val, ensure_ascii=False)
        values.append(val)
    
    if "cgtrader_draft_id" in checkpoint_data:
        updates.append("cgtrader_draft_id = ?")
        values.append(checkpoint_data["cgtrader_draft_id"])
    
    if "cgtrader_product_url" in checkpoint_data:
        updates.append("cgtrader_product_url = ?")
        values.append(checkpoint_data["cgtrader_product_url"])
    
    # Track started_at
    if status == STATUS_DOWNLOADING:
        updates.append("started_at = ?")
        values.append(now)
    
    # Track completed_at
    if status in (STATUS_DONE, STATUS_ERROR):
        updates.append("completed_at = ?")
        values.append(now)
    
    values.append(task_id)
    
    with get_connection() as conn:
        conn.execute(
            f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?",
            values
        )
        conn.commit()
    
    return True


def increment_attempts(task_id: str) -> int:
    """Increment the attempt counter and return new value."""
    with get_connection() as conn:
        conn.execute(
            "UPDATE tasks SET attempts = attempts + 1, updated_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), task_id)
        )
        conn.commit()
        
        row = conn.execute(
            "SELECT attempts FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        
        return row["attempts"] if row else 0


def get_next_pending_task() -> Optional[Dict[str, Any]]:
    """Get the next task to process (FIFO order)."""
    with get_connection() as conn:
        row = conn.execute("""
            SELECT * FROM tasks 
            WHERE status NOT IN (?, ?)
            ORDER BY created_at ASC
            LIMIT 1
        """, (STATUS_DONE, STATUS_ERROR)).fetchone()
        
        if row:
            return dict(row)
        return None


def get_interrupted_tasks() -> List[Dict[str, Any]]:
    """Get tasks that were interrupted (not done/error) for recovery."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM tasks 
            WHERE status NOT IN (?, ?)
            ORDER BY created_at ASC
        """, (STATUS_DONE, STATUS_ERROR)).fetchall()
        
        return [dict(row) for row in rows]


def get_queue_status() -> Dict[str, Any]:
    """Get current queue status."""
    with get_connection() as conn:
        # Count by status
        rows = conn.execute("""
            SELECT status, COUNT(*) as count 
            FROM tasks 
            GROUP BY status
        """).fetchall()
        
        counts = {row["status"]: row["count"] for row in rows}
        
        # Get current processing task
        processing = conn.execute("""
            SELECT * FROM tasks 
            WHERE status NOT IN (?, ?, ?)
            ORDER BY created_at ASC
            LIMIT 1
        """, (STATUS_DONE, STATUS_ERROR, STATUS_CREATED)).fetchone()
        
        # Get pending tasks
        pending = conn.execute("""
            SELECT id, input_url, created_at FROM tasks 
            WHERE status = ?
            ORDER BY created_at ASC
            LIMIT 10
        """, (STATUS_CREATED,)).fetchall()
        
        return {
            "counts": counts,
            "total": sum(counts.values()),
            "queue_length": counts.get(STATUS_CREATED, 0),
            "processing": dict(processing) if processing else None,
            "pending": [dict(row) for row in pending],
        }


def retry_task(task_id: str) -> bool:
    """Reset a failed task for retry."""
    task = get_task(task_id)
    if not task:
        return False
    
    if task["status"] != STATUS_ERROR:
        return False
    
    now = datetime.utcnow().isoformat()
    
    with get_connection() as conn:
        conn.execute("""
            UPDATE tasks SET 
                status = ?,
                step = NULL,
                error_message = NULL,
                updated_at = ?,
                started_at = NULL,
                completed_at = NULL
            WHERE id = ?
        """, (STATUS_CREATED, now, task_id))
        conn.commit()
    
    return True


def get_recent_tasks(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent tasks for status display."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM tasks 
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        
        return [dict(row) for row in rows]


def cleanup_old_tasks(days: int = 30) -> int:
    """Delete tasks older than specified days."""
    cutoff = datetime.utcnow().isoformat()[:10]  # Just date part for simplicity
    
    with get_connection() as conn:
        cursor = conn.execute("""
            DELETE FROM tasks 
            WHERE status IN (?, ?)
            AND date(created_at) < date(?, '-' || ? || ' days')
        """, (STATUS_DONE, STATUS_ERROR, cutoff, days))
        conn.commit()
        
        return cursor.rowcount


# Initialize database on module import
init_db()
