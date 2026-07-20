"""Pure timestamp rules for converter task hard-timeout checks."""

from datetime import datetime
from typing import Optional


def task_hard_timeout_reference(
    *,
    status: str,
    created_at: Optional[datetime],
    updated_at: Optional[datetime],
    last_progress_at: Optional[datetime],
) -> Optional[datetime]:
    """Return a hard-timeout epoch only after a task has reached a worker.

    ``created`` tasks are waiting in the backend queue.  When every eligible
    converter is busy, applying the worker-processing timeout to that wait
    incorrectly turns healthy queued work into a terminal error before it can
    ever be dispatched.  ``updated_at`` is intentionally ignored because
    polling/admin bookkeeping is not worker progress.
    """
    if str(status or "").strip().lower() != "processing":
        return None
    return last_progress_at or created_at
