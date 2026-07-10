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
    """Return the current dispatch/queue epoch without treating polls as progress."""
    if str(status or "").strip().lower() == "processing":
        return last_progress_at or created_at

    # admin_requeue_task_to_created intentionally refreshes updated_at.  Using
    # only the original created_at would make an old requeued task time out
    # immediately before it can be dispatched again.
    candidates = [value for value in (created_at, updated_at) if value is not None]
    return max(candidates) if candidates else None
