"""Pure admission rules for retrying failed collection conversion tasks."""

from datetime import datetime, timedelta
from typing import Optional


def collection_error_retry_due(
    *,
    status: str,
    collection_guid: Optional[str],
    restart_count: int,
    updated_at: Optional[datetime],
    now: datetime,
    max_retries: int,
    retry_delay_minutes: int,
) -> bool:
    """Return whether a terminal collection member should re-enter the queue.

    Capacity waits are represented by ``created`` and are never counted here.
    The same task row is retried only after a real terminal ``error`` and only
    while its bounded automatic-retry budget remains.
    """

    if str(status or "").strip().lower() != "error":
        return False
    if not str(collection_guid or "").strip():
        return False
    if int(restart_count or 0) >= max(0, int(max_retries)):
        return False
    if updated_at is None:
        return True
    delay = timedelta(minutes=max(0, int(retry_delay_minutes)))
    return updated_at <= now - delay
