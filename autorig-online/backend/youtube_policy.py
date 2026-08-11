"""Pure rolling-window policy shared by YouTube scheduling and tests."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional


def upload_window_cutoff(
    now: Optional[datetime] = None,
    *,
    window_hours: float = 24.0,
) -> datetime:
    return (now or datetime.utcnow()) - timedelta(hours=max(1.0, float(window_hours)))


def task_is_in_upload_window(
    created_at: Optional[datetime],
    now: Optional[datetime] = None,
    *,
    window_hours: float = 24.0,
) -> bool:
    return bool(created_at and created_at >= upload_window_cutoff(now, window_hours=window_hours))


def rolling_budget_available(success_count: int, *, limit: int = 9) -> bool:
    return max(0, int(success_count)) < max(0, int(limit))
