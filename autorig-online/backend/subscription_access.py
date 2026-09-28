"""AutoRig monthly membership entitlement helpers.

The database stores UTC timestamps without timezone information.  Keeping the
date arithmetic here makes webhook handling, task admission and UI responses
use one contract instead of each inventing its own interpretation.
"""
from __future__ import annotations

import calendar
from datetime import datetime, timezone
from typing import Any, Optional


ACTIVE_SUBSCRIPTION_STATES = frozenset({"active", "canceling"})


def utc_naive(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def parse_gumroad_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        return utc_naive(datetime.fromisoformat(normalized))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def add_calendar_month(value: datetime) -> datetime:
    """Return the same UTC wall time in the next month, clamping month-end."""
    source = utc_naive(value) or datetime.utcnow()
    year = source.year + (1 if source.month == 12 else 0)
    month = 1 if source.month == 12 else source.month + 1
    day = min(source.day, calendar.monthrange(year, month)[1])
    return source.replace(year=year, month=month, day=day)


def user_has_active_subscription(user: Any, *, now: Optional[datetime] = None) -> bool:
    if user is None:
        return False
    state = str(getattr(user, "autorig_subscription_status", "") or "").strip().lower()
    period_end = utc_naive(getattr(user, "autorig_subscription_period_end", None))
    current = utc_naive(now) or datetime.utcnow()
    return bool(state in ACTIVE_SUBSCRIPTION_STATES and period_end and period_end > current)


def subscription_summary(user: Any, *, now: Optional[datetime] = None) -> dict:
    active = user_has_active_subscription(user, now=now)
    state = str(getattr(user, "autorig_subscription_status", "") or "none").strip().lower()
    if state in ACTIVE_SUBSCRIPTION_STATES and not active:
        state = "expired"
    return {
        "active": active,
        "status": state or "none",
        "current_period_end": getattr(user, "autorig_subscription_period_end", None),
        "cancel_at_period_end": state == "canceling",
        "plan": "unlimited_monthly" if active or state not in {"", "none"} else None,
    }


def apply_subscription_event(
    user: Any,
    *,
    subscription_id: Optional[str],
    sale_at: Optional[datetime],
    cancelled_at: Optional[datetime] = None,
    failed_at: Optional[datetime] = None,
    ended_at: Optional[datetime] = None,
    refunded: bool = False,
    now: Optional[datetime] = None,
) -> str:
    """Apply one verified Gumroad membership event and return the new state."""
    current = utc_naive(now) or datetime.utcnow()
    sale_time = utc_naive(sale_at) or current
    explicit_end = next(
        (
            utc_naive(value)
            for value in (cancelled_at, failed_at, ended_at)
            if utc_naive(value) is not None
        ),
        None,
    )

    if subscription_id:
        user.autorig_subscription_id = str(subscription_id)[:255]
    if getattr(user, "autorig_subscription_started_at", None) is None:
        user.autorig_subscription_started_at = sale_time

    if refunded:
        user.autorig_subscription_status = "refunded"
        user.autorig_subscription_period_end = current
    elif explicit_end is not None:
        user.autorig_subscription_period_end = explicit_end
        user.autorig_subscription_status = "canceling" if explicit_end > current else "expired"
    else:
        charged_period_end = add_calendar_month(sale_time)
        existing_end = utc_naive(getattr(user, "autorig_subscription_period_end", None))
        # Duplicate or late webhook delivery must never shorten already-paid access.
        user.autorig_subscription_period_end = max(
            value for value in (charged_period_end, existing_end) if value is not None
        )
        user.autorig_subscription_status = "active"

    user.autorig_subscription_updated_at = current
    return str(user.autorig_subscription_status)
