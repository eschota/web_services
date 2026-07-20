"""Stable parsing rules for terminal worker progress messages."""

from __future__ import annotations

import re
from typing import Optional


_TERMINAL_LINE_RE = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\s+)?"
    r"(?:FAILURE|FATAL|ERROR)\s*:\s*(.*)$",
    re.IGNORECASE,
)


def terminal_failure_reason_from_line(line: object) -> Optional[str]:
    """Return a reason only when the marker starts a dedicated progress line."""
    text = "" if line is None else str(line)
    match = _TERMINAL_LINE_RE.match(text.lstrip("\ufeff \t"))
    if not match:
        return None
    return match.group(1).strip() or "Unknown failure"


def latest_terminal_failure_reason(progress_text: object) -> Optional[str]:
    """Return the most recent dedicated terminal line from a progress document."""
    text = "" if progress_text is None else str(progress_text)
    for line in reversed(text.replace("\r\n", "\n").replace("\r", "\n").splitlines()):
        reason = terminal_failure_reason_from_line(line)
        if reason is not None:
            return reason[:500]
    return None
