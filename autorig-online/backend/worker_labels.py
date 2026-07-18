"""
Human-readable labels for worker URLs (same port→converter mapping as admin-overlay.js CONVERTER_BY_PORT).
Used in Telegram alerts and anywhere we want F1/F7-style names instead of raw URLs only.
"""
from __future__ import annotations

import html
import re
from typing import Optional
from urllib.parse import urlparse

# Keep in sync with static/js/admin-overlay.js CONVERTER_BY_PORT
CONVERTER_BY_PORT: dict[int, dict[str, str]] = {
    5132: {"short": "F1", "hint": "конвертер F1, порт 5132"},
    5279: {"short": "F2", "hint": "конвертер F2, порт 5279"},
    5131: {"short": "F7", "hint": "конвертер F7, порт 5131"},
    5533: {"short": "F11", "hint": "конвертер F11, порт 5533"},
    5267: {"short": "F13", "hint": "конвертер F13, порт 5267"},
}

CONVERTER_BY_HOSTNAME: dict[str, dict[str, str]] = {
    f"converter-f{number}.freestock.online": {
        "short": f"F{number}",
        "hint": f"converter F{number}, FreeStock HTTPS gateway",
    }
    for number in (1, 2, 7, 11, 13)
}


def extract_hostname_from_worker_url(raw: Optional[str]) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        parsed = urlparse(s if "://" in s else "https://" + s)
    except Exception:
        return None
    return str(parsed.hostname or "").strip().lower() or None


def extract_port_from_worker_url(raw: Optional[str]) -> Optional[int]:
    """Match admin-overlay extractPortFromWorkerApi logic."""
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(r":(\d{2,5})(?:/|$|\?|#)", s)
    if m:
        return int(m.group(1))
    try:
        u = urlparse(s if "://" in s else "http://" + s)
        if u.port:
            return int(u.port)
    except Exception:
        pass
    return None


def worker_label_from_url(worker_url: Optional[str]) -> Optional[tuple[str, str]]:
    """
    Returns (short, hint) e.g. ('F2', 'конвертер F2, порт 5279') or None if unknown port.
    """
    hostname = extract_hostname_from_worker_url(worker_url)
    hostname_row = CONVERTER_BY_HOSTNAME.get(hostname or "")
    if hostname_row:
        return (hostname_row["short"], hostname_row["hint"])

    port = extract_port_from_worker_url(worker_url)
    if port is None:
        return None
    row = CONVERTER_BY_PORT.get(port)
    if not row:
        return None
    return (row["short"], row["hint"])


def format_worker_stalled_telegram_html(worker_url: Optional[str]) -> str:
    """HTML fragment for Telegram (HTML parse mode): label + URL."""
    u = (worker_url or "").strip() or "unknown"
    lab = worker_label_from_url(worker_url)
    if lab:
        short, hint = lab
        return (
            f"🔧 <b>{html.escape(short)}</b> · {html.escape(hint)}\n"
            f"<code>{html.escape(u)}</code>"
        )
    return f"🔧 <code>{html.escape(u)}</code>"
