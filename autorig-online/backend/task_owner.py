"""Who submitted which render task, so only they (or an admin) may cancel it.

A pure ASGI middleware watches POST /api/* responses; any JSON answer that
names a `task_id_string` is recorded against the caller's identity (session
cookie, else anon_id cookie, else API key, else client address), hashed.
Stored in SQLite so a restart does not orphan tasks (2026-09-27).
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

DB_PATH = Path("/srv/autorig/data/var/task-owners.sqlite3")
TTL_SECONDS = 7 * 24 * 3600
_MAX_CAPTURE = 256 * 1024
_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _conn.execute("CREATE TABLE IF NOT EXISTS owners (task_id TEXT PRIMARY KEY, owner TEXT, at REAL)")
        _conn.execute("DELETE FROM owners WHERE at < ?", (time.time() - TTL_SECONDS,))
        _conn.commit()
    return _conn


def identity_from(cookies: Dict[str, str], headers: Dict[str, str], client: str) -> str:
    for kind, value in (("s", cookies.get("session")), ("a", cookies.get("anon_id")),
                        ("k", headers.get("authorization") or headers.get("x-api-key")),
                        ("ip", client)):
        if value:
            return kind + ":" + hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:32]
    return ""


def record(task_id: str, owner: str) -> None:
    if not task_id or not owner:
        return
    try:
        with _lock:
            _db().execute("INSERT OR IGNORE INTO owners (task_id, owner, at) VALUES (?, ?, ?)",
                          (task_id, owner, time.time()))
            _db().commit()
    except Exception:
        logger.exception("task owner not recorded")


def owner_of(task_id: str) -> str:
    try:
        with _lock:
            row = _db().execute("SELECT owner FROM owners WHERE task_id = ?", (task_id,)).fetchone()
        return row[0] if row else ""
    except Exception:
        return ""


def recent_for(owner: str, limit: int = 40):
    """Newest task ids this identity submitted."""
    try:
        with _lock:
            rows = _db().execute("SELECT task_id, at FROM owners WHERE owner = ? ORDER BY at DESC LIMIT ?",
                                 (owner, int(limit))).fetchall()
        return [(row[0], float(row[1])) for row in rows]
    except Exception:
        return []


def _cookies(headers: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (headers.get("cookie") or "").split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            out[key.strip()] = value.strip()
    return out


def scope_identity(scope) -> str:
    headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
    client = (scope.get("client") or ("", 0))[0]
    real = headers.get("x-real-ip") or headers.get("x-forwarded-for", "").split(",")[0].strip() or client
    return identity_from(_cookies(headers), headers, real)


class TaskOwnerMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (scope.get("type") != "http" or scope.get("method") != "POST"
                or not str(scope.get("path") or "").startswith("/api/")):
            return await self.app(scope, receive, send)
        owner = scope_identity(scope)
        state = {"json": False, "buf": bytearray(), "status": 0}

        async def wrapped(message):
            if message["type"] == "http.response.start":
                state["status"] = message.get("status", 0)
                for key, value in message.get("headers") or []:
                    if key.lower() == b"content-type" and b"json" in value.lower():
                        state["json"] = True
            elif message["type"] == "http.response.body" and state["json"] and 200 <= state["status"] < 300:
                if len(state["buf"]) < _MAX_CAPTURE:
                    state["buf"].extend(message.get("body", b"")[: _MAX_CAPTURE - len(state["buf"])])
                if not message.get("more_body"):
                    try:
                        data = json.loads(bytes(state["buf"]).decode("utf-8"))
                        task = data.get("task_id_string") if isinstance(data, dict) else None
                        if task:
                            record(str(task), owner)
                    except Exception:
                        pass
            await send(message)

        return await self.app(scope, receive, wrapped)
