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
        # Idempotent submits (2026-09-27): a retried POST with the same
        # X-Client-Request-Id gets the answer the first one got.
        _conn.execute("CREATE TABLE IF NOT EXISTS idem (key TEXT PRIMARY KEY, owner TEXT, path TEXT, body BLOB, at REAL)")
        _conn.execute("DELETE FROM idem WHERE at < ?", (time.time() - 24 * 3600,))
        _conn.execute("DELETE FROM owners WHERE at < ?", (time.time() - TTL_SECONDS,))
        # Which graph node asked for a task (2026-09-28): the queue can then
        # be told what a graph no longer needs, whichever tab submitted it.
        _conn.execute("CREATE TABLE IF NOT EXISTS task_graph (task_id TEXT PRIMARY KEY, graph_id TEXT, "
                      "node_id TEXT, session TEXT, at REAL)")
        _conn.execute("CREATE INDEX IF NOT EXISTS task_graph_graph ON task_graph(graph_id)")
        _conn.execute("DELETE FROM task_graph WHERE at < ?", (time.time() - TTL_SECONDS,))
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


def record_graph(task_id: str, graph_id: str, node_id: str, session: str) -> None:
    if not task_id or not graph_id:
        return
    try:
        with _lock:
            _db().execute("INSERT OR REPLACE INTO task_graph (task_id, graph_id, node_id, session, at) "
                          "VALUES (?, ?, ?, ?, ?)", (task_id, graph_id, node_id, session, time.time()))
            _db().commit()
    except Exception:
        logger.exception("task graph link not recorded")


def graph_of(task_id: str) -> Dict[str, str]:
    try:
        with _lock:
            row = _db().execute("SELECT graph_id, node_id, session FROM task_graph WHERE task_id = ?",
                                (task_id,)).fetchone()
        return {"graph_id": row[0], "node_id": row[1], "session": row[2]} if row else {}
    except Exception:
        return {}


def tasks_of_graph(graph_id: str, limit: int = 400):
    """(task_id, node_id, session, at) of every task this graph asked for, newest first."""
    try:
        with _lock:
            rows = _db().execute("SELECT task_id, node_id, session, at FROM task_graph WHERE graph_id = ? "
                                 "ORDER BY at DESC LIMIT ?", (graph_id, int(limit))).fetchall()
        return [(str(r[0]), str(r[1] or ""), str(r[2] or ""), float(r[3])) for r in rows]
    except Exception:
        return []


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


def idem_get(key: str, owner: str, path: str):
    try:
        with _lock:
            row = _db().execute("SELECT body FROM idem WHERE key = ? AND owner = ? AND path = ?",
                                (key, owner, path)).fetchone()
        return bytes(row[0]) if row else None
    except Exception:
        return None


def _stored_before_start(key: str) -> bool:
    """A request id answered before this process started: its task was wiped."""
    try:
        with _lock:
            row = _db().execute("SELECT at FROM idem WHERE key = ?", (key,)).fetchone()
        return bool(row) and float(row[0]) < PROCESS_START
    except Exception:
        return False


def idem_put(key: str, owner: str, path: str, body: bytes) -> None:
    try:
        with _lock:
            _db().execute("INSERT OR REPLACE INTO idem (key, owner, path, body, at) VALUES (?, ?, ?, ?, ?)",
                          (key, owner, path, body, time.time()))
            _db().commit()
    except Exception:
        logger.exception("idempotent answer not stored")


# The editor build this release serves: the same content hash the page puts in
# ai-nodes.js?v= (tools/version_assets.py). A render submitted by an editor
# (it sends X-Client-Request-Id) from another build is refused, so a tab opened
# before a deploy cannot put back jobs the restart wiped (owner, 2026-09-28).
_build_cache = {"key": None, "value": ""}


def current_build() -> str:
    """The ?v= the LIVE /nodes page gives ai-nodes.js (what a fresh tab sends).

    Read through /srv/autorig/current, not this module's own release folder:
    a static-only deploy switches the page without restarting this process.
    Re-read whenever the page file changes.
    """
    import os
    import pathlib
    import re
    live = pathlib.Path("/srv/autorig/current/autorig-online/static/nodes.html")
    page = live if live.exists() else pathlib.Path(__file__).resolve().parent.parent / "static" / "nodes.html"
    try:
        real = os.path.realpath(page)
        key = (real, os.stat(real).st_mtime)
        if _build_cache["key"] != key:
            found = re.search(r'/static/js/ai-nodes\.js\?v=([^"&]+)"', pathlib.Path(real).read_text(encoding="utf-8"))
            _build_cache.update(key=key, value=found.group(1) if found else "")
        return _build_cache["value"]
    except Exception:
        return ""


PROCESS_START = time.time()
_file_cache = {"key": None, "value": ""}


def _live_file_hash() -> str:
    """sha1[:10] of the live ai-nodes.js: a page stamped by version_assets.py sends it."""
    import hashlib
    import os
    path = "/srv/autorig/current/autorig-online/static/js/ai-nodes.js"
    try:
        real = os.path.realpath(path)
        key = (real, os.stat(real).st_mtime)
        if _file_cache["key"] != key:
            with open(real, "rb") as handle:
                _file_cache.update(key=key, value=hashlib.sha1(handle.read()).hexdigest()[:10])
        return _file_cache["value"]
    except Exception:
        return ""


def _refuse(code: str, message: str) -> bytes:
    return json.dumps({"detail": {"error_string": code, "message_string": message,
                                  "current_build_string": current_build()}}).encode("utf-8")


class TaskOwnerMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (scope.get("type") != "http" or scope.get("method") != "POST"
                or not str(scope.get("path") or "").startswith("/api/")):
            return await self.app(scope, receive, send)
        owner = scope_identity(scope)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
        # The route (and every renderfin submit site under it) reads which
        # graph node this request renders for, so the farm task carries it.
        import ai_graph_context
        ai_graph_context.set_from_headers(headers)
        graph_link = ai_graph_context.current()
        key = (headers.get("x-client-request-id") or "").strip()[:80]
        path = str(scope.get("path") or "")
        live_build = current_build() if key else ""
        if key and live_build:
            build = (headers.get("x-editor-build") or "").strip()
            refusal = None
            if build != live_build and build != _live_file_hash():
                refusal = _refuse("editor_outdated", "This editor is older than the site — reload the page")
            elif _stored_before_start(key):
                refusal = _refuse("cancelled_by_restart",
                                  "This render was cancelled by a server restart — press Render again")
            if refusal is not None:
                await send({"type": "http.response.start", "status": 409,
                            "headers": [(b"content-type", b"application/json"),
                                        (b"content-length", str(len(refusal)).encode("ascii"))]})
                await send({"type": "http.response.body", "body": refusal})
                return
        if key:
            stored = idem_get(key, owner, path)
            if stored is not None:
                await send({"type": "http.response.start", "status": 200,
                            "headers": [(b"content-type", b"application/json"),
                                        (b"content-length", str(len(stored)).encode("ascii")),
                                        (b"x-idempotent-replay", b"1")]})
                await send({"type": "http.response.body", "body": stored})
                return
        state = {"json": False, "buf": bytearray(), "status": 0}

        async def wrapped(message):
            if message["type"] == "http.response.start":
                state["status"] = message.get("status", 0)
                for name, value in message.get("headers") or []:
                    if name.lower() == b"content-type" and b"json" in value.lower():
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
                            if graph_link.get("graph_id"):
                                record_graph(str(task), graph_link.get("graph_id", ""),
                                             graph_link.get("node_id", ""), graph_link.get("submit_session", ""))
                            if key:
                                idem_put(key, owner, path, bytes(state["buf"]))
                    except Exception:
                        pass
            await send(message)

        return await self.app(scope, receive, wrapped)
