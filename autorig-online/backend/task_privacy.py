"""Private models (owner 2026-10-17, spec docs/my-gallery-privacy-spec-20261017.md).

A task with ``tasks.is_public = 0`` is visible only to its owner (the signed-in user by e-mail, the anonymous
owner by the ``anon_id`` cookie, or an API key of either) and to admins. Everyone else gets 404 on every path
that names the task: the backend routes (this module's ASGI middleware), and the files nginx serves without the
backend (``/u/<upload>/``, ``/pw/<task>.webp``, ``/api/mt/...`` run files), which nginx checks with
``auth_request`` against the small gate service of this same module (``python3 task_privacy.py serve``).

One index serves both: the private tasks (id, owner, upload token from ``input_url``) and the MT runs that belong
to them (``runs/<run>/run.json`` ``request.task_id``). It is read straight from SQLite every few seconds, so the
gate never depends on the backend being up, and a toggle reaches every path within ``PRIVACY_INDEX_TTL_S``.

Requests from our own infrastructure (the VPS itself, the farm sites) are never blocked: the conveyor and the
workers fetch sources and run files through the public host name.
"""
from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple

UUID_RE = re.compile(r"(?<![0-9a-fA-F-])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?![0-9a-fA-F-])")
RUN_RE = re.compile(r"(?<![0-9a-f])[0-9a-f]{20}(?![0-9a-f])")
UPLOAD_RE = re.compile(r"/u/([0-9a-fA-F-]{36})/")

INDEX_TTL_S = float(os.getenv("PRIVACY_INDEX_TTL_S", "3"))
IDENTITY_TTL_S = 30.0
RUNS_DIR = Path(os.getenv("MT_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))
DEFAULT_ADMIN_EMAILS = ("eschota@gmail.com", "vladkcg@gmail.com")
# Our own hosts: the VPS (v4, v6, docker bridges) and the farm sites (farm, owner's site). Extra: env, comma list.
DEFAULT_TRUSTED = ("127.0.0.0/8", "::1/128", "37.187.57.177/32", "2001:41d0:b:5b1::/64", "172.16.0.0/12",
                   "5.129.157.224/32", "37.192.2.126/32")
# Worker / internal API prefixes that authenticate on their own and must never be blocked here.
EXEMPT_PREFIXES = ("/api/internal/", "/api/v3-export/worker", "/api/farmjobs/", "/api/worker", "/api/fleet",
                   "/api/admin/", "/api/my/", "/api/_privacy")


def _db_path() -> str:
    url = os.getenv("DATABASE_URL", "") or "sqlite+aiosqlite:////srv/autorig/data/db/autorig.db"
    if ":///" in url:
        path = url.split(":///", 1)[1]
        return path if path.startswith("/") or os.name == "nt" else os.path.abspath(path)
    return "/srv/autorig/data/db/autorig.db"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True, timeout=2.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def _trusted_networks() -> Tuple[Any, ...]:
    nets = []
    extra = [x.strip() for x in os.getenv("AUTORIG_PRIVACY_TRUSTED_IPS", "").split(",") if x.strip()]
    for item in list(DEFAULT_TRUSTED) + extra:
        try:
            nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            continue
    return tuple(nets)


_TRUSTED = _trusted_networks()


def is_trusted_ip(addr: Optional[str]) -> bool:
    if not addr:
        return False
    try:
        ip = ipaddress.ip_address(addr.strip())
    except ValueError:
        return False
    return any(ip in net for net in _TRUSTED if net.version == ip.version)


def is_admin_email(email: Optional[str]) -> bool:
    if not email:
        return False
    try:
        from config import is_admin_email as _cfg_is_admin  # backend process
        return bool(_cfg_is_admin(email))
    except Exception:
        return email.strip().lower() in DEFAULT_ADMIN_EMAILS


class PrivacyIndex:
    """Private tasks, their upload tokens and their MT runs, refreshed from SQLite every INDEX_TTL_S seconds."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._at = -1.0
        self.private: Dict[str, Tuple[str, str]] = {}      # task id -> (owner_type, owner_id)
        self.uploads: Dict[str, str] = {}                   # upload token -> task id
        self.run_task: Dict[str, str] = {}                  # MT run id -> task id (all runs)
        self._runs_seen: Set[str] = set()
        self._runs_scan_at = -1.0
        self.error: Optional[str] = None

    def invalidate(self) -> None:
        self._at = -1.0

    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and self._at >= 0 and now - self._at < INDEX_TTL_S:
            return
        with self._lock:
            if not force and self._at >= 0 and time.monotonic() - self._at < INDEX_TTL_S:
                return
            private: Dict[str, Tuple[str, str]] = {}
            uploads: Dict[str, str] = {}
            try:
                con = _connect()
                try:
                    for row in con.execute("SELECT id, owner_type, owner_id, input_url, viewer_settings "
                                           "FROM tasks WHERE is_public = 0"):
                        tid = str(row["id"]).lower()
                        private[tid] = (str(row["owner_type"] or ""), str(row["owner_id"] or ""))
                        m = UPLOAD_RE.search(str(row["input_url"] or ""))
                        if m:
                            uploads[m.group(1).lower()] = tid
                        run = _run_of_settings(row["viewer_settings"])
                        if run:
                            self.run_task.setdefault(run, tid)
                finally:
                    con.close()
                self.private, self.uploads, self.error = private, uploads, None
            except Exception as exc:  # keep the last good index
                self.error = f"{type(exc).__name__}: {exc}"
            self._scan_runs()
            self._at = time.monotonic()

    def _scan_runs(self) -> None:
        """New run directories only: read run.json request.task_id once per run."""
        now = time.monotonic()
        if self._runs_scan_at >= 0 and now - self._runs_scan_at < max(INDEX_TTL_S, 5.0):
            return
        self._runs_scan_at = now
        try:
            with os.scandir(RUNS_DIR) as it:
                names = [e.name for e in it if RUN_RE.fullmatch(e.name)]
        except OSError:
            return
        for name in names:
            if name in self._runs_seen:
                continue
            try:
                data = json.loads((RUNS_DIR / name / "run.json").read_text(encoding="utf-8"))
                req = data.get("request") if isinstance(data, dict) else None
                tid = str((req or {}).get("task_id") or "").lower() if isinstance(req, dict) else ""
                if UUID_RE.fullmatch(tid):
                    self.run_task[name] = tid
                self._runs_seen.add(name)
            except FileNotFoundError:
                continue  # a run being created; try again on the next scan
            except Exception:
                self._runs_seen.add(name)

    def private_task_in(self, text: str) -> Optional[str]:
        """The first private task named by a URI: a task id, an upload token or an MT run id."""
        self.refresh()
        if not self.private:
            return None
        for m in UUID_RE.finditer(text or ""):
            key = m.group(0).lower()
            if key in self.private:
                return key
            tid = self.uploads.get(key)
            if tid:
                return tid
        for m in RUN_RE.finditer(text or ""):
            tid = self.run_task.get(m.group(0))
            if tid and tid in self.private:
                return tid
        return None

    def is_private(self, task_id: Optional[str]) -> bool:
        self.refresh()
        return bool(task_id) and str(task_id).lower() in self.private


INDEX = PrivacyIndex()


def _run_of_settings(raw: Any) -> Optional[str]:
    try:
        settings = json.loads(raw or "{}")
        run = str(((settings.get("v3") or {}).get("session") or {}).get("mt_run_id") or "")
    except Exception:
        return None
    return run if RUN_RE.fullmatch(run) else None


# --------------------------------------------------------------------------------------------------- identity
_ident_cache: Dict[str, Tuple[float, Any]] = {}


def _cached(key: str, fn):
    now = time.monotonic()
    hit = _ident_cache.get(key)
    if hit and now - hit[0] < IDENTITY_TTL_S:
        return hit[1]
    value = fn()
    if len(_ident_cache) > 5000:
        _ident_cache.clear()
    _ident_cache[key] = (now, value)
    return value


def _email_for_session(token: str) -> Optional[str]:
    def load():
        con = _connect()
        try:
            row = con.execute("SELECT u.email FROM sessions s JOIN users u ON u.id = s.user_id "
                              "WHERE s.token = ? AND s.expires_at > ?",
                              (token, datetime.utcnow().isoformat(sep=" "))).fetchone()
            return str(row["email"]) if row else None
        finally:
            con.close()
    return _cached("s:" + hashlib.sha256(token.encode()).hexdigest(), load)


def _identity_for_api_key(api_key: str) -> Tuple[Optional[str], Optional[str]]:
    if not (api_key.startswith("ar_") and api_key.count("_") >= 2):
        return None, None
    prefix = api_key.split("_", 2)[1]
    key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()

    def load():
        con = _connect()
        try:
            row = con.execute("SELECT k.key_hash, k.anon_id, u.email FROM api_keys k LEFT JOIN users u ON u.id = k.user_id "
                              "WHERE k.key_prefix = ? AND k.revoked_at IS NULL", (prefix,)).fetchone()
            if not row or not hmac.compare_digest(str(row["key_hash"] or ""), key_hash):
                return (None, None)
            return (row["email"], row["anon_id"])
        finally:
            con.close()
    return _cached("k:" + key_hash, load)


def identity(cookies: Dict[str, str], headers: Dict[str, str]) -> Tuple[Optional[str], Set[str]]:
    """(e-mail of the signed-in user or None, the anon ids this request speaks for)."""
    email: Optional[str] = None
    anons: Set[str] = set()
    token = cookies.get("session")
    if token:
        try:
            email = _email_for_session(token)
        except Exception:
            email = None
    if cookies.get("anon_id"):
        anons.add(cookies["anon_id"])
    api_key = headers.get("x-api-key") or ""
    auth = headers.get("authorization") or ""
    if not api_key and auth.lower().startswith("bearer "):
        api_key = auth.split(" ", 1)[1].strip()
    if api_key:
        try:
            k_email, k_anon = _identity_for_api_key(api_key)
        except Exception:
            k_email, k_anon = None, None
        email = email or k_email
        if k_anon:
            anons.add(k_anon)
    return email, anons


def owner_allows(owner: Tuple[str, str], email: Optional[str], anons: Iterable[str]) -> bool:
    owner_type, owner_id = owner
    if email and is_admin_email(email):
        return True
    if owner_type == "user":
        return bool(email) and email.strip().lower() == owner_id.strip().lower()
    if owner_type == "anon":
        return owner_id in set(anons)
    return False


def parse_cookie_header(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not raw:
        return out
    try:
        jar = SimpleCookie()
        jar.load(raw)
        out = {k: v.value for k, v in jar.items()}
    except Exception:
        for part in raw.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def decide(uri: str, cookie_header: str, headers: Dict[str, str], client_ip: Optional[str]) -> Tuple[bool, Optional[str]]:
    """(allowed, private task id or None) for one request URI."""
    tid = INDEX.private_task_in(uri)
    if not tid:
        return True, None
    if is_trusted_ip(client_ip):
        return True, tid
    email, anons = identity(parse_cookie_header(cookie_header), headers)
    return owner_allows(INDEX.private.get(tid, ("", "")), email, anons), tid


# ------------------------------------------------------------------------------------------- ASGI middleware
_NOT_FOUND_HTML = (b"<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"robots\" content=\"noindex\">"
                   b"<title>404 - AutoRig</title></head><body style=\"background:#0a0a0f;color:#f0f0f5;font:16px system-ui;"
                   b"display:grid;place-items:center;min-height:90vh\"><div>404 &middot; <a style=\"color:#8b8ff8\" "
                   b"href=\"/gallery\">AutoRig gallery</a></div></body></html>")


ON_FIRST_REQUEST = None   # set by my_tasks_routes.install: starts the delete sweeper inside the running loop


class PrivacyMiddleware:
    """404 for every GET/HEAD that names a private task, unless the caller owns it or is an admin."""

    def __init__(self, app) -> None:
        self.app = app
        self._kicked = False

    async def __call__(self, scope, receive, send):
        if not self._kicked and ON_FIRST_REQUEST is not None:
            self._kicked = True
            try:
                ON_FIRST_REQUEST()
            except Exception:
                pass
        if scope.get("type") != "http" or scope.get("method") not in ("GET", "HEAD"):
            return await self.app(scope, receive, send)
        path = scope.get("path") or ""
        if path.startswith(EXEMPT_PREFIXES) or path.startswith("/static/"):
            return await self.app(scope, receive, send)
        query = (scope.get("query_string") or b"").decode("latin-1")
        uri = path + ("?" + query if query else "")
        try:
            tid = INDEX.private_task_in(uri)
        except Exception:
            tid = None
        if not tid:
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
        client = (scope.get("client") or ("", 0))[0]
        if not headers.get("x-real-ip") and not headers.get("x-forwarded-for"):
            ip = client                      # straight to 127.0.0.1:8200: our own services
        else:
            ip = headers.get("x-real-ip") or headers.get("x-forwarded-for", "").split(",")[0].strip()
        allowed, _ = decide(uri, headers.get("cookie", ""), headers, ip)
        if not allowed:
            is_api = path.startswith("/api/")
            body = (json.dumps({"detail": {"error_string": "task_not_found", "message_string": "Not found"}}).encode()
                    if is_api else _NOT_FOUND_HTML)
            await send({"type": "http.response.start", "status": 404, "headers": [
                (b"content-type", b"application/json" if is_api else b"text/html; charset=utf-8"),
                (b"cache-control", b"no-store"), (b"x-robots-tag", b"noindex, nofollow"),
                (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": b"" if scope.get("method") == "HEAD" else body})
            return

        async def send_private(message):
            if message.get("type") == "http.response.start":
                hdrs = [(k, v) for k, v in message.get("headers") or [] if k.lower() not in (b"cache-control",)]
                hdrs += [(b"cache-control", b"private, no-store"), (b"x-robots-tag", b"noindex, nofollow")]
                message = dict(message, headers=hdrs)
            await send(message)
        return await self.app(scope, receive, send_private)


# ------------------------------------------------------------------------------------------------ gate service
def serve(host: str = "127.0.0.1", port: int = 8292) -> None:
    """nginx auth_request target: 204 allowed, 403 denied (nginx answers the client a plain 404, so nothing
    confirms that a hidden id exists)."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            return

        def _answer(self, code: int, body: bytes = b"") -> None:
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/health"):
                INDEX.refresh()
                body = json.dumps({"ok": INDEX.error is None, "private": len(INDEX.private),
                                   "runs": len(INDEX.run_task), "error": INDEX.error}).encode()
                return self._answer(200, body)
            uri = self.headers.get("X-Original-URI") or ""
            hdrs = {k.lower(): v for k, v in self.headers.items()}
            try:
                allowed, _ = decide(uri, self.headers.get("Cookie") or "", hdrs, self.headers.get("X-Real-IP"))
            except Exception:
                allowed = True
            self._answer(204 if allowed else 403)   # nginx turns the 403 into a plain 404

        do_HEAD = do_GET

    INDEX.refresh(force=True)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer((host, port), Handler).serve_forever()


SETTINGS_PATH = Path(os.getenv("AUTORIG_PRIVACY_SETTINGS_PATH", "/srv/autorig/data/var/privacy/user_settings.json"))


def initial_is_public(owner_type: Optional[str], owner_id: Optional[str]) -> bool:
    """is_public of a new task: an active subscriber's models start private unless they switched that off
    (the «Private models» feature of the plan). Anyone else: public. Never raises."""
    try:
        if owner_type != "user" or not owner_id:
            return True
        email = str(owner_id).strip().lower()
        try:
            prefs = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            row = prefs.get(email) if isinstance(prefs, dict) else None
            if isinstance(row, dict) and row.get("private_by_default") is False:
                return True
        except (OSError, ValueError):
            pass
        con = _connect()
        try:
            row = con.execute("SELECT autorig_subscription_status, autorig_subscription_period_end FROM users "
                              "WHERE lower(email) = ?", (email,)).fetchone()
        finally:
            con.close()
        if not row:
            return True
        from subscription_access import user_has_active_subscription

        class _U:
            autorig_subscription_status = row["autorig_subscription_status"]
            autorig_subscription_period_end = _parse_dt(row["autorig_subscription_period_end"])
        return not user_has_active_subscription(_U())
    except Exception:
        return True


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "").replace("T", " ").split("+")[0])
    except ValueError:
        return None


# ---------------------------------------------------------------------------------------- admin-side CLI
def _cli_set(task_id: str, public: bool, by: str) -> None:
    con = sqlite3.connect(_db_path(), timeout=10)
    try:
        cur = con.execute("UPDATE tasks SET is_public = ?, updated_at = ? WHERE id = ?",
                          (1 if public else 0, datetime.utcnow().isoformat(sep=" "), task_id))
        con.commit()
        print(json.dumps({"task_id": task_id, "is_public": public, "updated": cur.rowcount, "by": by}))
    finally:
        con.close()
    audit("cli_set_public" if public else "cli_set_private", task_id, by=by)


AUDIT_PATH = Path(os.getenv("PRIVACY_AUDIT_PATH", "/srv/autorig/data/var/privacy/privacy_audit.jsonl"))


def audit(action: str, task_id: str, *, by: str = "", **extra: Any) -> None:
    """One line per privacy action: who, when, what (never the model's content)."""
    try:
        AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"at": datetime.utcnow().isoformat() + "Z", "action": action, "task_id": task_id,
                           "by": by, **extra}, ensure_ascii=False, default=str)
        with open(AUDIT_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    if args[:1] == ["serve"]:
        serve(port=int(args[1]) if len(args) > 1 else 8292)
    elif args[:1] in (["private"], ["public"]) and len(args) >= 2:
        _cli_set(args[1], args[0] == "public", args[2] if len(args) > 2 else "cli")
    elif args[:1] == ["check"] and len(args) >= 2:
        INDEX.refresh(force=True)
        print(json.dumps({"uri": args[1], "private_task": INDEX.private_task_in(args[1]), "error": INDEX.error}))
    else:
        print("usage: task_privacy.py serve [port] | private|public <task_id> [by] | check <uri>")
