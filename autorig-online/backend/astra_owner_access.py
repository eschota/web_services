"""Astra acts as the owner's admin account (owner 2026-10-10: «дай доступ для Астры его тоже, чтобы он мог моим
аккаунтом админским пользоваться»).

Astra never gets the owner's Google login or a browser cookie. Instead the root broker on this host
(`sudo astra-priv site-as-owner METHOD PATH [JSON]`, mt/astra/priv.py, only inside an owner-trusted Astra turn) mints
one short-lived token per call and sends it to 127.0.0.1:8200 in the header

    X-Astra-Owner: v1.<unix ts>.<nonce 32 hex>.<turn 12 hex>.<hmac-sha256 hex>

The HMAC covers the method, the exact path + query, the time, the nonce, the turn id and the user id, and is signed
with /srv/autorig/secrets/astra-admin.key (root:autorig 0640: the backend and the root broker read it; Astra's own
user cannot). `get_current_user` resolves a valid token to user id 2 (the owner's account) only, so `require_admin`
works unchanged. A token is accepted only:

  * from a direct local connection (client 127.0.0.1/::1, no X-Forwarded-For / X-Real-IP / Forwarded header, so
    nothing that came through nginx);
  * within TTL_SECONDS of its time stamp and not before this process started (a restart forgets used nonces);
  * once (the nonce is remembered for the TTL);
  * for the exact method and path + query it was signed for.

A request that carries the header and fails any check gets 401: it never falls back to cookie auth. Every decision is
appended to AUDIT_FILE (method, path, query keys without values, turn, decision); the broker logs the HTTP status.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import pathlib
import re
import threading
import time
from typing import Optional
from urllib.parse import parse_qsl

HEADER = "x-astra-owner"
OWNER_USER_ID = 2
TTL_SECONDS = 120                       # the broker sends at once; the order asked for at most 5 minutes
FUTURE_SKEW = 5
KEY_FILE = pathlib.Path(os.environ.get("ASTRA_ADMIN_KEY_FILE", "/srv/autorig/secrets/astra-admin.key"))
AUDIT_FILE = pathlib.Path(os.environ.get("ASTRA_ADMIN_AUDIT_FILE",
                                         "/srv/autorig/data/var/astra-owner-access/audit.jsonl"))
LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost"}
PROXY_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded", "x-forwarded-host")
TOKEN = re.compile(r"^v1\.([0-9]{9,11})\.([0-9a-f]{32})\.([0-9a-f]{12})\.([0-9a-f]{64})$")
PROCESS_STARTED = time.time()

_nonces: dict[str, float] = {}
_lock = threading.Lock()


class Rejected(Exception):
    """The header was present but is not acceptable."""


def signing_string(method: str, target: str, ts: int, nonce: str, turn: str, user_id: int = OWNER_USER_ID) -> bytes:
    return f"astra-owner/v1\n{method.upper()}\n{target}\n{ts}\n{nonce}\n{turn}\n{user_id}".encode("utf-8")


def sign(key: bytes, method: str, target: str, ts: int, nonce: str, turn: str) -> str:
    """The same function the broker uses (mt/astra/priv.py keeps a stdlib copy)."""
    return hmac.new(key, signing_string(method, target, ts, nonce, turn), hashlib.sha256).hexdigest()


def make_token(key: bytes, method: str, target: str, turn: str, ts: Optional[int] = None,
               nonce: Optional[str] = None) -> str:
    ts = int(time.time()) if ts is None else int(ts)
    nonce = nonce or os.urandom(16).hex()
    return f"v1.{ts}.{nonce}.{turn}.{sign(key, method, target, ts, nonce, turn)}"


def _key() -> bytes:
    try:
        raw = KEY_FILE.read_bytes().strip()
    except OSError:
        return b""
    return raw if len(raw) >= 32 else b""


def request_target(scope: dict) -> str:
    """The path + query exactly as the client sent them (raw_path: no decoding, untouched by path rewrites)."""
    raw = scope.get("raw_path")
    path = raw.decode("latin-1") if isinstance(raw, (bytes, bytearray)) and raw else str(scope.get("path") or "")
    qs = scope.get("query_string") or b""
    qs = qs.decode("latin-1") if isinstance(qs, (bytes, bytearray)) else str(qs)
    return f"{path}?{qs}" if qs else path


def _audit(decision: str, method: str, target: str, turn: str = "", client: str = "") -> None:
    path, _, qs = target.partition("?")
    keys = sorted({k for k, _v in parse_qsl(qs, keep_blank_values=True)})[:20]
    rec = {"at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z", "decision": decision,
           "method": method, "path": path[:300], "query_keys": keys, "turn": turn, "user_id": OWNER_USER_ID,
           "client": client}
    try:
        AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(AUDIT_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _remember(nonce: str, now: float) -> bool:
    with _lock:
        for n in [n for n, exp in _nonces.items() if exp < now]:
            _nonces.pop(n, None)
        if nonce in _nonces or len(_nonces) > 20000:
            return False
        _nonces[nonce] = now + TTL_SECONDS + FUTURE_SKEW + 1
        return True


def verify(scope: dict, headers, client_host: str, now: Optional[float] = None) -> str:
    """-> the turn id of a valid token; raises Rejected otherwise. `headers`: a case-insensitive mapping."""
    now = time.time() if now is None else now
    method = str(scope.get("method") or "GET").upper()
    target = request_target(scope)
    token = str(headers.get(HEADER) or "").strip()
    m = TOKEN.match(token)
    turn = m.group(3) if m else ""
    try:
        if (client_host or "") not in LOCAL_HOSTS:
            raise Rejected("not a local connection")
        if any(headers.get(h) for h in PROXY_HEADERS):
            raise Rejected("came through a proxy")
        if not m:
            raise Rejected("malformed token")
        key = _key()
        if not key:
            raise Rejected("owner access is not configured")
        ts, nonce, sig = int(m.group(1)), m.group(2), m.group(4)
        if not hmac.compare_digest(sign(key, method, target, ts, nonce, turn), sig):
            raise Rejected("bad signature (method/path binding)")
        if ts > now + FUTURE_SKEW or now - ts > TTL_SECONDS or ts < PROCESS_STARTED - 1:
            raise Rejected("expired")
        if not _remember(nonce, now):
            raise Rejected("replayed")
    except Rejected as exc:
        _audit(f"rejected: {exc}", method, target, turn, client_host or "")
        raise
    _audit("accepted", method, target, turn, client_host or "")
    return turn


async def resolve_owner(request, db, user_model, is_admin_email):
    """For get_current_user: None when the header is absent; the owner's User for a valid token; HTTP 401
    otherwise. The result is cached on the request, so a second get_current_user call never spends the nonce twice."""
    if HEADER not in request.headers:
        return None
    cached = getattr(request.state, "_astra_owner", None)
    if cached is not None:
        if isinstance(cached, Exception):
            raise cached
        return cached
    from fastapi import HTTPException

    try:
        turn = verify(request.scope, request.headers, request.client.host if request.client else "")
        user = await db.get(user_model, OWNER_USER_ID)
        if user is None or not is_admin_email(getattr(user, "email", "")):
            _audit("rejected: owner account missing or not admin", request.method, request_target(request.scope),
                   turn)
            raise Rejected("owner account unavailable")
    except Rejected:
        err = HTTPException(status_code=401, detail={"error_string": "astra_owner_token_rejected",
                                                     "message_string": "Owner access token rejected"})
        request.state._astra_owner = err
        raise err
    request.state._astra_owner = user
    request.state.astra_owner_turn = turn
    request.state.auth_via_api_key = False
    request.state.api_key_anon_id = None
    return user
