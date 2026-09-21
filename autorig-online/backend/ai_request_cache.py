"""Persistent request deduplication for AI services.

Only request hashes and task/result metadata are persisted. Request bodies,
uploaded media, source assets and graph files are never stored or deleted here.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping


DEFAULT_DB_PATH = Path(
    os.getenv("AUTORIG_AI_REQUEST_CACHE_DB", "/srv/autorig/data/var/ai-request-cache.sqlite3")
)
DEFAULT_TTL_SECONDS = int(os.getenv("AUTORIG_AI_REQUEST_CACHE_TTL_SECONDS", "86400"))
logger = logging.getLogger(__name__)

_EPHEMERAL_KEYS = {
    "wait_seconds",
    "poll_interval",
    "poll_interval_seconds",
    "client_id",
    "request_id",
    "ui_state",
    "ui_metadata",
}
_SECRET_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "token",
    "password",
    "secret",
}
_MEDIA_BASE64_KEYS = {
    "image_base64",
    "video_base64",
    "audio_base64",
    "mask_base64",
    "control_base64",
    "data_url",
}
_SUCCESS_STATES = {"complete", "completed", "done", "finished", "ready", "success", "succeeded"}
_FAILURE_STATES = {"error", "failed", "failure", "cancelled", "canceled", "rejected"}


def _media_digest(value: str) -> str:
    encoded = value.split(",", 1)[1] if value.startswith("data:") and "," in value else value
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, base64.binascii.Error):
        raw = value.encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _canonical_value(value: Any, key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for raw_key, item in value.items():
            name = str(raw_key)
            lowered = name.lower()
            if lowered in _EPHEMERAL_KEYS or lowered in _SECRET_KEYS:
                continue
            normalized[name] = _canonical_value(item, lowered)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, bytes):
        return "sha256:" + hashlib.sha256(value).hexdigest()
    if isinstance(value, str) and (key in _MEDIA_BASE64_KEYS or value.startswith("data:")):
        return _media_digest(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def canonical_request_hash(
    service: str,
    payload: Mapping[str, Any],
    *,
    namespace: str = "v1",
) -> str:
    profile_hash = payload.get("profile_hash", payload.get("profileHash"))
    material = {
        "namespace": namespace,
        "service": service,
        "profile_hash": profile_hash,
        "payload": _canonical_value(payload),
    }
    packed = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


async def _fingerprint_owned_media(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Own immutable upload URLs may differ while their file bytes are identical.

    Do not fetch arbitrary caller URLs here: this would add a new SSRF path.
    External sources remain exact-string keys.
    """
    result = dict(payload)
    owned = ("https://autorig.online/dev/api/scratch/", "https://autorig.online/renderfin/render/")
    keys = ("image_url", "image_url_end", "control_pose", "control_depth", "control_canny")
    candidates = [(key, result.get(key)) for key in keys
                  if isinstance(result.get(key), str) and result[key].startswith(owned)]
    if not candidates:
        return result
    import httpx
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        for key, url in candidates:
            try:
                digest = hashlib.sha256()
                size = 0
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > 12 * 1024 * 1024:
                            raise ValueError("Media fingerprint limit")
                        digest.update(chunk)
                result[key] = "sha256:" + digest.hexdigest()
            except (httpx.HTTPError, ValueError):
                pass
    return result


def _task_id(response: Mapping[str, Any]) -> str | None:
    for key in ("task_id", "task_id_string", "id"):
        value = response.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _status(response: Mapping[str, Any]) -> str:
    for key in ("status", "status_string", "state"):
        value = response.get(key)
        if value is not None:
            return str(value).strip().lower()
    return "accepted" if _task_id(response) else "completed"


def _redact_response(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _redact_response(item)
            for key, item in value.items()
            if str(key).lower() not in _SECRET_KEYS
        }
    if isinstance(value, list):
        return [_redact_response(item) for item in value]
    return value


def _requires_explicit_seed(service: str) -> bool:
    name = service.lower()
    if "vision" in name or "control" in name or "text" in name:
        return False
    return any(part in name for part in ("image", "video", "animation", "render"))


def _seed_is_cacheable(service: str, payload: Mapping[str, Any]) -> bool:
    if not _requires_explicit_seed(service):
        return True
    seed = payload.get("seed")
    return seed not in (None, "", 0, "0")


class AIRequestCache:
    def __init__(
        self,
        db_path: str | os.PathLike[str] = DEFAULT_DB_PATH,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.db_path = Path(db_path)
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._clock = clock
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()
        self._generation = 0
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_request_cache (
                    cache_key TEXT PRIMARY KEY,
                    service TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    task_id TEXT,
                    state TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_request_cache_task_id "
                "ON ai_request_cache(task_id)"
            )
            db.execute("CREATE INDEX IF NOT EXISTS idx_ai_request_cache_expiry ON ai_request_cache(expires_at)")

    async def _lock_for(self, cache_key: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._locks.setdefault(cache_key, asyncio.Lock())

    async def _release_lock(self, cache_key: str, lock: asyncio.Lock) -> None:
        async with self._locks_guard:
            if not lock.locked() and not getattr(lock, "_waiters", None):
                self._locks.pop(cache_key, None)

    def _lookup(self, cache_key: str) -> dict[str, Any] | None:
        now = self._clock()
        with self._connect() as db:
            row = db.execute(
                "SELECT response_json, state, expires_at FROM ai_request_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if row is None:
                return None
            if float(row["expires_at"]) <= now or str(row["state"]).lower() in _FAILURE_STATES:
                db.execute("DELETE FROM ai_request_cache WHERE cache_key = ?", (cache_key,))
                return None
        response = json.loads(str(row["response_json"]))
        response["cache_hit_bool"] = True
        return response

    def _store(
        self,
        cache_key: str,
        service: str,
        namespace: str,
        response: Mapping[str, Any],
        generation: int | None = None,
    ) -> None:
        now = self._clock()
        state = _status(response)
        if state in _FAILURE_STATES:
            return
        safe_response = _redact_response(dict(response))
        safe_response.pop("cache_hit_bool", None)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if generation is not None and generation != self._generation:
                return
            db.execute("DELETE FROM ai_request_cache WHERE expires_at <= ?", (now,))
            db.execute(
                """
                INSERT INTO ai_request_cache (
                    cache_key, service, namespace, task_id, state, response_json,
                    created_at, updated_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    task_id = excluded.task_id,
                    state = excluded.state,
                    response_json = excluded.response_json,
                    updated_at = excluded.updated_at,
                    expires_at = excluded.expires_at
                """,
                (
                    cache_key,
                    service,
                    namespace,
                    _task_id(response),
                    state,
                    json.dumps(safe_response, ensure_ascii=False, separators=(",", ":")),
                    now,
                    now,
                    now + self.ttl_seconds,
                ),
            )

    async def run_cached(
        self,
        service: str,
        payload: Mapping[str, Any],
        submit_async_callback: Callable[[], Awaitable[Mapping[str, Any]] | Mapping[str, Any]],
        *,
        namespace: str = "v1",
    ) -> dict[str, Any]:
        if not _seed_is_cacheable(service, payload):
            result = submit_async_callback()
            response = await result if inspect.isawaitable(result) else result
            return {**dict(response), "cache_hit_bool": False}

        fingerprinted = await _fingerprint_owned_media(payload)
        cache_key = canonical_request_hash(service, fingerprinted, namespace=namespace)
        try:
            cached = await asyncio.to_thread(self._lookup, cache_key)
        except sqlite3.Error:
            cached = None
        if cached is not None:
            return cached

        lock = await self._lock_for(cache_key)
        try:
            async with lock:
                try:
                    cached = await asyncio.to_thread(self._lookup, cache_key)
                except sqlite3.Error:
                    cached = None
                if cached is not None:
                    return cached
                generation = self._generation
                result = submit_async_callback()
                response = await result if inspect.isawaitable(result) else result
                response_dict = dict(response)
                try:
                    await asyncio.to_thread(self._store, cache_key, service, namespace, response_dict, generation)
                except sqlite3.Error:
                    logger.exception("Could not store request-cache metadata; task was already submitted")
                return {**response_dict, "cache_hit_bool": False}
        finally:
            await self._release_lock(cache_key, lock)

    def note_result(
        self,
        task_id: str,
        status: str,
        response: Mapping[str, Any] | None = None,
    ) -> bool:
        normalized = status.strip().lower()
        with self._connect() as db:
            if normalized in _FAILURE_STATES:
                cursor = db.execute("DELETE FROM ai_request_cache WHERE task_id = ?", (str(task_id),))
                return cursor.rowcount > 0
            if normalized not in _SUCCESS_STATES:
                return False
            row = db.execute(
                "SELECT cache_key, response_json FROM ai_request_cache WHERE task_id = ?",
                (str(task_id),),
            ).fetchone()
            if row is None:
                return False
            current = json.loads(str(row["response_json"]))
            if response:
                current.update(_redact_response(dict(response)))
            current.pop("cache_hit_bool", None)
            now = self._clock()
            db.execute(
                """
                UPDATE ai_request_cache
                SET state = 'completed', response_json = ?, updated_at = ?, expires_at = ?
                WHERE cache_key = ?
                """,
                (
                    json.dumps(current, ensure_ascii=False, separators=(",", ":")),
                    now,
                    now + self.ttl_seconds,
                    str(row["cache_key"]),
                ),
            )
            return True

    async def anote_result(
        self,
        task_id: str,
        status: str,
        response: Mapping[str, Any] | None = None,
    ) -> bool:
        return await asyncio.to_thread(self.note_result, task_id, status, response)

    def clear(self) -> int:
        """Clear cache metadata only; external graphs and assets are untouched."""
        with self._connect() as db:
            self._generation += 1
            cursor = db.execute("DELETE FROM ai_request_cache")
            return cursor.rowcount


_default_cache: AIRequestCache | None = None


def _get_default_cache() -> AIRequestCache:
    global _default_cache
    if _default_cache is None:
        _default_cache = AIRequestCache()
    return _default_cache


async def run_cached(
    service: str,
    payload: Mapping[str, Any],
    submit_async_callback: Callable[[], Awaitable[Mapping[str, Any]] | Mapping[str, Any]],
    *,
    namespace: str = "v1",
) -> dict[str, Any]:
    try:
        cache = _get_default_cache()
    except (OSError, sqlite3.Error):
        logger.exception("Request cache unavailable; using the uncached service")
        result = submit_async_callback()
        response = await result if inspect.isawaitable(result) else result
        return {**dict(response), "cache_hit_bool": False}
    return await cache.run_cached(
        service, payload, submit_async_callback, namespace=namespace
    )


def note_result(
    task_id: str,
    status: str,
    response: Mapping[str, Any] | None = None,
) -> bool:
    return _get_default_cache().note_result(task_id, status, response)


async def anote_result(
    task_id: str,
    status: str,
    response: Mapping[str, Any] | None = None,
) -> bool:
    try:
        return await _get_default_cache().anote_result(task_id, status, response)
    except (OSError, sqlite3.Error):
        logger.exception("Could not update request-cache metadata")
        return False


def clear() -> int:
    return _get_default_cache().clear()
