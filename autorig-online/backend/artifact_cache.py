"""Durable, verified artifact caching for completed AutoRig tasks.

The worker may hold the only copy of a result.  This module copies that result
to the web host with resumable HTTP ranges, records a checksum manifest, and
keeps the queue in the same SQLite database as the task.  It deliberately has
no FastAPI dependency so its path and retention contracts are unit-testable.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import struct
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Awaitable, Callable, Iterable, Optional, Sequence
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import httpx
from sqlalchemy import or_, select

from config import (
    ARTIFACT_CACHE_CONCURRENCY,
    ARTIFACT_CACHE_FULL_HOURS,
    ARTIFACT_CACHE_RESERVE_GB,
    ARTIFACT_CACHE_ROOT,
    ARTIFACT_CACHE_SOFT_CAP_GB,
)
from database import ArtifactCacheJob, AsyncSessionLocal, Task


RANGE_CHUNK_BYTES = 8 * 1024 * 1024
MAX_REDIRECTS = 5
RETRY_SECONDS = (30, 120, 600, 1800)
MANIFEST_VERSION = 1
PAUSE_MARKER = ".pause-new-tasks"
_TASK_ID_RE = re.compile(r"^[0-9a-fA-F-]{16,64}$")
_WORKER_RE = re.compile(r"(?:^|[.-])(?:converter-)?(f\d+)(?:[.-]|$)", re.IGNORECASE)
_claim_lock = asyncio.Lock()
_worker_locks: dict[str, asyncio.Lock] = {}


class ArtifactCacheReserveError(RuntimeError):
    """The next write would consume the filesystem reserve."""


@dataclass(frozen=True)
class ArtifactSource:
    url: str
    relative_path: str
    role: str
    assigned_worker: str = ""
    expected_size: Optional[int] = None
    required: bool = True
    long_lived: Optional[bool] = None


def utcnow() -> datetime:
    return datetime.utcnow()


def _aware_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _task_dir(root: Path, task_id: str) -> Path:
    if not _TASK_ID_RE.fullmatch(str(task_id or "")):
        raise ValueError("invalid task id")
    path = (root / task_id).resolve()
    if root.resolve() not in path.parents:
        raise ValueError("task path escaped cache root")
    return path


def safe_relative_path(value: str) -> str:
    raw = unquote(str(value or "")).replace("\\", "/").strip()
    if not raw or "\x00" in raw or re.match(r"^[a-zA-Z]:", raw):
        raise ValueError("invalid artifact path")
    candidate = PurePosixPath(raw)
    if candidate.is_absolute() or any(part in ("", ".", "..") for part in candidate.parts):
        raise ValueError("artifact path traversal rejected")
    return candidate.as_posix()


def _safe_destination(root: Path, task_id: str, relative_path: str) -> Path:
    task_dir = _task_dir(root, task_id)
    relative = safe_relative_path(relative_path)
    destination = (task_dir / Path(*PurePosixPath(relative).parts)).resolve()
    if task_dir not in destination.parents:
        raise ValueError("artifact destination escaped task directory")
    return destination


def worker_identity(value: str) -> str:
    parsed = urlsplit(str(value or ""))
    host = (parsed.hostname or str(value or "")).lower()
    match = _WORKER_RE.search(host)
    return match.group(1).lower() if match else ""


def validate_source_url(url: str, assigned_worker: str = "") -> str:
    parsed = urlsplit(str(url or "").strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("artifact URL must be http(s)")
    if parsed.username or parsed.password:
        raise ValueError("artifact URL credentials are forbidden")
    if parsed.fragment:
        parsed = parsed._replace(fragment="")

    assigned = urlsplit(str(assigned_worker or ""))
    assigned_host = (assigned.hostname or "").lower()
    source_host = (parsed.hostname or "").lower()
    assigned_identity = worker_identity(assigned_worker)
    source_identity = worker_identity(url)
    if assigned_worker:
        same_host = bool(assigned_host and assigned_host == source_host)
        allowed_aliases = (
            {
                f"{assigned_identity}.freestock.online",
                f"converter-{assigned_identity}.freestock.online",
            }
            if assigned_identity
            else set()
        )
        same_worker = bool(
            assigned_identity
            and assigned_identity == source_identity
            and assigned_host in allowed_aliases
            and source_host in allowed_aliases
        )
        if not same_host and not same_worker:
            raise ValueError(
                f"artifact host {source_host!r} is outside assigned worker {assigned_host!r}"
            )
    return urlunsplit(parsed)


def _is_long_lived(source: ArtifactSource) -> bool:
    if source.long_lived is not None:
        return bool(source.long_lived)
    name = source.relative_path.lower()
    role = source.role.lower()
    if role in {"full_bundle", "primary_glb", "primary_fbx", "viewer_glb", "poster"}:
        return True
    if name.endswith((".zip", ".glb", ".fbx")):
        return True
    return "poster" in role or "poster" in name


def _manifest_path(root: Path, task_id: str) -> Path:
    return _task_dir(root, task_id) / "manifest.json"


def read_manifest(root: Path, task_id: str) -> dict[str, Any]:
    path = _manifest_path(root, task_id)
    if not path.is_file():
        return {
            "version": MANIFEST_VERSION,
            "task_id": task_id,
            "files": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"version": MANIFEST_VERSION, "task_id": task_id, "files": []}
    if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
        return {"version": MANIFEST_VERSION, "task_id": task_id, "files": []}
    return payload


def write_manifest(root: Path, task_id: str, manifest: dict[str, Any]) -> None:
    path = _manifest_path(root, task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["version"] = MANIFEST_VERSION
    manifest["task_id"] = task_id
    manifest["updated_at"] = _iso_now()
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def manifest_stats(manifest: dict[str, Any]) -> tuple[int, int]:
    files = [item for item in manifest.get("files", []) if isinstance(item, dict)]
    return len(files), sum(max(0, int(item.get("size") or 0)) for item in files)


def _normalized_url(value: str) -> str:
    try:
        parsed = urlsplit(str(value or ""))
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, ""))
    except ValueError:
        return str(value or "")


def lookup_cached_artifact(
    task_id: str,
    *,
    source_url: Optional[str] = None,
    role: Optional[str] = None,
    basename: Optional[str] = None,
    root: Optional[Path] = None,
) -> Optional[dict[str, Any]]:
    cache_root = Path(root or ARTIFACT_CACHE_ROOT)
    try:
        manifest = read_manifest(cache_root, task_id)
    except ValueError:
        # Existing viewer helpers are also used with synthetic identifiers in
        # health checks/tests. An invalid cache key is a miss, never a reason
        # to break the worker fallback path.
        return None
    source_key = _normalized_url(source_url or "")
    basename_key = str(basename or "").lower()
    for item in manifest.get("files", []):
        if not isinstance(item, dict):
            continue
        relative = item.get("relative_path")
        try:
            path = _safe_destination(cache_root, task_id, str(relative or ""))
        except ValueError:
            continue
        if not path.is_file() or path.stat().st_size != int(item.get("size") or -1):
            continue
        if source_key and _normalized_url(str(item.get("source_url") or "")) != source_key:
            continue
        if role and str(item.get("role") or "") != role:
            continue
        if basename_key and path.name.lower() != basename_key:
            continue
        result = dict(item)
        result["path"] = path
        result["internal_uri"] = "/_autorig_artifacts/" + "/".join(
            quote(part, safe="") for part in (task_id, *PurePosixPath(str(relative)).parts)
        )
        return result
    return None


async def _safe_stream_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    assigned_worker: str,
    headers: Optional[dict[str, str]] = None,
) -> httpx.Response:
    current = validate_source_url(url, assigned_worker)
    for _ in range(MAX_REDIRECTS + 1):
        request = client.build_request(method, current, headers=headers)
        response = await client.send(request, stream=True)
        if response.status_code not in (301, 302, 303, 307, 308):
            return response
        location = response.headers.get("location")
        await response.aclose()
        if not location:
            raise RuntimeError("redirect without Location")
        current = str(httpx.URL(current).join(location))
        validate_source_url(current, assigned_worker)
    raise RuntimeError("too many artifact redirects")


def _parse_content_range(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", str(value or ""), re.IGNORECASE)
    if not match:
        raise RuntimeError("missing or invalid Content-Range")
    start, end, total = (int(part) for part in match.groups())
    if start < 0 or end < start or total <= end:
        raise RuntimeError("invalid Content-Range bounds")
    return start, end, total


def _ensure_cache_write_capacity(path: Path, incoming_bytes: int) -> None:
    """Fail before a cache write would cross the production disk reserve."""
    required = max(0, int(incoming_bytes or 0))
    if required <= 0:
        return
    probe_path = path.parent
    while not probe_path.exists() and probe_path != probe_path.parent:
        probe_path = probe_path.parent
    free = int(shutil.disk_usage(probe_path).free)
    reserve = int(ARTIFACT_CACHE_RESERVE_GB * 1024**3)
    if free - required < reserve:
        raise ArtifactCacheReserveError(
            f"artifact cache write paused: free={free / 1024**3:.2f}GB "
            f"reserve={ARTIFACT_CACHE_RESERVE_GB:.0f}GB "
            f"next_write={required / 1024**2:.2f}MiB"
        )


async def _probe_source(
    client: httpx.AsyncClient,
    source: ArtifactSource,
) -> dict[str, Any]:
    response = await _safe_stream_request(
        client,
        "GET",
        source.url,
        assigned_worker=source.assigned_worker,
        headers={"Range": "bytes=0-0", "Accept-Encoding": "identity"},
    )
    try:
        content_type = str(response.headers.get("content-type") or "").lower()
        if response.status_code == 206:
            start, end, total = _parse_content_range(response.headers.get("content-range") or "")
            if (start, end) != (0, 0):
                raise RuntimeError("range probe did not return byte zero")
            await response.aread()
            return {
                "size": total,
                "ranges": True,
                "etag": response.headers.get("etag"),
                "last_modified": response.headers.get("last-modified"),
                "content_type": content_type,
            }
        if response.status_code != 200:
            raise RuntimeError(f"source returned HTTP {response.status_code}")
        length = response.headers.get("content-length")
        if not length or not str(length).isdigit():
            raise RuntimeError("source does not support ranges and omitted Content-Length")
        size = int(length)
        if size > RANGE_CHUNK_BYTES:
            raise RuntimeError("source does not support resumable ranges")
        return {
            "size": size,
            "ranges": False,
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
            "content_type": content_type,
        }
    finally:
        await response.aclose()


async def _download_to_partial(
    client: httpx.AsyncClient,
    source: ArtifactSource,
    partial: Path,
    probe: dict[str, Any],
) -> None:
    total = int(probe["size"])
    partial.parent.mkdir(parents=True, exist_ok=True)
    if partial.exists() and partial.stat().st_size > total:
        partial.unlink()
    offset = partial.stat().st_size if partial.exists() else 0
    if offset == total:
        return

    if not probe["ranges"]:
        if offset:
            partial.unlink()
        response = await _safe_stream_request(
            client,
            "GET",
            source.url,
            assigned_worker=source.assigned_worker,
            headers={"Accept-Encoding": "identity"},
        )
        try:
            if response.status_code != 200:
                raise RuntimeError(f"source returned HTTP {response.status_code}")
            written = 0
            with partial.open("wb") as output:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    _ensure_cache_write_capacity(partial, len(chunk))
                    output.write(chunk)
                    written += len(chunk)
                    if written > total:
                        raise RuntimeError("source exceeded declared Content-Length")
            if written != total:
                raise RuntimeError(f"short download: expected {total}, received {written}")
        finally:
            await response.aclose()
        return

    with partial.open("ab") as output:
        while offset < total:
            end = min(total - 1, offset + RANGE_CHUNK_BYTES - 1)
            response = await _safe_stream_request(
                client,
                "GET",
                source.url,
                assigned_worker=source.assigned_worker,
                headers={"Range": f"bytes={offset}-{end}", "Accept-Encoding": "identity"},
            )
            try:
                if response.status_code != 206:
                    raise RuntimeError(f"range returned HTTP {response.status_code}")
                start, declared_end, declared_total = _parse_content_range(
                    response.headers.get("content-range") or ""
                )
                if start != offset or declared_end != end or declared_total != total:
                    raise RuntimeError("range response does not match requested block")
                written = 0
                async for chunk in response.aiter_bytes(1024 * 1024):
                    _ensure_cache_write_capacity(partial, len(chunk))
                    output.write(chunk)
                    written += len(chunk)
                if written != end - start + 1:
                    raise RuntimeError("range block was truncated")
                output.flush()
                os.fsync(output.fileno())
                offset += written
            finally:
                await response.aclose()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_file(
    path: Path,
    *,
    role: str,
    content_type: str = "",
    declared_name: Optional[str] = None,
) -> None:
    size = path.stat().st_size
    if size <= 0:
        raise RuntimeError("artifact is empty")
    with path.open("rb") as stream:
        header = stream.read(512)
    lower_type = str(content_type or "").lower()
    suffix = Path(declared_name or path.name).suffix.lower()
    htmlish = b"<html" in header.lower() or b"<!doctype html" in header.lower()
    if htmlish and suffix not in (".html", ".htm"):
        raise RuntimeError("source returned HTML instead of an artifact")
    if "text/html" in lower_type and suffix not in (".html", ".htm"):
        raise RuntimeError("source declared HTML instead of an artifact")
    if suffix == ".glb":
        if len(header) < 12 or header[:4] != b"glTF" or struct.unpack("<I", header[4:8])[0] != 2:
            raise RuntimeError("invalid GLB signature")
    elif suffix == ".zip" or role == "full_bundle":
        if header[:2] != b"PK":
            raise RuntimeError("invalid ZIP signature")
        try:
            with zipfile.ZipFile(path, "r") as archive:
                bad = archive.testzip()
                if bad:
                    raise RuntimeError(f"ZIP CRC failed for {bad}")
                if not archive.namelist():
                    raise RuntimeError("ZIP is empty")
        except zipfile.BadZipFile as exc:
            raise RuntimeError("invalid ZIP archive") from exc
    elif suffix == ".fbx":
        if not (header.startswith(b"Kaydara FBX Binary") or header.lstrip().startswith(b"; FBX")):
            raise RuntimeError("invalid FBX signature")
    elif suffix in (".mp4", ".mov", ".m4v"):
        if len(header) < 12 or b"ftyp" not in header[4:32]:
            raise RuntimeError("invalid video signature")
    elif suffix == ".png" and not header.startswith(b"\x89PNG\r\n\x1a\n"):
        raise RuntimeError("invalid PNG signature")
    elif suffix in (".jpg", ".jpeg") and not header.startswith(b"\xff\xd8\xff"):
        raise RuntimeError("invalid JPEG signature")
    elif suffix == ".json":
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("invalid JSON artifact") from exc


async def cache_source(
    root: Path,
    task_id: str,
    source: ArtifactSource,
    *,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    validated_url = validate_source_url(source.url, source.assigned_worker)
    destination = _safe_destination(root, task_id, source.relative_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file():
        validate_file(destination, role=source.role)
        return {
            "source_url": validated_url,
            "relative_path": safe_relative_path(source.relative_path),
            "size": destination.stat().st_size,
            "sha256": _sha256(destination),
            "etag": None,
            "last_modified": None,
            "role": source.role,
            "cached_at": _iso_now(),
            "long_lived": _is_long_lived(source),
        }

    probe = await _probe_source(client, source)
    if source.expected_size not in (None, 0) and int(source.expected_size) != int(probe["size"]):
        raise RuntimeError(
            f"size mismatch: catalog={int(source.expected_size)} source={int(probe['size'])}"
        )
    partial_name = hashlib.sha256(
        f"{validated_url}\n{source.relative_path}".encode("utf-8")
    ).hexdigest()
    partial = _task_dir(root, task_id) / ".partial" / f"{partial_name}.part"
    await _download_to_partial(client, source, partial, probe)
    validate_file(
        partial,
        role=source.role,
        content_type=probe.get("content_type") or "",
        declared_name=source.relative_path,
    )
    if partial.stat().st_size != int(probe["size"]):
        raise RuntimeError("downloaded size does not match source")
    os.replace(partial, destination)
    return {
        "source_url": validated_url,
        "relative_path": safe_relative_path(source.relative_path),
        "size": destination.stat().st_size,
        "sha256": _sha256(destination),
        "etag": probe.get("etag"),
        "last_modified": probe.get("last_modified"),
        "role": source.role,
        "cached_at": _iso_now(),
        "long_lived": _is_long_lived(source),
    }


async def cache_sources(
    task_id: str,
    sources: Sequence[ArtifactSource],
    *,
    root: Optional[Path] = None,
) -> tuple[dict[str, Any], list[str]]:
    cache_root = Path(root or ARTIFACT_CACHE_ROOT)
    manifest = read_manifest(cache_root, task_id)
    existing = {
        str(item.get("relative_path")): item
        for item in manifest.get("files", [])
        if isinstance(item, dict) and item.get("relative_path")
    }
    errors: list[str] = []
    timeout = httpx.Timeout(connect=30.0, read=180.0, write=60.0, pool=30.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        for source in sources:
            try:
                item = await cache_source(cache_root, task_id, source, client=client)
                existing[item["relative_path"]] = item
                manifest["files"] = sorted(existing.values(), key=lambda row: row["relative_path"])
                write_manifest(cache_root, task_id, manifest)
            except ArtifactCacheReserveError:
                manifest["files"] = sorted(existing.values(), key=lambda row: row["relative_path"])
                write_manifest(cache_root, task_id, manifest)
                raise
            except Exception as exc:
                errors.append(f"{source.role}:{source.url}: {exc}")
    manifest["files"] = sorted(existing.values(), key=lambda row: row["relative_path"])
    write_manifest(cache_root, task_id, manifest)
    return manifest, errors


def _worker_key(task: Task) -> str:
    return worker_identity(str(task.worker_api or task.progress_page or "")) or str(
        urlsplit(str(task.worker_api or "")).hostname or "local"
    ).lower()


async def enqueue_artifact_cache(
    db,
    task: Task,
    *,
    now: Optional[datetime] = None,
    force_refresh: bool = False,
) -> ArtifactCacheJob:
    if task.status != "done":
        raise ValueError("only completed tasks can enter the artifact cache")
    moment = now or utcnow()
    full_until = task.artifact_cache_full_until or moment + timedelta(hours=ARTIFACT_CACHE_FULL_HOURS)
    result = await db.execute(
        select(ArtifactCacheJob).where(ArtifactCacheJob.task_id == task.id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        job = ArtifactCacheJob(
            task_id=task.id,
            worker_key=_worker_key(task),
            status="pending",
            attempt_count=0,
            next_attempt_at=moment,
            deadline_at=full_until,
            created_at=moment,
            updated_at=moment,
        )
        db.add(job)
        task.artifact_cache_status = "pending"
        task.artifact_cache_error = None
    elif job.status == "failed" and moment < job.deadline_at:
        job.status = "pending"
        job.next_attempt_at = moment
        job.updated_at = moment
        task.artifact_cache_status = (
            "partial" if int(task.artifact_cache_file_count or 0) else "pending"
        )
        task.artifact_cache_error = None
    elif force_refresh and moment < job.deadline_at and job.status != "caching":
        job.status = "pending"
        job.next_attempt_at = moment
        job.finished_at = None
        job.updated_at = moment
        task.artifact_cache_status = (
            "partial" if int(task.artifact_cache_file_count or 0) else "pending"
        )
        task.artifact_cache_error = None
    elif task.artifact_cache_status is None:
        task.artifact_cache_status = "pending"
    task.artifact_cache_full_until = full_until
    await db.flush()
    return job


async def enqueue_recent_completed_tasks(db, *, limit: int = 200) -> int:
    cutoff = utcnow() - timedelta(hours=ARTIFACT_CACHE_FULL_HOURS)
    result = await db.execute(
        select(Task)
        .where(
            Task.status == "done",
            Task.updated_at >= cutoff,
            Task.artifact_cache_status.is_(None),
        )
        .order_by(Task.updated_at.desc())
        .limit(max(1, int(limit)))
    )
    count = 0
    for task in result.scalars().all():
        await enqueue_artifact_cache(db, task)
        count += 1
    return count


async def recover_interrupted_jobs() -> int:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ArtifactCacheJob).where(ArtifactCacheJob.status == "caching")
        )
        jobs = list(result.scalars().all())
        for job in jobs:
            job.status = "pending"
            job.next_attempt_at = utcnow()
            job.last_error = "backend restarted during cache transfer"
        if jobs:
            await db.commit()
        return len(jobs)


async def _claim_due_job() -> Optional[tuple[int, str, str]]:
    async with _claim_lock:
        async with AsyncSessionLocal() as db:
            moment = utcnow()
            result = await db.execute(
                select(ArtifactCacheJob)
                .where(
                    ArtifactCacheJob.status == "pending",
                    ArtifactCacheJob.next_attempt_at <= moment,
                )
                .order_by(ArtifactCacheJob.next_attempt_at, ArtifactCacheJob.id)
                .limit(1)
            )
            job = result.scalar_one_or_none()
            if job is None:
                return None
            job.status = "caching"
            job.started_at = moment
            job.updated_at = moment
            task = await db.get(Task, job.task_id)
            if task is not None:
                task.artifact_cache_status = "caching"
            await db.commit()
            return int(job.id), str(job.task_id), str(job.worker_key or "local")


def retry_delay_seconds(attempt_count: int) -> int:
    index = max(0, int(attempt_count) - 1)
    return RETRY_SECONDS[index] if index < len(RETRY_SECONDS) else RETRY_SECONDS[-1]


async def _finish_job(
    job_id: int,
    *,
    manifest: Optional[dict[str, Any]],
    errors: Sequence[str],
) -> None:
    async with AsyncSessionLocal() as db:
        job = await db.get(ArtifactCacheJob, job_id)
        if job is None:
            return
        task = await db.get(Task, job.task_id)
        moment = utcnow()
        job.attempt_count = int(job.attempt_count or 0) + 1
        count, size = manifest_stats(manifest or {"files": []})
        error_text = "\n".join(str(item) for item in errors)[:8000] or None
        if task is not None:
            task.artifact_cache_file_count = count
            task.artifact_cache_bytes = size
            task.artifact_cache_error = error_text

        if not errors and count > 0:
            job.status = "ready"
            job.finished_at = moment
            job.last_error = None
            if task is not None:
                task.artifact_cache_status = "ready"
                task.artifact_cache_error = None
        elif moment >= job.deadline_at:
            job.status = "failed"
            job.finished_at = moment
            job.last_error = error_text or "artifact cache deadline expired"
            if task is not None:
                task.artifact_cache_status = "partial" if count else "failed"
        else:
            job.status = "pending"
            job.next_attempt_at = moment + timedelta(seconds=retry_delay_seconds(job.attempt_count))
            job.last_error = error_text or "no cacheable artifacts discovered"
            if task is not None:
                task.artifact_cache_status = "partial" if count else "pending"
        job.updated_at = moment
        await db.commit()


async def _defer_job_for_reserve(
    job_id: int,
    *,
    manifest: Optional[dict[str, Any]],
    error: str,
) -> None:
    """Pause infrastructure-limited cache work without spending an attempt."""
    async with AsyncSessionLocal() as db:
        job = await db.get(ArtifactCacheJob, job_id)
        if job is None:
            return
        task = await db.get(Task, job.task_id)
        moment = utcnow()
        count, size = manifest_stats(manifest or {"files": []})
        message = str(error)[:8000]
        if moment >= job.deadline_at:
            job.status = "failed"
            job.finished_at = moment
        else:
            job.status = "pending"
            job.next_attempt_at = moment + timedelta(minutes=5)
        job.last_error = message
        job.updated_at = moment
        if task is not None:
            task.artifact_cache_file_count = count
            task.artifact_cache_bytes = size
            task.artifact_cache_error = message
            task.artifact_cache_status = "partial" if count else (
                "failed" if job.status == "failed" else "pending"
            )
        await db.commit()


async def _process_claimed_job(
    job_id: int,
    task_id: str,
    discover: Callable[[Task], Awaitable[Sequence[ArtifactSource]]],
) -> None:
    manifest: Optional[dict[str, Any]] = None
    errors: list[str] = []
    reserve_error: Optional[str] = None
    try:
        full_until: Optional[datetime] = None
        async with AsyncSessionLocal() as db:
            task = await db.get(Task, task_id)
            if task is None:
                raise RuntimeError("task row no longer exists")
            full_until = task.artifact_cache_full_until
            sources = list(await discover(task))
        deduped: list[ArtifactSource] = []
        seen: set[tuple[str, str]] = set()
        for source in sources:
            key = (_normalized_url(source.url), safe_relative_path(source.relative_path))
            if key not in seen:
                seen.add(key)
                deduped.append(source)
        if not deduped:
            raise RuntimeError("no artifact sources discovered")
        manifest, errors = await cache_sources(task_id, deduped)
        if full_until is not None:
            aware_until = _aware_utc(full_until)
            manifest["full_until"] = aware_until.isoformat() if aware_until else None
            write_manifest(Path(ARTIFACT_CACHE_ROOT), task_id, manifest)
        required_paths = {
            safe_relative_path(source.relative_path) for source in deduped if source.required
        }
        cached_paths = {
            str(item.get("relative_path"))
            for item in (manifest or {}).get("files", [])
            if isinstance(item, dict)
        }
        missing = sorted(required_paths - cached_paths)
        errors.extend(f"required artifact missing: {path}" for path in missing)
    except ArtifactCacheReserveError as exc:
        reserve_error = str(exc)
        try:
            manifest = read_manifest(Path(ARTIFACT_CACHE_ROOT), task_id)
        except (OSError, ValueError):
            manifest = None
    except Exception as exc:
        errors.append(str(exc))
    if reserve_error is not None:
        await _defer_job_for_reserve(job_id, manifest=manifest, error=reserve_error)
        return
    await _finish_job(job_id, manifest=manifest, errors=errors)


async def artifact_cache_worker(
    discover: Callable[[Task], Awaitable[Sequence[ArtifactSource]]],
    *,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        claim = await _claim_due_job()
        if claim is None:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        job_id, task_id, worker_key = claim
        lock = _worker_locks.setdefault(worker_key, asyncio.Lock())
        async with lock:
            await _process_claimed_job(job_id, task_id, discover)


async def start_artifact_cache_workers(
    discover: Callable[[Task], Awaitable[Sequence[ArtifactSource]]],
) -> tuple[asyncio.Event, list[asyncio.Task]]:
    await recover_interrupted_jobs()
    async with AsyncSessionLocal() as db:
        await enqueue_recent_completed_tasks(db)
        await db.commit()
    stop_event = asyncio.Event()
    workers = [
        asyncio.create_task(
            artifact_cache_worker(discover, stop_event=stop_event),
            name=f"artifact-cache-{index + 1}",
        )
        for index in range(ARTIFACT_CACHE_CONCURRENCY)
    ]
    return stop_event, workers


async def stop_artifact_cache_workers(
    stop_event: Optional[asyncio.Event],
    workers: Iterable[asyncio.Task],
    *,
    grace_seconds: float = 5.0,
) -> None:
    if stop_event is not None:
        stop_event.set()
    tasks = list(workers)
    if not tasks:
        return
    _, pending = await asyncio.wait(tasks, timeout=max(0.0, float(grace_seconds)))
    for task in pending:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def cache_usage_bytes(root: Optional[Path] = None) -> int:
    cache_root = Path(root or ARTIFACT_CACHE_ROOT)
    total = 0
    if not cache_root.exists():
        return 0
    for path in cache_root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def creation_block_reason(root: Optional[Path] = None) -> Optional[str]:
    cache_root = Path(root or ARTIFACT_CACHE_ROOT)
    marker = cache_root / PAUSE_MARKER
    if marker.is_file():
        try:
            return marker.read_text(encoding="utf-8").strip() or "artifact cache reserve exhausted"
        except OSError:
            return "artifact cache reserve exhausted"
    try:
        free = shutil.disk_usage(cache_root if cache_root.exists() else cache_root.parent).free
    except OSError:
        return None
    reserve = int(ARTIFACT_CACHE_RESERVE_GB * 1024**3)
    if free < reserve:
        return (
            f"artifact cache disk reserve is below {ARTIFACT_CACHE_RESERVE_GB:.0f} GB; "
            "new tasks are paused to preserve completed rigs"
        )
    return None


def run_retention(
    *,
    root: Optional[Path] = None,
    now: Optional[datetime] = None,
    soft_cap_gb: Optional[float] = None,
    reserve_gb: Optional[float] = None,
    disk_usage_fn: Callable[[Path], Any] = shutil.disk_usage,
) -> dict[str, Any]:
    cache_root = Path(root or ARTIFACT_CACHE_ROOT)
    cache_root.mkdir(parents=True, exist_ok=True)
    moment = _aware_utc(now or datetime.now(timezone.utc))
    soft_cap = int((soft_cap_gb if soft_cap_gb is not None else ARTIFACT_CACHE_SOFT_CAP_GB) * 1024**3)
    reserve = int((reserve_gb if reserve_gb is not None else ARTIFACT_CACHE_RESERVE_GB) * 1024**3)
    usage = cache_usage_bytes(cache_root)
    free = int(disk_usage_fn(cache_root).free)
    pressure = usage > soft_cap or free < reserve
    entries: list[tuple[datetime, Path, dict[str, Any], dict[str, Any]]] = []
    candidates: list[tuple[int, datetime, Path, dict[str, Any], dict[str, Any]]] = []
    sha_counts: dict[str, int] = {}
    long_lived_sha_counts: dict[str, int] = {}
    manifests: dict[str, dict[str, Any]] = {}

    for manifest_path in cache_root.glob("*/manifest.json"):
        task_id = manifest_path.parent.name
        manifest = read_manifest(cache_root, task_id)
        manifests[task_id] = manifest
        full_until_raw = manifest.get("full_until")
        try:
            full_until = datetime.fromisoformat(str(full_until_raw).replace("Z", "+00:00"))
            full_until = _aware_utc(full_until)
        except (TypeError, ValueError):
            full_until = None
        for item in manifest.get("files", []):
            if not isinstance(item, dict):
                continue
            sha = str(item.get("sha256") or "")
            if sha:
                sha_counts[sha] = sha_counts.get(sha, 0) + 1
                if bool(item.get("long_lived")):
                    long_lived_sha_counts[sha] = long_lived_sha_counts.get(sha, 0) + 1
            if full_until is None or moment < full_until:
                continue
            try:
                path = _safe_destination(cache_root, task_id, str(item.get("relative_path") or ""))
            except ValueError:
                continue
            if not path.is_file():
                continue
            try:
                cached_at = datetime.fromisoformat(str(item.get("cached_at") or "").replace("Z", "+00:00"))
                cached_at = _aware_utc(cached_at) or moment
            except ValueError:
                cached_at = moment
            entries.append((cached_at, path, manifest, item))

    # Rank only after every manifest has contributed to the hash counts. This
    # makes duplicate detection independent of directory traversal order.
    # Long-lived deliverables may lose redundant copies, but never their last
    # existing copy. Non-duplicate long-lived entries are not candidates.
    for cached_at, path, manifest, item in entries:
        sha = str(item.get("sha256") or "")
        duplicate = bool(sha and sha_counts.get(sha, 0) > 1)
        long_lived = bool(item.get("long_lived"))
        if long_lived and (not sha or long_lived_sha_counts.get(sha, 0) <= 1):
            continue
        role = str(item.get("role") or "").lower()
        rank = 0 if duplicate else (1 if "diagnostic" in role else (2 if "preview" in role else 3))
        candidates.append((rank, cached_at, path, manifest, item))

    removed = 0
    freed = 0
    if pressure:
        for _rank, _cached_at, path, manifest, item in sorted(candidates, key=lambda row: (row[0], row[1])):
            if usage <= soft_cap and free + freed >= reserve:
                break
            sha = str(item.get("sha256") or "")
            if bool(item.get("long_lived")) and (
                not sha or long_lived_sha_counts.get(sha, 0) <= 1
            ):
                continue
            try:
                size = path.stat().st_size
                path.unlink()
            except OSError:
                continue
            manifest["files"] = [row for row in manifest.get("files", []) if row is not item]
            if sha:
                sha_counts[sha] = max(0, sha_counts.get(sha, 0) - 1)
                if bool(item.get("long_lived")):
                    long_lived_sha_counts[sha] = max(
                        0, long_lived_sha_counts.get(sha, 0) - 1
                    )
            usage = max(0, usage - size)
            freed += size
            removed += 1

    for task_id, manifest in manifests.items():
        write_manifest(cache_root, task_id, manifest)

    # The cache cap is deliberately soft: it triggers pruning, but it must not
    # stop production when the only remaining files are last-copy
    # deliverables. New work is blocked only when the filesystem reserve is
    # actually exhausted. The next retention pass will keep trying to bring
    # the cache below the soft cap as redundant/short-lived files expire.
    blocked = free + freed < reserve
    marker = cache_root / PAUSE_MARKER
    marker_created = False
    if blocked:
        reason = (
            f"cache={usage / 1024**3:.2f}GB cap={soft_cap / 1024**3:.2f}GB "
            f"free={(free + freed) / 1024**3:.2f}GB reserve={reserve / 1024**3:.2f}GB; "
            "last-copy deliverables preserved"
        )
        if not marker.exists():
            marker_created = True
        marker.write_text(reason, encoding="utf-8")
    elif marker.exists():
        marker.unlink()

    return {
        "pressure": pressure,
        "blocked": blocked,
        "marker_created": marker_created,
        "removed_count": removed,
        "freed_bytes": freed,
        "cache_bytes": usage,
        "free_bytes": free + freed,
    }
