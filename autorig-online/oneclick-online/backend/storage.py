"""Bounded persistent cache. Original uploads and active task artifacts are protected."""
import asyncio
import contextlib
import json
import os
from pathlib import Path
import shutil
import time
import uuid

import httpx

ROOT = Path(os.getenv('ONECLICK_DATA_DIR', '/srv/oneclick/data'))
CACHE = ROOT / 'cache'
MIN_FREE = int(float(os.getenv('MIN_FREE_SPACE_GB', '20')) * 1024**3)
MAX_CACHE = int(float(os.getenv('ONECLICK_CACHE_GB', '8')) * 1024**3)
MAX_FILE = int(float(os.getenv('ONECLICK_CACHE_FILE_GB', '2')) * 1024**3)
MAX_UPLOADS = int(float(os.getenv('ONECLICK_UPLOAD_GB', '24')) * 1024**3)
DOWNLOAD_SLOTS = asyncio.Semaphore(2)
RESERVATION_LOCK = asyncio.Lock()
RESERVED = 0
_LOCKS = [asyncio.Lock() for _ in range(64)]
INFLIGHT = set()


def size_of(path):
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file() and not p.is_symlink()) if path.exists() else 0


def admission(required=0, upload=False):
    ROOT.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(ROOT).free - required < MIN_FREE:
        raise OSError('Insufficient disk reserve; retry after storage is available')
    if upload and size_of(ROOT / 'uploads') + required > MAX_UPLOADS:
        raise OSError('Upload storage quota reached; originals are preserved')


def upload_admission(required=0):
    """Account for disk-backed chunk-session reservations across requests/restarts."""
    committed = size_of(ROOT / 'uploads')
    remaining = 0
    for meta in (ROOT / 'uploads' / 'temp').glob('*/metadata.json'):
        try:
            data = json.loads(meta.read_text())
            # Merge needs one extra final copy while parts still exist.
            remaining += max(0, 2 * int(data['total_size']) - size_of(meta.parent))
        except (OSError, ValueError, KeyError):
            continue
    admission(required + remaining)
    if committed + remaining + required > MAX_UPLOADS:
        raise OSError('Upload storage quota reached; originals are preserved')


def valid_glb(path):
    try:
        with path.open('rb') as handle:
            h = handle.read(12)
        return len(h) == 12 and h[:4] == b'glTF' and int.from_bytes(h[4:8], 'little') in (1, 2) and int.from_bytes(h[8:12], 'little') == path.stat().st_size
    except OSError:
        return False


class BoundedClient(httpx.AsyncClient):
    """Cap buffered metadata/image responses; large assets must use streaming."""
    async def send(self, request, *, stream=False, **kwargs):
        response = await super().send(request, stream=True, **kwargs)
        if stream:
            return response
        try:
            body = bytearray()
            async for block in response.aiter_bytes(65536):
                body.extend(block)
                if len(body) > 16 * 1024**2:
                    raise ValueError('Buffered upstream response exceeds 16 MiB')
            response._content = bytes(body)
            return response
        finally:
            await response.aclose()


async def download(url, target, *, glb=False):
    global RESERVED
    target = Path(target)
    if not target.resolve().is_relative_to(CACHE.resolve()):
        raise ValueError('Cache target escapes cache root')
    async with _LOCKS[hash(str(target)) % len(_LOCKS)]:
        if target.exists() and (not glb or valid_glb(target)):
            os.utime(target, None)
            return target
        async with DOWNLOAD_SLOTS:
            target.parent.mkdir(parents=True, exist_ok=True)
            part = target.with_name(target.name + '.part.' + uuid.uuid4().hex)
            INFLIGHT.add(str(target))
            reserved = 0
            try:
                async with BoundedClient(timeout=120, follow_redirects=True) as client:
                    async with client.stream('GET', url) as response:
                        response.raise_for_status()
                        length = int(response.headers.get('content-length', '0'))
                        if length > MAX_FILE:
                            raise OSError('Asset exceeds per-file cache limit')
                        async with RESERVATION_LOCK:
                            requested = length or MAX_FILE
                            admission(RESERVED + requested)
                            if await asyncio.to_thread(size_of, CACHE) + RESERVED + requested > MAX_CACHE:
                                raise OSError('Cache quota reached')
                            reserved = requested
                            RESERVED += reserved
                        written = 0
                        with part.open('wb') as out:
                            async for block in response.aiter_bytes(65536):
                                written += len(block)
                                if written > reserved:
                                    raise OSError('Asset exceeds per-file cache limit')
                                if written % (4 * 1024**2) < len(block):
                                    admission(len(block))
                                    if await asyncio.to_thread(size_of, CACHE) > MAX_CACHE:
                                        raise OSError('Cache quota reached')
                                out.write(block)
                        if written == 0 or (length and written != length):
                            raise ValueError('Incomplete upstream asset')
                if glb and not valid_glb(part):
                    raise ValueError('Invalid GLB length/header')
                os.replace(part, target)
                return target
            finally:
                if reserved:
                    async with RESERVATION_LOCK:
                        RESERVED -= reserved
                INFLIGHT.discard(str(target))
                part.unlink(missing_ok=True)


def clean(active_ids=(), *, force=False):
    """Evict recoverable cache on quota/free-space pressure; never delete uploads or DB."""
    ROOT.mkdir(parents=True, exist_ok=True)
    files = [p for p in CACHE.rglob('*') if p.is_file() and not p.is_symlink()] if CACHE.exists() else []
    total = sum(p.stat().st_size for p in files)
    removed = freed = 0
    now = time.time()
    for p in sorted(files, key=lambda f: f.stat().st_mtime):
        if any(task in str(p) for task in active_ids) or str(p) in INFLIGHT or '.part.' in p.name:
            continue
        pressure = total > MAX_CACHE or shutil.disk_usage(ROOT).free < MIN_FREE
        if not pressure and not force:
            break
        if now - p.stat().st_mtime < 3600:
            continue
        amount = p.stat().st_size
        p.unlink()
        total -= amount
        freed += amount
        removed += 1
    # Incomplete spool files have no consumer after a crash; never touch an active download.
    for p in files:
        if p.exists() and '.part.' in p.name and now - p.stat().st_mtime > 86400 and str(p).split('.part.')[0] not in INFLIGHT:
            freed += p.stat().st_size
            p.unlink()
            removed += 1
    # Expired unfinished uploads; completed originals remain protected.
    temp = ROOT / 'uploads' / 'temp'
    if temp.exists():
        for d in temp.iterdir():
            if d.is_dir() and not d.is_symlink():
                newest = max([d.stat().st_mtime] + [p.stat().st_mtime for p in d.rglob('*')])
                if now - newest > 86400:
                    freed += size_of(d)
                    shutil.rmtree(d)
                    removed += 1
    # Empty task directories consume inodes even after every cached file is evicted.
    if CACHE.exists():
        for d in sorted((p for p in CACHE.rglob('*') if p.is_dir() and not p.is_symlink()), key=lambda p: len(p.parts), reverse=True):
            if any(task in str(d) for task in active_ids):
                continue
            with contextlib.suppress(OSError):
                d.rmdir()
    result = {'deleted_count': removed, 'freed_bytes': freed, 'freed_gb': freed / 1024**3, 'cache_bytes': size_of(CACHE), 'free_bytes': shutil.disk_usage(ROOT).free}
    (ROOT / 'storage-status.json').write_text(json.dumps(result))
    return result
