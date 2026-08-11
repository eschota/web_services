#!/usr/bin/env python3
"""
Pressure-only disk cleanup for AutoRig.

This keeps production writable when the web process is unhealthy or the root
filesystem is already close to full. It uses the same DB-aware cleanup logic as
the backend and never deletes task rows unless the environment explicitly opts
into that legacy behavior.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path


BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


def _free_gb() -> float:
    return shutil.disk_usage("/").free / (1024**3)


def _disk_snapshot() -> dict:
    usage = shutil.disk_usage("/")
    total_gb = usage.total / (1024**3)
    free_gb = usage.free / (1024**3)
    used_percent = 0.0
    if usage.total > 0:
        used_percent = ((usage.total - usage.free) / usage.total) * 100.0
    return {
        "total_gb": total_gb,
        "free_gb": free_gb,
        "used_percent": used_percent,
    }


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        try:
            total += item.stat().st_size
        except FileNotFoundError:
            continue
    return total


def _target_free_gb(*, min_free_gb: float, used_percent_threshold: float, buffer_gb: float) -> float:
    snapshot = _disk_snapshot()
    threshold_free_gb = snapshot["total_gb"] * max(0.0, 100.0 - float(used_percent_threshold)) / 100.0
    return max(float(min_free_gb), threshold_free_gb + float(buffer_gb))


def _age_cutoff_timestamp(min_age_hours: float) -> float:
    return time.time() - max(0.0, float(min_age_hours)) * 3600.0


# The GLB cache is *mostly* *_all_animations_unity.fbx, so a pruner that globbed
# only "*.glb" could never enforce the cap. But those files are NOT regenerable
# on their own: /api/task/{id}/animations.fbx is a pass-through proxy to the
# converter worker, and workers purge their output. Once a worker has purged it,
# the cache copy is the LAST copy of a user deliverable. So the pruner may only
# delete an entry after confirming the upstream still serves it.
GLB_CACHE_PRUNABLE_SUFFIXES = (".glb", ".fbx")
# Abandoned partial downloads carry no value once they are old.
GLB_CACHE_ORPHAN_SUFFIXES = (".tmp",)
# Never touch something that may still be streaming to disk.
GLB_CACHE_HARD_MIN_AGE_SECONDS = 600.0
# Upstream probing is network-bound; keep it bounded per run.
GLB_CACHE_MAX_PROBES = int(os.getenv("GLB_CACHE_MAX_UPSTREAM_PROBES", "60"))
# Verdicts are remembered so the probe budget is spent on entries nobody has
# asked about yet. Re-checked weekly: a worker can be rebuilt and start serving
# an artifact again, and this must not become a permanent blacklist.
LAST_COPY_MEMO_PATH = Path(
    os.getenv("GLB_CACHE_LAST_COPY_MEMO", "/var/autorig/glb_cache_last_copy.json")
)
LAST_COPY_MEMO_TTL_SECONDS = float(
    os.getenv("GLB_CACHE_LAST_COPY_TTL_SECONDS", str(7 * 24 * 3600))
)


def _load_last_copy_memo() -> dict:
    try:
        data = json.loads(LAST_COPY_MEMO_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
    except Exception:
        return {}
    fresh_after = time.time() - LAST_COPY_MEMO_TTL_SECONDS
    return {
        name: float(at)
        for name, at in data.items()
        if isinstance(at, (int, float)) and float(at) > fresh_after
    }


def _save_last_copy_memo(memo: dict) -> None:
    try:
        LAST_COPY_MEMO_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = LAST_COPY_MEMO_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(memo), encoding="utf-8")
        tmp.replace(LAST_COPY_MEMO_PATH)
    except Exception as exc:
        print(f"[Disk Prepass] could not persist last-copy memo: {exc}")
GLB_CACHE_PROBE_TIMEOUT_SECONDS = float(os.getenv("GLB_CACHE_PROBE_TIMEOUT", "8"))


async def _upstream_is_available(url: str) -> bool:
    """True only when the worker still serves this artifact."""
    if not url or not url.startswith(("http://", "https://")):
        return False
    try:
        import httpx

        async with httpx.AsyncClient(
            timeout=GLB_CACHE_PROBE_TIMEOUT_SECONDS, follow_redirects=True
        ) as client:
            resp = await client.head(url)
            if resp.status_code == 405:  # some workers reject HEAD
                resp = await client.get(url, headers={"Range": "bytes=0-1024"})
                return resp.status_code in (200, 206)
            return resp.status_code == 200
    except Exception:
        return False


async def _cache_entry_upstream_url(db, path: Path) -> str:
    """Resolve the worker URL a cache entry was copied from, if any."""
    from database import Task
    from sqlalchemy import select

    task_id, _, remainder = path.name.partition("_")
    if not task_id or not remainder:
        return ""
    # ready_urls is a python property over the "ready_urls" TEXT column, so the
    # ORM attribute cannot be selected directly.
    result = await db.execute(select(Task._ready_urls).where(Task.id == task_id))
    row = result.first()
    if not row or not row[0]:
        return ""
    try:
        urls = json.loads(row[0]) or []
    except Exception:
        return ""
    for url in urls:
        if isinstance(url, str) and url.rsplit("/", 1)[-1] == remainder:
            return url
    return ""


def _glb_cache_candidates(glb_cache_dir: Path, cutoff_ts: float) -> list[tuple[float, int, Path]]:
    candidates: list[tuple[float, int, Path, object]] = []
    for path in glb_cache_dir.iterdir():
        if not path.is_file() or path.suffix.lower() not in GLB_CACHE_PRUNABLE_SUFFIXES:
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if stat.st_mtime > cutoff_ts:
            continue
        candidates.append((stat.st_mtime, stat.st_size, path))
    candidates.sort(key=lambda item: item[0])
    return candidates


async def _purge_oldest_glb_cache_until(
    db,
    *,
    glb_cache_dir: Path,
    target_free_gb: float,
    max_cache_gb: float,
    min_age_hours: float,
) -> tuple[int, int]:
    """Evict cache entries oldest-first, but only ones the worker can still serve.

    The cache holds the only surviving copy of many deliverables (workers purge
    their outputs), so an entry is deleted exclusively after its upstream URL
    answers 200. Abandoned *.tmp partials are dropped without a probe.
    """
    removed = 0
    freed = 0
    if not glb_cache_dir.exists():
        return removed, freed

    cache_bytes = _dir_size_bytes(glb_cache_dir)

    def _pressured() -> bool:
        over_cap = max_cache_gb > 0 and (cache_bytes / (1024**3)) > max_cache_gb
        return _free_gb() < target_free_gb or over_cap

    # 1. Orphan partial downloads: pure garbage, no probe needed.
    orphan_cutoff = _age_cutoff_timestamp(max(1.0, min_age_hours))
    for path in sorted(glb_cache_dir.iterdir()):
        if not _pressured():
            break
        if not path.is_file() or path.suffix.lower() not in GLB_CACHE_ORPHAN_SUFFIXES:
            continue
        try:
            stat = path.stat()
            if stat.st_mtime > orphan_cutoff:
                continue
            size = stat.st_size
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
        freed += size
        cache_bytes = max(0, cache_bytes - size)
        print(f"[Disk Prepass] Removed abandoned partial {path.name} ({size / (1024**2):.1f} MB)")

    if not _pressured():
        return removed, freed

    # 2. Real cache entries, oldest first, each verified re-downloadable.
    cutoff_ts = _age_cutoff_timestamp(min_age_hours)
    candidates = _glb_cache_candidates(glb_cache_dir, cutoff_ts)
    if not candidates:
        relaxed_cutoff = time.time() - GLB_CACHE_HARD_MIN_AGE_SECONDS
        candidates = _glb_cache_candidates(glb_cache_dir, relaxed_cutoff)
        if candidates:
            print(
                f"[Disk Prepass] No GLB cache entry older than {min_age_hours}h; "
                f"relaxing to {GLB_CACHE_HARD_MIN_AGE_SECONDS / 60:.0f} min under pressure"
            )

    probes = 0
    kept_last_copy = 0
    memo = _load_last_copy_memo()
    memo_hits = 0
    now = time.time()
    for _mtime, size, path in candidates:
        if not _pressured():
            break
        if path.name in memo:
            # already established as the last copy; do not spend a probe on it
            memo_hits += 1
            kept_last_copy += 1
            continue
        if probes >= GLB_CACHE_MAX_PROBES:
            print(f"[Disk Prepass] Upstream probe budget reached ({GLB_CACHE_MAX_PROBES})")
            break
        upstream = await _cache_entry_upstream_url(db, path)
        probes += 1
        if not upstream or not await _upstream_is_available(upstream):
            kept_last_copy += 1
            memo[path.name] = now
            continue
        memo.pop(path.name, None)
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
        freed += size
        cache_bytes = max(0, cache_bytes - size)
        print(
            f"[Disk Prepass] Removed re-downloadable {path.name} "
            f"({size / (1024**2):.1f} MB); free now {_free_gb():.2f} GB, "
            f"glb_cache now {(cache_bytes / (1024**3)):.2f} GB"
        )
    _save_last_copy_memo(memo)
    if kept_last_copy:
        print(
            f"[Disk Prepass] Kept {kept_last_copy} cache entrie(s) ({memo_hits} from "
            "memo): the worker no longer serves them, so these are the last copy"
        )
    if _pressured() and removed == 0 and probes < GLB_CACHE_MAX_PROBES:
        # every candidate is the last copy and the cache is still over its cap:
        # nothing here can be freed safely, and silence would read as "handled"
        print(
            f"[Disk Prepass] OVER CAP with nothing safe to delete: "
            f"{cache_bytes / (1024**3):.2f} GB cached, cap {max_cache_gb:.1f} GB, "
            f"{kept_last_copy} entrie(s) are the only surviving copy"
        )
    return removed, freed


async def _filesystem_prepass(
    db,
    *,
    target_free_gb: float,
    glb_cache_max_gb: float,
    glb_cache_min_age_hours: float,
) -> dict:
    from main import GLB_CACHE_DIR, purge_task_cache_bundle_zips

    summary = {
        "prepass_zip_deleted": 0,
        "prepass_glb_deleted": 0,
        "prepass_freed_gb": 0.0,
        "prepass_glb_cache_gb_before": _dir_size_bytes(GLB_CACHE_DIR) / (1024**3),
        "prepass_glb_cache_gb_after": _dir_size_bytes(GLB_CACHE_DIR) / (1024**3),
        "prepass_initial_free_gb": _free_gb(),
        "prepass_final_free_gb": _free_gb(),
    }
    if (
        summary["prepass_initial_free_gb"] >= target_free_gb
        and (
            float(glb_cache_max_gb) <= 0
            or summary["prepass_glb_cache_gb_before"] <= float(glb_cache_max_gb)
        )
    ):
        return summary

    zd, zb = purge_task_cache_bundle_zips()
    summary["prepass_zip_deleted"] = int(zd)
    summary["prepass_freed_gb"] += float(zb) / (1024**3)
    summary["prepass_final_free_gb"] = _free_gb()
    if (
        summary["prepass_final_free_gb"] >= target_free_gb
        and (
            float(glb_cache_max_gb) <= 0
            or (_dir_size_bytes(GLB_CACHE_DIR) / (1024**3)) <= float(glb_cache_max_gb)
        )
    ):
        summary["prepass_glb_cache_gb_after"] = _dir_size_bytes(GLB_CACHE_DIR) / (1024**3)
        return summary

    gd, gb = await _purge_oldest_glb_cache_until(
        db,
        glb_cache_dir=GLB_CACHE_DIR,
        target_free_gb=target_free_gb,
        max_cache_gb=float(glb_cache_max_gb),
        min_age_hours=float(glb_cache_min_age_hours),
    )
    summary["prepass_glb_deleted"] = int(gd)
    summary["prepass_freed_gb"] += float(gb) / (1024**3)
    summary["prepass_final_free_gb"] = _free_gb()
    summary["prepass_glb_cache_gb_after"] = _dir_size_bytes(GLB_CACHE_DIR) / (1024**3)
    return summary


async def _enforce_periodic_task_cache_max_size(db, *, max_gb: float, min_age_hours: float) -> dict:
    from main import TASK_CACHE_DIR, _task_cache_eviction_candidates, purge_task_cache_bundle_zips

    if max_gb <= 0:
        return {"skipped": True, "reason": "cap_disabled"}

    cap_bytes = int(max_gb * 1024 * 1024 * 1024)
    total = _dir_size_bytes(TASK_CACHE_DIR)
    summary = {
        "cap_gb": round(max_gb, 4),
        "initial_bytes": total,
        "dirs_removed": 0,
        "bytes_freed_dirs": 0,
        "zips_deleted": 0,
        "zip_freed_bytes": 0,
        "final_bytes": total,
    }
    if total <= cap_bytes:
        return summary

    cutoff_ts = _age_cutoff_timestamp(min_age_hours)
    safety = 0
    while _dir_size_bytes(TASK_CACHE_DIR) > cap_bytes:
        safety += 1
        if safety > 50000:
            print("[TaskCacheCapPeriodic] Safety stop: too many iterations")
            break
        candidates = await _task_cache_eviction_candidates(db)
        if not candidates:
            break
        eligible = [item for item in candidates if item[0] <= cutoff_ts]
        if not eligible:
            print(
                f"[TaskCacheCapPeriodic] No terminal task-cache dirs older than "
                f"{float(min_age_hours):.1f}h; stop at {_dir_size_bytes(TASK_CACHE_DIR) / (1024**3):.2f} GB"
            )
            break
        _ts, dirname = eligible[0]
        target = TASK_CACHE_DIR / dirname
        if not target.is_dir():
            continue
        try:
            before = _dir_size_bytes(target)
            shutil.rmtree(target)
            summary["dirs_removed"] += 1
            summary["bytes_freed_dirs"] += before
            print(
                f"[TaskCacheCapPeriodic] Removed {dirname} (~{before / (1024**2):.1f} MB), "
                f"task_cache now ~{_dir_size_bytes(TASK_CACHE_DIR) / (1024**3):.2f} GB "
                f"(cap {max_gb} GB)"
            )
        except OSError as exc:
            print(f"[TaskCacheCapPeriodic] Failed to remove {target}: {exc}")
            break

    total = _dir_size_bytes(TASK_CACHE_DIR)
    summary["final_bytes"] = total
    if total > cap_bytes:
        zd, zb = purge_task_cache_bundle_zips()
        summary["zips_deleted"] = int(zd)
        summary["zip_freed_bytes"] = int(zb)
        summary["final_bytes"] = _dir_size_bytes(TASK_CACHE_DIR)
    return summary


def _task_has_preview_fallback(
    task,
    *,
    task_cache_dir: Path,
    glb_cache_dir: Path,
    preflight_render_dir: Path,
) -> bool:
    """Require a retained poster or interactive GLB before deleting an MP4."""
    task_id = str(getattr(task, "id", "") or "").strip()
    if not task_id:
        return False
    poster = preflight_render_dir / f"{task_id}.jpg"
    try:
        if poster.is_file() and poster.stat().st_size > 0:
            return True
    except OSError:
        pass
    task_dir = task_cache_dir / task_id
    if task_dir.is_dir():
        for suffix in ("*.jpg", "*.jpeg", "*.png", "*.webp", "*.glb"):
            for path in task_dir.rglob(suffix):
                try:
                    if path.is_file() and path.stat().st_size > 0:
                        return True
                except OSError:
                    continue
    if glb_cache_dir.is_dir():
        for path in glb_cache_dir.glob(f"{task_id}_*.glb"):
            try:
                if path.is_file() and path.stat().st_size > 0:
                    return True
            except OSError:
                continue
    return False


async def _purge_uploaded_video_cache_until(
    db,
    *,
    video_cache_dir: Path,
    target_free_gb: float,
    min_age_hours: float,
    task_cache_dir: Path | None = None,
    glb_cache_dir: Path | None = None,
    preflight_render_dir: Path | None = None,
) -> tuple[int, int]:
    """
    Last-resort pressure cleanup for backend-cached task previews.

    Once pressure is active, enforce preview retention for every terminal task.
    A poster or viewer GLB must remain available. Deliverables are never touched.
    """
    if _free_gb() >= target_free_gb or not video_cache_dir.exists():
        return 0, 0

    from database import Task
    from sqlalchemy import select

    result = await db.execute(select(Task).where(Task.status == "done"))
    cleanable_tasks = {str(task.id): task for task in result.scalars().all()}
    if not cleanable_tasks:
        return 0, 0

    task_cache_dir = task_cache_dir or (Path(BACKEND).parent / "static" / "tasks")
    glb_cache_dir = glb_cache_dir or (Path(BACKEND).parent / "static" / "glb_cache")
    preflight_render_dir = preflight_render_dir or Path("/var/autorig/preflight-renders")

    cutoff_ts = _age_cutoff_timestamp(min_age_hours)
    candidates: list[tuple[float, int, Path]] = []
    for path in video_cache_dir.glob("*.mp4"):
        task = cleanable_tasks.get(path.stem)
        if task is None:
            continue
        if not _task_has_preview_fallback(
            task,
            task_cache_dir=task_cache_dir,
            glb_cache_dir=glb_cache_dir,
            preflight_render_dir=preflight_render_dir,
        ):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if stat.st_mtime > cutoff_ts:
            continue
        candidates.append((stat.st_mtime, stat.st_size, path, task))

    candidates.sort(key=lambda item: item[0])
    removed = 0
    freed = 0
    now = datetime.utcnow()
    for _mtime, size, path, task in candidates:
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
        freed += size
        task.video_ready = False
        task.video_url = None
        if getattr(task, "youtube_video_id", None) is None and getattr(task, "youtube_upload_status", None) in (None, "deferred"):
            task.youtube_upload_status = "skipped"
            task.youtube_upload_error = "quota_window_expired"
        task.updated_at = now
        print(
            f"[Disk Video Cache] Removed expired preview {path.name} "
            f"({size / (1024**2):.1f} MB); free now {_free_gb():.2f} GB"
        )
    if removed and hasattr(db, "commit"):
        await db.commit()
    return removed, freed


async def run() -> None:
    from config import (
        AUTOMATIC_TASK_DB_DELETION,
        DISK_ALERT_USED_PERCENT,
        DISK_CLEANUP_TARGET_BUFFER_GB,
        DISK_CLEANUP_USED_PERCENT,
        GLB_CACHE_MAX_GB,
        GLB_CACHE_MIN_AGE_HOURS,
        MIN_FREE_SPACE_GB,
        PERIODIC_TASK_CACHE_MIN_AGE_HOURS,
        PERIODIC_TASK_CACHE_MAX_GB,
        VIDEO_CACHE_MIN_AGE_HOURS,
    )
    from database import AsyncSessionLocal
    from main import (
        GLB_CACHE_DIR,
        TASK_CACHE_DIR,
        cleanup_disk_space,
    )
    from telegram_bot import broadcast_disk_usage_warning

    target_free_gb = _target_free_gb(
        min_free_gb=float(MIN_FREE_SPACE_GB),
        used_percent_threshold=float(DISK_CLEANUP_USED_PERCENT),
        buffer_gb=float(DISK_CLEANUP_TARGET_BUFFER_GB),
    )
    before = _disk_snapshot()
    async with AsyncSessionLocal() as db:
        # the prepass needs the DB: an entry may only be evicted once its
        # upstream worker URL is confirmed to still serve it
        prepass = await _filesystem_prepass(
            db,
            target_free_gb=target_free_gb,
            glb_cache_max_gb=float(GLB_CACHE_MAX_GB),
            glb_cache_min_age_hours=float(GLB_CACHE_MIN_AGE_HOURS),
        )
        after_prepass = _disk_snapshot()
        task_cache_summary = await _enforce_periodic_task_cache_max_size(
            db,
            max_gb=float(PERIODIC_TASK_CACHE_MAX_GB),
            min_age_hours=float(PERIODIC_TASK_CACHE_MIN_AGE_HOURS),
        )
        # Videos are purged before cleanup_disk_space, not after it. These MP4s
        # are previews of tasks already uploaded to YouTube, so a second copy
        # exists; a task's cached downloads are what the user came for and are
        # routinely the last copy once the worker evicts its output. With the
        # purge running last the pressure was always relieved by deleting
        # deliverables first, and 8.7 GB of redundant video was never reached.
        video_cache_dir = Path("/var/autorig/videos")
        video_cache_gb_before = _dir_size_bytes(video_cache_dir) / (1024**3)
        video_deleted, video_freed_bytes = await _purge_uploaded_video_cache_until(
            db,
            video_cache_dir=video_cache_dir,
            target_free_gb=target_free_gb,
            min_age_hours=float(VIDEO_CACHE_MIN_AGE_HOURS),
        )
        video_cache_gb_after = _dir_size_bytes(video_cache_dir) / (1024**3)
        result = await cleanup_disk_space(
            min_free_gb=target_free_gb,
            db=db,
            delete_task_rows=AUTOMATIC_TASK_DB_DELETION,
        )
    after = _disk_snapshot()

    task_cache_gb = _dir_size_bytes(TASK_CACHE_DIR) / (1024**3)
    glb_cache_gb = _dir_size_bytes(GLB_CACHE_DIR) / (1024**3)

    if after["used_percent"] >= float(DISK_ALERT_USED_PERCENT):
        await broadcast_disk_usage_warning(
            free_gb=after["free_gb"],
            total_gb=after["total_gb"],
            used_percent=after["used_percent"],
            target_free_gb=target_free_gb,
            task_cache_gb=task_cache_gb,
            glb_cache_gb=glb_cache_gb,
            periodic_task_cache_cap_gb=float(PERIODIC_TASK_CACHE_MAX_GB),
            glb_cache_cap_gb=float(GLB_CACHE_MAX_GB),
        )

    summary = {
        "deleted_count": int(result.get("deleted_count", 0) or 0) + int(video_deleted),
        "deleted_task_rows": result.get("deleted_task_rows", 0),
        "freed_gb": round(
            float(result.get("freed_gb", 0.0)) + float(video_freed_bytes) / (1024**3),
            4,
        ),
        "initial_free_gb": round(float(before["free_gb"]), 4),
        "final_free_gb": round(float(after["free_gb"]), 4),
        "target_free_gb": round(float(target_free_gb), 4),
        "initial_used_percent": round(float(before["used_percent"]), 2),
        "post_prepass_free_gb": round(float(after_prepass["free_gb"]), 4),
        "post_prepass_used_percent": round(float(after_prepass["used_percent"]), 2),
        "final_used_percent": round(float(after["used_percent"]), 2),
        "disk_alert_used_percent": round(float(DISK_ALERT_USED_PERCENT), 2),
        "disk_cleanup_used_percent": round(float(DISK_CLEANUP_USED_PERCENT), 2),
        "task_cache_gb": round(float(task_cache_gb), 4),
        "glb_cache_gb": round(float(glb_cache_gb), 4),
        "periodic_task_cache_cap_gb": round(float(PERIODIC_TASK_CACHE_MAX_GB), 4),
        "periodic_task_cache_min_age_hours": round(float(PERIODIC_TASK_CACHE_MIN_AGE_HOURS), 2),
        "glb_cache_cap_gb": round(float(GLB_CACHE_MAX_GB), 4),
        "glb_cache_min_age_hours": round(float(GLB_CACHE_MIN_AGE_HOURS), 2),
        "video_cache_gb_before": round(float(video_cache_gb_before), 4),
        "video_cache_gb_after": round(float(video_cache_gb_after), 4),
        "video_cache_min_age_hours": round(float(VIDEO_CACHE_MIN_AGE_HOURS), 2),
        "video_cache_deleted": int(video_deleted),
        "video_cache_freed_gb": round(float(video_freed_bytes) / (1024**3), 4),
        "task_cache_dirs_removed": int(task_cache_summary.get("dirs_removed", 0) or 0),
        "task_cache_dirs_freed_gb": round(
            float(task_cache_summary.get("bytes_freed_dirs", 0) or 0) / (1024**3),
            4,
        ),
        "task_cache_zips_deleted": int(task_cache_summary.get("zips_deleted", 0) or 0),
        "task_cache_zip_freed_gb": round(
            float(task_cache_summary.get("zip_freed_bytes", 0) or 0) / (1024**3),
            4,
        ),
    }
    summary.update(
        {
            "prepass_zip_deleted": prepass["prepass_zip_deleted"],
            "prepass_glb_deleted": prepass["prepass_glb_deleted"],
            "prepass_freed_gb": round(float(prepass["prepass_freed_gb"]), 4),
            "prepass_initial_free_gb": round(float(prepass["prepass_initial_free_gb"]), 4),
            "prepass_final_free_gb": round(float(prepass["prepass_final_free_gb"]), 4),
            "prepass_glb_cache_gb_before": round(float(prepass["prepass_glb_cache_gb_before"]), 4),
            "prepass_glb_cache_gb_after": round(float(prepass["prepass_glb_cache_gb_after"]), 4),
        }
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(run())
