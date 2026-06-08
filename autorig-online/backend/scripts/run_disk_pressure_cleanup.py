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


def _purge_oldest_glb_cache_until(
    *,
    glb_cache_dir: Path,
    target_free_gb: float,
    max_cache_gb: float,
    min_age_hours: float,
) -> tuple[int, int]:
    removed = 0
    freed = 0
    if not glb_cache_dir.exists():
        return removed, freed

    candidates: list[tuple[float, int, Path]] = []
    cutoff_ts = _age_cutoff_timestamp(min_age_hours)
    for path in glb_cache_dir.glob("*.glb"):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if stat.st_mtime > cutoff_ts:
            continue
        candidates.append((stat.st_mtime, stat.st_size, path))

    candidates.sort(key=lambda item: item[0])
    for _mtime, size, path in candidates:
        cache_gb = _dir_size_bytes(glb_cache_dir) / (1024**3)
        needs_free_headroom = _free_gb() < target_free_gb
        exceeds_cap = max_cache_gb > 0 and cache_gb > max_cache_gb
        if not needs_free_headroom and not exceeds_cap:
            break
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
        freed += size
        print(
            f"[Disk Prepass] Removed GLB cache {path.name} "
            f"({size / (1024**2):.1f} MB); free now {_free_gb():.2f} GB, "
            f"glb_cache now {(_dir_size_bytes(glb_cache_dir) / (1024**3)):.2f} GB"
        )
    return removed, freed


def _filesystem_prepass(
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

    gd, gb = _purge_oldest_glb_cache_until(
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
    )
    from database import AsyncSessionLocal, init_db
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
    prepass = _filesystem_prepass(
        target_free_gb=target_free_gb,
        glb_cache_max_gb=float(GLB_CACHE_MAX_GB),
        glb_cache_min_age_hours=float(GLB_CACHE_MIN_AGE_HOURS),
    )
    after_prepass = _disk_snapshot()

    await init_db()
    async with AsyncSessionLocal() as db:
        task_cache_summary = await _enforce_periodic_task_cache_max_size(
            db,
            max_gb=float(PERIODIC_TASK_CACHE_MAX_GB),
            min_age_hours=float(PERIODIC_TASK_CACHE_MIN_AGE_HOURS),
        )
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
        "deleted_count": result.get("deleted_count", 0),
        "deleted_task_rows": result.get("deleted_task_rows", 0),
        "freed_gb": round(float(result.get("freed_gb", 0.0)), 4),
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
