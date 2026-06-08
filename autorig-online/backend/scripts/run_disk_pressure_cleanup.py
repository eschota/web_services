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
from pathlib import Path


BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


def _free_gb() -> float:
    return shutil.disk_usage("/").free / (1024**3)


def _purge_oldest_glb_cache_until(*, glb_cache_dir: Path, target_free_gb: float) -> tuple[int, int]:
    removed = 0
    freed = 0
    if not glb_cache_dir.exists():
        return removed, freed

    candidates: list[tuple[float, int, Path]] = []
    for path in glb_cache_dir.glob("*.glb"):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        candidates.append((stat.st_mtime, stat.st_size, path))

    candidates.sort(key=lambda item: item[0])
    for _mtime, size, path in candidates:
        if _free_gb() >= target_free_gb:
            break
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
        freed += size
        print(
            f"[Disk Prepass] Removed GLB cache {path.name} "
            f"({size / (1024**2):.1f} MB); free now {_free_gb():.2f} GB"
        )
    return removed, freed


def _filesystem_prepass(target_free_gb: float) -> dict:
    from main import GLB_CACHE_DIR, purge_task_cache_bundle_zips

    summary = {
        "prepass_zip_deleted": 0,
        "prepass_glb_deleted": 0,
        "prepass_freed_gb": 0.0,
        "prepass_initial_free_gb": _free_gb(),
        "prepass_final_free_gb": _free_gb(),
    }
    if summary["prepass_initial_free_gb"] >= target_free_gb:
        return summary

    zd, zb = purge_task_cache_bundle_zips()
    summary["prepass_zip_deleted"] = int(zd)
    summary["prepass_freed_gb"] += float(zb) / (1024**3)
    summary["prepass_final_free_gb"] = _free_gb()
    if summary["prepass_final_free_gb"] >= target_free_gb:
        return summary

    gd, gb = _purge_oldest_glb_cache_until(
        glb_cache_dir=GLB_CACHE_DIR,
        target_free_gb=target_free_gb,
    )
    summary["prepass_glb_deleted"] = int(gd)
    summary["prepass_freed_gb"] += float(gb) / (1024**3)
    summary["prepass_final_free_gb"] = _free_gb()
    return summary


async def run() -> None:
    from config import AUTOMATIC_TASK_DB_DELETION, MIN_FREE_SPACE_GB
    from database import AsyncSessionLocal, init_db
    from main import cleanup_disk_space

    prepass = _filesystem_prepass(float(MIN_FREE_SPACE_GB))

    await init_db()
    async with AsyncSessionLocal() as db:
        result = await cleanup_disk_space(
            min_free_gb=MIN_FREE_SPACE_GB,
            db=db,
            delete_task_rows=AUTOMATIC_TASK_DB_DELETION,
        )

    summary = {
        "deleted_count": result.get("deleted_count", 0),
        "deleted_task_rows": result.get("deleted_task_rows", 0),
        "freed_gb": round(float(result.get("freed_gb", 0.0)), 4),
        "initial_free_gb": round(float(result.get("initial_free_gb", 0.0)), 4),
        "final_free_gb": round(float(result.get("final_free_gb", result.get("initial_free_gb", 0.0))), 4),
        "target_free_gb": result.get("target_free_gb", MIN_FREE_SPACE_GB),
    }
    summary.update(
        {
            "prepass_zip_deleted": prepass["prepass_zip_deleted"],
            "prepass_glb_deleted": prepass["prepass_glb_deleted"],
            "prepass_freed_gb": round(float(prepass["prepass_freed_gb"]), 4),
            "prepass_initial_free_gb": round(float(prepass["prepass_initial_free_gb"]), 4),
            "prepass_final_free_gb": round(float(prepass["prepass_final_free_gb"]), 4),
        }
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(run())
