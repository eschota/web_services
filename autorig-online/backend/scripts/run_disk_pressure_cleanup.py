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
import sys


BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


async def run() -> None:
    from config import AUTOMATIC_TASK_DB_DELETION, MIN_FREE_SPACE_GB
    from database import AsyncSessionLocal, init_db
    from main import cleanup_disk_space

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
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(run())
