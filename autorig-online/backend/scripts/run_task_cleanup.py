#!/usr/bin/env python3
"""
Gallery/task DB cleanup: JSON poster paths + upstream HTTP probe (same logic as the app background worker).

Run on a schedule via systemd timer or cron so cleanup happens even if uvicorn workers are busy.
Uses the same file lock as main.py so it does not run concurrently with the in-app purge.
"""
from __future__ import annotations

import asyncio
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


async def run() -> None:
    from database import AsyncSessionLocal, init_db
    from config import GALLERY_UPSTREAM_PURGE_BATCH, GALLERY_UPSTREAM_PURGE_ROUNDS
    from main import (
        _release_gallery_purge_lock,
        _try_acquire_gallery_purge_lock,
        purge_gallery_upstream_dead_tasks,
        purge_tasks_without_poster_and_video,
    )

    await init_db()
    lock_f = _try_acquire_gallery_purge_lock()
    if lock_f is None:
        print("[run_task_cleanup] lock busy, skipping")
        return
    try:
        async with AsyncSessionLocal() as db:
            sp = await purge_tasks_without_poster_and_video(db)
            print("[run_task_cleanup] string_purge", sp)
            total = 0
            for i in range(GALLERY_UPSTREAM_PURGE_ROUNDS):
                up = await purge_gallery_upstream_dead_tasks(db, batch=GALLERY_UPSTREAM_PURGE_BATCH)
                total += up["deleted"]
                print(f"[run_task_cleanup] upstream_round_{i + 1}", up)
                if up["scanned"] == 0 or up["deleted"] == 0:
                    break
            print("[run_task_cleanup] upstream_deleted_total", total)
    finally:
        _release_gallery_purge_lock(lock_f)


if __name__ == "__main__":
    asyncio.run(run())
