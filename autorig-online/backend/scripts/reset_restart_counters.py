#!/usr/bin/env python3
"""
Reset restart_count for tasks in a recent time window (default: last 24 hours).

Why: after MAX_TASK_RESTARTS auto-requeue stops; tasks stay in error until restart_count is cleared.

Optional: --requeue-errors moves status=error tasks in the same window back to created (clears worker
state) so the background worker can dispatch them again.

Usage:
  cd backend && python scripts/reset_restart_counters.py
  python scripts/reset_restart_counters.py --hours 48 --dry-run
  python scripts/reset_restart_counters.py --requeue-errors
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


async def main() -> None:
    p = argparse.ArgumentParser(description="Reset restart_count for recent tasks")
    p.add_argument("--hours", type=float, default=24.0, help="Window: tasks with created_at >= now - hours")
    p.add_argument("--dry-run", action="store_true", help="Only print counts, no DB writes")
    p.add_argument(
        "--requeue-errors",
        action="store_true",
        help="Also set status=created for error tasks in window (full worker field clear, restart_count=0)",
    )
    args = p.parse_args()

    from sqlalchemy import select, update

    from database import AsyncSessionLocal, Task, init_db
    from tasks import admin_requeue_task_to_created

    await init_db()
    cutoff = datetime.utcnow() - timedelta(hours=args.hours)

    async with AsyncSessionLocal() as db:
        c_all = await db.execute(select(Task).where(Task.created_at >= cutoff))
        in_window = list(c_all.scalars().all())
        print(f"Tasks with created_at >= {cutoff.isoformat()} UTC: {len(in_window)}")

        if args.dry_run:
            n_err = sum(1 for t in in_window if t.status == "error")
            print(f"[dry-run] would set restart_count=0 for {len(in_window)} row(s)")
            if args.requeue_errors:
                print(f"[dry-run] would requeue {n_err} error task(s) to created")
            return

        r1 = await db.execute(
            update(Task).where(Task.created_at >= cutoff).values(restart_count=0)
        )
        await db.commit()
        print(f"Updated restart_count=0 for {r1.rowcount} row(s)")

        if args.requeue_errors:
            async with AsyncSessionLocal() as db2:
                r2 = await db2.execute(
                    select(Task).where(Task.created_at >= cutoff, Task.status == "error")
                )
                errors = list(r2.scalars().all())
                for t in errors:
                    await admin_requeue_task_to_created(db2, t)
                    print(f"  requeued to created: {t.id}")
                await db2.commit()
            print(f"Requeued {len(errors)} error task(s) to created")


if __name__ == "__main__":
    asyncio.run(main())
