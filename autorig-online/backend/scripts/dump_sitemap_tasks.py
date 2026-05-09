#!/usr/bin/env python3
"""
Daily JSON snapshot of tasks eligible for /sitemap/gallery and /m/{id} (same SQL filters).

Run via cron, e.g. 0 3 * * * /root/autorig-online/venv/bin/python /root/autorig-online/backend/scripts/dump_sitemap_tasks.py

Output default: backend/data/sitemap_tasks_snapshot.json (override with --output).
Does not replace live sitemap generation (still from DB on each HTTP request).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


def _iso(dt) -> str | None:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.isoformat() + "Z"
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


async def run(output_path: str) -> None:
    from sqlalchemy import func, select

    from database import AsyncSessionLocal, Task, init_db
    from seo_gallery import gallery_seo_task_conditions

    await init_db()
    conds = gallery_seo_task_conditions()
    q = (
        select(
            Task.id,
            Task.created_at,
            Task.updated_at,
            Task.poster_llm_title,
            Task.poster_llm_description,
            Task.poster_llm_keywords,
            Task.pipeline_kind,
        )
        .where(*conds)
        .order_by(func.coalesce(Task.updated_at, Task.created_at).desc())
    )
    async with AsyncSessionLocal() as db:
        result = await db.execute(q)
        rows = result.all()

    tasks_out = []
    for row in rows:
        tid, ca, ua, pt, pd, pk, pipe = row
        tasks_out.append(
            {
                "id": tid,
                "created_at": _iso(ca),
                "updated_at": _iso(ua),
                "poster_llm_title": pt,
                "poster_llm_description": pd,
                "poster_llm_keywords": pk,
                "pipeline_kind": pipe,
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "task_count": len(tasks_out),
        "tasks": tasks_out,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"[dump_sitemap_tasks] wrote {len(tasks_out)} tasks to {output_path}")


def main() -> None:
    default_out = os.path.join(BACKEND, "data", "sitemap_tasks_snapshot.json")
    p = argparse.ArgumentParser(description="Dump sitemap-eligible tasks to JSON")
    p.add_argument(
        "--output",
        "-o",
        default=default_out,
        help=f"Output file path (default: {default_out})",
    )
    args = p.parse_args()
    asyncio.run(run(args.output))


if __name__ == "__main__":
    main()
