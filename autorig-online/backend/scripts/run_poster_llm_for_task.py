#!/usr/bin/env python3
"""
One-off: fetch task poster, run OpenAI vision metadata, update poster_llm_* in DB.
Usage: PYTHONPATH=. python scripts/run_poster_llm_for_task.py <task_id>

Loads OPENAI_API_KEY from environment; optionally from /etc/autorig-backend.env via python-dotenv.
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Repo root: backend/scripts -> backend
_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

try:
    from dotenv import load_dotenv

    load_dotenv("/etc/autorig-backend.env")
except Exception:
    pass

from sqlalchemy import select

from config import OPENAI_API_KEY
from content_moderation import (
    OPENAI_POSTER_MODEL,
    analyze_poster_llm_metadata,
    find_poster_url,
)
from database import AsyncSessionLocal, Task


async def main(task_id: str) -> int:
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY is empty. Set it in the environment or /etc/autorig-backend.env")
        return 1

    async with AsyncSessionLocal() as db:
        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task:
            print(f"ERROR: task not found: {task_id}")
            return 1

        poster_url = find_poster_url(task.ready_urls or [])
        if not poster_url:
            print("ERROR: no video_poster*.jpg/jpeg in ready_urls")
            return 1

        print(f"Poster URL: {poster_url[:120]}...")

        import httpx

        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(poster_url)
            resp.raise_for_status()
            image_bytes = resp.content

        print(f"Image bytes: {len(image_bytes)}")

        llm = analyze_poster_llm_metadata(image_bytes)
        if not llm:
            print("ERROR: OpenAI returned no usable JSON (see logs above)")
            return 1

        now = datetime.utcnow()
        vbase = "nudenet-320n-3.4"
        task.poster_llm_title = llm["title"][:256]
        task.poster_llm_description = llm["description"][:5000]
        task.poster_llm_keywords = json.dumps(llm["keywords"])
        task.poster_llm_at = now
        task.content_classifier_version = f"{vbase}+{OPENAI_POSTER_MODEL}"
        task.updated_at = now
        await db.commit()

        print("OK: poster_llm_* updated")
        print("title:", llm["title"][:100])
        print("description (first 200 chars):", llm["description"][:200].replace("\n", " "))
        print("keywords count:", len(llm["keywords"]))
        return 0


if __name__ == "__main__":
    tid = (sys.argv[1] or "").strip() if len(sys.argv) > 1 else ""
    if not tid:
        print("Usage: python scripts/run_poster_llm_for_task.py <task_id>")
        sys.exit(2)
    raise SystemExit(asyncio.run(main(tid)))
