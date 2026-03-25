"""
Server-side NSFW classification for task posters (images referenced in ready_urls).

Uses NudeNet ONNX detector on poster bytes downloaded from the worker URL.
Policy: single pipeline — no alternate client-only source of truth for DB fields.
"""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from typing import Any, List, Optional, Tuple
from urllib.parse import unquote

import httpx
from sqlalchemy import select
from database import AsyncSessionLocal, Task
from youtube_upload import schedule_youtube_upload_if_eligible
from telegram_bot import reserve_and_broadcast_task_done

# Matches worker poster naming used by task.html filters (video_poster*.jpeg / jpg).
_POSTER_SUBSTR = "video_poster"

CONTENT_CLASSIFIER_VERSION = "nudenet-320n-3.4"

_EXPLICIT_LABELS = frozenset(
    {
        "FEMALE_GENITALIA_EXPOSED",
        "MALE_GENITALIA_EXPOSED",
        "ANUS_EXPOSED",
    }
)
_SUGGESTIVE_LABELS = frozenset(
    {
        "FEMALE_BREAST_EXPOSED",
        "MALE_BREAST_EXPOSED",
        "BUTTOCKS_EXPOSED",
        "BELLY_EXPOSED",
        "FEMALE_GENITALIA_COVERED",
        "ARMPITS_EXPOSED",
        "FEET_EXPOSED",
    }
)

_detector: Any = None
_detector_lock = threading.Lock()


def find_poster_url(ready_urls: Optional[List[str]]) -> Optional[str]:
    """Return first ready URL that looks like the task video poster image."""
    if not ready_urls:
        return None
    for raw in ready_urls:
        url = (raw or "").strip()
        if not url:
            continue
        path = unquote(url.split("?", 1)[0]).lower()
        if _POSTER_SUBSTR not in path:
            continue
        if path.endswith(".jpeg") or path.endswith(".jpg"):
            return url
    return None


def _get_detector():
    global _detector
    with _detector_lock:
        if _detector is None:
            from nudenet import NudeDetector

            _detector = NudeDetector()
        return _detector


def detections_to_rating(detections: List[dict]) -> Tuple[str, float]:
    """Map NudeNet detection dicts to content_rating and a 0..1 score."""
    if not detections:
        return "safe", 0.0

    explicit_max = max(
        (d["score"] for d in detections if d.get("class") in _EXPLICIT_LABELS),
        default=0.0,
    )
    suggestive_max = max(
        (d["score"] for d in detections if d.get("class") in _SUGGESTIVE_LABELS),
        default=0.0,
    )
    score = max(float(explicit_max), float(suggestive_max))

    if explicit_max >= 0.35:
        return "adult", score
    if suggestive_max >= 0.35 or explicit_max >= 0.2:
        return "suggestive", score
    return "safe", score


def classify_image_bytes(image_bytes: bytes) -> Tuple[str, float]:
    det = _get_detector()
    raw = det.detect(image_bytes)
    return detections_to_rating(raw)


async def run_task_poster_classification(task_id: str) -> None:
    """
    Download poster from ready_urls, classify, persist Task fields.
    Idempotent: skips if content_classified_at is already set.
    """
    async with AsyncSessionLocal() as db:
        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task:
            return
        if task.status != "done":
            return
        if task.content_classified_at is not None:
            await reserve_and_broadcast_task_done(task_id)
            return

        poster_url = find_poster_url(task.ready_urls or [])
        now = datetime.utcnow()
        version = CONTENT_CLASSIFIER_VERSION

        if not poster_url:
            task.content_rating = "unknown"
            task.content_score = None
            task.content_classified_at = now
            task.content_classifier_version = version
            task.updated_at = now
            await db.commit()
            schedule_youtube_upload_if_eligible(task_id)
            await reserve_and_broadcast_task_done(task_id)
            return

        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                resp = await client.get(poster_url)
                resp.raise_for_status()
                image_bytes = resp.content
        except Exception as e:
            print(f"[ContentModeration] Poster fetch failed for task {task_id}: {e}")
            task.content_rating = "unknown"
            task.content_score = None
            task.content_classified_at = now
            task.content_classifier_version = f"{version}:fetch_error"
            task.updated_at = now
            await db.commit()
            schedule_youtube_upload_if_eligible(task_id)
            await reserve_and_broadcast_task_done(task_id)
            return

        try:
            rating, score = await asyncio.to_thread(classify_image_bytes, image_bytes)
        except Exception as e:
            print(f"[ContentModeration] Classification failed for task {task_id}: {e}")
            task.content_rating = "unknown"
            task.content_score = None
            task.content_classified_at = now
            task.content_classifier_version = f"{version}:classify_error"
            task.updated_at = now
            await db.commit()
            schedule_youtube_upload_if_eligible(task_id)
            await reserve_and_broadcast_task_done(task_id)
            return

        task.content_rating = rating
        task.content_score = score
        task.content_classified_at = now
        task.content_classifier_version = version
        task.updated_at = now
        await db.commit()
        schedule_youtube_upload_if_eligible(task_id)
        await reserve_and_broadcast_task_done(task_id)


def schedule_task_poster_classification(task_id: str) -> None:
    """Fire-and-forget background classification (call after task is committed)."""
    asyncio.create_task(run_task_poster_classification(task_id))
