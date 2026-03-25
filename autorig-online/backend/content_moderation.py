"""
Server-side NSFW classification for task posters (images referenced in ready_urls).

Uses NudeNet ONNX detector on poster bytes downloaded from the worker URL.
Optional: OpenAI vision for YouTube title/description/keywords (same image bytes).
Policy: single pipeline — no alternate client-only source of truth for DB fields.
"""
from __future__ import annotations

import asyncio
import base64
import json
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
OPENAI_POSTER_MODEL = "gpt-4o-mini"

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


def _normalize_keyword_list(keywords: List[Any]) -> List[str]:
    """Ensure exactly 25 non-empty keyword strings for storage."""
    cleaned: List[str] = []
    for x in keywords:
        t = str(x).strip()
        if t and t not in cleaned:
            cleaned.append(t)
        if len(cleaned) >= 25:
            return cleaned[:25]
    pool = [
        "3d character",
        "character rig",
        "game ready",
        "glb",
        "fbx",
        "unity",
        "unreal",
        "animation",
        "t pose",
        "skeletal mesh",
        "rigging",
        "3d model",
        "low poly",
        "pbr",
        "download",
    ]
    pi = 0
    while len(cleaned) < 25:
        p = pool[pi % len(pool)]
        pi += 1
        if p not in cleaned:
            cleaned.append(p)
        else:
            cleaned.append(f"{p}-{pi}")
    return cleaned[:25]


def analyze_poster_llm_metadata(image_bytes: bytes) -> Optional[dict]:
    """
    Sync OpenAI vision call. Returns dict with title, description, keywords (25 strings), or None on failure.
    Title describes who/what is on the poster (role, outfit, gear); description is for YouTube only.
    """
    from config import OPENAI_API_KEY

    if not OPENAI_API_KEY:
        return None
    try:
        from openai import OpenAI
    except ImportError as e:
        print(f"[ContentModeration] openai package missing: {e}")
        return None

    b64 = base64.standard_b64encode(image_bytes).decode("ascii")
    data_url = f"data:image/jpeg;base64,{b64}"

    prompt = """You look at a preview render of a 3D character (AutoRig Online task poster). The viewer already knows it is a 3D model — do NOT waste the title on that.
Return a single JSON object with exactly these keys:
- "title": string, English, max 95 characters. Describe ONLY what is visible about the subject: role or archetype (soldier, knight, robot, …), clothing or uniform, notable gear (gas mask, sword, grenades, helmet, …), faction or style if clear. No marketing phrases, no "rigged for Unity/Unreal", no "3D character", no "perfect for games", no engine names.
- "description": string, English, 2-4 short paragraphs for a YouTube video description: what appears in the render, tone/style, suitable for games/Blender; mention rig/animations only if relevant. Plain text, no HTML.
- "keywords": JSON array of exactly 25 short English strings for YouTube tags. Order matters: put the MOST SPECIFIC tags first (role, outfit, weapons, props, art style). Put generic tags last (3d, character, rigging, game asset, unity, unreal, blender, animation, glb, fbx). No hashtags. No NSFW or policy-evading content.

Output only valid JSON, no markdown."""

    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=OPENAI_POSTER_MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
        response_format={"type": "json_object"},
        max_tokens=2500,
        temperature=0.4,
    )
    choice = resp.choices[0].message.content
    if not choice:
        return None
    data = json.loads(choice)
    title = (data.get("title") or "").strip()
    desc = (data.get("description") or "").strip()
    kw_raw = data.get("keywords")
    if not title or not desc or not isinstance(kw_raw, list):
        print("[ContentModeration] OpenAI JSON missing title/description/keywords array")
        return None
    keywords = _normalize_keyword_list(kw_raw)
    if len(keywords) != 25:
        return None
    return {
        "title": title[:256],
        "description": desc[:5000],
        "keywords": keywords,
    }


def build_free3d_query_from_keywords(keywords: Optional[List[str]]) -> Optional[str]:
    """Use at most 3 distinct keywords for Free3D semantic search query."""
    if not keywords:
        return None
    parts: List[str] = []
    for k in keywords:
        t = (k or "").strip()
        if not t:
            continue
        if t.lower() not in {p.lower() for p in parts}:
            parts.append(t)
        if len(parts) >= 3:
            break
    if not parts:
        return None
    return " ".join(parts)


def build_free3d_similar_query(
    title: Optional[str],
    keywords: Optional[List[str]],
    *,
    max_len: int = 280,
) -> Optional[str]:
    """
    Build a single semantic search string for Free3D Similar models: poster subject title
    plus up to 3 distinct keywords, skipping duplicates already covered by the title.
    """
    t = (title or "").strip()
    kw_parts: List[str] = []
    if keywords:
        for k in keywords:
            s = (k or "").strip()
            if not s:
                continue
            if s.lower() not in {p.lower() for p in kw_parts}:
                kw_parts.append(s)
            if len(kw_parts) >= 3:
                break

    if not t and not kw_parts:
        return None

    if not t:
        return build_free3d_query_from_keywords(kw_parts)

    title_lower = t.lower()
    title_tokens = set()
    for w in t.replace(",", " ").split():
        w = w.strip().lower()
        if len(w) > 1:
            title_tokens.add(w)

    parts: List[str] = [t]
    for k in kw_parts:
        kl = k.lower()
        if kl and kl in title_lower:
            continue
        if all((len(w) <= 2) or (w.lower() in title_tokens) for w in k.split()):
            continue
        parts.append(k)

    q = " ".join(parts).strip()
    if not q:
        return None
    if len(q) <= max_len:
        return q
    q = q[:max_len].rstrip()
    last_space = q.rfind(" ")
    if last_space > max_len // 2:
        q = q[:last_space]
    return q


async def run_task_poster_classification(task_id: str) -> None:
    """
    Download poster from ready_urls, classify, persist Task fields.
    Idempotent: skips if content_classified_at is already set.
    """
    from config import OPENAI_API_KEY

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

        llm_title: Optional[str] = None
        llm_desc: Optional[str] = None
        llm_keywords_json: Optional[str] = None
        llm_at: Optional[datetime] = None
        cv = version

        if OPENAI_API_KEY:
            try:
                llm = await asyncio.to_thread(analyze_poster_llm_metadata, image_bytes)
            except Exception as e:
                print(f"[ContentModeration] OpenAI poster metadata failed for task {task_id}: {e}")
                llm = None
            if llm:
                llm_title = llm["title"]
                llm_desc = llm["description"]
                llm_keywords_json = json.dumps(llm["keywords"])
                llm_at = datetime.utcnow()
                cv = f"{version}+{OPENAI_POSTER_MODEL}"
            else:
                cv = f"{version}:openai_error"

        task.content_rating = rating
        task.content_score = score
        task.content_classified_at = now
        task.content_classifier_version = cv
        task.poster_llm_title = llm_title
        task.poster_llm_description = llm_desc
        task.poster_llm_keywords = llm_keywords_json
        task.poster_llm_at = llm_at
        task.updated_at = now
        await db.commit()
        schedule_youtube_upload_if_eligible(task_id)
        await reserve_and_broadcast_task_done(task_id)


def schedule_task_poster_classification(task_id: str) -> None:
    """Fire-and-forget background classification (call after task is committed)."""
    asyncio.create_task(run_task_poster_classification(task_id))
