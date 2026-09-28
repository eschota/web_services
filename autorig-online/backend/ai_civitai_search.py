"""Search node: popular or random Civitai pictures and clips as media inputs.

POST /api/ai/civitai-search (owner, 2026-09-28). The public Civitai images API
(https://civitai.com/api/v1/images) is read server-side: no token is needed for
public media. Every answer has at most one item per author, carries the
original-quality file URL, the author, the image page, reactions and the
generation prompt when Civitai publishes it.

Modes: images_popular (default), videos_popular, images_random, videos_random.
Random picks deterministically by `seed` from the unique-author top of the
chosen period (pool of up to 200). Answers are cached per option set for ten
minutes; `refresh: true` fetches again.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter()

API = "https://civitai.com/api/v1/images"
CACHE_SECONDS = 600
MAX_COUNT = 20
POOL_TARGET = 200          # unique-author items a random pick is drawn from
MAX_PAGES = 6              # 100 per page
PERIODS = {"24h": "Day", "day": "Day", "week": "Week", "month": "Month", "year": "Year", "all": "AllTime"}
SORTS = {"reactions": "Most Reactions", "comments": "Most Comments", "newest": "Newest"}
NSFW = {"none": "None", "soft": "Soft", "mature": "Mature", "x": "X"}
MODES = {"images_popular", "videos_popular", "images_random", "videos_random"}

_cache: Dict[Tuple, Tuple[float, List[Dict[str, Any]]]] = {}
_locks: Dict[Tuple, asyncio.Lock] = {}


class SearchRequest(BaseModel):
    mode: str = Field("images_popular", description="images_popular, videos_popular, images_random, videos_random")
    count: int = Field(3, ge=1, le=MAX_COUNT)
    period: str = Field("24h", description="24h, week, month, year, all")
    nsfw: str = Field("none", description="none, soft, mature, x")
    sort: str = Field("reactions", description="reactions, comments, newest (popular modes)")
    seed: Optional[int] = Field(None, ge=0, le=9007199254740991, description="random modes: the pick")
    refresh: bool = Field(False, description="ignore the 10-minute cache")


def _item(raw: Dict[str, Any]) -> Dict[str, Any]:
    stats = raw.get("stats") or {}
    reactions = sum(int(stats.get(k) or 0) for k in ("likeCount", "heartCount", "laughCount", "cryCount"))
    meta = raw.get("meta") or {}
    prompt = str(meta.get("prompt") or "").strip() if isinstance(meta, dict) else ""
    kind = "video" if str(raw.get("type") or "") == "video" else "image"
    author = str(raw.get("username") or "").strip()
    link = f"https://civitai.com/images/{raw.get('id')}"
    info = [f"@{author}" if author else "", f"{reactions} reactions", f"{int(stats.get('commentCount') or 0)} comments",
            link, (f"{raw.get('width')}x{raw.get('height')}" if raw.get("width") else ""),
            (str(raw.get("baseModel")) if raw.get("baseModel") else ""),
            ("Prompt: " + prompt[:1500]) if prompt else ""]
    return {
        "url_string": str(raw.get("url") or ""),
        "kind_string": kind,
        "author_string": author,
        "link_string": link,
        "post_link_string": f"https://civitai.com/posts/{raw.get('postId')}" if raw.get("postId") else "",
        "reactions_int": reactions,
        "comments_int": int(stats.get("commentCount") or 0),
        "width_int": int(raw.get("width") or 0),
        "height_int": int(raw.get("height") or 0),
        "nsfw_level_string": str(raw.get("nsfwLevel") or ""),
        "prompt_string": prompt,
        "info_string": " · ".join(part for part in info[:6] if part) + ("\n" + info[6] if info[6] else ""),
    }


async def _pool(kind: str, period: str, nsfw: str, sort: str, want: int) -> List[Dict[str, Any]]:
    """Unique-author items in Civitai's order, up to `want`."""
    params = {"limit": 100, "sort": sort, "period": period, "nsfw": nsfw}
    if kind == "video":
        params["type"] = "video"
    out: List[Dict[str, Any]] = []
    seen = set()
    cursor = None
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "AutoRig nodes search"}) as client:
        for _page in range(MAX_PAGES):
            query = dict(params, **({"cursor": cursor} if cursor else {}))
            response = await client.get(API, params=query)
            if response.status_code != 200:
                raise HTTPException(status_code=502, detail={
                    "error_string": "civitai_unavailable",
                    "message_string": f"Civitai answered HTTP {response.status_code}"})
            data = response.json() or {}
            for raw in data.get("items") or []:
                if str(raw.get("type") or "image") != kind or not raw.get("url"):
                    continue
                author = str(raw.get("username") or "").strip().lower() or f"#{raw.get('id')}"
                if author in seen:
                    continue
                seen.add(author)
                out.append(_item(raw))
                if len(out) >= want:
                    return out
            cursor = (data.get("metadata") or {}).get("nextCursor")
            if not cursor:
                break
    return out


@router.post("/api/ai/civitai-search")
async def api_civitai_search(body: SearchRequest):
    mode = str(body.mode or "images_popular").strip().lower()
    if mode not in MODES:
        raise HTTPException(status_code=400, detail={"error_string": "bad_mode",
                                                     "message_string": "mode is one of " + ", ".join(sorted(MODES))})
    kind = "video" if mode.startswith("videos") else "image"
    randomised = mode.endswith("random")
    period = PERIODS.get(str(body.period or "24h").strip().lower(), "Day")
    nsfw = NSFW.get(str(body.nsfw or "none").strip().lower(), "None")
    # A random pick is drawn from the most reacted-to of the period.
    sort = "Most Reactions" if randomised else SORTS.get(str(body.sort or "reactions").strip().lower(), "Most Reactions")
    want = POOL_TARGET if randomised else body.count
    key = (kind, period, nsfw, sort, want if randomised else MAX_COUNT)
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        cached = _cache.get(key)
        fresh = cached and not body.refresh and time.time() - cached[0] < CACHE_SECONDS
        if fresh:
            pool, fetched_at = cached[1], cached[0]
        else:
            pool = await _pool(kind, period, nsfw, sort, want if randomised else MAX_COUNT)
            fetched_at = time.time()
            _cache[key] = (fetched_at, pool)
    seed = int(body.seed or 0)
    if randomised:
        picks = list(pool)
        random.Random(seed).shuffle(picks)
        items = picks[:body.count]
    else:
        items = pool[:body.count]
    label = {"Day": "24 h", "Week": "week", "Month": "month", "Year": "year", "AllTime": "all time"}[period]
    summary = (f"{len(items)} {'clip' if kind == 'video' else 'picture'}{'' if len(items) == 1 else 's'} · "
               f"{'random of top ' + str(len(pool)) + ' · seed ' + str(seed) if randomised else sort.lower()} · "
               f"{label} · NSFW {nsfw} · one per author")
    return {
        "success_bool": True,
        "mode_string": mode,
        "kind_string": kind,
        "items_array": items,
        "count_int": len(items),
        "pool_int": len(pool),
        "seed_int": seed,
        "summary_string": summary,
        "items_text_string": "\n\n".join(f"{i + 1}. {item['info_string']}" for i, item in enumerate(items)),
        "cached_bool": bool(fresh),
        "fetched_at_unix_int": int(fetched_at),
    }
