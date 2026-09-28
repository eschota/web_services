"""Search node: popular or random Civitai pictures and clips as media inputs.

Since 2026-09-28 (owner: "results differ from civitai.red") the node reads the
same feed the civitai.red Images page reads: tRPC image.getInfinite with the
page's own parameters (period + periodMode "published", sort, types,
browsingLevel bitmask, the anonymous default excludedTagIds, disablePoi,
withMeta false). The order is then the site's order. NSFW None is read
anonymously (the site's anonymous view); Soft / Mature / X need a signed-in
view, so they use the server's CIVITAI_API_TOKEN (account NoDeadLine) — that
account's hidden tags and users then apply, exactly as on the site for it.

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

API = "https://civitai.com/api/v1/images"   # kept for reference; the feed below is used
SITE = "https://civitai.red"
FEED = SITE + "/api/trpc/image.getInfinite"
CDN = "https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA"
# The site's browsing-level bits: PG 1, PG-13 2, R 4, X 8, XXX 16. A level
# shows everything up to and including it, like the site's checkboxes.
LEVELS = {"none": 1, "soft": 1 | 2, "mature": 1 | 2 | 4, "x": 1 | 2 | 4 | 8 | 16}
# What civitai.red sends for an anonymous visitor (captured 2026-09-28).
EXCLUDED_TAG_IDS = [5161, 5162, 5188, 5249, 130818, 130820, 133182, 130401, 110980]
CLIENT_VERSION = "5.1.142"
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
    unique_authors: bool = Field(True, description="at most one item per author (off = exactly the site's list)")


def _hydrate(flat: List[Any]) -> Any:
    """tRPC's flattened answer (devalue): every value is an index into the list."""
    memo: Dict[int, Any] = {}

    def h(i: int) -> Any:
        if i == -1:
            return None
        if i in memo:
            return memo[i]
        v = flat[i]
        if isinstance(v, list):
            if v and isinstance(v[0], str) and v[0] in ("Date", "Set", "Map", "BigInt", "RegExp", "URL"):
                out: Any = v[1] if len(v) > 1 else None
            else:
                out = []
                memo[i] = out
                out.extend(h(x) for x in v)
        elif isinstance(v, dict):
            out = {}
            memo[i] = out
            for k, x in v.items():
                out[k] = h(x)
        else:
            out = v
        memo[i] = out
        return out
    return h(0)


def _site_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """A site feed item in the shape of the public API item."""
    stats = raw.get("stats") or {}
    kind = str(raw.get("type") or "image")
    uuid = str(raw.get("url") or "")
    name = str(raw.get("name") or "").strip()
    if not name or "." not in name or "/" in name:
        name = uuid + (".mp4" if kind == "video" else ".jpeg")
    return {
        "id": raw.get("id"), "type": kind, "username": (raw.get("user") or {}).get("username"),
        "url": f"{CDN}/{uuid}/original=true/{name}" if uuid else "",
        "width": raw.get("width"), "height": raw.get("height"), "postId": raw.get("postId"),
        "nsfwLevel": {1: "None", 2: "Soft", 4: "Mature", 8: "X", 16: "XXX"}.get(raw.get("nsfwLevel"), raw.get("nsfwLevel")),
        "baseModel": raw.get("baseModel"), "meta": raw.get("meta") or {},
        "stats": {"likeCount": stats.get("likeCountAllTime"), "heartCount": stats.get("heartCountAllTime"),
                  "laughCount": stats.get("laughCountAllTime"), "cryCount": stats.get("cryCountAllTime"),
                  "commentCount": stats.get("commentCountAllTime")},
    }


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


async def _pool(kind: str, period: str, level: int, sort: str, want: int, unique: bool) -> List[Dict[str, Any]]:
    """The civitai.red feed in the site's order, up to `want` items."""
    import json as _json
    import os
    token = str(os.getenv("CIVITAI_API_TOKEN") or "").strip()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/140.0 Safari/537.36",
               "Accept": "*/*", "Referer": SITE + "/images", "x-client": "web", "x-client-version": CLIENT_VERSION}
    if level > 1:
        if not token:
            raise HTTPException(status_code=503, detail={
                "error_string": "civitai_token_missing",
                "message_string": "NSFW levels above None need the server's Civitai token"})
        headers["Authorization"] = "Bearer " + token
    out: List[Dict[str, Any]] = []
    seen = set()
    cursor = None
    async with httpx.AsyncClient(timeout=40.0) as client:
        for _page in range(MAX_PAGES):
            query = {"period": period, "periodMode": "published", "sort": sort, "types": [kind],
                     "withMeta": False, "browsingLevel": level, "include": ["cosmetics"],
                     "excludedTagIds": EXCLUDED_TAG_IDS, "disablePoi": True, "disableMinor": False,
                     "direction": "forward"}
            if cursor:
                query["cursor"] = cursor
            headers["x-client-date"] = str(int(time.time() * 1000))
            response = await client.get(FEED, params={"input": _json.dumps({"json": query}, separators=(",", ":"))},
                                        headers=headers)
            if response.status_code != 200:
                raise HTTPException(status_code=502, detail={
                    "error_string": "civitai_unavailable",
                    "message_string": f"civitai.red answered HTTP {response.status_code}"})
            data = ((response.json() or {}).get("result") or {}).get("data")
            if isinstance(data, dict) and "json" in data:
                data = data["json"]
            if isinstance(data, str):
                data = _hydrate(_json.loads(data))
            data = data or {}
            for raw in data.get("items") or []:
                if str(raw.get("type") or "image") != kind or not raw.get("url"):
                    continue
                item = _site_item(raw)
                author = str(item.get("username") or "").strip().lower() or f"#{item.get('id')}"
                if unique and author in seen:
                    continue
                seen.add(author)
                out.append(_item(item))
                if len(out) >= want:
                    return out
            cursor = data.get("nextCursor")
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
    level = LEVELS.get(nsfw.lower(), 1)
    unique = bool(body.unique_authors)
    # A random pick is drawn from the most reacted-to of the period.
    sort = "Most Reactions" if randomised else SORTS.get(str(body.sort or "reactions").strip().lower(), "Most Reactions")
    want = POOL_TARGET if randomised else body.count
    key = (kind, period, level, sort, unique, want if randomised else MAX_COUNT)
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        cached = _cache.get(key)
        fresh = cached and not body.refresh and time.time() - cached[0] < CACHE_SECONDS
        if fresh:
            pool, fetched_at = cached[1], cached[0]
        else:
            pool = await _pool(kind, period, level, sort, want if randomised else MAX_COUNT, unique)
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
               f"{label} · NSFW up to {nsfw} · " + ("one per author" if unique else "as listed") +
               " · matches civitai.red" + ("" if level == 1 else " (signed-in view)"))
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
        "source_string": "civitai.red image feed (tRPC image.getInfinite)",
        "signed_in_bool": level > 1,
        "fetched_at_unix_int": int(fetched_at),
    }
