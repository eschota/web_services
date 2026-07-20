"""
Public SEO surface for gallery tasks: /task?id={task_id} URLs and chunked sitemaps.

Sitemap rules:
- Root /sitemap.xml is a sitemap index.
- Static marketing URLs live in /sitemap/pages.xml (urlset, shipped as static/sitemap-pages.xml).
- Gallery /task?id={id} URLs for **indexing** are listed at /sitemap/gallery/part/{n}.xml (50 URLs max per part),
  including only tasks that pass :func:`seo_passes_indexing_gate` (LLM poster fields + enriched SEO check).

The broader public task pool still uses :func:`gallery_seo_task_conditions` and is exposed only through
diagnostic/all-public exports, not through the root sitemap.

A daily cron can mirror the same XML to disk; see scripts/daily_sitemap_refresh.py.
"""
from __future__ import annotations

import html
import json
import math
import time
from datetime import datetime, timezone
from typing import List, Sequence, Tuple
from urllib.parse import quote

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task

GALLERY_SEO_URLS_PER_SITEMAP = 50

# Server-side SEO theme terms merged with poster_llm_keywords on task pages.
SEO_LEXICON: Tuple[str, ...] = (
    "face rig",
    "animation",
    "retargeting",
    "ai animation",
    "ai rig",
    "animal rig",
    "humanoid rig",
)

_DEFAULT_PUBLIC_TITLE = "Rigged 3D character — AI rig & animation"
_DEFAULT_PUBLIC_DESC = (
    "Rigged 3D model with skeleton and animations. Open the viewer to preview and download "
    "GLB, FBX, OBJ, and engine packages."
)


def enrich_seo_metadata(task: Task) -> Tuple[str, str, List[str], str]:
    """
    Merge poster_llm_* with SEO_LEXICON for task pages.
    Returns: title, description, keywords (deduped, capped), visible semantic paragraph (plain text).
    """
    raw_title = (getattr(task, "poster_llm_title", None) or "").strip()
    title = raw_title if raw_title else _DEFAULT_PUBLIC_TITLE

    raw_desc = (getattr(task, "poster_llm_description", None) or "").strip()
    desc = raw_desc if raw_desc else _DEFAULT_PUBLIC_DESC
    if len(desc) < 160 and raw_desc:
        desc = f"{desc} Animation retargeting and engine-ready exports (Unity, Unreal)."
    elif len(desc) < 160:
        desc = (
            f"{_DEFAULT_PUBLIC_DESC} Ideal for AI rig workflows, humanoid or creature setups, "
            "and animation retargeting."
        )

    keywords: List[str] = []
    raw_kw = getattr(task, "poster_llm_keywords", None)
    if raw_kw:
        try:
            data = json.loads(raw_kw)
            if isinstance(data, list):
                keywords = [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            pass

    seen_lower = {_normalize_kw_for_dedupe(k) for k in keywords}
    for term in SEO_LEXICON:
        if len(keywords) >= 24:
            break
        tl = _normalize_kw_for_dedupe(term)
        if tl in seen_lower:
            continue
        keywords.append(term)
        seen_lower.add(tl)

    pk = (getattr(task, "pipeline_kind", None) or "rig").strip().lower()
    if pk not in ("rig", "convert"):
        pk = "rig"
    if pk == "convert":
        semantic = (
            "This model was processed with the convert pipeline: clean mesh workflow with rigging "
            "and animation outputs suitable for retargeting and real-time engines."
        )
    else:
        semantic = (
            "This AutoRig showcase highlights AI-assisted rigging: body and face-friendly setups, "
            "animation and retargeting options, plus exports for humanoid or stylized characters."
        )

    return title, desc[:2000], keywords[:24], semantic


def _normalize_kw_for_dedupe(s: str) -> str:
    return " ".join(s.lower().split())


def gallery_seo_indexing_sql_conditions() -> List:
    """
    SQL prefilter for indexing sitemap: gallery rules + non-empty LLM poster fields.
    Final inclusion uses :func:`seo_passes_indexing_gate` (enriched title/description/keywords).
    """
    return gallery_seo_task_conditions() + [
        Task.poster_llm_title.isnot(None),
        func.length(Task.poster_llm_title) > 2,
        Task.poster_llm_description.isnot(None),
        func.length(func.coalesce(Task.poster_llm_description, "")) >= 80,
        Task.poster_llm_keywords.isnot(None),
        func.length(Task.poster_llm_keywords) > 8,
    ]


def seo_passes_indexing_gate(task: Task) -> Tuple[bool, List[str]]:
    """
    Full SEO check before a task URL is listed in the indexing sitemap.
    Uses enrich_seo_metadata output plus poster_llm_* presence.
    """
    title, desc, kws, _sem = enrich_seo_metadata(task)
    issues: List[str] = []
    raw_t = (getattr(task, "poster_llm_title", None) or "").strip()
    if len(raw_t) < 2:
        issues.append("poster_llm_title_empty")
    if len(desc) < 95:
        issues.append("description_too_short_after_enrich")
    if len(kws) < 5:
        issues.append("keywords_too_few")
    if title.strip() == _DEFAULT_PUBLIC_TITLE.strip() and not raw_t:
        issues.append("generic_title_only")
    return (len(issues) == 0, issues)


_SITEMAP_INDEXABLE_CACHE: Tuple[float, List[Tuple[str, datetime | None]]] = (-1.0, [])
_SITEMAP_INDEXABLE_TTL_SEC = 300.0
_SITEMAP_PUBLIC_CACHE: Tuple[float, List[Tuple[str, datetime | None]]] = (-1.0, [])
_SITEMAP_PUBLIC_TTL_SEC = 300.0


def invalidate_sitemap_indexable_cache() -> None:
    """Call after bulk task updates if sitemap must refresh immediately."""
    global _SITEMAP_INDEXABLE_CACHE
    _SITEMAP_INDEXABLE_CACHE = (-1.0, [])


def invalidate_sitemap_public_cache() -> None:
    """Call after bulk task updates if public sitemap must refresh immediately."""
    global _SITEMAP_PUBLIC_CACHE
    _SITEMAP_PUBLIC_CACHE = (-1.0, [])


async def get_sitemap_public_entries(db: AsyncSession) -> List[Tuple[str, datetime | None]]:
    """All task URLs allowed in public sitemap (gallery conditions only, no SEO gate)."""
    global _SITEMAP_PUBLIC_CACHE
    now = time.monotonic()
    ts, cached = _SITEMAP_PUBLIC_CACHE
    if ts >= 0 and now - ts < _SITEMAP_PUBLIC_TTL_SEC:
        return cached

    conds = gallery_seo_task_conditions()
    result = await db.execute(
        select(Task)
        .where(*conds)
        .order_by(func.coalesce(Task.updated_at, Task.created_at).desc())
    )
    tasks = list(result.scalars().all())
    out: List[Tuple[str, datetime | None]] = []
    for t in tasks:
        lm = t.updated_at or t.created_at
        out.append((t.id, lm))
    _SITEMAP_PUBLIC_CACHE = (now, out)
    return out


async def gallery_sitemap_all_index_part_count(db: AsyncSession) -> int:
    """Number of sitemap parts for public task URLs without SEO gate."""
    entries = await get_sitemap_public_entries(db)
    n = len(entries)
    if n == 0:
        return 0
    return max(1, math.ceil(n / GALLERY_SEO_URLS_PER_SITEMAP))


async def gallery_sitemap_urls_for_all_part(
    db: AsyncSession, part: int
) -> List[Tuple[str, datetime | None]]:
    """Public sitemap chunk for /task?id={id} URLs (no SEO gate)."""
    if part < 0:
        return []
    entries = await get_sitemap_public_entries(db)
    offset = part * GALLERY_SEO_URLS_PER_SITEMAP
    slice_ = entries[offset : offset + GALLERY_SEO_URLS_PER_SITEMAP]
    return [(f"/task?id={quote(tid, safe='')}", lm) for tid, lm in slice_]


async def get_sitemap_indexable_entries(db: AsyncSession) -> List[Tuple[str, datetime | None]]:
    """All task URLs allowed in indexing sitemap, newest first (cached ~5 min per process)."""
    global _SITEMAP_INDEXABLE_CACHE
    now = time.monotonic()
    ts, cached = _SITEMAP_INDEXABLE_CACHE
    if ts >= 0 and now - ts < _SITEMAP_INDEXABLE_TTL_SEC:
        return cached

    conds = gallery_seo_indexing_sql_conditions()
    result = await db.execute(
        select(Task)
        .where(*conds)
        .order_by(func.coalesce(Task.updated_at, Task.created_at).desc())
    )
    tasks = list(result.scalars().all())
    out: List[Tuple[str, datetime | None]] = []
    for t in tasks:
        ok, _issues = seo_passes_indexing_gate(t)
        if not ok:
            continue
        lm = t.updated_at or t.created_at
        out.append((t.id, lm))
    _SITEMAP_INDEXABLE_CACHE = (now, out)
    return out


async def gallery_sitemap_index_part_count(db: AsyncSession) -> int:
    entries = await get_sitemap_indexable_entries(db)
    n = len(entries)
    if n == 0:
        return 0
    return max(1, math.ceil(n / GALLERY_SEO_URLS_PER_SITEMAP))


async def gallery_sitemap_urls_for_indexing_part(
    db: AsyncSession, part: int
) -> List[Tuple[str, datetime | None]]:
    if part < 0:
        return []
    entries = await get_sitemap_indexable_entries(db)
    offset = part * GALLERY_SEO_URLS_PER_SITEMAP
    slice_ = entries[offset : offset + GALLERY_SEO_URLS_PER_SITEMAP]
    return [(f"/task?id={quote(tid, safe='')}", lm) for tid, lm in slice_]


async def video_sitemap_entries(db: AsyncSession) -> List[dict]:
    """
    YouTube-backed video sitemap entries for Google Video indexing.
    Only includes public gallery tasks that already pass the SEO gate and have a completed YouTube upload.
    """
    conds = gallery_seo_indexing_sql_conditions() + [
        Task.youtube_upload_status == "uploaded",
        Task.youtube_video_id.isnot(None),
        func.length(func.coalesce(Task.youtube_video_id, "")) > 3,
    ]
    result = await db.execute(
        select(Task)
        .where(*conds)
        .order_by(func.coalesce(Task.youtube_uploaded_at, Task.updated_at, Task.created_at).desc())
    )
    tasks = list(result.scalars().all())
    out: List[dict] = []
    for task in tasks:
        ok, _issues = seo_passes_indexing_gate(task)
        yt_id = (getattr(task, "youtube_video_id", None) or "").strip()
        if not ok or not yt_id:
            continue
        title, desc, kws, _semantic = enrich_seo_metadata(task)
        out.append({
            "task_id": task.id,
            "youtube_video_id": yt_id,
            "title": title,
            "description": desc,
            "keywords": kws,
            "lastmod": task.updated_at or task.created_at,
            "publication_date": task.youtube_uploaded_at or task.updated_at or task.created_at,
        })
    return out


async def video_sitemap_entry_count(db: AsyncSession) -> int:
    return len(await video_sitemap_entries(db))


def _gallery_poster_sql():
    pats = ("_video_poster.jpg", "_poster.jpg", "icon.png", "Render_1_view.jpg")
    cols = (Task._ready_urls, Task._output_urls)
    return or_(*[func.instr(col, p) > 0 for col in cols for p in pats])


def gallery_seo_task_conditions() -> List:
    """SQLAlchemy WHERE fragments for indexable public showcase tasks."""
    return [
        Task.status == "done",
        Task.video_ready == True,
        _gallery_poster_sql(),
        Task.content_rating != "adult",
    ]


def xml_escape_loc(url: str) -> str:
    return html.escape(url, quote=True)


def xml_escape_text(s: str) -> str:
    return html.escape(s, quote=False)


def _w3c_datetime(dt: datetime | None) -> str:
    if dt is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_sitemap_index_xml(base_url: str, child_locs: Sequence[Tuple[str, datetime | None]]) -> str:
    """child_locs: (absolute loc URL, lastmod datetime or None)."""
    base = base_url.rstrip("/")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    now = datetime.now(timezone.utc)
    for loc, lm in child_locs:
        lines.append("  <sitemap>")
        lines.append(f"    <loc>{xml_escape_loc(loc)}</loc>")
        lines.append(f"    <lastmod>{_w3c_datetime(lm or now)}</lastmod>")
        lines.append("  </sitemap>")
    lines.append("</sitemapindex>")
    return "\n".join(lines) + "\n"
def build_urlset_xml(
    base_url: str,
    urls: Sequence[Tuple[str, datetime | None]],
    *,
    changefreq: str = "weekly",
    priority: str = "0.65",
) -> str:
    """urls: (path or full URL of public page, lastmod). Gallery task URLs use daily + higher priority."""
    base = base_url.rstrip("/")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path_or_url, lm in urls:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            loc = path_or_url
        else:
            loc = f"{base}{path_or_url if path_or_url.startswith('/') else '/' + path_or_url}"
        lines.append("  <url>")
        lines.append(f"    <loc>{xml_escape_loc(loc)}</loc>")
        lines.append(f"    <lastmod>{_w3c_datetime(lm)}</lastmod>")
        lines.append(f"    <changefreq>{xml_escape_text(changefreq)}</changefreq>")
        lines.append(f"    <priority>{priority}</priority>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines) + "\n"
def build_video_sitemap_xml(base_url: str, entries: Sequence[dict]) -> str:
    """Build Google video sitemap for /task?id={id} pages with YouTube player URLs."""
    base = base_url.rstrip("/")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">',
    ]
    for item in entries:
        task_id = str(item.get("task_id") or "").strip()
        yt_id = str(item.get("youtube_video_id") or "").strip()
        if not task_id or not yt_id:
            continue
        safe_yt_id = quote(yt_id, safe="")
        loc = f"{base}/task?id={quote(task_id, safe='')}"
        thumb_url = f"{base}/thumb/{task_id}"
        player_url = f"https://www.youtube.com/embed/{safe_yt_id}"
        title = xml_escape_text(str(item.get("title") or _DEFAULT_PUBLIC_TITLE).strip())[:100]
        description = xml_escape_text(str(item.get("description") or _DEFAULT_PUBLIC_DESC).strip())[:2048]
        publication_date = _w3c_datetime(item.get("publication_date"))
        tags = [str(k).strip() for k in (item.get("keywords") or []) if str(k).strip()][:32]

        lines.append("  <url>")
        lines.append(f"    <loc>{xml_escape_loc(loc)}</loc>")
        if item.get("lastmod"):
            lines.append(f"    <lastmod>{_w3c_datetime(item.get('lastmod'))}</lastmod>")
        lines.append("    <video:video>")
        lines.append(f"      <video:thumbnail_loc>{xml_escape_loc(thumb_url)}</video:thumbnail_loc>")
        lines.append(f"      <video:title>{title}</video:title>")
        lines.append(f"      <video:description>{description}</video:description>")
        lines.append(f"      <video:player_loc>{xml_escape_loc(player_url)}</video:player_loc>")
        lines.append(f"      <video:publication_date>{publication_date}</video:publication_date>")
        lines.append("      <video:family_friendly>yes</video:family_friendly>")
        lines.append("      <video:requires_subscription>no</video:requires_subscription>")
        for tag in tags:
            lines.append(f"      <video:tag>{xml_escape_text(tag)[:256]}</video:tag>")
        lines.append("    </video:video>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines) + "\n"
