"""
Public SEO surface for gallery tasks: lightweight /m/{task_id} pages and chunked sitemaps.

Sitemap rules:
- Root /sitemap.xml is a sitemap index.
- Static marketing URLs live in /sitemap/pages.xml (urlset, shipped as static/sitemap-pages.xml).
- Gallery /m/{id} URLs for **indexing** are listed at /sitemap/gallery/part/{n}.xml (50 URLs max per part),
  including only tasks that pass :func:`seo_passes_indexing_gate` (LLM poster fields + enriched SEO check).

Public /m/ pages still use :func:`gallery_seo_task_conditions` (broader than indexing sitemap).

A daily cron can mirror the same XML to disk; see scripts/daily_sitemap_refresh.py.
"""
from __future__ import annotations

import html
import json
import math
import time
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple
from urllib.parse import quote

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task

GALLERY_SEO_URLS_PER_SITEMAP = 50

# Server-side SEO theme terms merged with poster_llm_keywords on /m/{id} pages.
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
    Merge poster_llm_* with SEO_LEXICON for /m pages.
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
    Full SEO check before a /m/{id} URL is listed in the indexing sitemap.
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
    """All /m/{id} URLs allowed in public sitemap (gallery conditions only, no SEO gate)."""
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
    """Number of sitemap parts for public /m/{id} task URLs without SEO gate."""
    entries = await get_sitemap_public_entries(db)
    n = len(entries)
    if n == 0:
        return 0
    return max(1, math.ceil(n / GALLERY_SEO_URLS_PER_SITEMAP))


async def gallery_sitemap_urls_for_all_part(
    db: AsyncSession, part: int
) -> List[Tuple[str, datetime | None]]:
    """Public sitemap chunk for /m/{id} task URLs (no SEO gate)."""
    if part < 0:
        return []
    entries = await get_sitemap_public_entries(db)
    offset = part * GALLERY_SEO_URLS_PER_SITEMAP
    slice_ = entries[offset : offset + GALLERY_SEO_URLS_PER_SITEMAP]
    return [(f"/m/{tid}", lm) for tid, lm in slice_]


async def get_sitemap_indexable_entries(db: AsyncSession) -> List[Tuple[str, datetime | None]]:
    """All /m/{id} URLs allowed in indexing sitemap, newest first (cached ~5 min per process)."""
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
    return [(f"/m/{tid}", lm) for tid, lm in slice_]


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
    """urls: (path or full URL of public page, lastmod). Gallery /m URLs use daily + higher priority."""
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
    """Build Google video sitemap for /m/{id} landing pages with YouTube player URLs."""
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
        loc = f"{base}/m/{task_id}"
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


def build_public_model_page_html(
    base_url: str,
    task_id: str,
    title: str,
    description: str,
    keywords: List[str],
    *,
    semantic_section: str = "",
    youtube_video_id: Optional[str] = None,
    youtube_video_uploaded: bool = False,
    last_modified: Optional[datetime] = None,
) -> str:
    """Minimal indexable HTML: meta + OG + JSON-LD; full viewer stays on /task."""
    base = base_url.rstrip("/")
    canonical = f"{base}/m/{task_id}"
    viewer_url = f"{base}/task?id={task_id}"
    thumb_url = f"{base}/thumb/{task_id}"
    safe_title = xml_escape_text(title)[:200]
    safe_desc = xml_escape_text(description)[:500]
    kw_csv = ", ".join(
        html.escape(str(k).strip()[:80], quote=True) for k in keywords[:24] if str(k).strip()
    )

    json_ld: dict = {
        "@context": "https://schema.org",
        "@type": "CreativeWork",
        "name": title[:200],
        "description": description[:2000],
        "url": canonical,
        "image": thumb_url,
        "mainEntityOfPage": canonical,
    }
    if keywords:
        json_ld["keywords"] = ", ".join(keywords[:24])
    json_ld_str = json.dumps(json_ld, ensure_ascii=False)
    keywords_meta = f'  <meta name="keywords" content="{kw_csv}">\n' if kw_csv else ""

    about_block = ""
    if semantic_section.strip():
        st = xml_escape_text(semantic_section.strip())[:1200]
        about_block = (
            f'  <section class="seo-about" aria-labelledby="about-heading">\n'
            f'    <h2 id="about-heading">About this rig</h2>\n'
            f"    <p>{st}</p>\n"
            f"  </section>\n"
        )

    youtube_meta = ""
    youtube_json_ld = ""
    youtube_block = ""
    yt_id = (youtube_video_id or "").strip()
    if youtube_video_uploaded and yt_id:
        safe_yt_id = quote(yt_id, safe="")
        yt_watch_url = f"https://www.youtube.com/watch?v={safe_yt_id}"
        yt_embed_url = f"https://www.youtube.com/embed/{safe_yt_id}"
        youtube_meta = f"""  <meta property="og:video" content="{xml_escape_loc(yt_watch_url)}">
  <meta property="og:video:secure_url" content="{xml_escape_loc(yt_watch_url)}">
  <meta property="og:video:type" content="text/html">
  <meta property="og:video:width" content="1280">
  <meta property="og:video:height" content="720">
  <meta name="twitter:player" content="{xml_escape_loc(yt_embed_url)}">
  <meta name="twitter:player:width" content="1280">
  <meta name="twitter:player:height" content="720">
"""
        video_json_ld: dict = {
            "@context": "https://schema.org",
            "@type": "VideoObject",
            "name": title[:200],
            "description": description[:500],
            "thumbnailUrl": thumb_url,
            "url": yt_watch_url,
            "embedUrl": yt_embed_url,
            "uploadDate": _w3c_datetime(last_modified or datetime.utcnow().replace(tzinfo=timezone.utc)),
            "mainEntityOfPage": canonical,
        }
        if keywords:
            video_json_ld["keywords"] = ", ".join(keywords[:24])
        youtube_json_ld = f'\n  <script type="application/ld+json">{json.dumps(video_json_ld, ensure_ascii=False)}</script>'
        youtube_block = f"""  <section class="video-preview" aria-labelledby="video-heading">
    <h2 id="video-heading">Video preview</h2>
    <iframe src="{xml_escape_loc(yt_embed_url)}" title="{safe_title} video preview" loading="lazy"
      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
      allowfullscreen></iframe>
    <p><a href="{xml_escape_loc(yt_watch_url)}" rel="noopener noreferrer">Watch on YouTube</a></p>
  </section>
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="robots" content="index,follow">
  <link rel="canonical" href="{xml_escape_loc(canonical)}">
  <title>{safe_title} | AutoRig.online</title>
  <meta name="description" content="{safe_desc}">
{keywords_meta}
  <meta property="og:type" content="website">
  <meta property="og:url" content="{xml_escape_loc(canonical)}">
  <meta property="og:title" content="{safe_title} | AutoRig.online">
  <meta property="og:description" content="{safe_desc}">
  <meta property="og:image" content="{xml_escape_loc(thumb_url)}">
  <meta property="og:site_name" content="AutoRig.online">
  <meta name="twitter:card" content="{'player' if (youtube_video_uploaded and yt_id) else 'summary_large_image'}">
{youtube_meta}  <meta name="twitter:title" content="{safe_title} | AutoRig.online">
  <meta name="twitter:description" content="{safe_desc}">
  <meta name="twitter:image" content="{xml_escape_loc(thumb_url)}">
  <script type="application/ld+json">{json_ld_str}</script>{youtube_json_ld}
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 42rem; margin: 2rem auto; padding: 0 1rem;
      background: #0f111a; color: #e8e8ef; line-height: 1.5; }}
    img {{ max-width: 100%; border-radius: 12px; display: block; }}
    a.btn {{ display: inline-block; margin-top: 1rem; padding: 0.55rem 1.1rem; border-radius: 999px;
      background: #6366f1; color: #fff; text-decoration: none; font-weight: 600; }}
    a.btn:hover {{ filter: brightness(1.08); }}
    .muted {{ color: #9ca3af; font-size: 0.9rem; margin-top: 1.5rem; }}
    .seo-about {{ margin-top: 1.75rem; padding-top: 1.25rem; border-top: 1px solid rgba(255,255,255,0.1); }}
    .seo-about h2 {{ font-size: 1rem; font-weight: 600; margin: 0 0 0.5rem; color: #c4c9d8; }}
    .seo-about p {{ margin: 0; color: #b8c0d0; font-size: 0.95rem; }}
    .video-preview {{ margin-top: 1.75rem; padding-top: 1.25rem; border-top: 1px solid rgba(255,255,255,0.1); }}
    .video-preview h2 {{ font-size: 1rem; font-weight: 600; margin: 0 0 0.75rem; color: #c4c9d8; }}
    .video-preview iframe {{ display: block; width: 100%; aspect-ratio: 16 / 9; height: auto; border: 0; border-radius: 12px; background: #050711; }}
    .video-preview p {{ margin: 0.65rem 0 0; font-size: 0.9rem; }}
  </style>
</head>
<body>
  <h1>{safe_title}</h1>
  <p>{safe_desc}</p>
  <p><img src="{xml_escape_loc(thumb_url)}" width="640" height="360" alt="{safe_title}" loading="lazy"></p>
{youtube_block}{about_block}  <p><a class="btn" href="{xml_escape_loc(viewer_url)}">Open 3D viewer &amp; downloads</a></p>
  <p class="muted">AutoRig Online — automatic character rigging (GLB, FBX, Unity, Unreal).</p>
</body>
</html>
"""


async def load_task_for_public_model_page(db: AsyncSession, task_id: str) -> Task | None:
    conds = gallery_seo_task_conditions() + [Task.id == task_id]
    result = await db.execute(select(Task).where(*conds).limit(1))
    return result.scalar_one_or_none()
