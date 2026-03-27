"""
Public SEO surface for gallery tasks: lightweight /m/{task_id} pages and chunked sitemaps.

Sitemap rules:
- Root /sitemap.xml is a sitemap index.
- Static marketing URLs live in /sitemap/pages.xml (urlset, shipped as static/sitemap-pages.xml).
- Gallery tasks are listed in urlsets at /sitemap/gallery/{YYYY-MM-DD}/{part}.xml
  with at most GALLERY_SEO_URLS_PER_SITEMAP URLs per file (50). Multiple parts per day if needed.

Data source: rows in ``tasks`` that match the same visibility rules as the public gallery,
plus exclusion of ``content_rating == adult`` for safer indexing.
"""
from __future__ import annotations

import html
import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import List, Sequence, Tuple

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Task

GALLERY_SEO_URLS_PER_SITEMAP = 50


def _utc_day_bounds_naive(day_str: str) -> Tuple[datetime, datetime]:
    """Inclusive start, exclusive end in naive UTC (matches Task.created_at storage)."""
    start = datetime.strptime(day_str, "%Y-%m-%d")
    end = start + timedelta(days=1)
    return start, end


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


def build_urlset_xml(base_url: str, urls: Sequence[Tuple[str, datetime | None]]) -> str:
    """urls: (path or full URL of public page, lastmod)."""
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
        lines.append("    <changefreq>weekly</changefreq>")
        lines.append("    <priority>0.65</priority>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines) + "\n"


async def gallery_sitemap_day_parts(db: AsyncSession) -> List[Tuple[str, int]]:
    """
    Returns [(date_str 'YYYY-MM-DD', num_parts), ...] sorted by date descending.
    num_parts = ceil(count / GALLERY_SEO_URLS_PER_SITEMAP).
    """
    day_col = func.date(Task.created_at).label("day")
    conds = gallery_seo_task_conditions()
    q = (
        select(day_col, func.count(Task.id))
        .where(*conds)
        .group_by(day_col)
        .order_by(day_col.desc())
    )
    result = await db.execute(q)
    rows = result.all()
    out: List[Tuple[str, int]] = []

    for day_val, cnt in rows:
        if day_val is None:
            continue
        day_str = str(day_val)
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", day_str):
            day_str = str(day_val)[:10]
        n = int(cnt or 0)
        parts = max(1, math.ceil(n / GALLERY_SEO_URLS_PER_SITEMAP)) if n > 0 else 0
        if parts:
            out.append((day_str, parts))
    return out


async def gallery_sitemap_urls_for_chunk(
    db: AsyncSession, day_str: str, part: int
) -> List[Tuple[str, datetime | None]]:
    """Returns [(path /m/{id}, updated_at), ...] for one sitemap chunk."""
    if part < 0:
        return []
    offset = part * GALLERY_SEO_URLS_PER_SITEMAP
    day_start, day_end = _utc_day_bounds_naive(day_str)
    conds = gallery_seo_task_conditions() + [
        Task.created_at >= day_start,
        Task.created_at < day_end,
    ]
    q = (
        select(Task.id, Task.updated_at)
        .where(*conds)
        .order_by(Task.created_at.asc())
        .offset(offset)
        .limit(GALLERY_SEO_URLS_PER_SITEMAP)
    )
    result = await db.execute(q)
    rows = result.all()
    return [(f"/m/{tid}", u_at) for tid, u_at in rows]


def build_public_model_page_html(
    base_url: str,
    task_id: str,
    title: str,
    description: str,
    keywords: List[str],
) -> str:
    """Minimal indexable HTML: meta + OG + JSON-LD; full viewer stays on /task."""
    base = base_url.rstrip("/")
    canonical = f"{base}/m/{task_id}"
    viewer_url = f"{base}/task?id={task_id}"
    thumb_url = f"{base}/api/thumb/{task_id}"
    safe_title = xml_escape_text(title)[:200]
    safe_desc = xml_escape_text(description)[:500]
    kw_csv = ", ".join(
        html.escape(str(k).strip()[:80], quote=True) for k in keywords[:24] if str(k).strip()
    )

    json_ld = {
        "@context": "https://schema.org",
        "@type": "CreativeWork",
        "name": title[:200],
        "description": description[:2000],
        "url": canonical,
        "image": thumb_url,
        "mainEntityOfPage": canonical,
    }
    json_ld_str = json.dumps(json_ld, ensure_ascii=False)
    keywords_meta = f'  <meta name="keywords" content="{kw_csv}">\n' if kw_csv else ""

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
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{safe_title} | AutoRig.online">
  <meta name="twitter:description" content="{safe_desc}">
  <meta name="twitter:image" content="{xml_escape_loc(thumb_url)}">
  <script type="application/ld+json">{json_ld_str}</script>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 42rem; margin: 2rem auto; padding: 0 1rem;
      background: #0f111a; color: #e8e8ef; line-height: 1.5; }}
    img {{ max-width: 100%; border-radius: 12px; display: block; }}
    a.btn {{ display: inline-block; margin-top: 1rem; padding: 0.55rem 1.1rem; border-radius: 999px;
      background: #6366f1; color: #fff; text-decoration: none; font-weight: 600; }}
    a.btn:hover {{ filter: brightness(1.08); }}
    .muted {{ color: #9ca3af; font-size: 0.9rem; margin-top: 1.5rem; }}
  </style>
</head>
<body>
  <h1>{safe_title}</h1>
  <p>{safe_desc}</p>
  <p><img src="{xml_escape_loc(thumb_url)}" width="640" height="360" alt="{safe_title}" loading="lazy"></p>
  <p><a class="btn" href="{xml_escape_loc(viewer_url)}">Open 3D viewer &amp; downloads</a></p>
  <p class="muted">AutoRig Online — automatic character rigging (GLB, FBX, Unity, Unreal).</p>
</body>
</html>
"""


async def load_task_for_public_model_page(db: AsyncSession, task_id: str) -> Task | None:
    conds = gallery_seo_task_conditions() + [Task.id == task_id]
    result = await db.execute(select(Task).where(*conds).limit(1))
    return result.scalar_one_or_none()
