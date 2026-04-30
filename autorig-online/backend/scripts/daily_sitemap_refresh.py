#!/usr/bin/env python3
"""
Daily: rebuild local copies of sitemap index + gallery parts + SEO gate report.

Does not change how HTTP serves /sitemap.xml (still built from DB on request).
Output default: backend/data/sitemap_generated/

Cron example (03:15 UTC):
  15 3 * * * cd /root/autorig-online/backend && /root/autorig-online/venv/bin/python scripts/daily_sitemap_refresh.py
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


async def run(output_dir: str) -> None:
    from sqlalchemy import select

    from config import APP_URL
    from database import AsyncSessionLocal, Task, init_db
    from seo_gallery import (
        build_sitemap_index_xml,
        build_urlset_xml,
        gallery_seo_task_conditions,
        get_sitemap_public_entries,
        gallery_sitemap_index_part_count,
        gallery_sitemap_all_index_part_count,
        gallery_sitemap_urls_for_indexing_part,
        gallery_sitemap_urls_for_all_part,
        get_sitemap_indexable_entries,
        invalidate_sitemap_indexable_cache,
        invalidate_sitemap_public_cache,
        seo_passes_indexing_gate,
    )

    await init_db()
    invalidate_sitemap_indexable_cache()
    invalidate_sitemap_public_cache()
    base = (APP_URL or "https://autorig.online").rstrip("/")
    os.makedirs(output_dir, exist_ok=True)

    async with AsyncSessionLocal() as db:
        public_parts = await gallery_sitemap_all_index_part_count(db)
        indexing_parts = await gallery_sitemap_index_part_count(db)
        child_locs = [(f"{base}/sitemap/pages.xml", None)]
        for p in range(public_parts):
            child_locs.append((f"{base}/sitemap/gallery/part/{p}.xml", None))
        index_xml = build_sitemap_index_xml(base, child_locs)

        entries = await get_sitemap_indexable_entries(db)
        public_entries = await get_sitemap_public_entries(db)
        public_task_count = len(public_entries)

        res_all = await db.execute(
            select(Task).where(*gallery_seo_task_conditions()).order_by(Task.id)
        )
        all_gallery = list(res_all.scalars().all())

        failed_samples: list = []
        failed_total = 0
        for t in all_gallery:
            ok, issues = seo_passes_indexing_gate(t)
            if ok:
                continue
            failed_total += 1
            if len(failed_samples) < 200:
                failed_samples.append({"id": t.id, "issues": issues})

        report = {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "app_url": base,
            "public_sitemap_url_count": public_task_count,
            "public_sitemap_parts": public_parts,
            "indexing_sitemap_url_count": len(entries),
            "indexing_sitemap_parts": indexing_parts,
            "gallery_eligible_task_count": len(all_gallery),
            "failed_seo_gate_count": failed_total,
            "failed_seo_gate_sample": failed_samples,
            "failed_seo_gate_sample_truncated": failed_total > len(failed_samples),
        }

    index_path = os.path.join(output_dir, "sitemap.xml")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(index_xml)

    async with AsyncSessionLocal() as db:
        for p in range(public_parts):
            urls = await gallery_sitemap_urls_for_all_part(db, p)
            part_xml = build_urlset_xml(base, urls, changefreq="daily", priority="0.75")
            part_path = os.path.join(output_dir, f"gallery-part-{p}.xml")
            with open(part_path, "w", encoding="utf-8") as f:
                f.write(part_xml)

        for p in range(indexing_parts):
            urls = await gallery_sitemap_urls_for_indexing_part(db, p)
            part_xml = build_urlset_xml(base, urls, changefreq="daily", priority="0.75")
            part_path = os.path.join(output_dir, f"gallery-indexing-part-{p}.xml")
            with open(part_path, "w", encoding="utf-8") as f:
                f.write(part_xml)

    report_path = os.path.join(output_dir, "seo_indexing_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(
        f"[daily_sitemap_refresh] wrote {index_path}, {public_parts} public gallery parts, "
        f"{indexing_parts} indexed gallery parts, "
        f"{report_path} (indexing URLs: {len(entries)}, failed gate: {failed_total})"
    )


def main() -> None:
    default_dir = os.path.join(BACKEND, "data", "sitemap_generated")
    p = argparse.ArgumentParser(description="Write sitemap XML mirror + SEO report")
    p.add_argument(
        "--output-dir",
        "-d",
        default=default_dir,
        help=f"Directory for sitemap.xml, gallery-part-*.xml, seo_indexing_report.json (default: {default_dir})",
    )
    args = p.parse_args()
    asyncio.run(run(args.output_dir))


if __name__ == "__main__":
    main()
