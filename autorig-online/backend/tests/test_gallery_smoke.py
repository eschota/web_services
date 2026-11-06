"""Gallery smoke (release gate): /api/gallery answers with items on a real schema, V3 only with a poster."""
import asyncio
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402
from starlette.requests import Request  # noqa: E402

import main  # noqa: E402
import seo_social  # noqa: E402
from database import Base, Task  # noqa: E402

V2 = "11111111-2222-3333-4444-555555555555"
V3_POSTER = "22222222-3333-4444-5555-666666666666"
V3_NONE = "33333333-4444-5555-6666-777777777777"
POSTER_URL = f"https://worker.invalid/converter/glb/x/x_video_poster.jpg"


class GallerySmokeTest(unittest.TestCase):
    def test_gallery_lists_items_and_hides_v3_without_poster(self):
        async def run():
            engine = create_async_engine("sqlite+aiosqlite://")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with AsyncSession(engine) as db:
                now = datetime.utcnow()
                for tid, kind, status, video in ((V2, "rig", "done", True), (V3_POSTER, "v3", "needs_review", False),
                                                 (V3_NONE, "v3", "done", False)):
                    t = Task(id=tid, owner_type="anon", owner_id="a", status=status, pipeline_kind=kind,
                             video_ready=video, is_public=True, content_rating="safe", created_at=now, updated_at=now)
                    t.ready_urls = [POSTER_URL] if kind == "rig" else []
                    t.output_urls = []
                    db.add(t)
                await db.commit()
                request = Request({"type": "http", "method": "GET", "path": "/api/gallery", "headers": [],
                                   "query_string": b"", "state": {}})
                return await main.api_get_gallery(request=request, page=1, per_page=12, sort="date", rig_type="all",
                                                  author=None, user=None, db=db)

        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, f"{V3_POSTER}.jpg").write_bytes(b"x" * 9000)
            old = seo_social.V3_POSTER_DIR
            seo_social.V3_POSTER_DIR = Path(tmp)
            seo_social._POSTER_IDS.update(at=-1.0)
            try:
                page = asyncio.run(run())
            finally:
                seo_social.V3_POSTER_DIR = old
                seo_social._POSTER_IDS.update(at=-1.0)
        ids = [it.task_id for it in page.items]
        self.assertGreaterEqual(len(ids), 1)
        self.assertEqual(ids[0], V3_POSTER)          # V3 first
        self.assertIn(V2, ids)
        self.assertNotIn(V3_NONE, ids)               # no poster yet: not listed


if __name__ == "__main__":
    unittest.main()
