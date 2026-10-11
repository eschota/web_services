"""SEO · social previews: task page head, static page completion, cards (seo_social.py)."""
import io
import json
import re
import struct
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import seo_social as ss  # noqa: E402

TID = "7833a8e8-c1c8-4c5e-b539-505162d4c3e9"
LONG_DESC = ("This model represents a whimsical fantasy character designed for vibrant settings. The character "
             "wears a playful outfit adorned with stars and a distinctive hat, embodying a lighthearted style.")


def task(**kw):
    base = dict(id=TID, status="done", is_public=True, content_rating="safe", video_ready=True,
                poster_llm_title="Fantasy character in whimsical attire with stars",
                poster_llm_description=LONG_DESC,
                poster_llm_keywords=json.dumps(["fantasy", "character", "stars", "hat", "cartoon"]),
                pipeline_kind="rig", created_at=datetime(2026, 10, 9, 20, 0), updated_at=datetime(2026, 10, 10, 1, 0))
    base.update(kw)
    return SimpleNamespace(**base)


def meta(head, key):
    m = re.search(r'<meta (?:property|name)="' + re.escape(key) + r'" content="([^"]*)"', head)
    return m.group(1) if m else None


class TaskHeadTest(unittest.TestCase):
    def test_done_task_is_indexable_with_card_and_valid_json_ld(self):
        out = ss.task_head(task(), hidden=False, has_video=True, has_poster=True, poster_sig="6ac985cd",
                           video_dims=(540, 960))
        self.assertTrue(out["indexable"])
        self.assertEqual(out["robots"], ss.INDEX_ROBOTS)
        self.assertNotIn("AutoRig task 7833", out["title"])
        self.assertLessEqual(len(out["title"]), ss.TITLE_MAX)
        head = out["head_html"]
        self.assertEqual(meta(head, "og:image"), f"https://autorig.online/og/task/{TID}.jpg?v=6ac985cd.3")
        self.assertEqual(meta(head, "og:image:width"), "1200")
        self.assertEqual(meta(head, "og:image:height"), "630")
        self.assertEqual(meta(head, "twitter:card"), "summary_large_image")
        self.assertEqual(meta(head, "og:video:width"), "540")
        self.assertEqual(meta(head, "og:video:height"), "960")
        self.assertIsNone(meta(head, "twitter:player"))
        docs = [json.loads(x) for x in re.findall(r'<script type="application/ld\+json">(.*?)</script>', head)]
        types = [d["@type"] for d in docs]
        self.assertEqual(types, ["CreativeWork", "BreadcrumbList"])
        self.assertEqual(docs[0]["associatedMedia"]["@type"], "VideoObject")
        self.assertIn("uploadDate", docs[0]["associatedMedia"])
        self.assertIn("stars", docs[0]["keywords"])

    def test_failed_task_is_noindex_and_never_says_rigging_failed(self):
        out = ss.task_head(task(status="error", poster_llm_title=None), hidden=False, has_video=False,
                           has_poster=False)
        self.assertFalse(out["indexable"])
        self.assertEqual(out["robots"], "noindex, follow")
        self.assertNotIn("Failed", out["title"])
        self.assertNotIn("❌", out["title"] + out["head_html"])
        self.assertEqual(meta(out["head_html"], "og:image"), "https://autorig.online/og/site.jpg")
        self.assertNotIn("ld+json", out["head_html"])

    def test_untitled_v3_task_gets_its_own_card_and_vision_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "d3ef5ccb8a344ef81a14" / "analysis"
            run.mkdir(parents=True)
            (run / "category.json").write_text(json.dumps({"category": "hands", "what": "low-poly human hand"}))
            old, ss.MT_RUNS_DIR = ss.MT_RUNS_DIR, Path(tmp)
            try:
                t = task(status="needs_review", poster_llm_title=None, poster_llm_description=None,
                         poster_llm_keywords=None, video_ready=False,
                         ready_urls=["https://autorig.online/api/mt/files/d3ef5ccb8a344ef81a14/rig/rigged.glb"])
                out = ss.task_head(t, hidden=False, has_video=False, has_poster=True, poster_sig="abc")
            finally:
                ss.MT_RUNS_DIR = old
        self.assertFalse(out["indexable"])
        self.assertEqual(meta(out["head_html"], "og:title"), "Low-poly human hand")
        self.assertIn(f"/og/task/{TID}.jpg?v=abc", meta(out["head_html"], "og:image"))
        self.assertNotIn("3D model task", out["title"])

    def test_processing_and_needs_review_are_noindex(self):
        for status in ("processing", "created", "needs_review"):
            out = ss.task_head(task(status=status), hidden=False, has_video=False, has_poster=True)
            self.assertFalse(out["indexable"], status)
            self.assertNotIn("⏳", out["title"])

    def test_untitled_done_task_is_noindex_but_keeps_its_picture(self):
        out = ss.task_head(task(poster_llm_title=None, poster_llm_description=None, poster_llm_keywords=None),
                           hidden=False, has_video=False, has_poster=True)
        self.assertFalse(out["indexable"])
        self.assertIn("/og/task/", meta(out["head_html"], "og:image"))

    def test_adult_task_on_main_host_is_hidden(self):
        out = ss.task_head(task(content_rating="adult"), hidden=True, has_video=True, has_poster=True,
                           poster_sig="1")
        self.assertEqual(out["robots"], "noindex, nofollow")
        self.assertNotIn("Fantasy", out["title"] + out["head_html"])
        self.assertEqual(meta(out["head_html"], "og:image"), "https://autorig.online/og/site.jpg")
        self.assertIsNone(meta(out["head_html"], "og:video"))

    def test_private_task_is_noindex(self):
        out = ss.task_head(task(is_public=False), hidden=False, has_video=True, has_poster=True)
        self.assertFalse(out["indexable"])
        self.assertIsNone(meta(out["head_html"], "og:video"))

    def test_json_ld_cannot_close_the_script(self):
        out = ss.task_head(task(poster_llm_description=LONG_DESC + " </script><b>x</b>"), hidden=False,
                           has_video=False, has_poster=True)
        self.assertEqual(out["head_html"].count("</script>"), 2)


PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>Auto Rig Gallery — Real Results</title>
    <meta name="description" content="Browse real rigging results.">
    <meta property="og:title" content="AutoRig Gallery - Real Results">
    <meta property="og:url" content="https://autorig.online/gallery">
    <meta property="og:image" content="https://autorig.online/static/images/og-image.png">
    <link rel="canonical" href="https://autorig.online/ru/gallery">
    <link rel="alternate" hreflang="en" href="https://autorig.online/gallery">
    <link rel="alternate" hreflang="ru" href="https://autorig.online/ru/gallery">
    <link rel="alternate" hreflang="fa" href="https://autorig.online/fa/gallery">
    <meta property="og:locale" content="ru_RU">
</head>
<body><h1>Gallery</h1></body>
</html>"""


class CompleteSocialHeadTest(unittest.TestCase):
    def test_fills_missing_tags_and_swaps_legacy_picture(self):
        out = ss.complete_social_head(PAGE)
        self.assertEqual(meta(out, "og:image"), "https://autorig.online/og/site.jpg")
        self.assertEqual(meta(out, "og:image:width"), "1200")
        self.assertEqual(meta(out, "og:url"), "https://autorig.online/ru/gallery")
        self.assertEqual(meta(out, "twitter:card"), "summary_large_image")
        self.assertEqual(meta(out, "og:site_name"), "AutoRig.online")
        self.assertEqual(meta(out, "og:type"), "website")
        self.assertIn('<meta property="og:locale:alternate" content="en_US">', out)
        self.assertIn('<meta property="og:locale:alternate" content="fa_IR">', out)
        self.assertNotIn('og:locale:alternate" content="ru_RU"', out)
        self.assertEqual(out.count('property="og:locale"'), 1)
        self.assertIn("<body><h1>Gallery</h1></body>", out)

    def test_keeps_a_page_own_choices(self):
        page = PAGE.replace("/static/images/og-image.png", "/static/videos/home/poster.jpg").replace(
            "</head>", '<meta name="twitter:card" content="player">\n</head>')
        out = ss.complete_social_head(page)
        self.assertEqual(meta(out, "og:image"), "https://autorig.online/static/videos/home/poster.jpg")
        self.assertEqual(meta(out, "twitter:card"), "player")

    def test_private_noindex_tool_page_untouched(self):
        page = '<html lang="ru"><head><title>DEV</title><meta name="robots" content="noindex, nofollow"></head><body></body></html>'
        self.assertEqual(ss.complete_social_head(page), page)

    def test_idempotent(self):
        once = ss.complete_social_head(PAGE)
        self.assertEqual(ss.complete_social_head(once), once)


class CrawlerAndArticleTest(unittest.TestCase):
    def test_crawlers_are_recognised(self):
        for ua in ("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                   "TelegramBot (like TwitterBot)", "facebookexternalhit/1.1", "Twitterbot/1.0",
                   "Mozilla/5.0 (compatible; bingbot/2.0)", "Mozilla/5.0 (compatible; YandexBot/3.0)", ""):
            self.assertTrue(ss.is_crawler(ua), ua)
        self.assertFalse(ss.is_crawler("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                        "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"))

    def test_article_family_gets_hreflang(self):
        page = PAGE.replace('<link rel="alternate" hreflang="en" href="https://autorig.online/gallery">\n', "") \
            .replace('<link rel="alternate" hreflang="ru" href="https://autorig.online/ru/gallery">\n', "") \
            .replace('<link rel="alternate" hreflang="fa" href="https://autorig.online/fa/gallery">\n', "") \
            .replace("https://autorig.online/ru/gallery", "https://autorig.online/glb-vs-fbx-ru")
        out = ss.complete_social_head(page)
        for hl, url in (("en", "glb-vs-fbx"), ("ru", "glb-vs-fbx-ru"), ("zh", "glb-vs-fbx-zh"),
                        ("hi", "glb-vs-fbx-hi"), ("x-default", "glb-vs-fbx")):
            self.assertIn(f'<link rel="alternate" hreflang="{hl}" href="https://autorig.online/{url}">', out)
        self.assertIn('og:locale:alternate" content="zh_CN"', out)
        self.assertEqual(ss.complete_social_head(out), out)
        self.assertEqual(ss.article_alternates("https://autorig.online/gallery"), [])


class MediaTest(unittest.TestCase):
    def test_mp4_dimensions_reads_tkhd(self):
        def box(kind, payload):
            return struct.pack(">I4s", 8 + len(payload), kind) + payload

        tkhd = bytes([0, 0, 0, 3]) + b"\0" * 72 + struct.pack(">II", 540 << 16, 960 << 16)
        data = box(b"ftyp", b"isom" + b"\0" * 4) + box(b"mdat", b"\0" * 32) + box(b"moov", box(b"trak", box(b"tkhd", tkhd)))
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "v.mp4"
            p.write_bytes(data)
            self.assertEqual(ss.mp4_dimensions(p), (540, 960))
            p.write_bytes(b"not an mp4")
            self.assertIsNone(ss.mp4_dimensions(p))

    def test_cards_are_1200x630(self):
        try:
            from PIL import Image
        except ImportError:  # pragma: no cover
            self.skipTest("Pillow missing")
        buf = io.BytesIO()
        Image.new("RGB", (720, 1280), (120, 160, 220)).save(buf, "JPEG")
        card = Image.open(io.BytesIO(ss.compose_task_card(buf.getvalue())))
        self.assertEqual(card.size, (1200, 630))
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for i in range(5):
                p = Path(tmp) / f"{i}.jpg"
                Image.new("RGB", (720, 1280), (40 * i, 90, 160)).save(p, "JPEG")
                paths.append(p)
            site = Image.open(io.BytesIO(ss.compose_site_card(paths)))
            self.assertEqual(site.size, (1200, 630))
            self.assertEqual(Image.open(io.BytesIO(ss.compose_site_card([]))).size, (1200, 630))

    def test_gallery_cards_are_plain_links(self):
        items = [SimpleNamespace(task_id=TID, thumbnail_url=f"/thumb/{TID}?v=1"),
                 SimpleNamespace(task_id="../etc", thumbnail_url=None)]
        out = ss.gallery_cards_html(items, {TID: 'Hero "Star"'})
        self.assertEqual(out.count("<a "), 1)
        self.assertIn(f'href="/task?id={TID}"', out)
        self.assertIn('alt="Hero &quot;Star&quot;"', out)


class RoutesTest(unittest.TestCase):
    def test_card_routes_resolve_their_parameters(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()

        async def fake_db():
            yield None

        ss.install(app, get_db=fake_db, resolve_poster_url_for_task=lambda task: None, static_dir=Path("."))
        client = TestClient(app)
        self.assertEqual(client.get("/og/task/not-a-task.jpg?v=1").status_code, 404)
        self.assertEqual(client.head("/og/task/not-a-task.jpg").status_code, 404)


if __name__ == "__main__":
    unittest.main()
