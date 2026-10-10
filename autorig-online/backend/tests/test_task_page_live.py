import asyncio
import json
import os
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import task_page_live as live  # noqa: E402


class LiveRootsCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = pathlib.Path(self.tmp.name)
        self.overlay = base / "live" / "static"
        self.current = base / "current" / "static"
        for root in (self.overlay, self.current):
            (root / "js").mkdir(parents=True)
            (root / "css").mkdir(parents=True)
            (root / "partials").mkdir(parents=True)
        self.saved = (live.LIVE_OVERLAY, live.CURRENT_STATIC, live.BUNDLED_STATIC, live.ROLLOUT_FILE)
        live.LIVE_OVERLAY, live.CURRENT_STATIC = self.overlay, self.current
        live.BUNDLED_STATIC = base / "missing"
        live.ROLLOUT_FILE = base / "live" / "config" / "task-page.json"
        live._DIGESTS.clear()
        live._ROLLOUT_CACHE.update(key=None, value=None)

    def tearDown(self):
        live.LIVE_OVERLAY, live.CURRENT_STATIC, live.BUNDLED_STATIC, live.ROLLOUT_FILE = self.saved
        live._DIGESTS.clear()
        live._ROLLOUT_CACHE.update(key=None, value=None)
        self.tmp.cleanup()

    def write(self, root, rel, text):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(text.encode("utf-8"))
        os.replace(tmp, path)
        return path


class StampingTests(LiveRootsCase):
    def test_attributes_and_module_specifiers_get_content_stamps(self):
        import hashlib
        self.write(self.current, "js/a.js", "export const a = 1;\n")
        self.write(self.current, "css/s.css", "body{}\n")
        html = ('<script src="/static/js/a.js?v=old"></script>'
                '<link rel="stylesheet" href="/static/css/s.css">'
                "<script type=module>import x from '/static/js/a.js?v=69'; import('/static/js/a.js');</script>"
                '<img src="/static/images/x.png?v=1">'
                '<script src="/static/js/missing.js?v=7"></script>'
                '<script src="/static/js/a.js?v=1&x=2"></script>')
        out = live.stamp_static_assets(html)
        digest = hashlib.sha1(b"export const a = 1;\n").hexdigest()[:10]
        self.assertEqual(out.count(f"/static/js/a.js?v={digest}"), 3)
        self.assertIn(f'/static/css/s.css?v={hashlib.sha1(b"body{}" + bytes([10])).hexdigest()[:10]}"', out)
        self.assertIn('/static/images/x.png?v=1', out)
        self.assertIn('/static/js/missing.js?v=7', out)
        self.assertIn('/static/js/a.js?v=1&x=2', out)

    def test_overlay_wins_and_an_atomic_replace_changes_the_stamp(self):
        self.write(self.current, "js/a.js", "release\n")
        first = live.static_digest("js/a.js")
        self.write(self.overlay, "js/a.js", "live edit 1\n")
        second = live.static_digest("js/a.js")
        self.write(self.overlay, "js/a.js", "live edit 2\n")
        third = live.static_digest("js/a.js")
        self.assertEqual(len({first, second, third}), 3)
        (self.overlay / "js" / "a.js").unlink()
        self.assertEqual(live.static_digest("js/a.js"), first)

    def test_paths_cannot_escape_the_static_roots(self):
        self.assertIsNone(live.resolve_static("../secrets.txt"))
        self.assertIsNone(live.resolve_static("js/../../x.js"))
        self.assertIsNone(live.resolve_static(""))

    def test_templates_and_partials_come_from_the_live_roots(self):
        self.write(self.current, "task.html", "release page")
        self.assertEqual(live.read_static_text("task.html"), "release page")
        self.write(self.overlay, "task.html", "live page")
        self.assertEqual(live.read_static_text("task.html"), "live page")
        self.write(self.current, "partials/site-header.html", "<header>H</header>")
        self.write(self.overlay, "partials/site-footer.html", "<footer>F</footer>")
        self.write(self.current, "partials/site-free3d-search.html", "<form>S</form>")
        html = live.inject_layout('<body data-layout-free3d-ribbon="1"><div id="site-header"></div>'
                                  '<div id="site-footer"></div></body>')
        self.assertIn('<div id="site-header" data-server-rendered="1">\n<header>H</header>\n<form>S</form>', html)
        self.assertIn('<div id="site-footer" data-server-rendered="1">\n<footer>F</footer>\n</div>', html)

    def test_v3_description_repeats_the_escaped_meta_description(self):
        html = ('<meta name="description" content="A &quot;knight&quot; &amp; horse">'
                '<p><!-- TASK_V3_DESCRIPTION --></p>')
        self.assertIn('<p>A &quot;knight&quot; &amp; horse</p>', live.fill_v3_description(html))
        self.assertEqual(live.fill_v3_description("<p><!-- TASK_V3_DESCRIPTION --></p>"), "<p></p>")


class RolloutTests(LiveRootsCase):
    def test_missing_or_broken_rollout_means_admin_only(self):
        self.assertEqual(live.rollout()["mode"], "admin")
        live.ROLLOUT_FILE.parent.mkdir(parents=True)
        live.ROLLOUT_FILE.write_text("{broken", encoding="utf-8")
        self.assertEqual(live.rollout()["mode"], "admin")

    def test_rollout_is_reread_when_the_file_changes(self):
        live.ROLLOUT_FILE.parent.mkdir(parents=True)
        self.write(live.ROLLOUT_FILE.parent, "task-page.json", json.dumps(
            {"mode": "new", "new_since": "2026-10-10T12:00:00Z", "preview_keys": ["short", "k" * 32]}))
        config = live.rollout()
        self.assertEqual(config["mode"], "new")
        self.assertEqual(config["new_since"], datetime(2026, 10, 10, 12, tzinfo=timezone.utc))
        self.assertEqual(config["preview_keys"], ("k" * 32,))
        self.write(live.ROLLOUT_FILE.parent, "task-page.json", json.dumps({"mode": "all"}))
        self.assertEqual(live.rollout()["mode"], "all")
        self.write(live.ROLLOUT_FILE.parent, "task-page.json", json.dumps({"mode": "everyone"}))
        self.assertEqual(live.rollout()["mode"], "admin")

    def test_switch_preference(self):
        self.assertEqual(live.switch_preference({"v3": "1"}, {}), (True, "1"))
        self.assertEqual(live.switch_preference({"v3": "0"}, {live.SWITCH_COOKIE: "1"}), (False, "0"))
        self.assertEqual(live.switch_preference({}, {live.SWITCH_COOKIE: "1"}), (True, None))
        self.assertEqual(live.switch_preference({}, {live.SWITCH_COOKIE: "0"}), (False, None))
        self.assertEqual(live.switch_preference({"v3": "maybe"}, {}), (None, None))

    def test_choose_v3_matrix(self):
        since = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        old, new = since - timedelta(days=1), since + timedelta(minutes=1)

        def pick(mode, pref=None, admin=False, preview=False, created=old, webapp=False, allowed=False, v3=False):
            return live.choose_v3(mode=mode, preference=pref, is_admin=admin, preview=preview, created_at=created,
                                  new_since=since, webapp=webapp, webapp_allowed=allowed, v3_task=v3)

        self.assertFalse(pick("off", True, admin=True))
        self.assertFalse(pick("admin"))
        self.assertFalse(pick("admin", True))
        self.assertTrue(pick("admin", True, admin=True))
        self.assertTrue(pick("admin", True, preview=True))
        self.assertFalse(pick("admin", False, admin=True))
        self.assertFalse(pick("new", created=old))
        self.assertTrue(pick("new", created=new))
        self.assertTrue(pick("new", created=new.replace(tzinfo=None)))
        self.assertTrue(pick("new", True, created=old))
        self.assertFalse(pick("new", False, created=new))
        self.assertFalse(pick("new", created=new, webapp=True))
        self.assertTrue(pick("new", created=new, webapp=True, allowed=True))
        self.assertTrue(pick("new", True, admin=True, created=old, webapp=True))
        self.assertTrue(pick("all"))
        self.assertFalse(pick("all", False))
        self.assertTrue(pick("admin", v3=True))
        self.assertTrue(pick("admin", v3=True, webapp=True))
        self.assertFalse(pick("admin", False, v3=True))
        self.assertFalse(pick("off", v3=True))


class TemplateSelectionTests(LiveRootsCase):
    TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"

    class FakeResult:
        def __init__(self, row):
            self.row = row

        def first(self):
            return self.row

    class FakeDb:
        def __init__(self, created):
            self.created = created

        async def execute(self, statement):
            return TemplateSelectionTests.FakeResult(
                None if self.created is None else (TemplateSelectionTests.TASK, self.created))

    def request(self, query=None, cookies=None):
        return SimpleNamespace(query_params=query or {}, cookies=cookies or {}, state=SimpleNamespace())

    def select(self, request, user=None, created=datetime(2026, 1, 1), task_id=None):
        from sqlalchemy import Column, DateTime, String
        from sqlalchemy.orm import declarative_base

        base = declarative_base()

        class Task(base):
            __tablename__ = "tasks"
            id = Column(String, primary_key=True)
            created_at = Column(DateTime)

        return asyncio.run(live.task_template(
            request, task_id or self.TASK, user, self.FakeDb(created), task_model=Task,
            is_admin_email=lambda email: email == "owner@example.com"))

    def setUp(self):
        super().setUp()
        self.write(self.current, "task.html", "CLASSIC")
        self.write(self.current, "task-v3.html", "V3")

    def test_admin_switch_and_cookie(self):
        owner = SimpleNamespace(email="owner@example.com")
        req = self.request({"v3": "1"})
        self.assertEqual(self.select(req, owner), "V3")
        self.assertEqual(req.state.task_page_switch_cookie, "1")
        self.assertEqual(req.state.task_page_template, "task-v3.html")
        self.assertEqual(self.select(self.request({"v3": "1"}), SimpleNamespace(email="x@example.com")), "CLASSIC")
        self.assertEqual(self.select(self.request({"v3": "1"}), None), "CLASSIC")
        self.assertEqual(self.select(self.request({"v3": "1", "classic": "1"}), owner), "CLASSIC")
        self.assertEqual(self.select(self.request({"v3": "1"}), owner, created=None), "CLASSIC")
        self.assertEqual(self.select(self.request({"v3": "1"}), owner, task_id="not-a-task"), "CLASSIC")

    def test_preview_cookie_needs_a_configured_key(self):
        key = "p" * 32
        self.assertEqual(self.select(self.request({"v3": "1"}, {live.PREVIEW_COOKIE: key})), "CLASSIC")
        live.ROLLOUT_FILE.parent.mkdir(parents=True)
        self.write(live.ROLLOUT_FILE.parent, "task-page.json", json.dumps({"mode": "admin", "preview_keys": [key]}))
        self.assertEqual(self.select(self.request({"v3": "1"}, {live.PREVIEW_COOKIE: key})), "V3")
        self.assertEqual(self.select(self.request({"v3": "1"}, {live.PREVIEW_COOKIE: "q" * 32})), "CLASSIC")

    def test_missing_v3_template_or_broken_db_falls_back_to_classic(self):
        (self.current / "task-v3.html").unlink()
        owner = SimpleNamespace(email="owner@example.com")
        self.assertEqual(self.select(self.request({"v3": "1"}), owner), "CLASSIC")

        class BrokenDb:
            async def execute(self, statement):
                raise RuntimeError("db down")

        self.write(self.current, "task-v3.html", "V3")
        req = self.request({"v3": "1"})
        from sqlalchemy import Column, DateTime, String
        from sqlalchemy.orm import declarative_base
        base = declarative_base()

        class Task(base):
            __tablename__ = "tasks"
            id = Column(String, primary_key=True)
            created_at = Column(DateTime)

        page = asyncio.run(live.task_template(req, self.TASK, owner, BrokenDb(), task_model=Task,
                                              is_admin_email=lambda email: True))
        self.assertEqual(page, "CLASSIC")

    def test_switch_cookie_and_page_header_are_applied(self):
        from fastapi.responses import HTMLResponse

        req = self.request({"v3": "0"})
        self.select(req, SimpleNamespace(email="owner@example.com"))
        response = live.apply_switch_cookie(HTMLResponse("x"), req)
        cookie = response.headers.get("set-cookie", "")
        self.assertIn(f"{live.SWITCH_COOKIE}=0", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertEqual(response.headers["x-autorig-task-page"], "classic")


if __name__ == "__main__":
    unittest.main()
