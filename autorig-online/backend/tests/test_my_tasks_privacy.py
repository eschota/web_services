"""Private models (owner 2026-10-17): who may see a private task, on every path (task_privacy.py)."""
import asyncio
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import task_privacy as tp

OWNER = "owner@example.com"
OTHER = "other@example.com"
ADMIN = "eschota@gmail.com"
T_USER = "11111111-2222-4333-8444-555555555555"
T_ANON = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
T_PUB = "99999999-8888-4777-8666-555555555555"
UPLOAD = "0f0f0f0f-1111-4222-8333-444444444444"
RUN = "0123456789abcdef0123"
ANON = "anon-owner-cookie"


class PrivacyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        db = root / "autorig.db"
        con = sqlite3.connect(db)
        con.executescript("""
            CREATE TABLE tasks (id TEXT PRIMARY KEY, owner_type TEXT, owner_id TEXT, input_url TEXT,
                                viewer_settings TEXT, is_public INTEGER, updated_at TEXT);
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, autorig_subscription_status TEXT,
                                autorig_subscription_period_end TEXT);
            CREATE TABLE sessions (token TEXT, user_id INTEGER, expires_at TEXT);
            CREATE TABLE api_keys (key_prefix TEXT, key_hash TEXT, revoked_at TEXT, user_id INTEGER, anon_id TEXT);
        """)
        future = (datetime.utcnow() + timedelta(days=20)).isoformat(sep=" ")
        con.execute("INSERT INTO users VALUES (1, ?, 'active', ?)", (OWNER, future))
        con.execute("INSERT INTO users VALUES (2, ?, NULL, NULL)", (OTHER,))
        con.execute("INSERT INTO users VALUES (3, ?, NULL, NULL)", (ADMIN,))
        for tok, uid in (("s-owner", 1), ("s-other", 2), ("s-admin", 3)):
            con.execute("INSERT INTO sessions VALUES (?, ?, ?)", (tok, uid, future))
        con.execute("INSERT INTO sessions VALUES ('s-expired', 1, '2000-01-01 00:00:00')")
        key = "ar_abc123_secretpart"
        con.execute("INSERT INTO api_keys VALUES ('abc123', ?, NULL, NULL, ?)",
                    (hashlib.sha256(key.encode()).hexdigest(), ANON))
        con.execute("INSERT INTO tasks VALUES (?, 'user', ?, ?, ?, 0, NULL)",
                    (T_USER, OWNER, f"https://autorig.online/u/{UPLOAD}/m.glb",
                     json.dumps({"v3": {"session": {"mt_run_id": RUN}}})))
        con.execute("INSERT INTO tasks VALUES (?, 'anon', ?, NULL, NULL, 0, NULL)", (T_ANON, ANON))
        con.execute("INSERT INTO tasks VALUES (?, 'user', ?, NULL, NULL, 1, NULL)", (T_PUB, OTHER))
        con.commit()
        con.close()
        runs = root / "runs"
        (runs / "fedcba9876543210fedc").mkdir(parents=True)
        (runs / "fedcba9876543210fedc" / "run.json").write_text(json.dumps({"request": {"task_id": T_ANON}}))
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db.as_posix()}"
        cls._runs_dir = tp.RUNS_DIR
        tp.RUNS_DIR = runs
        tp.INDEX = tp.PrivacyIndex()
        tp._ident_cache.clear()

    @classmethod
    def tearDownClass(cls):
        tp.RUNS_DIR = cls._runs_dir
        cls.tmp.cleanup()

    def allowed(self, uri, cookie="", headers=None, ip="203.0.113.7"):
        return tp.decide(uri, cookie, headers or {}, ip)[0]

    def test_public_task_open_to_everyone(self):
        self.assertTrue(self.allowed(f"/api/task/{T_PUB}"))
        self.assertTrue(self.allowed(f"/task?id={T_PUB}"))

    def test_private_user_task_every_path(self):
        for uri in (f"/api/task/{T_USER}", f"/task?id={T_USER}", f"/pw/{T_USER}.webp", f"/u/{UPLOAD}/m.glb",
                    f"/api/mt/runs/{RUN}/rig", f"/api/mt/files/{RUN}/rig/model.glb", f"/og/task/{T_USER.upper()}.jpg"):
            self.assertFalse(self.allowed(uri), uri)                                  # anon
            self.assertFalse(self.allowed(uri, "session=s-other"), uri)               # another user (IDOR)
            self.assertFalse(self.allowed(uri, "session=s-expired"), uri)             # expired session
            self.assertTrue(self.allowed(uri, "session=s-owner"), uri)                # owner
            self.assertTrue(self.allowed(uri, "session=s-admin"), uri)                # admin

    def test_private_anon_task_by_cookie_and_api_key(self):
        uri = f"/api/task/{T_ANON}/model.glb"
        self.assertFalse(self.allowed(uri, "anon_id=someone-else"))
        self.assertTrue(self.allowed(uri, f"anon_id={ANON}"))
        self.assertTrue(self.allowed(uri, headers={"authorization": "Bearer ar_abc123_secretpart"}))
        self.assertFalse(self.allowed(uri, headers={"authorization": "Bearer ar_abc123_wrong"}))
        # its MT run, found through run.json (not in viewer_settings)
        self.assertFalse(self.allowed("/api/mt/runs/fedcba9876543210fedc"))
        self.assertTrue(self.allowed("/api/mt/runs/fedcba9876543210fedc", f"anon_id={ANON}"))

    def test_our_infrastructure_is_never_blocked(self):
        self.assertTrue(self.allowed(f"/u/{UPLOAD}/m.glb", ip="37.187.57.177"))
        self.assertTrue(self.allowed(f"/u/{UPLOAD}/m.glb", ip="127.0.0.1"))
        self.assertFalse(self.allowed(f"/u/{UPLOAD}/m.glb", ip="8.8.8.8"))

    def test_hex_inside_longer_hex_is_not_a_run(self):
        sha = "ab" + RUN + "cd" * 21
        self.assertTrue(self.allowed(f"/api/mt/cas/{sha}/x.glb"))

    def test_toggle_reaches_the_index(self):
        con = sqlite3.connect(os.environ["DATABASE_URL"].split(":///", 1)[1])
        con.execute("UPDATE tasks SET is_public = 0 WHERE id = ?", (T_PUB,))
        con.commit()
        tp.INDEX.invalidate()
        try:
            self.assertFalse(self.allowed(f"/api/task/{T_PUB}"))
        finally:
            con.execute("UPDATE tasks SET is_public = 1 WHERE id = ?", (T_PUB,))
            con.commit()
            con.close()
            tp.INDEX.invalidate()
        self.assertTrue(self.allowed(f"/api/task/{T_PUB}"))

    def test_initial_visibility_follows_subscription(self):
        self.assertFalse(tp.initial_is_public("user", OWNER))      # active subscriber: private by default
        self.assertTrue(tp.initial_is_public("user", OTHER))       # free: public
        self.assertTrue(tp.initial_is_public("anon", ANON))
        self.assertTrue(tp.initial_is_public("user", "nobody@example.com"))

    def test_middleware_answers_404_without_calling_the_route(self):
        called = []

        async def app(scope, receive, send):
            called.append(scope["path"])
            await send({"type": "http.response.start", "status": 200, "headers": [(b"cache-control", b"public")]})
            await send({"type": "http.response.body", "body": b"model"})

        mw = tp.PrivacyMiddleware(app)

        def run(path, query=b"", cookie=b"", method="GET"):
            sent = []

            async def send(m):
                sent.append(m)

            async def receive():
                return {"type": "http.request"}
            headers = [(b"x-real-ip", b"203.0.113.9")] + ([(b"cookie", cookie)] if cookie else [])
            scope = {"type": "http", "method": method, "path": path, "query_string": query, "headers": headers,
                     "client": ("127.0.0.1", 1)}
            asyncio.run(mw(scope, receive, send))
            return sent[0]["status"], dict(sent[0]["headers"])

        status, _ = run(f"/api/task/{T_USER}/model.glb")
        self.assertEqual(status, 404)
        self.assertEqual(called, [])
        status, _ = run("/task", query=f"id={T_USER}".encode())
        self.assertEqual(status, 404)
        status, hdrs = run(f"/api/task/{T_USER}", cookie=b"session=s-owner")
        self.assertEqual(status, 200)
        self.assertEqual(hdrs[b"cache-control"], b"private, no-store")      # never cached publicly
        self.assertIn(b"noindex", hdrs[b"x-robots-tag"])
        status, _ = run(f"/api/task/{T_PUB}")
        self.assertEqual(status, 200)
        status, _ = run(f"/api/task/{T_USER}/visibility", method="POST")   # writes check ownership themselves
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
