"""Downloads · V3: subscription gate (fail closed), formats, clip GLB cut, export queue and worker contract.

Run from backend/: python -m unittest tests.test_task_downloads_v3
"""
import asyncio
import hashlib
import json
import os
import struct
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# user_language / config need nothing from the database for these checks
os.environ.setdefault("AUTORIG_STATIC_DIR", str(Path(__file__).resolve().parents[2] / "static"))

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import task_downloads_v3 as D  # noqa: E402

ADMIN = "boss@example.com"
TASK = "11111111-2222-4333-8444-555555555555"
RUN = "0123456789abcdef0123"


def glb(anims):
    doc = {"asset": {"version": "2.0"}, "skins": [{"joints": [0]}], "nodes": [{"name": "Hips"}],
           "animations": [{"name": n, "channels": [], "samplers": []} for n in anims],
           "buffers": [{"byteLength": 4}]}
    body = json.dumps(doc).encode()
    body += b" " * ((4 - len(body) % 4) % 4)
    binc = b"\0\0\0\0"
    return (b"glTF" + struct.pack("<II", 2, 12 + 8 + len(body) + 8 + len(binc)) + struct.pack("<II", len(body), 0x4E4F534A)
            + body + struct.pack("<II", len(binc), 0x004E4942) + binc)


class User:
    def __init__(self, email, status="none", days=0):
        self.email = email
        self.autorig_subscription_status = status
        self.autorig_subscription_period_end = datetime.utcnow() + timedelta(days=days) if days else None


class Task:
    def __init__(self, owner_type="user", owner_id="owner@example.com", pipeline_kind="v3"):
        self.id = TASK
        self.owner_type, self.owner_id, self.pipeline_kind = owner_type, owner_id, pipeline_kind
        self.is_public = True
        self.poster_llm_title = "Knight Lady"
        self.viewer_settings = json.dumps({"v3": {"session": {"mt_run_id": RUN}}})


class Req:
    def __init__(self, cookies=None, query=None):
        self.cookies = cookies or {}
        self.query_params = query or {}


def is_admin(email):
    return email == ADMIN


class AccessTests(unittest.TestCase):
    def decide(self, user, task=None, cookies=None, anon=None):
        return D.download_access(task or Task(), user, Req(cookies), is_admin_email=is_admin, anon_id=anon)

    def test_anonymous_must_sign_in(self):
        a = self.decide(None, Task(owner_type="anon", owner_id="anon-1"), anon="anon-1")
        self.assertEqual((a["allowed"], a["status"], a["reason"]), (False, 401, "signin_required"))

    def test_owner_without_subscription_gets_402(self):
        a = self.decide(User("owner@example.com"))
        self.assertEqual((a["allowed"], a["status"]), (False, 402))

    def test_expired_subscription_is_not_active(self):
        u = User("owner@example.com", "active", days=0)
        u.autorig_subscription_period_end = datetime.utcnow() - timedelta(hours=1)
        self.assertFalse(self.decide(u)["allowed"])

    def test_owner_with_subscription_allowed(self):
        a = self.decide(User("owner@example.com", "active", days=10))
        self.assertEqual((a["allowed"], a["reason"]), (True, "subscription"))

    def test_subscriber_who_is_not_owner_is_refused(self):
        a = self.decide(User("other@example.com", "active", days=10))
        self.assertEqual((a["allowed"], a["status"]), (False, 403))

    def test_admin_always_allowed_and_view_as_free(self):
        self.assertTrue(self.decide(User(ADMIN))["allowed"])
        a = self.decide(User(ADMIN, "active", days=10), cookies={D.VIEW_AS_COOKIE: "free"})
        self.assertEqual((a["allowed"], a["status"], a["view_as_free"]), (False, 402, True))

    def test_view_as_query_for_admins_only(self):
        a = D.download_access(Task(), User(ADMIN), Req(query={"as": "free"}), is_admin_email=is_admin, anon_id=None)
        self.assertEqual((a["allowed"], a["status"]), (False, 402))
        a = D.download_access(Task(), User("owner@example.com", "active", days=3), Req(query={"as": "free"}),
                              is_admin_email=is_admin, anon_id=None)
        self.assertTrue(a["allowed"])

    def test_view_as_cookie_ignored_for_non_admins(self):
        a = self.decide(User("owner@example.com", "active", days=10), cookies={D.VIEW_AS_COOKIE: "free"})
        self.assertTrue(a["allowed"])
        self.assertFalse(a["view_as_free"])


class FileTests(unittest.TestCase):
    def test_parse_fmt(self):
        clips = ["Idle", "Walking"]
        self.assertEqual(D.parse_fmt("fbx", clips)["by"], "worker")
        self.assertEqual(D.parse_fmt("clip-1.fbx", clips)["clip"], "Walking")
        self.assertIsNone(D.parse_fmt("clip-2.glb", clips))
        self.assertIsNone(D.parse_fmt("../x", clips))

    def test_split_clip_keeps_a_valid_glb_with_one_animation(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / "a.glb", Path(tmp) / "b.glb"
            src.write_bytes(glb(["rig_check", "Idle", "Walking"]))
            D.split_clip(src, "Walking", dst)
            doc = D._glb_json(dst)
            self.assertEqual([a["name"] for a in doc["animations"]], ["Walking"])


class RouterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        D.MT_ROOT = root / "mt"
        D.QUEUE_DIR = root / "queue"
        D.WORKER_KEYS = root / "keys.json"
        D.WORKERS_SEEN.clear()
        rig = D.MT_ROOT / "runs" / RUN / "rig"
        rig.mkdir(parents=True)
        (rig / "rigged.glb").write_bytes(glb(["rig_check", "Idle", "Walking"]))
        self.cache = root / "glb_cache"
        self.cache.mkdir()
        D.WORKER_KEYS.write_text(json.dumps({"keys": [{"name": "f7", "sha256": hashlib.sha256(b"k3y").hexdigest()}]}))
        self.task = Task()
        self.user = None
        self._orig_select = D.select

    def tearDown(self):
        D.select = self._orig_select
        self.tmp.cleanup()

    def patch_load(self):
        # replace the database lookup inside the router closure: select(...) is never reached with this shim
        task = self.task

        class Result:
            def scalar_one_or_none(self):
                return task

        class DB:
            async def execute(self, *_):
                return Result()

        async def get_db():
            yield DB()

        app = FastAPI()
        D.select = lambda *_a, **_k: types.SimpleNamespace(where=lambda *_: None)
        app.include_router(D.build_task_downloads_v3_router(
            get_db=get_db, get_current_user=self._user_dep, task_model=types.SimpleNamespace(id=None),
            is_admin_email=is_admin, glb_cache_dir=self.cache, effective_anon_id=lambda r: None))
        self.client = TestClient(app)

    async def _user_dep(self):
        return self.user

    def test_non_subscriber_never_starts_an_export(self):
        self.patch_load()
        self.user = User("owner@example.com")
        r = self.client.post(f"/api/task/{TASK}/downloads-v3/fbx")
        self.assertEqual(r.status_code, 402)
        detail = r.json()["detail"]
        self.assertEqual(detail["error_string"], "download_subscription")
        self.assertIn("checkout_url", detail["plan"])
        self.assertFalse(D.QUEUE_DIR.exists() and any(D.QUEUE_DIR.iterdir()))
        self.assertFalse(any(self.cache.iterdir()))
        self.assertEqual(self.client.get(f"/api/task/{TASK}/downloads-v3/glb/file?v=0123456789abcdef").status_code, 402)

    def test_anonymous_gets_401(self):
        self.patch_load()
        r = self.client.post(f"/api/task/{TASK}/downloads-v3/glb")
        self.assertEqual(r.status_code, 401)

    def test_subscriber_glb_clip_and_fbx_through_a_worker(self):
        self.patch_load()
        self.user = User("owner@example.com", "active", days=5)
        m = self.client.get(f"/api/task/{TASK}/downloads-v3").json()
        self.assertTrue(m["available"])
        self.assertEqual(m["rig"]["clips"], ["Idle", "Walking"])
        r = self.client.post(f"/api/task/{TASK}/downloads-v3/glb").json()
        self.assertEqual(r["state"], "ready")
        r = self.client.post(f"/api/task/{TASK}/downloads-v3/clip-1.glb").json()
        self.assertEqual(r["state"], "ready")
        f = self.client.get(r["url"])
        self.assertEqual(f.status_code, 200)
        self.assertIn("X-Accel-Redirect", f.headers)
        self.assertIn("knight-lady-walking.glb", f.headers["content-disposition"])
        # FBX: queued once, deduplicated, taken by the worker, uploaded, ready
        a = self.client.post(f"/api/task/{TASK}/downloads-v3/fbx").json()
        b = self.client.post(f"/api/task/{TASK}/downloads-v3/fbx").json()
        self.assertEqual((a["state"], b["state"]), ("queued", "queued"))
        self.assertEqual(len(list(D.QUEUE_DIR.iterdir())), 1)
        self.assertEqual(self.client.get("/api/v3-export/worker/next").status_code, 401)
        auth = {"Authorization": "Bearer k3y"}
        job = self.client.get("/api/v3-export/worker/next", headers=auth).json()
        self.assertEqual(self.client.get("/api/v3-export/worker/next", headers=auth).status_code, 204)
        lease = {**auth, "X-Lease": job["lease"]}
        self.assertEqual(self.client.get(job["source"], headers=lease).status_code, 200)
        self.client.post(f"/api/v3-export/worker/jobs/{job['id']}/progress", headers=lease,
                         json={"progress": 0.5, "stage": "fbx"})
        s = self.client.get(f"/api/task/{TASK}/downloads-v3/fbx").json()
        self.assertEqual((s["state"], s["progress"], s["stage"]), ("running", 0.5, "fbx"))
        bad = self.client.put(f"/api/v3-export/worker/jobs/{job['id']}/result", headers=lease, content=b"nope")
        self.assertEqual(bad.status_code, 422)
        ok = self.client.put(f"/api/v3-export/worker/jobs/{job['id']}/result", headers=lease,
                             content=b"Kaydara FBX Binary  \x00" + b"x" * 64)
        self.assertEqual(ok.status_code, 200)
        s = self.client.get(f"/api/task/{TASK}/downloads-v3/fbx").json()
        self.assertEqual(s["state"], "ready")
        self.assertTrue(s["url"].endswith(f"?v={s['sha']}"))

    def test_classic_task_points_at_its_own_files(self):
        self.task = Task(pipeline_kind="rig")
        self.patch_load()
        self.user = User("owner@example.com", "active", days=5)
        m = self.client.get(f"/api/task/{TASK}/downloads-v3").json()
        self.assertFalse(m["available"])
        self.assertEqual(m["reason"], "legacy")
        self.assertIn("classic=1", m["legacy_url"])


if __name__ == "__main__":
    unittest.main()
