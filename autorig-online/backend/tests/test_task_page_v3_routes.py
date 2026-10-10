import asyncio
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import Boolean, Column, DateTime, Integer, String  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.orm import declarative_base  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

import task_page_v3_routes as routes  # noqa: E402

Base = declarative_base()


class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True)
    status = Column(String, default="created")
    progress = Column(Integer, default=0)
    owner_type = Column(String, default="anon")
    owner_id = Column(String, default="anon-owner")
    is_public = Column(Boolean, default=True)
    created_at = Column(DateTime)
    pipeline_kind = Column(String, default="rig")
    queue_class = Column(String, default="interactive")
    poster_llm_title = Column(String)
    collection_member_title = Column(String)
    viewer_settings = Column(String)
    error_message = Column(String)


DONE = "11111111-1111-4111-8111-111111111111"
STATIC = "22222222-2222-4222-8222-222222222222"
PRIVATE = "33333333-3333-4333-8333-333333333333"
Q1 = "44444444-4444-4444-8444-444444444444"
Q2 = "55555555-5555-4555-8555-555555555555"
Q3 = "66666666-6666-4666-8666-666666666666"
BOUND = "77777777-7777-4777-8777-777777777777"
RUN = "abcdef0123456789abcd"


def glb(size=64):
    body = b"glTF" + (2).to_bytes(4, "little") + size.to_bytes(4, "little")
    return body + b"\0" * (size - len(body))


class TaskPageV3RoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        base = pathlib.Path(cls.tmp.name)
        cls.cache = base / "glb_cache"
        cls.cache.mkdir()
        (cls.cache / f"{DONE}_animations.glb").write_bytes(glb(128))
        (cls.cache / f"{DONE}_prepared.glb").write_bytes(glb(96))
        (cls.cache / f"{STATIC}_prepared.glb").write_bytes(glb(80))
        (cls.cache / f"{PRIVATE}_animations.glb").write_bytes(glb(72))
        (cls.cache / f"{Q1}_animations.glb").write_bytes(b"not a glb at all")
        cls.warmups = []
        cls.saved_warm = routes.start_warmup
        routes.start_warmup = lambda task_id, kind: cls.warmups.append((task_id, kind)) or "started"

        cls.engine = create_async_engine(f"sqlite+aiosqlite:///{base / 'db.sqlite'}", poolclass=NullPool)
        cls.sessions = async_sessionmaker(cls.engine, expire_on_commit=False)
        now = datetime(2026, 10, 10, 10, 0, 0)

        async def seed():
            async with cls.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with cls.sessions() as db:
                db.add_all([
                    Task(id=DONE, status="done", progress=100, created_at=now, poster_llm_title="Knight"),
                    Task(id=STATIC, status="processing", progress=40, created_at=now),
                    Task(id=PRIVATE, status="done", progress=100, created_at=now, is_public=False,
                         owner_type="anon", owner_id="secret-anon"),
                    Task(id=Q1, status="created", created_at=now - timedelta(minutes=3)),
                    Task(id=Q2, status="created", created_at=now - timedelta(minutes=2)),
                    Task(id=Q3, status="created", created_at=now - timedelta(minutes=9), queue_class="background"),
                    Task(id=BOUND, status="processing", progress=10, created_at=now, pipeline_kind="v3",
                         viewer_settings=json.dumps({"v3": {"state": "running", "stage": "rig", "progress": 0.6,
                                                            "session": {"mt_run_id": RUN, "viewer_url":
                                                                        f"/api/mt/unity/test/index.html?run={RUN}"}}})),
                ])
                await db.commit()

        asyncio.run(seed())

        async def get_db():
            async with cls.sessions() as db:
                yield db

        cls.user = None

        async def get_user():
            return cls.user

        app = FastAPI()
        app.include_router(routes.build_task_page_v3_router(
            get_db=get_db, get_current_user=get_user, task_model=Task,
            is_admin_email=lambda email: email == "owner@example.com", glb_cache_dir=cls.cache))
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        routes.start_warmup = cls.saved_warm
        asyncio.run(cls.engine.dispose())
        cls.tmp.cleanup()

    def setUp(self):
        type(self).user = None
        self.warmups.clear()
        self.client.cookies.clear()

    def view(self, task_id):
        response = self.client.get(f"/api/task/{task_id}/v3-view")
        return response

    def test_done_task_opens_its_rigged_classic_model_in_the_unity_viewer(self):
        response = self.view(DONE)
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["cache-control"])
        state = response.json()
        self.assertEqual(state["schema"], "autorig.task-page-v3/1")
        self.assertEqual((state["status"], state["progress"], state["title"]), ("done", 1.0, "Knight"))
        viewer = state["viewer"]
        self.assertEqual(viewer["kind"], "legacy")
        self.assertEqual(viewer["page"], "/api/mt/unity/test/index.html")
        self.assertEqual(viewer["api_path"], f"/api/task-viewer/{DONE}")
        self.assertEqual(viewer["params"], {"run": "task", "agent": "0"})
        self.assertTrue(viewer["rigged"])
        self.assertEqual(state["model"]["state"], "ready")
        self.assertEqual(self.warmups, [])

        rig = self.client.get(f"/api/task-viewer/{DONE}/files/task/rig/rig.json")
        self.assertEqual(rig.status_code, 200)
        self.assertEqual(rig.json()["source"], "classic-task")
        self.assertTrue(rig.json()["built_at"].endswith("Z"))
        for method in ("GET", "HEAD"):
            glb_reply = self.client.request(method, f"/api/task-viewer/{DONE}/files/task/rig/rigged.glb")
            self.assertEqual(glb_reply.status_code, 200)
            self.assertEqual(glb_reply.headers["x-accel-redirect"], f"/_autorig_glb_cache/{DONE}_animations.glb")
        card = self.client.get(f"/api/task-viewer/{DONE}/files/task/card/card.json").json()
        self.assertEqual(card["name"], "Knight")
        self.assertEqual(self.client.get(f"/api/task-viewer/{DONE}/files/other/rig/rig.json").status_code, 404)
        self.assertEqual(self.client.get(f"/api/task-viewer/{DONE}/files/task/run.json").status_code, 404)

    def test_processing_task_shows_its_static_model_first(self):
        state = self.view(STATIC).json()
        self.assertEqual(state["status"], "processing")
        self.assertEqual(state["progress"], 0.4)
        self.assertFalse(state["viewer"]["rigged"])
        self.assertEqual(state["model"]["state"], "static")
        self.assertEqual(self.client.get(f"/api/task-viewer/{STATIC}/files/task/rig/rig.json").status_code, 404)
        model = self.client.head(f"/api/task-viewer/{STATIC}/files/task/proj/model.glb")
        self.assertEqual(model.headers["x-accel-redirect"], f"/_autorig_glb_cache/{STATIC}_prepared.glb")
        self.assertEqual(self.client.head(f"/api/task-viewer/{STATIC}/files/task/rig/rigged.glb").status_code, 404)
        redirect = self.client.get(f"/api/task-viewer/{STATIC}/files/task/rig/rigged.glb", follow_redirects=False)
        self.assertEqual((redirect.status_code, redirect.headers["location"]),
                         (307, f"/api/task/{STATIC}/animations.glb"))

    def test_private_task_needs_its_owner(self):
        self.assertEqual(self.view(PRIVATE).status_code, 404)
        self.assertEqual(self.client.head(f"/api/task-viewer/{PRIVATE}/files/task/rig/rigged.glb").status_code, 404)
        self.client.cookies.set("anon_id", "secret-anon")
        self.assertEqual(self.view(PRIVATE).status_code, 200)
        self.client.cookies.clear()
        type(self).user = SimpleNamespace(email="owner@example.com")
        state = self.view(PRIVATE).json()
        self.assertTrue(state["admin"])

    def test_queue_position_follows_dispatch_order(self):
        q1, q2, q3 = (self.view(t).json() for t in (Q1, Q2, Q3))
        self.assertEqual((q1["queue"]["ahead"], q2["queue"]["ahead"], q3["queue"]["ahead"]), (0, 1, 2))
        self.assertEqual(q1["queue"]["processing"], 2)
        self.assertIsNone(q1["viewer"])
        self.assertEqual(q1["stage"], "queued")
        self.assertEqual(self.warmups, [])

    def test_invalid_cache_file_is_ignored(self):
        self.assertIsNone(routes.cached_glb(self.cache, Q1, routes.RIGGED_KINDS))

    def test_v3_task_opens_the_run_its_projection_names(self):
        state = self.view(BOUND).json()
        self.assertEqual(state["pipeline"], "v3")
        self.assertEqual((state["status"], state["stage"], state["progress"]), ("processing", "rig", 0.6))
        self.assertIsNone(state["queue"])
        self.assertEqual(state["viewer"], {"kind": "mt-run", "page": "/api/mt/unity/test/index.html",
                                           "params": {"run": RUN}, "rigged": False, "revision": RUN})
        self.assertEqual(self.warmups, [])

    def test_v3_viewer_rejects_a_url_that_does_not_name_the_session_run(self):
        def task(session):
            return SimpleNamespace(viewer_settings=json.dumps({"v3": {"state": "running", "session": session}}))

        shell = {"status": "processing", "viewer_url": None}
        good = {"mt_run_id": RUN, "viewer_url": f"/api/mt/unity/test/index.html?run={RUN}"}
        self.assertIsNotNone(routes.v3_viewer(task(good), shell))
        for bad in ({"mt_run_id": RUN, "viewer_url": "/api/mt/unity/test/index.html?run=" + "f" * 20},
                    {"mt_run_id": RUN, "viewer_url": f"https://evil.example/api/mt/unity/test/index.html?run={RUN}"},
                    {"mt_run_id": RUN, "viewer_url": f"/api/mt/view/{RUN}"},
                    {"mt_run_id": "../" + RUN, "viewer_url": f"/api/mt/unity/test/index.html?run=../{RUN}"},
                    {"viewer_url": f"/api/mt/unity/test/index.html?run={RUN}"}):
            with self.subTest(bad=bad):
                self.assertIsNone(routes.v3_viewer(task(bad), shell))

    def test_shared_viewer_assets_redirect_to_motion_transfer(self):
        reply = self.client.get(f"/api/task-viewer/{DONE}/texlib/index.json", follow_redirects=False)
        self.assertEqual((reply.status_code, reply.headers["location"]), (307, "/api/mt/texlib/index.json"))
        reply = self.client.get(f"/api/task-viewer/{DONE}/scenes/index.json?platform=WebGL", follow_redirects=False)
        self.assertEqual(reply.headers["location"], "/api/mt/scenes/index.json?platform=WebGL")
        saved = self.client.post(f"/api/task-viewer/{DONE}/viewer/settings", json={"settings": {"x": 1}},
                                 follow_redirects=False)
        self.assertEqual(saved.json(), {"ok": True, "saved": False})
        type(self).user = SimpleNamespace(email="owner@example.com")
        saved = self.client.post(f"/api/task-viewer/{DONE}/viewer/settings", json={"settings": {}},
                                 follow_redirects=False)
        self.assertEqual((saved.status_code, saved.headers["location"]), (307, "/api/mt/viewer/settings"))
        self.assertEqual(self.client.get(f"/api/task-viewer/{DONE}/runs/task/agent").status_code, 404)
        self.assertEqual(self.client.get(f"/api/task-viewer/{DONE}/texlib/../secrets").status_code, 404)
        self.assertEqual(self.client.get("/api/task-viewer/not-a-task/texlib/index.json").status_code, 404)


if __name__ == "__main__":
    unittest.main()
