"""Build 3D model by Parts (2026-10-20): the child task endpoint and the parts block of the task page."""
import asyncio
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.orm import declarative_base  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

import parts_build_routes as routes  # noqa: E402

Base = declarative_base()


class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True)
    status = Column(String, default="created")
    owner_type = Column(String, default="anon")
    owner_id = Column(String, default="anon-owner")
    is_public = Column(Boolean, default=True)
    input_url = Column(String)
    input_type = Column(String, default="t_pose")
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    processing_started_at = Column(DateTime)
    pipeline_kind = Column(String, default="rig")
    queue_class = Column(String, default="interactive")
    created_via_api = Column(Boolean, default=False)
    poster_llm_title = Column(String)
    collection_guid = Column(String)
    collection_title = Column(String)
    collection_member_title = Column(String)
    content_rating = Column(String, default="unknown")
    content_score = Column(Float)
    owner_language = Column(String)
    telegram_new_notified_at = Column(DateTime)
    telegram_done_notified_at = Column(DateTime)
    youtube_upload_status = Column(String)
    ready_count = Column(Integer, default=0)
    total_count = Column(Integer, default=0)
    viewer_settings = Column(String)


PARENT = "11111111-1111-4111-8111-111111111111"
PRIVATE = "22222222-2222-4222-8222-222222222222"
PRUN = "abcdef0123456789abcd"
CRUN = "0123456789abcdef0123"
TOKEN = "test-dispatch-token-123"


class PartsBuildRoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        base = pathlib.Path(cls.tmp.name)
        (base / "token").write_text(TOKEN + "\n", encoding="utf-8")
        for run in (PRUN, CRUN):
            (base / "mt" / "runs" / run).mkdir(parents=True)
            (base / "mt" / "runs" / run / "run.json").write_text("{}", encoding="utf-8")
        cls.saved = (routes.TOKEN_FILE, routes.MT_ROOT)
        routes.TOKEN_FILE, routes.MT_ROOT = base / "token", base / "mt"
        cls.engine = create_async_engine(f"sqlite+aiosqlite:///{base / 'db.sqlite'}", poolclass=NullPool)
        cls.sessions = async_sessionmaker(cls.engine, expire_on_commit=False)

        async def seed():
            async with cls.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with cls.sessions() as db:
                db.add_all([
                    Task(id=PARENT, status="needs_review", owner_type="user", owner_id="a@example.com",
                         pipeline_kind="v3", poster_llm_title="Santa", content_rating="adult", is_public=True,
                         created_at=datetime(2026, 10, 20), owner_language="ru",
                         viewer_settings=json.dumps({"v3": {"state": "done", "session": {"mt_run_id": PRUN}}})),
                    Task(id=PRIVATE, status="done", owner_type="anon", owner_id="secret", is_public=False,
                         created_at=datetime(2026, 10, 20)),
                ])
                await db.commit()
        asyncio.run(seed())

        async def get_db():
            async with cls.sessions() as db:
                yield db

        async def get_user():
            return None

        app = FastAPI()
        app.include_router(routes.build_parts_build_router(get_db=get_db, get_current_user=get_user, task_model=Task,
                                                           is_admin_email=lambda e: e == "owner@example.com"))
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        routes.TOKEN_FILE, routes.MT_ROOT = cls.saved
        asyncio.run(cls.engine.dispose())
        cls.tmp.cleanup()

    def post(self, body, token=TOKEN, task=PARENT):
        h = {"Authorization": f"Bearer {token}"} if token else {}
        return self.client.post(f"/api/task/{task}/parts/child", json=body, headers=h)

    def body(self, **kw):
        return {"part": "hat", "label": "hat", "provider": "tripo", "variant": "v31std", "child_run": CRUN,
                "parent_run": PRUN, "color": "#c81e28", **kw}

    def load(self, tid):
        async def go():
            async with self.sessions() as db:
                return (await db.execute(select(Task).where(Task.id == tid))).scalar_one()
        return asyncio.run(go())

    def test_a_token_is_required(self):
        self.assertEqual(self.post(self.body(), token=None).status_code, 401)
        self.assertEqual(self.post(self.body(), token="wrong").status_code, 401)

    def test_child_task_inherits_owner_rating_and_links_both_ways(self):
        r = self.post(self.body())
        self.assertEqual(r.status_code, 200, r.text)
        doc = r.json()
        self.assertFalse(doc["existing"])
        child = self.load(doc["task"])
        self.assertEqual((child.owner_type, child.owner_id, child.content_rating, child.owner_language),
                         ("user", "a@example.com", "adult", "ru"))
        self.assertEqual((child.status, child.pipeline_kind), ("done", "v3"))
        self.assertEqual(child.poster_llm_title, "Santa · hat")
        self.assertEqual(child.collection_title, "Parts of Santa")
        self.assertIsNotNone(child.telegram_done_notified_at)
        vs = json.loads(child.viewer_settings)
        self.assertEqual(vs["v3"]["session"]["mt_run_id"], CRUN)
        self.assertEqual(vs["v3"]["session"]["viewer_url"], f"/api/mt/unity/test/index.html?run={CRUN}")
        self.assertEqual(vs["parts"]["parent_task"], PARENT)
        self.assertTrue(routes.is_child(child))
        parent = self.load(PARENT)
        kids = json.loads(parent.viewer_settings)["parts"]["children"]
        self.assertEqual([(k["task"], k["part"], k["run"]) for k in kids], [(child.id, "hat", CRUN)])
        self.assertEqual(json.loads(parent.viewer_settings)["v3"]["session"]["mt_run_id"], PRUN)   # untouched
        # the same call again answers the same task
        again = self.post(self.body())
        self.assertEqual(again.json()["task"], child.id)
        self.assertTrue(again.json()["existing"])
        # the navigation block of both
        view = routes.parts_view(self.load(PARENT))
        self.assertIsNone(view["parent"])
        self.assertEqual(view["children"][0]["url"], f"/task?id={child.id}")
        self.assertEqual(view["children"][0]["color"], "#c81e28")
        cview = routes.parts_view(child)
        self.assertEqual(cview["parent"]["parent_task"], PARENT)
        self.assertEqual(self.client.get(f"/api/task/{child.id}/parts").json()["parent"]["part"], "hat")
        self.assertEqual(routes.child_rig_version()["rerig"]["state"], "not_for_parts")

    def test_bad_input_is_refused(self):
        self.assertEqual(self.post(self.body(part="Hat!")).status_code, 400)
        self.assertEqual(self.post(self.body(child_run="ffffffffffffffffffff")).status_code, 400)   # no such run
        self.assertEqual(self.post(self.body(provider="openai")).status_code, 400)
        self.assertEqual(self.post(self.body(), task="33333333-3333-4333-8333-333333333333").status_code, 404)

    def test_private_task_parts_are_hidden_from_strangers(self):
        self.assertEqual(self.client.get(f"/api/task/{PRIVATE}/parts").status_code, 404)

    def test_task_without_parts_has_no_block(self):
        self.assertIsNone(routes.parts_view(self.load(PRIVATE)))


if __name__ == "__main__":
    unittest.main()
