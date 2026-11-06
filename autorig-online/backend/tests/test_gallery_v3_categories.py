"""Gallery V3 categories (tools/patch_gallery_v3_categories.py must be applied first).

Covers the pure helpers of gallery_categories, the SQL filter on a sqlite database with the real Task model (pagination
and totals are exact), the backfill from analysis/category.json, and the main.py block (GALLERY_RIG_TYPES and
_gallery_rig_icon_key), which is exec'd without importing the whole backend.
Run: py -3.11 -m pytest tests/test_gallery_v3_categories.py
"""
import asyncio
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

import gallery_categories as gc  # noqa: E402


def animal_settings(slug):
    return json.dumps({"rig_v2_animal_detection": {"animal_type": slug}})


class HelpersTest(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(gc.normalize_category("Quadruped"), "quadruped")
        self.assertEqual(gc.normalize_category("adult_human"), "humanoid")
        self.assertEqual(gc.normalize_category("snake"), "serpent")
        self.assertEqual(gc.normalize_category("insect"), "multilimb")
        self.assertEqual(gc.normalize_category("weapon"), "other")
        self.assertEqual(gc.normalize_category("something new"), "other")
        self.assertIsNone(gc.normalize_category(""))
        self.assertIsNone(gc.normalize_category(None))

    def test_category_from_doc(self):
        self.assertEqual(gc.category_from_doc({"category": "equine", "subcategory": " Horse "}), ("equine", "horse"))
        self.assertEqual(gc.category_from_doc({"what": "x"}), (None, None))
        self.assertEqual(gc.category_from_doc([]), (None, None))

    def test_category_for_run_reads_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = "c91d4e7f0123"
            d = Path(tmp) / run / "analysis"
            d.mkdir(parents=True)
            (d / "category.json").write_text(json.dumps({"category": "vehicle", "subcategory": "motorhome"}), encoding="utf-8")
            old = gc.MT_RUNS_DIR
            gc.MT_RUNS_DIR = Path(tmp)
            try:
                self.assertEqual(gc.category_for_run(run), ("vehicle", "motorhome"))
                self.assertEqual(gc.category_for_run("missing-run-0001"), (None, None))
                self.assertEqual(gc.category_for_run("../etc"), (None, None))
            finally:
                gc.MT_RUNS_DIR = old

    def test_category_for_task_fallbacks(self):
        self.assertEqual(gc.category_for_task(SimpleNamespace(v3_category="bird", input_type="t_pose")), "bird")
        self.assertEqual(gc.category_for_task(SimpleNamespace(v3_category=None, input_type="t_pose", viewer_settings=None)), "humanoid")
        legacy = SimpleNamespace(v3_category=None, input_type="animal", viewer_settings=animal_settings("horse"))
        self.assertEqual(gc.category_for_task(legacy), "equine")
        self.assertEqual(gc.category_for_task(SimpleNamespace(v3_category=None, input_type="animal", viewer_settings=animal_settings("dog"))), "quadruped")

    def test_icon_keys(self):
        icon = lambda c: gc.icon_key_for_task(SimpleNamespace(v3_category=c, input_type="t_pose", viewer_settings=None))
        self.assertEqual(icon("quadruped"), "dog")
        self.assertEqual(icon("equine"), "horse")
        self.assertEqual(icon("humanoid"), "humanoid")
        for c in gc.CATEGORIES_WITHOUT_ICON:
            self.assertEqual(icon(c), "humanoid")
        self.assertEqual(icon(None), "humanoid")


class SqlFilterTest(unittest.TestCase):
    def setUp(self):
        from sqlalchemy.ext.asyncio import create_async_engine
        from database import Base
        self.engine = create_async_engine("sqlite+aiosqlite://")

        async def create():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        asyncio.run(create())

    def run_async(self, coro_fn):
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from database import Base

        async def go():
            engine = create_async_engine("sqlite+aiosqlite://")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with AsyncSession(engine, expire_on_commit=False) as db:
                return await coro_fn(db)
        return asyncio.run(go())

    @staticmethod
    def make(i, category=None, sub=None, input_type="t_pose", settings=None, kind="v3"):
        from database import Task
        return Task(id=f"00000000-0000-0000-0000-{i:012d}", owner_type="anon", owner_id="a", status="done",
                    pipeline_kind=kind, input_type=input_type, viewer_settings=settings,
                    v3_category=category, v3_subcategory=sub)

    async def ids(self, db, slug, offset=0, limit=100):
        from sqlalchemy import func, select
        from database import Task
        cond = gc.rig_filter_condition(Task, slug)
        rows = (await db.execute(select(Task.id).where(cond).order_by(Task.id).offset(offset).limit(limit))).scalars().all()
        total = (await db.execute(select(func.count()).select_from(Task).where(cond))).scalar()
        return [int(r[-3:]) for r in rows], total

    def test_filters(self):
        make = self.make

        async def body(db):
            db.add_all([
                make(1, "humanoid"), make(2, None),                                   # humanoid + fallback
                make(3, "quadruped", "wolf"), make(4, "quadruped", "bear"),
                make(5, "equine", "horse"), make(6, "equine", "zebra"),
                make(7, "bird"), make(8, "vehicle", "car"), make(9, "hands"),
                make(10, None, input_type="animal", settings=animal_settings("dog"), kind="rig"),
                make(11, None, input_type="animal", settings=animal_settings("horse"), kind="rig"),
                make(12, "other"),
            ])
            await db.commit()
            self.assertEqual(await self.ids(db, "humanoid"), ([1, 2], 2))           # legacy V2 animals excluded
            self.assertEqual(await self.ids(db, "quadruped"), ([3, 4, 10], 3))
            self.assertEqual(await self.ids(db, "equine"), ([5, 6, 11], 3))
            self.assertEqual(await self.ids(db, "bird"), ([7], 1))
            self.assertEqual(await self.ids(db, "vehicle"), ([8], 1))
            self.assertEqual(await self.ids(db, "hands"), ([9], 1))
            self.assertEqual(await self.ids(db, "serpent"), ([], 0))
            # old slugs: legacy V2 rows + V3 rows of the mapped category with a matching subcategory
            self.assertEqual(await self.ids(db, "horse"), ([5, 11], 2))
            self.assertEqual(await self.ids(db, "dog"), ([3, 10], 2))
            self.assertEqual(await self.ids(db, "bear"), ([4], 1))
            self.assertEqual(await self.ids(db, "cat"), ([], 0))
            # pagination and total
            self.assertEqual(await self.ids(db, "equine", offset=1, limit=1), ([6], 3))
            self.assertIsNone(gc.rig_filter_condition(type("T", (), {}), "equine"))   # column missing -> caller falls back
        self.run_async(body)

    def test_backfill(self):
        make = self.make
        with tempfile.TemporaryDirectory() as tmp:
            for run, doc in (("run-aaaaaaaa", {"category": "bird", "subcategory": "eagle"}), ("run-bbbbbbbb", {"category": "snake"})):
                d = Path(tmp) / run / "analysis"
                d.mkdir(parents=True)
                (d / "category.json").write_text(json.dumps(doc), encoding="utf-8")
            old = gc.MT_RUNS_DIR
            gc.MT_RUNS_DIR = Path(tmp)

            async def body(db):
                from sqlalchemy import select
                from database import Task
                db.add_all([
                    make(1, None, settings=json.dumps({"v3": {"dispatch_run_id": "run-aaaaaaaa"}})),
                    make(2, None, settings=json.dumps({"v3": {"dispatch_run_id": "run-bbbbbbbb"}})),
                    make(3, None, settings=json.dumps({"v3": {"dispatch_run_id": "run-missing01"}})),
                    make(4, None, settings=json.dumps({"v3": {}})),
                    make(5, "equine", "horse", settings=json.dumps({"v3": {"dispatch_run_id": "run-aaaaaaaa"}})),
                ])
                await db.commit()
                stats = await gc.backfill_v3_categories(db)
                self.assertEqual(stats, {"seen": 4, "filled": 2, "no_run": 1, "no_file": 1})
                got = {t.id[-3:]: (t.v3_category, t.v3_subcategory) for t in (await db.execute(select(Task))).scalars()}
                self.assertEqual(got["001"], ("bird", "eagle"))
                self.assertEqual(got["002"], ("serpent", None))
                self.assertEqual(got["005"], ("equine", "horse"))                      # never overwritten
                self.assertEqual(await gc.backfill_v3_categories(db, dry_run=True), {"seen": 2, "filled": 0, "no_run": 1, "no_file": 1})
            try:
                self.run_async(body)
            finally:
                gc.MT_RUNS_DIR = old


class MainBlockTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        src = (BACKEND / "main.py").read_text(encoding="utf-8").replace("\r\n", "\n")
        a = src.index("GALLERY_RIG_TYPES = (")
        b = src.index("RIG_ARTICLE_LANGS: Dict[str, Dict[str, str]] = {")
        ns = {"json": json, "Task": object}
        exec(compile(src[a:b], "main.py", "exec"), ns)
        cls.ns = ns
        cls.src = src

    def test_rig_types(self):
        types = self.ns["GALLERY_RIG_TYPES"]
        for slug in gc.V3_CATEGORIES + tuple(gc.LEGACY_ANIMAL_CATEGORY):
            self.assertIn(slug, types)
        self.assertEqual(len(types), len(set(types)))

    def test_icon_key_function(self):
        f = self.ns["_gallery_rig_icon_key"]
        self.assertEqual(f(SimpleNamespace(v3_category="equine", input_type="t_pose", viewer_settings=None)), "horse")
        self.assertEqual(f(SimpleNamespace(v3_category="vehicle", input_type="t_pose", viewer_settings=None)), "vehicle")
        self.assertEqual(f(SimpleNamespace(input_type="t_pose", viewer_settings=None)), "humanoid")
        self.assertEqual(f(SimpleNamespace(v3_category=None, input_type="animal", viewer_settings=animal_settings("cat"))), "cat")

    def test_api_uses_the_sql_filter(self):
        block = self.src[self.src.index("async def api_get_gallery("):self.src.index('@app.get("/api/task/{task_id}/card"')]
        self.assertIn("rig_filter_condition(Task, rig_filter)", block)
        self.assertIn("category=_gallery_categories.category_for_task(t)", block)
        self.assertLess(block.index("rig_filter_condition"), block.index("query = ("))   # added before the query is built
        self.assertRegex(block, r"should_filter_rig = False")


if __name__ == "__main__":
    unittest.main()
