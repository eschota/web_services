import importlib
import json
import os
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from sqlalchemy import delete


class ArchiveBundleTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="autorig-archive-test-")
        db_path = Path(cls._tmp.name) / "test.db"
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
        os.environ["APP_URL"] = "https://autorig.test"

        backend_dir = str(Path(__file__).resolve().parents[1])
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)

        for mod in ("config", "database", "models", "tasks", "auth", "main"):
            sys.modules.pop(mod, None)

        cls.database = importlib.import_module("database")
        cls.models = importlib.import_module("models")
        cls.main = importlib.import_module("main")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        self.task_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        self._patch_tasks = tempfile.mkdtemp(prefix="autorig-task-cache-")
        self.main.TASK_CACHE_DIR = Path(self._patch_tasks)

        async with self.database.AsyncSessionLocal() as db:
            owner = self.database.User(email="owner@example.com", name="Owner", balance_credits=0)
            buyer = self.database.User(email="buyer@example.com", name="Buyer", balance_credits=50)
            guid = "ffffffff-1111-2222-3333-444444444444"
            w_url = f"http://worker.test/converter/glb/{guid}/{guid}_file.glb"
            task = self.database.Task(
                id=self.task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(UTC).replace(tzinfo=None),
                status="done",
                guid=guid,
                worker_api=f"http://worker.test/converter/glb/{guid}/",
                output_urls=[w_url],
                ready_urls=[w_url],
            )
            db.add_all([owner, buyer, task])
            await db.commit()

    async def asyncTearDown(self):
        async with self.database.AsyncSessionLocal() as db:
            await db.execute(delete(self.database.TaskFilePurchase))
            await db.execute(delete(self.database.TaskAnimationPurchase))
            await db.execute(delete(self.database.TaskAnimationBundlePurchase))
            await db.execute(delete(self.database.Task))
            await db.execute(delete(self.database.User))
            await db.commit()
        import shutil

        shutil.rmtree(self._patch_tasks, ignore_errors=True)

    async def test_prepare_requires_full_purchase(self):
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            with self.assertRaises(HTTPException) as ctx:
                await self.main.api_prepare_task_archive(self.task_id, user=buyer, db=db)
            self.assertEqual(ctx.exception.status_code, 402)

    async def test_archive_estimate_counts_entries(self):
        async with self.database.AsyncSessionLocal() as db:
            task = (
                await db.execute(self.main.select(self.database.Task).where(self.database.Task.id == self.task_id))
            ).scalar_one()
            entries = await self.main._collect_archive_bundle_entries(
                task, db, "", include_all_ready_animations=True
            )
            self.assertGreaterEqual(len(entries), 1)
        async with self.database.AsyncSessionLocal() as db:
            out = await self.main.api_task_archive_estimate(self.task_id, db=db)
        self.assertEqual(out["task_id"], self.task_id)
        self.assertEqual(out["file_count"], len(entries))
        self.assertIsInstance(out["approx_bytes"], int)
        self.assertIn("approx_bytes_complete", out)

    async def test_prepare_returns_job_when_purchased(self):
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            db.add(
                self.database.TaskFilePurchase(
                    task_id=self.task_id,
                    user_email=buyer.email,
                    file_index=None,
                    credits_spent=10,
                )
            )
            await db.commit()

        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            with patch.object(self.main, "_run_archive_bundle_job", new=AsyncMock()):
                out = await self.main.api_prepare_task_archive(self.task_id, user=buyer, db=db)
            self.assertIn("job_id", out)
            self.assertEqual(out["task_id"], self.task_id)

    def test_zip_bundle_uses_stored_only(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            f1 = tmp / "hello.bin"
            f1.write_bytes(b"abc123")
            entries = [
                {
                    "arcname": "models/hello.bin",
                    "source_url": "http://example.com/x",
                    "cache_path": f1,
                }
            ]
            zip_path = tmp / "bundle.zip"
            self.main._write_archive_job_state("tid", "jid", user_email="u@e.com")
            self.main._build_zip_bundle_sync("tid", "jid", entries, zip_path, 1)
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                self.assertEqual(names, ["models/hello.bin"])
                for zi in zf.infolist():
                    self.assertEqual(zi.compress_type, zipfile.ZIP_STORED)
                self.assertEqual(zf.read("models/hello.bin"), b"abc123")
        finally:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
