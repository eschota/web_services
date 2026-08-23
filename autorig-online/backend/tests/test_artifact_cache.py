import asyncio
import hashlib
import tempfile
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

import artifact_cache
from artifact_cache import ArtifactSource
from database import ArtifactCacheJob, Base, Task, _create_database_engine


TASK_ID = "00000000-0000-0000-0000-000000000901"
WORKER = "https://converter-f1.freestock.online/api-converter-glb"


class ArtifactPathAndHostTests(unittest.TestCase):
    def test_path_traversal_is_rejected(self):
        for value in ("../secret", "files/../../secret", "/absolute", "C:/secret"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                artifact_cache.safe_relative_path(value)

    def test_assigned_worker_allows_its_files_alias_only(self):
        accepted = artifact_cache.validate_source_url(
            "https://f1.freestock.online/abc/model.glb",
            WORKER,
        )
        self.assertIn("f1.freestock.online", accepted)
        with self.assertRaises(ValueError):
            artifact_cache.validate_source_url(
                "https://converter-f2.freestock.online/abc/model.glb",
                WORKER,
            )
        with self.assertRaises(ValueError):
            artifact_cache.validate_source_url(
                "https://f1.attacker.invalid/abc/model.glb",
                WORKER,
            )
        with self.assertRaises(ValueError):
            artifact_cache.validate_source_url(
                "https://evil-f1.freestock.online/abc/model.glb",
                WORKER,
            )

    def test_retry_schedule_matches_operational_contract(self):
        self.assertEqual(
            [artifact_cache.retry_delay_seconds(attempt) for attempt in range(1, 7)],
            [30, 120, 600, 1800, 1800, 1800],
        )


class RangeResumeTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_uses_verified_eight_mebibyte_ranges(self):
        payload = b"glTF" + (2).to_bytes(4, "little") + b"\x00\x00\x00\x00"
        payload += b"x" * (artifact_cache.RANGE_CHUNK_BYTES + 4096)
        seen_ranges = []

        def handler(request: httpx.Request) -> httpx.Response:
            header = request.headers.get("range")
            self.assertIsNotNone(header)
            start, end = [int(value) for value in header[6:].split("-", 1)]
            seen_ranges.append((start, end))
            block = payload[start : end + 1]
            return httpx.Response(
                206,
                content=block,
                headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}"},
            )

        source = ArtifactSource(
            url="https://converter-f1.freestock.online/task/model.glb",
            relative_path="files/model.glb",
            role="primary_glb",
            assigned_worker=WORKER,
        )
        with tempfile.TemporaryDirectory(prefix="autorig-cache-resume-") as tmp:
            partial = Path(tmp) / "partial"
            partial.write_bytes(payload[:1024])
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                await artifact_cache._download_to_partial(
                    client,
                    source,
                    partial,
                    {"size": len(payload), "ranges": True},
                )
            self.assertEqual(partial.read_bytes(), payload)
        self.assertEqual(seen_ranges[0][0], 1024)
        self.assertTrue(all(end - start + 1 <= 8 * 1024 * 1024 for start, end in seen_ranges))


class ArtifactValidationTests(unittest.TestCase):
    def test_html_cannot_be_published_as_a_model(self):
        with tempfile.TemporaryDirectory(prefix="autorig-cache-html-") as tmp:
            path = Path(tmp) / "model.glb"
            path.write_text("<!doctype html><html>bad gateway</html>", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "HTML"):
                artifact_cache.validate_file(path, role="primary_glb")

    def test_zip_crc_is_checked(self):
        with tempfile.TemporaryDirectory(prefix="autorig-cache-zip-") as tmp:
            path = Path(tmp) / "bundle.zip"
            with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as archive:
                archive.writestr("model-files/model.glb", b"glTF" + b"x" * 128)
            artifact_cache.validate_file(path, role="full_bundle")
            data = bytearray(path.read_bytes())
            index = data.find(b"glTF")
            data[index + 4] ^= 0xFF
            path.write_bytes(data)
            with self.assertRaises(RuntimeError):
                artifact_cache.validate_file(path, role="full_bundle")

    def test_manifest_lookup_builds_internal_nginx_uri(self):
        with tempfile.TemporaryDirectory(prefix="autorig-cache-lookup-") as tmp:
            root = Path(tmp)
            task_dir = root / TASK_ID
            artifact = task_dir / "files" / "model files" / "hero.glb"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b"glTF" + (2).to_bytes(4, "little") + b"x" * 16)
            artifact_cache.write_manifest(
                root,
                TASK_ID,
                {
                    "files": [
                        {
                            "source_url": "https://f1.freestock.online/hero.glb",
                            "relative_path": "files/model files/hero.glb",
                            "size": artifact.stat().st_size,
                            "role": "viewer_glb",
                        }
                    ]
                },
            )
            entry = artifact_cache.lookup_cached_artifact(
                TASK_ID,
                source_url="https://f1.freestock.online/hero.glb",
                root=root,
            )
            self.assertIsNotNone(entry)
            self.assertEqual(
                entry["internal_uri"],
                f"/_autorig_artifacts/{TASK_ID}/files/model%20files/hero.glb",
            )


class RetentionTests(unittest.TestCase):
    def _write_entry(self, root, task_id, name, *, full_until, long_lived, role):
        task_dir = root / task_id
        path = task_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 128)
        manifest = {
            "version": 1,
            "task_id": task_id,
            "full_until": full_until.isoformat(),
            "files": [
                {
                    "source_url": f"https://f1.freestock.online/{name}",
                    "relative_path": name,
                    "size": 128,
                    "sha256": hashlib.sha256(b"x" * 128).hexdigest(),
                    "role": role,
                    "cached_at": (full_until - timedelta(hours=24)).isoformat(),
                    "long_lived": long_lived,
                }
            ],
        }
        artifact_cache.write_manifest(root, task_id, manifest)
        return path

    def test_never_deletes_under_24_hours_or_last_copy_deliverables(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory(prefix="autorig-cache-retention-") as tmp:
            root = Path(tmp)
            fresh = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000911",
                "files/fresh.mp4",
                full_until=now + timedelta(hours=1),
                long_lived=False,
                role="preview_video",
            )
            durable = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000912",
                "files/model.glb",
                full_until=now - timedelta(hours=1),
                long_lived=True,
                role="primary_glb",
            )
            old_preview = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000913",
                "files/old.mp4",
                full_until=now - timedelta(hours=1),
                long_lived=False,
                role="preview_video",
            )
            result = artifact_cache.run_retention(
                root=root,
                now=now,
                soft_cap_gb=0,
                reserve_gb=1,
                disk_usage_fn=lambda _path: SimpleNamespace(free=0),
            )
            self.assertTrue(fresh.exists())
            self.assertTrue(durable.exists())
            self.assertFalse(old_preview.exists())
            self.assertTrue(result["blocked"], "last-copy GLB must win over the reserve target")
            self.assertTrue((root / artifact_cache.PAUSE_MARKER).is_file())

    def test_pressure_removes_only_redundant_long_lived_copy(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory(prefix="autorig-cache-duplicates-") as tmp:
            root = Path(tmp)
            first = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000914",
                "deliverables/first.zip",
                full_until=now - timedelta(hours=1),
                long_lived=True,
                role="full_bundle",
            )
            second = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000915",
                "deliverables/second.zip",
                full_until=now - timedelta(hours=1),
                long_lived=True,
                role="full_bundle",
            )
            result = artifact_cache.run_retention(
                root=root,
                now=now,
                soft_cap_gb=0,
                reserve_gb=1,
                disk_usage_fn=lambda _path: SimpleNamespace(free=0),
            )
            self.assertEqual(int(first.exists()) + int(second.exists()), 1)
            self.assertEqual(result["removed_count"], 1)

    def test_soft_cap_alone_does_not_block_last_copy_deliverables(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory(prefix="autorig-cache-soft-cap-") as tmp:
            root = Path(tmp)
            durable = self._write_entry(
                root,
                "00000000-0000-0000-0000-000000000916",
                "files/model.glb",
                full_until=now - timedelta(hours=1),
                long_lived=True,
                role="primary_glb",
            )
            result = artifact_cache.run_retention(
                root=root,
                now=now,
                soft_cap_gb=0,
                reserve_gb=1,
                disk_usage_fn=lambda _path: SimpleNamespace(free=2 * 1024**3),
            )
            self.assertTrue(result["pressure"])
            self.assertFalse(result["blocked"])
            self.assertTrue(durable.exists())
            self.assertFalse((root / artifact_cache.PAUSE_MARKER).exists())


class DurableQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_cancels_busy_workers_after_grace_period(self):
        stop_event = asyncio.Event()
        started = asyncio.Event()

        async def busy_worker():
            started.set()
            await asyncio.Event().wait()

        worker = asyncio.create_task(busy_worker())
        await started.wait()
        await artifact_cache.stop_artifact_cache_workers(
            stop_event,
            [worker],
            grace_seconds=0.01,
        )

        self.assertTrue(stop_event.is_set())
        self.assertTrue(worker.done())
        self.assertTrue(worker.cancelled())

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="autorig-cache-db-")
        db_path = Path(self.temp_dir.name) / "autorig.db"
        self.engine = _create_database_engine(f"sqlite+aiosqlite:///{db_path}")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def asyncTearDown(self):
        await self.engine.dispose()
        self.temp_dir.cleanup()

    async def test_completion_and_queue_row_commit_atomically_and_survive_session_restart(self):
        async with self.sessions() as db:
            task = Task(
                id=TASK_ID,
                owner_type="anon",
                owner_id="cache-test",
                input_url="https://example.test/model.glb",
                input_type="t_pose",
                worker_api=WORKER,
                status="done",
            )
            db.add(task)
            await artifact_cache.enqueue_artifact_cache(db, task)
            await db.commit()

        async with self.sessions() as verifier:
            task = await verifier.get(Task, TASK_ID)
            row = (
                await verifier.execute(
                    select(ArtifactCacheJob).where(ArtifactCacheJob.task_id == TASK_ID)
                )
            ).scalar_one()
            self.assertEqual(task.artifact_cache_status, "pending")
            self.assertGreaterEqual(
                task.artifact_cache_full_until,
                datetime.utcnow() + timedelta(hours=23, minutes=59),
            )
            self.assertEqual(row.status, "pending")
            self.assertEqual(row.worker_key, "f1")

    async def test_expired_failure_is_not_silently_cleared(self):
        now = datetime.utcnow()
        async with self.sessions() as db:
            task = Task(
                id="00000000-0000-0000-0000-000000000902",
                owner_type="anon",
                owner_id="cache-test",
                input_url="https://example.test/model.glb",
                input_type="t_pose",
                worker_api=WORKER,
                status="done",
                artifact_cache_status="failed",
                artifact_cache_error="worker copy expired",
                artifact_cache_full_until=now - timedelta(seconds=1),
            )
            db.add(task)
            db.add(
                ArtifactCacheJob(
                    task_id=task.id,
                    worker_key="f1",
                    status="failed",
                    next_attempt_at=now,
                    deadline_at=now - timedelta(seconds=1),
                    last_error="worker copy expired",
                )
            )
            await db.commit()
            await artifact_cache.enqueue_artifact_cache(db, task, now=now)
            await db.commit()
            self.assertEqual(task.artifact_cache_status, "failed")
            self.assertEqual(task.artifact_cache_error, "worker copy expired")


if __name__ == "__main__":
    unittest.main()
