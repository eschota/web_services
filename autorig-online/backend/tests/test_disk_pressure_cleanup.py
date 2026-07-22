import importlib.util
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_disk_pressure_cleanup.py"
SPEC = importlib.util.spec_from_file_location("run_disk_pressure_cleanup", SCRIPT_PATH)
cleanup = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(cleanup)


class _ScalarRows:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class _FakeDb:
    def __init__(self, cleanable_tasks):
        self.cleanable_tasks = cleanable_tasks

    async def execute(self, _query):
        return _ScalarRows(self.cleanable_tasks)


class DiskPressureVideoCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_old_youtube_uploaded_video_is_removed(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video_dir = Path(tmp)
            old_uploaded = video_dir / "uploaded-old.mp4"
            old_protected = video_dir / "deferred-old.mp4"
            fresh_uploaded = video_dir / "uploaded-fresh.mp4"
            for path in (old_uploaded, old_protected, fresh_uploaded):
                path.write_bytes(b"video-bytes")

            old_time = time.time() - 48 * 3600
            os.utime(old_uploaded, (old_time, old_time))
            os.utime(old_protected, (old_time, old_time))

            db = _FakeDb([
                ("uploaded-old", "uploaded"),
                ("uploaded-fresh", "uploaded"),
            ])
            with patch.object(cleanup, "_free_gb", side_effect=[4.0, 4.0, 6.0]):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=video_dir,
                    target_free_gb=5.5,
                    min_age_hours=24,
                )

            self.assertEqual(removed, 1)
            self.assertEqual(freed, len(b"video-bytes"))
            self.assertFalse(old_uploaded.exists())
            self.assertTrue(old_protected.exists())
            self.assertTrue(fresh_uploaded.exists())

    async def test_old_failed_video_is_removed_but_deferred_stays_protected(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video_dir = Path(tmp)
            old_failed = video_dir / "failed-old.mp4"
            old_deferred = video_dir / "deferred-old.mp4"
            for path in (old_failed, old_deferred):
                path.write_bytes(b"video-bytes")
                old_time = time.time() - 48 * 3600
                os.utime(path, (old_time, old_time))

            db = _FakeDb([("failed-old", "failed")])
            with patch.object(cleanup, "_free_gb", side_effect=[4.0, 4.0, 6.0]):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=video_dir,
                    target_free_gb=5.5,
                    min_age_hours=24,
                )

            self.assertEqual(removed, 1)
            self.assertEqual(freed, len(b"video-bytes"))
            self.assertFalse(old_failed.exists())
            self.assertTrue(old_deferred.exists())

    async def test_no_cleanup_when_headroom_is_healthy(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video = Path(tmp) / "uploaded-old.mp4"
            video.write_bytes(b"video-bytes")
            db = _FakeDb([("uploaded-old", "uploaded")])

            with patch.object(cleanup, "_free_gb", return_value=6.0):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=Path(tmp),
                    target_free_gb=5.5,
                    min_age_hours=24,
                )

            self.assertEqual((removed, freed), (0, 0))
            self.assertTrue(video.exists())


if __name__ == "__main__":
    unittest.main()
