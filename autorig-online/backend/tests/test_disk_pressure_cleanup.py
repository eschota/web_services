import importlib.util
import ast
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


class DiskPressureCleanupEntrypointContractTests(unittest.TestCase):
    def test_timer_does_not_import_or_call_schema_initializer(self):
        tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
        database_imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "database"
            for alias in node.names
        }
        called_names = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }

        self.assertNotIn("init_db", database_imports)
        self.assertNotIn("init_db", called_names)


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
        self.commits = 0

    async def execute(self, _query):
        return _ScalarRows(self.cleanable_tasks)

    async def commit(self):
        self.commits += 1


class _FakeTask:
    def __init__(self, task_id, upload_status=None):
        self.id = task_id
        self.youtube_upload_status = upload_status
        self.youtube_upload_error = None
        self.youtube_video_id = None
        self.video_ready = True
        self.video_url = f"https://worker/{task_id}.mp4"
        self.updated_at = None


class DiskPressureVideoCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_preview_with_poster_is_removed_and_db_reference_cleared(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video_dir = Path(tmp)
            old_uploaded = video_dir / "uploaded-old.mp4"
            old_protected = video_dir / "no-fallback-old.mp4"
            fresh_uploaded = video_dir / "uploaded-fresh.mp4"
            for path in (old_uploaded, old_protected, fresh_uploaded):
                path.write_bytes(b"video-bytes")

            old_time = time.time() - 48 * 3600
            os.utime(old_uploaded, (old_time, old_time))
            os.utime(old_protected, (old_time, old_time))

            uploaded = _FakeTask("uploaded-old", "uploaded")
            protected = _FakeTask("no-fallback-old", "deferred")
            fresh = _FakeTask("uploaded-fresh", "uploaded")
            db = _FakeDb([uploaded, protected, fresh])
            fallback = video_dir / "posters"
            fallback.mkdir()
            (fallback / "uploaded-old.jpg").write_bytes(b"poster")
            (fallback / "uploaded-fresh.jpg").write_bytes(b"poster")
            with patch.object(cleanup, "_free_gb", side_effect=[4.0, 6.0]):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=video_dir,
                    target_free_gb=5.5,
                    min_age_hours=24,
                    task_cache_dir=video_dir / "tasks",
                    glb_cache_dir=video_dir / "glb",
                    preflight_render_dir=fallback,
                )

            self.assertEqual(removed, 1)
            self.assertEqual(freed, len(b"video-bytes"))
            self.assertFalse(old_uploaded.exists())
            self.assertTrue(old_protected.exists())
            self.assertTrue(fresh_uploaded.exists())
            self.assertFalse(uploaded.video_ready)
            self.assertIsNone(uploaded.video_url)
            self.assertEqual(db.commits, 1)

    async def test_old_deferred_video_is_removed_when_viewer_glb_exists(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video_dir = Path(tmp)
            old_failed = video_dir / "failed-old.mp4"
            old_deferred = video_dir / "deferred-old.mp4"
            for path in (old_failed, old_deferred):
                path.write_bytes(b"video-bytes")
                old_time = time.time() - 48 * 3600
                os.utime(path, (old_time, old_time))

            failed = _FakeTask("failed-old", "failed")
            deferred = _FakeTask("deferred-old", "deferred")
            db = _FakeDb([failed, deferred])
            glb_dir = video_dir / "glb"
            glb_dir.mkdir()
            (glb_dir / "deferred-old_prepared_viewer.glb").write_bytes(b"glTF")
            with patch.object(cleanup, "_free_gb", side_effect=[4.0, 6.0]):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=video_dir,
                    target_free_gb=5.5,
                    min_age_hours=24,
                    task_cache_dir=video_dir / "tasks",
                    glb_cache_dir=glb_dir,
                    preflight_render_dir=video_dir / "posters",
                )

            self.assertEqual(removed, 1)
            self.assertEqual(freed, len(b"video-bytes"))
            self.assertTrue(old_failed.exists())
            self.assertFalse(old_deferred.exists())
            self.assertEqual(deferred.youtube_upload_status, "skipped")
            self.assertEqual(deferred.youtube_upload_error, "quota_window_expired")

    async def test_expired_preview_cleanup_stops_with_healthy_headroom(self):
        with tempfile.TemporaryDirectory(prefix="autorig-video-pressure-") as tmp:
            video = Path(tmp) / "uploaded-old.mp4"
            video.write_bytes(b"video-bytes")
            old_time = time.time() - 48 * 3600
            os.utime(video, (old_time, old_time))
            task = _FakeTask("uploaded-old", "uploaded")
            db = _FakeDb([task])
            poster_dir = Path(tmp) / "posters"
            poster_dir.mkdir()
            (poster_dir / "uploaded-old.jpg").write_bytes(b"poster")

            with patch.object(cleanup, "_free_gb", return_value=6.0):
                removed, freed = await cleanup._purge_uploaded_video_cache_until(
                    db,
                    video_cache_dir=Path(tmp),
                    target_free_gb=5.5,
                    min_age_hours=24,
                    task_cache_dir=Path(tmp) / "tasks",
                    glb_cache_dir=Path(tmp) / "glb",
                    preflight_render_dir=poster_dir,
                )

            self.assertEqual((removed, freed), (0, 0))
            self.assertTrue(video.exists())
            self.assertTrue(task.video_ready)
            self.assertIsNotNone(task.video_url)


if __name__ == "__main__":
    unittest.main()

async def _upstream_url(_db, path):
    return f"https://worker.example/{path.name}"


async def _upstream_alive(_url):
    return True


async def _upstream_gone(_url):
    return False


class _FakeRow:
    def __init__(self, value):
        self._value = value

    def first(self):
        return (self._value,) if self._value is not None else None


class _UrlDb:
    """Minimal db stub: maps task id -> ready_urls."""

    def __init__(self, mapping):
        self.mapping = mapping

    async def execute(self, query):
        # the query is select(Task.ready_urls).where(Task.id == task_id);
        # tests drive resolution through _cache_entry_upstream_url instead
        raise NotImplementedError


class GlbCachePruneTests(unittest.IsolatedAsyncioTestCase):
    """The cache holds the last copy of many deliverables (workers purge their
    outputs), so eviction must verify the upstream can still serve them."""

    def setUp(self):
        # the last-copy memo is a real file on a real path: without redirecting
        # it, one test's verdicts silently become another's, and a test that
        # counts probes sees none
        self._memo_dir = tempfile.TemporaryDirectory(prefix="autorig-memo-")
        patcher = patch.object(
            cleanup, "LAST_COPY_MEMO_PATH", Path(self._memo_dir.name) / "memo.json"
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self._memo_dir.cleanup)

    @staticmethod
    def _write(path: Path, size_mb: int, age_hours: float) -> Path:
        path.write_bytes(b"\0" * (size_mb * 1024 * 1024))
        when = time.time() - age_hours * 3600
        os.utime(path, (when, when))
        return path

    async def test_fbx_artifacts_are_evicted_oldest_first(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "a_all_animations_unity.fbx", 40, 48)
            self._write(cache / "b_all_animations_unity.fbx", 40, 36)
            self._write(cache / "c_prepared.glb", 20, 30)

            with patch.object(cleanup, "_free_gb", return_value=100.0),                  patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url),                  patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                removed, freed = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=1.0,      # no free-space pressure
                    max_cache_gb=0.05,       # ~51 MB cap forces eviction
                    min_age_hours=24,
                )

            self.assertGreater(removed, 0)
            self.assertGreater(freed, 0)
            self.assertFalse((cache / "a_all_animations_unity.fbx").exists())
            self.assertTrue((cache / "c_prepared.glb").exists())

    async def test_pressure_relaxes_the_age_preference(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            for name in ("x.fbx", "y.fbx", "z.glb"):
                self._write(cache / name, 30, 1)  # all younger than min_age

            with patch.object(cleanup, "_free_gb", return_value=0.5),                  patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url),                  patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                removed, _freed = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.02,
                    min_age_hours=24,
                )
            self.assertGreater(removed, 0)

    async def test_files_still_being_written_are_protected(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "fresh.fbx", 30, 0)

            with patch.object(cleanup, "_free_gb", return_value=0.5),                  patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url),                  patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )
            self.assertEqual(removed, 0)
            self.assertTrue((cache / "fresh.fbx").is_file())

    async def test_unrelated_files_are_never_touched(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "keep.json", 30, 100)
            self._write(cache / "drop.fbx", 30, 100)

            with patch.object(cleanup, "_free_gb", return_value=0.5),                  patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url),                  patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )
            self.assertTrue((cache / "keep.json").is_file())
            self.assertFalse((cache / "drop.fbx").is_file())

    async def test_eviction_stops_once_under_cap(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            for i in range(4):
                self._write(cache / f"f{i}.fbx", 25, 48 + i)

            with patch.object(cleanup, "_free_gb", return_value=100.0),                  patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url),                  patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=1.0,
                    max_cache_gb=0.06,
                    min_age_hours=24,
                )
            self.assertGreater(removed, 0)
            self.assertLess(removed, 4)

    async def test_entry_is_kept_when_the_worker_no_longer_serves_it(self):
        """Regression: workers purge their outputs, so an unreachable upstream
        means the cache file is the LAST copy of a user deliverable."""
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "task1_a_all_animations_unity.fbx", 40, 100)
            self._write(cache / "task2_b_all_animations_unity.fbx", 40, 90)

            with patch.object(cleanup, "_free_gb", return_value=0.5), \
                 patch.object(cleanup, "_cache_entry_upstream_url", new=_upstream_url), \
                 patch.object(cleanup, "_upstream_is_available", new=_upstream_gone):
                removed, freed = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )

            self.assertEqual(removed, 0)
            self.assertEqual(freed, 0)
            self.assertTrue((cache / "task1_a_all_animations_unity.fbx").is_file())
            self.assertTrue((cache / "task2_b_all_animations_unity.fbx").is_file())

    async def test_the_probe_budget_is_not_spent_on_the_same_entries_twice(self):
        """Regression: the oldest entries are the ones whose upstream is long
        gone, so every run burned its whole budget on them and freed nothing."""
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp, \
             tempfile.TemporaryDirectory(prefix="autorig-memo-") as memo_dir:
            cache = Path(tmp)
            # two ancient entries the worker has purged, one newer it still serves
            self._write(cache / "old1_all_animations_unity.fbx", 40, 300)
            self._write(cache / "old2_all_animations_unity.fbx", 40, 290)
            self._write(cache / "fresh_all_animations_unity.fbx", 40, 100)

            probed = []

            async def _probe(url):
                probed.append(url)
                return "fresh" in url

            async def _url(_db, path):
                return f"https://worker/{path.name}"

            memo = Path(memo_dir) / "memo.json"
            with patch.object(cleanup, "_free_gb", return_value=0.5), \
                 patch.object(cleanup, "LAST_COPY_MEMO_PATH", memo), \
                 patch.object(cleanup, "GLB_CACHE_MAX_PROBES", 2), \
                 patch.object(cleanup, "_cache_entry_upstream_url", new=_url), \
                 patch.object(cleanup, "_upstream_is_available", new=_probe):
                # first pass: the budget is spent on the two dead entries
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None, glb_cache_dir=cache, target_free_gb=5.0,
                    max_cache_gb=0.01, min_age_hours=24,
                )
                self.assertEqual(removed, 0)
                self.assertEqual(len(probed), 2)

                # second pass: the verdict is remembered, so the budget reaches
                # the entry that CAN be freed
                probed.clear()
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None, glb_cache_dir=cache, target_free_gb=5.0,
                    max_cache_gb=0.01, min_age_hours=24,
                )

            self.assertEqual(removed, 1, "the freeable entry was never reached")
            self.assertFalse((cache / "fresh_all_animations_unity.fbx").is_file())
            # the last copies are still untouched
            self.assertTrue((cache / "old1_all_animations_unity.fbx").is_file())
            self.assertTrue((cache / "old2_all_animations_unity.fbx").is_file())
            self.assertNotIn("https://worker/old1_all_animations_unity.fbx", probed)

    async def test_a_memo_entry_expires_so_it_is_not_a_blacklist(self):
        with tempfile.TemporaryDirectory(prefix="autorig-memo-") as memo_dir:
            memo = Path(memo_dir) / "memo.json"
            stale = time.time() - (cleanup.LAST_COPY_MEMO_TTL_SECONDS + 60)
            memo.write_text('{"gone.fbx": %f, "recent.fbx": %f}' % (stale, time.time()))
            with patch.object(cleanup, "LAST_COPY_MEMO_PATH", memo):
                loaded = cleanup._load_last_copy_memo()
            self.assertNotIn("gone.fbx", loaded, "a worker can start serving it again")
            self.assertIn("recent.fbx", loaded)

    async def test_a_corrupt_memo_is_ignored_rather_than_fatal(self):
        with tempfile.TemporaryDirectory(prefix="autorig-memo-") as memo_dir:
            memo = Path(memo_dir) / "memo.json"
            memo.write_text("not json at all")
            with patch.object(cleanup, "LAST_COPY_MEMO_PATH", memo):
                self.assertEqual(cleanup._load_last_copy_memo(), {})

    async def test_entry_without_a_known_upstream_is_kept(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "orphan_file.fbx", 30, 100)

            async def _no_url(_db, _path):
                return ""

            with patch.object(cleanup, "_free_gb", return_value=0.5), \
                 patch.object(cleanup, "_cache_entry_upstream_url", new=_no_url), \
                 patch.object(cleanup, "_upstream_is_available", new=_upstream_alive):
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )
            self.assertEqual(removed, 0)
            self.assertTrue((cache / "orphan_file.fbx").is_file())

    async def test_abandoned_partials_are_dropped_without_probing(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            self._write(cache / "half_download.fbx.ab12cd.tmp", 30, 100)

            probed = {"n": 0}

            async def _count(_db, _path):
                probed["n"] += 1
                return ""

            with patch.object(cleanup, "_free_gb", return_value=0.5), \
                 patch.object(cleanup, "_cache_entry_upstream_url", new=_count), \
                 patch.object(cleanup, "_upstream_is_available", new=_upstream_gone):
                removed, _ = await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )
            self.assertEqual(removed, 1)
            self.assertEqual(probed["n"], 0)
            self.assertFalse((cache / "half_download.fbx.ab12cd.tmp").exists())

    async def test_probe_budget_is_bounded(self):
        with tempfile.TemporaryDirectory(prefix="autorig-glb-cache-") as tmp:
            cache = Path(tmp)
            for i in range(8):
                self._write(cache / f"t{i}_x_all_animations_unity.fbx", 10, 50 + i)

            probed = {"n": 0}

            async def _count(_db, _path):
                probed["n"] += 1
                return ""

            with patch.object(cleanup, "_free_gb", return_value=0.5), \
                 patch.object(cleanup, "GLB_CACHE_MAX_PROBES", 3), \
                 patch.object(cleanup, "_cache_entry_upstream_url", new=_count), \
                 patch.object(cleanup, "_upstream_is_available", new=_upstream_gone):
                await cleanup._purge_oldest_glb_cache_until(
                    None,
                    glb_cache_dir=cache,
                    target_free_gb=5.0,
                    max_cache_gb=0.01,
                    min_age_hours=24,
                )
            self.assertEqual(probed["n"], 3)
