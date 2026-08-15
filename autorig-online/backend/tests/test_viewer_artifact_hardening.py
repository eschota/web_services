import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main
import tasks


def _glb_header(total_size: int = 128) -> bytes:
    return b"glTF" + (2).to_bytes(4, "little") + total_size.to_bytes(4, "little")


class _ProbeResponse:
    def __init__(self, header: bytes, total_size: int, status_code: int = 206):
        self.status_code = status_code
        self.headers = {
            "content-range": f"bytes 0-11/{total_size}",
            "content-length": "12",
        }
        self.header = header

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def aiter_bytes(self, chunk_size=12):
        yield self.header


class _ProbeClient:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def stream(self, *_args, **_kwargs):
        return self.response


class _StatusOnlyResponse:
    status_code = 404
    headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _CountingStatusClient:
    calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def stream(self, *_args, **_kwargs):
        type(self).calls += 1
        return _StatusOnlyResponse()


class ViewerArtifactHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_remote_probe_requires_http_glb_header_and_matching_total(self):
        valid = _ProbeResponse(_glb_header(128), 128)
        with patch.object(tasks.httpx, "AsyncClient", return_value=_ProbeClient(valid)):
            self.assertTrue(await tasks._probe_remote_glb_artifact("https://worker/viewer.glb"))

        wrong_total = _ProbeResponse(_glb_header(128), 256)
        with patch.object(tasks.httpx, "AsyncClient", return_value=_ProbeClient(wrong_total)):
            self.assertFalse(await tasks._probe_remote_glb_artifact("https://worker/viewer.glb"))

    async def test_dispatch_persists_only_validated_viewer_url(self):
        task = SimpleNamespace(
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
        )
        result = SimpleNamespace(
            viewer_prepared_glb_url="https://worker/bad_model_prepared_viewer.glb",
            viewer_animations_glb_url="https://worker/good_all_animations_viewer.glb",
        )
        with patch.object(
            tasks,
            "_probe_remote_glb_artifact",
            AsyncMock(side_effect=[False, True]),
        ):
            await tasks.persist_validated_worker_viewer_artifacts(task, result)
        self.assertIsNone(task.viewer_prepared_glb_url)
        self.assertEqual(
            task.viewer_animations_glb_url,
            result.viewer_animations_glb_url,
        )

    async def test_late_reconcile_persists_verified_reexport(self):
        task = SimpleNamespace(
            id="task-id",
            guid="guid",
            worker_api="https://worker/api-converter-glb",
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
            status="done",
            updated_at=main.datetime(2026, 7, 28, 10, 30, 0),
        )
        persisted = {}
        where_args = []
        update_stmt = SimpleNamespace()
        update_stmt.where = lambda *args: where_args.extend(args) or update_stmt
        update_stmt.values = lambda **kwargs: persisted.update(kwargs) or update_stmt
        db = SimpleNamespace(
            execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)),
            commit=AsyncMock(),
            rollback=AsyncMock(),
            refresh=AsyncMock(),
        )
        with patch.object(
            tasks,
            "_fetch_concrete_worker_artifacts",
            AsyncMock(return_value=([], "https://worker/prepared.glb", None)),
        ), patch.object(
            tasks,
            "_validated_viewer_artifact_urls",
            AsyncMock(return_value=("https://worker/prepared.glb", None)),
        ), patch.object(tasks, "update", return_value=update_stmt), patch(
            "artifact_cache.enqueue_artifact_cache",
            new=AsyncMock(),
        ) as enqueue_cache:
            await tasks.reconcile_task_viewer_artifacts(db, task, force=True)
        self.assertEqual(
            persisted["viewer_prepared_glb_url"],
            "https://worker/prepared.glb",
        )
        self.assertIs(persisted["updated_at"], tasks.Task.updated_at)
        self.assertEqual(len(where_args), 4)
        db.execute.assert_awaited_once_with(update_stmt)
        self.assertEqual(db.commit.await_count, 2)
        db.rollback.assert_not_awaited()
        self.assertEqual(db.refresh.await_count, 2)
        enqueue_cache.assert_awaited_once_with(db, task, force_refresh=True)

    async def test_late_reconcile_drops_stale_result_after_concurrent_restart(self):
        task = SimpleNamespace(
            id="task-id",
            guid="old-guid",
            worker_api="https://old-worker/api-converter-glb",
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
            status="done",
            updated_at=main.datetime(2026, 7, 28, 10, 30, 0),
        )
        update_stmt = SimpleNamespace()
        update_stmt.where = lambda *_args: update_stmt
        update_stmt.values = lambda **_kwargs: update_stmt
        db = SimpleNamespace(
            execute=AsyncMock(return_value=SimpleNamespace(rowcount=0)),
            commit=AsyncMock(),
            rollback=AsyncMock(),
            refresh=AsyncMock(),
        )
        with patch.object(
            tasks,
            "_fetch_concrete_worker_artifacts",
            AsyncMock(return_value=([], "https://old-worker/prepared.glb", None)),
        ), patch.object(
            tasks,
            "_validated_viewer_artifact_urls",
            AsyncMock(return_value=("https://old-worker/prepared.glb", None)),
        ), patch.object(tasks, "update", return_value=update_stmt):
            await tasks.reconcile_task_viewer_artifacts(db, task, force=True)
        db.execute.assert_awaited_once_with(update_stmt)
        db.rollback.assert_awaited_once()
        db.commit.assert_not_awaited()
        db.refresh.assert_awaited_once_with(task)

    async def test_bad_optimized_fetch_uses_negative_backoff(self):
        _CountingStatusClient.calls = 0
        main._GLB_FETCH_BACKOFF_UNTIL.clear()
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main.httpx,
            "AsyncClient",
            return_value=_CountingStatusClient(),
        ):
            kwargs = {
                "profile": "optimized",
                "timeout_seconds": 0.1,
                "failure_backoff_seconds": 30.0,
            }
            self.assertIsNone(
                await main._get_cached_glb("task", "https://worker/bad.glb", "prepared_viewer", **kwargs)
            )
            self.assertIsNone(
                await main._get_cached_glb("task", "https://worker/bad.glb", "prepared_viewer", **kwargs)
            )
        self.assertEqual(_CountingStatusClient.calls, 1)

    def test_glb_fetch_backoff_prunes_expired_entries_and_caps_size(self):
        main._GLB_FETCH_BACKOFF_UNTIL.clear()
        now = 100.0
        main._GLB_FETCH_BACKOFF_UNTIL.update(
            {
                ("expired", "https://worker/expired.glb"): 99.0,
                ("oldest", "https://worker/oldest.glb"): 110.0,
                ("newer", "https://worker/newer.glb"): 120.0,
            }
        )
        with patch.object(main.time, "monotonic", return_value=now), patch.object(
            main,
            "_GLB_FETCH_BACKOFF_MAX_ENTRIES",
            2,
        ):
            main._record_glb_fetch_backoff(
                ("current", "https://worker/current.glb"),
                30.0,
            )

        self.assertEqual(
            main._GLB_FETCH_BACKOFF_UNTIL,
            {
                ("newer", "https://worker/newer.glb"): 120.0,
                ("current", "https://worker/current.glb"): 130.0,
            },
        )

    async def test_bad_optimized_url_falls_back_to_valid_original_cache(self):
        task = SimpleNamespace(
            id="task-id",
            guid="guid",
            viewer_prepared_glb_url="https://worker/bad_model_prepared_viewer.glb",
            ready_urls=[],
            worker_api=None,
            fbx_glb_output_url=None,
            fbx_glb_ready=False,
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main,
            "get_task_by_id",
            AsyncMock(return_value=task),
        ), patch.object(
            main,
            "_get_cached_glb",
            AsyncMock(return_value=None),
        ) as optimized_fetch:
            (Path(tmp) / "task-id_prepared.glb").write_bytes(_glb_header(12))
            response = await main.api_proxy_prepared_glb("task-id", db=None)
        self.assertEqual(response.headers["x-autorig-viewer-profile"], "original")
        optimized_fetch.assert_awaited_once_with(
            "task-id",
            task.viewer_prepared_glb_url,
            "prepared_viewer",
            profile="optimized",
            timeout_seconds=6.0,
            failure_backoff_seconds=30.0,
        )
    def test_worker_file_totals_are_recomputed_after_filtering(self):
        files = [
            {"name": "a.zip", "size": 10},
            {"name": "b.fbx", "size": "25"},
            {"name": "unknown", "size": None},
        ]
        self.assertEqual(
            main._worker_file_totals(files),
            {"file_count": 3, "total_size": 35},
        )

    def test_done_task_reconciliation_is_scheduled_not_awaited(self):
        main._viewer_reconcile_schedule_throttle.clear()
        with patch.object(main.asyncio, "create_task") as create_task:
            main._schedule_viewer_artifact_reconciliation("task-id")
        create_task.assert_called_once()
        create_task.call_args.args[0].close()


if __name__ == "__main__":
    unittest.main()
