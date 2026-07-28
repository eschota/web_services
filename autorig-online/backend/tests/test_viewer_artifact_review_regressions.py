import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


def _glb_header(total_size: int = 12) -> bytes:
    return b"glTF" + (2).to_bytes(4, "little") + total_size.to_bytes(4, "little")


class _SlowResponse:
    status_code = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def aiter_bytes(self, _chunk_size):
        await asyncio.sleep(1)
        yield _glb_header()


class _SlowClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def stream(self, *_args, **_kwargs):
        return _SlowResponse()


class ViewerArtifactReviewRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_optional_glb_fetch_has_complete_transfer_deadline(self):
        main._GLB_FETCH_BACKOFF_UNTIL.clear()
        loop = asyncio.get_running_loop()
        started = loop.time()
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main.httpx,
            "AsyncClient",
            return_value=_SlowClient(),
        ):
            result = await main._get_cached_glb(
                "task",
                "https://worker/slow.glb",
                "prepared_viewer",
                profile="optimized",
                timeout_seconds=0.03,
                failure_backoff_seconds=30.0,
            )
        self.assertIsNone(result)
        self.assertLess(loop.time() - started, 0.3)

    def test_task_counts_exclude_viewer_artifacts_without_underflowing(self):
        outputs = [f"https://worker/result-{index}.bin" for index in range(6)]
        ready = outputs[:5]
        task = SimpleNamespace(
            output_urls=outputs + [
                "https://worker/task_model_prepared_viewer.glb",
                "https://worker/task_all_animations_viewer.glb",
            ],
            ready_urls=ready + [
                "https://worker/task_model_prepared_viewer.glb",
                "https://worker/task_all_animations_viewer.glb",
            ],
            total_count=8,
            ready_count=7,
        )
        self.assertEqual(main._downloadable_task_counts(task, outputs, ready), (5, 6))
        task.total_count = 6
        task.ready_count = 5
        self.assertEqual(main._downloadable_task_counts(task, outputs, ready), (5, 6))


if __name__ == "__main__":
    unittest.main()
