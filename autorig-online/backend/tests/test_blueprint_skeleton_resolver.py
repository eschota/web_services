import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.responses import Response


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class BlueprintSkeletonResolverTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _task(*, task_id="task-1", guid="guid-1", ready_urls=None, output_urls=None, worker_api=None):
        return SimpleNamespace(
            id=task_id,
            guid=guid,
            input_type="animal",
            ready_urls=list(ready_urls or []),
            output_urls=list(output_urls or []),
            worker_api=worker_api,
        )

    async def test_arbitrary_url_order_selects_exact_canonical_skeleton(self):
        guid = "72a5b495-31e7-4037-b680-69dfaa755b21"
        canonical = f"https://worker.example/converter/glb/{guid}/{guid}_skeleton.json?download=1"
        task = self._task(
            guid=guid,
            ready_urls=[
                f"https://worker.example/converter/glb/{guid}/{guid}_dog_front_skeleton.json",
                f"https://worker.example/converter/glb/{guid}/{guid}_horse_back_skeleton.json",
                canonical,
            ],
        )

        resolved = await main._resolve_task_worker_file_url(task, main.BLUEPRINT_SKELETON_SUFFIX)

        self.assertEqual(resolved, canonical)

    async def test_variant_only_skeleton_is_unavailable(self):
        guid = "guid-variant-only"
        task = self._task(
            guid=guid,
            ready_urls=[f"https://worker.example/{guid}_dog_front_skeleton.json"],
        )

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / task.id
            cache_dir.mkdir(parents=True)
            (cache_dir / "dog_front_skeleton.json").write_text("{}", encoding="utf-8")
            with patch.object(main, "TASK_CACHE_DIR", Path(tmp)):
                with patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)):
                    with self.assertRaises(HTTPException) as raised:
                        await main._blueprint_file_response(
                            task.id,
                            main.BLUEPRINT_SKELETON_SUFFIX,
                            "skeleton.json",
                            "application/json",
                            db=None,
                        )

        self.assertEqual(raised.exception.status_code, 404)

    async def test_cached_skeleton_json_has_priority_and_no_store(self):
        task = self._task(
            ready_urls=["https://worker.example/guid-1/guid-1_skeleton.json"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / task.id
            cache_dir.mkdir(parents=True)
            canonical_cache = cache_dir / "skeleton.json"
            canonical_cache.write_text('{"source":"canonical-cache"}', encoding="utf-8")
            (cache_dir / "dog_front_skeleton.json").write_text("{}", encoding="utf-8")

            with patch.object(main, "TASK_CACHE_DIR", Path(tmp)):
                with patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)):
                    with patch.object(
                        main,
                        "_resolve_task_worker_file_url",
                        new=AsyncMock(side_effect=AssertionError("worker resolver must not run for cached skeleton")),
                    ):
                        response = await main._blueprint_file_response(
                            task.id,
                            main.BLUEPRINT_SKELETON_SUFFIX,
                            "skeleton.json",
                            "application/json",
                            db=None,
                        )

        self.assertEqual(Path(response.path), canonical_cache)
        self.assertEqual(response.headers["cache-control"], "no-store")

    async def test_proxied_skeleton_response_has_no_store(self):
        task = self._task()
        proxy_response = Response(headers={"Cache-Control": "public, max-age=86400"})

        with tempfile.TemporaryDirectory() as tmp, patch.object(main, "TASK_CACHE_DIR", Path(tmp)):
            with patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)):
                with patch.object(
                    main,
                    "_resolve_task_worker_file_url",
                    new=AsyncMock(return_value="https://worker.example/guid-1_skeleton.json"),
                ):
                    with patch.object(main, "_proxy_model_file", new=AsyncMock(return_value=proxy_response)):
                        response = await main._blueprint_file_response(
                            task.id,
                            main.BLUEPRINT_SKELETON_SUFFIX,
                            "skeleton.json",
                            "application/json",
                            db=None,
                        )

        self.assertEqual(response.headers["cache-control"], "no-store")

    async def test_rig_preview_keeps_existing_suffix_match_behavior(self):
        first = "https://worker.example/guid-1_horse_front_rig_preview.mp4"
        task = self._task(
            ready_urls=[first, "https://worker.example/guid-1_rig_preview.mp4"],
        )

        resolved = await main._resolve_task_worker_file_url(task, main.BLUEPRINT_RIG_PREVIEW_SUFFIX)

        self.assertEqual(resolved, first)


if __name__ == "__main__":
    unittest.main()
