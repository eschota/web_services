import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main
from database import Task


TASK_ID = "3d2c282a-1475-4856-b51c-32330abf530d"
GUID = "226c54c6-8570-410c-b3cf-ddad22bd4e5b"


def _valid_glb() -> bytes:
    payload = json.dumps(
        {
            "asset": {"version": "2.0"},
            "meshes": [{"primitives": [{"attributes": {"POSITION": 0}}]}],
            "animations": [{"channels": [{"sampler": 0, "target": {"node": 0, "path": "rotation"}}], "samplers": [{"input": 1, "output": 2}]}],
        },
        separators=(",", ":"),
    ).encode("utf-8")
    payload += b" " * ((4 - len(payload) % 4) % 4)
    chunk = len(payload).to_bytes(4, "little") + b"JSON" + payload
    total = 12 + len(chunk)
    return b"glTF" + (2).to_bytes(4, "little") + total.to_bytes(4, "little") + chunk


def _request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": []})


def _task(**overrides):
    values = {
        "id": TASK_ID,
        "guid": GUID,
        "input_type": "t_pose",
        "created_at": None,
        "ready_urls": [],
        "output_urls": [],
        "worker_api": None,
        "viewer_prepared_glb_url": None,
        "viewer_animations_glb_url": None,
        "fbx_glb_output_url": None,
        "fbx_glb_ready": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class ViewerArtifactContractTests(unittest.IsolatedAsyncioTestCase):
    def test_viewer_headers_are_inline_not_download_attachments(self):
        headers = main._glb_viewer_headers("optimized")
        self.assertEqual(headers["Content-Disposition"], "inline")
        self.assertEqual(headers["X-AutoRig-Viewer-Profile"], "optimized")

    def test_task_model_has_nullable_viewer_url_columns(self):
        self.assertTrue(Task.viewer_prepared_glb_url.nullable)
        self.assertTrue(Task.viewer_animations_glb_url.nullable)

    def test_download_inventory_excludes_preview_only_glbs(self):
        viewer_prepared = f"https://worker/{GUID}/{GUID}_model_prepared_viewer.glb"
        viewer_animations = f"https://worker/{GUID}/{GUID}_all_animations_viewer.glb"
        downloadable = f"https://worker/{GUID}/{GUID}_model_prepared.glb"
        task = _task(
            ready_urls=[viewer_prepared, viewer_animations, downloadable],
            output_urls=[viewer_prepared, viewer_animations, downloadable],
        )
        self.assertEqual(main._task_primary_download_urls(task), [downloadable])

    async def test_animations_endpoint_prefers_valid_optimized_cache(self):
        task = _task(
            viewer_animations_glb_url="https://worker.invalid/optimized.glb",
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main,
            "get_task_by_id",
            AsyncMock(return_value=task),
        ):
            cache = Path(tmp) / f"{TASK_ID}_animations_viewer.glb"
            cache.write_bytes(_valid_glb())
            response = await main.api_proxy_animations_glb(
                TASK_ID,
                _request(),
                db=None,
            )
        self.assertEqual(response.headers["x-autorig-viewer-profile"], "optimized")
        self.assertEqual(Path(response.path), cache)

    async def test_prepared_endpoint_prefers_optimized_url_before_original_cache(self):
        task = _task(
            viewer_prepared_glb_url="https://worker.invalid/prepared-viewer.glb",
        )
        optimized_response = main.Response(
            content=_valid_glb(),
            media_type="model/gltf-binary",
            headers=main._glb_viewer_headers("optimized"),
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
            AsyncMock(return_value=optimized_response),
        ) as get_cached:
            (Path(tmp) / f"{TASK_ID}_prepared.glb").write_bytes(_valid_glb())
            response = await main.api_proxy_prepared_glb(TASK_ID, db=None)
        self.assertEqual(response.headers["x-autorig-viewer-profile"], "optimized")
        get_cached.assert_awaited_once_with(
            TASK_ID,
            task.viewer_prepared_glb_url,
            "prepared_viewer",
            profile="optimized",
            timeout_seconds=6.0,
            failure_backoff_seconds=30.0,
        )

    async def test_invalid_original_prepared_cache_is_not_served(self):
        task = _task()
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main,
            "get_task_by_id",
            AsyncMock(return_value=task),
        ):
            (Path(tmp) / f"{TASK_ID}_prepared.glb").write_bytes(b"not a glb")
            with self.assertRaises(HTTPException) as raised:
                await main.api_proxy_prepared_glb(TASK_ID, db=None)
        self.assertEqual(raised.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
