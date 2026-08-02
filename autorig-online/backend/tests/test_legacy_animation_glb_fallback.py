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


TASK_ID = "d7c7f72f-202f-4832-80f8-7958fe8b970d"
GUID = "39405fb6-45f5-4575-bb1d-7ed0d3346ecb"


def glb(payload):
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    encoded += b" " * ((4 - len(encoded) % 4) % 4)
    chunk = len(encoded).to_bytes(4, "little") + b"JSON" + encoded
    total = 12 + len(chunk)
    return b"glTF" + (2).to_bytes(4, "little") + total.to_bytes(4, "little") + chunk


def playable_payload():
    return {
        "asset": {"version": "2.0"},
        "meshes": [{"primitives": [{"attributes": {"POSITION": 0}}]}],
        "animations": [
            {
                "name": "Animation",
                "channels": [{"sampler": 0, "target": {"node": 0, "path": "rotation"}}],
                "samplers": [{"input": 1, "output": 2}],
            }
        ],
    }


def task(**overrides):
    values = {
        "id": TASK_ID,
        "guid": GUID,
        "worker_api": "https://converter-f13.freestock.online/api-converter-glb",
        "input_type": "t_pose",
        "created_at": None,
        "ready_urls": [],
        "output_urls": [],
        "viewer_animations_glb_url": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def request():
    return Request({"type": "http", "method": "GET", "path": "/", "headers": []})


class LegacyAnimationGlbFallbackTests(unittest.IsolatedAsyncioTestCase):
    def test_semantic_validator_requires_mesh_and_playable_animation(self):
        cases = {
            "valid": (playable_payload(), True),
            "mesh_only": ({"asset": {"version": "2.0"}, "meshes": [{"primitives": [{}]}]}, False),
            "animation_only": (
                {
                    "asset": {"version": "2.0"},
                    "animations": [{"channels": [{}], "samplers": [{}]}],
                },
                False,
            ),
            "empty_primitives": (
                {
                    **playable_payload(),
                    "meshes": [{"primitives": []}],
                },
                False,
            ),
            "empty_channels": (
                {
                    **playable_payload(),
                    "animations": [{"channels": [], "samplers": [{}]}],
                },
                False,
            ),
        }
        with tempfile.TemporaryDirectory() as tmp:
            for name, (payload, expected) in cases.items():
                with self.subTest(name=name):
                    path = Path(tmp) / f"{name}.glb"
                    path.write_bytes(glb(payload))
                    self.assertEqual(main._validate_viewer_animation_glb_file(path), expected)

            malformed = Path(tmp) / "header-only.glb"
            malformed.write_bytes(b"glTF" + (2).to_bytes(4, "little") + (12).to_bytes(4, "little"))
            self.assertFalse(main._validate_viewer_animation_glb_file(malformed))

    def test_direct_url_is_exact_uuid_and_canonical_files_host(self):
        self.assertEqual(
            main._derive_legacy_animations_glb_url(task()),
            f"https://f13.freestock.online/{GUID}/{GUID}_all_animations.glb",
        )
        self.assertIsNone(main._derive_legacy_animations_glb_url(task(guid="../bad")))
        self.assertIsNone(main._derive_legacy_animations_glb_url(task(worker_api=None)))
        self.assertIsNone(
            main._derive_legacy_animations_glb_url(
                task(worker_api="file:///tmp/api-converter-glb")
            )
        )

    async def test_endpoint_uses_trusted_direct_fallback_when_ready_urls_omit_glb(self):
        response = main.Response(
            content=glb(playable_payload()),
            media_type="model/gltf-binary",
            headers=main._glb_viewer_headers("runtime"),
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main,
            "get_task_by_id",
            AsyncMock(return_value=task()),
        ), patch.object(
            main,
            "_get_cached_glb",
            AsyncMock(return_value=response),
        ) as get_cached:
            result = await main.api_proxy_animations_glb(TASK_ID, request(), db=None)

        self.assertEqual(result.headers["x-autorig-viewer-profile"], "runtime")
        get_cached.assert_awaited_once_with(
            TASK_ID,
            f"https://f13.freestock.online/{GUID}/{GUID}_all_animations.glb",
            "animations",
            profile="runtime",
            failure_backoff_seconds=30.0,
            validator=main._validate_viewer_animation_glb_file,
        )

    async def test_semantically_bad_cache_is_deleted_and_returns_controlled_404(self):
        no_source = task(worker_api=None)
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ), patch.object(
            main,
            "get_task_by_id",
            AsyncMock(return_value=no_source),
        ):
            cache = Path(tmp) / f"{TASK_ID}_animations.glb"
            cache.write_bytes(
                glb({"asset": {"version": "2.0"}, "meshes": [{"primitives": [{}]}]})
            )
            with self.assertRaises(HTTPException) as raised:
                await main.api_proxy_animations_glb(TASK_ID, request(), db=None)
            self.assertFalse(cache.exists())

        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(raised.exception.detail, "Animations GLB is not available for this task")


if __name__ == "__main__":
    unittest.main()
