"""/api/upscale and /api/upscale2x carry the M+ tier into the Renderfin job (2026-11-07)."""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_enhance_api  # noqa: E402
import ai_request_cache  # noqa: E402

KEY = "m" * 32


class FakeRequest:
    def __init__(self, headers):
        self.headers = {k.lower(): v for k, v in headers.items()}


MPLUS = {"X-Real-IP": "5.6.7.8", "X-Autorig-Priority": "mplus", "X-Priority-Key": KEY}


class TierTests(unittest.TestCase):
    def setUp(self):
        self.sent = []

        async def fake_run(service, payload, wait_seconds, produces="image"):
            self.sent.append((service, dict(payload)))
            return {"success_bool": True, "task_id_string": "t1", "image_url_string": "u", "video_url_string": "v"}

        async def fake_size(client, url):
            return 1000, 800

        async def no_cache(service, payload, callback, namespace="v1"):
            return {**(await callback()), "cache_hit_bool": False}

        self.patches = [mock.patch.object(ai_enhance_api, "_run", side_effect=fake_run),
                        mock.patch.object(ai_enhance_api, "_source_size", side_effect=fake_size),
                        mock.patch.object(ai_request_cache, "run_cached", side_effect=no_cache),
                        mock.patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": KEY})]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def test_mplus_upscale_is_queued_as_mplus(self):
        body = ai_enhance_api.UpscaleRequest(image_url="https://x.example.com/a.jpg", scale=2)
        asyncio.run(ai_enhance_api.api_upscale(FakeRequest(MPLUS), body))
        self.assertEqual(self.sent[-1][1]["priority_class"], "mplus")

    def test_mplus_upscale2x_image_and_video(self):
        asyncio.run(ai_enhance_api.api_upscale2x(FakeRequest(MPLUS), ai_enhance_api.Upscale2xRequest(
            image_url="https://x.example.com/a.png")))
        self.assertEqual(self.sent[-1][1]["priority_class"], "mplus")
        asyncio.run(ai_enhance_api.api_upscale2x(FakeRequest(MPLUS), ai_enhance_api.Upscale2xRequest(
            video_url="https://x.example.com/a.mp4")))
        self.assertEqual(self.sent[-1][1]["type"], "upscale_video_x2")
        self.assertEqual(self.sent[-1][1]["priority_class"], "mplus")

    def test_without_a_valid_key_no_tier(self):
        for headers in ({"X-Real-IP": "5.6.7.8"}, dict(MPLUS, **{"X-Priority-Key": "w" * 32})):
            asyncio.run(ai_enhance_api.api_upscale(FakeRequest(headers), ai_enhance_api.UpscaleRequest(
                image_url="https://x.example.com/a.jpg")))
            self.assertNotIn("priority_class", self.sent[-1][1])

    def test_tier_is_not_in_the_cache_key(self):
        seen = []

        async def capture(service, payload, callback, namespace="v1"):
            seen.append(dict(payload))
            return {**(await callback()), "cache_hit_bool": False}

        with mock.patch.object(ai_request_cache, "run_cached", side_effect=capture):
            asyncio.run(ai_enhance_api.api_upscale(FakeRequest(MPLUS), ai_enhance_api.UpscaleRequest(
                image_url="https://x.example.com/a.jpg")))
            asyncio.run(ai_enhance_api.api_upscale(FakeRequest({}), ai_enhance_api.UpscaleRequest(
                image_url="https://x.example.com/a.jpg")))
        self.assertEqual(seen[0], seen[1])
        self.assertNotIn("priority_class", seen[0])


if __name__ == "__main__":
    unittest.main()
