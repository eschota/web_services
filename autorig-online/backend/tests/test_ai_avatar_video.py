import asyncio
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI, Header

import ai_avatar_video
from ai_avatar_video import build_avatar_video_router
from ai_avatars import AvatarDraft, AvatarOwner, AvatarStore


TEMP_ROOT = pathlib.Path(__file__).resolve().parents[3] / ".codex_tmp"


def _temporary_directory():
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=TEMP_ROOT)


def _reference(name, sha, role="body"):
    return {
        "asset_id": "asset_" + name,
        "role": role,
        "media_type": "image",
        "canonical_url": f"https://autorig.online/api/ai/avatar-assets/{name}/{sha}.png",
        "source_url": f"https://autorig.online/dev/api/scratch/{name}.png",
        "sha256": sha,
        "width": 960,
        "height": 540,
    }


def _draft(name, appearance, wardrobe, sha):
    return AvatarDraft.model_validate({
        "display_name": name,
        "identity_prompt": name + " private identity wording",
        "appearance": appearance,
        "wardrobe": wardrobe,
        "references": [_reference(name.lower(), sha)],
    })


class _Farm:
    def __init__(self):
        self.payloads = []

    def factory(self, *args, **kwargs):
        farm = self

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, exc_type, exc, tb): return False

            async def post(self, url, *, json, timeout):
                farm.payloads.append({"url": url, "json": json, "timeout": timeout})
                request = httpx.Request("POST", url)
                number = len(farm.payloads)
                return httpx.Response(202, request=request, json={
                    "task_id": f"wan-task-{number}",
                    "output_url": f"https://autorig.online/renderfin/render/wan-{number}.mp4",
                })

        return Client()

    @property
    def httpx_module(self):
        return SimpleNamespace(AsyncClient=self.factory, HTTPError=httpx.HTTPError)


class _Cache:
    def __init__(self):
        self.calls = []
        self.saved = {}

    async def run_cached(self, service, payload, submit, *, namespace):
        self.calls.append({"service": service, "payload": payload, "namespace": namespace})
        key = json.dumps(payload, sort_keys=True)
        if payload.get("seed") not in (None, 0, "0", "") and key in self.saved:
            return {**self.saved[key], "cache_hit_bool": True}
        result = dict(await submit())
        if payload.get("seed") not in (None, 0, "0", ""):
            self.saved[key] = result
        return {**result, "cache_hit_bool": False}


class AvatarVideoTests(unittest.TestCase):
    def _setup(self, folder):
        store = AvatarStore(pathlib.Path(folder) / "avatars")
        alice = AvatarOwner(owner_type="user", owner_id="alice@example.com")
        bob = AvatarOwner(owner_type="user", owner_id="bob@example.com")
        maya_v1 = store.create(alice, _draft(
            "Maya", "short black bob, oval face", "navy jacket", "1" * 64))
        store.update(maya_v1.avatar_id, alice, _draft(
            "Maya", "long auburn hair", "green dress", "2" * 64))
        leo = store.create(alice, _draft(
            "Leo", "dark wavy hair, angular face", "grey coat", "3" * 64))
        return store, alice, bob, maya_v1, leo

    def _app(self, store, owner_value=None):
        async def owner(x_test_owner: str = Header(default="")):
            if owner_value is not None:
                return owner_value
            return AvatarOwner(owner_type="user", owner_id=x_test_owner)

        app = FastAPI()
        app.include_router(build_avatar_video_router(owner, store=store))
        return app

    def _live_service(self, service_id):
        if service_id == "avatar_video":
            return {"id": service_id, "status": "live"}
        return None

    def test_exact_owner_version_payload_and_prompt_separation(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, _alice, _bob, maya_v1, _leo = self._setup(folder)
                app = self._app(store)
                farm, cache = _Farm(), _Cache()
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service), \
                     patch.object(ai_avatar_video, "httpx", farm.httpx_module), \
                     patch.object(ai_avatar_video.ai_request_cache, "run_cached",
                                  new=cache.run_cached):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        response = await client.post("/api/ai/avatar-video", json={
                            "avatar": f"{maya_v1.avatar_id}@1",
                            "control_video_url": "https://pvs1.microstock.plus/path/driver.mp4",
                            "prompt": "turns left; raises both hands",
                        }, headers={"X-Test-Owner": "alice@example.com"})
                self.assertEqual(response.status_code, 200, response.text)
                payload = farm.payloads[0]["json"]
                self.assertNotIn("type", payload)
                self.assertEqual(payload["work_flow"], ai_avatar_video.WORKFLOW)
                self.assertEqual(payload["checkpoint"], ai_avatar_video.CHECKPOINT)
                self.assertEqual(payload["image_url"], maya_v1.references[0].canonical_url)
                self.assertEqual(payload["pose_prompt"], "turns left; raises both hands")
                self.assertIn("short black bob, oval face", payload["prompt"])
                self.assertIn("navy jacket", payload["prompt"])
                self.assertNotIn("turns left", payload["prompt"])
                self.assertNotIn("private identity wording", payload["prompt"])
                self.assertNotIn("short black bob", payload["pose_prompt"])
                self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                                 (960, 540))
                self.assertEqual((payload["frame_count"], payload["steps"], payload["cfg"]),
                                 (97, 6, 1.0))
                self.assertEqual((payload["sampler"], payload["scheduler"]), ("lcm", "simple"))
                self.assertEqual(response.json()["avatar_versions_array"][0]["version"], 1)
                cache_call = cache.calls[0]
                self.assertEqual(cache_call["namespace"], "avatar-video-wan2-v1")
                self.assertEqual(cache_call["payload"]["owner"]["owner_id"], "alice@example.com")
                self.assertEqual(cache_call["payload"]["avatar_versions"][0]["version"], 1)
                self.assertEqual(cache_call["payload"]["control_video_url"],
                                 "https://pvs1.microstock.plus/path/driver.mp4")

        asyncio.run(scenario())

    def test_cross_owner_and_missing_version_are_hidden(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, _alice, _bob, maya, _leo = self._setup(folder)
                app = self._app(store)
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        body = {"avatar": maya.avatar_id,
                                "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4"}
                        foreign = await client.post("/api/ai/avatar-video", json=body,
                                                    headers={"X-Test-Owner": "bob@example.com"})
                        missing = await client.post("/api/ai/avatar-video",
                            json={**body, "avatar": f"{maya.avatar_id}@99"},
                            headers={"X-Test-Owner": "alice@example.com"})
                self.assertEqual(foreign.status_code, 404)
                self.assertEqual(missing.status_code, 404)

        asyncio.run(scenario())

    def test_invalid_driver_and_oversized_padded_resolution_never_submit(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, alice, _bob, maya, _leo = self._setup(folder)
                app = self._app(store, alice)
                farm = _Farm()
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service), \
                     patch.object(ai_avatar_video, "httpx", farm.httpx_module):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        invalid = await client.post("/api/ai/avatar-video", json={
                            "avatar": maya.avatar_id,
                            "control_video_url": "https://evil.example/driver.mp4",
                        })
                        too_large = await client.post("/api/ai/avatar-video", json={
                            "avatar": maya.avatar_id,
                            "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4",
                            "width": 1024, "height": 540,
                        })
                self.assertEqual(invalid.status_code, 400)
                self.assertEqual(too_large.status_code, 400)
                self.assertIn("will not silently resize", too_large.text)
                self.assertEqual(farm.payloads, [])

        asyncio.run(scenario())

    def test_second_avatar_requires_composite_keyframe(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, alice, _bob, maya, leo = self._setup(folder)
                app = self._app(store, alice)
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        response = await client.post("/api/ai/avatar-video", json={
                            "avatar": maya.avatar_id, "avatar_secondary": leo.avatar_id,
                            "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4",
                        })
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json()["detail"]["error_string"],
                                 "composite_keyframe_required")

        asyncio.run(scenario())

    def test_frame_and_strength_bounds_are_rejected_by_the_request_schema(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, alice, _bob, maya, _leo = self._setup(folder)
                app = self._app(store, alice)
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        common = {"avatar": maya.avatar_id,
                                  "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4"}
                        bad_frames = await client.post(
                            "/api/ai/avatar-video", json={**common, "frame_count": 10})
                        bad_strength = await client.post(
                            "/api/ai/avatar-video", json={**common, "control_strength": 1.1})
                        unsupported_cfg = await client.post(
                            "/api/ai/avatar-video", json={**common, "cfg": 7})
                        odd_width = await client.post(
                            "/api/ai/avatar-video", json={**common, "width": 959})
                self.assertEqual(
                    (bad_frames.status_code, bad_strength.status_code,
                     unsupported_cfg.status_code, odd_width.status_code),
                    (422, 422, 422, 422),
                )
                self.assertIn("extra_forbidden", unsupported_cfg.text)
                self.assertIn("must be even", odd_width.text)

        asyncio.run(scenario())

    def test_fixed_seed_caches_but_zero_seed_submits_twice(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, alice, _bob, maya, _leo = self._setup(folder)
                app = self._app(store, alice)
                farm, cache = _Farm(), _Cache()
                with patch.object(ai_avatar_video.ai_services, "service",
                                  side_effect=self._live_service), \
                     patch.object(ai_avatar_video, "httpx", farm.httpx_module), \
                     patch.object(ai_avatar_video.ai_request_cache, "run_cached",
                                  new=cache.run_cached):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        body = {"avatar": maya.avatar_id,
                                "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4"}
                        fixed1 = await client.post("/api/ai/avatar-video", json={**body, "seed": 42})
                        fixed2 = await client.post("/api/ai/avatar-video", json={**body, "seed": 42})
                        random1 = await client.post("/api/ai/avatar-video", json={**body, "seed": 0})
                        random2 = await client.post("/api/ai/avatar-video", json={**body, "seed": 0})
                self.assertFalse(fixed1.json()["cache_hit_bool"])
                self.assertTrue(fixed2.json()["cache_hit_bool"])
                self.assertFalse(random1.json()["cache_hit_bool"])
                self.assertFalse(random2.json()["cache_hit_bool"])
                self.assertEqual(len(farm.payloads), 3)

        asyncio.run(scenario())

    def test_service_guard_is_fail_closed_until_promoted(self):
        async def scenario():
            with _temporary_directory() as folder:
                store, alice, _bob, maya, _leo = self._setup(folder)
                app = self._app(store, alice)
                with patch.object(ai_avatar_video.ai_services, "service", return_value=None):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="https://testserver"
                    ) as client:
                        response = await client.post("/api/ai/avatar-video", json={
                            "avatar": maya.avatar_id,
                            "control_video_url": "https://pvs1.microstock.plus/a/driver.mp4",
                        })
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json()["detail"]["error_string"],
                                 "avatar_video_not_live")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
