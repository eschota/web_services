import asyncio
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI, Header

import ai_avatar_render
from ai_avatar_render import build_avatar_render_router
from ai_avatars import AvatarDraft, AvatarOwner, AvatarStore


TEMP_ROOT = pathlib.Path(__file__).resolve().parents[3] / ".codex_tmp"


def _temporary_directory():
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=TEMP_ROOT)


def _reference(asset_id, url, sha, *, role="face", media_type="image"):
    item = {
        "asset_id": asset_id,
        "role": role,
        "media_type": media_type,
        "canonical_url": url,
        "source_url": "https://source.example/original",
        "sha256": sha,
        "note": "",
    }
    if media_type == "image":
        item.update(width=960, height=540)
    else:
        item["duration_seconds"] = 2.0
    return item


def _draft(name, identity, references):
    return AvatarDraft.model_validate({
        "display_name": name,
        "identity_prompt": identity,
        "appearance": f"{name} appearance",
        "wardrobe": f"{name} wardrobe",
        "references": references,
    })


class _Farm:
    def __init__(self, *, malformed_json=False):
        self.payloads = []
        self.malformed_json = malformed_json

    def client_factory(self, *args, **kwargs):
        farm = self

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, url, *, json, timeout):
                farm.payloads.append({"url": url, "json": json, "timeout": timeout})
                number = len(farm.payloads)
                request = httpx.Request("POST", url)
                accepted = [] if farm.malformed_json else {
                    "task_id": f"avatar-task-{number}",
                    "output_url": f"https://autorig.online/renderfin/render/avatar-{number}.png",
                }
                return httpx.Response(200, request=request, json=accepted)

        return Client()

    @property
    def httpx_module(self):
        return SimpleNamespace(
            AsyncClient=self.client_factory,
            HTTPError=httpx.HTTPError,
        )


class _Cache:
    """Small cache double that exposes the render route's seed contract."""

    def __init__(self):
        self.responses = {}
        self.calls = []

    async def run_cached(self, service, payload, submit, *, namespace):
        self.calls.append({"service": service, "payload": payload, "namespace": namespace})
        seed = payload.get("seed")
        key = json.dumps(payload, sort_keys=True)
        if seed not in (None, "", 0, "0") and key in self.responses:
            return {**self.responses[key], "cache_hit_bool": True}
        result = dict(await submit())
        if seed not in (None, "", 0, "0"):
            self.responses[key] = result
        return {**result, "cache_hit_bool": False}


class AvatarRenderTests(unittest.TestCase):
    def test_owner_exact_version_reference_order_and_submission_contract(self):
        async def scenario():
            with _temporary_directory() as folder:
                root = pathlib.Path(folder)
                store = AvatarStore(root / "avatars")
                alice = AvatarOwner(owner_type="user", owner_id="alice@example.com")
                bob = AvatarOwner(owner_type="user", owner_id="bob@example.com")

                first_v1 = store.create(alice, _draft("Mira", "Mira v1", [
                    _reference("asset_mira_body", "https://autorig.online/dev/api/scratch/mira-body.png",
                               "1" * 64, role="body"),
                    _reference("asset_mira_face", "https://autorig.online/dev/api/scratch/mira-face-v1.png",
                               "2" * 64, role="face"),
                ]))
                store.update(first_v1.avatar_id, alice, _draft("Mira", "Mira v2", [
                    _reference("asset_mira_face_v2", "https://autorig.online/dev/api/scratch/mira-face-v2.png",
                               "3" * 64, role="face"),
                ]))
                second = store.create(alice, _draft("Noah", "Noah identity", [
                    _reference("asset_noah_face", "https://autorig.online/dev/api/scratch/noah-face.png",
                               "4" * 64, role="face"),
                ]))

                async def owner(x_test_owner: str = Header(...)):
                    return AvatarOwner(owner_type="user", owner_id=x_test_owner)

                app = FastAPI()
                app.include_router(build_avatar_render_router(owner, store=store))
                farm = _Farm()
                cache = _Cache()
                transport = httpx.ASGITransport(app=app)
                with patch.object(ai_avatar_render, "httpx", farm.httpx_module), patch.object(
                                      ai_avatar_render.ai_request_cache, "run_cached",
                                      new=cache.run_cached):
                    async with httpx.AsyncClient(
                        transport=transport, base_url="https://testserver"
                    ) as client:
                        body = {
                            "avatar": f"{first_v1.avatar_id}@1",
                            "avatar_secondary": f"{second.avatar_id}@1",
                            "image_url": "https://autorig.online/dev/api/scratch/scene.png",
                            "prompt": "They cross a rain-soaked street together",
                            "width": 832,
                            "height": 1216,
                            "seed": 12345,
                        }
                        result = await client.post(
                            "/api/ai/avatar-image", json=body,
                            headers={"X-Test-Owner": "alice@example.com"},
                        )
                        self.assertEqual(result.status_code, 200, result.text)
                        response = result.json()
                        self.assertEqual(response["conditioning_string"],
                                         "flux2_ordered_references")
                        self.assertEqual(response["avatar_versions_array"], [
                            {"avatar_id": first_v1.avatar_id, "version": 1,
                             "reference_sha256": "2" * 64},
                            {"avatar_id": second.avatar_id, "version": 1,
                             "reference_sha256": "4" * 64},
                        ])

                        submitted = farm.payloads[0]
                        self.assertTrue(submitted["url"].endswith("/api-render"))
                        payload = submitted["json"]
                        self.assertEqual(payload["work_flow"],
                                         "gen_image_flux2_avatar.json")
                        self.assertEqual(payload["reference_image_urls"], [
                            "https://autorig.online/dev/api/scratch/mira-face-v1.png",
                            "https://autorig.online/dev/api/scratch/noah-face.png",
                            "https://autorig.online/dev/api/scratch/scene.png",
                        ])
                        self.assertEqual((payload["main_size_width"],
                                          payload["main_size_height"]), (832, 1216))
                        self.assertEqual(payload["noise_seed"], 12345)
                        self.assertIn("Mira v1", payload["prompt"])
                        self.assertNotIn("Mira v2", payload["prompt"])
                        self.assertLess(payload["prompt"].find("character 1"),
                                        payload["prompt"].find("character 2"))
                        final = payload["prompt"].find("FINAL CANONICAL CHARACTER CONSTRAINTS")
                        self.assertGreater(final, payload["prompt"].find("Scene instruction:"))
                        self.assertIn("scene reference controls only pose, composition, camera, and background",
                                      payload["prompt"])
                        self.assertIn("Never borrow or blend the source actor's face, hair, skin",
                                      payload["prompt"])
                        self.assertIn("Canonical identity: Mira v1", payload["prompt"][final:])
                        self.assertIn("Canonical face, hair, and appearance: Mira appearance",
                                      payload["prompt"][final:])
                        self.assertIn("Canonical wardrobe: Mira wardrobe", payload["prompt"][final:])

                        denied = await client.post(
                            "/api/ai/avatar-image", json=body,
                            headers={"X-Test-Owner": "bob@example.com"},
                        )
                        self.assertEqual(denied.status_code, 404)
                        self.assertEqual(len(farm.payloads), 1)

        asyncio.run(scenario())

    def test_untrusted_scene_urls_are_rejected_before_submission(self):
        async def scenario():
            with _temporary_directory() as folder:
                store = AvatarStore(pathlib.Path(folder) / "avatars")
                owner_value = AvatarOwner(owner_type="user", owner_id="owner@example.com")
                profile = store.create(owner_value, _draft("Ari", "Ari identity", [
                    _reference("asset_ari", "https://autorig.online/dev/api/scratch/ari.png", "7" * 64),
                ]))

                async def owner():
                    return owner_value

                app = FastAPI()
                app.include_router(build_avatar_render_router(owner, store=store))
                farm = _Farm()
                with patch.object(ai_avatar_render, "httpx", farm.httpx_module):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="https://testserver",
                    ) as client:
                        for scene_url in (
                            "https://external.example/scene.png",
                            "https://127.0.0.1/scene.png",
                        ):
                            result = await client.post("/api/ai/avatar-image", json={
                                "avatar": f"{profile.avatar_id}@1",
                                "image_url": scene_url,
                                "prompt": "street scene",
                                "seed": 8,
                            })
                            self.assertEqual(result.status_code, 400, result.text)
                            self.assertIn("Upload the scene reference", result.text)
                self.assertEqual(farm.payloads, [])

        asyncio.run(scenario())

    def test_non_object_farm_json_is_controlled_502(self):
        async def scenario():
            with _temporary_directory() as folder:
                store = AvatarStore(pathlib.Path(folder) / "avatars")
                owner_value = AvatarOwner(owner_type="user", owner_id="owner@example.com")
                profile = store.create(owner_value, _draft("Ari", "Ari identity", [
                    _reference("asset_ari", "https://autorig.online/dev/api/scratch/ari.png", "8" * 64),
                ]))

                async def owner():
                    return owner_value

                app = FastAPI()
                app.include_router(build_avatar_render_router(owner, store=store))
                farm = _Farm(malformed_json=True)
                with patch.object(ai_avatar_render, "httpx", farm.httpx_module):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="https://testserver",
                    ) as client:
                        result = await client.post("/api/ai/avatar-image", json={
                            "avatar": f"{profile.avatar_id}@1",
                            "prompt": "portrait",
                            "seed": 9,
                        })
                self.assertEqual(result.status_code, 502, result.text)
                self.assertIn("incomplete Avatar job", result.text)
                self.assertEqual(len(farm.payloads), 1)

        asyncio.run(scenario())

    def test_avatar_without_image_is_rejected_before_submission(self):
        async def scenario():
            with _temporary_directory() as folder:
                store = AvatarStore(pathlib.Path(folder) / "avatars")
                owner_value = AvatarOwner(owner_type="anon", owner_id="anon-1")
                profile = store.create(owner_value, _draft("Motion", "Motion identity", [
                    _reference("asset_motion", "https://autorig.online/dev/api/scratch/motion.mp4",
                               "5" * 64, role="face", media_type="video"),
                ]))

                async def owner():
                    return owner_value

                app = FastAPI()
                app.include_router(build_avatar_render_router(owner, store=store))
                farm = _Farm()
                with patch.object(ai_avatar_render, "httpx", farm.httpx_module):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="https://testserver",
                    ) as client:
                        result = await client.post("/api/ai/avatar-image", json={
                            "avatar": f"{profile.avatar_id}@1",
                            "prompt": "walking",
                            "seed": 7,
                        })
                self.assertEqual(result.status_code, 400)
                self.assertIn("needs an image", result.text)
                self.assertEqual(farm.payloads, [])

        asyncio.run(scenario())

    def test_fixed_seed_is_cached_but_zero_seed_submits_each_time(self):
        async def scenario():
            with _temporary_directory() as folder:
                root = pathlib.Path(folder)
                store = AvatarStore(root / "avatars")
                owner_value = AvatarOwner(owner_type="user", owner_id="owner@example.com")
                profile = store.create(owner_value, _draft("Ari", "Ari identity", [
                    _reference("asset_ari", "https://autorig.online/dev/api/scratch/ari.png",
                               "6" * 64),
                ]))

                async def owner():
                    return owner_value

                app = FastAPI()
                app.include_router(build_avatar_render_router(owner, store=store))
                farm = _Farm()
                cache = _Cache()
                with patch.object(ai_avatar_render, "httpx", farm.httpx_module), patch.object(
                                      ai_avatar_render.ai_request_cache, "run_cached",
                                      new=cache.run_cached):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="https://testserver",
                    ) as client:
                        common = {"avatar": f"{profile.avatar_id}@1", "prompt": "portrait"}
                        fixed_a = await client.post(
                            "/api/ai/avatar-image", json={**common, "seed": 99})
                        fixed_b = await client.post(
                            "/api/ai/avatar-image", json={**common, "seed": 99})
                        random_a = await client.post(
                            "/api/ai/avatar-image", json={**common, "seed": 0})
                        random_b = await client.post(
                            "/api/ai/avatar-image", json={**common, "seed": 0})

                self.assertEqual([fixed_a.status_code, fixed_b.status_code,
                                  random_a.status_code, random_b.status_code], [200] * 4)
                self.assertFalse(fixed_a.json()["cache_hit_bool"])
                self.assertTrue(fixed_b.json()["cache_hit_bool"])
                self.assertFalse(random_a.json()["cache_hit_bool"])
                self.assertFalse(random_b.json()["cache_hit_bool"])
                self.assertEqual(len(farm.payloads), 3)
                self.assertEqual([call["payload"]["seed"] for call in cache.calls],
                                 [99, 99, 0, 0])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
