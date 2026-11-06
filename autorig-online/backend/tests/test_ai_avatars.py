import asyncio
import json
import pathlib
import tempfile
import unittest

import httpx
from fastapi import FastAPI, Header

from ai_avatars import (
    AvatarDraft,
    AvatarOwner,
    AvatarStore,
    AvatarStoreError,
    build_avatar_router,
    resolve_avatar_identity,
)


SHA_A = "a" * 64
SHA_B = "b" * 64


def draft(name="Alex", url="https://autorig.online/avatar-assets/alex-face.png", sha=SHA_A):
    return AvatarDraft.model_validate({
        "display_name": name,
        "identity_prompt": "adult fictional person, oval face, grey eyes",
        "appearance": "shoulder-length dark hair",
        "wardrobe": "navy jacket",
        "references": [{
            "asset_id": "asset_alex_face",
            "role": "face",
            "media_type": "image",
            "canonical_url": url,
            "source_url": "https://example.com/original.png",
            "sha256": sha,
            "width": 960,
            "height": 540,
        }],
        "provenance": {
            "source_kind": "generated",
            "source_task_ids": ["task-1"],
            "source_model": "example-model",
            "parameters_sha256": "c" * 64,
        },
    })


class AvatarStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.store = AvatarStore(self.root)
        self.alice = AvatarOwner(owner_type="user", owner_id="alice@example.com")
        self.bob = AvatarOwner(owner_type="user", owner_id="bob@example.com")

    def tearDown(self):
        self.temp.cleanup()

    def test_persists_and_update_appends_immutable_version(self):
        first = self.store.create(self.alice, draft())
        second = self.store.update(first.avatar_id, self.alice, draft(name="Alex in blue"))

        self.assertEqual(second.version, 2)
        self.assertEqual(self.store.get(first.avatar_id, self.alice, 1).display_name, "Alex")
        self.assertEqual(self.store.get(first.avatar_id, self.alice).display_name, "Alex in blue")
        reopened = AvatarStore(self.root)
        self.assertEqual(reopened.get(first.avatar_id, self.alice, 2).version, 2)

        raw = json.loads((self.root / f"{first.avatar_id}.json").read_text("utf-8"))
        self.assertEqual([item["version"] for item in raw["versions"]], [1, 2])
        self.assertEqual(raw["versions"][0]["references"][0]["asset_id"],
                         "asset_alex_face")
        self.assertFalse(list(self.root.glob("*.tmp")))

    def test_owner_isolation_hides_presence_and_list(self):
        profile = self.store.create(self.alice, draft())
        self.assertEqual(len(self.store.list(self.alice)), 1)
        self.assertEqual(self.store.list(self.bob), [])
        with self.assertRaises(AvatarStoreError) as raised:
            self.store.get(profile.avatar_id, self.bob)
        self.assertEqual((raised.exception.code, raised.exception.status_code),
                         ("avatar_not_found", 404))

    def test_rejects_bad_id_version_and_unsafe_or_ephemeral_reference(self):
        profile = self.store.create(self.alice, draft())
        with self.assertRaises(AvatarStoreError):
            self.store.get("../../secret", self.alice)
        with self.assertRaises(AvatarStoreError) as raised:
            self.store.get(profile.avatar_id, self.alice, 99)
        self.assertEqual(raised.exception.code, "avatar_version_not_found")

        for url in (
            "http://example.com/face.png",
            "https://127.0.0.1/face.png",
            "https://user:password@example.com/face.png",
            "file:///etc/passwd",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                draft(url=url)

    def test_reference_checksum_and_required_identity_material(self):
        with self.assertRaises(ValueError):
            draft(sha="short")
        payload = draft().model_dump()
        payload["references"][0]["role"] = "style"
        with self.assertRaises(ValueError):
            AvatarDraft.model_validate(payload)

    def test_adapter_resolution_is_explicit_and_family_checked(self):
        candidate = self.store.create(self.alice, draft())
        # Candidate/reference conditioning is valid, but not a trained adapter.
        self.assertEqual(resolve_avatar_identity(
            self.store, candidate.avatar_id, self.alice
        ).version, 1)
        with self.assertRaises(AvatarStoreError) as raised:
            resolve_avatar_identity(
                self.store, candidate.avatar_id, self.alice,
                pipeline_family="flux2", require_adapter=True,
            )
        self.assertEqual(raised.exception.code, "avatar_adapter_not_ready")

        ready_payload = draft(name="Ready").model_dump()
        ready_payload["adapter"] = {
            "status": "ready",
            "pipeline_family": "flux2",
            "artifact_url": "https://autorig.online/avatar-assets/alex.safetensors",
            "sha256": SHA_B,
            "trigger_token": "av_alex",
        }
        ready = self.store.create(self.alice, AvatarDraft.model_validate(ready_payload))
        with self.assertRaises(AvatarStoreError) as mismatch:
            resolve_avatar_identity(
                self.store, ready.avatar_id, self.alice,
                pipeline_family="ltx23", require_adapter=True,
            )
        self.assertEqual(mismatch.exception.code, "avatar_adapter_incompatible")


class AvatarApiTests(unittest.TestCase):
    def test_api_create_list_read_update_and_cross_owner_privacy(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder:
                store = AvatarStore(pathlib.Path(folder))

                async def owner(x_test_owner: str = Header(...)):
                    return AvatarOwner(owner_type="user", owner_id=x_test_owner)

                app = FastAPI()
                app.include_router(build_avatar_router(owner, store=store))
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="https://testserver"
                ) as client:
                    created = await client.post(
                        "/api/ai/avatars", json=draft().model_dump(mode="json"),
                        headers={"X-Test-Owner": "alice@example.com"},
                    )
                    self.assertEqual(created.status_code, 201, created.text)
                    avatar_id = created.json()["avatar_object"]["avatar_id"]

                    listed = await client.get(
                        "/api/ai/avatars",
                        headers={"X-Test-Owner": "alice@example.com"},
                    )
                    self.assertEqual([item["avatar_id"] for item in
                                      listed.json()["avatars_array"]], [avatar_id])

                    hidden = await client.get(
                        f"/api/ai/avatars/{avatar_id}",
                        headers={"X-Test-Owner": "bob@example.com"},
                    )
                    self.assertEqual(hidden.status_code, 404)
                    bob_list = await client.get(
                        "/api/ai/avatars",
                        headers={"X-Test-Owner": "bob@example.com"},
                    )
                    self.assertEqual(bob_list.json()["avatars_array"], [])

                    changed = draft(name="Alex v2").model_dump(mode="json")
                    updated = await client.patch(
                        f"/api/ai/avatars/{avatar_id}", json=changed,
                        headers={"X-Test-Owner": "alice@example.com"},
                    )
                    self.assertEqual(updated.json()["avatar_object"]["version"], 2)
                    old = await client.get(
                        f"/api/ai/avatars/{avatar_id}/versions/1",
                        headers={"X-Test-Owner": "alice@example.com"},
                    )
                    self.assertEqual(old.json()["avatar_object"]["display_name"], "Alex")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
