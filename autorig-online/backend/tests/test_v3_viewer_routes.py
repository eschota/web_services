import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace
from fastapi import HTTPException

from v3_viewer_routes import V3ManifestError, _can_access_task, _load_manifest, _task_dir, validate_v3_manifest


def _manifest(task_id, filename, payload):
    digest = hashlib.sha256(payload).hexdigest()
    stages = [
        {"name": "source", "status": "complete", "duration_ms": None, "receipt_sha256": "d" * 64},
        {"name": "c1_surface", "status": "complete", "duration_ms": 3, "receipt_sha256": "c" * 64},
    ]
    stages.extend(
        {"name": name, "status": "unavailable", "duration_ms": None}
        for name in ("c2_solid", "c3_thin", "c4_graph", "r1_bones", "s1_owner", "s2_weights", "s3_skin", "final_deformation")
    )
    return {
        "schema": "autorig.v3.viewer-manifest/1",
        "task_id": task_id,
        "build": {
            "version": "test",
            "source_sha256": "a" * 64,
            "manifest_input_sha256": "b" * 64,
        },
        "model": {"input_type": "animal", "unsafe_url": "https://private.invalid"},
        "stages": stages,
        "artifacts": [{"name": "surface", "type": "voxel_points", "stage": "c1_surface", "file": filename, "sha256": digest, "bytes": len(payload), "points": 1}],
    }


class V3ViewerManifestTests(unittest.TestCase):
    def test_hash_bound_manifest_is_accepted(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b'{"positions":[0,0,0]}'
            (root / "surface.json").write_bytes(payload)
            clean = validate_v3_manifest(_manifest(task_id, "surface.json", payload), task_id=task_id, task_dir=root)
            self.assertEqual(clean["artifacts"][0]["url"], f"/api/v3/task/{task_id}/artifact/surface")
            self.assertNotIn("unsafe_url", clean["model"])
            self.assertEqual(clean["artifacts"][0]["stage_receipt_sha256"], "c" * 64)

    def test_changed_artifact_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            (root / "surface.json").write_bytes(b"changed")
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(_manifest(task_id, "surface.json", payload), task_id=task_id, task_dir=root)

    def test_complete_stage_needs_receipt(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            (root / "surface.json").write_bytes(payload)
            manifest = _manifest(task_id, "surface.json", payload)
            manifest["stages"][1].pop("receipt_sha256")
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root)


class V3ViewerSecurityAndAccessTests(unittest.TestCase):
    @staticmethod
    def request(cookie=None, api_anon=None):
        return SimpleNamespace(state=SimpleNamespace(api_key_anon_id=api_anon), cookies={"anon_id": cookie} if cookie else {})

    @staticmethod
    def admin(email):
        return email == "admin@example.test"

    def test_public_task_is_visible_anonymously(self):
        task = SimpleNamespace(owner_type="user", owner_id="owner@example.test")
        self.assertTrue(_can_access_task(task, is_public=True, user=None, request=self.request(), is_admin_email=self.admin))

    def test_private_registered_owner_and_admin_are_visible(self):
        task = SimpleNamespace(owner_type="user", owner_id="owner@example.test")
        self.assertTrue(_can_access_task(task, is_public=False, user=SimpleNamespace(email="owner@example.test"), request=self.request(), is_admin_email=self.admin))
        self.assertTrue(_can_access_task(task, is_public=False, user=SimpleNamespace(email="admin@example.test"), request=self.request(), is_admin_email=self.admin))

    def test_private_anon_cookie_or_api_key_owner_is_visible(self):
        task = SimpleNamespace(owner_type="anon", owner_id="anon-1")
        self.assertTrue(_can_access_task(task, is_public=False, user=None, request=self.request(cookie="anon-1"), is_admin_email=self.admin))
        self.assertTrue(_can_access_task(task, is_public=False, user=None, request=self.request(api_anon="anon-1"), is_admin_email=self.admin))

    def test_private_unrelated_viewer_is_hidden(self):
        task = SimpleNamespace(owner_type="user", owner_id="owner@example.test")
        self.assertFalse(_can_access_task(task, is_public=False, user=SimpleNamespace(email="other@example.test"), request=self.request(), is_admin_email=self.admin))

    def test_visibility_revocation_takes_effect(self):
        task = SimpleNamespace(owner_type="user", owner_id="owner@example.test")
        request = self.request()
        self.assertTrue(_can_access_task(task, is_public=True, user=None, request=request, is_admin_email=self.admin))
        self.assertFalse(_can_access_task(task, is_public=False, user=None, request=request, is_admin_email=self.admin))

    def test_path_traversal_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            manifest = _manifest(task_id, "../surface.json", payload)
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_artifact_cannot_claim_an_incomplete_stage(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            manifest = _manifest(task_id, "surface.json", payload)
            manifest["stages"][1]["status"] = "pending"
            manifest["stages"][1].pop("receipt_sha256")
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_numeric_strings_are_not_counts(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            manifest = _manifest(task_id, "surface.json", payload)
            manifest["artifacts"][0]["bytes"] = str(len(payload))
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_artifact_type_is_bound_to_stage(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            manifest = _manifest(task_id, "surface.json", payload)
            manifest["artifacts"][0]["type"] = "skeleton_graph"
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_invalid_count_key_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = b"{}"
            manifest = _manifest(task_id, "surface.json", payload)
            manifest["stages"][1]["counts"] = {"Bad Key": 1}
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_aggregate_artifact_limit_is_enforced(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = _manifest(task_id, "surface.json", b"{}")
            manifest["artifacts"] = [
                {
                    "name": f"surface{i}", "type": "voxel_points", "stage": "c1_surface",
                    "file": f"surface{i}.json", "sha256": str(i) * 64,
                    "bytes": 50 * 1024 * 1024, "points": 0,
                }
                for i in (1, 2, 3)
            ]
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root, verify_files=False)

    def test_symlink_artifact_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "actual.json"
            target.write_bytes(b"{}")
            link = root / "surface.json"
            try:
                link.symlink_to(target.name)
            except OSError:
                self.skipTest("symlinks unavailable")
            manifest = _manifest(task_id, "surface.json", b"{}")
            with self.assertRaises(V3ManifestError):
                validate_v3_manifest(manifest, task_id=task_id, task_dir=root)

    def test_symlink_manifest_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_dir = root / task_id
            task_dir.mkdir()
            target = task_dir / "actual.json"
            target.write_text("{}", encoding="utf-8")
            try:
                (task_dir / "manifest.json").symlink_to(target.name)
            except OSError:
                self.skipTest("symlinks unavailable")
            with patch.dict("os.environ", {"AUTORIG_V3_ARTIFACT_ROOT": str(root)}):
                with self.assertRaises(HTTPException):
                    _load_manifest(task_id)

    def test_symlink_task_directory_is_rejected(self):
        task_id = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "actual"
            target.mkdir()
            try:
                (root / task_id).symlink_to(target.name, target_is_directory=True)
            except OSError:
                self.skipTest("symlinks unavailable")
            with patch.dict("os.environ", {"AUTORIG_V3_ARTIFACT_ROOT": str(root)}):
                with self.assertRaises(HTTPException):
                    _task_dir(task_id)


if __name__ == "__main__":
    unittest.main()
