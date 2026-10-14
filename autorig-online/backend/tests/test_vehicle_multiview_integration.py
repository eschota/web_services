from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import sys
import unittest
from unittest import mock

import cv2
import numpy as np


BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

MT_ROOT = pathlib.Path(os.environ.get("AUTORIG_MT_SOURCE_ROOT", r"R:\3d_video_motion_transfer"))
if str(MT_ROOT) not in sys.path:
    sys.path.insert(0, str(MT_ROOT))

from vehicle_generation.worker import _mt_options, _prepare_multiview_workspace, _validate_manifest

try:
    from mt import vehicle_multiview_pipeline as multiview
except ImportError as exc:  # The VPS audit runner supplies the copied MT tree through PYTHONPATH.
    multiview = None
    MT_IMPORT_ERROR = exc
else:
    MT_IMPORT_ERROR = None


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _png(image: np.ndarray) -> bytes:
    ok, encoded = cv2.imencode(".png", image)
    if not ok:
        raise AssertionError("fixture PNG encoding failed")
    return encoded.tobytes()


def _fixture_rgb() -> bytes:
    image = np.full((96, 96, 3), 224, np.uint8)
    cv2.rectangle(image, (18, 30), (77, 70), (54, 73, 91), -1)
    cv2.rectangle(image, (28, 20), (66, 38), (71, 91, 109), -1)
    return _png(image)


def _fixture_depth() -> bytes:
    depth = np.zeros((96, 96), np.uint8)
    cv2.rectangle(depth, (18, 20), (77, 70), 196, -1)
    return _png(depth)


@unittest.skipIf(multiview is None, f"MT vehicle modules unavailable: {MT_IMPORT_ERROR}")
class VehicleMultiviewCrossLayerTests(unittest.TestCase):
    """Run real MT orchestration receipts through the real backend validator.

    Images and provider answers are synthetic contract fixtures. They are not
    evidence of a generated vehicle or of model quality.
    """

    def setUp(self):
        work = pathlib.Path(__file__).parent / ".work" / "vehicle-multiview-integration"
        work.mkdir(parents=True, exist_ok=True)
        self.root = work / self._testMethodName
        if self.root.exists():
            import shutil
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        self.prepared = self.root / "prepared.png"
        self.prepared.write_bytes(_fixture_rgb())
        self.prepared_sha = _sha(self.prepared.read_bytes())
        self.original_sha = _sha(b"immutable-original-source-fixture")
        self.resolution = 32
        self.document = {
            "id": "veh_contract_fixture",
            "source_sha_chain": {"original_sha256": self.original_sha,
                                 "prepared_sha256": self.prepared_sha},
            "options": {"working_side": "right", "resolution": self.resolution,
                        "length_m": 4.0},
            "analysis": {"working_side_string": "image_right",
                         "canonical_working_side_string": "positive_x",
                         "estimated_dimensions_m_object": {
                             "length_float": 4.0, "width_float": 2.0,
                             "height_float": 1.5, "confidence_float": 1.0}},
        }
        self.workspace = _prepare_multiview_workspace(self.root, self.document, self.prepared)
        self.accepted_count = 0

    async def _publish(self, client, base, path):
        return "https://autorig.online/dev/api/scratch/fixture.png", "fixture-scratch"

    async def _accepted(self, client, out, key, url, body, source_sha):
        self.accepted_count += 1
        return {"task_id_string": f"fixture-task-{self.accepted_count}", "node_string": "fixture-node",
                "render_server_name": "fixture-worker", "_fixture_key": key,
                "_fixture_url": url, "_fixture_body": dict(body), "_fixture_source_sha": source_sha}

    async def _wait_image(self, client, base, accepted, field="image_url_string"):
        data = _fixture_depth() if accepted["_fixture_url"].endswith("/api/controlnet") else _fixture_rgb()
        return data, accepted["task_id_string"], "https://autorig.online/dev/api/scratch/output.png"

    async def _wait_text(self, client, base, accepted):
        prompt = str(accepted["_fixture_body"].get("prompt") or "")
        if "CLEAN STOCK BODY dimensions" in prompt:
            value = {"body_length_m_float": 4.0, "body_width_m_float": 2.0,
                     "body_height_m_float": 1.5, "visible_body_side_string": "right",
                     "front_image_direction_string": "right", "confidence_float": 1.0,
                     "basis_string": "synthetic contract fixture"}
        elif "Classify each listed source-visible object" in prompt:
            value = {"objects_array": []}
        elif "Required view is" in prompt:
            view = re.search(r"Required view is (top|front|left|back)", prompt).group(1)
            value = {"view_string": view, "orthographic_bool": True, "wheel_count_int": 0,
                     "mounted_equipment_count_int": 0, "loose_cargo_count_int": 0,
                     "target_vehicle_bbox_normalized_array": [.18, .2, .81, .75],
                     "preserved_clean_body_bool": True}
        else:
            value = {"wheels_removed_bool": True, "equipment_removed_bool": True,
                     "cargo_removed_bool": True, "tyre_or_rim_count_int": 0,
                     "spare_wheel_count_int": 0, "hub_brake_suspension_count_int": 0,
                     "prohibited_mounted_weapon_count_int": 0,
                     "prohibited_external_attachment_count_int": 0,
                     "loose_bed_or_trunk_cargo_count_int": 0,
                     "preserved_stock_body_bool": True,
                     "target_vehicle_bbox_normalized_array": [.18, .2, .81, .75],
                     "removed_equipment_labels_array": []}
        return json.dumps(value), accepted["task_id_string"]

    def _options(self):
        return _mt_options(self.document["options"], self.document["id"],
                           self.document["analysis"], True)

    @staticmethod
    def _artifact_map(manifest):
        return {row["path_string"]: row["sha256_string"] for row in manifest.get("artifacts_array") or []}

    def test_actual_mt_receipts_survive_backend_validation_and_preserve_lineage(self):
        patches = (
            mock.patch.object(multiview, "_publish", self._publish),
            mock.patch.object(multiview, "_accepted_or_submit", self._accepted),
            mock.patch.object(multiview, "_wait_image", self._wait_image),
            mock.patch.object(multiview, "_wait_ai_text", self._wait_text),
        )
        with patches[0], patches[1], patches[2], patches[3]:
            manifest = multiview.run_pipeline(self.prepared, self.workspace, "remove_wheels", self._options())
            self.assertEqual(manifest["schema_string"], "autorig.vehicle.multiview/1")
            self.assertEqual(manifest["original_source_sha256_string"], self.original_sha)
            self.assertEqual(manifest["prepared_source_sha256_string"], self.prepared_sha)
            _validate_manifest(self.workspace, manifest, self.original_sha, "remove_wheels", self.resolution)
            cumulative = self._artifact_map(manifest)
            cleanup_sha = manifest["body_cleanup_object"]["output_sha256_string"]
            primary_ref = manifest["primary_reference_object"]["reference_object"]
            primary_camera = manifest["primary_reference_object"]["camera_object"]
            self.assertEqual(primary_ref["sha256_string"], cleanup_sha)

            manifest = multiview.run_pipeline(self.prepared, self.workspace, "project_views", self._options())
            _validate_manifest(self.workspace, manifest, self.original_sha, "project_views", self.resolution)
            projected = self._artifact_map(manifest)
            self.assertLessEqual(cumulative.items(), projected.items(), "project_views dropped cleanup receipts")
            self.assertIn(primary_ref["path_string"], projected)
            self.assertIn(primary_camera["path_string"], projected)
            self.assertEqual((manifest.get("source_bindings_object") or {}).get(
                "original_source_sha256_string"), self.original_sha)
            self.assertEqual((manifest.get("source_bindings_object") or {}).get(
                "prepared_source_sha256_string"), self.prepared_sha)
            cumulative = projected

            manifest = multiview.run_pipeline(self.prepared, self.workspace, "depth_views", self._options())
            _validate_manifest(self.workspace, manifest, self.original_sha, "depth_views", self.resolution)
            depths = self._artifact_map(manifest)
            self.assertLessEqual(cumulative.items(), depths.items(), "depth_views dropped prior receipts")
            self.assertIn(primary_ref["path_string"], depths)
            self.assertIn(primary_camera["path_string"], depths)
            self.assertEqual({view["id_string"] for view in manifest["views_array"]},
                             {"top", "front", "left", "back"})
            for view in manifest["views_array"]:
                self.assertEqual(view["reference_object"]["source_sha256_string"], cleanup_sha)
                self.assertEqual(view["depth_object"]["source_sha256_string"],
                                 view["reference_object"]["sha256_string"])


if __name__ == "__main__":
    unittest.main()
