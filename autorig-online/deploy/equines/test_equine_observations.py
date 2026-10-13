import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

import equine_observations as adapter


def write_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ObservationAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=str(Path(__file__).parent))
        self.root = Path(self.temp.name)
        self.n, self.h, self.w = 12, 8, 12
        tracks = [
            ("fore_left.hoof", "LeftFrontToe:1", "LeftFrontToe", [-1, -1, 0]),
            ("fore_right.hoof", "RightFrontToe:2", "RightFrontToe", [1, -1, 0]),
            ("hind_left.hoof", "LeftHindToe:3", "LeftHindToe", [-1, 1, 0]),
            ("hind_right.hoof", "RightHindToe:4", "RightHindToe", [1, 1, 0]),
            ("head", "Head:5", "Head", [0, -2, 1]),
            ("tail", "Tail3:6", "Tail3", [0, 2, 1]),
        ]
        self.bundle = self.root / "fitting_bundle.json"
        bundle = {
            "schema": "autorig-actionless-fitting-bundle.v1",
            "source": {"sha256": "a" * 64},
            "camera": {"resolution": [self.w, self.h],
                       "intrinsics": {"fx": 10, "fy": 10, "cx": 6, "cy": 4},
                       "camera_to_world": np.eye(4).reshape(-1).tolist()},
            "ground_plane": {"normal": [0, 0, 1], "height": 0},
            "renderer": {"geometry_contract": {"raw_evaluated_vertex_identity": True}},
        }
        write_json(self.bundle, bundle)
        self.manifest = self.root / "immutable_manifest.json"
        write_json(self.manifest, {"schema": "autorig-fitting-immutable-bundle.v1",
                                  "bundle_manifest": {"sha256": sha(self.bundle)}})
        self.anchor = self.root / "anchor.json"
        write_json(self.anchor, {
            "schema": "autorig.equine-fitting-anchor-map/1", "source_sha256": "a" * 64,
            "bundle_manifest_sha256": sha(self.manifest),
            "coordinates": {"bundle_up": [0, 0, 1], "bundle_height": 2.0},
            "tracks": [{"track_id": t, "anchor_id": a, "bone": b, "world": p}
                       for t, a, b, p in tracks],
        })
        points = np.zeros((self.n, 6, 2), np.float32)
        points[..., 0], points[..., 1] = 6, 4
        visible = np.ones((self.n, 6), bool)
        confidence = np.ones((self.n, 6), np.float32)
        camera_z = np.full((self.n, self.h, self.w), 5.0, np.float32)
        self.npz = self.root / "observations.npz"
        np.savez_compressed(self.npz, tracks_xy=points, visible=visible, confidence=confidence,
                            camera_z=camera_z, track_ids=np.asarray([t[0] for t in tracks]),
                            anchor_ids=np.asarray([t[1] for t in tracks]), fps=np.asarray(24.0))
        self.obs = self.root / "observations.json"
        write_json(self.obs, {
            "schema": "autorig-fitting-observations.v1", "frame_count": self.n,
            "width": self.w, "height": self.h, "fps": 24.0,
            "tracks": [{"id": t, "anchor_id": a, "query_frame": 0,
                        "points": [{"frame": f, "x": 6, "y": 4, "visible": True, "confidence": 1.0}
                                   for f in range(self.n)]} for t, a, _, _ in tracks],
            "provenance": {"runtime": "autorig-official-animal-tracking.v1",
                           "bundle_sha256": sha(self.bundle),
                           "immutable_manifest_sha256": sha(self.manifest),
                           "source_video_sha256": "b" * 64,
                           "relative_depth_contract": "calibrated_to_camera_z_from_immutable_actionless_reference",
                           "camera_z_calibration": {"first_frame_output_coverage": .99}},
        })

    def tearDown(self): self.temp.cleanup()

    def test_visibility_completeness_cannot_be_relaxed(self):
        with self.assertRaises(adapter.ObservationContractError):
            adapter.options_get({"min_track_visible_fraction": .5})

    def call(self, ground_camera_evidence=None):
        return adapter.adapt_observations(
            observations_path=self.obs, observations_sha256=sha(self.obs),
            npz_path=self.npz, npz_sha256=sha(self.npz), anchor_map_path=self.anchor,
            anchor_map_sha256=sha(self.anchor), fitting_bundle_path=self.bundle,
            fitting_bundle_sha256=sha(self.bundle), immutable_manifest_path=self.manifest,
            immutable_manifest_sha256=sha(self.manifest), source_video_sha256="b" * 64,
            action="walk_forward", loop=True,
            clip_type="loop", ground_camera_evidence=ground_camera_evidence)

    def test_complete_measured_tracks_are_source_bound_but_root_stays_pending(self):
        result = self.call()
        self.assertEqual(set(result["sample"]["hooves"]), {"LF", "RF", "LH", "RH"})
        self.assertIn("post_retarget_root_position_track", result["pending"])
        self.assertFalse(result["provenance"]["animation_quality_approved"])
        self.assertEqual(result["sample"]["ground"]["normal"], [0.0, 1.0, -0.0])

    def test_reference_geometry_identity_is_not_temporal_ground_confidence(self):
        result = self.call()
        self.assertTrue(result["sample"]["ground"]["reference_plane_identity"])
        self.assertEqual(result["sample"]["ground"]["confidence"], 0.0)
        self.assertIn("weak_source_ground_plane", result["pending"])

    def test_hash_bound_numeric_ground_camera_receipt_supplies_confidence(self):
        evidence = {
            "schema": "autorig.equine-video-ground-evidence.v1",
            "source_video_sha256": "b" * 64, "observations_sha256": sha(self.obs),
            "receipt_id": "ground-camera-measured-1", "confidence": .98,
            "checks": {
                "temporal_ground_plane": {"value": .01, "operator": "<=", "threshold": .02},
                "camera_continuity": {"value": .4, "operator": "<=", "threshold": .5},
                "ground_visible_fraction": {"value": .97, "operator": ">=", "threshold": .95},
            },
        }
        result = self.call(evidence)
        self.assertEqual(result["sample"]["ground"]["confidence"], .98)
        self.assertNotIn("weak_source_ground_plane", result["pending"])

    def test_failing_numeric_ground_check_cannot_retain_claimed_confidence(self):
        evidence = {
            "schema": "autorig.equine-video-ground-evidence.v1",
            "source_video_sha256": "b" * 64, "observations_sha256": sha(self.obs),
            "receipt_id": "ground-camera-fail-1", "confidence": 1.0,
            "checks": {
                "temporal_ground_plane": {"value": .2, "operator": "<=", "threshold": .02},
                "camera_continuity": {"value": .4, "operator": "<=", "threshold": .5},
                "ground_visible_fraction": {"value": .97, "operator": ">=", "threshold": .95},
            },
        }
        result = self.call(evidence)
        self.assertEqual(result["sample"]["ground"]["confidence"], 0.0)
        self.assertIn("weak_source_ground_plane", result["pending"])

    def test_hash_mismatch_fails_before_reading_evidence(self):
        with self.assertRaisesRegex(adapter.ObservationContractError, "SHA-256 mismatch"):
            adapter.adapt_observations(
                observations_path=self.obs, observations_sha256="0" * 64,
                npz_path=self.npz, npz_sha256=sha(self.npz), anchor_map_path=self.anchor,
                anchor_map_sha256=sha(self.anchor), fitting_bundle_path=self.bundle,
                fitting_bundle_sha256=sha(self.bundle), immutable_manifest_path=self.manifest,
                immutable_manifest_sha256=sha(self.manifest), source_video_sha256="b" * 64,
                action="walk_forward", loop=True,
                clip_type="loop")

    def test_source_video_hash_is_exact(self):
        value = json.loads(self.obs.read_text())
        value["provenance"]["source_video_sha256"] = "c" * 64
        write_json(self.obs, value)
        with self.assertRaisesRegex(adapter.ObservationContractError, "source-video hash mismatch"):
            self.call()

    def test_hidden_leg_is_omitted_never_mirrored(self):
        archive = np.load(self.npz, allow_pickle=False)
        values = {key: archive[key] for key in archive.files}
        values["visible"] = values["visible"].copy(); values["visible"][3, 0] = False
        np.savez_compressed(self.npz, **values)
        obs = json.loads(self.obs.read_text())
        obs["tracks"][0]["points"][3]["visible"] = False
        write_json(self.obs, obs)
        result = self.call()
        self.assertNotIn("LF", result["sample"]["hooves"])
        self.assertIn("incomplete_observed_track:fore_left.hoof", result["pending"])
        self.assertFalse(any(row.get("mirrored") for row in result["sample"]["hooves"].values()))

    def test_weak_confidence_and_missing_depth_remain_pending(self):
        archive = np.load(self.npz, allow_pickle=False)
        values = {key: archive[key] for key in archive.files}
        values["confidence"] = values["confidence"].copy(); values["confidence"][4, 1] = .1
        values["camera_z"] = values["camera_z"].copy(); values["camera_z"][5, :, :] = np.nan
        np.savez_compressed(self.npz, **values)
        obs = json.loads(self.obs.read_text()); obs["tracks"][1]["points"][4]["confidence"] = .1
        write_json(self.obs, obs)
        result = self.call()
        self.assertNotIn("RF", result["sample"]["hooves"])
        self.assertTrue(any(value.startswith("incomplete_observed_track") for value in result["pending"]))

    def test_anchor_identity_swap_is_rejected(self):
        value = json.loads(self.anchor.read_text())
        value["tracks"][0]["anchor_id"] = "RightFrontToe:2"
        with self.assertRaises(adapter.ObservationContractError):
            write_json(self.anchor, value)
            self.call()

    def test_relative_depth_without_camera_z_calibration_is_rejected(self):
        value = json.loads(self.obs.read_text())
        value["provenance"]["relative_depth_contract"] = "relative_unscaled_diagnostics_only_not_camera_z"
        write_json(self.obs, value)
        with self.assertRaisesRegex(adapter.ObservationContractError, "not calibrated"):
            self.call()


if __name__ == "__main__": unittest.main()
