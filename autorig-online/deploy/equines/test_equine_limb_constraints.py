import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import equine_limb_constraints as constraints


def write(path, value): path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()


class LimbConstraintTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=str(Path(__file__).parent)); self.root = Path(self.temp.name)
        hoofs = [
            ("fore_left.hoof", "LeftFrontToe:1", "LeftFrontToe", [-.5, -1, 0]),
            ("fore_right.hoof", "RightFrontToe:2", "RightFrontToe", [.5, -1, 0]),
            ("hind_left.hoof", "LeftHindToe:3", "LeftHindToe", [-.5, 1, 0]),
            ("hind_right.hoof", "RightHindToe:4", "RightHindToe", [.5, 1, 0]),
        ]
        bones = []
        for _, _, toe, _ in hoofs:
            side = "Left" if toe.startswith("Left") else "Right"
            region = "Front" if "Front" in toe else "Hind"
            parent = "Spine2" if region == "Front" else "Hips"
            bones.extend([
                {"name": side + region + "UpLeg", "parent": parent},
                {"name": side + region + "Leg", "parent": side + region + "UpLeg"},
                {"name": side + region + "Foot", "parent": side + region + "Leg"},
                {"name": toe, "parent": side + region + "Foot"},
            ])
        bones.extend([{"name": "Hips", "parent": None}, {"name": "Spine2", "parent": "Hips"}])
        self.skeleton = self.root / "skeleton.json"; write(self.skeleton, {"armatures": [{"bones": bones}]})
        self.bundle = self.root / "fitting_bundle.json"
        # Local camera -Z points along world -X.  This is a true side camera.
        camera_to_world = [0, 0, 1, 5, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1]
        write(self.bundle, {
            "schema": "autorig-actionless-fitting-bundle.v1", "source": {"sha256": "a" * 64},
            "artifacts": {"skeleton": {"sha256": sha(self.skeleton)}},
            "camera": {"resolution": [12, 8], "intrinsics": {"fx": 10, "fy": 10, "cx": 6, "cy": 4},
                       "camera_to_world": camera_to_world},
        })
        self.manifest = self.root / "immutable_manifest.json"
        write(self.manifest, {"schema": "autorig-fitting-immutable-bundle.v1",
                              "bundle_manifest": {"sha256": sha(self.bundle)}})
        self.anchor = self.root / "anchor.json"
        write(self.anchor, {"schema": "autorig.equine-fitting-anchor-map/1", "source_sha256": "a" * 64,
                            "bundle_manifest_sha256": sha(self.manifest),
                            "coordinates": {"bundle_height": 2.0, "bundle_up": [0, 0, 1]},
                            "tracks": [{"track_id": t, "anchor_id": a, "bone": b, "world": p}
                                       for t, a, b, p in hoofs]})
        self.obs = self.root / "observations.json"
        write(self.obs, {
            "schema": "autorig-fitting-observations.v1", "width": 12, "height": 8, "frame_count": 4, "fps": 24,
            "tracks": [{"id": "runtime-" + t, "anchor_id": a,
                        "points": [{"frame": f, "x": 6 + f * .1, "y": 4, "visible": True, "confidence": .9}
                                   for f in range(4)]} for t, a, _, _ in hoofs],
            "provenance": {"source_video_sha256": "b" * 64, "bundle_sha256": sha(self.bundle),
                           "immutable_manifest_sha256": sha(self.manifest),
                           "runtime": "autorig-official-animal-tracking.v1"},
        })

    def tearDown(self): self.temp.cleanup()

    def call(self, action="walk_forward"):
        return constraints.build_constraints(
            observations_path=self.obs, observations_sha256=sha(self.obs),
            anchor_map_path=self.anchor, anchor_map_sha256=sha(self.anchor),
            fitting_bundle_path=self.bundle, fitting_bundle_sha256=sha(self.bundle),
            immutable_manifest_path=self.manifest, immutable_manifest_sha256=sha(self.manifest),
            skeleton_path=self.skeleton, skeleton_sha256=sha(self.skeleton),
            source_video_sha256="b" * 64, action=action)

    def test_visible_source_bound_hooves_become_candidate_constraints(self):
        result = self.call()
        self.assertEqual(result["state"], "fit_candidate_constraints")
        self.assertFalse(result["accepted_observations"])
        self.assertFalse(result["animation_qa_pass"])
        self.assertEqual(set(result["constraints"]), {"LF", "RF", "LH", "RH"})
        self.assertTrue(all(len(row["constraints"]) == 4 for row in result["constraints"].values()))
        self.assertTrue(result["assumptions"]["requires_real_post_retarget_3d_validation"])
        self.assertFalse(result["assumptions"]["metric_depth_used"])

    def test_hidden_point_is_not_interpolated_or_mirrored(self):
        value = json.loads(self.obs.read_text()); value["tracks"][0]["points"][2]["visible"] = False; write(self.obs, value)
        result = self.call(); row = result["constraints"]["LF"]
        self.assertEqual([item["frame"] for item in row["constraints"]], [0, 1, 3])
        self.assertIn("partial_constraint_track:fore_left.hoof", result["pending"])
        self.assertFalse(row["mirrored"]); self.assertFalse(row["generated"])

    def test_turn_and_multiaxis_actions_are_rejected(self):
        for action in ("turn_left_90", "turn_around_180", "jump_full", "attack_primary"):
            with self.assertRaisesRegex(constraints.ConstraintContractError, "restricted"):
                self.call(action)

    def test_exact_source_hashes_and_anchor_identity_are_required(self):
        value = json.loads(self.anchor.read_text()); value["tracks"][0]["bone"] = "RightFrontToe"; write(self.anchor, value)
        with self.assertRaises(constraints.ConstraintContractError): self.call()

    def test_all_options_are_documented_and_range_checked(self):
        rows = constraints.options_get()
        self.assertTrue(all(row["doc"] for row in rows.values()))
        with self.assertRaises(constraints.ConstraintContractError):
            constraints.options_get({"sagittal_plane_offset_ratio": .2})


if __name__ == "__main__": unittest.main()
