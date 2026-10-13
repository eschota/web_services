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
        self.surface = self.root / "surface_anchors.json"
        surface_bones = [
            {"bone": "Hips", "points": [{"vertex_id": 9, "world": [0, .8, .2]}]},
            {"bone": "Spine", "points": [{"vertex_id": 161, "world": [0, .4, .4]}]},
            {"bone": "Spine1", "points": [{"vertex_id": 4390, "world": [0, 0, .5]}]},
            {"bone": "Spine2", "points": [{"vertex_id": 6, "world": [0, -.4, .6]}]},
        ]
        self.joint_anchors = {}
        vertex = 100
        semantic_to_leg = {"fore_left.hoof": "LF", "fore_right.hoof": "RF",
                           "hind_left.hoof": "LH", "hind_right.hoof": "RH"}
        for semantic, _, toe, source in hoofs:
            leg = semantic_to_leg[semantic]
            side = "Left" if toe.startswith("Left") else "Right"; region = "Front" if "Front" in toe else "Hind"
            for level, suffix in enumerate(("Foot", "Leg", "UpLeg"), 1):
                bone = side + region + suffix; anchor_id = f"{bone}:{vertex}"
                self.joint_anchors.setdefault(leg, []).append((bone, anchor_id))
                surface_bones.append({"bone": bone, "points": [{"vertex_id": vertex,
                                      "world": [source[0], source[1], level * .2]}]})
                vertex += 1
        write(self.surface, {"bones": surface_bones})
        self.bundle = self.root / "fitting_bundle.json"
        # Local camera -Z points along world -X.  This is a true side camera.
        camera_to_world = [0, 0, 1, 5, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1]
        write(self.bundle, {
            "schema": "autorig-actionless-fitting-bundle.v1", "source": {"sha256": "a" * 64},
            "artifacts": {"skeleton": {"sha256": sha(self.skeleton)},
                          "surface_anchors": {"sha256": sha(self.surface)}},
            "camera": {"resolution": [12, 8], "intrinsics": {"fx": 10, "fy": 10, "cx": 6, "cy": 4},
                       "camera_to_world": camera_to_world},
        })
        self.manifest = self.root / "immutable_manifest.json"
        write(self.manifest, {"schema": "autorig-fitting-immutable-bundle.v1",
                              "bundle_manifest": {"sha256": sha(self.bundle)},
                              "files": [{"filename": "surface_anchors.json", "sha256": sha(self.surface)}]})
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

    def rebind_surface(self, surface):
        write(self.surface, surface)
        bundle = json.loads(self.bundle.read_text()); bundle["artifacts"]["surface_anchors"]["sha256"] = sha(self.surface)
        write(self.bundle, bundle)
        manifest = json.loads(self.manifest.read_text())
        manifest["bundle_manifest"]["sha256"] = sha(self.bundle)
        manifest["files"][0]["sha256"] = sha(self.surface); write(self.manifest, manifest)
        anchor = json.loads(self.anchor.read_text()); anchor["bundle_manifest_sha256"] = sha(self.manifest); write(self.anchor, anchor)
        obs = json.loads(self.obs.read_text()); obs["provenance"]["bundle_sha256"] = sha(self.bundle)
        obs["provenance"]["immutable_manifest_sha256"] = sha(self.manifest); write(self.obs, obs)

    def call(self, action="walk_forward", with_torso=False):
        kwargs = dict(
            observations_path=self.obs, observations_sha256=sha(self.obs),
            anchor_map_path=self.anchor, anchor_map_sha256=sha(self.anchor),
            fitting_bundle_path=self.bundle, fitting_bundle_sha256=sha(self.bundle),
            immutable_manifest_path=self.manifest, immutable_manifest_sha256=sha(self.manifest),
            skeleton_path=self.skeleton, skeleton_sha256=sha(self.skeleton),
            source_video_sha256="b" * 64, action=action)
        if with_torso:
            kwargs.update(surface_anchors_path=self.surface, surface_anchors_sha256=sha(self.surface))
        return constraints.build_constraints(**kwargs)

    def test_visible_source_bound_hooves_become_candidate_constraints(self):
        result = self.call()
        self.assertEqual(result["state"], "fit_candidate_constraints")
        self.assertFalse(result["accepted_observations"])
        self.assertFalse(result["animation_qa_pass"])
        self.assertEqual(set(result["constraints"]), {"LF", "RF", "LH", "RH"})
        self.assertTrue(all(len(row["constraints"]) == 4 for row in result["constraints"].values()))
        self.assertTrue(result["assumptions"]["requires_real_post_retarget_3d_validation"])
        self.assertFalse(result["assumptions"]["metric_depth_used"])
        self.assertEqual(result["timing"]["frame_count"], 4)
        self.assertEqual(result["timing"]["fps"], 24.0)
        self.assertEqual(result["timing"]["source_frame_indices"], [0, 1, 2, 3])
        self.assertEqual(result["constraints"]["LF"]["constraints"][2]["source_frame"], 2)
        self.assertIsNone(result["provenance"]["raw_npz_sha256"])

    def test_hidden_point_is_not_interpolated_or_mirrored(self):
        value = json.loads(self.obs.read_text()); value["tracks"][0]["points"][2]["visible"] = False; write(self.obs, value)
        result = self.call(); row = result["constraints"]["LF"]
        self.assertEqual([item["frame"] for item in row["constraints"]], [0, 1, 3])
        self.assertIn("partial_constraint_track:fore_left.hoof", result["pending"])
        self.assertFalse(row["mirrored"]); self.assertFalse(row["generated"])

    def test_optional_torso_tracks_use_exact_source_planes_without_yaw_or_ground_claims(self):
        value = json.loads(self.obs.read_text())
        for anchor_id in ("Hips:9", "Spine:161", "Spine1:4390", "Spine2:6"):
            value["tracks"].append({"id": "runtime-" + anchor_id, "anchor_id": anchor_id,
                                    "points": [{"frame": frame, "x": 6 + frame * .1, "y": 4,
                                                "visible": True, "confidence": .9} for frame in range(4)]})
        write(self.obs, value)
        result = self.call(with_torso=True)
        self.assertEqual(set(result["torso_constraints"]), {"Hips:9", "Spine:161", "Spine1:4390", "Spine2:6"})
        self.assertTrue(all(len(row["constraints"]) == 4 for row in result["torso_constraints"].values()))
        self.assertTrue(all(row["derivation"] == "derived_from_source_plane_constraint"
                            for row in result["torso_constraints"].values()))
        self.assertNotIn("root_yaw", result)
        self.assertFalse(result["assumptions"]["ground_confidence_claimed"])

    def test_all_observed_source_chain_joints_are_emitted_per_leg(self):
        value = json.loads(self.obs.read_text())
        for rows in self.joint_anchors.values():
            for _, anchor_id in rows:
                value["tracks"].append({"id": "runtime-" + anchor_id, "anchor_id": anchor_id,
                                        "points": [{"frame": frame, "x": 6 + frame * .1, "y": 4,
                                                    "visible": True, "confidence": .9} for frame in range(4)]})
        write(self.obs, value)
        result = self.call(with_torso=True)
        for leg, rows in self.joint_anchors.items():
            expected = {bone for bone, _ in rows}
            self.assertEqual(set(result["constraints"][leg]["joint_constraints"]), expected)
            self.assertTrue(all(len(row["constraints"]) == 4
                                for row in result["constraints"][leg]["joint_constraints"].values()))

    def test_hidden_joint_frame_stays_partial_without_mirroring(self):
        value = json.loads(self.obs.read_text())
        bone, anchor_id = self.joint_anchors["RF"][1]
        value["tracks"].append({"id": "runtime-" + anchor_id, "anchor_id": anchor_id,
                                "points": [{"frame": frame, "x": 6, "y": 4,
                                            "visible": frame != 2, "confidence": .9} for frame in range(4)]})
        write(self.obs, value)
        result = self.call(with_torso=True); row = result["constraints"]["RF"]["joint_constraints"][bone]
        self.assertEqual([item["frame"] for item in row["constraints"]], [0, 1, 3])
        self.assertFalse(row["generated"]); self.assertFalse(row["mirrored"])
        self.assertIn(f"partial_limb_joint_constraint_track:RF:{bone}", result["pending"])

    def test_torso_anchor_vertex_ids_are_derived_per_source_bundle(self):
        surface = json.loads(self.surface.read_text())
        surface["bones"][0]["points"][0]["vertex_id"] = 77
        self.rebind_surface(surface)
        value = json.loads(self.obs.read_text())
        for anchor_id in ("Hips:77", "Spine:161", "Spine1:4390", "Spine2:6"):
            value["tracks"].append({"id": "runtime-" + anchor_id, "anchor_id": anchor_id,
                                    "points": [{"frame": frame, "x": 6, "y": 4,
                                                "visible": True, "confidence": .9} for frame in range(4)]})
        write(self.obs, value)
        result = self.call(with_torso=True)
        self.assertIn("Hips:77", result["torso_constraints"])
        self.assertNotIn("Hips:9", result["torso_constraints"])

    def test_multiple_observed_anchors_for_one_torso_bone_stay_pending(self):
        surface = json.loads(self.surface.read_text())
        surface["bones"][0]["points"].append({"vertex_id": 77, "world": [0.1, .8, .2]})
        self.rebind_surface(surface)
        value = json.loads(self.obs.read_text())
        for anchor_id in ("Hips:9", "Hips:77", "Spine:161", "Spine1:4390", "Spine2:6"):
            value["tracks"].append({"id": "runtime-" + anchor_id, "anchor_id": anchor_id,
                                    "points": [{"frame": frame, "x": 6, "y": 4,
                                                "visible": True, "confidence": .9} for frame in range(4)]})
        write(self.obs, value)
        result = self.call(with_torso=True)
        self.assertNotIn("Hips:9", result["torso_constraints"])
        self.assertNotIn("Hips:77", result["torso_constraints"])
        self.assertIn("ambiguous_observed_torso_bone:Hips", result["pending"])

    def test_raw_2d_document_derives_constraints_but_never_acceptance(self):
        value = json.loads(self.obs.read_text())
        value.update({"schema": "autorig.equine-raw-2d-measurements.v1",
                      "measurement_state": "raw_2d_measurements", "calibration_accepted": False,
                      "accepted_observations": False, "animation_quality_approved": False,
                      "camera_z_accepted": False, "pending": ["unverified_source_fps"],
                      "timing": {"frame_count": 4, "fps": 24.0, "source_frame_indices": [0, 1, 2, 3],
                                 "full_source": True, "source_video_sha256": "b" * 64,
                                 "verified": False, "source": "caller_measured_unverified",
                                 "retimed": False, "loop_declared": False}})
        value["provenance"].update({"producer": "farm_raw_capture.tracks", "metric_depth_used": False,
                                    "points_generated": False, "points_mirrored": False,
                                    "hidden_points_interpolated": False,
                                    "raw_npz_sha256": "d" * 64,
                                    "anchor_map_sha256": sha(self.anchor), "source_model_sha256": "a" * 64,
                                    "surface_anchors_sha256": sha(self.surface),
                                    "raw_array_sha256": {name: "c" * 64 for name in
                                                         ("points_xy", "visible", "confidence", "anchor_ids", "seed_points_xy")}})
        write(self.obs, value)
        result = self.call()
        self.assertEqual(result["input_measurement_state"], "raw_2d_measurements")
        self.assertEqual(result["provenance"]["raw_npz_sha256"], "d" * 64)
        self.assertFalse(result["accepted_observations"]); self.assertFalse(result["animation_qa_pass"])
        self.assertTrue(all(row["derivation"] == "derived_from_source_plane_constraint"
                            for row in result["constraints"].values()))

    def test_raw_document_with_fake_accepted_flag_is_rejected(self):
        value = json.loads(self.obs.read_text())
        value.update({"schema": "autorig.equine-raw-2d-measurements.v1",
                      "measurement_state": "raw_2d_measurements", "calibration_accepted": False,
                      "accepted_observations": True, "animation_quality_approved": False,
                      "camera_z_accepted": False})
        value["provenance"].update({"producer": "farm_raw_capture.tracks", "metric_depth_used": False,
                                    "points_generated": False, "points_mirrored": False,
                                    "hidden_points_interpolated": False,
                                    "raw_npz_sha256": "d" * 64,
                                    "anchor_map_sha256": sha(self.anchor), "source_model_sha256": "a" * 64,
                                    "surface_anchors_sha256": sha(self.surface),
                                    "raw_array_sha256": {name: "c" * 64 for name in
                                                         ("points_xy", "visible", "confidence", "anchor_ids", "seed_points_xy")}})
        write(self.obs, value)
        with self.assertRaisesRegex(constraints.ConstraintContractError, "negative acceptance flags"):
            self.call()

    def test_turn_and_multiaxis_actions_are_rejected(self):
        for action in ("turn_left_90", "turn_around_180", "jump_full", "attack_primary"):
            with self.assertRaisesRegex(constraints.ConstraintContractError, "restricted"):
                self.call(action)

    def test_invalid_observation_timing_fails(self):
        value = json.loads(self.obs.read_text()); value["fps"] = 0; write(self.obs, value)
        with self.assertRaisesRegex(constraints.ConstraintContractError, "timing"):
            self.call()

    def test_exact_source_hashes_and_anchor_identity_are_required(self):
        value = json.loads(self.anchor.read_text()); value["tracks"][0]["bone"] = "RightFrontToe"; write(self.anchor, value)
        with self.assertRaises(constraints.ConstraintContractError): self.call()

    def test_all_options_are_documented_and_range_checked(self):
        rows = constraints.options_get()
        self.assertTrue(all(row["doc"] for row in rows.values()))
        with self.assertRaises(constraints.ConstraintContractError):
            constraints.options_get({"sagittal_plane_offset_ratio": .2})


if __name__ == "__main__": unittest.main()
