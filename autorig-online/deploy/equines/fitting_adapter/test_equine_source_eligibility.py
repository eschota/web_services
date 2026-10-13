from copy import deepcopy
from types import SimpleNamespace
import unittest

import numpy as np

import equine_source_eligibility as eligibility


def bone(name, parent, translation=.2):
    matrix = np.eye(4); matrix[0, 3] = 0 if parent is None else translation
    return SimpleNamespace(name=name, parent=parent, rest_local=matrix)


def anchor(anchor_id, bone_name, local=(0, 0, 0), weight=1.0):
    influence = SimpleNamespace(bone=bone_name, weight=weight, bone_local=np.asarray(local, float))
    return SimpleNamespace(id=anchor_id, influences=(influence,))


def rig():
    bones = {"Root": bone("Root", None), "Up": bone("Up", "Root"),
             "Leg": bone("Leg", "Up"), "Foot": bone("Foot", "Leg"), "Toe": bone("Toe", "Foot")}
    anchors = {name + ":1": anchor(name + ":1", name) for name in ("Toe", "Foot", "Leg", "Up")}
    return SimpleNamespace(bones=bones, anchors=anchors, metadata_sha256="a" * 64,
                           immutable_manifest_sha256="b" * 64)


def track(anchor_id, bone_name, x, frames=4):
    return {"source_anchor_id": anchor_id, "source_bone": bone_name,
            "derivation": "derived_from_source_plane_constraint", "generated": False, "mirrored": False,
            "constraints": [{"frame": frame, "source_frame": frame, "position": [x, 0, 0],
                             "confidence": .9, "plane_residual_ratio": 0} for frame in range(frames)]}


def constraints():
    legs = {}
    for leg in ("LF", "RF", "LH", "RH"):
        toe = track("Toe:1", "Toe", .6)
        toe["source_bone_chain"] = ["Toe", "Foot", "Leg", "Up"]
        toe["joint_constraints"] = {"Foot": track("Foot:1", "Foot", .4),
                                    "Leg": track("Leg:1", "Leg", .2),
                                    "Up": track("Up:1", "Up", 0)}
        legs[leg] = toe
    return {"schema": "autorig.equine-sagittal-limb-constraints.v1",
            "state": "fit_candidate_constraints", "source_height": 1.0,
            "timing": {"frame_count": 4, "source_frame_indices": [0, 1, 2, 3]},
            "constraints": legs, "provenance": {"source_model_sha256": "c" * 64}}


class SourceEligibilityTests(unittest.TestCase):
    def test_valid_source_chain_targets_are_eligible(self):
        result = eligibility.evaluate_source_eligibility(rig(), constraints(), constraints_sha256="d" * 64)
        for leg in result["targets"].values():
            for target in leg.values():
                self.assertTrue(all(row["eligible"] for row in target["frames"]))
        self.assertFalse(result["provenance"]["input_targets_mutated"])

    def test_impossible_toe_reach_blocks_toe_not_valid_proximal_chain(self):
        value = constraints()
        for leg in value["constraints"].values():
            leg["constraints"][2]["position"] = [1.2, 0, 0]
        result = eligibility.evaluate_source_eligibility(rig(), value, constraints_sha256="d" * 64)
        for leg in result["targets"].values():
            self.assertFalse(leg["Toe:1"]["frames"][2]["eligible"])
            self.assertIn("SOURCE_REACH_EXCEEDED", leg["Toe:1"]["frames"][2]["reasons"])
            self.assertIn("TEMPORAL_PAIR_JUMP", leg["Toe:1"]["frames"][2]["reasons"])
            self.assertTrue(leg["Foot:1"]["frames"][2]["eligible"])
        for leg in result["pairs"].values():
            self.assertEqual(leg["Toe|Foot"]["frames"][2]["reasons"],
                             ["SOURCE_REACH_EXCEEDED", "TEMPORAL_PAIR_JUMP"])
        self.assertEqual(result["contact_semantics"]["ineligible_toe_contact_action"], "suppress_to_pending")

    def test_consecutive_pair_jump_marks_downstream_toe_pending(self):
        value = constraints()
        # Toe-Foot distance: .05 at frame1 then .15 at frame2, both within .2 source reach.
        for leg in value["constraints"].values():
            leg["constraints"][1]["position"] = [.45, 0, 0]
            leg["constraints"][2]["position"] = [.55, 0, 0]
        result = eligibility.evaluate_source_eligibility(rig(), value, constraints_sha256="d" * 64,
                                                         options={"temporal_jump_tolerance_ratio": 0})
        for leg in result["targets"].values():
            self.assertFalse(leg["Toe:1"]["frames"][2]["eligible"])
            self.assertIn("TEMPORAL_PAIR_JUMP", leg["Toe:1"]["frames"][2]["reasons"])

    def test_missing_foot_support_keeps_toe_pending_without_opposite_leg_inference(self):
        value = constraints(); value["constraints"]["RF"]["joint_constraints"].pop("Foot")
        result = eligibility.evaluate_source_eligibility(rig(), value, constraints_sha256="d" * 64)
        self.assertTrue(all(not row["eligible"] for row in result["targets"]["RF"]["Toe:1"]["frames"]))
        self.assertFalse(result["contact_semantics"]["opposite_leg_inference"])

    def test_scaled_rest_transform_and_unnormalized_weights_fail_closed(self):
        scaled = rig(); scaled.bones["Leg"].rest_local[0, 0] = 2
        with self.assertRaisesRegex(eligibility.EligibilityError, "scale/shear"):
            eligibility.evaluate_source_eligibility(scaled, constraints(), constraints_sha256="d" * 64)
        bad = rig(); bad.anchors["Toe:1"] = anchor("Toe:1", "Toe", weight=.8)
        with self.assertRaisesRegex(eligibility.EligibilityError, "not normalized"):
            eligibility.evaluate_source_eligibility(bad, constraints(), constraints_sha256="d" * 64)

    def test_options_are_documented_and_range_checked(self):
        self.assertTrue(all(row["doc"] for row in eligibility.options_get().values()))
        with self.assertRaises(eligibility.EligibilityError):
            eligibility.options_get({"temporal_jump_multiplier": 1.0})


if __name__ == "__main__": unittest.main()
