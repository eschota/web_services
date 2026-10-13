import math
import unittest

import equine_motion_contract as contract


def q_yaw(degrees):
    angle = math.radians(degrees) / 2
    return [0.0, math.sin(angle), 0.0, math.cos(angle)]


def sample(action="idle", n=41, duration=2.0):
    times = [duration * i / (n - 1) for i in range(n)]
    hooves = {}
    for leg, x, z in (("LH", -.4, -.7), ("LF", -.4, .7), ("RH", .4, -.7), ("RF", .4, .7)):
        hooves[leg] = {
            "positions": [[x, 0.0, z] for _ in times], "identity_observed": True,
            "source_track": leg + "Foot", "generated": False, "mirrored": False,
        }
    return {
        "action": action, "times": times, "model_height": 2.0,
        "loop": action in contract.LOOPS,
        "clip_type": "loop" if action in contract.LOOPS else "one_shot",
        "ground": {"normal": [0, 1, 0], "offset": 0.0},
        "axes": {"up": [0, 1, 0], "forward": [0, 0, 1], "right": [1, 0, 0], "source_supported": True},
        "root": {"positions": [[0, 1, 0] for _ in times], "quaternions": [q_yaw(0) for _ in times], "source_track": "Root"},
        "hooves": hooves,
    }


def put_contacts(doc, phases, width=.12, travel=0.0):
    """Build measured 3D contact/flight signals; phases maps leg to strikes."""
    times, duration = doc["times"], doc["times"][-1]
    for leg, strikes in phases.items():
        base = doc["hooves"][leg]["positions"][0]
        rows = []
        for t in times:
            phase = t / duration
            planted = any(min(abs(phase - p), 1 - abs(phase - p)) <= width / 2 for p in strikes)
            rows.append([base[0], 0.0 if planted else .18, base[2] + travel * phase])
        doc["hooves"][leg]["positions"] = rows
    return doc


class ContractTests(unittest.TestCase):
    def test_options_are_documented_and_range_checked(self):
        options = contract.options_get({"max_slide_speed_ratio": .1})
        self.assertEqual(options["max_slide_speed_ratio"]["value"], .1)
        self.assertTrue(options["max_slide_speed_ratio"]["doc"])
        with self.assertRaises(ValueError):
            contract.options_get({"max_slide_speed_ratio": -1})
        with self.assertRaises(ValueError):
            contract.options_get({"invented": 1})

    def test_unknown_action_can_never_pass(self):
        doc = sample(); doc["action"] = "dance"
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("unknown_action", result["failures"])
        self.assertEqual(set(result), {"pass", "metrics", "failures", "pending"})

    def test_idle_with_real_stable_tracks_passes(self):
        result = contract.evaluate(sample())
        self.assertTrue(result["pass"], result)
        self.assertTrue(result["metrics"]["all_four_hooves_observed"])

    def test_all_four_half_cycle_mirror_is_not_evidence(self):
        doc = sample("canter")
        put_contacts(doc, {"LH": [.0], "RH": [.33], "LF": [.33], "RF": [.66]})
        for leg in contract.LEGS:
            doc["hooves"][leg]["generated"] = True
            doc["hooves"][leg]["mirrored"] = True
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertEqual(len([p for p in result["pending"] if p.startswith("observed_hoof_identity")]), 4)

    def test_impossible_canter_with_four_synchronous_legs_fails(self):
        doc = sample("canter")
        put_contacts(doc, {leg: [.25] for leg in contract.LEGS})
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("canter_must_be_lead_aware_three_beat", result["failures"])

    def test_gallop_requires_a_real_aerial_phase(self):
        doc = sample("gallop")
        put_contacts(doc, {"LH": [.0], "RH": [.25], "LF": [.5], "RF": [.75]}, width=.34)
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("gallop_missing_aerial_phase", result["failures"])

    def test_planted_hoof_slide_is_reported(self):
        doc = sample("idle")
        for i, row in enumerate(doc["hooves"]["LF"]["positions"]):
            row[2] = i * .05
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("hoof_slide:LF", result["failures"])
        self.assertGreater(result["metrics"]["hooves"]["LF"]["max_planted_slide_speed_ratio"], 0)

    def test_ambiguous_depth_axes_blocks_backward_and_turns(self):
        for action in ("backward", "turn_left"):
            doc = sample(action)
            doc["axes"]["source_supported"] = False
            result = contract.evaluate(doc)
            self.assertFalse(result["pass"])
            self.assertIn("source_supported_3d_axes", result["pending"])

    def test_turn_requires_signed_actual_root_yaw(self):
        left = sample("turn_left")
        left["root"]["quaternions"] = [q_yaw(45 * i / 40) for i in range(41)]
        result = contract.evaluate(left)
        self.assertFalse(result["pass"])
        self.assertIn("turn_left_missing_signed_root_yaw", result["failures"])

    def test_canonical_turns_use_declared_physical_right_and_exact_angle(self):
        right = sample("turn_right_90")
        right["root"]["quaternions"] = [q_yaw(90 * i / 40) for i in range(41)]
        self.assertTrue(contract.evaluate(right)["pass"])
        left = sample("turn_left_90")
        left["root"]["quaternions"] = [q_yaw(-90 * i / 40) for i in range(41)]
        self.assertTrue(contract.evaluate(left)["pass"])
        left["root"]["quaternions"] = [q_yaw(-30 * i / 40) for i in range(41)]
        result = contract.evaluate(left)
        self.assertFalse(result["pass"])
        self.assertIn("turn_left_90_wrong_root_yaw", result["failures"])

    def test_turn_sign_follows_anatomy_when_right_feet_are_negative_x(self):
        doc = sample("turn_right_90")
        doc["axes"]["right"] = [-1, 0, 0]
        for leg in contract.LEGS:
            for point in doc["hooves"][leg]["positions"]:
                point[0] *= -1
        # World -Y yaw faces -X, which is this rig's observed physical right.
        doc["root"]["quaternions"] = [q_yaw(-90 * i / 40) for i in range(41)]
        result = contract.evaluate(doc)
        self.assertTrue(result["pass"], result)
        self.assertAlmostEqual(result["metrics"]["root_yaw_delta_deg"], 90.0, delta=1.0)

    def test_declared_right_must_match_observed_left_right_feet(self):
        doc = sample("turn_right_90")
        doc["axes"]["right"] = [-1, 0, 0]
        doc["root"]["quaternions"] = [q_yaw(-90 * i / 40) for i in range(41)]
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("declared_right_disagrees_with_observed_anatomy", result["failures"])

    def test_turn_around_uses_unwrapped_trajectory_and_requires_180(self):
        doc = sample("turn_around_180")
        doc["root"]["quaternions"] = [q_yaw(180 * i / 40) for i in range(41)]
        self.assertTrue(contract.evaluate(doc)["pass"])
        doc["root"]["quaternions"] = [q_yaw(90 * i / 40) for i in range(41)]
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("turn_around_180_wrong_root_yaw", result["failures"])

    def test_wrong_one_shot_metadata_fails(self):
        doc = sample("jump")
        doc["loop"], doc["clip_type"] = True, "loop"
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("wrong_loop_metadata", result["failures"])
        self.assertIn("wrong_clip_type_metadata", result["failures"])

    def test_non_finite_track_and_zero_quaternion_are_missing_evidence(self):
        doc = sample()
        doc["hooves"]["LF"]["positions"][3][0] = float("nan")
        doc["root"]["quaternions"][2] = [0, 0, 0, 0]
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("hoof_track:LF", result["pending"])
        self.assertIn("root_rotation_track", result["pending"])

    def test_jump_requires_flight_takeoff_landing_and_root_lift(self):
        doc = sample("jump")
        phases = {leg: [0.0, .9] for leg in contract.LEGS}
        put_contacts(doc, phases, width=.18)
        doc["root"]["positions"] = [[0, 1 + .5 * math.sin(math.pi * i / 40), 0] for i in range(41)]
        result = contract.evaluate(doc)
        self.assertTrue(result["pass"], result)

    def test_look_requires_actual_head_yaw_and_stable_feet(self):
        doc = sample("look")
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("source_head_yaw_track", result["pending"])
        doc["head"] = {"source_track": "Head", "quaternions": [q_yaw(-15 + 30 * i / 40) for i in range(41)]}
        self.assertTrue(contract.evaluate(doc)["pass"])

    def test_tail_requires_actual_3d_track_and_stable_feet(self):
        doc = sample("tail")
        doc["tail"] = {"source_track": "TailTip", "positions": [[0, 1, .1 * math.sin(2 * math.pi * i / 40)] for i in range(41)]}
        self.assertTrue(contract.evaluate(doc)["pass"])
        del doc["tail"]["source_track"]
        self.assertFalse(contract.evaluate(doc)["pass"])

    def test_canonical_look_is_a_loop_and_requires_observed_source(self):
        doc = sample("idle_look_around")
        doc["head"] = {"source_track": "Head", "source_observed": True,
                       "quaternions": [q_yaw(-15 + 30 * i / 40) for i in range(41)]}
        self.assertTrue(contract.evaluate(doc)["pass"])
        doc["loop"] = False
        self.assertIn("wrong_loop_metadata", contract.evaluate(doc)["failures"])

    def test_canonical_in_place_walk_requires_verified_trajectory(self):
        doc = sample("walk_forward")
        put_contacts(doc, {"LH": [.0], "LF": [.25], "RH": [.5], "RF": [.75]})
        doc["root_motion_mode"] = "in_place"
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("verified_in_place_root_trajectory", result["pending"])

    def test_split_jump_start_needs_contact_to_air_transition(self):
        doc = sample("jump_start")
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("jump_start_missing_contact_to_air_transition", result["failures"])

    def test_fall_needs_observed_body_pose_and_height_drop(self):
        doc = sample("fall")
        result = contract.evaluate(doc)
        self.assertFalse(result["pass"])
        self.assertIn("source_body_quaternions_track", result["pending"])

    def test_attack_label_without_measured_joint_never_passes(self):
        result = contract.evaluate(sample("attack_primary"))
        self.assertFalse(result["pass"])
        self.assertIn("source_active_joint_positions_track", result["pending"])

    def test_gate_and_registry_adapters_never_turn_pending_true(self):
        report = contract.evaluate(sample("vocalize_emote"))
        gated = contract.gate_report(report)
        self.assertEqual(gated["schema"], "autorig.equine-motion-gates.v1")
        self.assertEqual(gated["gates"]["measured_evidence"], "PENDING")
        registry = contract.to_registry_qa(gated, "vocalize_emote")
        self.assertFalse(registry["pass"])
        self.assertFalse(registry["gates"]["measured_evidence"])


if __name__ == "__main__":
    unittest.main()
