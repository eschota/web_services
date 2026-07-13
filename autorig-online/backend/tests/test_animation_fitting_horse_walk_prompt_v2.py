import hashlib
import json
import unittest
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT_PATH = (
    BACKEND_ROOT
    / "animation_fitting"
    / "specs"
    / "experiments"
    / "horse_walk_prompt_v2_guide_strength_ab.v1.json"
)


class HorseWalkPromptV2ExperimentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.experiment = json.loads(EXPERIMENT_PATH.read_text(encoding="utf-8"))

    def test_experiment_is_a_non_approved_loop_with_pinned_inputs(self):
        spec = self.experiment
        self.assertEqual(spec["schema"], "autorig.animation-fitting-experiment.v1")
        self.assertEqual(spec["base_action_id_string"], "walk_forward")
        self.assertEqual(spec["species_string"], "horse")
        self.assertEqual(spec["generation_mode_string"], "loop")
        self.assertEqual(spec["frame_count_int"], 49)
        self.assertEqual((spec["frame_count_int"] - 1) % 8, 0)
        self.assertFalse(spec["approved_bool"])
        self.assertEqual(
            spec["reference_object"]["reference_rgb_sha256_string"],
            "94bf47cc137c0aaee975b2a75b7cd2b28f75215e282cdb6865bdd4095630a0b1",
        )
        self.assertEqual(
            spec["workflow_object"]["workflow_fingerprint_sha256_string"],
            "e0f549b58d3933027a4f4d3fde69d6e3dfb6d360f0200e8f00a9d2bff278bc56",
        )

    def test_ab_changes_only_both_guide_strengths_around_point_seven(self):
        variants = self.experiment["variants_array"]
        self.assertEqual(len(variants), 2)
        strengths = []
        for variant in variants:
            self.assertEqual(
                variant["start_guide_strength_float"],
                variant["end_guide_strength_float"],
            )
            strengths.append(variant["start_guide_strength_float"])
        self.assertEqual(strengths, [0.6, 0.8])
        self.assertAlmostEqual(sum(strengths) / len(strengths), 0.7)
        expected_seed = int(
            hashlib.sha256(
                b"horse-walk-prompt-v2-guide-strength-ab-v1"
            ).hexdigest()[:16],
            16,
        ) & ((1 << 63) - 1)
        self.assertEqual(self.experiment["seed_int"], expected_seed)

    def test_prompt_has_explicit_four_beat_order_and_anatomy_constraints(self):
        prompt = self.experiment["positive_prompt_string"].lower()
        order = ["near hind", "near fore", "far hind", "far fore"]
        positions = [prompt.index(term) for term in order]
        self.assertEqual(positions, sorted(positions))
        for required in (
            "four-beat lateral-sequence",
            "at least two hooves support",
            "stance hoof stays planted",
            "fixed rigid long-bone segment lengths",
            "compact solid block",
            "moving pass-through phase",
        ):
            self.assertIn(required, prompt)

    def test_negative_prompt_names_observed_failure_modes(self):
        negative = self.experiment["negative_prompt_string"].lower()
        for required in (
            "curled distal limb",
            "hooked hoof",
            "hoof morphing",
            "changing segment length",
            "foot sliding",
            "hoof skating",
            "toe drag",
            "endpoint freeze",
            "pause at seam",
        ):
            self.assertIn(required, negative)


if __name__ == "__main__":
    unittest.main()
