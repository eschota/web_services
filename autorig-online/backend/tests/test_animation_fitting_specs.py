import json
import unittest
from pathlib import Path

from animation_fitting.specs import load_animation_fitting_specs


BACKEND_ROOT = Path(__file__).resolve().parents[1]


class AnimationFittingSpecsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        load_animation_fitting_specs.cache_clear()
        cls.specs = load_animation_fitting_specs()
        cls.taxonomy = json.loads(
            (BACKEND_ROOT / "animal_animation_taxonomy.v1.json").read_text(encoding="utf-8-sig")
        )

    def test_exact_30_action_order_and_frame_contract_match_taxonomy(self):
        expected = tuple(row["id"] for row in self.taxonomy["clips"])
        self.assertEqual(self.specs.action_order, expected)
        self.assertEqual(len(self.specs.actions), 30)
        self.assertTrue(all((action.frame_count - 1) % 8 == 0 for action in self.specs.actions.values()))
        for row in self.taxonomy["clips"]:
            action = self.specs.action(row["id"])
            self.assertEqual(action.is_loop, row["loop"])
            self.assertEqual(action.frame_count, row["frame_profile"])
            self.assertEqual(action.input_fps, self.taxonomy["source_fps"])
            self.assertEqual(action.output_fps, self.taxonomy["output_fps"])

    def test_loop_and_one_shot_actions_use_distinct_conditioning_contracts(self):
        loop_action = self.specs.action("walk_forward")
        one_shot_action = self.specs.action("death")
        loop_workflow = self.specs.workflow_for_action(loop_action.action_id)
        one_shot_workflow = self.specs.workflow_for_action(one_shot_action.action_id)

        self.assertTrue(loop_action.is_loop)
        self.assertIn("end_image", loop_workflow.bindings)
        self.assertEqual(
            tuple(frame["frame_index_expression_string"] for frame in loop_workflow.conditioned_frames),
            ("0", "N-1"),
        )
        self.assertFalse(one_shot_action.is_loop)
        self.assertNotIn("end_image", one_shot_workflow.bindings)
        self.assertEqual(
            tuple(frame["frame_index_expression_string"] for frame in one_shot_workflow.conditioned_frames),
            ("0",),
        )
        self.assertEqual(loop_workflow.output_fps, 30)
        self.assertEqual(one_shot_workflow.output_fps, 30)
        self.assertRegex(loop_workflow.workflow_fingerprint, r"^[a-f0-9]{64}$")
        self.assertRegex(one_shot_workflow.workflow_fingerprint, r"^[a-f0-9]{64}$")
        self.assertTrue(loop_workflow.post_sampling_guide_crop_required)
        self.assertTrue(one_shot_workflow.post_sampling_guide_crop_required)

    def test_prompt_is_species_specific_and_preserves_mode_instruction(self):
        loop_prompt = self.specs.action("run").render_positive_prompt(
            "horse", "strong diagonal support and readable hoof contacts"
        )
        one_shot_prompt = self.specs.action("death").render_positive_prompt("horse")

        self.assertIn("exact horse", loop_prompt)
        self.assertIn("seamless cyclic action", loop_prompt)
        self.assertIn("Additional motion direction", loop_prompt)
        self.assertIn("beginning from the supplied base-pose image", one_shot_prompt)
        self.assertIn("does not return to the starting pose", one_shot_prompt)

    def test_candidate_policy_is_8_top3_retry8_max16(self):
        policy = self.specs.qa.candidate_policy
        self.assertEqual(
            (policy.initial_count, policy.top_k, policy.retry_batch_count, policy.max_count),
            (8, 3, 8, 16),
        )
        self.assertEqual(self.specs.qa.calibration_state, "provisional-horse-v1")


if __name__ == "__main__":
    unittest.main()
