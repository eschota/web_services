import hashlib
import json
import unittest
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS = BACKEND_ROOT / "animation_fitting" / "specs" / "experiments"
BASE_PATH = EXPERIMENTS / "horse_walk_prompt_v2_guide_strength_ab.v1.json"
SEMANTIC_PATH = (
    EXPERIMENTS
    / "horse_walk_prompt_v2_semantic_reference_guide_080.v1.json"
)


class HorseWalkSemanticReferenceExperimentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = json.loads(BASE_PATH.read_text(encoding="utf-8"))
        cls.semantic = json.loads(SEMANTIC_PATH.read_text(encoding="utf-8"))

    def test_generation_is_review_blocked_and_reference_is_fully_pinned(self):
        spec = self.semantic
        self.assertFalse(spec["approved_bool"])
        self.assertFalse(spec["generation_authorization_object"]["authorized_bool"])
        reference = spec["reference_object"]
        for key in (
            "source_bundle_immutable_manifest_sha256_string",
            "source_reference_rgb_sha256_string",
            "reference_png_sha256_string",
            "derivation_manifest_sha256_string",
            "immutable_manifest_sha256_string",
            "semantic_profile_sha256_string",
        ):
            self.assertRegex(reference[key], r"^[0-9a-f]{64}$")
        self.assertFalse(reference["manual_painting_used_bool"])
        self.assertFalse(reference["source_blend_required_bool"])
        self.assertFalse(reference["geometry_uv_normals_mutated_bool"])

    def test_reviewed_base_and_reference_artifact_hashes_are_exact(self):
        self.assertEqual(
            hashlib.sha256(BASE_PATH.read_bytes()).hexdigest(),
            self.semantic["base_experiment_object"]["sha256_string"],
        )
        reference = self.semantic["reference_object"]
        self.assertEqual(
            {
                "source_bundle_immutable_manifest_sha256_string": (
                    "f5e55c5073d09bc01dac90f4b7244f995fd42b0bdd37e09258cd4178e5573873"
                ),
                "source_reference_rgb_sha256_string": (
                    "94bf47cc137c0aaee975b2a75b7cd2b28f75215e282cdb6865bdd4095630a0b1"
                ),
                "reference_png_sha256_string": (
                    "92d0c2ac9e9570d2e2b4a221842943721fec3f143fbe1a6e783459e2d8c814f2"
                ),
                "derivation_manifest_sha256_string": (
                    "ca7cd2a412aa4eebb73b2579c7157b27a95eac27477c840125c411069f12d4c1"
                ),
                "immutable_manifest_sha256_string": (
                    "9a9ef0cebe407bad661b24ea5670a5fb76eb2b3c7a2888962a1985a516bd35b3"
                ),
                "semantic_profile_sha256_string": (
                    "0034d84e6d1eea841d02ee13708f9f84649224c41bce4a21074a7a21eea99562"
                ),
            },
            {
                key: reference[key]
                for key in (
                    "source_bundle_immutable_manifest_sha256_string",
                    "source_reference_rgb_sha256_string",
                    "reference_png_sha256_string",
                    "derivation_manifest_sha256_string",
                    "immutable_manifest_sha256_string",
                    "semantic_profile_sha256_string",
                )
            },
        )

    def test_only_reference_changes_from_selected_prompt_v2_point_eight(self):
        base = self.base
        semantic = self.semantic
        for key in (
            "base_action_id_string",
            "species_string",
            "generation_mode_string",
            "frame_count_int",
            "input_fps_int",
            "output_fps_int",
            "seed_int",
            "positive_prompt_string",
            "negative_prompt_string",
            "workflow_object",
        ):
            self.assertEqual(semantic[key], base[key], key)
        selected = next(
            variant
            for variant in base["variants_array"]
            if variant["variant_id_string"] == "guide_strength_0_80"
        )
        candidate = semantic["variants_array"]
        self.assertEqual(len(candidate), 1)
        self.assertEqual(
            candidate[0]["start_guide_strength_float"],
            selected["start_guide_strength_float"],
        )
        self.assertEqual(
            candidate[0]["end_guide_strength_float"],
            selected["end_guide_strength_float"],
        )
        self.assertIn("reference image only", semantic["controlled_variable_string"])

    def test_non_goals_block_weld_generic_reconciliation_and_ltx_repair_claims(self):
        text = " ".join(self.semantic["production_non_goals_array"]).lower()
        for required in (
            "physical glb weld",
            "generic postprocess weight averaging",
            "cannot repair disconnected topology",
            "connected-component count alone does not prove",
        ):
            self.assertIn(required, text)
        prerequisites = " ".join(
            self.semantic["production_prerequisites_array"]
        ).lower()
        for required in (
            "autorig weight generation",
            "coincident-rest-vertex separation gate",
            "fixed-camera visual phase qa",
        ):
            self.assertIn(required, prerequisites)


if __name__ == "__main__":
    unittest.main()
