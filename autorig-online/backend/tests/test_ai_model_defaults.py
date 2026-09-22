import json
import unittest
from pathlib import Path

import ai_model_defaults as defaults


class ModelDefaultsTests(unittest.TestCase):
    def test_pony_selects_sdxl_workflow_and_normalises_sampler(self):
        checkpoint = {"family": "pony", "recommended": {
            "steps": 30, "cfg": 5, "sampler": "DPM++ 2M SDE"},
            "recommended_from": "the author's example images"}
        got = defaults.resolve(checkpoint, None, {})
        self.assertEqual(got["work_flow"], "gen_image_sdxl.json")
        self.assertEqual(got["sampler"], "dpmpp_2m_sde")
        self.assertEqual(got["steps"], 30)

    def test_author_resolution_never_changes_product_or_user_resolution(self):
        entry = {"family": "pony", "recommended": {"steps": 30, "width": 1024, "height": 1536},
                 "recommended_from": "author example"}
        effective = defaults.resolve(entry, None, {})
        self.assertNotIn("width", effective)
        self.assertNotIn("height", effective)
        self.assertEqual(defaults.resolve(entry, None, {"width": 960, "height": 540})["height"], 540)

    def test_explicit_values_win(self):
        entry = {"family": "flux", "recommended": {"steps": 20, "cfg": 1},
                 "recommended_from": "author examples"}
        got = defaults.resolve(entry, None, {"steps": 12, "cfg": 2.5})
        self.assertEqual((got["steps"], got["cfg"]), (12, 2.5))

    def test_quality_auto_does_not_relabel_author_example_or_change_manual_steps(self):
        entry = {"family": "pony", "recommended": {"steps": 30, "cfg": 5},
                 "recommended_from": "Author: 30+",
                 "sampling_policy": {"auto_steps": 50, "auto_reason": "Product quality preset"}}
        self.assertEqual(defaults.resolve(entry, None, {})["steps"], 50)
        self.assertEqual(defaults.resolve(entry, None, {"steps": 34})["steps"], 34)
        self.assertEqual(entry["recommended"]["steps"], 30)

    def test_distilled_fixed_schedule_and_ineffective_controls_fail_before_queue(self):
        entry = {"family": "flux2", "recommended": {"steps": 4, "cfg": 1},
                 "recommended_from": "BFL reference",
                 "sampling_policy": {"fixed_steps": 4, "cfg_mode": "fixed", "cfg_value": 1,
                                     "scheduler_mode": "native", "scheduler_label": "FLUX.2 native"}}
        for explicit in ({"steps": 30}, {"cfg": 7}, {"scheduler": "karras"}):
            with self.assertRaises(ValueError):
                defaults.resolve(entry, None, explicit)
        effective = defaults.resolve(entry, None, {"cfg": 0})
        self.assertEqual(effective["cfg"], 1)
        self.assertNotIn("scheduler", effective)

    def test_schnell_auto_uses_top_of_author_range(self):
        entry = {"family": "flux", "recommended": {"steps": 4},
                 "recommended_from": "BFL: 1-4 steps", "sampling_policy": {"steps_max": 4}}
        self.assertEqual(defaults.resolve(entry, None, {"steps": 2})["steps"], 2)
        with self.assertRaises(ValueError):
            defaults.resolve(entry, None, {"steps": 20})

    def test_unattributed_metadata_is_not_applied(self):
        entry = {"family": "flux", "recommended": {"steps": 99}}
        self.assertNotIn("steps", defaults.resolve(entry, None, {}))

    def test_cross_family_pair_is_rejected(self):
        with self.assertRaises(ValueError):
            defaults.resolve({"family": "pony"}, {"family": "flux"}, {})

    def test_trigger_is_added_only_when_missing(self):
        entry = {"triggers": ["skin texture style"]}
        self.assertEqual(defaults.add_triggers("portrait", [entry]),
                         "skin texture style, portrait")
        self.assertEqual(defaults.add_triggers("Skin Texture Style portrait", [entry]),
                         "Skin Texture Style portrait")

    def test_flux2_uses_its_own_workflow(self):
        got = defaults.resolve({"family": "flux2"}, None, {})
        self.assertEqual(got["work_flow"], "gen_image_flux2_klein.json")

    def test_already_normalised_runtime_sampler_is_preserved(self):
        for sampler in ("dpmpp_2m", "euler_ancestral_cfg_pp"):
            entry = {"family": "pony", "recommended": {"sampler": sampler},
                     "recommended_from": "author documentation"}
            self.assertEqual(defaults.resolve(entry, None, {})["sampler"], sampler)

    def test_ltx23_and_legacy_ltx_do_not_mix(self):
        self.assertFalse(defaults.compatible(
            {"family": "ltx", "base": "LTXV 2.3"},
            {"family": "ltx", "base": "LTXV"}))

    def test_old_pony_filename_maps_to_installed_canonical_file(self):
        self.assertEqual(defaults.canonical_file(
            "cyberrealisticPony_v180Coreshift_2764472.safetensors"),
            "CyberRealisticPony_V18.0_F16.safetensors")

    def test_control_workflow_matches_model_family(self):
        self.assertEqual(defaults.control_workflow("pony", "pose"),
                         "gen_image_sdxl_control_pose.json")
        self.assertEqual(defaults.control_workflow("sdxl", "depth"),
                         "gen_image_sdxl_control_depth.json")
        self.assertEqual(defaults.control_workflow("zimage", "canny"),
                         "gen_image_control_canny.json")
        with self.assertRaises(ValueError):
            defaults.control_workflow("flux2", "pose")
        # FLUX.1 left the farm on 2026-09-23; its control graphs are Z-Image now.
        with self.assertRaises(ValueError):
            defaults.control_workflow("flux", "canny")

    def test_lora_family_default_is_explicit_not_catalogue_order(self):
        lora = {"kind": "lora", "family": "flux", "services": ["image"]}
        entries = [
            {"kind": "checkpoint", "family": "flux2", "usable": True,
             "services": ["image"], "file": "modern-default.safetensors"},
            {"kind": "checkpoint", "family": "flux", "usable": True,
             "services": ["image"], "file": "unmarked.safetensors"},
            {"kind": "checkpoint", "family": "flux", "usable": True,
             "services": ["image"], "default_for_families": ["flux"],
             "file": "flux1-schnell.safetensors"},
        ]
        chosen = defaults.family_default_checkpoint(entries, lora, "image")
        self.assertEqual(chosen["file"], "flux1-schnell.safetensors")

    def test_lora_family_without_declared_default_does_not_guess(self):
        lora = {"kind": "lora", "family": "flux", "services": ["image"]}
        entries = [{"kind": "checkpoint", "family": "flux", "usable": True,
                    "services": ["image"], "file": "first-is-not-a-rule.safetensors"}]
        self.assertIsNone(defaults.family_default_checkpoint(entries, lora, "image"))

    def test_explicit_pony_default_may_cover_compatible_sdxl_lora(self):
        lora = {"kind": "lora", "family": "sdxl", "services": ["image"]}
        pony = {"kind": "checkpoint", "family": "pony", "usable": True,
                "services": ["image"], "default_for_families": ["pony", "sdxl"],
                "file": "pony.safetensors"}
        self.assertIs(
            defaults.family_default_checkpoint([pony], lora, "image"), pony)

    def test_dev_lora_sampling_metadata_does_not_replace_schnell_defaults(self):
        checkpoint = {
            "kind": "checkpoint", "family": "flux", "base": "FLUX.1 Schnell",
            "recommended": {"steps": 4, "sampler": "euler", "scheduler": "simple"},
            "recommended_from": "BFL reference implementation",
        }
        lora = {
            "kind": "lora", "family": "flux", "base": "Flux.1 D",
            "recommended": {"steps": 30, "cfg": 5, "sampler": "DPM++ 2M",
                            "strength": 0.8},
            "recommended_from": "author examples on the training base",
            "sampling_recommendations_compatible": False,
        }
        got = defaults.resolve(checkpoint, lora, {})
        self.assertEqual(got["steps"], 4)
        self.assertEqual(got["sampler"], "euler")
        self.assertEqual(got["scheduler"], "simple")
        self.assertNotIn("cfg", got)
        self.assertEqual(got["lora_strength"], 0.8)

    def test_every_current_ltx23_lora_resolves_the_concrete_distilled_base(self):
        catalogue_path = (
            Path(__file__).resolve().parents[2]
            / "deploy" / "ai-models" / "model_catalogue.json"
        )
        entries = json.loads(catalogue_path.read_text(encoding="utf-8"))
        loras = [entry for entry in entries
                 if entry.get("kind") == "lora"
                 and defaults.model_family(entry) == "ltx23"
                 and entry.get("usable")]
        self.assertEqual(len(loras), 6)
        for lora in loras:
            checkpoint = defaults.family_default_checkpoint(entries, lora, "video")
            self.assertIsNotNone(checkpoint, lora["file"])
            # LTX 2.3 adapters render on LTX-2.5 since the 2026-09-23 migration.
            self.assertEqual(
                checkpoint["file"],
                "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors",
            )
            effective = defaults.resolve(checkpoint, lora, {})
            self.assertEqual(effective["work_flow"], "gen_animation_ltx25_by_url.json")
            self.assertEqual(effective["steps"], 8)
            explicit = defaults.resolve(checkpoint, lora, {"steps": 8, "cfg": 1.2})
            self.assertEqual((explicit["steps"], explicit["cfg"]), (8, 1.2))
            with self.assertRaises(ValueError):
                defaults.resolve(checkpoint, lora, {"steps": 12})

    def test_legacy_dream_ltxv_has_no_false_ltx23_fallback(self):
        catalogue_path = (
            Path(__file__).resolve().parents[2]
            / "deploy" / "ai-models" / "model_catalogue.json"
        )
        entries = json.loads(catalogue_path.read_text(encoding="utf-8"))
        dream = next(entry for entry in entries
                     if entry.get("file") == "DreamLTXV.safetensors")
        self.assertFalse(dream["usable"])
        self.assertTrue(dream["obsolete"])
        self.assertIsNone(defaults.family_default_checkpoint(entries, dream, "video"))


if __name__ == "__main__":
    unittest.main()
