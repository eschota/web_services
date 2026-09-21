import unittest

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

    def test_explicit_values_win(self):
        entry = {"family": "flux", "recommended": {"steps": 20, "cfg": 1},
                 "recommended_from": "author examples"}
        got = defaults.resolve(entry, None, {"steps": 12, "cfg": 2.5})
        self.assertEqual((got["steps"], got["cfg"]), (12, 2.5))

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
        self.assertEqual(defaults.control_workflow("flux", "canny"),
                         "gen_image_control_canny.json")
        with self.assertRaises(ValueError):
            defaults.control_workflow("flux2", "pose")


if __name__ == "__main__":
    unittest.main()
