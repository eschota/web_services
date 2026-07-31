import json
import unittest
from pathlib import Path

from renderfin.config import WORKFLOWS_DIR
from renderfin.templating import (
    SEED_MAX,
    render_workflow_text,
    sanitize_prompt,
    workflow_placeholders,
)

ALL_WORKFLOWS = (
    "t_pose.json",
    "gen_image.json",
    "gen_image_by_z_depth.json",
    "open_pose.json",
    "inpaint.json",
    "gen_animation_by_url.json",
    "image_to_3d.json",
)


def _load(name: str) -> str:
    return (WORKFLOWS_DIR / name).read_text(encoding="utf-8")


def _render(name: str, **kw):
    defaults = dict(
        width=1024,
        height=1024,
        prompt='a "brave" knight\nwith sword',
        negative_prompt="blurry",
        image_filename="input_abc.png",
        output_prefix="0b7a2f2e-1111-2222-3333-444455556666",
        workflow_type="",
    )
    defaults.update(kw)
    return render_workflow_text(_load(name), **defaults)


class AssetIntegrityTests(unittest.TestCase):
    def test_all_workflow_assets_present(self):
        for name in ALL_WORKFLOWS:
            self.assertTrue((WORKFLOWS_DIR / name).is_file(), name)

    def test_masks_present(self):
        from renderfin.config import MASKS_DIR

        for name in (
            "t_pose.jpg",
            "t_pose_long.jpg",
            "t_pose_fat.jpg",
            "t_pose_dwarf.jpg",
            "t_pose_goblin.jpg",
        ):
            self.assertTrue((MASKS_DIR / name).is_file(), name)

    def test_all_templates_render_to_valid_json(self):
        for name in ALL_WORKFLOWS:
            wf = _render(name, workflow_type="t_pose" if name == "t_pose.json" else "")
            self.assertIsInstance(wf, dict, name)
            self.assertTrue(all(isinstance(n, dict) for n in wf.values()), name)

    def test_t_pose_has_isolated_save_branch(self):
        wf = _render("t_pose.json", workflow_type="t_pose")
        prefixes = [
            n["inputs"].get("filename_prefix")
            for n in wf.values()
            if n.get("class_type") == "SaveImage"
        ]
        self.assertIn("0b7a2f2e-1111-2222-3333-444455556666", prefixes)
        self.assertIn("0b7a2f2e-1111-2222-3333-444455556666_Isolated", prefixes)
        rmbg = [n for n in wf.values() if n.get("class_type") == "RMBG"]
        self.assertEqual(len(rmbg), 1)
        self.assertEqual(rmbg[0]["inputs"]["background"], "Alpha")

    def test_image_to_3d_has_saveglb(self):
        wf = _render("image_to_3d.json")
        save = [n for n in wf.values() if n.get("class_type") == "SaveGLB"]
        self.assertEqual(len(save), 1)
        self.assertEqual(
            save[0]["inputs"]["filename_prefix"], "0b7a2f2e-1111-2222-3333-444455556666"
        )
        loads = [n for n in wf.values() if n.get("class_type") == "LoadImage"]
        self.assertEqual(loads[0]["inputs"]["image"], "input_abc.png")


class SubstitutionTests(unittest.TestCase):
    def test_prompt_json_escaped(self):
        wf = _render("t_pose.json", workflow_type="t_pose")
        node = wf["171"]
        self.assertEqual(node["inputs"]["text"], 'a "brave" knight\nwith sword')

    def test_glasses_stripped(self):
        self.assertEqual(sanitize_prompt("a man with glasses and a glass of water"), "a man with and a of water")
        self.assertEqual(sanitize_prompt("stained-glassware ok"), "stained-glassware ok")

    def test_prompt_length_clamped(self):
        self.assertLessEqual(len(sanitize_prompt("x" * 5000)), 2000)

    def test_gen_image_bare_width_height(self):
        wf = _render("gen_image.json", width=512, height=768)
        # bare $width/$height placeholders must produce valid ints
        node5 = wf["5"]
        self.assertEqual(node5["inputs"]["width"], 512)
        self.assertEqual(node5["inputs"]["height"], 768)


class SeedRandomizationTests(unittest.TestCase):
    def test_seeds_randomized(self):
        wf1 = _render("t_pose.json", workflow_type="t_pose")
        wf2 = _render("t_pose.json", workflow_type="t_pose")
        seeds1 = [n["inputs"]["noise_seed"] for n in wf1.values() if "noise_seed" in n.get("inputs", {})]
        seeds2 = [n["inputs"]["noise_seed"] for n in wf2.values() if "noise_seed" in n.get("inputs", {})]
        self.assertTrue(seeds1)
        self.assertNotEqual(seeds1, seeds2)
        for s in seeds1:
            self.assertTrue(1 <= s <= SEED_MAX)

    def test_ksampler_seed_randomized_in_image_to_3d(self):
        wf1 = _render("image_to_3d.json")
        wf2 = _render("image_to_3d.json")
        self.assertNotEqual(wf1["7"]["inputs"]["seed"], wf2["7"]["inputs"]["seed"])

    def test_fixed_seed(self):
        wf = _render("image_to_3d.json", seed=42)
        self.assertEqual(wf["7"]["inputs"]["seed"], 42)

    def test_disable_randomization(self):
        wf = _render("t_pose.json", workflow_type="t_pose", randomize_seeds=False)
        self.assertEqual(wf["25"]["inputs"]["noise_seed"], 650639755073450)


class NormalizationTests(unittest.TestCase):
    def test_width_height_stomped(self):
        wf = _render("t_pose.json", workflow_type="t_pose", width=1024, height=1024)
        helper = wf["139"]
        self.assertEqual(helper["inputs"]["width"], 1024)
        self.assertEqual(helper["inputs"]["height"], 1024)  # was 1280 on disk

    def test_t_pose_tiled_vae_rewritten(self):
        wf = _render("t_pose.json", workflow_type="t_pose")
        node = wf["223"]
        self.assertEqual(node["class_type"], "VAEDecode")
        self.assertEqual(set(node["inputs"].keys()), {"samples", "vae"})

    def test_tiled_vae_kept_for_other_types(self):
        wf = _render("t_pose.json", workflow_type="")
        self.assertEqual(wf["223"]["class_type"], "VAEDecodeTiled_TiledDiffusion")


class PlaceholderScanTests(unittest.TestCase):
    def test_expected_placeholders(self):
        self.assertEqual(
            workflow_placeholders(_load("image_to_3d.json")), ("$image", "$output_url")
        )
        self.assertIn("$prompt", workflow_placeholders(_load("t_pose.json")))


if __name__ == "__main__":
    unittest.main()
