"""FLUX.1 -> Z-Image Turbo / Krea 2 port of the farm image templates (2026-09-23).

The public workflow tokens (gen_image.json, t_pose.json, the control and
enhancement templates) keep their names so saved graphs and the farm's
advertised lists still match; only what is inside them changed. These tests pin
the parts of that contract a later edit could silently break.
"""
import json
import os
import re
import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from renderfin import routing, templating  # noqa: E402
from renderfin.models import RenderPrompt  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402
import ai_model_defaults  # noqa: E402

WORKFLOWS = BACKEND / "renderfin" / "assets" / "workflows"
ZIT = "z_image_turbo_fp8_e4m3fn.safetensors"
UNION = "Z-Image-Turbo-Fun-Controlnet-Union-2.1-2602-8steps.safetensors"
PORTED = (
    "gen_image.json", "t_pose.json", "open_pose.json", "gen_image_by_z_depth.json",
    "inpaint.json", "gen_image_control_pose.json", "gen_image_control_depth.json",
    "gen_image_control_canny.json", "detail_plain.json", "detail_tiled.json",
    "face_fix.json", "face_fix_skin.json", "upscale_refine.json",
)
FLUX1_FILES = (
    "flux1-schnell", "FLUX.1-dev-ControlNet-Union-Pro", "FLUX.1-Fill-dev", "aidmaMJ6.1",
    "t5xxl", "clip_l.safetensors",
)


def render(name, *, ptype="", control_strength=0.8, width=1024, height=1024, **extra):
    prompt = RenderPrompt(prompt="a knight", type=ptype, control_strength=control_strength,
                          main_size_width=width, main_size_height=height)
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    workflow = templating.render_workflow_text(
        text, width=width, height=height, prompt="a knight", negative_prompt="",
        image_filename="input.png", output_prefix="task123", workflow_type=ptype, **extra)
    apply_runtime_settings(workflow, prompt, width, height)
    return workflow


def nodes(workflow, class_type):
    return [node for node in workflow.values() if node.get("class_type") == class_type]


class PortedTemplateTests(unittest.TestCase):
    def test_no_ported_template_loads_a_flux1_file(self):
        for name in PORTED:
            text = (WORKFLOWS / name).read_text(encoding="utf-8")
            for needle in FLUX1_FILES:
                self.assertNotIn(needle, text, f"{name} still loads {needle}")

    def test_every_ported_template_runs_the_z_image_contract(self):
        for name in PORTED:
            workflow = render(name)
            unet = nodes(workflow, "UNETLoader")
            self.assertEqual([n["inputs"]["unet_name"] for n in unet], [ZIT], name)
            clip = nodes(workflow, "CLIPLoader")
            self.assertEqual([(n["inputs"]["clip_name"], n["inputs"]["type"]) for n in clip],
                             [("qwen_3_4b.safetensors", "lumina2")], name)
            self.assertEqual([n["inputs"]["vae_name"] for n in nodes(workflow, "VAELoader")],
                             ["ae.safetensors"], name)
            # Z-Image is a flow model sampled at AuraFlow shift 3.
            self.assertEqual([n["inputs"]["shift"] for n in nodes(workflow, "ModelSamplingAuraFlow")],
                             [3], name)
            self.assertFalse(nodes(workflow, "DualCLIPLoader"), name)

    def test_control_templates_use_the_union_patch_and_follow_the_request_strength(self):
        for channel in ("pose", "depth", "canny"):
            workflow = render(f"gen_image_control_{channel}.json",
                              ptype=f"image_control_{channel}", control_strength=0.65)
            self.assertEqual([n["inputs"]["name"] for n in nodes(workflow, "ModelPatchLoader")], [UNION])
            control = nodes(workflow, "ZImageFunControlnet")
            self.assertEqual([n["inputs"]["strength"] for n in control], [0.65])
            self.assertFalse(nodes(workflow, "ControlNetApplyAdvanced"))

    def test_tuned_control_strengths_survive_runtime_settings(self):
        tpose = render("t_pose.json", ptype="t_pose", control_strength=0.2)
        self.assertEqual([n["inputs"]["strength"] for n in nodes(tpose, "ZImageFunControlnet")], [0.85])
        inpaint = render("inpaint.json", ptype="inpaint", control_strength=0.2)
        control = nodes(inpaint, "ZImageFunControlnet")
        self.assertEqual([n["inputs"]["strength"] for n in control], [1.0])
        # the inpaint patch works from the cropped picture and its hole
        self.assertIn("inpaint_image", control[0]["inputs"])
        self.assertIn("mask", control[0]["inputs"])

    def test_t_pose_delivers_the_task_owned_full_and_isolated_pair(self):
        workflow = render("t_pose.json", ptype="t_pose")
        prefixes = sorted(n["inputs"]["filename_prefix"] for n in nodes(workflow, "SaveImage"))
        self.assertEqual(prefixes, ["task123", "task123_Isolated"])
        rmbg = nodes(workflow, "RMBG")
        self.assertEqual([n["inputs"]["background"] for n in rmbg], ["Alpha"])
        # the pose skeleton is padded to the render size, never cropped
        self.assertTrue(nodes(workflow, "ResizeAndPadImage"))

    def test_t_pose_still_routes_through_gen_image_token_at_1024(self):
        prompt = RenderPrompt(prompt="x", type="t_pose", image_url="https://x/t_pose.jpg")
        self.assertEqual(routing.scheduling_token(prompt), "gen_image.json")
        self.assertEqual(routing.resolve_workflow_file(prompt), ("t_pose.json", (1024, 1024)))

    def test_t_pose_renders_square_whatever_landscape_default_it_is_sent(self):
        # /api/image sends the product default 960x540; the square skeleton and
        # the T-pose quality gate need a square render.
        api = RenderPrompt(prompt="x", type="t_pose", image_url="https://x/t_pose.jpg",
                           main_size_width=960, main_size_height=540)
        self.assertEqual(routing.resolve_workflow_file(api), ("t_pose.json", (1024, 1024)))
        square = RenderPrompt(prompt="x", type="t_pose", image_url="https://x/t_pose.jpg",
                              main_size_width=1536, main_size_height=1536)
        self.assertEqual(routing.resolve_workflow_file(square), ("t_pose.json", None))

    def test_a_chosen_z_image_finetune_replaces_the_base(self):
        workflow = render("gen_image.json", checkpoint="cyberrealisticZImage_v80_fp8mixed.safetensors")
        self.assertEqual([n["inputs"]["unet_name"] for n in nodes(workflow, "UNETLoader")],
                         ["cyberrealisticZImage_v80_fp8mixed.safetensors"])

    def test_a_lora_stack_attaches_behind_the_unet(self):
        if not hasattr(templating, "apply_lora_stack"):
            self.skipTest("LoRA stacks are not in this tree")
        text = (WORKFLOWS / "gen_image.json").read_text(encoding="utf-8")
        workflow = templating.render_workflow_text(
            text, width=1024, height=1024, prompt="a knight", negative_prompt="",
            image_filename="", output_prefix="t",
            loras=[{"name": "Z-Detail-Slider.safetensors", "strength_model": 1.5}])
        lora = nodes(workflow, "LoraLoaderModelOnly")
        self.assertEqual([n["inputs"]["lora_name"] for n in lora], ["Z-Detail-Slider.safetensors"])
        shift = nodes(workflow, "ModelSamplingAuraFlow")[0]
        self.assertEqual(workflow[shift["inputs"]["model"][0]]["class_type"], "LoraLoaderModelOnly")

    def test_krea2_template_is_the_official_turbo_graph(self):
        workflow = render("gen_image_krea2.json")
        self.assertEqual([n["inputs"]["unet_name"] for n in nodes(workflow, "UNETLoader")],
                         ["krea2_turbo_fp8_scaled.safetensors"])
        self.assertEqual([(n["inputs"]["clip_name"], n["inputs"]["type"]) for n in nodes(workflow, "CLIPLoader")],
                         [("qwen3vl_4b_fp8_scaled.safetensors", "krea2")])
        sampler = nodes(workflow, "KSampler")[0]["inputs"]
        self.assertEqual((sampler["steps"], sampler["cfg"], sampler["sampler_name"]), (8, 1, "euler"))
        self.assertEqual(routing.scheduling_token(
            RenderPrompt(prompt="x", work_flow="gen_image_krea2.json")), "gen_image_krea2.json")


class FamilyRoutingTests(unittest.TestCase):
    def test_control_generation_moved_from_flux1_to_z_image(self):
        for channel in ("pose", "depth", "canny"):
            self.assertEqual(ai_model_defaults.control_workflow("zimage", channel),
                             f"gen_image_control_{channel}.json")
        with self.assertRaises(ValueError):
            ai_model_defaults.control_workflow("flux", "pose")

    def test_family_defaults_pick_the_new_templates(self):
        self.assertEqual(ai_model_defaults.FAMILY_WORKFLOWS["zimage"], "gen_image.json")
        self.assertEqual(ai_model_defaults.FAMILY_WORKFLOWS["krea2"], "gen_image_krea2.json")
        self.assertNotIn("flux", ai_model_defaults.FAMILY_WORKFLOWS)

    def test_catalogue_declares_the_new_image_defaults(self):
        path = BACKEND.parent / "deploy" / "ai-models" / "model_catalogue.json"
        entries = json.loads(path.read_text(encoding="utf-8"))
        by_id = {entry.get("id"): entry for entry in entries}
        zit = by_id["z_image_turbo_fp8_e4m3fn.safetensors"]
        krea = by_id["krea2_turbo_fp8_scaled.safetensors"]
        self.assertEqual(ai_model_defaults.model_family(zit), "zimage")
        self.assertEqual(ai_model_defaults.model_family(krea), "krea2")
        self.assertIn("zimage", zit["default_for_families"])
        image_defaults = [e["id"] for e in entries if "image" in (e.get("default_for_services") or [])]
        self.assertEqual(image_defaults, ["krea2_turbo_fp8_scaled.safetensors"])
        # every template a catalogue checkpoint names must exist
        for entry in entries:
            workflow = entry.get("workflow")
            if entry.get("usable") and workflow:
                self.assertTrue((WORKFLOWS / workflow).is_file(), workflow)


if __name__ == "__main__":
    unittest.main()
