"""LoRA stacks in renderfin: injection, legacy compatibility, dispatch eligibility."""

from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from pydantic import ValidationError  # noqa: E402

from renderfin import model_eligibility, templating  # noqa: E402
from renderfin.models import RenderPrompt, RenderServer  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402

WORKFLOWS = BACKEND / "renderfin" / "assets" / "workflows"
STACK = [{"name": "darth-vader-pxl.safetensors", "strength_model": 0.8, "strength_clip": 0.8},
         {"name": "add-detail-xl.safetensors", "strength_model": 0.6, "strength_clip": 0.6}]


def render(template: str, **kwargs):
    defaults = dict(width=1024, height=1024, prompt="p", negative_prompt="n",
                    image_filename="", output_prefix="task", seed=728158258896843)
    defaults.update(kwargs)
    return templating.render_workflow_text(
        (WORKFLOWS / template).read_text(encoding="utf-8"), **defaults)


class SdxlStackTests(unittest.TestCase):
    def test_the_stack_becomes_a_lora_loader_chain_with_clip(self):
        wf = render("gen_image_sdxl.json", loras=STACK)
        first, second = "autorig_lora_1_0", "autorig_lora_1_1"
        self.assertEqual(wf[first]["class_type"], "LoraLoader")
        self.assertEqual(wf[first]["inputs"]["model"], ["1", 0])
        self.assertEqual(wf[first]["inputs"]["clip"], ["1", 1])
        self.assertEqual(wf[first]["inputs"]["lora_name"], "darth-vader-pxl.safetensors")
        self.assertEqual(wf[second]["inputs"]["model"], [first, 0])
        self.assertEqual(wf[second]["inputs"]["clip"], [first, 1])
        self.assertEqual(wf[second]["inputs"]["strength_clip"], 0.6)
        # The sampler and the clip-skip node read the end of the chain; the
        # VAE still comes straight from the checkpoint.
        self.assertEqual(wf["5"]["inputs"]["model"], [second, 0])
        self.assertEqual(wf["8"]["inputs"]["clip"], [second, 1])
        self.assertEqual(wf["6"]["inputs"]["vae"], ["1", 2])

    def test_same_graph_as_the_civitai_reference(self):
        """Structure of civitai.com/images/139610195 (ComfyUI-made)."""
        wf = render("gen_image_sdxl.json", loras=STACK,
                    checkpoint="cyberrealisticPony_v180Coreshift_2764472.safetensors")
        chain = [wf[n]["inputs"]["lora_name"] for n in ("autorig_lora_1_0", "autorig_lora_1_1")]
        self.assertEqual(chain, ["darth-vader-pxl.safetensors", "add-detail-xl.safetensors"])
        self.assertEqual(wf["1"]["inputs"]["ckpt_name"],
                         "cyberrealisticPony_v180Coreshift_2764472.safetensors")
        self.assertEqual(wf["5"]["inputs"]["seed"], 728158258896843)

    def test_legacy_single_lora_still_works_and_combines_with_a_stack(self):
        single = render("gen_image_sdxl.json", lora="style.safetensors", lora_strength=0.5)
        self.assertEqual(single["autorig_selected_lora"]["inputs"]["model"], ["1", 0])
        both = render("gen_image_sdxl.json", lora="style.safetensors", lora_strength=0.5, loras=STACK)
        self.assertEqual(both["autorig_selected_lora"]["inputs"]["model"], ["autorig_lora_1_1", 0])
        self.assertEqual(both["5"]["inputs"]["model"], ["autorig_selected_lora", 0])

    def test_no_stack_changes_nothing(self):
        self.assertEqual(render("gen_image_sdxl.json"), render("gen_image_sdxl.json", loras=[]))


class UnetStackTests(unittest.TestCase):
    def test_a_unet_loader_gets_model_only_loaders(self):
        wf = render("gen_image_flux2_klein.json", loras=STACK)
        self.assertEqual(wf["autorig_lora_model_0"]["class_type"], "LoraLoaderModelOnly")
        self.assertEqual(wf["guider"]["inputs"]["model"], ["autorig_lora_model_1", 0])
        self.assertEqual(wf["positive"]["inputs"]["clip"], ["clip", 0])

    def test_single_lora_on_a_unet_workflow_survives_a_stack(self):
        prompt = RenderPrompt(lora="style.safetensors", lora_strength=0.7, loras=STACK)
        wf = render("gen_image_flux2_klein.json", lora=prompt.lora, loras=prompt.loras)
        apply_runtime_settings(wf, prompt, 1024, 1024)
        names = sorted(n["inputs"]["lora_name"] for n in wf.values()
                       if n.get("class_type", "").startswith("LoraLoader"))
        self.assertEqual(names, ["add-detail-xl.safetensors", "darth-vader-pxl.safetensors",
                                 "style.safetensors"])

    def test_a_workflow_without_a_loader_refuses(self):
        with self.assertRaises(ValueError):
            templating.apply_lora_stack({"1": {"class_type": "SaveImage", "inputs": {}}}, STACK)


class PromptModelTests(unittest.TestCase):
    def test_stack_round_trips_through_json(self):
        prompt = RenderPrompt(loras=STACK)
        again = RenderPrompt(**json.loads(prompt.model_dump_json()))
        self.assertEqual([i.model_dump() for i in again.loras], STACK)

    def test_old_saved_prompts_still_load(self):
        self.assertEqual(RenderPrompt(**{"prompt": "x", "lora": "a.safetensors"}).loras, [])

    def test_names_must_be_plain_files(self):
        for bad in ("../x.safetensors", "C:/x.safetensors", "x.exe", "/abs.safetensors"):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                RenderPrompt(loras=[{"name": bad}])
        with self.assertRaises(ValidationError):
            RenderPrompt(loras=[{"name": "a.safetensors", "strength_model": 9}])
        with self.assertRaises(ValidationError):
            RenderPrompt(loras=[{"name": f"l{i}.safetensors"} for i in range(9)])


class Response:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class Client:
    def __init__(self, files): self.files = files

    async def get(self, url, **kwargs):
        kind = url.rsplit("/", 1)[-1]
        slot = {"UNETLoader": "unet_name", "UnetLoaderGGUF": "unet_name",
                "CheckpointLoaderSimple": "ckpt_name"}.get(kind, "lora_name")
        return Response({kind: {"input": {"required": {slot: [self.files.get(kind, [])]}}}})


class EligibilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        model_eligibility._cache.clear()
        self.server = RenderServer(render_server_name="Raptor", render_server_url="http://127.0.0.1:8288")

    async def test_every_lora_of_the_stack_must_be_on_the_box(self):
        client = Client({"CheckpointLoaderSimple": ["pony.safetensors"],
                         "LoraLoader": ["darth-vader-pxl.safetensors"],
                         "LoraLoaderModelOnly": ["darth-vader-pxl.safetensors"]})
        prompt = RenderPrompt(checkpoint="pony.safetensors", loras=STACK)
        self.assertFalse(await model_eligibility.can_load(client, self.server, prompt))
        client.files["LoraLoader"].append("add-detail-xl.safetensors")
        model_eligibility._cache.clear()
        self.assertTrue(await model_eligibility.can_load(client, self.server, prompt))

    async def test_a_civitai_named_copy_of_the_checkpoint_serves_the_canonical_name(self):
        client = Client({"CheckpointLoaderSimple": ["cyberrealisticPony_v180Coreshift_2764472.safetensors"],
                         "LoraLoader": [], "LoraLoaderModelOnly": []})
        prompt = RenderPrompt(checkpoint="CyberRealisticPony_V18.0_F16.safetensors")
        self.assertTrue(await model_eligibility.can_load(client, self.server, prompt))
        self.assertEqual(model_eligibility.local_name(self.server, "checkpoint", prompt.checkpoint),
                         "cyberrealisticPony_v180Coreshift_2764472.safetensors")

    async def test_the_canonical_name_wins_when_the_box_has_it(self):
        client = Client({"CheckpointLoaderSimple": ["CyberRealisticPony_V18.0_F16.safetensors"]})
        prompt = RenderPrompt(checkpoint="CyberRealisticPony_V18.0_F16.safetensors")
        self.assertTrue(await model_eligibility.can_load(client, self.server, prompt))
        self.assertEqual(model_eligibility.local_name(self.server, "checkpoint", prompt.checkpoint),
                         "CyberRealisticPony_V18.0_F16.safetensors")


if __name__ == "__main__":
    unittest.main()
