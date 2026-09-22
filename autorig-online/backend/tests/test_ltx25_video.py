"""LTX-2.5: the video engine that replaces LTX 2.3 / LTX-2 19B / LTXV 13B.

Pinned here:
  * the LTX-2.5 templates parse after renderfin's substitution and load only
    LTX-2.5 files (no 2.3 transformer, Gemma 3 encoder or 2.3 VAEs),
  * the eight-step distilled schedule, 24 fps and the requested length,
  * the optional end frame is pruned when no end image is sent,
  * the two-stage HQ graph keeps its half-size first pass after runtime
    settings (the stage-1 scale is exempt from the delivery resize),
  * pose / depth / canny control is LTX-2.5 + the 2.3 Union IC-LoRA,
  * 2.3 LoRAs are accepted on LTX-2.5, not the other way round,
  * a video quality label picks the two-stage graph for LTX-2.5 only.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_model_defaults  # noqa: E402
from renderfin.config import WORKFLOWS_DIR  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402
from renderfin.templating import render_workflow_text  # noqa: E402

DIT = "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors"
TEXT_ENCODER = "gemma4-12b-with-proj-ltx-2.5-comfy-int8-convrot.safetensors"
VIDEO_VAE = "ltx-2.5-video-vae-bf16.safetensors"
AUDIO_VAE = "ltx-2.5-audio-vae-bf16.safetensors"
UPSCALER = "ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors"
UNION = "ltx-2.3-22b-ic-lora-union-control-ref0.5.safetensors"
STANDARD = "gen_animation_ltx25_by_url.json"
HQ = "gen_animation_ltx25_hq_by_url.json"
CONTROL = ("gen_video_ltx23_control_by_url.json", "gen_video_ltx23_pose_by_url.json",
           "gen_video_ltx23_depth_by_url.json")
RETIRED = ("ltx-2.3-22b-distilled", "gemma_3_12B", "LTX23_", "ltx-2-19b", "ltxv-13b", "t5xxl", "ltx10eros")


def _render(name, *, frames=97, width=960, height=540, image_end="", control_video=""):
    text = (WORKFLOWS_DIR / name).read_text(encoding="utf-8")
    return render_workflow_text(
        text, width=width, height=height, prompt="a woman turns and smiles",
        negative_prompt="blurry", image_filename="in.png", image_end_filename=image_end,
        control_video_filename=control_video, output_prefix="task-1", frames=frames,
        randomize_seeds=False)


def _runtime(workflow, *, frames=97, width=960, height=540, ptype=""):
    prompt = SimpleNamespace(type=ptype, lora="", lora_strength=None, frame_count=frames,
                             control_strength=0.8, steps=0, creativity=0, cfg=None,
                             sampler="", scheduler="", clip_skip=None)
    return apply_runtime_settings(workflow, prompt, width, height)


def _of(workflow, class_type):
    return [node for node in workflow.values() if node.get("class_type") == class_type]


class TemplateFilesTests(unittest.TestCase):
    def test_every_ltx25_template_loads_only_ltx25_files(self):
        for name in (STANDARD, HQ) + CONTROL:
            text = (WORKFLOWS_DIR / name).read_text(encoding="utf-8")
            for retired in RETIRED:
                self.assertNotIn(retired, text, f"{name} still names {retired}")
            workflow = _render(name, control_video="drive.mp4" if name in CONTROL else "")
            self.assertEqual([n["inputs"]["unet_name"] for n in _of(workflow, "UNETLoader")], [DIT], name)
            clip = _of(workflow, "CLIPLoader")
            self.assertEqual([(n["inputs"]["clip_name"], n["inputs"]["type"]) for n in clip],
                             [(TEXT_ENCODER, "ltxv")], name)
            vaes = sorted(n["inputs"]["vae_name"] for n in _of(workflow, "VAELoader"))
            self.assertEqual(vaes, sorted([VIDEO_VAE, AUDIO_VAE]), name)

    def test_the_distilled_schedule_and_the_products_frame_rate(self):
        for name in (STANDARD, HQ):
            workflow = _render(name)
            schedules = [n["inputs"]["sigmas"] for n in _of(workflow, "ManualSigmas")]
            self.assertEqual(len(schedules), 1, name)
            self.assertEqual(len(schedules[0].split(",")) - 1, 8, name)
            self.assertEqual({n["inputs"]["fps"] for n in _of(workflow, "CreateVideo")}, {24}, name)
            self.assertEqual({n["inputs"]["frame_rate"] for n in _of(workflow, "LTXVConditioning")}, {24}, name)
            self.assertEqual({n["inputs"]["frames_number"] for n in _of(workflow, "LTXVEmptyLatentAudio")},
                             {97}, name)

    def test_the_end_frame_is_optional(self):
        for name in (STANDARD, HQ):
            without = _render(name)
            self.assertFalse(any(n.get("inputs", {}).get("image") == "" for n in _of(without, "LoadImage")),
                             f"{name} kept an empty end-frame loader")
            self.assertEqual(len(_of(without, "LoadImage")), 1, name)
            with_end = _render(name, image_end="last.png")
            self.assertEqual(sorted(n["inputs"]["image"] for n in _of(with_end, "LoadImage")),
                             ["in.png", "last.png"], name)
            guide = next(n for n in _of(with_end, "LTXVAddGuide") if n["inputs"]["frame_idx"] == -1)
            self.assertEqual(guide["inputs"]["strength"], 0.7)

    def test_the_standard_graph_samples_at_the_delivery_size(self):
        workflow = _runtime(_render(STANDARD, frames=97), frames=97, width=960, height=540)
        latent = _of(workflow, "EmptyLTXVLatentVideo")[0]["inputs"]
        self.assertEqual((latent["width"], latent["height"], latent["length"]), (960, 544, 97))
        delivery = [n for n in _of(workflow, "ImageScale")
                    if n["inputs"].get("image") and n["inputs"]["width"] == 960 and n["inputs"]["height"] == 540]
        self.assertTrue(delivery, "no delivery resize to the requested 960x540")


class TwoStageTests(unittest.TestCase):
    def test_stage_one_runs_at_half_size_and_is_upscaled(self):
        workflow = _runtime(_render(HQ), width=960, height=540)
        halves = [n for n in _of(workflow, "ImageScale") if (n.get("_meta") or {}).get("title") == "delivery"
                  and isinstance(n["inputs"]["width"], list)]
        self.assertEqual(len(halves), 1, "the half-size stage-1 scale was overwritten")
        expressions = {n["inputs"]["expression"] for n in _of(workflow, "ComfyMathExpression")}
        self.assertEqual(expressions, {"ceil(a/64)*32"})
        # ceil(960/64)*32 = 480 and ceil(544/64)*32 = 288: x2 lands on 960x576,
        # which the delivery resize crops to the 960x540 asked for.
        self.assertEqual(sorted(n["inputs"]["values.a"] for n in _of(workflow, "ComfyMathExpression")),
                         [540, 960])
        self.assertEqual([n["inputs"]["model_name"] for n in _of(workflow, "LatentUpscaleModelLoader")],
                         [UPSCALER])
        self.assertEqual(len(_of(workflow, "SamplerCustomAdvanced")), 2)

    def test_the_refine_pass_keeps_its_own_schedule(self):
        workflow = _render(HQ)
        split = _of(workflow, "SplitSigmas")[0]["inputs"]
        self.assertEqual(split["step"], 5)  # the last three of eight: 0.909375 .. 0
        protected = [n for n in workflow.values() if (n.get("_meta") or {}).get("preserve_sampling")]
        self.assertEqual({n["class_type"] for n in protected}, {"KSamplerSelect", "CFGGuider"})


class ControlTests(unittest.TestCase):
    def test_control_is_ltx25_with_the_union_ic_lora(self):
        for name in CONTROL:
            workflow = _render(name, control_video="drive.mp4")
            loras = [n["inputs"]["lora_name"] for n in _of(workflow, "LTXICLoRALoaderModelOnly")]
            self.assertEqual(loras, [UNION], name)
            self.assertTrue(_of(workflow, "LTXAddVideoICLoRAGuide"), name)
            self.assertEqual({n["inputs"]["sampler_name"] for n in _of(workflow, "KSamplerSelect")},
                             {"euler_ancestral"}, name)


class FamilyTests(unittest.TestCase):
    LTX25 = {"kind": "checkpoint", "family": "ltx25", "base": "LTX-2.5"}
    LTX23_LORA = {"kind": "lora", "family": "ltx23", "base": "LTXV 2.3"}
    LTX23_CKPT = {"kind": "checkpoint", "family": "ltx23", "base": "LTXV 2.3"}
    LTX25_LORA = {"kind": "lora", "family": "ltx25", "base": "LTX-2.5"}

    def test_23_loras_load_onto_25(self):
        self.assertEqual(ai_model_defaults.model_family(self.LTX25), "ltx25")
        self.assertTrue(ai_model_defaults.compatible(self.LTX25, self.LTX23_LORA))

    def test_25_loras_are_not_promised_on_23(self):
        self.assertFalse(ai_model_defaults.compatible(self.LTX23_CKPT, self.LTX25_LORA))


class QualityTests(unittest.TestCase):
    def test_hq_picks_the_two_stage_graph_for_ltx25(self):
        import ai_vision_api
        self.assertEqual(ai_vision_api._video_quality_workflow("hq", "ltx25", STANDARD), HQ)
        self.assertEqual(ai_vision_api._video_quality_workflow("standard", "ltx25", STANDARD), "")
        self.assertEqual(ai_vision_api._video_quality_workflow("", "ltx25", STANDARD), "")
        self.assertTrue((WORKFLOWS_DIR / ai_vision_api.LTX25_HQ_WORKFLOW).is_file())

    def test_control_renders_on_the_ltx25_transformer(self):
        import ai_vision_api
        self.assertEqual(ai_vision_api.VIDEO_CONTROL_CHECKPOINT, DIT)


REPO_CATALOGUE = BACKEND.parent / "deploy" / "ai-models" / "model_catalogue.json"
RETIRED_CHECKPOINTS = (
    "ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors",
    "ltx10eros_v14_2989669.safetensors",
    "ltx-2-19b-distilled-fp8.safetensors",
    "ltxv-13b-0.9.8-distilled-fp8.safetensors",
)
LEGACY_TOKENS = {
    "gen_animation_by_url.json": STANDARD,
    "gen_animation_ltx23_by_url.json": STANDARD,
    "gen_animation_ltx10eros_by_url.json": STANDARD,
    "gen_animation_hq_by_url.json": HQ,
}


def _catalogue():
    return json.loads(REPO_CATALOGUE.read_text(encoding="utf-8"))


class SwitchoverTests(unittest.TestCase):
    """The historical tokens now run LTX-2.5; saved names still resolve."""

    def test_legacy_tokens_carry_the_ltx25_graphs(self):
        for legacy, current in LEGACY_TOKENS.items():
            self.assertEqual((WORKFLOWS_DIR / legacy).read_text(encoding="utf-8"),
                             (WORKFLOWS_DIR / current).read_text(encoding="utf-8"), legacy)

    def test_retired_checkpoints_render_on_ltx25(self):
        for name in RETIRED_CHECKPOINTS:
            self.assertEqual(ai_model_defaults.canonical_file(name), DIT, name)
        # renderfin must not treat a retired file as the same bytes.
        for name in RETIRED_CHECKPOINTS:
            self.assertNotIn(name, ai_model_defaults.MODEL_FILE_ALIASES)

    def test_ltx25_is_the_only_default_video_checkpoint(self):
        defaults = [e["file"] for e in _catalogue() if "video" in (e.get("default_for_services") or [])]
        self.assertEqual(defaults, [DIT])
        files = {e.get("file") for e in _catalogue() if e.get("kind") == "checkpoint"}
        for name in RETIRED_CHECKPOINTS:
            self.assertNotIn(name, files)

    def test_every_video_checkpoint_pins_a_shipped_workflow_and_its_schedule(self):
        for entry in _catalogue():
            if entry.get("kind") != "checkpoint" or "video" not in (entry.get("services") or []):
                continue
            workflow = _render(entry["workflow"])
            self.assertTrue(workflow, entry["file"])
            policy = entry.get("sampling_policy") or {}
            sigmas = [n["inputs"]["sigmas"] for n in _of(workflow, "ManualSigmas")]
            if sigmas:
                self.assertEqual(policy.get("fixed_steps"), len(sigmas[0].split(",")) - 1, entry["file"])

    def test_ltx23_loras_resolve_onto_ltx25(self):
        entries = _catalogue()
        loras = [e for e in entries if e.get("kind") == "lora" and e.get("usable")
                 and ai_model_defaults.model_family(e) == "ltx23"]
        for lora in loras:
            base = ai_model_defaults.family_default_checkpoint(entries, lora, "video")
            self.assertIsNotNone(base, lora["file"])
            self.assertEqual(base["file"], DIT)


if __name__ == "__main__":
    unittest.main()
