"""LTX-10Eros v1.4: the farm's only LTX model since 2026-09-29 (owner: it replaces
LTX-2.5 because LTX 2.3 LoRAs load on it and it generates audio).

Pinned here:
  * every LTX token (the four standard names, the two HQ names, pose / depth /
    canny control, CrossView camera orbit) loads the 10Eros checkpoint with
    its own video and audio VAE, the Gemma 3 12B text encoder and the author's
    DMD LoRA, and no LTX-2.5 file,
  * the author's nine-step DMD schedule, Euler ancestral, CFG 1, 24 fps and an
    audio track of the requested length,
  * the optional end frame is pruned when no end image is sent,
  * a user LoRA stack chains in front of the DMD LoRA and never replaces it,
  * the HQ tokens run the same single-stage graph (no 2.3 latent upscaler),
  * every retired LTX name, LTX-2.5 included, renders on 10Eros; the 1.6 GB
    Civitai "Other" file is not treated as the same bytes,
  * LTX 2.3 LoRAs load onto 10Eros and resolve it as their default base.
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
from renderfin.templating import apply_lora_stack, render_workflow_text  # noqa: E402

EROS = "ltx10eros_v14_2989669.safetensors"
GEMMA = "gemma_3_12B_it_fp4_mixed.safetensors"
DMD = "ltx10eros_v14_dmd_lora_2989633.safetensors"
SIGMAS = "1.000, 0.955, 0.893, 0.812, 0.715, 0.603, 0.482, 0.241, 0.121, 0.0"
UNION = "ltx-2.3-22b-ic-lora-union-control-ref0.5.safetensors"
CROSSVIEW = "ltx-2.3-22b-ic-lora-crossview-prompt-v0.9.safetensors"
LTX25 = "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors"
STANDARD = "gen_animation_ltx10eros_by_url.json"
STANDARD_TOKENS = ("gen_animation_ltx10eros_by_url.json", "gen_animation_ltx25_by_url.json",
                   "gen_animation_by_url.json", "gen_animation_ltx23_by_url.json")
HQ_TOKENS = ("gen_animation_ltx25_hq_by_url.json", "gen_animation_hq_by_url.json")
CONTROL = ("gen_video_ltx23_control_by_url.json", "gen_video_ltx23_pose_by_url.json",
           "gen_video_ltx23_depth_by_url.json")
CROSSVIEW_TOKEN = "gen_video_ltx25_crossview_by_url.json"
ALL_LTX = STANDARD_TOKENS + HQ_TOKENS + CONTROL + (CROSSVIEW_TOKEN,)
RETIRED_FILES = ("ltx-2.5-", "gemma4-12b", "ltx-2.3-22b-distilled", "LTX23_", "ltx-2-19b", "ltxv-13b", "t5xxl")


def _render(name, *, frames=97, width=960, height=540, image_end="", control_video=""):
    text = (WORKFLOWS_DIR / name).read_text(encoding="utf-8")
    return render_workflow_text(
        text, width=width, height=height, prompt="a woman turns and smiles",
        negative_prompt="blurry", image_filename="in.png", image_end_filename=image_end,
        control_video_filename=control_video, output_prefix="task-1", frames=frames,
        randomize_seeds=False)


def _runtime(workflow, *, frames=97, width=960, height=540):
    prompt = SimpleNamespace(type="", lora="", lora_strength=None, frame_count=frames,
                             control_strength=0.8, steps=0, creativity=0, cfg=None,
                             sampler="", scheduler="", clip_skip=None)
    return apply_runtime_settings(workflow, prompt, width, height)


def _of(workflow, class_type):
    return [node for node in workflow.values() if node.get("class_type") == class_type]


def _needs_video(name):
    return "control_video" if name in CONTROL or name == CROSSVIEW_TOKEN else ""


class TemplateFilesTests(unittest.TestCase):
    def test_every_ltx_token_loads_only_the_10eros_stack(self):
        for name in ALL_LTX:
            text = (WORKFLOWS_DIR / name).read_text(encoding="utf-8")
            for retired in RETIRED_FILES:
                self.assertNotIn(retired, text, f"{name} still names {retired}")
            workflow = _render(name, control_video="drive.mp4" if _needs_video(name) else "")
            self.assertEqual([n["inputs"]["ckpt_name"] for n in _of(workflow, "CheckpointLoaderSimple")],
                             [EROS], name)
            self.assertFalse(_of(workflow, "UNETLoader"), name)
            self.assertFalse(_of(workflow, "VAELoader"), name)
            self.assertEqual([(n["inputs"]["text_encoder"], n["inputs"]["ckpt_name"])
                              for n in _of(workflow, "LTXAVTextEncoderLoader")], [(GEMMA, EROS)], name)
            self.assertEqual([n["inputs"]["ckpt_name"] for n in _of(workflow, "LTXVAudioVAELoader")],
                             [EROS], name)
            dmd = [n for n in _of(workflow, "LoraLoaderModelOnly") if n["inputs"]["lora_name"] == DMD]
            self.assertEqual(len(dmd), 1, name)
            self.assertEqual(dmd[0]["inputs"]["model"][0],
                             next(k for k, n in workflow.items() if n["class_type"] == "CheckpointLoaderSimple"))

    def test_the_author_schedule_frame_rate_and_audio(self):
        for name in STANDARD_TOKENS + HQ_TOKENS:
            workflow = _render(name)
            self.assertEqual([n["inputs"]["sigmas"] for n in _of(workflow, "ManualSigmas")], [SIGMAS], name)
            self.assertEqual({n["inputs"]["sampler_name"] for n in _of(workflow, "KSamplerSelect")},
                             {"euler_ancestral"}, name)
            self.assertEqual({n["inputs"]["cfg"] for n in _of(workflow, "CFGGuider")}, {1.0}, name)
            self.assertEqual({n["inputs"]["fps"] for n in _of(workflow, "CreateVideo")}, {24}, name)
            self.assertEqual({n["inputs"]["frame_rate"] for n in _of(workflow, "LTXVConditioning")}, {24}, name)
            self.assertEqual({n["inputs"]["frames_number"] for n in _of(workflow, "LTXVEmptyLatentAudio")},
                             {97}, name)
            self.assertTrue(_of(workflow, "LTXVAudioVAEDecode"), name)
            self.assertTrue(all(isinstance(n["inputs"].get("audio"), list) for n in _of(workflow, "CreateVideo")),
                            f"{name} delivers no audio track")

    def test_the_control_and_camera_graphs_use_the_same_schedule(self):
        for name in CONTROL + (CROSSVIEW_TOKEN,):
            workflow = _render(name, control_video="drive.mp4")
            self.assertEqual([n["inputs"]["sigmas"] for n in _of(workflow, "ManualSigmas")], [SIGMAS], name)

    def test_the_end_frame_is_optional(self):
        without = _render(STANDARD)
        self.assertEqual(len(_of(without, "LoadImage")), 1)
        with_end = _render(STANDARD, image_end="last.png")
        self.assertEqual(sorted(n["inputs"]["image"] for n in _of(with_end, "LoadImage")), ["in.png", "last.png"])
        guide = next(n for n in _of(with_end, "LTXVAddGuide") if n["inputs"]["frame_idx"] == -1)
        self.assertEqual(guide["inputs"]["strength"], 0.7)

    def test_the_standard_graph_samples_at_the_delivery_size(self):
        workflow = _runtime(_render(STANDARD), frames=97, width=960, height=540)
        latent = _of(workflow, "EmptyLTXVLatentVideo")[0]["inputs"]
        self.assertEqual((latent["width"], latent["height"], latent["length"]), (960, 544, 97))

    def test_the_historical_tokens_are_the_same_graph(self):
        standard = (WORKFLOWS_DIR / STANDARD).read_text(encoding="utf-8")
        for name in STANDARD_TOKENS + HQ_TOKENS:
            self.assertEqual((WORKFLOWS_DIR / name).read_text(encoding="utf-8"), standard, name)

    def test_a_user_lora_stack_keeps_the_dmd_lora(self):
        workflow = _render(STANDARD)
        apply_lora_stack(workflow, [{"name": "bounce_ltx23.safetensors", "strength_model": 0.8}])
        names = [n["inputs"]["lora_name"] for n in _of(workflow, "LoraLoaderModelOnly")]
        self.assertEqual(sorted(names), sorted([DMD, "bounce_ltx23.safetensors"]))
        guider = _of(workflow, "CFGGuider")[0]["inputs"]["model"][0]
        self.assertEqual(workflow[guider]["inputs"]["lora_name"], DMD)


class ControlTests(unittest.TestCase):
    def test_control_is_10eros_with_the_union_ic_lora(self):
        for name in CONTROL:
            workflow = _render(name, control_video="drive.mp4")
            ic = _of(workflow, "LTXICLoRALoaderModelOnly")
            self.assertEqual([n["inputs"]["lora_name"] for n in ic], [UNION], name)
            self.assertEqual(workflow[ic[0]["inputs"]["model"][0]]["inputs"]["lora_name"], DMD, name)
            self.assertTrue(_of(workflow, "LTXAddVideoICLoRAGuide"), name)

    def test_camera_orbit_is_10eros_with_the_crossview_ic_lora(self):
        workflow = _render(CROSSVIEW_TOKEN, control_video="drive.mp4")
        self.assertEqual([n["inputs"]["lora_name"] for n in _of(workflow, "LTXICLoRALoaderModelOnly")],
                         [CROSSVIEW])

    def test_the_apis_name_the_10eros_checkpoint(self):
        import ai_camera_orbit_api
        import ai_vision_api
        self.assertEqual(ai_vision_api.VIDEO_CONTROL_CHECKPOINT, EROS)
        self.assertEqual(ai_camera_orbit_api.VIDEO_CHECKPOINT, EROS)
        self.assertTrue(any(name in LTX25 for name in ai_vision_api.VIDEO_CONTROL_CHECKPOINTS))


class RetiredNameTests(unittest.TestCase):
    def test_every_retired_ltx_name_renders_on_10eros(self):
        for name in (LTX25, "ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors",
                     "ltx-2.3-22b-distilled-fp8.safetensors", "ltx10eros_v14_2989633.safetensors",
                     "ltx-2-19b-distilled-fp8.safetensors", "ltxv-13b-0.9.8-distilled-fp8.safetensors"):
            self.assertEqual(ai_model_defaults.canonical_file(name), EROS, name)
        self.assertEqual(ai_model_defaults.canonical_file(EROS), EROS)

    def test_no_retired_name_is_a_byte_identical_alias(self):
        for name in (LTX25, "ltx10eros_v14_2989633.safetensors", EROS):
            self.assertNotIn(name, ai_model_defaults.MODEL_FILE_ALIASES)
            self.assertNotIn(name, ai_model_defaults.MODEL_FILE_ALIASES.values())


class FamilyTests(unittest.TestCase):
    EROS_ENTRY = {"kind": "checkpoint", "family": "ltx23", "base": "LTXV 2.3"}
    LTX23_LORA = {"kind": "lora", "family": "ltx23", "base": "LTXV 2.3"}
    H3_LORA = {"kind": "lora", "family": "minimax_h3", "base": "MiniMax H3"}

    def test_ltx23_loras_load_onto_10eros(self):
        self.assertEqual(ai_model_defaults.model_family(self.EROS_ENTRY), "ltx23")
        self.assertTrue(ai_model_defaults.compatible(self.EROS_ENTRY, self.LTX23_LORA))
        self.assertFalse(ai_model_defaults.compatible(self.EROS_ENTRY, self.H3_LORA))

    def test_the_owners_ltx25_loras_keep_a_base(self):
        # BTS Movie Set / BFS head swap were trained on LTX-2.5; every key loads on 10Eros.
        ltx25_lora = {"kind": "lora", "family": "ltx25", "base": "LTXV 2.5"}
        self.assertTrue(ai_model_defaults.compatible(self.EROS_ENTRY, ltx25_lora))


REPO_CATALOGUE = BACKEND.parent / "deploy" / "ai-models" / "model_catalogue.json"


def _catalogue():
    return json.loads(REPO_CATALOGUE.read_text(encoding="utf-8"))


class CatalogueTests(unittest.TestCase):
    def test_10eros_is_the_only_ltx_checkpoint_and_the_ltx_family_default(self):
        entries = _catalogue()
        ltx = [e for e in entries if e.get("kind") == "checkpoint"
               and ai_model_defaults.model_family(e) in ("ltx", "ltx2", "ltx23", "ltx25", "ltx098")]
        self.assertEqual([e["file"] for e in ltx], [EROS])
        self.assertTrue(ltx[0]["usable"])
        self.assertIn("ltx23", ltx[0]["default_for_families"])
        self.assertIn(LTX25, ltx[0]["legacy_files"])
        self.assertEqual(ltx[0]["workflow"], "gen_animation_by_url.json")
        self.assertEqual(ltx[0]["sampling_policy"]["fixed_steps"], len(SIGMAS.split(",")) - 1)

    def test_ltx23_loras_resolve_onto_10eros(self):
        entries = _catalogue()
        base = ai_model_defaults.family_default_checkpoint(entries, self.__class__._lora(), "video")
        self.assertIsNotNone(base)
        self.assertEqual(base["file"], EROS)

    @staticmethod
    def _lora():
        return {"kind": "lora", "family": "ltx23", "base": "LTXV 2.3", "services": ["video"]}

    def test_every_video_checkpoint_pins_a_shipped_workflow_and_its_schedule(self):
        for entry in _catalogue():
            if entry.get("kind") != "checkpoint" or "video" not in (entry.get("services") or []):
                continue
            workflow = _render(entry["workflow"])
            policy = entry.get("sampling_policy") or {}
            sigmas = [n["inputs"]["sigmas"] for n in _of(workflow, "ManualSigmas")]
            if sigmas:
                self.assertEqual(policy.get("fixed_steps"), len(sigmas[0].split(",")) - 1, entry["file"])


class QualityTests(unittest.TestCase):
    def test_hq_is_not_a_separate_tier_for_10eros(self):
        import ai_vision_api
        from fastapi import HTTPException
        token = "gen_animation_by_url.json"  # the catalogue's 10Eros workflow
        self.assertEqual(ai_vision_api._video_quality_workflow("standard", "ltx23", token), "")
        self.assertEqual(ai_vision_api._video_quality_workflow("", "ltx23", token), "")
        with self.assertRaises(HTTPException):
            ai_vision_api._video_quality_workflow("hq", "ltx23", token)


if __name__ == "__main__":
    unittest.main()
