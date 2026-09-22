"""The fast video tier: the checkpoints that run off the single RTX 4090.

Video used to mean one box. The 12 GB and 8 GB image boxes hold two distilled
LTX checkpoints of their own, so the catalogue offers them as their own
families with their own validated workflow each. What is pinned down here is
everything that would silently put the wrong template under a checkpoint, or
deliver a clip of the wrong length:

  * each video checkpoint's declared workflow file actually ships,
  * the sigma schedule in that file agrees with the entry's fixed step count,
  * a quality label can no longer substitute a different template,
  * the LTX 2.3 adapters stay off the new architectures,
  * the delivered frame rate is the product's 24, not the exporter's 25,
  * the finished clip is saved as an output, not a temporary preview.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_model_catalogue  # noqa: E402
import ai_model_defaults  # noqa: E402
import ai_vision_api  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from renderfin.config import WORKFLOWS_DIR  # noqa: E402
from renderfin.templating import VIDEO_FPS, render_workflow_text  # noqa: E402

REPO_CATALOGUE = BACKEND.parent / "deploy" / "ai-models" / "model_catalogue.json"

LTX2_19B = "ltx-2-19b-distilled-fp8.safetensors"
LTXV_13B = "ltxv-13b-0.9.8-distilled-fp8.safetensors"
LTX23_11 = "ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors"


def _catalogue() -> list:
    return json.loads(REPO_CATALOGUE.read_text(encoding="utf-8"))


def _entry(file_name: str) -> dict:
    for entry in _catalogue():
        if entry.get("file") == file_name:
            return entry
    raise AssertionError(f"{file_name} is not in the shipped catalogue")


def _video_checkpoints() -> list:
    return [entry for entry in _catalogue()
            if entry.get("kind") == "checkpoint"
            and "video" in (entry.get("services") or [])
            and entry.get("usable")]


class CataloguePolicyTests(unittest.TestCase):
    def test_both_fast_tier_checkpoints_are_selectable_for_video(self):
        for file_name in (LTX2_19B, LTXV_13B):
            entry = _entry(file_name)
            self.assertTrue(entry.get("usable"), file_name)
            self.assertEqual(entry.get("kind"), "checkpoint")
            self.assertEqual(entry.get("services"), ["video"])
            self.assertTrue(str(entry.get("title") or "").strip())

    def test_each_video_checkpoint_pins_a_workflow_that_ships(self):
        # The hq template was live on the farm for weeks while the repo had no
        # copy of it. A catalogue entry naming a file nobody can deploy is the
        # same outage waiting to happen again.
        for entry in _video_checkpoints():
            workflow = str(entry.get("workflow") or "")
            self.assertTrue(workflow, entry.get("file"))
            self.assertTrue((WORKFLOWS_DIR / workflow).is_file(),
                            f"{entry.get('file')} names a missing {workflow}")

    def test_fast_tier_families_are_their_own_architectures(self):
        self.assertEqual(ai_model_defaults.model_family(_entry(LTX2_19B)), "ltx2")
        self.assertEqual(ai_model_defaults.model_family(_entry(LTXV_13B)), "ltx098")
        self.assertEqual(ai_model_defaults.model_family(_entry(LTX23_11)), "ltx23")

    def test_ltx23_adapters_are_refused_on_the_new_architectures(self):
        loras = [entry for entry in _catalogue()
                 if entry.get("kind") == "lora" and "video" in (entry.get("services") or [])]
        self.assertTrue(loras, "the catalogue lists no video LoRAs to check")
        for checkpoint in (_entry(LTX2_19B), _entry(LTXV_13B)):
            for lora in loras:
                self.assertFalse(
                    ai_model_defaults.compatible(checkpoint, lora),
                    f"{lora.get('file')} must not be offered with {checkpoint.get('file')}")
        # The 2.3 adapters keep working on the 2.3 base they were trained for.
        ltx23_loras = [lora for lora in loras
                       if ai_model_defaults.model_family(lora) == "ltx23"]
        self.assertTrue(ltx23_loras)
        for lora in ltx23_loras:
            self.assertTrue(ai_model_defaults.compatible(_entry(LTX23_11), lora))

    def test_fixed_step_policy_matches_the_workflows_own_sigma_schedule(self):
        # runtime_settings refuses a step count the distilled schedule does not
        # have, so a policy that disagrees with the file is a render that fails
        # on the card instead of in the picker.
        for entry in (_entry(LTX2_19B), _entry(LTXV_13B)):
            text = (WORKFLOWS_DIR / entry["workflow"]).read_text(encoding="utf-8")
            schedules = []
            for node in json.loads(_substituted(text)).values():
                inputs = node.get("inputs") or {}
                if node.get("class_type") == "ManualSigmas":
                    schedules.append(inputs.get("sigmas", ""))
                elif node.get("class_type") == "StringToFloatList":
                    schedules.append(inputs.get("string", ""))
            self.assertEqual(len(schedules), 1, entry["file"])
            steps = len(schedules[0].split(",")) - 1
            policy = entry.get("sampling_policy") or {}
            self.assertEqual(policy.get("fixed_steps"), steps, entry["file"])
            self.assertEqual(policy.get("auto_steps"), steps, entry["file"])
            self.assertEqual((entry.get("recommended") or {}).get("steps"), steps,
                             entry["file"])

    def test_the_default_video_model_is_still_ltx_23(self):
        defaults = [entry for entry in _catalogue()
                    if "video" in (entry.get("default_for_services") or [])]
        self.assertEqual([entry["file"] for entry in defaults], [LTX23_11])


class QualityRoutingTests(unittest.TestCase):
    """A quality label may not swap the template under a chosen checkpoint."""

    def test_a_pinned_checkpoint_keeps_its_own_workflow(self):
        for workflow in ("gen_animation_hq_by_url.json", "gen_animation_by_url.json",
                         "gen_animation_ltx23_by_url.json"):
            self.assertEqual(
                ai_vision_api._video_quality_workflow("", "ltx2", workflow), "")

    def test_standard_under_the_13b_is_that_models_own_standard(self):
        self.assertEqual(
            ai_vision_api._video_quality_workflow(
                "standard", "ltx098", "gen_animation_by_url.json"), "")

    def test_hq_under_the_19b_is_that_models_own_workflow(self):
        self.assertEqual(
            ai_vision_api._video_quality_workflow(
                "hq", "ltx2", "gen_animation_hq_by_url.json"), "")

    def test_a_label_naming_another_template_is_refused_not_applied(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._video_quality_workflow(
                "hq", "ltx098", "gen_animation_by_url.json")
        self.assertEqual(caught.exception.detail["error_string"],
                         "unsupported_video_quality")
        with self.assertRaises(HTTPException):
            ai_vision_api._video_quality_workflow(
                "standard", "ltx2", "gen_animation_hq_by_url.json")

    def test_unselected_model_keeps_the_legacy_quality_routing(self):
        self.assertEqual(ai_vision_api._video_quality_workflow("hq", ""),
                         "gen_animation_hq_by_url.json")
        self.assertEqual(ai_vision_api._video_quality_workflow("standard", ""),
                         "gen_animation_by_url.json")
        self.assertEqual(ai_vision_api._video_quality_workflow("standard", "ltx23"), "")
        with self.assertRaises(HTTPException):
            ai_vision_api._video_quality_workflow("hq", "ltx23")


class ResolvedSettingsTests(unittest.TestCase):
    def test_each_checkpoint_resolves_to_its_own_template(self):
        for file_name, workflow in ((LTX2_19B, "gen_animation_hq_by_url.json"),
                                    (LTXV_13B, "gen_animation_by_url.json"),
                                    (LTX23_11, "gen_animation_ltx23_by_url.json")):
            effective = ai_model_defaults.resolve(_entry(file_name), None, {})
            self.assertEqual(effective.get("work_flow"), workflow, file_name)

    def test_a_step_count_the_schedule_does_not_have_is_refused(self):
        for file_name in (LTX2_19B, LTXV_13B):
            with self.assertRaises(ValueError):
                ai_model_defaults.resolve(_entry(file_name), None, {"steps": 20})

    def test_a_generic_scheduler_is_refused_for_a_distilled_schedule(self):
        with self.assertRaises(ValueError):
            ai_model_defaults.resolve(_entry(LTXV_13B), None, {"scheduler": "karras"})


class FrameRateContractTests(unittest.TestCase):
    """Frame count and frame rate are one contract: length in seconds."""

    def _render(self, frames: int = 97, **kw):
        text = (WORKFLOWS_DIR / "gen_animation_by_url.json").read_text(encoding="utf-8")
        return render_workflow_text(
            text, width=960, height=540, prompt="a cat", negative_prompt="",
            image_filename="in.png", output_prefix="task-1", frames=frames,
            randomize_seeds=False, **kw)

    def test_the_13b_template_delivers_the_products_24_fps(self):
        workflow = self._render()
        rates = [node["inputs"][key]
                 for node in workflow.values()
                 for key in ("frame_rate", "fps")
                 if key in (node.get("inputs") or {})]
        self.assertTrue(rates, "the template states no frame rate at all")
        self.assertEqual(set(rates), {VIDEO_FPS})
        self.assertEqual(VIDEO_FPS, 24)

    def test_the_requested_length_reaches_the_sampler(self):
        workflow = self._render(frames=97)
        sampler = next(node for node in workflow.values()
                       if node.get("class_type") == "LTXVBaseSampler")
        self.assertEqual(sampler["inputs"]["num_frames"], 97)
        # 97 frames at 24 fps is the 4.0 s the caller asked for, not the 3.88 s
        # the exporter's 25 would have delivered.
        self.assertAlmostEqual(97 / VIDEO_FPS, 4.04, places=2)

    def test_an_absent_or_nonsense_rate_falls_back_to_the_product_default(self):
        for value in (0, None, "", 999):
            workflow = self._render(fps=value)
            node = next(node for node in workflow.values()
                        if node.get("class_type") == "VHS_VideoCombine")
            self.assertEqual(node["inputs"]["frame_rate"], VIDEO_FPS, repr(value))

    def test_the_finished_clip_is_an_output_not_a_temporary_preview(self):
        # save_output false put the mp4 in ComfyUI's temp directory, where the
        # artifact resolver refuses to look; every such render failed with
        # real_output_artifact_missing after the GPU had already done the work.
        workflow = self._render()
        combines = [node for node in workflow.values()
                    if node.get("class_type") == "VHS_VideoCombine"]
        self.assertTrue(combines)
        for node in combines:
            self.assertIs(node["inputs"]["save_output"], True)


def _substituted(text: str) -> str:
    """Enough placeholder filling to parse a template that is not valid JSON."""
    for token, value in (("$long_side", "960"), ("$frames", "97"),
                         ("$fps", "24"), ("$width", "960"), ("$height", "544")):
        text = text.replace(token, value)
    return text


class CatalogueServiceTests(unittest.TestCase):
    """The shipped file is what /api/ai/model-catalogue would serve."""

    def setUp(self):
        self._saved = (ai_model_catalogue.CATALOGUE_FILE,
                       ai_model_catalogue._cache,
                       ai_model_catalogue._cache_at)
        ai_model_catalogue.CATALOGUE_FILE = REPO_CATALOGUE
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0

    def tearDown(self):
        (ai_model_catalogue.CATALOGUE_FILE,
         ai_model_catalogue._cache,
         ai_model_catalogue._cache_at) = self._saved

    def test_the_video_picker_offers_four_checkpoints(self):
        offered = [entry["file"] for entry in
                   ai_model_catalogue.for_service("video", "checkpoint")
                   if entry.get("usable")]
        self.assertIn(LTX2_19B, offered)
        self.assertIn(LTXV_13B, offered)
        self.assertIn(LTX23_11, offered)

    def test_a_fast_tier_file_name_is_accepted_by_the_request_validator(self):
        for file_name in (LTX2_19B, LTXV_13B):
            chosen = ai_vision_api._validate_model_choice("video", file_name, None)
            self.assertEqual(chosen["checkpoint"], file_name)

    def test_a_2_3_adapter_on_a_fast_tier_checkpoint_is_rejected(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._validate_model_choice(
                "video", LTXV_13B, "Pixar_Toon.safetensors")
        self.assertEqual(caught.exception.detail["error_string"],
                         "incompatible_model_pair")


if __name__ == "__main__":
    unittest.main()
