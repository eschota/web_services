"""MiniMax H3: the premium video engine (worker-4090 tier).

Pinned: the template parses after renderfin's substitution, keeps the turbo
distill LoRA under a user LoRA stack, anchors the first frame at the padded
delivery size, prunes the optional end frame, and samples four steps.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from renderfin.config import WORKFLOWS_DIR  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402
from renderfin.templating import apply_lora_stack, render_workflow_text  # noqa: E402

TEMPLATE = "gen_video_minimax_h3_by_url.json"
TURBO = "minimax_h3_fl2v_turbo_4step_v1.2_768p_comfyui_bf16.safetensors"


def _render(image_end=""):
    text = (WORKFLOWS_DIR / TEMPLATE).read_text(encoding="utf-8")
    return render_workflow_text(text, width=960, height=540, prompt="a woman smiles",
                                negative_prompt="", image_filename="in.png",
                                image_end_filename=image_end, output_prefix="task-1",
                                frames=97, randomize_seeds=False)


def _of(workflow, class_type):
    return [node for node in workflow.values() if node.get("class_type") == class_type]


class MiniMaxH3TemplateTests(unittest.TestCase):
    def test_loaders_and_turbo_schedule(self):
        workflow = _render()
        self.assertEqual([n["inputs"]["unet_name"] for n in _of(workflow, "UNETLoader")],
                         ["minimax_h3_fl2va_int8_convrot.safetensors"])
        self.assertEqual([(n["inputs"]["type"]) for n in _of(workflow, "CLIPLoader")], ["minimax"])
        self.assertEqual([n["inputs"]["lora_name"] for n in _of(workflow, "LoraLoaderModelOnly")], [TURBO])
        self.assertEqual(_of(workflow, "BasicScheduler")[0]["inputs"]["steps"], 4)
        self.assertEqual(_of(workflow, "MiniMaxH3ImageToVideo")[0]["inputs"]["length"], 97)
        self.assertEqual(_of(workflow, "CreateVideo")[0]["inputs"]["fps"], 24)

    def test_end_frame_is_optional(self):
        self.assertEqual(len(_of(_render(), "LoadImage")), 1)
        self.assertFalse(_of(_render(), "MiniMaxH3AddGuide"))
        with_end = _render(image_end="last.png")
        guide = _of(with_end, "MiniMaxH3AddGuide")[0]["inputs"]
        self.assertEqual(guide["frame_idx"], -1)

    def test_first_frame_is_scaled_to_a_latent_aligned_size(self):
        prompt = SimpleNamespace(type="", lora="", lora_strength=None, frame_count=97,
                                 control_strength=0.8, steps=0, creativity=0, cfg=None,
                                 sampler="", scheduler="", clip_skip=None)
        workflow = apply_runtime_settings(_render(), prompt, 960, 540)
        scale = next(n for n in _of(workflow, "ImageScale") if n["inputs"]["image"] == ["start_frame", 0])
        self.assertEqual((scale["inputs"]["width"], scale["inputs"]["height"]), (960, 544))

    def test_a_lora_stack_keeps_the_turbo_lora(self):
        workflow = _render()
        apply_lora_stack(workflow, [{"name": "style.safetensors", "strength_model": 0.8}])
        names = [n["inputs"]["lora_name"] for n in _of(workflow, "LoraLoaderModelOnly")]
        self.assertEqual(sorted(names), sorted([TURBO, "style.safetensors"]))


if __name__ == "__main__":
    unittest.main()
