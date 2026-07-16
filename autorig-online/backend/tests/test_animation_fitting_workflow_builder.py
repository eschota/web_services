import json
import unittest
from pathlib import Path

import httpx

from animation_fitting.comfy import workflow_fingerprint
from animation_fitting.workflow_builder import (
    BASE_CHECKPOINT,
    STATIC_CAMERA_LORA,
    convert_active_ui_graph_to_api,
    install_api_workflow,
)


WORKFLOW_ROOT = (
    Path(__file__).resolve().parents[1]
    / "animation_fitting"
    / "specs"
    / "workflows"
)


class AnimationFittingWorkflowBuilderTests(unittest.TestCase):
    def test_ui_importer_inlines_primitives_and_resolves_bypassed_nodes(self):
        ui_workflow = {
            "nodes": [
                {
                    "id": 1,
                    "type": "FakeSource",
                    "mode": 0,
                    "inputs": [],
                    "outputs": [{"name": "MODEL", "type": "MODEL", "links": [1]}],
                    "widgets_values": [],
                },
                {
                    "id": 2,
                    "type": "BypassedModel",
                    "mode": 4,
                    "inputs": [{"name": "model", "type": "MODEL", "link": 1}],
                    "outputs": [{"name": "MODEL", "type": "MODEL", "links": [2]}],
                    "widgets_values": [],
                },
                {
                    "id": 3,
                    "type": "PrimitiveInt",
                    "mode": 0,
                    "inputs": [],
                    "outputs": [{"name": "INT", "type": "INT", "links": [3]}],
                    "widgets_values": [49, "fixed"],
                },
                {
                    "id": 4,
                    "type": "FakeOutput",
                    "mode": 0,
                    "inputs": [
                        {"name": "model", "type": "MODEL", "link": 2},
                        {"name": "frames", "type": "INT", "link": 3, "widget": {"name": "frames"}},
                    ],
                    "outputs": [],
                    "widgets_values": [1],
                },
                {
                    "id": 5,
                    "type": "MissingDormantNode",
                    "mode": 0,
                    "inputs": [],
                    "outputs": [],
                    "widgets_values": [],
                },
            ],
            "links": [
                [1, 1, 0, 2, 0, "MODEL"],
                [2, 2, 0, 4, 0, "MODEL"],
                [3, 3, 0, 4, 1, "INT"],
            ],
        }
        object_info = {
            "FakeSource": {
                "input": {"required": {}},
                "input_order": {"required": []},
                "output": ["MODEL"],
                "output_node": False,
            },
            "FakeOutput": {
                "input": {
                    "required": {
                        "model": ["MODEL", {}],
                        "frames": ["INT", {"default": 1}],
                    }
                },
                "input_order": {"required": ["model", "frames"]},
                "output": [],
                "output_node": True,
            },
        }

        prompt, omitted, remainders = convert_active_ui_graph_to_api(ui_workflow, object_info)

        self.assertEqual(set(prompt), {"1", "4"})
        self.assertEqual(prompt["4"]["inputs"]["model"], ["1", 0])
        self.assertEqual(prompt["4"]["inputs"]["frames"], 49)
        self.assertEqual(omitted, ("2", "3", "5"))
        self.assertEqual(remainders, {})

    def test_versioned_templates_pin_19b_static_lora_and_distinct_guide_modes(self):
        loop = json.loads(
            (WORKFLOW_ROOT / "autorig_animal_loop_ltx2_19b_v1_api.json").read_text(encoding="utf-8")
        )
        one_shot = json.loads(
            (WORKFLOW_ROOT / "autorig_animal_oneshot_ltx2_19b_v1_api.json").read_text(encoding="utf-8")
        )
        manifest = json.loads(
            (WORKFLOW_ROOT / "workflow_manifest.v1.json").read_text(encoding="utf-8")
        )
        rows = {row["generation_mode_string"]: row for row in manifest["workflows_array"]}

        for mode, prompt in (("loop", loop), ("one_shot", one_shot)):
            class_types = [node["class_type"] for node in prompt.values()]
            self.assertNotIn("ClownSampler_Beta", class_types)
            checkpoint = next(node for node in prompt.values() if node["class_type"] == "CheckpointLoaderSimple")
            lora = next(node for node in prompt.values() if node["class_type"] == "LoraLoaderModelOnly")
            save = next(node for node in prompt.values() if node["class_type"] == "SaveVideo")
            self.assertEqual(checkpoint["inputs"]["ckpt_name"], BASE_CHECKPOINT)
            self.assertEqual(lora["inputs"]["lora_name"], STATIC_CAMERA_LORA)
            self.assertEqual(lora["inputs"]["strength_model"], 1.0)
            self.assertEqual((save["inputs"]["format"], save["inputs"]["codec"]), ("mp4", "h264"))
            latent = next(
                node for node in prompt.values()
                if node["class_type"] == "EmptyLTXVLatentVideo"
            )
            self.assertEqual(
                (latent["inputs"]["width"], latent["inputs"]["height"]),
                (384, 224),
            )
            self.assertEqual(
                latent["inputs"]["length"], 97 if mode == "loop" else 65
            )
            self.assertEqual(
                workflow_fingerprint(prompt),
                rows[mode]["workflow_fingerprint_sha256_string"],
            )
            guides = [node for node in prompt.values() if node["class_type"] == "LTXVAddGuide"]
            crop = next(node for node in prompt.values() if node["class_type"] == "LTXVCropGuides")
            separate_id = next(
                node_id
                for node_id, node in prompt.items()
                if node["class_type"] == "LTXVSeparateAVLatent"
            )
            decode = next(
                node for node in prompt.values() if node["class_type"] == "LTXVTiledVAEDecode"
            )
            audio_decode = next(
                node for node in prompt.values() if node["class_type"] == "LTXVAudioVAEDecode"
            )
            crop_id = next(
                node_id for node_id, node in prompt.items() if node["class_type"] == "LTXVCropGuides"
            )
            final_guide_id = next(
                node_id
                for node_id, node in prompt.items()
                if node is guides[-1]
            )
            self.assertEqual(crop["inputs"]["positive"], [final_guide_id, 0])
            self.assertEqual(crop["inputs"]["negative"], [final_guide_id, 1])
            self.assertEqual(crop["inputs"]["latent"], [separate_id, 0])
            self.assertEqual(decode["inputs"]["latents"], [crop_id, 2])
            self.assertEqual(audio_decode["inputs"]["samples"], [separate_id, 1])

        loop_guides = [node for node in loop.values() if node["class_type"] == "LTXVAddGuide"]
        one_shot_guides = [
            node for node in one_shot.values() if node["class_type"] == "LTXVAddGuide"
        ]
        self.assertEqual({node["inputs"]["frame_idx"] for node in loop_guides}, {0, -1})
        self.assertEqual(len({tuple(node["inputs"]["image"]) for node in loop_guides}), 1)
        self.assertEqual([node["inputs"]["frame_idx"] for node in one_shot_guides], [0])
        one_shot_texts = [
            node["inputs"]["text"]
            for node in one_shot.values()
            if node["class_type"] == "CLIPTextEncode"
        ]
        self.assertTrue(any("one complete non-looping action" in text for text in one_shot_texts))
        self.assertTrue(any("return to start" in text for text in one_shot_texts))

    def test_installer_verifies_server_round_trip_fingerprint(self):
        prompt = {"1": {"class_type": "Test", "inputs": {"value": 1}}}
        calls = []

        def handler(request):
            calls.append((request.method, request.url.path))
            if len(calls) == 1:
                return httpx.Response(404)
            if request.method == "POST":
                return httpx.Response(200, json={})
            return httpx.Response(200, json=prompt)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        try:
            installed = install_api_workflow(
                "http://127.0.0.1:8188",
                "test.json",
                prompt,
                client=client,
            )
        finally:
            client.close()

        self.assertEqual(installed, workflow_fingerprint(prompt))
        self.assertEqual([method for method, _ in calls], ["GET", "POST", "GET"])


if __name__ == "__main__":
    unittest.main()
