"""Several pictures into one edit: validation, routing, templates, sockets.

The order of the pictures is the contract with the prompt ("the person from
image 1 wearing the jacket from image 2"), so most of these tests are about
order surviving every hop: the node's sockets, the request list, renderfin's
payload and the ComfyUI graph that is finally built.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# queue.py imports modules only the production release carries; the helpers
# under test never call them.
if "renderfin.image_quality" not in sys.modules:
    _quality = types.ModuleType("renderfin.image_quality")
    _quality.RenderArtifactQualityError = RuntimeError
    sys.modules["renderfin.image_quality"] = _quality
if "renderfin.workload_lease" not in sys.modules:
    sys.modules["renderfin.workload_lease"] = types.ModuleType("renderfin.workload_lease")

from fastapi import HTTPException  # noqa: E402

import ai_graph  # noqa: E402
import ai_model_catalogue  # noqa: E402
import ai_multiref  # noqa: E402
import ai_qwen_image_api  # noqa: E402
import ai_services  # noqa: E402
import ai_vision_api  # noqa: E402
from renderfin import multiref, routing, templating  # noqa: E402
from renderfin.models import RenderPrompt, RenderServer  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402

WORKFLOWS = BACKEND / "renderfin" / "assets" / "workflows"
KLEIN = "flux-2-klein-4b.safetensors"
KREA = "krea2_turbo_fp8_scaled.safetensors"
QWEN_EDIT = "qwen-image-edit-2511-Q3_K_S.gguf"

CATALOGUE = [
    {"kind": "checkpoint", "family": "krea2", "base": "Krea 2", "file": KREA,
     "services": ["image"], "usable": True, "default_for_services": ["image"],
     "workflow": "gen_image_krea2.json",
     "recommended": {"steps": 8, "cfg": 1.0, "sampler": "euler", "scheduler": "simple"}},
    {"kind": "checkpoint", "family": "flux2", "base": "FLUX.2 klein 4B", "file": KLEIN,
     "services": ["image"], "usable": True, "default_for_services": [],
     "default_for_families": ["flux2"], "workflow": "gen_image_flux2_klein.json",
     "recommended": {"steps": 4, "cfg": 1.0, "sampler": "euler"}},
    {"kind": "checkpoint", "family": "qwen_image", "base": "Qwen-Image-Edit 2511",
     "file": QWEN_EDIT, "services": ["qwen_image"], "usable": True,
     "qwen_image_modes": ["edit"], "workflow": "qwen_image_edit.json"},
]


class CatalogueMixin:
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        path = Path(self._dir.name) / "model_catalogue.json"
        path.write_text(json.dumps(CATALOGUE), encoding="utf-8")
        self._saved = (ai_model_catalogue.CATALOGUE_FILE,
                       ai_model_catalogue._cache, ai_model_catalogue._cache_at)
        ai_model_catalogue.CATALOGUE_FILE = path
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0
        self._lora = mock.patch("ai_lora_manager.catalogue_entries", return_value=[])
        self._lora.start()

    def tearDown(self):
        self._lora.stop()
        (ai_model_catalogue.CATALOGUE_FILE,
         ai_model_catalogue._cache, ai_model_catalogue._cache_at) = self._saved
        self._dir.cleanup()


class _Response:
    def __init__(self, payload):
        self.status_code = 202
        self._payload = payload

    def json(self):
        return self._payload


def _capture_renderfin():
    """Patch the renderfin submit and hand back the payloads it was sent."""
    sent = []

    async def post(self, url, json=None, timeout=None):  # noqa: A002
        sent.append(json)
        return _Response({"task_id": "t1", "output_url": "https://autorig.online/out/t1.png"})

    return sent, mock.patch("httpx.AsyncClient.post", post)


async def _identity(url, client=None):
    return url


# ---------------------------------------------------------------- renderfin

class KleinTemplateTests(unittest.TestCase):
    def _workflow(self):
        return templating.render_workflow_text(
            (WORKFLOWS / multiref.WORKFLOW_KLEIN).read_text(encoding="utf-8"),
            width=1024, height=1024, prompt="the person from image 1 in the jacket from image 2",
            negative_prompt="", image_filename="", output_prefix="mr", randomize_seeds=False)

    def test_references_chain_in_the_order_given(self):
        workflow = self._workflow()
        multiref.inject_references(multiref.WORKFLOW_KLEIN, workflow, ["person.png", "jacket.png", "scene.png"])
        self.assertEqual(workflow["multiref_1_image"]["inputs"]["image"], "person.png")
        self.assertEqual(workflow["multiref_3_image"]["inputs"]["image"], "scene.png")
        self.assertEqual(workflow["multiref_1_conditioning"]["inputs"]["conditioning"], ["positive", 0])
        self.assertEqual(workflow["multiref_2_conditioning"]["inputs"]["conditioning"],
                         ["multiref_1_conditioning", 0])
        self.assertEqual(workflow["guider"]["inputs"]["conditioning"], ["multiref_3_conditioning", 0])
        self.assertEqual(sum(1 for n in workflow.values() if n["class_type"] == "LoadImage"), 3)
        # Three references share a smaller pixel budget than two.
        self.assertEqual(workflow["multiref_1_scale"]["inputs"]["megapixels"], 0.75)
        json.dumps(workflow)

    def test_klein_takes_one_to_four(self):
        for names in ([], ["a", "b", "c", "d", "e"]):
            with self.assertRaises(ValueError):
                multiref.inject_references(multiref.WORKFLOW_KLEIN, self._workflow(), names)
        workflow = self._workflow()
        multiref.inject_references(multiref.WORKFLOW_KLEIN, workflow, ["a", "b", "c", "d"])
        self.assertIn("multiref_4_conditioning", workflow)

    def test_runtime_settings_and_lora_stack_still_apply(self):
        workflow = templating.render_workflow_text(
            (WORKFLOWS / multiref.WORKFLOW_KLEIN).read_text(encoding="utf-8"),
            width=900, height=1200, prompt="p", negative_prompt="", image_filename="",
            output_prefix="mr", randomize_seeds=False,
            loras=[{"name": "klein_style.safetensors", "strength_model": 0.7, "strength_clip": 0.7}])
        multiref.inject_references(multiref.WORKFLOW_KLEIN, workflow, ["a", "b"])
        prompt = RenderPrompt(prompt="p", type=multiref.TYPE_KLEIN, steps=4,
                              reference_image_urls=["https://e/a.png", "https://e/b.png"])
        apply_runtime_settings(workflow, prompt, 900, 1200)
        self.assertEqual(workflow["latent"]["inputs"]["width"], 928)
        self.assertEqual(workflow["sigmas"]["inputs"]["steps"], 4)
        self.assertTrue(any(n["class_type"].startswith("Lora") and
                            n["inputs"].get("lora_name") == "klein_style.safetensors"
                            for n in workflow.values()))
        delivery = [n for k, n in workflow.items() if k.startswith("delivery_size_")]
        self.assertEqual((delivery[0]["inputs"]["width"], delivery[0]["inputs"]["height"]), (900, 1200))


class QwenTemplateTests(unittest.TestCase):
    def _workflow(self):
        return templating.render_workflow_text(
            (WORKFLOWS / multiref.WORKFLOW_QWEN).read_text(encoding="utf-8"),
            width=1024, height=1024, prompt="put the bottle from image 1 in the hand of image 2",
            negative_prompt="blurry", image_filename="", output_prefix="mr", randomize_seeds=False)

    def test_pictures_fill_image1_to_image3_of_both_encoders(self):
        workflow = self._workflow()
        multiref.inject_references(multiref.WORKFLOW_QWEN, workflow, ["bottle.png", "model.png"])
        for encoder in ("positive", "negative"):
            inputs = workflow[encoder]["inputs"]
            self.assertEqual(inputs["image1"], ["multiref_1_scale", 0])
            self.assertEqual(inputs["image2"], ["multiref_2_scale", 0])
            self.assertNotIn("image3", inputs)
        self.assertEqual(workflow["positive_method"]["inputs"]["reference_latents_method"],
                         "index_timestep_zero")
        self.assertEqual(workflow["sample"]["inputs"]["positive"], ["positive_method", 0])
        self.assertEqual(workflow["latent"]["class_type"], "EmptySD3LatentImage")

    def test_qwen_takes_at_most_three(self):
        with self.assertRaises(ValueError):
            multiref.inject_references(multiref.WORKFLOW_QWEN, self._workflow(), ["a", "b", "c", "d"])


class RoutingTests(unittest.TestCase):
    def test_types_pick_their_templates(self):
        klein = RenderPrompt(prompt="p", type=multiref.TYPE_KLEIN)
        qwen = RenderPrompt(prompt="p", type=multiref.TYPE_QWEN)
        self.assertEqual(routing.resolve_workflow_file(klein), (multiref.WORKFLOW_KLEIN, None))
        self.assertEqual(routing.resolve_workflow_file(qwen), (multiref.WORKFLOW_QWEN, None))
        self.assertEqual(routing.output_extension(klein), ".png")

    def test_scheduled_on_the_image_boxes_not_the_4090(self):
        token = routing.scheduling_token(RenderPrompt(prompt="p", type=multiref.TYPE_KLEIN))
        self.assertEqual(token, routing.ENHANCE_SCHEDULING_TOKEN)
        f5 = RenderServer(render_server_name="f5", render_server_url="http://f5",
                          available_workflows=["gen_image.json", "gen_image_control_canny.json"])
        rtx = RenderServer(render_server_name="worker-4090", render_server_url="http://w",
                           available_workflows=["gen_image_flux2_klein_edit.json"])
        self.assertTrue(routing.server_can_run(f5, token))
        self.assertFalse(routing.server_can_run(rtx, token))

    def test_multiref_is_not_an_avatar_and_not_a_plain_image(self):
        self.assertTrue(multiref.is_multiref_workflow(multiref.WORKFLOW_KLEIN))
        self.assertFalse(multiref.is_multiref_workflow("gen_image_flux2_avatar.json"))
        self.assertFalse(multiref.is_multiref_workflow("gen_image_flux2_klein_edit.json"))


# --------------------------------------------------------------- /api/image

class ImageApiTests(CatalogueMixin, unittest.TestCase):
    def _run(self, **fields):
        sent, patch = _capture_renderfin()
        with patch, mock.patch.object(ai_multiref, "as_picture", _identity):
            answer = asyncio.run(ai_vision_api._uncached_api_image(
                ai_vision_api.ImageRequest(**fields)))
        return answer, sent[0]

    def test_extra_pictures_become_an_ordered_list_on_klein(self):
        answer, payload = self._run(
            prompt="the woman from image 1 wearing the coat from image 2",
            image_url="https://e/person.png",
            reference_image_urls=["https://e/coat.png", "https://e/street.png"])
        self.assertEqual(payload["type"], "image_multiref")
        self.assertEqual(payload["reference_image_urls"],
                         ["https://e/person.png", "https://e/coat.png", "https://e/street.png"])
        self.assertNotIn("image_url", payload)
        # No checkpoint named: the model that composes pictures is chosen,
        # not the service default (Krea 2), which cannot.
        self.assertEqual(payload["checkpoint"], KLEIN)
        self.assertEqual(answer["effective_params_object"]["work_flow"],
                         "gen_image_flux2_klein_multiref.json")

    def test_a_model_that_cannot_take_pictures_is_a_400_that_says_so(self):
        with self.assertRaises(HTTPException) as caught:
            self._run(prompt="p", image_url="https://e/a.png", checkpoint=KREA,
                      reference_image_urls=["https://e/b.png"])
        self.assertEqual(caught.exception.status_code, 400)
        self.assertEqual(caught.exception.detail["error_string"], "model_takes_no_references")
        self.assertIn("klein", caught.exception.detail["message_string"])

    def test_more_than_four_pictures_is_refused(self):
        with self.assertRaises(HTTPException) as caught:
            self._run(prompt="p", image_url="https://e/a.png",
                      reference_image_urls=[f"https://e/{i}.png" for i in range(4)])
        self.assertEqual(caught.exception.detail["error_string"], "too_many_reference_images")
        self.assertEqual(caught.exception.detail["max_int"], 4)

    def test_references_do_not_mix_with_controlnet(self):
        with self.assertRaises(HTTPException) as caught:
            self._run(prompt="p", image_url="https://e/a.png", control_pose="https://e/pose.png",
                      reference_image_urls=["https://e/b.png"])
        self.assertEqual(caught.exception.detail["error_string"], "multi_reference_exclusive")

    def test_a_single_picture_keeps_the_old_edit_path(self):
        _answer, payload = self._run(prompt="p", image_url="https://e/a.png", checkpoint=KLEIN)
        self.assertEqual(payload["image_url"], "https://e/a.png")
        self.assertEqual(payload["work_flow"], "gen_image_flux2_klein_edit.json")
        self.assertNotIn("reference_image_urls", payload)


class VideoReferenceTests(unittest.TestCase):
    def test_extensions_settle_the_kind_without_a_request(self):
        self.assertTrue(asyncio.run(ai_multiref.is_video("https://autorig.online/r/clip.mp4?x=1")))
        self.assertFalse(asyncio.run(ai_multiref.is_video("https://autorig.online/r/a.png")))
        self.assertFalse(asyncio.run(ai_multiref.is_video("data:image/png;base64,AA==")))

    def test_a_video_is_read_as_its_first_frame(self):
        async def reference(url, view):
            self.assertEqual(view, "first_frame")
            return {"image_url_string": "https://autorig.online/api/ai/video-references/x/y.png"}

        with mock.patch("ai_video_reference.reference_for_video", reference):
            picture = asyncio.run(ai_multiref.as_picture("https://autorig.online/r/clip.mp4"))
            same = asyncio.run(ai_multiref.as_picture("https://autorig.online/r/a.png"))
        self.assertTrue(picture.endswith("/y.png"))
        self.assertEqual(same, "https://autorig.online/r/a.png")


# ----------------------------------------------------------- /api/qwen-image

class QwenApiTests(CatalogueMixin, unittest.TestCase):
    def _run(self, **fields):
        sent = []

        async def run(service, payload, wait):
            sent.append(payload)
            return {"success_bool": True}

        async def size(client, url):
            return (1200, 800)

        with mock.patch.object(ai_qwen_image_api, "_run", run), \
             mock.patch.object(ai_qwen_image_api, "_source_size", size), \
             mock.patch.object(ai_multiref, "as_picture", _identity):
            answer = asyncio.run(ai_qwen_image_api._uncached_qwen_image(
                ai_qwen_image_api.QwenImageRequest(**fields)))
        return answer, sent[0]

    def test_two_pictures_run_the_multi_template_at_image_one_size(self):
        answer, payload = self._run(prompt="the bottle from image 2 in her hand",
                                    image_url="https://e/model.png",
                                    reference_image_urls=["https://e/bottle.png"])
        self.assertEqual(payload["type"], "qwen_image_edit_multi")
        self.assertEqual(payload["reference_image_urls"],
                         ["https://e/model.png", "https://e/bottle.png"])
        self.assertEqual(payload["checkpoint"], QWEN_EDIT)
        self.assertEqual((answer["width_int"], answer["height_int"]), (1200, 800))

    def test_four_pictures_is_one_too_many(self):
        with self.assertRaises(HTTPException) as caught:
            self._run(prompt="p", image_url="https://e/a.png",
                      reference_image_urls=["https://e/b.png", "https://e/c.png", "https://e/d.png"])
        self.assertEqual(caught.exception.detail["max_int"], 3)

    def test_one_picture_is_the_old_single_edit(self):
        _answer, payload = self._run(prompt="p", image_url="https://e/a.png")
        self.assertEqual(payload["type"], "qwen_image_edit")
        self.assertEqual(payload["image_url"], "https://e/a.png")


# ------------------------------------------------------------ node catalogue

class SocketTests(unittest.TestCase):
    def test_reference_sockets_follow_every_older_socket(self):
        image = ai_services.service("image")
        fields = [item["field"] for item in image["inputs"]]
        self.assertEqual(fields[:5], ["prompt", "image", "control_pose", "control_depth", "control_canny"])
        self.assertEqual(fields[5:], ["reference_2", "reference_3", "reference_4"])
        qwen = [item["field"] for item in ai_services.service("qwen_image")["inputs"]]
        self.assertEqual(qwen, ["prompt", "image", "reference_2", "reference_3"])

    def test_a_video_may_be_wired_into_a_reference_socket(self):
        graph = ai_graph.Graph(**{
            "name": "t", "nodes": [
                {"id": "1", "kind": "input", "entity_type": "video", "value": "https://e/c.mp4"},
                {"id": "2", "kind": "input", "entity_type": "text", "value": "p"},
                {"id": "3", "kind": "service", "service": "image"}],
            "links": [{"from": "1", "output": "value", "to": "3", "input": "reference_2"},
                      {"from": "2", "output": "value", "to": "3", "input": "prompt"}]})
        ai_graph.validate(graph)
        bad = ai_graph.Graph(**{
            "name": "t", "nodes": [
                {"id": "1", "kind": "input", "entity_type": "video", "value": "https://e/c.mp4"},
                {"id": "3", "kind": "service", "service": "image"}],
            "links": [{"from": "1", "output": "value", "to": "3", "input": "prompt"}]})
        with self.assertRaises(HTTPException):
            ai_graph.validate(bad)


if __name__ == "__main__":
    unittest.main()
