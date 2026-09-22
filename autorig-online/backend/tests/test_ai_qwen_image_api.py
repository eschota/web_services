"""The Qwen-Image node: which model a request reaches, at what size, on which box.

The interesting decisions here are all ones a person never states: whether a
request is a generation or an edit, what size an edit comes back at, and which
of the farm's boxes may take the job. Each has a test because each of them
silently produces a picture nobody asked for when it is wrong.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from fastapi import HTTPException  # noqa: E402

import ai_model_catalogue  # noqa: E402
import ai_qwen_image_api  # noqa: E402
import ai_services  # noqa: E402
from renderfin import model_eligibility, routing, templating  # noqa: E402
from renderfin.models import RenderPrompt, RenderServer  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402

WORKFLOWS = BACKEND / "renderfin" / "assets" / "workflows"
REPO_CATALOGUE = BACKEND.parent / "deploy" / "ai-models" / "model_catalogue.json"

GENERATE_GGUF = "qwen-image-2512-Q3_K_S.gguf"
EDIT_GGUF = "qwen-image-edit-2511-Q3_K_S.gguf"


def _with_repo_catalogue(test):
    """Point the catalogue at the file this repo actually deploys.

    The shipped catalogue is the thing that decides what a caller may name, so
    these tests read it rather than a fixture: an entry dropped from it should
    break this suite, not production.
    """
    test._saved = (ai_model_catalogue.CATALOGUE_FILE,
                   ai_model_catalogue._cache, ai_model_catalogue._cache_at)
    ai_model_catalogue.CATALOGUE_FILE = REPO_CATALOGUE
    ai_model_catalogue._cache = []
    ai_model_catalogue._cache_at = 0.0


def _restore_catalogue(test):
    (ai_model_catalogue.CATALOGUE_FILE,
     ai_model_catalogue._cache, ai_model_catalogue._cache_at) = test._saved


class ModeTests(unittest.TestCase):
    def test_auto_reads_the_wiring(self):
        self.assertEqual(ai_qwen_image_api.resolve_mode("auto", False), "generate")
        self.assertEqual(ai_qwen_image_api.resolve_mode("auto", True), "edit")
        self.assertEqual(ai_qwen_image_api.resolve_mode(None, True), "edit")

    def test_generate_ignores_a_picture_that_is_wired_in(self):
        self.assertEqual(ai_qwen_image_api.resolve_mode("generate", True), "generate")

    def test_edit_without_a_picture_is_refused_rather_than_generating(self):
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.resolve_mode("edit", False)
        self.assertEqual(caught.exception.detail["error_string"], "image_required")

    def test_an_unknown_mode_says_what_the_modes_are(self):
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.resolve_mode("inpaint", True)
        self.assertEqual(caught.exception.detail["error_string"], "unknown_qwen_image_mode")


class SizeTests(unittest.TestCase):
    def test_a_requested_side_is_answered_exactly(self):
        # Not snapped to the model's 16 px grid: renderfin pads the latent and
        # scales back to the requested size, so snapping here would only move
        # the answer away from what was asked for.
        for side in (1024, 1000, 540, 833):
            self.assertEqual(ai_qwen_image_api._round_side(side), side)

    def test_a_side_outside_the_range_is_brought_inside_it(self):
        self.assertEqual(ai_qwen_image_api._round_side(4000), ai_qwen_image_api.MAX_SIDE)
        self.assertEqual(ai_qwen_image_api._round_side(10), ai_qwen_image_api.MIN_SIDE)

    def test_an_oversize_source_keeps_its_aspect_ratio(self):
        width, height = ai_qwen_image_api._fit_source(3000, 4500)
        self.assertLessEqual(max(width, height), ai_qwen_image_api.MAX_SIDE)
        self.assertAlmostEqual(width / height, 3000 / 4500, places=2)

    def test_a_source_that_already_fits_is_left_alone(self):
        self.assertEqual(ai_qwen_image_api._fit_source(1024, 768), (1024, 768))
        # Including one on no particular grid: an edit is delivered at the
        # size of the picture it was made from, to the pixel.
        self.assertEqual(ai_qwen_image_api._fit_source(833, 1211), (833, 1211))

    def test_a_tiny_source_is_raised_to_the_floor(self):
        self.assertEqual(ai_qwen_image_api._fit_source(8, 8),
                         (ai_qwen_image_api.MIN_SIDE, ai_qwen_image_api.MIN_SIDE))


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        _with_repo_catalogue(self)

    def tearDown(self):
        _restore_catalogue(self)

    def test_no_checkpoint_means_the_template_keeps_its_own(self):
        self.assertEqual(ai_qwen_image_api.validate_checkpoint(None, "generate"), "")

    def test_the_shipped_quantisations_are_accepted_for_their_own_mode(self):
        self.assertEqual(
            ai_qwen_image_api.validate_checkpoint(GENERATE_GGUF, "generate"), GENERATE_GGUF)
        self.assertEqual(
            ai_qwen_image_api.validate_checkpoint(EDIT_GGUF, "edit"), EDIT_GGUF)

    def test_the_edit_model_is_refused_for_a_generation(self):
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.validate_checkpoint(EDIT_GGUF, "generate")
        self.assertEqual(caught.exception.detail["error_string"], "checkpoint_wrong_mode")

    def test_a_file_outside_the_catalogue_never_reaches_a_worker(self):
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.validate_checkpoint("../../etc/passwd", "generate")
        self.assertEqual(caught.exception.detail["error_string"], "unknown_checkpoint")

    def test_a_flux_checkpoint_is_not_a_qwen_image_checkpoint(self):
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.validate_checkpoint("flux1-schnell.safetensors", "generate")
        self.assertEqual(caught.exception.detail["error_string"], "unknown_checkpoint")

    def test_the_uninstalled_two_point_one_build_explains_itself(self):
        entry = ai_model_catalogue.known_file("qwen-image-2.1-Q4_K_M.gguf", "checkpoint")
        self.assertIsNotNone(entry)
        self.assertFalse(entry.get("usable"))
        self.assertIn("ComfyUI", str(entry.get("unusable_reason")))

    def test_one_installed_model_is_offered_per_mode(self):
        self.assertEqual(ai_qwen_image_api.installed_checkpoints("generate"), [GENERATE_GGUF])
        self.assertEqual(ai_qwen_image_api.installed_checkpoints("edit"), [EDIT_GGUF])

    def test_a_model_the_farm_cannot_run_is_never_offered_as_installed(self):
        # It stays in the catalogue so the picker can grey it out with its
        # reason, which is not the same as being a name a caller may send.
        self.assertNotIn("qwen-image-2.1-Q4_K_M.gguf",
                         ai_qwen_image_api.installed_checkpoints())
        with self.assertRaises(HTTPException) as caught:
            ai_qwen_image_api.validate_checkpoint("qwen-image-2.1-Q4_K_M.gguf", "generate")
        detail = caught.exception.detail
        self.assertEqual(detail["error_string"], "checkpoint_not_usable")
        # Naming a model the farm has on record deserves the reason, not the
        # same "never heard of it" a typo gets.
        self.assertIn("ComfyUI", detail["message_string"])


class PayloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _with_repo_catalogue(self)
        self.sent = []

        async def capture(service, payload, wait_seconds, produces="image"):
            self.sent.append((service, payload, wait_seconds))
            return {"success_bool": True, "task_id_string": "t1",
                    "image_url_string": "https://example.test/out.png"}

        async def resolve(client, image_url, image_base64):
            return str(image_url)

        async def source_size(client, url):
            return (832, 1216)

        self._saved_calls = (ai_qwen_image_api._run,
                             ai_qwen_image_api._resolve_source,
                             ai_qwen_image_api._source_size)
        ai_qwen_image_api._run = capture
        ai_qwen_image_api._resolve_source = resolve
        ai_qwen_image_api._source_size = source_size

    async def asyncTearDown(self):
        (ai_qwen_image_api._run,
         ai_qwen_image_api._resolve_source,
         ai_qwen_image_api._source_size) = self._saved_calls
        _restore_catalogue(self)

    async def _call(self, **kwargs):
        body = ai_qwen_image_api.QwenImageRequest(**kwargs)
        return await ai_qwen_image_api._uncached_qwen_image(body)

    async def test_text_alone_asks_for_the_generation_template(self):
        answer = await self._call(prompt="a lighthouse at dusk")
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["type"], ai_qwen_image_api.TYPE_GENERATE)
        self.assertNotIn("image_url", payload)
        self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                         ai_qwen_image_api.DEFAULT_SIZE)
        self.assertEqual(answer["mode_string"], "generate")

    async def test_a_picture_wired_in_asks_for_the_edit_template(self):
        answer = await self._call(prompt="make it snow",
                                  image_url="https://example.test/in.png")
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["type"], ai_qwen_image_api.TYPE_EDIT)
        self.assertEqual(payload["image_url"], "https://example.test/in.png")
        self.assertEqual(answer["mode_string"], "edit")

    async def test_an_edit_comes_back_at_the_size_it_went_in_at(self):
        await self._call(prompt="make it snow", image_url="https://example.test/in.png")
        _, payload, _ = self.sent[-1]
        self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                         (832, 1216))

    async def test_an_explicit_size_beats_the_source(self):
        await self._call(prompt="make it snow", image_url="https://example.test/in.png",
                         width=1024, height=1024)
        _, payload, _ = self.sent[-1]
        self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                         (1024, 1024))

    async def test_generate_mode_never_sends_the_reference_picture(self):
        await self._call(prompt="a lighthouse", mode="generate",
                         image_url="https://example.test/in.png")
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["type"], ai_qwen_image_api.TYPE_GENERATE)
        self.assertNotIn("image_url", payload)

    async def test_the_model_is_named_even_when_nobody_picked_one(self):
        # Not cosmetic: renderfin only checks a worker's inventory for a file
        # that was named. Left unnamed, this job is eligible on every box that
        # advertises the scheduling token, including the two that have neither
        # ComfyUI-GGUF nor the weights, and the render fails there.
        await self._call(prompt="a lighthouse")
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["checkpoint"], GENERATE_GGUF)
        await self._call(prompt="make it snow", image_url="https://example.test/in.png")
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["checkpoint"], EDIT_GGUF)

    async def test_zeros_are_left_out_so_the_template_keeps_its_own_values(self):
        await self._call(prompt="a lighthouse", steps=0, cfg=0, seed=0)
        _, payload, _ = self.sent[-1]
        for key in ("steps", "cfg", "noise_seed"):
            self.assertNotIn(key, payload)

    async def test_chosen_settings_reach_the_farm(self):
        await self._call(prompt="a lighthouse", steps=24, cfg=3.5, seed=7,
                         negative_prompt="blurry", checkpoint=GENERATE_GGUF)
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["steps"], 24)
        self.assertEqual(payload["cfg"], 3.5)
        self.assertEqual(payload["noise_seed"], 7)
        self.assertEqual(payload["negative_prompt"], "blurry")
        self.assertEqual(payload["checkpoint"], GENERATE_GGUF)


class RoutingTests(unittest.TestCase):
    def test_both_modes_resolve_to_their_own_template(self):
        for ptype, expected in (("qwen_image", "qwen_image_generate.json"),
                                ("qwen_image_edit", "qwen_image_edit.json")):
            prompt = RenderPrompt(type=ptype, prompt="x")
            self.assertEqual(routing.resolve_workflow_file(prompt), (expected, None))

    def test_the_job_is_scheduled_on_the_image_boxes_only(self):
        # The boxes publish this list themselves, so the token has to be one
        # they already advertise. The canny-control token is carried by the
        # four image boxes and by no video box.
        for ptype in ("qwen_image", "qwen_image_edit"):
            token = routing.scheduling_token(RenderPrompt(type=ptype, prompt="x"))
            self.assertEqual(token, routing.QWEN_IMAGE_SCHEDULING_TOKEN)

        image_box = RenderServer(render_server_name="f5", available_workflows=[
            "gen_image.json", "gen_image_control_canny.json",
            "gen_image_control_depth.json", "gen_image_control_pose.json"])
        video_box = RenderServer(render_server_name="worker-4090", available_workflows=[
            "gen_image.json", "gen_image_flux1_schnell.json", "gen_image_sdxl.json",
            "gen_video_ltx23_control_by_url.json"])
        token = routing.QWEN_IMAGE_SCHEDULING_TOKEN
        self.assertTrue(routing.server_can_run(image_box, token))
        self.assertFalse(routing.server_can_run(video_box, token))

    def test_an_edit_is_still_an_image_request_despite_carrying_a_picture(self):
        prompt = RenderPrompt(type="qwen_image_edit", prompt="x",
                              image_url="https://example.test/in.png")
        self.assertTrue(routing.is_image_request(prompt))
        self.assertEqual(routing.output_extension(prompt), ".png")


class TemplateTests(unittest.TestCase):
    def _render(self, name, **kwargs):
        text = (WORKFLOWS / name).read_text(encoding="utf-8")
        defaults = dict(width=1024, height=1024, prompt="a lighthouse",
                        negative_prompt="", image_filename="in.png",
                        output_prefix="task-1")
        defaults.update(kwargs)
        return templating.render_workflow_text(text, **defaults)

    def test_both_templates_parse_and_save_to_the_task_prefix(self):
        for name in ("qwen_image_generate.json", "qwen_image_edit.json"):
            workflow = self._render(name)
            save = [n for n in workflow.values() if n["class_type"] == "SaveImage"]
            self.assertEqual(len(save), 1, name)
            self.assertEqual(save[0]["inputs"]["filename_prefix"], "task-1", name)

    def test_the_generation_template_takes_its_size_from_the_request(self):
        workflow = self._render("qwen_image_generate.json", width=1024, height=768)
        latent = [n for n in workflow.values()
                  if n["class_type"] == "EmptySD3LatentImage"][0]
        self.assertEqual((latent["inputs"]["width"], latent["inputs"]["height"]),
                         (1024, 768))

    def test_the_edit_template_conditions_on_the_uploaded_picture(self):
        workflow = self._render("qwen_image_edit.json", image_filename="abc_in.png")
        loads = [n for n in workflow.values() if n["class_type"] == "LoadImage"]
        self.assertEqual(len(loads), 1)
        self.assertEqual(loads[0]["inputs"]["image"], "abc_in.png")
        encoders = [n for n in workflow.values()
                    if n["class_type"] == "TextEncodeQwenImageEditPlus"]
        # Positive and negative both see the picture: dropping it from the
        # negative branch is what makes an edit drift off the source.
        self.assertEqual(len(encoders), 2)
        for node in encoders:
            self.assertIn("image1", node["inputs"])
            self.assertIn("vae", node["inputs"])

    def test_a_chosen_quantisation_reaches_the_gguf_loader(self):
        for name, chosen in (("qwen_image_generate.json", GENERATE_GGUF),
                             ("qwen_image_edit.json", EDIT_GGUF)):
            workflow = self._render(name, checkpoint=chosen)
            loaders = [n for n in workflow.values()
                       if n["class_type"] == "UnetLoaderGGUF"]
            self.assertEqual(len(loaders), 1, name)
            self.assertEqual(loaders[0]["inputs"]["unet_name"], chosen, name)

    def test_the_seed_is_the_one_that_was_asked_for(self):
        workflow = self._render("qwen_image_generate.json", seed=4242)
        sampler = [n for n in workflow.values() if n["class_type"] == "KSampler"][0]
        self.assertEqual(sampler["inputs"]["seed"], 4242)

    def test_steps_and_cfg_from_the_request_reach_the_sampler(self):
        workflow = self._render("qwen_image_generate.json")
        prompt = RenderPrompt(type="qwen_image", prompt="x", steps=24, cfg=3.5)
        apply_runtime_settings(workflow, prompt, 1024, 1024)
        sampler = [n for n in workflow.values() if n["class_type"] == "KSampler"][0]
        self.assertEqual(sampler["inputs"]["steps"], 24)
        self.assertEqual(sampler["inputs"]["cfg"], 3.5)

    def test_the_delivered_picture_is_resized_to_the_requested_size(self):
        workflow = self._render("qwen_image_edit.json")
        prompt = RenderPrompt(type="qwen_image_edit", prompt="x")
        apply_runtime_settings(workflow, prompt, 832, 1216)
        save = [n for n in workflow.values() if n["class_type"] == "SaveImage"][0]
        feeder = workflow[save["inputs"]["images"][0]]
        self.assertEqual(feeder["class_type"], "ImageScale")
        self.assertEqual((feeder["inputs"]["width"], feeder["inputs"]["height"]),
                         (832, 1216))


class EligibilityTests(unittest.IsolatedAsyncioTestCase):
    """A .gguf is listed by ComfyUI-GGUF's loader and by nothing else."""

    class _Response:
        def __init__(self, payload, status=200):
            self._payload = payload
            self.status_code = status

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code != 200:
                import httpx
                raise httpx.HTTPStatusError("nope", request=None, response=None)

    class _Client:
        def __init__(self, inventory):
            self.inventory = inventory
            self.asked = []

        async def get(self, url, **kwargs):
            class_name = url.rsplit("/", 1)[-1]
            self.asked.append(class_name)
            names = self.inventory.get(class_name)
            if names is None:
                return EligibilityTests._Response({}, status=404)
            slot = {"UnetLoaderGGUF": "unet_name", "UNETLoader": "unet_name",
                    "CheckpointLoaderSimple": "ckpt_name"}[class_name]
            return EligibilityTests._Response(
                {class_name: {"input": {"required": {slot: [list(names)]}}}})

    def setUp(self):
        model_eligibility._cache.clear()

    def tearDown(self):
        model_eligibility._cache.clear()

    async def test_a_box_holding_the_gguf_is_eligible(self):
        client = self._Client({
            "CheckpointLoaderSimple": [], "UNETLoader": ["flux1-schnell.safetensors"],
            "UnetLoaderGGUF": [GENERATE_GGUF]})
        server = RenderServer(render_server_name="f5",
                              render_server_url="http://127.0.0.1:18488")
        prompt = RenderPrompt(type="qwen_image", prompt="x", checkpoint=GENERATE_GGUF)
        self.assertTrue(await model_eligibility.can_load(client, server, prompt))

    async def test_a_box_without_the_gguf_loader_cannot_take_the_job(self):
        client = self._Client({
            "CheckpointLoaderSimple": [], "UNETLoader": ["flux1-schnell.safetensors"]})
        server = RenderServer(render_server_name="f12",
                              render_server_url="http://127.0.0.1:18288")
        prompt = RenderPrompt(type="qwen_image", prompt="x", checkpoint=GENERATE_GGUF)
        self.assertFalse(await model_eligibility.can_load(client, server, prompt))

    async def test_a_missing_gguf_loader_does_not_fail_an_ordinary_checkpoint(self):
        # The regression this guards: asking every box for a custom node it
        # may not have, and reading the 404 as an unreadable inventory, would
        # have made every FLUX render ineligible everywhere.
        client = self._Client({
            "CheckpointLoaderSimple": [], "UNETLoader": ["flux1-schnell.safetensors"]})
        server = RenderServer(render_server_name="f12",
                              render_server_url="http://127.0.0.1:18288")
        prompt = RenderPrompt(type="", prompt="x", checkpoint="flux1-schnell.safetensors")
        self.assertTrue(await model_eligibility.can_load(client, server, prompt))


class CatalogueContractTests(unittest.TestCase):
    def test_the_service_declares_the_endpoint_that_exists(self):
        entry = ai_services.service("qwen_image")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["api"], "/api/qwen-image")
        self.assertEqual(entry["status"], "live")
        routes = {getattr(route, "path", "") for route in ai_qwen_image_api.router.routes}
        self.assertIn(entry["api"], routes)

    def test_the_node_takes_a_prompt_and_an_optional_picture(self):
        entry = ai_services.service("qwen_image")
        fields = {item["field"]: item for item in entry["inputs"]}
        self.assertTrue(fields["prompt"]["required"])
        self.assertFalse(fields["image"]["required"])
        self.assertEqual(entry["outputs"][0]["field"], "image_url_string")

    def test_every_declared_parameter_is_one_the_request_accepts(self):
        accepted = set(ai_qwen_image_api.QwenImageRequest.model_fields)
        for param in ai_services.params_for("qwen_image"):
            self.assertIn(param["name"], accepted, param["name"])

    def test_the_size_controls_accept_any_whole_pixel(self):
        # The editor's follow-the-input-size feature writes a source picture's
        # exact dimensions into these controls and silently keeps the old
        # value when the control rejects one. A coarser step left a node at
        # 960 wide and 1024 high, which is a size nobody chose.
        sizes = {p["name"]: p for p in ai_services.params_for("qwen_image")}
        for name in ("width", "height"):
            self.assertEqual(sizes[name]["step"], 1, name)
            self.assertEqual(sizes[name]["min"], ai_qwen_image_api.MIN_SIDE, name)
            self.assertEqual(sizes[name]["max"], ai_qwen_image_api.MAX_SIDE, name)

    def test_the_mode_choices_are_the_ones_the_endpoint_knows(self):
        mode = [p for p in ai_services.params_for("qwen_image") if p["name"] == "mode"][0]
        self.assertEqual([option["value"] for option in mode["options"]],
                         list(ai_qwen_image_api.MODES))

    def test_the_shipped_catalogue_keeps_foreign_loras_off_the_model(self):
        import ai_model_defaults
        catalogue = json.loads(REPO_CATALOGUE.read_text(encoding="utf-8"))
        qwen = [e for e in catalogue if e.get("file") == GENERATE_GGUF][0]
        for other in catalogue:
            if other.get("kind") != "lora":
                continue
            self.assertFalse(ai_model_defaults.compatible(qwen, other),
                             f"{other.get('file')} was accepted onto Qwen-Image")


if __name__ == "__main__":
    unittest.main()
