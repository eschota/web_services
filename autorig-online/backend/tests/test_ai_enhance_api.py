"""The three enhancement services: routing, sizing and the shape they answer in."""
import asyncio
import json
import unittest
from pathlib import Path

from fastapi import HTTPException

import ai_enhance_api
import ai_services
from renderfin import routing, templating
from renderfin.models import RenderPrompt, RenderServer


WORKFLOWS = Path(__file__).parents[1] / "renderfin" / "assets" / "workflows"


class FakeClient:
    """Stands in for httpx where only the source picture matters."""

    def __init__(self, size=(832, 1216)):
        self.size = size


async def _fake_source_size(client, url):
    return client.size


class SizeMathTests(unittest.TestCase):
    def test_fit_keeps_the_aspect_ratio_at_the_ceiling(self):
        width, height = ai_enhance_api._fit(832 * 4, 1216 * 4)
        self.assertEqual(height, ai_enhance_api.MAX_SIDE)
        # Clamping each side on its own would have left 3328x4096.
        self.assertAlmostEqual(width / height, 832 / 1216, places=2)

    def test_fit_leaves_a_size_that_already_fits(self):
        self.assertEqual(ai_enhance_api._fit(1664, 2432), (1664, 2432))

    def test_fit_never_goes_below_the_floor(self):
        self.assertEqual(ai_enhance_api._fit(1, 1), (64, 64))


class UpscalePayloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sent = []

        async def capture(service, payload, wait_seconds, produces="image"):
            self.sent.append((service, payload, wait_seconds))
            return {"success_bool": True, "task_id_string": "t1",
                    "image_url_string": "https://example.test/out.png"}

        self._run = ai_enhance_api._run
        self._size = ai_enhance_api._source_size
        self._resolve = ai_enhance_api._resolve_source
        ai_enhance_api._run = capture
        ai_enhance_api._source_size = _fake_source_size

        async def resolve(client, image_url, image_base64):
            return str(image_url)

        ai_enhance_api._resolve_source = resolve

        class _Client:
            size = (832, 1216)

            async def __aenter__(self_inner):
                return self_inner

            async def __aexit__(self_inner, *exc):
                return False

        self._httpx_client = ai_enhance_api.httpx.AsyncClient
        ai_enhance_api.httpx.AsyncClient = _Client

    async def asyncTearDown(self):
        ai_enhance_api._run = self._run
        ai_enhance_api._source_size = self._size
        ai_enhance_api._resolve_source = self._resolve
        ai_enhance_api.httpx.AsyncClient = self._httpx_client

    async def test_fast_upscale_doubles_the_source_size(self):
        await ai_enhance_api._uncached_upscale(ai_enhance_api.UpscaleRequest(
            image_url="https://example.test/in.png", scale=2))
        service, payload, _ = self.sent[-1]
        self.assertEqual(service, "upscale")
        self.assertEqual(payload["type"], "upscale_fast")
        self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                         (1664, 2432))
        self.assertEqual(payload["upscale_model"], "4x_NMKD-Siax_200k.pth")
        self.assertNotIn("creativity", payload)

    async def test_refine_upscale_carries_the_prompt_and_a_low_denoise(self):
        await ai_enhance_api._uncached_upscale(ai_enhance_api.UpscaleRequest(
            image_url="https://example.test/in.png", scale=2, mode="refine",
            prompt="a woman in a yellow raincoat"))
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["type"], "upscale_refine")
        self.assertEqual(payload["prompt"], "a woman in a yellow raincoat")
        self.assertLess(payload["creativity"], 0.5)
        self.assertGreater(payload["creativity"], 0)

    async def test_four_times_stays_inside_the_ceiling_without_squashing(self):
        await ai_enhance_api._uncached_upscale(ai_enhance_api.UpscaleRequest(
            image_url="https://example.test/in.png", scale=4))
        _, payload, _ = self.sent[-1]
        width, height = payload["main_size_width"], payload["main_size_height"]
        self.assertLessEqual(max(width, height), ai_enhance_api.MAX_SIDE)
        self.assertAlmostEqual(width / height, 832 / 1216, places=2)

    async def test_an_uninstalled_upscaler_is_refused_by_name(self):
        with self.assertRaises(HTTPException) as caught:
            await ai_enhance_api._uncached_upscale(ai_enhance_api.UpscaleRequest(
                image_url="https://example.test/in.png", model="RealESRGAN_x4.pth"))
        self.assertEqual(caught.exception.detail["error_string"],
                         "upscale_model_not_installed")

    async def test_an_unknown_mode_is_refused(self):
        with self.assertRaises(HTTPException) as caught:
            await ai_enhance_api._uncached_upscale(ai_enhance_api.UpscaleRequest(
                image_url="https://example.test/in.png", mode="magic"))
        self.assertEqual(caught.exception.detail["error_string"], "unknown_upscale_mode")

    async def test_detail_keeps_the_source_size_and_maps_strength_to_denoise(self):
        await ai_enhance_api._uncached_detail(ai_enhance_api.DetailRequest(
            image_url="https://example.test/in.png", strength=1.0))
        service, payload, _ = self.sent[-1]
        self.assertEqual(service, "detail")
        self.assertEqual(payload["type"], "detail_tiled")
        self.assertEqual((payload["main_size_width"], payload["main_size_height"]),
                         (832, 1216))
        self.assertEqual(payload["creativity"], 0.6)

    async def test_detail_without_tiling_picks_the_single_pass_template(self):
        await ai_enhance_api._uncached_detail(ai_enhance_api.DetailRequest(
            image_url="https://example.test/in.png", tile=False))
        _, payload, _ = self.sent[-1]
        self.assertEqual(payload["type"], "detail_plain")

    async def test_detail_never_sends_a_zero_denoise(self):
        # Zero means "leave the template's own value" downstream, which would
        # silently ignore the slider.
        await ai_enhance_api._uncached_detail(ai_enhance_api.DetailRequest(
            image_url="https://example.test/in.png", strength=0))
        _, payload, _ = self.sent[-1]
        self.assertGreater(payload["creativity"], 0)

    async def test_face_fix_fidelity_is_the_inverse_of_denoise(self):
        await ai_enhance_api._uncached_facefix(ai_enhance_api.FaceFixRequest(
            image_url="https://example.test/in.png", fidelity=1.0))
        gentle = self.sent[-1][1]["creativity"]
        await ai_enhance_api._uncached_facefix(ai_enhance_api.FaceFixRequest(
            image_url="https://example.test/in.png", fidelity=0.05))
        strong = self.sent[-1][1]["creativity"]
        self.assertLess(gentle, strong)
        self.assertGreater(gentle, 0)

    async def test_face_fix_wider_region_uses_the_skin_template(self):
        await ai_enhance_api._uncached_facefix(ai_enhance_api.FaceFixRequest(
            image_url="https://example.test/in.png", hands_eyes=True))
        self.assertEqual(self.sent[-1][1]["type"], "face_fix_skin")


class SourceSizeTests(unittest.IsolatedAsyncioTestCase):
    """The probe must report real pixels, not a control-map ceiling."""

    class _Response:
        def __init__(self, content):
            self.status_code, self.content = 200, content

    class _Client:
        def __init__(self, content):
            self.content = content

        async def get(self, url, timeout=None, follow_redirects=False):
            return SourceSizeTests._Response(self.content)

    @staticmethod
    def _png(width, height):
        from io import BytesIO

        from PIL import Image
        buffer = BytesIO()
        Image.new("RGB", (width, height)).save(buffer, format="PNG")
        return buffer.getvalue()

    async def test_a_tall_picture_keeps_its_real_height(self):
        # An upscale's own 1664x2432 output feeding a face fix used to be read
        # as 1664x2048, and the delivery resize then squashed it.
        client = self._Client(self._png(1664, 2432))
        self.assertEqual(await ai_enhance_api._source_size(client, "https://x/y.png"),
                         (1664, 2432))

    async def test_an_unreadable_source_falls_back_rather_than_failing(self):
        class Broken:
            async def get(self, *args, **kwargs):
                raise RuntimeError("no")

        self.assertEqual(await ai_enhance_api._source_size(Broken(), "https://x/y.png"),
                         ai_enhance_api.DEFAULT_SIZE)


class WorkflowTemplateTests(unittest.TestCase):
    """Every template these services name must exist and render to valid JSON."""

    def test_every_enhance_type_has_a_template_that_renders(self):
        for render_type, file_name in routing.ENHANCE_WORKFLOWS.items():
            path = WORKFLOWS / file_name
            self.assertTrue(path.is_file(), f"{file_name} is missing")
            workflow = templating.render_workflow_text(
                path.read_text(encoding="utf-8"),
                width=1664, height=2432, prompt="a test subject", negative_prompt="",
                image_filename="input.png", output_prefix="task-1",
                workflow_type=render_type, upscale_model="4x_NMKD-Siax_200k.pth")
            self.assertTrue(workflow, file_name)
            classes = {node["class_type"] for node in workflow.values()}
            self.assertIn("LoadImage", classes, file_name)
            self.assertIn("SaveImage", classes, file_name)

    def test_the_upscaler_file_name_is_substituted(self):
        workflow = templating.render_workflow_text(
            (WORKFLOWS / "upscale_fast.json").read_text(encoding="utf-8"),
            width=1664, height=2432, prompt="", negative_prompt="",
            image_filename="input.png", output_prefix="task-1",
            upscale_model="RealESRGAN_x4.pth")
        loader = next(n for n in workflow.values()
                      if n["class_type"] == "UpscaleModelLoader")
        self.assertEqual(loader["inputs"]["model_name"], "RealESRGAN_x4.pth")

    def test_an_unchosen_upscaler_falls_back_to_the_universal_one(self):
        workflow = templating.render_workflow_text(
            (WORKFLOWS / "upscale_fast.json").read_text(encoding="utf-8"),
            width=100, height=100, prompt="", negative_prompt="",
            image_filename="input.png", output_prefix="task-1")
        loader = next(n for n in workflow.values()
                      if n["class_type"] == "UpscaleModelLoader")
        self.assertEqual(loader["inputs"]["model_name"], templating.DEFAULT_UPSCALE_MODEL)

    def test_no_template_uses_the_tiled_diffusion_vae_decoder(self):
        # ComfyUI-TiledDiffusion's VAEDecodeTiled_TiledDiffusion raises
        # "'Decoder' object has no attribute 'give_pre_end'" against the FLUX
        # VAE on the farm's build - the same breakage t_pose already works
        # around by rewriting the node. Core VAEDecodeTiled is the one to use.
        for file_name in routing.ENHANCE_WORKFLOWS.values():
            text = (WORKFLOWS / file_name).read_text(encoding="utf-8")
            self.assertNotIn("VAEDecodeTiled_TiledDiffusion", text, file_name)

    def test_the_refine_resize_is_exempt_from_the_internal_grid_rewrite(self):
        # runtime_settings rounds every other ImageScale up to a /32 grid; the
        # refinement pass has to run at the exact delivery size instead.
        workflow = json.loads((WORKFLOWS / "upscale_refine.json").read_text(encoding="utf-8")
                              .replace("$width", "1664").replace("$height", "2432"))
        scales = [n for n in workflow.values() if n["class_type"] == "ImageScale"]
        self.assertTrue(scales)
        for node in scales:
            self.assertEqual(node.get("_meta", {}).get("title"), "delivery")


class EnhanceRoutingTests(unittest.TestCase):
    def test_enhance_types_land_on_the_image_boxes_only(self):
        image_box = RenderServer(render_server_name="f5", available_workflows=[
            "gen_image.json", "gen_image_control_canny.json"])
        video_box = RenderServer(render_server_name="worker-4090", available_workflows=[
            "gen_image.json", "gen_image_sdxl.json", "gen_video_wan_animate2_by_url.json"])
        for render_type in routing.ENHANCE_TYPES:
            prompt = RenderPrompt(image_url="https://example.test/in.png", type=render_type)
            token = routing.scheduling_token(prompt)
            self.assertEqual(token, routing.ENHANCE_SCHEDULING_TOKEN)
            self.assertTrue(routing.server_can_run(image_box, token), render_type)
            self.assertFalse(routing.server_can_run(video_box, token), render_type)

    def test_each_enhance_type_resolves_to_its_own_template(self):
        for render_type, file_name in routing.ENHANCE_WORKFLOWS.items():
            prompt = RenderPrompt(image_url="https://example.test/in.png", type=render_type)
            self.assertEqual(routing.resolve_workflow_file(prompt), (file_name, None))
            self.assertEqual(routing.output_extension(prompt), ".png")

    def test_a_server_override_cannot_redirect_an_enhance_template(self):
        # worker-4090 maps gen_image.json onto its own schnell workflow; that
        # mapping must not follow an enhancement template around.
        server = RenderServer(render_server_name="worker-4090",
                              workflow_overrides={"gen_image.json": "gen_image_flux1_schnell.json"})
        self.assertEqual(
            routing.resolve_runtime_workflow(server, "upscale_refine.json"),
            "upscale_refine.json")

    def test_enlargement_is_allowed_past_the_ordinary_render_ceiling(self):
        self.assertEqual(routing.clamp_image_dims(3328, 4096), (2048, 2048))
        self.assertEqual(routing.clamp_enhance_dims(3328, 4096), (3328, 4096))
        self.assertEqual(routing.clamp_enhance_dims(9000, 9000),
                         (routing.ENHANCE_MAX_SIDE, routing.ENHANCE_MAX_SIDE))


class CatalogueTests(unittest.TestCase):
    def test_the_three_nodes_are_published_and_callable(self):
        for service_id, api in (("upscale", "/api/upscale"),
                                ("detail_enhance", "/api/detail"),
                                ("face_fix", "/api/facefix")):
            entry = ai_services.service(service_id)
            self.assertIsNotNone(entry, service_id)
            self.assertEqual(entry["status"], "live", service_id)
            self.assertEqual(entry["api"], api)
            self.assertEqual(ai_services.produced_types(entry), [ai_services.IMAGE])
            self.assertIn(ai_services.IMAGE, ai_services.accepted_types(entry))
            self.assertTrue(ai_services.params_for(service_id), service_id)

    def test_a_picture_can_be_handed_to_each_of_them(self):
        targets = {item["service_id"] for item in ai_services.targets_for(ai_services.IMAGE)}
        self.assertTrue({"upscale", "detail_enhance", "face_fix"} <= targets)

    def test_video_super_resolution_says_what_it_is_waiting_for(self):
        entry = ai_services.service("upscale_video")
        self.assertEqual(entry["status"], "planned")
        self.assertIn("SeedVR2", entry["blocked_reason"])

    def test_every_offered_upscaler_option_matches_the_api_allow_list(self):
        options = {option["value"]
                   for option in next(p for p in ai_services.params_for("upscale")
                                      if p["name"] == "model")["options"]
                   if not option.get("disabled")}
        self.assertEqual(options, set(ai_enhance_api.INSTALLED_UPSCALE_MODELS))


if __name__ == "__main__":
    unittest.main()
