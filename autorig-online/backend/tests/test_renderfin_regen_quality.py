"""The Qwen re-pose bundle: its alpha sanity check and how the queue collects it.

qwen_edit renders a FULL image and an RMBG `_Isolated_` cut-out exactly like
t_pose, but there is no control mask to compare with, so the cut-out itself is
read: a subject is present, the frame did not cut it, and it spans roughly its
own height the way a T-pose does. A miss there is the model's, not the box's.
"""
import hashlib
import io
import json
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from PIL import Image, ImageDraw

from renderfin import comfy_adapter, config, image_quality
from renderfin.models import TASK_DONE, TASK_ERROR, TASK_RENDERING, RenderPrompt
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry

from test_renderfin_queue import _Env, _server, run

SIZE = 1024
SKIN = (200, 120, 80, 255)


def _encode(image: Image.Image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, "PNG")
    return buffer.getvalue()


def _full() -> bytes:
    image = Image.new("RGB", (SIZE, SIZE), (127, 127, 127))
    ImageDraw.Draw(image).rectangle((150, 150, 874, 900), fill=(200, 120, 80))
    return _encode(image)


def _cutout(*shapes, size=SIZE, mode="RGBA") -> bytes:
    image = Image.new(mode, (size, size), (0, 0, 0, 0) if mode == "RGBA" else (0, 0, 0))
    draw = ImageDraw.Draw(image)
    for box in shapes:
        draw.rectangle(box, fill=SKIN if mode == "RGBA" else SKIN[:3])
    return _encode(image)


BODY = [(452, 150, 572, 600), (460, 600, 500, 900), (524, 600, 564, 900)]
ARMS_OUT = [(150, 300, 874, 340)]
ARMS_DOWN = [(410, 300, 440, 620), (584, 300, 614, 620)]
T_POSE = _cutout(*BODY, *ARMS_OUT)


class QwenEditValidatorTests(unittest.TestCase):
    def _reject(self, isolated, primary=None):
        with self.assertRaises(image_quality.RenderArtifactQualityError) as raised:
            image_quality.validate_qwen_edit_bundle(primary or _full(), isolated)
        return raised.exception

    def test_a_clean_t_pose_passes(self):
        report = image_quality.validate_qwen_edit_bundle(_full(), T_POSE)
        self.assertTrue(report["passed"])
        self.assertEqual(report["schema"], image_quality.QWEN_EDIT_REPORT_SCHEMA)
        self.assertGreaterEqual(report["subject"]["span_to_height"], 0.9)
        self.assertEqual(set(report["subject"]["edge_solid_pixels"].values()), {0.0})
        json.dumps(report, allow_nan=False)  # persisted and archived as-is

    def test_arms_down_is_a_content_miss(self):
        exc = self._reject(_cutout(*BODY, *ARMS_DOWN))
        self.assertIsInstance(exc, image_quality.RenderContentRejected)
        self.assertEqual(exc.machine_code, "qwen_edit_pose_not_spread")
        self.assertTrue(exc.report["failure"]["content"])
        self.assertLess(exc.report["subject"]["span_to_height"], 0.7)
        self.assertTrue(image_quality.is_content_rejection(str(exc)))

    def test_a_figure_cut_by_the_frame_is_a_content_miss(self):
        cuts = {
            "left": (0, 300, 200, 340),
            "right": (820, 300, SIZE - 1, 340),
            "top": (480, 0, 540, 200),
            "bottom": (460, 800, 500, SIZE - 1),
        }
        for edge, box in cuts.items():
            with self.subTest(edge=edge):
                exc = self._reject(_cutout(*BODY, *ARMS_OUT, box))
                self.assertIsInstance(exc, image_quality.RenderContentRejected)
                self.assertEqual(exc.machine_code, "qwen_edit_subject_clipped")
                self.assertIn(edge, exc.report["failure"]["details"]["edges"])

    def test_an_empty_cut_out_has_no_subject(self):
        exc = self._reject(_cutout())
        self.assertIsInstance(exc, image_quality.RenderContentRejected)
        self.assertEqual(exc.machine_code, "qwen_edit_subject_missing")

    def test_a_background_left_in_the_cut_out_is_a_content_miss(self):
        exc = self._reject(_cutout((0, 0, SIZE - 1, SIZE - 1)))
        self.assertIsInstance(exc, image_quality.RenderContentRejected)
        self.assertEqual(exc.machine_code, "qwen_edit_background_not_removed")

    def test_matting_specks_do_not_move_the_verdict(self):
        specks = [(0, 0, 1, 1), (1020, 1020, 1023, 1023), (1000, 500, 1003, 503), (20, 60, 22, 62)]
        report = image_quality.validate_qwen_edit_bundle(_full(), _cutout(*BODY, *ARMS_OUT, *specks))
        self.assertTrue(report["passed"])
        exc = self._reject(_cutout(*BODY, *ARMS_DOWN, *specks))
        self.assertEqual(exc.machine_code, "qwen_edit_pose_not_spread")

    def test_structural_faults_blame_the_box_not_the_pose(self):
        cases = {
            "isolated_rgba_required": dict(isolated=_cutout(*BODY, *ARMS_OUT, mode="RGB")),
            "isolated_dimensions_mismatch": dict(isolated=_cutout(*[(10, 10, 100, 100)], size=512)),
            "primary_decode_failed": dict(isolated=T_POSE, primary=b"not a png"),
            "edit_bundle_bytes_invalid": dict(isolated="not bytes"),
        }
        for code, kwargs in cases.items():
            with self.subTest(code=code):
                exc = self._reject(**kwargs)
                self.assertEqual(exc.machine_code, code)
                self.assertNotIsInstance(exc, image_quality.RenderContentRejected)
                self.assertFalse(image_quality.is_content_rejection(str(exc)))

    def test_only_content_verdicts_carry_the_prefix(self):
        self.assertTrue(image_quality.is_content_rejection(
            "render task t failed: render artifact quality rejected on f5: "
            "qwen_edit_subject_clipped: the frame cuts the figure at left"
        ))
        for text in ("tpose_output_bundle_incomplete: x", "primary_matches_control_mask: x", ""):
            self.assertFalse(image_quality.is_content_rejection(text))


class QueueQwenEditFinishTests(unittest.TestCase):
    """_finish collects the FULL + _Isolated_ pair like t_pose and reads the
    cut-out instead of the control mask."""

    def _scenario(self, body):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server(workflows=("gen_image.json", "qwen_edit.json"))
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    await body(queue, registry, server)
                finally:
                    await queue.stop()

        run(scenario())

    async def _rendering(self, queue, server, ptype="qwen_edit"):
        task = await queue.enqueue(
            RenderPrompt(
                prompt="Redraw the character from image 1 standing in a clean T-pose.",
                type=ptype,
                image_url=f"{config.PUBLIC_BASE_URL}/render/bot/job_regen_source.png",
                user_name="bot",
            )
        )
        task.status = TASK_RENDERING
        task.server_name = server.render_server_name
        task.started_at = time.time()
        return task

    @staticmethod
    def _entry(task, *, isolated=True):
        images = [
            # another prompt's output in the same history must never be taken
            {"filename": "someone-else_00001_.png", "subfolder": "", "type": "output"},
            {"filename": f"{task.id}_00001_.png", "subfolder": "", "type": "output"},
        ]
        if isolated:
            images.append(
                {"filename": f"{task.id}_Isolated_00001_.png", "subfolder": "", "type": "output"}
            )
        return {"outputs": {"9": {"images": images[:2]}, "301": {"images": images[2:]}}}

    @staticmethod
    def _downloads(cutout):
        async def download(client, server, artifact):
            name = artifact["filename"]
            assert not name.startswith("someone-else"), "took another prompt's output"
            return cutout if "_Isolated_" in name else _full()

        return download

    def test_the_full_render_and_its_cut_out_are_collected_together(self):
        async def body(queue, registry, server):
            task = await self._rendering(queue, server)
            no_mask = AsyncMock(side_effect=AssertionError("control input fetched"))
            with patch.object(comfy_adapter, "download_artifact", side_effect=self._downloads(T_POSE)), \
                    patch.object(comfy_adapter, "download_input_image", no_mask):
                await queue._finish_guarded(task, server, self._entry(task))

            self.assertEqual(task.status, TASK_DONE, task.error)
            no_mask.assert_not_awaited()  # no control-mask comparison for an edit
            user_dir = config.RENDER_DIR / "bot"
            self.assertEqual(Path(task.output_path), user_dir / f"{task.id}.png")
            self.assertEqual(Path(task.output_path).read_bytes(), _full())
            self.assertEqual((user_dir / f"{task.id}_Isolated.png").read_bytes(), T_POSE)
            self.assertEqual(
                task.extra_outputs["isolated"],
                f"{config.PUBLIC_BASE_URL}/render/bot/{task.id}_Isolated.png",
            )
            self.assertTrue((user_dir / f"{task.id}.jpg").is_file())
            self.assertEqual(task.artifact_sha256, hashlib.sha256(_full()).hexdigest())

        self._scenario(body)

    def test_a_pose_miss_fails_the_render_but_keeps_the_box_in_rotation(self):
        async def body(queue, registry, server):
            task = await self._rendering(queue, server)
            with patch.object(
                comfy_adapter, "download_artifact",
                side_effect=self._downloads(_cutout(*BODY, *ARMS_DOWN)),
            ):
                await queue._finish_guarded(task, server, self._entry(task))

            self.assertEqual(task.status, TASK_ERROR)
            self.assertIn("render artifact quality rejected on raptor", task.error)
            self.assertIn("qwen_edit_pose_not_spread", task.error)
            self.assertNotIn(server.render_server_name, queue._server_submit_cooldowns)
            self.assertEqual(registry.get(server.render_server_name).status, "online")
            self.assertFalse((config.RENDER_DIR / "bot" / f"{task.id}.png").exists())
            archive = config.DATA_DIR / "rejected" / "qwen_edit" / task.id
            reports = list(archive.glob("*/report.json"))
            self.assertEqual(len(reports), 1)
            report = json.loads(reports[0].read_text(encoding="utf-8"))
            self.assertTrue(report["failure"]["content"])
            self.assertEqual(report["context"]["task_id"], task.id)

        self._scenario(body)

    def test_a_missing_cut_out_is_still_the_box_fault(self):
        async def body(queue, registry, server):
            task = await self._rendering(queue, server)
            with patch.object(comfy_adapter, "download_artifact", side_effect=self._downloads(T_POSE)):
                await queue._finish_guarded(task, server, self._entry(task, isolated=False))
            self.assertEqual(task.status, TASK_ERROR)
            self.assertIn("tpose_output_bundle_incomplete", task.error)
            self.assertGreater(queue._server_submit_cooldowns[server.render_server_name], time.time())
            self.assertEqual(registry.get(server.render_server_name).status, "render_quality_error")

        self._scenario(body)

    def test_a_malformed_cut_out_quarantines_the_box(self):
        async def body(queue, registry, server):
            task = await self._rendering(queue, server)
            with patch.object(
                comfy_adapter, "download_artifact",
                side_effect=self._downloads(_cutout(*BODY, *ARMS_OUT, mode="RGB")),
            ):
                await queue._finish_guarded(task, server, self._entry(task))
            self.assertEqual(task.status, TASK_ERROR)
            self.assertIn("isolated_rgba_required", task.error)
            self.assertGreater(queue._server_submit_cooldowns[server.render_server_name], time.time())

        self._scenario(body)

    def test_t_pose_keeps_its_control_mask_validation(self):
        async def body(queue, registry, server):
            task = await self._rendering(queue, server, ptype="t_pose")
            tpose_check = AsyncMock(return_value={"passed": True})
            edit_check = Mock()
            with patch.object(comfy_adapter, "download_artifact", side_effect=self._downloads(T_POSE)), \
                    patch.object(queue, "_validate_tpose_bundle_bytes", tpose_check), \
                    patch.object(image_quality, "validate_qwen_edit_bundle", edit_check):
                await queue._finish_guarded(task, server, self._entry(task))
            self.assertEqual(task.status, TASK_DONE, task.error)
            tpose_check.assert_awaited_once()
            edit_check.assert_not_called()
            self.assertTrue(task.extra_outputs["isolated"].endswith("_Isolated.png"))

        self._scenario(body)

    def test_a_managed_edit_bundle_is_durable_only_with_its_cut_out(self):
        with _Env():
            primary = config.RENDER_DIR / "bot" / "t.png"
            primary.parent.mkdir(parents=True, exist_ok=True)
            primary.write_bytes(_full())
            from renderfin.models import RenderTask

            task = RenderTask(
                prompt=RenderPrompt(type="qwen_edit", image_url="https://x/s.png"),
                output_path=str(primary),
                artifact_sha256=hashlib.sha256(_full()).hexdigest(),
                managed_comfy_artifact_size_int=len(_full()),
            )
            self.assertFalse(RenderQueue._managed_bundle_is_durable(task))
            isolated = primary.with_name("t_Isolated.png")
            isolated.write_bytes(T_POSE)
            task.managed_comfy_isolated_output_path = str(isolated)
            task.managed_comfy_isolated_sha256 = hashlib.sha256(T_POSE).hexdigest()
            task.managed_comfy_isolated_size_int = len(T_POSE)
            self.assertTrue(RenderQueue._managed_bundle_is_durable(task))
            plain = task.model_copy(update={"prompt": RenderPrompt(prompt="x")})
            plain.managed_comfy_isolated_output_path = ""
            self.assertTrue(RenderQueue._managed_bundle_is_durable(plain))


if __name__ == "__main__":
    unittest.main()
