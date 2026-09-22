import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from renderfin import comfy_adapter
from renderfin.models import RenderPrompt, RenderServer, RenderTask
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry
from renderfin.routing import (
    is_image_request,
    output_extension,
    resolve_workflow_file,
    scheduling_token,
)
from renderfin.runtime_settings import apply_runtime_settings
from renderfin.templating import render_workflow_text

from ai_avatar_video import CHECKPOINT, WORKFLOW


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1]
    / "renderfin" / "assets" / "workflows" / WORKFLOW
)


class AvatarVideoWorkflowIntegrationTests(unittest.TestCase):
    def _render(self):
        pose_prompt = 'turns "left";\nraises both hands \\ safely'
        prompt = "Character 1 canonical appearance: short black bob. Canonical wardrobe: navy jacket."
        request = RenderPrompt(
            image_url="https://autorig.online/api/ai/avatar-assets/id/hash.png",
            control_video_url="https://pvs1.microstock.plus/path/driver.mp4",
            work_flow=WORKFLOW,
            prompt=prompt,
            pose_prompt=pose_prompt,
            main_size_width=960,
            main_size_height=540,
            frame_count=97,
            noise_seed=123,
            control_strength=1.0,
            checkpoint=CHECKPOINT,
            steps=6,
            cfg=1.0,
            sampler="lcm",
            scheduler="simple",
        )
        workflow = render_workflow_text(
            WORKFLOW_PATH.read_text(encoding="utf-8"),
            width=960,
            height=540,
            prompt=request.prompt,
            pose_prompt=request.pose_prompt,
            negative_prompt=request.negative_prompt,
            image_filename='key"frame.png',
            control_video_filename="driver.mp4",
            output_prefix="avatar-video-test",
            workflow_type=request.type,
            frames=request.frame_count,
            seed=request.noise_seed,
            checkpoint=request.checkpoint,
        )
        apply_runtime_settings(workflow, request, 960, 540)
        return request, workflow, prompt, pose_prompt

    def test_template_and_runtime_keep_verified_sampling_and_escape_prompts(self):
        request, workflow, prompt, pose_prompt = self._render()
        self.assertEqual(workflow["positive"]["inputs"]["text"], prompt)
        self.assertEqual(workflow["pose_prompt"]["inputs"]["text"], pose_prompt)
        self.assertEqual(workflow["reference_image"]["inputs"]["image"], 'key"frame.png')
        self.assertEqual(workflow["driving_video"]["inputs"]["file"], "driver.mp4")
        self.assertEqual(workflow["model"]["inputs"]["unet_name"], CHECKPOINT)
        self.assertEqual(workflow["scheduler"]["inputs"]["steps"], 6)
        self.assertEqual(workflow["scheduler"]["inputs"]["scheduler"], "simple")
        self.assertEqual(workflow["sampler"]["inputs"]["sampler_name"], "lcm")
        self.assertEqual(workflow["sample"]["inputs"]["cfg"], 1.0)
        self.assertEqual(workflow["sample"]["inputs"]["noise_seed"], 123)
        self.assertEqual(workflow["conditioning"]["inputs"]["pose_strength"], 1.0)
        self.assertEqual(request.type, "")

    def test_internal_size_and_timeline_are_padded_but_delivery_is_exact(self):
        _request, workflow, _prompt, _pose_prompt = self._render()
        for node_id in ("reference_scale", "driving_scale", "conditioning"):
            self.assertEqual(
                (workflow[node_id]["inputs"]["width"], workflow[node_id]["inputs"]["height"]),
                (960, 544),
            )
        self.assertEqual(workflow["conditioning"]["inputs"]["length"], 97)
        self.assertEqual(
            (workflow["delivery"]["inputs"]["width"], workflow["delivery"]["inputs"]["height"]),
            (960, 540),
        )
        final_resize = workflow["delivery_size_video"]
        self.assertEqual(
            (final_resize["inputs"]["width"], final_resize["inputs"]["height"]),
            (960, 540),
        )
        self.assertEqual(workflow["video"]["inputs"]["images"], ["delivery_size_video", 0])

    def test_no_placeholder_survives_and_routing_is_video_mp4(self):
        request, workflow, _prompt, _pose_prompt = self._render()
        serialized = json.dumps(workflow, ensure_ascii=False)
        for placeholder in (
            "$image", "$control_video", "$prompt", "$pose_prompt",
            "$width", "$height", "$frames", "$output_url",
        ):
            self.assertNotIn(placeholder, serialized)
        self.assertFalse(is_image_request(request))
        self.assertEqual(resolve_workflow_file(request), (WORKFLOW, None))
        self.assertEqual(scheduling_token(request), WORKFLOW)
        self.assertEqual(output_extension(request), ".mp4")
        self.assertEqual(workflow["save"]["class_type"], "SaveVideo")
        self.assertEqual(workflow["save"]["inputs"]["format"], "mp4")

    def test_queue_admits_wan_driver_and_submits_video_workflow(self):
        async def scenario():
            request = RenderPrompt(
                image_url="https://autorig.online/api/ai/avatar-assets/id/hash.png",
                control_video_url="https://pvs1.microstock.plus/path/driver.mp4",
                work_flow=WORKFLOW,
                prompt="canonical appearance",
                pose_prompt="turns left; raises hands",
                main_size_width=960,
                main_size_height=540,
                frame_count=97,
                noise_seed=123,
                control_strength=1.0,
                checkpoint=CHECKPOINT,
                steps=6,
                cfg=1.0,
                sampler="lcm",
                scheduler="simple",
            )
            task = RenderTask(
                prompt=request, workflow=WORKFLOW, workflow_file=WORKFLOW,
                output_ext=".mp4")
            server = RenderServer(
                render_server_name="worker-4090",
                render_server_url="http://127.0.0.1:8988",
                status="online", available_workflows=[WORKFLOW])
            queue = RenderQueue(ServerRegistry(), client=object())
            submitted = {}

            async def capture_submit(_client, _server, workflow, **kwargs):
                submitted["workflow"] = workflow
                return str(kwargs["prompt_id"])

            with patch.object(
                comfy_adapter, "download_input_image",
                new=AsyncMock(return_value=("keyframe.png", b"image"))), patch.object(
                comfy_adapter, "upload_image",
                new=AsyncMock(side_effect=lambda _client, _server, name, _data: name)), patch(
                "renderfin.video_input.download_prepare_video",
                new=AsyncMock(return_value=("driver.mp4", b"video"))) as prepare, patch.object(
                comfy_adapter, "submit", new=capture_submit), patch.object(
                queue, "_persist", new=AsyncMock()):
                await queue._submit_task(task, server)

            prepare.assert_awaited_once()
            self.assertEqual(prepare.await_args.args[1:], (
                "https://pvs1.microstock.plus/path/driver.mp4", 97))
            workflow = submitted["workflow"]
            self.assertEqual(workflow["driving_video"]["inputs"]["file"], "driver.mp4")
            self.assertEqual(workflow["reference_image"]["inputs"]["image"], "keyframe.png")
            self.assertEqual(workflow["save"]["class_type"], "SaveVideo")
            self.assertEqual(task.output_ext, ".mp4")
            self.assertEqual(task.server_name, "worker-4090")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
