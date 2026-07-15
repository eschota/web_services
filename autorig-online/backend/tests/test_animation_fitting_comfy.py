import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import httpx

from animation_fitting.comfy import (
    ComfyAnimationClient,
    ComfyContractError,
    ComfyWorker,
    apply_workflow_bindings,
    deterministic_prompt_id,
    workflow_fingerprint,
)
from animation_fitting.specs import load_animation_fitting_specs


def _api_prompt_for(workflow):
    path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / workflow.workflow_name
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _node_value(prompt, title, input_name):
    for node in prompt.values():
        if node.get("_meta", {}).get("title") == title:
            return node["inputs"][input_name]
    raise AssertionError(f"node title not found: {title}")


class AnimationFittingWorkflowBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.specs = load_animation_fitting_specs()

    def _bind(self, mode):
        workflow = self.specs.workflows[mode]
        return workflow, apply_workflow_bindings(
            _api_prompt_for(workflow),
            workflow,
            uploaded_start_image="autorig/ref.png",
            positive_prompt="horse motion",
            negative_prompt="camera movement",
            frame_count=49,
            seed=1234,
            output_prefix="animation_fitting/task/run/candidate_00",
        )

    def test_loop_reuses_identical_start_image_at_frame_zero_and_n_minus_one(self):
        workflow, prompt = self._bind("loop")
        start = workflow.bindings["start_image"]
        end = workflow.bindings["end_image"]
        self.assertEqual(_node_value(prompt, start.node_title, start.input_name), "autorig/ref.png")
        self.assertEqual(_node_value(prompt, end.node_title, end.input_name), "autorig/ref.png")
        for target in workflow.bindings["frame_count"].targets:
            self.assertEqual(_node_value(prompt, target.node_title, target.input_name), 49)
        for target in workflow.bindings["fps"].targets:
            self.assertEqual(_node_value(prompt, target.node_title, target.input_name), 24)
        for target in workflow.bindings["output_fps"].targets:
            self.assertEqual(_node_value(prompt, target.node_title, target.input_name), 30)

    def test_one_shot_uses_start_frame_only_and_rejects_hidden_end_conditioning(self):
        workflow, prompt = self._bind("one_shot")
        self.assertFalse(
            any(node.get("_meta", {}).get("title") == "AUTORIG_END_FRAME" for node in prompt.values())
        )
        poisoned = _api_prompt_for(workflow)
        poisoned["999"] = {
            "class_type": "LTXVAddGuide",
            "inputs": {"frame_idx": -1},
            "_meta": {"title": "AUTORIG_END_GUIDE_N_MINUS_1"},
        }
        with self.assertRaisesRegex(ComfyContractError, "must not include end-frame"):
            apply_workflow_bindings(
                poisoned,
                workflow,
                uploaded_start_image="autorig/ref.png",
                positive_prompt="horse motion",
                negative_prompt="camera movement",
                frame_count=49,
                seed=1234,
                output_prefix="animation_fitting/task/death/candidate_00",
            )

    def test_prompt_id_is_stable_uuid_v4_shape(self):
        first = deterministic_prompt_id("same-job")
        second = deterministic_prompt_id("same-job")
        self.assertEqual(first, second)
        parsed = uuid.UUID(first)
        self.assertEqual(parsed.version, 4)
        self.assertEqual(parsed.variant, uuid.RFC_4122)

    def test_worker_rejects_unpinned_or_plaintext_remote_route(self):
        with self.assertRaisesRegex(ComfyContractError, "SHA-256"):
            ComfyWorker("gpu", "http://127.0.0.1:8188", "workflow.json", "")
        with self.assertRaisesRegex(ComfyContractError, "loopback"):
            ComfyWorker("gpu", "http://render.example", "workflow.json", "0" * 64)


class AnimationFittingComfyClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_validates_pins_and_posts_the_same_read_once_bytes(self):
        original = b"pinned browser guide bytes\x00\x01\x02"
        mutated = b"mutated after the upload request was built"
        digest = hashlib.sha256(original).hexdigest()
        captured = {"post_count": 0}

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir) / "guide_000.png"
            image_path.write_bytes(original)

            def handler(request):
                if request.method == "POST" and request.url.path == "/upload/image":
                    captured["post_count"] += 1
                    image_path.write_bytes(mutated)
                    captured["body"] = request.content
                    captured["content_type"] = request.headers.get("content-type")
                    return httpx.Response(
                        200,
                        json={
                            "name": f"autorig_{digest[:32]}.png",
                            "subfolder": "autorig_animation_fitting",
                        },
                    )
                return httpx.Response(404)

            worker = ComfyWorker(
                "local-4090",
                "http://127.0.0.1:8188",
                "workflow.json",
                "0" * 64,
            )
            http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                client = ComfyAnimationClient(worker, client=http_client)
                uploaded = await client.upload_reference_image(
                    image_path,
                    expected_sha256=digest,
                    expected_size_bytes=len(original),
                )
                with self.assertRaisesRegex(ComfyContractError, "SHA-256 mismatch"):
                    await client.upload_reference_image(
                        image_path,
                        expected_sha256=digest,
                        expected_size_bytes=len(original),
                    )
            finally:
                await http_client.aclose()

        self.assertEqual(
            uploaded,
            f"autorig_animation_fitting/autorig_{digest[:32]}.png",
        )
        self.assertEqual(captured["post_count"], 1)
        self.assertIn("multipart/form-data", captured["content_type"])
        self.assertIn(original, captured["body"])
        self.assertNotIn(mutated, captured["body"])
        self.assertIn(f"autorig_{digest[:32]}.png".encode(), captured["body"])

    async def test_upload_rejects_server_side_filename_or_subfolder_rebinding(self):
        original = b"pinned browser guide bytes"
        digest = hashlib.sha256(original).hexdigest()
        expected_name = f"autorig_{digest[:32]}.png"
        invalid_responses = (
            ({"subfolder": "autorig_animation_fitting"}, "filename"),
            (
                {
                    "name": f"other_{digest[:32]}.png",
                    "subfolder": "autorig_animation_fitting",
                },
                "changed uploaded filename",
            ),
            (
                {
                    "name": f"nested/{expected_name}",
                    "subfolder": "autorig_animation_fitting",
                },
                "path separators",
            ),
            ({"name": expected_name}, "subfolder"),
            (
                {"name": expected_name, "subfolder": "another_folder"},
                "changed upload subfolder",
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir) / "guide_000.png"
            image_path.write_bytes(original)
            worker = ComfyWorker(
                "local-4090",
                "http://127.0.0.1:8188",
                "workflow.json",
                "0" * 64,
            )
            for response_json, expected_message in invalid_responses:
                with self.subTest(response=response_json):
                    def handler(request, payload=response_json):
                        if request.method == "POST" and request.url.path == "/upload/image":
                            return httpx.Response(200, json=payload)
                        return httpx.Response(404)

                    http_client = httpx.AsyncClient(
                        transport=httpx.MockTransport(handler)
                    )
                    try:
                        client = ComfyAnimationClient(worker, client=http_client)
                        with self.assertRaisesRegex(
                            ComfyContractError, expected_message
                        ):
                            await client.upload_reference_image(
                                image_path,
                                expected_sha256=digest,
                                expected_size_bytes=len(original),
                            )
                    finally:
                        await http_client.aclose()

    async def test_render_timeout_defaults_to_two_hours_and_is_environment_configurable(self):
        worker = ComfyWorker(
            "local-4090",
            "http://127.0.0.1:8188",
            "workflow.json",
            "0" * 64,
        )
        default_client = ComfyAnimationClient(worker, client=httpx.AsyncClient())
        self.assertEqual(default_client.render_timeout_seconds, 7200.0)
        await default_client._client.aclose()

        with patch.dict("os.environ", {"AUTORIG_LTX_RENDER_TIMEOUT_SECONDS": "10800"}):
            configured_http = httpx.AsyncClient()
            configured_client = ComfyAnimationClient(worker, client=configured_http)
            self.assertEqual(configured_client.render_timeout_seconds, 10800.0)
            await configured_http.aclose()

        with patch.dict("os.environ", {"AUTORIG_LTX_RENDER_TIMEOUT_SECONDS": "0"}):
            invalid_http = httpx.AsyncClient()
            try:
                with self.assertRaisesRegex(ComfyContractError, "must be positive"):
                    ComfyAnimationClient(worker, client=invalid_http)
            finally:
                await invalid_http.aclose()

    async def test_fetch_pins_canonical_workflow_and_submit_uses_deterministic_id(self):
        workflow = {
            "1": {
                "class_type": "TestNode",
                "inputs": {"text": "hello"},
                "_meta": {"title": "TEST"},
            }
        }
        fingerprint = workflow_fingerprint(workflow)
        captured = {}

        def handler(request):
            if request.method == "GET" and "/api/userdata/" in request.url.path:
                return httpx.Response(200, json=workflow)
            if request.method == "GET" and request.url.path.startswith("/history/"):
                return httpx.Response(200, json={})
            if request.method == "GET" and request.url.path == "/queue":
                return httpx.Response(200, json={"queue_running": [], "queue_pending": []})
            if request.method == "POST" and request.url.path == "/prompt":
                payload = json.loads(request.content)
                captured.update(payload)
                return httpx.Response(200, json={"prompt_id": payload["prompt_id"]})
            return httpx.Response(404)

        worker = ComfyWorker(
            "local-4090",
            "http://127.0.0.1:8188",
            "autorig_ltx2_animal_loop_v1_api.json",
            fingerprint,
        )
        transport = httpx.MockTransport(handler)
        http_client = httpx.AsyncClient(transport=transport)
        try:
            client = ComfyAnimationClient(worker, client=http_client)
            fetched, fetched_fingerprint = await client.fetch_api_workflow()
            submission = await client.submit(fetched, "animation-fitting-job-1")
        finally:
            await http_client.aclose()

        self.assertEqual(fetched, workflow)
        self.assertEqual(fetched_fingerprint, fingerprint)
        self.assertEqual(submission.prompt_id, deterministic_prompt_id("animation-fitting-job-1"))
        self.assertEqual(captured["prompt_id"], submission.prompt_id)
        self.assertEqual(captured["prompt"], workflow)
        self.assertFalse(submission.resumed_existing_bool)

    async def test_fetch_rejects_workflow_drift(self):
        workflow = {"1": {"class_type": "TestNode", "inputs": {}}}

        def handler(request):
            return httpx.Response(200, json=workflow)

        worker = ComfyWorker(
            "local-4090",
            "http://127.0.0.1:8188",
            "workflow.json",
            "0" * 64,
        )
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = ComfyAnimationClient(worker, client=http_client)
            with self.assertRaisesRegex(ComfyContractError, "fingerprint mismatch"):
                await client.fetch_api_workflow()
        finally:
            await http_client.aclose()

    async def test_wait_for_output_survives_transient_history_timeout(self):
        prompt_id = "300625be-8b0c-431d-8060-24e33821cdbd"
        history_calls = 0

        def handler(request):
            nonlocal history_calls
            if request.url.path == f"/history/{prompt_id}":
                history_calls += 1
                if history_calls == 1:
                    raise httpx.ReadTimeout("GPU-saturated Comfy poll", request=request)
                return httpx.Response(200, json={
                    prompt_id: {
                        "status": {"completed": True, "status_str": "success"},
                        "outputs": {
                            "save": {
                                "videos": [{
                                    "filename": "horse_walk_v8.mp4",
                                    "subfolder": "animation_fitting/controlled",
                                    "type": "output",
                                }],
                            },
                        },
                    },
                })
            if request.url.path == "/queue":
                return httpx.Response(200, json={
                    "queue_running": [[0, prompt_id]],
                    "queue_pending": [],
                })
            return httpx.Response(404)

        worker = ComfyWorker(
            "local-4090",
            "http://127.0.0.1:8188",
            "workflow.json",
            "0" * 64,
        )
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = ComfyAnimationClient(
                worker,
                client=http_client,
                render_timeout_seconds=1.0,
                poll_interval_seconds=0.001,
            )
            history, output = await client.wait_for_output(prompt_id)
        finally:
            await http_client.aclose()

        self.assertEqual(history_calls, 2)
        self.assertTrue(history["status"]["completed"])
        self.assertEqual(output.filename, "horse_walk_v8.mp4")


if __name__ == "__main__":
    unittest.main()
