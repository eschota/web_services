"""Public /api/vision and /api/text2text dispatch to the converter farm."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_vision_api  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

WORKER = {
    "name": "F1",
    "url": "https://converter-f1.freestock.online/api-converter-glb",
    "token": "test-token",
    "physical_node": "f1-pc",
}


def _app() -> TestClient:
    app = FastAPI()
    app.include_router(ai_vision_api.router)
    return TestClient(app)


class ModelCatalogueTests(unittest.TestCase):
    def test_models_endpoint_lists_a_default(self):
        response = _app().get("/api/ai/models")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        ids = [m["id"] for m in body["models_array"]]
        self.assertIn(body["default_model_string"], ids)

    def test_unknown_model_is_rejected_with_the_available_list(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._model_entry("gpt-nonexistent")
        detail = caught.exception.detail
        self.assertEqual(detail["error_string"], "unknown_model")
        self.assertIn(ai_vision_api.DEFAULT_MODEL_ID, detail["available_models_array"])

    def test_blank_model_falls_back_to_the_default(self):
        self.assertEqual(
            ai_vision_api._model_entry(None)["id"], ai_vision_api.DEFAULT_MODEL_ID
        )

    def test_get_mirrors_document_both_endpoints(self):
        client = _app()
        for path in ("/api/vision", "/api/text2text"):
            body = client.get(path).json()
            self.assertEqual(body["method_string"], "POST")
            self.assertEqual(body["url_string"], path)


class TaskIdRoutingTests(unittest.TestCase):
    def test_node_key_is_url_safe_and_stable(self):
        self.assertEqual(ai_vision_api._node_key(WORKER), "f1-pc")
        self.assertEqual(
            ai_vision_api._node_key({"physical_node": "Raptor / RYZEN 01"}),
            "raptor---ryzen-01",
        )

    def test_status_rejects_a_task_id_without_a_node(self):
        response = _app().get("/api/ai/status/no-node-here")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "malformed_task_id")

    def test_status_reports_an_unconfigured_node(self):
        with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[WORKER]):
            response = _app().get("/api/ai/status/ghost-node.abc")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"]["error_string"], "unknown_node")


class DispatchTests(unittest.TestCase):
    def test_vision_requires_an_image(self):
        response = _app().post("/api/vision", json={"prompt": "what is this"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "image_required")

    def test_empty_prompt_is_rejected(self):
        response = _app().post("/api/text2text", json={"prompt": "   "})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "prompt_required")

    def test_no_configured_worker_is_a_service_error(self):
        with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[]):
            response = _app().post("/api/text2text", json={"prompt": "hi"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["error_string"], "no_workers_configured"
        )

    def test_a_submitted_task_id_carries_its_node(self):
        async def fake_pick(client):
            return WORKER

        async def fake_submit(client, worker, path, payload):
            self.assertEqual(path, "/text2text")
            self.assertEqual(payload["prompt"], "hi")
            return "abc-123"

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit):
            response = _app().post("/api/text2text", json={"prompt": "hi"})
        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["task_id_string"], "f1-pc.abc-123")
        self.assertEqual(body["status_url_string"], "/api/ai/status/f1-pc.abc-123")
        self.assertFalse(body["finished_bool"])

    def test_wait_returns_the_finished_answer(self):
        async def fake_pick(client):
            return WORKER

        async def fake_submit(client, worker, path, payload):
            return "abc-123"

        async def fake_status(client, worker, worker_task_id):
            return {
                "status": "Completed",
                "answer": "a black lamp",
                "reasoning": "thinking",
                "elapsed_seconds": 6.2,
                "mode": "text",
            }

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit), \
             mock.patch.object(ai_vision_api, "_fetch_status", fake_status):
            response = _app().post(
                "/api/text2text", json={"prompt": "hi", "wait_seconds": 10}
            )
        body = response.json()
        self.assertTrue(body["finished_bool"])
        self.assertEqual(body["answer_string"], "a black lamp")
        self.assertEqual(body["elapsed_seconds_float"], 6.2)

    def test_a_failed_task_is_reported_as_unsuccessful(self):
        async def fake_pick(client):
            return WORKER

        async def fake_submit(client, worker, path, payload):
            return "abc-123"

        async def fake_status(client, worker, worker_task_id):
            return {"status": "Failed", "error": "model_returned_empty_answer"}

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit), \
             mock.patch.object(ai_vision_api, "_fetch_status", fake_status):
            body = _app().post(
                "/api/text2text", json={"prompt": "hi", "wait_seconds": 10}
            ).json()
        self.assertFalse(body["success_bool"])
        self.assertEqual(body["error_string"], "model_returned_empty_answer")


class InlineImageTests(unittest.TestCase):
    def test_data_url_prefix_is_stripped(self):
        data = ai_vision_api._decode_inline_image("data:image/png;base64,aGk=")
        self.assertEqual(data, b"hi")

    def test_non_base64_is_rejected(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._decode_inline_image("not base64 at all!!")
        self.assertEqual(caught.exception.detail["error_string"], "image_not_base64")

    def test_oversized_inline_image_is_rejected(self):
        import base64

        payload = base64.b64encode(b"x" * (ai_vision_api.MAX_INLINE_IMAGE_BYTES + 1))
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._decode_inline_image(payload.decode())
        self.assertEqual(caught.exception.status_code, 413)


class WorkerChoiceTests(unittest.TestCase):
    def test_the_least_loaded_reachable_node_wins(self):
        busy = dict(WORKER, physical_node="busy", name="busy")
        idle = dict(WORKER, physical_node="idle", name="idle")

        async def fake_probe(client, worker):
            if worker["physical_node"] == "busy":
                return True, 5
            return True, 0

        async def run():
            with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[busy, idle]), \
                 mock.patch.object(ai_vision_api, "_node_is_free", fake_probe):
                return await ai_vision_api._pick_worker(None)

        import asyncio

        chosen = asyncio.run(run())
        self.assertEqual(chosen["physical_node"], "idle")

    def test_all_nodes_unreachable_is_a_retryable_503(self):
        async def fake_probe(client, worker):
            return False, 0

        async def run():
            with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[WORKER]), \
                 mock.patch.object(ai_vision_api, "_node_is_free", fake_probe):
                return await ai_vision_api._pick_worker(None)

        import asyncio

        with self.assertRaises(HTTPException) as caught:
            asyncio.run(run())
        self.assertEqual(caught.exception.status_code, 503)
        self.assertEqual(caught.exception.detail["error_string"], "no_node_available")


if __name__ == "__main__":
    unittest.main()
