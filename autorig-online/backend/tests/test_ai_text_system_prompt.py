"""The graph editor's standing instructions survive the public text proxy."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import ai_vision_api as api
from fastapi import HTTPException


class TextSystemPromptTests(unittest.TestCase):
    def payload(self, body):
        run = AsyncMock(return_value={"success_bool": True})
        with patch.object(api, "_run", run):
            asyncio.run(api._uncached_api_text2text(None, api.TextRequest(**body)))
        return run.call_args.args[2]

    def test_system_instructions_are_separate_from_graph_and_user_request(self):
        result = self.payload({"system_prompt": "Only edit the graph.",
                               "input": '{"user_request":"add an image","graph":{}}'})
        self.assertEqual(result["system_prompt"], "Only edit the graph.")
        self.assertEqual(result["prompt"], '{"user_request":"add an image","graph":{}}')
        self.assertNotIn("Only edit", result["prompt"])

    def test_legacy_text_requests_keep_the_same_single_prompt(self):
        result = self.payload({"prompt": "Summarize", "input": "Material"})
        self.assertEqual(result["prompt"], "Summarize\n\n--- text ---\nMaterial")
        self.assertNotIn("system_prompt", result)

    def test_unlimited_output_survives_public_request_and_dispatch_payload(self):
        result = self.payload({"input": "Describe the graph", "max_output_tokens": -1})
        self.assertEqual(result["max_output_tokens"], -1)
        self.assertEqual(api.VisionRequest(prompt="Describe", max_output_tokens=-1).max_output_tokens, -1)
        self.assertEqual(api._output_budget({"default_output_tokens": 2048}, -1), -1)
        with self.assertRaises(ValueError):
            api.TextRequest(prompt="test", max_output_tokens=0)

    def test_unlimited_requests_cannot_silently_land_on_a_capped_worker(self):
        old = {"name": "old", "url": "http://old", "token": "test"}
        new = {"name": "new", "url": "http://new", "token": "test"}
        async def probe(client, worker):
            return True, {"load": 0, "models": ["bonsai2-27b"],
                          "unlimited_output_supported": worker == new}
        with patch.object(api, "_load_ai_workers", return_value=[old, new]), \
             patch.object(api, "_node_is_free", probe):
            self.assertEqual(asyncio.run(api._pick_worker(
                None, "bonsai2-27b", require_unlimited_output=True)), new)
        with patch.object(api, "_load_ai_workers", return_value=[old]), \
             patch.object(api, "_node_is_free", probe):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(api._pick_worker(None, "bonsai2-27b", require_unlimited_output=True))
            self.assertEqual(caught.exception.detail["error_string"], "unlimited_output_not_supported")

    def test_combined_system_and_user_budget_is_still_bounded(self):
        with self.assertRaises(HTTPException) as caught:
            self.payload({"system_prompt": "s" * 4000, "input": "u" * 4001})
        self.assertEqual(caught.exception.detail["error_string"], "prompt_too_long")

    def test_system_requests_skip_legacy_workers_instead_of_losing_instructions(self):
        old = {"name": "old", "url": "http://old", "token": "test"}
        new = {"name": "new", "url": "http://new", "token": "test"}
        async def probe(client, worker):
            return True, {"load": 0 if worker == old else 1,
                          "models": ["bonsai2-27b"],
                          "system_prompt_models": ["bonsai2-27b"] if worker == new else [],
                          "system_prompt_supported": worker == new}
        with patch.object(api, "_load_ai_workers", return_value=[old, new]), \
             patch.object(api, "_node_is_free", probe):
            self.assertEqual(asyncio.run(api._pick_worker(None, "bonsai2-27b")), old)
            self.assertEqual(asyncio.run(api._pick_worker(
                None, "bonsai2-27b", require_system_prompt=True)), new)
            with self.assertRaises(HTTPException):
                asyncio.run(api._pick_worker(None, "qwen35-9b-uncensored", require_system_prompt=True))
        with patch.object(api, "_load_ai_workers", return_value=[old]), \
             patch.object(api, "_node_is_free", probe):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(api._pick_worker(None, "bonsai2-27b", require_system_prompt=True))
            self.assertEqual(caught.exception.detail["error_string"], "system_prompt_not_supported")


if __name__ == "__main__":
    unittest.main()
