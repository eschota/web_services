"""Prompt-only answers: one JSON object in, the prompt alone out.

A Vision or Text node in the graph editor usually exists to write a prompt for
an image model. A language model that is not told so answers with its
reasoning, a preamble, or a apology — and that text then *is* the image prompt.
These tests pin the two halves of the fix: the standing instruction reaches the
model by a route that model actually obeys, and whatever wrapper comes back is
unwrapped before the node passes it on.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_services  # noqa: E402
import ai_vision_api as api  # noqa: E402


def _dispatch_vision(body: api.VisionRequest):
    """The payload `_uncached_api_vision` would hand a farm worker."""
    run = mock.AsyncMock(return_value={"success_bool": True})
    with mock.patch.object(api, "_run", run):
        asyncio.run(api._uncached_api_vision(None, body))
    return run.call_args.args[2]


def _dispatch_text(body: api.TextRequest):
    run = mock.AsyncMock(return_value={"success_bool": True})
    with mock.patch.object(api, "_run", run):
        asyncio.run(api._uncached_api_text2text(None, body))
    return run.call_args.args[2]


class OutputTextExtractionTests(unittest.TestCase):
    def test_a_bare_object_becomes_the_answer(self):
        text, structured = api._extract_output_text('{"output_text": "a green pine tree"}')
        self.assertEqual(text, "a green pine tree")
        self.assertTrue(structured)

    def test_a_fenced_object_is_unwrapped_too(self):
        text, structured = api._extract_output_text(
            '```json\n{"output_text": "a small fir tree, snow"}\n```')
        self.assertEqual(text, "a small fir tree, snow")
        self.assertTrue(structured)

    def test_text_after_the_object_is_discarded_not_kept(self):
        text, structured = api._extract_output_text(
            'Sure!\n{"output_text": "a decorated fir"}\nHope that helps.')
        self.assertEqual(text, "a decorated fir")
        self.assertTrue(structured)

    def test_an_unstructured_answer_survives_unchanged(self):
        raw = "A black street lamp on a magenta background."
        text, structured = api._extract_output_text(raw)
        self.assertEqual(text, raw)
        self.assertFalse(structured)

    def test_another_models_json_is_not_mistaken_for_an_answer(self):
        """The graph agent replies with operations; it must come back intact."""
        raw = '{"operations": [{"op": "add_node"}]}'
        text, structured = api._extract_output_text(raw)
        self.assertEqual(text, raw)
        self.assertFalse(structured)

    def test_a_nested_object_inside_output_text_is_still_one_string(self):
        text, structured = api._extract_output_text(
            '{"reasoning": {"steps": 2}, "output_text": "a lone pine"}')
        self.assertEqual(text, "a lone pine")
        self.assertTrue(structured)

    def test_an_empty_answer_is_not_a_crash(self):
        self.assertEqual(api._extract_output_text(""), ("", False))
        self.assertEqual(api._extract_output_text(None), ("", False))


class StandingInstructionTests(unittest.TestCase):
    def test_an_unstructured_request_carries_only_what_was_sent(self):
        self.assertEqual(api._standing_instruction(None, False), "")
        self.assertEqual(api._standing_instruction("Be terse.", False), "Be terse.")

    def test_a_structured_request_without_one_uses_the_catalogue_default(self):
        instruction = api._standing_instruction(None, True)
        self.assertIn(ai_services.SYSTEM_PROMPT_DEFAULT, instruction)
        self.assertIn("output_text", instruction)

    def test_an_edited_system_prompt_replaces_the_default_not_the_json_rule(self):
        instruction = api._standing_instruction("Answer in Latin.", True)
        self.assertIn("Answer in Latin.", instruction)
        self.assertNotIn(ai_services.SYSTEM_PROMPT_DEFAULT, instruction)
        self.assertIn("output_text", instruction)


class InstructionRoleTests(unittest.TestCase):
    def test_the_verified_model_gets_a_real_system_role(self):
        self.assertEqual(api._instruction_role(api._model_entry("bonsai2-27b")), "system")

    def test_the_unverified_model_gets_the_instruction_in_the_prompt(self):
        self.assertEqual(api._instruction_role(api._model_entry("qwen35-9b-uncensored")),
                         "prompt")

    def test_qwen_never_asks_for_a_worker_with_a_verified_system_role(self):
        """`_run` turns a system_prompt into require_system_prompt=True.

        Sending one for Qwen would not weaken the answer, it would fail the
        request: no worker has verified the role for that model.
        """
        payload = _dispatch_text(api.TextRequest(
            prompt="нарисуй ёлочку", model="qwen35-9b-uncensored", structured=True))
        self.assertNotIn("system_prompt", payload)
        self.assertTrue(payload["prompt"].startswith(ai_services.SYSTEM_PROMPT_DEFAULT))
        self.assertIn("нарисуй ёлочку", payload["prompt"])

    def test_bonsai_keeps_the_users_prompt_free_of_the_instruction(self):
        payload = _dispatch_text(api.TextRequest(
            prompt="нарисуй ёлочку", model="bonsai2-27b", structured=True))
        self.assertEqual(payload["prompt"], "нарисуй ёлочку")
        self.assertIn("output_text", payload["system_prompt"])

    def test_the_combined_request_is_still_bounded(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as caught:
            _dispatch_text(api.TextRequest(
                prompt="u" * (api.MAX_PROMPT_CHARS - 10),
                system_prompt="s" * 3000, structured=True))
        self.assertEqual(caught.exception.detail["error_string"], "prompt_too_long")


class SystemRoleFallbackTests(unittest.TestCase):
    """One farm node has the verified role. Losing it must not lose the answer."""

    def _run_with_workers(self, workers, probe):
        submitted = {}

        async def fake_submit(client, worker, path, payload):
            submitted["worker"] = worker
            submitted["payload"] = payload
            return "task-1"

        async def fake_status(client, worker, task_id):
            return {"status": "Completed", "answer": "ok"}

        with mock.patch.object(api, "_load_ai_workers", return_value=workers), \
             mock.patch.object(api, "_node_is_free", probe), \
             mock.patch.object(api, "_submit", fake_submit), \
             mock.patch.object(api, "_fetch_status", fake_status):
            asyncio.run(api._run(api._model_entry("bonsai2-27b"), "/ai-vision",
                                 {"prompt": "Describe", "system_prompt": "Only the prompt."},
                                 None, "vision"))
        return submitted

    def test_the_verified_node_still_gets_a_real_system_message(self):
        verified = {"name": "f13", "url": "http://f13", "token": "t"}

        async def probe(client, worker):
            return True, {"load": 0, "models": ["bonsai2-27b"],
                          "system_prompt_supported": True,
                          "system_prompt_models": ["bonsai2-27b"]}

        result = self._run_with_workers([verified], probe)
        self.assertEqual(result["payload"]["system_prompt"], "Only the prompt.")
        self.assertEqual(result["payload"]["prompt"], "Describe")

    def test_without_it_the_instruction_moves_into_the_prompt_and_still_runs(self):
        legacy = {"name": "f1", "url": "http://f1", "token": "t"}

        async def probe(client, worker):
            return True, {"load": 0, "models": ["bonsai2-27b"],
                          "system_prompt_supported": False,
                          "system_prompt_models": []}

        result = self._run_with_workers([legacy], probe)
        self.assertNotIn("system_prompt", result["payload"])
        self.assertTrue(result["payload"]["prompt"].startswith("Only the prompt."))
        self.assertIn("Describe", result["payload"]["prompt"])

    def test_an_unreachable_farm_is_still_an_error_not_a_silent_fold(self):
        from fastapi import HTTPException

        async def probe(client, worker):
            return False, {}

        with self.assertRaises(HTTPException) as caught:
            self._run_with_workers([{"name": "f1", "url": "http://f1", "token": "t"}], probe)
        self.assertEqual(caught.exception.detail["error_string"], "no_node_available")


class VisionStructuredTests(unittest.TestCase):
    def test_vision_sends_the_instruction_by_the_models_own_route(self):
        payload = _dispatch_vision(api.VisionRequest(
            prompt="What is this?", image_url="https://example.test/a.png",
            model="qwen35-9b-uncensored", structured=True))
        self.assertNotIn("system_prompt", payload)
        self.assertTrue(payload["prompt"].startswith(ai_services.SYSTEM_PROMPT_DEFAULT))
        payload = _dispatch_vision(api.VisionRequest(
            prompt="What is this?", image_url="https://example.test/a.png",
            model="bonsai2-27b", structured=True))
        self.assertEqual(payload["prompt"], "What is this?")
        self.assertIn("output_text", payload["system_prompt"])

    def test_an_unstructured_vision_call_is_exactly_what_it_was(self):
        payload = _dispatch_vision(api.VisionRequest(
            prompt="What is this?", image_url="https://example.test/a.png"))
        self.assertEqual(payload["prompt"], "What is this?")
        self.assertNotIn("system_prompt", payload)

    def test_the_system_prompt_is_never_part_of_the_answer(self):
        """Other nodes read `answer_string`; the instruction must not be in it."""
        raw = {"status": "Completed", "mode": "vision",
               "answer": '{"output_text": "a small decorated fir tree"}'}
        result = api._public_status("f1-pc.task", "bonsai2-27b", raw, "vision")
        self.assertEqual(result["answer_string"], "a small decorated fir tree")
        self.assertNotIn(ai_services.SYSTEM_PROMPT_DEFAULT, result["answer_string"])
        self.assertTrue(result["structured_answer_bool"])
        self.assertEqual(result["raw_answer_string"], raw["answer"])

    def test_a_plain_answer_still_reaches_the_node(self):
        raw = {"status": "Completed", "mode": "text", "answer": "just words"}
        result = api._public_status("f1-pc.task", "bonsai2-27b", raw, "text")
        self.assertEqual(result["answer_string"], "just words")
        self.assertFalse(result["structured_answer_bool"])


class CatalogueContractTests(unittest.TestCase):
    def test_vision_and_text_declare_they_can_carry_a_system_prompt(self):
        for service_id in ("vision", "text"):
            entry = ai_services.service(service_id)
            self.assertTrue(entry.get("system_prompt_capable"), service_id)
            self.assertEqual(entry.get("system_prompt_default"),
                             ai_services.SYSTEM_PROMPT_DEFAULT, service_id)

    def test_the_default_says_english_plain_text_and_image_prompt(self):
        default = ai_services.SYSTEM_PROMPT_DEFAULT
        self.assertIn("output_text", default)
        self.assertIn("English", default)
        self.assertIn("image generation", default)

    def test_the_editor_only_display_parameter_is_accepted_by_the_validator(self):
        import ai_graph_edits
        self.assertIn("_system_prompt", ai_graph_edits.DISPLAY_PARAM_KEYS)
        self.assertNotIn("_system_prompt", ai_graph_edits.BOOLEAN_DISPLAY_PARAM_KEYS)


if __name__ == "__main__":
    unittest.main()
