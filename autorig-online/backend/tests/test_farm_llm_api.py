"""No paid LLM (owner 2026-10-11): the switch, the vision-config swap and the OpenAI-shaped farm shim."""
import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import farm_llm_api  # noqa: E402
import paid_llm  # noqa: E402


class PaidSwitchTests(unittest.TestCase):
    def test_off_by_default_and_on_only_for_explicit_values(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AUTORIG_PAID_LLM", None)
            self.assertFalse(paid_llm.enabled())
        for value in ("on", "1", "true", "yes", "allow"):
            with mock.patch.dict(os.environ, {"AUTORIG_PAID_LLM": value}):
                self.assertTrue(paid_llm.enabled(), value)
        for value in ("off", "0", "", "false", "no"):
            with mock.patch.dict(os.environ, {"AUTORIG_PAID_LLM": value}):
                self.assertFalse(paid_llm.enabled(), value)

    def test_scrub_forgets_paid_keys_only_while_off(self):
        env = {"OPENAI_API_KEY": "k1", "OPENROUTER_API_KEY": "k2", "DEEPSEEK_API_KEY": "k3", "KEEP": "x"}
        with mock.patch.dict(os.environ, dict(env, AUTORIG_PAID_LLM="off")):
            paid_llm.scrub_environ()
            for name in paid_llm.PAID_ENV_KEYS:
                self.assertNotIn(name, os.environ)
            self.assertEqual(os.environ["KEEP"], "x")
        with mock.patch.dict(os.environ, dict(env, AUTORIG_PAID_LLM="on")):
            paid_llm.scrub_environ()
            self.assertEqual(os.environ["OPENAI_API_KEY"], "k1")

    def test_vision_cfg_points_at_the_farm_and_drops_paid_keys(self):
        cfg = {"open_AI_api_key": "sk-real", "open_router_api_key": "or-real",
               "open_ai_api_url_string": "https://api.openai.com/v1/chat/completions",
               "open_ai_vision_model_string": "gpt-4o-mini", "image_size_int": 512}
        with mock.patch.dict(os.environ, {"AUTORIG_PAID_LLM": "off"}):
            out = paid_llm.vision_cfg(cfg)
        self.assertEqual(out["open_ai_api_url_string"], paid_llm.FARM_CHAT_URL)
        self.assertEqual(out["open_AI_api_key"], paid_llm.FARM_KEY)
        self.assertNotIn("open_router_api_key", out)
        self.assertNotIn("api.openai.com", " ".join(str(v) for v in out.values()))
        self.assertEqual(out["image_size_int"], 512)
        self.assertEqual(cfg["open_AI_api_key"], "sk-real")      # the caller's dict is not touched
        with mock.patch.dict(os.environ, {"AUTORIG_PAID_LLM": "on"}):
            self.assertIs(paid_llm.vision_cfg(cfg), cfg)


class OpenRouterFreeTests(unittest.TestCase):
    def test_a_paid_model_id_never_passes(self):
        self.assertEqual(
            paid_llm.free_only(["qwen/qwen2.5-vl-72b-instruct:free", "openai/gpt-4o-mini", "x/y:free ", ""]),
            ["qwen/qwen2.5-vl-72b-instruct:free", "x/y:free"])

    def test_daily_allowance_is_enforced_and_persisted(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(paid_llm, "OPENROUTER_BUDGET_FILE", Path(tmp) / "b.json"),                     mock.patch.object(paid_llm, "OPENROUTER_DAILY_CAP", 3),                     mock.patch.dict(paid_llm._budget_mem, {"day": "", "n": 0}):
                self.assertEqual([paid_llm.openrouter_free_take() for _ in range(5)],
                                 [True, True, True, False, False])
                paid_llm._budget_mem.update({"day": "", "n": 0})      # a restart forgets memory, not the file
                self.assertFalse(paid_llm.openrouter_free_take())

    def test_vision_cfg_keeps_the_key_only_under_the_free_name(self):
        with mock.patch.dict(os.environ, {"AUTORIG_PAID_LLM": "off"}):
            out = paid_llm.vision_cfg({"open_router_api_key": "or-real"})
        self.assertNotIn("open_router_api_key", out)
        self.assertEqual(out["open_router_free_key"], "or-real")


class ShimTests(unittest.TestCase):
    def test_split_messages_reads_text_system_and_first_image(self):
        system, user, image = farm_llm_api.split_messages([
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": [
                {"type": "text", "text": "what is it?"},
                {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,AAAA", "detail": "low"}},
                {"type": "image_url", "image_url": {"url": "https://example.com/2.jpg"}},
            ]},
        ])
        self.assertEqual(system, "be brief")
        self.assertEqual(user, "what is it?")
        self.assertEqual(image, "data:image/jpeg;base64,AAAA")

    def test_only_json_object_cuts_fences_and_chatter(self):
        self.assertEqual(farm_llm_api.only_json_object('```json\n{"a": 1}\n``` done'), '{"a": 1}')
        self.assertEqual(farm_llm_api.only_json_object("no json here"), "no json here")
        self.assertEqual(farm_llm_api.only_json_object("{broken"), "{broken")

    def test_fit_keeps_the_tail_of_a_long_prompt_and_stays_in_limits(self):
        system, user = farm_llm_api._fit("sys", "x" * 9000 + "TAIL")
        self.assertLessEqual(len(user), farm_llm_api.PROMPT_LIMIT)
        self.assertTrue(user.endswith("TAIL"))
        self.assertLessEqual(len(system), farm_llm_api.SYSTEM_LIMIT)

    def test_chat_endpoint_answers_in_the_openai_shape_and_adds_the_json_rule(self):
        seen = {}

        async def fake_ask(**kwargs):
            seen.update(kwargs)
            return 'Sure: {"title": "Knight"} bye'

        class Req:
            async def json(self):
                return {"messages": [{"role": "user", "content": "hi"}],
                        "response_format": {"type": "json_object"}, "max_tokens": 160}

        with mock.patch.object(farm_llm_api, "ask_farm", fake_ask):
            resp = asyncio.run(farm_llm_api.farm_llm_chat(Req()))
        self.assertEqual(resp["choices"][0]["message"]["content"], '{"title": "Knight"}')
        self.assertIn("valid JSON object", seen["system"])
        self.assertGreaterEqual(seen["max_tokens"], farm_llm_api.MIN_TOKENS)

    def test_farm_failure_is_an_openai_style_502(self):
        async def fake_ask(**kwargs):
            raise farm_llm_api.FarmUnavailable("no free ai-node")

        async def fake_or(*args, **kwargs):
            raise farm_llm_api.FarmUnavailable("no OpenRouter free credential")

        class Req:
            async def json(self):
                return {"messages": [{"role": "user", "content": "hi"}]}

        with mock.patch.object(farm_llm_api, "ask_farm", fake_ask),                 mock.patch.object(farm_llm_api, "ask_openrouter_free", fake_or):
            resp = asyncio.run(farm_llm_api.farm_llm_chat(Req()))
        self.assertEqual(resp.status_code, 502)
        self.assertIn(b"farm_unavailable", resp.body)


if __name__ == "__main__":
    unittest.main()
