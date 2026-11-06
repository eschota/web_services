"""AI_MODEL_EXTRA: a second farm model served only when a request names it (A/B of Qwen3.5-4B, 2026-11-06)."""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_vision_api as api  # noqa: E402
from fastapi import HTTPException  # noqa: E402

NINE = "qwen35-9b-uncensored"
FOUR = "qwen35-4b"


class ResolveModelTests(unittest.TestCase):
    def test_default_is_unchanged_single_model_wins_over_any_name(self):
        for named in (None, "", "bonsai2-27b", FOUR, "gpt-nonexistent"):
            self.assertEqual(api._resolve_model_id(named, NINE, [], NINE), NINE, named)

    def test_extra_model_is_served_only_when_named(self):
        extra = [FOUR]
        self.assertEqual(api._resolve_model_id(FOUR, NINE, extra, NINE), FOUR)
        self.assertEqual(api._resolve_model_id(" Qwen35-4B ", NINE, extra, NINE), FOUR)
        self.assertEqual(api._resolve_model_id(None, NINE, extra, NINE), NINE)
        self.assertEqual(api._resolve_model_id("bonsai2-27b", NINE, extra, NINE), NINE)

    def test_without_single_the_named_model_or_default(self):
        self.assertEqual(api._resolve_model_id("bonsai2-27b", "", [], "bonsai2-27b"), "bonsai2-27b")
        self.assertEqual(api._resolve_model_id(None, "", [], "bonsai2-27b"), "bonsai2-27b")
        self.assertEqual(api._resolve_model_id(FOUR, "", [FOUR], "bonsai2-27b"), FOUR)

    def test_parse_model_list(self):
        self.assertEqual(api._parse_model_list(" qwen35-4B, ;qwen35-4b,,x "), [FOUR, "x"])
        self.assertEqual(api._parse_model_list(""), [])


class CatalogueTests(unittest.TestCase):
    BASE = [{"id": NINE}, {"id": "bonsai2-27b"}]

    def test_optional_models_hidden_by_default(self):
        cat = api._enabled_catalogue(self.BASE, api.OPTIONAL_AI_MODELS, [])
        self.assertEqual([m["id"] for m in cat], [NINE, "bonsai2-27b"])

    def test_enabled_optional_model_is_listed_once(self):
        cat = api._enabled_catalogue(self.BASE, api.OPTIONAL_AI_MODELS, [FOUR])
        self.assertEqual([m["id"] for m in cat], [NINE, "bonsai2-27b", FOUR])
        cat = api._enabled_catalogue(cat, api.OPTIONAL_AI_MODELS, [FOUR])
        self.assertEqual([m["id"] for m in cat].count(FOUR), 1)

    def test_live_module_default_has_no_4b(self):
        # The test run has no AI_MODEL_EXTRA: the public catalogue must not grow.
        if api.EXTRA_MODEL_IDS:
            self.skipTest("AI_MODEL_EXTRA is set in this environment")
        self.assertNotIn(FOUR, [m["id"] for m in api.AI_MODELS])
        try:
            served = api._model_entry(FOUR)["id"]       # AI_MODEL_SINGLE set: its model answers
        except HTTPException as exc:                     # unset: unknown model, as before
            served = exc.detail["error_string"]
        self.assertNotEqual(served, FOUR)

    def test_4b_entry_shape(self):
        entry = next(m for m in api.OPTIONAL_AI_MODELS if m["id"] == FOUR)
        for key in ("modes", "context_tokens", "max_output_tokens", "default_output_tokens", "reasons_first"):
            self.assertIn(key, entry)
        self.assertIn("vision", entry["modes"])
        self.assertFalse(entry["default"])


class ModelEntryWithExtraTests(unittest.TestCase):
    def setUp(self):
        four = next(m for m in api.OPTIONAL_AI_MODELS if m["id"] == FOUR)
        nine = next(m for m in api.AI_MODELS if m["id"] == NINE)
        self.patches = [
            mock.patch.object(api, "SINGLE_MODEL_ID", NINE),
            mock.patch.object(api, "DEFAULT_MODEL_ID", NINE),
            mock.patch.object(api, "EXTRA_MODEL_IDS", [FOUR]),
            mock.patch.object(api, "AI_MODELS", [nine, four]),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def test_named_4b_gets_the_4b_profile(self):
        self.assertEqual(api._model_entry(FOUR)["id"], FOUR)

    def test_everything_else_still_gets_the_single_model(self):
        for named in (None, NINE, "bonsai2-27b", "gpt-nonexistent"):
            self.assertEqual(api._model_entry(named)["id"], NINE, named)

    def test_pick_worker_sends_4b_only_to_a_node_that_lists_it(self):
        nodes = [
            {"name": "f11-ai", "physical_node": "f11-ai", "url": "http://f11.test", "token": "t"},
            {"name": "f13-ai", "physical_node": "f13-ai", "url": "http://f13.test", "token": "t"},
        ]
        catalogue = {
            "f11-ai": {"load": 0, "models": [NINE], "loaded": NINE},
            "f13-ai": {"load": 2, "models": [NINE, FOUR], "loaded": NINE},
        }

        async def probe(client, worker):
            return True, dict(catalogue[worker["physical_node"]], activities=[])

        async def run(model):
            with mock.patch.object(api, "_load_ai_workers", return_value=nodes), \
                 mock.patch.object(api, "_probe_for_ai", probe), \
                 mock.patch.object(api, "_pinned_keys", return_value=set()):
                return await api._pick_worker(None, model)

        self.assertEqual(asyncio.run(run(FOUR))["physical_node"], "f13-ai")
        self.assertEqual(asyncio.run(run(NINE))["physical_node"], "f11-ai")   # idle node first

    def test_pick_worker_refuses_4b_when_no_node_serves_it(self):
        nodes = [{"name": "f11-ai", "physical_node": "f11-ai", "url": "http://f11.test", "token": "t"}]

        async def probe(client, worker):
            return True, {"load": 0, "models": [NINE], "loaded": NINE, "activities": []}

        async def run():
            with mock.patch.object(api, "_load_ai_workers", return_value=nodes), \
                 mock.patch.object(api, "_probe_for_ai", probe), \
                 mock.patch.object(api, "_pinned_keys", return_value=set()):
                return await api._pick_worker(None, FOUR)

        with self.assertRaises(HTTPException) as caught:
            asyncio.run(run())
        self.assertEqual(caught.exception.detail["error_string"], "model_unavailable")


if __name__ == "__main__":
    unittest.main()
