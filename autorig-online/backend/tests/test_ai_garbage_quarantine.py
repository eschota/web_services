"""Garbage answers ("????", empty) fail as garbage_answer and quarantine the node (2026-11-07, f2-ai)."""

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

NINE = "qwen35-9b-uncensored"
GOOD = '{"keywords": ["valve", "flow", "meter"], "title": "Valves", "description": "Red valves."}'


class Clean(unittest.TestCase):
    def setUp(self):
        api._AI_GARBAGE_AT.clear()
        api._AI_GARBAGE_TASKS.clear()
        api._AI_QUARANTINED_UNTIL.clear()

    tearDown = setUp


class GarbageDetectionTests(Clean):
    def test_garbage_shapes(self):
        for text in ("", "   \n", "?" * 600, "? ? ? ?", "{}{}{}{}{}{}{}{}", "!!!!!!!!!a"):
            self.assertTrue(api._garbage_answer(text), repr(text[:20]))

    def test_real_answers_are_not_garbage(self):
        for text in (GOOD, "4", "Yes.", "Кошка на диване", '["a", "b", "c"]', "a?" * 50):
            self.assertFalse(api._garbage_answer(text), repr(text[:20]))

    def test_status_turns_garbage_into_a_failed_garbage_answer(self):
        result = api._public_status("f2-ai.t1", NINE, {"status": "Completed", "answer": "?" * 700})
        self.assertFalse(result["success_bool"])
        self.assertEqual(result["status_string"], "failed")
        self.assertTrue(result["error_string"].startswith("garbage_answer"))
        self.assertEqual(result["error_code_string"], "garbage_answer")

    def test_empty_completed_answer_is_garbage(self):
        result = api._public_status("f2-ai.t2", NINE, {"status": "Completed", "answer": ""})
        self.assertEqual(result["error_code_string"], "garbage_answer")

    def test_good_answer_and_unfinished_task_untouched(self):
        ok = api._public_status("f2-ai.t3", NINE, {"status": "Completed", "answer": GOOD})
        self.assertTrue(ok["success_bool"])
        self.assertNotIn("error_code_string", ok)
        pending = api._public_status("f2-ai.t4", NINE, {"status": "Processing", "answer": ""})
        self.assertTrue(pending["success_bool"])
        self.assertNotIn("error_code_string", pending)
        self.assertEqual(api._AI_GARBAGE_AT, {})


class QuarantineTests(Clean):
    def test_two_garbage_answers_within_window_quarantine_the_node(self):
        with mock.patch.object(api, "AI_GARBAGE_QUARANTINE_SECONDS", 900.0):
            self.assertFalse(api._note_garbage("f2-ai", "f2-ai.a", now=1000.0))
            self.assertEqual(api.quarantined_until("f2-ai", now=1000.0), 0.0)
            self.assertTrue(api._note_garbage("f2-ai", "f2-ai.b", now=1300.0))
            self.assertEqual(api.quarantined_until("f2-ai", now=1301.0), 2200.0)
            self.assertEqual(api.quarantined_until("f2-ai", now=2201.0), 0.0)

    def test_answers_far_apart_or_repeated_polls_do_not_quarantine(self):
        self.assertFalse(api._note_garbage("f2-ai", "f2-ai.a", now=1000.0))
        self.assertFalse(api._note_garbage("f2-ai", "f2-ai.a", now=1001.0))   # same task polled again
        self.assertFalse(api._note_garbage("f2-ai", "f2-ai.b", now=1000.0 + 601))
        self.assertEqual(api.quarantined_until("f2-ai", now=1700.0), 0.0)

    def test_status_poll_path_counts_per_task_and_logs(self):
        with self.assertLogs(api.logger, level="WARNING") as logs:
            for task in ("f2-ai.x", "f2-ai.x", "f2-ai.y"):
                api._public_status(task, NINE, {"status": "Completed", "answer": "?" * 300})
        self.assertGreater(api.quarantined_until("f2-ai"), 0)
        self.assertTrue(any("Node f2-ai quarantined: garbage answers" in line for line in logs.output))

    def _pick(self, quarantined):
        nodes = [
            {"name": "f2-ai", "physical_node": "f2-ai", "url": "http://f2.test", "token": "t"},
            {"name": "f13-ai", "physical_node": "f13-ai", "url": "http://f13.test", "token": "t"},
        ]
        load = {"f2-ai": 0, "f13-ai": 3}

        async def probe(client, worker):
            return True, {"load": load[worker["physical_node"]], "models": [NINE], "loaded": NINE,
                          "activities": []}

        async def run():
            with mock.patch.object(api, "_load_ai_workers", return_value=nodes), \
                 mock.patch.object(api, "_probe_for_ai", probe), \
                 mock.patch.object(api, "_pinned_keys", return_value=set()):
                return await api._pick_worker(None, NINE)

        api._AI_QUARANTINED_UNTIL.clear()
        for key in quarantined:
            api._AI_QUARANTINED_UNTIL[key] = 10 ** 12
        return asyncio.run(run())["physical_node"]

    def test_pick_worker_skips_a_quarantined_node(self):
        self.assertEqual(self._pick([]), "f2-ai")             # idle node first
        self.assertEqual(self._pick(["f2-ai"]), "f13-ai")     # even though f13 is busier

    def test_pick_worker_falls_back_when_every_node_is_quarantined(self):
        self.assertEqual(self._pick(["f2-ai", "f13-ai"]), "f2-ai")

    def test_quarantine_asks_the_node_to_reload_once(self):
        calls = []

        async def fake_reload(key):
            calls.append(key)

        async def run():
            with mock.patch.object(api, "_ask_node_to_reload", fake_reload):
                for task in ("f2-ai.r1", "f2-ai.r2", "f2-ai.r3"):
                    api._public_status(task, NINE, {"status": "Completed", "answer": "?" * 300})
                await asyncio.sleep(0)

        asyncio.run(run())
        self.assertEqual(calls, ["f2-ai"])


class FleetFieldTests(Clean):
    def test_fleet_ai_nodes_carry_quarantined_until(self):
        import ai_fleet

        workers = [{"name": "f2-ai", "physical_node": "f2-ai", "url": "http://f2.test", "token": "t"},
                   {"name": "f13-ai", "physical_node": "f13-ai", "url": "http://f13.test", "token": "t"}]

        async def free(client, worker):
            if worker["physical_node"] == "f13-ai":
                raise RuntimeError("down")
            return True, {"load": 0, "models": [NINE], "loaded": NINE, "activities": []}

        api._AI_QUARANTINED_UNTIL["f2-ai"] = 4_000_000_000.0
        fn = getattr(ai_fleet, "_converter_snapshot")
        with mock.patch.object(api, "_load_ai_workers", return_value=workers), \
             mock.patch.object(api, "_node_is_free", free):
            snap = asyncio.run(fn(None))
        by_id = {n["id"]: n for n in snap["nodes"]}
        self.assertEqual(by_id["f2-ai"]["quarantined_until"], 4_000_000_000)
        self.assertEqual(by_id["f13-ai"]["quarantined_until"], 0)


if __name__ == "__main__":
    unittest.main()
