"""A cached or replayed answer never names a task the farm has cancelled (2026-09-28)."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_request_cache  # noqa: E402
import task_owner  # noqa: E402
from ai_request_cache import AIRequestCache  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


class CacheDeadTaskTests(unittest.TestCase):
    def test_a_cancelled_task_is_a_miss_and_a_live_one_a_hit(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                cache = AIRequestCache(Path(tmp) / "cache.sqlite3")
                counter = {"n": 0}

                async def submit():
                    counter["n"] += 1
                    return {"task_id_string": f"task-{counter['n']}", "status_string": "pending"}

                payload = {"prompt": "cube", "seed": 5}
                first = await cache.run_cached("image", payload, submit)
                self.assertEqual(first["task_id_string"], "task-1")
                alive = {"value": True}

                async def fake_alive(task_id):
                    return alive["value"]

                with patch.object(ai_request_cache, "task_alive", fake_alive):
                    hit = await cache.run_cached("image", payload, submit)
                    self.assertTrue(hit["cache_hit_bool"])
                    self.assertEqual(hit["task_id_string"], "task-1")
                    alive["value"] = False  # a restart wipe cancelled it, nobody polled
                    fresh = await cache.run_cached("image", payload, submit)
                    self.assertFalse(fresh["cache_hit_bool"])
                    self.assertEqual(fresh["task_id_string"], "task-2")
                    alive["value"] = None  # farm unreachable: keep the hit, never double-submit
                    kept = await cache.run_cached("image", payload, submit)
                    self.assertEqual(kept["task_id_string"], "task-2")
                    # a completed answer is never re-checked
                    cache.note_result("task-2", "completed", {"output_url_string": "https://x/2.png"})
                    alive["value"] = False
                    done = await cache.run_cached("image", payload, submit)
                    self.assertEqual(done["task_id_string"], "task-2")
                    self.assertEqual(counter["n"], 2)

        asyncio.run(scenario())


class IdempotentReplayTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._patches = [patch.object(task_owner, "DB_PATH", Path(self._dir.name) / "owners.sqlite3"),
                         patch.object(task_owner, "_conn", None),
                         patch.object(task_owner, "current_build", lambda: "")]
        for p in self._patches:
            p.start()
        app = FastAPI()
        counter = {"n": 0}

        @app.post("/api/render-me")
        async def render_me():
            counter["n"] += 1
            return {"success_bool": True, "task_id_string": f"task-{counter['n']}"}

        app.add_middleware(task_owner.TaskOwnerMiddleware)
        self.counter = counter
        self.client = TestClient(app)

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self._dir.cleanup()

    def test_replay_of_a_dead_task_submits_afresh(self):
        headers = {"X-Client-Request-Id": "req-1"}
        alive = {"value": True}

        async def fake_alive(task_id):
            return alive["value"]

        with patch.object(ai_request_cache, "task_alive", fake_alive):
            first = self.client.post("/api/render-me", json={}, headers=headers).json()
            self.assertEqual(first["task_id_string"], "task-1")
            replay = self.client.post("/api/render-me", json={}, headers=headers)
            self.assertEqual(replay.headers.get("x-idempotent-replay"), "1")
            self.assertEqual(replay.json()["task_id_string"], "task-1")
            alive["value"] = False
            fresh = self.client.post("/api/render-me", json={}, headers=headers)
            self.assertIsNone(fresh.headers.get("x-idempotent-replay"))
            self.assertEqual(fresh.json()["task_id_string"], "task-2")
            self.assertEqual(self.counter["n"], 2)


if __name__ == "__main__":
    unittest.main()
