"""The task-owner middleware records which graph node asked for a task."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph_context  # noqa: E402
import task_owner  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


class GraphLinkTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._patches = [patch.object(task_owner, "DB_PATH", Path(self._dir.name) / "owners.sqlite3"),
                         patch.object(task_owner, "_conn", None)]
        for p in self._patches:
            p.start()
        app = FastAPI()
        seen = {}

        @app.post("/api/render-me")
        async def render_me():
            seen["context"] = ai_graph_context.current()
            return {"success_bool": True, "task_id_string": "task-1"}

        app.add_middleware(task_owner.TaskOwnerMiddleware)
        self.seen = seen
        self.client = TestClient(app)

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self._dir.cleanup()

    def test_headers_reach_the_route_and_the_link_is_stored(self):
        response = self.client.post("/api/render-me", json={}, headers={
            "X-Graph-Id": "abcdef123456", "X-Node-Id": "n7", "X-Submit-Session": "tab:3"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.seen["context"], {"graph_id": "abcdef123456", "node_id": "n7", "submit_session": "tab:3"})
        self.assertEqual(task_owner.graph_of("task-1"),
                         {"graph_id": "abcdef123456", "node_id": "n7", "session": "tab:3"})
        rows = task_owner.tasks_of_graph("abcdef123456")
        self.assertEqual([(r[0], r[1], r[2]) for r in rows], [("task-1", "n7", "tab:3")])

    def test_without_headers_nothing_is_linked(self):
        self.client.post("/api/render-me", json={})
        self.assertEqual(self.seen["context"], {})
        self.assertEqual(task_owner.graph_of("task-1"), {})

    def test_odd_header_values_are_ignored(self):
        self.client.post("/api/render-me", json={}, headers={"X-Graph-Id": "../etc", "X-Node-Id": "n7"})
        self.assertEqual(self.seen["context"], {"node_id": "n7"})
        self.assertEqual(task_owner.graph_of("task-1"), {})


if __name__ == "__main__":
    unittest.main()
