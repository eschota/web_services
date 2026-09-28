"""A result the store cannot hold never turns a save into a 422 (owner bug 2026-09-28)."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _graph(results=None):
    return {"name": "lenient", "nodes": [
        {"id": "a", "kind": "input", "entity_type": "text", "value": "hi"},
        {"id": "b", "kind": "service", "service": "image", "params": {}},
        {"id": "c", "kind": "service", "service": "video", "params": {}},
    ], "links": [{"from": "a", "output": "value", "to": "b", "input": "prompt"}],
        "results": results or {}}


class LenientResultTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._patch = patch.object(ai_graph, "GRAPH_DIR", Path(self._dir.name))
        self._patch.start()

        async def quiet(*_a, **_k):
            return {"cancelled_int": 0, "kept_int": 0, "reached_bool": True}

        self._reconcile = patch.object(ai_graph, "reconcile_graph_queue", quiet)
        self._reconcile.start()
        app = FastAPI()
        app.include_router(ai_graph.router)
        self.client = TestClient(app)

    def tearDown(self):
        self._reconcile.stop()
        self._patch.stop()
        self._dir.cleanup()

    def test_oversized_and_broken_results_are_trimmed_or_dropped_not_refused(self):
        big = {"status": "done", "type": "image", "value": "https://x/y.png",
               "items": [{"value": f"https://x/{i}.png"} for i in range(700)]}
        broken = {"status": "done", "type": "image", "value": "https://x/z.png",
                  "history": [{"type": "image", "value": "data:image/png;base64,AAA"},
                              {"type": "image", "value": "https://x/old.png"}]}
        hopeless = {"status": "done", "type": "image", "value": "https://x/w.png", "pick": "not-a-number"}
        response = self.client.post("/api/ai/graphs", json=_graph({"b": big, "c": broken, "a": hopeless}))
        self.assertEqual(response.status_code, 200, response.text)
        graph_id = response.json()["graph_id_string"]
        stored = self.client.get("/api/ai/graphs/" + graph_id).json()["graph_object"]["results"]
        self.assertEqual(len(stored["b"]["items"]), 512)
        self.assertEqual([h["value"] for h in stored["c"]["history"]], ["https://x/old.png"])
        self.assertNotIn("a", stored)
        edited = self.client.put("/api/ai/graphs/" + graph_id, json=_graph({"b": hopeless}),
                                 headers={"X-Graph-Revision": str(response.json()["revision_int"])})
        self.assertEqual(edited.status_code, 200, edited.text)
        results = self.client.put(f"/api/ai/graphs/{graph_id}/results", json={"b": hopeless, "c": broken})
        self.assertEqual(results.status_code, 200, results.text)
        self.assertEqual(results.json()["dropped_array"], ["b"])


if __name__ == "__main__":
    unittest.main()
