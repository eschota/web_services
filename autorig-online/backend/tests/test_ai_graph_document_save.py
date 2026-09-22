"""Saving a graph edits one document; only an explicit duplicate makes a copy."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(ai_graph.router)
    return TestClient(app)


def _payload(width=1024, extra=False):
    nodes = [
        {"id": "a", "kind": "input", "entity_type": "text", "value": "a lighthouse"},
        {"id": "b", "kind": "service", "service": "image", "params": {"width": width}},
    ]
    links = [{"from": "a", "output": "value", "to": "b", "input": "prompt"}]
    if extra:
        nodes.append({"id": "c", "kind": "service", "service": "video", "params": {}})
        links.append({"from": "b", "output": "image_url_string", "to": "c", "input": "image"})
    return {"name": "evolving", "nodes": nodes, "links": links}


class DocumentSaveTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._previous = ai_graph.GRAPH_DIR
        ai_graph.GRAPH_DIR = Path(self._dir.name)
        self.client = _client()

    def tearDown(self):
        ai_graph.GRAPH_DIR = self._previous
        self._dir.cleanup()

    def _files(self):
        return sorted(path.stem for path in Path(self._dir.name).glob("*.json"))

    def test_editing_and_saving_keeps_one_library_entry(self):
        first = self.client.post("/api/ai/graphs", json=_payload()).json()
        graph_id = first["graph_id_string"]
        for payload in (_payload(width=768), _payload(width=768, extra=True)):
            response = self.client.put(f"/api/ai/graphs/{graph_id}", json=payload)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["graph_id_string"], graph_id)
            self.assertEqual(response.json()["deep_link_string"], f"/nodes?g={graph_id}")
        self.assertEqual(self._files(), [graph_id])
        loaded = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]
        self.assertEqual(len(loaded["nodes"]), 3)
        library = self.client.get("/api/ai/graphs").json()
        self.assertEqual(library["total_int"], 1)
        self.assertEqual(library["graphs_array"][0]["node_count_int"], 3)
        self.assertGreater(library["graphs_array"][0]["updated_at_unix_int"], 0)

    def test_update_keeps_the_creation_time(self):
        first = self.client.post("/api/ai/graphs", json=_payload()).json()
        path = Path(self._dir.name) / f"{first['graph_id_string']}.json"
        stored = json.loads(path.read_text(encoding="utf-8"))
        stored["saved_at_unix_int"] = 1000
        path.write_text(json.dumps(stored), encoding="utf-8")
        self.client.put(f"/api/ai/graphs/{first['graph_id_string']}", json=_payload(width=512))
        stored = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(stored["saved_at_unix_int"], 1000)
        self.assertGreater(stored["updated_at_unix_int"], 1000)

    def test_a_legacy_content_addressed_link_continues_under_its_own_id(self):
        legacy = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        edited = self.client.put(f"/api/ai/graphs/{legacy}", json=_payload(extra=True)).json()
        self.assertEqual(edited["graph_id_string"], legacy)
        self.assertEqual(self._files(), [legacy])

    def test_posting_the_original_content_again_never_overwrites_an_edited_document(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        self.client.put(f"/api/ai/graphs/{graph_id}", json=_payload(extra=True))
        again = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        self.assertNotEqual(again, graph_id)
        kept = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]
        self.assertEqual(len(kept["nodes"]), 3)

    def test_updating_a_graph_nobody_saved_is_refused(self):
        response = self.client.put("/api/ai/graphs/nothinghere", json=_payload())
        self.assertEqual(response.status_code, 404)
        self.assertEqual(self._files(), [])

    def test_a_template_cannot_be_overwritten(self):
        response = self.client.put("/api/ai/graphs/SimpleTextToVideoByVision", json=_payload())
        self.assertEqual(response.status_code, 409)

    def test_an_invalid_graph_is_not_written(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        bad = _payload()
        bad["links"] = [{"from": "a", "output": "value", "to": "b", "input": "image"}]
        response = self.client.put(f"/api/ai/graphs/{graph_id}", json=bad)
        self.assertEqual(response.status_code, 400)
        kept = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]
        self.assertEqual(kept["links"][0]["input"], "prompt")

    def test_results_another_tab_finished_survive_a_structural_save(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        self.client.put(f"/api/ai/graphs/{graph_id}/results",
                        json={"b": {"status": "done", "type": "image",
                                    "value": "https://x/other-tab.png"}})
        moved = _payload(extra=True)
        moved["nodes"][1]["x"] = 400
        self.client.put(f"/api/ai/graphs/{graph_id}", json=moved)
        results = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]["results"]
        self.assertEqual(results["b"]["value"], "https://x/other-tab.png")

    def test_a_stored_result_is_not_carried_onto_a_different_node(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        self.client.put(f"/api/ai/graphs/{graph_id}/results",
                        json={"b": {"status": "done", "type": "image",
                                    "value": "https://x/old.png"}})
        # Same id, different settings: the old picture no longer describes it.
        self.client.put(f"/api/ai/graphs/{graph_id}", json=_payload(width=512))
        results = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]["results"]
        self.assertNotIn("b", results)

    def test_incoming_results_win_over_stored_ones(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        self.client.put(f"/api/ai/graphs/{graph_id}/results",
                        json={"b": {"status": "done", "type": "image", "value": "https://x/a.png"}})
        body = _payload()
        body["results"] = {"b": {"status": "done", "type": "image", "value": "https://x/b.png"}}
        self.client.put(f"/api/ai/graphs/{graph_id}", json=body)
        results = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]["results"]
        self.assertEqual(results["b"]["value"], "https://x/b.png")

    def test_duplicate_still_makes_a_separate_entry(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        copy = self.client.post("/api/ai/graphs/duplicate",
                                json={"currentGraph": _payload()}).json()["graph_id_string"]
        self.assertNotEqual(copy, graph_id)
        self.assertEqual(len(self._files()), 2)

    def test_an_archived_graph_still_opens_but_leaves_the_library(self):
        graph_id = self.client.post("/api/ai/graphs", json=_payload()).json()["graph_id_string"]
        archive = Path(self._dir.name) / ai_graph.ARCHIVE_SUBDIR
        archive.mkdir()
        (Path(self._dir.name) / f"{graph_id}.json").rename(archive / f"{graph_id}.json")
        self.assertEqual(self.client.get("/api/ai/graphs").json()["total_int"], 0)
        loaded = self.client.get(f"/api/ai/graphs/{graph_id}")
        self.assertEqual(loaded.status_code, 200)
        results = self.client.put(f"/api/ai/graphs/{graph_id}/results",
                                  json={"b": {"status": "done", "type": "image",
                                              "value": "https://x/y.png"}})
        self.assertEqual(results.status_code, 200)


if __name__ == "__main__":
    unittest.main()
