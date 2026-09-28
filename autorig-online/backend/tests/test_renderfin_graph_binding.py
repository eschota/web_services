"""Renderfin keeps which graph node asked for a task and stands down what a
saved graph no longer wants (2026-09-28)."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from renderfin import config


class GraphBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            patch.object(config, "DATA_DIR", root),
            patch.object(config, "RENDER_DIR", root / "render"),
            patch.object(config, "DB_DIR", root / "db"),
            patch.object(config, "TMP_DIR", root / "tmp"),
            patch.object(config, "SERVERS_DIR", root / "servers"),
            patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
        ]
        for p in self.patches:
            p.start()
        from renderfin.app import app  # noqa: WPS433

        self.client = TestClient(app)
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def submit(self, node_id, signature="sig-1", session="tab:1", graph_id="graph000001", prompt="knight"):
        response = self.client.post("/renderfin/api-render", json={
            "prompt": prompt, "user_name": "smoke", "graph_id": graph_id, "node_id": node_id,
            "node_signature": signature, "submit_session": session})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["task_id"]

    def status(self, task_id):
        return self.client.get("/renderfin/api-render/tasks/" + task_id).json()

    def test_task_carries_its_graph_node(self):
        task_id = self.submit("b")
        row = self.status(task_id)
        self.assertEqual(row["graph_id_string"], "graph000001")
        self.assertEqual(row["node_id_string"], "b")
        self.assertEqual(row["status"], "Pending")

    def test_save_cancels_gone_changed_and_bypassed_nodes_only(self):
        kept = self.submit("b", "sig-b")
        changed = self.submit("c", "sig-c-old")
        gone = self.submit("d", "sig-d")
        unsigned = self.submit("e", "")  # an older editor: judged by presence only
        other_graph = self.submit("b", "sig-b", graph_id="graph000002")
        response = self.client.post("/renderfin/api-render/cancel-stale-graph", json={
            "graph_id": "graph000001", "wanted": {"b": "sig-b", "c": "sig-c-new", "e": "sig-e"}})
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()
        self.assertEqual(data["cancelled_int"], 2)
        self.assertEqual(data["kept_int"], 2)
        self.assertEqual({row["node_id_string"]: row["why_string"] for row in data["cancelled_array"]},
                         {"c": "node changed", "d": "node gone or bypassed"})
        self.assertEqual(self.status(kept)["status"], "Pending")
        self.assertEqual(self.status(unsigned)["status"], "Pending")
        self.assertEqual(self.status(other_graph)["status"], "Pending")
        self.assertEqual(self.status(changed)["status"], "Error")
        self.assertIn("no longer needs", self.status(changed)["error"])
        self.assertEqual(self.status(gone)["status"], "Error")

    def test_a_new_render_press_replaces_the_same_nodes_queued_job(self):
        first = self.submit("b", "sig-b", session="tab:1")
        same_press = self.submit("b", "sig-b", session="tab:1")  # X9 sibling: kept
        self.assertEqual(self.status(first)["status"], "Pending")
        second = self.submit("b", "sig-b", session="tab:2")
        self.assertEqual(self.status(first)["status"], "Error")
        self.assertEqual(self.status(same_press)["status"], "Error")
        self.assertIn("rendered again", self.status(first)["error"])
        self.assertEqual(self.status(second)["status"], "Pending")
        # A task with no session (not a graph node) never supersedes anything.
        self.submit("b", "sig-b", session="")
        self.assertEqual(self.status(second)["status"], "Pending")

    def test_graph_summary_and_cancel_graph(self):
        mine_1 = self.submit("b", session="tab:1")
        mine_2 = self.submit("c", session="tab:1")
        theirs = self.submit("b", graph_id="graph000002")
        summary = self.client.get("/renderfin/api-render/graph/graph000001").json()
        self.assertEqual((summary["queued_int"], summary["other_queued_int"], summary["total_queued_int"]), (2, 1, 3))
        self.assertEqual({row["id"] for row in summary["tasks_array"]}, {mine_1, mine_2})
        partial = self.client.post("/renderfin/api-render/cancel-graph",
                                   json={"graph_id": "graph000001", "task_ids": [mine_1]}).json()
        self.assertEqual(partial["cancelled_int"], 1)
        self.assertEqual(self.status(mine_2)["status"], "Pending")
        everything = self.client.post("/renderfin/api-render/cancel-graph", json={"graph_id": "graph000001"}).json()
        self.assertEqual(everything["cancelled_int"], 1)
        self.assertEqual(self.status(theirs)["status"], "Pending")
        self.assertEqual(self.client.get("/renderfin/api-render/graph/graph000001").json()["queued_int"], 0)

    def test_bad_requests(self):
        self.assertEqual(self.client.post("/renderfin/api-render/cancel-stale-graph", json={"graph_id": "x"}).status_code, 400)
        self.assertEqual(self.client.post("/renderfin/api-render/cancel-graph", json={}).status_code, 400)


if __name__ == "__main__":
    unittest.main()
