"""Graph <-> queue binding (2026-09-28): signatures, stale-job reconcile, archive."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph  # noqa: E402
import ai_graph_context  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _graph(prompt="a lighthouse", width=1024, bypass_c=False, quality="normal", extra_node=True):
    nodes = [
        {"id": "a", "kind": "input", "entity_type": "text", "value": prompt, "x": 1, "y": 2},
        {"id": "b", "kind": "service", "service": "image", "params": {"width": width, "_label": "pic", "_display_mode": "small"}},
    ]
    links = [{"from": "a", "output": "value", "to": "b", "input": "prompt"}]
    if extra_node:
        nodes.append({"id": "c", "kind": "service", "service": "video",
                      "params": {"frame_count": 97, "_disabled": True} if bypass_c else {"frame_count": 97}})
        links.append({"from": "b", "output": "image_url_string", "to": "c", "input": "image"})
    return {"name": "bound", "render_quality": quality, "nodes": nodes, "links": links}


class NodeSignatureTests(unittest.TestCase):
    def test_drawing_only_changes_keep_the_signature(self):
        base = ai_graph.node_signatures(_graph())
        moved = _graph()
        moved["nodes"][1]["x"] = 500
        moved["nodes"][1]["params"]["_label"] = "renamed"
        moved["nodes"][1]["params"]["_display_mode"] = "large"
        self.assertEqual(base, ai_graph.node_signatures(moved))

    def test_a_param_change_moves_the_node_and_everything_downstream(self):
        base = ai_graph.node_signatures(_graph())
        changed = ai_graph.node_signatures(_graph(width=512))
        self.assertNotEqual(base["b"], changed["b"])
        self.assertNotEqual(base["c"], changed["c"])

    def test_an_upstream_input_change_moves_downstream_nodes(self):
        base = ai_graph.node_signatures(_graph())
        changed = ai_graph.node_signatures(_graph(prompt="a castle"))
        self.assertNotEqual(base["b"], changed["b"])
        self.assertNotEqual(base["c"], changed["c"])

    def test_bypassed_nodes_are_not_wanted(self):
        wanted = ai_graph.node_signatures(_graph(bypass_c=True))
        self.assertIn("b", wanted)
        self.assertNotIn("c", wanted)

    def test_input_nodes_are_never_wanted_and_quality_matters(self):
        wanted = ai_graph.node_signatures(_graph())
        self.assertNotIn("a", wanted)
        self.assertNotEqual(wanted["b"], ai_graph.node_signatures(_graph(quality="preview"))["b"])

    def test_integral_floats_equal_ints(self):
        one = _graph()
        one["nodes"][1]["params"]["cfg"] = 1
        two = _graph()
        two["nodes"][1]["params"]["cfg"] = 1.0
        self.assertEqual(ai_graph.node_signatures(one)["b"], ai_graph.node_signatures(two)["b"])


    def test_runtime_params_the_editor_rewrites_do_not_move_the_signature(self):
        base = _graph()
        base["nodes"][1]["params"].update({"seed": 0, "checkpoint": "krea2_turbo.safetensors", "lora": "", "_size_auto": True, "height": 448, "frame_count": 25, "_frames_auto": True})
        run = _graph()
        run["nodes"][1]["params"].update({"seed": 1456845360, "checkpoint": "krea2_turbo.safetensors", "_size_auto": True,
                                          "width": 544, "height": 960, "frame_count": 97, "_frames_auto": True})
        self.assertEqual(ai_graph.node_signatures(base)["b"], ai_graph.node_signatures(run)["b"])
        manual = _graph()
        manual["nodes"][1]["params"].update({"_size_auto": False, "width": 544})
        self.assertNotEqual(ai_graph.node_signatures(base)["b"], ai_graph.node_signatures(manual)["b"])
        model = _graph()
        model["nodes"][1]["params"].update({"checkpoint": "other.safetensors"})
        self.assertNotEqual(ai_graph.node_signatures(run)["b"], ai_graph.node_signatures(model)["b"])


class StoredSignatureAndReconcileTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._patch = patch.object(ai_graph, "GRAPH_DIR", Path(self._dir.name))
        self._patch.start()
        app = FastAPI()
        app.include_router(ai_graph.router)
        self.client = TestClient(app)
        self.calls = []

        async def fake_reconcile(graph_id, body, *, why="saved"):
            self.calls.append((graph_id, why, ai_graph.node_signatures(body)))
            return {"cancelled_int": 0, "kept_int": 0, "reached_bool": True}

        self._reconcile = patch.object(ai_graph, "reconcile_graph_queue", fake_reconcile)
        self._reconcile.start()

    def tearDown(self):
        self._reconcile.stop()
        self._patch.stop()
        self._dir.cleanup()

    def test_submit_context_reads_the_stored_signature(self):
        created = self.client.post("/api/ai/graphs", json=_graph()).json()
        graph_id = created["graph_id_string"]
        expected = ai_graph.node_signatures(_graph())["c"]
        self.assertEqual(ai_graph.stored_node_signature(graph_id, "c"), expected)
        self.assertEqual(ai_graph.stored_node_signature(graph_id, "zz"), "")
        self.assertEqual(ai_graph.stored_node_signature("no-such-graph", "c"), "")
        ai_graph_context.set_from_headers({"x-graph-id": graph_id, "x-node-id": "c", "x-submit-session": "tab:1"})
        fields = ai_graph_context.fields()
        self.assertEqual(fields["graph_id"], graph_id)
        self.assertEqual(fields["node_id"], "c")
        self.assertEqual(fields["node_signature"], expected)
        self.assertEqual(fields["submit_session"], "tab:1")
        ai_graph_context.set_from_headers({})
        self.assertEqual(ai_graph_context.fields(), {})

    def test_every_save_reconciles_the_queue_with_the_saved_nodes(self):
        created = self.client.post("/api/ai/graphs", json=_graph()).json()
        graph_id = created["graph_id_string"]
        self.assertEqual(self.calls, [])  # a first save has no queue to reconcile
        headers = {"X-Graph-Revision": str(created["revision_int"])}
        updated = self.client.put("/api/ai/graphs/" + graph_id, json=_graph(bypass_c=True), headers=headers)
        self.assertEqual(updated.status_code, 200, updated.text)
        self.assertEqual(len(self.calls), 1)
        called_graph, why, wanted = self.calls[0]
        self.assertEqual(called_graph, graph_id)
        self.assertEqual(why, "saved")
        self.assertEqual(set(wanted), {"b"})
        self.assertIn("queue_cancelled_int", updated.json())

    def test_a_stale_tab_is_refused_and_does_not_reconcile(self):
        created = self.client.post("/api/ai/graphs", json=_graph()).json()
        graph_id = created["graph_id_string"]
        refused = self.client.put("/api/ai/graphs/" + graph_id, json=_graph(width=64),
                                  headers={"X-Graph-Revision": "999"})
        self.assertEqual(refused.status_code, 409)
        self.assertEqual(refused.json()["detail"]["error_string"], "graph_stale")
        self.assertEqual(self.calls, [])


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._patch = patch.object(ai_graph, "GRAPH_DIR", Path(self._dir.name))
        self._patch.start()

        async def admin(request):
            return request.headers.get("x-test-admin") == "1"

        self._admin = patch.object(ai_graph, "_caller_is_admin", admin)
        self._admin.start()
        app = FastAPI()
        app.include_router(ai_graph.router)
        self.client = TestClient(app)

    def tearDown(self):
        self._admin.stop()
        self._patch.stop()
        self._dir.cleanup()

    def test_archive_hides_restore_returns_and_links_keep_opening(self):
        graph_id = self.client.post("/api/ai/graphs", json=_graph()).json()["graph_id_string"]
        self.assertEqual(self.client.post(f"/api/ai/graphs/{graph_id}/archive").status_code, 403)
        archived = self.client.post(f"/api/ai/graphs/{graph_id}/archive?kept=abcd1234abcd&note=dup",
                                    headers={"X-Test-Admin": "1"})
        self.assertEqual(archived.status_code, 200, archived.text)
        self.assertEqual(archived.json()["kept_id_string"], "abcd1234abcd")
        rows = self.client.get("/api/ai/graphs").json()["graphs_array"]
        self.assertEqual([r["graph_id_string"] for r in rows], [])
        self.assertEqual(self.client.get("/api/ai/graphs/" + graph_id).status_code, 200)
        listing = self.client.get("/api/ai/graph-archive", headers={"X-Test-Admin": "1"}).json()
        self.assertEqual([r["id"] for r in listing["archived_array"]], [graph_id])
        self.assertEqual(listing["archived_array"][0]["name_string"], "bound")
        restored = self.client.post(f"/api/ai/graphs/{graph_id}/restore", headers={"X-Test-Admin": "1"})
        self.assertEqual(restored.status_code, 200, restored.text)
        rows = self.client.get("/api/ai/graphs").json()["graphs_array"]
        self.assertEqual([r["graph_id_string"] for r in rows], [graph_id])
        self.assertEqual(self.client.post(f"/api/ai/graphs/{graph_id}/restore", headers={"X-Test-Admin": "1"}).status_code, 404)

    def test_an_edit_restores_an_archived_graph(self):
        created = self.client.post("/api/ai/graphs", json=_graph()).json()
        graph_id = created["graph_id_string"]
        self.client.post(f"/api/ai/graphs/{graph_id}/archive", headers={"X-Test-Admin": "1"})
        with patch.object(ai_graph, "reconcile_graph_queue") as reconcile:
            async def fake(*_a, **_k):
                return {"cancelled_int": 0, "kept_int": 0, "reached_bool": True}
            reconcile.side_effect = fake
            edited = self.client.put("/api/ai/graphs/" + graph_id, json=_graph(width=640),
                                     headers={"X-Graph-Revision": str(created["revision_int"])})
        self.assertEqual(edited.status_code, 200, edited.text)
        self.assertTrue((Path(self._dir.name) / f"{graph_id}.json").exists())
        self.assertFalse((Path(self._dir.name) / "archive" / f"{graph_id}.json").exists())
        manifest = json.loads((Path(self._dir.name) / "archive" / "archived.json").read_text())
        self.assertEqual(manifest, [])

    def test_an_aliased_graph_is_never_archived(self):
        graph_id = self.client.post("/api/ai/graphs", json=_graph()).json()["graph_id_string"]
        (Path(self._dir.name) / "aliases.json").write_text(json.dumps({"keep-me": graph_id}))
        refused = self.client.post(f"/api/ai/graphs/{graph_id}/archive", headers={"X-Test-Admin": "1"})
        self.assertEqual(refused.status_code, 409)


if __name__ == "__main__":
    unittest.main()
