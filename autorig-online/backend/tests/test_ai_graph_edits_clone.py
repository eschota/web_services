"""clone_nodes: copy a node set per variant, the way the editor's paste does."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph  # noqa: E402
import ai_graph_edits  # noqa: E402
import ai_model_catalogue  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _payload():
    return {
        "name": "chain",
        "nodes": [
            {"id": "pic", "kind": "input", "entity_type": "image",
             "value": "https://autorig.online/dev/api/scratch/pic.png", "x": 0, "y": 0},
            {"id": "see", "kind": "service", "service": "vision",
             "params": {"prompt": "Describe"}, "x": 300, "y": 0},
            {"id": "write", "kind": "service", "service": "text",
             "params": {"prompt": "Write a calm prompt"}, "x": 600, "y": 0},
            {"id": "clip", "kind": "service", "service": "video",
             "params": {"frame_count": 97, "width": 540, "height": 960}, "x": 900, "y": 40},
            {"id": "frame", "kind": "service", "service": "video_frame", "x": 1200, "y": 0},
        ],
        "links": [
            {"from": "pic", "output": "value", "to": "see", "input": "image"},
            {"from": "see", "output": "answer_string", "to": "write", "input": "input"},
            {"from": "write", "output": "answer_string", "to": "clip", "input": "prompt"},
            {"from": "pic", "output": "value", "to": "clip", "input": "image"},
            {"from": "clip", "output": "video_url_string", "to": "frame", "input": "video_url"},
        ],
        "results": {
            "write": {"status": "done", "type": "text", "value": "a calm prompt"},
            "clip": {"status": "done", "type": "video", "value": "https://autorig.online/x.mp4"},
        },
    }


def _links(graph):
    return {(link.from_node, link.output, link.to_node, link.input) for link in graph.links}


class CloneNodesTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(ai_model_catalogue, "entries", return_value=[])
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_chain_is_copied_per_variant_with_remapped_and_incoming_links(self):
        graph = ai_graph.Graph(**_payload())
        edited, summary, invalidated = ai_graph_edits.apply_operations(graph, [{
            "op": "clone_nodes", "ids": ["write", "clip"], "variants": [
                {"write": {"prompt": "She waves"}, "clip": {"frame_count": 49}},
                {"clip": {"frame_count": 145, "lora_strength": 0.8}},
                {},
            ]}])
        self.assertEqual(summary["cloned_node_ids_array"],
                         ["clip_1", "clip_2", "clip_3", "write_1", "write_2", "write_3"])
        self.assertEqual(summary["added_node_ids_array"], summary["cloned_node_ids_array"])
        self.assertEqual(invalidated, summary["cloned_node_ids_array"])
        by_id = {node.id: node for node in edited.nodes}
        self.assertEqual(len(edited.nodes), 11)
        self.assertEqual(by_id["write_1"].params["prompt"], "She waves")
        self.assertEqual(by_id["write_2"].params["prompt"], "Write a calm prompt")
        self.assertEqual(by_id["clip_1"].params["frame_count"], 49)
        self.assertEqual(by_id["clip_2"].params["lora_strength"], 0.8)
        self.assertEqual(by_id["clip_3"].params["frame_count"], 97)
        links = _links(edited)
        for n in ("1", "2", "3"):
            # Internal link remapped, external incoming links duplicated.
            self.assertIn(("write_" + n, "answer_string", "clip_" + n, "prompt"), links)
            self.assertIn(("see", "answer_string", "write_" + n, "input"), links)
            self.assertIn(("pic", "value", "clip_" + n, "image"), links)
            # An outgoing link is never copied: the original frame node keeps its input.
            self.assertNotIn(("clip_" + n, "video_url_string", "frame", "video_url"), links)
        self.assertEqual(sum(1 for link in edited.links if link.to_node == "frame"), 1)
        # Copies are stacked below the originals and carry no results.
        self.assertEqual(by_id["clip_1"].y, 40 + (40 - 0) + 320)
        self.assertEqual(by_id["write_3"].y, 3 * ((40 - 0) + 320))
        self.assertEqual(by_id["write_1"].x, 600)
        self.assertEqual(set(edited.results), {"write", "clip"})
        # The original graph is untouched.
        self.assertEqual(len(graph.nodes), 5)

    def test_count_makes_plain_copies_and_ids_skip_taken_names(self):
        payload = _payload()
        payload["nodes"].append({"id": "clip_1", "kind": "service", "service": "video",
                                 "params": {}, "x": 0, "y": 0})
        graph = ai_graph.Graph(**payload)
        edited, summary, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "clone_nodes", "ids": ["clip"], "count": 2, "dx": 250, "dy": 0}])
        self.assertEqual(summary["cloned_node_ids_array"], ["clip_2", "clip_3"])
        by_id = {node.id: node for node in edited.nodes}
        self.assertEqual((by_id["clip_2"].x, by_id["clip_2"].y), (900 + 250, 40))
        self.assertEqual((by_id["clip_3"].x, by_id["clip_3"].y), (900 + 500, 40))
        self.assertEqual(by_id["clip_3"].params, {"frame_count": 97, "width": 540, "height": 960})

    def test_params_wrapper_is_unwrapped(self):
        graph = ai_graph.Graph(**_payload())
        edited, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "clone_nodes", "ids": ["clip"],
             "variants": [{"clip": {"params": {"frame_count": 57}}}]}])
        by_id = {node.id: node for node in edited.nodes}
        self.assertEqual(by_id["clip_1"].params["frame_count"], 57)
        self.assertNotIn("params", by_id["clip_1"].params)

    def test_input_node_value_override(self):
        graph = ai_graph.Graph(**_payload())
        edited, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "clone_nodes", "ids": ["pic"], "variants": [{"pic": {"value": ""}}]}])
        by_id = {node.id: node for node in edited.nodes}
        self.assertEqual(by_id["pic_1"].value, "")
        self.assertEqual(by_id["pic_1"].entity_type, "image")

    def test_overrides_obey_declarations_and_atomic_rejection(self):
        graph = ai_graph.Graph(**_payload())
        with self.assertRaises(HTTPException) as unknown:
            ai_graph_edits.apply_operations(graph, [
                {"op": "clone_nodes", "ids": ["clip"], "variants": [{"clip": {"bogus": 1}}]}])
        self.assertEqual(unknown.exception.detail["error_string"], "unknown_parameter")
        self.assertEqual(unknown.exception.detail["operation_index_int"], 0)
        with self.assertRaises(HTTPException) as bounds:
            ai_graph_edits.apply_operations(graph, [
                {"op": "clone_nodes", "ids": ["clip"], "variants": [{"clip": {"frame_count": 9000}}]}])
        self.assertEqual(bounds.exception.detail["error_string"], "bad_parameter_value")
        with self.assertRaises(HTTPException) as stranger:
            ai_graph_edits.apply_operations(graph, [
                {"op": "clone_nodes", "ids": ["clip"], "variants": [{"write": {"prompt": "x"}}]}])
        self.assertEqual(stranger.exception.detail["error_string"], "bad_clone")
        with self.assertRaises(HTTPException) as missing:
            ai_graph_edits.apply_operations(graph, [
                {"op": "clone_nodes", "ids": ["nope"], "count": 1}])
        self.assertEqual(missing.exception.detail["error_string"], "unknown_node")
        with self.assertRaises(HTTPException) as too_many:
            ai_graph_edits.apply_operations(graph, [
                {"op": "clone_nodes", "ids": ["clip"], "count": 41}])
        self.assertEqual(too_many.exception.detail["error_string"], "too_many_variants")
        self.assertEqual(len(graph.nodes), 5)

    def test_node_limit_holds_for_clones(self):
        graph = ai_graph.Graph(**_payload())
        operations = [{"op": "clone_nodes", "ids": ["write", "clip"], "count": 40}
                      for _ in range(3)]
        with self.assertRaises(HTTPException) as limit:
            ai_graph_edits.apply_operations(graph, operations)
        self.assertEqual(limit.exception.detail["error_string"], "graph_too_large")

    def test_clones_can_be_edited_and_connected_in_the_same_patch(self):
        graph = ai_graph.Graph(**_payload())
        edited, summary, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "clone_nodes", "ids": ["clip"], "count": 1},
            {"op": "update_params", "id": "clip_1", "values": {"frame_count": 121}},
            {"op": "disconnect", "from": "write", "output": "answer_string", "to": "clip_1", "input": "prompt"},
            {"op": "connect", "from": "see", "output": "answer_string", "to": "clip_1", "input": "prompt"},
        ])
        by_id = {node.id: node for node in edited.nodes}
        self.assertEqual(by_id["clip_1"].params["frame_count"], 121)
        self.assertIn(("see", "answer_string", "clip_1", "prompt"), _links(edited))
        self.assertEqual(summary["updated_node_ids_array"], ["clip_1"])


class SchemaTests(unittest.TestCase):
    def test_schema_lists_clone_nodes(self):
        app = FastAPI()
        app.include_router(ai_graph_edits.router)
        with patch.object(ai_model_catalogue, "entries", return_value=[]):
            body = TestClient(app).get("/api/ai/graph-edits/schema").json()
        ops = {row["op"]: row for row in body["operations_array"]}
        self.assertIn("clone_nodes", ops)
        self.assertEqual(ops["clone_nodes"]["fields"], ["ids", "variants", "count", "dx", "dy"])


if __name__ == "__main__":
    unittest.main()
