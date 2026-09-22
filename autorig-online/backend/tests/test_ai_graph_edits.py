"""Safe, atomic graph-edit validation for the interactive AI composer."""

from __future__ import annotations

import copy
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
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _payload():
    return {
        "name": "demo",
        "nodes": [
            {"id": "text1", "kind": "input", "entity_type": "text", "value": "a cat",
             "x": 0, "y": 0},
            {"id": "image1", "kind": "service", "service": "image",
             "params": {"width": 960, "height": 540}, "x": 200, "y": 0},
            {"id": "video1", "kind": "service", "service": "video",
             "params": {"frame_count": 97}, "x": 400, "y": 0},
        ],
        "links": [
            {"from": "text1", "output": "value", "to": "image1", "input": "prompt"},
            {"from": "image1", "output": "image_url_string", "to": "video1", "input": "image"},
        ],
        "results": {
            "text1": {"status": "done", "type": "text", "value": "a cat"},
            "image1": {"status": "done", "type": "image", "value": "/cat.png"},
            "video1": {"status": "running", "type": "video", "task_id": "task-1"},
        },
    }


def _client():
    app = FastAPI()
    app.include_router(ai_graph_edits.router)
    return TestClient(app)


def _fixed_sampling_catalogue():
    return [{
        "file": "distilled.safetensors", "kind": "checkpoint",
        "family": "flux2", "services": ["image"], "usable": True,
        "recommended_from": "author reference workflow",
        "recommended": {"steps": 4, "cfg": 1.0, "sampler": "euler"},
        "sampling_policy": {
            "auto_steps": 4, "fixed_steps": 4, "steps_min": 4, "steps_max": 4,
            "auto_reason": "distilled schedule", "cfg_mode": "fixed", "cfg_value": 1.0,
            "scheduler_mode": "native", "scheduler_label": "native sigma schedule",
        },
    }]


class ApplyOperationsTests(unittest.TestCase):
    def test_parameter_change_invalidates_node_and_downstream_only(self):
        graph = ai_graph.Graph(**_payload())
        edited, summary, invalidated = ai_graph_edits.apply_operations(
            graph, [{"op": "update_params", "id": "image1", "values": {"width": 1280}}])
        self.assertEqual(edited.nodes[1].params["width"], 1280)
        self.assertEqual(invalidated, ["image1", "video1"])
        self.assertEqual(set(edited.results), {"text1"})
        self.assertEqual(summary["updated_node_ids_array"], ["image1"])
        self.assertEqual(graph.nodes[1].params["width"], 960)
        self.assertEqual(set(graph.results), {"text1", "image1", "video1"})

    def test_move_and_rename_preserve_results(self):
        graph = ai_graph.Graph(**_payload())
        edited, summary, invalidated = ai_graph_edits.apply_operations(graph, [
            {"op": "move_node", "id": "image1", "x": 222.5, "y": -4},
            {"op": "rename_graph", "name": "  Better graph  "},
        ])
        self.assertEqual(edited.name, "Better graph")
        self.assertEqual((edited.nodes[1].x, edited.nodes[1].y), (222.5, -4.0))
        self.assertEqual(invalidated, [])
        self.assertEqual(set(edited.results), {"text1", "image1", "video1"})
        self.assertTrue(summary["renamed_bool"])

    def test_add_connect_disconnect_and_remove_keep_valid_wiring(self):
        graph = ai_graph.Graph(**_payload())
        edited, summary, invalidated = ai_graph_edits.apply_operations(graph, [
            {"op": "add_node", "node": {"id": "caption2", "kind": "service",
                                           "service": "text", "x": 20, "y": 120}},
            {"op": "disconnect", "from": "text1", "output": "value",
             "to": "image1", "input": "prompt"},
            {"op": "connect", "from": "text1", "output": "value",
             "to": "caption2", "input": "prompt"},
            {"op": "connect", "from": "caption2", "output": "answer_string",
             "to": "image1", "input": "prompt"},
            {"op": "remove_node", "id": "video1"},
        ])
        ai_graph.validate(edited)
        self.assertEqual({node.id for node in edited.nodes}, {"text1", "caption2", "image1"})
        self.assertEqual(summary["connections_added_int"], 2)
        self.assertEqual(summary["connections_removed_int"], 1)
        self.assertIn("video1", invalidated)

    def test_set_input_rejects_a_service_node(self):
        graph = ai_graph.Graph(**_payload())
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(
                graph, [{"op": "set_input", "id": "image1", "value": "unsafe"}])
        self.assertEqual(caught.exception.detail["error_string"], "not_an_input_node")

    def test_unknown_parameter_rejects_the_entire_patch(self):
        graph = ai_graph.Graph(**_payload())
        before = graph.model_dump(by_alias=True)
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(graph, [
                {"op": "update_params", "id": "image1", "values": {"width": 1280}},
                {"op": "update_params", "id": "video1", "values": {"shell": "rm"}},
            ])
        self.assertEqual(caught.exception.detail["error_string"], "unknown_parameter")
        self.assertEqual(graph.model_dump(by_alias=True), before)

    def test_declared_parameter_still_obeys_its_bounds(self):
        graph = ai_graph.Graph(**_payload())
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(graph, [
                {"op": "update_params", "id": "image1", "values": {"width": 9000}},
            ])
        self.assertEqual(caught.exception.detail["error_string"], "bad_parameter_value")
        self.assertEqual(caught.exception.detail["operation_index_int"], 0)

    def test_custom_image_dimensions_do_not_need_to_be_select_presets(self):
        graph = ai_graph.Graph(**_payload())
        graph.nodes[1].params.update({"width": 1030, "height": 1527})
        unchanged, _, _ = ai_graph_edits.apply_operations(graph, [])
        self.assertEqual(unchanged.nodes[1].params["width"], 1030)
        edited, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "update_params", "id": "image1",
             "values": {"width": 383, "height": 686}},
        ])
        self.assertEqual((edited.nodes[1].params["width"], edited.nodes[1].params["height"]),
                         (383, 686))

    def test_wrong_port_type_is_rejected(self):
        graph = ai_graph.Graph(**_payload())
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(graph, [
                {"op": "disconnect", "from": "text1", "output": "value",
                 "to": "image1", "input": "prompt"},
                {"op": "connect", "from": "text1", "output": "value",
                 "to": "image1", "input": "image"},
            ])
        self.assertEqual(caught.exception.detail["error_string"], "type_mismatch")

    def test_cycle_is_rejected_without_mutating_input(self):
        graph = ai_graph.Graph(name="cycle", nodes=[
            ai_graph.GraphNode(id="text1", service="text"),
            ai_graph.GraphNode(id="text2", service="text"),
        ], links=[ai_graph.GraphLink(**{"from": "text1", "output": "answer_string",
                                       "to": "text2", "input": "prompt"})])
        before = graph.model_dump(by_alias=True)
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(graph, [
                {"op": "connect", "from": "text2", "output": "answer_string",
                 "to": "text1", "input": "prompt"},
            ])
        self.assertEqual(caught.exception.detail["error_string"], "graph_has_a_cycle")
        self.assertEqual(graph.model_dump(by_alias=True), before)

    def test_two_control_channels_to_one_image_are_rejected(self):
        graph = ai_graph.Graph(name="control", nodes=[
            ai_graph.GraphNode(id="source", kind="input", entity_type="image"),
            ai_graph.GraphNode(id="pose1", service="control_pose"),
            ai_graph.GraphNode(id="depth1", service="control_depth"),
            ai_graph.GraphNode(id="image1", service="image"),
        ], links=[
            ai_graph.GraphLink(**{"from": "source", "output": "value", "to": "pose1", "input": "image"}),
            ai_graph.GraphLink(**{"from": "source", "output": "value", "to": "depth1", "input": "image"}),
            ai_graph.GraphLink(**{"from": "pose1", "output": "image_url_string",
                                  "to": "image1", "input": "control_pose"}),
        ])
        with self.assertRaises(Exception) as caught:
            ai_graph_edits.apply_operations(graph, [
                {"op": "connect", "from": "depth1", "output": "image_url_string",
                 "to": "image1", "input": "control_depth"},
            ])
        self.assertEqual(caught.exception.detail["error_string"], "multiple_control_channels")

    def test_catalogue_blocks_unsupported_control_family(self):
        graph = ai_graph.Graph(name="control", nodes=[
            ai_graph.GraphNode(id="source", kind="input", entity_type="image"),
            ai_graph.GraphNode(id="pose1", service="control_pose"),
            ai_graph.GraphNode(id="image1", service="image",
                               params={"checkpoint": "flux2.safetensors"}),
        ], links=[
            ai_graph.GraphLink(**{"from": "source", "output": "value", "to": "pose1", "input": "image"}),
            ai_graph.GraphLink(**{"from": "pose1", "output": "image_url_string",
                                  "to": "image1", "input": "control_pose"}),
        ])
        catalogue = [{"file": "flux2.safetensors", "kind": "checkpoint",
                      "family": "flux2", "services": ["image"], "usable": True}]
        with patch.object(ai_model_catalogue, "entries", return_value=catalogue):
            with self.assertRaises(Exception) as caught:
                ai_graph_edits.apply_operations(graph, [])
        self.assertEqual(caught.exception.detail["error_string"], "unsupported_control_family")

    def test_fixed_sampling_steps_reject_entire_patch(self):
        graph = ai_graph.Graph(**_payload())
        before = graph.model_dump(by_alias=True)
        with patch.object(ai_model_catalogue, "entries", return_value=_fixed_sampling_catalogue()):
            with self.assertRaises(Exception) as caught:
                ai_graph_edits.apply_operations(graph, [{
                    "op": "update_params", "id": "image1",
                    "values": {"checkpoint": "distilled.safetensors", "steps": 5},
                }])
        self.assertEqual(caught.exception.detail["error_string"], "invalid_sampling_settings")
        self.assertIn("fixed 4-step", caught.exception.detail["message_string"])
        self.assertEqual(graph.model_dump(by_alias=True), before)

    def test_fixed_basic_guider_cfg_rejects_entire_patch(self):
        graph = ai_graph.Graph(**_payload())
        before = graph.model_dump(by_alias=True)
        with patch.object(ai_model_catalogue, "entries", return_value=_fixed_sampling_catalogue()):
            with self.assertRaises(Exception) as caught:
                ai_graph_edits.apply_operations(graph, [{
                    "op": "update_params", "id": "image1",
                    "values": {"checkpoint": "distilled.safetensors", "cfg": 2.0},
                }])
        self.assertEqual(caught.exception.detail["error_string"], "invalid_sampling_settings")
        self.assertIn("fixed CFG 1", caught.exception.detail["message_string"])
        self.assertEqual(graph.model_dump(by_alias=True), before)

    def test_auto_zero_sampling_values_are_accepted(self):
        graph = ai_graph.Graph(**_payload())
        with patch.object(ai_model_catalogue, "entries", return_value=_fixed_sampling_catalogue()):
            edited, _, _ = ai_graph_edits.apply_operations(graph, [{
                "op": "update_params", "id": "image1",
                "values": {
                    "checkpoint": "distilled.safetensors", "steps": 0, "cfg": 0,
                    "sampler": "", "scheduler": "", "lora_strength": 0,
                },
            }])
        self.assertEqual(edited.nodes[1].params["steps"], 0)
        self.assertEqual(edited.nodes[1].params["cfg"], 0)


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = _client()

    def test_validate_returns_aliases_summary_and_invalidations(self):
        response = self.client.post("/api/ai/graph-edits/validate", json={
            "graph": _payload(),
            "operations": [{"op": "set_input", "id": "text1", "value": "a dog"}],
        })
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["graph_object"]["links"][0]["from"], "text1")
        self.assertEqual(body["summary"]["operation_count_int"], 1)
        self.assertEqual(body["invalidated_node_ids_array"], ["image1", "text1", "video1"])
        self.assertEqual(set(body["graph_object"]["results"]), set())

    def test_unknown_operation_is_a_structured_atomic_error(self):
        response = self.client.post("/api/ai/graph-edits/validate", json={
            "graph": _payload(), "operations": [{"op": "run_shell", "command": "no"}]})
        self.assertEqual(response.status_code, 400)
        detail = response.json()["detail"]
        self.assertEqual(detail["error_string"], "unknown_operation")
        self.assertEqual(detail["operation_index_int"], 0)

    def test_operation_limit_is_a_structured_graph_edit_error(self):
        operations = [{"op": "rename_graph", "name": f"name-{index}"}
                      for index in range(ai_graph_edits.MAX_OPERATIONS + 1)]
        response = self.client.post("/api/ai/graph-edits/validate", json={
            "graph": _payload(), "operations": operations})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "too_many_operations")

    def test_schema_exposes_only_the_whitelist(self):
        with patch.object(ai_model_catalogue, "entries", return_value=[]):
            body = self.client.get("/api/ai/graph-edits/schema").json()
        self.assertEqual({row["op"] for row in body["operations_array"]}, {
            "add_node", "remove_node", "update_params", "set_input", "connect",
            "disconnect", "move_node", "rename_graph"})
        self.assertIn("does not save", body["side_effects_string"])

    def test_schema_exposes_sampling_policy_to_the_agent(self):
        with patch.object(ai_model_catalogue, "entries", return_value=_fixed_sampling_catalogue()):
            body = self.client.get("/api/ai/graph-edits/schema").json()
        model = body["models_array"][0]
        self.assertEqual(model["sampling_policy"]["fixed_steps"], 4)
        self.assertEqual(model["sampling_policy"]["scheduler_mode"], "native")


if __name__ == "__main__":
    unittest.main()
