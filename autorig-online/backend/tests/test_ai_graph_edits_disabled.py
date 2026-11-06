"""_disabled: a node the editor keeps but leaves out of the run.

Bypass is an editor decision, not a request to a service, so the flag travels
in params the way `_label` and `_follow_input_size` do: accepted on any node,
never matched against a service declaration, and boolean or refused.
"""

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
from fastapi import HTTPException  # noqa: E402


def _payload():
    return {
        "name": "bypass",
        "nodes": [
            {"id": "pic", "kind": "input", "entity_type": "image",
             "value": "https://autorig.online/dev/api/scratch/pic.png", "x": 0, "y": 0},
            {"id": "see", "kind": "service", "service": "vision",
             "params": {"prompt": "Describe"}, "x": 300, "y": 0},
            {"id": "image1", "kind": "service", "service": "image",
             "params": {"width": 832, "height": 1216}, "x": 600, "y": 0},
        ],
        "links": [
            {"from": "pic", "output": "value", "to": "see", "input": "image"},
            {"from": "see", "output": "answer_string", "to": "image1", "input": "prompt"},
        ],
    }


class DisabledParamTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(ai_model_catalogue, "entries", return_value=[])
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_disabled_is_a_display_parameter_validated_as_boolean(self):
        self.assertIn("_disabled", ai_graph_edits.DISPLAY_PARAM_KEYS)
        self.assertIn("_disabled", ai_graph_edits.BOOLEAN_DISPLAY_PARAM_KEYS)
        # The rule that already governs _follow_input_size now governs both.
        self.assertIn("_follow_input_size", ai_graph_edits.BOOLEAN_DISPLAY_PARAM_KEYS)

    def test_bypass_is_set_and_cleared_on_a_service_node(self):
        graph = ai_graph.Graph(**_payload())
        bypassed, _, invalidated = ai_graph_edits.apply_operations(graph, [
            {"op": "update_params", "id": "see", "values": {"_disabled": True}},
        ])
        by_id = {node.id: node for node in bypassed.nodes}
        self.assertIs(by_id["see"].params["_disabled"], True)
        # Everything reading its output is stale, exactly as any other change.
        self.assertIn("see", invalidated)
        self.assertIn("image1", invalidated)
        enabled, _, _ = ai_graph_edits.apply_operations(bypassed, [
            {"op": "update_params", "id": "see", "values": {"_disabled": False}},
        ])
        self.assertIs({node.id: node for node in enabled.nodes}["see"].params["_disabled"], False)

    def test_bypass_is_accepted_on_an_input_node_that_declares_no_parameters(self):
        graph = ai_graph.Graph(**_payload())
        edited, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "update_params", "id": "pic", "values": {"_disabled": True}},
        ])
        self.assertIs({node.id: node for node in edited.nodes}["pic"].params["_disabled"], True)

    def test_a_non_boolean_bypass_is_refused(self):
        graph = ai_graph.Graph(**_payload())
        for value in ("yes", 1, None, []):
            with self.subTest(value=value):
                with self.assertRaises(HTTPException) as caught:
                    ai_graph_edits.apply_operations(graph, [
                        {"op": "update_params", "id": "see", "values": {"_disabled": value}},
                    ])
                self.assertEqual(caught.exception.detail["error_string"], "bad_parameter_value")
                self.assertIn("_disabled", caught.exception.detail["message_string"])

    def test_bypass_survives_a_later_unrelated_edit(self):
        graph = ai_graph.Graph(**_payload())
        bypassed, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "update_params", "id": "image1", "values": {"_disabled": True}},
        ])
        moved, _, _ = ai_graph_edits.apply_operations(bypassed, [
            {"op": "move_node", "id": "image1", "x": 700, "y": 40},
        ])
        by_id = {node.id: node for node in moved.nodes}
        self.assertIs(by_id["image1"].params["_disabled"], True)
        self.assertEqual((by_id["image1"].x, by_id["image1"].y), (700.0, 40.0))

    def test_bypass_does_not_disturb_the_follow_input_size_rule(self):
        graph = ai_graph.Graph(**_payload())
        edited, _, _ = ai_graph_edits.apply_operations(graph, [
            {"op": "update_params", "id": "image1",
             "values": {"_disabled": True, "width": 1024}},
        ])
        params = {node.id: node for node in edited.nodes}["image1"].params
        self.assertIs(params["_disabled"], True)
        # An explicit width still switches the node to a fixed size.
        self.assertIs(params["_follow_input_size"], False)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
