"""FLUX 3 Image Klein edit route: template, reference chain, scheduling token, internal-only engine."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_qwen_image_api as api  # noqa: E402
from renderfin import multiref, routing  # noqa: E402
from renderfin.models import RenderPrompt  # noqa: E402

TEMPLATE = BACKEND / "renderfin" / "assets" / "workflows" / multiref.WORKFLOW_FLUX3


class Flux3EditRoute(unittest.TestCase):
    def _graph(self):
        text = TEMPLATE.read_text(encoding="utf-8").replace("$prompt", "x").replace("$output_url", "o")
        text = text.replace("$width", "1024").replace("$height", "768")
        return json.loads(text)

    def test_template_uses_flux3_files_and_type(self):
        g = self._graph()
        self.assertEqual(g["clip"]["inputs"]["type"], "flux3")
        self.assertEqual(g["sigmas"]["inputs"]["steps"], 12)
        self.assertIn("flux3_image_klein", g["model"]["inputs"]["unet_name"])

    def test_references_chain_into_the_guider(self):
        g = self._graph()
        multiref.inject_references(multiref.WORKFLOW_FLUX3, g, ["a.png", "b.png"])
        chain = [k for k, v in g.items() if v["class_type"] == "ReferenceLatent"]
        self.assertEqual(len(chain), 2)
        self.assertEqual(g["guider"]["inputs"]["conditioning"], [chain[-1], 0])

    def test_reference_limits(self):
        with self.assertRaises(ValueError):
            multiref.check_reference_count(multiref.WORKFLOW_FLUX3, 0)
        multiref.check_reference_count(multiref.WORKFLOW_FLUX3, 3)

    def test_scheduling_token_and_workflow(self):
        p = RenderPrompt(type=multiref.TYPE_FLUX3)
        self.assertEqual(routing.select_image_workflow(p)[0], multiref.WORKFLOW_FLUX3)
        self.assertEqual(routing.scheduling_token(p),
                         multiref.WORKFLOW_FLUX3)

    def test_qwen_and_klein_routes_are_unchanged(self):
        self.assertEqual(routing.select_image_workflow(RenderPrompt(type=multiref.TYPE_QWEN21))[0], multiref.WORKFLOW_QWEN21)
        self.assertEqual(routing.select_image_workflow(RenderPrompt(type=multiref.TYPE_KLEIN))[0], multiref.WORKFLOW_KLEIN)

    def test_engine_is_internal_only(self):
        class Req:
            def __init__(self, host, headers):
                self.client = type("C", (), {"host": host})()
                self.headers = headers
        self.assertTrue(api._internal_call(Req("127.0.0.1", {})))
        self.assertFalse(api._internal_call(Req("127.0.0.1", {"x-real-ip": "1.2.3.4"})))
        self.assertFalse(api._internal_call(Req("8.8.8.8", {})))


if __name__ == "__main__":
    unittest.main()
