"""Node graphs: the built-in composition, type safety, and the deep link."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_graph  # noqa: E402
import ai_services  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(ai_graph.router)
    return TestClient(app)


def _graph(nodes, links, name="t"):
    return ai_graph.Graph(name=name, nodes=nodes, links=links)


def _node(node_id, **kwargs):
    return ai_graph.GraphNode(id=node_id, **kwargs)


def _link(source, output, target, field):
    return ai_graph.GraphLink(**{"from": source, "output": output,
                                 "to": target, "input": field})


class TemplateTests(unittest.TestCase):
    """The reference composition the owner asked for, checked as a whole."""

    def setUp(self):
        self.template = ai_graph.templates()[0]
        self.graph = self.template["graph"]

    def test_it_is_called_what_it_was_asked_to_be_called(self):
        self.assertEqual(self.template["id"], "SimpleTextToVideoByVision")

    def test_the_template_is_a_graph_the_server_would_accept(self):
        ai_graph.validate(ai_graph.Graph(**self.graph))

    def test_a_picture_goes_in_and_vision_reads_it(self):
        links = self.graph["links"]
        self.assertIn({"from": "source", "output": "value",
                       "to": "vision", "input": "image"}, links)

    def test_visions_answer_becomes_the_prompt_for_the_picture(self):
        self.assertIn({"from": "vision", "output": "answer_string",
                       "to": "draw", "input": "prompt"}, links_of(self.graph))

    def test_the_generated_picture_feeds_both_clips_and_the_model(self):
        targets = {link["to"] for link in links_of(self.graph) if link["from"] == "draw"}
        self.assertEqual(targets, {"clip", "loop", "solid"})

    def test_the_looping_clip_gets_the_same_frame_at_both_ends(self):
        loop_inputs = {link["input"] for link in links_of(self.graph) if link["to"] == "loop"}
        self.assertEqual(loop_inputs, {"image", "image_url_end"})

    def test_the_plain_clip_only_gets_a_first_frame(self):
        clip_inputs = {link["input"] for link in links_of(self.graph) if link["to"] == "clip"}
        self.assertEqual(clip_inputs, {"image"})

    def test_two_videos_come_out_of_it(self):
        videos = [node for node in self.graph["nodes"] if node.get("service") == "video"]
        self.assertEqual(len(videos), 2)

    def test_every_service_it_names_exists_and_is_live(self):
        for node in self.graph["nodes"]:
            if node["kind"] != ai_graph.NODE_SERVICE:
                continue
            entry = ai_services.service(node["service"])
            self.assertIsNotNone(entry, node["service"])
            self.assertEqual(entry["status"], "live", node["service"])

    def test_vision_runs_on_a_model_whose_context_fits_a_farm_picture(self):
        """A 1024x1024 frame is about 4140 tokens before the prompt is added.

        The default 27B is served with a 4096-token context, so the template
        must not pick it: the request is refused outright and every node
        downstream is skipped.
        """
        vision = next(node for node in self.graph["nodes"] if node.get("service") == "vision")
        import ai_vision_api
        chosen = next(entry for entry in ai_vision_api.AI_MODELS
                      if entry["id"] == vision["params"]["model"])
        self.assertGreaterEqual(chosen["context_tokens"], 8192)

    def test_every_parameter_it_sets_is_one_the_service_declares(self):
        for node in self.graph["nodes"]:
            if node["kind"] != ai_graph.NODE_SERVICE:
                continue
            declared = {param["name"] for param in ai_services.params_for(node["service"])}
            self.assertTrue(set(node["params"]) <= declared,
                            f"{node['id']} sets {set(node['params']) - declared}")


def links_of(graph):
    return graph["links"]


class ValidationTests(unittest.TestCase):
    def test_text_cannot_be_wired_into_an_image_socket(self):
        graph = _graph(
            [_node("a", kind="input", entity_type="text", value="hi"),
             _node("b", kind="service", service="vision")],
            [_link("a", "value", "b", "image")],
        )
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "type_mismatch")

    def test_an_image_is_accepted_by_an_image_socket(self):
        graph = _graph(
            [_node("a", kind="input", entity_type="image", value="https://x/y.png"),
             _node("b", kind="service", service="vision")],
            [_link("a", "value", "b", "image")],
        )
        ai_graph.validate(graph)

    def test_a_service_nobody_has_heard_of_is_refused(self):
        graph = _graph([_node("a", kind="service", service="telepathy")], [])
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "unknown_service")

    def test_a_link_to_a_missing_node_is_refused(self):
        graph = _graph([_node("a", kind="service", service="text")],
                       [_link("a", "answer_string", "ghost", "prompt")])
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "dangling_link")

    def test_wiring_that_loops_back_is_refused(self):
        graph = _graph(
            [_node("a", kind="service", service="text"),
             _node("b", kind="service", service="text")],
            [_link("a", "answer_string", "b", "prompt"),
             _link("b", "answer_string", "a", "prompt")],
        )
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "graph_has_a_cycle")

    def test_a_branch_that_forks_and_never_rejoins_is_fine(self):
        graph = _graph(
            [_node("a", kind="input", entity_type="image", value="https://x/y.png"),
             _node("b", kind="service", service="3dmodel"),
             _node("c", kind="service", service="video")],
            [_link("a", "value", "b", "image"), _link("a", "value", "c", "image")],
        )
        ai_graph.validate(graph)

    def test_nothing_may_be_wired_into_a_source(self):
        graph = _graph(
            [_node("a", kind="service", service="image"),
             _node("b", kind="input", entity_type="text", value="")],
            [_link("a", "image_url_string", "b", "value")],
        )
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "link_into_input")

    def test_an_empty_graph_is_refused(self):
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(_graph([], []))
        self.assertEqual(caught.exception.detail["error_string"], "graph_empty")

    def test_two_nodes_may_not_share_an_id(self):
        graph = _graph([_node("a", kind="service", service="text"),
                        _node("a", kind="service", service="text")], [])
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(graph)
        self.assertEqual(caught.exception.detail["error_string"], "duplicate_node_id")

    def test_a_graph_larger_than_the_limit_is_refused(self):
        nodes = [_node(f"n{i}", kind="service", service="text")
                 for i in range(ai_graph.MAX_NODES + 1)]
        with self.assertRaises(Exception) as caught:
            ai_graph.validate(_graph(nodes, []))
        self.assertEqual(caught.exception.detail["error_string"], "graph_too_large")


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._previous = ai_graph.GRAPH_DIR
        ai_graph.GRAPH_DIR = Path(self._dir.name)
        self.client = _client()

    def tearDown(self):
        ai_graph.GRAPH_DIR = self._previous
        self._dir.cleanup()

    def _payload(self):
        return {
            "name": "two steps",
            "nodes": [
                {"id": "a", "kind": "input", "entity_type": "text", "value": "a lighthouse"},
                {"id": "b", "kind": "service", "service": "image", "params": {"width": 1024}},
            ],
            "links": [{"from": "a", "output": "value", "to": "b", "input": "prompt"}],
        }

    def test_the_template_is_served(self):
        body = self.client.get("/api/ai/graph/templates").json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["templates_array"][0]["id"], "SimpleTextToVideoByVision")

    def test_a_template_id_opens_without_ever_being_saved(self):
        body = self.client.get("/api/ai/graphs/SimpleTextToVideoByVision").json()
        self.assertTrue(body["template_bool"])
        self.assertEqual(len(body["graph_object"]["nodes"]), 7)

    def test_saving_returns_a_link_that_opens_the_same_graph(self):
        saved = self.client.post("/api/ai/graphs", json=self._payload()).json()
        self.assertTrue(saved["success_bool"])
        self.assertTrue(saved["deep_link_string"].startswith("/nodes?g="))
        loaded = self.client.get("/api/ai/graphs/" + saved["graph_id_string"]).json()
        self.assertEqual(loaded["graph_object"]["name"], "two steps")
        self.assertFalse(loaded["template_bool"])

    def test_saving_the_same_graph_twice_gives_one_link(self):
        first = self.client.post("/api/ai/graphs", json=self._payload()).json()
        second = self.client.post("/api/ai/graphs", json=self._payload()).json()
        self.assertEqual(first["graph_id_string"], second["graph_id_string"])

    def test_a_graph_that_could_not_run_is_not_saved(self):
        payload = self._payload()
        payload["links"][0]["input"] = "image"
        response = self.client.post("/api/ai/graphs", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(len(list(Path(self._dir.name).glob("*.json"))), 0)

    def test_an_id_that_was_never_saved_says_so(self):
        response = self.client.get("/api/ai/graphs/nothinghere")
        self.assertEqual(response.status_code, 404)

    def test_an_id_that_could_escape_the_store_is_refused(self):
        response = self.client.get("/api/ai/graphs/..%2F..%2Fetc")
        self.assertIn(response.status_code, (400, 404))


class CatalogueParameterTests(unittest.TestCase):
    """The editor draws its controls from these, so their shape matters."""

    def test_every_service_that_declares_parameters_declares_real_ones(self):
        for service_id, params in ai_services.PARAMS.items():
            self.assertIsNotNone(ai_services.service(service_id), service_id)
            for param in params:
                self.assertIn(param["type"], ("select", "range", "number", "text"))
                self.assertTrue(param["name"])
                self.assertTrue(param["title"])
                if param["type"] == "select" and "source" not in param:
                    self.assertTrue(param["options"], param["name"])

    def test_a_select_defaults_to_one_of_its_own_options(self):
        for params in ai_services.PARAMS.values():
            for param in params:
                if param["type"] != "select" or "source" in param:
                    continue
                values = {str(option["value"]) for option in param["options"]}
                self.assertIn(str(param["default"]), values, param["name"])

    def test_a_sliders_default_is_a_value_the_slider_can_hold(self):
        """A browser silently clamps a value outside a range's own bounds.

        `steps` defaulted to 0 on a slider that started at 4, so every render
        went out asking for four steps — a number nobody chose and which ruins
        the picture. A default has to be reachable or it is not a default.
        """
        for params in ai_services.PARAMS.values():
            for param in params:
                if param["type"] not in ("range", "number"):
                    continue
                self.assertGreaterEqual(param["default"], param["min"], param["name"])
                self.assertLessEqual(param["default"], param["max"], param["name"])

    def test_video_takes_a_last_frame_so_a_clip_can_loop(self):
        fields = {item["field"] for item in ai_services.service("video")["inputs"]}
        self.assertIn("image_url_end", fields)

    def test_the_last_frame_is_optional(self):
        end = next(item for item in ai_services.service("video")["inputs"]
                   if item["field"] == "image_url_end")
        self.assertFalse(end["required"])


if __name__ == "__main__":
    unittest.main()
