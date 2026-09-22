"""Node graphs: the built-in composition, type safety, and the deep link."""

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

    def test_empty_instance_keeps_the_legacy_graph_id(self):
        payload = self._payload()
        legacy_body = ai_graph.Graph(**payload).model_dump(by_alias=True)
        legacy_body.pop("instance_id")
        legacy_body.pop("comparison_anchor_id")
        legacy_body.pop("results")
        legacy_identity = json.dumps(legacy_body, ensure_ascii=False, sort_keys=True)
        expected = ai_graph._new_id(legacy_identity)
        absent = self.client.post("/api/ai/graphs", json=payload).json()
        payload["instance_id"] = ""
        explicit_empty = self.client.post("/api/ai/graphs", json=payload).json()
        self.assertEqual(absent["graph_id_string"], expected)
        self.assertEqual(explicit_empty["graph_id_string"], expected)

    def test_nonempty_instance_is_stable_across_result_updates(self):
        payload = self._payload()
        payload["instance_id"] = "447f9714-3ddb-4edd-9202-79b89ec77e31"
        first = self.client.post("/api/ai/graphs", json=payload).json()
        self.client.put(
            f"/api/ai/graphs/{first['graph_id_string']}/results",
            json={"b": {"status": "done", "type": "image",
                        "value": "https://x/result.png", "task_id": "task-1"}},
        )
        loaded = self.client.get(
            f"/api/ai/graphs/{first['graph_id_string']}"
        ).json()["graph_object"]
        second = self.client.post("/api/ai/graphs", json=loaded).json()
        self.assertEqual(first["graph_id_string"], second["graph_id_string"])
        self.assertEqual(loaded["results"]["b"]["task_id"], "task-1")

    def test_instance_identity_survives_drawflow_node_renumbering(self):
        first = {
            "name": "gapped",
            "instance_id": "same-copy-instance",
            "comparison_anchor_id": "7",
            "nodes": [
                {"id": "1", "kind": "input", "entity_type": "text", "value": "prompt"},
                {"id": "7", "kind": "service", "service": "image"},
            ],
            "links": [{"from": "1", "output": "value", "to": "7", "input": "prompt"}],
            "results": {"7": {"status": "done", "type": "image",
                               "value": "https://x/current.png",
                               "history": [{"type": "image", "value": "https://x/old.png"}]}},
        }
        compact = {
            **first,
            "comparison_anchor_id": "2",
            "nodes": [
                {**first["nodes"][0], "id": "1"},
                {**first["nodes"][1], "id": "2"},
            ],
            "links": [{"from": "1", "output": "value", "to": "2", "input": "prompt"}],
            "results": {"2": {"status": "done", "type": "image",
                               "value": "https://x/different-result.png"}},
        }
        first_saved = self.client.post("/api/ai/graphs", json=first).json()
        compact_saved = self.client.post("/api/ai/graphs", json=compact).json()
        self.assertEqual(first_saved["graph_id_string"], compact_saved["graph_id_string"])

        other_instance = {**compact, "instance_id": "different-copy-instance"}
        other_saved = self.client.post("/api/ai/graphs", json=other_instance).json()
        self.assertNotEqual(first_saved["graph_id_string"], other_saved["graph_id_string"])

    def test_duplicate_always_creates_an_independent_snapshot(self):
        original = self._payload()
        original["comparison_anchor_id"] = "b"
        original["results"] = {
            "b": {"status": "running", "type": "image",
                  "value": "https://x/pending.png", "task_id": "task-live",
                  "input_reference_url": "https://x/reference.png",
                  "history": [{"type": "image", "value": "https://x/older.png",
                               "input_reference_url": "https://x/reference.png",
                               "created_at": 10}]}
        }
        saved_original = self.client.post("/api/ai/graphs", json=original).json()
        first = self.client.post(
            "/api/ai/graphs/duplicate", json={"currentGraph": original}
        )
        second = self.client.post(
            "/api/ai/graphs/duplicate", json={"currentGraph": original}
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        first_body, second_body = first.json(), second.json()
        self.assertNotEqual(first_body["graph_id_string"], saved_original["graph_id_string"])
        self.assertNotEqual(second_body["graph_id_string"], saved_original["graph_id_string"])
        self.assertNotEqual(first_body["graph_id_string"], second_body["graph_id_string"])
        self.assertNotEqual(
            first_body["graph_object"]["instance_id"],
            second_body["graph_object"]["instance_id"],
        )
        self.assertEqual(
            first_body["graph_object"]["results"]["b"]["task_id"], "task-live"
        )
        self.assertEqual(
            first_body["graph_object"]["results"]["b"]["history"],
            original["results"]["b"]["history"],
        )
        self.assertEqual(first_body["graph_object"]["comparison_anchor_id"], "b")
        self.assertTrue(first_body["source_unchanged_bool"])
        loaded_original = self.client.get(
            "/api/ai/graphs/" + saved_original["graph_id_string"]
        ).json()["graph_object"]
        self.assertEqual(loaded_original.get("instance_id"), "")
        self.assertEqual(loaded_original["results"]["b"]["task_id"], "task-live")

    def test_comparison_anchor_must_name_an_existing_node(self):
        payload = self._payload()
        payload["comparison_anchor_id"] = "removed-node"
        response = self.client.post("/api/ai/graphs", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "unknown_comparison_anchor")

    def test_completed_results_become_bounded_history_without_task_ids(self):
        payload = self._payload()
        payload["instance_id"] = "history-test"
        saved = self.client.post("/api/ai/graphs", json=payload).json()
        graph_id = saved["graph_id_string"]
        first_result = {
            "b": {"status": "done", "type": "image",
                  "value": "https://x/first.png", "task_id": "secret-task-1",
                  "input_reference_url": "https://x/reference.png"}
        }
        self.assertEqual(self.client.put(
            f"/api/ai/graphs/{graph_id}/results", json=first_result
        ).status_code, 200)
        second_result = {
            "b": {"status": "done", "type": "image",
                  "value": "https://x/second.png", "task_id": "secret-task-2",
                  "input_reference_url": "https://x/reference.png"}
        }
        self.assertEqual(self.client.put(
            f"/api/ai/graphs/{graph_id}/results", json=second_result
        ).status_code, 200)
        loaded = self.client.get(f"/api/ai/graphs/{graph_id}").json()["graph_object"]
        result = loaded["results"]["b"]
        self.assertEqual(result["value"], "https://x/second.png")
        self.assertEqual(len(result["history"]), 1)
        self.assertEqual(result["history"][0]["value"], "https://x/first.png")
        self.assertEqual(result["history"][0]["input_reference_url"], "https://x/reference.png")
        self.assertNotIn("task_id", result["history"][0])
        resaved = self.client.post("/api/ai/graphs", json=loaded).json()
        self.assertEqual(resaved["graph_id_string"], graph_id)

    def test_history_is_limited_to_five_typed_non_inline_entries(self):
        saved = self.client.post("/api/ai/graphs", json=self._payload()).json()
        graph_id = saved["graph_id_string"]
        six = [
            {"type": "image", "value": f"https://x/{index}.png", "created_at": index}
            for index in range(6)
        ]
        response = self.client.put(
            f"/api/ai/graphs/{graph_id}/results",
            json={"b": {"status": "done", "type": "image",
                        "value": "https://x/current.png", "history": six}},
        )
        self.assertEqual(response.status_code, 422)
        inline = self.client.put(
            f"/api/ai/graphs/{graph_id}/results",
            json={"b": {"status": "done", "type": "image",
                        "value": "https://x/current.png",
                        "history": [{"type": "image", "value": "data:image/png;base64,AAAA"}]}},
        )
        self.assertEqual(inline.status_code, 422)

    def test_automatic_history_keeps_only_the_five_most_recent_values(self):
        payload = self._payload()
        payload["instance_id"] = "bounded-history"
        graph_id = self.client.post("/api/ai/graphs", json=payload).json()["graph_id_string"]
        for index in range(7):
            response = self.client.put(
                f"/api/ai/graphs/{graph_id}/results",
                json={"b": {"status": "done", "type": "image",
                            "value": f"https://x/current-{index}.png"}},
            )
            self.assertEqual(response.status_code, 200)
        result = self.client.get(
            f"/api/ai/graphs/{graph_id}"
        ).json()["graph_object"]["results"]["b"]
        self.assertEqual(len(result["history"]), 5)
        self.assertEqual(
            [entry["value"] for entry in result["history"]],
            [f"https://x/current-{index}.png" for index in range(1, 6)],
        )

    def test_a_graph_that_could_not_run_is_not_saved(self):
        payload = self._payload()
        payload["links"][0]["input"] = "image"
        response = self.client.post("/api/ai/graphs", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(len(list(Path(self._dir.name).glob("*.json"))), 0)

    def test_a_run_is_kept_with_the_graph_so_a_link_shows_it(self):
        saved = self.client.post("/api/ai/graphs", json=self._payload()).json()
        graph_id = saved["graph_id_string"]
        response = self.client.put(
            f"/api/ai/graphs/{graph_id}/results",
            json={"b": {"status": "done", "type": "image",
                        "value": "https://x/y.png", "task_id": "t1"}})
        self.assertEqual(response.status_code, 200)
        loaded = self.client.get("/api/ai/graphs/" + graph_id).json()
        result = loaded["graph_object"]["results"]["b"]
        self.assertEqual(result["value"], "https://x/y.png")
        self.assertEqual(result["status"], "done")

    def test_an_unfinished_task_keeps_its_id_so_the_link_can_carry_on(self):
        saved = self.client.post("/api/ai/graphs", json=self._payload()).json()
        graph_id = saved["graph_id_string"]
        self.client.put(f"/api/ai/graphs/{graph_id}/results",
                        json={"b": {"status": "running", "type": "image",
                                    "value": "https://x/pending.png",
                                    "task_id": "task-42"}})
        loaded = self.client.get("/api/ai/graphs/" + graph_id).json()
        self.assertEqual(loaded["graph_object"]["results"]["b"]["task_id"], "task-42")

    def test_recording_a_run_does_not_move_the_link(self):
        """A link shared while a clip renders has to stay the right link."""
        first = self.client.post("/api/ai/graphs", json=self._payload()).json()
        payload = self._payload()
        payload["results"] = {"b": {"status": "done", "type": "image",
                                    "value": "https://x/y.png"}}
        second = self.client.post("/api/ai/graphs", json=payload).json()
        self.assertEqual(first["graph_id_string"], second["graph_id_string"])

    def test_a_result_for_a_node_that_is_not_in_the_graph_is_refused(self):
        saved = self.client.post("/api/ai/graphs", json=self._payload()).json()
        response = self.client.put(
            f"/api/ai/graphs/{saved['graph_id_string']}/results",
            json={"ghost": {"status": "done", "type": "image", "value": "x"}})
        self.assertEqual(response.status_code, 400)

    def test_results_for_a_graph_nobody_saved_are_refused(self):
        response = self.client.put("/api/ai/graphs/nothinghere/results",
                                   json={"b": {"status": "done"}})
        self.assertEqual(response.status_code, 404)

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
                self.assertIn(param["type"],
                              ("select", "range", "number", "text", "textarea", "model"))
                self.assertTrue(param["name"])
                self.assertTrue(param["title"])
                if param["type"] == "model":
                    self.assertIn(param["source"], ("checkpoints", "loras"))
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


class FleetSnapshotTests(unittest.TestCase):
    """The fleet strip is only useful if the nodes in it are told apart."""

    def test_render_workers_keep_their_own_names(self):
        import ai_fleet
        nodes = [ai_fleet.render_node({"render_server_name": "f5", "status": "online"}),
                 ai_fleet.render_node({"render_server_name": "Raptor", "status": "online"})]
        self.assertEqual([node["id"] for node in nodes], ["f5", "Raptor"])

    def test_a_worker_holding_a_task_is_busy_even_while_it_says_online(self):
        import ai_fleet
        node = ai_fleet.render_node({"render_server_name": "f5", "status": "online",
                                     "current_render_task": "abc"})
        self.assertTrue(node["busy"])
        self.assertTrue(node["online"])

    def test_a_worker_with_a_queue_is_busy(self):
        import ai_fleet
        self.assertTrue(ai_fleet.render_node(
            {"render_server_name": "f5", "status": "online", "queue_size": 2})["busy"])

    def test_an_idle_worker_is_online_and_not_busy(self):
        import ai_fleet
        node = ai_fleet.render_node({"render_server_name": "f5", "status": "online"})
        self.assertTrue(node["online"])
        self.assertFalse(node["busy"])

    def test_an_offline_worker_is_neither(self):
        import ai_fleet
        node = ai_fleet.render_node({"render_server_name": "f5", "status": "offline"})
        self.assertFalse(node["online"])
        self.assertFalse(node["busy"])


class ActivityTests(unittest.TestCase):
    """A busy dot says which kind of work is on the card."""

    def test_a_render_worker_shows_what_its_held_task_is(self):
        import ai_fleet
        tasks = {"t1": {"task_id": "t1", "workflow": "gen_animation_by_url.json"}}
        node = ai_fleet.render_node(
            {"render_server_name": "f5", "status": "online", "current_render_task": "t1"},
            tasks)
        self.assertEqual(node["activity"], "video")

    def test_a_picture_and_a_clip_are_told_apart(self):
        import ai_fleet
        tasks = {"t1": {"task_id": "t1", "workflow": "gen_image.json"}}
        node = ai_fleet.render_node(
            {"render_server_name": "f5", "status": "online", "current_render_task": "t1"},
            tasks)
        self.assertEqual(node["activity"], "image")

    def test_an_idle_worker_claims_no_activity(self):
        import ai_fleet
        node = ai_fleet.render_node({"render_server_name": "f5", "status": "online"}, {})
        self.assertEqual(node["activity"], "")

    def test_hunyuan_on_a_converter_reads_as_3d_work(self):
        import ai_vision_api
        found = ai_vision_api._activities(
            {"processing_tasks": [{"workload_class": "hunyuan"}]})
        self.assertEqual(found, ["3dmodel"])

    def test_an_ai_task_is_split_into_vision_and_text(self):
        import ai_vision_api
        found = ai_vision_api._activities({"processing_tasks": [
            {"workload_class": "ai_vision", "mode": "vision"},
            {"workload_class": "ai_vision", "mode": "text"}]})
        self.assertEqual(found, ["vision", "text"])

    def test_the_farms_own_conversion_work_is_visible_too(self):
        import ai_vision_api
        found = ai_vision_api._activities(
            {"processing_tasks": [{"workload_class": "autorig_interactive"}]})
        self.assertEqual(found, ["conversion"])

    def test_a_workload_nobody_named_still_shows_as_occupied(self):
        import ai_vision_api
        found = ai_vision_api._activities(
            {"processing_tasks": [{"workload_class": "something_new"}]})
        self.assertEqual(found, ["conversion"])

    def test_an_idle_node_reports_nothing(self):
        import ai_vision_api
        self.assertEqual(ai_vision_api._activities({"processing_tasks": []}), [])


class RejectionMessageTests(unittest.TestCase):
    """A refusal should say why, because the reason is usually actionable."""

    def _submit(self, status_code, payload=None, text=""):
        import ai_vision_api
        import asyncio

        class Response:
            status_code = None
            def json(self):
                if payload is None:
                    raise ValueError("no body")
                return payload
            @property
            def text(self):
                return text

        response = Response()
        response.status_code = status_code

        class Client:
            async def post(self, *args, **kwargs):
                return response

        worker = {"name": "f13", "url": "http://x", "token": "t",
                  "physical_node": "f13"}
        return asyncio.run(
            ai_vision_api._submit(Client(), worker, "/generate-3d", {}))

    def test_the_nodes_own_reason_reaches_the_caller(self):
        with self.assertRaises(Exception) as caught:
            self._submit(400, {"error": "node is in maintenance"})
        message = caught.exception.detail["message_string"]
        self.assertIn("node is in maintenance", message)
        self.assertIn("f13", message)

    def test_a_silent_refusal_still_names_the_node_and_the_code(self):
        with self.assertRaises(Exception) as caught:
            self._submit(400, None, text="")
        message = caught.exception.detail["message_string"]
        self.assertIn("f13", message)
        self.assertIn("400", message)


class TextProcessingTests(unittest.TestCase):
    """The text service takes an instruction and the material separately."""

    def test_an_instruction_and_a_text_are_joined_with_a_marker(self):
        import ai_vision_api
        body = ai_vision_api.TextRequest(prompt="Summarise this.", input="A long story.")
        combined = body.combined_prompt()
        self.assertTrue(combined.startswith("Summarise this."))
        self.assertIn("A long story.", combined)
        self.assertIn("--- text ---", combined)

    def test_an_instruction_alone_is_sent_unchanged(self):
        import ai_vision_api
        self.assertEqual(
            ai_vision_api.TextRequest(prompt="Name three colours.").combined_prompt(),
            "Name three colours.")

    def test_material_alone_is_sent_unchanged(self):
        import ai_vision_api
        self.assertEqual(
            ai_vision_api.TextRequest(input="Just this.").combined_prompt(), "Just this.")

    def test_the_service_declares_both_of_them(self):
        fields = {item["field"] for item in ai_services.service("text")["inputs"]}
        self.assertEqual(fields, {"prompt", "input"})

    def test_the_instruction_can_be_typed_on_the_node_instead_of_wired(self):
        names = {param["name"] for param in ai_services.params_for("text")}
        self.assertIn("prompt", names)


class CachePurgeTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._out = tempfile.TemporaryDirectory()
        self._graphs, self._outputs = ai_graph.GRAPH_DIR, ai_graph.RENDER_OUTPUT_DIR
        ai_graph.GRAPH_DIR = Path(self._dir.name)
        ai_graph.RENDER_OUTPUT_DIR = Path(self._out.name)
        self.client = _client()

    def tearDown(self):
        ai_graph.GRAPH_DIR, ai_graph.RENDER_OUTPUT_DIR = self._graphs, self._outputs
        self._dir.cleanup()
        self._out.cleanup()

    def _store(self, graph_id, values):
        import json as _json
        results = {str(i): {"status": "done", "type": "image", "value": v}
                   for i, v in enumerate(values)}
        (Path(self._dir.name) / f"{graph_id}.json").write_text(
            _json.dumps({"id": graph_id, "graph": {"nodes": [], "links": [],
                                                   "results": results}}),
            encoding="utf-8")

    def _output(self, relative, data=b"0123456789"):
        path = Path(self._out.name) / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def test_the_files_a_composition_produced_are_really_gone(self):
        mine = self._output("default_user/clip.mp4")
        self._store("abc123", ["https://autorig.online/renderfin/render/default_user/clip.mp4"])
        body = self.client.delete("/api/ai/cache").json()
        self.assertEqual(body["graphs_removed_int"], 1)
        self.assertEqual(body["files_removed_int"], 1)
        self.assertEqual(body["bytes_freed_int"], 10)
        self.assertFalse(mine.exists())
        self.assertEqual(list(Path(self._dir.name).glob("*.json")), [])

    def test_a_render_no_composition_claims_is_left_alone(self):
        """The render tree is shared with the rest of the site."""
        someone_else = self._output("default_user/not-mine.png")
        self._store("abc123", ["https://autorig.online/renderfin/render/default_user/mine.png"])
        self._output("default_user/mine.png")
        self.client.delete("/api/ai/cache")
        self.assertTrue(someone_else.exists())

    def test_a_value_that_tries_to_walk_out_of_the_tree_is_ignored(self):
        outside = Path(self._out.name).parent / "escape.txt"
        outside.write_bytes(b"x")
        try:
            self._store("abc123", [
                "https://autorig.online/renderfin/render/../escape.txt"])
            self.client.delete("/api/ai/cache")
            self.assertTrue(outside.exists())
        finally:
            if outside.exists():
                outside.unlink()

    def test_a_text_answer_names_no_file_and_removes_none(self):
        self._store("abc123", ["just some words the model wrote"])
        body = self.client.delete("/api/ai/cache").json()
        self.assertEqual(body["files_removed_int"], 0)
        self.assertEqual(body["graphs_removed_int"], 1)

    def test_purging_an_empty_cache_is_not_an_error(self):
        body = self.client.delete("/api/ai/cache").json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["files_removed_int"], 0)


class CancelTests(unittest.TestCase):
    def setUp(self):
        self.client = _client()

    def test_a_converter_task_is_reported_as_left_running(self):
        """The converter has no way to stop one, so it is counted, not claimed."""
        body = self.client.post("/api/ai/cancel",
                                json={"task_ids": ["f13.abc"]}).json()
        self.assertEqual(body["running_int"], 1)
        self.assertEqual(body["cancelled_int"], 0)

    def test_an_empty_request_is_harmless(self):
        body = self.client.post("/api/ai/cancel", json={"task_ids": []}).json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["cancelled_int"], 0)


class VisionPromptTests(unittest.TestCase):
    """A Vision node can hold its own question.

    Wiring a whole Text node in just to carry a fixed sentence was the
    commonest way to submit a request with no prompt, which the API rejects
    with a validation error that used to surface as a bare "HTTP 422".
    """

    def test_the_question_can_be_typed_on_the_node(self):
        names = {param["name"] for param in ai_services.params_for("vision")}
        self.assertIn("prompt", names)

    def test_the_typed_question_has_a_usable_default(self):
        prompt = next(p for p in ai_services.params_for("vision") if p["name"] == "prompt")
        self.assertTrue(str(prompt["default"]).strip())
        self.assertEqual(prompt["type"], "textarea")

    def test_only_the_picture_is_required_as_a_wire(self):
        required = {i["field"] for i in ai_services.service("vision")["inputs"]
                    if i["required"]}
        self.assertEqual(required, {"image"})

    def test_a_wired_question_is_still_offered(self):
        fields = {i["field"] for i in ai_services.service("vision")["inputs"]}
        self.assertIn("prompt", fields)


class OutputBudgetTests(unittest.TestCase):
    """How many tokens the answer may take, when nobody says."""

    def setUp(self):
        import ai_vision_api
        self.api = ai_vision_api

    def test_a_reasoning_model_gets_a_bigger_budget_than_a_plain_one(self):
        """Reasoning is charged to the same budget, so it needs headroom."""
        thinking = next(m for m in self.api.AI_MODELS if m.get("reasons_first"))
        plain = next(m for m in self.api.AI_MODELS if not m.get("reasons_first"))
        self.assertGreater(self.api._output_budget(thinking, None),
                           self.api._output_budget(plain, None))

    def test_zero_means_automatic(self):
        model = self.api.AI_MODELS[0]
        self.assertEqual(self.api._output_budget(model, 0),
                         self.api._output_budget(model, None))

    def test_an_explicit_number_is_obeyed(self):
        self.assertEqual(self.api._output_budget(self.api.AI_MODELS[0], 333), 333)

    def test_every_model_declares_a_default(self):
        for model in self.api.AI_MODELS:
            self.assertGreater(int(model["default_output_tokens"]), 0, model["id"])

    def test_the_node_control_offers_automatic(self):
        """Zero has to be reachable on the control, or automatic is unusable."""
        for service in ("vision", "text"):
            param = next(p for p in ai_services.params_for(service)
                         if p["name"] == "max_output_tokens")
            self.assertEqual(param["default"], 0)
            self.assertEqual(param["min"], 0)
