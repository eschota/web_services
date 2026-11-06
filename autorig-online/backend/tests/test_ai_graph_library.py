"""The workflow library: what `/workflows` is allowed to say about a saved graph.

The listing is the one place where every stored composition is read at once and
shown to somebody who did not build it, so these tests care as much about what
never leaves the server — history, task ids, error text — as about the cards
themselves.
"""

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


def _input(node_id, entity_type, value=""):
    return {"id": node_id, "kind": ai_graph.NODE_INPUT, "entity_type": entity_type,
            "value": value, "x": 0, "y": 0, "params": {}}


def _service(node_id, service):
    return {"id": node_id, "kind": ai_graph.NODE_SERVICE, "service": service,
            "x": 0, "y": 0, "params": {}}


def _link(source, target):
    return {"from": source, "output": "value", "to": target, "input": "image"}


def _result(status, entity_type, value, **extra):
    record = {"status": status, "type": entity_type, "value": value,
              "task_id": "", "error": "", "started_at": 0,
              "input_reference_url": "", "history": []}
    record.update(extra)
    return record


class LibraryTestCase(unittest.TestCase):
    """Each test owns its own store, and none of them leave a cache behind."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self._previous = ai_graph.GRAPH_DIR
        ai_graph.GRAPH_DIR = Path(self._dir.name)
        self._reset_cache()
        app = FastAPI()
        app.include_router(ai_graph.router)
        self.client = TestClient(app)

    def tearDown(self):
        ai_graph.GRAPH_DIR = self._previous
        self._reset_cache()
        self._dir.cleanup()

    @staticmethod
    def _reset_cache():
        ai_graph._library_index["key"] = None
        ai_graph._library_index["rows"] = []
        ai_graph._library_summaries.clear()

    def write(self, graph_id, *, name="Untitled", nodes=(), links=(),
              results=None, saved_at=1000, results_at=0):
        stored = {
            "id": graph_id,
            "saved_at_unix_int": saved_at,
            "graph": {"name": name, "nodes": list(nodes), "links": list(links),
                      "results": dict(results or {})},
        }
        if results_at:
            stored["results_at_unix_int"] = results_at
        (ai_graph.GRAPH_DIR / f"{graph_id}.json").write_text(
            json.dumps(stored, ensure_ascii=False, indent=2), encoding="utf-8")

    def library(self, **params):
        response = self.client.get("/api/ai/graphs", params=params)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success_bool"])
        return payload

    def only(self, **params):
        payload = self.library(**params)
        self.assertEqual(payload["total_int"], 1)
        return payload["graphs_array"][0]


class EmptyStoreTests(LibraryTestCase):
    def test_a_store_with_nothing_in_it_lists_nothing(self):
        payload = self.library()
        self.assertEqual(payload["total_int"], 0)
        self.assertEqual(payload["graphs_array"], [])

    def test_a_store_that_does_not_exist_yet_is_not_an_error(self):
        ai_graph.GRAPH_DIR = Path(self._dir.name) / "never-created"
        payload = self.library()
        self.assertEqual(payload["total_int"], 0)

    def test_a_file_that_is_not_a_graph_is_skipped_rather_than_fatal(self):
        (ai_graph.GRAPH_DIR / "broken.json").write_text("{not json", encoding="utf-8")
        self.write("aaaaaaaaaaaa", name="Real one")
        payload = self.library()
        self.assertEqual(payload["total_int"], 1)
        self.assertEqual(payload["graphs_array"][0]["name_string"], "Real one")


class OrderingTests(LibraryTestCase):
    def test_the_newest_run_comes_first_even_when_it_was_saved_first(self):
        self.write("aaaaaaaaaaaa", name="old save, fresh run",
                   saved_at=100, results_at=9000)
        self.write("bbbbbbbbbbbb", name="newer save, never run", saved_at=5000)
        self.write("cccccccccccc", name="oldest", saved_at=50)
        names = [row["name_string"] for row in self.library()["graphs_array"]]
        self.assertEqual(names, ["old save, fresh run", "newer save, never run", "oldest"])

    def test_a_page_is_a_window_onto_the_same_order(self):
        for index in range(5):
            self.write(f"{index:012d}", name=f"graph {index}", saved_at=100 + index)
        payload = self.library(limit=2, offset=1)
        self.assertEqual(payload["total_int"], 5)
        self.assertEqual([row["name_string"] for row in payload["graphs_array"]],
                         ["graph 3", "graph 2"])

    def test_a_caller_cannot_ask_for_the_whole_store_at_once(self):
        self.write("aaaaaaaaaaaa")
        payload = self.library(limit=100000)
        self.assertEqual(payload["limit_int"], ai_graph.LIBRARY_LIMIT_MAX)

    def test_a_nonsense_offset_is_read_as_the_beginning(self):
        self.write("aaaaaaaaaaaa", name="only")
        payload = self.library(offset=-5)
        self.assertEqual(payload["offset_int"], 0)
        self.assertEqual(len(payload["graphs_array"]), 1)

    def test_a_graph_saved_later_is_picked_up_without_a_restart(self):
        self.write("aaaaaaaaaaaa", name="first", saved_at=100)
        self.assertEqual(self.library()["total_int"], 1)
        self.write("bbbbbbbbbbbb", name="second", saved_at=200)
        payload = self.library()
        self.assertEqual(payload["total_int"], 2)
        self.assertEqual(payload["graphs_array"][0]["name_string"], "second")


class SearchTests(LibraryTestCase):
    def setUp(self):
        super().setUp()
        self.write("aaaaaaaaaaaa", name="Portrait pipeline", saved_at=300)
        self.write("bbbbbbbbbbbb", name="Video motion transfer", saved_at=200)
        self.write("cccccccccccc", name="portrait to 3D", saved_at=100)

    def test_the_search_ignores_case(self):
        payload = self.library(q="PORTRAIT")
        self.assertEqual(payload["total_int"], 2)
        self.assertEqual([row["name_string"] for row in payload["graphs_array"]],
                         ["Portrait pipeline", "portrait to 3D"])

    def test_the_search_matches_anywhere_in_the_name(self):
        self.assertEqual(self.library(q="motion")["total_int"], 1)

    def test_a_search_that_matches_nothing_is_an_empty_page_not_an_error(self):
        payload = self.library(q="zzzz")
        self.assertEqual(payload["total_int"], 0)
        self.assertEqual(payload["graphs_array"], [])

    def test_a_blank_search_is_no_search(self):
        self.assertEqual(self.library(q="  ")["total_int"], 3)


class CountTests(LibraryTestCase):
    def test_a_card_says_what_the_graph_is_made_of(self):
        self.write(
            "aaaaaaaaaaaa", name="mixed",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/a.png"),
                   _input("2", ai_services.TEXT, "draw something"),
                   _service("3", "image"), _service("4", "image"),
                   _service("5", "video")],
            links=[_link("1", "3"), _link("3", "4"), _link("4", "5")],
            results={"3": _result("done", ai_services.IMAGE, "https://autorig.online/3.png"),
                     "4": _result("running", "", ""),
                     "5": _result("failed", "", "", error="the farm said no")},
        )
        row = self.only()
        self.assertEqual(row["node_count_int"], 5)
        self.assertEqual(row["link_count_int"], 3)
        self.assertEqual(row["services_object"],
                         {"input:image": 1, "input:text": 1, "image": 2, "video": 1})
        self.assertEqual(row["result_counts_object"],
                         {"done": 1, "running": 1, "failed": 1})

    def test_the_deep_link_reopens_the_graph_in_the_editor(self):
        self.write("abcdef012345", name="linked")
        row = self.only()
        self.assertEqual(row["graph_id_string"], "abcdef012345")
        self.assertEqual(row["deep_link_string"], "/nodes?g=abcdef012345")


class PreviewTests(LibraryTestCase):
    def test_the_supplied_picture_is_the_input_and_the_final_clip_the_output(self):
        self.write(
            "aaaaaaaaaaaa", name="picture to clip",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image"), _service("3", "video")],
            links=[_link("1", "2"), _link("2", "3")],
            results={"2": _result("done", ai_services.IMAGE, "https://autorig.online/mid.png"),
                     "3": _result("done", ai_services.VIDEO, "https://autorig.online/out.mp4")},
        )
        row = self.only()
        self.assertEqual(row["preview_a_object"]["value_string"], "https://autorig.online/in.png")
        self.assertEqual(row["preview_a_object"]["type_string"], ai_services.IMAGE)
        self.assertEqual(row["preview_a_object"]["label_string"], "input")
        self.assertEqual(row["preview_a_object"]["node_id_string"], "1")
        self.assertEqual(row["preview_b_object"]["value_string"], "https://autorig.online/out.mp4")
        self.assertEqual(row["preview_b_object"]["type_string"], ai_services.VIDEO)
        self.assertEqual(row["preview_b_object"]["label_string"], "output")

    def test_a_picture_input_wins_over_a_video_input(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.VIDEO, "https://autorig.online/drive.mp4"),
                   _input("2", ai_services.IMAGE, "https://autorig.online/face.png")],
        )
        self.assertEqual(self.only()["preview_a_object"]["value_string"],
                         "https://autorig.online/face.png")

    def test_a_driving_video_stands_in_when_no_picture_was_supplied(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.TEXT, "a prompt"),
                   _input("2", ai_services.VIDEO, "https://autorig.online/drive.mp4")],
        )
        preview = self.only()["preview_a_object"]
        self.assertEqual(preview["type_string"], ai_services.VIDEO)
        self.assertEqual(preview["value_string"], "https://autorig.online/drive.mp4")

    def test_an_unfilled_input_is_not_shown_as_the_input(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, ""), _service("2", "image")],
            links=[_link("1", "2")],
            results={"2": _result("done", ai_services.IMAGE, "https://autorig.online/made.png")},
        )
        preview = self.only()["preview_a_object"]
        self.assertEqual(preview["node_id_string"], "2")
        self.assertEqual(preview["value_string"], "https://autorig.online/made.png")

    def test_a_graph_that_begins_with_a_prompt_shows_what_it_made_first(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.TEXT, "draw a house"),
                   _service("2", "image"), _service("3", "video")],
            links=[_link("1", "2"), _link("2", "3")],
            results={"2": _result("done", ai_services.IMAGE, "https://autorig.online/first.png"),
                     "3": _result("done", ai_services.VIDEO, "https://autorig.online/last.mp4")},
        )
        row = self.only()
        self.assertEqual(row["preview_a_object"]["value_string"], "https://autorig.online/first.png")
        self.assertEqual(row["preview_b_object"]["value_string"], "https://autorig.online/last.mp4")

    def test_the_sink_is_the_output_even_when_an_earlier_node_finished_later(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image"), _service("3", "3dmodel"), _service("4", "vision")],
            links=[_link("1", "2"), _link("2", "3"), _link("2", "4"), _link("4", "3")],
            results={"2": _result("done", ai_services.IMAGE, "https://autorig.online/mid.png"),
                     "3": _result("done", ai_services.MODEL3D, "https://autorig.online/out.glb"),
                     "4": _result("done", ai_services.TEXT, "a description")},
        )
        preview = self.only()["preview_b_object"]
        self.assertEqual(preview["node_id_string"], "3")
        self.assertEqual(preview["type_string"], ai_services.MODEL3D)

    def test_text_is_an_acceptable_output_and_is_cut_to_a_card_sized_quote(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "vision")],
            links=[_link("1", "2")],
            results={"2": _result("done", ai_services.TEXT, "word " * 200)},
        )
        preview = self.only()["preview_b_object"]
        self.assertEqual(preview["type_string"], ai_services.TEXT)
        self.assertLessEqual(len(preview["value_string"]), ai_graph.PREVIEW_TEXT_LIMIT)
        self.assertTrue(preview["value_string"].startswith("word word"))
        self.assertTrue(preview["value_string"].endswith("…"))

    def test_a_finished_result_outranks_a_stale_one_further_down(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image"), _service("3", "video")],
            links=[_link("1", "2"), _link("2", "3")],
            results={"2": _result("done", ai_services.IMAGE, "https://autorig.online/kept.png"),
                     "3": _result("stale", ai_services.VIDEO, "https://autorig.online/old.mp4")},
        )
        self.assertEqual(self.only()["preview_b_object"]["node_id_string"], "2")

    def test_a_stale_result_is_still_better_than_no_output_at_all(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image")],
            links=[_link("1", "2")],
            results={"2": _result("stale", ai_services.IMAGE, "https://autorig.online/old.png")},
        )
        self.assertEqual(self.only()["preview_b_object"]["value_string"],
                         "https://autorig.online/old.png")

    def test_a_graph_that_has_never_run_has_an_input_and_no_output(self):
        self.write(
            "aaaaaaaaaaaa", name="never run",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image")],
            links=[_link("1", "2")],
        )
        row = self.only()
        self.assertIsNotNone(row["preview_a_object"])
        self.assertIsNone(row["preview_b_object"])

    def test_a_running_node_is_not_mistaken_for_an_output(self):
        self.write(
            "aaaaaaaaaaaa",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "video")],
            links=[_link("1", "2")],
            results={"2": _result("running", "", "", task_id="41")},
        )
        row = self.only()
        self.assertIsNone(row["preview_b_object"])
        self.assertEqual(row["result_counts_object"], {"running": 1})

    def test_an_empty_graph_has_no_previews_and_still_gets_a_card(self):
        self.write("aaaaaaaaaaaa", name="blank")
        row = self.only()
        self.assertIsNone(row["preview_a_object"])
        self.assertIsNone(row["preview_b_object"])
        self.assertEqual(row["node_count_int"], 0)


class PrivacyTests(LibraryTestCase):
    """A listing says what a composition is, never how its run went wrong."""

    def setUp(self):
        super().setUp()
        self.write(
            "aaaaaaaaaaaa", name="with a past",
            nodes=[_input("1", ai_services.IMAGE, "https://autorig.online/in.png"),
                   _service("2", "image"), _service("3", "video")],
            links=[_link("1", "2"), _link("2", "3")],
            results={
                "2": _result(
                    "done", ai_services.IMAGE, "https://autorig.online/now.png",
                    task_id="secret-task-9",
                    history=[{"type": ai_services.IMAGE,
                              "value": "https://autorig.online/before.png",
                              "input_reference_url": "", "created_at": 1}],
                ),
                "3": _result("failed", "", "", task_id="secret-task-10",
                             error="worker f13 refused: out of memory"),
            },
        )

    def test_neither_history_nor_task_ids_nor_errors_reach_the_listing(self):
        body = self.client.get("/api/ai/graphs").text
        for leak in ("history", "secret-task-9", "secret-task-10",
                     "out of memory", "before.png", "input_reference_url"):
            self.assertNotIn(leak, body)

    def test_the_failure_is_still_counted_so_a_card_can_say_so(self):
        self.assertEqual(self.only()["result_counts_object"], {"done": 1, "failed": 1})


class RouteTests(LibraryTestCase):
    def test_the_listing_does_not_shadow_a_single_graph(self):
        self.write("abcdef012345", name="one")
        loaded = self.client.get("/api/ai/graphs/abcdef012345")
        self.assertEqual(loaded.status_code, 200)
        self.assertEqual(loaded.json()["graph_object"]["name"], "one")

    def test_the_listing_never_writes_to_the_store(self):
        self.write("abcdef012345", name="one")
        path = ai_graph.GRAPH_DIR / "abcdef012345.json"
        before = (path.read_bytes(), sorted(p.name for p in ai_graph.GRAPH_DIR.iterdir()))
        self.library()
        self.library(q="one")
        after = (path.read_bytes(), sorted(p.name for p in ai_graph.GRAPH_DIR.iterdir()))
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
