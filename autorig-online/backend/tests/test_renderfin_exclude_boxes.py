"""A client can be kept off given boxes (2026-10-16).

The civitai.red autopost renders only on the free boxes while worker-4090 is the
owner's R&D card and f12 / f5 / Raptor train LoRAs. RENDERFIN_EXCLUDE_BOXES_FOR
names a client (prompt.priority_class or user_name) and the boxes it must never
run on; empty keeps today's behaviour.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ai_graph_context
from renderfin import config
from renderfin.models import TASK_PENDING, RenderPrompt, RenderServer, RenderTask
from renderfin.queue import RenderQueue, excluded_boxes
from renderfin.registry import ServerRegistry


def _task(task_id, *, user_name="tester", priority_class=""):
    return RenderTask(id=task_id, prompt=RenderPrompt(prompt="x", user_name=user_name, priority_class=priority_class),
                      status=TASK_PENDING, created_at=1.0, workflow="gen_image.json", queue_class="interactive")


def _server(name):
    return RenderServer(render_server_name=name, render_server_url=f"http://{name}:8188", status="online",
                        available_workflows=["gen_image.json"])


class ParseTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(config.parse_exclude_boxes(""), {})
        self.assertEqual(config.parse_exclude_boxes("red_autopost:worker-4090, f12;sura-bot:f5;bad;x:"),
                         {"red_autopost": {"worker-4090", "f12"}, "sura-bot": {"f5"}})

    def test_key_by_class_then_user(self):
        rules = {"red_autopost": {"f12"}, "sura-bot": {"f5"}}
        with patch.object(config, "EXCLUDE_BOXES_FOR", rules):
            self.assertEqual(excluded_boxes(_task("a", priority_class="red_autopost")), ("red_autopost", {"f12"}))
            self.assertEqual(excluded_boxes(_task("b", user_name="sura-bot")), ("sura-bot", {"f5"}))
            self.assertEqual(excluded_boxes(_task("c")), ("", set()))
        with patch.object(config, "EXCLUDE_BOXES_FOR", {}):
            self.assertEqual(excluded_boxes(_task("a", priority_class="red_autopost")), ("", set()))


class QueueTests(unittest.TestCase):
    def test_excluded_box_is_never_picked(self):
        with tempfile.TemporaryDirectory() as root:
            patches = [patch.object(config, "DATA_DIR", Path(root)),
                       patch.object(config, "RENDER_DIR", Path(root) / "render"),
                       patch.object(config, "DB_DIR", Path(root) / "db"),
                       patch.object(config, "TMP_DIR", Path(root) / "tmp"),
                       patch.object(config, "SERVERS_DIR", Path(root) / "servers"),
                       patch.object(config, "DB_PATH", Path(root) / "db" / "renderfin.db"),
                       patch.object(config, "AUTORIG_PRIORITY_BOXES", frozenset()),
                       patch.object(config, "OOM_RECOVERY_ENABLED", False),
                       patch.object(config, "EXCLUDE_BOXES_FOR", {"red_autopost": {"worker-4090", "f12"}})]
            for p in patches:
                p.start()
            try:
                registry = ServerRegistry()
                for name in ("worker-4090", "f12", "f15"):
                    registry.save(_server(name))
                queue = RenderQueue(registry, db_path=Path(root) / "renderfin.db")
                ours = _task("ours", priority_class="red_autopost")
                other = _task("other", user_name="sura-bot")
                queue._tasks = {ours.id: ours, other.id: other}
                for box in ("worker-4090", "f12"):
                    server = queue.registry.get(box)
                    self.assertEqual(queue._exclusion_reason(server, ours.workflow, {}, None, 100.0, task=ours),
                                     ("excluded-for-client", "red_autopost"))
                    # relaxing the soft filters never lifts it
                    self.assertEqual(queue._exclusion_reason(server, ours.workflow, {}, None, 100.0, task=ours,
                                                             relax_soft=True),
                                     ("excluded-for-client", "red_autopost"))
                    self.assertIsNone(queue._exclusion_reason(server, other.workflow, {}, None, 100.0, task=other))
                self.assertIsNone(queue._exclusion_reason(queue.registry.get("f15"), ours.workflow, {}, None, 100.0,
                                                          task=ours))
            finally:
                for p in patches:
                    p.stop()


class HeaderTests(unittest.TestCase):
    def test_internal_client_class_reaches_the_payload(self):
        ai_graph_context.set_from_headers({"x-autorig-priority": "red_autopost"})
        self.assertEqual(ai_graph_context.fields(), {"priority_class": "red_autopost"})
        ai_graph_context.set_from_headers({"x-autorig-priority": "autorig"})
        self.assertEqual(ai_graph_context.fields(), {"priority_class": "autorig"})

    def test_proxied_or_malformed_class_is_ignored(self):
        ai_graph_context.set_from_headers({"x-autorig-priority": "red_autopost", "x-real-ip": "1.2.3.4"})
        self.assertEqual(ai_graph_context.fields(), {})
        ai_graph_context.set_from_headers({"x-autorig-priority": "Bad Class!"})
        self.assertEqual(ai_graph_context.fields(), {})
        ai_graph_context.set_from_headers({})
        self.assertEqual(ai_graph_context.fields(), {})


if __name__ == "__main__":
    unittest.main()
