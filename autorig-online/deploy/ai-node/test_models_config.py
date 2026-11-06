"""Unit tests for the multi-model config of ai_node.py (2026-11-06, Qwen3.5-4B A/B).

No GPU, no network, no subprocess:  python -m unittest test_models_config  (or pytest).
The end-to-end contract stays covered by selftest.py.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

import ai_node

NINE = "qwen35-9b-uncensored"
FOUR = "qwen35-4b"
FLAGS_4B = ["-np", "2", "-b", "2048", "-ub", "1024", "-fa", "on", "-ctk", "q8_0", "-ctv", "q8_0",
            "--image-min-tokens", "64", "--image-max-tokens", "1024"]


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="ainode-models-")
        self.models = os.path.join(self.dir, "models")
        os.makedirs(os.path.join(self.models, "qwen35-4b"))
        self.exe = os.path.join(self.dir, "llama-server.exe")
        for path in (self.exe,
                     os.path.join(self.models, "qwen35-9b-uncensored-Q4_K_M.gguf"),
                     os.path.join(self.models, "qwen35-9b-uncensored-mmproj-BF16.gguf")):
            open(path, "wb").close()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def install_4b(self):
        for name in ("Qwen3.5-4B-Q4_K_M.gguf", "mmproj-F16.gguf"):
            open(os.path.join(self.models, "qwen35-4b", name), "wb").close()

    def cfg(self, **raw):
        base = {"model_id": NINE, "llama_server": self.exe, "extra_llama_args": ["-fa", "on"]}
        base.update(raw)
        base = {k: v for k, v in base.items() if v is not None}
        return ai_node.Config(base, self.dir)


class ConfigTests(Base):
    def test_single_model_config_is_unchanged(self):
        cfg = self.cfg()
        self.assertEqual(list(cfg.profiles), [NINE])
        self.assertEqual(cfg.primary_model_id, NINE)
        self.assertEqual(cfg.model_id, NINE)
        self.assertEqual(cfg.context_tokens, 8192)
        self.assertEqual(cfg.reasoning, "off")
        self.assertEqual(cfg.extra_llama_args, ["-fa", "on"])
        argv = ai_node.LlamaServer(cfg).launch_argv()
        self.assertEqual(argv[:3], [self.exe, "-m", os.path.join(self.models, "qwen35-9b-uncensored-Q4_K_M.gguf")])
        self.assertEqual(argv[-2:], ["-fa", "on"])
        self.assertIn("--reasoning", argv)

    def test_models_list_adds_a_profile_with_its_own_flags(self):
        cfg = self.cfg(models=[{"model_id": FOUR, "extra_llama_args": FLAGS_4B}])
        self.assertEqual(list(cfg.profiles), [NINE, FOUR])
        self.assertEqual(cfg.model_id, NINE)                     # the primary stays selected
        self.assertEqual(cfg.extra_llama_args, ["-fa", "on"])    # 9B: node-wide flags only
        cfg.select_model(FOUR)
        self.assertEqual(cfg.context_tokens, 4096)
        self.assertEqual(cfg.weights, os.path.join(self.models, "qwen35-4b", "Qwen3.5-4B-Q4_K_M.gguf"))
        self.assertEqual(cfg.mmproj, os.path.join(self.models, "qwen35-4b", "mmproj-F16.gguf"))
        self.assertEqual(cfg.extra_llama_args, ["-fa", "on"] + FLAGS_4B)
        argv = ai_node.LlamaServer(cfg).launch_argv()
        self.assertEqual(argv[argv.index("--alias") + 1], FOUR)
        self.assertEqual(argv[argv.index("-c") + 1], "4096")
        self.assertEqual(argv[-len(FLAGS_4B):], FLAGS_4B)
        cfg.select_model(NINE)
        self.assertEqual(cfg.extra_llama_args, ["-fa", "on"])

    def test_models_only_config_takes_the_first_as_primary(self):
        cfg = self.cfg(model_id="", models=[{"model_id": FOUR}, {"model_id": NINE}])
        self.assertEqual(cfg.primary_model_id, FOUR)
        self.assertEqual(cfg.model_id, FOUR)

    def test_bad_configs_are_rejected(self):
        with self.assertRaises(ValueError):
            self.cfg(model_id="")
        with self.assertRaises(ValueError):
            self.cfg(models=[{"model_id": NINE}])                 # listed twice
        with self.assertRaises(ValueError):
            self.cfg(models=[{"model_id": FOUR, "llama_port": 1}])  # node-wide key in a profile
        with self.assertRaises(ValueError):
            self.cfg(models=[{"model_id": "mystery-7b"}])         # unknown model without weights
        with self.assertRaises(ValueError):
            self.cfg(models="qwen35-4b")

    def test_installed_ids_follow_the_files(self):
        cfg = self.cfg(models=[{"model_id": FOUR}])
        self.assertEqual(cfg.installed_model_ids(), [NINE])
        self.install_4b()
        self.assertEqual(cfg.installed_model_ids(), [NINE, FOUR])

    def test_stray_process_of_either_model_is_ours(self):
        cfg = self.cfg(models=[{"model_id": FOUR}])
        llama = ai_node.LlamaServer(cfg)
        four = cfg.profiles[FOUR]
        line = '%s -m %s --alias %s --port %d' % (self.exe, four["weights"], FOUR, cfg.llama_port)
        self.assertTrue(llama._is_ours(line))
        self.assertFalse(llama._is_ours(line.replace(str(cfg.llama_port), "9999")))
        self.assertFalse(llama._is_ours(line.replace("--alias " + FOUR, "--alias other")))


class NodeTests(Base):
    def node(self, cfg):
        node = ai_node.Node(cfg)
        node.refresh_comfy = lambda: {"busy": False}
        node.refresh_vram = lambda: {"ok": True, "free_mb": 20000}
        node.blocker = lambda *a: None
        return node

    def submit(self, node, model):
        body = {"prompt": "Name three colours.", "max_output_tokens": 64}
        if model is not None:
            body["model"] = model
        return node.submit("text", body, "http://127.0.0.1:5480")

    def test_submit_routes_each_installed_model(self):
        self.install_4b()
        node = self.node(self.cfg(models=[{"model_id": FOUR}]))
        code, body = self.submit(node, FOUR)
        self.assertEqual(code, 202, body)
        self.assertEqual(body["model"], FOUR)
        self.assertEqual(node.tasks[body["task_id"]].model, FOUR)
        code, body = self.submit(node, None)
        self.assertEqual((code, body["model"]), (202, NINE))     # no model named = the primary

    def test_submit_refuses_a_model_that_is_not_installed(self):
        node = self.node(self.cfg(models=[{"model_id": FOUR}]))
        code, body = self.submit(node, FOUR)
        self.assertEqual(code, 503)
        self.assertEqual(body["error"], "runtime_not_installed")
        self.assertEqual(body["available_models"], [NINE])
        code, body = self.submit(node, "bonsai2-27b")
        self.assertEqual((code, body["available_models"]), (503, [NINE]))

    def test_status_lists_every_installed_model(self):
        self.install_4b()
        node = self.node(self.cfg(models=[{"model_id": FOUR}]))
        with mock.patch.object(ai_node, "running_game", return_value=""):
            status = node.server_status()
        ids = [m["id"] for m in status["ai_models"]["models"]]
        self.assertEqual(ids, [NINE, FOUR])
        self.assertTrue(all(m["vision"] for m in status["ai_models"]["models"]))
        self.assertEqual(status["ai_models"]["system_prompt_models"], [])

    def test_task_for_the_other_model_swaps_the_weights(self):
        self.install_4b()
        cfg = self.cfg(models=[{"model_id": FOUR, "extra_llama_args": FLAGS_4B}])
        node = self.node(cfg)
        stops = []
        node.llama.running = lambda: True
        node.llama.stop = lambda reason: stops.append(reason) or True
        task = ai_node.AiTask(mode="text", prompt="x", system_prompt="", image_url="", model=FOUR,
                              requested_model=FOUR, max_output_tokens=64, queue_class="interactive", backend_task_id="")
        node._select_task_model(task)
        self.assertEqual(cfg.model_id, FOUR)
        self.assertEqual(stops, ["swap %s -> %s" % (NINE, FOUR)])
        self.assertEqual(node.swaps, 1)
        node._select_task_model(task)                            # same model: nothing happens
        self.assertEqual((len(stops), node.swaps), (1, 1))
        back = ai_node.AiTask(mode="text", prompt="x", system_prompt="", image_url="", model=NINE,
                              requested_model="", max_output_tokens=64, queue_class="interactive", backend_task_id="")
        node._select_task_model(back)
        self.assertEqual((cfg.model_id, node.swaps), (NINE, 2))

    def test_vram_check_uses_the_requested_models_need(self):
        self.install_4b()
        cfg = self.cfg(vram_margin_mb=1000, vram_need_mb=8800,
                       models=[{"model_id": FOUR, "vram_need_mb": 5600}])
        node = ai_node.Node(cfg)
        node.vram = {"ok": True, "free_mb": 8000}
        node.comfy = {"busy": False}
        with mock.patch.object(ai_node, "running_game", return_value=""):
            self.assertEqual(node.blocker(NINE)[0], "insufficient_vram")   # 8800 + 1000 > 8000
            self.assertIsNone(node.blocker(FOUR))                          # 5600 + 1000 <= 8000
            self.assertEqual(node.blocker()[0], "insufficient_vram")       # no model named = the selected one

    def test_single_model_node_never_swaps(self):
        node = self.node(self.cfg())
        node.llama.stop = lambda reason: self.fail("no stop expected")
        task = ai_node.AiTask(mode="text", prompt="x", system_prompt="", image_url="", model=NINE,
                              requested_model="", max_output_tokens=64, queue_class="interactive", backend_task_id="")
        node._select_task_model(task)
        self.assertEqual(node.swaps, 0)



class ReloadTests(NodeTests):
    """POST /ai-reload (2026-11-07): the backend asks a node that answers "????" to reload its model."""

    def test_idle_server_is_stopped_now(self):
        node = self.node(self.cfg())
        stops = []
        node.llama.running = lambda: True
        node.llama.stop_if_idle = lambda reason, min_idle=0.0: stops.append(reason) or True
        self.assertEqual(node.request_reload("garbage"), {"reload": "stopped"})
        self.assertEqual((node.reloads, node.reload_pending), (1, False))
        self.assertIn("garbage", stops[0])

    def test_busy_server_is_stopped_after_the_running_task(self):
        node = self.node(self.cfg())
        node.llama.running = lambda: True
        node.llama.stop_if_idle = lambda reason, min_idle=0.0: False
        self.assertEqual(node.request_reload(), {"reload": "after_running_task"})
        self.assertTrue(node.reload_pending)

    def test_nothing_loaded_nothing_to_do(self):
        node = self.node(self.cfg())
        node.llama.running = lambda: False
        self.assertEqual(node.request_reload(), {"reload": "not_loaded"})
        self.assertFalse(node.reload_pending)

    def test_status_reports_reloads(self):
        node = self.node(self.cfg())
        with mock.patch.object(ai_node, "running_game", return_value=""):
            status = node.server_status()
        self.assertEqual((status["ai_node"]["reloads"], status["ai_node"]["reload_pending"]), (0, False))


if __name__ == "__main__":
    unittest.main()
