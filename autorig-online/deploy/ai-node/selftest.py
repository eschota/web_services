#!/usr/bin/env python3
"""Self-test for ai_node.py against stdlib fakes. No GPU, no network, no real model.

    python selftest.py            # runs everything, exit code 0 on success

What runs:
* ai_node.py as a real subprocess with a temporary config.
* A fake llama-server (`selftest.py --fake-llama ...`) that ai_node launches the
  way it launches the real one; it answers /health (503 while "loading"),
  /v1/models and /v1/chat/completions and logs every start to a file.
* A fake nvidia-smi (`selftest.py --fake-smi`) whose free VRAM the test sets.
* A fake ComfyUI (/prompt and /queue) and an image host, in this process.
All of it binds to 127.0.0.1 on free ports.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
TOKEN = "selftest-token-0123456789"
MODEL_ID = "bonsai2-27b"
# A real 1x1 PNG.
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")


# ======================================================================= fakes
def fake_llama_main(argv):
    """Stand-in for llama-server.exe: same flags, canned answers."""
    opts = {"--fake-load-delay": "1.0"}
    flags = set()
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--jinja",):
            flags.add(arg)
            i += 1
            continue
        opts[arg] = argv[i + 1] if i + 1 < len(argv) else ""
        i += 2
    alias = opts.get("--alias", "")
    port = int(opts["--port"])
    delay = float(opts["--fake-load-delay"])
    delay_file = os.environ.get("FAKE_LLAMA_DELAY_FILE", "")
    if delay_file and os.path.isfile(delay_file):
        with open(delay_file, encoding="utf-8") as handle:
            delay = float(handle.read().strip())
    ready_at = time.time() + delay
    log_path = os.environ.get("FAKE_LLAMA_LOG", "")

    def log(event):
        if log_path:
            with open(log_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(event) + "\n")

    log({"event": "start", "pid": os.getpid(), "alias": alias, "argv": argv})

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, payload):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                if time.time() < ready_at:
                    self._send(503, {"error": {"code": 503, "message": "Loading model"}})
                else:
                    self._send(200, {"status": "ok"})
            elif self.path == "/v1/models":
                self._send(200, {"object": "list", "data": [{"id": alias}]})
            else:
                self._send(404, {"error": "nope"})

        def do_POST(self):
            if self.path != "/v1/chat/completions":
                self._send(404, {"error": "nope"})
                return
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            system = ""
            text = ""
            media = "none"
            nbytes = 0
            for message in body["messages"]:
                if message["role"] == "system":
                    system = message["content"]
                elif isinstance(message["content"], str):
                    text = message["content"]
                else:
                    for part in message["content"]:
                        if part["type"] == "text":
                            text = part["text"]
                        else:
                            url = part["image_url"]["url"]
                            media = url[5:].split(";", 1)[0]
                            nbytes = len(base64.b64decode(url.split(",", 1)[1]))
            log({"event": "completion", "text": text})
            if "SLOW" in text:
                time.sleep(3)
            if "THINK_ONLY" in text:
                message = {"role": "assistant", "content": "", "reasoning_content": "x" * 50}
            else:
                message = {
                    "role": "assistant",
                    "content": (f"ECHO|alias={alias}|system={system}|user={text}|image={media}"
                                f"|bytes={nbytes}|max_tokens={body['max_tokens']}"
                                f"|temperature={body['temperature']}"),
                    "reasoning_content": "considered it",
                }
            self._send(200, {"choices": [{"message": message}],
                             "usage": {"prompt_tokens": 11, "completion_tokens": 7}})

    class S(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = False  # a real llama-server cannot share its port

    S(("127.0.0.1", port), H).serve_forever()


def fake_smi_main():
    with open(os.environ["FAKE_SMI_STATE"], encoding="utf-8") as handle:
        state = json.load(handle)
    if state.get("fail"):
        sys.exit(9)
    total, free = int(state["total"]), int(state["free"])
    print(f"{total}, {total - free}, {free}")


class FakeComfy:
    """ComfyUI's /prompt and /queue, plus an image host on the same port."""

    def __init__(self):
        self.running = 0
        self.pending = 0
        self.prompt_endpoint = True
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body, ctype="application/json"):
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path == "/prompt" and outer.prompt_endpoint:
                    self._send(200, {"exec_info": {
                        "queue_remaining": outer.running + outer.pending}})
                elif self.path == "/queue":
                    self._send(200, {
                        "queue_running": [[i, f"r{i}", {}, {}, []] for i in range(outer.running)],
                        "queue_pending": [[i, f"p{i}", {}, {}, []] for i in range(outer.pending)]})
                elif self.path == "/img.png":
                    self._send(200, PNG_1X1, "image/png")
                elif self.path == "/text":
                    self._send(200, b"not an image", "text/plain")
                else:
                    self._send(404, {"error": "nope"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


# ===================================================================== harness
RESULTS = []


def check(name, condition, detail=""):
    RESULTS.append((name, bool(condition), detail))
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not condition else ""), flush=True)
    return bool(condition)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(predicate, timeout, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception:  # noqa: BLE001
            pass
        time.sleep(interval)
    return None


class Client:
    def __init__(self, port):
        self.base = f"http://127.0.0.1:{port}"

    def call(self, method, path, body=None, token=TOKEN):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw or b"{}")
            except ValueError:
                return exc.code, {"raw": raw.decode("utf-8", "replace")}

    def status(self):
        return self.call("GET", "/api-converter-glb/server-status")[1]

    def submit(self, mode, body):
        path = "/api-converter-glb/ai-vision" if mode == "vision" else "/api-converter-glb/text2text"
        return self.call("POST", path, body)

    def task(self, task_id):
        return self.call("GET", f"/api-converter-glb/ai-vision/status/{task_id}")[1]

    def finish(self, task_id, timeout=30):
        done = wait_until(lambda: self.task(task_id)
                          if self.task(task_id).get("status") in ("Completed", "Failed") else None,
                          timeout)
        return done or self.task(task_id)


def llama_answering(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def starts(log_path):
    if not os.path.isfile(log_path):
        return 0
    with open(log_path, encoding="utf-8") as handle:
        return sum(1 for line in handle if '"event": "start"' in line)


# ================================================================ unit checks
def unit_checks(ai_node, tmp):
    ok, why = ai_node.validate_bearer_header("Bearer abc", "abc")
    check("unit: bearer accepts the right token", ok and why == "ok")
    check("unit: bearer rejects a wrong token",
          ai_node.validate_bearer_header("Bearer abd", "abc") == (False, "unauthorized"))
    check("unit: bearer without configured token",
          ai_node.validate_bearer_header("Bearer abc", "") == (False, "token_not_configured"))
    check("unit: max tokens default is the ceiling", ai_node.validate_max_tokens(None, 2048) == 2048)
    check("unit: max tokens -1 is unlimited", ai_node.validate_max_tokens(-1, 2048) == -1)
    check("unit: max tokens clamp", ai_node.validate_max_tokens(5000, 2048) == 2048)
    for bad in (0, "x"):
        try:
            ai_node.validate_max_tokens(bad, 2048)
            check(f"unit: max tokens {bad!r} rejected", False)
        except ai_node.RequestError:
            check(f"unit: max tokens {bad!r} rejected", True)
    for url in ("http://127.0.0.1/x.png", "http://localhost/x.png",
                "http://example.com:8080/x.png", "ftp://example.com/x.png",
                "http://user:pw@example.com/x.png", "http://10.1.2.3/x.png"):
        try:
            ai_node.validate_public_image_url(url)
            check(f"unit: SSRF guard rejects {url}", False)
        except ai_node.RequestError:
            check(f"unit: SSRF guard rejects {url}", True)
    data_uri = "data:image/png;base64," + base64.b64encode(PNG_1X1).decode()
    data, media = ai_node.decode_data_image(data_uri)
    check("unit: data URI decodes", data == PNG_1X1 and media == "image/png")
    try:
        ai_node.decode_data_image("data:text/plain;base64,aGVsbG8=")
        check("unit: data URI non-image rejected", False)
    except ai_node.RequestError:
        check("unit: data URI non-image rejected", True)

    # Launch line must be the converter's (bonsai_adapter._launch_argv 60101f4).
    weights = os.path.join(tmp, "unit", "w.gguf")
    mmproj = os.path.join(tmp, "unit", "p.gguf")
    os.makedirs(os.path.dirname(weights), exist_ok=True)
    for path in (weights, mmproj):
        with open(path, "wb") as handle:
            handle.write(b"x")
    cfg = ai_node.Config({"model_id": "bonsai2-27b", "weights": weights, "mmproj": mmproj,
                          "llama_server": r"C:\llama\llama-server.exe", "llama_port": 8092},
                         tmp)
    argv = ai_node.LlamaServer(cfg).launch_argv()
    check("unit: bonsai launch argv matches converter", argv == [
        r"C:\llama\llama-server.exe", "-m", weights, "--mmproj", mmproj, "--alias",
        "bonsai2-27b", "-ngl", "99", "-c", "4096", "--host", "127.0.0.1", "--port", "8092",
        "--jinja"], argv)
    cfg_q = ai_node.Config({"model_id": "qwen35-9b-uncensored", "weights": weights,
                            "mmproj": mmproj, "llama_server": "llama-server"}, tmp)
    argv_q = ai_node.LlamaServer(cfg_q).launch_argv()
    check("unit: qwen launch has --reasoning off and 8192 context",
          "--reasoning" in argv_q and argv_q[argv_q.index("--reasoning") + 1] == "off"
          and argv_q[argv_q.index("-c") + 1] == "8192", argv_q)
    check("unit: qwen inherits converter ceiling 4096", cfg_q.max_output_tokens == 4096)
    check("unit: system role only verified for bonsai",
          cfg.system_prompt_supported and not cfg_q.system_prompt_supported)
    check("unit: VRAM need estimate = files + overhead", cfg.need_mb() == 0 + 1536)
    ours = ai_node.LlamaServer(cfg)
    check("unit: stray matcher accepts our own launch line",
          ours._is_ours(subprocess.list2cmdline(argv)))
    check("unit: stray matcher ignores another port",
          not ours._is_ours(subprocess.list2cmdline(argv).replace("8092", "8091")))
    check("unit: stray matcher ignores other weights",
          not ours._is_ours(subprocess.list2cmdline(argv).replace("w.gguf", "o.gguf")))


# ========================================================== integration checks
def integration(tmp):
    comfy = FakeComfy()
    listen_port = free_port()
    llama_port = free_port()
    llama_log = os.path.join(tmp, "fake_llama.log")
    smi_state = os.path.join(tmp, "smi.json")
    models = os.path.join(tmp, "models")
    os.makedirs(models, exist_ok=True)
    weights = os.path.join(models, "bonsai-test.gguf")
    mmproj = os.path.join(models, "mmproj-test.gguf")
    for path in (weights, mmproj):
        with open(path, "wb") as handle:
            handle.write(b"GGUF" + b"\0" * 64)
    with open(os.path.join(tmp, "token.txt"), "w", encoding="utf-8") as handle:
        handle.write(TOKEN + "\n")

    def set_smi(**state):
        merged = {"total": 24564, "free": 20000}
        merged.update(state)
        with open(smi_state, "w", encoding="utf-8") as handle:
            json.dump(merged, handle)

    set_smi()
    me = os.path.abspath(__file__)
    config = {
        "node_name": "selftest-node",
        "listen_port": listen_port,
        "token_file": "token.txt",
        "model_id": MODEL_ID,
        "weights": weights,
        "mmproj": mmproj,
        "vram_need_mb": 8000,
        "vram_margin_mb": 1536,
        "llama_server": [sys.executable, me, "--fake-llama", "--fake-load-delay", "1.0"],
        "llama_port": llama_port,
        "nvidia_smi": [sys.executable, me, "--fake-smi"],
        "comfy_url": f"http://127.0.0.1:{comfy.port}",
        "comfy_poll_seconds": 0.5,
        "keepalive_seconds": 3,
        "max_tasks": 2,
        "comfy_wait_seconds": 6,
        "start_timeout_seconds": 30,
        "allow_private_image_urls": True,
        "log_file": "ai-node.log",
        "log_level": "DEBUG",
    }
    config_path = os.path.join(tmp, "config.json")
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
    delay_file = os.path.join(tmp, "fake_llama_delay.txt")
    env = dict(os.environ, FAKE_LLAMA_LOG=llama_log, FAKE_SMI_STATE=smi_state,
               FAKE_LLAMA_DELAY_FILE=delay_file)
    env.pop("AI_NODE_TOKEN", None)
    node_out = open(os.path.join(tmp, "ai-node.stdout.log"), "wb")
    node = subprocess.Popen([sys.executable, os.path.join(HERE, "ai_node.py"),
                             "--config", config_path],
                            env=env, stdout=node_out, stderr=subprocess.STDOUT)
    extra = []  # processes this test starts itself
    api = Client(listen_port)
    try:
        if not check("boot: /healthz answers",
                     wait_until(lambda: api.call("GET", "/healthz", token=None)[0] == 200, 20)):
            return

        # ---------------------------------------------------------- auth
        code, _ = api.call("GET", "/api-converter-glb/server-status", token=None)
        check("auth: no token -> 401", code == 401, code)
        code, _ = api.call("GET", "/api-converter-glb/server-status", token="wrong")
        check("auth: wrong token -> 401", code == 401, code)
        code, _ = api.call("POST", "/api-converter-glb/text2text", {"prompt": "x"},
                           token="wrong")
        check("auth: submit with wrong token -> 401", code == 401, code)

        # ------------------------------------------ server-status contract
        wait_until(lambda: api.status()["ai_node"]["vram"].get("ok"), 5)
        st = api.status()
        am = st.get("ai_models", {})
        ts = st.get("tasks_summary", {})
        model = (am.get("models") or [{}])[0]
        check("status: tasks_summary has queue_size/processing",
              ts.get("queue_size") == 0 and ts.get("processing") == 0, ts)
        check("status: ai_models.models carries the pinned model",
              [m.get("id") for m in am.get("models", [])] == [MODEL_ID], am.get("models"))
        check("status: model entry fields",
              model.get("context_tokens") == 4096 and model.get("vision") is True
              and model.get("system_prompt_supported") is True
              and model.get("unlimited_output_supported") is True, model)
        check("status: catalogue flags",
              am.get("system_prompt_supported") is True
              and am.get("system_prompt_models") == [MODEL_ID]
              and am.get("unlimited_output_supported") is True
              and am.get("loaded_model") == "", am)
        check("status: idle node accepts", st.get("accepting_ai_vision") is True, st.get("ai_node"))
        check("status: processing_tasks is a list", st.get("processing_tasks") == [])

        # -------------------------------------------------- validation
        code, body = api.submit("text", {"prompt": "hi", "model": "qwen35-9b-uncensored"})
        check("submit: other model -> 503 runtime_not_installed",
              code == 503 and body.get("error") == "runtime_not_installed"
              and body.get("available_models") == [MODEL_ID], (code, body))
        code, body = api.submit("text", {"prompt": "   "})
        check("submit: empty prompt -> 400", code == 400 and body.get("error") == "invalid_request",
              (code, body))
        code, body = api.submit("vision", {"prompt": "what"})
        check("submit: vision without image_url -> 400", code == 400, (code, body))
        code, body = api.submit("text", {"prompt": "hi", "max_output_tokens": 0})
        check("submit: max_output_tokens 0 -> 400", code == 400, (code, body))
        code, body = api.submit("text", {"prompt": "p" * 5000, "system_prompt": "s" * 3001})
        check("submit: prompt+system over 8000 -> 400", code == 400, (code, body))
        code, body = api.call("GET", "/api-converter-glb/ai-vision/status/nope")
        check("status: unknown task -> 404", code == 404, (code, body))

        # ------------------------------------------------- text task
        code, body = api.submit("text", {"prompt": "hello", "system_prompt": "be brief",
                                         "max_output_tokens": 64, "model": MODEL_ID})
        check("text: accepted 202 with task_id",
              code == 202 and body.get("task_id") and body.get("status") == "Pending"
              and body.get("model") == MODEL_ID and body.get("workload_class") == "ai_vision",
              (code, body))
        result = api.finish(body.get("task_id", ""))
        answer = result.get("answer", "")
        check("text: completed", result.get("status") == "Completed", result)
        check("text: system prompt went in the system role", "|system=be brief|" in answer, answer)
        check("text: user prompt and max_tokens passed", "|user=hello|" in answer
              and "|max_tokens=64|" in answer and "|temperature=0.3" in answer, answer)
        check("text: reasoning split off", result.get("reasoning") == "considered it", result)
        check("text: status fields",
              result.get("mode") == "text" and result.get("model") == MODEL_ID
              and result.get("elapsed_seconds", 0) > 0 and result.get("error") is None
              and result.get("model_usage", {}).get("requested_max_tokens") == 64, result)
        check("text: model stays loaded", api.status()["ai_models"]["loaded_model"] == MODEL_ID)
        check("text: one llama start", starts(llama_log) == 1, starts(llama_log))
        argv = json.loads(open(llama_log, encoding="utf-8").readline())["argv"]
        check("text: launch flags reach llama-server",
              argv[argv.index("--alias") + 1] == MODEL_ID and "--jinja" in argv
              and argv[argv.index("--port") + 1] == str(llama_port)
              and argv[argv.index("--mmproj") + 1] == mmproj, argv)

        # ------------------------------------------------ vision tasks
        code, body = api.submit("vision", {"prompt": "describe",
                                           "image_url": f"http://127.0.0.1:{comfy.port}/img.png"})
        result = api.finish(body.get("task_id", ""))
        check("vision: URL image completed",
              result.get("status") == "Completed" and "|image=image/png|" in result.get("answer", "")
              and f"|bytes={len(PNG_1X1)}|" in result.get("answer", "")
              and result.get("mode") == "vision", result)
        data_uri = "data:image/png;base64," + base64.b64encode(PNG_1X1).decode()
        code, body = api.submit("vision", {"prompt": "describe", "image_url": data_uri})
        result = api.finish(body.get("task_id", ""))
        check("vision: data URI image completed",
              result.get("status") == "Completed" and "|image=image/png|" in result.get("answer", "")
              and result.get("image_url", "").endswith(",<inline>"), result)
        code, body = api.submit("vision", {"prompt": "describe",
                                           "image_url": f"http://127.0.0.1:{comfy.port}/text"})
        result = api.finish(body.get("task_id", ""))
        check("vision: non-image download fails the task",
              result.get("status") == "Failed"
              and "image_unexpected_content_type" in (result.get("error") or ""), result)
        check("keepalive: warm model reused, no reload", starts(llama_log) == 1, starts(llama_log))

        # ------------------------------------------- token budgets
        for asked, expect in ((-1, "-1"), (None, "2048"), (5000, "2048")):
            payload = {"prompt": f"budget {asked}"}
            if asked is not None:
                payload["max_output_tokens"] = asked
            code, body = api.submit("text", payload)
            result = api.finish(body.get("task_id", ""))
            check(f"budget: max_output_tokens={asked} -> max_tokens={expect}",
                  f"|max_tokens={expect}|" in result.get("answer", ""), result.get("answer"))
        code, body = api.submit("text", {"prompt": "THINK_ONLY please", "max_output_tokens": 32})
        result = api.finish(body.get("task_id", ""))
        check("budget: reasoning-only reply fails with the converter's message",
              result.get("status") == "Failed"
              and (result.get("error") or "").startswith("model_spent_its_budget_thinking")
              and "max_output_tokens=32" in result.get("error", ""), result)

        # ---------------------------------- one at a time, queue limit
        code_a, a = api.submit("text", {"prompt": "SLOW a"})
        wait_until(lambda: api.task(a["task_id"]).get("current_stage") == "generating", 10)
        st = api.status()
        check("queue: running task reported",
              st["tasks_summary"]["processing"] == 1
              and st["processing_tasks"][0]["workload_class"] == "ai_vision"
              and st["processing_tasks"][0]["mode"] == "text", st["processing_tasks"])
        code_b, b = api.submit("text", {"prompt": "after a"})
        check("queue: second task accepted behind the first", code_b == 202, (code_b, b))
        code_c, c = api.submit("text", {"prompt": "third"})
        check("queue: third task refused 503 queue_full",
              code_c == 503 and c.get("error") == "queue_full", (code_c, c))
        st = api.status()
        check("queue: full node reports queue_size 99",
              st["tasks_summary"]["queue_size"] == 99 and st["accepting_ai_vision"] is False,
              st["tasks_summary"])
        check("queue: pending task listed", st["tasks_summary"]["pending"] == 1, st["tasks_summary"])
        ra, rb = api.finish(a["task_id"]), api.finish(b["task_id"])
        check("queue: both accepted tasks complete",
              ra.get("status") == "Completed" and rb.get("status") == "Completed", (ra, rb))
        check("queue: tasks ran one at a time",
              rb.get("started_at", 0) >= ra.get("completed_at", 1) - 0.05, (ra, rb))

        # ------------------------------ ComfyUI busy -> unload, refuse
        check("comfy: model loaded before ComfyUI starts",
              api.status()["ai_models"]["loaded_model"] == MODEL_ID)
        comfy.running = 1
        t0 = time.monotonic()
        unloaded = wait_until(lambda: api.status()["ai_models"]["loaded_model"] == ""
                              and not port_open(llama_port), 5)
        check("comfy: idle model unloaded within a poll or two",
              unloaded, f"{time.monotonic() - t0:.1f}s")
        st = api.status()
        check("comfy: busy node reports queue_size 99 and not accepting",
              st["tasks_summary"]["queue_size"] == 99 and st["accepting_ai_vision"] is False
              and st["ai_node"]["blocker"] == "gpu_busy_comfyui", st["ai_node"])
        check("comfy: catalogue still published while busy",
              [m["id"] for m in st["ai_models"]["models"]] == [MODEL_ID])
        code, body = api.submit("text", {"prompt": "while comfy busy"})
        check("comfy: submit refused 503 gpu_busy_comfyui",
              code == 503 and body.get("error") == "gpu_busy_comfyui"
              and body.get("retryable") is True, (code, body))
        comfy.running = 0
        comfy.pending = 2
        code, body = api.submit("text", {"prompt": "while comfy has pending"})
        check("comfy: pending prompts also count as busy", code == 503, (code, body))
        comfy.prompt_endpoint = False
        comfy.pending = 1
        time.sleep(1.2)
        st = api.status()
        check("comfy: /queue fallback sees the pending prompt",
              st["ai_node"]["comfy"].get("source") == "queue"
              and st["accepting_ai_vision"] is False, st["ai_node"]["comfy"])
        comfy.pending = 0
        accepting = wait_until(lambda: api.status()["accepting_ai_vision"], 5)
        check("comfy: accepting again when ComfyUI drains", accepting)
        comfy.prompt_endpoint = True

        # ------------------------------------------------- VRAM gate
        set_smi(free=5000)
        time.sleep(1.2)
        st = api.status()
        check("vram: short VRAM reports not accepting / 99",
              st["ai_node"]["blocker"] == "insufficient_vram"
              and st["tasks_summary"]["queue_size"] == 99, st["ai_node"])
        code, body = api.submit("text", {"prompt": "no room"})
        check("vram: submit refused 503 insufficient_vram",
              code == 503 and body.get("error") == "insufficient_vram", (code, body))
        set_smi(free=9535)  # 8000 + 1536 - 1
        code, body = api.submit("text", {"prompt": "one MiB short"})
        check("vram: margin is enforced to the MiB", code == 503, (code, body))
        set_smi(fail=True)
        code, body = api.submit("text", {"prompt": "unknown vram"})
        check("vram: unreadable nvidia-smi refuses a cold load",
              code == 503 and body.get("error") == "vram_unknown", (code, body))
        set_smi(free=9536)
        before = starts(llama_log)
        code, body = api.submit("text", {"prompt": "exactly enough"})
        result = api.finish(body.get("task_id", ""))
        check("vram: exactly need + margin loads and completes",
              code == 202 and result.get("status") == "Completed"
              and starts(llama_log) == before + 1, (code, result))
        set_smi(free=500)
        code, body = api.submit("text", {"prompt": "warm model needs no free VRAM"})
        result = api.finish(body.get("task_id", ""))
        check("vram: a warm model is used even when free VRAM is low",
              code == 202 and result.get("status") == "Completed", (code, body, result))
        set_smi(free=20000)

        # ------------------------------------------- keepalive expiry
        t0 = time.monotonic()
        unloaded = wait_until(lambda: api.status()["ai_models"]["loaded_model"] == ""
                              and not port_open(llama_port), 8)
        waited = time.monotonic() - t0
        check("keepalive: model unloaded after keepalive_seconds",
              unloaded and waited >= 1.5, f"{waited:.1f}s")
        check("keepalive: stop reason recorded",
              api.status()["ai_node"]["last_stop_reason"] == "keepalive expired",
              api.status()["ai_node"]["last_stop_reason"])

        # ---- running task survives ComfyUI; queued task waits for it
        before = starts(llama_log)
        _, a = api.submit("text", {"prompt": "SLOW running when comfy starts"})
        _, b = api.submit("text", {"prompt": "queued behind"})
        wait_until(lambda: api.task(a["task_id"]).get("current_stage") == "generating", 10)
        comfy.running = 1
        ra = api.finish(a["task_id"])
        check("comfy-mid: running generation is not killed",
              ra.get("status") == "Completed", ra)
        unloaded = wait_until(lambda: not port_open(llama_port), 5)
        check("comfy-mid: model unloaded right after the running task", unloaded)
        waiting = wait_until(lambda: api.task(b["task_id"]).get("current_stage")
                             == "waiting_for_gpu", 5)
        check("comfy-mid: queued task waits for the GPU",
              waiting and api.task(b["task_id"]).get("wait_reason") == "comfyui_busy",
              api.task(b["task_id"]))
        comfy.running = 0
        rb = api.finish(b["task_id"])
        check("comfy-mid: queued task runs once ComfyUI is idle",
              rb.get("status") == "Completed" and starts(llama_log) == before + 2,
              (rb, starts(llama_log), before))

        # ---------------------------- queued task gives up after a while
        _, a = api.submit("text", {"prompt": "SLOW again"})
        _, b = api.submit("text", {"prompt": "will time out"})
        wait_until(lambda: api.task(a["task_id"]).get("current_stage") == "generating", 10)
        comfy.running = 1
        rb = api.finish(b["task_id"], timeout=20)
        check("comfy-timeout: queued task fails with gpu_unavailable after comfy_wait_seconds",
              rb.get("status") == "Failed"
              and (rb.get("error") or "").startswith("gpu_unavailable: comfyui_busy"), rb)
        comfy.running = 0

        # ---------------- load abandoned when ComfyUI starts mid-load
        wait_until(lambda: not port_open(llama_port), 5)
        with open(delay_file, "w", encoding="utf-8") as handle:
            handle.write("6")  # a slow cold load, so ComfyUI can start during it
        before = starts(llama_log)
        _, a = api.submit("text", {"prompt": "load gets interrupted"})
        wait_until(lambda: port_open(llama_port), 5)
        comfy.running = 1
        stopped = wait_until(lambda: not port_open(llama_port), 5)
        check("comfy-load: half-loaded server stopped when ComfyUI starts",
              stopped and api.status()["ai_node"]["last_stop_reason"]
              == "comfyui_busy during load", api.status()["ai_node"]["last_stop_reason"])
        waiting = wait_until(lambda: api.task(a["task_id"]).get("wait_reason")
                             == "comfyui_busy", 3)
        check("comfy-load: task waits instead of failing",
              waiting and api.task(a["task_id"]).get("status") == "Processing",
              api.task(a["task_id"]))
        os.remove(delay_file)
        comfy.running = 0
        ra = api.finish(a["task_id"], timeout=20)
        check("comfy-load: task loads again and completes once ComfyUI is idle",
              ra.get("status") == "Completed" and starts(llama_log) == before + 2,
              (ra, starts(llama_log), before))

        # --------------------------- ComfyUI unreachable = not busy
        comfy.stop()
        idle = wait_until(lambda: api.status()["comfy_online"] is False
                          and api.status()["accepting_ai_vision"], 5)
        check("comfy-down: unreachable ComfyUI does not block the node (default)", idle)
        code, body = api.submit("text", {"prompt": "comfy is down"})
        result = api.finish(body.get("task_id", ""))
        check("comfy-down: task completes", result.get("status") == "Completed", result)
        wait_until(lambda: not port_open(llama_port), 8)

        # ---------------------- someone else's server on the llama port
        foreign_weights = os.path.join(models, "someone-else.gguf")
        foreign = subprocess.Popen([sys.executable, me, "--fake-llama", "--fake-load-delay", "0",
                                    "-m", foreign_weights, "--alias", "someone-else",
                                    "--port", str(llama_port)],
                                   env=dict(env, FAKE_LLAMA_LOG=""))
        extra.append(foreign)
        wait_until(lambda: llama_answering(llama_port), 10)
        code, body = api.submit("text", {"prompt": "port taken"})
        result = api.finish(body.get("task_id", ""), timeout=60)
        check("port: foreign llama-server is not killed and the task fails clearly",
              result.get("status") == "Failed"
              and (result.get("error") or "").startswith("llama_port_in_use")
              and foreign.poll() is None, (result, foreign.poll()))
        foreign.kill()
        foreign.wait(10)
        wait_until(lambda: not port_open(llama_port), 5)

        # -------------------- our own orphan on the port is released
        orphan = subprocess.Popen([sys.executable, me, "--fake-llama", "--fake-load-delay", "0",
                                   "-m", weights, "--mmproj", mmproj, "--alias", MODEL_ID,
                                   "-ngl", "99", "-c", "4096", "--host", "127.0.0.1",
                                   "--port", str(llama_port), "--jinja"],
                                  env=dict(env, FAKE_LLAMA_LOG=""))
        extra.append(orphan)
        wait_until(lambda: llama_answering(llama_port), 10)
        code, body = api.submit("text", {"prompt": "orphan in the way"})
        result = api.finish(body.get("task_id", ""), timeout=60)
        check("port: an orphan with our weights/port/alias is released and replaced",
              result.get("status") == "Completed" and orphan.poll() is not None,
              (result, orphan.poll()))

        # ----------------------------- killing ai_node kills llama
        if os.name == "nt":
            check("job: model loaded before the hard kill",
                  api.status()["ai_models"]["loaded_model"] == MODEL_ID)
            node.kill()
            node.wait(10)
            gone = wait_until(lambda: not port_open(llama_port), 10)
            check("job: llama-server dies with a hard-killed ai_node (no orphan)", gone)
    finally:
        if node.poll() is None:
            node.terminate()
            try:
                node.wait(15)
            except subprocess.TimeoutExpired:
                node.kill()
        for proc in extra:
            if proc.poll() is None:
                proc.kill()
        node_out.close()
        try:
            comfy.stop()
        except Exception:  # noqa: BLE001
            pass
        # Never leave a fake llama behind.
        if port_open(llama_port):
            print("WARNING: something still listens on the fake llama port", llama_port)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--fake-llama":
        fake_llama_main(sys.argv[2:])
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == "--fake-smi":
        fake_smi_main()
        return 0
    sys.path.insert(0, HERE)
    import ai_node  # noqa: E402

    tmp = tempfile.mkdtemp(prefix="ai-node-selftest-")
    started = time.monotonic()
    try:
        unit_checks(ai_node, tmp)
        integration(tmp)
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        check("selftest ran to the end", False)
    failed = [r for r in RESULTS if not r[1]]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed "
          f"in {time.monotonic() - started:.0f} s")
    if failed:
        log = os.path.join(tmp, "ai-node.log")
        print(f"FAILED; service log kept at {log}")
        if os.path.isfile(log):
            with open(log, encoding="utf-8", errors="replace") as handle:
                print("---- last lines of ai-node.log ----")
                print("".join(handle.readlines()[-40:]))
        return 1
    shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
