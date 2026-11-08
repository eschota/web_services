"""Media-edit source download retries (2026-11-08: WinError 10060 on f7 and raptor for M+ JPEGs).

    python -m unittest test_media_download -v
"""

from __future__ import annotations

import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ai_node  # noqa: E402

PAYLOAD = bytes(range(256)) * 4096          # 1 MiB


class Script:
    """What the fake server does on each request, in order; the last entry repeats."""

    def __init__(self, *steps):
        self.steps = list(steps)
        self.requests = []

    def next(self, headers):
        self.requests.append(dict(headers))
        return self.steps[min(len(self.requests) - 1, len(self.steps) - 1)]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):
        pass

    def do_GET(self):  # noqa: N802
        step = self.server.script.next(self.headers)
        if step == "drop":                      # the connection dies before any answer
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if step == "stall":                     # accepted, never answers (read timeout)
            time.sleep(2)
            return
        if isinstance(step, int):               # an HTTP error status
            self.send_response(step)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        ranged = self.headers.get("Range")
        start = int(ranged.split("=")[1].split("-")[0]) if ranged and step != "ignore-range" else 0
        body = PAYLOAD[start:]
        if start:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}")
        else:
            self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        if step == "half":                      # half the body, then the connection breaks
            self.wfile.write(body[: len(body) // 2])
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.wfile.write(body)


class DownloadRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/src.jpg"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def fetch(self, *steps, **kw):
        self.server.script = Script(*steps)
        stats = {}
        kw.setdefault("backoff", (0.0, 0.0, 0.0))
        try:
            return ai_node.download_media_source(self.url, self.tmp, max_bytes=10 ** 8, cancel=threading.Event(),
                                                 deadline=time.time() + 60, allow_private=True, stats=stats,
                                                 **kw), stats
        finally:
            self.stats = stats

    def read(self, path):
        with open(path, "rb") as handle:
            return handle.read()

    def test_first_connection_dropped_then_ok(self):
        (path, size), stats = self.fetch("drop", "ok")
        self.assertEqual((size, stats["attempts"]), (len(PAYLOAD), 2))
        self.assertEqual(self.read(path), PAYLOAD)

    def test_partial_read_resumes_with_range(self):
        (path, size), stats = self.fetch("half", "ok")
        self.assertEqual(self.read(path), PAYLOAD)
        self.assertEqual(stats["attempts"], 2)
        self.assertTrue(stats["resumed"])
        self.assertEqual(self.server.script.requests[1].get("Range"), f"bytes={len(PAYLOAD) // 2}-")

    def test_server_without_range_starts_over(self):
        (path, size), stats = self.fetch("half", "ignore-range")
        self.assertEqual(self.read(path), PAYLOAD)
        self.assertFalse(stats["resumed"])

    def test_5xx_is_retried(self):
        (path, size), stats = self.fetch(503, 502, "ok")
        self.assertEqual(stats["attempts"], 3)
        self.assertEqual(self.read(path), PAYLOAD)

    def test_4xx_is_not_retried(self):
        for code in (403, 404):
            with self.assertRaises(ai_node.NodeError) as ctx:
                self.fetch(code, "ok")
            self.assertIn(f"HTTP {code}", str(ctx.exception))
            self.assertEqual(self.stats["attempts"], 1)

    def test_gives_up_after_four_attempts(self):
        with self.assertRaises(ai_node.NodeError) as ctx:
            self.fetch("drop")
        self.assertTrue(str(ctx.exception).startswith("source_download_failed"))
        self.assertIn("attempt 4 of 4", str(ctx.exception))
        self.assertEqual(len(self.server.script.requests), 4)

    def test_read_timeout_is_retried(self):
        (path, size), stats = self.fetch("stall", "ok", read_timeout=0.5, connect_timeout=0.5)
        self.assertEqual(stats["attempts"], 2)
        self.assertEqual(self.read(path), PAYLOAD)

    def test_backoff_schedule_is_2_5_10(self):
        self.assertEqual(ai_node.MEDIA_DOWNLOAD_BACKOFF, (2.0, 5.0, 10.0))
        self.assertEqual((ai_node.MEDIA_CONNECT_TIMEOUT, ai_node.MEDIA_READ_TIMEOUT), (20.0, 120.0))
        waits = []

        class Clock(threading.Event):
            def wait(self, timeout=None):
                waits.append(timeout)
                return False

        self.server.script = Script("drop")
        with self.assertRaises(ai_node.NodeError):
            ai_node.download_media_source(self.url, self.tmp, max_bytes=10 ** 8, cancel=Clock(),
                                          deadline=time.time() + 60, allow_private=True)
        self.assertEqual(waits, [2.0, 5.0, 10.0])

    def test_attempts_reach_the_task_status(self):
        self.server.script = Script("drop", "ok")
        task = ai_node.MediaTask(ai_node.parse_media_edit_request(
            {"source_url": self.url, "media_kind": "image", "operation": {"hflip": True}}))
        task.work_dir = self.tmp
        task.started_at = time.time()
        with unittest.mock.patch.object(ai_node, "MEDIA_DOWNLOAD_BACKOFF", (0.0, 0.0, 0.0)), \
                unittest.mock.patch.object(ai_node, "probe_media", side_effect=ai_node.NodeError("stop here")):
            with self.assertRaises(ai_node.NodeError):
                ai_node.execute_media_task(task, ffmpeg="ffmpeg", ffprobe="", max_source_bytes=10 ** 8,
                                           timeout_seconds=60, allow_private=True, use_nvenc=False)
        self.assertEqual(task.to_dict()["download_attempts"], 2)


import unittest.mock  # noqa: E402

if __name__ == "__main__":
    unittest.main()
