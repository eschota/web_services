import os
import sys
import unittest
from unittest.mock import patch

import httpx


BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from worker_transport import (  # noqa: E402
    ENV_NAME,
    WorkerTransportConfigError,
    worker_transport_map,
    worker_transport_url,
    worker_http_client,
)


class WorkerTransportTests(unittest.TestCase):
    def test_unset_mapping_is_identity(self):
        url = "https://converter-f2.freestock.online/api-converter-glb/status/a?x=1"
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(worker_transport_url(url), url)

    def test_exact_origin_is_rewritten_and_suffix_preserved(self):
        mapping = '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279"}'
        logical = "https://converter-f2.freestock.online/api-converter-glb/status/a?x=1#part"
        with patch.dict(os.environ, {ENV_NAME: mapping}, clear=True):
            self.assertEqual(
                worker_transport_url(logical),
                "http://127.0.0.1:15279/api-converter-glb/status/a?x=1#part",
            )

    def test_all_worker_api_suffixes_share_the_same_transport(self):
        mapping = '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279"}'
        suffixes = (
            "/api-converter-glb",
            "/api-converter-glb/server-status",
            "/api-converter-glb/status/task-1",
            "/api-converter-glb/model-files/task-1",
            "/api-converter-glb/control/tasks/task-1/preempt",
            "/converter/glb/task-1/task-1_progress.txt",
        )
        with patch.dict(os.environ, {ENV_NAME: mapping}, clear=True):
            for suffix in suffixes:
                with self.subTest(suffix=suffix):
                    self.assertEqual(
                        worker_transport_url(
                            f"https://converter-f2.freestock.online{suffix}"
                        ),
                        f"http://127.0.0.1:15279{suffix}",
                    )

    def test_different_origin_is_not_rewritten(self):
        mapping = '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279"}'
        unrelated = "https://converter-f2.freestock.online.evil/api-converter-glb"
        with patch.dict(os.environ, {ENV_NAME: mapping}, clear=True):
            self.assertEqual(worker_transport_url(unrelated), unrelated)

    def test_default_port_and_unicode_query_are_preserved(self):
        mapping = '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279"}'
        logical = "https://converter-f2.freestock.online:443/converter/%D1%82.glb?q=%2F%3F&name=тест"
        with patch.dict(os.environ, {ENV_NAME: mapping}, clear=True):
            self.assertEqual(
                worker_transport_url(logical),
                "http://127.0.0.1:15279/converter/%D1%82.glb?q=%2F%3F&name=тест",
            )

    def test_client_factory_rewrites_request_and_preserves_existing_hook(self):
        seen = []

        async def existing_hook(request):
            seen.append((str(request.url), request.headers.get("host")))

        async def handler(request):
            return httpx.Response(200, json={"ok": True})

        async def scenario():
            mapping = '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279"}'
            with patch.dict(os.environ, {ENV_NAME: mapping}, clear=True):
                async with worker_http_client(
                    transport=httpx.MockTransport(handler),
                    event_hooks={"request": [existing_hook]},
                ) as client:
                    response = await client.get(
                        "https://converter-f2.freestock.online/api-converter-glb?x=1"
                    )
            self.assertEqual(response.status_code, 200)

        import asyncio
        asyncio.run(scenario())
        self.assertEqual(
            seen,
            [("http://127.0.0.1:15279/api-converter-glb?x=1", "127.0.0.1:15279")],
        )

    def test_mapping_rejects_path_query_credentials_and_non_http(self):
        invalid = (
            '{"https://converter-f2.freestock.online/api":"http://127.0.0.1:15279"}',
            '{"https://converter-f2.freestock.online":"http://127.0.0.1:15279/base"}',
            '{"https://user@converter-f2.freestock.online":"http://127.0.0.1:15279"}',
            '{"https://converter-f2.freestock.online":"file:///tmp/worker"}',
        )
        for raw in invalid:
            with self.subTest(raw=raw):
                with self.assertRaises(WorkerTransportConfigError):
                    worker_transport_map(raw)

    def test_malformed_explicit_mapping_fails_closed(self):
        with patch.dict(os.environ, {ENV_NAME: "not-json"}, clear=True):
            with self.assertRaises(WorkerTransportConfigError):
                worker_transport_url("https://converter-f2.freestock.online/api-converter-glb")


if __name__ == "__main__":
    unittest.main()
