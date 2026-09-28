import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import workers


class _Response:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class _Client:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    async def get(self, url, timeout=None):
        self.calls.append((url, timeout))
        return _Response(self.payloads[url])


class WorkerDispatchAdmissionTests(unittest.TestCase):
    def setUp(self):
        workers.clear_worker_dispatch_health_cache()

    def test_explicit_maintenance_blocks_new_dispatch(self):
        result = workers.parse_worker_dispatch_admission(
            {"maintenance": True, "hunyuan": {"disk": {"free_gb": 80}}},
            min_free_disk_gb=25,
        )
        self.assertFalse(result.allowed)
        self.assertEqual(result.reason, "maintenance")
        self.assertEqual(result.free_disk_gb, 80.0)

    def test_reported_low_disk_blocks_new_dispatch(self):
        result = workers.parse_worker_dispatch_admission(
            {"maintenance": False, "hunyuan": {"disk": {"free_gb": 24.99}}},
            min_free_disk_gb=25,
        )
        self.assertFalse(result.allowed)
        self.assertIn("low_disk", result.reason)

    def test_missing_extended_telemetry_keeps_legacy_worker_compatible(self):
        result = workers.parse_worker_dispatch_admission(
            {"server_version": "legacy"}, min_free_disk_gb=25
        )
        self.assertTrue(result.allowed)
        self.assertIsNone(result.free_disk_gb)

    def test_filter_excludes_confirmed_maintenance_and_low_disk(self):
        worker_urls = [
            "https://f1.example/api-converter-glb",
            "https://f2.example/api-converter-glb",
            "https://f3.example/api-converter-glb",
        ]
        rows = [SimpleNamespace(url=url) for url in worker_urls]
        payloads = {
            worker_urls[0] + "/server-status": {
                "maintenance": True,
                "hunyuan": {"disk": {"free_gb": 50}},
            },
            worker_urls[1] + "/server-status": {
                "maintenance": False,
                "hunyuan": {"disk": {"free_gb": 10}},
            },
            worker_urls[2] + "/server-status": {
                "maintenance": False,
                "hunyuan": {"disk": {"free_gb": 50}},
            },
        }
        client = _Client(payloads)

        allowed = asyncio.run(
            workers.filter_workers_for_dispatch(rows, client=client)
        )

        self.assertEqual(allowed, [rows[2]])
        self.assertEqual(rows[0].dispatch_block_reason, "maintenance")
        self.assertIn("low_disk", rows[1].dispatch_block_reason)
        self.assertIsNone(rows[2].dispatch_block_reason)
        self.assertEqual(len(client.calls), 3)

    def test_select_best_worker_does_not_optimistically_reuse_blocked_node(self):
        responsive = [
            workers.WorkerInfo(
                url="https://f1.example/api-converter-glb",
                available=True,
                load=0,
            )
        ]
        with (
            patch.object(
                workers,
                "get_configured_workers_with_weight",
                AsyncMock(return_value=[(responsive[0].url, 1)]),
            ),
            patch.object(
                workers,
                "get_all_workers_status",
                AsyncMock(return_value=responsive),
            ),
            patch.object(
                workers,
                "filter_workers_for_dispatch",
                AsyncMock(return_value=[]),
            ),
        ):
            selected = asyncio.run(workers.select_best_worker())
        self.assertIsNone(selected)


if __name__ == "__main__":
    unittest.main()
