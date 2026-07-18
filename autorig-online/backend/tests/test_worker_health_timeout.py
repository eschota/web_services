"""Regression tests for FreeStock relay worker health deadlines."""

import unittest

from workers import (
    WORKER_HEALTH_TIMEOUT_SECONDS,
    get_worker_load,
    get_worker_queue_status,
)


class _Response:
    status_code = 200

    @staticmethod
    def json():
        return {
            "total_active": 0,
            "total_pending": 0,
            "queue_size": 0,
            "max_concurrent": 1,
        }


class _Client:
    def __init__(self):
        self.timeouts = []

    async def get(self, _url, *, timeout):
        self.timeouts.append(timeout)
        return _Response()


class WorkerHealthTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_and_queue_health_use_same_relay_deadline(self):
        client = _Client()

        load = await get_worker_load("https://worker.example/api-converter-glb", client)
        queue = await get_worker_queue_status(
            "https://worker.example/api-converter-glb",
            client,
        )

        self.assertTrue(load.available)
        self.assertTrue(queue.available)
        self.assertEqual(
            [WORKER_HEALTH_TIMEOUT_SECONDS, WORKER_HEALTH_TIMEOUT_SECONDS],
            client.timeouts,
        )
        self.assertGreaterEqual(WORKER_HEALTH_TIMEOUT_SECONDS, 10.0)


if __name__ == "__main__":
    unittest.main()
