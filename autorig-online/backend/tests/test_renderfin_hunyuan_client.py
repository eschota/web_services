import asyncio
import unittest
from unittest.mock import patch

import httpx

from renderfin import config, hunyuan_client


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


POOL = [
    {"name": "f7", "url": "http://127.0.0.1:15131", "token": "tok-f7"},
    {"name": "f13", "url": "http://127.0.0.1:15267", "token": "tok-f13"},
]


class StatusUrlRebaseTests(unittest.TestCase):
    def test_status_url_is_rebased_on_the_worker(self):
        """The worker builds status_url from the Host header and drops the port,
        which would point the client at an unrelated service."""

        async def scenario():
            seen = {}

            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                    })
                if request.url.path.endswith("/generate-3d"):
                    seen["auth"] = request.headers.get("Authorization")
                    return httpx.Response(202, json={
                        "task_id": "h-42",
                        "status": "Pending",
                        # port-less, points at whatever runs on :80 of the caller
                        "status_url": "http://127.0.0.1/api-converter-glb/generate-3d/status/h-42",
                    })
                return httpx.Response(404)

            transport = httpx.MockTransport(handler)
            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=transport) as client:
                    worker, status_url = await hunyuan_client.submit(
                        client, image_url="https://x/iso.png"
                    )
            self.assertEqual(worker["name"], "f7")
            self.assertEqual(
                status_url, "http://127.0.0.1:15131/api-converter-glb/generate-3d/status/h-42"
            )
            self.assertEqual(seen["auth"], "Bearer tok-f7")

        run(scenario())

    def test_status_url_without_task_id_keeps_path_only(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                    })
                return httpx.Response(202, json={
                    "status_url": "http://127.0.0.1/api-converter-glb/generate-3d/status/abc"
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    _, status_url = await hunyuan_client.submit(client, image_url="https://x/i.png")
            self.assertEqual(
                status_url, "http://127.0.0.1:15131/api-converter-glb/generate-3d/status/abc"
            )

        run(scenario())


class WorkerSelectionTests(unittest.TestCase):
    def test_busy_worker_skipped_for_idle_one(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if "15131" in str(request.url):  # f7 busy
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True,
                                    "service_state": "GeneratingShape", "queue_size": 2}
                    })
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker = await hunyuan_client.pick_worker(client)
            self.assertEqual(worker["name"], "f13")

        run(scenario())

    def test_all_busy_picks_shortest_queue(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                q = 5 if "15131" in str(request.url) else 1
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True,
                                "service_state": "GeneratingPBR", "queue_size": q}
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker = await hunyuan_client.pick_worker(client)
            self.assertEqual(worker["name"], "f13")

        run(scenario())

    def test_box_level_queue_beats_idle_hunyuan_flag(self):
        """Hunyuan shares the box queue with rig/convert jobs: an 'idle' hunyuan
        runtime on a box with a backlog must lose to an emptier box."""

        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                busy = "15131" in str(request.url)
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True,
                                "service_state": "idle", "queue_size": 0},
                    "tasks_summary": {
                        "queue_size": 2 if busy else 0,
                        "processing": 1,
                    },
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker = await hunyuan_client.pick_worker(client)
            self.assertEqual(worker["name"], "f13")

        run(scenario())

    def test_unreachable_worker_skipped(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if "15131" in str(request.url):
                    raise httpx.ConnectError("boom")
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker = await hunyuan_client.pick_worker(client)
            self.assertEqual(worker["name"], "f13")

        run(scenario())

    def test_no_enabled_worker_raises(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"hunyuan": {"enabled": False, "installed": False}})

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    # a distinct type: an empty fleet says nothing about the job,
                    # so the pipeline waits it out instead of spending attempts
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable):
                        await hunyuan_client.pick_worker(client)

        run(scenario())

    def test_empty_fleet_is_not_a_job_failure(self):
        """NoWorkerAvailable must not be caught as a per-job HunyuanClientError."""
        self.assertFalse(
            issubclass(hunyuan_client.NoWorkerAvailable, hunyuan_client.HunyuanClientError)
        )
        self.assertTrue(issubclass(hunyuan_client.NoWorkerAvailable, RuntimeError))

    def test_worker_for_url_resolves_owner(self):
        with patch.object(config, "hunyuan_workers", lambda: POOL):
            w = hunyuan_client.worker_for_url(
                "http://127.0.0.1:15267/api-converter-glb/generate-3d/status/x"
            )
            self.assertIsNotNone(w)
            self.assertEqual(w["name"], "f13")
            self.assertIsNone(hunyuan_client.worker_for_url("http://otherhost/x"))


class PollToleranceTests(unittest.TestCase):
    def test_transient_404_tolerated_then_completes(self):
        async def scenario():
            calls = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                calls["n"] += 1
                if calls["n"] <= 2:
                    return httpx.Response(404, json={"error": "Task not found"})
                return httpx.Response(200, json={
                    "status": "Completed",
                    "output_urls": {"model": "http://127.0.0.1:15131/out/model.glb"},
                })

            worker = POOL[0]
            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    payload = await hunyuan_client.wait_for_model(
                        client, worker, worker["url"] + "/status/x", timeout=10
                    )
            self.assertEqual(payload["status"], "Completed")

        run(scenario())

    def test_persistent_404_is_a_lost_task_not_a_job_failure(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(404, json={"error": "Task not found"})

            worker = POOL[0]
            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.TaskVanished):
                        await hunyuan_client.wait_for_model(
                            client, worker, worker["url"] + "/status/x", timeout=10
                        )

        run(scenario())

    def test_failed_status_raises_with_worker_name(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"status": "Failed", "error": "gpu oom"})

            worker = POOL[0]
            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.HunyuanClientError) as ctx:
                        await hunyuan_client.wait_for_model(
                            client, worker, worker["url"] + "/status/x", timeout=10
                        )
            self.assertIn("gpu oom", str(ctx.exception))
            self.assertIn("f7", str(ctx.exception))

        run(scenario())


if __name__ == "__main__":
    unittest.main()


class StaleTokenTests(unittest.TestCase):
    """A box re-provisions its token on restart; that is not the job's fault."""

    def _submit(self, status_code):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                    })
                return httpx.Response(status_code, json={"error": "unauthorized"})

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    await hunyuan_client.submit(client, image_url="https://x/a.png")

        return scenario

    def test_401_is_a_wait_not_a_job_failure(self):
        with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
            run(self._submit(401)())
        self.assertIn("rejected our token", str(caught.exception))

    def test_403_is_a_wait_too(self):
        with self.assertRaises(hunyuan_client.NoWorkerAvailable):
            run(self._submit(403)())

    def test_a_real_rejection_still_fails_the_job(self):
        with self.assertRaises(hunyuan_client.HunyuanClientError):
            run(self._submit(500)())


class ParkedWorkerTests(unittest.TestCase):
    """A box can be taken out of the pool without losing how to reach it."""

    def _pool_from(self, entries):
        import json as _json
        import tempfile
        from pathlib import Path as _P

        with tempfile.TemporaryDirectory() as tmp:
            path = _P(tmp) / "workers.json"
            path.write_text(_json.dumps(entries), encoding="utf-8")
            with patch.object(config, "HUNYUAN_WORKERS_FILE", path):
                return config.hunyuan_workers()

    def test_a_disabled_worker_is_not_offered(self):
        pool = self._pool_from([
            {"name": "f7", "url": "https://f7", "token": "t", "enabled": False,
             "disabled_reason": "reboots without shutting down cleanly"},
            {"name": "f13", "url": "https://f13", "token": "t"},
        ])
        self.assertEqual([w["name"] for w in pool], ["f13"])

    def test_the_disabled_alias_works_too(self):
        pool = self._pool_from([
            {"name": "f7", "url": "https://f7", "token": "t", "disabled": True},
            {"name": "f13", "url": "https://f13", "token": "t"},
        ])
        self.assertEqual([w["name"] for w in pool], ["f13"])

    def test_re_enabling_is_one_word(self):
        entry = {"name": "f7", "url": "https://f7", "token": "t", "enabled": True}
        self.assertEqual([w["name"] for w in self._pool_from([entry])], ["f7"])

    def test_a_parked_worker_cannot_be_picked(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                })

            with patch.object(config, "hunyuan_workers", lambda: []):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable):
                        await hunyuan_client.pick_worker(client)

        run(scenario())
