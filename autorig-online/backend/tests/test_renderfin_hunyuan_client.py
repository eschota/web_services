import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from renderfin import config, hunyuan_client


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


POOL = [
    {"name": "f7", "url": "http://127.0.0.1:15131", "token": "tok-f7", "pool": "dedicated"},
    {"name": "f13", "url": "http://127.0.0.1:15267", "token": "tok-f13", "pool": "dedicated"},
]
SHARED_POOL = [
    dict(worker, pool="shared_converter", capability_mode="full") for worker in POOL
]


class PriorityPreemptionTests(unittest.TestCase):
    def test_multiple_hunyuan_victims_are_recalled_in_one_parallel_window(self):
        async def scenario():
            entered = []
            both_entered = asyncio.Event()
            release = asyncio.Event()

            async def fake_status(_client, worker):
                return {
                    "feature_flags": {
                        "collection_preemption_v1": True,
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "processing_tasks": [{
                        "task_id": f"h-{worker['name']}",
                        "backend_task_id": f"job-{worker['name']}",
                        "type": "HunyuanGenerationTask",
                        "queue_class": "collection_background",
                        "preemptible": True,
                    }],
                    "pending_tasks": [],
                }

            async def fake_preempt(_client, candidate, **_kwargs):
                entered.append(candidate[3]["name"])
                if len(entered) == 2:
                    both_entered.set()
                await asyncio.wait_for(both_entered.wait(), timeout=0.5)
                await release.wait()
                return candidate[3]

            with patch.object(hunyuan_client, "workers", lambda: SHARED_POOL), patch.object(
                hunyuan_client, "server_status", new=fake_status
            ), patch.object(
                hunyuan_client, "_preempt_hunyuan_candidate", new=fake_preempt
            ):
                recall = asyncio.create_task(
                    hunyuan_client.preempt_background_hunyuan_many(
                        object(), limit=2, shared_full_converter_only=True
                    )
                )
                await asyncio.wait_for(both_entered.wait(), timeout=0.5)
                release.set()
                released = await recall

            self.assertEqual(set(entered), {"f7", "f13"})
            self.assertEqual({worker["name"] for worker in released}, {"f7", "f13"})

        run(scenario())

    def test_shared_recall_requires_explicit_full_converter_capability(self):
        self.assertFalse(
            hunyuan_client._status_is_full_converter({
                "feature_flags": {
                    "collection_preemption_v1": True,
                    "converter_capability_mode": "hunyuan_only",
                    "legacy_conversion_enabled": False,
                },
                "capabilities": {
                    "mode": "hunyuan_only",
                    "legacy_conversion": False,
                },
            })
        )
        self.assertTrue(
            hunyuan_client._status_is_full_converter({
                "feature_flags": {
                    "collection_preemption_v1": True,
                    "converter_capability_mode": "full",
                    "legacy_conversion_enabled": True,
                },
                "capabilities": {"mode": "full", "legacy_conversion": True},
            })
        )

    def test_shared_recall_requires_full_capability_in_registry_too(self):
        async def scenario():
            probed = []

            async def fake_status(_client, worker):
                probed.append(worker["name"])
                return {
                    "feature_flags": {
                        "collection_preemption_v1": True,
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                }

            contradictory = [
                dict(SHARED_POOL[0], capability_mode="hunyuan_only")
            ]
            with patch.object(
                hunyuan_client, "workers", lambda: contradictory
            ), patch.object(hunyuan_client, "server_status", new=fake_status):
                released = await hunyuan_client.preempt_background_hunyuan_many(
                    object(), limit=1, shared_full_converter_only=True
                )
            self.assertEqual(released, [])
            self.assertEqual(probed, [])

        run(scenario())

    def test_hunyuan_preempt_http_cannot_run_past_absolute_deadline(self):
        async def scenario():
            class SlowClient:
                async def post(self, *_args, **_kwargs):
                    await asyncio.sleep(0.2)
                    return httpx.Response(202)

            candidate = (
                0.0,
                0.0,
                "h-bg",
                SHARED_POOL[0],
                {"backend_task_id": "job-bg"},
            )
            started = asyncio.get_running_loop().time()
            with self.assertRaises(asyncio.TimeoutError):
                await hunyuan_client._preempt_hunyuan_candidate(
                    SlowClient(),
                    candidate,
                    deadline=asyncio.get_running_loop().time() + 0.03,
                )
            self.assertLess(asyncio.get_running_loop().time() - started, 0.15)

        run(scenario())

    def test_full_converter_recall_never_uses_dedicated_hunyuan_nodes(self):
        async def scenario():
            probes = []

            async def fake_status(_client, worker):
                probes.append(worker["name"])
                return {
                    "feature_flags": {"collection_preemption_v1": True},
                    "processing_tasks": [{
                        "task_id": "h-bg",
                        "backend_task_id": "job-bg",
                        "queue_class": "collection_background",
                        "preemptible": True,
                    }],
                }

            with patch.object(config, "hunyuan_workers", lambda: POOL), patch.object(
                hunyuan_client, "server_status", new=fake_status
            ):
                released = await hunyuan_client.preempt_background_hunyuan(
                    object(), shared_full_converter_only=True
                )
            self.assertIsNone(released)
            self.assertEqual(probes, [])

        run(scenario())

    def test_accepted_pending_hunyuan_task_is_a_recall_candidate(self):
        async def scenario():
            selected = []

            async def fake_status(_client, worker):
                return {
                    "feature_flags": {
                        "collection_preemption_v1": True,
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "processing_tasks": [],
                    "pending_tasks": [{
                        "task_id": "h-pending",
                        "backend_task_id": "job-pending",
                        "type": "HunyuanGenerationTask",
                        "queue_class": "collection_background",
                        "preemptible": True,
                    }],
                }

            async def fake_preempt(_client, candidate, **_kwargs):
                selected.append(candidate[2])
                return candidate[3]

            with patch.object(
                hunyuan_client, "workers", lambda: [SHARED_POOL[0]]
            ), patch.object(
                hunyuan_client, "server_status", new=fake_status
            ), patch.object(
                hunyuan_client, "_preempt_hunyuan_candidate", new=fake_preempt
            ):
                released = await hunyuan_client.preempt_background_hunyuan_many(
                    object(), limit=1, shared_full_converter_only=True
                )
            self.assertEqual(selected, ["h-pending"])
            self.assertEqual(released[0]["name"], SHARED_POOL[0]["name"])

        run(scenario())

    def test_release_proof_requires_explicit_zero_queue_telemetry(self):
        for payload in (
            {},
            {"processing_tasks": []},
            {"processing_tasks": "none", "tasks_summary": {}},
            {
                "processing_tasks": [],
                "tasks_summary": {"processing": 0, "pending": 0},
            },
        ):
            self.assertFalse(
                hunyuan_client._status_proves_hunyuan_idle(payload, "h-bg")
            )
        self.assertTrue(
            hunyuan_client._status_proves_hunyuan_idle(
                {
                    "processing_tasks": [],
                    "pending_tasks": [],
                    "tasks_summary": {"processing": 0, "pending": 0, "queue_size": 0},
                },
                "h-bg",
            )
        )

    def test_released_victim_is_removed_from_in_flight_before_repick(self):
        async def scenario():
            seen_in_flight = []

            async def fake_pick(
                _client, in_flight=None, excluded=None, *, queue_class="interactive"
            ):
                seen_in_flight.append(dict(in_flight or {}))
                if len(seen_in_flight) == 1:
                    raise hunyuan_client.NoWorkerAvailable("at capacity")
                return POOL[0]

            async def fake_preempt(_client):
                return POOL[0]

            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/generate-3d"):
                    return httpx.Response(202, json={"task_id": "h-interactive"})
                return httpx.Response(404)

            with patch.object(hunyuan_client, "pick_worker", new=fake_pick), patch.object(
                hunyuan_client, "preempt_background_hunyuan", new=fake_preempt
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    worker, _status = await hunyuan_client.submit(
                        client,
                        image_url="https://autorig.online/i.png",
                        queue_class="interactive",
                        in_flight={"f7": 1},
                    )
            self.assertEqual(worker["name"], "f7")
            self.assertEqual(seen_in_flight, [{"f7": 1}, {}])

        run(scenario())


class StatusUrlRebaseTests(unittest.TestCase):
    def test_worker_dns_failure_is_rotatable_infrastructure_error(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                    })
                return httpx.Response(
                    400,
                    json={
                        "error": "invalid_request",
                        "message": "image_url host cannot be resolved: getaddrinfo failed",
                    },
                )

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.WorkerInputFetchError) as caught:
                        await hunyuan_client.submit(client, image_url="https://autorig.online/i.png")
            self.assertEqual(caught.exception.worker_name, "f7")

        run(scenario())

    def test_excluded_worker_rotates_to_next_box(self):
        async def scenario():
            seen = []

            def handler(request: httpx.Request) -> httpx.Response:
                seen.append(str(request.url))
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                    })
                return httpx.Response(202, json={"task_id": "h-rotated"})

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker, _ = await hunyuan_client.submit(
                        client,
                        image_url="https://autorig.online/i.png",
                        excluded={"f7"},
                    )
            self.assertEqual(worker["name"], "f13")
            self.assertFalse(any("15131" in url for url in seen))

        run(scenario())

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


class ModelDownloadTests(unittest.TestCase):
    WORKER = {"name": "f13", "url": "http://127.0.0.1:15267", "token": "tok-f13"}
    ADVERTISED = "https://converter-f13.freestock.online/api-converter-glb/output/x.glb"

    def test_model_uses_worker_origin_before_public_facade(self):
        async def scenario():
            seen = []

            def handler(request: httpx.Request) -> httpx.Response:
                seen.append(str(request.url))
                return httpx.Response(200, content=b"G" * 2048)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                data = await hunyuan_client.download_model(client, self.WORKER, self.ADVERTISED)
            self.assertEqual(len(data), 2048)
            self.assertEqual(
                seen,
                ["http://127.0.0.1:15267/api-converter-glb/output/x.glb"],
            )

        run(scenario())

    def test_transient_502_is_retried(self):
        async def scenario():
            attempts = []

            def handler(request: httpx.Request) -> httpx.Response:
                attempts.append(str(request.url))
                if len(attempts) <= 2:
                    return httpx.Response(502, text="bad gateway")
                return httpx.Response(200, content=b"G" * 4096)

            with patch.object(hunyuan_client, "_DOWNLOAD_RETRY_SECONDS", 0):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    data = await hunyuan_client.download_model(client, self.WORKER, self.ADVERTISED)
            self.assertEqual(len(data), 4096)
            self.assertEqual(attempts[1], self.ADVERTISED)

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

    def test_all_busy_waits_in_the_central_queue(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                q = 5 if "15131" in str(request.url) else 1
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True,
                                "service_state": "GeneratingPBR", "queue_size": q}
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable):
                        await hunyuan_client.pick_worker(client)

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
                        "processing": 1 if busy else 0,
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

    def test_dedicated_worker_wins_over_an_idle_shared_converter(self):
        async def scenario():
            tiered = [
                dict(SHARED_POOL[0], priority=1),
                dict(POOL[1], priority=100),
            ]

            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"},
                    "accepting_hunyuan": True,
                    "tasks_summary": {"queue_size": 0, "processing": 0},
                })

            with patch.object(config, "hunyuan_workers", lambda: tiered):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    worker = await hunyuan_client.pick_worker(client)
            self.assertEqual(worker["name"], "f13")

        run(scenario())

    def test_gpu_busy_response_parks_without_spending_a_job_attempt(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/server-status"):
                    return httpx.Response(200, json={
                        "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"},
                        "accepting_hunyuan": True,
                    })
                return httpx.Response(503, json={
                    "error": "gpu_busy_comfy",
                    "retryable": True,
                })

            with patch.object(config, "hunyuan_workers", lambda: POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.submit(client, image_url="https://x/i.png")
            self.assertIn("gpu_busy_comfy", str(caught.exception))

        run(scenario())

    def test_ordinary_queue_blocks_shared_fallback(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={
                    "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"},
                    "tasks_summary": {"queue_size": 0, "processing": 0},
                })

            with patch.object(config, "hunyuan_workers", lambda: SHARED_POOL), \
                 patch.object(hunyuan_client, "ordinary_conversion_waiting", return_value=True):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.pick_worker(client)
            self.assertIn("ordinary conversion", str(caught.exception))

        run(scenario())

    def test_background_hunyuan_counts_persisted_full_converter_occupancy(self):
        async def scenario(database: Path):
            pool = [
                {
                    "name": name,
                    "url": f"http://127.0.0.1:{port}",
                    "token": f"tok-{name}",
                    "pool": "shared_converter",
                    "capability_mode": "full",
                }
                for name, port in (
                    ("f11", 15533),
                    ("f2", 15279),
                    ("f1", 15132),
                    ("f13", 15267),
                )
            ]
            busy = {"f11", "f1", "f13"}

            def handler(request: httpx.Request) -> httpx.Response:
                name = next(
                    worker["name"]
                    for worker in pool
                    if request.url.port == int(worker["url"].rsplit(":", 1)[1])
                )
                processing = name in busy
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": True,
                        "installed": True,
                        "service_state": "idle",
                    },
                    # Simulate rolling-upgrade tasks whose worker payload still
                    # says interactive; the durable AutoRig row is authoritative.
                    "processing_tasks": ([{
                        "task_id": f"convert-{name}",
                        "queue_class": "interactive",
                    }] if processing else []),
                    "pending_tasks": [],
                    "tasks_summary": {
                        "queue_size": 0,
                        "processing": 1 if processing else 0,
                    },
                })

            with patch.object(config, "hunyuan_workers", lambda: pool), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", database
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.pick_worker(
                            client, queue_class="collection_background"
                        )
            self.assertIn("3/4", str(caught.exception))
            self.assertIn("reserve=1", str(caught.exception))

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "autorig.db"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE tasks (worker_api TEXT, status TEXT, queue_class TEXT)"
                )
                connection.executemany(
                    "INSERT INTO tasks VALUES (?, 'processing', 'collection_background')",
                    [
                        ("https://converter-f11.freestock.online/api-converter-glb",),
                        ("http://127.0.0.1:15132/api-converter-glb",),
                        ("https://converter-f13.freestock.online/api-converter-glb",),
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            run(scenario(database))

    def test_interactive_hunyuan_can_use_the_reserved_shared_slot(self):
        async def scenario(database: Path):
            pool = [
                {
                    "name": name,
                    "url": f"http://127.0.0.1:{port}",
                    "token": f"tok-{name}",
                    "pool": "shared_converter",
                    "capability_mode": "full",
                }
                for name, port in (
                    ("f11", 15533),
                    ("f2", 15279),
                    ("f1", 15132),
                    ("f13", 15267),
                )
            ]

            def handler(request: httpx.Request) -> httpx.Response:
                idle = request.url.port == 15279
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": True,
                        "installed": True,
                        "service_state": "idle",
                    },
                    "processing_tasks": [] if idle else [{
                        "task_id": "background-conversion",
                        "queue_class": "collection_background",
                    }],
                    "pending_tasks": [],
                    "tasks_summary": {
                        "queue_size": 0,
                        "processing": 0 if idle else 1,
                    },
                })

            with patch.object(config, "hunyuan_workers", lambda: pool), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", database
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    worker = await hunyuan_client.pick_worker(
                        client, queue_class="interactive"
                    )
            self.assertEqual(worker["name"], "f2")

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "autorig.db"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE tasks (worker_api TEXT, status TEXT, queue_class TEXT)"
                )
                connection.executemany(
                    "INSERT INTO tasks VALUES (?, 'processing', 'collection_background')",
                    [
                        ("https://converter-f11.freestock.online/api-converter-glb",),
                        ("https://converter-f1.freestock.online/api-converter-glb",),
                        ("https://converter-f13.freestock.online/api-converter-glb",),
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            run(scenario(database))

    def test_cross_pipeline_snapshot_reports_and_repairs_one_excess_slot(self):
        async def scenario(database: Path):
            pool = [
                {
                    "name": name,
                    "url": f"http://127.0.0.1:{port}",
                    "token": f"tok-{name}",
                    "pool": "shared_converter",
                    "capability_mode": "full",
                }
                for name, port in (
                    ("f11", 15533),
                    ("f2", 15279),
                    ("f1", 15132),
                    ("f13", 15267),
                )
            ]

            def handler(request: httpx.Request) -> httpx.Response:
                name = next(
                    worker["name"]
                    for worker in pool
                    if request.url.port == int(worker["url"].rsplit(":", 1)[1])
                )
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": True,
                        "installed": True,
                        "service_state": "idle",
                    },
                    "processing_tasks": ([{
                        "task_id": f"hunyuan-{name}",
                        "queue_class": "collection_background",
                    }] if name in {"f2", "f13"} else [{
                        "task_id": f"convert-{name}",
                        "queue_class": "collection_background",
                    }]),
                    "pending_tasks": [],
                    "tasks_summary": {"queue_size": 0, "processing": 1},
                })

            with patch.object(config, "hunyuan_workers", lambda: pool), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", database
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    snapshot = await hunyuan_client.shared_full_background_capacity(
                        client
                    )
            self.assertEqual(snapshot["healthy"], 4)
            self.assertEqual(snapshot["background_occupied"], 4)
            self.assertEqual(snapshot["background_limit"], 3)
            self.assertEqual(snapshot["available_background_slots"], 0)
            self.assertEqual(snapshot["excess_background_slots"], 1)

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "autorig.db"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE tasks (worker_api TEXT, status TEXT, queue_class TEXT)"
                )
                connection.executemany(
                    "INSERT INTO tasks VALUES (?, 'processing', 'collection_background')",
                    [
                        ("https://converter-f11.freestock.online/api-converter-glb",),
                        ("https://converter-f1.freestock.online/api-converter-glb",),
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            run(scenario(database))

    def test_cross_pipeline_capacity_counts_hunyuan_quarantined_full_worker(self):
        async def scenario(database: Path):
            pool = [
                {
                    "name": name,
                    "url": f"https://converter-{name}.freestock.online",
                    "token": f"tok-{name}",
                    "pool": "shared_converter",
                    "capability_mode": "full",
                }
                for name in ("f1", "f2", "f13")
            ]

            def handler(request: httpx.Request) -> httpx.Response:
                name = request.url.host.split(".", 1)[0].split("converter-", 1)[-1]
                occupied = name in {"f1", "f13"}
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": name != "f11",
                        "installed": True,
                        "service_state": "idle",
                    },
                    "processing_tasks": ([{
                        "task_id": f"background-{name}",
                        "queue_class": "collection_background",
                    }] if occupied else []),
                    "pending_tasks": [],
                    "tasks_summary": {
                        "queue_size": 0,
                        "processing": 1 if occupied else 0,
                    },
                })

            with patch.object(config, "hunyuan_workers", lambda: pool), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", database
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    snapshot = await hunyuan_client.shared_full_background_capacity(
                        client
                    )
            self.assertEqual(snapshot["healthy"], 4)
            self.assertEqual(snapshot["background_occupied"], 2)
            self.assertEqual(snapshot["background_limit"], 3)
            self.assertEqual(snapshot["available_background_slots"], 1)

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "autorig.db"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE tasks (worker_api TEXT, status TEXT, queue_class TEXT)"
                )
                connection.execute(
                    "CREATE TABLE worker_endpoints (url TEXT, enabled INTEGER)"
                )
                connection.executemany(
                    "INSERT INTO worker_endpoints VALUES (?, 1)",
                    [
                        (f"https://converter-{name}.freestock.online/api-converter-glb",)
                        for name in ("f1", "f2", "f11", "f13")
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            run(scenario(database))

    def test_background_hunyuan_reserve_counts_conversion_only_full_worker(self):
        async def scenario(database: Path):
            pool = [
                {
                    "name": name,
                    "url": f"https://converter-{name}.freestock.online",
                    "token": f"tok-{name}",
                    "pool": "shared_converter",
                    "capability_mode": "full",
                }
                for name in ("f1", "f2", "f13")
            ]

            def handler(request: httpx.Request) -> httpx.Response:
                name = request.url.host.split(".", 1)[0].split("converter-", 1)[-1]
                occupied = name in {"f1", "f13"}
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": name != "f11",
                        "installed": True,
                        "service_state": "idle",
                    },
                    "processing_tasks": ([{
                        "task_id": f"background-{name}",
                        "queue_class": "collection_background",
                    }] if occupied else []),
                    "pending_tasks": [],
                    "tasks_summary": {
                        "queue_size": 0,
                        "processing": 1 if occupied else 0,
                    },
                })

            with patch.object(config, "hunyuan_workers", lambda: pool), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", database
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    worker = await hunyuan_client.pick_worker(
                        client, queue_class="collection_background"
                    )
            self.assertEqual(worker["name"], "f2")

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "autorig.db"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE tasks (worker_api TEXT, status TEXT, queue_class TEXT)"
                )
                connection.execute(
                    "CREATE TABLE worker_endpoints (url TEXT, enabled INTEGER)"
                )
                connection.executemany(
                    "INSERT INTO worker_endpoints VALUES (?, 1)",
                    [
                        (f"https://converter-{name}.freestock.online/api-converter-glb",)
                        for name in ("f1", "f2", "f11", "f13")
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            run(scenario(database))

    def test_background_hunyuan_never_uses_the_only_full_converter(self):
        async def scenario():
            worker = {
                "name": "f2",
                "url": "http://127.0.0.1:15279",
                "token": "tok-f2",
                "pool": "shared_converter",
                "capability_mode": "full",
            }

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={
                    "capabilities": {"mode": "full", "legacy_conversion": True},
                    "feature_flags": {
                        "converter_capability_mode": "full",
                        "legacy_conversion_enabled": True,
                    },
                    "hunyuan": {
                        "enabled": True,
                        "installed": True,
                        "service_state": "idle",
                    },
                    "processing_tasks": [],
                    "pending_tasks": [],
                    "tasks_summary": {
                        "queue_size": 0,
                        "processing": 0,
                        "pending": 0,
                    },
                })

            with tempfile.TemporaryDirectory() as tmp, patch.object(
                config, "hunyuan_workers", lambda: [worker]
            ), patch.object(
                config, "AUTORIG_QUEUE_DB_PATH", Path(tmp) / "missing.db"
            ), patch.object(hunyuan_client, "RESERVED_FOR_OTHER_WORK", 1):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.pick_worker(
                            client, queue_class="collection_background"
                        )
            self.assertIn("0/1", str(caught.exception))

        run(scenario())


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

    def test_a_tunnel_blip_is_ridden_out(self):
        """The supervisor restarts a tunnel every 10s; that must not cost a job."""
        async def scenario():
            calls = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                calls["n"] += 1
                if calls["n"] <= 3:
                    raise httpx.ConnectError("All connection attempts failed")
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

    def test_a_route_that_stays_down_frees_the_slot_instead_of_pinning_it(self):
        """Polling an address that refuses connections holds that worker's only
        slot. Held to the 4h ceiling on every job, the fleet reads as fully busy
        and the queue stops dead - which is exactly what happened when the
        tunnel unit died on 2026-08-03."""
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectError("All connection attempts failed")

            worker = POOL[0]
            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.001), \
                 patch.object(hunyuan_client, "MAX_TRANSPORT_MISSES", 4):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    with self.assertRaises(hunyuan_client.WorkerUnreachable) as ctx:
                        await hunyuan_client.wait_for_model(
                            client, worker, worker["url"] + "/status/x", timeout=30
                        )
            self.assertIn("f7", str(ctx.exception))

        run(scenario())

    def test_the_miss_counter_resets_on_any_answer(self):
        """Otherwise a long generation with occasional blips eventually trips."""
        async def scenario():
            calls = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                calls["n"] += 1
                # fail, answer, fail, answer, ... never 3 failures in a row
                if calls["n"] % 2 == 1 and calls["n"] < 12:
                    raise httpx.ConnectError("All connection attempts failed")
                if calls["n"] < 12:
                    return httpx.Response(200, json={"status": "Generating"})
                return httpx.Response(200, json={
                    "status": "Completed",
                    "output_urls": {"model": "http://127.0.0.1:15131/out/model.glb"},
                })

            worker = POOL[0]
            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.001), \
                 patch.object(hunyuan_client, "MAX_TRANSPORT_MISSES", 3):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    payload = await hunyuan_client.wait_for_model(
                        client, worker, worker["url"] + "/status/x", timeout=30
                    )
            self.assertEqual(payload["status"], "Completed")

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

    def test_unchanged_parked_worker_notice_is_logged_once(self):
        import json as _json
        import tempfile
        from pathlib import Path as _P

        with tempfile.TemporaryDirectory() as tmp:
            path = _P(tmp) / "workers.json"
            path.write_text(_json.dumps([
                {"name": "f7", "url": "https://f7", "token": "t", "enabled": False,
                 "disabled_reason": "maintenance"},
                {"name": "f13", "url": "https://f13", "token": "t"},
            ]), encoding="utf-8")
            with patch.object(config, "HUNYUAN_WORKERS_FILE", path), patch.object(
                config, "_HUNYUAN_WORKER_NOTICE_STATE", set()
            ), patch("builtins.print") as output:
                config.hunyuan_workers()
                config.hunyuan_workers()

        self.assertEqual(output.call_count, 1)
        self.assertIn("maintenance", str(output.call_args))

    def test_cleared_parked_notice_is_logged_again_if_it_returns(self):
        import json as _json
        import tempfile
        from pathlib import Path as _P

        disabled = [
            {"name": "f7", "url": "https://f7", "token": "t", "enabled": False},
            {"name": "f13", "url": "https://f13", "token": "t"},
        ]
        enabled = [dict(disabled[0], enabled=True), disabled[1]]
        with tempfile.TemporaryDirectory() as tmp:
            path = _P(tmp) / "workers.json"
            with patch.object(config, "HUNYUAN_WORKERS_FILE", path), patch.object(
                config, "_HUNYUAN_WORKER_NOTICE_STATE", set()
            ), patch("builtins.print") as output:
                path.write_text(_json.dumps(disabled), encoding="utf-8")
                config.hunyuan_workers()
                path.write_text(_json.dumps(enabled), encoding="utf-8")
                config.hunyuan_workers()
                path.write_text(_json.dumps(disabled), encoding="utf-8")
                config.hunyuan_workers()

        self.assertEqual(output.call_count, 2)

    def test_unreadable_authoritative_file_records_resolution_error(self):
        import tempfile
        from pathlib import Path as _P

        with tempfile.TemporaryDirectory() as tmp:
            path = _P(tmp) / "workers.json"
            path.write_text("[]", encoding="utf-8")
            with patch.object(config, "HUNYUAN_WORKERS_FILE", path), patch.object(
                _P, "read_text", side_effect=PermissionError("denied")
            ):
                self.assertEqual(config.hunyuan_workers(), [])
                self.assertIn("denied", config.hunyuan_workers_last_error())
        config.hunyuan_workers()
        self.assertEqual(config.hunyuan_workers_last_error(), "")

    def test_the_disabled_alias_works_too(self):
        pool = self._pool_from([
            {"name": "f7", "url": "https://f7", "token": "t", "disabled": True},
            {"name": "f13", "url": "https://f13", "token": "t"},
        ])
        self.assertEqual([w["name"] for w in pool], ["f13"])

    def test_re_enabling_is_one_word(self):
        entry = {"name": "f7", "url": "https://f7", "token": "t", "enabled": True}
        self.assertEqual([w["name"] for w in self._pool_from([entry])], ["f7"])

    def test_canary_gate_keeps_unapproved_render_node_parked(self):
        pool = self._pool_from([
            {"name": "f5", "url": "https://f5", "token": "t", "pool": "dedicated",
             "canary_approved": False},
            {"name": "f12", "url": "https://f12", "token": "t", "pool": "dedicated"},
        ])
        self.assertEqual([worker["name"] for worker in pool], ["f12"])

    def test_physical_node_alias_is_not_counted_twice(self):
        pool = self._pool_from([
            {"name": "Raptor", "url": "https://raptor", "token": "t",
             "pool": "dedicated", "physical_node": "ryzen-server"},
            {"name": "RYZEN-SERVER", "url": "https://duplicate", "token": "t",
             "pool": "dedicated", "physical_node": "ryzen-server"},
        ])
        self.assertEqual([worker["name"] for worker in pool], ["Raptor"])

    def test_tier_fields_are_preserved(self):
        pool = self._pool_from([
            {"name": "f12", "url": "https://f12", "token": "t", "pool": "dedicated",
             "priority": 10, "capability_mode": "hunyuan_only"},
        ])
        self.assertEqual(pool[0]["pool"], "dedicated")
        self.assertEqual(pool[0]["priority"], 10)
        self.assertEqual(pool[0]["capability_mode"], "hunyuan_only")

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


class InFlightCapTests(unittest.TestCase):
    """A box that runs one generation at a time is handed one at a time."""

    @staticmethod
    def _ok_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "capabilities": {"mode": "full", "legacy_conversion": True},
            "feature_flags": {
                "converter_capability_mode": "full",
                "legacy_conversion_enabled": True,
            },
            "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"},
            "processing_tasks": [],
            "pending_tasks": [],
            "tasks_summary": {"queue_size": 0, "processing": 0, "pending": 0},
        })

    def test_a_busy_worker_is_not_offered(self):
        async def scenario():
            with patch.object(config, "hunyuan_workers", lambda: SHARED_POOL):
                async with httpx.AsyncClient(transport=httpx.MockTransport(self._ok_handler)) as c:
                    busy = {w["name"]: 1 for w in SHARED_POOL}
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.pick_worker(c, busy)
            # the message must say queue, not outage: they are handled differently
            self.assertIn("at capacity", str(caught.exception))

        run(scenario())

    def test_a_free_worker_is_still_offered(self):
        async def scenario():
            with patch.object(config, "hunyuan_workers", lambda: SHARED_POOL), patch.object(
                hunyuan_client, "RESERVED_FOR_OTHER_WORK", 0
            ):
                async with httpx.AsyncClient(transport=httpx.MockTransport(self._ok_handler)) as c:
                    worker = await hunyuan_client.pick_worker(c, {SHARED_POOL[0]["name"]: 1})
            self.assertNotEqual(worker["name"], SHARED_POOL[0]["name"])

        run(scenario())

    def test_an_empty_pool_still_reads_as_an_outage(self):
        async def scenario():
            with patch.object(config, "hunyuan_workers", lambda: []):
                async with httpx.AsyncClient(transport=httpx.MockTransport(self._ok_handler)) as c:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable) as caught:
                        await hunyuan_client.pick_worker(c, {})
            self.assertNotIn("at capacity", str(caught.exception))

        run(scenario())
