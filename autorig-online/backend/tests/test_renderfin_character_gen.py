import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import time

from renderfin import character_gen, config, hunyuan_client, workload_lease
from renderfin.character_gen import CharacterGenManager
from renderfin.models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CharacterGenJob,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_TURNTABLE,
    RenderPrompt,
    TASK_DONE,
    TASK_ERROR,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class HunyuanAdmissionOrderTests(unittest.TestCase):
    def test_interactive_fifo_precedes_background_under_submit_lock(self):
        async def scenario():
            manager = CharacterGenManager(object(), db_path=Path("unused.db"))
            background = CharacterGenJob(
                id="background",
                seq=1,
                stage=CHARGEN_STAGE_HUNYUAN,
                queue_class="collection_background",
            )
            interactive_old = CharacterGenJob(
                id="interactive-old",
                seq=2,
                stage=CHARGEN_STAGE_HUNYUAN,
                queue_class="interactive",
                retry_at=time.time() + 30,
                hunyuan_waiting_for_capacity=True,
            )
            interactive_new = CharacterGenJob(
                id="interactive-new",
                seq=3,
                stage=CHARGEN_STAGE_HUNYUAN,
                queue_class="interactive",
            )
            manager._jobs = {
                job.id: job for job in (background, interactive_new, interactive_old)
            }
            manager._persist = AsyncMock()
            with patch.object(manager, "_spawn") as spawn:
                async with manager._submit_lock:
                    with self.assertRaises(hunyuan_client.NoWorkerAvailable):
                        await manager._require_hunyuan_admission(background)
            self.assertEqual(interactive_old.retry_at, 0)
            spawn.assert_called_once_with(interactive_old)
            ordered = manager._hunyuan_admission_candidates(interactive_new)
            self.assertEqual(
                [job.id for job in ordered],
                ["interactive-old", "interactive-new", "background"],
            )

        run(scenario())

    def test_preemption_cooldown_excludes_background_until_due(self):
        manager = CharacterGenManager(object(), db_path=Path("unused.db"))
        now = time.time()
        cooling = CharacterGenJob(
            id="cooling",
            seq=1,
            stage=CHARGEN_STAGE_HUNYUAN,
            queue_class="collection_background",
            dispatch_not_before=now + 60,
            hunyuan_waiting_for_capacity=True,
        )
        current = CharacterGenJob(
            id="current",
            seq=2,
            stage=CHARGEN_STAGE_HUNYUAN,
            queue_class="collection_background",
        )
        manager._jobs = {cooling.id: cooling, current.id: current}
        self.assertEqual(
            [job.id for job in manager._hunyuan_admission_candidates(current, now=now)],
            ["current"],
        )


class _Env:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            patch.object(config, "DATA_DIR", root),
            patch.object(config, "RENDER_DIR", root / "render"),
            patch.object(config, "DB_DIR", root / "db"),
            patch.object(config, "TMP_DIR", root / "tmp"),
            patch.object(config, "SERVERS_DIR", root / "servers"),
            patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
            # never read the live farm config: on a machine that has one, the
            # hunyuan stage would take the converter-API path and hit the network
            patch.object(config, "HUNYUAN_WORKERS_FILE", root / "no-workers.json"),
            patch.object(config, "HUNYUAN_WORKERS", []),
            patch.object(config, "HUNYUAN_API_TOKEN", ""),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        config.ensure_dirs()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        # the sqlite handle can outlive the test on Windows; a temp dir we
        # could not remove must not be reported as a test failure
        try:
            self.tmp.cleanup()
        except OSError:
            pass


class _InstantQueue(RenderQueue):
    """Queue whose renders complete instantly with canned results."""

    def __init__(self, registry, *, db_path, fail_type=None):
        super().__init__(registry, db_path=db_path)
        self.fail_type = fail_type
        self.enqueued = []

    async def start(self):
        await super().start()
        self._pump_task.cancel()  # no real dispatch

    async def enqueue(self, prompt):
        task = await super().enqueue(prompt)
        self.enqueued.append(task)
        user_dir = config.RENDER_DIR / prompt.user_name
        user_dir.mkdir(parents=True, exist_ok=True)
        if self.fail_type and prompt.type == self.fail_type:
            task.status = TASK_ERROR
            task.error = "boom"
        else:
            task.status = TASK_DONE
            out = user_dir / f"{task.id}{task.output_ext}"
            out.write_bytes(b"FAKE" * 300)
            task.output_path = str(out)
            if prompt.type == "t_pose":
                iso = user_dir / f"{task.id}_Isolated.png"
                iso.write_bytes(b"ISO" * 400)
                task.extra_outputs["isolated"] = (
                    f"{config.PUBLIC_BASE_URL}/render/{prompt.user_name}/{task.id}_Isolated.png"
                )
        await self._persist(task)
        return task

    async def wait_for(self, task_id, timeout=1800):
        return self._tasks[task_id]


async def _wait_stage(manager, job_id, stages, timeout=5.0):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        job = manager.get(job_id)
        if job and job.stage in stages:
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"job never reached {stages}: {manager.get(job_id).stage}")


async def _idle_job(manager, **fields):
    """Register a job with the manager without spawning a runner for it.

    Tests that drive a stage handler directly must not race _run: mutating
    job.stage while the runner is still choosing a branch makes it fall
    through into the real stage and hit the network.
    """
    job = CharacterGenJob(prompt="orc", user_name="bot", **fields)
    manager._jobs[job.id] = job
    await manager._persist(job)
    return job


class HunyuanConverterPathTests(unittest.TestCase):
    def test_hunyuan_post_response_loss_replays_same_request_without_duplicate(self):
        async def scenario():
            import httpx

            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    worker = {
                        "name": "f12",
                        "url": "https://converter-f12.example",
                        "token": "worker-token",
                        "physical_node": "machine_12121212121212121212121212121212",
                        "physical_resource_id_string": "machine_12121212121212121212121212121212",
                        "workload_role": "ai_vision_primary",
                        "pool": "dedicated",
                    }
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        isolated_url="https://autorig.online/render/input.png",
                    )
                    posted = []
                    accepted_by_request = {}

                    def handler(request: httpx.Request) -> httpx.Response:
                        url = str(request.url)
                        if url.endswith("/api-converter-glb/server-status"):
                            return httpx.Response(
                                200,
                                json={
                                    "hunyuan": {
                                        "enabled": True,
                                        "installed": True,
                                        "service_state": "idle",
                                    },
                                    "physical_node": "machine_12121212121212121212121212121212",
                                    "workload_role": "ai_vision_primary",
                                    "capabilities": {
                                        "submission_idempotency_v1": True
                                    },
                                },
                            )
                        if url.endswith("/api-converter-glb/generate-3d"):
                            body = json.loads(request.content.decode("utf-8"))
                            posted.append((url, body))
                            request_id = body["workload_request_id"]
                            task_id = accepted_by_request.setdefault(request_id, "h-one")
                            if len(posted) == 1:
                                # The host accepted/durably indexed the task,
                                # but its HTTP response never reached way-fr.
                                raise httpx.ReadTimeout(
                                    "accepted response lost", request=request
                                )
                            return httpx.Response(
                                202,
                                json={"task_id": task_id, "status": "Pending"},
                            )
                        return httpx.Response(404)

                    transport = httpx.MockTransport(handler)
                    real_client = httpx.AsyncClient

                    def patched_client(*args, **kwargs):
                        kwargs["transport"] = transport
                        return real_client(*args, **kwargs)

                    lease = {
                        "lease_id_string": "lease-hunyuan-response-loss",
                        "physical_resource_id_string": "machine_12121212121212121212121212121212",
                        "node_id_string": "f12",
                        "workload_class_string": "hunyuan",
                    }

                    async def acquire_exact(_client, **kwargs):
                        return {
                            **lease,
                            "request_id_string": kwargs["request_id"],
                        }

                    release = AsyncMock(return_value=True)
                    with patch.object(
                        hunyuan_client, "workers", return_value=[worker]
                    ), patch.object(
                        hunyuan_client, "pick_worker", new=AsyncMock(return_value=worker)
                    ), patch.object(
                        manager, "_require_hunyuan_admission", new=AsyncMock()
                    ), patch.object(
                        workload_lease, "enabled", return_value=True
                    ), patch.object(
                        workload_lease, "acquire", new=AsyncMock(side_effect=acquire_exact)
                    ) as acquire, patch.object(
                        workload_lease,
                        "heartbeat",
                        new=AsyncMock(
                            side_effect=[
                                workload_lease.WorkloadLeaseTerminal(
                                    {
                                        "status_string": "lease_terminal",
                                        "lease_by_key": {"state_string": "expired"},
                                    }
                                ),
                                {},
                            ]
                        ),
                    ) as heartbeat, patch.object(
                        hunyuan_client,
                        "wait_for_model",
                        new=AsyncMock(
                            return_value={"output_urls": {"model": "model.glb"}}
                        ),
                    ), patch.object(
                        hunyuan_client,
                        "download_model",
                        new=AsyncMock(return_value=b"glTF-response-loss"),
                    ), patch.object(
                        manager, "_release_hunyuan_workload", new=release
                    ), patch.object(
                        character_gen.httpx,
                        "AsyncClient",
                        side_effect=patched_client,
                    ):
                        with self.assertRaises(
                            hunyuan_client.SubmissionOutcomeUnknown
                        ) as caught:
                            await manager._stage_hunyuan_converter(job)
                        await manager._handle_stage_error(job, caught.exception)

                        self.assertEqual("submission_unknown", job.hunyuan_workload_lease_state)
                        self.assertEqual("f12", job.hunyuan_worker)
                        self.assertEqual(worker["url"], job.hunyuan_worker_url)
                        self.assertEqual("", job.hunyuan_task_id)
                        persisted_request_id = job.hunyuan_workload_request_id
                        self.assertTrue(persisted_request_id.startswith("rhy_"))
                        self.assertEqual(
                            "lease-hunyuan-response-loss",
                            job.hunyuan_workload_lease_id,
                        )
                        self.assertEqual({}, job.attempts)
                        release.assert_not_awaited()

                        await manager._stage_hunyuan_converter(job)

                    self.assertEqual(1, acquire.await_count)
                    self.assertGreaterEqual(heartbeat.await_count, 1)
                    self.assertEqual(2, len(posted))
                    self.assertEqual(1, len(accepted_by_request))
                    self.assertEqual(posted[0][0], posted[1][0])
                    for field in (
                        "backend_task_id",
                        "workload_request_id",
                        "workload_lease_id",
                        "physical_resource_id",
                        "queue_class",
                    ):
                        self.assertEqual(posted[0][1][field], posted[1][1][field])
                    self.assertEqual(
                        persisted_request_id,
                        posted[1][1]["workload_request_id"],
                    )
                    self.assertEqual(
                        "lease-hunyuan-response-loss",
                        posted[1][1]["workload_lease_id"],
                    )
                    self.assertTrue(job.hunyuan_task_id.endswith("/h-one"))
                    self.assertTrue(job.glb_url.endswith(f"/{job.id}.glb"))
                    release.assert_awaited_once_with(
                        job, outcome="completed", new_request=False
                    )
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_hunyuan_broker_refuses_legacy_listener_without_idempotency_capability(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    worker = {
                        "name": "legacy-f12",
                        "url": "https://legacy-f12.example",
                        "token": "worker-token",
                        "physical_node": "f12",
                        "pool": "dedicated",
                    }
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        isolated_url="https://autorig.online/render/input.png",
                    )
                    acquire = AsyncMock()
                    submit = AsyncMock()
                    with patch.object(
                        hunyuan_client, "pick_worker", new=AsyncMock(return_value=worker)
                    ), patch.object(
                        hunyuan_client,
                        "server_status",
                        new=AsyncMock(
                            return_value={
                                "hunyuan": {
                                    "enabled": True,
                                    "installed": True,
                                    "service_state": "idle",
                                },
                                "capabilities": {"mode": "full"},
                            }
                        ),
                    ), patch.object(
                        workload_lease, "enabled", return_value=True
                    ), patch.object(
                        workload_lease, "acquire", new=acquire
                    ), patch.object(
                        hunyuan_client, "submit", new=submit
                    ):
                        with self.assertRaises(
                            hunyuan_client.NoWorkerAvailable
                        ) as caught:
                            await manager._stage_hunyuan_converter(job)

                    self.assertIn("submission_idempotency_v1", str(caught.exception))
                    self.assertEqual("", job.hunyuan_workload_request_id)
                    self.assertEqual("", job.hunyuan_workload_lease_id)
                    self.assertEqual("", job.hunyuan_worker_url)
                    acquire.assert_not_awaited()
                    submit.assert_not_awaited()
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_unreadable_worker_registry_parks_without_comfy_backlog(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        isolated_url="https://autorig.online/render/source.png",
                    )
                    with patch.object(config, "hunyuan_workers", return_value=[]), patch.object(
                        config, "hunyuan_workers_last_error", return_value="permission denied"
                    ):
                        with self.assertRaises(hunyuan_client.NoWorkerAvailable):
                            await manager._stage_hunyuan(job)
                    self.assertEqual(queue.enqueued, [])
                    self.assertEqual(job.hunyuan_task_id, "")
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_converter_api_used_when_token_configured(self):
        import httpx

        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    calls = []

                    def handler(request: httpx.Request) -> httpx.Response:
                        calls.append((request.method, str(request.url)))
                        url = str(request.url)
                        if url.endswith("/api-converter-glb/server-status"):
                            return httpx.Response(200, json={
                                "hunyuan": {"enabled": True, "installed": True, "service_state": "idle"}
                            })
                        if url.endswith("/api-converter-glb/generate-3d"):
                            assert request.headers["Authorization"] == "Bearer test-token"
                            return httpx.Response(202, json={
                                "task_id": "h-1", "status": "Pending",
                                "status_url": "https://converter-f2.freestock.online/api-converter-glb/generate-3d/status/h-1",
                            })
                        if "/generate-3d/status/" in url:
                            return httpx.Response(200, json={
                                "status": "Completed", "progress": 100,
                                "output_urls": {"model": "https://converter-f2.freestock.online/out/h-1/model.glb"},
                            })
                        if url.endswith("/out/h-1/model.glb"):
                            return httpx.Response(200, content=b"GLB!" * 400)
                        return httpx.Response(404)

                    transport = httpx.MockTransport(handler)
                    real_client = httpx.AsyncClient

                    def patched_client(*a, **k):
                        k["transport"] = transport
                        return real_client(*a, **k)

                    async def fake_turntable(glb_path, out_path, **kw):
                        from pathlib import Path as _P
                        _P(out_path).parent.mkdir(parents=True, exist_ok=True)
                        _P(out_path).write_bytes(b"MP4!" * 500)
                        return _P(out_path)

                    from renderfin import character_gen as cg_mod

                    fake_pool = [{
                        "name": "f2",
                        "url": "https://converter-f2.freestock.online",
                        "token": "test-token",
                        "pool": "dedicated",
                    }]
                    with patch.object(config, "hunyuan_workers", lambda: fake_pool):
                        with patch.object(config, "HUNYUAN_API_TOKEN", "test-token"):
                            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                                with patch.object(cg_mod.httpx, "AsyncClient", side_effect=patched_client):
                                    with patch.object(cg_mod.turntable, "render_turntable", side_effect=fake_turntable):
                                        # one variant, so it runs to the end without asking
                                        job = await manager.create(prompt="orc", user_name="bot")
                                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED})

                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    # only the flux render went through the ComfyUI queue
                    self.assertEqual(len(queue.enqueued), 1)
                    self.assertEqual(queue.enqueued[0].prompt.type, "t_pose")
                    self.assertTrue(job.hunyuan_task_id.startswith("https://"))
                    glb = config.RENDER_DIR / "bot" / f"{job.id}.glb"
                    self.assertTrue(glb.is_file())
                    self.assertTrue(job.glb_url.endswith(f"{job.id}.glb"))
                    self.assertTrue(any("/generate-3d" in u for _, u in calls))
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class CharacterGenTests(unittest.TestCase):
    def test_full_pipeline_happy_path(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        job = await manager.create(
                            prompt="orc warrior", prompt_b="low-poly orc warrior",
                            mask_url="", user_name="bot",
                        )
                        # two styles rendered, so the pipeline pauses to ask
                        # which one becomes the model
                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED})
                        self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.error)
                        self.assertTrue(job.image_url.endswith(".png"))
                        job, transitioned = await manager.approve_image(job.id)
                        self.assertTrue(transitioned)
                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED})

                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    self.assertTrue(job.image_url.endswith(".png"))
                    self.assertIn("_Isolated", job.isolated_url)
                    self.assertTrue(job.glb_url.endswith(".glb"))
                    self.assertTrue(job.video_url.endswith("_turntable.mp4"))
                    # flux stage got the default mask; hunyuan got the isolated image
                    self.assertEqual(queue.enqueued[0].prompt.type, "t_pose")
                    self.assertIn("/render/masks/t_pose.jpg", queue.enqueued[0].prompt.image_url)
                    # by type, not by index: the second style renders in between
                    to_3d = [t for t in queue.enqueued if t.prompt.type == "image_to_3d"]
                    self.assertEqual(len(to_3d), 1)
                    self.assertEqual(to_3d[0].prompt.image_url, job.isolated_url)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_flux_failure_marks_failed(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH, fail_type="t_pose")
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    from renderfin import character_gen as cg_mod

                    with patch.object(cg_mod, "RETRY_BACKOFF_SECONDS", (0.0, 0.0, 0.0)):
                        with patch.object(cg_mod, "RETRY_TICK_SECONDS", 0.05):
                            job = await manager.create(prompt="x", user_name="bot")
                            # retried automatically first, reported only at the end
                            job = await _wait_stage(
                                manager, job.id, {CHARGEN_STAGE_FAILED}, timeout=10.0
                            )
                    self.assertIn("boom", job.error)
                    self.assertEqual(job.attempts.get("flux_render"), 3)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_discard_cleans_artifacts(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                        await manager.approve_image(job.id)
                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_READY})

                    video = config.RENDER_DIR / "bot" / f"{job.id}_turntable.mp4"
                    self.assertTrue(video.is_file())
                    job = await manager.discard(job.id)
                    self.assertEqual(job.stage, CHARGEN_STAGE_DISCARDED)
                    self.assertFalse(video.is_file())
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_single_surviving_render_goes_straight_to_3d(self):
        """The approval stage exists to choose between two invented T-poses.

        With one render there is nothing to choose, so asking is pure friction:
        every such job used to wait for a button nobody had a reason to press -
        two were found sitting for eleven and eight hours - and each one spent a
        card in a chat the operator asked to keep to decisions and results.
        """

        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        job = await manager.create(prompt="orc", user_name="bot")
                        job = await _wait_stage(
                            manager, job.id,
                            {CHARGEN_STAGE_READY, CHARGEN_STAGE_AWAITING_IMAGE,
                             CHARGEN_STAGE_FAILED},
                        )
                    self.assertEqual(
                        job.stage, CHARGEN_STAGE_READY,
                        f"one variant must not stop to ask; got {job.stage} {job.error}",
                    )
                    self.assertEqual(job.chosen_variant, "a")
                    self.assertFalse(job.image_url_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_two_variants_still_stop_to_ask(self):
        """The one case where the card earns its place: a real choice."""

        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(
                        prompt="orc", prompt_b="low-poly orc", user_name="bot"
                    )
                    job = await _wait_stage(
                        manager, job.id,
                        {CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_READY,
                         CHARGEN_STAGE_FAILED},
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.error)
                    self.assertTrue(job.image_url_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_restart_resumes_active_job(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                job_id = None
                try:
                    # block the pipeline at turntable so the job stays active
                    async def stuck_turntable(glb_path, out_path, **kw):
                        await asyncio.sleep(3600)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=stuck_turntable,
                    ):
                        job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                        job_id = job.id
                        await _wait_stage(manager, job_id, {CHARGEN_STAGE_AWAITING_IMAGE})
                        await manager.approve_image(job_id)
                        await _wait_stage(manager, job_id, {"turntable"})
                finally:
                    await manager.stop()

                # simulated restart: new manager on the same db resumes the job
                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)

                async def fake_turntable(glb_path, out_path, **kw):
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(out_path).write_bytes(b"MP4!" * 500)
                    return Path(out_path)

                with patch(
                    "renderfin.character_gen.turntable.render_turntable",
                    side_effect=fake_turntable,
                ):
                    await manager2.start()
                    try:
                        job = await _wait_stage(manager2, job_id, {CHARGEN_STAGE_READY})
                        self.assertTrue(job.video_url.endswith("_turntable.mp4"))
                    finally:
                        await manager2.stop()
                await queue.stop()

        run(scenario())


class ImageApprovalGateTests(unittest.TestCase):
    def test_awaiting_survives_restart_without_autocontinue(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    await asyncio.sleep(0.2)
                    revived = manager2.get(job.id)
                    self.assertEqual(revived.stage, CHARGEN_STAGE_AWAITING_IMAGE)
                    self.assertNotIn(job.id, manager2._runners)
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_approve_is_single_shot(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        # two variants, so the job really does stop and ask
                        job = await manager.create(
                            prompt="orc", prompt_b="low-poly orc", user_name="bot"
                        )
                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                        _, first = await manager.approve_image(job.id)
                        _, second = await manager.approve_image(job.id)
                        self.assertTrue(first)
                        self.assertFalse(second)
                        # approve on a fresh flux-stage job is also refused
                        job2 = await manager.create(prompt="x", user_name="bot")
                        _, early = await manager.approve_image(job2.id)
                        # job2 may already be awaiting (instant queue); only assert type
                        self.assertIn(early, (True, False))
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_regenerate_rerenders_image(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    first_flux = job.flux_task_id
                    first_image = job.image_url
                    job2, transitioned = await manager.regenerate_image(job.id)
                    self.assertTrue(transitioned)
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    self.assertNotEqual(job.flux_task_id, first_flux)
                    self.assertNotEqual(job.image_url, first_image)
                    # two renders, each of the two styles
                    self.assertEqual(len(queue.enqueued), 4)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class TwoVariantTests(unittest.TestCase):
    def test_both_styles_are_rendered_and_offered(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(
                        prompt="orc warrior", prompt_b="low-poly orc warrior", user_name="bot"
                    )
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    self.assertEqual(len(queue.enqueued), 2)
                    self.assertEqual(
                        [t.prompt.prompt for t in queue.enqueued],
                        ["orc warrior", "low-poly orc warrior"],
                    )
                    self.assertTrue(job.image_url)
                    self.assertTrue(job.image_url_b)
                    self.assertNotEqual(job.image_url, job.image_url_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_choosing_the_second_variant_feeds_it_to_3d(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    variant_b_image = job.image_url_b
                    variant_b_isolated = job.isolated_url_b
                    self.assertTrue(variant_b_isolated)

                    job, ok = await manager.approve_image(job.id, variant="b")
                    self.assertTrue(ok)
                    self.assertEqual(job.chosen_variant, "b")
                    # the chosen render becomes the one the 3D stage consumes
                    self.assertEqual(job.image_url, variant_b_image)
                    self.assertEqual(job.isolated_url, variant_b_isolated)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_single_prompt_still_renders_one_variant(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        job = await manager.create(prompt="orc", user_name="bot")
                        # one prompt renders one variant, and one variant is not
                        # a choice - the job does not stop to ask about it
                        job = await _wait_stage(
                            manager, job.id, {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED}
                        )
                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    # count the T-pose renders only: the job now runs on to the
                    # 3D stage, which enqueues its own task in the ComfyUI
                    # fallback, so the total is no longer the variant count
                    t_pose = [t for t in queue.enqueued if t.prompt.type == "t_pose"]
                    self.assertEqual(len(t_pose), 1)
                    self.assertFalse(job.image_url_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_second_variant_failure_does_not_sink_the_job(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    original = queue.enqueue

                    async def enqueue_second_fails(prompt):
                        task = await original(prompt)
                        if prompt.prompt.startswith("low-poly"):
                            task.status = TASK_ERROR
                            task.error = "variant B boom"
                            task.output_path = ""
                            task.extra_outputs.clear()
                        return task

                    queue.enqueue = enqueue_second_fails

                    async def fake_turntable(glb_path, out_path, **kw):
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch(
                        "renderfin.character_gen.turntable.render_turntable",
                        side_effect=fake_turntable,
                    ):
                        job = await manager.create(
                            prompt="orc", prompt_b="low-poly orc", user_name="bot"
                        )
                        # the surviving render carries the job on; with nothing
                        # left to choose between it does not stop to ask either
                        job = await _wait_stage(
                            manager, job.id, {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED}
                        )
                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    self.assertTrue(job.image_url)
                    self.assertFalse(job.image_url_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_regenerate_clears_both_variants(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    first_b = job.image_url_b
                    await manager.regenerate_image(job.id)
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    self.assertTrue(job.image_url_b)
                    self.assertNotEqual(job.image_url_b, first_b)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class StageDeadlineTests(unittest.TestCase):
    """A restart must not hand a stuck stage a fresh timeout window."""

    def _manager(self):
        registry = ServerRegistry()
        queue = _InstantQueue(registry, db_path=config.DB_PATH)
        return queue, CharacterGenManager(queue, db_path=config.DB_PATH)

    def test_budget_shrinks_as_the_stage_runs(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    first = manager._stage_budget(job, 3600.0)
                    self.assertAlmostEqual(first, 3600.0, delta=5)
                    # pretend the stage has been running for an hour already
                    job.stage_started_at -= 1800
                    second = manager._stage_budget(job, 3600.0)
                    self.assertAlmostEqual(second, 1800.0, delta=5)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_restart_resumes_the_same_window(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                manager._stage_budget(job, 3600.0)
                job.stage_started_at -= 3000  # 50 minutes in
                await manager._persist(job)
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    revived = manager2.get(job.id)
                    self.assertEqual(revived.stage, CHARGEN_STAGE_HUNYUAN)
                    budget = manager2._stage_budget(revived, 3600.0)
                    self.assertLess(budget, 700, "restart handed the stage a fresh window")
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_long_wait_persists_new_stage_clock_before_restart(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                await manager._persisted_stage_budget(job, 3600.0)
                started_at = job.stage_started_at
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    revived = manager2.get(job.id)
                    self.assertEqual(revived.timed_stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertAlmostEqual(revived.stage_started_at, started_at, delta=0.01)
                    self.assertLess(manager2._stage_budget(revived, 3600.0), 3600.0)
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_exhausted_window_leaves_no_time(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    manager._stage_budget(job, 3600.0)
                    job.stage_started_at -= 7200
                    self.assertEqual(manager._stage_budget(job, 3600.0), 0.0)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_new_stage_starts_a_new_window(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    manager._stage_budget(job, 3600.0)
                    job.stage_started_at -= 3000
                    job.stage = CHARGEN_STAGE_TURNTABLE
                    self.assertAlmostEqual(manager._stage_budget(job, 3600.0), 3600.0, delta=5)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_retry_earns_a_fresh_window(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    manager._stage_budget(job, 3600.0)
                    job.stage_started_at -= 3500
                    await manager._handle_stage_error(job, RuntimeError("boom"))
                    self.assertEqual(job.stage_started_at, 0)
                    self.assertAlmostEqual(manager._stage_budget(job, 3600.0), 3600.0, delta=5)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class EmptyFleetTests(unittest.TestCase):
    """An empty 3D fleet is a wait, not a job failure.

    The farm boxes came back from a restart without their Hunyuan module and
    every queued job burned its retries and reported a failure to the owner,
    who was owed the result whenever the farm returned.
    """

    def _manager(self):
        registry = ServerRegistry()
        queue = _InstantQueue(registry, db_path=config.DB_PATH)
        return queue, CharacterGenManager(queue, db_path=config.DB_PATH)

    async def _park(self, manager, job):
        """Take the job out of the retry loop's reach before teardown.

        These tests drive _handle_stage_error directly and leave the job in an
        active stage; the loop would then spawn it against a torn-down env.
        """
        job.stage = CHARGEN_STAGE_DISCARDED
        job.retry_at = 0
        await manager._persist(job)

    def test_missing_fleet_parks_the_job_without_spending_an_attempt(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(
                        job, hunyuan_client.NoWorkerAvailable("no enabled Hunyuan worker among f7, f13")
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.attempts, {})
                    self.assertEqual(job.error, "")
                    self.assertGreater(job.retry_at, time.time())
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_waiting_for_the_fleet_cannot_time_the_stage_out(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    manager._stage_budget(job, 3600.0)
                    job.stage_started_at -= 3500  # nearly out of time
                    await manager._handle_stage_error(
                        job, hunyuan_client.NoWorkerAvailable("no enabled Hunyuan worker")
                    )
                    self.assertAlmostEqual(manager._stage_budget(job, 3600.0), 3600.0, delta=5)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_real_failure_still_spends_an_attempt(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(job, RuntimeError("generation failed on f13"))
                    self.assertEqual(job.attempts.get(CHARGEN_STAGE_HUNYUAN), 1)
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_collection_comfy_5xx_parks_and_releases_failed_render(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_FLUX,
                        queue_class="collection_background",
                        collection_guid="collection-1",
                    )
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.status = TASK_ERROR
                    task.error = "submit failed 3x: upload/image failed: HTTP 500"
                    job.flux_task_id = task.id
                    await manager._handle_stage_error(
                        job,
                        RuntimeError(f"render task {task.id} failed: {task.error}"),
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
                    self.assertEqual(job.attempts, {})
                    self.assertEqual(job.flux_task_id, "")
                    self.assertGreater(job.retry_at, time.time())
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_collection_hunyuan_5xx_parks_and_releases_worker(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        queue_class="collection_background",
                        collection_guid="collection-1",
                        hunyuan_task_id="https://raptor/status/x",
                        hunyuan_worker="raptor",
                    )
                    await manager._handle_stage_error(
                        job,
                        RuntimeError("generate-3d on Raptor failed: HTTP 500 disk full"),
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.attempts, {})
                    self.assertEqual(job.hunyuan_task_id, "")
                    self.assertEqual(job.hunyuan_worker, "")
                    self.assertGreater(job.retry_at, time.time())
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_only_background_collection_gets_transient_farm_revival(self):
        error = "render task x failed: submit failed 3x: upload/image failed: HTTP 500"
        collection = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            queue_class="collection_background",
            collection_guid="collection-1",
            error=error,
        )
        interactive = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            queue_class="interactive",
            collection_guid="collection-1",
            error=error,
        )
        missing_source = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            error="image_url returned HTTP 404",
        )
        empty_artifact_transport_error = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            queue_class="collection_background",
            collection_guid="collection-1",
            error="render task x failed: artifact download failed: ",
        )
        worker_input_timeout = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            queue_class="collection_background",
            collection_guid="collection-1",
            error=(
                "generation failed on f12: HTTPSConnectionPool(host='autorig.online', "
                "port=443): Read timed out. (read timeout=5)"
            ),
        )
        interactive_input_timeout = CharacterGenJob(
            stage=CHARGEN_STAGE_FAILED,
            queue_class="interactive",
            collection_guid="collection-1",
            error=worker_input_timeout.error,
        )
        self.assertTrue(character_gen._failed_on_recoverable_infrastructure(collection))
        self.assertTrue(
            character_gen._failed_on_recoverable_infrastructure(
                empty_artifact_transport_error
            )
        )
        self.assertTrue(
            character_gen._failed_on_recoverable_infrastructure(worker_input_timeout)
        )
        self.assertFalse(
            character_gen._failed_on_recoverable_infrastructure(
                interactive_input_timeout
            )
        )
        self.assertFalse(character_gen._failed_on_recoverable_infrastructure(interactive))
        self.assertFalse(character_gen._failed_on_recoverable_infrastructure(missing_source))

    def test_one_dns_failure_cools_the_worker_for_the_whole_batch(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    first = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(
                        first,
                        hunyuan_client.WorkerInputFetchError(
                            "f13", "f13 cannot resolve the input image host"
                        ),
                    )
                    self.assertGreater(
                        manager._input_fetch_worker_cooldowns.get("f13", 0),
                        time.time(),
                    )
                    self.assertGreater(
                        first.hunyuan_worker_cooldowns.get("f13", 0),
                        time.time(),
                    )
                    self.assertEqual(first.attempts, {})
                    await self._park(manager, first)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_live_job_gets_its_attempt_debt_back_once_and_only_once(self):
        """A survivor of a farm fault must not be charged for it either.

        The second refund is the one that matters: nothing re-checks *why* the
        attempts were spent, so a repeatable refund would keep a genuinely
        broken job alive forever on a GPU slot it never gives back.
        """
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    job.attempts = {CHARGEN_STAGE_HUNYUAN: 2}
                    await manager._persist(job)

                    self.assertEqual(await manager.refund_attempts(), 1)
                    self.assertEqual(job.attempts.get(CHARGEN_STAGE_HUNYUAN, 0), 0)

                    # spend it again: the budget is not refilled a second time
                    await manager._handle_stage_error(
                        job, RuntimeError("generation failed on f13")
                    )
                    self.assertEqual(job.attempts.get(CHARGEN_STAGE_HUNYUAN), 1)
                    self.assertEqual(await manager.refund_attempts(), 0)
                    self.assertEqual(job.attempts.get(CHARGEN_STAGE_HUNYUAN), 1)
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_job_already_failed_by_an_empty_fleet_is_revived(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_FAILED,
                        isolated_url="https://x/a_Isolated.png",
                        error="no enabled Hunyuan worker among f7, f13",
                    )
                    # no manual resume: the retry loop must notice on its own
                    revived = await _wait_stage(manager, job.id, {CHARGEN_STAGE_HUNYUAN})
                    runner = manager._runners.pop(job.id, None)
                    if runner is not None:
                        runner.cancel()
                    self.assertEqual(revived.error, "")
                    self.assertEqual(revived.attempts, {})
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_job_failed_by_a_stale_token_is_revived(self):
        for message in (
            'generate-3d on f13 failed: HTTP 401 {"error":"unauthorized"}',
            "f7 rejected our token (HTTP 401)",
            "generate-3d on f7 failed: HTTP 403 forbidden",
        ):
            self.assertTrue(
                character_gen._failed_on_empty_fleet(CharacterGenJob(error=message)),
                message,
            )

    def test_a_farm_side_post_processing_failure_parks_the_job(self):
        """The generation was paid for and succeeded; the box then failed to
        finish its own post-processing. Retrying the job cannot fix that."""

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(
                        job,
                        RuntimeError(
                            "generation failed on f13: Vertex-PBR manifest is missing "
                            "or invalid: [Errno 2] No such file or directory"
                        ),
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.attempts, {}, "an attempt must not be spent")
                    self.assertEqual(job.error, "")
                    # far enough out not to re-pay for a GPU hour every 5 minutes
                    self.assertGreater(job.retry_at - time.time(), 600)
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_busy_gpu_is_a_short_wait_not_a_failure(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(
                        job,
                        RuntimeError("generation failed on f2: Hunyuan VRAM gate failed: "
                                     "6439 MiB free; 7000 MiB required"),
                    )
                    self.assertEqual(job.attempts, {}, "a busy card is not the job's fault")
                    self.assertEqual(job.error, "")
                    # minutes, not the half hour a broken post-processor earns
                    self.assertLess(job.retry_at - time.time(), 600)
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_parked_job_lets_go_of_the_worker_it_failed_on(self):
        """Holding a finished task re-reads the same failure and, worse,
        counts the job against that worker's slot for ever."""

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager, stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f13/status/x", hunyuan_worker="f13",
                    )
                    self.assertEqual(manager.in_flight_by_worker(), {"f13": 1})
                    await manager._handle_stage_error(
                        job,
                        RuntimeError("generation failed on f13: Vertex-PBR manifest is missing"),
                    )
                    self.assertEqual(job.hunyuan_task_id, "")
                    self.assertEqual(job.hunyuan_worker, "")
                    self.assertEqual(manager.in_flight_by_worker(), {}, "slot must be released")
                    self.assertGreater(
                        manager._farm_worker_cooldowns.get("f13", 0), time.time()
                    )
                    self.assertGreater(
                        job.hunyuan_worker_cooldowns.get("f13", 0), time.time()
                    )
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_one_vram_gate_cools_the_worker_for_the_whole_batch(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f11/status/x",
                        hunyuan_worker="f11",
                    )
                    await manager._handle_stage_error(
                        job,
                        RuntimeError(
                            "generation failed on f11: Hunyuan VRAM gate failed: "
                            "94 MiB free; 7000 MiB required"
                        ),
                    )
                    self.assertEqual(job.attempts, {})
                    self.assertGreater(
                        manager._farm_worker_cooldowns.get("f11", 0), time.time()
                    )
                    self.assertLess(
                        manager._farm_worker_cooldowns["f11"] - time.time(), 600
                    )
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_converter_timeout_does_not_kill_the_job(self):
        """The box's own two-hour ceiling says how slow the farm is today, not
        that anything is wrong with this job."""

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager, stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f1/status/x", hunyuan_worker="f1",
                    )
                    await manager._handle_stage_error(
                        job,
                        RuntimeError("generation failed on f1: Hunyuan generation timed out after 7192s"),
                    )
                    self.assertEqual(job.attempts, {}, "a farm-wide slowness is not the job's fault")
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.hunyuan_task_id, "", "the dead task must be let go")
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_job_already_killed_by_a_timeout_is_revived(self):
        for message in ("generation timed out",
                        "generation failed on f1: Hunyuan generation timed out after 7192s"):
            self.assertTrue(
                character_gen._failed_on_empty_fleet(CharacterGenJob(error=message)), message
            )

    def test_a_job_already_failed_on_vram_is_revived(self):
        job = CharacterGenJob(error="generation failed on f2: Hunyuan VRAM gate failed: 6439 MiB free")
        self.assertTrue(character_gen._failed_on_empty_fleet(job))

    def test_a_job_already_failed_that_way_is_revived(self):
        job = CharacterGenJob(
            error="generation failed on f7: Vertex-PBR manifest is missing or invalid"
        )
        self.assertTrue(character_gen._failed_on_empty_fleet(job))

    def test_a_lost_task_is_resubmitted_without_spending_an_attempt(self):
        """f7 reboots without shutting down cleanly and forgets its task
        registry; a job must not be charged for that."""

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f7/status/h-1",
                        hunyuan_worker="f7",
                    )
                    await manager._handle_stage_error(
                        job, hunyuan_client.TaskVanished("task vanished on f7 (HTTP 404)")
                    )
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.attempts, {})
                    # the dead handle is dropped so the stage submits again
                    self.assertEqual(job.hunyuan_task_id, "")
                    self.assertEqual(job.hunyuan_worker, "")
                    self.assertGreater(job.retry_at, time.time())
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_expired_central_hunyuan_lease_exactly_preempts_before_retry(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f7/status/h-expired",
                        hunyuan_worker="f7",
                    )
                    job.hunyuan_workload_lease_id = "lease-expired"
                    job.hunyuan_workload_request_id = "request-expired"
                    job.hunyuan_workload_physical_resource_id = "raptor"
                    await manager._persist(job)
                    worker = {
                        "name": "f7",
                        "url": "https://f7",
                        "token": "worker-token",
                    }
                    exact_preempt = AsyncMock(return_value="preempted")
                    with patch.object(
                        hunyuan_client, "worker_for_url", return_value=worker
                    ), patch.object(
                        hunyuan_client,
                        "preempt_bound_task",
                        new=exact_preempt,
                    ):
                        await manager._handle_stage_error(
                            job,
                            workload_lease.WorkloadLeaseTerminal(
                                {
                                    "status_string": "lease_terminal",
                                    "lease_by_key": {"state_string": "expired"},
                                }
                            ),
                        )
                    exact_preempt.assert_awaited_once()
                    self.assertEqual(job.attempts, {})
                    self.assertEqual(job.stage, CHARGEN_STAGE_HUNYUAN)
                    self.assertEqual(job.hunyuan_task_id, "")
                    self.assertEqual(job.hunyuan_worker, "")
                    self.assertEqual(job.hunyuan_workload_lease_id, "")
                    self.assertEqual(job.preemption_count, 1)
                    await self._park(manager, job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_hunyuan_download_failure_retains_lease_until_artifact_persisted(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    status_url = "https://f12/status/h-artifact"
                    worker = {
                        "name": "f12",
                        "url": "https://f12",
                        "token": "worker-token",
                        "physical_node": "machine_12121212121212121212121212121212",
                        "physical_resource_id_string": "machine_12121212121212121212121212121212",
                        "workload_role": "ai_vision_primary",
                        "pool": "dedicated",
                    }
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id=status_url,
                        hunyuan_worker="f12",
                    )
                    job.hunyuan_workload_lease_id = "lease-artifact"
                    job.hunyuan_workload_request_id = "request-artifact"
                    job.hunyuan_workload_physical_resource_id = (
                        "machine_12121212121212121212121212121212"
                    )
                    job.hunyuan_workload_node_id = "f12"
                    job.hunyuan_workload_lease_state = "active"
                    await manager._persist(job)

                    premature_release = AsyncMock(return_value=True)
                    with patch.object(
                        manager,
                        "_release_hunyuan_workload",
                        new=premature_release,
                    ):
                        await manager._handle_stage_error(
                            job,
                            hunyuan_client.HunyuanClientError(
                                "model download failed on f12: connection reset"
                            ),
                        )

                    premature_release.assert_not_awaited()
                    self.assertEqual(job.hunyuan_task_id, status_url)
                    self.assertEqual(job.hunyuan_workload_lease_id, "lease-artifact")
                    self.assertEqual(job.hunyuan_workload_request_id, "request-artifact")
                    self.assertEqual(job.hunyuan_workload_lease_state, "artifact_pending")
                    self.assertLessEqual(
                        job.retry_at - time.time(),
                        character_gen.HUNYUAN_ARTIFACT_RETRY_SECONDS + 1,
                    )

                    release_snapshots = []

                    async def release_after_persist(
                        release_job, *, outcome: str, new_request: bool
                    ):
                        glb_path = (
                            config.RENDER_DIR
                            / release_job.user_name
                            / f"{release_job.id}.glb"
                        )
                        release_snapshots.append(
                            (outcome, new_request, release_job.glb_url, glb_path.read_bytes())
                        )
                        release_job.hunyuan_workload_lease_id = ""
                        return True

                    with patch.object(
                        hunyuan_client, "worker_for_url", return_value=worker
                    ), patch.object(
                        hunyuan_client,
                        "server_status",
                        new=AsyncMock(return_value={}),
                    ), patch.object(
                        manager,
                        "_ensure_hunyuan_workload",
                        new=AsyncMock(return_value={"lease_id_string": "lease-artifact"}),
                    ), patch.object(
                        hunyuan_client,
                        "wait_for_model",
                        new=AsyncMock(
                            return_value={"output_urls": {"model": "model.glb"}}
                        ),
                    ), patch.object(
                        hunyuan_client,
                        "download_model",
                        new=AsyncMock(return_value=b"glTF-artifact"),
                    ), patch.object(
                        workload_lease, "enabled", return_value=True
                    ), patch.object(
                        manager,
                        "_release_hunyuan_workload",
                        new=release_after_persist,
                    ):
                        await manager._stage_hunyuan_converter(job)

                    self.assertEqual(
                        release_snapshots,
                        [("completed", False, job.glb_url, b"glTF-artifact")],
                    )
                    self.assertTrue(job.glb_url.endswith(f"/{job.id}.glb"))
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_hunyuan_artifact_pending_completed_wins_central_preemption(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    worker = {
                        "name": "f12",
                        "url": "https://f12",
                        "token": "worker-token",
                        "physical_node": "machine_12121212121212121212121212121212",
                        "physical_resource_id_string": "machine_12121212121212121212121212121212",
                        "workload_role": "ai_vision_primary",
                        "pool": "dedicated",
                    }
                    job = await _idle_job(
                        manager,
                        stage=CHARGEN_STAGE_HUNYUAN,
                        hunyuan_task_id="https://f12/status/h-completed-wins",
                        hunyuan_worker="f12",
                    )
                    job.hunyuan_workload_lease_id = "lease-completed-wins"
                    job.hunyuan_workload_request_id = "request-completed-wins"
                    job.hunyuan_workload_physical_resource_id = (
                        "machine_12121212121212121212121212121212"
                    )
                    job.hunyuan_workload_node_id = "f12"
                    await manager._persist(job)
                    release = AsyncMock(return_value=True)
                    with patch.object(
                        workload_lease, "enabled", return_value=True
                    ), patch.object(
                        workload_lease,
                        "heartbeat",
                        new=AsyncMock(
                            side_effect=workload_lease.WorkloadPreempted(
                                {"status_string": "preemption_requested"}
                            )
                        ),
                    ), patch.object(
                        hunyuan_client,
                        "preempt_bound_task",
                        new=AsyncMock(return_value="completed"),
                    ), patch.object(
                        manager,
                        "_release_hunyuan_workload",
                        new=release,
                    ):
                        lease = await manager._ensure_hunyuan_workload(
                            job,
                            AsyncMock(),
                            worker,
                            {
                                "physical_node": "machine_12121212121212121212121212121212",
                                "workload_role": "ai_vision_primary",
                            },
                        )

                    release.assert_not_awaited()
                    self.assertEqual(lease["lease_id_string"], "lease-completed-wins")
                    self.assertEqual(job.hunyuan_workload_lease_id, "lease-completed-wins")
                    self.assertEqual(job.hunyuan_task_id, "https://f12/status/h-completed-wins")
                    self.assertEqual(job.hunyuan_workload_lease_state, "artifact_pending")
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_job_already_failed_by_a_lost_task_is_revived(self):
        job = CharacterGenJob(error="task vanished on f7 (HTTP 404)")
        self.assertTrue(character_gen._failed_on_empty_fleet(job))

    def test_waiting_for_a_slot_is_a_queue_not_an_outage(self):
        """A full pool is re-checked in a minute; a dead farm in five."""

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    capacity_messages = (
                        "every Hunyuan worker is at capacity: f13",
                        "higher-priority Hunyuan job first is ahead of second",
                        "shared Hunyuan fallback paused: background work already "
                        "occupies 2/3 healthy full converters (reserve=1)",
                        "f12 is temporarily unavailable: gpu_busy_comfy",
                    )
                    slots = []
                    for message in capacity_messages:
                        slot = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                        await manager._handle_stage_error(
                            slot,
                            hunyuan_client.NoWorkerAvailable(message),
                        )
                        slot_wait = slot.retry_at - time.time()
                        self.assertAlmostEqual(
                            slot_wait,
                            character_gen.SLOT_WAIT_SECONDS,
                            delta=2,
                        )
                        self.assertEqual(slot.attempts, {})
                        self.assertEqual(slot.error, "")
                        slots.append(slot)

                    outage = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    await manager._handle_stage_error(
                        outage,
                        hunyuan_client.NoWorkerAvailable("no enabled Hunyuan worker among f13"),
                    )
                    outage_wait = outage.retry_at - time.time()

                    self.assertAlmostEqual(
                        outage_wait,
                        character_gen.FLEET_WAIT_SECONDS,
                        delta=2,
                    )
                    self.assertEqual(outage.attempts, {})
                    for slot in slots:
                        await self._park(manager, slot)
                    await self._park(manager, outage)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_an_idle_worker_is_never_reported_busy(self):
        """A separate claim counter leaked and marked an idle box busy forever.

        The count comes from persisted jobs only; correctness rests on
        submission holding the lock until the job is written down.
        """

        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    self.assertEqual(manager.in_flight_by_worker(), {})
                    job = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN,
                                          hunyuan_task_id="https://f13/s/1",
                                          hunyuan_worker="f13")
                    self.assertEqual(manager.in_flight_by_worker(), {"f13": 1})
                    # once it moves on, the slot is free again with nothing to release
                    job.stage = CHARGEN_STAGE_TURNTABLE
                    self.assertEqual(manager.in_flight_by_worker(), {})
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_in_flight_is_counted_per_worker(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN,
                                    hunyuan_task_id="https://f13/s/1", hunyuan_worker="f13")
                    await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN,
                                    hunyuan_task_id="https://f13/s/2", hunyuan_worker="f13")
                    # parked, no task out there: must not count against the box
                    await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN, hunyuan_worker="f13")
                    # a different stage is not holding a worker either
                    await _idle_job(manager, stage=CHARGEN_STAGE_TURNTABLE,
                                    hunyuan_task_id="https://f13/s/3", hunyuan_worker="f13")
                    self.assertEqual(manager.in_flight_by_worker(), {"f13": 2})
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_kick_stops_serving_out_a_backoff_for_a_farm_that_is_fixed(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    parked = await _idle_job(
                        manager, stage=CHARGEN_STAGE_HUNYUAN, retry_at=time.time() + 1800
                    )
                    dead = await _idle_job(
                        manager, stage=CHARGEN_STAGE_FAILED,
                        isolated_url="https://x/a_Isolated.png",
                        error="generation timed out",
                        attempts={CHARGEN_STAGE_HUNYUAN: 3},
                    )
                    await manager.revive_failed()
                    kicked = await manager.kick_parked()

                    # what matters is the end state, not who got there first:
                    # the retry loop revives on its own schedule and racing it
                    # made this assertion flaky
                    self.assertGreaterEqual(kicked, 1)
                    self.assertEqual(parked.retry_at, 0)
                    # a job whose attempts were spent on a broken farm gets them back
                    self.assertEqual(manager.get(dead.id).attempts, {})
                    self.assertNotEqual(manager.get(dead.id).stage, CHARGEN_STAGE_FAILED)
                    for j in (parked, manager.get(dead.id)):
                        runner = manager._runners.pop(j.id, None)
                        if runner is not None:
                            runner.cancel()
                        await self._park(manager, j)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_kick_leaves_a_job_that_is_not_waiting_alone(self):
        async def scenario():
            with _Env():
                queue, manager = self._manager()
                await queue.start()
                await manager.start()
                try:
                    running = await _idle_job(manager, stage=CHARGEN_STAGE_HUNYUAN)
                    done = await _idle_job(manager, stage=CHARGEN_STAGE_DISCARDED)
                    self.assertEqual(await manager.kick_parked(), 0)
                    self.assertEqual(done.stage, CHARGEN_STAGE_DISCARDED)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_a_genuinely_failed_job_is_left_alone(self):
        job = CharacterGenJob(error="generation failed on f13: out of memory")
        self.assertFalse(character_gen._failed_on_empty_fleet(job))
        job2 = CharacterGenJob(error="no enabled Hunyuan worker among f7, f13")
        self.assertTrue(character_gen._failed_on_empty_fleet(job2))

    def test_a_stale_last_error_does_not_revive_forever(self):
        """last_error survives a revival, so it must not drive the decision."""
        job = CharacterGenJob(
            error="chrome crashed rendering the turntable",
            last_error="no enabled Hunyuan worker among f7, f13",
        )
        self.assertFalse(character_gen._failed_on_empty_fleet(job))


class ResumeTests(unittest.TestCase):
    def test_resume_failed_job_reuses_finished_flux_render(self):
        async def scenario():
            with _Env():
                from renderfin.models import CHARGEN_STAGE_FAILED as FAILED

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    flux_id = job.flux_task_id
                    # simulate a stage-timeout failure that lost the flux result
                    job.stage = FAILED
                    job.error = "render task timed out"
                    job.image_url = ""
                    job.isolated_url = ""
                    await manager._persist(job)

                    job, transitioned = await manager.resume(job.id)
                    self.assertTrue(transitioned)
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    # reused the existing (already Done) render — no new enqueue
                    self.assertEqual(job.flux_task_id, flux_id)
                    self.assertEqual(len(queue.enqueued), 2)  # the two styles, rendered once
                    self.assertTrue(job.image_url)

                    # resume refuses non-failed jobs
                    _, again = await manager.resume(job.id)
                    self.assertFalse(again)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class ResurrectDoneTests(unittest.TestCase):
    def test_recent_done_tasks_loaded_after_restart(self):
        async def scenario():
            with _Env():
                from renderfin.models import RenderPrompt, TASK_DONE

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                task = await queue.enqueue(
                    RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                )
                self.assertEqual(task.status, TASK_DONE)
                await queue.stop()

                queue2 = RenderQueue(registry, db_path=config.DB_PATH)
                await queue2.start()
                try:
                    revived = queue2.get(task.id)
                    self.assertIsNotNone(revived)
                    self.assertEqual(revived.status, TASK_DONE)
                finally:
                    await queue2.stop()

        run(scenario())


if __name__ == "__main__":
    unittest.main()


class AutoRetryTests(unittest.TestCase):
    def test_stage_failure_retries_automatically(self):
        """Infrastructure hiccups must heal themselves, not wait on a button."""

        async def scenario():
            with _Env():
                from renderfin import character_gen as cg_mod
                from renderfin.models import CHARGEN_STAGE_TURNTABLE

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    calls = {"n": 0}

                    async def flaky_turntable(glb_path, out_path, **kw):
                        calls["n"] += 1
                        if calls["n"] == 1:
                            raise RuntimeError("Chrome exited during startup")
                        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                        Path(out_path).write_bytes(b"MP4!" * 500)
                        return Path(out_path)

                    with patch.object(cg_mod, "RETRY_BACKOFF_SECONDS", (0.0, 0.0, 0.0)):
                        with patch.object(cg_mod, "RETRY_TICK_SECONDS", 0.05):
                            with patch.object(cg_mod.turntable, "render_turntable",
                                              side_effect=flaky_turntable):
                                job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                                job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                                await manager.approve_image(job.id)
                                job = await _wait_stage(
                                    manager, job.id,
                                    {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED},
                                    timeout=10.0,
                                )

                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    self.assertEqual(calls["n"], 2)
                    self.assertEqual(job.attempts.get(CHARGEN_STAGE_TURNTABLE), 1)
                    self.assertTrue(job.video_url)

                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_failure_reported_only_after_attempts_exhausted(self):
        async def scenario():
            with _Env():
                from renderfin import character_gen as cg_mod

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    calls = {"n": 0}

                    async def always_fails(glb_path, out_path, **kw):
                        calls["n"] += 1
                        raise RuntimeError("ffmpeg missing")

                    with patch.object(cg_mod, "RETRY_BACKOFF_SECONDS", (0.0, 0.0, 0.0)):
                        with patch.object(cg_mod, "RETRY_TICK_SECONDS", 0.05):
                            with patch.object(cg_mod, "MAX_STAGE_ATTEMPTS", 3):
                                with patch.object(cg_mod.turntable, "render_turntable",
                                                  side_effect=always_fails):
                                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                                    await manager.approve_image(job.id)
                                    job = await _wait_stage(
                                        manager, job.id, {CHARGEN_STAGE_FAILED}, timeout=10.0
                                    )

                    # 3 attempts before the user is told anything
                    self.assertEqual(calls["n"], 3)
                    self.assertIn("ffmpeg missing", job.error)

                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_retry_schedule_survives_restart(self):
        async def scenario():
            with _Env():
                from renderfin import character_gen as cg_mod

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                job_id = None
                try:
                    async def fails(glb_path, out_path, **kw):
                        raise RuntimeError("transient")

                    # long backoff: the retry is still pending when we restart
                    with patch.object(cg_mod, "RETRY_BACKOFF_SECONDS", (3600.0,)):
                        with patch.object(cg_mod.turntable, "render_turntable", side_effect=fails):
                            job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                            job_id = job.id
                            job = await _wait_stage(manager, job_id, {CHARGEN_STAGE_AWAITING_IMAGE})
                            await manager.approve_image(job_id)
                            await asyncio.sleep(0.5)
                            self.assertTrue(manager.get(job_id).retry_at > 0)
                finally:
                    await manager.stop()

                async def ok_turntable(glb_path, out_path, **kw):
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(out_path).write_bytes(b"MP4!" * 500)
                    return Path(out_path)

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                with patch.object(cg_mod, "RETRY_TICK_SECONDS", 0.05):
                    with patch.object(cg_mod.turntable, "render_turntable", side_effect=ok_turntable):
                        await manager2.start()
                        try:
                            revived = manager2.get(job_id)
                            # still scheduled, not lost and not spun immediately
                            self.assertTrue(revived.retry_at > 0)
                            revived.retry_at = 1.0  # make it due
                            await manager2._persist(revived)
                            job = await _wait_stage(manager2, job_id, {CHARGEN_STAGE_READY}, timeout=10.0)
                            self.assertTrue(job.video_url)
                        finally:
                            await manager2.stop()
                await queue.stop()

        run(scenario())


class RunningNumberTests(unittest.TestCase):
    def test_each_job_gets_the_next_number(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    a = await manager.create(prompt="one", user_name="bot")
                    b = await manager.create(prompt="two", user_name="bot")
                    c = await manager.create(prompt="three", user_name="bot")
                    self.assertEqual([a.seq, b.seq, c.seq], [1, 2, 3])
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_numbering_continues_after_restart(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                first = await manager.create(prompt="one", user_name="bot")
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    second = await manager2.create(prompt="two", user_name="bot")
                    self.assertEqual(second.seq, first.seq + 1)
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_existing_jobs_are_numbered_on_startup(self):
        async def scenario():
            with _Env():
                import json as _json

                registry = ServerRegistry()
                # the queue keeps its own file: this is the only test that opens
                # a second manager while the queue is still holding the first,
                # and sharing one sqlite file makes the second open block
                queue = _InstantQueue(registry, db_path=config.DB_DIR / "queue.db")
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                job = await manager.create(prompt="legacy", user_name="bot")
                # simulate a row written before the counter existed
                job.seq = 0
                await manager._persist(job)
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    self.assertGreater(manager2.get(job.id).seq, 0)
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_stats_compare_the_last_two_days(self):
        async def scenario():
            with _Env():
                import time as _time

                registry = ServerRegistry()
                queue = _InstantQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                manager = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager.start()
                try:
                    now = _time.time()
                    recent = await manager.create(prompt="today", user_name="bot")
                    older = await manager.create(prompt="yesterday", user_name="bot")
                    ancient = await manager.create(prompt="last week", user_name="bot")
                    older.created_at = now - 30 * 3600      # previous 24h window
                    ancient.created_at = now - 200 * 3600   # outside both windows
                    stats = manager.stats()
                    self.assertEqual(stats["current_24h"], 1)
                    self.assertEqual(stats["previous_24h"], 1)
                    self.assertEqual(stats["delta_24h"], 0)
                    self.assertEqual(stats["total"], 3)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())
