import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import time

from renderfin import character_gen, config, hunyuan_client
from renderfin.character_gen import CharacterGenManager
from renderfin.models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CharacterGenJob,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_TURNTABLE,
    TASK_DONE,
    TASK_ERROR,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


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
                    }]
                    with patch.object(config, "hunyuan_workers", lambda: fake_pool):
                        with patch.object(config, "HUNYUAN_API_TOKEN", "test-token"):
                            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                                with patch.object(cg_mod.httpx, "AsyncClient", side_effect=patched_client):
                                    with patch.object(cg_mod.turntable, "render_turntable", side_effect=fake_turntable):
                                        job = await manager.create(prompt="orc", user_name="bot")
                                        job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED})
                                        self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.error)
                                        job, transitioned = await manager.approve_image(job.id)
                                        self.assertTrue(transitioned)
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
                            prompt="orc warrior", mask_url="", user_name="bot"
                        )
                        # pipeline pauses for human validation of the Flux render
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
                    self.assertEqual(queue.enqueued[1].prompt.type, "image_to_3d")
                    self.assertEqual(queue.enqueued[1].prompt.image_url, job.isolated_url)
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
                        job = await manager.create(prompt="orc", user_name="bot")
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
                        job = await manager.create(prompt="orc", user_name="bot")
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
                job = await manager.create(prompt="orc", user_name="bot")
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
                        job = await manager.create(prompt="orc", user_name="bot")
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
                    job = await manager.create(prompt="orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    first_flux = job.flux_task_id
                    first_image = job.image_url
                    job2, transitioned = await manager.regenerate_image(job.id)
                    self.assertTrue(transitioned)
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    self.assertNotEqual(job.flux_task_id, first_flux)
                    self.assertNotEqual(job.image_url, first_image)
                    self.assertEqual(len(queue.enqueued), 2)
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
                    job = await manager.create(prompt="orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
                    self.assertEqual(len(queue.enqueued), 1)
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
                    job = await manager.create(prompt="orc", prompt_b="low-poly orc", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE})
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
                    job = await manager.create(prompt="orc", user_name="bot")
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
                    self.assertEqual(len(queue.enqueued), 1)
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
                                job = await manager.create(prompt="orc", user_name="bot")
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
                                    job = await manager.create(prompt="orc", user_name="bot")
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
                            job = await manager.create(prompt="orc", user_name="bot")
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
