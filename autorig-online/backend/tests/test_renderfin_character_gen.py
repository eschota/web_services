import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import config
from renderfin.character_gen import CharacterGenManager
from renderfin.models import (
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_READY,
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
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        config.ensure_dirs()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()


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

                    with patch.object(config, "HUNYUAN_API_TOKEN", "test-token"):
                        with patch.object(config, "HUNYUAN_WORKERS", ["https://converter-f2.freestock.online"]):
                            with patch.object(config, "HUNYUAN_POLL_SECONDS", 0.01):
                                with patch.object(cg_mod.httpx, "AsyncClient", side_effect=patched_client):
                                    with patch.object(cg_mod.turntable, "render_turntable", side_effect=fake_turntable):
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
                            prompt="orc warrior", mask_url="", user_name="bot"
                        )
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
                    job = await manager.create(prompt="x", user_name="bot")
                    job = await _wait_stage(manager, job.id, {CHARGEN_STAGE_FAILED})
                    self.assertIn("boom", job.error)
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


if __name__ == "__main__":
    unittest.main()
