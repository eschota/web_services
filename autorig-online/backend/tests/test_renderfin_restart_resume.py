"""Restart mid-render -> the job resumes (owner rule 2026-10-11).

Production, 2026-10-10 19:11 UTC: autorig-storage was restarted on the owner's
order while two Qwen edits were rendering (c6dbd111 on f15, 149ada75 on
Raptor). The startup FARM RESET cancelled both with "cancelled: server
restarted — press Render again". The owner: «перезапускай сервер не дожидаясь
завершения графов, они должны подхватываться автоматически при рестарте».

These tests pin the fix: a restart keeps the queue, follows a render that is
still on its box, re-queues one it cannot follow under the SAME task id (same
output URL, nothing rendered twice), brings back a job an older wipe cancelled,
caps the resumes per task, frees only the task's own prompt on its box, and the
backend no longer wipes at startup.
"""
import asyncio
import importlib
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_BACKEND = Path(__file__).resolve().parents[1]
_PACKAGE = _BACKEND / "renderfin"
for _name in ("image_quality", "workload_lease"):
    _dotted = f"renderfin.{_name}"
    if (_PACKAGE / f"{_name}.py").is_file() and not getattr(
        sys.modules.get(_dotted), "__file__", None
    ):
        sys.modules.pop(_dotted, None)
        sys.modules.pop("renderfin.queue", None)
        importlib.import_module(_dotted)

from renderfin import comfy_adapter, config
from renderfin.models import TASK_ERROR, TASK_PENDING, TASK_RENDERING, RenderPrompt, RenderServer
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry

WF = "gen_image.json"


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
            patch.object(config, "DISPATCH_INTERVAL_SECONDS", 0.0),
            patch.object(config, "WIPE_QUEUE_ON_START", False),
            patch.object(config, "RESTART_RESUME_MAX", 3),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        config.ensure_dirs()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()


def _server(name):
    # Port 9 (discard): never a real box.
    return RenderServer(render_server_name=name, render_server_url="http://127.0.0.1:9",
                        status="online", available_workflows=[WF], average_render_time=30.0)


def _prompt(**kw):
    return RenderPrompt(prompt="a robot", work_flow=WF, **kw)


async def _start(registry):
    queue = RenderQueue(registry, db_path=config.DB_PATH)
    await queue.start()
    queue._pump_task.cancel()
    return queue


async def _restart(queue, registry):
    """Stop the service (as systemd does) and start a fresh process on the same DB."""
    await queue.stop()
    return await _start(registry)


class RestartResumeTests(unittest.TestCase):
    def test_default_policy_is_resume(self):
        if os.getenv("RENDERFIN_RESTART_POLICY"):
            self.skipTest("policy overridden in this environment")
        self.assertEqual(config.RESTART_POLICY, "resume")
        self.assertFalse(config.WIPE_QUEUE_ON_START)
        self.assertGreaterEqual(config.RESTART_RESUME_MAX, 1)

    def test_running_render_on_a_known_box_is_followed_not_cancelled(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                with patch.object(comfy_adapter, "interrupt", AsyncMock()) as interrupt:
                    queue = await _start(registry)
                    task = await queue.enqueue(_prompt())
                    task.status, task.server_name, task.comfy_prompt_id = TASK_RENDERING, "f15", "p1"
                    task.started_at = time.time() - 20
                    await queue._persist(task)
                    queue = await _restart(queue, registry)
                    try:
                        again = queue.get(task.id)
                        self.assertEqual(again.status, TASK_RENDERING)
                        self.assertEqual(again.comfy_prompt_id, "p1")
                        self.assertEqual(again.error, "")
                        self.assertEqual(again.notice, config.RESUMED_NOTICE)
                        self.assertIn("followed@f15", again.recovery_log)
                        interrupt.assert_not_called()
                    finally:
                        await queue.stop()

        run(scenario())

    def test_restart_mid_render_requeues_under_the_same_id(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = await _start(registry)
                task = await queue.enqueue(_prompt())
                url = task.output_url
                # Its box is no longer registered: it cannot be followed.
                task.status, task.server_name, task.comfy_prompt_id = TASK_RENDERING, "gone", ""
                await queue._persist(task)
                queue = await _restart(queue, registry)
                try:
                    again = queue.get(task.id)
                    self.assertEqual(again.status, TASK_PENDING)
                    self.assertEqual(again.output_url, url)
                    self.assertEqual(again.restart_resumes, 1)
                    self.assertEqual(again.notice, config.RESUMED_NOTICE)
                    self.assertEqual(again.public_dict()["notice_string"], "resumed_after_restart")
                    self.assertNotIn("press Render again", again.error)
                    self.assertEqual(len(queue.all_tasks()), 1)  # no second job
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_job_an_old_wipe_cancelled_comes_back_at_the_next_start(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = await _start(registry)
                task = await queue.enqueue(_prompt())
                task.status, task.error = TASK_ERROR, config.RESTART_CANCEL_REASON
                task.server_name, task.comfy_prompt_id = "f15", "old"
                task.finished_at = time.time()
                await queue._persist(task)
                queue = await _restart(queue, registry)
                try:
                    again = queue.get(task.id)
                    self.assertEqual(again.status, TASK_PENDING)
                    self.assertEqual(again.error, "")
                    self.assertIn("old", again.retired_comfy_prompt_ids)
                    self.assertEqual(queue.last_start_resume["revived_int"], 1)
                    # Asking again is idempotent: nothing more to bring back.
                    second = await queue.resume_after_restart()
                    self.assertEqual(second["revived_int"], 0)
                finally:
                    await queue.stop()

        run(scenario())

    def test_resumes_are_capped(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = await _start(registry)
                task = await queue.enqueue(_prompt())
                task.status, task.server_name = TASK_RENDERING, "gone"
                task.restart_resumes = config.RESTART_RESUME_MAX
                await queue._persist(task)
                queue = await _restart(queue, registry)
                try:
                    again = queue.get(task.id)
                    self.assertEqual(again.status, TASK_ERROR)
                    self.assertTrue(again.error.startswith(config.RESTART_RESUME_EXHAUSTED))
                finally:
                    await queue.stop()

        run(scenario())

    def test_explicit_resume_skips_a_superseded_node_and_dry_run_changes_nothing(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = await _start(registry)
                try:
                    old = await queue.enqueue(_prompt(graph_id="g", node_id="n1"))
                    old.status, old.error, old.finished_at = TASK_ERROR, config.RESTART_CANCEL_REASON, time.time()
                    other = await queue.enqueue(_prompt(graph_id="g", node_id="n2"))
                    other.status, other.error, other.finished_at = TASK_ERROR, config.RESTART_CANCEL_REASON, time.time()
                    newer = await queue.enqueue(_prompt(graph_id="g", node_id="n1"))
                    newer.created_at = old.created_at + 5
                    dry = await queue.resume_after_restart(task_ids=[old.id, other.id], dry_run=True)
                    self.assertEqual([r["id"] for r in dry["revived_array"]], [other.id])
                    self.assertEqual(queue.get(other.id).status, TASK_ERROR)
                    done = await queue.resume_after_restart(task_ids=[old.id, other.id])
                    self.assertEqual(done["revived_int"], 1)
                    self.assertEqual(queue.get(other.id).status, TASK_PENDING)
                    self.assertEqual(queue.get(old.id).status, TASK_ERROR)
                    self.assertIn("superseded", done["skipped_array"][0]["why_string"])
                finally:
                    await queue.stop()

        run(scenario())

    def test_the_box_is_freed_of_only_this_tasks_prompt(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                queue = await _start(registry)
                try:
                    with patch.object(comfy_adapter, "running_prompt_ids", AsyncMock(return_value=(["p1"], []))), \
                         patch.object(comfy_adapter, "interrupt", AsyncMock()) as interrupt, \
                         patch.object(comfy_adapter, "free_memory", AsyncMock(return_value=True)) as free:
                        self.assertEqual(await queue._free_box_after_restart("f15", "p1"), "interrupted and freed")
                        interrupt.assert_awaited_once()
                        free.assert_awaited_once()
                    with patch.object(comfy_adapter, "running_prompt_ids",
                                      AsyncMock(return_value=(["someone-else"], []))), \
                         patch.object(comfy_adapter, "interrupt", AsyncMock()) as interrupt:
                        self.assertEqual(await queue._free_box_after_restart("f15", "p1"), "already gone")
                        interrupt.assert_not_called()
                finally:
                    await queue.stop()

        run(scenario())


class BackendStartupTests(unittest.TestCase):
    def test_storage_start_resumes_instead_of_wiping(self):
        source = (_BACKEND / "main.py").read_text(encoding="utf-8")
        start = source.index("async def lifespan(")
        block = source[start:start + 6000]
        self.assertIn("/api-render/resume", block)
        self.assertLess(block.index('AUTORIG_RESTART_POLICY", "resume"'), block.index("/api-render/reset"))

    def test_editor_replay_is_not_refused_after_a_restart(self):
        if os.getenv("AUTORIG_RESTART_POLICY"):
            self.skipTest("policy overridden in this environment")
        sys.path.insert(0, str(_BACKEND))
        import task_owner
        self.assertFalse(task_owner._restart_policy_wipe())

    def test_avatar_build_resumes(self):
        source = (_BACKEND / "ai_avatar_build.py").read_text(encoding="utf-8")
        self.assertIn('job["restart_resumes"] = resumes + 1', source)


class VideoToolsResumeTests(unittest.TestCase):
    def test_a_job_interrupted_by_a_restart_runs_again_under_its_id(self):
        sys.path.insert(0, str(_BACKEND))
        import ai_video_tools as V

        calls = []

        async def fake_worker(body, work):
            calls.append(body.video_url)
            return {"frames_array": [], "count_int": 0}

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(V, "WORK_ROOT", Path(tmp)), \
                    patch.object(V, "_extract_frames", fake_worker), \
                    patch.object(V, "_JOBS", {}):
                (Path(tmp) / "work").mkdir()
                body = V.ExtractFramesRequest(video_url="https://autorig.online/x.mp4")
                job_id = V._job_id("extract_frames", body.model_dump())
                # The process died with the job queued: only its pending record is left.
                V._write_pending(job_id, {"kind": "extract_frames", "body": body.model_dump(),
                                          "created": time.time(), "restart_resumes": 0})
                status = await V.api_video_tools_status(job_id)
                self.assertEqual(status["task_id_string"], job_id)
                self.assertEqual(status.get("notice_string"), "resumed_after_restart")
                for _ in range(50):
                    await asyncio.sleep(0.02)
                    if V._JOBS[job_id]["status"] == "completed":
                        break
                self.assertEqual(V._JOBS[job_id]["status"], "completed")
                self.assertEqual(calls, ["https://autorig.online/x.mp4"])
                self.assertFalse(V._pending_path(job_id).exists())

        run(scenario())


if __name__ == "__main__":
    unittest.main()
