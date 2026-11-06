"""GPU memory budgets, out-of-memory retries and hung-box recovery (2026-09-29).

Production, 2026-09-29: an H3 Eros Max job of 960x640x356 frames went to f15
(RTX 3070 Ti, 8 GB) because it was the idle box, ran 885 s and died with
torch.OutOfMemoryError; the task failed for good. Its sibling of the same size
went to f5, reset the GPU driver (nvlddmkm 153 at 14:19:32) and left f5's
ComfyUI a one-thread process that held its port and answered nothing: the task
stayed Rendering for the whole 90-minute timeout, and every poll first waited
30 s, stalling the pump. Five more 768x960x356 H3 jobs then died of OOM on
f15 within 20 minutes, and a small Qwen edit failed there with comfy-aimdo's
"Fault failed: 2".

These tests pin the fix: jobs are routed by a VRAM budget, an out-of-memory
job is retried (on a bigger box when it was too big), a hung or faulted box is
restarted where that is allowed (never worker-4090) and its task moved, with a
retry cap and a final error a person can act on.
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

import httpx

_PACKAGE = Path(__file__).resolve().parents[1] / "renderfin"
for _name in ("image_quality", "workload_lease"):
    _dotted = f"renderfin.{_name}"
    if (_PACKAGE / f"{_name}.py").is_file() and not getattr(
        sys.modules.get(_dotted), "__file__", None
    ):
        sys.modules.pop(_dotted, None)
        sys.modules.pop("renderfin.queue", None)
        importlib.import_module(_dotted)

from renderfin import comfy_adapter, config, gpu_budget
from renderfin.models import (
    TASK_ERROR,
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry

H3 = "gen_video_minimax_h3_by_url.json"
GIB = 2 ** 30


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _Env:
    """Temp data dirs patched into renderfin.config."""

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
            # RenderQueue.start() otherwise runs the owner's FARM RESET (POST
            # /queue clear + /interrupt) against every "online" test server.
            patch.object(config, "WIPE_QUEUE_ON_START", False),
            patch.object(config, "OOM_RECOVERY_ENABLED", True),
            patch.dict(os.environ, {"RENDERFIN_BOX_VRAM_GB": gpu_budget.DEFAULT_BOX_VRAM_GB}),
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


def _server(name, workflows=(H3,), average=100.0, status="online"):
    # Port 9 (discard): nothing a stray request could reach. Never a real box
    # URL - on the storage host 127.0.0.1:18588 IS f15.
    return RenderServer(
        render_server_name=name,
        render_server_url="http://127.0.0.1:9",
        status=status,
        available_workflows=list(workflows),
        average_render_time=average,
    )


def _video(width=960, height=640, frames=356, workflow=H3):
    return RenderPrompt(
        prompt="a",
        image_url="https://h/start.png",
        work_flow=workflow,
        main_size_width=width,
        main_size_height=height,
        frame_count=frames,
    )


def _error_entry(exception_type, message, prompt_id="p1"):
    return {
        "status": {
            "status_str": "error",
            "completed": False,
            "messages": [
                ["execution_start", {"prompt_id": prompt_id}],
                ["execution_error", {
                    "prompt_id": prompt_id,
                    "node_id": "sample",
                    "node_type": "SamplerCustomAdvanced",
                    "exception_type": exception_type,
                    "exception_message": message,
                }],
            ],
        }
    }


OOM_ENTRY = _error_entry(
    "torch.OutOfMemoryError",
    "Allocation on device \nThis error means you ran out of memory on your GPU.",
)
FAULT_ENTRY = _error_entry("RuntimeError", "Fault failed: 2\n")
BUG_ENTRY = _error_entry("RuntimeError", "The size of tensor a (1097226) must match the size of tensor b")
INTERRUPTED_ENTRY = {
    "status": {
        "status_str": "error",
        "completed": False,
        "messages": [
            ["execution_start", {"prompt_id": "p1"}],
            ["execution_interrupted", {"prompt_id": "p1", "node_id": "sample"}],
        ],
    }
}
STATS_8GB_IDLE = {"system": {}, "devices": [{"vram_total": 8589279232, "vram_free": 7307819004}]}


async def _started_queue(registry):
    queue = RenderQueue(registry, db_path=config.DB_PATH)
    await queue.start()
    queue._pump_task.cancel()
    queue._restart_poll_seconds = 0.0
    return queue


def _rendering(task, box, prompt_id="p1", started=None):
    task.status = TASK_RENDERING
    task.server_name = box
    task.comfy_prompt_id = prompt_id
    task.started_at = time.time() if started is None else started


async def _drain(queue):
    while queue._background:
        await asyncio.gather(*list(queue._background), return_exceptions=True)


class BudgetMathTests(unittest.TestCase):
    def test_workflow_families(self):
        self.assertEqual(gpu_budget.family(H3), "h3")
        self.assertEqual(gpu_budget.family("gen_animation_by_url.json"), "ltx")
        self.assertEqual(gpu_budget.family("gen_animation_ltx10eros_by_url.json"), "ltx")
        self.assertEqual(gpu_budget.family("gen_video_ltx23_pose_by_url.json"), "ltx_control")
        self.assertEqual(gpu_budget.family("gen_video_ltx25_crossview_by_url.json"), "ltx_control")
        self.assertIsNone(gpu_budget.family("qwen_image21_edit_multi.json"))
        self.assertIsNone(gpu_budget.family("gen_image.json"))

    def test_cost_is_taken_at_the_model_grid(self):
        # 356 requested frames run as 353 (8k+1), exactly what f15's history shows
        self.assertEqual(gpu_budget.model_frames(356), 353)
        self.assertEqual(gpu_budget.job_cost(_video(960, 640, 356), "h3"), 960 * 640 * 353)
        # 540 pads to 544 on the /32 grid, to 576 on the /64 IC-LoRA grid
        self.assertEqual(gpu_budget.job_cost(_video(960, 540, 97), "ltx"), 960 * 544 * 97)
        self.assertEqual(gpu_budget.job_cost(_video(960, 540, 97), "ltx_control"), 960 * 576 * 97)

    def test_budget_follows_the_vram_class(self):
        self.assertEqual(gpu_budget.budget_pxf("h3", 8589279232), 165_000_000)      # 3070 Ti
        self.assertEqual(gpu_budget.budget_pxf("h3", 12884377600), 300_000_000)     # 3080 Ti
        self.assertEqual(gpu_budget.budget_pxf("h3", 25757220864), 520_000_000)     # 4090
        self.assertEqual(gpu_budget.budget_pxf("h3", 6 * GIB), int(165e6 * 6 / 8))  # smaller card
        self.assertIsNone(gpu_budget.budget_pxf("h3", 0))
        self.assertIsNone(gpu_budget.budget_pxf(None, 8 * GIB))

    def test_the_production_failures_are_over_the_8gb_budget(self):
        budget = gpu_budget.budget_pxf("h3", 8589279232)
        for width, height, frames in ((960, 640, 356), (768, 960, 356), (640, 960, 324)):
            self.assertGreater(gpu_budget.job_cost(_video(width, height, frames), "h3"), budget)
        # what f5/f15 finished in 11-14 minutes stays on them
        for width, height, frames in ((640, 960, 252), (704, 960, 236), (544, 960, 124)):
            self.assertLessEqual(gpu_budget.job_cost(_video(width, height, frames), "h3"), budget)

    def test_budgets_can_be_overridden_without_a_code_change(self):
        with patch.dict(os.environ, {"RENDERFIN_VRAM_BUDGETS_JSON": '{"h3": {"8": 100}}'}):
            self.assertEqual(gpu_budget.budget_pxf("h3", 8 * GIB), 100_000_000)
            self.assertEqual(gpu_budget.budget_pxf("ltx", 8 * GIB), 150_000_000)
        with patch.dict(os.environ, {"RENDERFIN_VRAM_BUDGETS_JSON": "{broken"}):
            self.assertEqual(gpu_budget.budget_pxf("h3", 8 * GIB), 165_000_000)

    def test_failure_classes(self):
        self.assertEqual(gpu_budget.classify_failure(OOM_ENTRY), "oom")
        self.assertEqual(
            gpu_budget.classify_failure(_error_entry("RuntimeError", "VRAM grow failed: 427571200 bytes")),
            "oom",
        )
        self.assertEqual(gpu_budget.classify_failure(FAULT_ENTRY), "gpu_fault")
        self.assertEqual(
            gpu_budget.classify_failure(_error_entry("torch.AcceleratorError", "CUDA error: an illegal memory access was encountered")),
            "gpu_fault",
        )
        self.assertEqual(gpu_budget.classify_failure(INTERRUPTED_ENTRY), "interrupted")
        self.assertIsNone(gpu_budget.classify_failure(BUG_ENTRY))
        self.assertIsNone(gpu_budget.classify_failure({}))

    def test_hang_limit_scales_with_the_job(self):
        cost = gpu_budget.job_cost(_video(960, 640, 356), "h3")
        limit = gpu_budget.hang_after_seconds(
            "h3", 12 * GIB, cost, factor=3.0, floor=1200.0, ceiling=5400.0)
        self.assertAlmostEqual(limit, 3 * (60 + 3.6 * cost / 1e6), places=3)
        small = gpu_budget.hang_after_seconds(
            "ltx", 24 * GIB, 50_000_000, factor=3.0, floor=1200.0, ceiling=5400.0)
        self.assertEqual(small, 1200.0)
        self.assertIsNone(gpu_budget.hang_after_seconds(
            None, 24 * GIB, 50_000_000, factor=3.0, floor=1200.0, ceiling=5400.0))


class BudgetRoutingTests(unittest.TestCase):
    def test_a_217m_h3_job_skips_the_idle_8gb_box(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15", average=140.0))
                registry.save(_server("Raptor", average=330.0))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    picked = queue._pick_server(task.workflow, {"f15": 0, "Raptor": 0}, task)
                    self.assertEqual(picked.render_server_name, "Raptor")
                    small = await queue.enqueue(_video(544, 960, 124))
                    picked = queue._pick_server(small.workflow, {"f15": 0, "Raptor": 0}, small)
                    self.assertEqual(picked.render_server_name, "f15")
                finally:
                    await queue.stop()

        run(scenario())

    def test_the_stall_line_names_the_budget(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    busy = await queue.enqueue(_video(544, 960, 124))
                    _rendering(busy, "Raptor", prompt_id="other")
                    task = await queue.enqueue(_video(960, 640, 356))
                    self.assertIsNone(queue._pick_server(task.workflow, {}, task))
                    _key, text = queue._starvation_report(task)
                    self.assertIn("f15 too small for this job", text)
                    self.assertIn("165M", text)
                    self.assertIn("Raptor busy with", text)
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_measured_card_beats_the_static_table(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    self.assertIsNone(queue._pick_server(task.workflow, {}, task))
                    queue._box_vram["f15"] = (time.time(), 12884377600, 12 * GIB)
                    self.assertEqual(
                        queue._pick_server(task.workflow, {}, task).render_server_name, "f15")
                finally:
                    await queue.stop()

        run(scenario())

    def test_too_large_for_every_box_fails_at_once(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("Raptor"))
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(2048, 2048, 400))
                    with patch.object(queue, "_queue_depths", new=AsyncMock(return_value={})):
                        await queue._dispatch_one()
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertIn("Too large for the render farm", task.error)
                    self.assertIn("worker-4090", task.error)
                    self.assertIn("lower the size or the frame count", task.error)
                finally:
                    await queue.stop()

        run(scenario())

    def test_image_jobs_are_not_budgeted(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15", workflows=("gen_image.json",)))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", main_size_width=2048, main_size_height=2048))
                    self.assertEqual(
                        queue._pick_server(task.workflow, {}, task).render_server_name, "f15")
                finally:
                    await queue.stop()

        run(scenario())


class OomRecoveryTests(unittest.TestCase):
    def _patches(self, queue, *, history, stats=STATS_8GB_IDLE, restart="requested", queue_ids=([], [])):
        self.free = AsyncMock(return_value=True)
        self.restart = AsyncMock(return_value=restart)
        if isinstance(stats, list):
            self.stats = AsyncMock(side_effect=stats)
        else:
            self.stats = AsyncMock(return_value=stats)
        return [
            patch.object(comfy_adapter, "poll_history", new=AsyncMock(return_value=history)),
            patch.object(comfy_adapter, "free_memory", new=self.free),
            patch.object(comfy_adapter, "system_stats", new=self.stats),
            patch.object(comfy_adapter, "request_restart", new=self.restart),
            patch.object(comfy_adapter, "running_prompt_ids", new=AsyncMock(return_value=queue_ids)),
        ]

    async def _poll(self, queue, patches):
        for p in patches:
            p.start()
        try:
            await queue._poll_rendering()
            await _drain(queue)
        finally:
            for p in reversed(patches):
                p.stop()

    def test_oom_retries_the_task_on_a_bigger_box_and_frees_the_card(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    # the pre-budget world: the job is already on the 8 GB box
                    task = await queue.enqueue(_video(960, 640, 356))
                    _rendering(task, "f15")
                    await self._poll(queue, self._patches(queue, history=("error", OOM_ENTRY)))

                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.oom_retries, 1)
                    self.assertEqual(task.recovery_log, ["oom@f15"])
                    self.assertIn("f15", task.avoid_servers)
                    self.assertIn("p1", task.retired_comfy_prompt_ids)
                    self.assertEqual(task.server_name, "")
                    self.free.assert_awaited()
                    self.restart.assert_not_awaited()  # healthy after /free, first OOM
                    self.assertIn(("f15", "h3"), queue._oom_ceilings)
                    picked = queue._pick_server(task.workflow, {}, task)
                    self.assertEqual(picked.render_server_name, "Raptor")
                finally:
                    await queue.stop()

        run(scenario())

    def test_the_retry_cap_gives_a_clear_final_error(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("Raptor"))
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    for box in ("f15", "Raptor", "worker-4090"):
                        _rendering(task, box, prompt_id=f"p-{box}")
                        await self._poll(queue, self._patches(queue, history=("error", OOM_ENTRY)))
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertEqual(task.oom_retries, config.OOM_MAX_RETRIES + 1)
                    self.assertIn("Out of GPU memory at 960x640x353 frames", task.error)
                    self.assertIn("f15, Raptor, worker-4090", task.error)
                    self.assertIn("3 attempts", task.error)
                    self.assertIn("lower the size or the frame count", task.error)
                finally:
                    await queue.stop()

        run(scenario())

    def test_the_same_box_is_used_again_when_nothing_else_fits(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("Raptor"))
                registry.save(_server("f15"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))  # fits 12 GB only
                    _rendering(task, "Raptor")
                    stats_12gb = {"devices": [{"vram_total": 12884377600, "vram_free": 11 * GIB}]}
                    await self._poll(queue, self._patches(
                        queue, history=("error", OOM_ENTRY), stats=stats_12gb))
                    self.assertEqual(task.status, TASK_PENDING)
                    self.restart.assert_not_awaited()
                    picked = queue._pick_server(task.workflow, {}, task)
                    self.assertEqual(picked.render_server_name, "Raptor")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_second_oom_restarts_comfy_and_waits_for_it(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    queue._box_ooms["f15"] = [time.time() - 60]
                    task = await queue.enqueue(_video(768, 960, 356))
                    _rendering(task, "f15")
                    # health probe answers, then the restarted box goes down and comes back
                    stats = [STATS_8GB_IDLE, None, STATS_8GB_IDLE]
                    await self._poll(queue, self._patches(queue, history=("error", OOM_ENTRY), stats=stats))
                    self.restart.assert_awaited_once()
                    self.assertNotIn("f15", queue._recovering)  # it answered again
                    self.assertEqual(task.status, TASK_PENDING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_worker_4090_is_never_restarted(self):
        async def scenario():
            with _Env(), patch.object(config, "COMFY_RESTART_BOXES", {"worker-4090", "f15"}):
                registry = ServerRegistry()
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    queue._box_ooms["worker-4090"] = [time.time() - 60]
                    task = await queue.enqueue(_video(960, 640, 356))
                    _rendering(task, "worker-4090")
                    await self._poll(queue, self._patches(queue, history=("error", OOM_ENTRY)))
                    self.free.assert_awaited()          # freeing VRAM is fine
                    self.restart.assert_not_awaited()   # restarting is not
                    self.assertEqual(task.status, TASK_PENDING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_box_with_foreign_prompts_is_not_restarted(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                queue = await _started_queue(registry)
                try:
                    queue._box_ooms["f15"] = [time.time() - 60]
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "f15")
                    await self._poll(queue, self._patches(
                        queue, history=("error", OOM_ENTRY), queue_ids=(["someone-else"], [])))
                    self.restart.assert_not_awaited()
                    self.assertNotIn("f15", queue._recovering)
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_gpu_fault_moves_the_job_and_restarts_the_box(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15", workflows=("gen_image_control_canny.json",)))
                registry.save(_server("f5", workflows=("gen_image_control_canny.json",)))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="qwen_image21_edit", image_url="https://h/a.png"))
                    _rendering(task, "f15")
                    await self._poll(queue, self._patches(
                        queue, history=("error", FAULT_ENTRY), stats=[None, STATS_8GB_IDLE]))
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.hang_retries, 1)
                    self.assertEqual(task.recovery_log, ["gpu_fault@f15"])
                    self.restart.assert_awaited_once()
                    self.assertEqual(queue._oom_ceilings, {})  # not a size problem
                    picked = queue._pick_server(task.workflow, {}, task)
                    self.assertEqual(picked.render_server_name, "f5")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_request_bug_is_not_retried(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "f15")
                    await self._poll(queue, self._patches(queue, history=("error", BUG_ENTRY)))
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertIn("The size of tensor a", task.error)
                    self.free.assert_not_awaited()
                finally:
                    await queue.stop()

        run(scenario())

    def test_switched_off_it_behaves_as_before(self):
        async def scenario():
            with _Env(), patch.object(config, "OOM_RECOVERY_ENABLED", False):
                registry = ServerRegistry()
                registry.save(_server("f15"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    self.assertEqual(
                        queue._pick_server(task.workflow, {}, task).render_server_name, "f15")
                    _rendering(task, "f15")
                    await self._poll(queue, self._patches(queue, history=("error", OOM_ENTRY)))
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertIn("Out of GPU memory on f15", task.error)
                finally:
                    await queue.stop()

        run(scenario())


class HangRecoveryTests(unittest.TestCase):
    def test_an_unanswering_box_is_polled_briefly_and_rarely(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f5"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "f5")
                    poll = AsyncMock(side_effect=httpx.RemoteProtocolError("Server disconnected"))
                    with patch.object(comfy_adapter, "poll_history", new=poll):
                        await queue._poll_rendering()
                        await queue._poll_rendering()  # inside the back-off: not asked
                    self.assertEqual(poll.await_count, 1)
                    self.assertEqual(task.status, TASK_RENDERING)
                    queue._poll_not_before[task.id] = 0
                    with patch.object(comfy_adapter, "poll_history", new=poll):
                        await queue._poll_rendering()
                    # the retry after a failure uses the short timeout
                    self.assertEqual(poll.await_args.kwargs["timeout"], config.POLL_FAILURE_TIMEOUT_SECONDS)
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_box_silent_past_the_limit_loses_the_task(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f5"))
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "f5", prompt_id="d6adabcb")
                    queue._poll_failing_since[task.id] = time.time() - config.UNREACHABLE_REQUEUE_SECONDS - 1
                    restart = AsyncMock(return_value="unreachable")
                    with patch.object(comfy_adapter, "poll_history",
                                      new=AsyncMock(side_effect=httpx.ReadTimeout(""))), \
                            patch.object(comfy_adapter, "running_prompt_ids", new=AsyncMock(return_value=None)), \
                            patch.object(comfy_adapter, "request_restart", new=restart):
                        await queue._poll_rendering()
                        await _drain(queue)
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.recovery_log, ["hang@f5"])
                    self.assertIn("d6adabcb", task.retired_comfy_prompt_ids)
                    restart.assert_awaited_once()          # asked, even if it cannot be heard
                    self.assertNotIn("f5", queue._recovering)
                    self.assertEqual(queue._busy_servers(), {})
                    picked = queue._pick_server(task.workflow, {}, task)
                    self.assertEqual(picked.render_server_name, "Raptor")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_502_from_the_edge_counts_as_silence(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "worker-4090")
                    with patch.object(comfy_adapter, "poll_history",
                                      new=AsyncMock(return_value=("unavailable", None))):
                        await queue._poll_rendering()
                    self.assertIn(task.id, queue._poll_failing_since)
                    self.assertEqual(task.status, TASK_RENDERING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_prompt_running_far_too_long_is_interrupted_and_moved(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("Raptor"))
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    _rendering(task, "Raptor", started=time.time() - 3000)  # limit ~2520 s
                    interrupt = AsyncMock(return_value=None)
                    restart = AsyncMock(return_value="requested")
                    with patch.object(comfy_adapter, "poll_history", new=AsyncMock(return_value=("unknown", None))), \
                            patch.object(comfy_adapter, "queue_contains", new=AsyncMock(return_value=True)), \
                            patch.object(comfy_adapter, "running_prompt_ids", new=AsyncMock(return_value=(["p1"], []))), \
                            patch.object(comfy_adapter, "interrupt", new=interrupt), \
                            patch.object(comfy_adapter, "request_restart", new=restart), \
                            patch.object(comfy_adapter, "system_stats", new=AsyncMock(side_effect=[None, {"devices": [{"vram_total": 12884377600, "vram_free": 11 * GIB}]}])):
                        await queue._poll_rendering()
                        await _drain(queue)
                    interrupt.assert_awaited_once()
                    restart.assert_awaited_once()
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.recovery_log, ["hang@Raptor"])
                    self.assertEqual(
                        queue._pick_server(task.workflow, {}, task).render_server_name, "worker-4090")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_prompt_queued_behind_other_clients_is_not_hung(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(960, 640, 356))
                    _rendering(task, "Raptor", started=time.time() - 3000)
                    interrupt = AsyncMock(return_value=None)
                    with patch.object(comfy_adapter, "poll_history", new=AsyncMock(return_value=("unknown", None))), \
                            patch.object(comfy_adapter, "queue_contains", new=AsyncMock(return_value=True)), \
                            patch.object(comfy_adapter, "running_prompt_ids", new=AsyncMock(return_value=(["foreign"], ["p1"]))), \
                            patch.object(comfy_adapter, "interrupt", new=interrupt):
                        await queue._poll_rendering()
                    interrupt.assert_not_awaited()
                    self.assertEqual(task.status, TASK_RENDERING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_an_interrupt_from_outside_is_retried(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("Raptor"))
                registry.save(_server("worker-4090"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(768, 960, 353))
                    _rendering(task, "Raptor")
                    with patch.object(comfy_adapter, "poll_history",
                                      new=AsyncMock(return_value=("error", INTERRUPTED_ENTRY))):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.recovery_log, ["interrupted@Raptor"])
                finally:
                    await queue.stop()

        run(scenario())

    def test_our_own_cancel_is_not_retried_as_an_outside_interrupt(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("Raptor"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(768, 960, 353))
                    _rendering(task, "Raptor")

                    async def interrupt_then_poll(*args, **kwargs):
                        # the pump polls while cancel() waits for /interrupt
                        with patch.object(comfy_adapter, "poll_history",
                                          new=AsyncMock(return_value=("error", INTERRUPTED_ENTRY))):
                            await queue._poll_rendering()

                    with patch.object(comfy_adapter, "interrupt", new=interrupt_then_poll):
                        self.assertTrue(await queue.cancel(task.id, reason="cancelled by the owner"))
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertEqual(task.error, "cancelled by the owner")
                    self.assertEqual(task.recovery_log, [])
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_vanished_video_prompt_tries_another_box_first(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("f15"))
                registry.save(_server("f5"))
                queue = await _started_queue(registry)
                try:
                    task = await queue.enqueue(_video(544, 960, 124))
                    _rendering(task, "f15")
                    with patch.object(comfy_adapter, "poll_history", new=AsyncMock(return_value=("unknown", None))), \
                            patch.object(comfy_adapter, "queue_contains", new=AsyncMock(return_value=False)):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.avoid_servers, ["f15"])
                    self.assertEqual(task.hang_retries, 0)  # a crash-restart is not charged
                    self.assertEqual(
                        queue._pick_server(task.workflow, {}, task).render_server_name, "f5")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_restart_that_never_comes_back_keeps_the_box_out(self):
        async def scenario():
            with _Env(), patch.object(config, "COMFY_RESTART_WAIT_SECONDS", 0.05):
                registry = ServerRegistry()
                registry.save(_server("f5"))
                queue = await _started_queue(registry)
                try:
                    with patch.object(comfy_adapter, "running_prompt_ids", new=AsyncMock(return_value=None)), \
                            patch.object(comfy_adapter, "request_restart", new=AsyncMock(return_value="requested")), \
                            patch.object(comfy_adapter, "system_stats", new=AsyncMock(return_value=None)):
                        back = await queue._restart_comfy(registry.get("f5"), reason="test")
                    self.assertFalse(back)
                    # rate limit: a second request right away is refused
                    self.assertIn("already restarted", queue._restart_refusal("f5"))
                finally:
                    await queue.stop()

        run(scenario())


class AdapterTests(unittest.TestCase):
    def test_restart_request_outcomes(self):
        server = _server("f15")

        def client_for(handler):
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        async def scenario():
            seen = []

            def post_then_ok(request):
                seen.append(request.method)
                return httpx.Response(200, json={})

            async with client_for(post_then_ok) as client:
                self.assertEqual(await comfy_adapter.request_restart(client, server), "requested")
            self.assertEqual(seen, ["POST"])

            seen.clear()

            def legacy_get(request):
                seen.append(request.method)
                return httpx.Response(405 if request.method == "POST" else 200)

            async with client_for(legacy_get) as client:
                self.assertEqual(await comfy_adapter.request_restart(client, server), "requested")
            self.assertEqual(seen, ["POST", "GET"])

            async with client_for(lambda request: httpx.Response(403)) as client:
                self.assertEqual(await comfy_adapter.request_restart(client, server), "forbidden")

            def dropped(request):
                raise httpx.RemoteProtocolError("Server disconnected", request=request)

            async with client_for(dropped) as client:
                self.assertEqual(await comfy_adapter.request_restart(client, server), "requested")

            def refused(request):
                raise httpx.ConnectError("refused", request=request)

            async with client_for(refused) as client:
                self.assertEqual(await comfy_adapter.request_restart(client, server), "unreachable")

            freed = []

            def free(request):
                freed.append((request.url.path, request.content))
                return httpx.Response(200)

            async with client_for(free) as client:
                self.assertTrue(await comfy_adapter.free_memory(client, server))
            self.assertEqual(freed[0][0], "/free")
            self.assertIn(b'"unload_models"', freed[0][1])

            async with client_for(lambda request: httpx.Response(502)) as client:
                state, _ = await comfy_adapter.poll_history(client, server, "p1", strict=True)
                self.assertEqual(state, "unavailable")
                state, _ = await comfy_adapter.poll_history(client, server, "p1")
                self.assertEqual(state, "pending")

        run(scenario())


if __name__ == "__main__":
    unittest.main()
