"""A bad request must not be allowed to quarantine the render fleet.

2026-09-22, production: one video task carried an image_url that answered 404.
The dispatcher's generic submit handler cooled down and marked render_error
every box it offered the task to - Raptor, f12, f5, then f15 - so all four
workers that can run gen_animation_by_url.json were out of rotation for ten
minutes because of a single unreachable URL. Every later video sat Pending and
the journal said nothing at all, because _pick_server answers None in silence.

These tests pin both halves of the fix: the fault taxonomy that keeps a
request's failure off the box's record, and the throttled line that names the
filter which emptied the candidate list.
"""
import asyncio
import contextlib
import io
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import comfy_adapter, config, errors, video_input
from renderfin.models import (
    TASK_ERROR,
    TASK_PENDING,
    RenderPrompt,
    RenderServer,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry


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


def _server(name="raptor", workflows=("gen_image.json",), status="online"):
    return RenderServer(
        render_server_name=name,
        render_server_url="http://5.129.157.224:8288",
        status=status,
        available_workflows=list(workflows),
    )


def _prompt():
    return RenderPrompt(prompt="a", type="t_pose", image_url="https://h/gone.png")


class SubmitFaultClassificationTests(unittest.TestCase):
    """Whose fault was the submit failure: the request's, or the box's?"""

    def _run_dispatch(self, registry, failure, passes):
        async def scenario():
            queue = RenderQueue(registry, db_path=config.DB_PATH)
            await queue.start()
            queue._pump_task.cancel()
            try:
                task = await queue.enqueue(_prompt())
                depths = {s.render_server_name: 0 for s in registry.all()}
                with patch.object(queue, "_queue_depths", return_value=depths), \
                        patch.object(queue, "_submit_task", side_effect=failure):
                    for _ in range(passes):
                        await queue._dispatch_one()
                return queue, task
            finally:
                await queue.stop()

        return run(scenario())

    def test_an_unreachable_image_url_never_touches_the_fleet(self):
        """The exact production failure: HTTP 404 on the caller's image."""
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            registry.save(_server("f5"))
            failure = comfy_adapter.ComfyRequestError(
                "failed to download image https://h/gone.png: HTTP 404"
            )
            queue, task = self._run_dispatch(registry, failure, passes=3)

            self.assertEqual(queue._server_submit_cooldowns, {})
            self.assertEqual(registry.get("raptor").status, "online")
            self.assertEqual(registry.get("f5").status, "online")
            self.assertEqual(task.status, TASK_ERROR)
            self.assertIn("bad render request 3x", task.error)
            self.assertIn("HTTP 404", task.error)

    def test_a_broken_control_video_is_the_request_not_the_box(self):
        """VideoInputError only ever describes the caller's URL."""
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            failure = video_input.VideoInputError("control video host is not allowed")
            queue, task = self._run_dispatch(registry, failure, passes=1)

            self.assertTrue(errors.is_request_fault(failure))
            self.assertEqual(queue._server_submit_cooldowns, {})
            self.assertEqual(registry.get("raptor").status, "online")
            self.assertEqual(task.status, TASK_PENDING)
            self.assertEqual(task.submit_failures, 1)

    def test_a_worker_fault_still_quarantines_the_worker(self):
        """A box that will not take the upload is still a box to avoid."""
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            failure = comfy_adapter.ComfyAdapterError("upload/image failed: HTTP 500")
            before = time.time()
            queue, task = self._run_dispatch(registry, failure, passes=1)

            self.assertIn("raptor", queue._server_submit_cooldowns)
            self.assertGreaterEqual(
                queue._server_submit_cooldowns["raptor"],
                before + config.SUBMIT_FAILURE_COOLDOWN_SECONDS - 5,
            )
            self.assertEqual(registry.get("raptor").status, "render_error")
            self.assertEqual(task.status, TASK_PENDING)
            self.assertEqual(task.submit_failures, 1)

    def test_a_worker_fault_still_fails_the_task_after_three_tries(self):
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            registry.save(_server("f5"))
            registry.save(_server("f15"))
            failure = comfy_adapter.ComfyAdapterError("upload/image failed: HTTP 500")
            queue, task = self._run_dispatch(registry, failure, passes=3)

            self.assertEqual(task.status, TASK_ERROR)
            self.assertIn("submit failed 3x", task.error)
            self.assertEqual(len(queue._server_submit_cooldowns), 3)

    def test_the_journal_still_names_the_box_and_the_reason(self):
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                self._run_dispatch(
                    registry,
                    comfy_adapter.ComfyRequestError("failed to download image x: HTTP 404"),
                    passes=1,
                )
            request_line = buffer.getvalue()
            self.assertIn("submit", request_line)
            self.assertIn("raptor", request_line)
            self.assertIn("HTTP 404", request_line)
            self.assertIn("stays in rotation", request_line)

        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                self._run_dispatch(
                    registry,
                    comfy_adapter.ComfyAdapterError("upload/image failed: HTTP 500"),
                    passes=1,
                )
            worker_line = buffer.getvalue()
            self.assertIn("raptor", worker_line)
            self.assertIn("HTTP 500", worker_line)
            self.assertIn("cooling until", worker_line)


class ComfyRejectionShapeTests(unittest.TestCase):
    """Which ComfyUI answers describe the prompt rather than the machine."""

    def _server(self):
        return RenderServer(
            render_server_name="test", render_server_url="http://127.0.0.1:8988"
        )

    def _submit(self, status_code, payload=None, text=""):
        class Response:
            def __init__(self):
                self.status_code = status_code
                self.text = text

            def json(self):
                return payload or {}

        class Client:
            async def post(self, url, **kwargs):
                return Response()

        return run(comfy_adapter.submit(Client(), self._server(), {}))

    def test_http_400_from_prompt_is_the_prompt_not_the_box(self):
        with self.assertRaises(comfy_adapter.ComfyRequestError):
            self._submit(400, text="invalid prompt")

    def test_http_500_from_prompt_is_the_box(self):
        with self.assertRaises(comfy_adapter.ComfyAdapterError) as caught:
            self._submit(502, text="bad gateway")
        self.assertFalse(errors.is_request_fault(caught.exception))

    def test_node_errors_on_an_accepted_prompt_are_the_prompt(self):
        payload = {
            "prompt_id": "p1",
            "node_errors": {"4": {"errors": [{"message": "value not in list"}]}},
        }
        with self.assertRaises(comfy_adapter.ComfyRequestError):
            self._submit(200, payload=payload)

    def test_a_request_error_is_still_a_comfy_adapter_error(self):
        """Nothing that already catches ComfyAdapterError may start missing."""
        self.assertTrue(
            issubclass(comfy_adapter.ComfyRequestError, comfy_adapter.ComfyAdapterError)
        )
        self.assertTrue(
            issubclass(comfy_adapter.ComfyRequestError, errors.RequestFaultError)
        )
        self.assertFalse(
            errors.is_request_fault(comfy_adapter.ComfyAdapterError("boom"))
        )


class StarvationLogTests(unittest.TestCase):
    """A starved queue must not look identical to an empty one."""

    def _dispatch(self, registry, prepare=None, passes=1):
        async def scenario():
            queue = RenderQueue(registry, db_path=config.DB_PATH)
            await queue.start()
            queue._pump_task.cancel()
            try:
                await queue.enqueue(_prompt())
                if prepare is not None:
                    prepare(queue)
                depths = {s.render_server_name: 0 for s in registry.all()}
                buffer = io.StringIO()
                with patch.object(queue, "_queue_depths", return_value=depths):
                    with contextlib.redirect_stdout(buffer):
                        for _ in range(passes):
                            dispatched = await queue._dispatch_one()
                            self.assertFalse(dispatched)
                return buffer.getvalue()
            finally:
                await queue.stop()

        return run(scenario())

    def test_a_cooling_fleet_says_so_with_an_absolute_deadline(self):
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))
            until = time.time() + 600

            def prepare(queue):
                queue._server_submit_cooldowns["raptor"] = until

            output = self._dispatch(registry, prepare)
            self.assertIn("no server for 1 pending task(s) on gen_image.json", output)
            self.assertIn("raptor cooling down until", output)
            self.assertIn(
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(until)), output
            )

    def test_every_filter_gets_named(self):
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("cold"))
            registry.save(_server("dark", status="offline"))
            registry.save(_server("wrongtool", workflows=("image_to_3d.json",)))

            def prepare(queue):
                queue._server_submit_cooldowns["cold"] = time.time() + 600

            output = self._dispatch(registry, prepare)
            self.assertIn("cold cooling down until", output)
            self.assertIn("dark offline (status=offline)", output)
            self.assertIn("wrongtool does not advertise gen_image.json", output)

    def test_an_empty_registry_says_there_are_no_servers(self):
        with _Env():
            output = self._dispatch(ServerRegistry())
            self.assertIn("no render servers are registered", output)

    def test_the_same_stall_is_printed_once_not_once_per_pass(self):
        with _Env():
            registry = ServerRegistry()
            registry.save(_server("raptor"))

            def prepare(queue):
                queue._server_submit_cooldowns["raptor"] = time.time() + 600

            output = self._dispatch(registry, prepare, passes=5)
            self.assertEqual(output.count("no server for"), 1)

    def test_a_new_quarantine_is_a_new_fact_and_is_printed(self):
        """Throttling by deadline, so a fresh cooldown never hides."""
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("raptor"))
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    await queue.enqueue(_prompt())
                    buffer = io.StringIO()
                    with patch.object(
                        queue, "_queue_depths", return_value={"raptor": 0}
                    ):
                        with contextlib.redirect_stdout(buffer):
                            queue._server_submit_cooldowns["raptor"] = time.time() + 600
                            await queue._dispatch_one()
                            await queue._dispatch_one()
                            queue._server_submit_cooldowns["raptor"] = time.time() + 1200
                            await queue._dispatch_one()
                    return buffer.getvalue()
                finally:
                    await queue.stop()

        output = run(scenario())
        self.assertEqual(output.count("no server for"), 2)

    def test_thirty_waiting_tasks_are_one_line_with_a_count(self):
        """The throttle must not be defeated by the size of the backlog."""
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("raptor"))
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    for _ in range(30):
                        await queue.enqueue(_prompt())
                    queue._server_submit_cooldowns["raptor"] = time.time() + 600
                    buffer = io.StringIO()
                    with patch.object(
                        queue, "_queue_depths", return_value={"raptor": 0}
                    ):
                        with contextlib.redirect_stdout(buffer):
                            await queue._dispatch_one()
                    return buffer.getvalue()
                finally:
                    await queue.stop()

        output = run(scenario())
        self.assertEqual(output.count("no server for"), 1)
        self.assertIn("no server for 30 pending task(s)", output)

    def test_a_busy_fleet_is_reported_as_busy_not_as_broken(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("raptor"))
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    running = await queue.enqueue(_prompt())
                    running.status = "Rendering"
                    running.server_name = "raptor"
                    running.started_at = 1e18
                    await queue._persist(running)
                    await queue.enqueue(_prompt())
                    buffer = io.StringIO()
                    with patch.object(
                        queue, "_queue_depths", return_value={"raptor": 0}
                    ):
                        with contextlib.redirect_stdout(buffer):
                            await queue._dispatch_one()
                    return buffer.getvalue(), running.id
                finally:
                    await queue.stop()

        output, running_id = run(scenario())
        self.assertIn(f"raptor busy with {running_id}", output)


if __name__ == "__main__":
    unittest.main()
