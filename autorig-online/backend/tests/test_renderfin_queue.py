import asyncio
import tempfile
import time
import unittest
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from renderfin import comfy_adapter, config, workload_lease
from renderfin.models import (
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
)
from renderfin.queue import RenderQueue, _host_terminal_outcome
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


def _server(name="raptor", workflows=("gen_image.json",)):
    return RenderServer(
        render_server_name=name,
        render_server_url="http://5.129.157.224:8288",
        status="online",
        available_workflows=list(workflows),
    )


class QueueDispatchTests(unittest.TestCase):
    def test_real_host_bridge_terminal_status_aliases(self):
        self.assertEqual(_host_terminal_outcome({"status": "Completed"}), "completed")
        self.assertEqual(_host_terminal_outcome({"status": "Preempted"}), "preempted")
        self.assertEqual(_host_terminal_outcome({"status": "Released"}), "released")

    def test_host_bridge_409_completed_is_terminal_not_capacity(self):
        async def scenario():
            server = _server()
            server.managed_workload = True

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(request.headers["Authorization"], "Bearer bridge-token")
                return httpx.Response(409, json={"status": "Completed"})

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                    }
                }
            )
            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    result = await workload_lease.host_comfy_control(
                        client,
                        server=server,
                        action="preempt",
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                    )
            self.assertEqual(result["status"], "Completed")

        run(scenario())

    def test_host_bridge_423_artifact_pending_is_completed_not_capacity(self):
        async def scenario():
            server = _server()
            server.managed_workload = True

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    423,
                    json={
                        "status_string": "artifact_pending",
                        "prompt_id": "prompt-1",
                        "logical_task_id": "task-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                    }
                }
            )
            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    result = await workload_lease.host_comfy_control(
                        client,
                        server=server,
                        action="preempt",
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                    )
            self.assertEqual(_host_terminal_outcome(result), "completed")
            self.assertEqual(result["status_string"], "artifact_pending")

        run(scenario())

    def test_central_release_client_rejects_unconfirmed_terminal_state(self):
        async def scenario():
            responses = iter(("expired", "completed"))

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"status_string": next(responses)})

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_WORKLOAD_BROKER_TOKEN": "broker-token",
                },
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(RuntimeError):
                        await workload_lease.release(
                            client,
                            lease_id="lease-1",
                            owner_task_id="task-1",
                            request_id="request-1",
                            outcome="completed",
                        )
                    await workload_lease.release(
                        client,
                        lease_id="lease-1",
                        owner_task_id="task-1",
                        request_id="request-1",
                        outcome="completed",
                    )

        run(scenario())

    def test_exact_host_preempt_accepts_already_expired_central_lease(self):
        async def scenario():
            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "status_string": "expired",
                        "lease_by_key": {"state_string": "expired"},
                    },
                )

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_WORKLOAD_BROKER_TOKEN": "broker-token",
                },
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    await workload_lease.release(
                        client,
                        lease_id="lease-expired",
                        owner_task_id="task-1",
                        request_id="request-1",
                        outcome="preempted",
                    )

        run(scenario())

    def test_expired_central_lease_exactly_reconciles_host_before_requeue(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "expired-prompt"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-expired"
                    task.workload_request_id = "request-expired"
                    task.workload_physical_resource_id = "raptor"
                    await queue._persist(task)

                    terminal = workload_lease.WorkloadLeaseTerminal(
                        {
                            "status_string": "lease_terminal",
                            "lease_by_key": {"state_string": "expired"},
                        }
                    )
                    host_control = AsyncMock(
                        return_value={"status": "Preempted"}
                    )
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(side_effect=terminal)
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host_control
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter,
                        "poll_history",
                        new=AsyncMock(side_effect=AssertionError("old prompt polled")),
                    ):
                        await queue._poll_rendering()

                    restored = queue.get(task.id)
                    self.assertEqual(restored.status, TASK_PENDING)
                    self.assertEqual(restored.started_at, 0)
                    self.assertEqual(restored.comfy_prompt_id, "")
                    self.assertEqual(restored.workload_lease_id, "")
                    self.assertNotEqual(
                        restored.workload_request_id, "request-expired"
                    )
                    host_control.assert_awaited_once()
                    self.assertEqual(
                        host_control.await_args.kwargs["action"], "preempt"
                    )
                    release.assert_awaited_once()
                finally:
                    await queue.stop()

        run(scenario())

    def test_expired_central_and_local_artifact_hold_continue_old_prompt(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "artifact-prompt"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-expired"
                    task.workload_request_id = "request-expired"
                    task.workload_physical_resource_id = "raptor"
                    await queue._persist(task)
                    terminal = workload_lease.WorkloadLeaseTerminal(
                        {
                            "status_string": "lease_terminal",
                            "lease_by_key": {"state_string": "expired"},
                        }
                    )
                    history = AsyncMock(return_value=("running", {}))
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(side_effect=terminal)
                    ), patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(
                            return_value={
                                "status_string": "artifact_pending",
                                "outcome_string": "completed",
                            }
                        ),
                    ), patch.object(
                        comfy_adapter, "poll_history", new=history
                    ):
                        await queue._poll_rendering()
                    restored = queue.get(task.id)
                    self.assertEqual(restored.status, TASK_RENDERING)
                    self.assertEqual(restored.comfy_prompt_id, "artifact-prompt")
                    self.assertEqual(restored.workload_lease_id, "lease-expired")
                    history.assert_awaited_once()
                finally:
                    await queue.stop()

        run(scenario())

    def test_both_ttls_after_history_success_download_once_and_complete(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="portrait", user_name="bot")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "artifact-prompt"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-expired"
                    task.workload_request_id = "request-expired"
                    task.workload_physical_resource_id = "raptor"
                    await queue._persist(task)
                    entry = {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "artifact.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        }
                    }
                    terminal = workload_lease.WorkloadLeaseTerminal(
                        {
                            "status_string": "lease_terminal",
                            "lease_by_key": {"state_string": "expired"},
                        }
                    )

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "complete":
                            return {"status": "Completed"}
                        return {
                            "status_string": "artifact_pending",
                            "outcome_string": "completed",
                        }

                    host = AsyncMock(side_effect=host_control)
                    download = AsyncMock(return_value=b"PNG-EXACT-ARTIFACT")
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(side_effect=terminal)
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter,
                        "poll_history",
                        new=AsyncMock(return_value=("completed", entry)),
                    ), patch.object(
                        comfy_adapter, "download_artifact", new=download
                    ):
                        await queue._poll_rendering()
                        await queue._finishers[task.id]

                    restored = queue.get(task.id)
                    self.assertEqual(restored.status, TASK_DONE)
                    self.assertTrue(restored.artifact_sha256)
                    self.assertEqual(restored.workload_lease_id, "")
                    download.assert_awaited_once()
                    release.assert_awaited_once()
                    actions = [call.kwargs["action"] for call in host.await_args_list]
                    self.assertEqual(actions, ["preempt", "preempt", "complete"])
                finally:
                    await queue.stop()

        run(scenario())

    def test_managed_timeout_completed_wins_and_persists_artifact(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="portrait", user_name="bot")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "timeout-completed-prompt"
                    task.started_at = 1.0
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-timeout-completed"
                    task.workload_request_id = "request-timeout-completed"
                    task.workload_physical_resource_id = "raptor"
                    await queue._persist(task)
                    entry = {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "timeout-completed.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        }
                    }

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "preempt":
                            return {
                                "status_string": "artifact_pending",
                                "outcome_string": "completed",
                            }
                        if kwargs["action"] == "complete":
                            return {"status": "Completed"}
                        return {"status": "Registered"}

                    host = AsyncMock(side_effect=host_control)
                    history = AsyncMock(return_value=("completed", entry))
                    download = AsyncMock(return_value=b"TIMEOUT-COMPLETED-ARTIFACT")
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value=None)
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter, "poll_history", new=history
                    ), patch.object(
                        comfy_adapter, "download_artifact", new=download
                    ):
                        await queue._poll_rendering()

                    restored = queue.get(task.id)
                    self.assertEqual(restored.status, TASK_DONE)
                    self.assertEqual(
                        Path(restored.output_path).read_bytes(),
                        b"TIMEOUT-COMPLETED-ARTIFACT",
                    )
                    self.assertTrue(restored.artifact_sha256)
                    self.assertEqual(restored.workload_lease_id, "")
                    self.assertNotIn(
                        "timeout-completed-prompt", restored.retired_comfy_prompt_ids
                    )
                    history.assert_awaited_once()
                    download.assert_awaited_once()
                    release.assert_awaited_once()
                    self.assertEqual(release.await_args.kwargs["outcome"], "completed")
                    self.assertEqual(
                        [call.kwargs["action"] for call in host.await_args_list],
                        ["heartbeat", "preempt", "complete"],
                    )
                finally:
                    await queue.stop()

        run(scenario())

    def test_managed_timeout_ambiguous_preempt_retains_central_lease(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="portrait"))
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "timeout-ambiguous-prompt"
                    task.started_at = 1.0
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-timeout-ambiguous"
                    task.workload_request_id = "request-timeout-ambiguous"
                    task.workload_physical_resource_id = "raptor"
                    await queue._persist(task)

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "preempt":
                            return {"status_string": "preempting"}
                        return {"status": "Registered"}

                    host = AsyncMock(side_effect=host_control)
                    history = AsyncMock(
                        side_effect=AssertionError("ambiguous prompt must not be polled")
                    )
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value=None)
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter, "poll_history", new=history
                    ):
                        await queue._poll_rendering()

                    restored = queue.get(task.id)
                    self.assertEqual(restored.status, TASK_RENDERING)
                    self.assertEqual(restored.error, "")
                    self.assertEqual(restored.finished_at, 0)
                    self.assertEqual(restored.server_name, server.render_server_name)
                    self.assertEqual(
                        restored.comfy_prompt_id, "timeout-ambiguous-prompt"
                    )
                    self.assertEqual(
                        restored.workload_lease_id, "lease-timeout-ambiguous"
                    )
                    self.assertEqual(
                        restored.workload_request_id, "request-timeout-ambiguous"
                    )
                    self.assertTrue(restored.host_comfy_registered)
                    self.assertNotIn(
                        "timeout-ambiguous-prompt", restored.retired_comfy_prompt_ids
                    )
                    history.assert_not_awaited()
                    release.assert_not_awaited()
                    self.assertEqual(
                        [call.kwargs["action"] for call in host.await_args_list],
                        ["heartbeat", "preempt"],
                    )
                finally:
                    await queue.stop()

        run(scenario())

    def test_central_heartbeat_surfaces_terminal_lease_identity(self):
        async def scenario():
            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    409,
                    json={
                        "status_string": "lease_terminal",
                        "lease_by_key": {
                            "lease_id_string": "lease-expired",
                            "state_string": "expired",
                        },
                    },
                )

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_WORKLOAD_BROKER_TOKEN": "broker-token",
                },
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(
                        workload_lease.WorkloadLeaseTerminal
                    ) as raised:
                        await workload_lease.heartbeat(
                            client,
                            lease_id="lease-expired",
                            owner_task_id="task-1",
                            request_id="request-1",
                        )
            self.assertEqual(raised.exception.lease_state, "expired")
            self.assertEqual(
                raised.exception.payload["lease_by_key"]["lease_id_string"],
                "lease-expired",
            )

        run(scenario())

    def test_managed_register_423_returns_same_task_pending_without_attempt(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.managed_prompt = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    task.workload_physical_resource_id = "raptor"
                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(
                            side_effect=workload_lease.WorkloadCapacityWait(
                                "gpu_busy", 2
                            )
                        ),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        with self.assertRaises(workload_lease.WorkloadCapacityWait):
                            await queue._submit_task(task, server)
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.started_at, 0)
                    self.assertEqual(task.submit_failures, 0)
                    self.assertFalse(task.workload_lease_id)
                    self.assertNotEqual(task.workload_request_id, "request-1")
                    self.assertTrue(task.retired_comfy_prompt_ids)
                finally:
                    await queue.stop()

        run(scenario())

    def test_managed_prompt_423_exactly_cleans_host_then_returns_pending(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.managed_prompt = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    task.workload_physical_resource_id = "raptor"

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "register":
                            return {"status_string": "registered"}
                        return {"status_string": "preempted"}

                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(side_effect=host_control),
                    ), patch.object(
                        comfy_adapter,
                        "submit",
                        new=AsyncMock(
                            side_effect=comfy_adapter.ComfyCapacityWait("gpu leased")
                        ),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        with self.assertRaises(comfy_adapter.ComfyCapacityWait):
                            await queue._submit_task(task, server)
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.started_at, 0)
                    self.assertEqual(task.submit_failures, 0)
                    self.assertFalse(task.workload_lease_id)
                    self.assertFalse(task.host_comfy_registered)
                    self.assertTrue(task.retired_comfy_prompt_ids)
                finally:
                    await queue.stop()

        run(scenario())

    def test_unknown_register_result_retries_same_id_without_deadline(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.managed_prompt = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    task.workload_physical_resource_id = "raptor"
                    register = AsyncMock(side_effect=RuntimeError("response lost"))
                    with patch.object(
                        workload_lease, "host_comfy_control", new=register
                    ):
                        with self.assertRaises(Exception):
                            await queue._submit_task(task, server)
                    prompt_id = task.comfy_prompt_id
                    self.assertTrue(prompt_id)
                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.started_at, 0)
                    self.assertFalse(task.host_comfy_registered)

                    async def recovered_control(*_args, **kwargs):
                        return {"status": "registered"}

                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(side_effect=recovered_control),
                    ), patch.object(
                        comfy_adapter,
                        "submit",
                        new=AsyncMock(return_value=prompt_id),
                    ):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.comfy_prompt_id, prompt_id)
                    self.assertTrue(task.host_comfy_registered)
                    self.assertGreater(task.started_at, 0)
                    self.assertEqual(task.submit_failures, 0)
                finally:
                    await queue.stop()

        run(scenario())

    def test_lost_register_then_host_ttl_preempt_requeues_attempt_neutrally(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    task.managed_prompt = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    task.workload_physical_resource_id = "raptor"
                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(side_effect=RuntimeError("response lost")),
                    ):
                        with self.assertRaises(Exception):
                            await queue._submit_task(task, server)
                    old_prompt_id = task.comfy_prompt_id
                    old_request_id = task.workload_request_id

                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(return_value={"status": "Preempted"}),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.started_at, 0)
                    self.assertEqual(task.submit_failures, 0)
                    self.assertFalse(task.workload_lease_id)
                    self.assertNotEqual(task.workload_request_id, old_request_id)
                    self.assertIn(old_prompt_id, task.retired_comfy_prompt_ids)
                finally:
                    await queue.stop()

        run(scenario())

    def test_resurrect_missing_managed_server_preserves_exact_binding(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                task.status = TASK_RENDERING
                task.server_name = "missing-farm-node"
                task.comfy_prompt_id = "managed-prompt"
                task.started_at = time.time()
                task.managed_prompt = True
                task.host_comfy_registered = True
                task.workload_lease_id = "lease-1"
                task.workload_request_id = "request-1"
                task.workload_physical_resource_id = "missing-gpu"
                await queue._persist(task)
                await queue.stop()

                queue2 = RenderQueue(ServerRegistry(), db_path=config.DB_PATH)
                await queue2.start()
                queue2._pump_task.cancel()
                try:
                    restored = queue2.get(task.id)
                    self.assertEqual(restored.status, TASK_RENDERING)
                    self.assertEqual(restored.server_name, "missing-farm-node")
                    self.assertEqual(restored.comfy_prompt_id, "managed-prompt")
                    self.assertEqual(restored.workload_lease_id, "lease-1")
                    await queue2._poll_rendering()
                    self.assertEqual(restored.status, TASK_RENDERING)
                    self.assertEqual(restored.comfy_prompt_id, "managed-prompt")
                finally:
                    await queue2.stop()

        run(scenario())

    def test_artifact_download_gpu_lease_is_capacity_wait(self):
        async def scenario():
            server = _server()

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    423, json={"error": "gpu_leased", "retryable": True}
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                with self.assertRaises(comfy_adapter.ComfyCapacityWait):
                    await comfy_adapter.download_artifact(
                        client,
                        server,
                        {"filename": "done.png", "subfolder": "", "type": "output"},
                    )

        run(scenario())

    def test_artifact_gpu_lease_keeps_completed_prompt_bound_for_retry(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "completed-prompt"
                    task.started_at = time.time()
                    await queue._persist(task)

                    with patch.object(
                        queue,
                        "_finish",
                        side_effect=comfy_adapter.ComfyCapacityWait("gpu leased"),
                    ):
                        await queue._finish_guarded(task, server, {})

                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.server_name, server.render_server_name)
                    self.assertEqual(task.comfy_prompt_id, "completed-prompt")
                    self.assertEqual(task.error, "")
                    persisted = queue.get(task.id)
                    self.assertEqual(persisted.status, TASK_RENDERING)
                    self.assertEqual(persisted.comfy_prompt_id, "completed-prompt")
                finally:
                    await queue.stop()

        run(scenario())

    def test_submit_failure_cooldown_rotates_to_a_healthy_peer(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server("raptor"))
                registry.save(_server("f5"))
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    queue._server_submit_cooldowns["raptor"] = time.time() + 600
                    chosen = queue._pick_server(
                        "gen_image.json", {"raptor": 0, "f5": 0}
                    )
                    self.assertIsNotNone(chosen)
                    self.assertEqual(chosen.render_server_name, "f5")

                    queue._server_submit_cooldowns["raptor"] = time.time() - 1
                    chosen = queue._pick_server(
                        "gen_image.json", {"raptor": 0, "f5": 1}
                    )
                    self.assertIsNotNone(chosen)
                    self.assertEqual(chosen.render_server_name, "raptor")
                finally:
                    await queue.stop()

        run(scenario())

    def test_gpu_lease_race_keeps_render_pending_without_submit_failure(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(RenderPrompt(prompt="a", type="t_pose"))
                    with patch.object(queue, "_queue_depths", return_value={"raptor": 0}), \
                         patch.object(
                             queue,
                             "_submit_task",
                             side_effect=comfy_adapter.ComfyCapacityWait("gpu leased"),
                         ):
                        dispatched = await queue._dispatch_one()
                    self.assertFalse(dispatched)
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.submit_failures, 0)
                    self.assertEqual(registry.get("raptor").status, "offline")
                finally:
                    await queue.stop()

        run(scenario())

    def test_one_in_flight_per_server_and_token_rule(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    submitted = []

                    async def fake_submit(task, server):
                        submitted.append((task.id, server.render_server_name))
                        task.server_name = server.render_server_name
                        task.status = TASK_RENDERING
                        task.started_at = 1e18  # never times out in this test
                        await queue._persist(task)

                    t1 = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    t2 = await queue.enqueue(
                        RenderPrompt(prompt="b", type="t_pose", image_url="https://h/m.jpg")
                    )
                    t3 = await queue.enqueue(
                        RenderPrompt(type="image_to_3d", image_url="https://h/i.png")
                    )
                    self.assertEqual(t1.workflow, "gen_image.json")
                    self.assertEqual(t3.workflow, "image_to_3d.json")

                    with patch.object(queue, "_submit_task", side_effect=fake_submit):
                        with patch.object(queue, "_refresh_servers"):
                            with patch.object(queue, "_poll_rendering"):
                                await queue.tick()
                                await queue.tick()
                                await queue.tick()

                    # only one server slot -> only the first t_pose dispatched;
                    # image_to_3d never dispatches (no server advertises it)
                    self.assertEqual([s[0] for s in submitted], [t1.id])
                    self.assertEqual(queue.get(t2.id).status, TASK_PENDING)
                    self.assertEqual(queue.get(t3.id).status, TASK_PENDING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_timeout_marks_error_and_frees_server(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.started_at = 1.0  # long ago
                    await queue._persist(task)

                    await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertIn("timeout", task.error)
                    self.assertEqual(queue._busy_servers(), {})
                finally:
                    await queue.stop()

        run(scenario())

    def test_restart_resurrects_rendering_as_pending(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                task = await queue.enqueue(
                    RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                )
                task.status = TASK_RENDERING
                task.server_name = "raptor"
                task.comfy_prompt_id = "p1"
                await queue._persist(task)
                await queue.stop()

                queue2 = RenderQueue(registry, db_path=config.DB_PATH)
                await queue2.start()
                try:
                    revived = queue2.get(task.id)
                    self.assertIsNotNone(revived)
                    self.assertEqual(revived.status, TASK_PENDING)
                    self.assertEqual(revived.server_name, "")
                finally:
                    await queue2.stop()

        run(scenario())

    def test_finish_saves_primary_and_isolated(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(
                            prompt="a", type="t_pose",
                            image_url="https://h/m.jpg", user_name="bot",
                        )
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.started_at = 100.0

                    entry = {
                        "outputs": {
                            "9": {"images": [
                                {"filename": f"{task.id}_00001_.png", "subfolder": "", "type": "output"}
                            ]},
                            "301": {"images": [
                                {"filename": f"{task.id}_Isolated_00001_.png", "subfolder": "", "type": "output"}
                            ]},
                        }
                    }

                    async def fake_download(client, srv, artifact):
                        return b"PNGDATA-" + artifact["filename"].encode()

                    with patch(
                        "renderfin.comfy_adapter.download_artifact", side_effect=fake_download
                    ):
                        await queue._finish(task, server, entry)

                    self.assertEqual(task.status, TASK_DONE)
                    out = Path(task.output_path)
                    self.assertTrue(out.is_file())
                    self.assertIn(f"{task.id}_00001_", out.read_bytes().decode())
                    self.assertIn("isolated", task.extra_outputs)
                    iso = config.RENDER_DIR / "bot" / f"{task.id}_Isolated.png"
                    self.assertTrue(iso.is_file())
                    self.assertIn("_Isolated_", iso.read_bytes().decode())
                finally:
                    await queue.stop()

        run(scenario())


if __name__ == "__main__":
    unittest.main()


class LostPromptTests(unittest.TestCase):
    """ComfyUI answers /history for a forgotten prompt with HTTP 200 and {} -
    identical to 'queued but not started'. Waiting that out held the only
    worker hostage for the whole timeout."""

    def test_prompt_missing_from_history_and_queue_is_requeued(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.comfy_prompt_id = "gone-prompt"
                    task.started_at = time.time()

                    async def unknown(*a, **k):
                        return "unknown", None

                    async def not_queued(*a, **k):
                        return False

                    with patch("renderfin.comfy_adapter.poll_history", side_effect=unknown):
                        with patch("renderfin.comfy_adapter.queue_contains", side_effect=not_queued):
                            await queue._poll_rendering()

                    revived = queue.get(task.id)
                    self.assertEqual(revived.status, TASK_PENDING)
                    self.assertEqual(revived.server_name, "")
                    self.assertEqual(revived.comfy_prompt_id, "")
                    # the server is free again immediately
                    self.assertEqual(queue._busy_servers(), {})
                finally:
                    await queue.stop()

        run(scenario())

    def test_prompt_still_queued_upstream_keeps_waiting(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.comfy_prompt_id = "waiting-prompt"
                    task.started_at = time.time()

                    async def unknown(*a, **k):
                        return "unknown", None

                    async def queued(*a, **k):
                        return True

                    with patch("renderfin.comfy_adapter.poll_history", side_effect=unknown):
                        with patch("renderfin.comfy_adapter.queue_contains", side_effect=queued):
                            await queue._poll_rendering()

                    still = queue.get(task.id)
                    self.assertEqual(still.status, TASK_RENDERING)
                    self.assertEqual(still.comfy_prompt_id, "waiting-prompt")
                finally:
                    await queue.stop()

        run(scenario())


class StageTimeoutOrderingTests(unittest.TestCase):
    def test_stage_ceiling_exceeds_the_queue_ceiling(self):
        """A stage that gives up before the queue does abandons a task that is
        still holding a worker."""
        from renderfin import character_gen

        self.assertGreater(character_gen.FLUX_STAGE_TIMEOUT, config.TASK_TIMEOUT_SECONDS)
        self.assertGreater(character_gen.HUNYUAN_STAGE_TIMEOUT, config.HUNYUAN_TIMEOUT_SECONDS)


class ServerChoiceTests(unittest.TestCase):
    """The box that finishes first is the emptiest one, not the fastest one.

    These ComfyUI machines also serve renderfin.com, so their backlog is
    invisible to our own dispatch records. A t_pose render was handed to a box
    with fifteen queued prompts while another sat completely idle, and the job
    it belonged to spent all three attempts on render timeouts.
    """

    def _queue(self, registry):
        return RenderQueue(registry, db_path=config.DB_PATH)

    def test_an_idle_box_beats_a_faster_box_with_a_backlog(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                fast = _server("f15")
                fast.average_render_time = 10.0
                idle = _server("raptor")
                idle.average_render_time = 90.0
                registry.save(fast)
                registry.save(idle)
                queue = self._queue(registry)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    picked = queue._pick_server(
                        "gen_image.json", {"f15": 16, "raptor": 0}
                    )
                    self.assertEqual(picked.render_server_name, "raptor")
                finally:
                    await queue.stop()

        run(scenario())

    def test_speed_still_decides_between_equally_loaded_boxes(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                fast = _server("f15")
                fast.average_render_time = 10.0
                slow = _server("raptor")
                slow.average_render_time = 90.0
                registry.save(fast)
                registry.save(slow)
                queue = self._queue(registry)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    picked = queue._pick_server("gen_image.json", {"f15": 2, "raptor": 2})
                    self.assertEqual(picked.render_server_name, "f15")
                finally:
                    await queue.stop()

        run(scenario())

    def test_a_box_we_cannot_ask_is_not_treated_as_empty(self):
        """Otherwise a probe failure makes the worst box look like the best."""
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                unknown = _server("f15")
                unknown.average_render_time = 10.0
                known = _server("raptor")
                known.average_render_time = 90.0
                registry.save(unknown)
                registry.save(known)
                queue = self._queue(registry)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    # f15 missing from depths entirely: unreachable, not idle
                    picked = queue._pick_server("gen_image.json", {"raptor": 3})
                    self.assertEqual(picked.render_server_name, "raptor")
                finally:
                    await queue.stop()

        run(scenario())


class RenderClockTests(unittest.TestCase):
    """A render's deadline must survive a service restart.

    Refreshing it hands a stuck render a fresh window every restart, so the
    timeout never fires: the render keeps its box marked busy, and the
    character_gen job re-attaches to it - its status is Rendering, not Error -
    and spends an attempt per window until the job dies. That killed a job on
    2026-08-03 after three restarts in an afternoon.
    """

    def test_a_surviving_render_keeps_its_original_deadline(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = self._q(registry)
                await queue.start()
                queue._pump_task.cancel()
                started = time.time() - 3600  # an hour in already
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="x", type="t_pose", user_name="u")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.comfy_prompt_id = "p-1"
                    task.started_at = started
                    await queue._persist(task)
                    await queue.stop()

                    revived = self._q(registry)
                    await revived.start()
                    revived._pump_task.cancel()
                    try:
                        again = revived.get(task.id)
                        self.assertEqual(again.status, TASK_RENDERING)
                        self.assertAlmostEqual(again.started_at, started, delta=2)
                    finally:
                        await revived.stop()
                except Exception:
                    await queue.stop()
                    raise

        run(scenario())

    def test_a_render_with_no_clock_gets_one(self):
        """started_at 0 would otherwise read as 'running since 1970' and fail."""
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = self._q(registry)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="x", type="t_pose", user_name="u")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.comfy_prompt_id = "p-2"
                    task.started_at = 0
                    await queue._persist(task)
                    await queue.stop()

                    revived = self._q(registry)
                    await revived.start()
                    revived._pump_task.cancel()
                    try:
                        again = revived.get(task.id)
                        self.assertGreater(again.started_at, time.time() - 60)
                    finally:
                        await revived.stop()
                except Exception:
                    await queue.stop()
                    raise

        run(scenario())

    def _q(self, registry):
        return RenderQueue(registry, db_path=config.DB_PATH)
