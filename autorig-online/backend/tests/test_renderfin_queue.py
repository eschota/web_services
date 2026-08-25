import asyncio
import hashlib
import os
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
from renderfin.queue import (
    ManagedComfyCleanupPending,
    RenderQueue,
    _host_terminal_outcome,
)
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


class _CentralToken:
    value = "central-control-token-1234567890"

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.path = root / "renderfin-central-token"
        self.ca_path = root / "central-control-ca.pem"
        self.cert_path = root / "central-control-client.pem"
        self.key_path = root / "central-control-client.key"

    def __enter__(self):
        self.path.write_text(self.value, encoding="utf-8")
        self.ca_path.write_text("TEST-CA", encoding="utf-8")
        self.cert_path.write_text("TEST-CLIENT-CERT", encoding="utf-8")
        self.key_path.write_text("TEST-CLIENT-KEY", encoding="utf-8")
        for path in (self.path, self.key_path):
            path.chmod(0o400)
        for path in (self.ca_path, self.cert_path):
            path.chmod(0o444)
        return self

    def __exit__(self, *exc):
        self.tmp.cleanup()

    def fields(self):
        return {
            "arbiter_control_url_string": "https://127.0.0.1:15200",
            "central_token_file_string": str(self.path),
            "central_tls_ca_file_string": str(self.ca_path),
            "central_tls_client_cert_file_string": str(self.cert_path),
            "central_tls_client_key_file_string": str(self.key_path),
        }


def _central_fields(testcase):
    protected = _CentralToken()
    protected.__enter__()
    testcase.addCleanup(protected.__exit__)
    return protected.fields()


def _token_file_fields(testcase, value, mode=0o400):
    root = tempfile.TemporaryDirectory()
    testcase.addCleanup(root.cleanup)
    path = Path(root.name) / "central-token"
    path.write_text(value, encoding="utf-8")
    path.chmod(mode)
    return {
        "arbiter_control_url_string": "http://127.0.0.1:15199",
        "central_token_file_string": str(path),
    }


def _server(name="raptor", workflows=("gen_image.json",)):
    return RenderServer(
        render_server_name=name,
        render_server_url="http://5.129.157.224:8288",
        status="online",
        available_workflows=list(workflows),
    )


def _terminal_receipt(task, outcome="Completed", **values):
    return {
        "status": outcome,
        "prompt_id": task.comfy_prompt_id,
        "logical_task_id": task.id,
        "central_lease_id": task.workload_lease_id,
        "request_id": task.workload_request_id,
        **values,
    }


def _spool_stage_receipt(task, data=b"FULL-ARTIFACT"):
    return {
        "status": "artifact_spooled",
        "status_string": "artifact_spooled",
        "artifact_spool_ready_bool": True,
        "artifact_spool_protocol_string": (
            workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
        ),
        "artifact_cpu_spool_persisted_bool": True,
        "artifact_checksum_persisted_bool": True,
        "gpu_detached_bool": True,
        "artifact_sha256": hashlib.sha256(data).hexdigest(),
        "artifact_size_int": len(data),
        "prompt_id": task.comfy_prompt_id,
        "logical_task_id": task.id,
        "lease_id": task.workload_lease_id,
        "request_id": task.workload_request_id,
    }


def _spool_ack_receipt(task):
    return _terminal_receipt(
        task,
        "Completed",
        status_string="Completed",
        artifact_spool_protocol_string=(
            workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
        ),
        artifact_sha256=task.artifact_sha256,
        artifact_size_int=task.managed_comfy_artifact_size_int,
        artifact_ack_tombstone_bool=True,
        central_persisted_bool=True,
        central_persistence_receipt_id_string=(
            task.managed_comfy_central_persistence_receipt_id_string
        ),
    )


class QueueDispatchTests(unittest.TestCase):
    def test_renderfin_broker_feature_flag_defaults_off(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(workload_lease.enabled())

    def test_renderfin_broker_uses_scoped_token_and_rejects_legacy_fallback(self):
        with patch.dict(
            "os.environ",
            {"AUTORIG_WORKLOAD_BROKER_TOKEN": "legacy-token-1234567890"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "not configured"):
                workload_lease._headers()
        with patch.dict(
            "os.environ",
            {
                "AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN": (
                    "renderfin-token-1234567890"
                )
            },
            clear=True,
        ):
            self.assertEqual(
                workload_lease._headers(),
                {"Authorization": "Bearer renderfin-token-1234567890"},
            )

    def test_central_control_transport_is_mtls_no_proxy_no_redirect(self):
        calls = {}

        class FakeTlsContext:
            minimum_version = None
            check_hostname = False
            verify_mode = None

            def load_cert_chain(self, *, certfile, keyfile):
                calls["certfile"] = certfile
                calls["keyfile"] = keyfile

        class FakeClient:
            pass

        context = FakeTlsContext()

        def fake_client(**kwargs):
            calls["client_kwargs"] = kwargs
            return FakeClient()

        control_config = {
            "ca_file_string": "R:/secrets/ca.pem",
            "client_cert_file_string": "R:/secrets/client.pem",
            "client_key_file_string": "R:/secrets/client.key",
        }
        with patch.object(
            workload_lease.ssl,
            "create_default_context",
            return_value=context,
        ) as create_context, patch.object(
            workload_lease.httpx,
            "AsyncClient",
            side_effect=fake_client,
        ):
            client, owned = workload_lease._central_control_http_client(
                None, control_config
            )

        self.assertIsInstance(client, FakeClient)
        self.assertTrue(owned)
        create_context.assert_called_once_with(cafile="R:/secrets/ca.pem")
        self.assertEqual(calls["certfile"], "R:/secrets/client.pem")
        self.assertEqual(calls["keyfile"], "R:/secrets/client.key")
        self.assertIs(calls["client_kwargs"]["verify"], context)
        self.assertIs(calls["client_kwargs"]["trust_env"], False)
        self.assertIs(calls["client_kwargs"]["follow_redirects"], False)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, workload_lease.ssl.CERT_REQUIRED)

    def test_register_sends_canonical_submission_binding_and_requires_exact_receipt(self):
        async def scenario():
            server = _server()
            server.managed_workload = True
            expected_sha256 = "b" * 64

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    str(request.url),
                    "https://127.0.0.1:15200/comfy/register",
                )
                body = json.loads(request.content)
                self.assertEqual(
                    body["expected_canonical_submission_sha256"],
                    expected_sha256,
                )
                return httpx.Response(
                    200,
                    json={
                        "status": "registered",
                        "status_string": "registered",
                        "prompt_id": "prompt-1",
                        "logical_task_id": "task-1",
                        "central_lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "token": "bridge-token",
                        **_central_fields(self),
                    }
                }
            )
            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    return await workload_lease.host_comfy_control(
                        client,
                        server=server,
                        action="register",
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                        expected_canonical_submission_sha256=expected_sha256,
                    )

        self.assertEqual(run(scenario())["status"], "registered")

        workflow = {"1": {"class_type": "Example", "inputs": {"x": 1}}}
        identity = {
            "logical_task_id": "task-1",
            "lease_id": "lease-1",
            "request_id": "request-1",
        }
        body, _headers = comfy_adapter.managed_submission_payload(
            workflow,
            managed_identity=identity,
            prompt_id="prompt-1",
        )
        expected = hashlib.sha256(
            json.dumps(
                body,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(
            comfy_adapter.managed_submission_sha256(
                workflow,
                managed_identity=identity,
                prompt_id="prompt-1",
            ),
            expected,
        )
        self.assertNotEqual(
            comfy_adapter.managed_submission_sha256(
                {"1": {"class_type": "Example", "inputs": {"x": 2}}},
                managed_identity=identity,
                prompt_id="prompt-1",
            ),
            expected,
        )

    def test_exact_host_spool_stage_get_ack_wire_and_central_fsync_copy(self):
        async def scenario():
            server = _server()
            server.managed_workload = True
            artifact = b"EXACT-HOST-SPOOL-ARTIFACT"
            checksum = hashlib.sha256(artifact).hexdigest()
            receipt = "renderfin_bundle_v1_" + "a" * 64
            seen = []

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    request.headers["Authorization"],
                    "Bearer central-control-token-1234567890",
                )
                if request.url.path.endswith("/comfy/stage"):
                    seen.append("stage")
                    body = json.loads(request.content)
                    self.assertEqual(
                        set(body),
                        {
                            "prompt_id",
                            "logical_task_id",
                            "lease_id",
                            "request_id",
                            "artifact_relative_path_string",
                        },
                    )
                    self.assertEqual(
                        body["artifact_relative_path_string"], "render/full.png"
                    )
                    return httpx.Response(
                        200,
                        json={
                            "status": "artifact_spooled",
                            "status_string": "artifact_spooled",
                            "artifact_spool_ready_bool": True,
                            "artifact_spool_protocol_string": (
                                workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                            ),
                            "artifact_cpu_spool_persisted_bool": True,
                            "artifact_checksum_persisted_bool": True,
                            "gpu_detached_bool": True,
                            "artifact_sha256": checksum,
                            "artifact_size_int": len(artifact),
                            "prompt_id": "prompt-1",
                            "logical_task_id": "task-1",
                            "lease_id": "lease-1",
                            "request_id": "request-1",
                        },
                    )
                if request.url.path.endswith("/comfy/artifact"):
                    seen.append("get")
                    self.assertEqual(
                        dict(request.url.params),
                        {
                            "prompt_id": "prompt-1",
                            "logical_task_id": "task-1",
                            "lease_id": "lease-1",
                            "request_id": "request-1",
                        },
                    )
                    return httpx.Response(
                        200,
                        content=artifact,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "X-AutoRig-Artifact-SHA256": checksum,
                            "X-AutoRig-Artifact-Size": str(len(artifact)),
                            "X-AutoRig-Artifact-Protocol": (
                                workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                            ),
                        },
                    )
                if request.url.path.endswith("/comfy/ack"):
                    seen.append("ack")
                    body = json.loads(request.content)
                    self.assertEqual(
                        body,
                        {
                            "prompt_id": "prompt-1",
                            "logical_task_id": "task-1",
                            "lease_id": "lease-1",
                            "request_id": "request-1",
                            "artifact_sha256": checksum,
                            "artifact_size_int": len(artifact),
                            "central_persisted_bool": True,
                            "central_persistence_receipt_id_string": receipt,
                        },
                    )
                    return httpx.Response(
                        200,
                        json={
                            "status": "Completed",
                            "status_string": "Completed",
                            "artifact_spool_protocol_string": (
                                workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                            ),
                            "artifact_sha256": checksum,
                            "artifact_size_int": len(artifact),
                            "artifact_ack_tombstone_bool": True,
                            "central_persisted_bool": True,
                            "central_persistence_receipt_id_string": receipt,
                            "prompt_id": "prompt-1",
                            "logical_task_id": "task-1",
                            "lease_id": "lease-1",
                            "request_id": "request-1",
                        },
                    )
                return httpx.Response(404)

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
                    }
                }
            )
            with tempfile.TemporaryDirectory() as tmp, patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                destination = Path(tmp) / "central" / "artifact.png"
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    staged = await workload_lease.host_comfy_stage_artifact(
                        client,
                        server=server,
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                        artifact_relative_path_string="render/full.png",
                    )
                    await workload_lease.host_comfy_download_artifact(
                        client,
                        server=server,
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                        destination_path=destination,
                        expected_sha256=staged["artifact_sha256"],
                        expected_size_int=staged["artifact_size_int"],
                    )
                    acknowledged = await workload_lease.host_comfy_ack_artifact(
                        client,
                        server=server,
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                        artifact_sha256=checksum,
                        artifact_size_int=len(artifact),
                        central_persistence_receipt_id_string=receipt,
                    )
                self.assertEqual(destination.read_bytes(), artifact)
                self.assertEqual(acknowledged["status"], "Completed")
                self.assertEqual(seen, ["stage", "get", "ack"])

        run(scenario())

    def test_spool_terminal_receipts_require_exact_four_ids(self):
        async def scenario():
            server = _server()
            server.managed_workload = True

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "status": "Completed",
                        "prompt_id": "wrong-prompt",
                        "logical_task_id": "task-1",
                        "lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
                    }
                }
            )
            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(
                        workload_lease.HostComfyReceiptMismatch
                    ):
                        await workload_lease.host_comfy_stage_artifact(
                            client,
                            server=server,
                            prompt_id="prompt-1",
                            logical_task_id="task-1",
                            lease_id="lease-1",
                            request_id="request-1",
                            artifact_relative_path_string="full.png",
                        )

        run(scenario())

    def test_managed_tpose_spool_persists_isolated_before_stage_and_ack(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                server.managed_comfy_artifact_spool_required_bool = True
                server.managed_comfy_artifact_spool_ready_bool = True
                server.managed_comfy_artifact_spool_protocol_string = (
                    workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                )
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", user_name="bot")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "spool-prompt"
                    task.started_at = time.time() - 30
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-spool"
                    task.workload_request_id = "request-spool"
                    await queue._persist(task)
                    entry = {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "collection/FULL.png",
                                        "subfolder": "",
                                        "type": "output",
                                    },
                                    {
                                        "filename": "collection/FULL_Isolated_output.png",
                                        "subfolder": "",
                                        "type": "output",
                                    },
                                ]
                            }
                        }
                    }
                    order = []
                    full = b"FULL-ARTIFACT"

                    async def download_isolated(*_args, **_kwargs):
                        order.append("isolated")
                        return b"ISOLATED-ARTIFACT"

                    async def stage(*_args, **kwargs):
                        order.append("stage")
                        self.assertTrue(
                            workload_lease.verify_central_artifact(
                                Path(task.managed_comfy_isolated_output_path),
                                expected_sha256=task.managed_comfy_isolated_sha256,
                                expected_size=task.managed_comfy_isolated_size_int,
                            )
                        )
                        self.assertEqual(
                            kwargs["artifact_relative_path_string"],
                            "collection/FULL.png",
                        )
                        return _spool_stage_receipt(task, full)

                    async def get_artifact(*_args, **kwargs):
                        order.append("get")
                        destination = Path(kwargs["destination_path"])
                        destination.write_bytes(full)
                        return {
                            "artifact_sha256": hashlib.sha256(full).hexdigest(),
                            "artifact_size_int": len(full),
                        }

                    async def ack(*_args, **kwargs):
                        order.append("ack")
                        self.assertTrue(queue._managed_bundle_is_durable(task))
                        async with queue._db.execute(
                            "SELECT payload FROM render_tasks WHERE id = ?",
                            (task.id,),
                        ) as cursor:
                            payload = json.loads((await cursor.fetchone())[0])
                        self.assertEqual(
                            payload["managed_comfy_artifact_spool_state"],
                            "central_persisted",
                        )
                        self.assertEqual(
                            kwargs["central_persistence_receipt_id_string"],
                            payload[
                                "managed_comfy_central_persistence_receipt_id_string"
                            ],
                        )
                        return _spool_ack_receipt(task)

                    with patch.object(
                        comfy_adapter,
                        "download_artifact",
                        new=AsyncMock(side_effect=download_isolated),
                    ), patch.object(
                        workload_lease,
                        "host_comfy_stage_artifact",
                        new=AsyncMock(side_effect=stage),
                    ), patch.object(
                        workload_lease,
                        "host_comfy_download_artifact",
                        new=AsyncMock(side_effect=get_artifact),
                    ), patch.object(
                        workload_lease,
                        "host_comfy_ack_artifact",
                        new=AsyncMock(side_effect=ack),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        await queue._finish(task, server, entry)

                    self.assertEqual(order, ["isolated", "stage", "get", "ack"])
                    self.assertEqual(task.status, TASK_DONE)
                    self.assertEqual(
                        task.managed_comfy_artifact_spool_state, "acknowledged"
                    )
                    self.assertEqual(Path(task.output_path).read_bytes(), full)
                    self.assertIn("isolated", task.extra_outputs)
                    self.assertEqual(task.submit_failures, 0)
                finally:
                    await queue.stop()

        run(scenario())

    def test_managed_tpose_missing_isolated_keeps_binding_attempt_neutral(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                server.managed_comfy_artifact_spool_required_bool = True
                server.managed_comfy_artifact_spool_ready_bool = True
                server.managed_comfy_artifact_spool_protocol_string = (
                    workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                )
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", user_name="bot")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = server.render_server_name
                    task.comfy_prompt_id = "spool-prompt"
                    task.started_at = time.time() - 10
                    original_started_at = task.started_at
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-spool"
                    task.workload_request_id = "request-spool"
                    await queue._persist(task)
                    entry = {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "FULL.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        }
                    }
                    stage = AsyncMock(
                        side_effect=AssertionError("incomplete bundle staged")
                    )
                    with patch.object(
                        workload_lease,
                        "host_comfy_stage_artifact",
                        new=stage,
                    ):
                        await queue._finish_guarded(task, server, entry)
                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.started_at, original_started_at)
                    self.assertEqual(task.workload_lease_id, "lease-spool")
                    self.assertEqual(task.submit_failures, 0)
                    self.assertEqual(task.managed_comfy_artifact_spool_state, "")
                    stage.assert_not_awaited()
                finally:
                    await queue.stop()

        run(scenario())

    def test_required_spool_status_fails_closed_until_exact_v1_ready(self):
        async def scenario(protocol, ready):
            server = _server("f5")
            server.managed_workload = True
            machine = "machine_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            mapping = json.dumps(
                {
                    "f5": {
                        "url": "https://converter-f5.freestock.online",
                        "token": "bridge-token",
                        "physical_resource_id_string": machine,
                        "workload_role": "shared",
                    }
                }
            )

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "physical_node": machine,
                        "workload_role": "shared",
                        "gpu_arbiter_enabled": True,
                        "managed_comfy_central_control_ready_bool": True,
                        "accepting_ai_vision": True,
                        "managed_comfy_artifact_spool_required_bool": True,
                        "managed_comfy_artifact_spool_ready_bool": ready,
                        "managed_comfy_artifact_spool_protocol_string": protocol,
                    },
                )

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_GPU_CONTROL_NODES_JSON": mapping,
                },
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    accepted = await workload_lease.refresh_managed_identity(
                        client, server
                    )
            return accepted, server

        accepted, server = run(
            scenario(workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL, True)
        )
        self.assertTrue(accepted)
        self.assertTrue(server.managed_comfy_artifact_spool_required_bool)

        accepted, server = run(scenario("wrong-protocol", True))
        self.assertFalse(accepted)
        self.assertTrue(server.workload_identity_verified_bool)

        accepted, _server_state = run(
            scenario(workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL, False)
        )
        self.assertFalse(accepted)

    def test_comfy_ai_identity_uses_direct_mtls_arbiter_status_without_converter(self):
        async def scenario(central_ready):
            server = _server("f5")
            server.managed_workload = True
            machine = "machine_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            mapping = json.dumps(
                {
                    "f5": {
                        "capability_mode": "comfy_ai",
                        "token": "workload-token-1234567890",
                        "physical_resource_id_string": machine,
                        "workload_role": "ai_vision_primary",
                        **_central_fields(self),
                    }
                }
            )

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(str(request.url), "https://127.0.0.1:15200/status")
                self.assertEqual(
                    request.headers["Authorization"],
                    "Bearer central-control-token-1234567890",
                )
                return httpx.Response(
                    200,
                    json={
                        "physical_gpu_id": machine,
                        "workload_role": "ai_vision_primary",
                        "capability_mode": "comfy_ai",
                        "managed_comfy_central_control_ready_bool": central_ready,
                        "accepting_ai_vision": True,
                        "managed_comfy_artifact_spool_required_bool": True,
                        "managed_comfy_artifact_spool_ready_bool": True,
                        "managed_comfy_artifact_spool_protocol_string": (
                            workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                        ),
                    },
                )

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_GPU_CONTROL_NODES_JSON": mapping,
                },
                clear=False,
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    accepted = await workload_lease.refresh_managed_identity(
                        client, server
                    )
            return accepted, server

        accepted, server = run(scenario(True))
        self.assertTrue(accepted)
        self.assertTrue(server.workload_identity_verified_bool)
        self.assertTrue(server.managed_comfy_central_control_ready_bool)
        self.assertTrue(server.arbiter_online_bool)

        accepted, server = run(scenario(False))
        self.assertFalse(accepted)
        self.assertTrue(server.workload_identity_verified_bool)
        self.assertFalse(server.managed_comfy_central_control_ready_bool)
        self.assertFalse(server.arbiter_online_bool)

    def test_spool_ack_mismatch_is_attempt_neutral_and_restarts_at_ack(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                server.managed_comfy_artifact_spool_required_bool = True
                server.managed_comfy_artifact_spool_ready_bool = True
                server.managed_comfy_artifact_spool_protocol_string = (
                    workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                )
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
                    task.comfy_prompt_id = "spool-prompt"
                    task.started_at = time.time() - 20
                    original_started_at = task.started_at
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-spool"
                    task.workload_request_id = "request-spool"
                    await queue._persist(task)
                    full = b"FULL-ARTIFACT"
                    entry = {
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "FULL.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        }
                    }

                    async def get_artifact(*_args, **kwargs):
                        Path(kwargs["destination_path"]).write_bytes(full)
                        return {}

                    acknowledgements = AsyncMock(
                        side_effect=[
                            workload_lease.HostComfyReceiptMismatch(
                                "ack", {"prompt_id": "other"}
                            ),
                            None,
                        ]
                    )

                    async def ack_dispatch(*_args, **_kwargs):
                        result = await acknowledgements()
                        return result or _spool_ack_receipt(task)

                    stage = AsyncMock(return_value=_spool_stage_receipt(task, full))
                    get = AsyncMock(side_effect=get_artifact)
                    with patch.object(
                        workload_lease,
                        "host_comfy_stage_artifact",
                        new=stage,
                    ), patch.object(
                        workload_lease,
                        "host_comfy_download_artifact",
                        new=get,
                    ), patch.object(
                        workload_lease,
                        "host_comfy_ack_artifact",
                        new=AsyncMock(side_effect=ack_dispatch),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        await queue._finish_guarded(task, server, entry)
                        self.assertEqual(task.status, TASK_RENDERING)
                        self.assertEqual(
                            task.managed_comfy_artifact_spool_state,
                            "central_persisted",
                        )
                        self.assertEqual(task.workload_lease_id, "lease-spool")
                        self.assertEqual(task.started_at, original_started_at)
                        self.assertEqual(task.submit_failures, 0)
                        # Crash/restart after central persistence but before a
                        # trustworthy ACK response resumes at ACK only.
                        await queue.stop()
                        queue = RenderQueue(registry, db_path=config.DB_PATH)
                        await queue.start()
                        queue._pump_task.cancel()
                        task = queue.get(task.id)
                        self.assertEqual(
                            task.managed_comfy_artifact_spool_state,
                            "central_persisted",
                        )
                        await queue._poll_rendering()
                        await queue._finishers[task.id]

                    self.assertEqual(task.status, TASK_DONE)
                    self.assertEqual(stage.await_count, 1)
                    self.assertEqual(get.await_count, 1)
                    self.assertEqual(acknowledgements.await_count, 2)
                finally:
                    await queue.stop()

        run(scenario())

    def test_renderfin_f7_and_raptor_are_distinct_transport_aliases(self):
        self.assertEqual(workload_lease._safe_node("F7"), "f7")
        self.assertEqual(workload_lease._safe_node("FARM-F7"), "farm-f7")
        self.assertEqual(workload_lease._safe_node("RYZEN-SERVER"), "raptor")
        self.assertEqual(workload_lease._safe_node("Raptor-GPU0"), "raptor")
        self.assertLess(
            workload_lease.server_role_rank("ai_vision", "ai_vision_primary"),
            workload_lease.server_role_rank("ai_vision", "autorig_primary"),
        )
        self.assertLess(
            workload_lease.server_role_rank("comfy", "shared"),
            workload_lease.server_role_rank("comfy", "ai_vision_primary"),
        )

    def test_managed_identity_requires_authenticated_exact_machine_and_role(self):
        async def scenario(reported_physical, reported_role):
            server = _server("f5")
            server.managed_workload = True
            mapping = json.dumps(
                {
                    "f5": {
                        "url": "https://converter-f5.freestock.online",
                        "token": "bridge-token",
                        "physical_resource_id_string": (
                            "machine_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                        "workload_role": "ai_vision_primary",
                    }
                }
            )

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    str(request.url),
                    "https://converter-f5.freestock.online/api-converter-glb/server-status",
                )
                self.assertEqual(
                    request.headers["Authorization"], "Bearer bridge-token"
                )
                return httpx.Response(
                    200,
                    json={
                        "physical_node": reported_physical,
                        "workload_role": reported_role,
                        "gpu_arbiter_enabled": True,
                        "managed_comfy_central_control_ready_bool": True,
                        "accepting_ai_vision": True,
                    },
                )

            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_GPU_CONTROL_NODES_JSON": mapping,
                },
                clear=False,
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    verified = await workload_lease.refresh_managed_identity(
                        client, server
                    )
            return verified, server

        exact = "machine_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        verified, server = run(scenario(exact, "ai_vision_primary"))
        self.assertTrue(verified)
        self.assertTrue(server.workload_identity_verified_bool)
        self.assertEqual(workload_lease.server_identity(server)[1], exact)
        self.assertEqual(server.reserve_role_string, "ai_vision_primary")
        self.assertTrue(server.arbiter_online_bool)

        verified, mismatch = run(
            scenario(
                "machine_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "ai_vision_primary",
            )
        )
        self.assertFalse(verified)
        self.assertFalse(mismatch.workload_identity_verified_bool)
        self.assertEqual(workload_lease.server_identity(mismatch)[1], "")

        verified, mismatch = run(scenario(exact, "autorig_primary"))
        self.assertFalse(verified)
        self.assertFalse(mismatch.workload_identity_verified_bool)

    def test_managed_identity_config_without_exact_machine_is_fail_closed(self):
        async def scenario():
            server = _server("f5")
            server.managed_workload = True
            mapping = json.dumps(
                {
                    "f5": {
                        "url": "https://converter-f5.freestock.online",
                        "token": "bridge-token",
                        "workload_role": "shared",
                    }
                }
            )
            with patch.dict(
                "os.environ",
                {
                    "RENDERFIN_WORKLOAD_BROKER_ENABLED": "1",
                    "RENDERFIN_GPU_CONTROL_NODES_JSON": mapping,
                },
                clear=False,
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(
                        lambda _request: httpx.Response(500)
                    )
                ) as client:
                    return await workload_lease.refresh_managed_identity(
                        client, server
                    )

        self.assertFalse(run(scenario()))

    def test_real_host_bridge_terminal_status_aliases(self):
        self.assertEqual(_host_terminal_outcome({"status": "Completed"}), "completed")
        self.assertEqual(_host_terminal_outcome({"status": "Preempted"}), "preempted")
        self.assertEqual(_host_terminal_outcome({"status": "Released"}), "released")

    def test_host_bridge_409_completed_is_terminal_not_capacity(self):
        async def scenario():
            server = _server()
            server.managed_workload = True

            def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(
                    request.headers["Authorization"],
                    "Bearer central-control-token-1234567890",
                )
                return httpx.Response(
                    409,
                    json={
                        "status": "Completed",
                        "prompt_id": "prompt-1",
                        "logical_task_id": "task-1",
                        "central_lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
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

    def test_managed_comfy_never_falls_back_to_workload_bridge_credentials(self):
        async def scenario(entry):
            server = _server()
            server.managed_workload = True
            mapping = json.dumps({"raptor": entry})

            def unexpected_request(_request: httpx.Request) -> httpx.Response:
                raise AssertionError("managed Comfy attempted legacy/fallback transport")

            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(unexpected_request)
                ) as client:
                    with self.assertRaises(
                        workload_lease.WorkloadCapacityWait
                    ) as raised:
                        await workload_lease.host_comfy_control(
                            client,
                            server=server,
                            action="register",
                            prompt_id="prompt-1",
                            logical_task_id="task-1",
                            lease_id="lease-1",
                            request_id="request-1",
                            expected_canonical_submission_sha256="a" * 64,
                        )
            self.assertEqual(
                raised.exception.status, "host_comfy_control_not_configured"
            )

        invalid_entries = (
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "same-token-123456789012345",
                **_token_file_fields(
                    self, "same-token-123456789012345"
                ),
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
                **_central_fields(self),
                "arbiter_control_url_string": "https://converter-f7.freestock.online",
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
                **_central_fields(self),
                "arbiter_control_url_string": "http://0.0.0.0:15199",
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
                # Inline credentials are rejected even with a direct URL.
                "arbiter_control_url_string": "http://127.0.0.1:15199",
                "central_token_string": "central-token-123456789012",
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
                **_token_file_fields(
                    self, "central-token-123456789012", mode=0o644
                ),
            },
            {
                "url": "https://converter-f7.freestock.online",
                "token": "workload-token-1234567890",
                "arbiter_control_url_string": "http://127.0.0.1:15199",
                "central_token_file_string": "R:/definitely-missing-central-token",
            },
        )
        for entry in invalid_entries:
            with self.subTest(entry=entry):
                run(scenario(entry))

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
                        "central_lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
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

    def test_host_terminal_receipt_requires_exact_four_ids_for_every_action(self):
        async def scenario(action):
            server = _server()
            server.managed_workload = True

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "status": "Completed",
                        "prompt_id": "different-prompt",
                        "logical_task_id": "task-1",
                        "central_lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
                    }
                }
            )
            with patch.dict(
                "os.environ", {"RENDERFIN_GPU_CONTROL_NODES_JSON": mapping}
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with self.assertRaises(
                        workload_lease.HostComfyReceiptMismatch
                    ) as raised:
                        await workload_lease.host_comfy_control(
                            client,
                            server=server,
                            action=action,
                            prompt_id="prompt-1",
                            logical_task_id="task-1",
                            lease_id="lease-1",
                            request_id="request-1",
                            expected_canonical_submission_sha256=(
                                "a" * 64 if action == "register" else ""
                            ),
                        )
            self.assertEqual(
                raised.exception.status,
                f"host_comfy_{action}_receipt_mismatch",
            )
            self.assertEqual(raised.exception.retry_after, 2)

        for action in ("register", "heartbeat", "preempt", "complete"):
            with self.subTest(action=action):
                run(scenario(action))

    def test_completed_wins_with_exact_receipt_for_every_control_action(self):
        async def scenario(action):
            server = _server()
            server.managed_workload = True

            def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    json={
                        "status": "Completed",
                        "prompt_id": "prompt-1",
                        "logical_task_id": "task-1",
                        "central_lease_id": "lease-1",
                        "request_id": "request-1",
                    },
                )

            mapping = json.dumps(
                {
                    "raptor": {
                        "url": "https://converter-f7.freestock.online",
                        "token": "bridge-token",
                        **_central_fields(self),
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
                        action=action,
                        prompt_id="prompt-1",
                        logical_task_id="task-1",
                        lease_id="lease-1",
                        request_id="request-1",
                        expected_canonical_submission_sha256=(
                            "a" * 64 if action == "register" else ""
                        ),
                    )
            self.assertEqual(_host_terminal_outcome(result), "completed")

        for action in ("register", "heartbeat", "preempt", "complete"):
            with self.subTest(action=action):
                run(scenario(action))

    def test_mismatched_register_receipt_keeps_exact_binding_attempt_neutral(self):
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
                    submit = AsyncMock(
                        side_effect=AssertionError("mismatched terminal resubmitted")
                    )

                    def mismatched(*_args, **_kwargs):
                        return {
                            "status": "Completed",
                            "prompt_id": "some-other-prompt",
                            "logical_task_id": task.id,
                            "central_lease_id": task.workload_lease_id,
                            "request_id": task.workload_request_id,
                        }

                    with patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(side_effect=mismatched),
                    ), patch.object(comfy_adapter, "submit", new=submit):
                        with self.assertRaises(ManagedComfyCleanupPending):
                            await queue._submit_task(task, server)

                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.started_at, 0)
                    self.assertEqual(task.workload_lease_id, "lease-1")
                    self.assertEqual(task.workload_request_id, "request-1")
                    self.assertFalse(task.host_comfy_registered)
                    self.assertEqual(task.submit_failures, 0)
                    submit.assert_not_awaited()
                finally:
                    await queue.stop()

        run(scenario())

    def test_mismatched_heartbeat_receipt_is_retryable_noop(self):
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
                    task.comfy_prompt_id = "prompt-1"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    await queue._persist(task)
                    mismatched = _terminal_receipt(task, "Completed")
                    mismatched["request_id"] = "some-other-request"
                    release = AsyncMock(return_value=None)
                    history = AsyncMock(
                        side_effect=AssertionError("mismatched heartbeat was trusted")
                    )
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value={})
                    ), patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(return_value=mismatched),
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter, "poll_history", new=history
                    ):
                        await queue._poll_rendering()

                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.comfy_prompt_id, "prompt-1")
                    self.assertEqual(task.workload_lease_id, "lease-1")
                    self.assertEqual(task.workload_request_id, "request-1")
                    self.assertEqual(task.submit_failures, 0)
                    release.assert_not_awaited()
                    history.assert_not_awaited()
                finally:
                    await queue.stop()

        run(scenario())

    def test_mismatched_preempt_receipt_cannot_requeue_bound_prompt(self):
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
                    task.comfy_prompt_id = "prompt-1"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
                    await queue._persist(task)
                    mismatch = _terminal_receipt(task, "Preempted")
                    mismatch["central_lease_id"] = "some-other-lease"
                    release = AsyncMock(return_value=None)
                    terminal = workload_lease.WorkloadPreempted(
                        {"status_string": "preemption_requested"}
                    )
                    with patch.object(
                        workload_lease,
                        "heartbeat",
                        new=AsyncMock(side_effect=terminal),
                    ), patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(return_value=mismatch),
                    ), patch.object(
                        workload_lease, "release", new=release
                    ):
                        await queue._poll_rendering()

                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.comfy_prompt_id, "prompt-1")
                    self.assertEqual(task.workload_lease_id, "lease-1")
                    self.assertEqual(task.workload_request_id, "request-1")
                    self.assertNotIn("prompt-1", task.retired_comfy_prompt_ids)
                    release.assert_not_awaited()
                finally:
                    await queue.stop()

        run(scenario())

    def test_mismatched_complete_receipt_keeps_durable_done_lease_bound(self):
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
                    task.comfy_prompt_id = "prompt-1"
                    task.started_at = time.time()
                    task.managed_prompt = True
                    task.host_comfy_registered = True
                    task.workload_lease_id = "lease-1"
                    task.workload_request_id = "request-1"
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
                    mismatch = _terminal_receipt(task, "Completed")
                    mismatch["logical_task_id"] = "some-other-task"
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        comfy_adapter,
                        "download_artifact",
                        new=AsyncMock(return_value=b"DURABLE-ARTIFACT"),
                    ), patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(return_value=mismatch),
                    ), patch.object(
                        workload_lease, "release", new=release
                    ):
                        await queue._finish(
                            task, server, entry, skip_lease_heartbeat=True
                        )

                    self.assertEqual(task.status, TASK_DONE)
                    self.assertTrue(task.artifact_sha256)
                    self.assertEqual(task.workload_lease_id, "lease-1")
                    self.assertTrue(task.host_comfy_registered)
                    self.assertEqual(task.submit_failures, 0)
                    release.assert_not_awaited()
                finally:
                    await queue.stop()

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
                        return_value=_terminal_receipt(task, "Preempted")
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
                            return_value=_terminal_receipt(
                                task,
                                "Completed",
                                status_string="artifact_pending",
                                outcome_string="completed",
                            )
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
                            return _terminal_receipt(task, "Completed")
                        return _terminal_receipt(
                            task,
                            "Completed",
                            status_string="artifact_pending",
                            outcome_string="completed",
                        )

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
                                "prompt_id": task.comfy_prompt_id,
                                "logical_task_id": task.id,
                                "central_lease_id": task.workload_lease_id,
                                "request_id": task.workload_request_id,
                            }
                        if kwargs["action"] == "complete":
                            return _terminal_receipt(task, "Completed")
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
                            return {
                                "status_string": "registered",
                                "prompt_id": task.comfy_prompt_id,
                                "logical_task_id": task.id,
                                "central_lease_id": task.workload_lease_id,
                                "request_id": task.workload_request_id,
                            }
                        return _terminal_receipt(
                            task, "Preempted", status_string="preempted"
                        )

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
                        return {
                            "status": "registered",
                            "prompt_id": task.comfy_prompt_id,
                            "logical_task_id": task.id,
                            "central_lease_id": task.workload_lease_id,
                            "request_id": task.workload_request_id,
                        }

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
                        new=AsyncMock(
                            return_value=_terminal_receipt(task, "Preempted")
                        ),
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

    def _q(self, registry):
        return RenderQueue(registry, db_path=config.DB_PATH)

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


class ManagedComfyNoProgressWatchdogTests(unittest.TestCase):
    def test_default_managed_no_progress_ceiling_is_one_hour(self):
        self.assertEqual(
            config.MANAGED_COMFY_NO_PROGRESS_TIMEOUT_SECONDS,
            3600.0,
        )

    @staticmethod
    async def _managed_task(queue, server, *, started_at):
        task = await queue.enqueue(RenderPrompt(prompt="a", type="portrait"))
        task.status = TASK_RENDERING
        task.server_name = server.render_server_name
        task.comfy_prompt_id = "watchdog-prompt"
        task.started_at = started_at
        task.managed_prompt = True
        task.host_comfy_registered = True
        task.workload_lease_id = "watchdog-lease"
        task.workload_request_id = "watchdog-request"
        task.workload_physical_resource_id = "machine_aaaaaaaaaaaaaaaaaaaaaaaa"
        task.managed_comfy_progress_signature = json.dumps(
            {"state": "submitted", "stage": "", "marker": ""},
            sort_keys=True,
            separators=(",", ":"),
        )
        task.managed_comfy_last_progress_at = started_at
        await queue._persist(task)
        return task

    @staticmethod
    def _host_status(task, **values):
        return {
            "prompt_id": task.comfy_prompt_id,
            "logical_task_id": task.id,
            "central_lease_id": task.workload_lease_id,
            "request_id": task.workload_request_id,
            **values,
        }

    def test_one_hour_without_progress_exactly_requeues_same_task_retry_neutrally(self):
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
                    task = await self._managed_task(
                        queue, server, started_at=time.time() - 3605
                    )
                    original_id = task.id
                    original_request = task.workload_request_id

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "heartbeat":
                            return self._host_status(task, state="queued")
                        if kwargs["action"] == "preempt":
                            return self._host_status(task, state="Preempted")
                        raise AssertionError(kwargs["action"])

                    host = AsyncMock(side_effect=host_control)
                    release = AsyncMock(return_value=None)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value={})
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        workload_lease, "release", new=release
                    ), patch.object(
                        comfy_adapter,
                        "poll_history",
                        new=AsyncMock(side_effect=AssertionError("old prompt polled")),
                    ):
                        await queue._poll_rendering()

                    restored = queue.get(original_id)
                    self.assertIs(restored, task)
                    self.assertEqual(restored.status, TASK_PENDING)
                    self.assertEqual(restored.submit_failures, 0)
                    self.assertEqual(restored.error, "")
                    self.assertEqual(restored.started_at, 0)
                    self.assertEqual(restored.comfy_prompt_id, "")
                    self.assertIn("watchdog-prompt", restored.retired_comfy_prompt_ids)
                    self.assertNotEqual(restored.workload_request_id, original_request)
                    self.assertEqual(
                        [call.kwargs["action"] for call in host.await_args_list],
                        ["heartbeat", "preempt"],
                    )
                    release.assert_awaited_once()
                    self.assertEqual(release.await_args.kwargs["outcome"], "preempted")
                finally:
                    await queue.stop()

        run(scenario())

    def test_exact_host_stale_signal_recalls_before_central_hour(self):
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
                    task = await self._managed_task(
                        queue, server, started_at=time.time() - 120
                    )

                    async def host_control(*_args, **kwargs):
                        if kwargs["action"] == "heartbeat":
                            return self._host_status(
                                task, state="stale", stale_at=time.time() - 1
                            )
                        return self._host_status(task, state="Preempted")

                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value={})
                    ), patch.object(
                        workload_lease,
                        "host_comfy_control",
                        new=AsyncMock(side_effect=host_control),
                    ), patch.object(
                        workload_lease, "release", new=AsyncMock(return_value=None)
                    ):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_PENDING)
                    self.assertEqual(task.submit_failures, 0)
                finally:
                    await queue.stop()

        run(scenario())

    def test_real_host_progress_advances_durable_clock(self):
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
                    task = await self._managed_task(
                        queue, server, started_at=time.time() - 7200
                    )
                    observed_at = time.time() - 2
                    host = AsyncMock(
                        return_value=self._host_status(
                            task,
                            state="running",
                            progress_by_key={
                                "current_stage_string": "sampler",
                                "progress_percent": 25,
                                "last_progress_at": observed_at,
                            },
                        )
                    )
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value={})
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        comfy_adapter,
                        "poll_history",
                        new=AsyncMock(return_value=("pending", None)),
                    ):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(task.comfy_prompt_id, "watchdog-prompt")
                    self.assertEqual(task.managed_comfy_progress_percent, 25)
                    self.assertAlmostEqual(
                        task.managed_comfy_last_progress_at, observed_at, delta=1
                    )
                    self.assertEqual(
                        [call.kwargs["action"] for call in host.await_args_list],
                        ["heartbeat"],
                    )
                finally:
                    await queue.stop()

        run(scenario())

    def test_mismatched_or_malformed_stale_schema_is_ignored(self):
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
                    task = await self._managed_task(
                        queue, server, started_at=time.time() - 120
                    )
                    mismatched = self._host_status(
                        task,
                        prompt_id="some-other-prompt",
                        state="stale",
                        stale_at=float("nan"),
                        progress_percent=float("inf"),
                    )
                    host = AsyncMock(return_value=mismatched)
                    with patch.object(
                        workload_lease, "heartbeat", new=AsyncMock(return_value={})
                    ), patch.object(
                        workload_lease, "host_comfy_control", new=host
                    ), patch.object(
                        comfy_adapter,
                        "poll_history",
                        new=AsyncMock(return_value=("pending", None)),
                    ):
                        await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_RENDERING)
                    self.assertEqual(
                        [call.kwargs["action"] for call in host.await_args_list],
                        ["heartbeat"],
                    )
                finally:
                    await queue.stop()

        run(scenario())

    def test_watchdog_clock_survives_service_restart(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                server.managed_workload = True
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                started = time.time() - 3500
                task = await self._managed_task(queue, server, started_at=started)
                task.managed_comfy_progress_percent = 17
                await queue._persist(task)
                await queue.stop()

                revived_queue = RenderQueue(registry, db_path=config.DB_PATH)
                await revived_queue.start()
                revived_queue._pump_task.cancel()
                try:
                    revived = revived_queue.get(task.id)
                    self.assertEqual(revived.status, TASK_RENDERING)
                    self.assertAlmostEqual(
                        revived.managed_comfy_last_progress_at, started, delta=1
                    )
                    self.assertEqual(revived.managed_comfy_progress_percent, 17)
                    self.assertEqual(
                        revived.managed_comfy_progress_signature,
                        task.managed_comfy_progress_signature,
                    )
                finally:
                    await revived_queue.stop()

        run(scenario())
