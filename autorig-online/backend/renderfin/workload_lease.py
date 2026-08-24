"""Central workload-lease client used by Renderfin GPU stages.

The request id belongs to the logical Renderfin task and is persisted before
the first network call.  A capacity response is therefore a durable queue wait,
not a render attempt.  This client intentionally knows nothing about retries or
stage deadlines; callers start those clocks only after a lease is persisted.
"""
from __future__ import annotations

import os
import json
import re
from typing import Any, Dict, Optional

import httpx

from .models import RenderServer


class WorkloadCapacityWait(RuntimeError):
    def __init__(self, status: str, retry_after: int = 2):
        super().__init__(status or "workload capacity wait")
        self.status = status or "capacity_wait"
        self.retry_after = max(1, int(retry_after or 2))


class WorkloadPreempted(RuntimeError):
    def __init__(self, payload: Optional[Dict[str, Any]] = None):
        self.payload = dict(payload or {})
        self.status = str(self.payload.get("status_string") or "preemption_requested")
        reason = str(
            (self.payload.get("lease_by_key") or {}).get("preemption_reason_string")
            or self.payload.get("preemption_reason_string")
            or ""
        )
        self.preemption_reason = reason
        self.requester_workload_class = ""
        for candidate in (
            "ai_vision",
            "autorig_interactive",
            "comfy",
            "hunyuan",
            "collection_background",
        ):
            if candidate in reason:
                self.requester_workload_class = candidate
                break
        super().__init__(self.status)


class WorkloadLeaseTerminal(RuntimeError):
    """The broker lease expired or otherwise became terminal.

    The persisted Renderfin task still owns a host-side logical binding until
    that host reports Completed/Preempted.  Callers must therefore reconcile
    the exact host task instead of treating this as a generic heartbeat error.
    """

    def __init__(self, payload: Optional[Dict[str, Any]] = None):
        self.payload = dict(payload or {})
        lease = self.payload.get("lease_by_key") or {}
        self.lease_state = str(
            lease.get("state_string")
            or self.payload.get("lease_state_string")
            or "terminal"
        ).strip().lower()
        self.status = str(self.payload.get("status_string") or "lease_terminal")
        super().__init__(f"{self.status}:{self.lease_state}")


def enabled() -> bool:
    return str(os.getenv("RENDERFIN_WORKLOAD_BROKER_ENABLED", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }


def _base_url() -> str:
    return str(
        os.getenv(
            "RENDERFIN_WORKLOAD_BROKER_URL",
            "http://127.0.0.1:8000/api/workload-broker",
        )
    ).rstrip("/")


def _headers() -> Dict[str, str]:
    token = str(
        os.getenv("RENDERFIN_WORKLOAD_BROKER_TOKEN")
        or os.getenv("AUTORIG_WORKLOAD_BROKER_TOKEN")
        or ""
    ).strip()
    if not token:
        raise RuntimeError("Renderfin workload broker token is not configured")
    return {"Authorization": f"Bearer {token}"}


def _safe_node(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if re.fullmatch(
        r"(?:raptor|f7|farm-f7|ryzen-server|ryzen_server)(?:[-_:]?gpu[-_:]?0)",
        raw,
    ):
        return "raptor"
    aliases = {
        "f7": "raptor",
        "farm-f7": "raptor",
        "ryzen-server": "raptor",
        "ryzen_server": "raptor",
    }
    return aliases.get(raw, raw)


def managed_server(server: RenderServer) -> bool:
    if bool(getattr(server, "managed_workload", False)):
        return True
    configured = {
        _safe_node(value)
        for value in str(
            os.getenv(
                "RENDERFIN_MANAGED_COMFY_NODES",
                "f5,f7,f12,f15,raptor,ryzen-server",
            )
        ).split(",")
        if value.strip()
    }
    return _safe_node(server.render_server_name) in configured


def server_identity(server: RenderServer) -> tuple[str, str]:
    node_id = str(getattr(server, "node_id_string", "") or server.render_server_name)
    physical = str(
        getattr(server, "physical_resource_id_string", "")
        or getattr(server, "physical_resource_id", "")
        or node_id
    )
    return node_id, _safe_node(physical)


def server_status(server: RenderServer) -> Dict[str, Any]:
    managed = managed_server(server)
    accepting = str(server.status or "").lower() == "online"
    return {
        "node_kind_string": "managed_farm" if managed else "external_comfy",
        "managed_farm_bool": managed,
        "full_converter_bool": bool(getattr(server, "full_converter_bool", False)),
        "ai_capable_bool": bool(getattr(server, "ai_capable_bool", False)),
        "healthy_bool": accepting,
        "accepting_bool": accepting,
        "reserve_role_string": str(
            getattr(server, "reserve_role_string", "shared") or "shared"
        ),
        "arbiter_by_key": {
            # Never manufacture AI capacity from a registry row.  These bits
            # are populated only by a live host probe/heartbeat.
            "online_bool": bool(getattr(server, "arbiter_online_bool", False)),
            "accepting_ai_vision_bool": bool(
                getattr(server, "arbiter_accepting_ai_vision_bool", False)
            ),
        },
    }


async def node_heartbeat(client: httpx.AsyncClient, *, server: RenderServer) -> Dict[str, Any]:
    """Publish transport liveness without overwriting host capabilities."""
    if not enabled() or not managed_server(server):
        return {"status_string": "not_required"}
    node_id, physical = server_identity(server)
    response = await client.post(
        f"{_base_url()}/nodes/heartbeat",
        headers=_headers(),
        json={
            "node_id_string": node_id,
            "physical_resource_id_string": physical,
            "source_scope_string": "renderfin_probe",
            "node_status_by_key": server_status(server),
        },
        timeout=15.0,
    )
    payload = response.json() if response.content else {}
    if response.status_code != 200:
        status = str(payload.get("status_string") or response.status_code)
        raise WorkloadCapacityWait(f"node_heartbeat_{status}", 5)
    return payload


async def acquire(
    client: httpx.AsyncClient,
    *,
    server: RenderServer,
    workload_class: str,
    owner_task_id: str,
    request_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not enabled() or not managed_server(server):
        return {}
    await node_heartbeat(client, server=server)
    node_id, physical = server_identity(server)
    response = await client.post(
        f"{_base_url()}/leases/acquire",
        headers=_headers(),
        json={
            "node_id_string": node_id,
            "physical_resource_id_string": physical,
            "workload_class_string": workload_class,
            "owner_service_string": "renderfin",
            "owner_task_id_string": owner_task_id,
            "request_id_string": request_id,
            "priority_int": 0,
            "ttl_seconds_int": 300,
            "node_status_by_key": server_status(server),
            "metadata_by_key": dict(metadata or {}),
        },
        timeout=15.0,
    )
    payload = response.json() if response.content else {}
    if response.status_code == 200:
        lease = payload.get("lease_by_key")
        if isinstance(lease, dict) and lease.get("lease_id_string"):
            return lease
        raise RuntimeError("workload broker returned no lease identity")
    status = str(
        payload.get("status_string") or payload.get("error_code_string") or "capacity_wait"
    )
    if response.status_code in {423, 429, 503} or payload.get("retryable_bool") is True:
        raise WorkloadCapacityWait(status, int(payload.get("retry_after_seconds_int") or 2))
    raise RuntimeError(f"workload broker acquire failed: {status}")


async def heartbeat(
    client: httpx.AsyncClient,
    *,
    lease_id: str,
    owner_task_id: str,
    request_id: str,
    server: Optional[RenderServer] = None,
    ttl_seconds: int = 300,
) -> Dict[str, Any]:
    if not enabled() or not lease_id:
        return {"status_string": "not_required"}
    body: Dict[str, Any] = {
        "owner_service_string": "renderfin",
        "owner_task_id_string": owner_task_id,
        "request_id_string": request_id,
        "ttl_seconds_int": max(60, min(3600, int(ttl_seconds or 300))),
    }
    if server is not None:
        await node_heartbeat(client, server=server)
        body["node_status_by_key"] = server_status(server)
    response = await client.post(
        f"{_base_url()}/leases/{lease_id}/heartbeat",
        headers=_headers(),
        json=body,
        timeout=15.0,
    )
    payload = response.json() if response.content else {}
    status = str(payload.get("status_string") or payload.get("error_code_string") or "")
    if response.status_code == 200:
        return payload
    if status == "preemption_requested":
        raise WorkloadPreempted(payload)
    if status == "lease_terminal":
        raise WorkloadLeaseTerminal(payload)
    if response.status_code in {423, 429, 503} or payload.get("retryable_bool") is True:
        raise WorkloadCapacityWait(status, int(payload.get("retry_after_seconds_int") or 2))
    raise RuntimeError(f"workload broker heartbeat failed: {status or response.status_code}")


def _host_control_config(server: RenderServer) -> tuple[str, str]:
    raw = str(os.getenv("RENDERFIN_GPU_CONTROL_NODES_JSON") or "").strip()
    if not raw:
        return "", ""
    try:
        configured = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return "", ""
    if not isinstance(configured, dict):
        return "", ""
    node_id, physical = server_identity(server)
    entry = None
    for key in (physical, _safe_node(node_id), server.render_server_name):
        candidate = configured.get(key)
        if isinstance(candidate, dict):
            entry = candidate
            break
    if not isinstance(entry, dict):
        return "", ""
    url = str(entry.get("url_string") or entry.get("url") or "").strip().rstrip("/")
    token = str(entry.get("token_string") or entry.get("token") or "").strip()
    if url.lower().endswith("/api-converter-glb"):
        url = url[: -len("/api-converter-glb")].rstrip("/")
    return url, token


async def host_comfy_control(
    client: httpx.AsyncClient,
    *,
    server: RenderServer,
    action: str,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
    artifact_sha256: str = "",
    ttl_seconds: int = 300,
) -> Dict[str, Any]:
    """Bridge way-fr to the host-local 5199 arbiter through protected 5198."""
    if not managed_server(server):
        return {"status_string": "not_required"}
    action = str(action or "").strip().lower()
    if action not in {"register", "heartbeat", "complete", "preempt"}:
        raise ValueError(f"unsupported managed Comfy control action: {action}")
    url, token = _host_control_config(server)
    if not url or not token:
        raise WorkloadCapacityWait("host_comfy_control_not_configured", 10)
    body: Dict[str, Any] = {
        "prompt_id": prompt_id,
        "logical_task_id": logical_task_id,
        "lease_id": lease_id,
        "request_id": request_id,
        "ttl_seconds_int": max(60, min(3600, int(ttl_seconds or 300))),
    }
    if artifact_sha256:
        body["artifact_sha256"] = artifact_sha256
    response = await client.post(
        f"{url}/api-converter-glb/control/comfy/{action}",
        headers={"Authorization": f"Bearer {token}"},
        json=body,
        timeout=30.0,
    )
    payload = response.json() if response.content else {}
    status = str(payload.get("status_string") or payload.get("error_code_string") or "")
    if response.status_code == 200:
        return payload
    terminal = str(
        payload.get("outcome_string")
        or payload.get("status_string")
        or payload.get("state_string")
        or payload.get("status")
        or ""
    ).strip().lower()
    if response.status_code == 423 and terminal == "artifact_pending":
        # The host crossed the GPU completion boundary and is deliberately
        # holding the lease while Renderfin downloads/checksums the artifact.
        # This is Completed-wins, not a capacity retry and not permission to
        # requeue the logical task.
        normalized = dict(payload)
        normalized["outcome_string"] = "completed"
        normalized.setdefault("status", "Completed")
        return normalized
    if response.status_code == 409 and terminal in {
        "completed",
        "preempted",
        "released",
    }:
        # Completed-wins is an exact terminal acknowledgement, not capacity.
        return payload
    if response.status_code in {423, 429, 503} or payload.get("retryable_bool") is True:
        raise WorkloadCapacityWait(status or f"host_comfy_{action}_busy", 2)
    raise RuntimeError(
        f"host managed Comfy {action} failed: {status or response.status_code}"
    )


async def release(
    client: httpx.AsyncClient,
    *,
    lease_id: str,
    owner_task_id: str,
    request_id: str,
    outcome: str,
) -> None:
    if not enabled() or not lease_id:
        return
    response = await client.post(
        f"{_base_url()}/leases/{lease_id}/release",
        headers=_headers(),
        json={
            "owner_service_string": "renderfin",
            "owner_task_id_string": owner_task_id,
            "request_id_string": request_id,
            "outcome_string": outcome,
            "reason_string": outcome,
        },
        timeout=15.0,
    )
    if response.status_code != 200:
        raise RuntimeError(f"workload broker release HTTP {response.status_code}")
    payload = response.json() if response.content else {}
    terminal = str(
        payload.get("status_string")
        or (payload.get("lease_by_key") or {}).get("state_string")
        or ""
    ).strip().lower()
    allowed = {
        "completed": {"completed"},
        # `expired` is a valid acknowledgement only after the caller has
        # already obtained exact host-side Preempted proof.  It means central
        # admission is terminal too; no lease/binding can be duplicated.
        "preempted": {"preempted", "completed", "expired"},
        "released": {"released", "completed"},
    }.get(str(outcome or "").strip().lower(), {str(outcome or "").strip().lower()})
    if terminal not in allowed:
        raise RuntimeError(
            f"workload broker release outcome mismatch: requested={outcome} got={terminal or 'empty'}"
        )


async def cancel_waiter(
    client: httpx.AsyncClient,
    *,
    request_id: str,
    owner_task_id: str,
) -> None:
    if not enabled() or not request_id:
        return
    response = await client.post(
        f"{_base_url()}/requests/{request_id}/cancel",
        headers=_headers(),
        json={"owner_task_id_string": owner_task_id},
        timeout=15.0,
    )
    if response.status_code != 200:
        raise RuntimeError(f"workload waiter cancel HTTP {response.status_code}")
