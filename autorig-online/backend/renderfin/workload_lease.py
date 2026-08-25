"""Central workload-lease client used by Renderfin GPU stages.

The request id belongs to the logical Renderfin task and is persisted before
the first network call.  A capacity response is therefore a durable queue wait,
not a render attempt.  This client intentionally knows nothing about retries or
stage deadlines; callers start those clocks only after a lease is persisted.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
import stat
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx

from .models import RenderServer


_STABLE_MACHINE_RE = re.compile(r"machine_[a-f0-9]{24,128}")
_ALLOWED_WORKLOAD_ROLES = {
    "ai_vision_primary",
    "autorig_primary",
    "shared",
    "background_only",
    "maintenance",
}
MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL = "autorig-managed-comfy-spool-v1"
_ARTIFACT_SHA256_RE = re.compile(r"[a-f0-9]{64}")
_MAX_MANAGED_COMFY_ARTIFACT_BYTES = 512 * 1024 * 1024


class WorkloadCapacityWait(RuntimeError):
    def __init__(self, status: str, retry_after: int = 2):
        super().__init__(status or "workload capacity wait")
        self.status = status or "capacity_wait"
        self.retry_after = max(1, int(retry_after or 2))


class HostComfyReceiptMismatch(WorkloadCapacityWait):
    """An authenticated host terminal receipt did not name our exact work.

    The bridge bearer authenticates the host, not an individual prompt.  A
    terminal response is therefore authoritative only when it echoes the
    prompt, logical task, central lease and request ids supplied by the caller.
    Treat a missing/mismatched echo as an attempt-neutral retry while retaining
    the old binding fail-closed.
    """

    def __init__(self, action: str, payload: Optional[Dict[str, Any]] = None):
        self.action = str(action or "host_control").strip().lower()
        self.payload = dict(payload or {})
        super().__init__(f"host_comfy_{self.action}_receipt_mismatch", 2)


class HostComfyArtifactWait(WorkloadCapacityWait):
    """Fail-closed, attempt-neutral durable artifact handoff wait."""

    def __init__(self, status: str, retry_after: int = 2):
        super().__init__(str(status or "host_comfy_artifact_wait"), retry_after)


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
        or os.getenv("AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN")
        or ""
    ).strip()
    if not token:
        raise RuntimeError("Renderfin workload broker token is not configured")
    return {"Authorization": f"Bearer {token}"}


def _safe_node(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if re.fullmatch(
        r"(?:raptor|ryzen-server|ryzen_server)(?:[-_:]?gpu[-_:]?0)",
        raw,
    ):
        return "raptor"
    aliases = {
        "ryzen-server": "raptor",
        "ryzen_server": "raptor",
    }
    return aliases.get(raw, raw)


def _safe_role(value: Any) -> str:
    role = str(value or "").strip().lower()
    if role == "ai_primary":
        role = "ai_vision_primary"
    return role if role in _ALLOWED_WORKLOAD_ROLES else ""


def verified_machine_role(physical: Any, role: Any) -> bool:
    return bool(
        _STABLE_MACHINE_RE.fullmatch(str(physical or "").strip().lower())
        and _safe_role(role)
    )


def canonical_workload_role(role: Any) -> str:
    return _safe_role(role)


def host_comfy_terminal_outcome(payload: Dict[str, Any]) -> str:
    """Return a host terminal outcome without implying receipt ownership."""

    if not isinstance(payload, dict):
        return ""
    entry = host_comfy_receipt_entry(payload)
    for source in (entry, payload):
        value = str(
            source.get("outcome_string")
            or source.get("status_string")
            or source.get("state_string")
            or source.get("state")
            or source.get("status")
            or ""
        ).strip().lower()
        if value in {"completed", "preempted", "released"}:
            return value
        if source is payload:
            break
    return ""


def host_comfy_receipt_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return the host prompt ledger entry from known response envelopes."""

    if not isinstance(payload, dict):
        return {}
    for key in (
        "managed_prompt_by_key",
        "managed_comfy_prompt_by_key",
        "prompt_by_key",
    ):
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            return candidate
    return payload


def host_comfy_receipt_matches(
    payload: Dict[str, Any],
    *,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
) -> bool:
    """Require the exact four-part identity from an authenticated host."""

    if not isinstance(payload, dict):
        return False
    entry = host_comfy_receipt_entry(payload)

    def first(*keys: str) -> str:
        for key in keys:
            value = entry.get(key)
            if value is None:
                value = payload.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    actual = (
        first("prompt_id", "prompt_id_string"),
        first("logical_task_id", "logical_task_id_string"),
        first(
            "central_lease_id",
            "central_lease_id_string",
            "lease_id",
            "lease_id_string",
        ),
        first("request_id", "request_id_string"),
    )
    expected = tuple(
        str(value or "").strip()
        for value in (prompt_id, logical_task_id, lease_id, request_id)
    )
    return bool(all(actual) and all(expected) and actual == expected)


def validate_host_comfy_terminal_receipt(
    payload: Dict[str, Any],
    *,
    action: str,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
) -> str:
    """Validate the exact four-ID receipt for every lifecycle response.

    Completed is intentionally valid for register, heartbeat and preempt: the
    prompt may cross its terminal boundary before the requested control action.
    Non-terminal progress is still bound to the same prompt/task/lease/request;
    accepting an unbound `registered` or `heartbeat` response could authorize a
    different prompt on the same physical GPU.
    """

    outcome = host_comfy_terminal_outcome(payload)
    if not host_comfy_receipt_matches(
        payload,
        prompt_id=prompt_id,
        logical_task_id=logical_task_id,
        lease_id=lease_id,
        request_id=request_id,
    ):
        raise HostComfyReceiptMismatch(action, payload)
    return outcome


def _exact_artifact_metadata(payload: Dict[str, Any], *, action: str) -> tuple[str, int]:
    checksum = str(payload.get("artifact_sha256") or "").strip().lower()
    size = payload.get("artifact_size_int")
    if (
        not _ARTIFACT_SHA256_RE.fullmatch(checksum)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size <= 0
        or size > _MAX_MANAGED_COMFY_ARTIFACT_BYTES
    ):
        raise HostComfyArtifactWait(
            f"host_comfy_{action}_artifact_metadata_invalid", 2
        )
    return checksum, size


def _artifact_protocol_matches(payload: Dict[str, Any]) -> bool:
    return str(
        payload.get("artifact_spool_protocol_string") or ""
    ).strip() == MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL


def _exact_spool_identity_matches(
    payload: Dict[str, Any], identity: Dict[str, str]
) -> bool:
    """Spool v1 echoes the exact request field names, not legacy aliases."""

    return bool(isinstance(payload, dict) and all(identity.values())) and all(
        str(payload.get(key) or "").strip() == str(value or "").strip()
        for key, value in identity.items()
    )


def _fsync_directory(path: Path) -> None:
    """Persist an atomic rename on filesystems which support directory fsync."""

    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        # Windows does not permit opening a directory this way. Production is
        # Linux, where this is required and supported; the file itself has
        # already been fsynced on every platform.
        if os.name == "nt":
            return
        raise HostComfyArtifactWait(
            "central_managed_comfy_directory_fsync_open_failed", 2
        ) from exc
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if os.name != "nt":
                raise HostComfyArtifactWait(
                    "central_managed_comfy_directory_fsync_failed", 2
                ) from exc
    finally:
        os.close(descriptor)


def verify_central_artifact(
    path: Path,
    *,
    expected_sha256: str,
    expected_size: int,
) -> bool:
    """Re-hash a central artifact before an irreversible host ACK."""

    expected_sha256 = str(expected_sha256 or "").strip().lower()
    if (
        not _ARTIFACT_SHA256_RE.fullmatch(expected_sha256)
        or isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size <= 0
    ):
        return False
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size != expected_size:
            return False
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        return size == expected_size and digest.hexdigest() == expected_sha256
    except OSError:
        return False


def server_role_rank(workload_class: str, role: Any) -> int:
    canonical = _safe_role(role) or "maintenance"
    if workload_class == "ai_vision":
        order = ("ai_vision_primary", "shared", "autorig_primary")
    elif workload_class == "autorig_interactive":
        order = ("autorig_primary", "shared", "ai_vision_primary")
    else:
        order = ("shared", "autorig_primary", "ai_vision_primary")
    try:
        return order.index(canonical)
    except ValueError:
        return 100


def _host_control_nodes() -> Dict[str, Dict[str, Any]]:
    raw = str(os.getenv("RENDERFIN_GPU_CONTROL_NODES_JSON") or "").strip()
    if not raw:
        return {}
    try:
        configured = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(configured, dict):
        return {}
    return {
        str(key).strip().lower(): value
        for key, value in configured.items()
        if str(key).strip() and isinstance(value, dict)
    }


def _host_control_entry(server: RenderServer) -> Dict[str, Any]:
    configured = _host_control_nodes()
    names = [
        str(server.render_server_name or "").strip().lower(),
        str(getattr(server, "node_id_string", "") or "").strip().lower(),
        str(getattr(server, "physical_resource_id_string", "") or "").strip().lower(),
    ]
    expanded = []
    for name in names:
        if not name:
            continue
        expanded.extend((name, _safe_node(name)))
    for name in expanded:
        entry = configured.get(name)
        if isinstance(entry, dict):
            return entry
    return {}


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
    physical = _safe_node(physical)
    if managed_server(server):
        if not bool(getattr(server, "workload_identity_verified_bool", False)):
            return node_id, ""
        if not _STABLE_MACHINE_RE.fullmatch(physical):
            return node_id, ""
        if not _safe_role(getattr(server, "reserve_role_string", "")):
            return node_id, ""
    return node_id, physical


async def refresh_managed_identity(
    client: httpx.AsyncClient,
    server: RenderServer,
) -> bool:
    """Authenticate and bind a Renderfin transport name to one physical GPU.

    Display names and tunnel URLs are not resource identities. Both the
    deployment registry and the live converter must independently report the
    same stable ``machine_*`` id and canonical workload role before Renderfin
    may publish/acquire a central lease for the node.
    """
    if not managed_server(server):
        return True
    if not enabled():
        # Rollout order is converter -> registry -> broker flag. Until the
        # broker is deliberately enabled, identity enforcement must not alter
        # the existing production Comfy pool.
        return True
    server.workload_identity_verified_bool = False
    server.arbiter_online_bool = False
    server.arbiter_accepting_ai_vision_bool = False
    server.managed_comfy_artifact_spool_required_bool = False
    server.managed_comfy_artifact_spool_ready_bool = False
    server.managed_comfy_artifact_spool_protocol_string = ""
    server.managed_comfy_central_control_ready_bool = False
    entry = _host_control_entry(server)
    capability_mode = str(
        entry.get("capability_mode_string")
        or entry.get("capability_mode")
        or "full"
    ).strip().lower()
    url = str(entry.get("url_string") or entry.get("url") or "").strip().rstrip("/")
    token = str(entry.get("token_string") or entry.get("token") or "").strip()
    expected_physical = str(
        entry.get("physical_resource_id_string")
        or entry.get("physical_node")
        or ""
    ).strip().lower()
    expected_role = _safe_role(
        entry.get("reserve_role_string")
        or entry.get("workload_role")
        or entry.get("workload_role_string")
    )
    if url.lower().endswith("/api-converter-glb"):
        url = url[: -len("/api-converter-glb")].rstrip("/")
    if not (
        _STABLE_MACHINE_RE.fullmatch(expected_physical)
        and expected_role
        and capability_mode in {"full", "comfy_ai"}
    ):
        return False
    if capability_mode == "comfy_ai":
        control_config = _central_arbiter_control_config(server)
        if not control_config:
            return False
        control_client, close_client_bool = _central_control_http_client(
            client, control_config
        )
        try:
            response = await control_client.get(
                f"{control_config['url_string']}/status",
                headers={
                    "Authorization": (
                        f"Bearer {control_config['token_string']}"
                    )
                },
                timeout=15.0,
                follow_redirects=False,
            )
            if response.status_code != 200:
                return False
            payload = response.json() if response.content else {}
        except Exception:
            return False
        finally:
            if close_client_bool:
                await control_client.aclose()
    else:
        if not url or not token:
            return False
        try:
            response = await client.get(
                f"{url}/api-converter-glb/server-status",
                headers={"Authorization": f"Bearer {token}"},
                timeout=15.0,
                follow_redirects=False,
            )
            if response.status_code != 200:
                return False
            payload = response.json() if response.content else {}
        except Exception:
            return False
    if not isinstance(payload, dict):
        return False
    control = payload.get("workload_control")
    control = control if isinstance(control, dict) else {}
    reported_physical = str(
        payload.get("physical_node")
        or payload.get("physical_resource_id_string")
        or payload.get("physical_gpu_id")
        or control.get("physical_node")
        or ""
    ).strip().lower()
    reported_role = _safe_role(
        payload.get("workload_role")
        or payload.get("workload_role_string")
        or control.get("workload_role")
    )
    if reported_physical != expected_physical or reported_role != expected_role:
        return False
    reported_capability_mode = str(
        payload.get("capability_mode")
        or payload.get("capability_mode_string")
        or control.get("capability_mode")
        or capability_mode
    ).strip().lower()
    if reported_capability_mode != capability_mode:
        return False
    central_control_ready = bool(
        payload.get("managed_comfy_central_control_ready_bool")
        if "managed_comfy_central_control_ready_bool" in payload
        else control.get("managed_comfy_central_control_ready_bool")
    )
    arbiter_enabled = bool(
        central_control_ready
        and (
            capability_mode == "comfy_ai"
            or (
                payload.get("gpu_arbiter_enabled")
                if "gpu_arbiter_enabled" in payload
                else control.get("arbiter_enabled")
            )
        )
    )
    spool_required = bool(
        payload.get("managed_comfy_artifact_spool_required_bool")
        if "managed_comfy_artifact_spool_required_bool" in payload
        else control.get("managed_comfy_artifact_spool_required_bool")
    )
    spool_ready = bool(
        payload.get("managed_comfy_artifact_spool_ready_bool")
        if "managed_comfy_artifact_spool_ready_bool" in payload
        else control.get("managed_comfy_artifact_spool_ready_bool")
    )
    spool_protocol = str(
        payload.get("managed_comfy_artifact_spool_protocol_string")
        or control.get("managed_comfy_artifact_spool_protocol_string")
        or ""
    ).strip()
    server.physical_resource_id_string = reported_physical
    server.reserve_role_string = reported_role
    server.node_id_string = str(
        payload.get("node_id_string") or server.render_server_name
    )
    server.arbiter_online_bool = arbiter_enabled
    server.arbiter_accepting_ai_vision_bool = bool(
        payload.get("accepting_ai_vision")
    ) and arbiter_enabled
    server.managed_comfy_artifact_spool_required_bool = spool_required
    server.managed_comfy_artifact_spool_ready_bool = spool_ready
    server.managed_comfy_artifact_spool_protocol_string = spool_protocol
    server.managed_comfy_central_control_ready_bool = central_control_ready
    server.workload_identity_verified_bool = True
    # A host which requires the durable handoff but cannot currently provide
    # the exact protocol is kept in maintenance for new dispatch. Existing
    # bound tasks retain the server object and reconcile fail-closed.
    if spool_required and (
        not spool_ready
        or spool_protocol != MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
    ):
        return False
    if not central_control_ready:
        return False
    return True


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
            _safe_role(getattr(server, "reserve_role_string", "")) or "maintenance"
        ),
        "managed_comfy_artifact_spool_required_bool": bool(
            getattr(server, "managed_comfy_artifact_spool_required_bool", False)
        ),
        "managed_comfy_artifact_spool_ready_bool": bool(
            getattr(server, "managed_comfy_artifact_spool_ready_bool", False)
        ),
        "managed_comfy_artifact_spool_protocol_string": str(
            getattr(server, "managed_comfy_artifact_spool_protocol_string", "")
            or ""
        ),
        "managed_comfy_central_control_ready_bool": bool(
            getattr(
                server,
                "managed_comfy_central_control_ready_bool",
                False,
            )
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
    if not physical:
        raise WorkloadCapacityWait("managed_identity_unverified", 5)
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
    if not physical:
        raise WorkloadCapacityWait("managed_identity_unverified", 5)
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
    entry = _host_control_entry(server)
    if not entry:
        return "", ""
    url = str(entry.get("url_string") or entry.get("url") or "").strip().rstrip("/")
    token = str(entry.get("token_string") or entry.get("token") or "").strip()
    if url.lower().endswith("/api-converter-glb"):
        url = url[: -len("/api-converter-glb")].rstrip("/")
    return url, token


def _protected_control_file_path(
    path_text: str,
    *,
    secret_bool: bool,
) -> str:
    path = Path(str(path_text or "").strip())
    try:
        if (
            not path.is_absolute()
            or path.is_symlink()
            or not path.is_file()
            or path.resolve(strict=True) != path.absolute()
        ):
            return ""
        file_stat = path.stat()
        mode = stat.S_IMODE(file_stat.st_mode)
        allowed_modes = {0o400, 0o440}
        if not secret_bool:
            allowed_modes.add(0o444)
        # Windows chmod emulation exposes read-only fixture files as 0444.
        # Production is Linux and still requires 0400/0440 for secret files.
        if os.name == "nt":
            allowed_modes.add(0o444)
        if mode not in allowed_modes:
            return ""
        if hasattr(os, "geteuid"):
            effective_uid = os.geteuid()
            effective_gid = os.getegid()
            supplementary = set(os.getgroups())
            if file_stat.st_uid not in {0, effective_uid}:
                return ""
            if mode == 0o440 and file_stat.st_gid not in {
                effective_gid,
                *supplementary,
            }:
                return ""
    except (OSError, ValueError):
        return ""
    return str(path)


def _central_arbiter_control_config(server: RenderServer) -> Dict[str, str]:
    """Return strict mTLS configuration for one node's central listener.

    Managed-Comfy durability traffic must not traverse the converter or
    Hunyuan webserver because those processes run as the workload SID.  Each
    node therefore has a distinct SSH tunnel whose way-fr end binds only to
    127.0.0.1 and reaches the SYSTEM arbiter's mTLS-only 127.0.0.1:5200.
    Missing files, plaintext HTTP, a non-loopback URL, or an aliased workload
    token are deliberately not backward compatible: callers wait without
    consuming an attempt and never fall back to the workload bridge.
    """

    entry = _host_control_entry(server)
    if not entry:
        return {}
    url = str(entry.get("arbiter_control_url_string") or "").strip().rstrip("/")
    # Inline/env tokens are intentionally rejected.  The systemd environment
    # contains only a path into /srv/autorig/secrets; the service reads the
    # credential at request time so rotation does not require putting it in a
    # process environment or command line.
    if str(entry.get("central_token_string") or "").strip():
        return {}
    token_path_text = str(entry.get("central_token_file_string") or "").strip()
    ca_path_text = str(entry.get("central_tls_ca_file_string") or "").strip()
    cert_path_text = str(
        entry.get("central_tls_client_cert_file_string") or ""
    ).strip()
    key_path_text = str(
        entry.get("central_tls_client_key_file_string") or ""
    ).strip()
    workload_token = str(
        entry.get("token_string") or entry.get("token") or ""
    ).strip()
    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError:
        return {}
    if not (
        parsed.scheme == "https"
        and parsed.hostname == "127.0.0.1"
        and port
        and parsed.path in {"", "/"}
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    ):
        return {}
    token_path_string = _protected_control_file_path(
        token_path_text, secret_bool=True
    )
    ca_path_string = _protected_control_file_path(
        ca_path_text, secret_bool=False
    )
    cert_path_string = _protected_control_file_path(
        cert_path_text, secret_bool=False
    )
    key_path_string = _protected_control_file_path(
        key_path_text, secret_bool=True
    )
    if not all(
        (
            token_path_string,
            ca_path_string,
            cert_path_string,
            key_path_string,
        )
    ):
        return {}
    try:
        raw = Path(token_path_string).read_text(encoding="utf-8")
    except (OSError, UnicodeError, ValueError):
        return {}
    token = raw.strip()
    if (
        len(token) < 20
        or token == workload_token
        or raw not in {token, token + "\n"}
    ):
        return {}
    return {
        "url_string": url,
        "token_string": token,
        "ca_file_string": ca_path_string,
        "client_cert_file_string": cert_path_string,
        "client_key_file_string": key_path_string,
    }


def _central_control_http_client(
    injected_client: Optional[httpx.AsyncClient],
    control_config: Dict[str, str],
) -> tuple[httpx.AsyncClient, bool]:
    """Return an isolated no-proxy/no-redirect mTLS client.

    A MockTransport is accepted only as an in-process test seam.  Runtime
    callers pass the queue's ordinary client, which is deliberately ignored so
    its proxy, redirect and public-web trust policy cannot leak into central
    lifecycle traffic.
    """

    if isinstance(
        getattr(injected_client, "_transport", None), httpx.MockTransport
    ):
        return injected_client, False
    context = ssl.create_default_context(
        cafile=control_config["ca_file_string"]
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(
        certfile=control_config["client_cert_file_string"],
        keyfile=control_config["client_key_file_string"],
    )
    return (
        httpx.AsyncClient(
            verify=context,
            trust_env=False,
            follow_redirects=False,
        ),
        True,
    )


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
    expected_canonical_submission_sha256: str = "",
    ttl_seconds: int = 300,
) -> Dict[str, Any]:
    """Call the host's central-only 5200 mTLS managed-Comfy listener."""
    if not managed_server(server):
        return {"status_string": "not_required"}
    action = str(action or "").strip().lower()
    if action not in {"register", "heartbeat", "complete", "preempt"}:
        raise ValueError(f"unsupported managed Comfy control action: {action}")
    control_config = _central_arbiter_control_config(server)
    if not control_config:
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
    if action == "register":
        submission_sha256 = str(
            expected_canonical_submission_sha256 or ""
        ).strip().lower()
        if not _ARTIFACT_SHA256_RE.fullmatch(submission_sha256):
            raise WorkloadCapacityWait(
                "host_comfy_submission_binding_not_configured", 10
            )
        body["expected_canonical_submission_sha256"] = submission_sha256
    control_client, close_client_bool = _central_control_http_client(
        client, control_config
    )
    try:
        response = await control_client.post(
            f"{control_config['url_string']}/comfy/{action}",
            headers={
                "Authorization": (
                    f"Bearer {control_config['token_string']}"
                )
            },
            json=body,
            timeout=30.0,
            follow_redirects=False,
        )
    finally:
        if close_client_bool:
            await control_client.aclose()
    payload = response.json() if response.content else {}
    status = str(payload.get("status_string") or payload.get("error_code_string") or "")
    receipt_identity = {
        "action": action,
        "prompt_id": prompt_id,
        "logical_task_id": logical_task_id,
        "lease_id": lease_id,
        "request_id": request_id,
    }
    if response.status_code == 200:
        validate_host_comfy_terminal_receipt(payload, **receipt_identity)
        return payload
    control_state = str(
        payload.get("outcome_string")
        or payload.get("status_string")
        or payload.get("state_string")
        or payload.get("status")
        or ""
    ).strip().lower()
    if response.status_code == 423 and control_state in {
        "artifact_pending",
        "artifact_spooled",
    }:
        # The host crossed the GPU completion boundary and is deliberately
        # holding the lease while Renderfin downloads/checksums the artifact.
        # ``artifact_spooled`` is the response-loss/restart variant: retry the
        # exact stage/GET/ACK flow rather than treating it as fresh capacity.
        # This is Completed-wins, not a capacity retry and not permission to
        # requeue the logical task.
        normalized = dict(payload)
        normalized["outcome_string"] = "completed"
        normalized.setdefault("status", "Completed")
        validate_host_comfy_terminal_receipt(normalized, **receipt_identity)
        return normalized
    terminal = host_comfy_terminal_outcome(payload)
    if response.status_code in {409, 423} and terminal in {
        "completed",
        "preempted",
        "released",
    }:
        # Completed-wins is an exact terminal acknowledgement, not capacity.
        validate_host_comfy_terminal_receipt(payload, **receipt_identity)
        return payload
    if response.status_code in {423, 429, 503} or payload.get("retryable_bool") is True:
        raise WorkloadCapacityWait(status or f"host_comfy_{action}_busy", 2)
    raise RuntimeError(
        f"host managed Comfy {action} failed: {status or response.status_code}"
    )


async def host_comfy_stage_artifact(
    client: httpx.AsyncClient,
    *,
    server: RenderServer,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
    artifact_relative_path_string: str,
    artifact_sha256: str = "",
    artifact_size_int: int = 0,
) -> Dict[str, Any]:
    """Copy one exact successful Comfy output into the host CPU spool."""

    control_config = _central_arbiter_control_config(server)
    if not control_config:
        raise HostComfyArtifactWait("host_comfy_artifact_stage_not_configured", 10)
    identity = {
        "prompt_id": str(prompt_id or "").strip(),
        "logical_task_id": str(logical_task_id or "").strip(),
        "lease_id": str(lease_id or "").strip(),
        "request_id": str(request_id or "").strip(),
    }
    if not all(identity.values()):
        raise HostComfyReceiptMismatch("stage", identity)
    relative = str(artifact_relative_path_string or "").strip()
    if not relative:
        raise HostComfyArtifactWait("host_comfy_artifact_stage_path_missing", 2)
    body: Dict[str, Any] = {
        **identity,
        "artifact_relative_path_string": relative,
    }
    checksum = str(artifact_sha256 or "").strip().lower()
    if checksum:
        if not _ARTIFACT_SHA256_RE.fullmatch(checksum):
            raise HostComfyArtifactWait("host_comfy_artifact_stage_sha_invalid", 2)
        body["artifact_sha256"] = checksum
    if artifact_size_int:
        if (
            isinstance(artifact_size_int, bool)
            or not isinstance(artifact_size_int, int)
            or artifact_size_int <= 0
        ):
            raise HostComfyArtifactWait("host_comfy_artifact_stage_size_invalid", 2)
        body["artifact_size_int"] = artifact_size_int
    control_client, close_client_bool = _central_control_http_client(
        client, control_config
    )
    try:
        response = await control_client.post(
            f"{control_config['url_string']}/comfy/stage",
            headers={
                "Authorization": (
                    f"Bearer {control_config['token_string']}"
                )
            },
            json=body,
            timeout=30.0,
            follow_redirects=False,
        )
    finally:
        if close_client_bool:
            await control_client.aclose()
    try:
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}
    receipt_identity = {
        "action": "stage",
        **identity,
    }
    if response.status_code == 200:
        terminal = validate_host_comfy_terminal_receipt(
            payload, **receipt_identity
        )
        if not _exact_spool_identity_matches(payload, identity):
            raise HostComfyReceiptMismatch("stage", payload)
        if terminal:
            # A response-lost retry after an ACK is exact Completed. The queue
            # may trust it only alongside its already-persisted central bundle.
            return payload
        if str(payload.get("status_string") or "").strip() != "artifact_spooled":
            raise HostComfyArtifactWait("host_comfy_artifact_stage_status_invalid", 2)
        if not _artifact_protocol_matches(payload):
            raise HostComfyArtifactWait("host_comfy_artifact_stage_protocol_invalid", 2)
        if not (
            payload.get("artifact_spool_ready_bool") is True
            and payload.get("artifact_cpu_spool_persisted_bool") is True
            and payload.get("artifact_checksum_persisted_bool") is True
            and payload.get("gpu_detached_bool") is True
        ):
            raise HostComfyArtifactWait("host_comfy_artifact_stage_not_durable", 2)
        _exact_artifact_metadata(payload, action="stage")
        return payload
    if response.status_code == 409:
        raise HostComfyReceiptMismatch("stage", payload)
    status = str(
        payload.get("status_string")
        or payload.get("error")
        or payload.get("error_code_string")
        or f"http_{response.status_code}"
    ).strip()
    if response.status_code in {423, 429, 503} or payload.get("retryable") is True:
        raise HostComfyArtifactWait(f"host_comfy_artifact_stage_{status}", 2)
    # Invalid provenance must never cause a second render or release an exact
    # binding. It is a fail-closed operational error for this same prompt.
    raise HostComfyArtifactWait(f"host_comfy_artifact_stage_{status}", 10)


async def host_comfy_download_artifact(
    client: httpx.AsyncClient,
    *,
    server: RenderServer,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
    destination_path: Path,
    expected_sha256: str,
    expected_size_int: int,
) -> Dict[str, Any]:
    """Stream, checksum, fsync and atomically persist the exact host spool."""

    control_config = _central_arbiter_control_config(server)
    if not control_config:
        raise HostComfyArtifactWait("host_comfy_artifact_get_not_configured", 10)
    identity = {
        "prompt_id": str(prompt_id or "").strip(),
        "logical_task_id": str(logical_task_id or "").strip(),
        "lease_id": str(lease_id or "").strip(),
        "request_id": str(request_id or "").strip(),
    }
    if not all(identity.values()):
        raise HostComfyReceiptMismatch("artifact_get", identity)
    checksum = str(expected_sha256 or "").strip().lower()
    size_expected = expected_size_int
    if (
        not _ARTIFACT_SHA256_RE.fullmatch(checksum)
        or isinstance(size_expected, bool)
        or not isinstance(size_expected, int)
        or size_expected <= 0
        or size_expected > _MAX_MANAGED_COMFY_ARTIFACT_BYTES
    ):
        raise HostComfyArtifactWait("host_comfy_artifact_get_expectation_invalid", 2)

    destination = Path(destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{uuid.uuid4().hex}.managed-comfy-part"
    )
    control_client, close_client_bool = _central_control_http_client(
        client, control_config
    )
    try:
        async with control_client.stream(
            "GET",
            f"{control_config['url_string']}/comfy/artifact",
            headers={
                "Authorization": (
                    f"Bearer {control_config['token_string']}"
                )
            },
            params=identity,
            timeout=httpx.Timeout(300.0, connect=30.0),
            follow_redirects=False,
        ) as response:
            if response.status_code != 200:
                raw = await response.aread()
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                except Exception:
                    payload = {}
                if response.status_code == 409:
                    raise HostComfyReceiptMismatch("artifact_get", payload)
                status = str(
                    payload.get("status_string")
                    or payload.get("error")
                    or f"http_{response.status_code}"
                ).strip()
                raise HostComfyArtifactWait(
                    f"host_comfy_artifact_get_{status}",
                    2 if response.status_code in {410, 423, 429, 503} else 10,
                )
            protocol = str(
                response.headers.get("X-AutoRig-Artifact-Protocol") or ""
            ).strip()
            header_sha = str(
                response.headers.get("X-AutoRig-Artifact-SHA256") or ""
            ).strip().lower()
            header_size_text = str(
                response.headers.get("X-AutoRig-Artifact-Size") or ""
            ).strip()
            if (
                protocol != MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
                or not _ARTIFACT_SHA256_RE.fullmatch(header_sha)
                or not header_size_text.isdigit()
                or int(header_size_text) <= 0
                or header_sha != checksum
                or int(header_size_text) != size_expected
            ):
                raise HostComfyArtifactWait(
                    "host_comfy_artifact_get_headers_mismatch", 2
                )
            digest = hashlib.sha256()
            actual_size = 0
            with temporary.open("xb") as sink:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    actual_size += len(chunk)
                    if actual_size > _MAX_MANAGED_COMFY_ARTIFACT_BYTES:
                        raise HostComfyArtifactWait(
                            "host_comfy_artifact_get_too_large", 10
                        )
                    digest.update(chunk)
                    sink.write(chunk)
                sink.flush()
                os.fsync(sink.fileno())
            if actual_size != size_expected or digest.hexdigest() != checksum:
                raise HostComfyArtifactWait(
                    "host_comfy_artifact_get_checksum_mismatch", 2
                )
        os.replace(str(temporary), str(destination))
        _fsync_directory(destination.parent)
        if not verify_central_artifact(
            destination,
            expected_sha256=checksum,
            expected_size=size_expected,
        ):
            raise HostComfyArtifactWait(
                "host_comfy_artifact_get_persistence_verification_failed", 2
            )
        return {
            "artifact_sha256": checksum,
            "artifact_size_int": size_expected,
            "artifact_spool_protocol_string": (
                MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL
            ),
            "central_persisted_bool": True,
        }
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        if close_client_bool:
            await control_client.aclose()


async def host_comfy_ack_artifact(
    client: httpx.AsyncClient,
    *,
    server: RenderServer,
    prompt_id: str,
    logical_task_id: str,
    lease_id: str,
    request_id: str,
    artifact_sha256: str,
    artifact_size_int: int,
    central_persistence_receipt_id_string: str,
) -> Dict[str, Any]:
    """Tombstone the host spool only after verified central persistence."""

    control_config = _central_arbiter_control_config(server)
    if not control_config:
        raise HostComfyArtifactWait("host_comfy_artifact_ack_not_configured", 10)
    identity = {
        "prompt_id": str(prompt_id or "").strip(),
        "logical_task_id": str(logical_task_id or "").strip(),
        "lease_id": str(lease_id or "").strip(),
        "request_id": str(request_id or "").strip(),
    }
    checksum = str(artifact_sha256 or "").strip().lower()
    receipt = str(central_persistence_receipt_id_string or "").strip()
    if not all(identity.values()):
        raise HostComfyReceiptMismatch("ack", identity)
    if (
        not _ARTIFACT_SHA256_RE.fullmatch(checksum)
        or isinstance(artifact_size_int, bool)
        or not isinstance(artifact_size_int, int)
        or artifact_size_int <= 0
        or not receipt
        or len(receipt) > 256
        or any(ord(char) < 33 or ord(char) > 126 for char in receipt)
    ):
        raise HostComfyArtifactWait("host_comfy_artifact_ack_input_invalid", 2)
    body: Dict[str, Any] = {
        **identity,
        "artifact_sha256": checksum,
        "artifact_size_int": artifact_size_int,
        "central_persisted_bool": True,
        "central_persistence_receipt_id_string": receipt,
    }
    control_client, close_client_bool = _central_control_http_client(
        client, control_config
    )
    try:
        response = await control_client.post(
            f"{control_config['url_string']}/comfy/ack",
            headers={
                "Authorization": (
                    f"Bearer {control_config['token_string']}"
                )
            },
            json=body,
            timeout=30.0,
            follow_redirects=False,
        )
    finally:
        if close_client_bool:
            await control_client.aclose()
    try:
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}
    if response.status_code == 200:
        outcome = validate_host_comfy_terminal_receipt(
            payload,
            action="ack",
            **identity,
        )
        if not _exact_spool_identity_matches(payload, identity):
            raise HostComfyReceiptMismatch("ack", payload)
        returned_sha, returned_size = _exact_artifact_metadata(
            payload, action="ack"
        )
        if (
            outcome != "completed"
            or not _artifact_protocol_matches(payload)
            or payload.get("artifact_ack_tombstone_bool") is not True
            or payload.get("central_persisted_bool") is not True
            or returned_sha != checksum
            or returned_size != artifact_size_int
            or str(
                payload.get("central_persistence_receipt_id_string") or ""
            ).strip()
            != receipt
        ):
            raise HostComfyArtifactWait(
                "host_comfy_artifact_ack_receipt_invalid", 2
            )
        return payload
    if response.status_code == 409:
        raise HostComfyReceiptMismatch("ack", payload)
    status = str(
        payload.get("status_string")
        or payload.get("error")
        or f"http_{response.status_code}"
    ).strip()
    raise HostComfyArtifactWait(
        f"host_comfy_artifact_ack_{status}",
        2 if response.status_code in {423, 429, 503} else 10,
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
