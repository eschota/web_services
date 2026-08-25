"""Durable admission broker for every GPU workload on the shared farm.

The converter scheduler, Renderfin and Freestock Gateway run in different
processes (and, for Freestock, on a different host).  A file mutex alone cannot
describe who owns a physical GPU.  This module provides a small authenticated
lease API backed by the AutoRig database.  Capacity waits are represented by a
retryable response and therefore never create/consume a product task attempt.

Priority is deliberately two-dimensional:

* AI Vision is the highest ordinary workload.
* AutoRig keeps a hard reserve of two *healthy* full converters while it has
  interactive demand.  AI may preempt AutoRig only above that reserve.
* Hunyuan, Comfy and collection work use remaining capacity and are recalled
  before interactive work.

The host process/arbiter remains responsible for stopping the exact child
process.  The broker only records ``preemption_requested``; heartbeats make the
request durable across tunnel loss and service restarts.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import (
    Task,
    WorkerEndpoint,
    WorkloadLease,
    WorkloadNodeState,
    WorkloadWaiter,
    get_db,
)
from fleet_admission import fleet_admission_lock


router = APIRouter(prefix="/api/workload-broker", tags=["workload-broker"])

ACTIVE_LEASE_STATES = ("active", "preemption_requested")
ACTIVE_WAITER_STATES = ("waiting",)
WORKLOAD_CLASS_AI = "ai_vision"
WORKLOAD_CLASS_AUTORIG = "autorig_interactive"
WORKLOAD_CLASS_HUNYUAN = "hunyuan"
WORKLOAD_CLASS_COMFY = "comfy"
WORKLOAD_CLASS_BACKGROUND = "collection_background"
WORKLOAD_CLASSES = {
    WORKLOAD_CLASS_AI,
    WORKLOAD_CLASS_AUTORIG,
    WORKLOAD_CLASS_HUNYUAN,
    WORKLOAD_CLASS_COMFY,
    WORKLOAD_CLASS_BACKGROUND,
}

# Lower is more important.  The hard AutoRig reserve is evaluated separately.
WORKLOAD_CLASS_PRIORITY = {
    WORKLOAD_CLASS_AI: 0,
    WORKLOAD_CLASS_AUTORIG: 10,
    # Image rendering wins over background 3D generation on the host arbiter.
    # Keeping the same ordering here prevents the two admission layers from
    # issuing contradictory preemption requests.
    WORKLOAD_CLASS_COMFY: 50,
    WORKLOAD_CLASS_HUNYUAN: 60,
    WORKLOAD_CLASS_BACKGROUND: 100,
}

_DEFAULT_PHYSICAL_ALIASES = {
    "raptor": "raptor",
    "ryzen-server": "raptor",
    "ryzen_server": "raptor",
}
RESERVE_ROLE_AI_PRIMARY = "ai_vision_primary"
RESERVE_ROLE_AUTORIG_PRIMARY = "autorig_primary"
RESERVE_ROLE_SHARED = "shared"
RESERVE_ROLE_MAINTENANCE = "maintenance"
RESERVE_ROLES = {
    RESERVE_ROLE_AI_PRIMARY,
    RESERVE_ROLE_AUTORIG_PRIMARY,
    RESERVE_ROLE_SHARED,
    "background_only",
    RESERVE_ROLE_MAINTENANCE,
}
_SECRET_KEY_RE = re.compile(r"token|secret|password|authorization|cookie|api.?key", re.I)
_STABLE_MACHINE_RE = re.compile(r"machine_[a-f0-9]{24,128}")


def _enabled() -> bool:
    return str(os.getenv("AUTORIG_WORKLOAD_BROKER_ENABLED", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }


def workload_broker_enabled() -> bool:
    return _enabled()


@asynccontextmanager
async def _admission_guard(already_locked: bool):
    if already_locked:
        yield
    else:
        async with fleet_admission_lock():
            yield


def _reserve_count() -> int:
    try:
        return max(0, min(16, int(os.getenv("AUTORIG_WORKLOAD_AUTORIG_RESERVE", "2"))))
    except ValueError:
        return 2


def _heartbeat_ttl_seconds() -> int:
    try:
        return max(30, min(900, int(os.getenv("AUTORIG_WORKLOAD_NODE_HEARTBEAT_TTL", "180"))))
    except ValueError:
        return 180


def _waiter_ttl_seconds() -> int:
    """Bound how long an absent claimant can retain its FIFO position.

    Waiting is attempt-neutral and clients refresh ``last_seen_at`` by
    replaying the same acquire request.  A crashed claimant must not leave a
    permanent high-priority tombstone at the head of the global queue.
    """
    try:
        return max(
            30,
            min(900, int(os.getenv("AUTORIG_WORKLOAD_WAITER_TTL", "90"))),
        )
    except ValueError:
        return 90


def _lease_ttl_seconds(value: Any) -> int:
    try:
        parsed = int(value or 300)
    except (TypeError, ValueError):
        parsed = 300
    return max(60, min(3600, parsed))


def _safe_identifier(value: Any, *, maximum: int = 240) -> str:
    return re.sub(r"[^a-zA-Z0-9_.:@/-]+", "_", str(value or "").strip())[:maximum]


def _physical_aliases() -> Dict[str, str]:
    aliases = dict(_DEFAULT_PHYSICAL_ALIASES)
    raw = str(os.getenv("AUTORIG_WORKLOAD_NODE_ALIASES_JSON", "") or "").strip()
    if not raw:
        return aliases
    try:
        configured = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return aliases
    if isinstance(configured, dict):
        for key, value in configured.items():
            source = _safe_identifier(key).lower()
            target = _safe_identifier(value).lower()
            if source and target:
                aliases[source] = target
    return aliases


def canonical_physical_resource_id(physical_resource_id: Any, node_id: Any = "") -> str:
    """Return one stable resource id; aliases cannot manufacture GPU slots."""
    raw = _safe_identifier(physical_resource_id or node_id).lower().strip("./")
    if not raw:
        return ""
    # Machine fingerprints are already stable and should win over display names.
    if len(raw) >= 24 and re.fullmatch(r"[a-f0-9:_-]+", raw):
        return raw
    if re.fullmatch(
        r"(?:raptor|ryzen-server|ryzen_server)(?:[-_:]?gpu[-_:]?0)",
        raw,
    ):
        return "raptor"
    return _physical_aliases().get(raw, raw)


def normalize_workload_class(value: Any) -> str:
    normalized = _safe_identifier(value, maximum=48).lower()
    return normalized if normalized in WORKLOAD_CLASSES else ""


def normalize_reserve_role(value: Any, *, missing: str = RESERVE_ROLE_SHARED) -> str:
    """Return the one fleet-wide role spelling used by hosts and schedulers.

    ``ai_primary`` existed briefly in the AutoRig admin API while the converter
    arbiter shipped ``ai_vision_primary``.  Accept the old spelling only as a
    migration alias; persisted/status output is always canonical.
    """
    raw = _safe_identifier(value, maximum=32).lower()
    if not raw:
        raw = missing
    if raw == "ai_primary":
        raw = RESERVE_ROLE_AI_PRIMARY
    return raw if raw in RESERVE_ROLES else RESERVE_ROLE_MAINTENANCE


def reserve_role_rank(workload_class: str, reserve_role: Any) -> int:
    """Prefer a workload's home role while allowing idle-capacity borrowing."""
    role = normalize_reserve_role(reserve_role)
    if role in {RESERVE_ROLE_MAINTENANCE, "background_only"}:
        return 100
    if workload_class == WORKLOAD_CLASS_AI:
        order = (
            RESERVE_ROLE_AI_PRIMARY,
            RESERVE_ROLE_SHARED,
            RESERVE_ROLE_AUTORIG_PRIMARY,
        )
    elif workload_class == WORKLOAD_CLASS_AUTORIG:
        order = (
            RESERVE_ROLE_AUTORIG_PRIMARY,
            RESERVE_ROLE_SHARED,
            RESERVE_ROLE_AI_PRIMARY,
        )
    else:
        # Background/render work borrows neutral capacity first and protects
        # both latency-sensitive role pools from avoidable occupation.
        order = (
            RESERVE_ROLE_SHARED,
            RESERVE_ROLE_AUTORIG_PRIMARY,
            RESERVE_ROLE_AI_PRIMARY,
        )
    try:
        return order.index(role)
    except ValueError:
        return 100


def _redacted_json_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 5:
        return "[truncated]"
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for key, child in list(value.items())[:100]:
            key_string = str(key)[:120]
            result[key_string] = "[redacted]" if _SECRET_KEY_RE.search(key_string) else _redacted_json_value(child, depth=depth + 1)
        return result
    if isinstance(value, list):
        return [_redacted_json_value(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value if not isinstance(value, str) else value[:1000]
    return str(value)[:1000]


def _dict_at(payload: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _node_status_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    nested = payload.get("node_status_by_key")
    if isinstance(nested, dict) and nested:
        return nested
    # Bootstrap compatibility for early Freestock agents which posted their
    # readiness fields at the heartbeat root.
    return {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "node_id_string",
            "physical_resource_id_string",
            "workload_class_string",
            "owner_service_string",
            "owner_task_id_string",
            "request_id_string",
            "priority_int",
            "ttl_seconds_int",
            "metadata_by_key",
            "source_scope_string",
        }
    }


def _bool_from(payload: Dict[str, Any], names: Iterable[str], default: bool = False) -> bool:
    for name in names:
        if name not in payload:
            continue
        value = payload.get(name)
        if isinstance(value, bool):
            return value
        if str(value or "").strip().lower() in {"1", "true", "yes", "on", "ready", "online", "healthy", "idle"}:
            return True
        if str(value or "").strip().lower() in {"0", "false", "no", "off", "error", "offline", "unhealthy"}:
            return False
    return default


def node_state_from_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    capability = _dict_at(payload, "capability_by_key")
    ai_status = _dict_at(payload, "ai_vision_by_key") or _dict_at(capability, "ai_vision_by_key")
    arbiter = _dict_at(payload, "arbiter_by_key") or _dict_at(capability, "arbiter_by_key")
    reported_node_kind = _safe_identifier(
        payload.get("node_kind_string") or "", maximum=48
    ).lower()
    full_converter = _bool_from(
        payload,
        ("full_converter_bool", "is_full_converter_bool"),
        _bool_from(capability, ("full_converter_bool", "converter_bool"), False),
    )
    managed_farm = (
        _bool_from(
            payload,
            ("managed_farm_bool", "farm_managed_bool"),
            _bool_from(capability, ("managed_farm_bool",), False),
        )
        or reported_node_kind == "managed_farm"
    )
    freestock_runtime_ready = all(
        _bool_from(payload, (key,), False)
        for key in ("runtime_ready_bool", "model_ready_bool", "ffmpeg_ready_bool")
    )
    ai_capable = _bool_from(
        payload,
        ("ai_capable_bool", "ai_vision_ready_bool"),
        _bool_from(
            ai_status,
            ("ready_bool", "available_bool", "installed_bool"),
            freestock_runtime_ready,
        ),
    )
    healthy = _bool_from(
        payload,
        ("healthy_bool", "online_bool", "worker_healthy_bool"),
        _bool_from(capability, ("healthy_bool",), True if payload else False),
    )
    accepting = _bool_from(
        payload,
        ("accepting_bool", "accepting_work_bool", "task_acceptance_allowed_bool"),
        _bool_from(
            arbiter,
            ("accepting_ai_vision_bool", "accepting_work_bool", "lease_available_bool"),
            _bool_from(payload, ("arbiter_accepting_ai_vision_bool",), False),
        ),
    )
    reported_role = (
        payload.get("reserve_role_string")
        or payload.get("workload_role")
        or payload.get("workload_role_string")
        or arbiter.get("workload_role")
        or arbiter.get("workload_role_string")
    )
    # An older desktop client may omit a role and retain the historical shared
    # policy. Managed farm capacity is different: only an explicit canonical
    # role may make it healthy/accepting, otherwise it is maintenance fail-closed.
    reserve_role = normalize_reserve_role(
        reported_role,
        missing=(RESERVE_ROLE_MAINTENANCE if managed_farm else RESERVE_ROLE_SHARED),
    )
    maintenance = _bool_from(payload, ("maintenance_bool", "maintenance"), False)
    if maintenance or reserve_role == RESERVE_ROLE_MAINTENANCE:
        healthy = False
        accepting = False
    # Managed farm AI admission is fail-closed unless the host arbiter is
    # explicitly online.  Desktop clients are handled by their legacy policy.
    if managed_farm and ai_capable:
        arbiter_online = _bool_from(
            arbiter,
            ("online_bool", "healthy_bool", "available_bool"),
            _bool_from(payload, ("arbiter_ready_bool",), False),
        )
        accepting = accepting and arbiter_online
    return {
        "node_kind": reported_node_kind or ("managed_farm" if managed_farm else "desktop"),
        "full_converter": full_converter,
        "ai_capable": ai_capable,
        "managed_farm": managed_farm,
        "healthy": healthy,
        "accepting": accepting,
        "reserve_role": reserve_role,
        "status_json": json.dumps(_redacted_json_value(payload), ensure_ascii=False, separators=(",", ":")),
    }


_BROKER_PRINCIPAL_TOKEN_ENV = {
    "gateway": "AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN",
    "renderfin": "AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN",
    "host_agent": "AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN",
    "admin": "AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN",
}


def _legacy_token_enabled() -> bool:
    return str(
        os.getenv("AUTORIG_WORKLOAD_BROKER_ALLOW_LEGACY_TOKEN", "0") or "0"
    ).strip().lower() in {"1", "true", "yes", "on"}


def _configured_broker_principals() -> Tuple[Dict[str, str], bool]:
    configured = {
        principal: str(os.getenv(env_name, "") or "").strip()
        for principal, env_name in _BROKER_PRINCIPAL_TOKEN_ENV.items()
    }
    configured = {
        principal: token
        for principal, token in configured.items()
        if len(token) >= 20
    }
    if _legacy_token_enabled():
        legacy = str(os.getenv("AUTORIG_WORKLOAD_BROKER_TOKEN", "") or "").strip()
        if len(legacy) >= 20:
            configured["legacy"] = legacy
    values = list(configured.values())
    return configured, len(values) != len(set(values))


def _broker_auth_principal(request: Request) -> Tuple[str, Optional[JSONResponse]]:
    if not _enabled():
        return "", JSONResponse(
            status_code=503,
            content={"status_string": "disabled", "retryable_bool": True},
        )
    configured, ambiguous = _configured_broker_principals()
    if ambiguous:
        return "", JSONResponse(
            status_code=503,
            content={
                "status_string": "credential_configuration_invalid",
                "retryable_bool": True,
            },
        )
    if not configured:
        return "", JSONResponse(
            status_code=503,
            content={
                "status_string": "scoped_tokens_not_configured",
                "retryable_bool": True,
            },
        )
    authorization = str(request.headers.get("authorization") or "")
    supplied = (
        authorization.split(" ", 1)[1].strip()
        if authorization.lower().startswith("bearer ")
        else ""
    )
    matched = ""
    for principal, token in configured.items():
        if supplied and hmac.compare_digest(token, supplied):
            matched = principal
    if not matched:
        return "", JSONResponse(
            status_code=401, content={"status_string": "unauthorized"}
        )
    return matched, None


def _principal_allows_payload(
    principal: str, action: str, payload: Dict[str, Any]
) -> bool:
    if principal == "legacy":
        return True
    owner_service = _safe_identifier(
        payload.get("owner_service_string"), maximum=120
    ).lower()
    workload_class = normalize_workload_class(payload.get("workload_class_string"))
    source_scope = _safe_identifier(
        payload.get("source_scope_string"), maximum=64
    ).lower()
    if action == "status":
        return principal == "admin"
    if action == "node_heartbeat":
        return bool(
            (principal == "gateway" and source_scope == "lease_probe")
            or (principal == "renderfin" and source_scope == "renderfin_probe")
            or (principal == "host_agent" and source_scope == "host_agent")
        )
    if principal == "gateway":
        if owner_service != "freestock_gateway":
            return False
        return action != "acquire" or workload_class == WORKLOAD_CLASS_AI
    if principal == "renderfin":
        if owner_service != "renderfin":
            return False
        return action != "acquire" or workload_class in {
            WORKLOAD_CLASS_COMFY,
            WORKLOAD_CLASS_HUNYUAN,
            WORKLOAD_CLASS_BACKGROUND,
        }
    return False


def _principal_error(
    request: Request, action: str, payload: Dict[str, Any]
) -> Tuple[str, Optional[JSONResponse]]:
    principal, error = _broker_auth_principal(request)
    if error is not None:
        return "", error
    if not _principal_allows_payload(principal, action, payload):
        return "", JSONResponse(
            status_code=403,
            content={
                "status_string": "credential_scope_forbidden",
                "retryable_bool": False,
            },
        )
    return principal, None


async def _expire_leases(db: AsyncSession, now: datetime) -> int:
    expired_result = await db.execute(
        select(WorkloadLease.lease_id).where(
            WorkloadLease.state.in_(ACTIVE_LEASE_STATES),
            WorkloadLease.expires_at <= now,
        )
    )
    expired_ids = [str(value) for value in expired_result.scalars().all()]
    result = await db.execute(
        update(WorkloadLease)
        .where(WorkloadLease.state.in_(ACTIVE_LEASE_STATES), WorkloadLease.expires_at <= now)
        .values(state="expired", released_at=now, updated_at=now)
    )
    if expired_ids:
        await db.execute(
            update(WorkloadWaiter)
            .where(
                WorkloadWaiter.lease_id.in_(expired_ids),
                WorkloadWaiter.state == "acquired",
            )
            .values(state="expired", terminal_at=now, updated_at=now)
        )
    return int(result.rowcount or 0)


async def _expire_waiters(db: AsyncSession, now: datetime) -> int:
    """Retire claimants which stopped polling without consuming attempts."""
    cutoff = now - timedelta(seconds=_waiter_ttl_seconds())
    result = await db.execute(
        update(WorkloadWaiter)
        .where(
            WorkloadWaiter.state == "waiting",
            WorkloadWaiter.last_seen_at < cutoff,
        )
        .values(
            state="abandoned",
            terminal_at=now,
            updated_at=now,
        )
    )
    return int(result.rowcount or 0)


async def _expire_admission_state(db: AsyncSession, now: datetime) -> None:
    await _expire_leases(db, now)
    await _expire_waiters(db, now)


def _waiter_order(waiter: WorkloadWaiter) -> tuple:
    return (
        WORKLOAD_CLASS_PRIORITY.get(str(waiter.workload_class or ""), 999),
        int(waiter.priority or 0),
        waiter.created_at or datetime.min,
        str(waiter.request_id or ""),
    )


def _node_supports(node: WorkloadNodeState, workload_class: str) -> bool:
    if not bool(node.healthy):
        return False
    if workload_class == WORKLOAD_CLASS_AI:
        return bool(node.ai_capable)
    if workload_class == WORKLOAD_CLASS_AUTORIG:
        return bool(node.full_converter)
    return True


def _preferred_idle_node(
    *,
    current: WorkloadNodeState,
    workload_class: str,
    nodes: Iterable[WorkloadNodeState],
    active_by_resource: Dict[str, WorkloadLease],
) -> Optional[WorkloadNodeState]:
    """Find a strictly better idle home-role node for targeted admission.

    Callers still choose a concrete host, but the broker is the last admission
    authority. Rejecting a worse borrowed role while a better compatible node
    is idle prevents stale client scoring from defeating the fleet partition.
    Busy home-role capacity does not block borrowing; higher-priority waiters
    and normal preemption rules handle that case.
    """
    current_rank = reserve_role_rank(workload_class, current.reserve_role)
    candidates = [
        node
        for node in nodes
        if str(node.physical_resource_id) != str(current.physical_resource_id)
        and _node_supports(node, workload_class)
        and bool(node.accepting)
        and str(node.physical_resource_id) not in active_by_resource
        and reserve_role_rank(workload_class, node.reserve_role) < current_rank
    ]
    candidates.sort(
        key=lambda node: (
            reserve_role_rank(workload_class, node.reserve_role),
            str(node.physical_resource_id),
        )
    )
    return candidates[0] if candidates else None


async def _upsert_waiter(
    db: AsyncSession,
    *,
    request_id: str,
    physical_resource_id: str,
    node_id: str,
    workload_class: str,
    priority: int,
    owner_service: str,
    owner_task_id: str,
    metadata_json: str,
    now: datetime,
) -> Tuple[Optional[WorkloadWaiter], Optional[Tuple[int, Dict[str, Any]]]]:
    waiter = await db.get(WorkloadWaiter, request_id)
    if waiter is None:
        waiter = WorkloadWaiter(
            request_id=request_id,
            physical_resource_id=physical_resource_id,
            node_id=node_id,
            workload_class=workload_class,
            priority=priority,
            owner_service=owner_service,
            owner_task_id=owner_task_id,
            state="waiting",
            metadata_json=metadata_json,
            created_at=now,
            last_seen_at=now,
            updated_at=now,
        )
        db.add(waiter)
        await db.flush()
        return waiter, None
    same_owner = (
        waiter.workload_class == workload_class
        and waiter.owner_service == owner_service
        and waiter.owner_task_id == owner_task_id
    )
    if not same_owner:
        return None, (409, {"status_string": "request_id_conflict", "error_code_string": "request_id_conflict", "retryable_bool": False})
    if waiter.state == "abandoned":
        # No lease was ever issued for a waiting tombstone.  The exact same
        # logical claimant may safely rejoin FIFO after a long pause without a
        # new task attempt or request id.
        waiter.state = "waiting"
        waiter.lease_id = None
        waiter.assigned_at = None
        waiter.terminal_at = None
        waiter.created_at = now
        waiter.last_seen_at = now
        waiter.updated_at = now
        waiter.physical_resource_id = physical_resource_id
        waiter.node_id = node_id
        waiter.priority = priority
        waiter.metadata_json = metadata_json
        await db.flush()
        return waiter, None
    if waiter.state not in {"waiting", "acquired"}:
        return waiter, (409, {
            "status_string": "request_already_terminal",
            "error_code_string": "request_already_terminal",
            "retryable_bool": False,
        })
    if waiter.state == "waiting":
        # A durable waiter may try another compatible node without creating a
        # second queue entry.  Once acquired, physical identity is immutable.
        waiter.physical_resource_id = physical_resource_id
        waiter.node_id = node_id
        waiter.priority = priority
        waiter.metadata_json = metadata_json
    waiter.last_seen_at = now
    waiter.updated_at = now
    await db.flush()
    return waiter, None


async def _blocking_waiter(
    db: AsyncSession,
    *,
    current: WorkloadWaiter,
    node: WorkloadNodeState,
    allow_reserve_admission: bool = False,
) -> Optional[WorkloadWaiter]:
    result = await db.execute(
        select(WorkloadWaiter).where(WorkloadWaiter.state.in_(ACTIVE_WAITER_STATES))
    )
    waiters = sorted(result.scalars().all(), key=_waiter_order)
    current_rank = WORKLOAD_CLASS_PRIORITY.get(str(current.workload_class or ""), 999)
    current_priority = int(current.priority or 0)
    current_resource = str(current.physical_resource_id or "")
    for waiter in waiters:
        if waiter.request_id == current.request_id:
            return None
        waiter_workload = str(waiter.workload_class or "")
        if not _node_supports(node, waiter_workload):
            continue
        waiter_rank = WORKLOAD_CLASS_PRIORITY.get(waiter_workload, 999)
        waiter_priority = int(waiter.priority or 0)
        if waiter_rank < current_rank or (
            waiter_rank == current_rank and waiter_priority < current_priority
        ):
            if allow_reserve_admission and waiter_rank < current_rank:
                # AI Vision remains fleet-wide first above the guaranteed
                # AutoRig reserve.  It must not, however, turn a node-pinned
                # durable waiter into a global barrier that prevents an
                # interactive task from establishing either reserved slot.
                continue
            # A genuinely higher-priority request may use any compatible GPU
            # and therefore blocks lower work fleet-wide.
            return waiter
        if (
            waiter_rank == current_rank
            and waiter_priority == current_priority
            and str(waiter.physical_resource_id or "") == current_resource
        ):
            # FIFO within one class is resource-scoped.  Otherwise four idle
            # AI-capable GPUs would be serialized behind one busy target.
            return waiter
    return None


async def _interactive_demand(db: AsyncSession) -> int:
    result = await db.execute(
        select(func.count(Task.id)).where(
            Task.queue_class == "interactive",
            or_(Task.pipeline_kind.is_(None), Task.pipeline_kind != "generate"),
            Task.status.in_(("created", "processing")),
        )
    )
    return int(result.scalar() or 0)


async def _fresh_nodes(db: AsyncSession, now: datetime) -> list[WorkloadNodeState]:
    cutoff = now - timedelta(seconds=_heartbeat_ttl_seconds())
    result = await db.execute(
        select(WorkloadNodeState).where(
            WorkloadNodeState.heartbeat_at >= cutoff,
            WorkloadNodeState.healthy.is_(True),
        )
    )
    return list(result.scalars().all())


async def _active_leases(db: AsyncSession) -> list[WorkloadLease]:
    result = await db.execute(select(WorkloadLease).where(WorkloadLease.state.in_(ACTIVE_LEASE_STATES)))
    return list(result.scalars().all())


def _autorig_usable_full_count(
    nodes: Iterable[WorkloadNodeState],
    active_by_resource: Dict[str, WorkloadLease],
    *,
    simulated_unavailable_resource: str = "",
) -> int:
    count = 0
    for node in nodes:
        if not bool(node.full_converter):
            continue
        resource = str(node.physical_resource_id or "")
        if resource == simulated_unavailable_resource:
            continue
        lease = active_by_resource.get(resource)
        if lease is not None:
            if lease.workload_class == WORKLOAD_CLASS_AI:
                # AI Vision is the only ordinary active workload that AutoRig
                # cannot recall.  A busy host advertises accepting=false, but
                # lower-priority leases remain exact-preemptible capacity.
                continue
            count += 1
            continue
        if not bool(node.accepting):
            continue
        count += 1
    return count


def _active_autorig_full_count(
    nodes: Iterable[WorkloadNodeState],
    active_by_resource: Dict[str, WorkloadLease],
) -> int:
    full_resources = {
        str(node.physical_resource_id or "")
        for node in nodes
        if bool(node.full_converter)
    }
    return sum(
        1
        for resource, lease in active_by_resource.items()
        if resource in full_resources
        and lease.workload_class == WORKLOAD_CLASS_AUTORIG
        and lease.state == "active"
    )


def _accepting_ai_for_resource(
    node: WorkloadNodeState,
    *,
    now: datetime,
    nodes: Iterable[WorkloadNodeState],
    active_by_resource: Dict[str, WorkloadLease],
    interactive_demand: int,
    reserve: int,
) -> bool:
    authority_cutoff = now - timedelta(seconds=_heartbeat_ttl_seconds())
    if not (
        node.healthy
        and node.ai_capable
        and node.authority_heartbeat_at is not None
        and node.authority_heartbeat_at >= authority_cutoff
    ):
        return False
    resource = str(node.physical_resource_id or "")
    active = active_by_resource.get(resource)
    if active is not None:
        if active.state != "active":
            return False
        active_rank = WORKLOAD_CLASS_PRIORITY.get(active.workload_class, 999)
        if active_rank <= WORKLOAD_CLASS_PRIORITY[WORKLOAD_CLASS_AI]:
            return False
        if active.workload_class == WORKLOAD_CLASS_AUTORIG and interactive_demand > 0:
            return _autorig_usable_full_count(
                nodes,
                active_by_resource,
                simulated_unavailable_resource=resource,
            ) >= reserve
        return True
    if not node.accepting:
        return False
    if node.full_converter and interactive_demand > 0:
        return _autorig_usable_full_count(
            nodes,
            active_by_resource,
            simulated_unavailable_resource=resource,
        ) >= reserve
    return True


def _lease_payload(lease: WorkloadLease) -> Dict[str, Any]:
    expires = lease.expires_at.isoformat() + "Z" if lease.expires_at else ""
    return {
        "lease_id_string": str(lease.lease_id),
        "request_id_string": str(lease.request_id),
        "physical_resource_id_string": str(lease.physical_resource_id),
        "node_id_string": str(lease.node_id),
        "workload_class_string": str(lease.workload_class),
        "priority_int": int(lease.priority or 0),
        "owner_service_string": str(lease.owner_service),
        "owner_task_id_string": str(lease.owner_task_id),
        "state_string": str(lease.state),
        "preemption_reason_string": str(lease.preemption_reason or ""),
        "acquired_at_string": lease.acquired_at.isoformat() + "Z" if lease.acquired_at else "",
        "heartbeat_at_string": lease.heartbeat_at.isoformat() + "Z" if lease.heartbeat_at else "",
        "expires_at_string": expires,
        # Freestock bootstrap aliases; canonical clients use expires_at_string.
        "lease_expires_at_utc_timestamp": expires,
        "expires_at_utc_timestamp": expires,
    }


def _exact_lease_owner_matches(
    lease: WorkloadLease, payload: Dict[str, Any]
) -> bool:
    """A shared bearer token never substitutes for per-lease ownership."""
    owner_service = _safe_identifier(payload.get("owner_service_string"), maximum=120)
    owner_task_id = _safe_identifier(payload.get("owner_task_id_string"))
    request_id = _safe_identifier(payload.get("request_id_string"), maximum=128)
    return bool(owner_service and owner_task_id and request_id) and (
        owner_service == lease.owner_service
        and owner_task_id == lease.owner_task_id
        and request_id == lease.request_id
    )


async def _upsert_node(
    db: AsyncSession,
    *,
    physical_resource_id: str,
    node_id: str,
    node_status: Dict[str, Any],
    now: datetime,
    source_scope: str = "host_agent",
) -> WorkloadNodeState:
    parsed = node_state_from_status(node_status)
    node = await db.get(WorkloadNodeState, physical_resource_id)
    created = node is None
    if node is None:
        node = WorkloadNodeState(
            physical_resource_id=physical_resource_id,
            node_id=node_id,
            created_at=now,
        )
        db.add(node)
    node.node_id = node_id
    source = _safe_identifier(source_scope, maximum=64).lower() or "host_agent"
    probe_only = source in {"renderfin_probe", "autorig_worker_probe", "lease_probe"}
    if created or not probe_only:
        node.node_kind = parsed["node_kind"]
        node.full_converter = bool(parsed["full_converter"])
        node.ai_capable = bool(parsed["ai_capable"])
        node.managed_farm = bool(parsed["managed_farm"])
        node.healthy = bool(parsed["healthy"])
        node.accepting = bool(parsed["accepting"])
        node.reserve_role = parsed["reserve_role"]
    else:
        # A workload-specific probe may update transport availability, but it
        # cannot erase capability/arbiter facts registered by the host agent.
        authority_fresh = bool(
            node.authority_heartbeat_at
            and node.authority_heartbeat_at
            >= now - timedelta(seconds=_heartbeat_ttl_seconds())
        )
        if not authority_fresh:
            node.healthy = bool(parsed["healthy"])
            node.accepting = bool(parsed["accepting"])
    try:
        previous_status = json.loads(node.status_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        previous_status = {}
    if not isinstance(previous_status, dict):
        previous_status = {}
    snapshots = previous_status.get("_source_snapshots_by_key")
    if not isinstance(snapshots, dict):
        snapshots = {}
    snapshots[source] = _redacted_json_value(node_status)
    previous_status["_source_snapshots_by_key"] = snapshots
    previous_status["last_source_scope_string"] = source
    node.status_json = json.dumps(
        _redacted_json_value(previous_status),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    node.heartbeat_at = now
    if not probe_only:
        node.authority_source = source
        node.authority_heartbeat_at = now
    node.updated_at = now
    await db.flush()
    return node


async def _mark_preemption(lease: WorkloadLease, *, reason: str, now: datetime) -> None:
    if lease.state != "preemption_requested":
        lease.state = "preemption_requested"
        lease.preemption_requested_at = now
        lease.preemption_reason = reason[:240]
        lease.updated_at = now


async def _reconcile_autorig_reserve(db: AsyncSession, now: datetime) -> int:
    demand = await _interactive_demand(db)
    if demand <= 0:
        return 0
    nodes = await _fresh_nodes(db, now)
    leases = await _active_leases(db)
    active_by_resource = {str(lease.physical_resource_id): lease for lease in leases}
    full_nodes = [
        node for node in nodes
        if bool(node.full_converter)
        and (
            bool(node.accepting)
            or active_by_resource.get(str(node.physical_resource_id)) is not None
        )
    ]
    reserve = min(_reserve_count(), len(full_nodes))
    if reserve <= 0:
        return 0
    usable = _autorig_usable_full_count(full_nodes, active_by_resource)
    missing = max(0, reserve - usable)
    if missing <= 0:
        return 0
    victims = [
        lease for lease in leases
        if lease.physical_resource_id in {node.physical_resource_id for node in full_nodes}
        and lease.workload_class != WORKLOAD_CLASS_AUTORIG
        and lease.state == "active"
    ]
    role_by_resource = {
        str(node.physical_resource_id): node.reserve_role for node in full_nodes
    }
    # Restore an AutoRig-primary slot before recalling borrowed work from the
    # AI-primary pool. Within the same role, lowest-priority/newest work yields
    # first. AI remains recallable only when required by the hard reserve.
    victims.sort(
        key=lambda lease: (
            reserve_role_rank(
                WORKLOAD_CLASS_AUTORIG,
                role_by_resource.get(str(lease.physical_resource_id), "shared"),
            ),
            -WORKLOAD_CLASS_PRIORITY.get(lease.workload_class, 999),
            -int(lease.priority or 0),
            -(lease.acquired_at.timestamp() if lease.acquired_at else 0.0),
        )
    )
    for lease in victims[:missing]:
        await _mark_preemption(lease, reason="autorig_reserve_recall", now=now)
    return min(missing, len(victims))


async def acquire_lease(
    db: AsyncSession,
    payload: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    admission_locked: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    now = now or datetime.utcnow()
    workload_class = normalize_workload_class(payload.get("workload_class_string"))
    node_id = _safe_identifier(payload.get("node_id_string"))
    physical_resource_id = canonical_physical_resource_id(payload.get("physical_resource_id_string"), node_id)
    owner_service = _safe_identifier(payload.get("owner_service_string"), maximum=120)
    owner_task_id = _safe_identifier(payload.get("owner_task_id_string"))
    request_id = _safe_identifier(payload.get("request_id_string"), maximum=128)
    if not all((workload_class, node_id, physical_resource_id, owner_service, owner_task_id, request_id)):
        return 400, {"status_string": "invalid_request", "retryable_bool": False}
    try:
        requested_priority = max(0, min(1000, int(payload.get("priority_int") or 0)))
    except (TypeError, ValueError):
        requested_priority = 0
    ttl = _lease_ttl_seconds(payload.get("ttl_seconds_int"))
    node_status = _node_status_payload(payload)
    metadata_json = json.dumps(
        _redacted_json_value(
            payload.get("metadata_by_key")
            if isinstance(payload.get("metadata_by_key"), dict)
            else {}
        ),
        ensure_ascii=False,
        separators=(",", ":"),
    )

    async with _admission_guard(admission_locked):
        await _expire_admission_state(db, now)
        existing_request_result = await db.execute(select(WorkloadLease).where(WorkloadLease.request_id == request_id))
        existing_request = existing_request_result.scalar_one_or_none()
        if existing_request is not None:
            same_owner = (
                existing_request.physical_resource_id == physical_resource_id
                and existing_request.workload_class == workload_class
                and existing_request.owner_service == owner_service
                and existing_request.owner_task_id == owner_task_id
            )
            if not same_owner:
                return 409, {"status_string": "request_id_conflict", "retryable_bool": False}
            if existing_request.state == "active":
                existing_node = await db.get(
                    WorkloadNodeState, existing_request.physical_resource_id
                )
                if (
                    existing_node is None
                    or existing_node.heartbeat_at
                    < now - timedelta(seconds=_heartbeat_ttl_seconds())
                ):
                    await db.commit()
                    return 423, {
                        "status_string": "node_heartbeat_required",
                        "error_code_string": "node_heartbeat_required",
                        "retryable_bool": True,
                        "retry_after_seconds_int": 5,
                        "lease_by_key": _lease_payload(existing_request),
                    }
                existing_request.heartbeat_at = now
                existing_request.expires_at = now + timedelta(seconds=ttl)
                existing_request.updated_at = now
                existing_waiter = await db.get(WorkloadWaiter, request_id)
                if existing_waiter is not None:
                    existing_waiter.last_seen_at = now
                    existing_waiter.updated_at = now
                await db.commit()
                return 200, {"status_string": "renewed", "lease_by_key": _lease_payload(existing_request)}
            if existing_request.state == "preemption_requested":
                await db.commit()
                return 423, {
                    "status_string": "preemption_requested",
                    "retryable_bool": True,
                    "retry_after_seconds_int": 2,
                    "lease_by_key": _lease_payload(existing_request),
                }
            return 409, {"status_string": "request_already_terminal", "error_code_string": "request_already_terminal", "retryable_bool": False, "lease_by_key": _lease_payload(existing_request)}

        node = await db.get(WorkloadNodeState, physical_resource_id)
        if node is None or node.heartbeat_at < now - timedelta(seconds=_heartbeat_ttl_seconds()):
            await db.commit()
            return 423, {
                "status_string": "node_heartbeat_required",
                "error_code_string": "node_heartbeat_required",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
            }
        if workload_class == WORKLOAD_CLASS_AI and (
            not node.authority_heartbeat_at
            or node.authority_heartbeat_at
            < now - timedelta(seconds=_heartbeat_ttl_seconds())
        ):
            await db.commit()
            return 423, {
                "status_string": "authoritative_heartbeat_required",
                "error_code_string": "authoritative_heartbeat_required",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
            }
        waiter, waiter_error = await _upsert_waiter(
            db,
            request_id=request_id,
            physical_resource_id=physical_resource_id,
            node_id=node_id,
            workload_class=workload_class,
            priority=requested_priority,
            owner_service=owner_service,
            owner_task_id=owner_task_id,
            metadata_json=metadata_json,
            now=now,
        )
        if waiter_error is not None:
            await db.commit()
            return waiter_error
        assert waiter is not None
        if not node.healthy:
            await db.commit()
            return 423, {
                "status_string": "node_not_accepting",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
            }
        if workload_class == WORKLOAD_CLASS_AI and not node.ai_capable:
            await db.commit()
            return 423, {
                "status_string": "ai_runtime_not_ready",
                "retryable_bool": True,
                "retry_after_seconds_int": 10,
            }

        active_result = await db.execute(
            select(WorkloadLease).where(
                WorkloadLease.physical_resource_id == physical_resource_id,
                WorkloadLease.state.in_(ACTIVE_LEASE_STATES),
            ).order_by(WorkloadLease.acquired_at.asc())
        )
        active = active_result.scalars().first()
        demand = await _interactive_demand(db)
        fresh_nodes = await _fresh_nodes(db, now)
        leases = await _active_leases(db)
        active_by_resource = {str(lease.physical_resource_id): lease for lease in leases}
        reserve_population = [
            item for item in fresh_nodes
            if item.full_converter
            and (
                item.accepting
                or active_by_resource.get(str(item.physical_resource_id)) is not None
            )
        ]
        reserve = min(_reserve_count(), len(reserve_population))
        reserve_admission_target = min(reserve, demand)
        reserve_admission = (
            workload_class == WORKLOAD_CLASS_AUTORIG
            and bool(node.full_converter)
            and reserve_admission_target > 0
            and _active_autorig_full_count(fresh_nodes, active_by_resource)
            < reserve_admission_target
        )

        preferred_idle = _preferred_idle_node(
            current=node,
            workload_class=workload_class,
            nodes=fresh_nodes,
            active_by_resource=active_by_resource,
        )
        if preferred_idle is not None:
            await db.commit()
            return 423, {
                "status_string": "preferred_role_available",
                "error_code_string": "preferred_role_available",
                "retryable_bool": True,
                "retry_after_seconds_int": 1,
                "preferred_physical_resource_id_string": str(
                    preferred_idle.physical_resource_id
                ),
                "preferred_reserve_role_string": normalize_reserve_role(
                    preferred_idle.reserve_role
                ),
            }

        blocker = await _blocking_waiter(
            db,
            current=waiter,
            node=node,
            allow_reserve_admission=reserve_admission,
        )
        if blocker is not None:
            await db.commit()
            return 423, {
                "status_string": "higher_priority_waiting",
                "error_code_string": "higher_priority_waiting",
                "retryable_bool": True,
                "retry_after_seconds_int": 2,
                "blocking_request_id_string": str(blocker.request_id),
                "blocking_workload_class_string": str(blocker.workload_class),
            }

        if active is not None:
            incoming_rank = WORKLOAD_CLASS_PRIORITY[workload_class]
            active_rank = WORKLOAD_CLASS_PRIORITY.get(active.workload_class, 999)
            incoming_order = (incoming_rank, requested_priority)
            active_order = (active_rank, int(active.priority or 0))
            if active.state == "preemption_requested" and incoming_order < active_order:
                await db.commit()
                return 423, {
                    "status_string": "preemption_requested",
                    "retryable_bool": True,
                    "retry_after_seconds_int": 2,
                    "victim_lease_by_key": _lease_payload(active),
                }
            # Gateway urgency is a strict sub-priority inside AI Vision:
            # p0 can recall p90, while p90 can never recall p0. The same tuple
            # is already used for durable waiter ordering, so queued and active
            # admission cannot contradict each other.
            may_preempt = incoming_order < active_order
            if workload_class == WORKLOAD_CLASS_AI and active.workload_class == WORKLOAD_CLASS_AUTORIG and demand > 0:
                other_usable = _autorig_usable_full_count(
                    fresh_nodes,
                    active_by_resource,
                    simulated_unavailable_resource=physical_resource_id,
                )
                may_preempt = other_usable >= reserve
            if workload_class == WORKLOAD_CLASS_AUTORIG and active.workload_class == WORKLOAD_CLASS_AI:
                usable = _autorig_usable_full_count(fresh_nodes, active_by_resource)
                may_preempt = demand > 0 and usable < reserve
            if may_preempt and active.state == "active":
                await _mark_preemption(active, reason=f"higher_priority_{workload_class}", now=now)
                await db.commit()
                return 423, {
                    "status_string": "preemption_requested",
                    "retryable_bool": True,
                    "retry_after_seconds_int": 2,
                    "victim_lease_by_key": _lease_payload(active),
                }
            await db.commit()
            return 423, {
                "status_string": "gpu_busy",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
                "active_lease_by_key": _lease_payload(active),
            }

        if not node.accepting:
            await db.commit()
            return 423, {
                "status_string": "node_not_accepting",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
            }

        if node.full_converter and workload_class != WORKLOAD_CLASS_AUTORIG and demand > 0:
            usable_after = _autorig_usable_full_count(
                fresh_nodes,
                active_by_resource,
                simulated_unavailable_resource=physical_resource_id,
            )
            if usable_after < reserve:
                await db.commit()
                return 423, {
                    "status_string": "autorig_reserve",
                    "retryable_bool": True,
                    "retry_after_seconds_int": 5,
                    "reserve_slots_int": reserve,
                    "usable_after_grant_int": usable_after,
                }

        lease = WorkloadLease(
            lease_id=f"wl_{secrets.token_urlsafe(24)}",
            request_id=request_id,
            physical_resource_id=physical_resource_id,
            node_id=node_id,
            workload_class=workload_class,
            priority=requested_priority,
            owner_service=owner_service,
            owner_task_id=owner_task_id,
            state="active",
            metadata_json=metadata_json,
            acquired_at=now,
            heartbeat_at=now,
            expires_at=now + timedelta(seconds=ttl),
            created_at=now,
            updated_at=now,
        )
        db.add(lease)
        await db.flush()
        waiter.state = "acquired"
        waiter.lease_id = lease.lease_id
        waiter.assigned_at = now
        waiter.last_seen_at = now
        waiter.updated_at = now
        await _reconcile_autorig_reserve(db, now)
        await db.commit()
        return 200, {
            "status_string": "acquired",
            "retryable_bool": False,
            "lease_by_key": _lease_payload(lease),
        }


async def heartbeat_lease(
    db: AsyncSession,
    lease_id: str,
    payload: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    admission_locked: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    now = now or datetime.utcnow()
    async with _admission_guard(admission_locked):
        await _expire_admission_state(db, now)
        lease = await db.get(WorkloadLease, lease_id)
        if lease is None:
            await db.commit()
            return 404, {"status_string": "lease_not_found", "retryable_bool": False}
        if not _exact_lease_owner_matches(lease, payload):
            await db.commit()
            return 409, {
                "status_string": "lease_owner_mismatch",
                "error_code_string": "lease_owner_mismatch",
                "retryable_bool": False,
            }
        # Lease heartbeats intentionally do not mutate node capability state.
        # Producers send readiness through /nodes/heartbeat with an explicit
        # source scope; otherwise a Renderfin status default could erase a
        # host's authoritative AI/full-converter registration.
        await _reconcile_autorig_reserve(db, now)
        if lease.state == "preemption_requested":
            await db.commit()
            return 423, {
                "status_string": "preemption_requested",
                "retryable_bool": True,
                "retry_after_seconds_int": 1,
                "lease_by_key": _lease_payload(lease),
            }
        if lease.state != "active":
            await db.commit()
            return 409, {"status_string": "lease_terminal", "retryable_bool": False, "lease_by_key": _lease_payload(lease)}
        node = await db.get(WorkloadNodeState, lease.physical_resource_id)
        if node is None or node.heartbeat_at < now - timedelta(
            seconds=_heartbeat_ttl_seconds()
        ):
            await db.commit()
            return 423, {
                "status_string": "node_heartbeat_required",
                "error_code_string": "node_heartbeat_required",
                "retryable_bool": True,
                "retry_after_seconds_int": 5,
                "lease_by_key": _lease_payload(lease),
            }
        lease.heartbeat_at = now
        lease.expires_at = now + timedelta(seconds=_lease_ttl_seconds(payload.get("ttl_seconds_int")))
        lease.updated_at = now
        await db.commit()
        return 200, {"status_string": "active", "lease_by_key": _lease_payload(lease)}


async def release_lease(
    db: AsyncSession,
    lease_id: str,
    payload: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    admission_locked: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    now = now or datetime.utcnow()
    async with _admission_guard(admission_locked):
        lease = await db.get(WorkloadLease, lease_id)
        if lease is None:
            return 200, {"status_string": "already_released", "lease_id_string": lease_id}
        if not _exact_lease_owner_matches(lease, payload):
            return 409, {
                "status_string": "lease_owner_mismatch",
                "error_code_string": "lease_owner_mismatch",
                "retryable_bool": False,
            }
        outcome = _safe_identifier(payload.get("outcome_string"), maximum=32).lower()
        if not outcome:
            reason = _safe_identifier(payload.get("reason_string"), maximum=120).lower()
            outcome = "completed" if any(
                marker in reason for marker in ("complete", "success", "done", "terminal")
            ) else "released"
        if lease.state in ACTIVE_LEASE_STATES:
            # Natural completion wins a race with a preemption request.
            lease.state = "completed" if outcome == "completed" else ("preempted" if outcome == "preempted" else "released")
            lease.released_at = now
            lease.updated_at = now
            lease.expires_at = now
        elif lease.state in {"preempted", "expired"} and outcome == "completed":
            # Reverse race: exact durable artifact completion observed after a
            # preempt acknowledgement still wins. No other terminal transition
            # is mutable.
            lease.state = "completed"
            lease.updated_at = now
        waiter = await db.get(WorkloadWaiter, lease.request_id)
        if waiter is not None and waiter.state in {"waiting", "acquired"}:
            waiter.state = lease.state
            waiter.terminal_at = now
            waiter.last_seen_at = now
            waiter.updated_at = now
        elif (
            waiter is not None
            and waiter.state in {"preempted", "expired"}
            and lease.state == "completed"
        ):
            waiter.state = "completed"
            waiter.terminal_at = now
            waiter.last_seen_at = now
            waiter.updated_at = now
        await db.commit()
        return 200, {"status_string": lease.state, "lease_by_key": _lease_payload(lease)}


async def broker_status(db: AsyncSession, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.utcnow()
    async with fleet_admission_lock():
        await _expire_admission_state(db, now)
        await _reconcile_autorig_reserve(db, now)
        nodes = await _fresh_nodes(db, now)
        leases = await _active_leases(db)
        waiter_result = await db.execute(
            select(WorkloadWaiter).where(WorkloadWaiter.state == "waiting")
        )
        waiters = sorted(waiter_result.scalars().all(), key=_waiter_order)
        active_by_resource = {str(lease.physical_resource_id): lease for lease in leases}
        demand = await _interactive_demand(db)
        full_nodes = [
            node for node in nodes
            if node.full_converter
            and (
                node.accepting
                or active_by_resource.get(str(node.physical_resource_id)) is not None
            )
        ]
        authority_cutoff = now - timedelta(seconds=_heartbeat_ttl_seconds())
        ai_nodes = [
            node
            for node in nodes
            if node.ai_capable
            and node.authority_heartbeat_at is not None
            and node.authority_heartbeat_at >= authority_cutoff
        ]
        reserve = min(_reserve_count(), len(full_nodes))
        ai_accepting = [
            node
            for node in ai_nodes
            if _accepting_ai_for_resource(
                node,
                now=now,
                nodes=full_nodes,
                active_by_resource=active_by_resource,
                interactive_demand=demand,
                reserve=reserve,
            )
        ]
        ai_available = [
            node
            for node in ai_accepting
            if node.physical_resource_id not in active_by_resource
        ]
        ai_preemptible = [
            node
            for node in ai_accepting
            if node.physical_resource_id in active_by_resource
        ]
        queued_by_class = {workload: 0 for workload in WORKLOAD_CLASSES}
        for waiter in waiters:
            queued_by_class[str(waiter.workload_class)] = (
                queued_by_class.get(str(waiter.workload_class), 0) + 1
            )
        active_by_class = {workload: 0 for workload in WORKLOAD_CLASSES}
        for lease in leases:
            active_by_class[str(lease.workload_class)] = (
                active_by_class.get(str(lease.workload_class), 0) + 1
            )
        ai_waiters = [
            waiter for waiter in waiters if waiter.workload_class == WORKLOAD_CLASS_AI
        ]
        oldest_ai_wait_seconds = max(
            0.0,
            (now - min(waiter.created_at for waiter in ai_waiters)).total_seconds(),
        ) if ai_waiters else 0.0
        assignment_result = await db.execute(
            select(WorkloadWaiter).where(WorkloadWaiter.assigned_at.is_not(None))
            .order_by(WorkloadWaiter.assigned_at.desc())
            .limit(500)
        )
        assignment_seconds = sorted(
            max(0.0, (waiter.assigned_at - waiter.created_at).total_seconds())
            for waiter in assignment_result.scalars().all()
            if waiter.assigned_at is not None and waiter.created_at is not None
        )
        assignment_p95 = 0.0
        if assignment_seconds:
            assignment_p95 = assignment_seconds[
                min(len(assignment_seconds) - 1, max(0, int(len(assignment_seconds) * 0.95) - 1))
            ]
        lease_metric_result = await db.execute(
            select(WorkloadLease.state, func.count(WorkloadLease.lease_id)).group_by(
                WorkloadLease.state
            )
        )
        lease_state_counts = {
            str(state): int(count or 0) for state, count in lease_metric_result.all()
        }
        ai_backlog = int(queued_by_class.get(WORKLOAD_CLASS_AI, 0))
        alarm_reasons = []
        if oldest_ai_wait_seconds > 60:
            alarm_reasons.append("oldest_ai_wait_over_60s")
        if ai_backlog > 0 and len(ai_accepting) < 4:
            alarm_reasons.append("accepting_ai_slots_below_4")
        resources_list = []
        for node in sorted(nodes, key=lambda item: item.physical_resource_id):
            active = active_by_resource.get(str(node.physical_resource_id))
            resources_list.append(
                {
                    "physical_resource_id_string": node.physical_resource_id,
                    "node_id_string": node.node_id,
                    "reserve_role_string": normalize_reserve_role(node.reserve_role),
                    "accepting_ai_vision_bool": bool(
                        node in ai_accepting
                    ),
                    "current_workload_string": str(active.workload_class if active else ""),
                    "lease_owner_string": str(active.owner_service if active else ""),
                    "lease_owner_task_id_string": str(active.owner_task_id if active else ""),
                    "lease_expires_at_utc_timestamp": (
                        active.expires_at.isoformat() + "Z" if active and active.expires_at else ""
                    ),
                    "preemption_requested_bool": bool(
                        active is not None and active.state == "preemption_requested"
                    ),
                }
            )
        await db.commit()
        return {
            "status_string": "ok",
            "generated_at_string": now.isoformat() + "Z",
            "policy_by_key": {
                "priority_order_list": [
                    WORKLOAD_CLASS_AI,
                    WORKLOAD_CLASS_AUTORIG,
                    WORKLOAD_CLASS_COMFY,
                    WORKLOAD_CLASS_HUNYUAN,
                    WORKLOAD_CLASS_BACKGROUND,
                ],
                "autorig_reserve_slots_int": reserve,
                "interactive_demand_int": demand,
            },
            "capacity_by_key": {
                "fresh_nodes_int": len(nodes),
                "healthy_full_converters_int": len(full_nodes),
                "ai_capable_nodes_int": len(ai_nodes),
                "accepting_ai_slots_int": len(ai_accepting),
                "eligible_ai_slots_int": len(ai_nodes),
                "preemptible_ai_slots_int": len(ai_preemptible),
                "autorig_usable_full_slots_int": _autorig_usable_full_count(full_nodes, active_by_resource),
                "active_leases_int": len(leases),
            },
            "queue_by_class_key": queued_by_class,
            "active_by_class_key": active_by_class,
            "metrics_by_key": {
                "oldest_ai_wait_seconds_float": round(oldest_ai_wait_seconds, 3),
                "assignment_seconds_p95_float": round(assignment_p95, 3),
                "preemption_requested_total_int": int(
                    lease_state_counts.get("preemption_requested", 0)
                    + lease_state_counts.get("preempted", 0)
                ),
                "preempted_total_int": int(lease_state_counts.get("preempted", 0)),
                "expired_recovery_total_int": int(lease_state_counts.get("expired", 0)),
                "completed_total_int": int(lease_state_counts.get("completed", 0)),
            },
            "alarm_by_key": {
                "active_bool": bool(alarm_reasons),
                "reasons_list": alarm_reasons,
            },
            "leases_list": [_lease_payload(lease) for lease in sorted(leases, key=lambda item: (item.priority, item.acquired_at))],
            "waiters_list": [
                {
                    "request_id_string": waiter.request_id,
                    "workload_class_string": waiter.workload_class,
                    "priority_int": int(waiter.priority or 0),
                    "owner_service_string": waiter.owner_service,
                    "owner_task_id_string": waiter.owner_task_id,
                    "physical_resource_id_string": waiter.physical_resource_id or "",
                    "node_id_string": waiter.node_id or "",
                    "wait_seconds_float": round(
                        max(0.0, (now - waiter.created_at).total_seconds()), 3
                    ),
                }
                for waiter in waiters
            ],
            "resources_list": resources_list,
            "nodes_list": [
                {
                    "physical_resource_id_string": node.physical_resource_id,
                    "node_id_string": node.node_id,
                    "node_kind_string": node.node_kind,
                    "full_converter_bool": bool(node.full_converter),
                    "ai_capable_bool": bool(node.ai_capable),
                    "managed_farm_bool": bool(node.managed_farm),
                    "healthy_bool": bool(node.healthy),
                    "accepting_bool": bool(node.accepting),
                    "reserve_role_string": node.reserve_role,
                    "heartbeat_at_string": node.heartbeat_at.isoformat() + "Z" if node.heartbeat_at else "",
                    "authority_source_string": node.authority_source or "",
                    "authority_heartbeat_at_string": (
                        node.authority_heartbeat_at.isoformat() + "Z"
                        if node.authority_heartbeat_at
                        else ""
                    ),
                }
                for node in sorted(nodes, key=lambda item: item.physical_resource_id)
            ],
        }


async def node_heartbeat(
    db: AsyncSession,
    payload: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    admission_locked: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    now = now or datetime.utcnow()
    node_id = _safe_identifier(payload.get("node_id_string"))
    physical_resource_id = canonical_physical_resource_id(payload.get("physical_resource_id_string"), node_id)
    node_status = _node_status_payload(payload)
    if not node_id or not physical_resource_id or not node_status:
        return 400, {"status_string": "invalid_request"}
    async with _admission_guard(admission_locked):
        await _expire_admission_state(db, now)
        node = await _upsert_node(
            db,
            physical_resource_id=physical_resource_id,
            node_id=node_id,
            node_status=node_status,
            now=now,
            source_scope=str(payload.get("source_scope_string") or "host_agent"),
        )
        await _reconcile_autorig_reserve(db, now)
        await db.commit()
        return 200, {
            "status_string": "ok",
            "physical_resource_id_string": node.physical_resource_id,
            "heartbeat_at_string": node.heartbeat_at.isoformat() + "Z",
            "heartbeat_at_utc_timestamp": node.heartbeat_at.isoformat() + "Z",
            "bootstrap_accepted_bool": True,
        }


async def cancel_waiter(
    db: AsyncSession,
    request_id: str,
    payload: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    admission_locked: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    now = now or datetime.utcnow()
    async with _admission_guard(admission_locked):
        waiter = await db.get(WorkloadWaiter, request_id)
        if waiter is None:
            return 200, {"status_string": "already_cancelled", "request_id_string": request_id}
        owner_service = _safe_identifier(
            payload.get("owner_service_string"), maximum=120
        )
        owner_task_id = _safe_identifier(payload.get("owner_task_id_string"))
        if (
            not owner_service
            or not owner_task_id
            or owner_service != waiter.owner_service
            or owner_task_id != waiter.owner_task_id
        ):
            return 409, {
                "status_string": "request_owner_mismatch",
                "error_code_string": "request_owner_mismatch",
                "retryable_bool": False,
            }
        if waiter.state == "waiting":
            waiter.state = "cancelled"
            waiter.terminal_at = now
            waiter.last_seen_at = now
            waiter.updated_at = now
            await db.commit()
        return 200, {
            "status_string": waiter.state,
            "request_id_string": waiter.request_id,
        }


async def _json_payload(request: Request) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _response(result: Tuple[int, Dict[str, Any]]) -> JSONResponse:
    status_code, payload = result
    payload = dict(payload)
    if status_code >= 400 and not payload.get("error_code_string"):
        payload["error_code_string"] = str(payload.get("status_string") or "request_failed")
    headers = {}
    if status_code in {423, 429, 503}:
        headers["Retry-After"] = str(int(payload.get("retry_after_seconds_int") or 5))
    return JSONResponse(status_code=status_code, content=payload, headers=headers)


@router.post("/leases/acquire")
async def route_acquire_lease(request: Request, db: AsyncSession = Depends(get_db)):
    payload = await _json_payload(request)
    _, error = _principal_error(request, "acquire", payload)
    if error:
        return error
    return _response(await acquire_lease(db, payload))


@router.post("/leases/{lease_id}/heartbeat")
async def route_heartbeat_lease(lease_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    payload = await _json_payload(request)
    _, error = _principal_error(request, "heartbeat", payload)
    if error:
        return error
    return _response(
        await heartbeat_lease(
            db, _safe_identifier(lease_id, maximum=64), payload
        )
    )


@router.post("/leases/{lease_id}/release")
async def route_release_lease(lease_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    payload = await _json_payload(request)
    _, error = _principal_error(request, "release", payload)
    if error:
        return error
    return _response(
        await release_lease(
            db, _safe_identifier(lease_id, maximum=64), payload
        )
    )


@router.post("/nodes/heartbeat")
async def route_node_heartbeat(request: Request, db: AsyncSession = Depends(get_db)):
    payload = await _json_payload(request)
    _, error = _principal_error(request, "node_heartbeat", payload)
    if error:
        return error
    return _response(await node_heartbeat(db, payload))


@router.post("/requests/{request_id}/cancel")
async def route_cancel_waiter(request_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    payload = await _json_payload(request)
    _, error = _principal_error(request, "cancel", payload)
    if error:
        return error
    return _response(
        await cancel_waiter(
            db,
            _safe_identifier(request_id, maximum=128),
            payload,
        )
    )


@router.get("/status")
async def route_broker_status(request: Request, db: AsyncSession = Depends(get_db)):
    _, error = _principal_error(request, "status", {})
    if error:
        return error
    return JSONResponse(content=await broker_status(db))


def token_fingerprint() -> str:
    """Non-secret deployment diagnostic; never expose the token itself."""
    configured, ambiguous = _configured_broker_principals()
    if ambiguous:
        return "invalid"
    token = str(configured.get("admin") or "")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12] if token else ""


def task_workload_class(task: Task) -> str:
    return (
        WORKLOAD_CLASS_BACKGROUND
        if str(getattr(task, "queue_class", "") or "").strip().lower()
        == WORKLOAD_CLASS_BACKGROUND
        else WORKLOAD_CLASS_AUTORIG
    )


async def _worker_identity(
    db: AsyncSession,
    worker_url: str,
    node_status: Dict[str, Any],
) -> Tuple[str, str, Dict[str, Any]]:
    normalized_url = str(worker_url or "").strip().rstrip("/").lower()
    endpoint_result = await db.execute(
        select(WorkerEndpoint).where(
            func.rtrim(func.lower(WorkerEndpoint.url), "/") == normalized_url
        )
    )
    endpoint = endpoint_result.scalar_one_or_none()
    parsed = re.sub(r"^converter-", "", str(urlparse(worker_url).hostname or "").lower())
    parsed = parsed.split(".", 1)[0]
    control = (
        node_status.get("workload_control")
        if isinstance(node_status.get("workload_control"), dict)
        else {}
    )
    reported_physical = canonical_physical_resource_id(
        node_status.get("physical_resource_id_string")
        or node_status.get("physical_gpu_id")
        or node_status.get("physical_node")
        or control.get("physical_node")
    )
    configured_physical = canonical_physical_resource_id(
        endpoint.physical_resource_id if endpoint is not None else ""
    )
    reported_role = normalize_reserve_role(
        node_status.get("reserve_role_string")
        or node_status.get("workload_role")
        or node_status.get("workload_role_string")
        or control.get("workload_role"),
        missing=RESERVE_ROLE_MAINTENANCE,
    )
    configured_role = normalize_reserve_role(
        endpoint.role if endpoint is not None else "",
        missing=RESERVE_ROLE_MAINTENANCE,
    )
    identity_error = ""
    if _enabled():
        if endpoint is None:
            identity_error = "worker_endpoint_not_registered"
        elif not _STABLE_MACHINE_RE.fullmatch(configured_physical):
            identity_error = "worker_config_physical_identity_unverified"
        elif not _STABLE_MACHINE_RE.fullmatch(reported_physical):
            identity_error = "worker_live_physical_identity_unverified"
        elif configured_physical != reported_physical:
            identity_error = "worker_physical_identity_mismatch"
        elif configured_role != reported_role:
            identity_error = "worker_role_mismatch"
    if identity_error:
        physical = ""
    elif reported_physical:
        physical = reported_physical
    elif configured_physical:
        physical = configured_physical
    else:
        physical = parsed
    node_id = _safe_identifier(
        node_status.get("node_id_string")
        or node_status.get("node_name")
        or parsed
        or physical
    )
    snapshot = dict(node_status or {})
    if identity_error:
        snapshot["_identity_error_string"] = identity_error
        snapshot["healthy_bool"] = False
        snapshot["accepting_bool"] = False
        snapshot["reserve_role_string"] = RESERVE_ROLE_MAINTENANCE
        return node_id, "", snapshot
    snapshot.setdefault("node_kind_string", "managed_farm")
    snapshot.setdefault("managed_farm_bool", True)
    snapshot.setdefault("full_converter_bool", True)
    snapshot.setdefault("healthy_bool", True)
    snapshot.setdefault(
        "accepting_bool",
        not _bool_from(snapshot, ("maintenance_bool", "maintenance"), False),
    )
    if endpoint is not None:
        snapshot["reserve_role_string"] = configured_role
    snapshot["physical_resource_id_string"] = physical
    return node_id, physical, snapshot


async def acquire_task_workload_lease(
    db: AsyncSession,
    task: Task,
    worker_url: str,
    node_status: Dict[str, Any],
    *,
    admission_locked: bool = False,
) -> Tuple[bool, Dict[str, Any]]:
    """Persist admission on the Task before the worker receives its POST."""
    if not _enabled():
        return True, {}
    node_id, physical, snapshot = await _worker_identity(db, worker_url, node_status)
    if not physical:
        return False, {
            "status_string": str(
                snapshot.get("_identity_error_string")
                or "worker_identity_unverified"
            ),
            "error_code_string": "worker_identity_unverified",
            "retryable_bool": True,
            "retry_after_seconds_int": 5,
        }
    workload_class = task_workload_class(task)
    await node_heartbeat(
        db,
        {
            "node_id_string": node_id,
            "physical_resource_id_string": physical,
            "source_scope_string": "autorig_worker_probe",
            "node_status_by_key": snapshot,
        },
        admission_locked=admission_locked,
    )
    if task.workload_lease_id:
        submission_outcome_unknown = (
            str(task.workload_lease_state or "") == "submission_unknown"
        )
        code, heartbeat = await heartbeat_lease(
            db,
            str(task.workload_lease_id),
            {
                "owner_service_string": "autorig_dispatcher",
                "owner_task_id_string": task.id,
                "request_id_string": task.workload_request_id,
                "ttl_seconds_int": 300,
                "node_status_by_key": snapshot,
            },
            admission_locked=admission_locked,
        )
        if code == 200:
            if task.workload_physical_resource_id != physical:
                if submission_outcome_unknown:
                    # The live worker probe may have drifted, but the POST may
                    # already exist on the persisted endpoint.  Fail closed on
                    # that exact endpoint instead of releasing/rotating.
                    return True, {
                        "lease_id_string": task.workload_lease_id,
                        "request_id_string": task.workload_request_id,
                        "physical_resource_id_string": task.workload_physical_resource_id,
                        "node_id_string": task.workload_node_id,
                        "workload_class_string": task.workload_class,
                    }
                return False, {
                    "status_string": "lease_bound_to_other_worker",
                    "retryable_bool": True,
                    "retry_after_seconds_int": 2,
                }
            task.workload_lease_state = "active"
            task.workload_lease_heartbeat_at = datetime.utcnow()
            await db.commit()
            return True, heartbeat.get("lease_by_key") or {}
        if str(heartbeat.get("status_string") or "") == "preemption_requested":
            if submission_outcome_unknown:
                # First recover the exact worker binding by replaying the same
                # request.  Releasing central admission here could leave an
                # accepted but hidden host process running while a new request
                # is dispatched elsewhere.
                task.workload_lease_state = "preemption_requested"
                task.workload_lease_heartbeat_at = datetime.utcnow()
                await db.commit()
                return True, heartbeat.get("lease_by_key") or {
                    "lease_id_string": task.workload_lease_id,
                    "request_id_string": task.workload_request_id,
                    "physical_resource_id_string": task.workload_physical_resource_id,
                    "node_id_string": task.workload_node_id,
                    "workload_class_string": task.workload_class,
                }
            await release_task_workload_lease(
                db,
                task,
                outcome="preempted",
                clear_for_retry=True,
                admission_locked=admission_locked,
            )
            return False, heartbeat
        if submission_outcome_unknown:
            # A lease expiring while the POST response is ambiguous is not
            # proof that the host did not accept it.  Preserve the exact ids
            # and replay the host request; only its idempotency ledger can
            # resolve this state without creating an orphan/duplicate.
            task.workload_lease_state = "submission_unknown"
            task.workload_lease_heartbeat_at = datetime.utcnow()
            await db.commit()
            return True, {
                "lease_id_string": task.workload_lease_id,
                "request_id_string": task.workload_request_id,
                "physical_resource_id_string": task.workload_physical_resource_id,
                "node_id_string": task.workload_node_id,
                "workload_class_string": task.workload_class,
            }
        # The persisted lease expired or was already terminal while the
        # backend was down. Clear only central admission identity; task inputs
        # and retry budgets remain untouched and a fresh request can be made.
        task.workload_request_id = None
        task.workload_lease_id = None
        task.workload_physical_resource_id = None
        task.workload_node_id = None
        task.workload_class = None
        task.workload_lease_state = None
        task.workload_lease_heartbeat_at = None
        task.worker_api = None
        await db.commit()

    if not task.workload_request_id:
        task.workload_request_id = f"ar_{secrets.token_urlsafe(32)}"[:128]
    task.workload_physical_resource_id = physical
    task.workload_node_id = node_id
    task.workload_class = workload_class
    task.workload_lease_state = "waiting"
    task.worker_api = None
    task.processing_started_at = None
    await db.commit()

    code, response = await acquire_lease(
        db,
        {
            "node_id_string": node_id,
            "physical_resource_id_string": physical,
            "workload_class_string": workload_class,
            "owner_service_string": "autorig_dispatcher",
            "owner_task_id_string": task.id,
            "request_id_string": task.workload_request_id,
            "priority_int": 0,
            "ttl_seconds_int": 300,
            "node_status_by_key": snapshot,
            "metadata_by_key": {
                "worker_url": worker_url,
                "queue_class": str(task.queue_class or "interactive"),
                "pipeline_kind": str(task.pipeline_kind or "rig"),
            },
        },
        admission_locked=admission_locked,
    )
    if code != 200:
        task.workload_lease_state = "waiting"
        task.dispatch_not_before = datetime.utcnow() + timedelta(
            seconds=max(1, int(response.get("retry_after_seconds_int") or 2))
        )
        await db.commit()
        return False, response
    lease = response.get("lease_by_key") or {}
    task.workload_lease_id = str(lease.get("lease_id_string") or "") or None
    task.workload_physical_resource_id = str(
        lease.get("physical_resource_id_string") or physical
    )
    task.workload_node_id = str(lease.get("node_id_string") or node_id)
    task.workload_lease_state = "active"
    task.workload_lease_heartbeat_at = datetime.utcnow()
    # A restart between this commit and the POST must retry only this worker.
    task.worker_api = worker_url
    await db.commit()
    return True, lease


async def heartbeat_task_workload_lease(
    db: AsyncSession,
    task: Task,
    node_status: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any]]:
    if not _enabled() or not task.workload_lease_id:
        return 200, {"status_string": "not_required"}
    if node_status and task.workload_physical_resource_id:
        await node_heartbeat(
            db,
            {
                "node_id_string": task.workload_node_id,
                "physical_resource_id_string": task.workload_physical_resource_id,
                "source_scope_string": "autorig_worker_probe",
                "node_status_by_key": node_status,
            },
        )
    code, response = await heartbeat_lease(
        db,
        str(task.workload_lease_id),
        {
            "owner_service_string": "autorig_dispatcher",
            "owner_task_id_string": task.id,
            "request_id_string": task.workload_request_id,
            "ttl_seconds_int": 300,
        },
    )
    task.workload_lease_state = str(response.get("status_string") or "unknown")
    if code == 200:
        task.workload_lease_heartbeat_at = datetime.utcnow()
    await db.commit()
    return code, response


async def release_task_workload_lease(
    db: AsyncSession,
    task: Task,
    *,
    outcome: str,
    clear_for_retry: bool = False,
    admission_locked: bool = False,
) -> None:
    if not _enabled():
        return
    if task.workload_lease_id:
        await release_lease(
            db,
            str(task.workload_lease_id),
            {
                "owner_service_string": "autorig_dispatcher",
                "owner_task_id_string": task.id,
                "request_id_string": task.workload_request_id,
                "outcome_string": outcome,
            },
            admission_locked=admission_locked,
        )
    elif task.workload_request_id:
        await cancel_waiter(
            db,
            str(task.workload_request_id),
            {"owner_task_id_string": task.id},
            admission_locked=admission_locked,
        )
    task.workload_lease_state = outcome
    task.workload_lease_heartbeat_at = datetime.utcnow()
    if clear_for_retry:
        task.workload_request_id = None
        task.workload_lease_id = None
        task.workload_physical_resource_id = None
        task.workload_node_id = None
        task.workload_class = None
        task.workload_lease_state = None
        task.workload_lease_heartbeat_at = None
    await db.commit()
