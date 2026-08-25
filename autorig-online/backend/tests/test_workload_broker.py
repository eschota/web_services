import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import (
    Base,
    Task,
    WorkerEndpoint,
    WorkloadLease,
    WorkloadNodeState,
    WorkloadWaiter,
)
from workload_broker import (
    _broker_auth_principal,
    _principal_allows_payload,
    _principal_error,
    acquire_lease as broker_acquire_lease,
    acquire_task_workload_lease,
    broker_status,
    cancel_waiter,
    canonical_physical_resource_id,
    heartbeat_lease,
    heartbeat_task_workload_lease,
    node_state_from_status,
    node_heartbeat,
    normalize_reserve_role,
    release_lease,
    release_task_workload_lease,
    reserve_role_rank,
    route_acquire_lease,
    route_broker_status,
    route_cancel_waiter,
    route_heartbeat_lease,
    route_node_heartbeat,
    route_release_lease,
    workload_broker_api_enabled,
    workload_broker_enabled,
)


class _HeaderRequest:
    def __init__(self, token):
        self.headers = {"authorization": f"Bearer {token}"}


class _JsonRequest(_HeaderRequest):
    def __init__(self, token, payload):
        super().__init__(token)
        self._payload = payload

    async def json(self):
        return self._payload


def _node_status(*, full=True, ai=True, accepting=True):
    return {
        "managed_farm_bool": True,
        "workload_role": "shared",
        "full_converter_bool": full,
        "ai_capable_bool": ai,
        "healthy_bool": True,
        "accepting_work_bool": accepting,
        "arbiter_by_key": {
            "online_bool": True,
            "accepting_ai_vision_bool": accepting,
        },
    }


def _request(node, workload, task, request, *, status=None, priority=0):
    return {
        "node_id_string": node,
        "physical_resource_id_string": node,
        "workload_class_string": workload,
        "owner_service_string": "test",
        "owner_task_id_string": task,
        "request_id_string": request,
        "priority_int": priority,
        "ttl_seconds_int": 60,
        "node_status_by_key": status or _node_status(),
    }


def _worker_status(machine: str, role: str, *, ai: bool = False):
    status = _node_status(ai=ai)
    status["physical_node"] = machine
    status["workload_role"] = role
    return status


async def acquire_lease(db, payload, *, now=None, admission_locked=False):
    """Tests model the required host bootstrap before lease traffic."""
    now = now or datetime.utcnow()
    physical = canonical_physical_resource_id(
        payload.get("physical_resource_id_string"), payload.get("node_id_string")
    )
    existing = await db.get(WorkloadNodeState, physical)
    if existing is None:
        await node_heartbeat(
            db,
            {
                "node_id_string": payload.get("node_id_string"),
                "physical_resource_id_string": payload.get("physical_resource_id_string"),
                "source_scope_string": "host_agent",
                "node_status_by_key": payload.get("node_status_by_key") or _node_status(),
            },
            now=now,
            admission_locked=admission_locked,
        )
    return await broker_acquire_lease(
        db, payload, now=now, admission_locked=admission_locked
    )


async def _with_database(callback):
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as session:
            return await callback(session)
    finally:
        await engine.dispose()


def _run(callback):
    with TemporaryDirectory() as directory, patch.dict(
        "os.environ",
        {
            "AUTORIG_FLEET_ADMISSION_LOCK": str(Path(directory) / "broker.lock"),
            "AUTORIG_WORKLOAD_AUTORIG_RESERVE": "2",
        },
    ):
        return asyncio.run(_with_database(callback))


def test_physical_aliases_keep_f7_distinct_and_preserve_machine_hash():
    assert canonical_physical_resource_id("F7") == "f7"
    assert canonical_physical_resource_id("FARM-F7") == "farm-f7"
    assert canonical_physical_resource_id("RYZEN-SERVER") == "raptor"
    assert canonical_physical_resource_id("Raptor-GPU0") == "raptor"
    fingerprint = "a1234567890bcdefa1234567890bcdef"
    assert canonical_physical_resource_id(fingerprint) == fingerprint


def test_scoped_broker_credentials_cannot_upgrade_owner_or_class():
    gateway = {
        "owner_service_string": "freestock_gateway",
        "workload_class_string": "ai_vision",
    }
    assert _principal_allows_payload("gateway", "acquire", gateway)
    assert not _principal_allows_payload(
        "gateway",
        "acquire",
        {**gateway, "workload_class_string": "autorig_interactive"},
    )
    assert not _principal_allows_payload(
        "gateway",
        "acquire",
        {**gateway, "owner_service_string": "renderfin"},
    )
    renderfin = {
        "owner_service_string": "renderfin",
        "workload_class_string": "comfy",
    }
    assert _principal_allows_payload("renderfin", "acquire", renderfin)
    assert not _principal_allows_payload(
        "renderfin",
        "acquire",
        {**renderfin, "workload_class_string": "ai_vision"},
    )
    assert _principal_allows_payload(
        "gateway",
        "node_heartbeat",
        {"source_scope_string": "lease_probe"},
    )
    assert not _principal_allows_payload(
        "gateway",
        "node_heartbeat",
        {"source_scope_string": "host_agent"},
    )
    assert _principal_allows_payload(
        "host_agent",
        "node_heartbeat",
        {"source_scope_string": "host_agent"},
    )
    assert _principal_allows_payload("admin", "status", {})
    assert not _principal_allows_payload("gateway", "status", {})


def test_broker_auth_rejects_legacy_and_aliased_scoped_tokens_by_default():
    scoped = {
        "AUTORIG_WORKLOAD_BROKER_ENABLED": "1",
        "AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN": "gateway-token-1234567890",
        "AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN": "renderfin-token-123456789",
        "AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN": "host-agent-token-12345678",
        "AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN": "admin-token-123456789012",
        "AUTORIG_WORKLOAD_BROKER_TOKEN": "legacy-token-12345678901",
    }
    with patch.dict("os.environ", scoped, clear=True):
        principal, error = _broker_auth_principal(
            _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN"])
        )
        assert principal == "gateway" and error is None
        principal, error = _broker_auth_principal(
            _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_TOKEN"])
        )
        assert not principal and error is not None and error.status_code == 401

    aliased = dict(scoped)
    aliased["AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN"] = aliased[
        "AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN"
    ]
    with patch.dict("os.environ", aliased, clear=True):
        principal, error = _broker_auth_principal(
            _HeaderRequest(aliased["AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN"])
        )
        assert not principal and error is not None and error.status_code == 503


def test_central_broker_feature_flag_defaults_off(monkeypatch):
    monkeypatch.delenv("AUTORIG_WORKLOAD_BROKER_ENABLED", raising=False)
    monkeypatch.delenv("AUTORIG_WORKLOAD_BROKER_API_ENABLED", raising=False)
    assert workload_broker_enabled() is False
    assert workload_broker_api_enabled() is False


def test_api_only_flag_exposes_only_host_heartbeat_and_admin_status():
    scoped = {
        "AUTORIG_WORKLOAD_BROKER_API_ENABLED": "1",
        "AUTORIG_WORKLOAD_BROKER_ENABLED": "0",
        "AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN": "gateway-token-1234567890",
        "AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN": "renderfin-token-123456789",
        "AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN": "host-agent-token-12345678",
        "AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN": "admin-token-123456789012",
    }
    with patch.dict("os.environ", scoped, clear=True):
        assert workload_broker_api_enabled() is True
        assert workload_broker_enabled() is False
        principal, error = _broker_auth_principal(
            _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN"])
        )
        assert principal == "host_agent" and error is None

        principal, error = _principal_error(
            _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN"]),
            "node_heartbeat",
            {"source_scope_string": "host_agent"},
        )
        assert principal == "host_agent" and error is None
        principal, error = _principal_error(
            _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN"]),
            "status",
            {},
        )
        assert principal == "admin" and error is None

        blocked = [
            (
                "gateway",
                "acquire",
                {
                    "owner_service_string": "freestock_gateway",
                    "workload_class_string": "ai_vision",
                },
            ),
            (
                "gateway",
                "heartbeat",
                {"owner_service_string": "freestock_gateway"},
            ),
            ("gateway", "release", {"owner_service_string": "freestock_gateway"}),
            ("renderfin", "cancel", {"owner_service_string": "renderfin"}),
            ("gateway", "node_heartbeat", {"source_scope_string": "lease_probe"}),
        ]
        for principal_name, action, payload in blocked:
            _, error = _principal_error(
                _HeaderRequest(scoped[
                    "AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN"
                    if principal_name == "gateway"
                    else "AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN"
                ]),
                action,
                payload,
            )
            assert error is not None and error.status_code == 503
            assert json.loads(error.body)["status_string"] == "api_staging_only"

        gateway_request = _JsonRequest(
            scoped["AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN"],
            {
                "owner_service_string": "freestock_gateway",
                "workload_class_string": "ai_vision",
            },
        )
        for route_call in (
            lambda: route_acquire_lease(gateway_request, None),
            lambda: route_heartbeat_lease("lease-1", gateway_request, None),
            lambda: route_release_lease("lease-1", gateway_request, None),
            lambda: route_cancel_waiter("request-1", gateway_request, None),
        ):
            response = asyncio.run(route_call())
            assert response.status_code == 503
            assert json.loads(response.body)["status_string"] == "api_staging_only"

        async def verify_staging_routes(db):
            node_response = await route_node_heartbeat(
                _JsonRequest(
                    scoped["AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN"],
                    {
                        "node_id_string": "F7",
                        "physical_resource_id_string": "F7",
                        "source_scope_string": "host_agent",
                        "node_status_by_key": _node_status(),
                    },
                ),
                db,
            )
            assert node_response.status_code == 200
            assert json.loads(node_response.body)["bootstrap_accepted_bool"] is True
            status_response = await route_broker_status(
                _HeaderRequest(scoped["AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN"]),
                db,
            )
            assert status_response.status_code == 200
            assert json.loads(status_response.body)["broker_mode_by_key"] == {
                "api_enabled_bool": True,
                "lease_enforcement_enabled_bool": False,
            }

        _run(verify_staging_routes)

        assert asyncio.run(
            acquire_task_workload_lease(None, None, "", {})
        ) == (True, {})
        assert asyncio.run(
            heartbeat_task_workload_lease(None, None)
        ) == (200, {"status_string": "not_required"})
        assert asyncio.run(
            release_task_workload_lease(None, None, outcome="preempted")
        ) is None
        status = _run(lambda db: broker_status(db))
        assert status["broker_mode_by_key"] == {
            "api_enabled_bool": True,
            "lease_enforcement_enabled_bool": False,
        }


def test_lease_enforcement_always_implies_api_availability():
    with patch.dict(
        "os.environ",
        {
            "AUTORIG_WORKLOAD_BROKER_API_ENABLED": "0",
            "AUTORIG_WORKLOAD_BROKER_ENABLED": "1",
        },
        clear=True,
    ):
        assert workload_broker_enabled() is True
        assert workload_broker_api_enabled() is True


def test_workload_role_alias_is_canonical_and_nested_host_role_is_used():
    assert normalize_reserve_role("ai_primary") == "ai_vision_primary"
    assert reserve_role_rank("ai_vision", "ai_vision_primary") == 0
    assert reserve_role_rank("autorig_interactive", "autorig_primary") == 0
    parsed = node_state_from_status(
        {
            **_node_status(),
            "workload_role": "ai_vision_primary",
        }
    )
    assert parsed["reserve_role"] == "ai_vision_primary"
    invalid = node_state_from_status(
        {
            **_node_status(),
            "workload_role": "invented_role",
        }
    )
    assert invalid["reserve_role"] == "maintenance"
    assert invalid["healthy"] is False
    assert invalid["accepting"] is False


def test_managed_missing_role_is_maintenance_but_explicit_shared_is_healthy():
    missing = _node_status()
    missing.pop("workload_role")
    parsed_missing = node_state_from_status(missing)
    assert parsed_missing["reserve_role"] == "maintenance"
    assert parsed_missing["healthy"] is False
    assert parsed_missing["accepting"] is False

    kind_only = dict(missing)
    kind_only.pop("managed_farm_bool")
    kind_only["node_kind_string"] = "managed_farm"
    parsed_kind_only = node_state_from_status(kind_only)
    assert parsed_kind_only["managed_farm"] is True
    assert parsed_kind_only["reserve_role"] == "maintenance"
    assert parsed_kind_only["healthy"] is False
    assert parsed_kind_only["accepting"] is False

    parsed_shared = node_state_from_status(_node_status())
    assert parsed_shared["reserve_role"] == "shared"
    assert parsed_shared["healthy"] is True
    assert parsed_shared["accepting"] is True

    # Preserve compatibility only for non-managed desktop clients. A missing
    # role can never silently manufacture managed shared-farm capacity.
    desktop = node_state_from_status(
        {
            "managed_farm_bool": False,
            "healthy_bool": True,
            "accepting_work_bool": True,
        }
    )
    assert desktop["reserve_role"] == "shared"
    assert desktop["healthy"] is True
    assert desktop["accepting"] is True


def test_targeted_admission_prefers_home_role_but_borrows_when_home_busy():
    async def scenario(db):
        start = datetime.utcnow()
        nodes = {
            "machine_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": "ai_vision_primary",
            "machine_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": "shared",
            "machine_cccccccccccccccccccccccccccccccc": "autorig_primary",
        }
        for index, (node, role) in enumerate(nodes.items()):
            status = _node_status()
            status["workload_role"] = role
            code, _ = await node_heartbeat(
                db,
                {
                    "node_id_string": f"node-{index}",
                    "physical_resource_id_string": node,
                    "source_scope_string": "host_agent",
                    "node_status_by_key": status,
                },
                now=start,
            )
            assert code == 200

        ai_home, shared, autorig_home = nodes
        code, redirected = await broker_acquire_lease(
            db,
            _request(shared, "ai_vision", "vision-borrow", "vision-borrow"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        assert redirected["status_string"] == "preferred_role_available"
        assert redirected["preferred_physical_resource_id_string"] == ai_home

        code, first = await broker_acquire_lease(
            db,
            _request(ai_home, "ai_vision", "vision-home", "vision-home"),
            now=start + timedelta(seconds=2),
        )
        assert code == 200
        code, borrowed = await broker_acquire_lease(
            db,
            _request(shared, "ai_vision", "vision-borrow", "vision-borrow"),
            now=start + timedelta(seconds=3),
        )
        assert code == 200
        assert borrowed["lease_by_key"]["physical_resource_id_string"] == shared

        code, redirected = await broker_acquire_lease(
            db,
            _request(ai_home, "autorig_interactive", "rig-redirect", "rig-redirect"),
            now=start + timedelta(seconds=4),
        )
        # The AI home is occupied, but the response must still never grant a
        # second lease. The idle AutoRig-primary node is the preferred target.
        assert code == 423
        assert redirected["status_string"] in {
            "preferred_role_available",
            "gpu_busy",
            "higher_priority_waiting",
        }
        if redirected["status_string"] == "preferred_role_available":
            assert redirected["preferred_physical_resource_id_string"] == autorig_home

    _run(scenario)


def test_all_raptor_aliases_share_one_physical_lease_slot():
    async def scenario(db):
        code, first = await acquire_lease(
            db, _request("raptor", "ai_vision", "vision-a", "request-a")
        )
        assert code == 200
        assert first["lease_by_key"]["physical_resource_id_string"] == "raptor"
        for index, alias in enumerate(("RYZEN-SERVER", "Raptor-GPU0"), 1):
            code, busy = await acquire_lease(
                db,
                _request(alias, "ai_vision", f"vision-{index}", f"request-{index}"),
            )
            assert code == 423
            assert busy["status_string"] in {"gpu_busy", "higher_priority_waiting"}

    _run(scenario)


def test_acquire_is_idempotent_and_one_active_lease_per_gpu():
    async def scenario(db):
        first_code, first = await acquire_lease(db, _request("f1", "ai_vision", "task-a", "request-a"))
        second_code, second = await acquire_lease(db, _request("f1", "ai_vision", "task-a", "request-a"))
        busy_code, busy = await acquire_lease(db, _request("f1", "ai_vision", "task-b", "request-b"))
        assert first_code == second_code == 200
        assert first["lease_by_key"]["lease_id_string"] == second["lease_by_key"]["lease_id_string"]
        assert busy_code == 423
        assert busy["status_string"] == "gpu_busy"

    _run(scenario)


def test_ai_requests_durable_preemption_of_background_then_acquires_same_gpu():
    async def scenario(db):
        code, background = await acquire_lease(
            db,
            _request("f11", "collection_background", "collection-1", "background-request"),
        )
        assert code == 200
        code, waiting = await acquire_lease(
            db,
            _request("f11", "ai_vision", "vision-1", "vision-request"),
        )
        assert code == 423
        assert waiting["status_string"] == "preemption_requested"
        victim_id = background["lease_by_key"]["lease_id_string"]
        victim = await db.get(WorkloadLease, victim_id)
        assert victim.state == "preemption_requested"
        release_code, _ = await release_lease(
            db, victim_id, {
                "owner_service_string": "test",
                "owner_task_id_string": "collection-1",
                "request_id_string": "background-request",
                "outcome_string": "preempted",
            }
        )
        assert release_code == 200
        code, acquired = await acquire_lease(
            db,
            _request("f11", "ai_vision", "vision-1", "vision-request"),
        )
        assert code == 200
        assert acquired["status_string"] == "acquired"

    _run(scenario)


def test_two_healthy_full_converters_are_reserved_during_interactive_demand():
    async def scenario(db):
        now = datetime.utcnow()
        for node in ("f1", "f2"):
            code, _ = await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "node_status_by_key": _node_status(),
                },
                now=now,
            )
            assert code == 200
        db.add(
            Task(
                id="interactive-waiting",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="created",
            )
        )
        await db.commit()
        code, result = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-reserve-test", "vision-reserve-request"),
            now=now + timedelta(seconds=1),
        )
        assert code == 423
        assert result["status_string"] == "autorig_reserve"
        assert result["reserve_slots_int"] == 2

    _run(scenario)


def test_ai_may_preempt_autorig_only_above_the_two_slot_reserve():
    async def scenario(db):
        now = datetime.utcnow()
        for node in ("f1", "f2", "f13"):
            await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "node_status_by_key": _node_status(),
                },
                now=now,
            )
        db.add(
            Task(
                id="interactive-active",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="processing",
            )
        )
        await db.commit()
        code, autorig = await acquire_lease(
            db,
            _request("f1", "autorig_interactive", "autorig-1", "autorig-request"),
            now=now + timedelta(seconds=1),
        )
        assert code == 200
        code, result = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-1", "vision-request"),
            now=now + timedelta(seconds=2),
        )
        assert code == 423
        assert result["status_string"] == "preemption_requested"
        victim = await db.get(WorkloadLease, autorig["lease_by_key"]["lease_id_string"])
        assert victim.state == "preemption_requested"

    _run(scenario)


def test_autorig_reserve_never_preempts_active_ai_vision():
    async def scenario(db):
        now = datetime.utcnow()
        status = _node_status()
        status["workload_role"] = "ai_vision_primary"
        code, _ = await node_heartbeat(
            db,
            {
                "node_id_string": "f1",
                "physical_resource_id_string": "f1",
                "source_scope_string": "host_agent",
                "node_status_by_key": status,
            },
            now=now,
        )
        assert code == 200

        code, vision = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-active", "vision-active-request"),
            now=now + timedelta(seconds=1),
        )
        assert code == 200
        db.add(
            Task(
                id="interactive-needs-reserve",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="created",
            )
        )
        await db.commit()

        await broker_status(db, now=now + timedelta(seconds=2))
        active_ai = await db.get(
            WorkloadLease, vision["lease_by_key"]["lease_id_string"]
        )
        assert active_ai is not None
        assert active_ai.state == "active"
        assert active_ai.preemption_requested_at is None

        code, blocked = await acquire_lease(
            db,
            _request(
                "f1",
                "autorig_interactive",
                "autorig-cannot-recall-ai",
                "autorig-cannot-recall-ai-request",
            ),
            now=now + timedelta(seconds=3),
        )
        assert code == 423
        assert blocked["status_string"] == "gpu_busy"
        active_ai = await db.get(
            WorkloadLease, vision["lease_by_key"]["lease_id_string"]
        )
        assert active_ai.state == "active"

        reserve_status = _node_status()
        reserve_status["workload_role"] = "autorig_primary"
        code, _ = await node_heartbeat(
            db,
            {
                "node_id_string": "f11",
                "physical_resource_id_string": "f11",
                "source_scope_string": "host_agent",
                "node_status_by_key": reserve_status,
            },
            now=now + timedelta(seconds=3),
        )
        assert code == 200
        code, reserved = await acquire_lease(
            db,
            _request(
                "f11",
                "autorig_interactive",
                "autorig-reserved",
                "autorig-reserved-request",
            ),
            now=now + timedelta(seconds=4),
        )
        assert code == 200
        assert reserved["status_string"] == "acquired"

    _run(scenario)


def test_busy_lower_priority_full_leases_still_satisfy_autorig_reserve():
    async def scenario(db):
        now = datetime.utcnow()
        for node in ("f1", "f2", "f13"):
            await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "node_status_by_key": _node_status(),
                },
                now=now,
            )
        for node, workload in (
            ("f2", "hunyuan"),
            ("f13", "collection_background"),
        ):
            code, _ = await acquire_lease(
                db,
                _request(node, workload, f"{workload}-task", f"{workload}-request"),
                now=now + timedelta(seconds=1),
            )
            assert code == 200
            code, _ = await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "node_status_by_key": _node_status(accepting=False),
                },
                now=now + timedelta(seconds=2),
            )
            assert code == 200
        db.add(
            Task(
                id="interactive-reserve-demand",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="created",
            )
        )
        await db.commit()

        code, result = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-with-recallable-reserve", "vision-recallable-request"),
            now=now + timedelta(seconds=3),
        )

        assert code == 200
        assert result["status_string"] == "acquired"

    _run(scenario)


def test_node_pinned_ai_waiter_cannot_block_establishing_two_autorig_slots():
    async def scenario(db):
        start = datetime.utcnow()
        for node in ("raptor", "f1", "f2", "f13"):
            code, _ = await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "node_status_by_key": _node_status(),
                },
                now=start,
            )
            assert code == 200

        code, _ = await acquire_lease(
            db,
            _request(
                "raptor",
                "collection_background",
                "raptor-background",
                "raptor-background-request",
            ),
            now=start + timedelta(seconds=1),
        )
        assert code == 200
        code, waiting = await acquire_lease(
            db,
            _request("raptor", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=2),
        )
        assert code == 423
        assert waiting["status_string"] == "preemption_requested"

        for index in range(3):
            db.add(
                Task(
                    id=f"interactive-reserve-{index}",
                    owner_type="user",
                    owner_id="user@example.com",
                    queue_class="interactive",
                    pipeline_kind="rig",
                    status="created",
                )
            )
        await db.commit()

        for index, node in enumerate(("f1", "f2"), 1):
            code, acquired = await acquire_lease(
                db,
                _request(
                    node,
                    "autorig_interactive",
                    f"autorig-{index}",
                    f"autorig-request-{index}",
                ),
                now=start + timedelta(seconds=2 + index),
            )
            assert code == 200
            assert acquired["status_string"] == "acquired"

        code, blocked = await acquire_lease(
            db,
            _request(
                "f13",
                "autorig_interactive",
                "autorig-above-reserve",
                "autorig-request-above-reserve",
            ),
            now=start + timedelta(seconds=5),
        )
        assert code == 423
        assert blocked["status_string"] == "higher_priority_waiting"
        assert blocked["blocking_request_id_string"] == "vision-request"

    _run(scenario)


def test_capacity_wait_never_creates_a_second_attempt_or_lease():
    async def scenario(db):
        code, first = await acquire_lease(db, _request("f2", "ai_vision", "one", "one-request"))
        assert code == 200
        for index in range(5):
            code, _ = await acquire_lease(
                db,
                _request("f2", "ai_vision", f"wait-{index}", f"wait-request-{index}"),
            )
            assert code == 423
        result = await db.execute(select(WorkloadLease))
        leases = list(result.scalars().all())
        assert len(leases) == 1
        assert leases[0].lease_id == first["lease_by_key"]["lease_id_string"]

    _run(scenario)


def test_ttl_expiry_and_completed_wins_preemption_race():
    async def scenario(db):
        start = datetime.utcnow()
        code, first = await acquire_lease(
            db, _request("f13", "collection_background", "background", "background-request"), now=start
        )
        assert code == 200
        code, _ = await acquire_lease(
            db, _request("f13", "ai_vision", "vision", "vision-request"), now=start + timedelta(seconds=1)
        )
        assert code == 423
        lease_id = first["lease_by_key"]["lease_id_string"]
        code, released = await release_lease(
            db,
            lease_id,
            {
                "owner_service_string": "test",
                "owner_task_id_string": "background",
                "request_id_string": "background-request",
                "outcome_string": "completed",
            },
            now=start + timedelta(seconds=2),
        )
        assert code == 200
        assert released["status_string"] == "completed"

        code, second = await acquire_lease(
            db, _request("f13", "ai_vision", "vision", "vision-request"), now=start + timedelta(seconds=3)
        )
        assert code == 200
        second_id = second["lease_by_key"]["lease_id_string"]
        status = await broker_status(db, now=start + timedelta(seconds=64))
        assert status["capacity_by_key"]["active_leases_int"] == 0
        expired = await db.get(WorkloadLease, second_id)
        assert expired.state == "expired"
        code, late = await release_lease(
            db,
            second_id,
            {
                "owner_service_string": "test",
                "owner_task_id_string": "vision",
                "request_id_string": "vision-request",
                "outcome_string": "completed",
            },
            now=start + timedelta(seconds=65),
        )
        assert code == 200
        assert late["status_string"] == "completed"

    _run(scenario)


def test_heartbeat_reports_preemption_without_extending_execution():
    async def scenario(db):
        code, background = await acquire_lease(
            db, _request("f5", "collection_background", "background", "background-request")
        )
        assert code == 200
        await acquire_lease(db, _request("f5", "ai_vision", "vision", "vision-request"))
        lease_id = background["lease_by_key"]["lease_id_string"]
        code, response = await heartbeat_lease(
            db,
            lease_id,
            {
                "owner_service_string": "test",
                "owner_task_id_string": "background",
                "request_id_string": "background-request",
                "ttl_seconds_int": 900,
            },
        )
        assert code == 423
        assert response["status_string"] == "preemption_requested"

    _run(scenario)


def test_ai_priority_zero_preempts_priority_ninety_but_not_reverse():
    async def scenario(db):
        start = datetime.utcnow()
        code, p90 = await acquire_lease(
            db,
            _request(
                "f5",
                "ai_vision",
                "vision-p90",
                "vision-p90-request",
                priority=90,
            ),
            now=start,
        )
        assert code == 200
        code, waiting = await acquire_lease(
            db,
            _request(
                "f5",
                "ai_vision",
                "vision-p0",
                "vision-p0-request",
                priority=0,
            ),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        assert waiting["status_string"] == "preemption_requested"
        victim = await db.get(
            WorkloadLease, p90["lease_by_key"]["lease_id_string"]
        )
        assert victim.state == "preemption_requested"
        assert victim.priority == 90

        await release_lease(
            db,
            victim.lease_id,
            {
                "owner_service_string": "test",
                "owner_task_id_string": "vision-p90",
                "request_id_string": "vision-p90-request",
                "outcome_string": "preempted",
            },
            now=start + timedelta(seconds=2),
        )
        code, p0 = await acquire_lease(
            db,
            _request(
                "f5",
                "ai_vision",
                "vision-p0",
                "vision-p0-request",
                priority=0,
            ),
            now=start + timedelta(seconds=3),
        )
        assert code == 200
        code, lower = await acquire_lease(
            db,
            _request(
                "f5",
                "ai_vision",
                "vision-later-p90",
                "vision-later-p90-request",
                priority=90,
            ),
            now=start + timedelta(seconds=4),
        )
        assert code == 423
        assert lower["status_string"] == "gpu_busy"
        active = await db.get(
            WorkloadLease, p0["lease_by_key"]["lease_id_string"]
        )
        assert active.state == "active"
        assert active.priority == 0

    _run(scenario)


def test_durable_ai_waiter_blocks_lower_work_and_retry_is_idempotent():
    async def scenario(db):
        start = datetime.utcnow()
        code, background = await acquire_lease(
            db,
            _request("f1", "collection_background", "background", "background-request"),
            now=start,
        )
        assert code == 200
        code, waiting = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        assert waiting["status_string"] == "preemption_requested"

        # The same Gateway retry remains one durable waiter and does not mint
        # another lease or queue position.
        code, again = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=2),
        )
        assert code == 423
        assert again["status_string"] == "preemption_requested"
        waiters = list((await db.execute(select(WorkloadWaiter))).scalars().all())
        assert [waiter.request_id for waiter in waiters].count("vision-request") == 1

        # A free second GPU still cannot be leapfrogged by lower work while the
        # earlier AI request is durably waiting.
        code, lower = await acquire_lease(
            db,
            _request("f2", "comfy", "image", "comfy-request"),
            now=start + timedelta(seconds=3),
        )
        assert code == 423
        assert lower["status_string"] == "higher_priority_waiting"
        assert lower["blocking_request_id_string"] == "vision-request"

        # Completed-wins/release remains exact and does not change the waiter's
        # FIFO timestamp.
        await release_lease(
            db,
            background["lease_by_key"]["lease_id_string"],
            {
                "owner_service_string": "test",
                "owner_task_id_string": "background",
                "request_id_string": "background-request",
                "outcome_string": "preempted",
            },
            now=start + timedelta(seconds=4),
        )
        code, acquired = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=5),
        )
        assert code == 200
        assert acquired["lease_by_key"]["request_id_string"] == "vision-request"

    _run(scenario)


def test_stale_waiter_is_attempt_neutral_and_cannot_block_the_farm_forever():
    async def scenario(db):
        start = datetime.utcnow()
        code, background = await acquire_lease(
            db,
            _request(
                "f1",
                "collection_background",
                "background",
                "background-request",
            ),
            now=start,
        )
        assert code == 200
        code, _ = await acquire_lease(
            db,
            _request("f1", "ai_vision", "crashed-ai", "crashed-ai-request"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423

        # The claimant disappears.  After the waiter TTL, unrelated lower
        # work on another compatible node is no longer leapfrogged by a stale
        # priority-0 row.  No product Task/attempt or extra lease is created.
        with patch.dict("os.environ", {"AUTORIG_WORKLOAD_WAITER_TTL": "30"}):
            code, lower = await acquire_lease(
                db,
                _request("f2", "comfy", "image", "comfy-after-stale"),
                now=start + timedelta(seconds=32),
            )
        assert code == 200
        assert lower["status_string"] == "acquired"
        stale = await db.get(WorkloadWaiter, "crashed-ai-request")
        assert stale.state == "abandoned"
        leases = list((await db.execute(select(WorkloadLease))).scalars().all())
        assert len(leases) == 2
        assert {item.request_id for item in leases} == {
            "background-request",
            "comfy-after-stale",
        }

        # Reappearance of the exact same never-acquired request safely rejoins
        # FIFO under the same identity instead of consuming a retry/attempt.
        with patch.dict("os.environ", {"AUTORIG_WORKLOAD_WAITER_TTL": "30"}):
            code, replay = await acquire_lease(
                db,
                _request("f1", "ai_vision", "crashed-ai", "crashed-ai-request"),
                now=start + timedelta(seconds=33),
            )
        assert code == 423
        assert replay["status_string"] in {"preemption_requested", "gpu_busy"}
        revived = await db.get(WorkloadWaiter, "crashed-ai-request")
        assert revived.state == "waiting"
        assert revived.created_at == start + timedelta(seconds=33)
        assert len(
            list((await db.execute(select(WorkloadWaiter))).scalars().all())
        ) == 3

    _run(scenario)


def test_fifo_within_same_class_is_resource_scoped_and_uses_independent_gpus():
    async def scenario(db):
        start = datetime.utcnow()
        for node in ("f1", "f2"):
            code, _ = await acquire_lease(
                db,
                _request(
                    node,
                    "collection_background",
                    f"background-{node}",
                    f"background-request-{node}",
                ),
                now=start,
            )
            assert code == 200
        code, _ = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-old", "vision-old-request"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        code, newer = await acquire_lease(
            db,
            _request("f2", "ai_vision", "vision-new", "vision-new-request"),
            now=start + timedelta(seconds=2),
        )
        assert code == 423
        assert newer["status_string"] == "preemption_requested"
        victim_f2 = next(
            lease
            for lease in (
                await db.execute(select(WorkloadLease))
            ).scalars().all()
            if lease.request_id == "background-request-f2"
        )
        await release_lease(
            db,
            victim_f2.lease_id,
            {
                "owner_service_string": "test",
                "owner_task_id_string": "background-f2",
                "request_id_string": "background-request-f2",
                "outcome_string": "preempted",
            },
            now=start + timedelta(seconds=3),
        )
        code, acquired = await acquire_lease(
            db,
            _request("f2", "ai_vision", "vision-new", "vision-new-request"),
            now=start + timedelta(seconds=4),
        )
        assert code == 200
        assert acquired["status_string"] == "acquired"
        old = await db.get(WorkloadWaiter, "vision-old-request")
        assert old.state == "waiting"


def test_fifo_within_same_class_still_prevents_same_gpu_leapfrog():
    async def scenario(db):
        start = datetime.utcnow()
        code, _ = await acquire_lease(
            db,
            _request("f1", "collection_background", "background", "background-request"),
            now=start,
        )
        assert code == 200
        code, _ = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-old", "vision-old-request"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        code, newer = await acquire_lease(
            db,
            _request("f1", "ai_vision", "vision-new", "vision-new-request"),
            now=start + timedelta(seconds=2),
        )
        assert code == 423
        assert newer["status_string"] == "higher_priority_waiting"
        assert newer["blocking_request_id_string"] == "vision-old-request"

    _run(scenario)


def test_ai_stays_active_while_dedicated_autorig_primary_nodes_supply_reserve():
    async def scenario(db):
        start = datetime.utcnow()
        for node, role in (
            ("f1", "ai_vision_primary"),
            ("f2", "ai_vision_primary"),
            ("f11", "autorig_primary"),
            ("f13", "autorig_primary"),
        ):
            node_status = _node_status()
            node_status["workload_role"] = role
            code, _ = await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "source_scope_string": "host_agent",
                    "node_status_by_key": node_status,
                },
                now=start,
            )
            assert code == 200
        for node in ("f1", "f2"):
            code, acquired = await acquire_lease(
                db,
                _request(node, "ai_vision", f"vision-{node}", f"vision-request-{node}"),
                now=start + timedelta(seconds=1),
            )
            assert code == 200
            assert acquired["status_string"] == "acquired"

        db.add(
            Task(
                id="interactive-arrival",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="created",
            )
        )
        await db.commit()
        status = await broker_status(db, now=start + timedelta(seconds=2))
        assert status["policy_by_key"]["interactive_demand_int"] == 1
        assert status["metrics_by_key"]["preemption_requested_total_int"] == 0
        active_ai = list(
            (await db.execute(select(WorkloadLease))).scalars().all()
        )
        assert {lease.physical_resource_id for lease in active_ai} == {"f1", "f2"}
        assert all(lease.state == "active" for lease in active_ai)

        for index, node in enumerate(("f11", "f13"), 1):
            code, acquired = await acquire_lease(
                db,
                _request(
                    node,
                    "autorig_interactive",
                    f"autorig-{index}",
                    f"autorig-request-{index}",
                ),
                now=start + timedelta(seconds=3),
            )
            assert code == 200
            assert acquired["status_string"] == "acquired"

        final_status = await broker_status(db, now=start + timedelta(seconds=4))
        assert final_status["active_by_class_key"]["ai_vision"] == 2
        assert final_status["active_by_class_key"]["autorig_interactive"] == 2

    _run(scenario)


def test_freestock_flat_heartbeat_aliases_are_fail_closed_and_expiry_is_canonical():
    async def scenario(db):
        start = datetime.utcnow()
        code, heartbeat = await node_heartbeat(
            db,
            {
                "node_id_string": "farm-f7",
                "physical_resource_id_string": "farm-f7",
                "node_kind_string": "managed_farm",
                "managed_farm_bool": True,
                "workload_role": "shared",
                "full_converter_bool": True,
                "runtime_ready_bool": True,
                "model_ready_bool": True,
                "ffmpeg_ready_bool": True,
                "arbiter_ready_bool": True,
                "arbiter_accepting_ai_vision_bool": True,
                "healthy_bool": True,
                "accepting_bool": True,
            },
            now=start,
        )
        assert code == 200
        assert heartbeat["physical_resource_id_string"] == "farm-f7"
        code, acquired = await acquire_lease(
            db,
            _request(
                "farm-f7",
                "ai_vision",
                "freestock-task",
                "freestock-request",
                status={
                    "managed_farm_bool": True,
                    "workload_role": "shared",
                    "full_converter_bool": True,
                    "runtime_ready_bool": True,
                    "model_ready_bool": True,
                    "ffmpeg_ready_bool": True,
                    "arbiter_ready_bool": True,
                    "arbiter_accepting_ai_vision_bool": True,
                    "healthy_bool": True,
                    "accepting_bool": True,
                },
            ),
            now=start + timedelta(seconds=1),
        )
        assert code == 200
        lease = acquired["lease_by_key"]
        assert lease["expires_at_string"]
        assert lease["lease_expires_at_utc_timestamp"] == lease["expires_at_string"]
        assert lease["expires_at_utc_timestamp"] == lease["expires_at_string"]

    _run(scenario)


def test_autorig_task_persists_one_lease_before_binding_and_reuses_it():
    async def scenario(db):
        worker_url = "https://converter-f1.example/api-converter-glb/"
        physical = "machine_11111111111111111111111111111111"
        db.add(
            WorkerEndpoint(
                url=worker_url,
                enabled=True,
                physical_resource_id=physical,
                role="autorig_primary",
            )
        )
        task = Task(
            id="autorig-task",
            owner_type="user",
            owner_id="user@example.com",
            queue_class="interactive",
            pipeline_kind="rig",
            status="created",
        )
        db.add(task)
        await db.commit()
        with patch.dict(
            "os.environ",
            {"AUTORIG_WORKLOAD_BROKER_ENABLED": "1"},
        ):
            acquired, lease = await acquire_task_workload_lease(
                db,
                task,
                worker_url.rstrip("/"),
                _worker_status(physical, "autorig_primary"),
            )
            assert acquired is True
            assert task.status == "created"
            assert task.worker_api == worker_url.rstrip("/")
            assert task.workload_lease_id == lease["lease_id_string"]
            first_request = task.workload_request_id
            first_lease = task.workload_lease_id
            code, _ = await heartbeat_task_workload_lease(db, task)
            assert code == 200
            acquired, again = await acquire_task_workload_lease(
                db,
                task,
                worker_url.rstrip("/"),
                _worker_status(physical, "autorig_primary"),
            )
            assert acquired is True
            assert task.workload_request_id == first_request
            assert task.workload_lease_id == first_lease == again["lease_id_string"]
            await release_task_workload_lease(
                db,
                task,
                outcome="released",
                clear_for_retry=True,
            )
            assert task.workload_request_id is None
            assert task.workload_lease_id is None

    _run(scenario)


def test_autorig_waiter_cancel_preserves_binding_until_exact_owner_confirmation():
    async def scenario(db):
        now = datetime.utcnow()
        task = Task(
            id="autorig-waiter-task",
            owner_type="user",
            owner_id="user@example.com",
            queue_class="interactive",
            pipeline_kind="rig",
            status="created",
            workload_request_id="autorig-waiter-request",
            workload_lease_state="waiting",
        )
        db.add(task)
        db.add(
            WorkloadWaiter(
                request_id="autorig-waiter-request",
                physical_resource_id="f11",
                node_id="f11",
                workload_class="autorig_interactive",
                priority=0,
                owner_service="wrong-owner",
                owner_task_id=task.id,
                state="waiting",
                metadata_json="{}",
                created_at=now,
                last_seen_at=now,
                updated_at=now,
            )
        )
        await db.commit()
        with patch.dict("os.environ", {"AUTORIG_WORKLOAD_BROKER_ENABLED": "1"}):
            await release_task_workload_lease(
                db,
                task,
                outcome="released",
                clear_for_retry=True,
            )
            assert task.workload_request_id == "autorig-waiter-request"
            assert task.workload_lease_state == "request_owner_mismatch"

            waiter = await db.get(WorkloadWaiter, "autorig-waiter-request")
            waiter.owner_service = "autorig_dispatcher"
            await db.commit()
            await release_task_workload_lease(
                db,
                task,
                outcome="released",
                clear_for_retry=True,
            )
            assert task.workload_request_id is None
            assert task.workload_lease_id is None
            assert waiter.state == "cancelled"

    _run(scenario)


def test_ambiguous_autorig_submission_survives_terminal_lease_and_preemption():
    async def scenario(db):
        worker_url = "https://converter-f13.example/api-converter-glb"
        physical = "machine_13131313131313131313131313131313"
        db.add(
            WorkerEndpoint(
                url=worker_url,
                enabled=True,
                physical_resource_id=physical,
                role="autorig_primary",
            )
        )
        task = Task(
            id="autorig-ambiguous-task",
            owner_type="user",
            owner_id="user@example.com",
            queue_class="collection_background",
            pipeline_kind="rig",
            status="created",
        )
        db.add(task)
        await db.commit()
        with patch.dict("os.environ", {"AUTORIG_WORKLOAD_BROKER_ENABLED": "1"}):
            acquired, _response = await acquire_task_workload_lease(
                db, task, worker_url, _worker_status(physical, "autorig_primary")
            )
            assert acquired is True
            original = (
                task.workload_request_id,
                task.workload_lease_id,
                task.worker_api,
            )
            task.workload_lease_state = "submission_unknown"
            lease = await db.get(WorkloadLease, task.workload_lease_id)
            lease.state = "expired"
            lease.expires_at = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()

            acquired, terminal_replay = await acquire_task_workload_lease(
                db, task, worker_url, _worker_status(physical, "autorig_primary")
            )
            assert acquired is True
            assert (
                task.workload_request_id,
                task.workload_lease_id,
                task.worker_api,
            ) == original
            assert task.workload_lease_state == "submission_unknown"
            assert terminal_replay["request_id_string"] == original[0]
            assert terminal_replay["lease_id_string"] == original[1]

            # A recall in the same ambiguous interval must first recover the
            # exact host task instead of releasing/rotating central identity.
            lease.state = "preemption_requested"
            lease.preemption_reason = "preempted_by_ai_vision"
            lease.expires_at = datetime.utcnow() + timedelta(minutes=5)
            await db.commit()
            acquired, preempt_replay = await acquire_task_workload_lease(
                db, task, worker_url, _worker_status(physical, "autorig_primary")
            )
            assert acquired is True
            assert (
                task.workload_request_id,
                task.workload_lease_id,
                task.worker_api,
            ) == original
            assert task.workload_lease_state == "preemption_requested"
            assert preempt_replay["request_id_string"] == original[0]
            assert preempt_replay["lease_id_string"] == original[1]

    _run(scenario)


def test_autorig_worker_identity_mismatch_is_capacity_wait_without_lease():
    async def scenario(db):
        worker_url = "https://converter-f2.example/api-converter-glb"
        configured = "machine_22222222222222222222222222222222"
        reported = "machine_33333333333333333333333333333333"
        db.add(
            WorkerEndpoint(
                url=worker_url,
                enabled=True,
                physical_resource_id=configured,
                role="autorig_primary",
            )
        )
        task = Task(
            id="autorig-identity-mismatch",
            owner_type="user",
            owner_id="user@example.com",
            queue_class="interactive",
            pipeline_kind="rig",
            status="created",
        )
        db.add(task)
        await db.commit()
        with patch.dict("os.environ", {"AUTORIG_WORKLOAD_BROKER_ENABLED": "1"}):
            acquired, response = await acquire_task_workload_lease(
                db,
                task,
                worker_url,
                _worker_status(reported, "autorig_primary"),
            )
        assert acquired is False
        assert response["status_string"] == "worker_physical_identity_mismatch"
        assert response["retryable_bool"] is True
        assert task.workload_lease_id is None
        leases = list((await db.execute(select(WorkloadLease))).scalars().all())
        assert leases == []

    _run(scenario)


def test_renderfin_probe_cannot_erase_authoritative_host_capabilities():
    async def scenario(db):
        start = datetime.utcnow()
        code, _ = await node_heartbeat(
            db,
            {
                "node_id_string": "f12",
                "physical_resource_id_string": "f12",
                "source_scope_string": "host_agent",
                "node_status_by_key": _node_status(full=True, ai=True, accepting=True),
            },
            now=start,
        )
        assert code == 200
        node = await db.get(WorkloadNodeState, "f12")
        authority_at = node.authority_heartbeat_at

        # A Renderfin registry/probe does not know the host's AI runtime or
        # full-converter capability. Its false defaults must not erase them.
        code, _ = await node_heartbeat(
            db,
            {
                "node_id_string": "f12",
                "physical_resource_id_string": "f12",
                "source_scope_string": "renderfin_probe",
                "node_status_by_key": {
                    "managed_farm_bool": True,
                    "full_converter_bool": False,
                    "ai_capable_bool": False,
                    "healthy_bool": True,
                    "accepting_bool": True,
                    "arbiter_by_key": {
                        "online_bool": False,
                        "accepting_ai_vision_bool": False,
                    },
                },
            },
            now=start + timedelta(seconds=1),
        )
        assert code == 200
        node = await db.get(WorkloadNodeState, "f12")
        assert node.full_converter is True
        assert node.ai_capable is True
        assert node.authority_source == "host_agent"
        assert node.authority_heartbeat_at == authority_at

        code, acquired = await broker_acquire_lease(
            db,
            _request("f12", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=2),
        )
        assert code == 200
        assert acquired["status_string"] == "acquired"

    _run(scenario)


def test_ai_lease_requires_fresh_authoritative_host_heartbeat():
    async def scenario(db):
        start = datetime.utcnow()
        probe = {
            "node_id_string": "f5",
            "physical_resource_id_string": "f5",
            "source_scope_string": "lease_probe",
            "node_status_by_key": _node_status(full=False, ai=True, accepting=True),
        }
        code, _ = await node_heartbeat(db, probe, now=start)
        assert code == 200
        code, denied = await broker_acquire_lease(
            db,
            _request("f5", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=1),
        )
        assert code == 423
        assert denied["status_string"] == "authoritative_heartbeat_required"

        authority = dict(probe)
        authority["source_scope_string"] = "host_agent"
        code, _ = await node_heartbeat(
            db, authority, now=start + timedelta(seconds=2)
        )
        assert code == 200
        code, acquired = await broker_acquire_lease(
            db,
            _request("f5", "ai_vision", "vision", "vision-request"),
            now=start + timedelta(seconds=3),
        )
        assert code == 200
        lease = acquired["lease_by_key"]
        code, _ = await release_lease(
            db,
            lease["lease_id_string"],
            {
                "owner_service_string": "test",
                "owner_task_id_string": "vision",
                "request_id_string": "vision-request",
                "outcome_string": "completed",
            },
            now=start + timedelta(seconds=4),
        )
        assert code == 200

        # A later probe refreshes transport liveness but not host authority.
        code, _ = await node_heartbeat(
            db, probe, now=start + timedelta(seconds=901)
        )
        assert code == 200
        code, expired = await broker_acquire_lease(
            db,
            _request("f5", "ai_vision", "vision-2", "vision-request-2"),
            now=start + timedelta(seconds=902),
        )
        assert code == 423
        assert expired["status_string"] == "authoritative_heartbeat_required"

    _run(scenario)


def test_probe_cannot_keep_active_ai_lease_alive_after_host_authority_expires():
    async def scenario(db):
        start = datetime.utcnow()
        authority = {
            "node_id_string": "f5",
            "physical_resource_id_string": "f5",
            "source_scope_string": "host_agent",
            "node_status_by_key": _node_status(
                full=False, ai=True, accepting=True
            ),
        }
        code, _ = await node_heartbeat(db, authority, now=start)
        assert code == 200

        request = _request(
            "f5", "ai_vision", "vision-active", "vision-request-active"
        )
        request["ttl_seconds_int"] = 600
        code, acquired = await broker_acquire_lease(
            db, request, now=start + timedelta(seconds=1)
        )
        assert code == 200
        lease_id = acquired["lease_by_key"]["lease_id_string"]
        exact_owner = {
            "owner_service_string": "test",
            "owner_task_id_string": "vision-active",
            "request_id_string": "vision-request-active",
            "ttl_seconds_int": 600,
        }
        lease = await db.get(WorkloadLease, lease_id)
        waiter = await db.get(WorkloadWaiter, "vision-request-active")
        original_heartbeat = lease.heartbeat_at
        original_expiry = lease.expires_at
        original_waiter_seen = waiter.last_seen_at
        original_owner = (
            lease.owner_service,
            lease.owner_task_id,
            lease.request_id,
            lease.physical_resource_id,
        )

        # A transport probe is fresh, but host arbitration authority is not.
        probe = dict(authority)
        probe["source_scope_string"] = "lease_probe"
        code, _ = await node_heartbeat(
            db, probe, now=start + timedelta(seconds=181)
        )
        assert code == 200
        node = await db.get(WorkloadNodeState, "f5")
        assert node.heartbeat_at == start + timedelta(seconds=181)
        assert node.authority_heartbeat_at == start
        assert node.authority_source == "host_agent"

        code, replay_denied = await broker_acquire_lease(
            db, request, now=start + timedelta(seconds=182)
        )
        assert code == 423
        assert (
            replay_denied["status_string"]
            == "authoritative_heartbeat_required"
        )
        assert replay_denied["retryable_bool"] is True
        assert replay_denied["lease_by_key"]["lease_id_string"] == lease_id

        code, heartbeat_denied = await heartbeat_lease(
            db,
            lease_id,
            exact_owner,
            now=start + timedelta(seconds=183),
        )
        assert code == 423
        assert (
            heartbeat_denied["status_string"]
            == "authoritative_heartbeat_required"
        )
        assert heartbeat_denied["retryable_bool"] is True
        assert heartbeat_denied["lease_by_key"]["lease_id_string"] == lease_id

        lease = await db.get(WorkloadLease, lease_id)
        waiter = await db.get(WorkloadWaiter, "vision-request-active")
        assert lease.state == "active"
        assert lease.heartbeat_at == original_heartbeat
        assert lease.expires_at == original_expiry
        assert waiter.last_seen_at == original_waiter_seen
        assert (
            lease.owner_service,
            lease.owner_task_id,
            lease.request_id,
            lease.physical_resource_id,
        ) == original_owner

        # Restoring host authority lets the exact same request and lease IDs
        # continue idempotently; no replacement owner or lease is created.
        code, _ = await node_heartbeat(
            db, authority, now=start + timedelta(seconds=184)
        )
        assert code == 200
        code, replay = await broker_acquire_lease(
            db, request, now=start + timedelta(seconds=185)
        )
        assert code == 200
        assert replay["status_string"] == "renewed"
        assert replay["lease_by_key"]["lease_id_string"] == lease_id
        code, heartbeat = await heartbeat_lease(
            db,
            lease_id,
            exact_owner,
            now=start + timedelta(seconds=186),
        )
        assert code == 200
        assert heartbeat["status_string"] == "active"
        assert heartbeat["lease_by_key"]["lease_id_string"] == lease_id

        leases = list((await db.execute(select(WorkloadLease))).scalars().all())
        assert len(leases) == 1
        assert (
            leases[0].owner_service,
            leases[0].owner_task_id,
            leases[0].request_id,
            leases[0].physical_resource_id,
        ) == original_owner

    _run(scenario)


def test_lease_heartbeat_and_release_require_exact_three_part_owner():
    async def scenario(db):
        code, acquired = await acquire_lease(
            db, _request("f5", "ai_vision", "vision", "vision-request")
        )
        assert code == 200
        lease_id = acquired["lease_by_key"]["lease_id_string"]
        exact = {
            "owner_service_string": "test",
            "owner_task_id_string": "vision",
            "request_id_string": "vision-request",
        }
        for missing in exact:
            payload = dict(exact)
            payload.pop(missing)
            hb_code, _ = await heartbeat_lease(db, lease_id, payload)
            release_code, _ = await release_lease(
                db, lease_id, {**payload, "outcome_string": "released"}
            )
            assert hb_code == 409
            assert release_code == 409
        for field in exact:
            payload = dict(exact)
            payload[field] += "-wrong"
            hb_code, _ = await heartbeat_lease(db, lease_id, payload)
            release_code, _ = await release_lease(
                db, lease_id, {**payload, "outcome_string": "released"}
            )
            assert hb_code == 409
            assert release_code == 409
        hb_code, _ = await heartbeat_lease(db, lease_id, exact)
        assert hb_code == 200
        release_code, first = await release_lease(
            db, lease_id, {**exact, "outcome_string": "released"}
        )
        duplicate_code, duplicate = await release_lease(
            db, lease_id, {**exact, "outcome_string": "released"}
        )
        assert release_code == duplicate_code == 200
        assert first["status_string"] == duplicate["status_string"] == "released"

    _run(scenario)


def test_waiter_cancel_requires_exact_service_and_task_owner():
    async def scenario(db):
        now = datetime.utcnow()
        db.add(
            WorkloadWaiter(
                request_id="vision-request",
                physical_resource_id="f5",
                node_id="f5",
                workload_class="ai_vision",
                priority=0,
                owner_service="freestock_gateway",
                owner_task_id="vision",
                state="waiting",
                metadata_json="{}",
                created_at=now,
                last_seen_at=now,
                updated_at=now,
            )
        )
        await db.commit()

        for candidate in (
            {},
            {"owner_service_string": "freestock_gateway"},
            {"owner_task_id_string": "vision"},
            {
                "owner_service_string": "wrong_service",
                "owner_task_id_string": "vision",
            },
            {
                "owner_service_string": "freestock_gateway",
                "owner_task_id_string": "wrong_task",
            },
        ):
            cancel_code, rejected = await cancel_waiter(
                db, "vision-request", candidate
            )
            assert cancel_code == 409
            assert rejected["status_string"] == "request_owner_mismatch"
            waiter = await db.get(WorkloadWaiter, "vision-request")
            assert waiter is not None and waiter.state == "waiting"

        exact = {
            "owner_service_string": "freestock_gateway",
            "owner_task_id_string": "vision",
        }
        cancel_code, cancelled = await cancel_waiter(
            db, "vision-request", exact
        )
        assert cancel_code == 200
        assert cancelled["status_string"] == "cancelled"
        waiter = await db.get(WorkloadWaiter, "vision-request")
        assert waiter is not None and waiter.state == "cancelled"

        replay_code, replay = await cancel_waiter(
            db, "vision-request", exact
        )
        assert replay_code == 200
        assert replay["status_string"] == "cancelled"

    _run(scenario)


def test_late_durable_completion_overrides_preempted_terminal():
    async def scenario(db):
        code, background = await acquire_lease(
            db,
            _request("f13", "collection_background", "background", "background-request"),
        )
        assert code == 200
        await acquire_lease(
            db, _request("f13", "ai_vision", "vision", "vision-request")
        )
        lease_id = background["lease_by_key"]["lease_id_string"]
        owner = {
            "owner_service_string": "test",
            "owner_task_id_string": "background",
            "request_id_string": "background-request",
        }
        code, preempted = await release_lease(
            db, lease_id, {**owner, "outcome_string": "preempted"}
        )
        assert code == 200
        assert preempted["status_string"] == "preempted"
        code, completed = await release_lease(
            db, lease_id, {**owner, "outcome_string": "completed"}
        )
        assert code == 200
        assert completed["status_string"] == "completed"
        code, immutable = await release_lease(
            db, lease_id, {**owner, "outcome_string": "released"}
        )
        assert code == 200
        assert immutable["status_string"] == "completed"

    _run(scenario)


def test_every_advertised_ai_resource_can_acquire_under_same_snapshot():
    async def scenario(db):
        now = datetime.utcnow()
        for node in ("f1", "f2", "f13"):
            await node_heartbeat(
                db,
                {
                    "node_id_string": node,
                    "physical_resource_id_string": node,
                    "source_scope_string": "host_agent",
                    "node_status_by_key": _node_status(),
                },
                now=now,
            )
        db.add(
            Task(
                id="interactive-demand",
                owner_type="user",
                owner_id="user@example.com",
                queue_class="interactive",
                pipeline_kind="rig",
                status="created",
            )
        )
        await db.commit()
        status = await broker_status(db, now=now + timedelta(seconds=1))
        advertised = [
            item["physical_resource_id_string"]
            for item in status["resources_list"]
            if item["accepting_ai_vision_bool"]
        ]
        assert advertised
        target = advertised[0]
        code, acquired = await broker_acquire_lease(
            db,
            _request(target, "ai_vision", "vision", "vision-request"),
            now=now + timedelta(seconds=1),
        )
        assert code == 200, acquired

    _run(scenario)
