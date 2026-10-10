"""Unmounted composition layer for the AutoRig V3-only task runtime.

The binding row is written in the *same database transaction* as ``Task``.
Consequently a process death between Task commit and the file-backed dispatch
outbox cannot lose the task: startup recovery reconstructs the exact attempt.
Importing this module performs no schema migration, networking, or task work.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

try:  # Production AsyncSession requires SQLAlchemy text(); tests use strings.
    from sqlalchemy import text as _sql
except ImportError:  # pragma: no cover - exercised by the dependency-light tests
    _sql = lambda value: value

from v3_dispatch_outbox import DispatchIdentity, OutboxRecord, V3DispatchOutbox


PIPELINE_REVISION = "autorig-v3-rig/1"
RUNTIME_SCHEMA_VERSION = 1
NONTERMINAL_BINDING_STATES = frozenset({"pending", "dispatching", "queued", "running",
                                        "awaiting_artifact_verification", "needs_review"})


class RuntimeContractError(ValueError):
    pass


@dataclass(frozen=True)
class TaskBinding:
    task_id: str
    source_sha256: str
    source_manifest: Mapping[str, Any]
    requested_intent: str
    dispatch_intent: str
    pipeline_revision: str
    attempt: int
    state: str
    upstream_receipt: Mapping[str, Any] | None = None
    intent_plan: Mapping[str, Any] | None = None

    @property
    def identity(self) -> DispatchIdentity:
        return DispatchIdentity.create(self.task_id, self.source_sha256, self.dispatch_intent,
                                       self.pipeline_revision, self.attempt)


@dataclass(frozen=True)
class TaskStatusPatch:
    status: str
    error_message: str | None
    v3_state: str
    terminal: bool


def task_status_patch(record: OutboxRecord) -> TaskStatusPatch:
    """Map V3 state without ever presenting review/unverified work as done."""
    state = record.state
    if state == "done":
        return TaskStatusPatch("done", None, state, True)
    if state in {"failed", "blocked_protocol"}:
        return TaskStatusPatch("error", record.error or "V3 processing failed", state, True)
    if state == "needs_review":
        return TaskStatusPatch("processing", "V3 result requires review", state, False)
    if state in {"pending_register", "pending_submit"}:
        return TaskStatusPatch("created", None, state, False)
    return TaskStatusPatch("processing", None, state, False)


def normalize_task_binding(*, task_id: str, source_sha256: str,
                           source_manifest: Mapping[str, Any], requested_intent: str,
                           attempt: int = 1,
                           upstream_receipt: Mapping[str, Any] | None = None,
                           intent_plan: Mapping[str, Any] | None = None) -> TaskBinding:
    """Create the one current V3 task contract.

    ``generate`` is accepted only after a hash-bound generation receipt;
    ``convert`` only after an explicit classification plan.  Thus neither is
    silently relabelled as a rig request before source normalization.
    """
    requested = str(requested_intent or "").strip().lower()
    if requested not in {"rig", "convert", "generate"}:
        raise RuntimeContractError("unsupported V3 task intent")
    manifest = dict(source_manifest)
    if manifest.get("intent") != "rig":
        raise RuntimeContractError("V3 task source manifest must declare rig intent")
    identity = DispatchIdentity.create(task_id, source_sha256, "rig", PIPELINE_REVISION, attempt)
    if str(manifest.get("sha256") or "").lower() != identity.source_sha256:
        raise RuntimeContractError("source manifest hash mismatch")
    receipt = dict(upstream_receipt) if isinstance(upstream_receipt, Mapping) else None
    plan = dict(intent_plan) if isinstance(intent_plan, Mapping) else None
    if requested == "generate":
        _validate_evidence(receipt, schema="autorig.v3.generation-receipt/1",
                           task_id=identity.task_id, source_sha256=identity.source_sha256,
                           digest_field="receipt_sha256")
        if manifest.get("upstream_receipt_sha256") != receipt["receipt_sha256"]:
            raise RuntimeContractError("normalized source is not bound to its generation receipt")
    if requested == "convert":
        _validate_evidence(plan, schema="autorig.v3.intent-plan/1", task_id=identity.task_id,
                           source_sha256=identity.source_sha256, digest_field="plan_sha256")
        if plan.get("requested_intent") != "convert" or plan.get("dispatch_intent") != "rig":
            raise RuntimeContractError("convert classification plan does not explicitly select V3 rig")
    return TaskBinding(identity.task_id, identity.source_sha256, manifest, requested, "rig",
                       PIPELINE_REVISION, attempt, "pending", receipt, plan)


def _validate_evidence(value: Mapping[str, Any] | None, *, schema: str, task_id: str,
                       source_sha256: str, digest_field: str) -> None:
    if not isinstance(value, Mapping) or value.get("schema") != schema or \
            value.get("status") != "accepted" or value.get("task_id") != task_id or \
            value.get("source_sha256") != source_sha256:
        raise RuntimeContractError(f"{schema} evidence is missing or not source-bound")
    body = {key: item for key, item in value.items() if key != digest_field}
    actual = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if value.get(digest_field) != actual:
        raise RuntimeContractError(f"{schema} evidence digest mismatch")


class SameTaskDbBindings:
    """SQL-only binding store designed for the existing Task AsyncSession."""

    async def install(self, session: Any) -> None:
        await session.execute(_sql("""
            CREATE TABLE IF NOT EXISTS v3_dispatch_runtime_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """))
        version = await session.execute(_sql(
            "SELECT value FROM v3_dispatch_runtime_meta WHERE key='schema_version'"
        ))
        rows = version.mappings().all()
        if rows and int(rows[0]["value"]) > RUNTIME_SCHEMA_VERSION:
            raise RuntimeError("V3 dispatch runtime schema is newer than this code")
        await session.execute(_sql("""
            CREATE TABLE IF NOT EXISTS v3_dispatch_task_bindings (
                task_id VARCHAR(36) NOT NULL,
                attempt INTEGER NOT NULL,
                source_sha256 VARCHAR(64) NOT NULL,
                source_manifest_json TEXT NOT NULL,
                requested_intent VARCHAR(32) NOT NULL,
                dispatch_intent VARCHAR(32) NOT NULL,
                pipeline_revision VARCHAR(128) NOT NULL,
                upstream_receipt_json TEXT,
                intent_plan_json TEXT,
                state VARCHAR(40) NOT NULL,
                run_id VARCHAR(128),
                idempotency_key VARCHAR(67),
                error TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(task_id, attempt),
                FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
            )
        """))
        await session.execute(_sql(
            "CREATE INDEX IF NOT EXISTS ix_v3_dispatch_task_bindings_state "
            "ON v3_dispatch_task_bindings(state, updated_at)"
        ))
        await session.execute(_sql("""
            INSERT INTO v3_dispatch_runtime_meta(key,value) VALUES('schema_version',:version)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """), {"version": str(RUNTIME_SCHEMA_VERSION)})

    async def bind_in_task_transaction(self, session: Any, binding: TaskBinding) -> None:
        """Call after session.add(Task), before the caller's single commit."""
        canonical = normalize_task_binding(
            task_id=binding.task_id, source_sha256=binding.source_sha256,
            source_manifest=binding.source_manifest, requested_intent=binding.requested_intent,
            attempt=binding.attempt, upstream_receipt=binding.upstream_receipt,
            intent_plan=binding.intent_plan,
        )
        encoded = json.dumps(dict(canonical.source_manifest), ensure_ascii=False,
                             sort_keys=True, separators=(",", ":"))
        flush = getattr(session, "flush", None)
        if flush is not None:
            await flush()
        await session.execute(_sql("""
            INSERT INTO v3_dispatch_task_bindings(
                task_id,attempt,source_sha256,source_manifest_json,requested_intent,
                dispatch_intent,pipeline_revision,upstream_receipt_json,intent_plan_json,state
            ) VALUES(:task_id,:attempt,:source_sha256,:manifest,:requested_intent,
                     :dispatch_intent,:pipeline_revision,:upstream_receipt,:intent_plan,'pending')
        """), {"task_id": canonical.task_id, "attempt": canonical.attempt,
                "source_sha256": canonical.source_sha256, "manifest": encoded,
                "requested_intent": canonical.requested_intent,
                "dispatch_intent": canonical.dispatch_intent,
                "pipeline_revision": canonical.pipeline_revision,
                "upstream_receipt": json.dumps(canonical.upstream_receipt, sort_keys=True) if canonical.upstream_receipt else None,
                "intent_plan": json.dumps(canonical.intent_plan, sort_keys=True) if canonical.intent_plan else None})

    async def recover_pending(self, session: Any) -> list[TaskBinding]:
        result = await session.execute(_sql("""
            SELECT b.* FROM v3_dispatch_task_bindings b
            JOIN tasks t ON t.id=b.task_id
            WHERE b.state NOT IN ('done','failed','blocked_protocol','superseded')
              AND b.attempt=(SELECT MAX(x.attempt) FROM v3_dispatch_task_bindings x WHERE x.task_id=b.task_id)
            ORDER BY b.created_at,b.task_id
        """))
        rows = result.mappings().all()
        return [self._binding(row) for row in rows]

    async def record_outbox(self, session: Any, record: OutboxRecord) -> None:
        result = await session.execute(_sql("""
            UPDATE v3_dispatch_task_bindings SET state=:state,run_id=:run_id,
                idempotency_key=:idempotency_key,error=:error,updated_at=CURRENT_TIMESTAMP
            WHERE task_id=:task_id AND attempt=:attempt AND source_sha256=:source_sha256
              AND pipeline_revision=:pipeline_revision
        """), {"state": record.state, "run_id": record.run_id,
                "idempotency_key": record.idempotency_key, "error": record.error,
                "task_id": record.identity.task_id, "attempt": record.identity.attempt,
                "source_sha256": record.identity.source_sha256,
                "pipeline_revision": record.identity.pipeline_revision})
        if getattr(result, "rowcount", 1) != 1:
            raise RuntimeContractError("V3 Task binding disappeared or changed")

    async def create_retry_in_transaction(self, session: Any, current: TaskBinding) -> TaskBinding:
        if current.state not in {"needs_review", "failed", "blocked_protocol"}:
            raise RuntimeContractError("a new V3 attempt requires a review or failed prior attempt")
        changed = await session.execute(_sql("""
            UPDATE v3_dispatch_task_bindings SET state='superseded',updated_at=CURRENT_TIMESTAMP
            WHERE task_id=:task_id AND attempt=:attempt AND state=:state
        """), {"task_id": current.task_id, "attempt": current.attempt, "state": current.state})
        if getattr(changed, "rowcount", 1) != 1:
            raise RuntimeContractError("V3 retry lost its current-attempt compare-and-swap")
        retry = normalize_task_binding(task_id=current.task_id, source_sha256=current.source_sha256,
                                       source_manifest=current.source_manifest,
                                       requested_intent=current.requested_intent,
                                       attempt=current.attempt + 1,
                                       upstream_receipt=current.upstream_receipt,
                                       intent_plan=current.intent_plan)
        await self.bind_in_task_transaction(session, retry)
        return retry

    @staticmethod
    def _binding(row: Mapping[str, Any]) -> TaskBinding:
        receipt = json.loads(row["upstream_receipt_json"]) if row.get("upstream_receipt_json") else None
        plan = json.loads(row["intent_plan_json"]) if row.get("intent_plan_json") else None
        return TaskBinding(str(row["task_id"]), str(row["source_sha256"]),
                           json.loads(row["source_manifest_json"]),
                           str(row["requested_intent"]), str(row["dispatch_intent"]),
                           str(row["pipeline_revision"]), int(row["attempt"]), str(row["state"]),
                           receipt, plan)


Projector = Callable[[Any, TaskBinding, TaskStatusPatch, OutboxRecord], Awaitable[None]]


class V3TaskRuntime:
    """Factory-owned bounded poll lifecycle; not started by module import."""

    def __init__(self, *, session_factory: Callable[[], Any], bindings: SameTaskDbBindings,
                 outbox: V3DispatchOutbox, remote_factory: Callable[[], Any],
                 projector: Projector, artifact_verifier: Any, workers: int = 1,
                 idle_seconds: float = 1.0) -> None:
        if workers < 1 or workers > 8:
            raise RuntimeContractError("V3 runtime worker count must be between 1 and 8")
        if artifact_verifier is None:
            raise RuntimeContractError("independent artifact verifier is required")
        self.session_factory, self.bindings, self.outbox = session_factory, bindings, outbox
        self.remote_factory, self.projector = remote_factory, projector
        self.artifact_verifier, self.worker_count = artifact_verifier, workers
        self.idle_seconds = max(.05, float(idle_seconds))
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task] = []

    async def start(self) -> None:
        if self._workers:
            return
        self.outbox.migrate()
        await self._recover()
        self._stop.clear()
        self._workers = [asyncio.create_task(self._loop(), name=f"v3-task-runtime-{i}")
                         for i in range(self.worker_count)]

    async def close(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def _recover(self) -> None:
        async with self.session_factory() as session:
            for binding in await self.bindings.recover_pending(session):
                self.outbox.enqueue(binding.identity, binding.source_manifest)

    async def run_once(self) -> bool:
        worked = await self.outbox.process_one(
            self.remote_factory(), artifact_verifier=self.artifact_verifier)
        if not worked:
            await self._recover()
            return False
        await self._project_all()
        return True

    async def _project_all(self) -> None:
        async with self.session_factory() as session:
            bindings = await self.bindings.recover_pending(session)
            for binding in bindings:
                try:
                    record = self.outbox.get(binding.identity)
                except KeyError:
                    continue
                await self.bindings.record_outbox(session, record)
                await self.projector(session, binding, task_status_patch(record), record)
            await session.commit()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            if not await self.run_once():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.idle_seconds)
                except asyncio.TimeoutError:
                    pass
