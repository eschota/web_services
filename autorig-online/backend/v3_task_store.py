"""Durable SQLite binding and callback ledger for AutoRig V3 dispatches.

This module owns only its ``v3_*`` tables.  ``migrate()`` is explicit so an
import cannot alter production.  A caller may supply ``task_projector`` to
update the existing ``tasks`` row on the same sqlite connection and inside the
same transaction as the callback event, artifacts and notification outbox.
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import hashlib
import inspect
import json
import pathlib
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from v3_callback_receiver import (
    CallbackBinding,
    CallbackCommit,
    CallbackConflict,
    callback_event_id,
)
from v3_pipeline_adapter import (
    REQUIRED_RIG_ARTIFACTS,
    V3ProtocolError,
    V3State,
    V3Status,
    V3Submission,
    parse_status,
)


SCHEMA_VERSION = 2
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_KEY = re.compile(r"^v3-[0-9a-f]{64}$")
_CREDENTIAL = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_TERMINAL = frozenset({"done", "needs_review", "failed"})


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS v3_store_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS v3_task_bindings (
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    pipeline_kind TEXT NOT NULL DEFAULT 'v3',
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'queued',
    stage TEXT,
    progress REAL NOT NULL DEFAULT 0 CHECK(progress >= 0 AND progress <= 1),
    last_sequence INTEGER NOT NULL DEFAULT -1,
    terminal_event_id TEXT,
    qa_status TEXT,
    artifact_manifest_json TEXT,
    artifact_manifest_sha256 TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(task_id, attempt),
    UNIQUE(run_id),
    UNIQUE(idempotency_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_v3_task_binding_active
    ON v3_task_bindings(task_id) WHERE active = 1;

CREATE TABLE IF NOT EXISTS v3_callback_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    receipt_id TEXT NOT NULL UNIQUE,
    committed_at TEXT NOT NULL,
    UNIQUE(task_id, attempt, sequence),
    FOREIGN KEY(task_id, attempt) REFERENCES v3_task_bindings(task_id, attempt)
);

CREATE TABLE IF NOT EXISTS v3_task_artifacts (
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    role TEXT NOT NULL,
    path TEXT NOT NULL,
    url TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    source_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(task_id, attempt, role),
    FOREIGN KEY(task_id, attempt) REFERENCES v3_task_bindings(task_id, attempt)
);

CREATE TABLE IF NOT EXISTS v3_notification_outbox (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'leased', 'sent')),
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY(event_id) REFERENCES v3_callback_events(event_id)
);
CREATE INDEX IF NOT EXISTS ix_v3_notification_pending
    ON v3_notification_outbox(state, created_at);
"""

MIGRATE_V1_TO_V2_SQL = """
DROP INDEX IF EXISTS ix_v3_notification_pending;
ALTER TABLE v3_notification_outbox RENAME TO v3_notification_outbox_v1;
CREATE TABLE v3_notification_outbox (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'leased', 'sent')),
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY(event_id) REFERENCES v3_callback_events(event_id)
);
INSERT INTO v3_notification_outbox(
    event_id,task_id,attempt,kind,state,delivery_attempts,last_error,created_at,sent_at
) SELECT event_id,task_id,attempt,kind,state,delivery_attempts,last_error,created_at,sent_at
  FROM v3_notification_outbox_v1;
DROP TABLE v3_notification_outbox_v1;
CREATE INDEX ix_v3_notification_pending ON v3_notification_outbox(state, created_at);
"""


TASK_PROJECTION_COLUMNS = frozenset({
    "status", "output_urls", "ready_urls", "ready_count", "total_count", "error_message",
    "worker_api", "worker_task_id", "progress_page", "last_progress_at", "updated_at",
    "pipeline_kind", "viewer_prepared_glb_url", "viewer_animations_glb_url",
    "fbx_glb_output_url", "fbx_glb_model_name", "fbx_glb_ready", "fbx_glb_error",
    "video_url", "video_ready",
})


@dataclass(frozen=True)
class TaskProjection:
    """Values for exactly one existing ``tasks.id`` row."""
    values: Mapping[str, Any]


TaskProjector = Callable[
    [CallbackBinding, Mapping[str, Any], V3Status | None, tuple[Any, ...]],
    TaskProjection | None,
]


@dataclass(frozen=True)
class BindingSnapshot:
    binding: CallbackBinding
    status: str
    stage: str | None
    progress: float
    last_sequence: int
    terminal_event_id: str | None
    artifact_manifest_sha256: str | None


@dataclass(frozen=True)
class OutboxItem:
    event_id: str
    task_id: str
    attempt: int
    kind: str
    state: str
    delivery_attempts: int
    lease_owner: str | None = None
    lease_until: str | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _canonical_task(value: Any) -> str:
    try:
        task = str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise CallbackConflict("task_id is not a canonical UUID") from exc
    if task != value:
        raise CallbackConflict("task_id is not canonical")
    return task


def _validate_binding(binding: CallbackBinding) -> None:
    if not isinstance(binding, CallbackBinding):
        raise CallbackConflict("V3 binding has an invalid type")
    _canonical_task(binding.task_id)
    if not _RUN_ID.fullmatch(binding.run_id):
        raise CallbackConflict("V3 binding run_id is invalid")
    if not _SHA256.fullmatch(binding.source_sha256):
        raise CallbackConflict("V3 binding source_sha256 is invalid")
    if isinstance(binding.attempt, bool) or not isinstance(binding.attempt, int) or binding.attempt < 1:
        raise CallbackConflict("V3 binding attempt must be positive")
    if not _KEY.fullmatch(binding.idempotency_key):
        raise CallbackConflict("V3 binding idempotency_key is invalid")
    expected = V3Submission.create(binding.task_id, binding.source_sha256, binding.attempt)
    if binding.idempotency_key != expected.idempotency_key:
        raise CallbackConflict("V3 binding idempotency_key is not bound to task/source/attempt")
    if not _CREDENTIAL.fullmatch(binding.credential_id):
        raise CallbackConflict("V3 binding credential_id is invalid")
    if binding.pipeline_kind != "v3":
        raise CallbackConflict("V3 binding pipeline_kind is invalid")


def _binding_from_row(row: sqlite3.Row) -> CallbackBinding:
    return CallbackBinding(
        row["task_id"], row["run_id"], row["source_sha256"], row["attempt"],
        row["idempotency_key"], row["credential_id"], row["pipeline_kind"],
    )


class V3TaskStore:
    """Implements both BindingStore and CallbackTransaction protocols."""

    def __init__(self, database: str | pathlib.Path, *, task_projector: TaskProjector | None = None):
        self.database = pathlib.Path(database)
        self.task_projector = task_projector
        self._callback_claim = contextvars.ContextVar(f"v3_callback_claim_{id(self)}", default=None)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    @contextlib.contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    async def migrate(self) -> None:
        await asyncio.to_thread(self._migrate_sync)

    def _migrate_sync(self) -> None:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            version = 0
            meta_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='v3_store_meta'"
            ).fetchone()
            if meta_exists:
                row = connection.execute(
                    "SELECT value FROM v3_store_meta WHERE key='schema_version'"
                ).fetchone()
                version = int(row["value"]) if row is not None else 0
                if version > SCHEMA_VERSION:
                    raise RuntimeError("V3 task store schema is newer than this code")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            upgrade = MIGRATE_V1_TO_V2_SQL if version == 1 else ""
            script = ("BEGIN IMMEDIATE;\n" + upgrade + "\n" + SCHEMA_SQL +
                      f"\nINSERT INTO v3_store_meta(key,value) VALUES('schema_version','{SCHEMA_VERSION}') "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value;\nCOMMIT;")
            try:
                connection.executescript(script)
            except Exception:
                with contextlib.suppress(sqlite3.Error):
                    connection.execute("ROLLBACK")
                raise
        finally:
            connection.close()

    async def bind_dispatch(self, binding: CallbackBinding) -> BindingSnapshot:
        _validate_binding(binding)
        return await asyncio.to_thread(self._bind_dispatch_sync, binding)

    def _bind_dispatch_sync(self, binding: CallbackBinding) -> BindingSnapshot:
        now = _utcnow()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT * FROM v3_task_bindings WHERE task_id=? AND active=1", (binding.task_id,)
                ).fetchone()
                if current is not None:
                    exact = all((
                        current["attempt"] == binding.attempt,
                        current["run_id"] == binding.run_id,
                        current["source_sha256"] == binding.source_sha256,
                        current["idempotency_key"] == binding.idempotency_key,
                        current["credential_id"] == binding.credential_id,
                    ))
                    if exact:
                        connection.commit()
                        return self._snapshot(current)
                    if binding.attempt <= current["attempt"]:
                        raise CallbackConflict("V3 dispatch attempt is stale or conflicts with the active attempt")
                    connection.execute(
                        "UPDATE v3_task_bindings SET active=0, updated_at=? WHERE task_id=? AND active=1",
                        (now, binding.task_id),
                    )
                connection.execute(
                    "INSERT INTO v3_task_bindings(task_id,attempt,run_id,source_sha256,idempotency_key,"
                    "credential_id,pipeline_kind,active,status,progress,last_sequence,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,1,'queued',0,-1,?,?)",
                    (binding.task_id, binding.attempt, binding.run_id, binding.source_sha256,
                     binding.idempotency_key, binding.credential_id, binding.pipeline_kind, now, now),
                )
                row = connection.execute(
                    "SELECT * FROM v3_task_bindings WHERE task_id=? AND attempt=?",
                    (binding.task_id, binding.attempt),
                ).fetchone()
                connection.commit()
                return self._snapshot(row)
            except Exception:
                connection.rollback()
                raise

    async def get_binding(self, task_id: str) -> CallbackBinding | None:
        try:
            task_id = _canonical_task(task_id)
        except CallbackConflict:
            return None
        return await asyncio.to_thread(self._get_binding_sync, task_id)

    def _get_binding_sync(self, task_id: str) -> CallbackBinding | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM v3_task_bindings WHERE task_id=? AND active=1", (task_id,)
            ).fetchone()
            return _binding_from_row(row) if row is not None else None

    async def snapshot(self, task_id: str) -> BindingSnapshot | None:
        task = _canonical_task(task_id)
        return await asyncio.to_thread(self._snapshot_sync, task)

    def _snapshot_sync(self, task_id: str) -> BindingSnapshot | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM v3_task_bindings WHERE task_id=? AND active=1", (task_id,)
            ).fetchone()
            return self._snapshot(row) if row is not None else None

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> BindingSnapshot:
        return BindingSnapshot(
            _binding_from_row(row), row["status"], row["stage"], float(row["progress"]),
            int(row["last_sequence"]), row["terminal_event_id"], row["artifact_manifest_sha256"],
        )

    async def accept(
        self,
        binding: CallbackBinding,
        event_id: str,
        payload: Mapping[str, Any],
        terminal_status: V3Status | None,
    ) -> CallbackCommit:
        _validate_binding(binding)
        owner = "callback:" + uuid.uuid4().hex
        commit = await asyncio.to_thread(self._accept_sync, binding, event_id, dict(payload), terminal_status, owner)
        self._callback_claim.set((binding.task_id, binding.attempt, event_id, owner)
                                 if commit.notification_required else None)
        return commit

    def _accept_sync(
        self,
        binding: CallbackBinding,
        event_id: str,
        payload: dict[str, Any],
        terminal_status: V3Status | None,
        callback_owner: str,
    ) -> CallbackCommit:
        payload_json = _canonical(payload)
        payload_sha = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        if event_id != callback_event_id(payload):
            raise CallbackConflict("callback event_id does not match its payload")
        sequence = payload.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise CallbackConflict("callback sequence is invalid")
        now = _utcnow()
        receipt = "v3receipt-" + event_id.removeprefix("v3cb-")

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM v3_task_bindings WHERE task_id=? AND active=1", (binding.task_id,)
                ).fetchone()
                if row is None or _binding_from_row(row) != binding:
                    raise CallbackConflict("callback attempt is stale or no longer active")

                existing = connection.execute(
                    "SELECT * FROM v3_callback_events WHERE event_id=?", (event_id,)
                ).fetchone()
                if existing is not None:
                    if existing["payload_sha256"] != payload_sha or existing["payload_json"] != payload_json:
                        raise CallbackConflict("callback event replay payload mismatch")
                    claimed = self._claim_callback_notification(connection, event_id, callback_owner, now)
                    connection.commit()
                    return CallbackCommit(existing["receipt_id"], True, claimed)

                if sequence <= int(row["last_sequence"]):
                    raise CallbackConflict("callback sequence is not monotonic")
                if row["terminal_event_id"]:
                    raise CallbackConflict("callback arrived after terminal state")
                if (payload.get("task_id"), payload.get("run_id"), payload.get("source_sha256"),
                        payload.get("attempt"), payload.get("idempotency_key")) != (
                        binding.task_id, binding.run_id, binding.source_sha256,
                        binding.attempt, binding.idempotency_key):
                    raise CallbackConflict("callback no longer matches its active binding")

                event_type = payload.get("event")
                if event_type not in {"progress", "terminal"}:
                    raise CallbackConflict("callback event type is invalid")
                status = str(payload.get("status") or "").strip().lower()
                stage = row["stage"]
                progress = float(row["progress"])
                artifacts: tuple[Any, ...] = ()
                manifest_json = None
                manifest_sha = None
                qa_status = None
                terminal_event_id = None
                notify = False

                if event_type == "progress":
                    if status not in {"queued", "running", "needs_review"}:
                        raise CallbackConflict("progress status is invalid")
                    incoming = payload.get("progress")
                    if isinstance(incoming, bool) or not isinstance(incoming, (int, float)) or not 0 <= incoming <= 1:
                        raise CallbackConflict("callback progress is invalid")
                    if float(incoming) < progress:
                        raise CallbackConflict("callback progress is not monotonic")
                    state_order = {"queued": 0, "running": 1, "needs_review": 2}
                    current_rank = state_order.get(str(row["status"]), -1)
                    if state_order[status] < current_rank:
                        raise CallbackConflict("callback status is not monotonic")
                    progress = float(incoming)
                    stage = str(payload.get("stage") or "").strip()
                    if not stage:
                        raise CallbackConflict("callback stage is empty")
                else:
                    if status == "partial":
                        status = "needs_review"
                    if status not in _TERMINAL:
                        raise CallbackConflict("terminal status is invalid")
                    terminal_event_id = event_id
                    if status == "done":
                        try:
                            verified = parse_status(
                                {**payload, "schema": "autorig.v3.dispatch/1"},
                                V3Submission(binding.task_id, binding.source_sha256, binding.attempt,
                                             binding.idempotency_key),
                                _allow_unverified=True,
                            )
                        except V3ProtocolError as exc:
                            raise CallbackConflict("done payload does not pass V3 validation") from exc
                        if not isinstance(terminal_status, V3Status) or terminal_status != verified or \
                                verified.state is not V3State.DONE or verified.qa_status != "accepted" or \
                                verified.run_id != binding.run_id:
                            raise CallbackConflict("done requires accepted V3 QA")
                        terminal_status = verified
                        artifacts = tuple(verified.artifacts)
                        roles = {artifact.role for artifact in artifacts}
                        if REQUIRED_RIG_ARTIFACTS - roles:
                            raise CallbackConflict("done artifact manifest is incomplete")
                        manifest = payload.get("artifact_manifest")
                        manifest_json = _canonical(manifest) if isinstance(manifest, dict) else None
                        manifest_sha = str(payload.get("artifact_manifest_sha256") or "")
                        if not manifest_json or not _SHA256.fullmatch(manifest_sha) or \
                                hashlib.sha256(manifest_json.encode("utf-8")).hexdigest() != manifest_sha or \
                                terminal_status.manifest_sha256 != manifest_sha:
                            raise CallbackConflict("done artifact manifest is not hash-bound")
                        qa_status = "accepted"
                        progress = 1.0
                        notify = True
                    elif terminal_status is not None:
                        raise CallbackConflict("non-done terminal callback cannot carry a done result")

                connection.execute(
                    "INSERT INTO v3_callback_events(event_id,task_id,attempt,sequence,event_type,payload_sha256,"
                    "payload_json,receipt_id,committed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (event_id, binding.task_id, binding.attempt, sequence, event_type, payload_sha,
                     payload_json, receipt, now),
                )
                if artifacts:
                    for artifact in artifacts:
                        if artifact.source_sha256 != binding.source_sha256:
                            raise CallbackConflict("artifact source hash differs from the active binding")
                        connection.execute(
                            "INSERT INTO v3_task_artifacts(task_id,attempt,role,path,url,sha256,bytes,"
                            "source_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (binding.task_id, binding.attempt, artifact.role, artifact.path, artifact.url,
                             artifact.sha256, artifact.bytes, artifact.source_sha256, now),
                        )
                connection.execute(
                    "UPDATE v3_task_bindings SET status=?,stage=?,progress=?,last_sequence=?,terminal_event_id=?,"
                    "qa_status=?,artifact_manifest_json=?,artifact_manifest_sha256=?,updated_at=? "
                    "WHERE task_id=? AND attempt=? AND active=1",
                    (status, stage, progress, sequence, terminal_event_id, qa_status, manifest_json,
                     manifest_sha, now, binding.task_id, binding.attempt),
                )
                if notify:
                    connection.execute(
                        "INSERT INTO v3_notification_outbox(event_id,task_id,attempt,kind,state,created_at) "
                        "VALUES(?,?,?,'task_done','pending',?)",
                        (event_id, binding.task_id, binding.attempt, now),
                    )
                    notify = self._claim_callback_notification(connection, event_id, callback_owner, now)
                if self.task_projector is not None:
                    projection = self.task_projector(binding, payload, terminal_status, artifacts)
                    if projection is not None:
                        if not isinstance(projection, TaskProjection) or not projection.values:
                            raise CallbackConflict("task projector returned an invalid projection")
                        values = dict(projection.values)
                        if set(values) - TASK_PROJECTION_COLUMNS:
                            raise CallbackConflict("task projector returned disallowed task columns")
                        columns = sorted(values)
                        sql = "UPDATE tasks SET " + ",".join(f'"{name}"=?' for name in columns) + " WHERE id=?"
                        cursor = connection.execute(sql, tuple(values[name] for name in columns) + (binding.task_id,))
                        if cursor.rowcount != 1:
                            raise CallbackConflict("task projector did not update exactly one bound task")
                connection.commit()
                return CallbackCommit(receipt, False, notify)
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise CallbackConflict("callback conflicts with the durable V3 ledger") from exc
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _claim_callback_notification(
        connection: sqlite3.Connection, event_id: str, owner: str, now: str,
    ) -> bool:
        row = connection.execute(
            "SELECT state,lease_until,next_attempt_at FROM v3_notification_outbox WHERE event_id=?", (event_id,),
        ).fetchone()
        if row is None or row["state"] == "sent":
            return False
        eligible = (row["state"] == "pending" and
                    (row["next_attempt_at"] is None or row["next_attempt_at"] <= now)) or \
                   (row["state"] == "leased" and row["lease_until"] is not None and row["lease_until"] <= now)
        if not eligible:
            return False
        lease_until = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(timespec="microseconds")
        connection.execute(
            "UPDATE v3_notification_outbox SET state='leased',lease_owner=?,lease_until=?,"
            "delivery_attempts=delivery_attempts+1 WHERE event_id=?", (owner, lease_until, event_id),
        )
        return True

    async def mark_notified(self, binding: CallbackBinding, event_id: str) -> None:
        _validate_binding(binding)
        claim = self._callback_claim.get()
        if not claim or claim[:3] != (binding.task_id, binding.attempt, event_id):
            raise CallbackConflict("callback notification lease is missing")
        await asyncio.to_thread(self._mark_notified_sync, binding, event_id, claim[3])
        self._callback_claim.set(None)

    def _mark_notified_sync(self, binding: CallbackBinding, event_id: str, lease_owner: str) -> None:
        now = _utcnow()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                event = connection.execute(
                    "SELECT task_id,attempt FROM v3_callback_events WHERE event_id=?", (event_id,)
                ).fetchone()
                if event is None or (event["task_id"], event["attempt"]) != (binding.task_id, binding.attempt):
                    raise CallbackConflict("notification event does not belong to this binding")
                row = connection.execute(
                    "SELECT state,lease_owner FROM v3_notification_outbox WHERE event_id=?", (event_id,)
                ).fetchone()
                if row is None:
                    raise CallbackConflict("notification outbox item is missing")
                if row["state"] == "sent":
                    connection.commit()
                    return
                if row["state"] != "leased" or row["lease_owner"] != lease_owner:
                    raise CallbackConflict("callback notification lease is owned by another delivery")
                connection.execute(
                    "UPDATE v3_notification_outbox SET state='sent',last_error=NULL,lease_owner=NULL,"
                    "lease_until=NULL,next_attempt_at=NULL,sent_at=? WHERE event_id=?", (now, event_id),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def wrap_notifier(self, notifier: Callable[[str], Any]):
        """Release the callback lease immediately when the external send fails."""
        async def wrapped(task_id: str):
            claim = self._callback_claim.get()
            if not claim or claim[0] != task_id:
                raise CallbackConflict("callback notifier has no matching delivery lease")
            try:
                result = notifier(task_id)
                return await result if inspect.isawaitable(result) else result
            except Exception as exc:
                await asyncio.to_thread(self._release_callback_claim_sync, claim, str(exc)[:1000])
                self._callback_claim.set(None)
                raise
        return wrapped

    def _release_callback_claim_sync(self, claim: tuple[str, int, str, str], error: str) -> None:
        task_id, attempt, event_id, owner = claim
        now = _utcnow()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    "UPDATE v3_notification_outbox SET state='pending',last_error=?,lease_owner=NULL,"
                    "lease_until=NULL,next_attempt_at=? WHERE event_id=? AND task_id=? AND attempt=? "
                    "AND state='leased' AND lease_owner=?",
                    (error, now, event_id, task_id, attempt, owner),
                )
                if cursor.rowcount != 1:
                    raise CallbackConflict("callback notification lease could not be released")
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    async def pending_notifications(self, limit: int = 100) -> tuple[OutboxItem, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return await asyncio.to_thread(self._pending_notifications_sync, limit)

    def _pending_notifications_sync(self, limit: int) -> tuple[OutboxItem, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT event_id,task_id,attempt,kind,state,delivery_attempts,lease_owner,lease_until "
                "FROM v3_notification_outbox "
                "WHERE state='pending' ORDER BY created_at,event_id LIMIT ?", (limit,),
            ).fetchall()
            return tuple(OutboxItem(row["event_id"], row["task_id"], row["attempt"], row["kind"],
                                    row["state"], row["delivery_attempts"], row["lease_owner"], row["lease_until"])
                         for row in rows)

    async def claim_notifications(
        self, worker_id: str, *, limit: int = 20, lease_seconds: int = 120,
    ) -> tuple[OutboxItem, ...]:
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", str(worker_id or "")):
            raise ValueError("worker_id is invalid")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or not 10 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 10 and 3600")
        return await asyncio.to_thread(self._claim_notifications_sync, worker_id, limit, lease_seconds)

    def _claim_notifications_sync(self, worker_id: str, limit: int, lease_seconds: int) -> tuple[OutboxItem, ...]:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        lease_until = (now_dt + timedelta(seconds=lease_seconds)).isoformat(timespec="microseconds")
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    "SELECT event_id FROM v3_notification_outbox WHERE state!='sent' AND "
                    "(next_attempt_at IS NULL OR next_attempt_at<=?) AND "
                    "(state='pending' OR (state='leased' AND lease_until<=?)) "
                    "ORDER BY created_at,event_id LIMIT ?", (now, now, limit),
                ).fetchall()
                ids = [row["event_id"] for row in rows]
                for event_id in ids:
                    connection.execute(
                        "UPDATE v3_notification_outbox SET state='leased',lease_owner=?,lease_until=?,"
                        "delivery_attempts=delivery_attempts+1 WHERE event_id=?",
                        (worker_id, lease_until, event_id),
                    )
                claimed = []
                for event_id in ids:
                    row = connection.execute(
                        "SELECT event_id,task_id,attempt,kind,state,delivery_attempts,lease_owner,lease_until "
                        "FROM v3_notification_outbox WHERE event_id=?", (event_id,),
                    ).fetchone()
                    claimed.append(OutboxItem(row["event_id"], row["task_id"], row["attempt"], row["kind"],
                                              row["state"], row["delivery_attempts"], row["lease_owner"],
                                              row["lease_until"]))
                connection.commit()
                return tuple(claimed)
            except Exception:
                connection.rollback()
                raise

    async def mark_claim_sent(self, event_id: str, worker_id: str) -> None:
        await asyncio.to_thread(self._finish_claim_sync, event_id, worker_id, True, "", 0)

    async def record_claim_failure(
        self, event_id: str, worker_id: str, error: str, *, retry_seconds: int = 60,
    ) -> None:
        if isinstance(retry_seconds, bool) or not isinstance(retry_seconds, int) or not 1 <= retry_seconds <= 86400:
            raise ValueError("retry_seconds must be between 1 and 86400")
        await asyncio.to_thread(self._finish_claim_sync, event_id, worker_id, False, str(error)[:1000], retry_seconds)

    def _finish_claim_sync(self, event_id: str, worker_id: str, sent: bool, error: str, retry_seconds: int) -> None:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT state,lease_owner FROM v3_notification_outbox WHERE event_id=?", (event_id,),
                ).fetchone()
                if row is None or row["state"] != "leased" or row["lease_owner"] != worker_id:
                    raise CallbackConflict("notification claim is missing or owned by another worker")
                if sent:
                    connection.execute(
                        "UPDATE v3_notification_outbox SET state='sent',last_error=NULL,lease_owner=NULL,"
                        "lease_until=NULL,next_attempt_at=NULL,sent_at=? WHERE event_id=?", (now, event_id),
                    )
                else:
                    retry = (now_dt + timedelta(seconds=retry_seconds)).isoformat(timespec="microseconds")
                    connection.execute(
                        "UPDATE v3_notification_outbox SET state='pending',last_error=?,lease_owner=NULL,"
                        "lease_until=NULL,next_attempt_at=? WHERE event_id=?", (error, retry, event_id),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise


def integration_contract(store: V3TaskStore, notify_done: Callable[[str], Any]) -> dict[str, Any]:
    """Dependency map for ``create_v3_callback_router``.

    Pass ``notify_done`` here (rather than separately to the router) so a failed
    external send releases its event-scoped callback lease for immediate replay.
    """
    if not callable(notify_done):
        raise TypeError("notify_done callable is required for lease-safe callback delivery")
    return {"binding_store": store.get_binding, "transaction": store,
            "notify_done": store.wrap_notifier(notify_done)}
