"""Durable V3 dispatch-v2 outbox for AutoRig task creation.

This module is deliberately not mounted by importing it.  A creation route must
first commit its normal ``Task`` row, then enqueue the exact source manifest
here.  A separately mounted runner may call :meth:`V3DispatchOutbox.process_one`
until the remote V3 run reaches a terminal state.

``v3_task_store`` remains the proven callback ledger for dispatch protocol v1.
It cannot represent v2's registered ``source_ref``, ``intent`` or
``pipeline_revision`` and therefore is not reused as a v2 queue.  This outbox
does reuse the strict dispatch-v2 identity and response validation in
``v3_pipeline_adapter``.  There is intentionally no legacy-pipeline fallback.
"""
from __future__ import annotations

import json
import hashlib
import inspect
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol

import httpx

from v3_pipeline_adapter import (
    V2_REQUIRED_ARTIFACTS,
    V3DispatchClient,
    V3DispatchStatus,
    V3DispatchSubmission,
    V3ProtocolError,
    V3State,
)


SCHEMA_VERSION = 1
ACTIVE_STATES = frozenset({"pending_register", "pending_submit", "queued", "running",
                           "awaiting_artifact_verification"})
TERMINAL_STATES = frozenset({"needs_review", "done", "failed", "blocked_protocol"})
ALL_STATES = ACTIVE_STATES | TERMINAL_STATES


class OutboxConflict(ValueError):
    """The durable identity is already bound to different input."""


class LeaseLost(RuntimeError):
    """Another runner reclaimed this row; the stale runner must not overwrite it."""


class DispatchRemote(Protocol):
    async def register_source(self, manifest: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def submit(self, submission: V3DispatchSubmission) -> V3DispatchStatus: ...
    async def status(self, submission: V3DispatchSubmission, run_id: str) -> V3DispatchStatus: ...


class ArtifactVerifier(Protocol):
    async def __call__(self, status: V3DispatchStatus) -> Mapping[str, Any]: ...


async def verify_dispatch_artifacts(status: V3DispatchStatus, fetch_bytes: Any) -> Mapping[str, Any]:
    """Independently fetch and hash every required v2 artifact.

    ``fetch_bytes`` receives one manifest row and must return its exact bytes.
    This function intentionally does not trust the remote ``done`` parser alone.
    """
    if status.state is not V3State.DONE or status.qa.get("status") != "accepted":
        raise V3ProtocolError("artifact verification requires accepted remote QA")
    rows = status.artifact_manifest.get("artifacts") \
        if isinstance(status.artifact_manifest, Mapping) else None
    if not isinstance(rows, list):
        raise V3ProtocolError("artifact manifest is missing")
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise V3ProtocolError("artifact row is invalid")
        role = str(row.get("role") or "")
        if not role or role in seen:
            raise V3ProtocolError("artifact roles must be non-empty and unique")
        seen.add(role)
        if row.get("source_sha256") != status.submission.source_sha256:
            raise V3ProtocolError(f"artifact {role} source binding mismatch")
        size = row.get("bytes")
        digest = str(row.get("sha256") or "")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0 or \
                len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise V3ProtocolError(f"artifact {role} metadata is invalid")
        data = fetch_bytes(row)
        data = await data if inspect.isawaitable(data) else data
        if not isinstance(data, bytes) or len(data) != size or hashlib.sha256(data).hexdigest() != digest:
            raise V3ProtocolError(f"artifact {role} byte verification failed")
    missing = V2_REQUIRED_ARTIFACTS[status.submission.intent] - seen
    if missing:
        raise V3ProtocolError("required artifacts missing: " + ", ".join(sorted(missing)))
    manifest_bytes = json.dumps(dict(status.artifact_manifest), sort_keys=True,
                                separators=(",", ":")).encode("utf-8")
    return {
        "schema": "autorig.v3.artifact-verification/1",
        "task_id": status.submission.task_id,
        "source_sha256": status.submission.source_sha256,
        "run_id": status.run_id,
        "idempotency_key": status.submission.idempotency_key,
        "artifact_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "status": "accepted",
    }


@dataclass(frozen=True)
class DispatchIdentity:
    task_id: str
    source_sha256: str
    intent: str
    pipeline_revision: str
    attempt: int

    @classmethod
    def create(cls, task_id: str, source_sha256: str, intent: str,
               pipeline_revision: str, attempt: int = 1) -> "DispatchIdentity":
        try:
            task = str(uuid.UUID(str(task_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise OutboxConflict("task_id must be a canonical UUID") from exc
        if task != task_id:
            raise OutboxConflict("task_id must be a canonical UUID")
        source = str(source_sha256 or "").lower()
        if len(source) != 64 or any(ch not in "0123456789abcdef" for ch in source):
            raise OutboxConflict("source_sha256 must be lowercase 64-hex")
        if intent not in V2_REQUIRED_ARTIFACTS:
            raise OutboxConflict("intent is not supported by dispatch v2")
        revision = str(pipeline_revision or "").strip()
        if not revision or len(revision) > 128:
            raise OutboxConflict("pipeline_revision is required")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise OutboxConflict("attempt must be a positive integer")
        return cls(task, source, intent, revision, attempt)

    @property
    def key(self) -> str:
        # This is a local pre-registration identity.  The remote idempotency key
        # is created only after the server returns the source_ref and binds it too.
        return "\0".join((self.task_id, self.source_sha256, self.intent,
                           self.pipeline_revision, str(self.attempt)))


@dataclass(frozen=True)
class OutboxRecord:
    identity: DispatchIdentity
    state: str
    source_ref: str | None
    idempotency_key: str | None
    run_id: str | None
    remote_stage: str
    progress: float
    error: str
    next_run_at: float
    # Last validated remote document: qa, artifact_manifest, viewer session and,
    # for done, the independent artifact verification receipt.
    remote_status: Mapping[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0


class V3DispatchOutbox:
    """SQLite outbox with leases so process/service restarts resume safely."""

    def __init__(self, db_path: str | Path, *, clock=time.time) -> None:
        self.path = Path(db_path)
        self.clock = clock

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=10000")
        return db

    def migrate(self) -> None:
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS v3_dispatch_meta (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS v3_dispatch_outbox (
                    identity_key TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    source_sha256 TEXT NOT NULL,
                    intent TEXT NOT NULL,
                    pipeline_revision TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    source_manifest_json TEXT NOT NULL,
                    source_ref TEXT,
                    idempotency_key TEXT,
                    run_id TEXT,
                    state TEXT NOT NULL,
                    remote_stage TEXT NOT NULL DEFAULT '',
                    progress REAL NOT NULL DEFAULT 0,
                    remote_status_json TEXT,
                    error TEXT NOT NULL DEFAULT '',
                    next_run_at REAL NOT NULL,
                    lease_until REAL,
                    lease_token TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(task_id, source_sha256, intent, pipeline_revision, attempt)
                );
                CREATE INDEX IF NOT EXISTS ix_v3_dispatch_due
                    ON v3_dispatch_outbox(state, next_run_at, lease_until);
            """)
            current = db.execute(
                "SELECT value FROM v3_dispatch_meta WHERE key='schema_version'"
            ).fetchone()
            if current and int(current["value"]) > SCHEMA_VERSION:
                raise RuntimeError("V3 dispatch outbox schema is newer than this code")
            columns = {row[1] for row in db.execute("PRAGMA table_info(v3_dispatch_outbox)")}
            if "lease_token" not in columns:
                db.execute("ALTER TABLE v3_dispatch_outbox ADD COLUMN lease_token TEXT")
            db.execute(
                "INSERT OR REPLACE INTO v3_dispatch_meta(key,value) VALUES('schema_version',?)",
                (str(SCHEMA_VERSION),),
            )

    def enqueue(self, identity: DispatchIdentity,
                source_manifest: Mapping[str, Any]) -> OutboxRecord:
        """Persist one exact attempt; duplicates are harmless, conflicts fail."""
        checked = DispatchIdentity.create(identity.task_id, identity.source_sha256,
                                          identity.intent, identity.pipeline_revision,
                                          identity.attempt)
        if checked != identity:
            raise OutboxConflict("dispatch identity is not canonical")
        manifest = dict(source_manifest)
        if str(manifest.get("sha256") or "").lower() != identity.source_sha256:
            raise OutboxConflict("source manifest hash does not match dispatch identity")
        if manifest.get("intent") != identity.intent:
            raise OutboxConflict("source manifest intent does not match dispatch identity")
        encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"))
        now = float(self.clock())
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM v3_dispatch_outbox WHERE identity_key=?", (identity.key,)
            ).fetchone()
            if row:
                if row["source_manifest_json"] != encoded:
                    raise OutboxConflict("dispatch identity is bound to another source manifest")
                db.commit()
                return self._record(row)
            db.execute("""
                INSERT INTO v3_dispatch_outbox(
                    identity_key,task_id,source_sha256,intent,pipeline_revision,attempt,
                    source_manifest_json,state,next_run_at,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,'pending_register',?,?,?)
            """, (identity.key, identity.task_id, identity.source_sha256, identity.intent,
                  identity.pipeline_revision, identity.attempt, encoded, now, now, now))
            row = db.execute(
                "SELECT * FROM v3_dispatch_outbox WHERE identity_key=?", (identity.key,)
            ).fetchone()
            db.commit()
        return self._record(row)

    def get(self, identity: DispatchIdentity) -> OutboxRecord:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM v3_dispatch_outbox WHERE identity_key=?", (identity.key,)
            ).fetchone()
        if row is None:
            raise KeyError(identity.key)
        return self._record(row)

    def resumable(self) -> list[OutboxRecord]:
        """Return all non-terminal work, including leases left by a dead process."""
        marks = ",".join("?" for _ in ACTIVE_STATES)
        with self._connect() as db:
            rows = db.execute(
                f"SELECT * FROM v3_dispatch_outbox WHERE state IN ({marks}) "
                "ORDER BY created_at, identity_key", tuple(sorted(ACTIVE_STATES))
            ).fetchall()
        return [self._record(row) for row in rows]

    def _claim(self, lease_seconds: float) -> sqlite3.Row | None:
        now = float(self.clock())
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            marks = ",".join("?" for _ in ACTIVE_STATES)
            row = db.execute(
                f"SELECT * FROM v3_dispatch_outbox WHERE state IN ({marks}) "
                "AND next_run_at<=? AND (lease_until IS NULL OR lease_until<?) "
                "ORDER BY next_run_at, created_at LIMIT 1",
                (*sorted(ACTIVE_STATES), now, now),
            ).fetchone()
            if row is None:
                db.commit()
                return None
            lease_token = uuid.uuid4().hex
            db.execute(
                "UPDATE v3_dispatch_outbox SET lease_until=?,lease_token=?,updated_at=? "
                "WHERE identity_key=? AND (lease_until IS NULL OR lease_until<?)",
                (now + lease_seconds, lease_token, now, row["identity_key"], now),
            )
            claimed = db.execute(
                "SELECT * FROM v3_dispatch_outbox WHERE identity_key=?", (row["identity_key"],)
            ).fetchone()
            db.commit()
            return claimed

    def _update(self, key: str, lease_token: str, *, state: str, source_ref: str | None = None,
                idempotency_key: str | None = None, run_id: str | None = None,
                remote_stage: str | None = None, progress: float | None = None, error: str = "",
                remote_status: Mapping[str, Any] | None = None,
                delay: float = 0) -> None:
        if state not in ALL_STATES:
            raise ValueError("invalid outbox state")
        now = float(self.clock())
        encoded = json.dumps(dict(remote_status), sort_keys=True, separators=(",", ":")) \
            if remote_status is not None else None
        with self._connect() as db:
            changed = db.execute("""
                UPDATE v3_dispatch_outbox SET state=?,source_ref=COALESCE(?,source_ref),
                    idempotency_key=COALESCE(?,idempotency_key),run_id=COALESCE(?,run_id),
                    remote_stage=COALESCE(?,remote_stage),progress=COALESCE(?,progress),
                    remote_status_json=COALESCE(?,remote_status_json),
                    error=?,next_run_at=?,lease_until=NULL,lease_token=NULL,updated_at=?
                WHERE identity_key=? AND lease_token=?
            """, (state, source_ref, idempotency_key, run_id, remote_stage, progress,
                  encoded, error[:1000], now + max(0, delay), now, key, lease_token))
            if changed.rowcount != 1:
                raise LeaseLost("V3 dispatch lease was reclaimed")

    @staticmethod
    def _verify_source_file(manifest: Mapping[str, Any], expected_sha256: str) -> None:
        """Re-read exact source bytes immediately before remote registration."""
        path = Path(str(manifest.get("path") or ""))
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise V3ProtocolError("source manifest path is not an absolute regular file")
        before = path.stat()
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                total += len(chunk)
        after = path.stat()
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after:
            raise V3ProtocolError("source changed while being verified")
        if digest.hexdigest() != expected_sha256 or total != manifest.get("bytes"):
            raise V3ProtocolError("source bytes do not match the durable manifest")

    @staticmethod
    def _validate_verification_receipt(status: V3DispatchStatus,
                                       receipt: Mapping[str, Any]) -> None:
        manifest_bytes = json.dumps(dict(status.artifact_manifest), sort_keys=True,
                                    separators=(",", ":")).encode("utf-8")
        expected = {
            "schema": "autorig.v3.artifact-verification/1",
            "task_id": status.submission.task_id,
            "source_sha256": status.submission.source_sha256,
            "run_id": status.run_id,
            "idempotency_key": status.submission.idempotency_key,
            "artifact_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "status": "accepted",
        }
        if not isinstance(receipt, Mapping) or any(receipt.get(k) != v for k, v in expected.items()):
            raise V3ProtocolError("independent artifact verification receipt mismatch")

    async def process_one(self, remote: DispatchRemote, *, artifact_verifier: ArtifactVerifier | None = None,
                          poll_seconds: float = 5,
                          retry_seconds: float = 15, lease_seconds: float = 120) -> DispatchIdentity | None:
        """Advance one due row exactly one remote step; return its identity (``None`` = idle).

        Transport failures are retried from durable state.  Contract violations
        become ``blocked_protocol`` and never fall through to the legacy path.
        """
        row = self._claim(lease_seconds)
        if row is None:
            return None
        identity = DispatchIdentity.create(
            row["task_id"], row["source_sha256"], row["intent"],
            row["pipeline_revision"], row["attempt"],
        )
        lease_token = str(row["lease_token"] or "")
        try:
            source_ref = row["source_ref"]
            if not source_ref:
                source_manifest = json.loads(row["source_manifest_json"])
                self._verify_source_file(source_manifest, identity.source_sha256)
                registered = await remote.register_source(source_manifest)
                if str(registered.get("source_sha256") or "").lower() != identity.source_sha256:
                    raise V3ProtocolError("registered source hash mismatch")
                if registered.get("intent") not in {None, identity.intent}:
                    raise V3ProtocolError("registered source intent mismatch")
                source_ref = str(registered.get("source_ref") or "")
                submission = V3DispatchSubmission.create(
                    identity.task_id, source_ref, identity.source_sha256, identity.attempt,
                    identity.intent, identity.pipeline_revision,
                )
                self._update(identity.key, lease_token, state="pending_submit", source_ref=source_ref,
                             idempotency_key=submission.idempotency_key)
                return identity

            submission = V3DispatchSubmission.create(
                identity.task_id, source_ref, identity.source_sha256, identity.attempt,
                identity.intent, identity.pipeline_revision,
            )
            if row["idempotency_key"] and row["idempotency_key"] != submission.idempotency_key:
                raise V3ProtocolError("durable idempotency key mismatch")
            try:
                status = await (remote.status(submission, row["run_id"]) if row["run_id"]
                                else remote.submit(submission))
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                if code == 404 and row["run_id"]:
                    raise V3ProtocolError("the V3 worker no longer knows this run") from exc
                if code in (409, 422):
                    raise V3ProtocolError(f"the V3 worker rejected the submission: HTTP {code}") from exc
                raise
            mapped = status.state.value
            remote_doc = {"qa": status.qa, "artifact_manifest": status.artifact_manifest,
                          "session": dict(status.session)}
            if status.state is V3State.DONE:
                if artifact_verifier is None:
                    self._update(identity.key, lease_token,
                                 state="awaiting_artifact_verification", source_ref=source_ref,
                                 idempotency_key=submission.idempotency_key, run_id=status.run_id,
                                 remote_stage=status.stage, progress=status.progress,
                                 error="independent artifact verification required",
                                 remote_status=remote_doc, delay=poll_seconds)
                    return identity
                receipt = artifact_verifier(status)
                receipt = await receipt if inspect.isawaitable(receipt) else receipt
                self._validate_verification_receipt(status, receipt)
                remote_doc["artifact_verification"] = dict(receipt)
            delay = poll_seconds if mapped in {"queued", "running"} else 0
            self._update(
                identity.key, lease_token, state=mapped, source_ref=source_ref,
                idempotency_key=submission.idempotency_key, run_id=status.run_id,
                remote_stage=status.stage, progress=status.progress, error=status.error,
                remote_status=remote_doc, delay=delay,
            )
            return identity
        except V3ProtocolError as exc:
            self._update(identity.key, lease_token, state="blocked_protocol", error=str(exc))
            return identity
        except LeaseLost:
            raise
        except Exception as exc:
            # Unknown network/service failures are retryable and preserve the
            # exact stage; no attempt number or deadline is silently refreshed.
            self._update(identity.key, lease_token, state=row["state"],
                         error=f"{type(exc).__name__}: {exc}",
                         delay=retry_seconds)
            return identity

    @staticmethod
    def _record(row: sqlite3.Row) -> OutboxRecord:
        identity = DispatchIdentity.create(
            row["task_id"], row["source_sha256"], row["intent"],
            row["pipeline_revision"], row["attempt"],
        )
        try:
            remote = json.loads(row["remote_status_json"] or "{}")
        except (TypeError, ValueError):
            remote = {}
        return OutboxRecord(identity, row["state"], row["source_ref"],
                            row["idempotency_key"], row["run_id"],
                            row["remote_stage"], float(row["progress"]), row["error"],
                            float(row["next_run_at"]), remote if isinstance(remote, dict) else {},
                            float(row["updated_at"]))


def http_remote(base_url: str, token: str, client: Any) -> V3DispatchClient:
    """Explicit composition helper; importing this module performs no I/O."""
    return V3DispatchClient(base_url, token, client)
