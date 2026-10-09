import asyncio
import dataclasses
import hashlib
import hmac
import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest

import httpx
from fastapi import FastAPI


BACKEND = pathlib.Path(__file__).resolve().parents[1]
REPO = pathlib.Path(__file__).resolve().parents[3]
WORK = REPO / ".work" / "v3-task-store-tests"
WORK.mkdir(parents=True, exist_ok=True)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from v3_callback_receiver import (  # noqa: E402
    CALLBACK_SCHEMA,
    CallbackBinding,
    CallbackConflict,
    callback_event_id,
    create_v3_callback_router,
)
from v3_pipeline_adapter import V3Submission, parse_status  # noqa: E402
from v3_task_store import TaskProjection, V3TaskStore, integration_contract  # noqa: E402


TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE = "a" * 64
CREDENTIAL = "v3-callback-primary"
SECRET = b"v3-store-test-secret"


def canonical(doc):
    return json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def binding(attempt=1, source=SOURCE, run_id=None):
    submission = V3Submission.create(TASK, source, attempt)
    return CallbackBinding(TASK, run_id or f"run_{attempt:08d}", source, attempt,
                           submission.idempotency_key, CREDENTIAL)


def event(bound, sequence, event_type="progress", **extra):
    doc = {"schema": CALLBACK_SCHEMA, "event": event_type, "task_id": bound.task_id,
           "run_id": bound.run_id, "source_sha256": bound.source_sha256,
           "attempt": bound.attempt, "idempotency_key": bound.idempotency_key,
           "sequence": sequence}
    if event_type == "progress":
        doc.update(stage="rig", status="running", progress=0.5)
    doc.update(extra)
    return doc


def accepted_done(bound, sequence=9):
    rows = []
    for role, suffix in (("rigged_glb", ".glb"), ("rig_json", ".json"), ("rig_qa", ".json")):
        content = role.encode()
        rows.append({"role": role, "path": f"rig/{role}{suffix}",
                     "url": f"https://autorig.online/api/mt/files/{bound.run_id}/rig/{role}{suffix}",
                     "sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content),
                     "source_sha256": bound.source_sha256})
    manifest = {"schema": "autorig.v3.artifacts/1", "task_id": bound.task_id,
                "source_sha256": bound.source_sha256, "artifacts": rows}
    digest = hashlib.sha256(canonical(manifest)).hexdigest()
    doc = event(bound, sequence, "terminal", status="done", qa={"status": "accepted"},
                artifact_manifest=manifest, artifact_manifest_sha256=digest)
    submission = V3Submission(bound.task_id, bound.source_sha256, bound.attempt, bound.idempotency_key)
    status = parse_status({**doc, "schema": "autorig.v3.dispatch/1"}, submission, _allow_unverified=True)
    return doc, status


class V3TaskStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=WORK)
        self.db = pathlib.Path(self.temp.name) / "tasks.sqlite3"
        self.store = V3TaskStore(self.db)
        await self.store.migrate()

    async def asyncTearDown(self):
        self.temp.cleanup()

    def rows(self, table):
        connection = sqlite3.connect(self.db)
        try:
            return connection.execute(f"SELECT * FROM {table}").fetchall()
        finally:
            connection.close()

    async def test_migration_is_explicit_idempotent_and_contract_is_injectable(self):
        await self.store.migrate()
        tables = {row[1] for row in self.rows("sqlite_master") if row[1].startswith("v3_")}
        self.assertTrue({"v3_task_bindings", "v3_callback_events", "v3_task_artifacts",
                         "v3_notification_outbox"}.issubset(tables))
        contract = integration_contract(self.store, lambda _task_id: None)
        self.assertIs(contract["transaction"], self.store)
        self.assertEqual((await contract["binding_store"](TASK)), None)

    async def test_future_schema_is_rejected_before_any_ddl(self):
        future = pathlib.Path(self.temp.name) / "future.sqlite3"
        connection = sqlite3.connect(future)
        try:
            connection.execute("CREATE TABLE v3_store_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            connection.execute("INSERT INTO v3_store_meta VALUES('schema_version','999')")
            connection.execute("CREATE TABLE sentinel(value TEXT)")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(RuntimeError):
            await V3TaskStore(future).migrate()
        connection = sqlite3.connect(future)
        try:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        finally:
            connection.close()
        self.assertEqual(tables, {"v3_store_meta", "sentinel"})

    async def test_v1_outbox_is_atomically_rebuilt_to_v2_with_pending_row(self):
        legacy = pathlib.Path(self.temp.name) / "legacy.sqlite3"
        connection = sqlite3.connect(legacy)
        try:
            connection.executescript("""
                CREATE TABLE v3_store_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                INSERT INTO v3_store_meta VALUES('schema_version','1');
                CREATE TABLE v3_callback_events(event_id TEXT PRIMARY KEY);
                INSERT INTO v3_callback_events VALUES('v3cb-legacy');
                CREATE TABLE v3_notification_outbox(
                    event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                    kind TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending'
                        CHECK(state IN ('pending','sent')),
                    delivery_attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                    created_at TEXT NOT NULL, sent_at TEXT,
                    FOREIGN KEY(event_id) REFERENCES v3_callback_events(event_id));
                CREATE INDEX ix_v3_notification_pending ON v3_notification_outbox(state,created_at);
                INSERT INTO v3_notification_outbox VALUES(
                    'v3cb-legacy','dfc5ccce-3e0c-4729-99dc-286eb23dac88',1,
                    'task_done','pending',2,'old error','2026-10-09T00:00:00+00:00',NULL);
            """)
            connection.commit()
        finally:
            connection.close()
        legacy_store = V3TaskStore(legacy)
        await legacy_store.migrate()
        connection = sqlite3.connect(legacy)
        try:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(v3_notification_outbox)")}
            version = connection.execute(
                "SELECT value FROM v3_store_meta WHERE key='schema_version'").fetchone()[0]
            row = connection.execute(
                "SELECT state,delivery_attempts,last_error FROM v3_notification_outbox").fetchone()
        finally:
            connection.close()
        self.assertTrue({"lease_owner", "lease_until", "next_attempt_at"}.issubset(columns))
        self.assertEqual((version, row), ("2", ("pending", 2, "old error")))
        claimed = await legacy_store.claim_notifications("migration-worker")
        self.assertEqual((len(claimed), claimed[0].state), (1, "leased"))

    async def test_dispatch_binding_is_idempotent_and_new_attempt_rejects_stale_callback(self):
        first = binding(1)
        one = await self.store.bind_dispatch(first)
        again = await self.store.bind_dispatch(first)
        self.assertEqual(one, again)
        second = binding(2, source="b" * 64)
        await self.store.bind_dispatch(second)
        self.assertEqual(await self.store.get_binding(TASK), second)
        with self.assertRaises(CallbackConflict):
            await self.store.accept(first, callback_event_id(event(first, 1)), event(first, 1), None)

    async def test_binding_rejects_well_formed_but_unbound_idempotency_key(self):
        forged = dataclasses.replace(binding(), idempotency_key="v3-" + "f" * 64)
        with self.assertRaises(CallbackConflict):
            await self.store.bind_dispatch(forged)

    async def test_event_dedup_sequence_and_progress_are_monotonic(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        first_doc = event(bound, 1, progress=0.4)
        event_id = callback_event_id(first_doc)
        first = await self.store.accept(bound, event_id, first_doc, None)
        replay = await self.store.accept(bound, event_id, first_doc, None)
        self.assertFalse(first.duplicate)
        self.assertTrue(replay.duplicate)
        self.assertEqual(first.receipt_id, replay.receipt_id)
        with self.assertRaises(CallbackConflict):
            lower = event(bound, 2, progress=0.3)
            await self.store.accept(bound, callback_event_id(lower), lower, None)
        with self.assertRaises(CallbackConflict):
            same_sequence = event(bound, 1, progress=0.8)
            await self.store.accept(bound, callback_event_id(same_sequence), same_sequence, None)
        snapshot = await self.store.snapshot(TASK)
        self.assertEqual((snapshot.last_sequence, snapshot.progress), (1, 0.4))

    async def test_progress_status_cannot_regress(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        running = event(bound, 1, status="running", progress=0.4)
        await self.store.accept(bound, callback_event_id(running), running, None)
        queued = event(bound, 2, status="queued", progress=0.5)
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(queued), queued, None)
        review = event(bound, 3, status="needs_review", progress=0.6)
        await self.store.accept(bound, callback_event_id(review), review, None)
        back_to_running = event(bound, 4, status="running", progress=0.7)
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(back_to_running), back_to_running, None)

    async def test_done_requires_accepted_qa_and_hash_bound_complete_manifest(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        bad = event(bound, 1, "terminal", status="done", qa={"status": "rejected"})
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(bad), bad, None)
        self.assertFalse(self.rows("v3_callback_events"))
        self.assertEqual((await self.store.snapshot(TASK)).status, "queued")

    async def test_store_revalidates_typed_done_status_and_rolls_back_mismatch(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        doc, status = accepted_done(bound)
        forged = dataclasses.replace(status, run_id="run_forged00")
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(doc), doc, forged)
        self.assertFalse(self.rows("v3_callback_events"))
        self.assertFalse(self.rows("v3_task_artifacts"))
        self.assertEqual((await self.store.snapshot(TASK)).status, "queued")

    async def test_done_commits_status_artifacts_and_pending_outbox_atomically(self):
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("CREATE TABLE tasks(id TEXT PRIMARY KEY,status TEXT,error_message TEXT)")
            connection.execute("INSERT INTO tasks VALUES(?, 'processing', NULL)", (TASK,))
            connection.commit()
        finally:
            connection.close()

        def project(bound, payload, terminal, artifacts):
            return TaskProjection({"status": terminal.state.value, "error_message": terminal.manifest_sha256})

        self.store = V3TaskStore(self.db, task_projector=project)
        bound = binding()
        await self.store.bind_dispatch(bound)
        doc, status = accepted_done(bound)
        commit = await self.store.accept(bound, callback_event_id(doc), doc, status)
        snapshot = await self.store.snapshot(TASK)
        self.assertEqual((snapshot.status, snapshot.progress, snapshot.artifact_manifest_sha256),
                         ("done", 1.0, status.manifest_sha256))
        self.assertEqual(len(self.rows("v3_task_artifacts")), 3)
        self.assertEqual(self.rows("v3_notification_outbox")[0][4], "leased")
        async def fail_notify(_task_id):
            raise RuntimeError("network")
        with self.assertRaises(RuntimeError):
            await self.store.wrap_notifier(fail_notify)(TASK)
        self.assertEqual(len(await self.store.pending_notifications()), 1)
        connection = sqlite3.connect(self.db)
        try:
            self.assertEqual(connection.execute("SELECT status,error_message FROM tasks").fetchone(),
                             ("done", status.manifest_sha256))
        finally:
            connection.close()
        replay = await self.store.accept(bound, callback_event_id(doc), doc, status)
        self.assertTrue(replay.duplicate and replay.notification_required)
        await self.store.mark_notified(bound, callback_event_id(doc))
        replay_after_notify = await self.store.accept(bound, callback_event_id(doc), doc, status)
        self.assertFalse(replay_after_notify.notification_required)
        self.assertFalse(await self.store.pending_notifications())
        await self.store.bind_dispatch(binding(2, source="b" * 64))
        self.assertEqual((await self.store.get_binding(TASK)).attempt, 2)

    async def test_pending_done_notification_survives_corrective_new_attempt(self):
        first = binding()
        await self.store.bind_dispatch(first)
        doc, status = accepted_done(first)
        await self.store.accept(first, callback_event_id(doc), doc, status)
        async def fail_notify(_task_id):
            raise RuntimeError("network")
        with self.assertRaises(RuntimeError):
            await self.store.wrap_notifier(fail_notify)(TASK)
        await self.store.bind_dispatch(binding(2, source="b" * 64))
        claimed = await self.store.claim_notifications("worker-a")
        self.assertEqual(len(claimed), 1)
        self.assertEqual((claimed[0].event_id, claimed[0].task_id, claimed[0].attempt),
                         (callback_event_id(doc), TASK, 1))
        await self.store.mark_claim_sent(claimed[0].event_id, "worker-a")
        self.assertFalse(await self.store.pending_notifications())

    async def test_projector_failure_rolls_back_event_artifacts_status_and_outbox(self):
        def fail(*_args):
            return TaskProjection({"not_a_task_column": "bad"})

        self.store = V3TaskStore(self.db, task_projector=fail)
        bound = binding()
        await self.store.bind_dispatch(bound)
        doc, status = accepted_done(bound)
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(doc), doc, status)
        self.assertFalse(self.rows("v3_callback_events"))
        self.assertFalse(self.rows("v3_task_artifacts"))
        self.assertFalse(self.rows("v3_notification_outbox"))
        snapshot = await self.store.snapshot(TASK)
        self.assertEqual((snapshot.status, snapshot.last_sequence), ("queued", -1))

    async def test_projector_requires_exactly_one_existing_bound_task(self):
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("CREATE TABLE tasks(id TEXT PRIMARY KEY,status TEXT)")
            connection.execute("INSERT INTO tasks VALUES('11111111-1111-1111-1111-111111111111','created')")
            connection.commit()
        finally:
            connection.close()
        self.store = V3TaskStore(self.db, task_projector=lambda *_: TaskProjection({"status": "done"}))
        bound = binding()
        await self.store.bind_dispatch(bound)
        doc, status = accepted_done(bound)
        with self.assertRaises(CallbackConflict):
            await self.store.accept(bound, callback_event_id(doc), doc, status)
        self.assertFalse(self.rows("v3_callback_events"))
        connection = sqlite3.connect(self.db)
        try:
            state = connection.execute("SELECT status FROM tasks").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(state, "created")

    async def test_terminal_needs_review_has_no_done_outbox_and_stops_later_events(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        terminal = event(bound, 4, "terminal", status="needs_review", qa={"status": "needs_review"})
        commit = await self.store.accept(bound, callback_event_id(terminal), terminal, None)
        self.assertFalse(commit.notification_required)
        self.assertEqual((await self.store.snapshot(TASK)).status, "needs_review")
        self.assertFalse(self.rows("v3_notification_outbox"))
        with self.assertRaises(CallbackConflict):
            later = event(bound, 5, progress=0.9)
            await self.store.accept(bound, callback_event_id(later), later, None)

    async def test_receiver_retry_reuses_receipt_and_retries_pending_notification(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        calls = []

        async def notify(task_id):
            calls.append(task_id)
            if len(calls) == 1:
                raise RuntimeError("temporary")

        app = FastAPI()
        app.include_router(create_v3_callback_router(
            secret_provider=lambda _: SECRET,
            **integration_contract(self.store, notify),
        ))
        doc, _ = accepted_done(bound)
        raw = canonical(doc)
        headers = {"X-AutoRig-V3-Credential": CREDENTIAL,
                   "X-AutoRig-V3-Signature": "v1=" + hmac.new(SECRET, raw, hashlib.sha256).hexdigest(),
                   "Idempotency-Key": callback_event_id(doc)}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as client:
            first = await client.post("/internal/v3/task-callbacks", content=raw, headers=headers)
            second = await client.post("/internal/v3/task-callbacks", content=raw, headers=headers)
        self.assertEqual((first.status_code, second.status_code), (503, 200))
        self.assertTrue(second.json()["duplicate"])
        self.assertEqual(calls, [TASK, TASK])
        self.assertFalse(await self.store.pending_notifications())

    async def test_concurrent_callback_replay_cannot_double_send_notification(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def notify(task_id):
            calls.append(task_id)
            entered.set()
            await release.wait()

        app = FastAPI()
        app.include_router(create_v3_callback_router(
            secret_provider=lambda _: SECRET,
            **integration_contract(self.store, notify),
        ))
        doc, _ = accepted_done(bound)
        raw = canonical(doc)
        headers = {"X-AutoRig-V3-Credential": CREDENTIAL,
                   "X-AutoRig-V3-Signature": "v1=" + hmac.new(SECRET, raw, hashlib.sha256).hexdigest(),
                   "Idempotency-Key": callback_event_id(doc)}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://test") as client:
            first_task = asyncio.create_task(
                client.post("/internal/v3/task-callbacks", content=raw, headers=headers))
            await asyncio.wait_for(entered.wait(), 2)
            replay = await client.post("/internal/v3/task-callbacks", content=raw, headers=headers)
            release.set()
            first = await first_task
        self.assertEqual((first.status_code, replay.status_code), (200, 200))
        self.assertTrue(replay.json()["duplicate"])
        self.assertEqual(calls, [TASK])

    async def test_outbox_claim_is_exclusive_and_failure_releases_with_backoff(self):
        bound = binding()
        await self.store.bind_dispatch(bound)
        doc, status = accepted_done(bound)
        await self.store.accept(bound, callback_event_id(doc), doc, status)
        async def fail_notify(_task_id):
            raise RuntimeError("network")
        with self.assertRaises(RuntimeError):
            await self.store.wrap_notifier(fail_notify)(TASK)
        first, second = await asyncio.gather(
            self.store.claim_notifications("worker-a"),
            self.store.claim_notifications("worker-b"),
        )
        claims = [rows for rows in (first, second) if rows]
        self.assertEqual(len(claims), 1)
        item = claims[0][0]
        self.assertEqual(item.delivery_attempts, 2)
        wrong = "worker-b" if item.lease_owner == "worker-a" else "worker-a"
        with self.assertRaises(CallbackConflict):
            await self.store.mark_claim_sent(item.event_id, wrong)
        await self.store.record_claim_failure(item.event_id, item.lease_owner, "network", retry_seconds=60)
        self.assertFalse(await self.store.claim_notifications("worker-c"))
        connection = sqlite3.connect(self.db)
        try:
            row = connection.execute(
                "SELECT state,last_error,delivery_attempts FROM v3_notification_outbox WHERE event_id=?",
                (item.event_id,),
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row, ("pending", "network", 2))


if __name__ == "__main__":
    unittest.main()
