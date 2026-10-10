import hashlib
import json
import pathlib
import sqlite3
import sys
import tempfile
import unittest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from v3_task_runtime import (SameTaskDbBindings, RuntimeContractError,
                             normalize_task_binding, task_status_patch)
from v3_dispatch_outbox import OutboxRecord


TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
DATA = b"source"
SHA = hashlib.sha256(DATA).hexdigest()


class Result:
    def __init__(self, cursor): self.cursor = cursor; self.rowcount = cursor.rowcount
    def mappings(self): return self
    def all(self): return [dict(row) for row in self.cursor.fetchall()]


class Session:
    def __init__(self, db): self.db = db
    async def execute(self, sql, params=None):
        return Result(self.db.execute(str(sql), params or {}))
    async def commit(self): self.db.commit()


def source_manifest(path):
    return {"schema": "autorig.v3.source/1", "intent": "rig", "path": str(path),
            "sha256": SHA, "bytes": len(DATA)}


def evidence(schema, digest_field, **extra):
    row = {"schema": schema, "status": "accepted", "task_id": TASK,
           "source_sha256": SHA, **extra}
    row[digest_field] = hashlib.sha256(
        json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return row


class V3TaskRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def make(self):
        work = BACKEND / ".work" / "v3-task-runtime-tests"
        work.mkdir(parents=True, exist_ok=True)
        temp = tempfile.TemporaryDirectory(dir=work)
        path = pathlib.Path(temp.name) / "source.glb"; path.write_bytes(DATA)
        db = sqlite3.connect(pathlib.Path(temp.name) / "tasks.sqlite3")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("CREATE TABLE tasks(id VARCHAR(36) PRIMARY KEY,status TEXT)")
        return temp, path, db, Session(db), SameTaskDbBindings()

    async def test_task_and_binding_commit_or_rollback_together(self):
        temp, path, db, session, store = self.make()
        with temp:
            await store.install(session); db.commit()
            db.execute("BEGIN")
            db.execute("INSERT INTO tasks(id,status) VALUES(?,?)", (TASK, "created"))
            binding = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                             source_manifest=source_manifest(path),
                                             requested_intent="rig")
            await store.bind_in_task_transaction(session, binding)
            db.rollback()
            self.assertEqual(db.execute("SELECT count(*) FROM tasks").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM v3_dispatch_task_bindings").fetchone()[0], 0)
            db.execute("BEGIN")
            db.execute("INSERT INTO tasks(id,status) VALUES(?,?)", (TASK, "created"))
            await store.bind_in_task_transaction(session, binding)
            db.commit()
            self.assertEqual(db.execute("SELECT count(*) FROM v3_dispatch_task_bindings").fetchone()[0], 1)
            db.close()

    async def test_recovery_returns_latest_nonterminal_exact_binding(self):
        temp, path, db, session, store = self.make()
        with temp:
            await store.install(session)
            db.execute("INSERT INTO tasks(id,status) VALUES(?,?)", (TASK, "processing"))
            first = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                           source_manifest=source_manifest(path), requested_intent="rig")
            await store.bind_in_task_transaction(session, first)
            db.execute("UPDATE v3_dispatch_task_bindings SET state='needs_review' WHERE task_id=?", (TASK,))
            current = (await store.recover_pending(session))[0]
            retry = await store.create_retry_in_transaction(session, current)
            db.commit()
            recovered = await store.recover_pending(session)
            self.assertEqual([(x.attempt, x.state) for x in recovered], [(2, "pending")])
            self.assertEqual(retry.identity.attempt, 2)
            db.close()

    def test_normalizer_records_requested_intent_but_dispatches_only_v3_rig(self):
        work = BACKEND / ".work" / "v3-task-runtime-tests"; work.mkdir(parents=True, exist_ok=True)
        path = work / "source-fixture.glb"; path.write_bytes(DATA)
        binding = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                         source_manifest=source_manifest(path), requested_intent="rig")
        self.assertEqual((binding.requested_intent, binding.dispatch_intent), ("rig", "rig"))
        with self.assertRaises(RuntimeContractError):
            normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                   source_manifest=source_manifest(path), requested_intent="generate")
        receipt = evidence("autorig.v3.generation-receipt/1", "receipt_sha256")
        generated_manifest = source_manifest(path)
        generated_manifest["upstream_receipt_sha256"] = receipt["receipt_sha256"]
        generated = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                           source_manifest=generated_manifest,
                                           requested_intent="generate", upstream_receipt=receipt)
        self.assertEqual((generated.requested_intent, generated.dispatch_intent), ("generate", "rig"))
        with self.assertRaises(RuntimeContractError):
            normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                   source_manifest=source_manifest(path), requested_intent="convert")
        plan = evidence("autorig.v3.intent-plan/1", "plan_sha256",
                        requested_intent="convert", dispatch_intent="rig")
        converted = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                           source_manifest=source_manifest(path),
                                           requested_intent="convert", intent_plan=plan)
        self.assertEqual((converted.requested_intent, converted.dispatch_intent), ("convert", "rig"))
        with self.assertRaises(RuntimeContractError):
            normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                   source_manifest=source_manifest(path),
                                   requested_intent="legacy_animal")

    def test_status_mapping_never_marks_review_or_unverified_done(self):
        identity = normalize_task_binding(task_id=TASK, source_sha256=SHA,
                                          source_manifest={"intent": "rig", "sha256": SHA},
                                          requested_intent="rig").identity
        def record(state, error=""):
            return OutboxRecord(identity, state, None, None, None, "", 0, error, 0)
        self.assertEqual(task_status_patch(record("needs_review")).status, "processing")
        self.assertEqual(task_status_patch(record("awaiting_artifact_verification")).status, "processing")
        self.assertEqual(task_status_patch(record("done")).status, "done")
        self.assertEqual(task_status_patch(record("blocked_protocol", "bad")).status, "error")

    async def test_v1_binding_schema_coexists_without_column_collision(self):
        temp, path, db, session, store = self.make()
        with temp:
            db.execute("CREATE TABLE v3_task_bindings(task_id TEXT,attempt INTEGER,run_id TEXT NOT NULL)")
            await store.install(session); db.commit()
            v1_columns = [row[1] for row in db.execute("PRAGMA table_info(v3_task_bindings)")]
            v2_columns = [row[1] for row in db.execute("PRAGMA table_info(v3_dispatch_task_bindings)")]
            self.assertEqual(v1_columns, ["task_id", "attempt", "run_id"])
            self.assertIn("source_manifest_json", v2_columns)
            db.close()

    async def test_future_runtime_schema_fails_before_binding_table_mutation(self):
        temp, path, db, session, store = self.make()
        with temp:
            db.execute("CREATE TABLE v3_dispatch_runtime_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("INSERT INTO v3_dispatch_runtime_meta VALUES('schema_version','99')")
            db.commit()
            with self.assertRaisesRegex(RuntimeError, "newer"):
                await store.install(session)
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("v3_dispatch_task_bindings", names)
            self.assertEqual(db.execute("SELECT value FROM v3_dispatch_runtime_meta").fetchone()[0], "99")
            db.close()


if __name__ == "__main__":
    unittest.main()
