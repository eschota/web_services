import pathlib
import hashlib
import sys
import tempfile
import unittest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from v3_dispatch_outbox import (DispatchIdentity, LeaseLost, OutboxConflict,
                                V3DispatchOutbox, verify_dispatch_artifacts)
from v3_pipeline_adapter import V3DispatchStatus, V3State


TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE_BYTES = b"exact-source-model"
SOURCE = hashlib.sha256(SOURCE_BYTES).hexdigest()
REF = "src-" + "b" * 64


def manifest(path, intent="rig"):
    return {"schema": "autorig.v3.source/1", "intent": intent,
            "path": str(path), "sha256": SOURCE, "bytes": len(SOURCE_BYTES)}


class Remote:
    def __init__(self):
        self.registers = 0
        self.submits = 0
        self.polls = 0

    async def register_source(self, _manifest):
        self.registers += 1
        return {"source_ref": REF, "source_sha256": SOURCE}

    async def submit(self, submission):
        self.submits += 1
        return V3DispatchStatus(V3State.QUEUED, "v3run-" + "c" * 32,
                                submission, "source_registered", 0, {}, {}, "")

    async def status(self, submission, run_id):
        self.polls += 1
        blobs = {role: role.encode() for role in ("rigged_glb", "rig_json", "rig_qa")}
        rows = [{"role": role, "path": role, "url": "/" + role,
                 "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
                 "source_sha256": SOURCE} for role, data in blobs.items()]
        return V3DispatchStatus(V3State.DONE, run_id, submission, "publish", 1,
                                {"status": "accepted"}, {"artifacts": rows}, "")


class V3DispatchOutboxTests(unittest.IsolatedAsyncioTestCase):
    def make(self):
        root = pathlib.Path(__file__).resolve().parents[1] / ".work" / "v3-outbox-tests"
        root.mkdir(parents=True, exist_ok=True)
        temp = tempfile.TemporaryDirectory(dir=root)
        store = V3DispatchOutbox(pathlib.Path(temp.name) / "outbox.sqlite3", clock=lambda: 100.0)
        store.migrate()
        source = pathlib.Path(temp.name) / "model.glb"
        source.write_bytes(SOURCE_BYTES)
        return temp, store, source

    def identity(self, attempt=1):
        return DispatchIdentity.create(TASK, SOURCE, "rig", "autorig-v3-rig/1", attempt)

    def test_exact_identity_is_idempotent_and_manifest_conflict_fails(self):
        temp, store, source = self.make()
        with temp:
            first = store.enqueue(self.identity(), manifest(source))
            second = store.enqueue(self.identity(), manifest(source))
            self.assertEqual((first.state, second.state), ("pending_register", "pending_register"))
            altered = manifest(source); altered["bytes"] += 1
            with self.assertRaises(OutboxConflict):
                store.enqueue(self.identity(), altered)

    async def test_register_submit_poll_survives_new_store_instance(self):
        temp, store, source = self.make()
        with temp:
            store.enqueue(self.identity(), manifest(source))
            remote = Remote()
            self.assertTrue(await store.process_one(remote))
            after_register = store.get(self.identity())
            self.assertEqual((after_register.state, after_register.source_ref), ("pending_submit", REF))
            restarted = V3DispatchOutbox(store.path, clock=lambda: 100.0)
            restarted.migrate()
            self.assertEqual(len(restarted.resumable()), 1)
            self.assertTrue(await restarted.process_one(remote))
            queued = restarted.get(self.identity())
            self.assertEqual((queued.state, queued.run_id), ("queued", "v3run-" + "c" * 32))
            # Advance the deterministic test clock past the poll delay.
            restarted.clock = lambda: 106.0
            self.assertTrue(await restarted.process_one(remote))
            self.assertEqual(restarted.get(self.identity()).state, "awaiting_artifact_verification")
            restarted.clock = lambda: 112.0
            blobs = {role: role.encode() for role in ("rigged_glb", "rig_json", "rig_qa")}
            async def verifier(status):
                return await verify_dispatch_artifacts(status, lambda row: blobs[row["role"]])
            self.assertTrue(await restarted.process_one(remote, artifact_verifier=verifier))
            self.assertEqual(restarted.get(self.identity()).state, "done")
            self.assertEqual((remote.registers, remote.submits, remote.polls), (1, 1, 2))

    async def test_protocol_mismatch_blocks_without_legacy_fallback(self):
        class WrongSource(Remote):
            async def register_source(self, _manifest):
                return {"source_ref": REF, "source_sha256": "d" * 64}

        temp, store, source = self.make()
        with temp:
            store.enqueue(self.identity(), manifest(source))
            await store.process_one(WrongSource())
            result = store.get(self.identity())
            self.assertEqual(result.state, "blocked_protocol")
            self.assertIn("hash mismatch", result.error)
            self.assertEqual(store.resumable(), [])

    async def test_transport_failure_keeps_attempt_and_resumes_same_stage(self):
        class Offline(Remote):
            async def register_source(self, _manifest):
                raise OSError("offline")

        temp, store, source = self.make()
        with temp:
            store.enqueue(self.identity(attempt=3), manifest(source))
            await store.process_one(Offline(), retry_seconds=20)
            result = store.get(self.identity(attempt=3))
            self.assertEqual((result.state, result.identity.attempt), ("pending_register", 3))
            self.assertIn("offline", result.error)

    def test_v1_shape_cannot_be_silently_enqueued_as_v2(self):
        temp, store, source = self.make()
        with temp:
            bad = {"schema": "autorig.v3.dispatch/1", "sha256": SOURCE,
                   "path": "/private/model.glb"}
            with self.assertRaises(OutboxConflict):
                store.enqueue(self.identity(), bad)

    def test_forged_identity_dataclass_is_rejected_at_enqueue(self):
        temp, store, source = self.make()
        with temp:
            forged = DispatchIdentity(TASK.upper(), SOURCE, "rig", "autorig-v3-rig/1", 1)
            with self.assertRaises(OutboxConflict):
                store.enqueue(forged, manifest(source))

    def test_expired_lease_cannot_be_overwritten_by_stale_runner(self):
        now = [100.0]
        temp, store, source = self.make()
        with temp:
            store.clock = lambda: now[0]
            store.enqueue(self.identity(), manifest(source))
            old = store._claim(5)
            now[0] = 106.0
            new = store._claim(5)
            self.assertNotEqual(old["lease_token"], new["lease_token"])
            with self.assertRaises(LeaseLost):
                store._update(self.identity().key, old["lease_token"], state="pending_submit")
            store._update(self.identity().key, new["lease_token"], state="pending_submit")

    async def test_artifact_verifier_rejects_bytes_roles_and_qa_mismatch(self):
        remote = Remote()
        submission = __import__("v3_pipeline_adapter").V3DispatchSubmission.create(
            TASK, REF, SOURCE, 1, "rig", "autorig-v3-rig/1")
        good = await remote.status(submission, "v3run-" + "c" * 32)
        blobs = {role: role.encode() for role in ("rigged_glb", "rig_json", "rig_qa")}
        await verify_dispatch_artifacts(good, lambda row: blobs[row["role"]])
        with self.assertRaisesRegex(Exception, "byte verification"):
            await verify_dispatch_artifacts(good, lambda _row: b"corrupt")
        missing_doc = {"artifacts": good.artifact_manifest["artifacts"][:-1]}
        missing = V3DispatchStatus(V3State.DONE, good.run_id, submission, "publish", 1,
                                   good.qa, missing_doc, "")
        with self.assertRaisesRegex(Exception, "required artifacts"):
            await verify_dispatch_artifacts(missing, lambda row: blobs[row["role"]])
        rejected = V3DispatchStatus(V3State.DONE, good.run_id, submission, "publish", 1,
                                    {"status": "rejected"}, good.artifact_manifest, "")
        with self.assertRaisesRegex(Exception, "accepted remote QA"):
            await verify_dispatch_artifacts(rejected, lambda row: blobs[row["role"]])


if __name__ == "__main__":
    unittest.main()
