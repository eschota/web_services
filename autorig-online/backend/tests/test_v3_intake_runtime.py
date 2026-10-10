"""V3 intake, runtime projection and read-API mapping (dependency-light, no network)."""
import asyncio
import hashlib
import json
import os
import pathlib
import sqlite3
import struct
import sys
import tempfile
import types
import unittest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import v3_intake as intake
from v3_dispatch_outbox import DispatchIdentity, V3DispatchOutbox
from v3_pipeline_adapter import V3DispatchStatus, V3State, parse_dispatch_v2_status, V3DispatchSubmission
from v3_task_runtime import SameTaskDbBindings, V3TaskRuntime, normalize_task_binding


def glb(extra=b""):
    doc = {"asset": {"version": "2.0"}, "meshes": [{"primitives": [{"attributes": {"POSITION": 0}}]}]}
    raw = json.dumps(doc, separators=(",", ":")).encode()
    raw += b" " * ((4 - len(raw) % 4) % 4)
    body = struct.pack("<II", len(raw), 0x4E4F534A) + raw + extra
    return b"glTF" + struct.pack("<II", 2, 12 + len(body)) + body


class Result:
    def __init__(self, cursor):
        self.cursor, self.rowcount = cursor, cursor.rowcount

    def mappings(self):
        return self

    def all(self):
        return [dict(row) for row in self.cursor.fetchall()]


class Session:
    def __init__(self, db):
        self.db = db

    async def execute(self, sql, params=None):
        return Result(self.db.execute(str(sql), params or {}))

    async def commit(self):
        self.db.commit()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class Work(unittest.IsolatedAsyncioTestCase):
    def temp(self):
        root = BACKEND / ".work" / "v3-intake-tests"
        root.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(dir=root)


class IntakeTests(Work):
    def test_route_switch_defaults_admin_only_and_explicit_always(self):
        old = {k: os.environ.pop(k, None) for k in ("AUTORIG_V3_ROUTES", "AUTORIG_V3_ADMIN_ROUTES")}
        try:
            self.assertFalse(intake.route_enabled("website"))
            self.assertTrue(intake.route_enabled("website", is_admin=True))
            self.assertTrue(intake.route_enabled("telegram", explicit=True))
            os.environ["AUTORIG_V3_ROUTES"] = "website, generation"
            self.assertTrue(intake.route_enabled("generation"))
            self.assertFalse(intake.route_enabled("telegram"))
            os.environ["AUTORIG_V3_ADMIN_ROUTES"] = "website"
            self.assertFalse(intake.route_enabled("convert", is_admin=True))
            os.environ["AUTORIG_V3_ROUTES"] = "all"
            self.assertTrue(intake.route_enabled("convert"))
        finally:
            for key, value in old.items():
                os.environ.pop(key, None)
                if value is not None:
                    os.environ[key] = value

    def test_formats_glb_facts_and_immutable_source_store(self):
        with self.temp() as tmp:
            root = pathlib.Path(tmp)
            (root / "m.glb").write_bytes(glb())
            (root / "m.fbx").write_bytes(b"Kaydara FBX Binary  \x00\x1a\x00")
            (root / "m.obj").write_text("# obj\no mesh\nv 0 0 0\nv 1 0 0\nv 0 1 0\nf 1 2 3\n")
            (root / "m.png").write_bytes(b"\x89PNG\r\n\x1a\n")
            self.assertEqual([intake.sniff_format(root / n) for n in ("m.glb", "m.fbx", "m.obj", "m.png")],
                             ["glb", "fbx", "obj", None])
            self.assertEqual(intake.glb_facts(glb())["primitives"], 1)
            broken = bytearray(glb()); broken[8:12] = struct.pack("<I", 9)
            with self.assertRaises(intake.V3IntakeError):
                intake.glb_facts(bytes(broken))
            old = intake.SOURCE_ROOT
            intake.SOURCE_ROOT = root / "sources"
            try:
                first, digest = intake.store_source(glb())
                second, again = intake.store_source(glb())
                self.assertEqual((first, digest), (second, again))
                self.assertEqual(hashlib.sha256(first.read_bytes()).hexdigest(), digest)
                self.assertEqual(first.name, digest + ".glb")
            finally:
                intake.SOURCE_ROOT = old

    def test_local_upload_path_accepts_only_our_upload_files(self):
        with self.temp() as tmp:
            root = pathlib.Path(tmp)
            token = "0d0f3c7e-2b1a-4c55-9a43-6a6c2b8e0e11"
            (root / token).mkdir()
            (root / token / "model one.glb").write_bytes(glb())
            fake = types.ModuleType("config")
            fake.APP_URL, fake.UPLOAD_DIR = "https://autorig.online", str(root)
            saved = sys.modules.get("config")
            sys.modules["config"] = fake
            try:
                ok = intake.local_upload_path(f"https://autorig.online/u/{token}/model%20one.glb")
                self.assertEqual(ok, (root / token / "model one.glb").resolve())
                self.assertIsNone(intake.local_upload_path(f"https://evil.example/u/{token}/model%20one.glb"))
                self.assertIsNone(intake.local_upload_path(f"https://autorig.online/u/{token}/..%2F..%2Fx"))
                self.assertIsNone(intake.local_upload_path("https://autorig.online/u/not-a-token!/x.glb"))
                self.assertIsNone(intake.local_upload_path(f"https://autorig.online/u/{token}/missing.glb"))
            finally:
                if saved is None:
                    sys.modules.pop("config", None)
                else:
                    sys.modules["config"] = saved

    def test_bindings_keep_intent_and_bind_generation_receipts(self):
        with self.temp() as tmp:
            path = pathlib.Path(tmp) / "s.glb"; path.write_bytes(glb())
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            task = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
            rig = intake.build_binding(task, path, digest, origin="website", filename="s.glb")
            self.assertEqual((rig.requested_intent, rig.dispatch_intent, rig.state), ("rig", "rig", "pending"))
            receipt = intake.generation_receipt(task, digest, renderfin_job_id="j1", glb_url="https://x/y.glb")
            gen = intake.build_binding(task, path, digest, requested_intent="generate", origin="generation",
                                       receipt=receipt)
            self.assertEqual(gen.source_manifest["upstream_receipt_sha256"], receipt["receipt_sha256"])
            conv = intake.build_binding(task, path, digest, requested_intent="convert", origin="website")
            self.assertEqual((conv.requested_intent, conv.dispatch_intent), ("convert", "rig"))
            self.assertEqual(conv.intent_plan["source_sha256"], digest)


class Remote:
    def __init__(self, final):
        self.final, self.polls = final, 0

    async def register_source(self, manifest):
        return {"source_ref": "src-" + "a" * 64, "source_sha256": manifest["sha256"], "intent": "rig"}

    async def submit(self, submission):
        return V3DispatchStatus(V3State.QUEUED, "v3run-" + "b" * 32, submission, "queued", 0, {}, {}, "",
                                {"mt_run_id": "c" * 20})

    async def status(self, submission, run_id):
        self.polls += 1
        return V3DispatchStatus(self.final, run_id, submission, "qa_review", 1,
                                {"status": "needs_review", "reasons": ["layers"]},
                                {"artifacts": [{"role": "rigged_glb", "public_url": "https://x/rig.glb",
                                                "sha256": "d" * 64, "bytes": 1}]}, "", {"mt_run_id": "c" * 20})


class RuntimeTests(Work):
    async def test_runtime_projects_each_step_and_settles_needs_review(self):
        with self.temp() as tmp:
            root = pathlib.Path(tmp)
            source = root / "s.glb"; source.write_bytes(glb())
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            task = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
            db = sqlite3.connect(root / "tasks.sqlite3", isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("CREATE TABLE tasks(id VARCHAR(36) PRIMARY KEY,status TEXT)")
            db.execute("INSERT INTO tasks VALUES(?, 'created')", (task,))
            store = SameTaskDbBindings()
            session = Session(db)
            await store.install(session)
            manifest = {"schema": "autorig.v3.source/1", "intent": "rig", "path": str(source),
                        "sha256": digest, "bytes": source.stat().st_size}
            await store.bind_in_task_transaction(session, normalize_task_binding(
                task_id=task, source_sha256=digest, source_manifest=manifest, requested_intent="rig"))
            projected = []

            async def projector(_session, binding, patch, record):
                projected.append((record.state, patch.status, dict(record.remote_status).get("session")))

            clock = [100.0]
            outbox = V3DispatchOutbox(root / "outbox.sqlite3", clock=lambda: clock[0])
            remote = Remote(V3State.NEEDS_REVIEW)
            runtime = V3TaskRuntime(session_factory=lambda: Session(db), bindings=store, outbox=outbox,
                                    remote_factory=lambda: remote, projector=projector,
                                    artifact_verifier=lambda status: None)
            outbox.migrate()
            await runtime._recover()
            for _ in range(3):
                self.assertTrue(await runtime.run_once())
                clock[0] += 10
            self.assertEqual([p[0] for p in projected], ["pending_submit", "queued", "needs_review"])
            self.assertEqual(projected[-1][1], "needs_review")
            self.assertEqual(projected[-1][2], {"mt_run_id": "c" * 20})   # the remote's session is persisted
            self.assertFalse(await runtime.run_once())          # settled: nothing left to dispatch
            self.assertEqual(await store.recover_pending(session), [])
            self.assertEqual(remote.polls, 1)
            db.close()

    def test_status_parser_keeps_only_a_wellformed_session(self):
        submission = V3DispatchSubmission.create("dfc5ccce-3e0c-4729-99dc-286eb23dac88", "src-" + "a" * 64,
                                                 "e" * 64, 1, "rig", "autorig-v3-rig/1")
        payload = {**submission.payload(), "status": "running", "run_id": "v3run-" + "b" * 32, "progress": .4,
                   "session": {"mt_run_id": "0123456789abcdef0123", "viewer_url": "https://evil/x"}}
        status = parse_dispatch_v2_status(payload, submission)
        self.assertEqual(status.session["viewer_url"], "/api/mt/unity/test/index.html?run=0123456789abcdef0123")
        payload["session"] = {"mt_run_id": "../../etc"}
        self.assertEqual(parse_dispatch_v2_status(payload, submission).session, {})


class ShellTests(unittest.TestCase):
    def test_shell_states_are_explicit(self):
        import v3_runtime_mount as mount

        def task(state, **v3):
            settings = {"v3": {"state": state, **v3}}
            return types.SimpleNamespace(id="t", status="processing", error_message=None,
                                         viewer_settings=json.dumps(settings), created_at=None)
        session = {"mt_run_id": "a" * 20, "viewer_url": "/api/mt/unity/test/index.html?run=" + "a" * 20}
        running = mount._shell(task("running", stage="rig", progress=.4, session=session))
        self.assertEqual((running["status"], running["viewer_state"], running["progress"]), ("processing", "loading", .4))
        self.assertEqual(running["viewer_url"], session["viewer_url"])
        review = mount._shell(task("needs_review", qa={"reasons": ["hair layer"]}, session=session))
        self.assertEqual((review["status"], review["viewer_state"], review["progress"]), ("needs_review", "ready", 1.0))
        self.assertIn("hair layer", review["message"])
        waiting = mount._shell(task("pending"))
        self.assertEqual((waiting["status"], waiting["viewer_url"], waiting["viewer_state"]),
                         ("created", None, "awaiting_binding"))
        failed = mount._shell(task("blocked_protocol", error="bad"))
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(mount._shell(task("done", session=session))["status"], "done")


if __name__ == "__main__":
    unittest.main()
