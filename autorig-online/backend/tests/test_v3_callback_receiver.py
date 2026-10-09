import asyncio
import hashlib
import hmac
import json
import os
import sys
import unittest
from dataclasses import dataclass

import httpx
from fastapi import FastAPI


BACKEND = os.path.dirname(os.path.dirname(__file__))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

from v3_callback_receiver import (  # noqa: E402
    CALLBACK_SCHEMA,
    CallbackBinding,
    CallbackCommit,
    CallbackConflict,
    callback_event_id,
    create_v3_callback_router,
)
from v3_pipeline_adapter import V3Submission  # noqa: E402


TASK_ID = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE = "a" * 64
RUN_ID = "run_12345678"
SECRET = b"receiver-test-secret"
CREDENTIAL = "v3-callback-primary"


class MemoryTransaction:
    def __init__(self):
        self.rows = {}

    async def accept(self, binding, event_id, payload, terminal_status):
        key = binding.task_id
        existing = self.rows.get(key)
        if existing and existing["event_id"] == event_id:
            if existing["payload"] != payload:
                raise CallbackConflict("event id replay payload mismatch")
            return CallbackCommit(existing["receipt"], True, existing["notify_pending"])
        if existing and payload["sequence"] <= existing["sequence"]:
            raise CallbackConflict("callback sequence is not monotonic")
        terminal = payload["event"] == "terminal"
        if existing and existing["terminal"]:
            raise CallbackConflict("callback arrived after terminal state")
        receipt = "receipt-" + event_id[-24:]
        self.rows[key] = {
            "event_id": event_id,
            "payload": dict(payload),
            "sequence": payload["sequence"],
            "terminal": terminal,
            "receipt": receipt,
            "notify_pending": terminal_status is not None,
        }
        return CallbackCommit(receipt, False, terminal_status is not None)

    async def mark_notified(self, binding, event_id):
        row = self.rows[binding.task_id]
        if row["event_id"] != event_id:
            raise CallbackConflict("notification event mismatch")
        row["notify_pending"] = False


def canonical(doc):
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()


def manifest(binding):
    rows = []
    for role, suffix in (("rigged_glb", ".glb"), ("rig_json", ".json"), ("rig_qa", ".json")):
        data = role.encode()
        rows.append({
            "role": role,
            "path": f"rig/{role}{suffix}",
            "url": f"https://autorig.online/api/mt/files/{RUN_ID}/rig/{role}{suffix}",
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "source_sha256": binding.source_sha256,
        })
    doc = {"schema": "autorig.v3.artifacts/1", "task_id": binding.task_id,
           "source_sha256": binding.source_sha256, "artifacts": rows}
    return doc, hashlib.sha256(canonical(doc)).hexdigest()


class V3CallbackReceiverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        submission = V3Submission.create(TASK_ID, SOURCE, 2)
        self.binding = CallbackBinding(TASK_ID, RUN_ID, SOURCE, 2,
                                       submission.idempotency_key, CREDENTIAL)
        self.transaction = MemoryTransaction()
        self.notifications = []

        async def notify(task_id):
            self.notifications.append(task_id)

        app = FastAPI()
        app.include_router(create_v3_callback_router(
            secret_provider=lambda credential: SECRET if credential == CREDENTIAL else b"",
            binding_store=lambda task_id: self.binding if task_id == TASK_ID else None,
            transaction=self.transaction,
            notify_done=notify,
        ))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                        base_url="https://testserver")

    async def asyncTearDown(self):
        await self.client.aclose()

    def event(self, sequence, event="progress", **extra):
        doc = {
            "schema": CALLBACK_SCHEMA,
            "event": event,
            "task_id": self.binding.task_id,
            "run_id": self.binding.run_id,
            "source_sha256": self.binding.source_sha256,
            "attempt": self.binding.attempt,
            "idempotency_key": self.binding.idempotency_key,
            "sequence": sequence,
        }
        doc.update(extra or ({"stage": "rig", "status": "running", "progress": 0.5}
                             if event == "progress" else {}))
        if event == "progress" and not extra:
            doc.update(stage="rig", status="running", progress=0.5)
        return doc

    async def post(self, doc, *, secret=SECRET, event_id=None):
        body = canonical(doc)
        signature = hmac.new(secret, body, hashlib.sha256).hexdigest()
        return await self.client.post("/internal/v3/task-callbacks", content=body, headers={
            "Content-Type": "application/json",
            "X-AutoRig-V3-Credential": CREDENTIAL,
            "X-AutoRig-V3-Signature": "v1=" + signature,
            "Idempotency-Key": event_id or callback_event_id(doc),
        })

    async def test_bad_signature_and_event_id_replay_are_rejected(self):
        doc = self.event(1)
        self.assertEqual((await self.post(doc, secret=b"wrong")).status_code, 401)
        self.assertEqual((await self.post(doc, event_id="v3cb-" + "0" * 64)).status_code, 409)
        self.assertFalse(self.transaction.rows)

    async def test_out_of_order_sequence_and_binding_switch_fail(self):
        self.assertEqual((await self.post(self.event(2))).status_code, 200)
        self.assertEqual((await self.post(self.event(1))).status_code, 409)
        switched = self.event(3)
        switched["run_id"] = "run_other123"
        self.assertEqual((await self.post(switched)).status_code, 409)

    async def test_needs_review_is_terminal_without_done_notification(self):
        doc = self.event(4, "terminal", status="needs_review", qa={"status": "needs_review"})
        response = await self.post(doc)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.notifications, [])
        self.assertEqual((await self.post(self.event(5))).status_code, 409)

    async def test_done_requires_accepted_qa_and_valid_manifest(self):
        doc = self.event(5, "terminal", status="done", qa={"status": "rejected"})
        self.assertEqual((await self.post(doc)).status_code, 409)
        doc["qa"] = {"status": "accepted"}
        doc["artifact_manifest"] = {"schema": "autorig.v3.artifacts/1"}
        doc["artifact_manifest_sha256"] = "f" * 64
        self.assertEqual((await self.post(doc)).status_code, 409)
        self.assertFalse(self.transaction.rows)

    async def test_done_retry_uses_same_receipt_and_notifies_once(self):
        artifact_manifest, digest = manifest(self.binding)
        doc = self.event(6, "terminal", status="done", qa={"status": "accepted"},
                         artifact_manifest=artifact_manifest,
                         artifact_manifest_sha256=digest)
        first = await self.post(doc)
        second = await self.post(doc)
        self.assertEqual((first.status_code, second.status_code), (200, 200))
        self.assertEqual(first.json()["receipt_id"], second.json()["receipt_id"])
        self.assertFalse(first.json()["duplicate"])
        self.assertTrue(second.json()["duplicate"])
        self.assertEqual(self.notifications, [TASK_ID])

    async def test_notification_failure_retries_after_terminal_commit(self):
        calls = []

        async def flaky(task_id):
            calls.append(task_id)
            if len(calls) == 1:
                raise RuntimeError("temporary")

        app = FastAPI()
        app.include_router(create_v3_callback_router(
            secret_provider=lambda _: SECRET,
            binding_store=lambda _: self.binding,
            transaction=self.transaction,
            notify_done=flaky,
        ))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="https://testserver") as client:
            old = self.client
            self.client = client
            artifact_manifest, digest = manifest(self.binding)
            doc = self.event(7, "terminal", status="done", qa={"status": "accepted"},
                             artifact_manifest=artifact_manifest,
                             artifact_manifest_sha256=digest)
            self.assertEqual((await self.post(doc)).status_code, 503)
            retried = await self.post(doc)
            self.assertEqual(retried.status_code, 200)
            self.assertTrue(retried.json()["duplicate"])
            self.assertEqual(calls, [TASK_ID, TASK_ID])
            self.client = old


if __name__ == "__main__":
    unittest.main()
