import asyncio
import hashlib
import json
import os
import sys
import unittest

import httpx


BACKEND = os.path.dirname(os.path.dirname(__file__))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)

from v3_pipeline_adapter import (  # noqa: E402
    PROTOCOL_SCHEMA,
    UnsupportedServerRevision,
    V3PipelineClient,
    V3ProtocolError,
    V3State,
    V3Submission,
    idempotency_key,
    parse_status,
)


TASK_ID = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE_SHA = "a" * 64


def capability():
    return {
        "schema": PROTOCOL_SCHEMA,
        "endpoints": {
            "submit": "/api/mt/v3/jobs",
            "status": "/api/mt/v3/jobs/{run_id}",
        },
    }


def manifest(submission, roles=("rigged_glb", "rig_json", "rig_qa")):
    suffixes = {"rigged_glb": ".glb", "rig_json": ".json", "rig_qa": ".json"}
    return {
        "schema": "autorig.v3.artifacts/1",
        "task_id": submission.task_id,
        "source_sha256": submission.source_sha256,
        "artifacts": [
            {
                "role": role,
                "path": f"rig/{role}{suffixes.get(role, '.bin')}",
                "url": f"https://autorig.online/api/mt/files/run/rig/{role}{suffixes.get(role, '.bin')}",
                "sha256": hashlib.sha256(role.encode()).hexdigest(),
                "bytes": len(role.encode()),
                "source_sha256": submission.source_sha256,
            }
            for role in roles
        ],
    }


def fetched(doc):
    return {row["role"]: row["role"].encode() for row in doc["artifacts"]}


def status(submission, state="running", **extra):
    return {
        "schema": PROTOCOL_SCHEMA,
        "task_id": submission.task_id,
        "source_sha256": submission.source_sha256,
        "attempt": submission.attempt,
        "idempotency_key": submission.idempotency_key,
        "run_id": "run_12345678",
        "status": state,
        **extra,
    }


class V3PipelineAdapterTests(unittest.TestCase):
    def setUp(self):
        self.submission = V3Submission.create(TASK_ID, SOURCE_SHA, 2)

    def test_submission_is_typed_and_key_is_stable_for_timeout_retries(self):
        same = V3Submission.create(TASK_ID, SOURCE_SHA, 2)
        next_attempt = V3Submission.create(TASK_ID, SOURCE_SHA, 3)
        self.assertEqual(self.submission, same)
        self.assertEqual(self.submission.idempotency_key, same.idempotency_key)
        self.assertNotEqual(self.submission.idempotency_key, next_attempt.idempotency_key)
        self.assertEqual(
            self.submission.idempotency_key,
            idempotency_key(TASK_ID, SOURCE_SHA, 2),
        )

    def test_submission_rejects_noncanonical_uuid_hash_attempt_or_forged_key(self):
        with self.assertRaises(V3ProtocolError):
            V3Submission.create(TASK_ID.upper(), SOURCE_SHA, 1)
        with self.assertRaises(V3ProtocolError):
            V3Submission.create(TASK_ID, "bad", 1)
        with self.assertRaises(V3ProtocolError):
            V3Submission.create(TASK_ID, SOURCE_SHA, 0)
        with self.assertRaises(V3ProtocolError):
            V3Submission(TASK_ID, SOURCE_SHA, 1, "v3-" + "0" * 64)

    def test_current_plain_text_mt_contract_is_explicitly_unsupported(self):
        async def handler(request):
            return httpx.Response(200, text="AutoRig motion-transfer API", request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                client = V3PipelineClient("https://autorig.online", "secret", http)
                with self.assertRaises(UnsupportedServerRevision):
                    await client.submit(self.submission)

        asyncio.run(scenario())

    def test_unsupported_json_revision_fails_before_submission(self):
        seen = []

        async def handler(request):
            seen.append((request.method, request.url.path))
            return httpx.Response(200, json={"schema": "autorig.v3.dispatch/2"}, request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                client = V3PipelineClient("https://autorig.online", "secret", http)
                with self.assertRaises(UnsupportedServerRevision):
                    await client.submit(self.submission)

        asyncio.run(scenario())
        self.assertEqual(seen, [("GET", "/api/mt")])

    def test_timeout_retry_reuses_exact_body_and_idempotency_header(self):
        posts = []

        async def handler(request):
            if request.method == "GET":
                return httpx.Response(200, json=capability(), request=request)
            posts.append((request.headers["idempotency-key"], request.content))
            if len(posts) == 1:
                raise httpx.ReadTimeout("lost response", request=request)
            return httpx.Response(200, json=status(self.submission), request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                result = await V3PipelineClient(
                    "https://autorig.online", "secret", http
                ).submit(self.submission, timeout_retries=1)
                self.assertEqual(result.state, V3State.RUNNING)

        asyncio.run(scenario())
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0], posts[1])

    def test_capacity_429_remains_queued_without_parsing_error_body(self):
        async def handler(request):
            if request.method == "GET":
                return httpx.Response(200, json=capability(), request=request)
            return httpx.Response(
                429, text="fleet full", headers={"Retry-After": "17"}, request=request
            )

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                result = await V3PipelineClient(
                    "https://autorig.online", "secret", http
                ).submit(self.submission)
                self.assertEqual(result.state, V3State.QUEUED)
                self.assertEqual(result.retry_after_seconds, 17.0)
                self.assertEqual(result.reason, "capacity")

        asyncio.run(scenario())

    def test_partial_is_always_needs_review(self):
        result = parse_status(status(self.submission, "partial"), self.submission)
        self.assertEqual(result.state, V3State.NEEDS_REVIEW)

    def test_done_requires_accepted_qa(self):
        doc = manifest(self.submission)
        payload = status(
            self.submission,
            "done",
            qa={"status": "needs_review"},
            artifact_manifest=doc,
            artifact_manifest_sha256=hashlib.sha256(
                json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        with self.assertRaisesRegex(V3ProtocolError, "qa.status=accepted"):
            parse_status(payload, self.submission)

    def test_done_requires_all_rig_artifacts(self):
        doc = manifest(self.submission, ("rigged_glb", "rig_json"))
        payload = status(
            self.submission,
            "done",
            qa={"status": "accepted"},
            artifact_manifest=doc,
            artifact_manifest_sha256=hashlib.sha256(
                json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        with self.assertRaisesRegex(V3ProtocolError, "rig_qa"):
            parse_status(payload, self.submission)

    def test_done_validates_manifest_hash_bytes_and_source_binding(self):
        for mutation, expected in (
            (lambda d: d.update(bytes=0), "bytes"),
            (lambda d: d.update(source_sha256="b" * 64), "source_sha256 mismatch"),
            (lambda d: d.update(sha256="bad"), "sha256"),
        ):
            doc = manifest(self.submission)
            mutation(doc["artifacts"][0])
            payload = status(
                self.submission,
                "done",
                qa={"status": "accepted"},
                artifact_manifest=doc,
                artifact_manifest_sha256=hashlib.sha256(
                    json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            )
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(V3ProtocolError, expected):
                    parse_status(payload, self.submission, artifact_bytes=fetched(doc))

    def test_done_rejects_tampered_manifest_digest(self):
        doc = manifest(self.submission)
        payload = status(
            self.submission,
            "done",
            qa={"status": "accepted"},
            artifact_manifest=doc,
            artifact_manifest_sha256="f" * 64,
        )
        with self.assertRaisesRegex(V3ProtocolError, "manifest hash mismatch"):
            parse_status(payload, self.submission)

    def test_done_accepts_only_fully_bound_manifest(self):
        doc = manifest(self.submission)
        digest = hashlib.sha256(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        result = parse_status(
            status(
                self.submission,
                "done",
                qa={"status": "accepted"},
                artifact_manifest=doc,
                artifact_manifest_sha256=digest,
            ),
            self.submission,
            artifact_bytes=fetched(doc),
        )
        self.assertEqual(result.state, V3State.DONE)
        self.assertEqual(result.manifest_sha256, digest)
        self.assertEqual({a.role for a in result.artifacts}, {"rigged_glb", "rig_json", "rig_qa"})

    def test_done_without_fetched_bytes_is_not_accepted(self):
        doc = manifest(self.submission)
        digest = hashlib.sha256(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        with self.assertRaisesRegex(V3ProtocolError, "byte verification"):
            parse_status(
                status(
                    self.submission,
                    "done",
                    qa={"status": "accepted"},
                    artifact_manifest=doc,
                    artifact_manifest_sha256=digest,
                ),
                self.submission,
            )

    def test_status_identity_cannot_switch_task_source_attempt_or_key(self):
        changes = (
            {"task_id": "87f7e167-3a98-49c0-a05d-f47cb808e495"},
            {"source_sha256": "b" * 64},
            {"attempt": 3},
            {"idempotency_key": "v3-" + "0" * 64},
        )
        for change in changes:
            payload = status(self.submission)
            payload.update(change)
            with self.subTest(change=change):
                with self.assertRaises(V3ProtocolError):
                    parse_status(payload, self.submission)

    def test_status_url_is_discovered_and_preserves_task_identity(self):
        seen = []

        async def handler(request):
            seen.append(request.url.path)
            if request.url.path == "/api/mt":
                return httpx.Response(200, json=capability(), request=request)
            return httpx.Response(200, json=status(self.submission), request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                result = await V3PipelineClient(
                    "https://autorig.online", "secret", http
                ).status(self.submission, "run_12345678")
                self.assertEqual(result.task_id, TASK_ID)

        asyncio.run(scenario())
        self.assertEqual(seen, ["/api/mt", "/api/mt/v3/jobs/run_12345678"])

    def test_client_downloads_and_verifies_done_artifact_bytes(self):
        doc = manifest(self.submission)
        digest = hashlib.sha256(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        final = status(
            self.submission,
            "done",
            qa={"status": "accepted"},
            artifact_manifest=doc,
            artifact_manifest_sha256=digest,
        )

        async def handler(request):
            if request.url.path == "/api/mt":
                return httpx.Response(200, json=capability(), request=request)
            if request.url.path == "/api/mt/v3/jobs/run_12345678":
                return httpx.Response(200, json=final, request=request)
            role = request.url.path.rsplit("/", 1)[-1].split(".", 1)[0]
            return httpx.Response(200, content=role.encode(), request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                result = await V3PipelineClient(
                    "https://autorig.online", "secret", http
                ).status(self.submission, "run_12345678")
                self.assertEqual(result.state, V3State.DONE)

        asyncio.run(scenario())

    def test_client_rejects_done_artifact_with_wrong_actual_bytes(self):
        doc = manifest(self.submission)
        digest = hashlib.sha256(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        final = status(
            self.submission,
            "done",
            qa={"status": "accepted"},
            artifact_manifest=doc,
            artifact_manifest_sha256=digest,
        )

        async def handler(request):
            if request.url.path == "/api/mt":
                return httpx.Response(200, json=capability(), request=request)
            if request.url.path == "/api/mt/v3/jobs/run_12345678":
                return httpx.Response(200, json=final, request=request)
            return httpx.Response(200, content=b"tampered", request=request)

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                with self.assertRaisesRegex(V3ProtocolError, "byte count mismatch"):
                    await V3PipelineClient(
                        "https://autorig.online", "secret", http
                    ).status(self.submission, "run_12345678")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
