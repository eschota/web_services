import asyncio
import json
import os
import pathlib
import sys
import unittest

import httpx


BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from v3_pipeline_adapter import (  # noqa: E402
    DISPATCH_V2_SCHEMA,
    V3DispatchClient,
    V3DispatchSubmission,
    V3ProtocolError,
    V3State,
    parse_dispatch_v2_capabilities,
    parse_dispatch_v2_status,
)


TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE = "a" * 64
REF = "src-" + "b" * 64


def capabilities():
    return {"schema": DISPATCH_V2_SCHEMA, "intents": ["accessory"],
            "required_artifact_roles": {"accessory": ["model_glb", "source_manifest", "qa_report", "attachment_mask"]},
            "endpoints": {"register_source": "/api/mt/v3/sources", "submit": "/api/mt/v3/runs",
                          "status": "/api/mt/v3/runs/{run_id}"}}


class V3DispatchAdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_v2_key_binds_intent_revision_and_source_ref(self):
        one = V3DispatchSubmission.create(TASK, REF, SOURCE, 1, "accessory", "autorig-v3-media/1")
        two = V3DispatchSubmission.create(TASK, REF, SOURCE, 1, "rig", "autorig-v3-rig/1")
        other_ref = V3DispatchSubmission.create(
            TASK, "src-" + "c" * 64, SOURCE, 1, "accessory", "autorig-v3-media/1")
        self.assertNotEqual(one.idempotency_key, two.idempotency_key)
        self.assertNotEqual(one.idempotency_key, other_ref.idempotency_key)
        self.assertEqual(one.payload()["source_ref"], REF)

    def test_capability_requires_exact_intent_artifact_contract(self):
        response = httpx.Response(200, json=capabilities(), headers={"content-type": "application/json"})
        parsed = parse_dispatch_v2_capabilities(response)
        self.assertEqual(parsed.submit_path, "/api/mt/v3/runs")
        broken = capabilities(); broken["required_artifact_roles"]["accessory"].remove("attachment_mask")
        with self.assertRaises(Exception):
            parse_dispatch_v2_capabilities(httpx.Response(
                200, json=broken, headers={"content-type": "application/json"}))

    def test_needs_review_can_expose_preview_but_done_requires_mask_and_accepted_qa(self):
        submission = V3DispatchSubmission.create(TASK, REF, SOURCE, 1, "accessory", "autorig-v3-media/1")
        base = {**submission.payload(), "run_id": "v3run-" + "c" * 32,
                "stage": "attachment_mask_review", "progress": .9,
                "qa": {"status": "needs_review"},
                "artifact_manifest": {"artifacts": [{"role": "model_glb"}]}, "error": ""}
        review = parse_dispatch_v2_status({**base, "status": "needs_review"}, submission)
        self.assertIs(review.state, V3State.NEEDS_REVIEW)
        with self.assertRaises(V3ProtocolError):
            parse_dispatch_v2_status({**base, "status": "done", "qa": {"status": "accepted"}}, submission)

    async def test_client_uses_real_v2_paths(self):
        submission = V3DispatchSubmission.create(TASK, REF, SOURCE, 1, "accessory", "autorig-v3-media/1")
        seen = []
        async def handler(request):
            seen.append((request.method, request.url.path))
            if request.url.path == "/api/mt/v3":
                return httpx.Response(200, json=capabilities(), headers={"content-type": "application/json"})
            if request.url.path == "/api/mt/v3/runs":
                return httpx.Response(202, json={**submission.payload(), "run_id": "v3run-" + "d" * 32,
                    "status": "queued", "stage": "source_registered", "progress": 0,
                    "qa": {}, "artifact_manifest": {}, "error": ""})
            raise AssertionError(request.url.path)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            result = await V3DispatchClient("https://autorig.online", "secret", http).submit(submission)
        self.assertIs(result.state, V3State.QUEUED)
        self.assertEqual(seen, [("GET", "/api/mt/v3"), ("POST", "/api/mt/v3/runs")])


if __name__ == "__main__":
    unittest.main()
