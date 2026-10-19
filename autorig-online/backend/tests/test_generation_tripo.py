"""generation_tripo (owner 2026-10-19: «если задачу делаю я — отправляй сразу в Tripo»): the policy by task owner, the
apiv3 calls through a mock transport, the renderfin-shaped poll answers and the one-time download of the GLB."""
import asyncio
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest

import httpx


BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

TMP = pathlib.Path(tempfile.mkdtemp(prefix="tripo-test-"))
os.environ["AUTORIG_TRIPO_ENV"] = str(TMP / "tripo.env")
os.environ["AUTORIG_TRIPO_VAR"] = str(TMP / "var")
os.environ["AUTORIG_PICTURE_UPLOAD_ROOT"] = str(TMP / "uploads")
(TMP / "tripo.env").write_text("TRIPO_API_KEY=tsk_test_key_0123456789\n", encoding="utf-8")

import generation_tripo as gt  # noqa: E402

TOKEN = "b0c4ec32-bb59-45df-a9a2-dabf196fc4de"
IMAGE = f"https://autorig.online/u/{TOKEN}/cutout.png"
TASK = "9e7c06e8-3163-40da-b7fa-ad909beebd35"


def task(owner_type="user", owner_id="eschota@gmail.com"):
    return types.SimpleNamespace(id=TASK, owner_type=owner_type, owner_id=owner_id, input_url=IMAGE)


class FakeTripo:
    """apiv3 as the SDK describes it: POST /v3/generation/image-to-model -> {code: 0, data: {task_id}};
    GET /v3/tasks/<id> -> {code: 0, data: {status, progress, output: {model_url}}}; the signed model URL serves bytes."""

    def __init__(self, statuses, model=b"glTF-binary-bytes"):
        self.statuses = list(statuses)
        self.model = model
        self.calls = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path, request.headers.get("Authorization")))
        if request.url.path == "/v3/generation/image-to-model":
            body = json.loads(request.content)
            self.payload = body
            return httpx.Response(200, json={"code": 0, "data": {"task_id": "tripo-task-1"}})
        if request.url.path == "/v3/tasks/tripo-task-1":
            st = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
            data = {"task_id": "tripo-task-1", "status": st, "progress": 40 if st == "running" else 100,
                    "credits_consumed": 25 if st == "success" else None}
            if st == "success":
                data["output"] = {"model_url": "https://cdn.tripo3d.example/signed/model.glb?sig=1"}
            if st == "failed":
                data["error_msg"] = "nsfw"
            return httpx.Response(200, json={"code": 0, "data": data})
        if request.url.host == "cdn.tripo3d.example":
            return httpx.Response(200, content=self.model)
        return httpx.Response(404, json={"code": 2001, "message": "no such route"})


class Policy(unittest.TestCase):
    def test_admin_owner_goes_to_tripo(self):
        os.environ["AUTORIG_PICTURE_TRIPO"] = "admin"
        self.assertTrue(gt.enabled_for(task()))
        self.assertFalse(gt.enabled_for(task("anon", "anon-123")))
        self.assertFalse(gt.enabled_for(task("user", "someone@example.com")))

    def test_switches(self):
        os.environ["AUTORIG_PICTURE_TRIPO"] = "off"
        self.assertFalse(gt.enabled_for(task()))
        os.environ["AUTORIG_PICTURE_TRIPO"] = "all"
        self.assertTrue(gt.enabled_for(task("anon", "anon-123")))
        os.environ["AUTORIG_PICTURE_TRIPO"] = "admin"

    def test_no_key_means_off(self):
        old = os.environ["AUTORIG_TRIPO_ENV"]
        gt.TRIPO_ENV = pathlib.Path(TMP / "missing.env")
        try:
            self.assertFalse(gt.enabled_for(task()))
        finally:
            gt.TRIPO_ENV = pathlib.Path(old)

    def test_job_ids(self):
        self.assertTrue(gt.is_job("tripo:abc"))
        self.assertFalse(gt.is_job("4029d31c-bd3c-41db-9e54-9356a5777695"))
        self.assertEqual(gt.token_of(IMAGE), TOKEN)
        self.assertIsNone(gt.token_of("https://autorig.online/x/y.png"))


class Flow(unittest.TestCase):
    def run_flow(self, statuses):
        fake = FakeTripo(statuses)
        transport = httpx.MockTransport(fake.handler)
        job = asyncio.run(gt.start(IMAGE, TASK, transport=transport))
        polls = []
        for _ in range(len(statuses) + 1):
            polls.append(asyncio.run(gt.poll(job, transport=transport)))
        return fake, job, polls

    def test_start_then_running_then_success(self):
        fake, job, polls = self.run_flow(["queued", "running", "success"])
        self.assertEqual(job, "tripo:tripo-task-1")
        self.assertEqual(fake.payload["input"], IMAGE)
        self.assertEqual(fake.payload["texture_quality"], "detailed")
        self.assertTrue(fake.payload["pbr"] and fake.payload["texture"])
        self.assertTrue(all(a == "Bearer tsk_test_key_0123456789" for _, _, a in fake.calls[:2]))
        self.assertEqual(polls[0]["stage"], "hunyuan")
        self.assertEqual(polls[0]["hunyuan_worker"], "tripo")
        self.assertEqual(polls[1]["stage"], "hunyuan")
        self.assertEqual(polls[1]["progress"], 40)
        self.assertEqual(polls[2]["stage"], "ready")
        self.assertEqual(polls[2]["glb_url"], f"https://autorig.online/u/{TOKEN}/tripo.glb")
        glb = pathlib.Path(os.environ["AUTORIG_PICTURE_UPLOAD_ROOT"]) / TOKEN / "tripo.glb"
        self.assertEqual(glb.read_bytes(), b"glTF-binary-bytes")
        # the fourth poll answers from the record: no Tripo call, no second download
        n_calls = len(fake.calls)
        self.assertEqual(polls[3]["stage"], "ready")
        self.assertEqual(len(fake.calls), n_calls)
        rec = json.loads((pathlib.Path(os.environ["AUTORIG_TRIPO_VAR"]) / "jobs" / "tripo-task-1.json").read_text())
        self.assertEqual(rec["status"], "success")
        self.assertEqual(rec["credits_consumed"], 25)
        ledger = (pathlib.Path(os.environ["AUTORIG_TRIPO_VAR"]) / "ledger.jsonl").read_text().splitlines()
        self.assertEqual([json.loads(l)["event"] for l in ledger][-2:], ["start", "done"])

    def test_failed(self):
        glb = pathlib.Path(os.environ["AUTORIG_PICTURE_UPLOAD_ROOT"]) / TOKEN / "tripo.glb"
        if glb.exists():
            glb.unlink()
        fake, job, polls = self.run_flow(["failed"])
        self.assertEqual(polls[0]["stage"], "failed")
        self.assertIn("nsfw", polls[0]["error"])
        self.assertFalse(glb.exists())

    def test_renderfin_poll_routes_tripo_ids(self):
        import render_prompting

        fake = FakeTripo(["running"])
        asyncio.run(gt.start(IMAGE, TASK, transport=httpx.MockTransport(fake.handler)))
        real = gt.poll

        async def fake_poll(job_id, **kw):
            return await real(job_id, transport=httpx.MockTransport(fake.handler))

        gt.poll = fake_poll
        try:
            out = asyncio.run(render_prompting.poll_character_gen("tripo:tripo-task-1"))
        finally:
            gt.poll = real
        self.assertEqual(out["stage"], "hunyuan")
        self.assertEqual(out["provider"], "tripo")


if __name__ == "__main__":
    unittest.main()
