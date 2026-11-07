"""Renderfin media edit (/api/media-edit, 2026-11-07): validation, access/tier, node choice, dispatch,
status mapping, publishing, render-status delegation, purge, fleet."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import httpx
from fastapi import HTTPException

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_vision_api  # noqa: E402
import media_edit_api as me  # noqa: E402

KEY = "k" * 32
WORKERS = [
    {"name": "f13-ai", "url": "http://n13", "token": "t13", "physical_node": "f13-ai"},
    {"name": "f2-ai", "url": "http://n2", "token": "t2", "physical_node": "f2-ai"},
    {"name": "worker-4090-ai", "url": "http://n4090", "token": "t4090", "physical_node": "worker-4090-ai"},
]


def run(coro):
    return asyncio.run(coro)


def media_block(capable=True, running=0, pending=0, parallel=2, max_tasks=16, nvenc=True):
    return {"enabled": True, "capable": capable, "parallel": parallel, "running": running, "pending": pending,
            "free_slots": max(0, parallel - running), "max_tasks": max_tasks, "nvenc": nvenc,
            "selftest": {"ok": capable, "seconds": 4.2, "error": ""}}


class FakeRequest:
    def __init__(self, headers):
        self.headers = {k.lower(): v for k, v in headers.items()}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            mock.patch.object(me, "DATA_DIR", root / "data"),
            mock.patch.object(me, "OUTPUT_DIR", root / "out"),
            mock.patch.object(me, "OUTPUT_URL", "https://autorig.online/renderfin/render/media-edit"),
            mock.patch.object(ai_vision_api, "_load_ai_workers", lambda: [dict(w) for w in WORKERS]),
        ]
        for p in self.patches:
            p.start()
        me._RECORDS.clear()
        me._LOCKS.clear()
        me._PROBES.clear()
        me._RECENT_PICKS.clear()
        me._loaded = False

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        me._RECORDS.clear()
        me._PROBES.clear()
        me._RECENT_PICKS.clear()
        self.tmp.cleanup()

    def transport(self, handler):
        """Patch httpx.AsyncClient inside media_edit_api to go through `handler`."""
        real = httpx.AsyncClient

        def factory(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return real(*args, **kwargs)

        patcher = mock.patch.object(me.httpx, "AsyncClient", side_effect=factory)
        patcher.start()
        self.addCleanup(patcher.stop)


# ------------------------------------------------------------------ validation
class ValidationTests(unittest.TestCase):
    def ok(self, **payload):
        body = {"source_url": "https://cdn.example.com/a.mp4", "media_kind": "video"}
        body.update(payload)
        return me.parse_media_edit_request(body)

    def bad(self, message, **payload):
        with self.assertRaises(me.RequestError) as ctx:
            self.ok(**payload)
        self.assertIn(message, str(ctx.exception))

    def test_every_mplus_operation_is_accepted(self):
        self.assertTrue(self.ok(media_kind="image", operation={"hflip": True})["hflip"])
        self.assertTrue(self.ok(media_kind="image", operation={"vflip": True})["vflip"])
        self.assertAlmostEqual(self.ok(media_kind="image", operation={"rotate_rad": 1.5708})["rotate_rad"], 1.5708)
        spec = self.ok(operation={"smartcrop": {"mode": "anchor_camera", "target_aspect": 0.5625,
                                                "anchors": [{"t": 8, "x": 0.7, "y": 0.5}, {"t": 0, "x": 0.2, "y": 0.5}]}})
        self.assertEqual([a[0] for a in spec["smartcrop"]["anchors"]], [0, 8])     # sorted by time
        spec = self.ok(operation={"speed": {"factor": 2.0, "interpolate": True}})
        self.assertEqual((spec["speed"], spec["interpolate"]), (2.0, True))
        spec = self.ok(operation={"cut_frame_at_seconds": 12.5}, output={"container": "jpg"})
        self.assertEqual(spec["cut_frame_at"], 12.5)
        self.assertEqual(spec["container"], "jpg")
        spec = self.ok(media_kind="image", operation={"crop": {"w": 1530, "h": 2720, "x": 0, "y": 0},
                                                       "scale": {"w": 1080, "h": 1920}})
        self.assertEqual(spec["crop"]["w"], 1530)
        self.assertTrue(self.ok(operation={}, output={"keep_audio": True, "container": "mov",
                                                      "codec": "prores"})["keep_audio"])

    def test_contract_violations(self):
        self.bad("media_kind", media_kind="audio")
        self.bad("unknown fields", operation={"blur": 3})
        self.bad("speed is for video", media_kind="image", operation={"speed": {"factor": 2}})
        self.bad("between 0.1 and 8", operation={"speed": {"factor": 20}})
        self.bad("radians", operation={"rotate_rad": 90})
        self.bad("crop.w", operation={"crop": {"h": 10}})
        self.bad("exclude each other", operation={"crop": {"w": 10, "h": 10}, "smartcrop": {}})
        self.bad("0..1", operation={"smartcrop": {"anchors": [{"t": 0, "x": 640, "y": 1}]}})
        self.bad("jpg or png", operation={"cut_frame_at_seconds": 1}, output={"container": "mp4"})
        self.bad("mp4 or mov", output={"container": "jpg"})
        self.bad("ProRes goes into mov", output={"container": "mp4", "codec": "prores"})
        self.bad("keep_audio", output={"keep_audio": "yes"})
        self.bad("hflip", operation={"hflip": "true"})
        with self.assertRaises(me.RequestError):
            me.parse_media_edit_request({"media_kind": "image"})

    def test_source_host(self):
        me.validate_source_host("https://storage.microstock.plus/download/abc?sig=1")
        for url in ("http://cdn.example.com/a.mp4", "https://localhost/a", "https://127.0.0.1/a",
                    "https://10.0.0.5/a", "https://user:pw@cdn.example.com/a", "https://cdn.example.com:8443/a",
                    "file:///etc/passwd"):
            with self.assertRaises(me.RequestError, msg=url):
                me.validate_source_host(url)


# ---------------------------------------------------------------------- access
class AccessTests(unittest.TestCase):
    def test_mplus_key_gives_mplus_tier(self):
        with mock.patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": KEY}):
            tier = me.caller_tier(FakeRequest({"X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus",
                                               "X-Priority-Key": KEY}))
        self.assertEqual(tier, "mplus")

    def test_stranger_without_key_is_refused(self):
        with mock.patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": KEY}):
            for headers in ({"X-Real-IP": "1.2.3.4"},
                            {"X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus", "X-Priority-Key": "x" * 32}):
                with self.assertRaises(HTTPException) as ctx:
                    me.caller_tier(FakeRequest(headers))
                self.assertEqual(ctx.exception.status_code, 403)

    def test_host_itself_and_open_access(self):
        self.assertEqual(me.caller_tier(FakeRequest({})), "autorig")
        with mock.patch.object(me, "OPEN_ACCESS", True):
            self.assertEqual(me.caller_tier(FakeRequest({"X-Real-IP": "1.2.3.4"})), "generation")


# -------------------------------------------------------------------- the nodes
class RankTests(Base):
    def info(self, **kw):
        return me.media_info({"server_version": "v", "media_edit": media_block(**kw), "processing_tasks": []})

    def test_free_slot_and_load_order(self):
        pairs = [(WORKERS[0], self.info(running=2, pending=1)), (WORKERS[1], self.info(running=1)),
                 (WORKERS[2], self.info())]
        order = [ai_vision_api._node_key(w) for w, _ in me.rank_candidates(pairs)]
        self.assertEqual(order, ["worker-4090-ai", "f2-ai", "f13-ai"])

    def test_incapable_and_full_nodes_are_skipped(self):
        pairs = [(WORKERS[0], self.info(capable=False)), (WORKERS[1], self.info(running=2, pending=14)),
                 (WORKERS[2], None)]
        self.assertEqual(me.rank_candidates(pairs), [])

    def test_conversion_node_goes_last(self):
        busy = me.media_info({"media_edit": media_block(), "processing_tasks": [{"workload_class": "glb"}]})
        self.assertEqual(busy["busy_with"], ["conversion"])
        pairs = [(WORKERS[0], busy), (WORKERS[1], self.info(running=1))]
        self.assertEqual(ai_vision_api._node_key(me.rank_candidates(pairs)[0][0]), "f2-ai")

    def test_old_node_without_media_block(self):
        self.assertIsNone(me.media_info({"server_version": "ai-node-20261007"}))

    def test_recent_picks_spread_a_burst(self):
        pairs = [(WORKERS[0], self.info()), (WORKERS[1], self.info())]
        first = ai_vision_api._node_key(me.rank_candidates(pairs)[0][0])
        me._RECENT_PICKS.setdefault(first, me.deque()).extend([time.monotonic()] * 2)
        second = ai_vision_api._node_key(me.rank_candidates(pairs)[0][0])
        self.assertNotEqual(first, second)


class DispatchTests(Base):
    def test_submit_goes_to_best_node_with_tier_and_record(self):
        seen = []

        def handler(request: httpx.Request):
            seen.append((request.method, str(request.url), request.headers.get("authorization")))
            if request.url.path.endswith("/server-status"):
                load = 2 if request.url.host == "n13" else 0
                return httpx.Response(200, json={"server_version": "ai-node-20261107-media-edit",
                                                 "media_edit": media_block(running=load)})
            if request.url.path.endswith("/media-edit"):
                body = json.loads(request.content)
                self.assertEqual(body["tier"], "mplus")
                self.assertEqual(body["operation"], {"hflip": True})
                return httpx.Response(202, json={"task_id": "abc-123"})
            return httpx.Response(404)

        self.transport(handler)
        payload = {"source_url": "https://cdn.example.com/a.jpg", "media_kind": "image",
                   "operation": {"hflip": True}, "client_ref": "mplus:b:i:flip"}
        spec = me.parse_media_edit_request(payload)
        answer = run(me.submit(spec, payload, "mplus"))
        self.assertEqual(answer["status_string"], "queued")
        self.assertTrue(answer["task_id_string"].startswith("me.f2-ai.abc-123")
                        or answer["task_id_string"].startswith("me.worker-4090-ai.abc-123"))
        self.assertEqual(answer["status_url_string"], "/api/ai/render-status/" + answer["task_id_string"])
        self.assertEqual(answer["output_url_string"], "")
        record = json.loads((me.DATA_DIR / "tasks" / (answer["task_id_string"] + ".json")).read_text())
        self.assertEqual(record["client_ref"], "mplus:b:i:flip")
        self.assertEqual(record["source_host"], "cdn.example.com")
        self.assertNotIn("source_url", record)                     # a signed download key is not stored
        posts = [s for s in seen if s[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertNotIn("n13", posts[0][1])                       # the loaded node was not chosen

    def test_busy_node_hands_over_to_the_next(self):
        def handler(request):
            if request.url.path.endswith("/server-status"):
                if request.url.host == "n4090":
                    return httpx.Response(200, json={"server_version": "old"})       # no media block
                return httpx.Response(200, json={"media_edit": media_block()})
            if request.url.host == "n13":
                return httpx.Response(503, json={"error": "media_edit_queue_full", "message": "full"})
            return httpx.Response(202, json={"task_id": "t2"})

        self.transport(handler)
        payload = {"source_url": "https://cdn.example.com/a.mp4", "media_kind": "video", "operation": {}}
        answer = run(me.submit(me.parse_media_edit_request(payload), payload, "autorig"))
        self.assertEqual(answer["node_string"], "f2-ai")

    def test_no_capable_node(self):
        self.transport(lambda request: httpx.Response(200, json={"server_version": "ai-node-20261007"}))
        payload = {"source_url": "https://cdn.example.com/a.mp4", "media_kind": "video", "operation": {}}
        with self.assertRaises(HTTPException) as ctx:
            run(me.submit(me.parse_media_edit_request(payload), payload, "mplus"))
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.detail["error_string"], "no_media_edit_node")

    def test_node_400_is_the_callers_error(self):
        def handler(request):
            if request.url.path.endswith("/server-status"):
                return httpx.Response(200, json={"media_edit": media_block()})
            return httpx.Response(400, json={"error": "invalid_request", "message": "source_url host is not public"})

        self.transport(handler)
        payload = {"source_url": "https://cdn.example.com/a.mp4", "media_kind": "video", "operation": {}}
        with self.assertRaises(HTTPException) as ctx:
            run(me.submit(me.parse_media_edit_request(payload), payload, "mplus"))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("not public", ctx.exception.detail["message_string"])


# --------------------------------------------------------------- status mapping
class StatusMappingTests(unittest.TestCase):
    def record(self, **kw):
        base = {"task_id": "me.f13-ai.x", "node": "f13-ai", "node_task_id": "x", "status": "queued",
                "created_at": 1000.0, "started_at": 0.0, "finished_at": 0.0}
        base.update(kw)
        return base

    def test_states(self):
        rec = self.record()
        self.assertEqual(me.apply_node_status(rec, {"status": "Pending", "queue_position": 3}, 1001), "")
        self.assertEqual((rec["status"], rec["queue_position"]), ("queued", 3))
        me.apply_node_status(rec, {"status": "Processing", "stage": "encoding", "progress": 42.5,
                                   "started_at": 1002.0}, 1003)
        self.assertEqual((rec["status"], rec["stage"], rec["progress"]), ("rendering", "encoding", 42.5))
        self.assertEqual(me.apply_node_status(rec, {"status": "Completed", "output_ready": True,
                                                    "output_meta": {"w": 1080, "h": 1920, "bytes": 5}}, 1004),
                         "publish")
        self.assertEqual((rec["status"], rec["stage"]), ("rendering", "publishing"))
        self.assertEqual(rec["started_at"], 1003)                 # backend clock, not the node's 1002
        rec["finished_at"] = 1300.0
        me.apply_node_status(rec, {"status": "Completed", "output_ready": True, "elapsed_seconds": 4.25,
                                   "output_meta": {"w": 1080, "h": 1920, "bytes": 5}}, 1005)
        self.assertEqual(me.public_status(rec)["render_seconds_float"], 4.2)   # the node's own elapsed time
        out = me.public_status(rec)
        self.assertFalse(out["finished_bool"])
        self.assertEqual(out["output_meta"]["w"], 1080)

    def test_failed_cancelled_and_lost_output(self):
        rec = self.record()
        me.apply_node_status(rec, {"status": "Failed", "error": "ffmpeg_failed: boom", "completed_at": 1005.0})
        out = me.public_status(rec)
        self.assertEqual((out["status_string"], out["success_bool"], out["finished_bool"]), ("failed", False, True))
        self.assertIn("boom", out["error_string"])
        rec = self.record()
        me.apply_node_status(rec, {"status": "Cancelled", "error": "cancelled"})
        self.assertEqual(rec["status"], "cancelled")
        rec = self.record()
        me.apply_node_status(rec, {"status": "Completed", "output_ready": False})
        self.assertTrue(rec["error"].startswith("output_lost"))

    def test_two_hour_limit(self):
        rec = self.record(started_at=1000.0)
        self.assertEqual(me.apply_node_status(rec, {"status": "Processing"}, 1000 + 7200 + 301), "cancel")
        self.assertTrue(rec["error"].startswith("timeout"))

    def test_task_ids(self):
        self.assertEqual(me.parse_task_id("me.f13-ai.0b1c-22"), ("f13-ai", "0b1c-22"))
        for bad in ("f13-ai.x", "me.f13-ai", "me..x", "me.f1/../x.y", "x" * 200):
            self.assertFalse(me.is_media_task_id(bad), bad)


class AdvanceTests(Base):
    def make_record(self):
        record = {"task_id": "me.f13-ai.n1", "node": "f13-ai", "node_task_id": "n1", "status": "rendering",
                  "created_at": time.time() - 5, "started_at": time.time() - 4, "finished_at": 0.0,
                  "last_seen_at": time.time(), "output_meta": {}, "client_ref": "r"}
        me._load_all()
        me._RECORDS[record["task_id"]] = record
        me._save(record)
        return record

    def test_completed_output_is_published_and_released(self):
        self.make_record()
        calls = []
        data = b"\xff\xd8\xff" + b"x" * 5000

        def handler(request):
            calls.append((request.method, request.url.path))
            if request.url.path.endswith("/media-edit/status/n1"):
                return httpx.Response(200, json={"status": "Completed", "progress": 100, "output_ready": True,
                                                 "encoder": "image", "started_at": time.time() - 3,
                                                 "output_meta": {"w": 1920, "h": 1080, "bytes": len(data),
                                                                 "container": "jpg", "duration": 0}})
            if request.url.path.endswith("/media-edit/output/n1"):
                return httpx.Response(200, content=data)
            if request.url.path.endswith("/media-edit/release/n1"):
                return httpx.Response(200, json={"released": True})
            return httpx.Response(404)

        self.transport(handler)
        out = run(me.status_response("me.f13-ai.n1"))
        self.assertEqual(out["status_string"], "completed")
        self.assertTrue(out["output_url_string"].startswith("https://autorig.online/renderfin/render/media-edit/"))
        self.assertTrue(out["output_url_string"].endswith("/result.jpg"))
        rel = out["output_url_string"].split("/media-edit/", 1)[1]
        self.assertEqual((me.OUTPUT_DIR / rel).read_bytes(), data)
        self.assertEqual(out["output_meta"]["bytes"], len(data))
        self.assertIn(("POST", "/api-converter-glb/media-edit/release/n1"), calls)
        # Terminal: a second poll does not talk to the node again.
        before = len(calls)
        self.assertEqual(run(me.status_response("me.f13-ai.n1"))["status_string"], "completed")
        self.assertEqual(len(calls), before)

    def test_truncated_output_is_retried_then_failed(self):
        self.make_record()

        def handler(request):
            if request.url.path.endswith("/status/n1"):
                return httpx.Response(200, json={"status": "Completed", "output_ready": True,
                                                 "output_meta": {"bytes": 100, "container": "mp4"}})
            if request.url.path.endswith("/output/n1"):
                return httpx.Response(200, content=b"short")
            return httpx.Response(200, json={})

        self.transport(handler)
        for _ in range(4):
            self.assertEqual(run(me.status_response("me.f13-ai.n1"))["status_string"], "rendering")
        out = run(me.status_response("me.f13-ai.n1"))
        self.assertEqual(out["status_string"], "failed")
        self.assertIn("publish_failed", out["error_string"])
        self.assertEqual(list(me.OUTPUT_DIR.rglob("*.mp4")), [])

    def test_node_restart_loses_task(self):
        self.make_record()
        self.transport(lambda request: httpx.Response(404, json={"error": "Task not found"}))
        out = run(me.status_response("me.f13-ai.n1"))
        self.assertEqual(out["status_string"], "failed")
        self.assertIn("node_lost", out["error_string"])

    def test_unreachable_node_keeps_waiting_then_fails(self):
        record = self.make_record()

        def handler(request):
            raise httpx.ConnectError("down")

        self.transport(handler)
        self.assertEqual(run(me.status_response("me.f13-ai.n1"))["status_string"], "rendering")
        record["last_seen_at"] = time.time() - me.NODE_LOST_SECONDS - 5
        self.assertEqual(run(me.status_response("me.f13-ai.n1"))["status_string"], "failed")

    def test_records_survive_a_restart(self):
        self.make_record()
        me._RECORDS.clear()
        me._loaded = False
        self.assertEqual(me._record("me.f13-ai.n1")["client_ref"], "r")

    def test_render_status_delegates_media_ids(self):
        self.make_record()
        self.transport(lambda request: httpx.Response(200, json={"status": "Processing", "progress": 30,
                                                                 "stage": "encoding"}))
        out = run(ai_vision_api.api_render_task_status("me.f13-ai.n1"))
        self.assertEqual((out["status_string"], out["progress_percent_float"], out["service_string"]),
                         ("rendering", 30.0, "media_edit"))
        with self.assertRaises(HTTPException):
            run(me.status_response("me.bad"))

    def test_cancel(self):
        self.make_record()
        seen = []

        def handler(request):
            seen.append(request.url.path)
            return httpx.Response(200, json={"status": "Cancelled"})

        self.transport(handler)
        out = run(me.api_media_edit_cancel("me.f13-ai.n1", FakeRequest({})))
        self.assertEqual(out["status_string"], "cancelled")
        self.assertIn("/api-converter-glb/media-edit/cancel/n1", seen)

    def test_purge_after_24h(self):
        record = self.make_record()
        folder = me.OUTPUT_DIR / "20261101" / "abc"
        folder.mkdir(parents=True)
        (folder / "result.mp4").write_bytes(b"1")
        record.update(status="completed", output_path=str(folder / "result.mp4"), output_url="u",
                      finished_at=time.time() - me.KEEP_SECONDS - 10)
        files, records = me.purge()
        self.assertEqual((files, records), (1, 0))
        self.assertFalse(folder.exists())
        self.assertEqual(record["output_url"], "")
        record["finished_at"] = time.time() - me.RECORD_KEEP_SECONDS - 10
        self.assertEqual(me.purge()[1], 1)
        self.assertIsNone(me._record("me.f13-ai.n1"))


class FleetTests(Base):
    def test_fleet_object(self):
        def handler(request):
            if request.url.host == "n2":
                return httpx.Response(500)
            if request.url.host == "n4090":
                return httpx.Response(200, json={"server_version": "ai-node-20261011c"})
            return httpx.Response(200, json={"media_edit": media_block(running=1)})

        async def go():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await me.fleet_object(client)

        out = run(go())
        self.assertEqual(out["capable_nodes_int"], 1)
        self.assertEqual(out["free_slots_int"], 1)
        self.assertTrue(out["nodes_object"]["f13-ai"]["capable"])
        self.assertFalse(out["nodes_object"]["f2-ai"]["capable"])
        self.assertFalse(out["nodes_object"]["worker-4090-ai"]["capable"])
        self.assertTrue(out["nodes_object"]["worker-4090-ai"]["online"])        # old node: online, not capable
        self.assertFalse(out["nodes_object"]["f2-ai"]["online"])


class EndpointTests(Base):
    def test_post_validates_before_dispatch(self):
        class Req(FakeRequest):
            def __init__(self, body):
                super().__init__({})
                self.body = body

            async def json(self):
                return self.body

        with self.assertRaises(HTTPException) as ctx:
            run(me.api_media_edit(Req({"source_url": "http://x.example.com/a", "media_kind": "image"})))
        self.assertEqual(ctx.exception.status_code, 400)
        with mock.patch.object(me, "_disk_free", return_value=0):
            with self.assertRaises(HTTPException) as ctx:
                run(me.api_media_edit(Req({"source_url": "https://x.example.com/a", "media_kind": "image"})))
        self.assertEqual(ctx.exception.detail["error_string"], "storage_low")


if __name__ == "__main__":
    unittest.main()
