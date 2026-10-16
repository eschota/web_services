"""Picture intake (owner 2026-10-16): the viewer opens with the picture, the picture is isolated, the 3D job starts at
once, the Vision detection runs alongside, every generated character is rigged.

Fakes stand in for renderfin, Vision, the vehicle service and the mask boxes; nothing here touches a network."""
import asyncio
import base64
import json
import pathlib
import sys
import tempfile
import time
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import cv2  # noqa: E402
import numpy as np  # noqa: E402

import generation_tasks as gt  # noqa: E402
import picture_isolate as pi  # noqa: E402
import task_page_v3_routes as routes  # noqa: E402

TOKEN = "0e7e5b1a-1c2d-4e3f-8a9b-0c1d2e3f4a5b"
TASK_ID = "77cc1b88-be21-4d73-85a3-9c0e9551c4d1"
SRC_URL = f"https://autorig.online/u/{TOKEN}/source.png"


class FakeDb:
    def __init__(self):
        self.commits = 0

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        pass


def make_task(stage="isolate", **meta):
    task = SimpleNamespace(
        id=TASK_ID, pipeline_kind="generate", status="created", input_url=SRC_URL, owner_type="user",
        owner_id="someone@example.com", viewer_settings=None, error_message=None, processing_started_at=None,
        updated_at=None, viewer_prepared_glb_url=None, created_at=datetime.utcnow(), output_urls=None,
        ready_urls=None, ready_count=0, total_count=0, worker_api=None, worker_task_id=None, input_type="t_pose",
        source_attempt_count=0, source_next_retry_at=None)
    gt.set_generation_meta(task, stage=stage, **meta)
    return task


def picture(w=160, h=240):
    img = np.full((h, w, 3), 235, np.uint8)
    cv2.rectangle(img, (50, 40), (110, 200), (30, 90, 200), -1)
    ok, png = cv2.imencode(".png", img)
    return img, png.tobytes()


def mask_doc(w, h, box=(50, 40, 110, 200), sam=0.97, pixels=None):
    m = np.zeros((h, w), np.uint8)
    cv2.rectangle(m, (box[0], box[1]), (box[2] - 1, box[3] - 1), 255, -1)
    ok, png = cv2.imencode(".png", m)
    return {"objects": [{"kind": "subject", "mask": "subject_0.png", "pixels": pixels or int((m > 0).sum()),
                         "sam_score": sam, "dino_score": 0.7}],
            "masks": {"subject_0.png": base64.b64encode(png.tobytes()).decode()}, "seconds": {"total": 0.5}}


class Env(unittest.TestCase):
    """A temp upload root with the source picture, and the module-level fakes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        (self.root / TOKEN).mkdir()
        self.img, png = picture()
        (self.root / TOKEN / "source.png").write_bytes(png)
        p = mock.patch.object(pi, "UPLOAD_ROOT", self.root)
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)
        pi._JOBS.clear()
        gt._DETECT.clear()
        gt._STARTING.clear()
        self.started = []
        self.discarded = []
        self.bound = []
        self.routed = []

    def patches(self, *, verdict=None, isolate=None, route_result=False, route_clears=True):
        import content_moderation
        import render_prompting
        import vehicle_intake

        async def start(image_url, source_task_id="", user_name=""):
            self.started.append(image_url)
            return "job-1"

        async def route(db, task, v):
            self.routed.append(v)
            return route_result

        async def discard(job_id):
            self.discarded.append(job_id)

        def detect(_bytes):
            if isinstance(verdict, Exception):
                raise verdict
            return verdict if verdict is not None else {"vision_ok": True, "riggable": False, "subject": "Santa Claus",
                                                        "pose": "other", "is_character": True,
                                                        "reason": "arms hang down"}

        async def iso(src, token):
            if isinstance(isolate, Exception):
                raise isolate
            if isolate is None:
                _, png = picture()
                (self.root / token / "cutout.png").write_bytes(png)
                return {"ok": True, "seconds": 1.2, "box": "raptor"}
            return isolate

        out = [mock.patch.object(render_prompting, "start_character_gen_from_image", start),
               mock.patch.object(vehicle_intake, "maybe_route", route),
               mock.patch.object(content_moderation, "detect_character_for_generation", detect),
               mock.patch.object(pi, "isolate_file", iso),
               mock.patch.object(gt, "_discard_job", discard)]
        for p in out:
            p.start()
            self.addCleanup(p.stop)


class ViewContract(Env):
    def state(self, task):
        return asyncio.run(routes.build_state(task, None, task_model=None, cache_dir=self.root, is_admin=False))

    def test_isolate_stage_gives_the_picture_viewer_and_cutout_null_then_url(self):
        task = make_task("isolate", isolate_started_at=time.time() - 3)
        out = self.state(task)
        self.assertEqual(out["schema"], "autorig.task-page-v3/1")
        self.assertEqual(out["pipeline"], "v3")
        self.assertEqual((out["stage_key"], out["sub_key"], out["status"]), ("generation", "isolate", "processing"))
        self.assertGreaterEqual(out["stage_seconds"], 3)
        self.assertTrue(0.02 <= out["progress"] <= 0.30)
        view = out["viewer"]
        self.assertEqual((view["kind"], view["rigged"], view["page"]), ("picture", False, routes.UNITY_PAGE))
        self.assertEqual(view["params"], {"picture": SRC_URL, "cutout": None, "task": TASK_ID})
        self.assertEqual(view["revision"], "gen:detect")
        self.assertEqual(out["model"], {"state": "generating"})
        (self.root / TOKEN / "cutout.png").write_bytes(b"\x89PNG-ish")
        out = self.state(task)
        self.assertEqual(out["viewer"]["params"]["cutout"], f"https://autorig.online/u/{TOKEN}/cutout.png")

    def test_model_stage_sub_keys_and_revision(self):
        for sub in ("waiting_3d_worker", "hunyuan", "turntable", "ready"):
            task = make_task("model", job_id="job-9", sub_key=sub, sub_started_at=time.time() - 20)
            out = self.state(task)
            self.assertEqual(out["sub_key"], sub)
            self.assertEqual(out["viewer"]["revision"], "gen:job-9")
            self.assertGreaterEqual(out["stage_seconds"], 20)
        self.assertEqual(self.state(make_task("detect"))["sub_key"], "detect")
        self.assertEqual(self.state(make_task("rigging", job_id="j"))["sub_key"], "ready")

    def test_other_rows_keep_their_answers(self):
        done = make_task("done")
        done.status = "done"
        self.assertIsNone(routes._generate_state(done))
        failed = make_task("failed")
        failed.status = "error"
        self.assertIsNone(routes._generate_state(failed))
        rig = make_task("model")
        rig.pipeline_kind = "convert"
        self.assertIsNone(routes._generate_state(rig))


class Dispatch(Env):
    def run_start(self, task):
        db = FakeDb()
        asyncio.run(self._start(db, task))
        return db

    async def _start(self, db, task):
        await gt._start_generation(db, task)
        fut = gt._DETECT.get(task.id)
        if fut is not None:
            await asyncio.wait([fut], timeout=5)        # the alongside detection, let it end inside this loop

    def test_cutout_goes_to_hunyuan_and_the_job_starts_before_the_verdict(self):
        self.patches()
        task = make_task("isolate")
        asyncio.run(self._start_only(FakeDb(), task))
        self.assertEqual(self.started, [f"https://autorig.online/u/{TOKEN}/cutout.png"])
        meta = gt.generation_meta(task)
        self.assertEqual((meta["stage"], meta["job_id"], meta["dispatch_input"]), ("model", "job-1", "cutout"))
        self.assertIs(meta["riggable"], True)                  # always rig
        self.assertNotIn("detect_ok", meta)                    # the verdict had not been collected: it never blocked
        self.assertEqual(task.status, "processing")

    async def _start_only(self, db, task):
        await gt._start_generation(db, task)
        for fut in list(gt._DETECT.values()):
            fut.cancel()

    def test_isolate_failure_keeps_the_original(self):
        for isolate in ({"ok": False, "error": "no mask service answers", "seconds": 0.3}, RuntimeError("boom")):
            self.started.clear()
            self.patches(isolate=isolate)
            task = make_task("isolate")
            asyncio.run(self._start_only(FakeDb(), task))
            self.assertEqual(self.started, [SRC_URL])
            meta = gt.generation_meta(task)
            self.assertEqual((meta["stage"], meta["dispatch_input"]), ("model", "original"))
            self.assertTrue(meta.get("isolate_error"))
            self.assertIsNone(meta.get("cutout_url"))

    def test_riggable_is_always_true_even_when_the_verdict_says_no(self):
        self.patches()
        task = make_task("isolate")
        asyncio.run(self._start(FakeDb(), task))
        asyncio.run(self._harvest(task))
        meta = gt.generation_meta(task)
        self.assertTrue(meta["detect_ok"])
        self.assertEqual((meta["subject"], meta["pose"]), ("Santa Claus", "other"))
        self.assertIs(meta["riggable"], True)                  # verdict riggable False: the hint only
        with mock.patch.dict("os.environ", {"AUTORIG_GENERATION_RIG_ALL": "off"}):
            self.assertFalse(gt.rig_all())

    async def _harvest(self, task, loop_wait=True):
        # the detection ran in the previous asyncio.run loop in `_start`; ask again inside this one
        gt._DETECT.pop(task.id, None)
        gt._start_detect(task)
        await asyncio.wait([gt._DETECT[task.id]], timeout=5)
        return await gt._harvest_detect(FakeDb(), task, gt.generation_meta(task))

    def test_detection_failure_does_not_block_the_dispatch_or_the_binding(self):
        self.patches(verdict=RuntimeError("vision down"))
        task = make_task("isolate")
        asyncio.run(self._start(FakeDb(), task))
        self.assertEqual(self.started, [f"https://autorig.online/u/{TOKEN}/cutout.png"])
        self.assertEqual(gt.generation_meta(task)["stage"], "model")
        import render_prompting
        import v3_intake

        async def poll(job_id):
            return {"stage": "ready", "glb_url": "https://autorig.online/x/model.glb"}

        async def bind(db, t, *, glb_url, receipt_facts):
            self.bound.append((glb_url, receipt_facts))

        async def go():
            db = FakeDb()
            with mock.patch.object(render_prompting, "poll_character_gen", poll), \
                    mock.patch.object(v3_intake, "bind_generated_task", bind), \
                    mock.patch.object(v3_intake, "route_enabled", lambda *a, **k: True), \
                    mock.patch.object(gt, "DETECT_BIND_GRACE_SECONDS", 0.0):
                await gt._advance_generation(db, task, gt.generation_meta(task))
        asyncio.run(go())
        meta = gt.generation_meta(task)
        self.assertFalse(meta.get("detect_ok"))                # vision never answered
        self.assertEqual(len(self.bound), 1)                   # the mesh was rigged anyway
        self.assertEqual(meta["stage"], "rigging")
        self.assertNotEqual(task.status, "done")               # never finished as a bare model

    def test_mesh_waits_for_the_detection_inside_the_grace_then_binds(self):
        self.patches(verdict=RuntimeError("vision down"))
        task = make_task("isolate")
        asyncio.run(self._start(FakeDb(), task))
        import render_prompting
        import v3_intake

        async def poll(job_id):
            return {"stage": "ready", "glb_url": "https://autorig.online/x/model.glb"}

        async def bind(db, t, *, glb_url, receipt_facts):
            self.bound.append(glb_url)

        async def go():
            with mock.patch.object(render_prompting, "poll_character_gen", poll), \
                    mock.patch.object(v3_intake, "bind_generated_task", bind), \
                    mock.patch.object(v3_intake, "route_enabled", lambda *a, **k: True), \
                    mock.patch.object(gt, "DETECT_BIND_GRACE_SECONDS", 3600.0):
                await gt._advance_generation(FakeDb(), task, gt.generation_meta(task))
        asyncio.run(go())
        self.assertEqual(self.bound, [])                       # still inside the grace

    def test_late_vehicle_verdict_routes_to_the_vehicle_conveyor_and_drops_the_job(self):
        verdict = {"vision_ok": True, "riggable": False, "subject": "red pickup truck", "is_character": False,
                   "reason": "a vehicle"}
        self.patches(verdict=verdict)
        import vehicle_intake

        async def route(db, task, v):
            self.routed.append(v)
            task.pipeline_kind = "v3"
            return True
        task = make_task("isolate")
        asyncio.run(self._start(FakeDb(), task))
        with mock.patch.object(vehicle_intake, "maybe_route", route):
            out = asyncio.run(self._harvest(task))
        self.assertEqual(out, "routed")
        self.assertEqual(self.discarded, ["job-1"])
        self.assertEqual(self.routed[-1]["subject"], "red pickup truck")

    def test_vehicle_service_down_keeps_the_character_job_and_retries_the_route(self):
        verdict = {"vision_ok": True, "riggable": False, "subject": "red pickup truck", "is_character": False,
                   "reason": "a vehicle"}
        self.patches(verdict=verdict)
        import vehicle_intake

        async def route(db, task, v):                          # the service refused: it resets the stage to detect
            gt.set_generation_meta(task, stage="detect", detect_last_attempt=time.time())
            return True
        task = make_task("isolate")
        asyncio.run(self._start(FakeDb(), task))
        with mock.patch.object(vehicle_intake, "maybe_route", route):
            out = asyncio.run(self._harvest(task))
        self.assertEqual(out, "continue")
        meta = gt.generation_meta(task)
        self.assertEqual(meta["stage"], "model")               # not a second dispatch
        self.assertTrue(meta["vehicle_verdict"])
        self.assertEqual(self.discarded, [])
        self.assertEqual(len(self.started), 1)
        # an already dispatched task is never dispatched again, whatever stage it is reset to
        gt.set_generation_meta(task, stage="detect")
        asyncio.run(gt._start_generation(FakeDb(), task))
        self.assertEqual(len(self.started), 1)
        self.assertEqual(gt.generation_meta(task)["stage"], "model")

    def test_renderfin_refusal_is_retryable_not_dead(self):
        self.patches()
        import render_prompting

        async def refuse(*a, **k):
            raise RuntimeError("renderfin busy")
        task = make_task("isolate")
        with mock.patch.object(render_prompting, "start_character_gen_from_image", refuse):
            asyncio.run(self._start_only(FakeDb(), task))
        meta = gt.generation_meta(task)
        self.assertEqual(meta["stage"], "isolate")
        self.assertTrue(meta.get("dispatch_last_attempt"))
        self.assertIn("could not start", task.error_message)
        self.assertNotEqual(task.status, "error")


class Isolation(Env):
    def test_cutout_is_cropped_feathered_and_written_atomically(self):
        h, w = self.img.shape[:2]
        doc = mask_doc(w, h)

        async def ask(client, row, body):
            return doc

        async def boxes(client):
            return [{"url": "http://127.0.0.1:1", "box": "raptor", "ok": True, "loaded": True, "gpu": "", "last_s": 1}]
        with mock.patch.object(pi, "_ask", ask), mock.patch.object(pi, "mask_boxes", boxes):
            rep = asyncio.run(pi.isolate_file(self.root / TOKEN / "source.png", TOKEN))
        self.assertTrue(rep["ok"], rep)
        out = cv2.imread(str(pi.cutout_path(TOKEN)), cv2.IMREAD_UNCHANGED)
        self.assertEqual(out.shape[2], 4)
        # bbox 60x160 plus the 4 % margin, never the whole picture
        self.assertLess(out.shape[1], 80)
        self.assertLess(out.shape[0], 180)
        alpha = out[:, :, 3]
        self.assertEqual(int(alpha[out.shape[0] // 2, out.shape[1] // 2]), 255)
        self.assertEqual(int(alpha[0, 0]), 0)
        self.assertTrue(((alpha > 0) & (alpha < 255)).any())   # feathered edge
        self.assertEqual([p.name for p in (self.root / TOKEN).iterdir() if p.name.endswith(".tmp")], [])
        self.assertTrue(json.loads((self.root / TOKEN / "isolate.json").read_text())["ok"])
        self.assertTrue(pi.cutout_ready(TOKEN))

    def test_failures_are_reports_not_exceptions(self):
        async def boxes(client):
            return []
        with mock.patch.object(pi, "mask_boxes", boxes):
            rep = asyncio.run(pi.isolate_file(self.root / TOKEN / "source.png", TOKEN))
        self.assertFalse(rep["ok"])
        self.assertIn("no mask service", rep["error"])
        self.assertFalse(pi.cutout_ready(TOKEN))
        self.assertFalse(json.loads((self.root / TOKEN / "isolate.json").read_text())["ok"])

    def test_text_then_central_box_then_next_box(self):
        h, w = self.img.shape[:2]
        calls = []

        async def ask(client, row, body):
            calls.append((row["box"], "boxes" in body and bool(body["boxes"])))
            if row["box"] == "f1":
                raise RuntimeError("tunnel down")
            if "boxes" in body and body["boxes"]:
                return mask_doc(w, h)
            return {"objects": [], "masks": {}}                # DINO found nothing

        async def boxes(client):
            return [{"url": "u1", "box": "f1", "ok": True, "loaded": True, "gpu": "", "last_s": 1},
                    {"url": "u2", "box": "f2", "ok": True, "loaded": False, "gpu": "", "last_s": 2}]
        with mock.patch.object(pi, "_ask", ask), mock.patch.object(pi, "mask_boxes", boxes):
            rep = asyncio.run(pi.isolate_file(self.root / TOKEN / "source.png", TOKEN))
        self.assertTrue(rep["ok"], rep)
        self.assertEqual(rep["mode"], "central-box")
        self.assertEqual(rep["box"], "f2")
        self.assertEqual(calls[0], ("f1", False))              # the warm box first, then the next one on an error

    def test_pick_refuses_background_specks_and_unsure_masks(self):
        size = (100, 100)
        masks = {"a": "x"}
        for pixels, sam in ((9900, 0.99), (100, 0.99), (4000, 0.2)):
            doc = {"objects": [{"mask": "a", "pixels": pixels, "sam_score": sam}], "masks": masks}
            self.assertIsNone(pi._pick(doc, size))
        doc = {"objects": [{"mask": "a", "pixels": 3000, "sam_score": 0.9}, {"mask": "b", "pixels": 5000, "sam_score": 0.9}],
               "masks": {"a": "x", "b": "y"}}
        self.assertEqual(pi._pick(doc, size)["mask"], "b")

    def test_box_order_warm_first_and_the_owners_pc_last(self):
        rows = [{"url": "a", "box": "worker-4090", "ok": True, "loaded": True, "gpu": "RTX 4090", "last_s": 0.5},
                {"url": "b", "box": "f1", "ok": True, "loaded": False, "gpu": "GTX 1080 Ti", "last_s": 14},
                {"url": "c", "box": "raptor", "ok": True, "loaded": True, "gpu": "RTX 3080 Ti", "last_s": 3},
                {"url": "d", "box": "f13", "ok": False, "loaded": True, "gpu": "", "last_s": None},
                {"url": "e", "box": "f12", "ok": True, "loaded": False, "gpu": "RTX 3080 Ti", "last_s": 8}]
        with mock.patch.dict(pi._SECONDS, clear=True), mock.patch.dict(pi._INFLIGHT, clear=True):
            self.assertEqual([r["box"] for r in pi.order_boxes(rows)], ["raptor", "f12", "f1", "worker-4090"])

    def test_token_of(self):
        self.assertEqual(pi.token_of(SRC_URL), TOKEN)
        self.assertEqual(pi.token_of(f"https://autorig.online/u/{TOKEN}/source.jpg?x=1"), TOKEN)
        self.assertIsNone(pi.token_of("https://example.com/cat.png"))


if __name__ == "__main__":
    unittest.main()
