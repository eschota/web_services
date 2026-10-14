"""V3 FBX intake (2026-10-15): the converters lost /api-converter-glb-to-fbx, so a binary FBX goes to the farm Blender
(v3_fbx_farm), then assimp, then the converter.  The farm queue is faked here; a real binary FBX was converted on f1
(prota.fbx, 29,921 verts, 6.4 s) when this landed."""
import asyncio
import io
import json
import pathlib
import sys
import tempfile
import unittest
import zipfile

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import v3_fbx_farm
import v3_intake

BIN_HEAD = b"Kaydara FBX Binary  \x00\x1a\x00\xe8\x1c\x00\x00" + b"\x00" * 64
OBJ = b"v 0 0 0\nv 1 0 0\nv 0 1 0\nf 1 2 3\n"


def result_zip(report, glb=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("report.json", json.dumps(report))
        if glb is not None:
            z.writestr("model.glb", glb)
    return buf.getvalue()


class FakeQueue:
    def __init__(self, tmp, row, payload=None):
        self.row, self.tmp, self.payload, self.cancelled = row, tmp, payload, []

    def get(self, jid):
        return self.row

    def result_path(self, jid):
        p = pathlib.Path(self.tmp) / "r.zip"
        if self.payload is not None:
            p.write_bytes(self.payload)
        return p

    def cancel(self, jid):
        self.cancelled.append(jid)


class Task:
    def __init__(self, tid="t1"):
        self.id, self.viewer_settings, self.updated_at = tid, json.dumps({"v3": {"state": "normalizing"}}), None


class Db:
    async def commit(self):
        pass


class FbxFarmTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.orig_queue = v3_fbx_farm._queue
        self.orig_start, self.orig_poll = v3_fbx_farm.start, v3_fbx_farm.poll
        self.orig_avail = v3_fbx_farm.available
        v3_fbx_farm.available = lambda: True

    def tearDown(self):
        v3_fbx_farm._queue, v3_fbx_farm.start, v3_fbx_farm.poll = self.orig_queue, self.orig_start, self.orig_poll
        v3_fbx_farm.available = self.orig_avail

    def fbx(self, head=BIN_HEAD):
        p = pathlib.Path(self.tmp) / "m.fbx"
        p.write_bytes(head)
        return p

    def test_binary_and_ascii_fbx_are_told_apart(self):
        self.assertTrue(v3_fbx_farm.is_binary_fbx(self.fbx()))
        self.assertFalse(v3_fbx_farm.is_binary_fbx(self.fbx(b"; FBX 7.3.0 project file\n" + b"x" * 40)))

    def test_poll_states(self):
        glb = v3_intake.obj_to_glb(OBJ)
        cases = [
            ({"state": "queued", "box": None}, None, 10, "pending"),
            ({"state": "queued", "box": None}, None, 10_000, "failed"),
            ({"state": "leased", "box": "f1"}, None, 10, "pending"),
            ({"state": "error", "box": "f1", "error": "exit 3"}, None, 10, "failed"),
            ({"state": "done", "box": "f1"}, result_zip({"ok": True, "code": "ok"}, glb), 10, "done"),
            ({"state": "done", "box": "f2"}, result_zip({"ok": False, "code": "no_mesh"}), 10, "no_mesh"),
            ({"state": "done", "box": "f2"}, result_zip({"ok": False, "code": "import_failed", "error": "x"}), 10, "failed"),
        ]
        for row, payload, age, want in cases:
            v3_fbx_farm._queue = lambda row=row, payload=payload: FakeQueue(self.tmp, row, payload)
            state, _info, data = v3_fbx_farm.poll("j", 0.0, float(age))
            self.assertEqual(state, want, row)
            self.assertEqual(data is not None, want == "done")

    def test_normalize_goes_to_the_farm_then_binds_its_glb(self):
        glb = v3_intake.obj_to_glb(OBJ)
        calls = {"start": 0}
        v3_fbx_farm.start = lambda tid, path: calls.__setitem__("start", calls["start"] + 1) or "job1"
        answers = iter([("pending", {}, None), ("done", {"box": "f1", "job": "job1", "importer": "import_scene.fbx"}, glb)])
        v3_fbx_farm.poll = lambda *a: next(answers)
        task, fbx = Task(), self.fbx()

        async def go():
            with self.assertRaises(v3_intake._Pending):
                await v3_intake._normalize_fbx(Db(), task, "x", fbx)
            with self.assertRaises(v3_intake._Pending):
                await v3_intake._normalize_fbx(Db(), task, "x", fbx)
            return await v3_intake._normalize_fbx(Db(), task, "x", fbx)

        data, norm = asyncio.run(go())
        self.assertEqual(calls["start"], 1)
        self.assertTrue(norm["method"].startswith("blender-farm"))
        self.assertEqual(norm["box"], "f1")
        self.assertEqual(v3_intake.glb_facts(data)["meshes"] if "meshes" in v3_intake.glb_facts(data) else 1, 1)
        self.assertNotIn("normalize_job", json.loads(task.viewer_settings)["v3"])

    def test_skeleton_only_fbx_is_a_final_no_mesh_verdict(self):
        v3_fbx_farm.start = lambda tid, path: "job2"
        v3_fbx_farm.poll = lambda *a: ("no_mesh", {"code": "no_mesh"}, None)
        task, fbx = Task("t2"), self.fbx()
        task.viewer_settings = json.dumps({"v3": {"normalize_job": "job2", "normalize_job_at": 1.0}})

        async def go():
            await v3_intake._normalize_fbx(Db(), task, "x", fbx)

        with self.assertRaises(v3_intake.V3IntakeError) as ctx:
            asyncio.run(go())
        self.assertEqual(ctx.exception.code, "no_mesh")

    def test_unreadable_fbx_is_final_once_blender_and_assimp_both_fail(self):
        import v3_textures
        orig = v3_textures.normalize_textured
        v3_textures.normalize_textured = lambda path: (None, {"assimp_error": "FBX-Parser expected compound scope"})
        try:
            v3_fbx_farm.poll = lambda *a: ("failed", {"code": "import_failed", "error": "bad file"}, None)
            task, fbx = Task("t3"), self.fbx()
            task.viewer_settings = json.dumps({"v3": {"normalize_job": "job3", "normalize_job_at": 1.0}})
            with self.assertRaises(v3_intake.V3IntakeError) as ctx:
                asyncio.run(v3_intake._normalize_fbx(Db(), task, "x", fbx))
        finally:
            v3_textures.normalize_textured = orig
        self.assertEqual(ctx.exception.code, "invalid_fbx")

    def test_converter_without_the_route_is_not_waited_for(self):
        import workers

        class R:
            success, output_url, error, model_name = False, None, "Worker returned HTTP 404: <!doctype html>", None

        async def configured(db):
            return ["https://converter-f1.example/api-converter-glb"]

        async def send(worker, url):
            return R()

        orig = workers.get_configured_workers, workers.send_fbx_to_glb
        workers.get_configured_workers, workers.send_fbx_to_glb = configured, send
        try:
            with self.assertRaises(v3_intake.V3IntakeError) as ctx:
                asyncio.run(v3_intake._normalize_with_converter(Db(), "x"))
        finally:
            workers.get_configured_workers, workers.send_fbx_to_glb = orig
        self.assertEqual(ctx.exception.code, "converter_route_missing")


if __name__ == "__main__":
    unittest.main()
