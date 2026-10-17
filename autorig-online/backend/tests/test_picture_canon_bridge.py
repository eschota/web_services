"""The picture normalisation bridge (rig contract V3.2 §3): started once per picture, read back for the viewer and the
full generation, the waiting job replaced once and never a job already on a worker."""
import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import picture_canon_bridge as pcb  # noqa: E402

TOKEN = "0a3499b3-8be7-48ad-8b7b-35eaf8d83c8b"


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.up = root / "uploads"
        (self.up / TOKEN).mkdir(parents=True)
        self.mt = root / "mt_root"
        (self.mt / "mt").mkdir(parents=True)
        (self.mt / "mt" / "picture_canon.py").write_text("# stub\n")
        for name, value in (("UPLOAD_ROOT", self.up), ("MT_ROOT", self.mt)):
            p = mock.patch.object(pcb, name, value)
            p.start()
            self.addCleanup(p.stop)
        pcb._STARTED.clear()
        self.addCleanup(self.tmp.cleanup)

    def _view(self, doc):
        d = self.up / TOKEN / "picture"
        d.mkdir(parents=True, exist_ok=True)
        (d / "view.json").write_text(json.dumps(doc))
        return d

    def test_kick_starts_once_and_survives_a_restart(self):
        with mock.patch.object(pcb.subprocess, "Popen") as popen:
            self.assertTrue(pcb.kick(TOKEN, "task-1"))
            self.assertFalse(pcb.kick(TOKEN, "task-1"))
            pcb._STARTED.clear()                              # a backend restart: the marker on disk still holds
            self.assertFalse(pcb.kick(TOKEN, "task-1"))
        self.assertEqual(popen.call_count, 1)
        args = popen.call_args[0][0]
        self.assertIn("mt.picture_canon", args)
        self.assertIn(TOKEN, args)

    def test_kick_skips_a_picture_already_read(self):
        self._view({"status": "ok"})
        with mock.patch.object(pcb.subprocess, "Popen") as popen:
            self.assertFalse(pcb.kick(TOKEN, "task-1"))
        popen.assert_not_called()

    def test_kick_off_switch_and_missing_folder(self):
        with mock.patch.object(pcb.subprocess, "Popen") as popen, \
                mock.patch.dict(os.environ, {"AUTORIG_PICTURE_CANON": "off"}):
            self.assertFalse(pcb.kick(TOKEN, "t"))
        with mock.patch.object(pcb.subprocess, "Popen") as popen2:
            self.assertFalse(pcb.kick("ffffffff-0000-0000-0000-000000000000", "t"))
        popen.assert_not_called()
        popen2.assert_not_called()

    def test_generation_input_only_when_decided(self):
        d = self._view({"status": "working", "generation_input": {"kind": "front", "cutout": "front_cutout.png"}})
        (d / "front_cutout.png").write_bytes(b"png")
        self.assertIsNone(pcb.generation_input(TOKEN, "https://x"))
        self._view({"status": "ok", "generation_input": {"kind": "front", "cutout": "front_cutout.png"}})
        gi = pcb.generation_input(TOKEN, "https://x")
        self.assertEqual(gi["kind"], "front")
        self.assertTrue(gi["url"].startswith(f"https://x/u/{TOKEN}/picture/front_cutout.png?v="))
        self._view({"status": "ok", "generation_input": {"kind": "cutout"}})
        self.assertIsNone(pcb.generation_input(TOKEN, "https://x"))

    def test_summary_for_the_viewer(self):
        d = self._view({"status": "ok", "view": "back", "view_conf": 1.0, "needs_front": True,
                        "characters": {"detected": 1}, "front": {"status": "ok", "picture": "front.png"},
                        "props_result": {"clean": {"status": "ok"},
                                         "props": [{"i": 0, "name": "sack", "hand": "right", "status": "ok",
                                                    "picture": "props/0.png", "generation": {"job": "abc"}}]},
                        "generation_input": {"kind": "front"}})
        for rel in ("front.png", "back.png", "clean.png", "props/0.png"):
            (d / rel).parent.mkdir(parents=True, exist_ok=True)
            (d / rel).write_bytes(b"x")
        s = pcb.summary(TOKEN, "https://x")
        self.assertEqual(s["view"], "back")
        self.assertIn("front.png", s["front"]["url"])
        self.assertIn("back.png", s["front"]["back_url"])
        self.assertEqual(s["props"][0]["name"], "sack")
        self.assertEqual(s["props"][0]["generation"], "abc")
        self.assertIsNone(pcb.summary("ffffffff-0000-0000-0000-000000000000", "https://x"))


class RedispatchTest(unittest.TestCase):
    def _run(self, job, meta, gi):
        import generation_tasks as gt

        task = mock.Mock()
        task.id = "t1"
        task.input_url = f"https://autorig.online/u/{TOKEN}/source.png"
        task.viewer_settings = json.dumps({"generation": dict(meta)})
        db = mock.Mock()
        db.commit = mock.AsyncMock()
        start = mock.AsyncMock(return_value="new-job")
        with mock.patch.object(pcb, "generation_input", return_value=gi), \
                mock.patch("render_prompting.start_character_gen_from_image", start), \
                mock.patch.object(gt, "_discard_job", mock.AsyncMock()) as discard:
            done = asyncio.run(gt._canon_redispatch(db, task, gt.generation_meta(task), job))
        return done, gt.generation_meta(task), start, discard

    def test_waiting_job_is_replaced_once(self):
        gi = {"kind": "front", "url": "https://x/front_cutout.png"}
        done, meta, start, discard = self._run({"stage": "hunyuan"}, {"job_id": "old"}, gi)
        self.assertTrue(done)
        self.assertEqual(meta["job_id"], "new-job")
        self.assertEqual(meta["previous_job_ids"], ["old"])
        self.assertEqual(meta["dispatch_input"], "picture_canon:front")
        discard.assert_awaited_once_with("old")
        done2, _, start2, _ = self._run({"stage": "hunyuan"}, meta, gi)
        self.assertFalse(done2)
        start2.assert_not_awaited()

    def test_job_on_a_worker_is_kept(self):
        gi = {"kind": "clean", "url": "https://x/clean_cutout.png"}
        done, meta, start, discard = self._run({"stage": "hunyuan", "hunyuan_worker": "f13", "hunyuan_task_id": "h1"},
                                               {"job_id": "old"}, gi)
        self.assertFalse(done)
        start.assert_not_awaited()
        self.assertEqual(meta["canon_after_start"], gi["url"])

    def test_nothing_decided_nothing_done(self):
        done, meta, start, _ = self._run({"stage": "hunyuan"}, {"job_id": "old"}, None)
        self.assertFalse(done)
        start.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
