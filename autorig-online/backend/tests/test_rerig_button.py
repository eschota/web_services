"""Re-rig button (owner 2026-10-19): the task page's «re-rig with the new pipeline» request.

* `new_pipeline` is true when the server's rig version differs from the rig's, or when the rig-path sha differs under
  the same version (an install without a bump: the Santa 718a99f3 case, rig sha 3eeb574b5d25 vs 28fbaf20c534).
* The button writes one request per (task, version, sha), O_EXCL; a second press answers the same request; a failed
  request is asked again; a rig built by the current pipeline gets `up_to_date`.
* An admin and the owner may press it; another signed-in person may not; an anonymous task's visitor may.
"""
from __future__ import annotations

import json
import os
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

HERE = Path(__file__).resolve().parent
BACKEND = HERE.parent if HERE.name == "tests" else HERE
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import rerig_on_open as RO  # noqa: E402

RIG_VERSION_PY = '''"""test"""
RIG_VERSION = "v3.test.1"

RIG_PATH = ("fastrig.py", "rig_first.py")
'''


class _Task:
    def __init__(self, tid, status="done", owner_type="user", owner_id="owner@example.com", pipeline_kind="v3", run=None):
        self.id = tid
        self.status = status
        self.owner_type = owner_type
        self.owner_id = owner_id
        self.pipeline_kind = pipeline_kind
        self.viewer_settings = json.dumps({"v3": {"session": {"mt_run_id": run}}}) if run else "{}"


class _Req:
    client = types.SimpleNamespace(host="203.0.113.7")
    headers = {"user-agent": "Mozilla/5.0 (test)"}


class RerigButton(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        root = Path(self.tmp.name)
        (root / "mt").mkdir()
        (root / "mt" / "rig_version.py").write_text(RIG_VERSION_PY, encoding="utf-8")
        (root / "mt" / "fastrig.py").write_text("# fit v1\n", encoding="utf-8")
        (root / "mt" / "rig_first.py").write_text("# first v1\n", encoding="utf-8")
        self.root = root
        self._saved = (RO.MT_ROOT, RO.REQ_DIR)
        RO.MT_ROOT, RO.REQ_DIR = root, root / "rerig_requests"
        RO._cache.update(ver=None, mtime=None)
        RO._sha_cache.update(sha=None, key=None, at=0.0)
        RO._task_cache.clear()
        RO._task_sha.clear()
        RO._ip_hits.clear()
        self.run = "a" * 20
        (root / "runs" / self.run / "rig").mkdir(parents=True)
        self.sha = RO.current_sha()
        self.assertTrue(self.sha and len(self.sha) == 12)

    def tearDown(self):
        RO.MT_ROOT, RO.REQ_DIR = self._saved
        self.tmp.cleanup()

    def _rig(self, version="v3.test.1", sha=None):
        doc = {"rig_version": version, "rig_path_sha": sha or self.sha, "source": "v3-rerig"}
        (self.root / "runs" / self.run / "rig" / "rig.json").write_text(json.dumps(doc), encoding="utf-8")
        RO._task_cache.clear()

    def test_up_to_date_rig_shows_no_button(self):
        self._rig()
        t = _Task("11111111-1111-4111-8111-111111111111", run=self.run)
        st = RO.state(t, _Req(), trigger=False)
        self.assertFalse(st["new_pipeline"])
        self.assertEqual(st["button"]["state"], "none")
        out = RO.request_button(t, _Req(), user_email="owner@example.com", is_admin=False)
        self.assertEqual(out["button"]["state"], "up_to_date")
        self.assertFalse(list((self.root / "rerig_requests").glob("*.json")) if (self.root / "rerig_requests").is_dir() else [])

    def test_same_version_other_sha_is_a_new_pipeline(self):
        self._rig(sha="3eeb574b5d25")
        t = _Task("22222222-2222-4222-8222-222222222222", run=self.run)
        st = RO.state(t, _Req(), trigger=False)
        self.assertTrue(st["current_version"])
        self.assertTrue(st["new_pipeline"])
        self.assertEqual(st["task_sha"], "3eeb574b5d25")
        self.assertEqual(st["current_sha"], self.sha)

    def test_owner_press_writes_one_request(self):
        self._rig(sha="3eeb574b5d25")
        t = _Task("33333333-3333-4333-8333-333333333333", run=self.run)
        out = RO.request_button(t, _Req(), user_email="Owner@example.com", is_admin=False)
        self.assertEqual(out["button"]["state"], "queued")
        p = RO.button_path(t.id, "v3.test.1", self.sha)
        doc = json.loads(p.read_text(encoding="utf-8"))
        self.assertEqual((doc["task"], doc["state"], doc["by"], doc["run"]), (t.id, "queued", "button", self.run))
        self.assertEqual(doc["rig_path_sha"], self.sha)
        self.assertEqual(doc["from_rig_path_sha"], "3eeb574b5d25")
        again = RO.request_button(t, _Req(), user_email="owner@example.com", is_admin=False)   # the same request
        self.assertEqual(again["button"]["state"], "queued")
        self.assertEqual(len(list((self.root / "rerig_requests").glob("*.json"))), 1)
        st = RO.state(t, _Req(), trigger=False)          # the page sees the request's state
        self.assertEqual(st["button"]["state"], "queued")

    def test_failed_request_is_asked_again(self):
        self._rig(sha="3eeb574b5d25")
        t = _Task("44444444-4444-4444-8444-444444444444", run=self.run)
        p = RO.button_path(t.id, "v3.test.1", self.sha)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"state": "failed", "why": "test"}), encoding="utf-8")
        out = RO.request_button(t, _Req(), user_email="owner@example.com", is_admin=False)
        self.assertEqual(out["button"]["state"], "queued")
        self.assertEqual(json.loads(p.read_text(encoding="utf-8"))["state"], "queued")

    def test_a_stranger_is_forbidden_but_anon_and_admin_pass(self):
        self._rig(sha="3eeb574b5d25")
        t = _Task("55555555-5555-4555-8555-555555555555", run=self.run)
        self.assertEqual(RO.request_button(t, _Req(), user_email="other@example.com", is_admin=False)["button"]["state"],
                         "forbidden")
        self.assertEqual(RO.request_button(t, _Req(), user_email=None, is_admin=False)["button"]["state"], "forbidden")
        self.assertEqual(RO.request_button(t, _Req(), user_email="other@example.com", is_admin=True)["button"]["state"],
                         "queued")
        anon = _Task("66666666-6666-4666-8666-666666666666", owner_type="anon", owner_id="anon:x", run=self.run)
        self.assertEqual(RO.request_button(anon, _Req(), user_email=None, is_admin=False)["button"]["state"], "queued")

    def test_processing_and_vehicles_wait(self):
        self._rig(sha="3eeb574b5d25")
        t = _Task("77777777-7777-4777-8777-777777777777", status="processing", run=self.run)
        self.assertEqual(RO.request_button(t, _Req(), user_email="owner@example.com", is_admin=True)["button"]["state"],
                         "not_now")
        (self.root / "runs" / self.run / "run.json").write_text(json.dumps({"kind": "vehicle_v3"}), encoding="utf-8")
        t = _Task("88888888-8888-4888-8888-888888888888", run=self.run)
        self.assertEqual(RO.request_button(t, _Req(), user_email="owner@example.com", is_admin=True)["button"]["state"],
                         "not_for_vehicles")

    def test_sha_follows_an_install(self):
        a = RO.current_sha()
        (self.root / "mt" / "fastrig.py").write_text("# fit v2\n", encoding="utf-8")
        os.utime(self.root / "mt" / "fastrig.py", ns=(2_000_000_000_000_000_000, 2_000_000_000_000_000_000))
        RO._sha_cache["at"] = 0.0
        b = RO.current_sha()
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
