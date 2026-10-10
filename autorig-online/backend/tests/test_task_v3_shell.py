import pathlib
import sys
import unittest
from types import SimpleNamespace


BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from task_v3_shell import TaskV3BindingError, resolve_task_v3_shell  # noqa: E402


TASK = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"
SOURCE = "a" * 64


class TaskV3ShellTests(unittest.TestCase):
    def task(self, status="processing", progress=35):
        return SimpleNamespace(id=TASK, status=status, progress=progress)

    def publication(self, run_id):
        return {"schema": "autorig.v3.viewer-publication/1", "verified": True,
                "task_id": TASK, "source_sha256": SOURCE, "run_id": run_id,
                "serving_artifact_sha256": "f" * 64, "receipt_id": "publication:test"}

    def test_missing_binding_reports_real_queue_without_starting_any_run(self):
        result = resolve_task_v3_shell(self.task(), None).public()
        self.assertEqual(result["viewer_state"], "awaiting_binding")
        self.assertEqual(result["progress"], .35)
        self.assertIsNone(result["viewer_url"])
        self.assertNotIn("run_id", result)
        self.assertNotIn("source_sha256", result)

    def test_authoritative_legacy_mt_binding_is_the_only_current_viewer_url(self):
        binding = {"task_id": TASK, "source_sha256": SOURCE, "run_id": "b" * 20, "persisted": True,
                   "viewer_publication": self.publication("b" * 20)}
        remote = {"run_id": "b" * 20, "task_id": TASK, "source_sha256": SOURCE,
                  "status": "running", "stage": "rig", "progress": .6}
        result = resolve_task_v3_shell(self.task(), binding, remote).public()
        self.assertEqual(result["viewer_url"], "/api/mt/unity/test/index.html?run=" + "b" * 20)
        self.assertEqual((result["status"], result["stage"], result["progress"]), ("running", "rig", .6))

    def test_binding_requires_source_persistence_and_serving_publication_receipt(self):
        base = {"task_id": TASK, "run_id": "b" * 20, "persisted": True}
        with self.assertRaisesRegex(TaskV3BindingError, "source_sha256"):
            resolve_task_v3_shell(self.task(), base)
        base["source_sha256"] = SOURCE
        base["persisted"] = None
        with self.assertRaisesRegex(TaskV3BindingError, "not persisted"):
            resolve_task_v3_shell(self.task(), base)
        base["persisted"] = True
        remote = {"run_id": "b" * 20, "task_id": TASK, "source_sha256": SOURCE,
                  "status": "running", "stage": "rig", "progress": .6}
        result = resolve_task_v3_shell(self.task(), base, remote).public()
        self.assertEqual(result["viewer_state"], "awaiting_viewer_publication")
        self.assertIsNone(result["viewer_url"])

    def test_dispatch_run_waits_for_publication_instead_of_rewriting_identity(self):
        run_id = "v3run-" + "c" * 32
        binding = {"task_id": TASK, "source_sha256": SOURCE, "run_id": run_id, "persisted": True}
        remote = {"run_id": run_id, "task_id": TASK, "source_sha256": SOURCE,
                  "status": "running", "stage": "qa", "progress": .8}
        result = resolve_task_v3_shell(self.task(), binding, remote).public()
        self.assertEqual(result["viewer_state"], "awaiting_viewer_publication")
        self.assertIsNone(result["viewer_url"])

    def test_missing_status_identity_waits_for_verification(self):
        binding = {"task_id": TASK, "source_sha256": SOURCE, "run_id": "b" * 20, "persisted": True,
                   "viewer_publication": self.publication("b" * 20)}
        for remote in (None, {"run_id": "b" * 20},
                       {"run_id": "b" * 20, "task_id": TASK}):
            with self.subTest(remote=remote):
                result = resolve_task_v3_shell(self.task(), binding, remote).public()
                self.assertEqual(result["viewer_state"], "awaiting_verification")
                self.assertNotEqual(result["status"], "done")
                self.assertIsNone(result["viewer_url"])

    def test_raw_done_without_independent_source_bound_qa_is_needs_review(self):
        binding = {"task_id": TASK, "source_sha256": SOURCE, "run_id": "b" * 20, "persisted": True,
                   "viewer_publication": self.publication("b" * 20)}
        remote = {"run_id": "b" * 20, "task_id": TASK, "source_sha256": SOURCE,
                  "status": "done", "stage": "publish", "progress": 1}
        result = resolve_task_v3_shell(self.task("done", 100), binding, remote).public()
        self.assertEqual((result["status"], result["stage"]), ("needs_review", "qa_verification"))
        self.assertLess(result["progress"], 1)
        self.assertEqual(result["viewer_state"], "ready")
        remote["qa"] = {"status": "accepted", "source_sha256": SOURCE,
                        "report_sha256": "e" * 64, "verifier_receipt_id": "qa:test"}
        accepted = resolve_task_v3_shell(self.task("done", 100), binding, remote).public()
        self.assertEqual((accepted["status"], accepted["progress"]), ("done", 1))

    def test_identity_mismatches_fail_closed(self):
        binding = {"task_id": "23b2c690-cbec-44ae-989b-9188e95692c8",
                   "source_sha256": SOURCE, "run_id": "d" * 20, "persisted": True}
        with self.assertRaisesRegex(TaskV3BindingError, "task_id mismatch"):
            resolve_task_v3_shell(self.task(), binding)
        binding["task_id"] = TASK
        with self.assertRaisesRegex(TaskV3BindingError, "run status identity"):
            resolve_task_v3_shell(self.task(), binding, {
                "run_id": "e" * 20, "task_id": TASK, "source_sha256": SOURCE,
            })

    def test_failed_task_is_not_presented_as_success(self):
        result = resolve_task_v3_shell(self.task("error", 72), None).public()
        self.assertEqual((result["status"], result["viewer_state"]), ("failed", "failed"))
        self.assertIsNone(result["viewer_url"])


if __name__ == "__main__":
    unittest.main()
