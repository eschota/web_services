"""V3 · killed runs: a failed attempt whose child was killed from outside is retried by itself, twice at most."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest import mock

import v3_runtime_mount as m
from v3_task_runtime import task_status_patch


def rec(error, state="failed"):
    return SimpleNamespace(state=state, error=error)


class KilledRetryDueTests(unittest.TestCase):
    def test_killed_errors_are_retried(self):
        for err in ("RuntimeError: mt.project --glb failed: ",
                    "RuntimeError: mt.project --glb failed:",
                    "ChildKilled: mt.project --glb killed by signal 15 (SIGTERM)",
                    "RuntimeError: mt.fast_analysis exited with code -9"):
            self.assertTrue(m.killed_retry_due(rec(err), {}), err)

    def test_real_failures_are_not(self):
        for err in ("RuntimeError: mt.project --glb failed: Traceback (most recent call last): ValueError: no mesh",
                    "RuntimeError: the fast rig produced no skeleton", "", "DispatchConflict: accepted rig output lacks required roles"):
            self.assertFalse(m.killed_retry_due(rec(err), {}), err)

    def test_at_most_two_automatic_retries(self):
        err = "RuntimeError: mt.project --glb failed: "
        self.assertTrue(m.killed_retry_due(rec(err), {"auto_retries": 1}))
        self.assertFalse(m.killed_retry_due(rec(err), {"auto_retries": 2}))

    def test_motion_transfer_already_retried_in_process(self):
        err = "RuntimeError: [killed; auto-retries exhausted 2/2] ChildKilled: mt.project --glb killed by signal 15 (SIGTERM)"
        self.assertFalse(m.killed_retry_due(rec(err), {}))

    def test_only_failed_attempts(self):
        self.assertFalse(m.killed_retry_due(rec("mt.project failed: ", state="needs_review"), {}))
        self.assertFalse(m.killed_retry_due(rec("mt.project failed: ", state="running"), {}))


class FakeSession:
    def __init__(self, task):
        self.task = task

    async def get(self, _model, _id):
        return self.task


def project(error, auto_retries=0):
    task = SimpleNamespace(id="t1", pipeline_kind="v3", status="processing", error_message=None, output_urls=None,
                           ready_urls=None, total_count=0, ready_count=0, processing_started_at=None,
                           last_progress_at=None, updated_at=None, viewer_prepared_glb_url=None,
                           viewer_settings=json.dumps({"v3": {"attempt": 1, "auto_retries": auto_retries}}))
    record = SimpleNamespace(state="failed", error=error, remote_status={}, remote_stage="failed:analysis", progress=.2,
                             run_id="v3run-x")
    binding = SimpleNamespace(task_id="t1", attempt=1, requested_intent="rig", dispatch_intent="rig",
                              pipeline_revision="r", source_sha256="s")
    asyncio.run(m.project_task(FakeSession(task), binding, task_status_patch(record), record))
    return task, json.loads(task.viewer_settings)["v3"]


class ProjectionTests(unittest.TestCase):
    def test_killed_failure_is_not_an_error_for_the_customer(self):
        task, v3 = project("RuntimeError: mt.project --glb failed: ")
        self.assertEqual(task.status, "processing")
        self.assertIsNone(task.error_message)
        self.assertEqual((v3["state"], v3["auto_retries"], v3["error"]), ("retrying", 1, None))
        self.assertIn("mt.project", v3["last_killed"]["error"])

    def test_real_failure_and_spent_retries_stay_errors(self):
        task, v3 = project("RuntimeError: the fast rig produced no skeleton")
        self.assertEqual((task.status, v3["state"]), ("error", "failed"))
        task, v3 = project("RuntimeError: mt.project --glb failed: ", auto_retries=2)
        self.assertEqual((task.status, v3["state"]), ("error", "failed"))
        self.assertIn("mt.project", task.error_message)


if __name__ == "__main__":
    unittest.main()
