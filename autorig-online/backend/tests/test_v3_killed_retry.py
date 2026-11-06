"""V3 · killed runs: a failed attempt whose child was killed from outside is retried by itself, twice at most."""
import unittest
from types import SimpleNamespace

import v3_runtime_mount as m


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


if __name__ == "__main__":
    unittest.main()
