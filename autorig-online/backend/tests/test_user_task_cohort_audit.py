"""Pure observer checks: no production connection, task creation or HTTP calls."""
import importlib.util
import sys
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[2] / "deploy" / "healthcheck" / "user_task_cohort_audit.py"
spec = importlib.util.spec_from_file_location("cohort_observer", MODULE)
observer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = observer
spec.loader.exec_module(observer)


class RealUserObserverTests(unittest.TestCase):
    def test_foreground_collection_metadata_is_not_excluded(self):
        row = {"owner_type": "user", "owner_id": "customer@example.com", "queue_class": "interactive", "collection_guid": "collection", "input_url": "https://autorig.online/u/customer/model.glb"}
        self.assertIsNone(observer.excluded_reason(row, set(), set()))
        row["queue_class"] = "collection_background"
        self.assertEqual(observer.excluded_reason(row, set(), set()), "automatic_background_collection")
        row["queue_class"] = "interactive"
        self.assertEqual(observer.excluded_reason(row, {"customer@example.com"}, set()), "administrator")

    def test_download_event_redacts_identifiers_and_excludes_audit_requests(self):
        task_id = "11111111-1111-1111-1111-111111111111"
        state = {"salt": "secret", "started_at": "2026-10-05T11:00:00+00:00", "members": {task_id: {}}}
        line = f'192.0.2.1 - - [05/Oct/2026:11:02:00 +0000] "GET /api/task/{task_id}/bundle.zip?token=secret HTTP/2.0" 200 12345 "https://autorig.online/task?id={task_id}" "Mozilla/5.0 (Windows NT 10.0) Chrome/140"'
        event = observer.parse_access(line, state)
        self.assertEqual(event["kind"], "bundle_response")
        self.assertEqual(event["response_bytes"], 12345)
        self.assertNotIn("192.0.2.1", str(event))
        self.assertNotIn("token", str(event))
        self.assertIsNone(observer.parse_access(line.replace('Mozilla/5.0 (Windows NT 10.0) Chrome/140', 'AutoRigAudit/1.0'), state))

    def test_terminal_twenty_still_require_followup(self):
        state = {"target": 20, "started_at": "2026-10-05T11:00:00+00:00", "followup_hours": 2, "limitations": {}, "members": {}}
        for i in range(20):
            state["members"][str(i)] = {"current": {"status": "done", "participant": "user-a"}, "terminal_first_observed_at": "2026-10-05T12:00:00+00:00", "http_events": []}
        self.assertFalse(observer.make_summary(state, "2026-10-05T13:00:00+00:00")["final_report_ready"])
        self.assertTrue(observer.make_summary(state, "2026-10-05T14:00:00+00:00")["final_report_ready"])
        state["members"]["0"]["current"]["status"] = "processing"
        self.assertFalse(observer.make_summary(state, "2026-10-06T14:00:00+00:00")["final_report_ready"])


if __name__ == "__main__":
    unittest.main()
