"""Regression tests for converter task requeue timeout epochs."""

import unittest
from datetime import datetime, timedelta
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from task_timeout_contract import task_hard_timeout_reference


class TaskTimeoutContractTests(unittest.TestCase):
    def test_created_task_waiting_for_worker_has_no_hard_timeout(self):
        now = datetime(2026, 7, 10, 12, 0, 0)
        self.assertIsNone(
            task_hard_timeout_reference(
                status="created",
                created_at=now - timedelta(hours=3),
                processing_started_at=None,
                updated_at=now,
                last_progress_at=None,
            ),
        )

    def test_old_requeued_created_task_also_waits_for_dispatch(self):
        now = datetime(2026, 7, 10, 12, 0, 0)
        self.assertIsNone(
            task_hard_timeout_reference(
                status="created",
                created_at=now - timedelta(days=2),
                processing_started_at=None,
                updated_at=now - timedelta(hours=3),
                last_progress_at=None,
            ),
        )

    def test_processing_poll_update_does_not_hide_stale_progress(self):
        now = datetime(2026, 7, 10, 12, 0, 0)
        last_progress = now - timedelta(minutes=30)
        self.assertEqual(
            last_progress,
            task_hard_timeout_reference(
                status="processing",
                created_at=now - timedelta(hours=1),
                processing_started_at=now - timedelta(minutes=35),
                updated_at=now,
                last_progress_at=last_progress,
            ),
        )

    def test_processing_without_progress_uses_creation_epoch(self):
        now = datetime(2026, 7, 10, 12, 0, 0)
        created = now - timedelta(minutes=121)
        self.assertEqual(
            created,
            task_hard_timeout_reference(
                status="processing",
                created_at=created,
                processing_started_at=None,
                updated_at=now,
                last_progress_at=None,
            ),
        )

    def test_old_queued_task_gets_fresh_dispatch_epoch(self):
        now = datetime(2026, 7, 10, 12, 0, 0)
        processing_started = now - timedelta(minutes=5)
        self.assertEqual(
            processing_started,
            task_hard_timeout_reference(
                status="processing",
                created_at=now - timedelta(hours=4),
                processing_started_at=processing_started,
                updated_at=now,
                last_progress_at=None,
            ),
        )


if __name__ == "__main__":
    unittest.main()
