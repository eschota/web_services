"""Regression tests for bounded collection-member retry admission."""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collection_retry_policy import collection_error_retry_due


class CollectionRetryPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 8, 23, 4, 0, 0)
        self.base = {
            "status": "error",
            "collection_guid": "11111111-2222-3333-4444-555566667777",
            "restart_count": 0,
            "updated_at": self.now - timedelta(minutes=3),
            "now": self.now,
            "max_retries": 3,
            "retry_delay_minutes": 2,
        }

    def test_terminal_collection_error_is_retried_after_delay(self):
        self.assertTrue(collection_error_retry_due(**self.base))

    def test_capacity_wait_is_not_a_retry_attempt(self):
        self.assertFalse(collection_error_retry_due(**{**self.base, "status": "created"}))

    def test_standalone_error_is_untouched(self):
        self.assertFalse(collection_error_retry_due(**{**self.base, "collection_guid": None}))

    def test_retry_budget_is_bounded(self):
        self.assertFalse(collection_error_retry_due(**{**self.base, "restart_count": 3}))

    def test_retry_delay_prevents_hot_loop(self):
        self.assertFalse(
            collection_error_retry_due(
                **{**self.base, "updated_at": self.now - timedelta(seconds=30)}
            )
        )


if __name__ == "__main__":
    unittest.main()
