"""Conversion funnel log and the backfill digest (Downloads · V3, 2026-10-12).

Run from backend/: python -m unittest tests.test_funnel
"""
import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import email_service  # noqa: E402
import funnel  # noqa: E402


class FunnelTests(unittest.TestCase):
    def test_events_are_counted_by_source_and_unknown_ones_refused(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(funnel, "FUNNEL_DIR", Path(tmp)):
            self.assertTrue(funnel.record("task_created", source="Google.com", visitor="a"))
            self.assertTrue(funnel.record("task_created", source="google.com", visitor="b"))
            self.assertTrue(funnel.record("paywall_view", source="", visitor="a"))
            self.assertFalse(funnel.record("drop_tables", source="x"))
            day = next(iter(funnel.page_events(1).values()))
            self.assertEqual(day["google.com"]["task_created"], 2)
            self.assertEqual(day["google.com"]["task_created_visitors"], 2)
            self.assertEqual(day["direct"]["paywall_view"], 1)


class DigestTests(unittest.TestCase):
    def test_one_email_per_owner_and_tasks_never_twice(self):
        reserved, sent = set(), []

        async def reserve(task_id, to_email):
            if task_id in reserved:
                return False
            reserved.add(task_id)
            return True

        async def finish(*_a, **_k):
            return None

        tasks = [("11111111-2222-4333-8444-555555555555", "Knight"), ("21111111-2222-4333-8444-555555555555", None)]
        with mock.patch.object(email_service, "RESEND_API_KEY", "test"), \
                mock.patch.object(email_service, "_reserve_task_completion_email", reserve), \
                mock.patch.object(email_service, "_finish_task_completion_email", finish), \
                mock.patch.object(email_service.resend.Emails, "send",
                                  lambda params, opts=None: sent.append(params) or {"id": "x"}):
            first = asyncio.run(email_service.send_task_digest_email("o@example.com", tasks, lang="ru"))
            second = asyncio.run(email_service.send_task_digest_email("o@example.com", tasks, lang="ru"))
        self.assertTrue(first["sent"])
        self.assertEqual(len(first["tasks"]), 2)
        self.assertFalse(second["sent"])
        self.assertEqual(len(sent), 1)
        self.assertIn("/email/click?token=", sent[0]["html"])


if __name__ == "__main__":
    unittest.main()
