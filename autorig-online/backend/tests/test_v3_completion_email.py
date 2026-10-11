"""V3 completion -> exactly one «model is ready» email, with the classic guards (Downloads · V3, 2026-10-11).

Run from backend/: python -m unittest tests.test_v3_completion_email
"""
import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import database  # noqa: E402
import email_service  # noqa: E402
import v3_notify  # noqa: E402


class FakeDb:
    def __init__(self, task, owner):
        self.task, self.owner = task, owner

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, model, key):
        return self.task if self.task is not None and key == self.task.id else None

    async def execute(self, *_a, **_k):
        owner = self.owner
        return types.SimpleNamespace(scalar_one_or_none=lambda: owner)


class CompletionEmailTests(unittest.TestCase):
    def run_case(self, *, owner_type="user", opted_in=True, invalid=False, calls=2, lang=None):
        task = types.SimpleNamespace(id="11111111-2222-4333-8444-555555555555", owner_type=owner_type,
                                     owner_id="owner@example.com" if owner_type == "user" else "anon-1")
        owner = types.SimpleNamespace(email="owner@example.com", email_task_completed=opted_in,
                                      email_invalid_at="2026-10-10" if invalid else None,
                                      preferred_language=lang, detected_language=None)
        reserved = set()

        async def reserve(task_id, to_email):
            if task_id in reserved:
                return False
            reserved.add(task_id)
            return True

        async def finish(*_a, **_k):
            return None

        async def no_image(_url):
            return None

        sent = []
        with mock.patch.object(database, "AsyncSessionLocal", lambda: FakeDb(task, owner)), \
                mock.patch.object(email_service, "RESEND_API_KEY", "test"), \
                mock.patch.object(email_service, "_reserve_task_completion_email", reserve), \
                mock.patch.object(email_service, "_finish_task_completion_email", finish), \
                mock.patch.object(email_service, "download_image", no_image), \
                mock.patch.object(email_service.resend.Emails, "send",
                                  lambda params, opts=None: sent.append(params) or {"id": "x"}):
            results = [asyncio.run(v3_notify.completion_email(task.id, poster_wait=0)) for _ in range(calls)]
        return results, sent

    def test_v3_completion_sends_exactly_one_email(self):
        results, sent = self.run_case()
        self.assertEqual(len(sent), 1)
        self.assertEqual(results[0], "sent")
        self.assertIn("/task?id=11111111-2222-4333-8444-555555555555", sent[0]["html"])

    def test_anonymous_owner_gets_no_email(self):
        results, sent = self.run_case(owner_type="anon", calls=1)
        self.assertEqual((results, sent), (["skipped_not_signed_in"], []))

    def test_opt_out_and_bounced_addresses_are_respected(self):
        self.assertEqual(self.run_case(opted_in=False, calls=1)[1], [])
        self.assertEqual(self.run_case(invalid=True, calls=1)[1], [])

    def test_email_language_and_direction(self):
        html_fa = email_service.get_email_html("t", True, None, lang="fa")
        self.assertIn('dir="rtl"', html_fa)
        html_en = email_service.get_email_html("t", True, None)
        self.assertIn("Your 3D Model is Ready", html_en)
        self.assertEqual(v3_notify.owner_ui_language(types.SimpleNamespace(preferred_language="ru-RU",
                                                                            detected_language=None)), "ru")


if __name__ == "__main__":
    unittest.main()
