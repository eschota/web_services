import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

import email_service
from database import Base, TaskCompletionEmail, _create_database_engine


TASK_ID = "00000000-0000-0000-0000-000000000931"


class TaskCompletionEmailIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="autorig-email-db-")
        db_path = Path(self.temp_dir.name) / "autorig.db"
        self.engine = _create_database_engine(f"sqlite+aiosqlite:///{db_path}")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.original_sessions = email_service.AsyncSessionLocal
        email_service.AsyncSessionLocal = self.sessions

    async def asyncTearDown(self):
        email_service.AsyncSessionLocal = self.original_sessions
        await self.engine.dispose()
        self.temp_dir.cleanup()

    async def test_concurrent_claims_have_one_winner(self):
        claims = await asyncio.gather(
            *[
                email_service._reserve_task_completion_email(TASK_ID, "user@example.test")
                for _ in range(5)
            ]
        )
        self.assertEqual(sum(claims), 1)

    async def test_provider_receives_stable_idempotency_key_and_second_send_is_skipped(self):
        sent = []

        def fake_send(params, options=None):
            sent.append((params, options))
            return {"id": "email-provider-id"}

        with (
            patch.object(email_service, "RESEND_API_KEY", "test-key"),
            patch.object(email_service, "download_image", return_value=None),
            patch.object(email_service.resend.Emails, "send", side_effect=fake_send),
        ):
            first = await email_service.send_task_completed_email(
                "user@example.test",
                TASK_ID,
                "00000000-0000-0000-0000-000000000932",
                "https://converter-f1.freestock.online",
            )
            second = await email_service.send_task_completed_email(
                "user@example.test",
                TASK_ID,
                "00000000-0000-0000-0000-000000000932",
                "https://converter-f1.freestock.online",
            )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1], {"idempotency_key": f"task-completed/{TASK_ID}"})
        async with self.sessions() as db:
            row = await db.scalar(
                select(TaskCompletionEmail).where(TaskCompletionEmail.task_id == TASK_ID)
            )
            self.assertEqual(row.status, "sent")
            self.assertEqual(row.provider_message_id, "email-provider-id")


if __name__ == "__main__":
    unittest.main()
