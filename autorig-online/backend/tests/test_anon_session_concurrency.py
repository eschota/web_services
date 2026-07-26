import asyncio
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from auth import get_or_create_anon_session
from database import AnonSession, Base


class AnonymousSessionConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "anon-session.db"
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        async with self.engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: AnonSession.__table__.create(
                    sync_connection,
                    checkfirst=True,
                )
            )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )

    async def asyncTearDown(self):
        await self.engine.dispose()
        self.temp_dir.cleanup()

    async def test_parallel_requests_create_one_reusable_session(self):
        anon_id = "parallel-anonymous-session"

        async def request_session():
            async with self.session_factory() as session:
                anon = await get_or_create_anon_session(session, anon_id)
                return anon.anon_id

        results = await asyncio.gather(*(request_session() for _ in range(8)))
        self.assertEqual(results, [anon_id] * 8)

        async with self.session_factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(AnonSession)
                .where(AnonSession.anon_id == anon_id)
            )
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
