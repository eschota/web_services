import tempfile
import unittest
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.pool import NullPool, StaticPool

from database import Base, Task, _create_database_engine


class SQLiteSessionIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="autorig-sqlite-isolation-")
        db_path = Path(self.temp_dir.name) / "autorig.db"
        self.engine = _create_database_engine(f"sqlite+aiosqlite:///{db_path}")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def asyncTearDown(self):
        await self.engine.dispose()
        self.temp_dir.cleanup()

    async def test_file_sqlite_uses_separate_connections_and_live_pragmas(self):
        self.assertIsInstance(self.engine.pool, NullPool)
        async with self.engine.connect() as connection:
            journal_mode = (await connection.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
            busy_timeout = (await connection.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            synchronous = (await connection.exec_driver_sql("PRAGMA synchronous")).scalar_one()

        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(int(busy_timeout), 30000)
        self.assertEqual(int(synchronous), 1)  # NORMAL

    async def test_sibling_rollback_cannot_erase_an_uncommitted_task(self):
        task_id = "00000000-0000-0000-0000-000000000001"
        async with self.sessions() as writer, self.sessions() as sibling:
            # Check out both physical connections before the writer takes the
            # SQLite write lock. They must not share a transaction.
            await writer.connection()
            await sibling.connection()
            task = Task(
                id=task_id,
                owner_type="anon",
                owner_id="isolation-test",
                input_url="https://example.test/model.glb",
                input_type="t_pose",
                status="created",
            )
            writer.add(task)
            await writer.flush()

            self.assertIsNone(await sibling.get(Task, task_id))
            await sibling.rollback()
            await writer.commit()
            await writer.refresh(task)

        async with self.sessions() as verifier:
            self.assertIsNotNone(await verifier.get(Task, task_id))

    async def test_sibling_rollback_cannot_erase_a_committed_task(self):
        task_id = "00000000-0000-0000-0000-000000000002"
        async with self.sessions() as writer:
            writer.add(
                Task(
                    id=task_id,
                    owner_type="anon",
                    owner_id="committed-test",
                    input_url="https://example.test/model.glb",
                    input_type="t_pose",
                    status="created",
                )
            )
            await writer.commit()

        async with self.sessions() as sibling:
            await sibling.connection()
            await sibling.rollback()

        async with self.sessions() as verifier:
            self.assertIsNotNone(await verifier.get(Task, task_id))

    async def test_in_memory_sqlite_retains_static_pool(self):
        memory_engine = _create_database_engine("sqlite+aiosqlite:///:memory:")
        try:
            self.assertIsInstance(memory_engine.pool, StaticPool)
        finally:
            await memory_engine.dispose()


if __name__ == "__main__":
    unittest.main()
