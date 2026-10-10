import ast
import pathlib
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from sqlalchemy import Column, DateTime, String, case, insert, or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import StaticPool

# Execute the actual SQL builder without importing runtime configuration or
# opening the production app's engine. Only its declared SQL dependencies enter.
source = pathlib.Path(__file__).resolve().parents[1] / "task_priority.py"
node = next(node for node in ast.parse(source.read_text()).body
            if isinstance(node, ast.FunctionDef) and node.name == "dispatch_queue_statement")
scope = {"Any": object, "datetime": datetime, "case": case, "or_": or_,
         "select": select, "QUEUE_CLASS_BACKGROUND": "collection_background", "PREEMPTION_NONE": "none"}
exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), scope)
dispatch_queue_statement = scope["dispatch_queue_statement"]
Base = declarative_base()


class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True)
    status = Column(String)
    pipeline_kind = Column(String)
    queue_class = Column(String)
    preemption_state = Column(String)
    created_at = Column(DateTime)
    source_next_retry_at = Column(DateTime)
    dispatch_not_before = Column(DateTime)


class SchedulerOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_v3_backlog_is_excluded_before_sql_limit(self):
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Task.__table__.create)
            now = datetime.utcnow()
            rows = [dict(id=f"v3-{index}",
                         status="created", pipeline_kind="v3", queue_class="interactive",
                         preemption_state="none", created_at=now - timedelta(hours=1))
                    for index in range(20)]
            rows.append(dict(id="old-rig",
                             status="created", pipeline_kind="rig", queue_class="interactive",
                             preemption_state="none", created_at=now))
            async with sessions() as db:
                await db.execute(insert(Task), rows)
                await db.commit()
                selected = (await db.execute(dispatch_queue_statement(Task, now, limit=1))).scalars().all()
            self.assertEqual([row.id for row in selected], ["old-rig"])
        finally:
            await engine.dispose()


if __name__ == "__main__":
    unittest.main()
