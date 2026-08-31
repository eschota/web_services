import unittest
import asyncio
import httpx
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import task_priority
from task_priority import (
    QUEUE_CLASS_BACKGROUND,
    QUEUE_CLASS_INTERACTIVE,
    background_dispatch_budget,
    dispatch_released_interactive,
    dispatch_sort_key,
    normalize_queue_class,
    select_preemption_victims,
    worker_supports_preemption,
)


def task(task_id, queue_class, created, *, progress=(0, 0), started=None,
         status="processing", preemption_state="none"):
    return SimpleNamespace(
        id=task_id,
        queue_class=queue_class,
        created_at=created,
        status=status,
        ready_count=progress[0],
        total_count=progress[1],
        processing_started_at=started,
        preemption_state=preemption_state,
    )


class QueueClassTests(unittest.TestCase):
    def test_default_is_interactive_even_with_unknown_value(self):
        for value in (None, "", "manual", "INTERACTIVE"):
            self.assertEqual(normalize_queue_class(value), QUEUE_CLASS_INTERACTIVE)

    def test_interactive_fifo_precedes_older_background(self):
        now = datetime.utcnow()
        tasks = [
            task("bg-old", QUEUE_CLASS_BACKGROUND, now - timedelta(hours=2)),
            task("user-2", QUEUE_CLASS_INTERACTIVE, now + timedelta(seconds=2)),
            task("user-1", QUEUE_CLASS_INTERACTIVE, now + timedelta(seconds=1)),
        ]
        self.assertEqual(
            [item.id for item in sorted(tasks, key=dispatch_sort_key)],
            ["user-1", "user-2", "bg-old"],
        )

    def test_sql_orders_interactive_before_limit_and_excludes_preempting(self):
        import database

        async def scenario():
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:", poolclass=StaticPool
            )
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(database.Task.__table__.create)
            now = datetime.utcnow()
            rows = [
                {
                    "id": f"bg-{index:03}",
                    "owner_type": "anon",
                    "owner_id": "test",
                    "status": "created",
                    "pipeline_kind": "convert",
                    "queue_class": QUEUE_CLASS_BACKGROUND,
                    "preemption_state": "none",
                    "created_at": now - timedelta(hours=2),
                }
                for index in range(500)
            ]
            rows.extend(
                [
                    {
                        "id": "interactive",
                        "owner_type": "anon",
                        "owner_id": "test",
                        "status": "created",
                        "pipeline_kind": "convert",
                        "queue_class": QUEUE_CLASS_INTERACTIVE,
                        "preemption_state": "none",
                        "created_at": now,
                    },
                    {
                        "id": "preempting",
                        "owner_type": "anon",
                        "owner_id": "test",
                        "status": "created",
                        "pipeline_kind": "convert",
                        "queue_class": QUEUE_CLASS_INTERACTIVE,
                        "preemption_state": "stopping",
                        "created_at": now - timedelta(days=1),
                    },
                ]
            )
            async with sessions() as session:
                await session.execute(insert(database.Task), rows)
                await session.commit()
                result = await session.execute(
                    task_priority.dispatch_queue_statement(database.Task, now)
                )
                ids = [row.id for row in result.scalars().all()]
            await engine.dispose()
            return ids

        ids = asyncio.run(scenario())
        self.assertEqual(len(ids), 500)
        self.assertEqual(ids[0], "interactive")
        self.assertNotIn("preempting", ids)

    def test_transient_worker_rejection_keeps_fifo_head_for_next_worker(self):
        oldest = SimpleNamespace(
            id="oldest", status="created", source_attempt_count=0
        )
        newer = SimpleNamespace(id="newer", status="created", source_attempt_count=0)
        candidates = [oldest, newer]

        async def reject(task_row):
            self.assertIs(task_row, oldest)
            return task_row, "worker temporarily unavailable"

        accepted = asyncio.run(
            task_priority.dispatch_fifo_candidate(candidates, reject)
        )
        self.assertFalse(accepted)
        self.assertEqual(candidates, [oldest, newer])

    def test_unclassified_dispatch_exception_does_not_drop_fifo_head(self):
        oldest = SimpleNamespace(
            id="oldest", status="created", source_attempt_count=0
        )
        candidates = [oldest]

        async def fail(_task_row):
            raise RuntimeError("database temporarily unavailable")

        self.assertFalse(
            asyncio.run(task_priority.dispatch_fifo_candidate(candidates, fail))
        )
        self.assertEqual(candidates, [oldest])

    def test_transient_cooldown_yields_fifo_head_to_next_task(self):
        now = datetime.utcnow()
        oldest = SimpleNamespace(
            id="oldest",
            status="created",
            source_attempt_count=0,
            dispatch_not_before=now + timedelta(seconds=60),
        )
        newer = SimpleNamespace(
            id="newer",
            status="created",
            source_attempt_count=0,
            dispatch_not_before=None,
        )
        candidates = [oldest, newer]

        async def reject(task_row):
            return task_row, "worker timeout"

        accepted = asyncio.run(
            task_priority.dispatch_fifo_candidate(candidates, reject)
        )
        self.assertFalse(accepted)
        self.assertEqual(candidates, [newer])


class ReserveTests(unittest.TestCase):
    def test_background_can_use_at_most_n_minus_one(self):
        workers = [object(), object(), object(), object()]
        self.assertEqual(background_dispatch_budget(workers, 0), 2)

    def test_background_never_uses_only_worker(self):
        self.assertEqual(background_dispatch_budget([object()], 0), 0)

    def test_any_interactive_backlog_blocks_new_background_dispatch(self):
        self.assertEqual(background_dispatch_budget([object()] * 8, 1), 0)

    def test_incompatible_worker_is_not_preemption_capable(self):
        old = SimpleNamespace(feature_flags={})
        new = SimpleNamespace(feature_flags={"collection_preemption_v1": True})
        self.assertFalse(worker_supports_preemption(old))
        self.assertTrue(worker_supports_preemption(new))

    def test_released_slots_dispatch_interactive_fifo_in_same_cycle(self):
        oldest = SimpleNamespace(
            id="user-oldest", status="created", source_attempt_count=0
        )
        newer = SimpleNamespace(
            id="user-newer", status="created", source_attempt_count=0
        )
        candidates = [oldest, newer]
        workers = [
            SimpleNamespace(url="https://converter-f1.example/api-converter-glb"),
            SimpleNamespace(url="https://converter-reserve.example/api-converter-glb"),
            SimpleNamespace(url="https://converter-f13.example/api-converter-glb/"),
        ]
        calls = []

        async def start(task_row, worker):
            calls.append((task_row.id, worker.url))
            task_row.status = "processing"
            return task_row, None

        dispatched = asyncio.run(
            dispatch_released_interactive(
                candidates,
                workers,
                {
                    "https://converter-f1.example/api-converter-glb/",
                    "HTTPS://CONVERTER-F13.EXAMPLE/api-converter-glb",
                },
                start,
            )
        )
        self.assertEqual(dispatched, 2)
        self.assertEqual(
            calls,
            [
                ("user-oldest", workers[0].url),
                ("user-newer", workers[2].url),
            ],
        )
        self.assertEqual(candidates, [])


class VictimSelectionTests(unittest.TestCase):
    def test_low_progress_then_latest_start_is_recalled_first(self):
        now = datetime.utcnow()
        candidates = [
            task("almost-done", QUEUE_CLASS_BACKGROUND, now, progress=(9, 10), started=now),
            task("older-zero", QUEUE_CLASS_BACKGROUND, now, progress=(0, 10),
                 started=now - timedelta(hours=1)),
            task("newer-zero", QUEUE_CLASS_BACKGROUND, now, progress=(0, 10), started=now),
        ]
        self.assertEqual(select_preemption_victims(candidates, 2), [candidates[2], candidates[1]])

    def test_interactive_and_already_stopping_tasks_are_never_victims(self):
        now = datetime.utcnow()
        interactive = task("user", QUEUE_CLASS_INTERACTIVE, now)
        stopping = task(
            "stopping", QUEUE_CLASS_BACKGROUND, now, preemption_state="stopping"
        )
        eligible = task("background", QUEUE_CLASS_BACKGROUND, now)
        self.assertEqual(select_preemption_victims([interactive, stopping, eligible], 5), [eligible])


class PreemptionRecoveryTests(unittest.TestCase):
    def _run(
        self,
        worker_status,
        *,
        reboot=False,
        recovering=False,
        initial_worker_status="Processing",
    ):
        import database

        task_row = database.Task(
            id="backend-1",
            owner_type="anon",
            owner_id="test",
            status="processing",
            pipeline_kind="convert",
            queue_class=QUEUE_CLASS_BACKGROUND,
            worker_api="https://converter-f1.freestock.online/api-converter-glb",
            worker_task_id="worker-1",
            progress_page="https://worker/progress",
            guid="guid-1",
            ready_count=2,
            total_count=17,
            video_ready=False,
            video_url=None,
            fbx_glb_output_url=None,
            fbx_glb_model_name=None,
            fbx_glb_ready=False,
            fbx_glb_error=None,
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
            error_message=None,
            processing_started_at=datetime.utcnow(),
            last_progress_at=datetime.utcnow(),
            preemption_state="stopping" if recovering else "none",
            preemption_count=2,
            preempted_at=None,
            dispatch_not_before=None,
            preemption_request_id="request-recovery" if recovering else None,
            preemption_worker_boot_id="boot-1" if recovering else None,
            updated_at=datetime.utcnow(),
            restart_count=4,
            source_attempt_count=3,
            created_at=datetime.utcnow() - timedelta(days=2),
        )
        task_row.output_urls = ["partial"]
        task_row.ready_urls = ["partial"]
        original_created = task_row.created_at

        control = {"posted": False}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "POST" and "/control/tasks/" in path:
                control["posted"] = True
                return httpx.Response(202, json={"status": "Preempting"})
            if path.endswith("/status/worker-1"):
                if reboot and control["posted"]:
                    return httpx.Response(404)
                return httpx.Response(200, json={"status": worker_status})
            if path.endswith("/server-status"):
                released = control["posted"] or recovering
                return httpx.Response(
                    200,
                    json={
                        "process_boot_id": (
                            "boot-2" if reboot and released else "boot-1"
                        ),
                        "processing_tasks": (
                            []
                            if released or initial_worker_status == "Pending"
                            else [{"task_id": "worker-1", "status": "Processing"}]
                        ),
                        "pending_tasks": (
                            [{"task_id": "worker-1", "status": "Pending"}]
                            if not released and initial_worker_status == "Pending"
                            else []
                        ),
                        "tasks_summary": {
                            "processing": 0 if released else 1,
                            "pending": 0,
                            "queue_size": 0,
                        },
                    },
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        real_client = httpx.AsyncClient

        def client(*args, **kwargs):
            kwargs["transport"] = transport
            return real_client(*args, **kwargs)

        async def scenario():
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:",
                poolclass=StaticPool,
            )
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(database.Task.__table__.create)
            async with sessions() as session:
                session.add(task_row)
                await session.commit()
            try:
                with patch.object(task_priority, "PREEMPTION_ENABLED", True), patch.object(
                    task_priority, "_control_worker", return_value={
                        "name": "f1", "url": "https://control-f1.test", "token": "secret"
                    }
                ), patch.object(database, "AsyncSessionLocal", sessions), patch.object(
                    task_priority.httpx, "AsyncClient", side_effect=client
                ):
                    result = await task_priority.preempt_background_task(task_row.id)
                async with sessions() as session:
                    persisted = await session.get(database.Task, task_row.id)
                    return result, persisted
            finally:
                await engine.dispose()

        result, persisted = asyncio.run(scenario())
        return result, persisted, original_created

    def test_preempted_row_is_requeued_without_spending_retry_budgets(self):
        result, row, original_created = self._run("Preempted")
        self.assertTrue(result)
        self.assertEqual(row.status, "created")
        self.assertIsNone(row.worker_api)
        self.assertEqual(row.preemption_count, 3)
        self.assertEqual((row.restart_count, row.source_attempt_count), (4, 3))
        self.assertEqual(row.created_at, original_created)
        self.assertGreater(row.dispatch_not_before, row.preempted_at)

    def test_completed_race_wins_and_is_not_requeued(self):
        result, row, _ = self._run("Completed")
        self.assertFalse(result)
        self.assertEqual(row.status, "processing")
        self.assertEqual(row.worker_task_id, "worker-1")

    def test_worker_reboot_requeues_only_after_boot_change_and_empty_slot_proof(self):
        result, row, _ = self._run("", reboot=True)
        self.assertTrue(result)
        self.assertEqual(row.status, "created")
        self.assertIsNone(row.worker_task_id)

    def test_accepted_pending_worker_task_can_be_recalled(self):
        result, row, _ = self._run(
            "Preempted", initial_worker_status="Pending"
        )
        self.assertTrue(result)
        self.assertEqual(row.status, "created")
        self.assertIsNone(row.worker_task_id)

    def test_restart_recovery_accepts_persisted_preempted_release_proof(self):
        result, row, _ = self._run("Preempted", recovering=True)
        self.assertTrue(result)
        self.assertEqual(row.status, "created")
        self.assertIsNone(row.preemption_worker_boot_id)

    def test_restart_recovery_accepts_changed_boot_and_empty_slot(self):
        result, row, _ = self._run("", reboot=True, recovering=True)
        self.assertTrue(result)
        self.assertEqual(row.status, "created")
        self.assertIsNone(row.preemption_worker_boot_id)

    def test_cas_refuses_to_clear_a_rebound_worker_attempt(self):
        import database

        async def scenario():
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:", poolclass=StaticPool
            )
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(database.Task.__table__.create)
            row = database.Task(
                id="backend-cas",
                owner_type="anon",
                owner_id="test",
                status="processing",
                pipeline_kind="convert",
                queue_class=QUEUE_CLASS_BACKGROUND,
                worker_api="https://new-worker/api-converter-glb",
                worker_task_id="new-attempt",
                preemption_state="stopping",
                preemption_request_id="request-1",
            )
            async with sessions() as session:
                session.add(row)
                await session.commit()
            async with sessions() as session:
                changed = await task_priority._cas_requeue_preempted_task(
                    session,
                    database.Task,
                    task_id=row.id,
                    worker_api="https://old-worker/api-converter-glb",
                    worker_task_id="old-attempt",
                    request_id="request-1",
                    now=datetime.utcnow(),
                )
            async with sessions() as session:
                persisted = await session.get(database.Task, row.id)
                self.assertFalse(changed)
                self.assertEqual(persisted.worker_task_id, "new-attempt")
                self.assertEqual(persisted.status, "processing")
            await engine.dispose()

        asyncio.run(scenario())


class StrictReleaseProofTests(unittest.TestCase):
    def test_missing_or_malformed_telemetry_never_proves_empty(self):
        for payload in (
            {},
            {"processing_tasks": []},
            {"processing_tasks": "none", "tasks_summary": {}},
            {
                "processing_tasks": [],
                "tasks_summary": {"processing": 0, "pending": 0},
            },
        ):
            self.assertFalse(task_priority._status_is_slot_empty(payload, "worker-1"))

    def test_explicit_empty_buckets_prove_empty(self):
        self.assertTrue(
            task_priority._status_is_slot_empty(
                {
                    "processing_tasks": [],
                    "pending_tasks": [],
                    "tasks_summary": {"processing": 0, "pending": 0, "queue_size": 0},
                },
                "worker-1",
            )
        )


class ResetGuardTests(unittest.TestCase):
    def test_requested_and_stopping_rows_cannot_be_reset(self):
        import tasks

        async def scenario():
            for state in ("requested", "stopping"):
                row = SimpleNamespace(
                    id=f"task-{state}",
                    preemption_state=state,
                    status="processing",
                )
                self.assertFalse(await tasks.admin_requeue_task_to_created(None, row))
                self.assertFalse(await tasks.reset_stale_task(None, row))
                self.assertEqual(row.status, "processing")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
