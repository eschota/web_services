import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tasks
from database import Task
from workers import WorkerTaskResult


class FakeDb:
    def __init__(self):
        self.commit = AsyncMock()
        self.refresh = AsyncMock()


def make_task() -> Task:
    return Task(
        id="00000000-0000-0000-0000-000000000001",
        owner_type="anon",
        owner_id="anon",
        input_url="https://example.test/model.glb",
        input_type="t_pose",
        status="created",
        created_at=datetime.utcnow(),
        source_attempt_count=0,
    )


class SourcePreflightDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_unavailable_source_schedules_retry_without_worker_quarantine(self):
        db = FakeDb()
        task = make_task()
        with (
            patch.object(
                tasks,
                "preflight_task_source",
                AsyncMock(return_value=(False, "source did not respond within 8s", False)),
            ),
            patch.object(tasks, "send_task_to_worker", AsyncMock()) as send,
            patch.object(tasks, "quarantine_worker") as quarantine,
        ):
            result, error = await tasks.start_task_on_worker(
                db,
                task,
                "https://converter-f13.example/api-converter-glb",
            )

        self.assertEqual("created", result.status)
        self.assertEqual(1, result.source_attempt_count)
        self.assertIsNotNone(result.source_next_retry_at)
        self.assertIn("Source preflight retry scheduled", error)
        send.assert_not_awaited()
        quarantine.assert_not_called()

    async def test_source_exhaustion_is_terminal_and_does_not_touch_worker(self):
        db = FakeDb()
        task = make_task()
        task.source_attempt_count = tasks.SOURCE_PREFLIGHT_MAX_ATTEMPTS - 1
        with (
            patch.object(
                tasks,
                "preflight_task_source",
                AsyncMock(return_value=(False, "source returned HTTP 404", False)),
            ),
            patch.object(tasks, "_schedule_task_error_notification") as notify,
            patch.object(tasks, "send_task_to_worker", AsyncMock()) as send,
            patch.object(tasks, "quarantine_worker") as quarantine,
        ):
            result, error = await tasks.start_task_on_worker(
                db,
                task,
                "https://converter-f13.example/api-converter-glb",
            )

        self.assertEqual("error", result.status)
        self.assertIn("Source asset unavailable", error)
        notify.assert_called_once_with(task.id)
        send.assert_not_awaited()
        quarantine.assert_not_called()

    async def test_real_worker_timeout_still_quarantines_worker(self):
        db = FakeDb()
        task = make_task()
        worker_url = "https://converter-f13.example/api-converter-glb"
        with (
            patch.object(
                tasks,
                "preflight_task_source",
                AsyncMock(return_value=(True, "", False)),
            ),
            patch.object(
                tasks,
                "send_task_to_worker",
                AsyncMock(return_value=WorkerTaskResult(success=False, error="Worker timeout")),
            ),
            patch.object(tasks, "quarantine_worker") as quarantine,
        ):
            result, error = await tasks.start_task_on_worker(db, task, worker_url)

        self.assertEqual("created", result.status)
        self.assertIsNone(result.processing_started_at)
        self.assertEqual("Worker timeout", error)
        quarantine.assert_called_once()

    def test_glb_magic_validation(self):
        self.assertIsNone(
            tasks._source_format_error(
                "https://example.test/model.glb",
                b"glTF\x02\x00\x00\x00",
                "model/gltf-binary",
            )
        )
        self.assertIn(
            "not a valid binary glTF",
            tasks._source_format_error(
                "https://example.test/model.glb",
                b"not-a-glb-payload",
                "application/octet-stream",
            ),
        )


if __name__ == "__main__":
    unittest.main()
