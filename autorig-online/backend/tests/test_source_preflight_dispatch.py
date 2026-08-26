import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
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
        self.execute = AsyncMock(return_value=SimpleNamespace(rowcount=0))


class WorkerCapacityClassificationTests(unittest.TestCase):
    def test_http_423_is_retryable_capacity_without_worker_quarantine(self):
        error = 'Worker returned HTTP 423: {"status_string":"autorig_reserve"}'
        self.assertTrue(tasks._is_transient_worker_dispatch_error(error))
        self.assertTrue(tasks._is_capacity_worker_dispatch_error(error))

    def test_transport_failure_is_transient_but_not_capacity(self):
        error = "Worker connection timed out"
        self.assertTrue(tasks._is_transient_worker_dispatch_error(error))
        self.assertFalse(tasks._is_capacity_worker_dispatch_error(error))


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
    async def test_full_post_response_loss_replays_same_request_without_duplicate(self):
        db = FakeDb()
        task = make_task()
        task.queue_class = "collection_background"
        worker_url = "https://converter-f13.example/api-converter-glb"
        lease = {
            "lease_id_string": "lease-full-response-loss",
            "request_id_string": "request-full-response-loss",
            "physical_resource_id_string": "f13",
            "node_id_string": "f13",
            "workload_class_string": "collection_background",
        }
        acquired_calls = []

        async def acquire_exact(_db, acquired_task, acquired_worker, _status, **_kwargs):
            acquired_calls.append(
                (
                    acquired_worker,
                    acquired_task.workload_request_id,
                    acquired_task.workload_lease_id,
                )
            )
            if not acquired_task.workload_lease_id:
                acquired_task.workload_request_id = lease["request_id_string"]
                acquired_task.workload_lease_id = lease["lease_id_string"]
                acquired_task.workload_physical_resource_id = "f13"
                acquired_task.workload_node_id = "f13"
                acquired_task.workload_class = "collection_background"
                acquired_task.worker_api = acquired_worker
                acquired_task.workload_lease_state = "active"
            else:
                # Model a higher-priority recall arriving while the first POST
                # response is still ambiguous.  The exact replay must resolve
                # the host task before control-plane preemption is scheduled.
                acquired_task.workload_lease_state = "preemption_requested"
            return True, dict(lease)

        accepted = WorkerTaskResult(
            success=True,
            task_id="worker-task-one",
            output_urls=["https://converter-f13.example/out/result.zip"],
            progress_page=(
                "https://converter-f13.example/converter/glb/"
                "11111111-1111-1111-1111-111111111111/index.html"
            ),
            guid="11111111-1111-1111-1111-111111111111",
        )
        dispatch = AsyncMock(
            side_effect=[
                WorkerTaskResult(
                    success=False,
                    error="Worker submission outcome unknown after timeout",
                    unknown_outcome=True,
                ),
                accepted,
            ]
        )
        preempt = AsyncMock(return_value=True)
        with (
            patch.object(
                tasks, "autorig_workload_broker_enabled", return_value=True
            ),
            patch.object(
                tasks,
                "preflight_task_source",
                AsyncMock(return_value=(True, "", False)),
            ) as preflight,
            patch.object(tasks.asyncio, "to_thread", AsyncMock(return_value=None)),
            patch.object(
                tasks,
                "get_worker_workload_status",
                AsyncMock(
                    return_value={
                        "physical_resource_id_string": "f13",
                        "capabilities": {"submission_idempotency_v1": True},
                    }
                ),
            ),
            patch.object(
                tasks,
                "acquire_task_workload_lease",
                new=acquire_exact,
            ),
            patch.object(tasks, "send_task_to_worker", new=dispatch),
            patch.object(
                tasks,
                "release_task_workload_lease",
                AsyncMock(),
            ) as release,
            patch.object(
                tasks,
                "persist_validated_worker_viewer_artifacts",
                AsyncMock(),
            ),
            patch("task_priority.preempt_background_task", new=preempt),
        ):
            first, first_error = await tasks.start_task_on_worker(
                db, task, worker_url
            )
            self.assertEqual("created", first.status)
            self.assertIn("outcome unknown", first_error)
            self.assertEqual("submission_unknown", first.workload_lease_state)
            self.assertEqual(lease["request_id_string"], first.workload_request_id)
            self.assertEqual(lease["lease_id_string"], first.workload_lease_id)
            self.assertEqual(worker_url, first.worker_api)

            second, second_error = await tasks.start_task_on_worker(
                db, task, first.worker_api
            )
            await __import__("asyncio").sleep(0)

        self.assertIsNone(second_error)
        self.assertEqual("processing", second.status)
        self.assertEqual("worker-task-one", second.worker_task_id)
        self.assertEqual(1, preflight.await_count)
        release.assert_not_awaited()
        preempt.assert_awaited_once_with(task.id, broker_requested=True)
        self.assertEqual(2, dispatch.await_count)
        first_call = dispatch.await_args_list[0]
        second_call = dispatch.await_args_list[1]
        self.assertEqual(worker_url, first_call.args[0])
        self.assertEqual(worker_url, second_call.args[0])
        self.assertEqual(lease, first_call.kwargs["workload_lease"])
        self.assertEqual(lease, second_call.kwargs["workload_lease"])
        self.assertEqual(
            [
                (worker_url, None, None),
                (
                    worker_url,
                    lease["request_id_string"],
                    lease["lease_id_string"],
                ),
            ],
            acquired_calls,
        )

    async def test_legacy_full_listener_never_receives_central_identity(self):
        db = FakeDb()
        task = make_task()
        worker_url = "https://converter-legacy.example/api-converter-glb"
        with (
            patch.object(
                tasks, "autorig_workload_broker_enabled", return_value=True
            ),
            patch.object(
                tasks,
                "preflight_task_source",
                AsyncMock(return_value=(True, "", False)),
            ),
            patch.object(
                tasks,
                "get_worker_workload_status",
                AsyncMock(
                    return_value={
                        "physical_resource_id_string": "legacy",
                        "capabilities": {"mode": "full"},
                    }
                ),
            ),
            patch.object(
                tasks, "acquire_task_workload_lease", AsyncMock()
            ) as acquire,
            patch.object(tasks, "send_task_to_worker", AsyncMock()) as send,
        ):
            result, error = await tasks.start_task_on_worker(db, task, worker_url)

        self.assertEqual("created", result.status)
        self.assertIsNone(result.worker_api)
        self.assertFalse(result.workload_request_id)
        self.assertFalse(result.workload_lease_id)
        self.assertIn("worker_missing_submission_idempotency_v1", error)
        acquire.assert_not_awaited()
        send.assert_not_awaited()

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
