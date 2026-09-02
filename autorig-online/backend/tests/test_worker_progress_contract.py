"""Regression tests for worker terminal-line parsing."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import tasks
import workers

from worker_progress_contract import (
    latest_terminal_failure_reason,
    terminal_failure_reason_from_line,
)


class WorkerProgressContractTests(unittest.TestCase):
    def test_nested_failure_in_warning_is_not_terminal(self):
        line = (
            "WARNING: Pose preparation failed; switched to SOSISKA fallback. "
            "Reason: FAILURE: After 6 attempts, rig creation failed"
        )
        self.assertIsNone(terminal_failure_reason_from_line(line))
        self.assertIsNone(latest_terminal_failure_reason(line))

    def test_dedicated_failure_line_is_terminal(self):
        text = "working\n  FAILURE: invalid model buffer\n"
        self.assertEqual("invalid model buffer", latest_terminal_failure_reason(text))

    def test_worker_timestamp_prefix_preserves_marker_anchoring(self):
        self.assertEqual(
            "invalid buffer",
            terminal_failure_reason_from_line(
                "2026-07-10 15:32:58.141 FAILURE: invalid buffer"
            ),
        )
        self.assertIsNone(
            terminal_failure_reason_from_line(
                "2026-07-10 15:32:58.141 WARNING: fallback: FAILURE: nested"
            )
        )

    def test_bom_and_legacy_terminal_prefixes_are_supported(self):
        self.assertEqual(
            "fatal import",
            terminal_failure_reason_from_line("\ufeffFATAL: fatal import"),
        )
        self.assertEqual("Unknown failure", terminal_failure_reason_from_line("ERROR:"))

    def test_latest_terminal_line_wins(self):
        text = "FAILURE: first\r\nWARNING: recovered\r\nFAILURE: final"
        self.assertEqual("final", latest_terminal_failure_reason(text))

    def test_v2_completion_state_requires_worker_completed_and_finalized(self):
        self.assertEqual(
            (True, False, None),
            tasks._completion_contract_v2_state({
                "completion_contract_version": 2,
                "status": "Processing",
                "finalized": False,
            }),
        )
        self.assertEqual(
            (True, True, None),
            tasks._completion_contract_v2_state({
                "completion_contract_version": 2,
                "status": "Completed",
                "finalized": True,
            }),
        )

    def test_v2_finalization_error_is_terminal(self):
        self.assertEqual(
            (True, False, "missing max; missing mview"),
            tasks._completion_contract_v2_state({
                "completion_contract_version": 2,
                "status": "Processing",
                "finalized": False,
                "finalization_errors": ["missing max", "missing mview"],
            }),
        )

    def test_progress_page_persists_v2_without_database_migration(self):
        task = SimpleNamespace(
            progress_page="https://worker/model.html?completion_contract_version=2"
        )
        self.assertTrue(tasks._task_declares_completion_v2(task))
        self.assertFalse(
            tasks._task_declares_completion_v2(
                SimpleNamespace(progress_page="https://worker/legacy.html")
            )
        )


class ManagerCompletionGateTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def task(**overrides):
        values = dict(
            id="task-id",
            status="processing",
            worker_api="https://worker/api-converter-glb",
            worker_task_id="worker-task-id",
            progress_page=None,
            guid="33333333-3333-3333-3333-333333333333",
            input_type="static",
            output_urls=["https://worker/expected.glb"],
            ready_urls=[],
            ready_count=0,
            total_count=1,
            video_ready=False,
            video_url=None,
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
            workload_lease_id=None,
            preemption_state="none",
            last_progress_at=None,
            error_message=None,
            owner_type="agent",
            owner_id="agent-id",
            youtube_video_id=None,
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    async def _run(self, task, completion, *, ready=([], 0), concrete=([], None, None)):
        db = SimpleNamespace(commit=AsyncMock(), refresh=AsyncMock(), execute=AsyncMock())
        with patch.object(
            tasks, "_fetch_worker_completion_contract", AsyncMock(return_value=completion)
        ), patch.object(
            tasks, "check_urls_batch", AsyncMock(return_value=ready)
        ), patch.object(
            tasks, "_fetch_concrete_worker_artifacts", AsyncMock(return_value=concrete)
        ), patch.object(
            tasks, "_validated_viewer_artifact_urls", AsyncMock(return_value=(None, None))
        ), patch.object(
            tasks, "_mark_task_worker_failed_if_reported", AsyncMock(return_value=False)
        ), patch.object(
            tasks, "check_video_availability", AsyncMock(return_value=(False, None))
        ), patch.object(
            tasks, "_schedule_task_error_notification"
        ):
            result = await tasks.update_task_progress(db, task)
        return result, db

    async def test_all_urls_ready_but_v2_not_finalized_stays_processing(self):
        task = self.task()
        result, _db = await self._run(
            task,
            {"completion_contract_version": 2, "status": "Processing", "finalized": False},
            ready=([task.output_urls[0]], 1),
        )
        self.assertEqual(result.status, "processing")

    async def test_early_model_files_does_not_replace_v2_contract(self):
        task = self.task()
        original = list(task.output_urls)
        concrete = [
            "https://worker/model_video.mp4",
            "https://worker/model_video_poster.jpg",
            "https://worker/model.zip",
        ]
        result, _db = await self._run(
            task,
            {"completion_contract_version": 2, "status": "Processing", "finalized": False},
            concrete=(concrete, None, None),
        )
        self.assertEqual(result.output_urls, original)
        self.assertEqual(result.total_count, 1)
        self.assertEqual(result.status, "processing")

    async def test_finalized_v2_can_complete(self):
        task = self.task(status="queued")
        result, _db = await self._run(
            task,
            {"completion_contract_version": 2, "status": "Completed", "finalized": True},
            ready=([task.output_urls[0]], 1),
        )
        self.assertEqual(result.status, "done")

    async def test_finalized_v2_without_optional_preview_reconciles_and_completes(self):
        task = self.task()
        concrete = [
            "https://worker/model_hdrp.unitypackage",
            "https://worker/model.glb",
        ]
        result, _db = await self._run(
            task,
            {
                "completion_contract_version": 2,
                "status": "Completed",
                "finalized": True,
            },
            ready=([], 0),
            concrete=(concrete, None, None),
        )
        self.assertEqual(result.status, "done")
        self.assertEqual(result.output_urls, concrete)
        self.assertEqual(result.ready_count, len(concrete))
        self.assertFalse(result.video_ready)

    async def test_legacy_concrete_outputs_still_require_preview_evidence(self):
        task = self.task()
        concrete = [
            "https://worker/model_hdrp.unitypackage",
            "https://worker/model.glb",
        ]
        result, _db = await self._run(
            task,
            {"status": "Completed"},
            ready=([], 0),
            concrete=(concrete, None, None),
        )
        self.assertEqual(result.status, "processing")
        self.assertEqual(result.output_urls, ["https://worker/expected.glb"])

    async def test_v1_fallback_can_complete_from_ready_urls(self):
        task = self.task(status="queued")
        result, _db = await self._run(
            task,
            {"status": "Completed"},
            ready=([task.output_urls[0]], 1),
        )
        self.assertEqual(result.status, "done")

    async def test_status_probe_outage_cannot_bypass_v2_gate(self):
        task = self.task(
            progress_page="https://worker/model.html?completion_contract_version=2"
        )
        result, _db = await self._run(
            task,
            None,
            ready=([task.output_urls[0]], 1),
            concrete=(["https://worker/model.zip"], None, None),
        )
        self.assertEqual(result.status, "processing")
        self.assertEqual(result.output_urls, ["https://worker/expected.glb"])

    async def test_historical_v1_status_404_keeps_ready_url_fallback(self):
        task = self.task(status="queued", progress_page="https://worker/legacy.html")
        result, _db = await self._run(
            task,
            None,
            ready=([task.output_urls[0]], 1),
        )
        self.assertEqual(result.status, "done")

    async def test_worker_finalization_failure_sets_central_error(self):
        task = self.task()
        result, db = await self._run(
            task,
            {
                "completion_contract_version": 2,
                "status": "Failed",
                "finalized": False,
                "finalization_errors": ["100k:max missing"],
            },
        )
        self.assertEqual(result.status, "error")
        self.assertIn("100k:max missing", result.error_message)
        db.commit.assert_awaited_once()

    async def test_post_timeout_recovery_persists_worker_v2_declaration(self):
        guid = "44444444-4444-4444-4444-444444444444"
        response = SimpleNamespace(
            status_code=200,
            json=lambda: {
                "total_active": 1,
                "active_tasks": {
                    guid: {
                        "task_id": guid,
                        "created_at": 999.0,
                        "completion_contract_version": 2,
                        "output_urls": [
                            f"https://worker/converter/glb/{guid}/{guid}.glb"
                        ],
                    }
                },
            },
        )
        client = SimpleNamespace(get=AsyncMock(return_value=response))
        recovered = await workers._recover_worker_task_after_post_timeout(
            client,
            "https://worker/api-converter-glb",
            "https://source/input.glb",
            1000.0,
        )
        self.assertIsNotNone(recovered)
        self.assertIn("completion_contract_version=2", recovered.progress_page)


if __name__ == "__main__":
    unittest.main()
