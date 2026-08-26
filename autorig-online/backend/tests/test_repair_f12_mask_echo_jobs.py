import asyncio
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from scripts import repair_f12_mask_echo_jobs as repair


class F12MaskEchoRepairTests(unittest.TestCase):
    @staticmethod
    def _create_broker_db(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(path)
        connection.execute(
            "CREATE TABLE workload_waiters ("
            "request_id TEXT PRIMARY KEY, owner_service TEXT, owner_task_id TEXT, "
            "state TEXT, lease_id TEXT)"
        )
        connection.execute(
            "CREATE TABLE workload_leases ("
            "lease_id TEXT PRIMARY KEY, request_id TEXT, owner_service TEXT, "
            "owner_task_id TEXT, state TEXT)"
        )
        return connection

    def test_stale_broker_identity_is_proved_absent_without_enabling_broker(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "autorig.db"
            connection = self._create_broker_db(db_path)
            connection.commit()
            connection.close()
            evidence = repair._inspect_broker_identities(
                db_path,
                {
                    "job-1": {
                        "id": "job-1",
                        "hunyuan_workload_request_id": "request-stale",
                    }
                },
            )
        self.assertFalse(evidence["job-1"]["active"])
        self.assertEqual(
            evidence["job-1"]["disposition"], "broker_identity_absent"
        )

    def test_live_broker_waiter_is_never_treated_as_stale(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "autorig.db"
            connection = self._create_broker_db(db_path)
            connection.execute(
                "INSERT INTO workload_waiters VALUES(?,?,?,?,?)",
                ("request-1", "renderfin", "job-1", "waiting", None),
            )
            connection.commit()
            connection.close()
            evidence = repair._inspect_broker_identities(
                db_path,
                {
                    "job-1": {
                        "id": "job-1",
                        "hunyuan_workload_request_id": "request-1",
                    }
                },
            )
        self.assertTrue(evidence["job-1"]["active"])
        self.assertEqual(
            evidence["job-1"]["disposition"], "broker_identity_active"
        )

    def test_pruned_task_requires_complete_idle_proof(self):
        job = {
            "id": "job-1",
            "hunyuan_task_id": "http://worker/status/task-1",
        }
        idle = {
            "processing_tasks": [],
            "pending_tasks": [],
            "tasks_summary": {"processing": 0, "pending": 0, "queue_size": 0},
        }
        self.assertTrue(repair._worker_idle_for_exact_job(idle, job))
        busy_other = {
            **idle,
            "processing_tasks": [{"task_id": "task-2", "backend_task_id": "job-2"}],
            "tasks_summary": {"processing": 1, "pending": 0, "queue_size": 0},
        }
        self.assertFalse(repair._worker_idle_for_exact_job(busy_other, job))
        self.assertFalse(
            repair._worker_idle_for_exact_job(
                {"processing_tasks": [], "pending_tasks": []}, job
            )
        )

    def test_waiting_broker_request_is_cancelled_before_identity_is_cleared(self):
        job = {
            "id": "job-1",
            "stage": "hunyuan",
            "hunyuan_workload_request_id": "request-1",
        }
        cancel = AsyncMock()
        receipts = []
        with patch.object(repair.workload_lease, "cancel_waiter", cancel):
            result = asyncio.run(
                repair._retire_live_bindings({"job-1": job}, receipts=receipts)
            )
        cancel.assert_awaited_once()
        self.assertIs(result, receipts)
        self.assertEqual(receipts[0]["state"], "terminal")
        self.assertEqual(receipts[0]["lease_outcome"], "waiter_cancelled")

    def test_historical_terminal_binding_needs_no_live_worker_registry_row(self):
        job = {
            "id": "job-1",
            "stage": "submitted",
            "hunyuan_worker": "retired-f2",
            "hunyuan_task_id": "http://retired/status/task-1",
        }
        receipts = []
        with patch.object(
            repair.hunyuan_client,
            "worker_for_url",
            side_effect=AssertionError("historical URL must not be resolved"),
        ), patch.object(
            repair.hunyuan_client,
            "preempt_bound_task",
            new_callable=AsyncMock,
        ) as preempt:
            result = asyncio.run(
                repair._retire_live_bindings({"job-1": job}, receipts=receipts)
            )
        preempt.assert_not_awaited()
        self.assertIs(result, receipts)
        self.assertEqual(receipts[0]["state"], "terminal")
        self.assertEqual(
            receipts[0]["worker_outcome"], "historical_terminal"
        )
        self.assertEqual(receipts[0]["lease_outcome"], "no_lease")

    def test_rewind_keeps_task_identity_artifacts_and_preemption_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "model.glb"
            artifact.write_bytes(b"incident-evidence")
            db_path = root / "renderfin.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                "CREATE TABLE chargen_jobs ("
                "id TEXT PRIMARY KEY, payload TEXT NOT NULL, stage TEXT NOT NULL, "
                "created_at REAL NOT NULL)"
            )
            payload = {
                "id": "job-1",
                "stage": "submitted",
                "submitted_task_id": "autorig-task-1",
                "artifact_revision": 2,
                "attempts": {"hunyuan": 3},
                "attempts_refunded": True,
                "preemption_count": 4,
                "preempted_at": 123.0,
                "dispatch_not_before": 456.0,
                "glb_url": str(artifact),
            }
            connection.execute(
                "INSERT INTO chargen_jobs(id,payload,stage,created_at) VALUES(?,?,?,?)",
                ("job-1", json.dumps(payload), "submitted", 1.0),
            )
            connection.commit()
            result = repair._apply_rewind(connection, {"job-1": payload})
            repaired = json.loads(
                connection.execute(
                    "SELECT payload FROM chargen_jobs WHERE id='job-1'"
                ).fetchone()[0]
            )
            connection.close()

            self.assertEqual(result["rewound"], 1)
            self.assertEqual(result["before"][0]["attempts"], {"hunyuan": 3})
            self.assertEqual(repaired["submitted_task_id"], "autorig-task-1")
            self.assertEqual(repaired["artifact_revision"], 3)
            self.assertEqual(repaired["attempts"], {})
            self.assertEqual(repaired["preemption_count"], 4)
            self.assertEqual(repaired["preempted_at"], 123.0)
            self.assertEqual(repaired["dispatch_not_before"], 0)
            self.assertEqual(artifact.read_bytes(), b"incident-evidence")

    def test_apply_preflight_rejects_active_service_and_repeat_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            common = {
                "db_path": root / "renderfin.db",
                "manifest_path": root / "manifest.json",
                "backup_path": root / "backup.db",
                "receipt_path": root / "receipt.json",
            }
            active = subprocess.CompletedProcess(
                [], 0, stdout="LoadState=loaded\nActiveState=active\n", stderr=""
            )
            with patch.object(repair.subprocess, "run", return_value=active):
                with self.assertRaisesRegex(RuntimeError, "loaded and stopped"):
                    repair._assert_apply_preconditions(
                        **common, jobs={"job-1": {"id": "job-1"}}
                    )

            inactive = subprocess.CompletedProcess(
                [], 0, stdout="LoadState=loaded\nActiveState=inactive\n", stderr=""
            )
            with patch.object(repair.subprocess, "run", return_value=inactive):
                with self.assertRaisesRegex(RuntimeError, "repair marker already present"):
                    repair._assert_apply_preconditions(
                        **common,
                        jobs={
                            "job-1": {
                                "id": "job-1",
                                "quality_repair_reason": repair.REASON,
                            }
                        },
                    )


if __name__ == "__main__":
    unittest.main()
