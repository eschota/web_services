import importlib.util
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / "deploy" / "healthcheck" / "postmigration_monitor.py"
SPEC = importlib.util.spec_from_file_location("postmigration_monitor", SCRIPT)
monitor = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


SCHEMA = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY, owner_type TEXT, owner_id TEXT, status TEXT,
    error_message TEXT, updated_at TEXT
);
CREATE TABLE users (
    id TEXT, email TEXT, email_invalid_at TEXT, email_task_completed INTEGER
);
CREATE TABLE task_completion_emails (
    task_id TEXT PRIMARY KEY, recipient_hash TEXT, status TEXT,
    attempt_count INTEGER, provider_message_id TEXT, last_error TEXT,
    claimed_at TEXT, sent_at TEXT, created_at TEXT, updated_at TEXT
);
CREATE TABLE artifact_cache_jobs (task_id TEXT, status TEXT, updated_at TEXT);
"""


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


class PostmigrationMonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="autorig-postmigration-")
        self.db_path = Path(self.temp.name) / "autorig.db"
        with closing(sqlite3.connect(self.db_path)) as db:
            db.executescript(SCHEMA)
            db.commit()
        self.state = monitor.load_state(time.time())
        self.state["started_at"] = time.time() - 3600

    def tearDown(self):
        self.temp.cleanup()

    def _run_email_audit(self, now=None):
        active, events, metrics = [], [], {}
        with patch.object(monitor, "AUTORIG_DB", str(self.db_path)):
            rows = monitor.audit_completion_email(
                self.state, now or time.time(), active, events, metrics
            )
        return active, metrics, rows

    def test_new_completed_task_without_ledger_is_reported_after_grace(self):
        now = time.time()
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,datetime('now'))",
                ("task-new", "user", "person@example.test", "processing", None),
            )
            db.commit()
        self._run_email_audit(now)
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute("UPDATE tasks SET status='done' WHERE id='task-new'")
            db.commit()
        self._run_email_audit(now + 1)
        active, _, _ = self._run_email_audit(now + monitor.EMAIL_GRACE_SECONDS + 2)
        self.assertTrue(any("no email ledger row" in value for value in active))

    def test_opted_out_user_does_not_require_completion_email(self):
        now = time.time()
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,datetime('now'))",
                ("task-optout", "user", "person@example.test", "processing", None),
            )
            db.execute(
                "INSERT INTO users VALUES (?,?,?,?)",
                ("user-1", "person@example.test", None, 0),
            )
            db.commit()
        self._run_email_audit(now)
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute("UPDATE tasks SET status='done' WHERE id='task-optout'")
            db.commit()
        active, _, _ = self._run_email_audit(now + monitor.EMAIL_GRACE_SECONDS + 1)
        self.assertFalse(any("no email ledger row" in value for value in active))

    def test_failed_and_stale_email_rows_are_active_problems(self):
        old = "2026-08-01 00:00:00"
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                "INSERT INTO task_completion_emails VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("failed-task", "hash", "failed", 1, None, "provider down", old, None, old, old),
            )
            db.execute(
                "INSERT INTO task_completion_emails VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("stale-task", "hash", "sending", 1, None, None, old, None, old, old),
            )
            db.commit()
        self.state["started_at"] = 0
        active, _, _ = self._run_email_audit(time.time())
        self.assertTrue(any("failed for failed-t" in value for value in active))
        self.assertTrue(any("stale-ta stuck in sending" in value for value in active))

    def test_scrub_removes_email_and_bearer(self):
        value = monitor.scrub("user@example.com Authorization: Bearer secret-value")
        self.assertNotIn("user@example.com", value)
        self.assertNotIn("secret-value", value)

    def test_journal_signature_ignores_time_pid_and_uuid(self):
        first = (
            "2026/08/15 16:29:21 [error] 901298#901298: failed task "
            "11111111-2222-3333-4444-555555555555"
        )
        second = (
            "2026/08/15 16:39:21 [error] 901999#901999: failed task "
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        self.assertEqual(
            monitor.journal_signature("nginx.service", first),
            monitor.journal_signature("nginx.service", second),
        )

    def test_renderfin_heartbeat_error_count_is_not_an_exception(self):
        heartbeat = "[Renderfin][Queue] heartbeat tick=3600 tasks={'Done': 20, 'Error': 3}"
        self.assertFalse(monitor.is_journal_error(6, heartbeat))
        self.assertTrue(monitor.is_journal_error(6, "[Renderfin] worker failed"))
        self.assertTrue(monitor.is_journal_error(3, "nginx resolver warning"))

    def test_telegram_probe_and_notification_paths_execute(self):
        active, metrics = [], {}
        response = _JsonResponse(b'{"ok":true}')
        with (
            patch.dict(
                monitor.os.environ,
                {"TELEGRAM_BOT_TOKEN": "test-token", "HEALTHCHECK_CHAT_ID": "123"},
            ),
            patch.object(monitor.urllib.request, "urlopen", return_value=response),
        ):
            monitor.check_telegram_api(active, metrics)
            sent = monitor.telegram_notify("Title <unsafe>", ["user@example.com failed"])
        self.assertEqual(active, [])
        self.assertEqual(metrics["telegram_api"], "ok")
        self.assertTrue(sent)

    def test_state_window_is_exactly_configured_duration(self):
        with tempfile.TemporaryDirectory(prefix="autorig-monitor-state-") as folder:
            with patch.object(monitor, "STATE_PATH", Path(folder) / "state.json"):
                state = monitor.load_state(1000.0)
        self.assertEqual(state["ends_at"] - state["started_at"], monitor.DURATION_SECONDS)

    def test_systemd_contract_self_disables_after_completion(self):
        storage_host = Path(__file__).resolve().parents[2] / "deploy" / "storage-host"
        unit = (storage_host / "autorig-storage-postmigration-monitor.service").read_text(
            encoding="utf-8"
        )
        timer = (storage_host / "autorig-storage-postmigration-monitor.timer").read_text(
            encoding="utf-8"
        )
        self.assertIn("SupplementaryGroups=systemd-journal", unit)
        self.assertIn("postmigration-72h.complete", unit)
        self.assertIn("disable --now autorig-storage-postmigration-monitor.timer", unit)
        self.assertIn("OnUnitActiveSec=10min", timer)


if __name__ == "__main__":
    unittest.main()
