import gzip
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from daily_email_report import (  # noqa: E402
    ReportWindow,
    collect_database_stats,
    collect_download_stats,
    render_html,
    report_window,
)


NSK = ZoneInfo("Asia/Novosibirsk")


class DailyEmailReportTest(unittest.TestCase):
    def test_window_is_anchored_at_ten_novosibirsk(self):
        window = report_window(datetime(2026, 7, 20, 14, 30, tzinfo=NSK))
        self.assertEqual(window.start_local.isoformat(), "2026-07-19T10:00:00+07:00")
        self.assertEqual(window.end_local.isoformat(), "2026-07-20T10:00:00+07:00")

        before_ten = report_window(datetime(2026, 7, 20, 9, 0, tzinfo=NSK))
        self.assertEqual(before_ten.end_local.isoformat(), "2026-07-19T10:00:00+07:00")

    def test_download_parser_counts_only_successful_download_routes(self):
        window = ReportWindow(
            datetime(2026, 7, 19, 10, tzinfo=NSK),
            datetime(2026, 7, 20, 10, tzinfo=NSK),
        )
        lines = [
            '1.2.3.4 - - [20/Jul/2026:02:59:59 +0000] "GET /api/file/t1/download/model.glb HTTP/1.1" 200 12 "-" "ua"\n',
            '1.2.3.4 - - [20/Jul/2026:02:10:00 +0000] "GET /api/task/t1/downloads/bundle HTTP/1.1" 206 12 "-" "ua"\n',
            '5.6.7.8 - - [19/Jul/2026:08:00:00 +0000] "GET /api/task/t1/animations/download/walk HTTP/1.1" 200 12 "-" "ua"\n',
            '5.6.7.8 - - [19/Jul/2026:08:00:00 +0000] "GET /api/file/t1/download/bad.glb HTTP/1.1" 404 12 "-" "ua"\n',
            '5.6.7.8 - - [19/Jul/2026:08:00:00 +0000] "GET /task?id=t1 HTTP/1.1" 200 12 "-" "ua"\n',
            '5.6.7.8 - - [20/Jul/2026:03:00:00 +0000] "GET /api/file/t1/download/late.glb HTTP/1.1" 200 12 "-" "ua"\n',
        ]
        with tempfile.TemporaryDirectory() as directory:
            plain = Path(directory) / "access.log"
            zipped = Path(directory) / "access.log.1.gz"
            plain.write_text("".join(lines[:3]), encoding="utf-8")
            with gzip.open(zipped, "wt", encoding="utf-8") as stream:
                stream.writelines(lines[3:])
            stats = collect_download_stats([plain, zipped], window)
        self.assertEqual(stats["downloads"], 3)
        self.assertEqual(stats["download_clients"], 2)
        self.assertEqual(stats["download_kinds"], {"animation": 1, "bundle": 1, "file": 1})

    def test_database_stats_and_html(self):
        window = ReportWindow(
            datetime(2026, 7, 19, 10, tzinfo=NSK),
            datetime(2026, 7, 20, 10, tzinfo=NSK),
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "autorig.db"
            connection = sqlite3.connect(database)
            connection.executescript(
                """
                CREATE TABLE tasks (id TEXT, created_at DATETIME, updated_at DATETIME, status TEXT,
                  owner_type TEXT, worker_api TEXT, error_message TEXT);
                CREATE TABLE rig_completion_events (task_id TEXT, completed_at DATETIME);
                CREATE TABLE users (created_at DATETIME);
                CREATE TABLE gumroad_purchases (created_at DATETIME, price INTEGER, refunded BOOLEAN, test BOOLEAN);
                CREATE TABLE task_file_purchases (created_at DATETIME, credits_spent INTEGER);
                CREATE TABLE task_animation_purchases (created_at DATETIME, credits_spent INTEGER);
                CREATE TABLE task_animation_bundle_purchases (created_at DATETIME, credits_spent INTEGER);
                CREATE TABLE task_animal_animation_pack_purchases (created_at DATETIME, credits_spent INTEGER);
                CREATE TABLE purchase_checkout_intents (created_at DATETIME);
                CREATE TABLE crypto_payment_reports (created_at DATETIME, status TEXT);
                INSERT INTO tasks VALUES ('abc', '2026-07-19 04:00:00', '2026-07-19 05:00:00', 'done', 'user',
                  'https://converter-f11.freestock.online/api-converter-glb', NULL);
                INSERT INTO rig_completion_events VALUES ('abc', '2026-07-19 05:00:00');
                INSERT INTO users VALUES ('2026-07-19 06:00:00');
                INSERT INTO gumroad_purchases VALUES ('2026-07-19 07:00:00', 300, 0, 0);
                INSERT INTO task_file_purchases VALUES ('2026-07-19 07:00:00', 10);
                """
            )
            connection.commit()
            connection.close()
            stats = collect_database_stats(database, window)
        self.assertEqual(stats["tasks_completed"], 1)
        self.assertEqual(stats["registered_tasks_completed"], 1)
        self.assertEqual(stats["anonymous_tasks_completed"], 0)
        self.assertEqual(stats["average_duration_seconds"], 3600)
        self.assertEqual(stats["workers"], {"F11": 1})
        self.assertEqual(stats["gumroad_revenue_cents"], 300)
        self.assertEqual(stats["credits_spent"], 10)
        stats.update(
            downloads=2,
            download_clients=1,
            download_kinds={"file": 2},
            completion_emails=1,
            email_statuses={"delivered": 1},
            email_error=None,
        )
        report = render_html(stats, window, test=True)
        self.assertIn("ТЕСТОВОЕ ПИСЬМО", report)
        self.assertIn("F11", report)
        self.assertIn("$3.00", report)
        self.assertIn("https://autorig.online/task?id=abc", report)


if __name__ == "__main__":
    unittest.main()
