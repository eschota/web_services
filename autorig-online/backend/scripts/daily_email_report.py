#!/usr/bin/env python3
"""Build and send the AutoRig daily operations report."""

from __future__ import annotations

import argparse
import gzip
import html
import json
import re
import sqlite3
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

NOVOSIBIRSK = ZoneInfo("Asia/Novosibirsk")
UTC = timezone.utc
REPORT_HOUR = 10
DEFAULT_RECIPIENT = "eschota@gmail.com"
DOWNLOAD_LOG_RE = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^]]+)\]\s+'
    r'"(?P<method>[A-Z]+)\s+(?P<target>\S+)\s+HTTP/[^\"]+"\s+'
    r'(?P<status>\d{3})\s+'
)
DOWNLOAD_MARKERS = (
    "GET /api/file/",
    "GET /api/task/",
)


@dataclass(frozen=True)
class ReportWindow:
    start_local: datetime
    end_local: datetime

    @property
    def start_utc(self) -> datetime:
        return self.start_local.astimezone(UTC)

    @property
    def end_utc(self) -> datetime:
        return self.end_local.astimezone(UTC)

    @property
    def key(self) -> str:
        return f"daily:{self.end_local.isoformat()}"


def report_window(now: datetime | None = None) -> ReportWindow:
    current = now or datetime.now(NOVOSIBIRSK)
    if current.tzinfo is None:
        current = current.replace(tzinfo=NOVOSIBIRSK)
    current = current.astimezone(NOVOSIBIRSK)
    end = datetime.combine(
        current.date(), datetime_time(REPORT_HOUR), tzinfo=NOVOSIBIRSK
    )
    if current < end:
        end -= timedelta(days=1)
    return ReportWindow(start_local=end - timedelta(days=1), end_local=end)


def _db_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(tzinfo=None).isoformat(sep=" ")


def _scalar(connection: sqlite3.Connection, sql: str, params: Sequence[object]) -> int:
    row = connection.execute(sql, params).fetchone()
    return int((row[0] if row else 0) or 0)


def _worker_name(worker_api: str | None) -> str:
    match = re.search(r"converter-(f\d+)", worker_api or "", re.IGNORECASE)
    if match:
        return match.group(1).upper()
    if not worker_api:
        return "Unknown"
    return urlsplit(worker_api).hostname or worker_api


def collect_database_stats(database: Path, window: ReportWindow) -> dict:
    start = _db_timestamp(window.start_utc)
    end = _db_timestamp(window.end_utc)
    params = (start, end)
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        stats: dict[str, object] = {
            "tasks_created": _scalar(
                connection,
                "SELECT COUNT(*) FROM tasks WHERE created_at >= ? AND created_at < ?",
                params,
            ),
            "tasks_completed": _scalar(
                connection,
                "SELECT COUNT(*) FROM rig_completion_events WHERE completed_at >= ? AND completed_at < ?",
                params,
            ),
            "tasks_failed": _scalar(
                connection,
                "SELECT COUNT(*) FROM tasks WHERE status = 'error' AND updated_at >= ? AND updated_at < ?",
                params,
            ),
            "tasks_processing_now": _scalar(
                connection,
                "SELECT COUNT(*) FROM tasks WHERE status IN ('created', 'queued', 'processing')",
                (),
            ),
            "new_users": _scalar(
                connection,
                "SELECT COUNT(*) FROM users WHERE created_at >= ? AND created_at < ?",
                params,
            ),
        }

        completed_rows = connection.execute(
            """
            SELECT r.task_id, r.completed_at, t.created_at, t.owner_type,
                   t.worker_api, t.error_message
            FROM rig_completion_events AS r
            LEFT JOIN tasks AS t ON t.id = r.task_id
            WHERE r.completed_at >= ? AND r.completed_at < ?
            ORDER BY r.completed_at DESC
            """,
            params,
        ).fetchall()
        durations = []
        workers: Counter[str] = Counter()
        completed_tasks = []
        for row in completed_rows:
            if row["created_at"] and row["completed_at"]:
                created = datetime.fromisoformat(str(row["created_at"]))
                completed = datetime.fromisoformat(str(row["completed_at"]))
                duration = max(0, int((completed - created).total_seconds()))
                durations.append(duration)
            else:
                duration = None
            worker = _worker_name(row["worker_api"])
            workers[worker] += 1
            completed_tasks.append(
                {
                    "id": row["task_id"],
                    "worker": worker,
                    "owner_type": row["owner_type"] or "unknown",
                    "duration_seconds": duration,
                }
            )
        stats["average_duration_seconds"] = (
            int(sum(durations) / len(durations)) if durations else 0
        )
        stats["registered_tasks_completed"] = sum(
            1 for row in completed_rows if row["owner_type"] == "user"
        )
        stats["anonymous_tasks_completed"] = sum(
            1 for row in completed_rows if row["owner_type"] == "anon"
        )
        stats["workers"] = dict(sorted(workers.items()))
        stats["completed_tasks"] = completed_tasks[:12]

        failed_rows = connection.execute(
            """
            SELECT id, error_message FROM tasks
            WHERE status = 'error' AND updated_at >= ? AND updated_at < ?
            ORDER BY updated_at DESC LIMIT 8
            """,
            params,
        ).fetchall()
        stats["failed_tasks"] = [
            {"id": row["id"], "error": (row["error_message"] or "Unknown error")[:180]}
            for row in failed_rows
        ]

        gumroad = connection.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(price), 0) AS cents
            FROM gumroad_purchases
            WHERE created_at >= ? AND created_at < ?
              AND COALESCE(refunded, 0) = 0 AND COALESCE(test, 0) = 0
            """,
            params,
        ).fetchone()
        stats["gumroad_sales"] = int(gumroad["count"] or 0)
        stats["gumroad_revenue_cents"] = int(gumroad["cents"] or 0)

        purchase_tables = (
            "task_file_purchases",
            "task_animation_purchases",
            "task_animation_bundle_purchases",
            "task_animal_animation_pack_purchases",
        )
        internal_count = 0
        internal_credits = 0
        for table in purchase_tables:
            row = connection.execute(
                f"""
                SELECT COUNT(*) AS count, COALESCE(SUM(credits_spent), 0) AS credits
                FROM {table} WHERE created_at >= ? AND created_at < ?
                """,
                params,
            ).fetchone()
            internal_count += int(row["count"] or 0)
            internal_credits += int(row["credits"] or 0)
        stats["internal_purchases"] = internal_count
        stats["credits_spent"] = internal_credits
        stats["checkout_intents"] = _scalar(
            connection,
            "SELECT COUNT(*) FROM purchase_checkout_intents WHERE created_at >= ? AND created_at < ?",
            params,
        )
        stats["crypto_credited"] = _scalar(
            connection,
            """
            SELECT COUNT(*) FROM crypto_payment_reports
            WHERE created_at >= ? AND created_at < ? AND status = 'credited'
            """,
            params,
        )
        return stats
    finally:
        connection.close()


def _iter_log_lines(paths: Iterable[Path]) -> Iterator[str]:
    for path in sorted(set(paths), key=lambda value: str(value)):
        if not path.is_file():
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        try:
            with opener(path, "rt", encoding="utf-8", errors="replace") as stream:
                yield from stream
        except OSError as exc:
            print(f"[DailyReport] Cannot read nginx log {path}: {exc}", file=sys.stderr)


def _download_kind(path: str) -> str | None:
    if re.match(r"^/api/file/[^/]+/download/", path):
        return "file"
    if re.match(r"^/api/task/[^/]+/downloads/(?:bundle|bundle\.zip)(?:/)?$", path):
        return "bundle"
    if re.match(r"^/api/task/[^/]+/animations/download-pack(?:/)?$", path):
        return "animation_pack"
    if re.match(r"^/api/task/[^/]+/animations/download(?:-with-base)?/", path):
        return "animation"
    if re.match(r"^/api/task/[^/]+/animal-variants/[^/]+/[^/]+/download/", path):
        return "animal_variant"
    return None


def collect_download_stats(paths: Iterable[Path], window: ReportWindow) -> dict:
    counts: Counter[str] = Counter()
    clients: set[str] = set()
    for line in _iter_log_lines(paths):
        if not any(marker in line for marker in DOWNLOAD_MARKERS):
            continue
        match = DOWNLOAD_LOG_RE.match(line)
        if not match or match.group("method") != "GET":
            continue
        status = int(match.group("status"))
        if status < 200 or status >= 400:
            continue
        try:
            occurred = datetime.strptime(
                match.group("timestamp"), "%d/%b/%Y:%H:%M:%S %z"
            )
        except ValueError:
            continue
        if not (window.start_utc <= occurred.astimezone(UTC) < window.end_utc):
            continue
        path = unquote(urlsplit(match.group("target")).path)
        kind = _download_kind(path)
        if not kind:
            continue
        counts[kind] += 1
        clients.add(match.group("ip"))
    return {
        "downloads": sum(counts.values()),
        "download_clients": len(clients),
        "download_kinds": dict(sorted(counts.items())),
    }


def collect_email_delivery_stats(window: ReportWindow, limit: int = 500) -> dict:
    try:
        import resend
        from config import RESEND_API_KEY

        if not RESEND_API_KEY:
            raise RuntimeError("RESEND_API_KEY is not configured")
        resend.api_key = RESEND_API_KEY
        counts: Counter[str] = Counter()
        completion_messages = 0
        cursor = None
        inspected = 0
        while inspected < limit:
            params: dict[str, object] = {"limit": min(100, limit - inspected)}
            if cursor:
                params["after"] = cursor
            response = resend.Emails.list(params)
            data = response.get("data", []) if isinstance(response, dict) else []
            if not data:
                break
            stop = False
            for item in data:
                inspected += 1
                created_at = datetime.fromisoformat(str(item.get("created_at")).replace("Z", "+00:00"))
                if created_at < window.start_utc:
                    stop = True
                    break
                if created_at >= window.end_utc:
                    continue
                if "Your 3D Model is Ready" not in str(item.get("subject") or ""):
                    continue
                completion_messages += 1
                counts[str(item.get("last_event") or "unknown")] += 1
            if stop or not response.get("has_more"):
                break
            cursor = str(data[-1].get("id") or "")
            if not cursor:
                break
        return {
            "completion_emails": completion_messages,
            "email_statuses": dict(sorted(counts.items())),
            "email_error": None,
        }
    except Exception as exc:
        return {
            "completion_emails": 0,
            "email_statuses": {},
            "email_error": str(exc)[:180],
        }


def _duration(value: int) -> str:
    if value <= 0:
        return "-"
    minutes, seconds = divmod(value, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин {seconds} сек"


def _metric(label: str, value: object, detail: str = "") -> str:
    detail_html = (
        f'<div style="color:#667085;font-size:12px;margin-top:5px;">{html.escape(detail)}</div>'
        if detail
        else ""
    )
    return f"""
    <td width="25%" valign="top" style="padding:8px;">
      <div style="border:1px solid #d0d5dd;border-radius:6px;padding:14px;min-height:74px;background:#ffffff;">
        <div style="color:#667085;font-size:12px;">{html.escape(label)}</div>
        <div style="color:#101828;font-size:24px;font-weight:700;margin-top:5px;">{html.escape(str(value))}</div>
        {detail_html}
      </div>
    </td>"""


def render_html(stats: dict, window: ReportWindow, *, test: bool) -> str:
    period = (
        f"{window.start_local:%d.%m.%Y %H:%M} - "
        f"{window.end_local:%d.%m.%Y %H:%M} (Новосибирск)"
    )
    completion_rate = (
        round(100 * int(stats["tasks_completed"]) / int(stats["tasks_created"]))
        if stats["tasks_created"]
        else 0
    )
    worker_rows = "".join(
        f"<tr><td style='padding:8px;border-bottom:1px solid #eaecf0;'>{html.escape(name)}</td>"
        f"<td align='right' style='padding:8px;border-bottom:1px solid #eaecf0;font-weight:600;'>{count}</td></tr>"
        for name, count in stats.get("workers", {}).items()
    ) or "<tr><td colspan='2' style='padding:10px;color:#667085;'>Нет завершённых задач</td></tr>"
    task_rows = "".join(
        "<tr>"
        f"<td style='padding:8px;border-bottom:1px solid #eaecf0;'><a href='https://autorig.online/task?id={html.escape(row['id'])}' style='color:#087e8b;text-decoration:none;'>{html.escape(row['id'][:8])}</a></td>"
        f"<td style='padding:8px;border-bottom:1px solid #eaecf0;'>{html.escape(row['worker'])}</td>"
        f"<td style='padding:8px;border-bottom:1px solid #eaecf0;'>{html.escape(row['owner_type'])}</td>"
        f"<td align='right' style='padding:8px;border-bottom:1px solid #eaecf0;'>{html.escape(_duration(row['duration_seconds'] or 0))}</td>"
        "</tr>"
        for row in stats.get("completed_tasks", [])
    ) or "<tr><td colspan='4' style='padding:10px;color:#667085;'>Нет завершённых задач</td></tr>"
    failed_rows = "".join(
        f"<li style='margin:6px 0;'><a href='https://autorig.online/task?id={html.escape(row['id'])}' style='color:#b42318;'>{html.escape(row['id'][:8])}</a>: {html.escape(row['error'])}</li>"
        for row in stats.get("failed_tasks", [])
    )
    failed_block = (
        f"<div style='margin-top:20px;border-left:4px solid #d92d20;background:#fef3f2;padding:12px 16px;'><strong>Ошибки задач</strong><ul style='margin:8px 0 0;padding-left:20px;'>{failed_rows}</ul></div>"
        if failed_rows
        else ""
    )
    download_detail = ", ".join(
        f"{name}: {count}" for name, count in stats.get("download_kinds", {}).items()
    ) or "успешные download-запросы"
    email_detail = ", ".join(
        f"{name}: {count}" for name, count in stats.get("email_statuses", {}).items()
    ) or (stats.get("email_error") or "писем не было")
    email_detail = (
        f"{email_detail}; user-задач {stats['registered_tasks_completed']}"
    )
    test_banner = (
        "<div style='background:#fffaeb;border-bottom:1px solid #fedf89;color:#93370d;padding:12px 24px;text-align:center;font-weight:700;'>ТЕСТОВОЕ ПИСЬМО - расписание ещё не включено</div>"
        if test
        else ""
    )
    return f"""<!doctype html>
<html lang="ru">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;background:#f2f4f7;font-family:Arial,Helvetica,sans-serif;color:#101828;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f2f4f7;padding:24px 10px;"><tr><td align="center">
<table role="presentation" width="680" cellpadding="0" cellspacing="0" style="width:100%;max-width:680px;background:#ffffff;border:1px solid #d0d5dd;border-radius:8px;overflow:hidden;">
<tr><td>{test_banner}<div style="background:#101828;padding:24px;"><div style="color:#ffffff;font-size:24px;font-weight:700;">AutoRig.online</div><div style="color:#98f5e1;font-size:15px;margin-top:6px;">Ежедневная операционная сводка</div><div style="color:#d0d5dd;font-size:12px;margin-top:10px;">{html.escape(period)}</div></div></td></tr>
<tr><td style="padding:18px 16px 8px;"><table width="100%" cellpadding="0" cellspacing="0"><tr>
{_metric('Создано задач', stats['tasks_created'], f"завершено {completion_rate}%")}
{_metric('Завершено', stats['tasks_completed'], f"среднее {_duration(int(stats['average_duration_seconds']))}")}
{_metric('Ошибки', stats['tasks_failed'], f"в работе сейчас {stats['tasks_processing_now']}")}
{_metric('Новые пользователи', stats['new_users'])}
</tr><tr>
{_metric('Скачивания', stats['downloads'], f"уникальных IP {stats['download_clients']}")}
{_metric('Покупки файлов', stats['internal_purchases'], f"списано {stats['credits_spent']} credits")}
{_metric('Gumroad', stats['gumroad_sales'], f"${int(stats['gumroad_revenue_cents']) / 100:.2f}")}
{_metric('Completion email', stats['completion_emails'], email_detail)}
</tr></table></td></tr>
<tr><td style="padding:10px 24px;"><div style="font-size:12px;color:#667085;">Скачивания: {html.escape(download_detail)}. Checkout-переходы: {stats['checkout_intents']}. Подтверждённые crypto-платежи: {stats['crypto_credited']}.</div>{failed_block}</td></tr>
<tr><td style="padding:14px 24px 8px;"><h2 style="font-size:17px;margin:0 0 8px;">Завершения по воркерам</h2><table width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;">{worker_rows}</table></td></tr>
<tr><td style="padding:14px 24px 24px;"><h2 style="font-size:17px;margin:0 0 8px;">Последние завершённые задачи</h2><table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;"><tr><th align="left" style="padding:8px;background:#f9fafb;">Задача</th><th align="left" style="padding:8px;background:#f9fafb;">Воркер</th><th align="left" style="padding:8px;background:#f9fafb;">Тип</th><th align="right" style="padding:8px;background:#f9fafb;">Время</th></tr>{task_rows}</table></td></tr>
<tr><td style="background:#f9fafb;border-top:1px solid #eaecf0;padding:16px 24px;color:#667085;font-size:11px;">Данные: AutoRig SQLite, nginx access log и Resend API. Период отчёта фиксирован по Asia/Novosibirsk.</td></tr>
</table></td></tr></table></body></html>"""


def ensure_report_log(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_email_reports (
            report_key TEXT PRIMARY KEY,
            recipient TEXT NOT NULL,
            provider_message_id TEXT,
            status TEXT NOT NULL,
            error TEXT,
            created_at DATETIME NOT NULL,
            sent_at DATETIME
        )
        """
    )
    connection.commit()


def reserve_report(database: Path, report_key: str, recipient: str, force: bool) -> bool:
    connection = sqlite3.connect(database)
    try:
        ensure_report_log(connection)
        existing = connection.execute(
            "SELECT status FROM daily_email_reports WHERE report_key = ?", (report_key,)
        ).fetchone()
        if existing and existing[0] in {"accepted", "delivered"} and not force:
            return False
        connection.execute(
            """
            INSERT INTO daily_email_reports (report_key, recipient, status, created_at)
            VALUES (?, ?, 'sending', ?)
            ON CONFLICT(report_key) DO UPDATE SET
                recipient=excluded.recipient, status='sending', error=NULL,
                created_at=excluded.created_at
            """,
            (report_key, recipient, datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")),
        )
        connection.commit()
        return True
    finally:
        connection.close()


def finish_report(
    database: Path, report_key: str, status: str, message_id: str | None, error: str | None
) -> None:
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            UPDATE daily_email_reports
            SET status = ?, provider_message_id = ?, error = ?, sent_at = ?
            WHERE report_key = ?
            """,
            (
                status,
                message_id,
                error,
                datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" "),
                report_key,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def send_report(recipient: str, subject: str, report_html: str) -> str:
    import resend
    from config import EMAIL_FROM, RESEND_API_KEY

    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not configured")
    resend.api_key = RESEND_API_KEY
    response = resend.Emails.send(
        {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [recipient],
            "subject": subject,
            "html": report_html,
        }
    )
    message_id = response.get("id") if isinstance(response, dict) else getattr(response, "id", None)
    if not message_id:
        raise RuntimeError(f"Resend did not return a message id: {response!r}")
    return str(message_id)


def _database_from_config() -> Path:
    from config import DATABASE_URL

    prefix = "sqlite+aiosqlite:///"
    if not DATABASE_URL.startswith(prefix):
        raise RuntimeError("Daily report currently supports the production SQLite database only")
    raw = DATABASE_URL[len(prefix) :]
    return (BACKEND_DIR / raw).resolve() if not raw.startswith("/") else Path(raw)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--to", default=DEFAULT_RECIPIENT)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--nginx-log", action="append", type=Path)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--now", help="ISO timestamp used for deterministic diagnostics")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    now = datetime.fromisoformat(args.now) if args.now else None
    window = report_window(now)
    database = (args.database or _database_from_config()).resolve()
    if args.nginx_log:
        log_paths = args.nginx_log
    else:
        active_log = Path("/var/log/nginx/access.log")
        previous_log = Path("/var/log/nginx/access.log.1")
        log_paths = [active_log, previous_log]
        if not previous_log.exists():
            log_paths.append(Path("/var/log/nginx/access.log.1.gz"))
    stats = collect_database_stats(database, window)
    stats.update(collect_download_stats(log_paths, window))
    stats.update(collect_email_delivery_stats(window))
    report_html = render_html(stats, window, test=args.test)
    if args.output:
        args.output.write_text(report_html, encoding="utf-8")
    if args.dry_run:
        print(json.dumps({"window": window.key, "stats": stats}, ensure_ascii=False, indent=2))
        return 0

    report_key = f"test:{uuid.uuid4()}" if args.test else window.key
    if not reserve_report(database, report_key, args.to, args.force):
        print(f"[DailyReport] Already sent: {report_key}")
        return 0
    subject_prefix = "[ТЕСТ] " if args.test else ""
    subject = f"{subject_prefix}AutoRig: сводка за 24 часа · {window.end_local:%d.%m.%Y}"
    try:
        message_id = send_report(args.to, subject, report_html)
        finish_report(database, report_key, "accepted", message_id, None)
        print(f"[DailyReport] Accepted by Resend: {message_id}")
        return 0
    except Exception as exc:
        finish_report(database, report_key, "failed", None, str(exc)[:500])
        print(f"[DailyReport] Send failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
