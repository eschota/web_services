#!/usr/bin/env python3
"""Intensive, self-expiring production monitor for the first 72h after cutover.

The normal AutoRig health check runs every six hours. This temporary safety net
runs every ten minutes, persists its cursor/state, records every run as JSONL,
and sends Telegram only when a new event appears, the active problem set
changes, or the service recovers.

Completion email is a first-class production dependency. The monitor watches
the durable per-task ledger, checks recent Resend events, and sends a
deterministic end-to-end probe to Resend's non-user test sink every 12 hours.
No customer receives a monitor probe.
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


AUTORIG_DB = os.getenv("AUTORIG_POSTMIGRATION_DB", "/srv/autorig/data/db/autorig.db")
STATE_DIR = Path(os.getenv("AUTORIG_POSTMIGRATION_STATE_DIR", "/srv/autorig/data/monitor"))
STATE_PATH = STATE_DIR / "postmigration-72h.json"
EVENTS_PATH = STATE_DIR / "postmigration-72h-events.jsonl"
COMPLETE_MARKER = STATE_DIR / "postmigration-72h.complete"

DURATION_SECONDS = float(os.getenv("AUTORIG_POSTMIGRATION_DURATION_HOURS", "72")) * 3600
FULL_CHECK_INTERVAL_SECONDS = float(
    os.getenv("AUTORIG_POSTMIGRATION_FULL_CHECK_MINUTES", "60")
) * 60
EMAIL_PROBE_INTERVAL_SECONDS = float(
    os.getenv("AUTORIG_POSTMIGRATION_EMAIL_PROBE_HOURS", "12")
) * 3600
EMAIL_GRACE_SECONDS = float(
    os.getenv("AUTORIG_POSTMIGRATION_EMAIL_GRACE_MINUTES", "15")
) * 60
CACHE_STALL_SECONDS = float(
    os.getenv("AUTORIG_POSTMIGRATION_CACHE_STALL_HOURS", "6")
) * 3600
MIN_FREE_GB = float(os.getenv("AUTORIG_POSTMIGRATION_MIN_FREE_GB", "120"))

SITE_URLS = tuple(
    value.strip()
    for value in os.getenv(
        "AUTORIG_POSTMIGRATION_URLS",
        "https://autorig.online/,https://autorig.online/api/gallery?per_page=1&sort=date,"
        "http://127.0.0.1:8210/renderfin/health",
    ).split(",")
    if value.strip()
)
SERVICES = tuple(
    value.strip()
    for value in os.getenv(
        "AUTORIG_POSTMIGRATION_SERVICES",
        "autorig-storage.service,autorig-storage-renderfin.service,"
        "autorig-storage-telegram.service,autorig-storage-tunnels.service,nginx.service,"
        "autorig-storage-disk-pressure-cleanup.timer,autorig-storage-healthcheck.timer",
    ).split(",")
    if value.strip()
)
LISTENERS = (("backend", 8200), ("renderfin", 8210))
JOURNAL_UNITS = tuple(
    value.strip()
    for value in os.getenv(
        "AUTORIG_POSTMIGRATION_JOURNAL_UNITS",
        "autorig-storage.service,autorig-storage-renderfin.service,"
        "autorig-storage-telegram.service,autorig-storage-tunnels.service,nginx.service,"
        "autorig-storage-disk-pressure-cleanup.service,autorig-storage-healthcheck.service",
    ).split(",")
    if value.strip()
)
FULL_HEALTHCHECK = os.getenv(
    "AUTORIG_POSTMIGRATION_FULL_HEALTHCHECK",
    "/srv/autorig/current/autorig-online/deploy/healthcheck/renderfin_healthcheck.py",
)
RESEND_TEST_RECIPIENT = "delivered@resend.dev"
ERROR_RE = re.compile(
    r"(?:traceback|\bexception\b|\bcritical\b|\bfatal\b|\berror\b|\bfailed\b|"
    r"timed?\s*out|connection refused|database is locked|no space left)",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
SECRET_RE = re.compile(r"(?i)(authorization|bearer|token|api[_-]?key)([\s:=]+)([^\s,;]+)")
BEARER_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f-]{27,36}\b", re.IGNORECASE)
JOURNAL_TIME_RE = re.compile(r"\b\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\b")
JOURNAL_PID_RE = re.compile(r"\b\d+#\d+\b")


def utc_iso(epoch: Optional[float] = None) -> str:
    return datetime.fromtimestamp(epoch or time.time(), tz=timezone.utc).isoformat()


def scrub(value: Any, limit: int = 500) -> str:
    text = EMAIL_RE.sub("<redacted-email>", str(value or ""))
    text = BEARER_RE.sub("Bearer <redacted>", text)
    text = SECRET_RE.sub(r"\1\2<redacted>", text)
    return " ".join(text.split())[:limit]


def _atomic_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def load_state(now: float) -> Dict[str, Any]:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("monitor state is not an object")
        return state
    except FileNotFoundError:
        return {
            "version": 1,
            "started_at": now,
            "ends_at": now + DURATION_SECONDS,
            "last_check_at": now - 60,
            "last_full_check_at": 0,
            "last_email_probe_at": 0,
            "last_active_signature": "",
            "last_active_problems": [],
            "baseline_max_task_rowid": None,
            "baseline_active_user_task_ids": [],
            "first_seen_done": {},
            "seen_journal_events": [],
            "provider_events": {},
            "email_probe_task_ids": [],
            "seen_email_events": [],
            "runs": 0,
            "task_errors_seen": 0,
            "journal_errors_seen": 0,
            "email_probes_sent": 0,
            "email_probes_delivered": 0,
            "email_probe_failures": 0,
        }


def save_state(state: Dict[str, Any]) -> None:
    _atomic_json(STATE_PATH, state)


def append_event(payload: Dict[str, Any]) -> None:
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def db_connect() -> sqlite3.Connection:
    db = sqlite3.connect(f"file:{AUTORIG_DB}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    return db


@contextmanager
def open_db() -> Iterable[sqlite3.Connection]:
    db = db_connect()
    try:
        yield db
    finally:
        db.close()


def check_endpoints(active: List[str], metrics: Dict[str, Any]) -> None:
    statuses: Dict[str, Any] = {}
    for url in SITE_URLS:
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "AutoRig-72h-monitor/1"})
            with urllib.request.urlopen(request, timeout=20) as response:
                statuses[url] = int(response.status)
                if response.status != 200:
                    active.append(f"endpoint {url} returned HTTP {response.status}")
        except Exception as exc:
            statuses[url] = scrub(repr(exc), 180)
            active.append(f"endpoint {url} unreachable: {scrub(repr(exc), 180)}")
    metrics["http"] = statuses


def check_services(active: List[str], metrics: Dict[str, Any]) -> None:
    states: Dict[str, str] = {}
    for unit in SERVICES:
        try:
            result = subprocess.run(
                ["systemctl", "is-active", unit], capture_output=True, text=True, timeout=15
            )
            state = result.stdout.strip() or result.stderr.strip() or "unknown"
        except Exception as exc:
            state = scrub(repr(exc), 160)
        states[unit] = state
        if state != "active":
            active.append(f"{unit} is {state}")
    for label, port in LISTENERS:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5):
                states[f"listener:{label}"] = "open"
        except Exception as exc:
            states[f"listener:{label}"] = scrub(repr(exc), 120)
            active.append(f"{label} listener 127.0.0.1:{port} is unavailable")
    metrics["services"] = states


def check_disk(active: List[str], metrics: Dict[str, Any]) -> None:
    usage = shutil.disk_usage("/srv/autorig")
    free_gb = usage.free / (1024**3)
    used_percent = (usage.total - usage.free) / usage.total * 100
    metrics["disk_free_gb"] = round(free_gb, 2)
    metrics["disk_used_percent"] = round(used_percent, 2)
    if free_gb < MIN_FREE_GB:
        active.append(
            f"storage reserve is {free_gb:.1f} GB, below required {MIN_FREE_GB:.1f} GB"
        )


def initialize_task_baseline(state: Dict[str, Any], db: sqlite3.Connection) -> None:
    if state.get("baseline_max_task_rowid") is not None:
        return
    state["baseline_max_task_rowid"] = int(
        db.execute("SELECT COALESCE(MAX(rowid), 0) FROM tasks").fetchone()[0]
    )
    rows = db.execute(
        "SELECT id FROM tasks WHERE owner_type = 'user' AND status NOT IN ('done', 'error')"
    ).fetchall()
    state["baseline_active_user_task_ids"] = [str(row[0]) for row in rows]


def _eligible_completion_candidates(
    state: Dict[str, Any], db: sqlite3.Connection
) -> List[sqlite3.Row]:
    baseline = int(state.get("baseline_max_task_rowid") or 0)
    active_at_start = [str(value) for value in state.get("baseline_active_user_task_ids") or []]
    clauses = ["t.rowid > ?"]
    params: List[Any] = [baseline]
    if active_at_start:
        clauses.append("t.id IN (%s)" % ",".join("?" for _ in active_at_start))
        params.extend(active_at_start)
    return db.execute(
        f"""
        SELECT t.id, t.status, e.status AS email_status, e.attempt_count,
               e.provider_message_id, e.last_error,
               u.id AS user_id, u.email_invalid_at, u.email_task_completed
        FROM tasks t
        LEFT JOIN users u ON lower(u.email) = lower(t.owner_id)
        LEFT JOIN task_completion_emails e ON e.task_id = t.id
        WHERE t.owner_type = 'user'
          AND ({' OR '.join(clauses)})
          AND t.status IN ('done', 'error')
        """,
        params,
    ).fetchall()


def _sqlite_epoch(value: Any) -> Optional[float]:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


def audit_completion_email(
    state: Dict[str, Any],
    now: float,
    active: List[str],
    events: List[str],
    metrics: Dict[str, Any],
) -> List[Tuple[str, str]]:
    provider_rows: List[Tuple[str, str]] = []
    with open_db() as db:
        quick = str(db.execute("PRAGMA quick_check").fetchone()[0])
        metrics["sqlite_quick_check"] = quick
        if quick.lower() != "ok":
            active.append(f"autorig SQLite quick_check: {quick}")
        initialize_task_baseline(state, db)
        rows = db.execute(
            """
            SELECT task_id, status, attempt_count, provider_message_id, last_error,
                   claimed_at, sent_at, updated_at
            FROM task_completion_emails
            WHERE created_at >= datetime(?, 'unixepoch')
            """,
            (float(state["started_at"]),),
        ).fetchall()
        metrics["completion_email_statuses"] = dict(
            Counter(str(row["status"] or "unknown") for row in rows)
        )
        for row in rows:
            task_id = str(row["task_id"])
            if row["status"] == "failed":
                active.append(
                    f"completion email failed for {task_id[:8]}: {scrub(row['last_error'], 180)}"
                )
            if row["status"] == "sending":
                claimed = _sqlite_epoch(row["claimed_at"])
                if claimed and now - claimed > EMAIL_GRACE_SECONDS:
                    active.append(f"completion email {task_id[:8]} stuck in sending")
            if int(row["attempt_count"] or 0) > 1:
                event_key = f"retry:{task_id}:{int(row['attempt_count'])}"
                seen_email_events = set(state.get("seen_email_events") or [])
                if event_key not in seen_email_events:
                    events.append(
                        f"completion email {task_id[:8]} required "
                        f"{int(row['attempt_count'])} attempts"
                    )
                    seen_email_events.add(event_key)
                    state["seen_email_events"] = sorted(seen_email_events)[-1000:]
            if row["status"] == "sent" and row["provider_message_id"]:
                provider_rows.append((task_id, str(row["provider_message_id"])))
        duplicates = db.execute(
            """
            SELECT provider_message_id, COUNT(*) AS n
            FROM task_completion_emails
            WHERE provider_message_id IS NOT NULL
              AND created_at >= datetime(?, 'unixepoch')
            GROUP BY provider_message_id HAVING COUNT(*) > 1
            """,
            (float(state["started_at"]),),
        ).fetchall()
        if duplicates:
            active.append(f"completion email ledger has {len(duplicates)} duplicate provider IDs")
        first_seen = dict(state.get("first_seen_done") or {})
        for row in _eligible_completion_candidates(state, db):
            task_id = str(row["id"])
            if row["status"] == "error":
                first_seen.pop(task_id, None)
                continue
            opted_out = row["user_id"] is not None and not bool(row["email_task_completed"])
            if row["email_invalid_at"] or opted_out:
                first_seen.pop(task_id, None)
                continue
            if row["email_status"]:
                first_seen.pop(task_id, None)
                continue
            seen_at = float(first_seen.setdefault(task_id, now))
            if now - seen_at > EMAIL_GRACE_SECONDS:
                active.append(f"completed user task {task_id[:8]} has no email ledger row")
        state["first_seen_done"] = first_seen
    return provider_rows


def check_artifact_cache(active: List[str], metrics: Dict[str, Any], now: float) -> None:
    with open_db() as db:
        counts = {
            str(row[0]): int(row[1])
            for row in db.execute(
                "SELECT status, COUNT(*) FROM artifact_cache_jobs GROUP BY status"
            ).fetchall()
        }
        metrics["artifact_cache_jobs"] = counts
        recent_failed = int(
            db.execute(
                """
                SELECT COUNT(*) FROM artifact_cache_jobs
                WHERE status = 'failed' AND updated_at >= datetime(?, 'unixepoch')
                """,
                (now - 3600,),
            ).fetchone()[0]
        )
        stale = db.execute(
            """
            SELECT task_id, status, updated_at FROM artifact_cache_jobs
            WHERE status IN ('pending', 'caching')
              AND updated_at < datetime(?, 'unixepoch')
            ORDER BY updated_at LIMIT 5
            """,
            (now - CACHE_STALL_SECONDS,),
        ).fetchall()
        if recent_failed:
            active.append(f"artifact cache has {recent_failed} newly failed job(s) in 1h")
        if stale:
            active.append(
                f"artifact cache has {len(stale)} sampled job(s) stalled > "
                f"{CACHE_STALL_SECONDS / 3600:.0f}h: "
                + ", ".join(str(row["task_id"])[:8] for row in stale)
            )


def collect_task_errors(
    state: Dict[str, Any], since: float, now: float, events: List[str], metrics: Dict[str, Any]
) -> None:
    with open_db() as db:
        rows = db.execute(
            """
            SELECT id, error_message FROM tasks
            WHERE status = 'error'
              AND updated_at >= datetime(?, 'unixepoch')
              AND updated_at < datetime(?, 'unixepoch')
            ORDER BY updated_at
            """,
            (since, now + 1),
        ).fetchall()
    metrics["new_task_errors"] = len(rows)
    if not rows:
        return
    groups = Counter(_error_class(row["error_message"]) for row in rows)
    state["task_errors_seen"] = int(state.get("task_errors_seen") or 0) + len(rows)
    summary = ", ".join(f"{name}={count}" for name, count in groups.most_common(8))
    events.append(f"{len(rows)} new failed task(s): {summary}")


def _error_class(message: Any) -> str:
    text = scrub(message, 240).lower()
    markers = (
        ("animation retargeting", "animation_retargeting"),
        ("unity export failed", "unity_export"),
        ("auto forward failed", "animal_auto_forward"),
        ("non_manifold", "non_manifold"),
        ("watchdog timed out", "watchdog_timeout"),
        ("no_mesh", "no_mesh"),
        ("source asset unavailable", "source_unavailable"),
    )
    for marker, label in markers:
        if marker in text:
            return label
    return (text[:80] or "unknown").replace(",", ";")


def journal_signature(unit: str, message: str) -> str:
    normalized = scrub(message, 1000).lower()
    normalized = JOURNAL_TIME_RE.sub("<time>", normalized)
    normalized = JOURNAL_PID_RE.sub("<pid>", normalized)
    normalized = UUID_RE.sub("<uuid>", normalized)
    return hashlib.sha256(f"{unit}|{normalized}".encode("utf-8", "replace")).hexdigest()[:24]


def collect_journal_errors(
    state: Dict[str, Any], since: float, now: float, events: List[str], metrics: Dict[str, Any]
) -> None:
    command = [
        "journalctl", "--no-pager", "--output=json", "--since", f"@{max(0, since - 2):.3f}",
        "--until", f"@{now + 1:.3f}",
    ]
    for unit in JOURNAL_UNITS:
        command.extend(("--unit", unit))
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=40)
    except Exception as exc:
        events.append(f"journal scan failed: {scrub(repr(exc), 180)}")
        return
    if result.returncode != 0:
        events.append(f"journal scan failed: {scrub(result.stderr, 180)}")
        return
    seen = set(str(value) for value in state.get("seen_journal_events") or [])
    added: List[str] = []
    new_hashes: List[str] = []
    occurrences: List[str] = []
    for line in result.stdout.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        message = str(entry.get("MESSAGE") or "")
        priority = int(entry.get("PRIORITY") or 6)
        if priority > 3 and not ERROR_RE.search(message):
            continue
        unit = str(entry.get("_SYSTEMD_UNIT") or entry.get("SYSLOG_IDENTIFIER") or "journal")
        sample = f"{unit}: {scrub(message, 260)}"
        occurrences.append(sample)
        event_hash = journal_signature(unit, message)
        if event_hash in seen:
            continue
        seen.add(event_hash)
        new_hashes.append(event_hash)
        added.append(sample)
    if added:
        events.append(
            f"{len(added)} new journal error signature(s): " + " | ".join(added[:6])
            + (" | …" if len(added) > 6 else "")
        )
    state["journal_errors_seen"] = int(state.get("journal_errors_seen") or 0) + len(
        occurrences
    )
    state["seen_journal_events"] = (
        list(state.get("seen_journal_events") or []) + new_hashes
    )[-4000:]
    metrics["journal_error_occurrences"] = len(occurrences)
    metrics["new_journal_error_signatures"] = len(added)
    metrics["journal_error_samples"] = occurrences[:6]


def run_full_healthcheck(state: Dict[str, Any], now: float, active: List[str]) -> None:
    if now - float(state.get("last_full_check_at") or 0) < FULL_CHECK_INTERVAL_SECONDS:
        active.extend(str(value) for value in state.get("last_full_problems") or [])
        return
    try:
        result = subprocess.run(
            [sys.executable, FULL_HEALTHCHECK], capture_output=True, text=True, timeout=240
        )
        problems = [
            scrub(line, 500)
            for line in result.stdout.splitlines()
            if line.startswith(("❌", "⚠️"))
        ]
        if result.returncode not in (0, 1):
            problems.append(f"full healthcheck exited {result.returncode}: {scrub(result.stderr, 180)}")
    except Exception as exc:
        problems = [f"full healthcheck failed: {scrub(repr(exc), 180)}"]
    state["last_full_check_at"] = now
    state["last_full_problems"] = problems
    active.extend(problems)


def send_email_probe(state: Dict[str, Any], now: float, active: List[str]) -> None:
    if now - float(state.get("last_email_probe_at") or 0) < EMAIL_PROBE_INTERVAL_SECONDS:
        return
    bucket = int((now - float(state["started_at"])) // EMAIL_PROBE_INTERVAL_SECONDS)
    task_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"autorig-postmigration-email-probe:{state['started_at']}:{bucket}",
        )
    )
    guid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{task_id}:guid"))
    try:
        from email_service import send_task_completed_email

        sent = asyncio.run(
            send_task_completed_email(
                RESEND_TEST_RECIPIENT, task_id, guid, "https://autorig.online"
            )
        )
        with open_db() as db:
            row = db.execute(
                "SELECT status, provider_message_id, last_error "
                "FROM task_completion_emails WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row and row["status"] == "sent" and row["provider_message_id"]:
            state["last_email_probe_at"] = now
            probe_ids = list(state.get("email_probe_task_ids") or [])
            if task_id not in probe_ids:
                probe_ids.append(task_id)
                state["email_probes_sent"] = int(state.get("email_probes_sent") or 0) + 1
            state["email_probe_task_ids"] = probe_ids
        else:
            state["email_probe_failures"] = int(state.get("email_probe_failures") or 0) + 1
            detail = row["last_error"] if row else f"send returned {sent!r} without ledger row"
            active.append(f"end-to-end completion email probe failed: {scrub(detail, 180)}")
    except Exception as exc:
        state["email_probe_failures"] = int(state.get("email_probe_failures") or 0) + 1
        active.append(f"end-to-end completion email probe crashed: {scrub(repr(exc), 180)}")


def check_resend_events(
    state: Dict[str, Any], provider_rows: Sequence[Tuple[str, str]], now: float, active: List[str]
) -> None:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    if not api_key:
        active.append("RESEND_API_KEY is absent from monitor environment")
        return
    known = dict(state.get("provider_events") or {})
    probe_ids = set(str(value) for value in state.get("email_probe_task_ids") or [])
    delivered_probes = 0
    for task_id, provider_id in provider_rows[-25:]:
        previous = known.get(provider_id) or {}
        last_checked = float(previous.get("checked_at") or 0)
        last_event = str(previous.get("last_event") or "")
        if last_event in {"delivered", "opened", "clicked"} and now - last_checked < 12 * 3600:
            if task_id in probe_ids:
                delivered_probes += 1
            continue
        request = urllib.request.Request(
            f"https://api.resend.com/emails/{urllib.parse.quote(provider_id, safe='')}",
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "AutoRig-72h-monitor/1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
            last_event = str(payload.get("last_event") or "unknown").lower()
            created_at = _sqlite_epoch(payload.get("created_at"))
            known[provider_id] = {
                "last_event": last_event,
                "checked_at": now,
                "task_id": task_id,
                "created_at": created_at,
            }
        except Exception as exc:
            active.append(f"Resend event lookup failed for {task_id[:8]}: {scrub(repr(exc), 160)}")
            continue
        if last_event in {"bounced", "complained", "failed", "canceled"}:
            active.append(f"Resend reports {last_event} for completion email {task_id[:8]}")
        elif last_event not in {"delivered", "opened", "clicked"}:
            created_at = float((known.get(provider_id) or {}).get("created_at") or now)
            if now - created_at > EMAIL_GRACE_SECONDS:
                active.append(
                    f"Resend completion email {task_id[:8]} remains {last_event} "
                    f"for {(now - created_at) / 60:.0f}m"
                )
        if task_id in probe_ids and last_event in {"delivered", "opened", "clicked"}:
            delivered_probes += 1
    state["provider_events"] = known
    state["email_probes_delivered"] = max(
        int(state.get("email_probes_delivered") or 0), delivered_probes
    )


def telegram_notify(title: str, lines: Iterable[str]) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = (
        os.getenv("HEALTHCHECK_CHAT_ID", "").strip()
        or os.getenv("TELEGRAM_NOTIFICATION_CHAT_ID", "").strip()
    )
    if not token or not chat_id:
        print("[postmigration] Telegram notify skipped: token/chat missing")
        return False
    body = "\n".join(scrub(line, 900) for line in lines)
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": f"🛡 <b>{html.escape(title)}</b>\n{html.escape(body)}"[:4000],
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
    ).encode()
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=30
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return bool(payload.get("ok"))
    except Exception as exc:
        print(f"[postmigration] Telegram notify failed: {scrub(repr(exc), 180)}")
        return False


def check_telegram_api(active: List[str], metrics: Dict[str, Any]) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        active.append("TELEGRAM_BOT_TOKEN is absent from monitor environment")
        metrics["telegram_api"] = "token_missing"
        return
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/getMe", timeout=20
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not payload.get("ok"):
            raise RuntimeError("Telegram getMe returned ok=false")
        metrics["telegram_api"] = "ok"
    except Exception as exc:
        metrics["telegram_api"] = scrub(repr(exc), 160)
        active.append(f"Telegram Bot API probe failed: {scrub(repr(exc), 160)}")


def _signature(values: Sequence[str]) -> str:
    return hashlib.sha256("\n".join(sorted(set(values))).encode("utf-8")).hexdigest()


def finish_monitor(state: Dict[str, Any], now: float) -> None:
    if COMPLETE_MARKER.exists():
        return
    summary = [
        f"72-hour window completed at {utc_iso(now)}",
        f"runs: {int(state.get('runs') or 0)}",
        f"task errors observed: {int(state.get('task_errors_seen') or 0)}",
        f"journal errors observed: {int(state.get('journal_errors_seen') or 0)}",
        f"email probes delivered: {int(state.get('email_probes_delivered') or 0)}/"
        f"{int(state.get('email_probes_sent') or 0)}",
        f"email probe failures: {int(state.get('email_probe_failures') or 0)}",
        f"active problems at close: {len(state.get('last_active_problems') or [])}",
    ]
    telegram_notify("AutoRig 72h monitoring complete", summary)
    COMPLETE_MARKER.write_text(
        json.dumps({"completed_at": now, "summary": summary}), encoding="utf-8"
    )
    state["completed_at"] = now
    save_state(state)
    append_event({"at": utc_iso(now), "kind": "completed", "summary": summary})


def main() -> int:
    now = time.time()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state(now)
    if now >= float(state["ends_at"]):
        finish_monitor(state, now)
        print("post-migration monitoring window is complete")
        return 0

    since = min(now, float(state.get("last_check_at") or now - 60))
    active: List[str] = []
    events: List[str] = []
    metrics: Dict[str, Any] = {}
    check_endpoints(active, metrics)
    check_services(active, metrics)
    check_disk(active, metrics)
    check_telegram_api(active, metrics)
    try:
        audit_completion_email(state, now, active, events, metrics)
        check_artifact_cache(active, metrics, now)
        collect_task_errors(state, since, now, events, metrics)
    except Exception as exc:
        active.append(f"database monitoring failed: {scrub(repr(exc), 220)}")
    collect_journal_errors(state, since, now, events, metrics)
    run_full_healthcheck(state, now, active)
    send_email_probe(state, now, active)
    try:
        with open_db() as db:
            provider_rows = [
                (str(row[0]), str(row[1]))
                for row in db.execute(
                    """
                    SELECT task_id, provider_message_id FROM task_completion_emails
                    WHERE status = 'sent' AND provider_message_id IS NOT NULL
                      AND created_at >= datetime(?, 'unixepoch')
                    ORDER BY created_at
                    """,
                    (float(state["started_at"]),),
                ).fetchall()
            ]
        check_resend_events(state, provider_rows, now, active)
    except Exception as exc:
        active.append(f"completion email provider audit failed: {scrub(repr(exc), 180)}")

    active = sorted(set(active))
    state["runs"] = int(state.get("runs") or 0) + 1
    state["last_check_at"] = now
    previous_active = list(state.get("last_active_problems") or [])
    previous_signature = str(state.get("last_active_signature") or "")
    active_signature = _signature(active)
    state["last_active_signature"] = active_signature
    state["last_active_problems"] = active
    notification_lines: List[str] = []
    title = "AutoRig 72h monitor"
    if events:
        notification_lines.extend(f"NEW: {value}" for value in events)
    if active and active_signature != previous_signature:
        notification_lines.extend(f"ACTIVE: {value}" for value in active[:15])
    elif previous_active and not active:
        title = "AutoRig recovered"
        notification_lines.append("All previously active monitor findings are clear.")
    if int(state["runs"]) == 1:
        notification_lines.insert(
            0,
            f"Monitoring started; automatic stop at {utc_iso(float(state['ends_at']))}.",
        )
    if notification_lines:
        telegram_notify(title, notification_lines)

    record = {
        "at": utc_iso(now),
        "run": state["runs"],
        "window_ends_at": utc_iso(float(state["ends_at"])),
        "active": active,
        "events": events,
        "metrics": metrics,
    }
    append_event(record)
    save_state(state)
    print(json.dumps(record, indent=2, sort_keys=True))
    return 1 if active or events else 0


if __name__ == "__main__":
    sys.exit(main())
