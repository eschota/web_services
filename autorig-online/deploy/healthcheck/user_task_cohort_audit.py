"""Observe a fixed cohort of incoming customer tasks without mutating production.

Run with the system Python on way-fr. All production SQLite connections use
mode=ro and query_only. Only the separate audit state and report are written.
"""
from __future__ import annotations

import argparse
import ast
import collections
import datetime as dt
import hashlib
import json
import os
import re
import secrets
import sqlite3
import statistics
import subprocess
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

UTC = dt.timezone.utc
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
EMAIL = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
ACCESS = re.compile(r'^(\S+) .*?\[([^]]+)\] "([A-Z]+) ([^ ]+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"')
AUTOMATION_UA = re.compile(r"bot|spider|crawler|curl|python|httpx|headless|playwright|audit|monitor|wget|node-fetch", re.I)
DB_FIELDS = (
    "id owner_type owner_id created_at updated_at status input_url input_type input_bytes "
    "pipeline_kind created_via_api queue_class worker_api worker_task_id processing_started_at "
    "last_progress_at ready_count total_count video_ready restart_count source_attempt_count "
    "stuck_hour_requeue_count dispatch_not_before preemption_state preemption_count is_public "
    "collection_guid error_message artifact_cache_status artifact_cache_error viewer_settings "
    "telegram_new_notified_at telegram_done_notified_at"
).split()


def utc_now():
    return dt.datetime.now(UTC).isoformat()


def timestamp(value):
    if not value:
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, UTC)
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def sql_time(value):
    return timestamp(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def anonymize(value, state):
    return "user-" + hashlib.sha256((state["salt"] + "|" + str(value)).encode()).hexdigest()[:10]


def scrub(value):
    value = EMAIL.sub("[email]", str(value or ""))
    value = re.sub(r"https?://\S+", "[url]", value)
    return value[:400]


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def read_db(path):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def admin_emails(config_path):
    result = {"eschota@gmail.com", "vladkcg@gmail.com"}
    try:
        for node in ast.walk(ast.parse(config_path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ADMIN_EMAILS" for t in node.targets):
                result.update(str(x).lower() for x in ast.literal_eval(node.value))
    except (OSError, SyntaxError, ValueError):
        pass
    return result


def excluded_reason(row, admins, agent_ids):
    owner = str(row.get("owner_id") or "").lower()
    if str(row.get("queue_class") or "interactive") == "collection_background":
        return "automatic_background_collection"
    if owner in admins:
        return "administrator"
    if owner in agent_ids:
        return "registered_agent"
    if row.get("owner_type") not in {"user", "anon"}:
        return "non_customer_owner"
    if "/dev/api/scratch/" in str(row.get("input_url") or ""):
        return "developer_scratch_source"
    if str(row.get("input_url") or "").startswith(("http://127.0.0.1", "http://localhost")):
        return "local_test_source"
    return None


def update_members(connection, state, now, config_path):
    admins = admin_emails(config_path)
    agent_ids = {str(r[0]).lower() for r in connection.execute("SELECT anon_id FROM anon_sessions WHERE registered_as_agent = 1")}
    columns = {r[1] for r in connection.execute("PRAGMA table_info(tasks)")}
    fields = [x for x in DB_FIELDS if x in columns]
    candidates = connection.execute(
        "SELECT " + ",".join(fields) + " FROM tasks WHERE created_at >= ? ORDER BY created_at,id",
        (sql_time(state["started_at"]),),
    ).fetchall()
    for raw in candidates:
        row = dict(raw)
        task_id = row["id"]
        if task_id in state["members"]:
            continue
        reason = excluded_reason(row, admins, agent_ids)
        if reason:
            state["excluded"][task_id] = reason
            continue
        if len(state["members"]) >= state["target"]:
            break
        state["members"][task_id] = {"index": len(state["members"]) + 1, "task_id": task_id, "history": [], "http_events": [], "journal_events": []}
    for task_id, member in state["members"].items():
        raw = connection.execute("SELECT " + ",".join(fields) + " FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not raw:
            member["missing_from_live_db"] = True
            continue
        row = dict(raw)
        owner = row.pop("owner_id")
        previous = connection.execute("SELECT count(*) FROM tasks WHERE owner_type=? AND owner_id=? AND created_at<?", (row["owner_type"], owner, row["created_at"])).fetchone()[0]
        source = str(row.pop("input_url") or "")
        parsed = urlsplit(source)
        row["participant"] = anonymize(row["owner_type"] + ":" + owner, state)
        row["previous_task_count"] = previous
        row["source"] = "uploaded_file" if parsed.netloc == "autorig.online" and parsed.path.startswith("/u/") else "generated_3d" if "/renderfin/" in parsed.path else "remote_url"
        row["source_extension"] = Path(parsed.path).suffix.lower()[:10]
        row["source_fingerprint"] = hashlib.sha256((state["salt"] + source).encode()).hexdigest()[:16]
        row["error_message"] = scrub(row.get("error_message")) or None
        row["artifact_cache_error"] = scrub(row.get("artifact_cache_error")) or None
        settings = json.loads(row.pop("viewer_settings") or "{}")
        row["viewer_settings_keys"] = sorted(settings) if isinstance(settings, dict) else []
        member["current"] = row
        if "subscription_at_first_observation" not in member:
            subscription = connection.execute("SELECT autorig_subscription_status,autorig_subscription_period_end FROM users WHERE lower(email)=lower(?)", (owner,)).fetchone() if row["owner_type"] == "user" else None
            member["subscription_at_first_observation"] = dict(subscription) if subscription else None
        signature = [row.get(k) for k in ("status", "worker_api", "worker_task_id", "ready_count", "total_count", "video_ready", "restart_count", "preemption_count", "error_message", "is_public")]
        if member.get("last_signature") != signature:
            member["history"].append({"observed_at": now, **{k: row.get(k) for k in ("status", "worker_api", "worker_task_id", "ready_count", "total_count", "video_ready", "restart_count", "preemption_count", "error_message", "is_public")}})
            member["last_signature"] = signature
        completion = connection.execute("SELECT completed_at FROM rig_completion_events WHERE task_id=? ORDER BY completed_at DESC LIMIT 1", (task_id,)).fetchone()
        if completion:
            member["completed_at"] = completion[0]
        if row["status"] in {"done", "error"} and not member.get("terminal_first_observed_at"):
            member["terminal_first_observed_at"] = now
        member["completion_email"] = [dict(r) for r in connection.execute("SELECT status,attempt_count,sent_at,last_error FROM task_completion_emails WHERE task_id=?", (task_id,))]
        member["bundle_download_notice"] = [dict(r) for r in connection.execute("SELECT event_type,created_at FROM telegram_notifications WHERE event_type='bundle_download' AND event_key LIKE ?", (task_id + "%",))]
        intents = connection.execute("SELECT product_kind,created_at,used_at,auto_unlock_status FROM purchase_checkout_intents WHERE (task_id=? OR lower(user_email)=lower(?)) AND created_at>=?", (task_id, owner, row["created_at"])).fetchall()
        member["checkout_intents"] = [dict(r) for r in intents]
        member["payment_receipts"] = [dict(r) for r in connection.execute("SELECT product_permalink,price,refunded,test,created_at,credited,credits_added FROM gumroad_purchases WHERE lower(email)=lower(?) AND created_at>=?", (owner, row["created_at"]))]
        member["support_sessions"] = [dict(r) for r in connection.execute("SELECT id,created_at,updated_at FROM support_chat_sessions WHERE (page_url LIKE ? OR (lower(user_email)=lower(?) AND updated_at>=?))", ("%" + task_id + "%", owner, row["created_at"]))]
        support_messages = []
        for support in member["support_sessions"]:
            for message in connection.execute("SELECT direction,created_at,body_text FROM support_chat_messages WHERE session_id=? AND created_at>=?", (support["id"], row["created_at"])):
                text = str(message["body_text"]).lower()
                topics = [name for name, pattern in (
                    ("subscription_purchase", r"subscr|payment|not for sale|buy|подпис|оплат|купит"),
                    ("privacy_gallery", r"private|public|gallery|remove|приват|публич|галере|удал"),
                    ("failed_task", r"fail|error|stuck|ошиб|не работа|завис"),
                    ("download_export", r"download|export|fbx|glb|unity|unreal|скач|экспорт"),
                    ("model_quality", r"distort|quality|deform|bone|skin|качест|деформ|кость"),
                ) if re.search(pattern, text)]
                support_messages.append({"at": message["created_at"], "direction": message["direction"], "topics": topics or ["unclassified"]})
        member["support_messages"] = support_messages
        member["feedback_count"] = connection.execute("SELECT count(*) FROM feedback WHERE lower(user_email)=lower(?) AND created_at>=?", (owner, row["created_at"])).fetchone()[0]
    return fields


def http_kind(method, path):
    if path == "/task":
        return "task_page_view"
    if method in {"POST", "PUT", "PATCH"} and "viewer-settings" in path:
        return "viewer_settings_saved"
    if method in {"POST", "PUT", "PATCH"} and "visibility" in path:
        return "visibility_change"
    if method == "POST" and path.endswith("/retry"):
        return "retry_request"
    if path.endswith("/bundle.zip") or path.endswith("/downloads/bundle"):
        return "bundle_response"
    if "/download/" in path or path.endswith("/animations/download-pack"):
        return "file_download_response"
    if "/animations/preview/" in path:
        return "animation_preview"
    if re.fullmatch(r"/api/task/" + UUID.pattern, path, re.I):
        return "task_status_poll"
    if "checkout" in path:
        return "checkout_navigation"
    if path.endswith(("/model.glb", "/animations.glb")):
        return "viewer_asset_response"
    return None


def parse_access(line, state):
    match = ACCESS.match(line)
    if not match:
        return None
    ip, date, method, target, status, size, referrer, agent = match.groups()
    if AUTOMATION_UA.search(agent) or not agent.startswith("Mozilla/"):
        return None
    try:
        at = dt.datetime.strptime(date, "%d/%b/%Y:%H:%M:%S %z").astimezone(UTC)
    except ValueError:
        return None
    if at < timestamp(state["started_at"]):
        return None
    target_url = urlsplit(target)
    task_id = None
    for part in (target_url.path, parse_qs(target_url.query).get("id", [""])[0], referrer):
        found = UUID.search(part)
        if found and found.group().lower() in state["members"]:
            task_id = found.group().lower()
            break
    if not task_id:
        return None
    kind = http_kind(method, target_url.path)
    if not kind:
        return None
    return {"task_id": task_id, "at": at.isoformat(), "kind": kind, "method": method,
            "status": int(status), "response_bytes": int(size) if size.isdigit() else None,
            "visitor": anonymize(ip + "|" + agent, state),
            "artifact_extension": Path(target_url.path).suffix.lower()[:10],
            "referrer_host": urlsplit(referrer).netloc if referrer != "-" else None,
            "referrer_path": urlsplit(referrer).path if urlsplit(referrer).netloc == "autorig.online" else None,
            "device": "mobile" if re.search(r"Android|iPhone|Mobile", agent, re.I) else "desktop"}


def append_unique(member, key, event):
    if event not in member[key]:
        member[key].append(event)


def collect_access(state, path):
    if not path.exists():
        state["limitations"]["access_log"] = "missing"
        return
    stat = path.stat()
    cursor = state.setdefault("access_cursor", {"inode": stat.st_ino, "offset": stat.st_size})
    rotation = cursor["inode"] != stat.st_ino or stat.st_size < cursor["offset"]
    sources = []
    if rotation:
        for rotated in path.parent.glob(path.name + "*"):
            if rotated.is_file() and rotated.stat().st_ino == cursor["inode"]:
                sources.append((rotated, cursor["offset"]))
                break
        else:
            state["limitations"]["access_rotation_gap"] = "Previous log inode is unavailable; download/page-view counts are lower bounds."
        sources.append((path, 0))
    else:
        sources.append((path, cursor["offset"]))
    for source, offset in sources:
        with source.open("rb") as handle:
            handle.seek(offset)
            for binary in handle:
                event = parse_access(binary.decode("utf-8", errors="replace"), state)
                if event:
                    append_unique(state["members"][event.pop("task_id")], "http_events", event)
            if source == path:
                cursor.update(inode=stat.st_ino, offset=handle.tell())


def collect_journal(state, now):
    since = state.get("journal_until") or state["started_at"]
    result = subprocess.run(["journalctl", "-u", "autorig-storage.service", "--since", sql_time(since), "--until", sql_time(now), "-o", "json", "--no-pager"], capture_output=True, text=True, timeout=45)
    if result.returncode:
        state["limitations"]["journal"] = scrub(result.stderr)
        return
    for line in result.stdout.splitlines():
        try:
            entry = json.loads(line)
            message = str(entry.get("MESSAGE") or "")
            ids = set(UUID.findall(message)) & set(state["members"])
            if not ids:
                continue
            marker = next((name for name in ("broadcast_full_bundle_download", "[Tasks]", "[PreConvertMeta]", "[PreflightRender]", "[Bundle]", "[Workers]", "[Priority]", "[Email]", "[Telegram]") if name in message), None)
            if not marker:
                continue
            at = dt.datetime.fromtimestamp(int(entry["__REALTIME_TIMESTAMP"]) / 1e6, UTC).isoformat()
            for task_id in ids:
                append_unique(state["members"][task_id], "journal_events", {"at": at, "message": scrub(message)})
        except (ValueError, KeyError, TypeError):
            continue
    state["journal_until"] = now


def make_summary(state, now):
    members = list(state["members"].values())
    current = [m.get("current", {}) for m in members]
    queues, processing = [], []
    for member in members:
        row = member.get("current", {})
        created, started, finished = (timestamp(row.get("created_at")), timestamp(row.get("processing_started_at")), timestamp(member.get("completed_at")))
        if created and started:
            queues.append(max(0, (started - created).total_seconds()))
        if finished and started:
            processing.append(max(0, (finished - started).total_seconds()))
    terminal = sum(row.get("status") in {"done", "error"} for row in current)
    latest = max((timestamp(m.get("terminal_first_observed_at") or m.get("current", {}).get("created_at")) for m in members), default=timestamp(state["started_at"]))
    final_ready = len(members) == state["target"] and terminal == state["target"] and (timestamp(now) - latest).total_seconds() >= state["followup_hours"] * 3600
    return {"started_at": state["started_at"], "observed_at": now, "target": state["target"], "admitted": len(members), "unique_participants": len({r.get("participant") for r in current}),
            "status_counts": dict(collections.Counter(r.get("status", "unknown") for r in current)),
            "input_types": dict(collections.Counter(r.get("input_type", "unknown") for r in current)),
            "source_extensions": dict(collections.Counter(r.get("source_extension", "unknown") for r in current)),
            "channels": dict(collections.Counter("api" if r.get("created_via_api") else "website" for r in current)),
            "returning_task_count": sum(bool(r.get("previous_task_count")) for r in current),
            "queue_seconds_median": statistics.median(queues) if queues else None,
            "processing_seconds_median": statistics.median(processing) if processing else None,
            "download_response_task_count": sum(any(e["kind"] in {"bundle_response", "file_download_response"} and e["status"] in {200, 206} for e in m["http_events"]) for m in members),
            "support_task_count": sum(any(message.get("direction") == "user" for message in m.get("support_messages", [])) for m in members),
            "checkout_task_count": sum(bool(m.get("checkout_intents")) for m in members),
            "final_report_ready": final_ready, "limitations": state["limitations"]}


def report_markdown(state, summary):
    out = ["# Наблюдение за 20 реальными входящими задачами AutoRig", "", f"Начало (UTC): {state['started_at']}", f"Обновлено (UTC): {summary['observed_at']}", f"Выборка: {summary['admitted']}/{summary['target']}; пользователей: {summary['unique_participants']}.", "",
           "Участники отбираются по времени создания после начала наблюдения. Администраторы, зарегистрированные агенты, scratch-тесты и автоматические фоновые коллекции исключены. Повторная попытка под тем же task ID не увеличивает выборку; новая ручная отправка считается новой задачей.", "",
           "После терминального результата последнего участника продолжается 2 часа наблюдения за скачиваниями, повторными отправками, оплатой и поддержкой. Отсутствие зарегистрированного действия не доказывает уход пользователя.", "",
           "| № | Участник | Task | Канал / файл | Тип | Статус | Выдано | Видео |", "|---|---|---|---|---|---|---|---|"]
    for member in state["members"].values():
        r = member.get("current", {})
        out.append(f"| {member['index']} | {r.get('participant', '?')} | [{member['task_id'][:8]}](https://autorig.online/task?id={member['task_id']}) | {'API' if r.get('created_via_api') else 'Web'} / {r.get('source_extension', '?')} | {r.get('input_type', '?')} | {r.get('status', '?')} | {r.get('ready_count', 0)}/{r.get('total_count', 0)} | {bool(r.get('video_ready'))} |")
    out += ["", "## Измерения", "", "```json", json.dumps(summary, ensure_ascii=False, indent=2), "```", "", "## Ограничения интерпретации", "", "HTTP 200/206 при скачивании подтверждает ответ сервера и число переданных байтов, но не сохранение/использование модели на компьютере. Посетители task page обезличены по IP+User-Agent и не приравниваются к владельцу. Поллинги статуса не считаются отдельными визитами. Нажатие checkout не считается покупкой; email sent не считается прочитанным письмом. Причины ошибок и предложения по качеству требуют отдельного анализа фактов после заполнения выборки."]
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--db", type=Path, default=Path("/srv/autorig/data/db/autorig.db"))
    parser.add_argument("--access-log", type=Path, default=Path("/home/log/nginx/access.log"))
    parser.add_argument("--config", type=Path, default=Path("/srv/autorig/current/autorig-online/backend/config.py"))
    parser.add_argument("--target", type=int, default=20)
    args = parser.parse_args()
    args.audit_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(args.audit_dir, 0o700)
    now = utc_now()
    state_path = args.audit_dir / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        stat = args.access_log.stat()
        state = {"schema": 1, "started_at": now, "target": args.target, "followup_hours": 2, "salt": secrets.token_hex(24), "members": {}, "excluded": {}, "limitations": {}, "access_cursor": {"inode": stat.st_ino, "offset": stat.st_size}}
    with read_db(args.db) as connection:
        update_members(connection, state, now, args.config)
    collect_access(state, args.access_log)
    collect_journal(state, now)
    summary = make_summary(state, now)
    write_json(state_path, state)
    write_json(args.audit_dir / "summary.json", summary)
    (args.audit_dir / "observations.md").write_text(report_markdown(state, summary), encoding="utf-8")
    os.chmod(args.audit_dir / "observations.md", 0o600)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
