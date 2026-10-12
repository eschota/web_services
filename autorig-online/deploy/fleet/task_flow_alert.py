#!/usr/bin/env python3
"""CRITICAL DEV alerts on the task flow (owner 2026-10-12). Run by autorig-task-flow-alert.timer (root, every 5 min).

  * no new task created for 3 hours (tasks.created_at): alert at once, then every hour while it lasts, with the probable
    cause attached (new_tasks_paused, VPS disk vs reserve, 5xx on /api/task/create, MT health, load);
  * >= 5 errors (5xx) on POST /api/task/create in the last 10 minutes (nginx access log), re-alert every 30 min.
State: /var/lib/autorig-task-flow-alert.json. Messages are UTF-8 multipart to the DEV channel.
"""
import calendar
import json
import os
import re
import shutil
import sqlite3
import time
import urllib.request

DB = "/srv/autorig/data/db/autorig.db"
ACCESS = "/home/log/nginx/access.log"
STATE = "/var/lib/autorig-task-flow-alert.json"
DEV = "https://autorig.online/dev/api/send"
MARKER = "/srv/autorig/data/artifact-cache/.pause-new-tasks"
SILENCE_S = 3 * 3600
NOW = time.time()


def reserve_gb() -> float:
    try:
        for line in open("/srv/autorig/secrets/backend.env", encoding="utf-8"):
            if line.startswith("ARTIFACT_CACHE_RESERVE_GB="):
                return float(line.split("=", 1)[1].strip().strip("'\""))
    except (OSError, ValueError):
        pass
    return 50.0


def dev(text: str):
    b = "tf" + os.urandom(8).hex()
    body = b""
    for k, v in (("agent", "Fleet · V3"), ("project", "AutoRig V3"), ("kind", "text"), ("caption", text)):
        body += (f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n"
                 ).encode() + v.encode() + b"\r\n"
    body += f"--{b}--\r\n".encode()
    req = urllib.request.Request(DEV, data=body, method="POST", headers={
        "Content-Type": f"multipart/form-data; boundary={b}", "User-Agent": "autorig-task-flow-alert/1"})
    try:
        urllib.request.urlopen(req, timeout=30).read()
    except Exception as exc:                                       # noqa: BLE001
        print("dev send failed", exc)


def create_status(window_s=600):
    """(count of POST /api/task/create, count of 5xx) in the last window, from the tail of the nginx log."""
    total = bad = 0
    try:
        size = os.path.getsize(ACCESS)
        with open(ACCESS, "rb") as fh:
            fh.seek(max(0, size - 6_000_000))
            lines = fh.read().decode("utf-8", "replace").splitlines()
    except OSError:
        return None, None
    rx = re.compile(r'\[(\d+/\w+/\d+:\d+:\d+:\d+) [^\]]+\] "POST /api/task/create[^"]*" (\d{3})')
    for line in lines:
        m = rx.search(line)
        if not m:
            continue
        try:
            t = calendar.timegm(time.strptime(m.group(1), "%d/%b/%Y:%H:%M:%S"))
        except ValueError:
            continue
        if NOW - t <= window_s:
            total += 1
            bad += m.group(2).startswith("5")
    return total, bad


def causes() -> list:
    out = []
    if os.path.isfile(MARKER):
        out.append("новые задачи на ПАУЗЕ (диск ниже резерва)")
    free = shutil.disk_usage("/srv/autorig").free / 2 ** 30
    r = reserve_gb()
    if free < r + 20:
        out.append(f"диск {free:.0f} ГБ свободно при резерве {r:.0f} ГБ")
    total, bad = create_status(3 * 3600)
    if bad:
        out.append(f"{bad} ошибок 5xx на /api/task/create за 3 ч (из {total})")
    elif total == 0:
        out.append("за 3 ч в логе nginx нет ни одного POST /api/task/create (трафика нет или сайт недоступен)")
    for name, url in (("backend", "http://127.0.0.1:8200/api/gallery?per_page=1"),
                      ("MT", "http://127.0.0.1:8251/api/mt/blender")):
        try:
            urllib.request.urlopen(url, timeout=8).read(10)
        except Exception as exc:                                   # noqa: BLE001
            out.append(f"{name} не отвечает: {str(exc)[:60]}")
    load = os.getloadavg()[0]
    if load > 20:
        out.append(f"нагрузка VPS {load:.0f}")
    return out or ["причина не найдена: пауза нет, диск и сервисы в порядке"]


def main():
    try:
        state = json.load(open(STATE))
    except (OSError, ValueError):
        state = {}
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
    last = con.execute("SELECT strftime('%s', MAX(created_at)) FROM tasks").fetchone()[0]
    con.close()
    idle = NOW - float(last or 0)
    if idle >= SILENCE_S:
        if NOW - float(state.get("silent_alerted_at") or 0) >= 3500:
            dev(f"🔴 КРИТИЧНО: нет новых задач {idle / 3600:.1f} ч (последняя "
                f"{time.strftime('%Y-%m-%d %H:%M', time.gmtime(float(last)))} UTC). Вероятная причина: "
                + "; ".join(causes()))
            state["silent_alerted_at"] = NOW
            state["silent"] = True
    elif state.get("silent"):
        dev("🟢 Новые задачи снова создаются")
        state["silent"] = False
    total, bad = create_status(600)
    if bad is not None and bad >= 5 and NOW - float(state.get("errors_alerted_at") or 0) >= 1800:
        dev(f"🔴 КРИТИЧНО: {bad} ошибок 5xx на /api/task/create за 10 минут (всего {total}). Вероятная причина: "
            + "; ".join(causes()))
        state["errors_alerted_at"] = NOW
    tmp = STATE + ".tmp"
    json.dump(state, open(tmp, "w"))
    os.replace(tmp, STATE)


if __name__ == "__main__":
    main()
