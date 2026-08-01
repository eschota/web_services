#!/usr/bin/env python3
"""Stability check for autorig.online and the 3D generation pipeline.

Run on the VPS (systemd timer every 6h, or by hand). Prints a digest and exits
non-zero when something needs attention. With --notify it posts the digest to
the Telegram group whenever there is at least one problem, so a silent failure
of the generation pipeline cannot go unnoticed.

    python3 renderfin_healthcheck.py [--notify]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple

RENDERFIN = "http://127.0.0.1:8010/renderfin"
SITE = "https://autorig.online/"
SERVICES = ("autorig", "autorig-renderfin", "autorig-telegram", "autorig-farm-tunnels")
CHARGEN_DB = "/var/autorig/renderfin/db/renderfin.db"
DISK_WARN_PERCENT = 90.0
# A stage that has not moved in this long is stuck, not slow.
STAGE_STALL_SECONDS = 4 * 3600
ACTIVE_STAGES = ("flux_render", "hunyuan", "turntable")


class Report:
    def __init__(self) -> None:
        self.lines: List[str] = []
        self.problems: List[str] = []

    def ok(self, text: str) -> None:
        self.lines.append(f"✅ {text}")

    def warn(self, text: str) -> None:
        self.lines.append(f"⚠️ {text}")
        self.problems.append(text)

    def fail(self, text: str) -> None:
        self.lines.append(f"❌ {text}")
        self.problems.append(text)


def _get_json(url: str, timeout: float = 15.0) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def check_site(report: Report) -> None:
    try:
        req = urllib.request.Request(SITE, method="GET")
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status == 200:
                report.ok(f"site {SITE} → 200")
            else:
                report.fail(f"site {SITE} → HTTP {resp.status}")
    except Exception as exc:
        report.fail(f"site {SITE} unreachable: {exc!r}")


def check_services(report: Report) -> None:
    for unit in SERVICES:
        try:
            out = subprocess.run(
                ["systemctl", "is-active", unit],
                capture_output=True, text=True, timeout=20,
            ).stdout.strip()
        except Exception as exc:
            report.fail(f"{unit}: cannot query ({exc!r})")
            continue
        if out == "active":
            report.ok(f"{unit}: active")
        else:
            report.fail(f"{unit}: {out or 'unknown'}")


def check_disk(report: Report) -> None:
    usage = shutil.disk_usage("/")
    used_percent = (usage.total - usage.free) / usage.total * 100.0
    free_gb = usage.free / (1024**3)
    text = f"disk {used_percent:.1f}% used, {free_gb:.1f} GB free"
    if used_percent >= DISK_WARN_PERCENT:
        report.warn(text)
    else:
        report.ok(text)


def check_renderfin(report: Report) -> Dict[str, Any]:
    try:
        health = _get_json(f"{RENDERFIN}/health")
    except Exception as exc:
        report.fail(f"renderfin /health unreachable: {exc!r}")
        return {}
    workers = health.get("hunyuan_workers") or []
    if workers:
        report.ok(f"hunyuan workers: {', '.join(workers)} ({health.get('hunyuan_path')})")
    else:
        report.fail("no hunyuan workers configured — 3D stage would fall back to ComfyUI")

    try:
        dashboard = _get_json(f"{RENDERFIN}/api-render")
    except Exception as exc:
        report.fail(f"renderfin /api-render unreachable: {exc!r}")
        return health

    online = [s["render_server_name"] for s in dashboard.get("servers", []) if s.get("status") == "online"]
    if online:
        report.ok(f"render workers online: {', '.join(online)}")
    else:
        report.fail("no render workers online — nothing can render")

    tasks = dashboard.get("tasks", [])
    now = time.time()
    stuck = [
        t for t in tasks
        if t.get("status") == "Rendering"
        and t.get("started_at")
        and now - float(t["started_at"]) > STAGE_STALL_SECONDS
    ]
    pending = sum(1 for t in tasks if t.get("status") == "Pending")
    rendering = sum(1 for t in tasks if t.get("status") == "Rendering")
    report.ok(f"render queue: {rendering} rendering, {pending} pending")
    if stuck:
        report.fail(f"{len(stuck)} render task(s) stuck > {STAGE_STALL_SECONDS // 3600}h: "
                    + ", ".join(t["id"][:8] for t in stuck[:5]))
    return health


def check_generation_jobs(report: Report) -> None:
    if not os.path.isfile(CHARGEN_DB):
        report.warn(f"no chargen db at {CHARGEN_DB}")
        return
    try:
        db = sqlite3.connect(f"file:{CHARGEN_DB}?mode=ro", uri=True)
        rows = db.execute("SELECT payload FROM chargen_jobs").fetchall()
    except Exception as exc:
        report.fail(f"chargen db unreadable: {exc!r}")
        return

    now = time.time()
    stages: Dict[str, int] = {}
    failed: List[str] = []
    stalled: List[str] = []
    undelivered: List[str] = []
    for (payload,) in rows:
        try:
            job = json.loads(payload)
        except Exception:
            continue
        stage = job.get("stage", "?")
        stages[stage] = stages.get(stage, 0) + 1
        if stage == "failed":
            failed.append(f"{job['id'][:8]}: {(job.get('error') or '')[:60]}")
        if stage in ACTIVE_STAGES:
            idle = now - float(job.get("updated_at") or job.get("created_at") or now)
            if idle > STAGE_STALL_SECONDS and not job.get("retry_at"):
                stalled.append(f"{job['id'][:8]} at {stage} for {idle / 3600:.1f}h")
        delivered = job.get("delivered") or {}
        if stage == "ready" and job.get("telegram_chat_id") and not delivered.get("model"):
            undelivered.append(job["id"][:8])

    report.ok("generation jobs: " + ", ".join(f"{k}={v}" for k, v in sorted(stages.items())))
    if failed:
        report.fail(f"{len(failed)} job(s) failed: " + "; ".join(failed[:3]))
    if stalled:
        report.fail(f"{len(stalled)} job(s) stalled: " + "; ".join(stalled[:3]))
    if undelivered:
        report.fail(f"{len(undelivered)} finished job(s) never delivered: " + ", ".join(undelivered[:3]))


def check_farm_tunnels(report: Report) -> None:
    conf = "/etc/autorig-farm-tunnels.conf"
    if not os.path.isfile(conf):
        report.warn("no farm tunnel config; Hunyuan reachable only if facades pass auth")
        return
    for line in open(conf, encoding="utf-8"):
        parts = line.split()
        if len(parts) != 4 or line.strip().startswith("#"):
            continue
        name, _ssh_port, local_port, _remote = parts
        for path in ("/api-converter-glb/server-status", "/queue"):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{local_port}{path}", timeout=10
                ) as resp:
                    if resp.status == 200:
                        report.ok(f"tunnel {name} (:{local_port}) alive")
                        break
            except Exception:
                continue
        else:
            report.fail(f"tunnel {name} (:{local_port}) down")


def notify(report: Report) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("HEALTHCHECK_CHAT_ID", "").strip() or os.getenv(
        "TELEGRAM_NOTIFICATION_CHAT_ID", ""
    ).strip()
    if not token or not chat_id:
        print("[healthcheck] notify skipped: no token/chat configured")
        return
    text = "🩺 <b>AutoRig health check</b>\n" + "\n".join(report.lines)
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text[:4000],
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=30
        ) as resp:
            resp.read()
    except Exception as exc:
        print(f"[healthcheck] notify failed: {exc!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--notify", action="store_true", help="post to Telegram when unhealthy")
    args = parser.parse_args()

    report = Report()
    check_site(report)
    check_services(report)
    check_disk(report)
    check_renderfin(report)
    check_generation_jobs(report)
    check_farm_tunnels(report)

    print(f"=== AutoRig health {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())} ===")
    for line in report.lines:
        print(line)
    if report.problems:
        print(f"\n{len(report.problems)} problem(s) need attention")
        if args.notify:
            notify(report)
        return 1
    print("\nall green")
    return 0


if __name__ == "__main__":
    import urllib.parse  # noqa: E402  (used by notify)

    sys.exit(main())
