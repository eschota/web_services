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


VISION_CONFIG = "/root/autorig/ai_vision_animal_type_detect.json"
BACKEND_ENV = "/etc/autorig-backend.env"


def _llm_credentials() -> List[Tuple[str, str, str, str]]:
    """(label, url, key, model) in the order content_moderation tries them."""
    creds: List[Tuple[str, str, str, str]] = []
    openai_url = "https://api.openai.com/v1/chat/completions"
    try:
        for line in open(BACKEND_ENV, encoding="utf-8"):
            if line.strip().startswith("OPENAI_API_KEY"):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
                if key:
                    creds.append(("env openai", openai_url, key, "gpt-4o-mini"))
                break
    except Exception:
        pass
    try:
        cfg = json.load(open(VISION_CONFIG, encoding="utf-8"))
    except Exception:
        cfg = {}
    key = str(cfg.get("open_AI_api_key") or "").strip()
    if key:
        creds.append(("config openai", openai_url, key, "gpt-4o-mini"))
    key = str(cfg.get("open_router_api_key") or "").strip()
    if key:
        url = str(cfg.get("open_router_api_url_string") or "").strip() \
            or "https://openrouter.ai/api/v1/chat/completions"
        creds.append(("config openrouter", url, key, "openai/gpt-4o-mini"))
    return creds


def check_llm_credentials(report: Report) -> None:
    """Ask every vision credential whether it still has money.

    The pipeline alerts by itself when a call has to fall back, but that only
    fires while calls are being made. A key can empty during a quiet night and
    the first anyone hears of it is a morning of tasks with filename titles and
    generations routed as "not riggable" - which is exactly how ten hours were
    lost on 2026-08-09. So the balance is probed on a timer too, not only on
    demand.
    """
    creds = _llm_credentials()
    if not creds:
        report.fail("no vision credential configured at all")
        return
    alive: List[str] = []
    broke: List[str] = []
    for label, url, key, model in creds:
        body = json.dumps(
            {"model": model, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 1}
        ).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=25):
                alive.append(label)
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", "replace")[:200].lower()
            if "quota" in text or "credit" in text or "billing" in text:
                broke.append(f"{label}: OUT OF CREDIT")
            elif exc.code in (401, 403):
                broke.append(f"{label}: key rejected ({exc.code})")
            else:
                broke.append(f"{label}: HTTP {exc.code}")
        except Exception as exc:
            broke.append(f"{label}: {repr(exc)[:40]}")

    summary = f"vision credentials: {len(alive)} alive ({', '.join(alive) or 'none'})"
    if not alive:
        report.fail(f"{summary} — metadata and rig routing are DOWN: {'; '.join(broke)}")
    elif broke:
        # Still serving, but on the reserve. This is the warning worth acting
        # on: the next one to empty takes the pipeline with it.
        report.warn(f"{summary}; spent: {'; '.join(broke)}")
    else:
        report.ok(summary)


def check_renderfin(report: Report) -> Dict[str, Any]:
    try:
        health = _get_json(f"{RENDERFIN}/health")
    except Exception as exc:
        report.fail(f"renderfin /health unreachable: {exc!r}")
        return {}
    workers = health.get("hunyuan_workers") or []
    if not workers:
        report.fail("no hunyuan workers configured — 3D stage would fall back to ComfyUI")
    else:
        usable, unusable = _hunyuan_worker_health()
        report.ok(f"hunyuan workers: {', '.join(workers)} ({health.get('hunyuan_path')})")
        if unusable:
            # a configured box that cannot take a job is not a spare: every job
            # piles onto whatever is left, and throughput drops with no error
            report.fail(
                f"{len(unusable)} hunyuan worker(s) cannot take work: "
                + "; ".join(unusable)
                + (f" | usable: {', '.join(usable)}" if usable else " | NONE usable")
            )

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
            # updated_at is refreshed by a service restart, so a job stuck for a
            # day looked minutes old; stage_started_at is the real stage clock
            anchor = job.get("stage_started_at") or job.get("updated_at") or job.get("created_at")
            idle = now - float(anchor or now)
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


HUNYUAN_WORKERS_FILE = "/etc/autorig-renderfin-hunyuan.json"


def _hunyuan_worker_health() -> Tuple[List[str], List[str]]:
    """(usable, unusable) worker names, judged the same way renderfin judges them."""
    try:
        with open(HUNYUAN_WORKERS_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return [], []
    entries = data.get("workers") if isinstance(data, dict) else data
    usable: List[str] = []
    unusable: List[str] = []
    for entry in entries or []:
        name = str(entry.get("name") or entry.get("url") or "?")
        url = str(entry.get("url") or "").rstrip("/")
        token = str(entry.get("token") or "")
        if not url:
            continue
        if entry.get("enabled") is False or entry.get("disabled") is True:
            # parked on purpose: reported, but not as something to fix
            usable.append(f"{name} (parked: {entry.get('disabled_reason') or 'no reason'})")
            continue
        request = urllib.request.Request(
            f"{url}/api-converter-glb/server-status",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as resp:
                status = json.load(resp)
        except Exception as exc:
            unusable.append(f"{name} unreachable ({repr(exc)[:50]})")
            continue
        hunyuan = status.get("hunyuan")
        if not isinstance(hunyuan, dict):
            unusable.append(f"{name} reports no hunyuan module")
            continue
        if not hunyuan.get("enabled") or not hunyuan.get("installed"):
            unusable.append(
                f"{name} hunyuan enabled={hunyuan.get('enabled')} "
                f"installed={hunyuan.get('installed')}"
            )
            continue
        # server-status does not check the bearer, so a stale token only shows
        # up on the endpoint that matters. Jobs wait out a rejection rather
        # than failing, which is right for them and silent for everyone else -
        # this is the one place it gets said out loud.
        reason = _hunyuan_auth_error(url, token)
        if reason:
            unusable.append(f"{name} {reason}")
        else:
            usable.append(name)
    return usable, unusable


def _hunyuan_auth_error(url: str, token: str) -> str:
    """Empty when the worker accepts our credentials.

    Probes with a deliberately invalid body: a 400 means the bearer was
    accepted and the request got as far as validation, which is all we ask.
    """
    request = urllib.request.Request(
        f"{url}/api-converter-glb/generate-3d",
        data=b"{}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=20).close()
        return ""
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return f"rejects our token (HTTP {exc.code}) - refresh /etc/autorig-renderfin-hunyuan.json"
        return ""
    except Exception as exc:
        return f"generate-3d unreachable ({repr(exc)[:50]})"


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
        last_error = "no response"
        for path in ("/api-converter-glb/server-status", "/queue"):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{local_port}{path}", timeout=10
                ) as resp:
                    if resp.status == 200:
                        report.ok(f"tunnel {name} (:{local_port}) alive")
                        break
                    last_error = f"HTTP {resp.status}"
            except urllib.error.HTTPError as exc:
                # the port answered, so the tunnel itself is up
                report.ok(f"tunnel {name} (:{local_port}) alive (HTTP {exc.code})")
                break
            except Exception as exc:
                last_error = repr(exc)[:80]
                continue
        else:
            # carry the reason: "down" alone does not say whether the tunnel
            # died or the box behind it stopped answering
            report.fail(f"tunnel {name} (:{local_port}) down: {last_error}")


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
    check_llm_credentials(report)
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
