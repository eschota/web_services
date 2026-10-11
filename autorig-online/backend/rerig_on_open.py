"""Only current-version rigs are shown (owner 2026-10-11, AGENTS.md «Only Current-Version Rigs Are Shown»):
«сделай чтобы все задачи если их кто то открывает, а версия сервера не совпадает чтобы заново ригались … только при
открытии сцены триггерить перериг».

* The server's rig version is MT's `mt/rig_version.py` RIG_VERSION (read here by path, cached by mtime).
* A task's rig version is `rig_version` in the rig.json of its Motion Transfer run: the newest V3 attempt's run for a V3
  task, the mirror run (task_agents/<task>.json) for a classic one. A classic converter rig has none.
* When a person opens a task (the task page polls /api/task/{id}/v3-view) and the versions differ, one request
  MT_ROOT/rerig_requests/<task>__<version>.json is created with O_EXCL - the lock: parallel visitors, reloads and polls
  never queue it twice. mt/rerig_open.py (autorig-rerig-open.service) does the re-rig after fresh uploads.
* Never for: crawlers and link previews (user agent), a task still in progress, an adult task on the SFW site
  (site_mode.hides), more than PER_IP new requests an hour from one address or GLOBAL_PER_HOUR in all, a backlog
  of MAX_QUEUED. A failed task gets its chance too.
GET /api/task/{id}/rig-version answers the same facts without starting anything.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

MT_ROOT = Path(os.getenv("MT_ROOT", "/srv/autorig/data/motion_transfer"))
REQ_DIR = MT_ROOT / "rerig_requests"
ENABLED = os.getenv("AUTORIG_RERIG_ON_OPEN", "on").lower() not in ("0", "off", "false", "no")
PER_IP = int(os.getenv("AUTORIG_RERIG_PER_IP_HOUR", "6"))
GLOBAL_PER_HOUR = int(os.getenv("AUTORIG_RERIG_PER_HOUR", "60"))
MAX_QUEUED = int(os.getenv("AUTORIG_RERIG_MAX_QUEUED", "25"))
_RUN = re.compile(r"^[0-9a-f]{20}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_VER = re.compile(r'^RIG_VERSION\s*=\s*"([A-Za-z0-9._-]{1,64})"\s*$', re.M)
_BOT = re.compile(r"bot|crawl|spider|slurp|preview|facebookexternalhit|embedly|whatsapp|telegram|discord|skype|"
                  r"curl|wget|python|httpx|aiohttp|go-http|java/|okhttp|headless|phantom|lighthouse|pingdom|monitor|"
                  r"uptime|scanner|validator|feedfetcher|mediapartners|yandex|bing|baidu|duckduck|semrush|ahrefs",
                  re.I)
_cache: dict[str, Any] = {"ver": None, "mtime": None}
_task_cache: dict[str, tuple[float, Optional[str], Optional[str]]] = {}
_ip_hits: dict[str, list[float]] = {}
_all_hits: list[float] = []


def current_version() -> Optional[str]:
    path = MT_ROOT / "mt" / "rig_version.py"
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if _cache["mtime"] != mtime:
        m = _VER.search(path.read_text(encoding="utf-8", errors="replace"))
        _cache.update(ver=m.group(1) if m else None, mtime=mtime)
    return _cache["ver"]


def _read(path: Path) -> dict:
    try:
        v = json.loads(path.read_text(encoding="utf-8"))
        return v if isinstance(v, dict) else {}
    except (OSError, ValueError):
        return {}


def run_of(task: Any) -> tuple[Optional[str], str]:
    """-> (MT run id, 'v3' | 'classic')."""
    tid = str(task.id)
    if str(getattr(task, "pipeline_kind", "") or "").strip().lower() == "v3":
        try:
            settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
        except (TypeError, ValueError):
            settings = {}
        session = ((settings.get("v3") or {}).get("session") or {}) if isinstance(settings, dict) else {}
        run = str(session.get("mt_run_id") or "")
        return (run if _RUN.fullmatch(run) else None), "v3"
    run = str(_read(MT_ROOT / "task_agents" / f"{tid}.json").get("run_id") or "")
    return (run if _RUN.fullmatch(run) else None), "classic"


def task_version(task: Any) -> tuple[Optional[str], Optional[str], str]:
    """-> (the task's rig version or None, run, kind); cached 15 s per task."""
    tid = str(task.id)
    hit = _task_cache.get(tid)
    run, kind = run_of(task)
    if hit and time.time() - hit[0] < 15 and hit[2] == run:
        return hit[1], run, kind
    ver = None
    if run:
        doc = _read(MT_ROOT / "runs" / run / "rig" / "rig.json")
        if kind == "v3" or doc.get("source") in ("fast-v3", "v3-rerig"):
            ver = str(doc.get("rig_version") or "") or None
    _task_cache[tid] = (time.time(), ver, run)
    return ver, run, kind


def request_path(task_id: str, version: str) -> Path:
    return REQ_DIR / f"{task_id}__{version}.json"


def is_bot(request: Any) -> bool:
    """The SEO agent's crawler list (seo_social.is_crawler) plus scripts and monitors."""
    ua = str(request.headers.get("user-agent") or "") if request is not None else ""
    try:
        import seo_social
        if seo_social.is_crawler(ua):
            return True
    except Exception:                                    # noqa: BLE001 - an older release without it
        pass
    return not ua or bool(_BOT.search(ua))


def _client(request: Any) -> str:
    return str(request.headers.get("x-real-ip") or (request.client.host if request.client else "?"))


def _allowed_now(ip: str) -> Optional[str]:
    now = time.time()
    hits = [t for t in _ip_hits.get(ip, []) if now - t < 3600]
    _ip_hits[ip] = hits
    _all_hits[:] = [t for t in _all_hits if now - t < 3600]
    if len(hits) >= PER_IP:
        return "rate_limited_ip"
    if len(_all_hits) >= GLOBAL_PER_HOUR:
        return "rate_limited_global"
    try:
        queued = sum(1 for p in REQ_DIR.glob("*.json") if _read(p).get("state") in ("queued", "running"))
    except OSError:
        queued = 0
    if queued >= MAX_QUEUED:
        return "backlog_full"
    return None


def state(task: Any, request: Any = None, *, trigger: bool = True) -> dict:
    """The facts for the task page (`rig_version` in /api/task/{id}/v3-view) and, on a person's open, the request."""
    cur = current_version()
    ver, run, kind = task_version(task)
    out: dict[str, Any] = {"schema": "autorig.rig-version-state/1", "current": cur, "task": ver, "run": run,
                           "pipeline": kind, "current_version": bool(cur and ver == cur)}
    if not cur:
        return out
    req = request_path(str(task.id), cur)
    doc = _read(req)
    if doc:
        out["rerig"] = {k: doc.get(k) for k in ("state", "at", "started_at", "finished_at", "version", "why",
                                                 "seconds", "timings_s")}
    if out["current_version"] or doc or not trigger or not ENABLED:
        return out
    status = str(getattr(task, "status", "") or "")
    if status in ("created", "processing"):
        out["rerig"] = {"state": "not_now", "why": "the task is still being processed"}
        return out
    if request is not None and is_bot(request):
        out["rerig"] = {"state": "not_for_bots"}
        return out
    try:
        import site_mode
        if request is not None and site_mode.hides(request, task):
            out["rerig"] = {"state": "not_on_this_site"}
            return out
    except Exception:                                    # noqa: BLE001 - an older release without site_mode
        pass
    ip = _client(request) if request is not None else "?"
    why = _allowed_now(ip)
    if why:
        out["rerig"] = {"state": "deferred", "why": why}
        return out
    REQ_DIR.mkdir(parents=True, exist_ok=True)
    body = {"schema": "autorig.rerig-request/1", "task": str(task.id), "version": cur, "from_version": ver,
            "run": run, "pipeline": kind, "task_status": status, "state": "queued", "at": round(time.time(), 1),
            "by": "open", "ip_hash": __import__("hashlib").sha256(ip.encode()).hexdigest()[:12]}
    try:
        fd = os.open(req, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o664)     # the lock per (task, version)
    except FileExistsError:
        out["rerig"] = {k: _read(req).get(k) for k in ("state", "at")}
        return out
    except OSError as exc:
        out["rerig"] = {"state": "error", "why": type(exc).__name__}
        return out
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(body, fh, ensure_ascii=False)
    _ip_hits.setdefault(ip, []).append(time.time())
    _all_hits.append(time.time())
    out["rerig"] = {"state": "queued", "at": body["at"]}
    print(f"[rerig-on-open] {task.id} {kind} {ver or 'none'} -> {cur} queued")
    return out
