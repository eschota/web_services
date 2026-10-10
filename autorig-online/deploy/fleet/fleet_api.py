"""AutoRig fleet status in one call: GET https://autorig.online/api/fleet

Owner rule, 2026-10-10: every agent working with a session must know the current
state of every fleet box with one ordinary API request, instead of probing boxes
one by one over SSH.

A small standalone service (standard library only) so it can be deployed and
restarted without touching autorig-storage, whose restart wipes the render
queue. It collects, in background threads, from sources that already exist:

* the converter registry /srv/autorig/secrets/renderfin-hunyuan.json
  (converters, Hunyuan adapters, ai-nodes) -> /api-converter-glb/server-status
  through the VPS tunnels, with the per-node bearer token (server side only);
* AutoRig dispatch: the worker_endpoints table of autorig.db (read-only);
* Renderfin: /renderfin/api-render, plus each ComfyUI's /queue and /system_stats;
* box agents: /srv/autorig/data/var/fleet/boxes/<box>.json (fleet-agent.ps1,
  per-drive disk, GPU, scheduled tasks, quarantine sizes) and the LoRA sync
  reports in /srv/autorig/data/var/ai-models/loras/boxes/<box>.json;
* the MT Blender workers (/api/mt/blender) and the VPS itself.

GET /api/fleet              JSON snapshot (rebuilt every few seconds, served from memory)
GET /api/fleet?format=text  the same as a plain-text table, one line per box
GET /api/fleet/box/<id>     one box
POST /api/fleet/report      box agent report (X-AutoRig-Box + Bearer key)
GET /api/fleet/agent.ps1    the box agent script (self-update source)

No secret ever leaves this process: tokens are only used for outgoing probes,
task identifiers are shortened to 8 characters, internal URLs are not echoed.
"""

from __future__ import annotations

import concurrent.futures
import datetime as _dt
import hmac
import json
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

SCHEMA = "autorig.fleet/1"
PORT = int(os.getenv("FLEET_PORT", "8255"))
REGISTRY = os.getenv("FLEET_REGISTRY", "/srv/autorig/secrets/renderfin-hunyuan.json")
DB_PATH = os.getenv("FLEET_DB", "/srv/autorig/data/db/autorig.db")
LORA_BOXES = os.getenv("FLEET_LORA_BOXES", "/srv/autorig/data/var/ai-models/loras/boxes")
STATE_DIR = os.getenv("FLEET_STATE_DIR", "/srv/autorig/data/var/fleet")
AGENT_KEYS = os.getenv("FLEET_AGENT_KEYS", "/srv/autorig/secrets/fleet-agent-keys.json")
LORA_KEYS = os.getenv("FLEET_LORA_KEYS", "/srv/autorig/secrets/lora-sync-keys.json")
RENDERFIN = os.getenv("FLEET_RENDERFIN", "http://127.0.0.1:8210/renderfin").rstrip("/")
MT_BASE = os.getenv("FLEET_MT", "http://127.0.0.1:8251").rstrip("/")
ARTIFACT_CACHE_ROOT = os.getenv("FLEET_ARTIFACT_CACHE_ROOT", "/srv/autorig/data/artifact-cache")
ARTIFACT_CACHE_RESERVE_GB = float(os.getenv("FLEET_ARTIFACT_CACHE_RESERVE_GB", "50"))
AGENT_SCRIPT = os.getenv(
    "FLEET_AGENT_SCRIPT", "/srv/autorig/fleet/fleet-agent.ps1")
CURRENT_LINK = "/srv/autorig/current"

FAST_SECONDS = 5.0       # renderfin, ComfyUI queues, ai-nodes, Hunyuan adapters
CONVERTER_SECONDS = 10.0  # full converters: their status is hundreds of KB
SLOW_SECONDS = 60.0      # V3 endpoint probe, ComfyUI system_stats
DB_SECONDS = 15.0
VPS_SECONDS = 15.0
AGENT_STALE_SECONDS = 15 * 60
LOW_DISK_GB = 15.0
LOW_DISK_SYSTEM_GB = 10.0

VPS_UNITS = [
    "autorig-storage", "autorig-storage-renderfin", "autorig-mt", "autorig-storage-telegram",
    "autorig-admin-bot", "autorig-devbot", "autorig-storage-tunnels",
    "autorig-storage-raptor-tunnel", "autorig-ai-node-tunnels", "autorig-fleet", "nginx",
]

# The physical boxes. Names in the sources differ (renderfin "Raptor", the
# registry "raptor"/"raptor-ai", LoRA sync "Raptor"); this table joins them.
BOXES: List[Dict[str, Any]] = [
    {"id": "f1", "hosts": ["f1-pc"], "converter": "f1", "endpoint": "converter-f1.",
     "gpu_hint": "GTX 1080 Ti", "v3_target": True, "work_drives": ["C:"]},
    {"id": "f2", "hosts": ["f2-pc"], "converter": "f2", "endpoint": "converter-f2.",
     "gpu_hint": "GTX 1080 Ti", "v3_target": True, "work_drives": ["C:"]},
    {"id": "f7", "hosts": ["f7-pc"], "converter": "f7", "ai": "f7-ai", "endpoint": "converter-f7.",
     "gpu_hint": "GTX 1080 Ti", "v3_target": True, "work_drives": ["C:", "D:"]},
    {"id": "f11", "hosts": ["f11-pc"], "converter": "f11", "ai": "f11-ai", "endpoint": "converter-f11.",
     "gpu_hint": "GTX 1080 Ti", "v3_target": True, "work_drives": ["C:"]},
    {"id": "f13", "hosts": ["f13-pc"], "converter": "f13", "endpoint": "converter-f13.",
     "gpu_hint": "GTX 1080 Ti", "v3_target": True, "work_drives": ["C:"]},
    {"id": "f12", "hosts": ["win-5io40pmgn0p", "win-hgerejimqdo"], "render": "f12", "hunyuan": "f12",
     "ai": "f12-ai", "lora": "f12", "gpu_hint": "RTX 3080 Ti", "work_drives": ["C:"],
     "note": "home LAN; the owner's programs run here: ask before a reboot"},
    {"id": "f15", "hosts": ["f15-pc"], "render": "f15", "lora": "f15", "gpu_hint": "RTX 3070 Ti",
     "work_drives": ["C:", "D:"]},
    {"id": "raptor", "hosts": ["ryzen-server"], "render": "Raptor", "hunyuan": "raptor",
     "ai": "raptor-ai", "lora": "Raptor", "gpu_hint": "RTX 3080 Ti", "work_drives": ["C:", "D:", "X:"]},
    {"id": "worker-4090", "hosts": ["win-giv14mf4pfc"], "render": "worker-4090", "ai": "worker-4090-ai",
     "lora": "worker-4090", "gpu_hint": "RTX 4090", "work_drives": ["C:", "R:"],
     "note": "the owner's own PC: his GPU first; never start, stop or reboot without his go-ahead"},
    {"id": "f5", "hosts": ["f5-pc"], "render": "f5", "lora": "f5", "gpu_hint": "RTX 3070",
     "work_drives": ["C:", "D:"],
     "out_of_fleet": "owner order 2026-10-08: another project runs there; do not touch or route work"},
]
BOX_BY_ID = {box["id"]: box for box in BOXES}


def _box_id_for(name: str) -> Optional[str]:
    value = str(name or "").strip().lower()
    if not value:
        return None
    if value in BOX_BY_ID:
        return value
    for box in BOXES:
        if value in box.get("hosts", []):
            return box["id"]
        for key in ("render", "lora", "converter", "hunyuan", "ai"):
            if str(box.get(key) or "").lower() == value:
                return box["id"]
    return None


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float]) -> Optional[str]:
    if not ts:
        return None
    return _dt.datetime.fromtimestamp(float(ts), _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts(value: Any) -> Optional[float]:
    """Seconds since the epoch from a number or an ISO string (best effort)."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        text = text.replace("Z", "+00:00")
        # Python 3.11 handles up to 6 fractional digits; trim the 7th from .NET stamps.
        text = re.sub(r"(\.\d{6})\d+", r"\1", text)
        parsed = _dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        return parsed.timestamp()
    except Exception:
        return None


def _short(task_id: Any) -> str:
    return str(task_id or "")[:8]


def _read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _write_json_atomic(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def _http_json(url: str, token: str = "", timeout: float = 6.0,
               method: str = "GET") -> Tuple[Optional[int], Any, str, float]:
    started = time.monotonic()
    headers = {"User-Agent": "autorig-fleet/1", "Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            elapsed = time.monotonic() - started
            try:
                return response.status, json.loads(raw.decode("utf-8")), "", elapsed
            except Exception:
                return response.status, None, "not_json", elapsed
    except urllib.error.HTTPError as exc:
        return exc.code, None, f"http_{exc.code}", time.monotonic() - started
    except Exception as exc:  # timeouts, refused tunnels
        reason = getattr(exc, "reason", None) or exc
        return None, None, f"{type(exc).__name__}: {str(reason)[:120]}", time.monotonic() - started


# --------------------------------------------------------------- collection

class Collector:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sources: Dict[str, Dict[str, Any]] = {}
        self.due: Dict[str, float] = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=24, thread_name_prefix="fleet-probe")
        self.snapshot_bytes = b"{}"
        self.snapshot: Dict[str, Any] = {}
        self.snapshot_at = 0.0
        self.text = ""
        self.last_build_error = ""
        self.persisted_at = 0.0

    # Each source keeps its own timestamp so a slow one never ages the rest.
    def _store(self, key: str, value: Dict[str, Any]) -> None:
        with self.lock:
            self.sources[key] = value

    def _get(self, key: str) -> Dict[str, Any]:
        with self.lock:
            return dict(self.sources.get(key) or {})

    def _is_due(self, key: str, every: float) -> bool:
        now = time.monotonic()
        if now >= self.due.get(key, 0.0):
            self.due[key] = now + every
            return True
        return False

    # -- registry workers --------------------------------------------------
    def _workers(self) -> List[Dict[str, Any]]:
        data = _read_json(REGISTRY, {})
        workers = data.get("workers") if isinstance(data, dict) else data
        return [w for w in (workers or []) if isinstance(w, dict) and w.get("url")]

    def _probe_worker(self, worker: Dict[str, Any]) -> None:
        name = str(worker.get("name") or "")
        url = str(worker.get("url") or "").rstrip("/")
        heavy = str(worker.get("capability_mode") or "") == "full"
        status, body, error, elapsed = _http_json(
            url + "/api-converter-glb/server-status", str(worker.get("token") or ""),
            timeout=15.0 if heavy else 8.0)
        entry = {
            "name": name, "checked_at": _now(), "http_status": status, "error": error,
            "elapsed": round(elapsed, 3), "ok": status == 200 and isinstance(body, dict),
            "registry": {
                "enabled": bool(worker.get("enabled")),
                "capability_mode": worker.get("capability_mode"),
                "pool": worker.get("pool"),
                "ai_vision_enabled": worker.get("ai_vision_enabled", True) is not False,
                "disabled_reason": str(worker.get("disabled_reason") or "")[:300],
            },
        }
        previous = self._get("worker:" + name)
        if entry["ok"]:
            entry["status"] = _slim_status(body)
            entry["last_ok_at"] = entry["checked_at"]
        else:
            entry["last_ok_at"] = previous.get("last_ok_at")
            last_good = previous.get("status") or previous.get("stale_status")
            recent = bool(entry["last_ok_at"] and _now() - float(entry["last_ok_at"]) < 60)
            if last_good and recent and status is None and "timed out" in str(error).lower():
                # Slow answer right after a good one: keep serving the last status.
                entry["ok"] = True
                entry["slow"] = True
                entry["status"] = last_good
            elif last_good:
                entry["stale_status"] = last_good
        self._store("worker:" + name, entry)

    def _probe_v3(self, worker: Dict[str, Any]) -> None:
        name = str(worker.get("name") or "")
        url = str(worker.get("url") or "").rstrip("/")
        status, _body, error, _elapsed = _http_json(
            url + "/api-converter-glb/v3/normalize", str(worker.get("token") or ""), timeout=6.0)
        # A POST-only route answers GET with 405; a missing one with 404.
        present = status in (200, 400, 401, 403, 405, 422)
        self._store("v3:" + name, {"checked_at": _now(), "http_status": status,
                                   "endpoint_present": present if status else None,
                                   "error": error if not status else ""})

    # -- renderfin ------------------------------------------------------------
    def _probe_renderfin(self) -> None:
        status, body, error, elapsed = _http_json(RENDERFIN + "/api-render", timeout=8.0)
        entry = {"checked_at": _now(), "ok": status == 200 and isinstance(body, dict),
                 "error": error, "elapsed": round(elapsed, 3)}
        if entry["ok"]:
            servers = [s for s in (body.get("servers") or []) if isinstance(s, dict)]
            tasks = [t for t in (body.get("tasks") or []) if isinstance(t, dict)]
            active = [t for t in tasks if str(t.get("status") or "").lower()
                      not in ("done", "failed", "error", "cancelled", "canceled")]
            entry["servers"] = [{
                "name": s.get("render_server_name"), "url": s.get("render_server_url"),
                "status": s.get("status"), "gpu_name": s.get("gpu_name"),
                "queue_size": s.get("queue_size"), "current_task": s.get("current_render_task"),
                "workflows": len(s.get("available_workflows") or []),
                "average_render_time": s.get("average_render_time"),
                "date_update": s.get("date_update"),
            } for s in servers]
            entry["active"] = [{
                "id": _short(t.get("id") or t.get("task_id")), "status": t.get("status"),
                "server": t.get("render_server_name") or "",
                "workflow": t.get("workflow") or t.get("workflow_file") or "",
                "started_at": t.get("started_at"), "created_at": t.get("created_at"),
            } for t in active]
        else:
            previous = self._get("renderfin")
            for key in ("servers", "active"):
                if previous.get(key):
                    entry[key] = previous[key]
            entry["stale"] = True
        self._store("renderfin", entry)

    def _probe_comfy(self, name: str, url: str, with_stats: bool) -> None:
        base = str(url or "").rstrip("/")
        if not base:
            return
        status, body, error, _elapsed = _http_json(base + "/queue", timeout=4.0)
        entry = self._get("comfy:" + name)
        entry.update({"checked_at": _now(), "queue_ok": status == 200 and isinstance(body, dict),
                      "queue_error": error})
        if entry["queue_ok"]:
            entry["running"] = len(body.get("queue_running") or [])
            entry["pending"] = len(body.get("queue_pending") or [])
        if with_stats:
            s_status, s_body, s_error, _ = _http_json(base + "/system_stats", timeout=5.0)
            if s_status == 200 and isinstance(s_body, dict):
                devices = s_body.get("devices") or []
                system = s_body.get("system") or {}
                dev = devices[0] if devices and isinstance(devices[0], dict) else {}
                entry["stats"] = {
                    "checked_at": _now(),
                    "comfyui_version": system.get("comfyui_version"),
                    "pytorch_version": system.get("pytorch_version"),
                    "ram_total_gb": round((system.get("ram_total") or 0) / 1024 ** 3, 1),
                    "ram_free_gb": round((system.get("ram_free") or 0) / 1024 ** 3, 1),
                    "gpu_name": re.sub(r"^cuda:\d+\s+|\s*:\s*\w+$", "", str(dev.get("name") or "")).strip(),
                    "vram_total_mb": int((dev.get("vram_total") or 0) / 1024 ** 2),
                    "vram_free_mb": int((dev.get("vram_free") or 0) / 1024 ** 2),
                }
            else:
                entry["stats_error"] = s_error
        self._store("comfy:" + name, entry)

    # -- database -------------------------------------------------------------
    def _probe_db(self) -> None:
        entry: Dict[str, Any] = {"checked_at": _now()}
        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=3.0)
            try:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT id, url, enabled, weight, pool, role FROM worker_endpoints").fetchall()
                entry["endpoints"] = [dict(row) for row in rows]
                entry["task_counts"] = {
                    row[0]: row[1] for row in conn.execute(
                        "SELECT status, count(*) FROM tasks WHERE status IN ('created','processing') "
                        "GROUP BY status")}
                entry["processing_by_worker"] = {
                    str(row[0] or ""): row[1] for row in conn.execute(
                        "SELECT worker_api, count(*) FROM tasks WHERE status='processing' GROUP BY worker_api")}
                day = conn.execute(
                    "SELECT worker_api, status, count(*) FROM tasks "
                    "WHERE created_at > datetime('now','-1 day') AND status IN ('done','error') "
                    "GROUP BY worker_api, status").fetchall()
                entry["last_24h"] = [list(row) for row in day]
                entry["ok"] = True
            finally:
                conn.close()
        except Exception as exc:
            entry["ok"] = False
            entry["error"] = f"{type(exc).__name__}: {exc}"[:200]
        self._store("db", entry)

    # -- live dispatch environment of autorig-storage ------------------------
    def _probe_dispatch_env(self) -> None:
        """AUTORIG_DISABLED_WORKERS / AUTORIG_WORKER_TRANSPORTS as the running backend sees them.

        They decide dispatch on top of worker_endpoints. The env file is root-only,
        but the live process environment is readable by the same service user.
        Only origins are kept: the values carry no secret.
        """
        entry: Dict[str, Any] = {"checked_at": _now()}
        try:
            out = subprocess.run(["systemctl", "show", "-p", "MainPID", "--value", "autorig-storage.service"],
                                 capture_output=True, text=True, timeout=5)
            pid = int((out.stdout or "0").strip() or 0)
            entry["pid"] = pid
            if pid > 0:
                with open(f"/proc/{pid}/environ", "rb") as handle:
                    pairs = handle.read().split(b"\0")
                env = {}
                for pair in pairs:
                    if b"=" in pair:
                        key, value = pair.split(b"=", 1)
                        if key in (b"AUTORIG_DISABLED_WORKERS", b"AUTORIG_WORKER_TRANSPORTS"):
                            env[key.decode()] = value.decode("utf-8", "replace")
                entry["disabled_urls"] = [u.strip() for u in env.get("AUTORIG_DISABLED_WORKERS", "").split(",")
                                          if u.strip()]
                transports = {}
                try:
                    raw = json.loads(env.get("AUTORIG_WORKER_TRANSPORTS") or "{}")
                    for origin in (raw or {}):
                        transports[str(origin)] = "internal tunnel"
                except Exception:
                    entry["transport_error"] = "unparseable"
                entry["transports"] = transports
                entry["ok"] = True
        except Exception as exc:
            entry["ok"] = False
            entry["error"] = f"{type(exc).__name__}: {exc}"[:160]
        self._store("dispatch_env", entry)

    def _probe_gateway(self, box_id: str) -> None:
        """Is the public converter-<box>.freestock.online route (result downloads) alive?"""
        url = f"https://converter-{box_id}.freestock.online/converter/glb/__fleet_probe__"
        status, _body, error, elapsed = _http_json(url, timeout=12.0)
        offline = bool(status == 502)
        self._store("gateway:" + box_id, {"checked_at": _now(), "http_status": status,
                                          "online": (status is not None and not offline),
                                          "error": error if not status else "",
                                          "elapsed": round(elapsed, 2)})

    # -- MT blender workers ---------------------------------------------------
    def _probe_mt(self) -> None:
        status, body, error, _ = _http_json(MT_BASE + "/api/mt/blender", timeout=4.0)
        entry = {"checked_at": _now(), "ok": status == 200 and isinstance(body, dict), "error": error}
        if entry["ok"]:
            entry["online"] = bool(body.get("online"))
            entry["workers"] = {str(k): v for k, v in (body.get("workers") or {}).items()}
        self._store("mt", entry)

    # -- VPS ------------------------------------------------------------------
    def _probe_vps(self) -> None:
        entry: Dict[str, Any] = {"checked_at": _now()}
        try:
            usage = shutil.disk_usage("/srv/autorig")
            entry["disk"] = {"path": "/srv/autorig", "size_gb": round(usage.total / 1024 ** 3, 1),
                             "free_gb": round(usage.free / 1024 ** 3, 1),
                             "used_percent": round(100.0 * (usage.total - usage.free) / usage.total, 1)}
        except Exception as exc:
            entry["disk_error"] = str(exc)[:120]
        marker = os.path.join(ARTIFACT_CACHE_ROOT, ".pause-new-tasks")
        entry["new_tasks_paused"] = os.path.isfile(marker)
        free_gb = (entry.get("disk") or {}).get("free_gb")
        entry["below_reserve"] = bool(free_gb is not None and free_gb < ARTIFACT_CACHE_RESERVE_GB)
        try:
            entry["release"] = os.path.basename(os.path.realpath(CURRENT_LINK))
        except Exception:
            entry["release"] = ""
        units = {}
        try:
            out = subprocess.run(["systemctl", "is-active"] + [u + ".service" for u in VPS_UNITS],
                                 capture_output=True, text=True, timeout=5)
            for unit, state in zip(VPS_UNITS, out.stdout.split()):
                units[unit] = state
        except Exception as exc:
            entry["units_error"] = str(exc)[:120]
        entry["units"] = units
        try:
            entry["load"] = [round(x, 2) for x in os.getloadavg()]
        except Exception:
            pass
        self._store("vps", entry)

    # -- refresh loop ---------------------------------------------------------
    def refresh(self) -> None:
        futures = []
        workers = self._workers()
        for worker in workers:
            mode = str(worker.get("capability_mode") or "")
            every = CONVERTER_SECONDS if mode == "full" else FAST_SECONDS
            if self._is_due("worker:" + str(worker.get("name")), every):
                futures.append(self.pool.submit(self._probe_worker, worker))
            if mode == "full" and self._is_due("v3:" + str(worker.get("name")), SLOW_SECONDS):
                futures.append(self.pool.submit(self._probe_v3, worker))
        if self._is_due("renderfin", FAST_SECONDS):
            futures.append(self.pool.submit(self._probe_renderfin))
        renderfin = self._get("renderfin")
        stats_due = self._is_due("comfy-stats", SLOW_SECONDS)
        if self._is_due("comfy", FAST_SECONDS):
            for server in renderfin.get("servers") or []:
                if str(server.get("status") or "").lower() in ("online", "busy"):
                    futures.append(self.pool.submit(
                        self._probe_comfy, str(server.get("name")), str(server.get("url") or ""), stats_due))
        if self._is_due("db", DB_SECONDS):
            futures.append(self.pool.submit(self._probe_db))
        if self._is_due("dispatch_env", 30.0):
            futures.append(self.pool.submit(self._probe_dispatch_env))
        if self._is_due("gateway", SLOW_SECONDS):
            for box in BOXES:
                if box.get("converter"):
                    futures.append(self.pool.submit(self._probe_gateway, box["id"]))
        if self._is_due("mt", FAST_SECONDS * 2):
            futures.append(self.pool.submit(self._probe_mt))
        if self._is_due("vps", VPS_SECONDS):
            futures.append(self.pool.submit(self._probe_vps))
        for future in futures:
            try:
                future.result(timeout=20)
            except Exception:
                traceback.print_exc()
        self.build()

    def loop(self) -> None:
        while True:
            started = time.monotonic()
            try:
                self.refresh()
            except Exception:
                self.last_build_error = traceback.format_exc()[-500:]
                traceback.print_exc()
            time.sleep(max(0.5, 2.0 - (time.monotonic() - started)))

    # -- composition ----------------------------------------------------------
    def build(self) -> None:
        snapshot = compose(self)
        body = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        text = render_text(snapshot)
        with self.lock:
            self.snapshot = snapshot
            self.snapshot_bytes = body
            self.snapshot_at = time.monotonic()
            self.text = text
        # Keep the last answer on disk, so a restart serves it at once instead
        # of making callers wait for the first full probe round.
        if time.monotonic() - self.persisted_at > 30:
            self.persisted_at = time.monotonic()
            try:
                _write_json_atomic(os.path.join(STATE_DIR, "snapshot.json"), snapshot)
            except Exception:
                traceback.print_exc()

    def load_persisted(self) -> None:
        data = _read_json(os.path.join(STATE_DIR, "snapshot.json"), None)
        if not isinstance(data, dict) or not data.get("boxes_array"):
            return
        data["restored_from_disk_bool"] = True
        with self.lock:
            self.snapshot = data
            self.snapshot_bytes = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.text = render_text(data)
            self.snapshot_at = time.monotonic()


def _slim_status(j: Dict[str, Any]) -> Dict[str, Any]:
    """The part of a converter/adapter/ai-node status the fleet view needs."""
    workload = j.get("workload_control") if isinstance(j.get("workload_control"), dict) else {}
    accepting = workload.get("accepting") if isinstance(workload.get("accepting"), dict) else {}
    preflight = j.get("asset_preflight") if isinstance(j.get("asset_preflight"), dict) else None
    hunyuan = j.get("hunyuan") if isinstance(j.get("hunyuan"), dict) else {}
    ai_node = j.get("ai_node") if isinstance(j.get("ai_node"), dict) else None
    flags = j.get("feature_flags") if isinstance(j.get("feature_flags"), dict) else {}
    caps = j.get("capabilities") if isinstance(j.get("capabilities"), dict) else {}

    def tasks(key: str) -> List[Dict[str, Any]]:
        out = []
        for item in (j.get(key) or [])[:20]:
            if not isinstance(item, dict):
                continue
            minutes = item.get("running_time_mins")
            out.append({
                "ref": _short(item.get("backend_task_id") or item.get("task_id") or item.get("model_name")),
                "mode": item.get("mode") or item.get("workload_class") or item.get("type") or "",
                "stage": item.get("current_stage") or item.get("status") or "",
                "progress": item.get("progress_percent") if item.get("progress_percent") is not None
                else item.get("progress"),
                "running_minutes": round(float(minutes), 1) if isinstance(minutes, (int, float)) else None,
            })
        return out

    slim: Dict[str, Any] = {
        "hostname": j.get("hostname"),
        "server_version": j.get("server_version"),
        "build_id": j.get("build_id"),
        "boot_build_id": j.get("boot_build_id"),
        "deploy_commit": j.get("deploy_commit"),
        "deploy_artifact_sha256": j.get("deploy_artifact_sha256"),
        "boot_artifact_sha256": j.get("boot_artifact_sha256"),
        "deploy_protocol": j.get("deploy_protocol"),
        "deploy_drift": j.get("deploy_drift"),
        "deploy_drift_count": j.get("deploy_drift_count"),
        "deploy_applied_at": j.get("deploy_applied_at"),
        "process_started_at": j.get("process_started_at"),
        "gpu_mode": j.get("gpu_mode"),
        "accepting_autorig": j.get("accepting_autorig"),
        "accepting_hunyuan": j.get("accepting_hunyuan"),
        "accepting_ai_vision": j.get("accepting_ai_vision"),
        "accepting_collection_background": accepting.get("collection_background"),
        "workload_state": workload.get("state"),
        "last_error": j.get("last_error") or workload.get("last_error"),
        "maintenance": j.get("maintenance"),
        "tasks_summary": j.get("tasks_summary") if isinstance(j.get("tasks_summary"), dict) else {},
        "processing": tasks("processing_tasks"),
        "pending": tasks("pending_tasks"),
        "stuck_count": len(j.get("stuck_tasks") or []) if isinstance(j.get("stuck_tasks"), list) else 0,
        "capability_mode": flags.get("converter_capability_mode"),
        "normalized_source": caps.get("normalized_source"),
        "comfy_online": j.get("comfy_online"),
        "comfy_running": j.get("comfy_running"),
        "comfy_pending": j.get("comfy_pending"),
        "memory_percent": (j.get("system_info") or {}).get("memory_percent")
        if isinstance(j.get("system_info"), dict) else j.get("memory_percent"),
        "system_disk_percent": (j.get("system_info") or {}).get("disk_percent")
        if isinstance(j.get("system_info"), dict) else None,
        "uptime_seconds": (j.get("system_info") or {}).get("uptime")
        if isinstance(j.get("system_info"), dict) else None,
    }
    if preflight is not None:
        slim["asset_preflight"] = {
            "status": preflight.get("status") or ("healthy" if preflight.get("healthy") else
                                                  "unhealthy" if preflight.get("healthy") is False else "checked"),
            "healthy": preflight.get("healthy"),
            "checked_at": preflight.get("checked_at_utc"),
            "issues": len(preflight.get("issues") or []),
        }
    cache = j.get("server_status_cache") if isinstance(j.get("server_status_cache"), dict) else {}
    stale = [name for name, comp in (cache.get("components") or {}).items()
             if isinstance(comp, dict) and comp.get("fresh") is False]
    if stale:
        slim["stale_components"] = stale
        slim["stale_detail"] = {name: (cache.get("components") or {}).get(name, {}).get("age_seconds")
                                for name in stale}
    if hunyuan:
        disk = hunyuan.get("disk") if isinstance(hunyuan.get("disk"), dict) else {}
        gpu = hunyuan.get("gpu") if isinstance(hunyuan.get("gpu"), dict) else {}
        slim["hunyuan"] = {
            "installed": hunyuan.get("installed"),
            "enabled": hunyuan.get("enabled"),
            "active": bool(hunyuan.get("active_task")),
            "disk_path": disk.get("path"), "disk_free_gb": disk.get("free_gb"),
            "disk_used_percent": disk.get("used_percent"),
            "gpu_name": gpu.get("name"), "vram_total_mb": gpu.get("vram_total_mb"),
            "vram_free_mb": gpu.get("vram_free_mb"), "vram_used_mb": gpu.get("vram_used_mb"),
            "gpu_util_percent": gpu.get("utilization_percent"),
            "output_contract": hunyuan.get("output_contract_version"),
        }
    if ai_node is not None:
        vram = ai_node.get("vram") if isinstance(ai_node.get("vram"), dict) else {}
        slim["ai_node"] = {
            "blocker": ai_node.get("blocker") or "",
            "blocker_message": ai_node.get("blocker_message") or "",
            "loaded": bool(ai_node.get("llama_pid")),
            "vram_ok": vram.get("ok"), "vram_total_mb": vram.get("total_mb"),
            "vram_free_mb": vram.get("free_mb"), "vram_used_mb": vram.get("used_mb"),
            "vram_error": vram.get("error") or "",
            "max_tasks": ai_node.get("max_tasks"),
            "last_error": ai_node.get("last_error") or "",
        }
        slim["status_word"] = j.get("status")
    models = j.get("ai_models")
    if isinstance(models, dict):
        slim["ai_loaded_model"] = models.get("loaded") or models.get("active") or ""
    return slim


# ----------------------------------------------------------------- compose

def _age(ts: Optional[float]) -> Optional[int]:
    if not ts:
        return None
    return max(0, int(_now() - float(ts)))


def _agent_report(box_id: str) -> Dict[str, Any]:
    report = _read_json(os.path.join(STATE_DIR, "boxes", f"{box_id}.json"), {})
    return report if isinstance(report, dict) else {}


def _lora_report(name: str) -> Dict[str, Any]:
    if not name:
        return {}
    report = _read_json(os.path.join(LORA_BOXES, f"{name}.json"), {})
    return report if isinstance(report, dict) else {}


def _v3_target() -> Dict[str, Any]:
    data = _read_json(os.path.join(STATE_DIR, "v3_target.json"), {})
    return data if isinstance(data, dict) else {}


def _v3_prep(box_id: str) -> Dict[str, Any]:
    data = _read_json(os.path.join(STATE_DIR, "v3_prep.json"), {})
    entry = (data.get("boxes") or {}).get(box_id) if isinstance(data, dict) else None
    return entry if isinstance(entry, dict) else {}


def _endpoint_for(db: Dict[str, Any], marker: str) -> Optional[Dict[str, Any]]:
    if not marker:
        return None
    hits = [e for e in (db.get("endpoints") or []) if marker in str(e.get("url") or "")]
    if not hits:
        return None
    hits.sort(key=lambda e: (int(e.get("enabled") or 0), int(e.get("id") or 0)), reverse=True)
    return hits[0]


def _gpu_from_agent(agent: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    gpu = agent.get("gpu") if isinstance(agent.get("gpu"), dict) else None
    if not gpu:
        return None
    out = {"source": "agent", "name": gpu.get("name") or "",
           "vram_total_mb": gpu.get("vram_total_mb"), "vram_used_mb": gpu.get("vram_used_mb"),
           "vram_free_mb": gpu.get("vram_free_mb"), "util_percent": gpu.get("util_percent"),
           "temp_c": gpu.get("temp_c"), "driver": gpu.get("driver") or "",
           "error": gpu.get("error") or ""}
    return out


def compose(col: Collector) -> Dict[str, Any]:
    now = _now()
    renderfin = col._get("renderfin")
    db = col._get("db")
    mt = col._get("mt")
    vps = col._get("vps")
    target = _v3_target()
    target_sha = str(target.get("artifact_sha256") or "").strip().lower() or None
    servers = {str(s.get("name")): s for s in renderfin.get("servers") or []}
    active_by_server: Dict[str, List[Dict[str, Any]]] = {}
    pending_render = 0
    for task in renderfin.get("active") or []:
        if task.get("server"):
            active_by_server.setdefault(str(task["server"]), []).append(task)
        else:
            pending_render += 1
    processing_by_worker = db.get("processing_by_worker") or {}
    blender_workers = (mt.get("workers") or {}) if mt.get("ok") else {}

    boxes_out: List[Dict[str, Any]] = []
    for box in BOXES:
        bid = box["id"]
        roles: List[str] = []
        blockers: List[str] = []
        warnings: List[str] = []
        notes: List[str] = []
        busy_with: List[Dict[str, Any]] = []
        services: Dict[str, Any] = {}
        queue_depth = 0
        seen: List[float] = []
        online_parts: List[bool] = []
        build: Dict[str, Any] = {}
        gpu: Optional[Dict[str, Any]] = None
        disks: List[Dict[str, Any]] = []
        disk_source = ""
        last_error = ""
        agent = _agent_report(bid)
        agent_age = _age(agent.get("received_at"))
        agent_fresh = agent_age is not None and agent_age <= AGENT_STALE_SECONDS

        # ---- full converter (f1/f2/f7/f11/f13)
        conv_name = box.get("converter")
        dispatch: Dict[str, Any] = {}
        if conv_name:
            entry = col._get("worker:" + conv_name)
            endpoint = _endpoint_for(db, box.get("endpoint", ""))
            table_enabled = bool(endpoint and int(endpoint.get("enabled") or 0))
            env = col._get("dispatch_env")
            logical = f"https://converter-{bid}.freestock.online"
            env_disabled = any(logical in str(u) for u in env.get("disabled_urls") or [])
            transport = (env.get("transports") or {}).get(logical) or "public gateway"
            gateway = col._get("gateway:" + bid)
            dispatch["worker_endpoints_enabled"] = table_enabled
            dispatch["env_disabled"] = env_disabled if env.get("ok") else None
            dispatch["autorig_endpoint_enabled"] = bool(table_enabled and not env_disabled)
            dispatch["autorig_endpoint_role"] = (endpoint or {}).get("role")
            dispatch["backend_route"] = transport
            dispatch["public_gateway_online"] = gateway.get("online")
            if table_enabled and env_disabled:
                warnings.append("AutoRig dispatch off: listed in AUTORIG_DISABLED_WORKERS of autorig-storage")
            if (transport == "public gateway" and gateway.get("online") is False
                    and table_enabled and not env_disabled):
                blockers.append("backend reaches it through the public gateway, whose tunnel is offline")
            reg = entry.get("registry") or {}
            dispatch["hunyuan_enabled"] = bool(reg.get("enabled"))
            if reg.get("disabled_reason") and not reg.get("enabled"):
                dispatch["hunyuan_disabled_reason"] = reg.get("disabled_reason")
            dispatch["ai_route_enabled"] = bool(reg.get("ai_vision_enabled"))
            status = entry.get("status") or {}
            reachable = bool(entry.get("ok"))
            online_parts.append(reachable)
            if entry.get("last_ok_at"):
                seen.append(float(entry["last_ok_at"]))
            conv: Dict[str, Any] = {"reachable": reachable,
                                    "probe_error": entry.get("error") or "",
                                    "slow_status_answer": bool(entry.get("slow")),
                                    "last_ok_age_seconds": _age(entry.get("last_ok_at"))}
            roles.append("converter")
            if reachable:
                summary = status.get("tasks_summary") or {}
                processing = status.get("processing") or []
                pending = status.get("pending") or []
                healthy = (status.get("maintenance") is not True
                           and (status.get("asset_preflight") or {}).get("healthy") is not False
                           and not status.get("stuck_count"))
                conv.update({
                    "accepting": {
                        "autorig": status.get("accepting_autorig"),
                        "hunyuan": status.get("accepting_hunyuan"),
                        "ai_vision": status.get("accepting_ai_vision"),
                        "collection_background": status.get("accepting_collection_background"),
                    },
                    "workload_state": status.get("workload_state"),
                    "processing": processing, "pending_count": len(pending),
                    "pending": pending[:5],
                    "tasks_summary": summary,
                    "asset_preflight": status.get("asset_preflight"),
                    "counts_as_healthy_full_converter": bool(healthy),
                    "maintenance": status.get("maintenance"),
                    "memory_percent": status.get("memory_percent"),
                    "uptime_hours": round((status.get("uptime_seconds") or 0) / 3600.0, 1)
                    if status.get("uptime_seconds") else None,
                })
                queue_depth += int(summary.get("queue_size") or len(pending) or 0)
                for task in processing:
                    busy_with.append({"service": "converter", "what": task.get("mode") or "conversion",
                                      "stage": task.get("stage"), "ref": task.get("ref"),
                                      "progress": task.get("progress"),
                                      "running_minutes": task.get("running_minutes")})
                conv["can_take_autorig_work"] = bool(
                    dispatch.get("autorig_endpoint_enabled") and healthy
                    and (status.get("accepting_autorig") or processing))
                if status.get("last_error"):
                    last_error = str(status.get("last_error"))
                accepting_nothing = not any([status.get("accepting_autorig"),
                                             status.get("accepting_hunyuan"),
                                             status.get("accepting_ai_vision")])
                if accepting_nothing and not processing:
                    blockers.append("converter accepts nothing" +
                                    (f" ({status.get('last_error')})" if status.get("last_error") else ""))
                if status.get("stale_components"):
                    warnings.append("status cache stale: " + ", ".join(
                        f"{k} {v}s" for k, v in (status.get("stale_detail") or {}).items()))
                preflight = status.get("asset_preflight") or {}
                if preflight.get("healthy") is False:
                    blockers.append("asset_preflight " + str(preflight.get("status") or "unhealthy") +
                                    ": not counted as a healthy converter")
                if status.get("maintenance") is True:
                    blockers.append("maintenance mode")
                if status.get("stuck_count"):
                    blockers.append(f"{status.get('stuck_count')} stuck tasks")
                if pending and not processing and accepting_nothing:
                    blockers.append(f"{len(pending)} tasks waiting in its queue while it accepts nothing")
                if status.get("deploy_drift") not in (None, "clean"):
                    warnings.append(f"deploy drift: {status.get('deploy_drift')}")
                build = {
                    "server_version": status.get("server_version"),
                    "build_id": status.get("build_id"),
                    "deploy_commit": status.get("deploy_commit"),
                    "deploy_artifact_sha256": status.get("deploy_artifact_sha256"),
                    "deploy_protocol": status.get("deploy_protocol"),
                    "deploy_drift": status.get("deploy_drift"),
                    "deploy_applied_at": status.get("deploy_applied_at"),
                    "process_started_at": status.get("process_started_at"),
                }
                hy = status.get("hunyuan") or {}
                if hy.get("installed"):
                    roles.append("hunyuan")
                if hy.get("gpu_name"):
                    gpu = {"source": "converter", "name": hy.get("gpu_name"),
                           "vram_total_mb": hy.get("vram_total_mb"), "vram_used_mb": hy.get("vram_used_mb"),
                           "vram_free_mb": hy.get("vram_free_mb"), "util_percent": hy.get("gpu_util_percent"),
                           "error": ""}
                if hy.get("disk_path") and hy.get("disk_free_gb") is not None:
                    disks.append({"drive": str(hy.get("disk_path"))[:2], "free_gb": hy.get("disk_free_gb"),
                                  "used_percent": hy.get("disk_used_percent"), "source": "converter"})
                    disk_source = "converter"
                if status.get("accepting_ai_vision") or (status.get("ai_loaded_model")):
                    roles.append("ai-node")
            else:
                if dispatch.get("autorig_endpoint_enabled"):
                    blockers.append(f"converter unreachable ({entry.get('error') or 'no answer'}) "
                                    "while AutoRig dispatch has it enabled")
                else:
                    blockers.append(f"converter unreachable ({entry.get('error') or 'no answer'})")
                stale = entry.get("stale_status") or {}
                if stale:
                    build = {"server_version": stale.get("server_version"),
                             "deploy_artifact_sha256": stale.get("deploy_artifact_sha256"),
                             "stale": True}
            services["converter"] = conv
            # V3 readiness
            v3_probe = col._get("v3:" + conv_name)
            artifact = (build or {}).get("deploy_artifact_sha256")
            v3 = {
                "target_bool": True,
                "endpoint_present": v3_probe.get("endpoint_present"),
                "endpoint_checked_age_seconds": _age(v3_probe.get("checked_at")),
                "normalized_source_capability": (status or {}).get("normalized_source"),
                "deployed_artifact_sha256": artifact,
                "target_artifact_sha256": target_sha,
                "matches_target": bool(target_sha and artifact and str(artifact).lower() == target_sha),
                "deploy_protocol": (build or {}).get("deploy_protocol"),
            }
            prep = _v3_prep(bid)
            if prep:
                v3["prep"] = prep
            v3["ready"] = bool(reachable and v3["endpoint_present"] and v3["matches_target"]
                               and (status.get("asset_preflight") or {}).get("healthy") is not False)
            reasons = []
            if not reachable:
                reasons.append("converter not serving")
            if not v3["endpoint_present"]:
                reasons.append("no V3 endpoint (/api-converter-glb/v3/normalize)")
            if not target_sha:
                reasons.append("no accepted V3 artifact yet")
            elif not v3["matches_target"]:
                reasons.append("serving build is not the accepted V3 artifact")
            if bid == "f7":
                reasons.append("dispatch off on purpose: Unity export broken")
            v3["blocked_by"] = reasons
        else:
            v3 = {"target_bool": False, "ready": None,
                  "note": "not a converter target of the V3 cutover (render/LLM box)"}

        # ---- Hunyuan-only adapter (f12, raptor)
        hy_name = box.get("hunyuan")
        if hy_name:
            entry = col._get("worker:" + hy_name)
            reg = entry.get("registry") or {}
            status = entry.get("status") or {}
            if entry.get("last_ok_at"):
                seen.append(float(entry["last_ok_at"]))
            hy_service = {"reachable": bool(entry.get("ok")), "enabled_in_pool": bool(reg.get("enabled")),
                          "probe_error": entry.get("error") or ""}
            if entry.get("ok"):
                hy = status.get("hunyuan") or {}
                hy_service.update({"accepting": status.get("accepting_hunyuan"),
                                   "active": hy.get("active"), "server_version": status.get("server_version"),
                                   "gpu_mode": status.get("gpu_mode")})
                if hy.get("disk_path") and hy.get("disk_free_gb") is not None and not disks:
                    disks.append({"drive": str(hy.get("disk_path"))[:2], "free_gb": hy.get("disk_free_gb"),
                                  "used_percent": hy.get("disk_used_percent"), "source": "hunyuan-adapter"})
                    disk_source = disk_source or "hunyuan-adapter"
                if hy.get("active"):
                    busy_with.append({"service": "hunyuan", "what": "image-to-3D generation"})
                if not build:
                    build = {"server_version": status.get("server_version"),
                             "process_started_at": status.get("process_started_at")}
            if reg.get("enabled"):
                roles.append("hunyuan")
            elif reg.get("disabled_reason"):
                hy_service["disabled_reason"] = reg.get("disabled_reason")
            services["hunyuan"] = hy_service

        # ---- ai-node
        ai_name = box.get("ai")
        if ai_name:
            entry = col._get("worker:" + ai_name)
            status = entry.get("status") or {}
            ai = {"reachable": bool(entry.get("ok")), "probe_error": entry.get("error") or ""}
            online_parts.append(bool(entry.get("ok")))
            if entry.get("last_ok_at"):
                seen.append(float(entry["last_ok_at"]))
            node = status.get("ai_node") or {}
            if entry.get("ok"):
                roles.append("ai-node")
                summary = status.get("tasks_summary") or {}
                ai.update({"accepting": status.get("accepting_ai_vision"),
                           "loaded": node.get("loaded"), "blocker": node.get("blocker") or "",
                           "processing": int(summary.get("processing") or 0),
                           "pending": int(summary.get("pending") or 0),
                           "server_version": status.get("server_version")})
                if summary.get("processing"):
                    busy_with.append({"service": "ai-node", "what": "LLM/Vision",
                                      "count": int(summary.get("processing") or 0)})
                queue_depth += int(summary.get("pending") or 0)
                if node.get("blocker"):
                    msg = str(node.get("blocker_message") or node.get("blocker"))
                    low = msg.lower()
                    if "timed out" in low:
                        # nvidia-smi stalls while the card renders: slow, not broken.
                        warnings.append("ai-node: nvidia-smi timed out (GPU busy)")
                    elif node.get("blocker") == "vram_unknown" or "nvidia" in low:
                        blockers.append(f"ai-node blocked: {msg}")
                    else:
                        # Yielding the GPU to ComfyUI/the converter is the design, not a fault.
                        notes.append(f"ai-node yields: {msg}")
                if node.get("vram_total_mb") and not gpu:
                    gpu = {"source": "ai-node", "name": box.get("gpu_hint"),
                           "vram_total_mb": node.get("vram_total_mb"), "vram_used_mb": node.get("vram_used_mb"),
                           "vram_free_mb": node.get("vram_free_mb"), "error": ""}
                if node.get("vram_error") and not gpu and "timed out" not in str(node.get("vram_error")).lower():
                    gpu = {"source": "ai-node", "name": box.get("gpu_hint"), "error": node.get("vram_error")}
            else:
                warnings.append(f"ai-node unreachable ({entry.get('error') or 'no answer'})")
            services["ai_node"] = ai

        # ---- render (ComfyUI via renderfin)
        render_name = box.get("render")
        if render_name:
            server = servers.get(render_name) or {}
            comfy = col._get("comfy:" + render_name)
            state = str(server.get("status") or "").lower()
            on = state in ("online", "busy")
            online_parts.append(on)
            render = {"renderfin_status": server.get("status") or "unknown",
                      "gpu_name": server.get("gpu_name"), "workflows": server.get("workflows"),
                      "comfy_queue_running": comfy.get("running"), "comfy_queue_pending": comfy.get("pending"),
                      "queue_error": comfy.get("queue_error") if not comfy.get("queue_ok") else ""}
            if comfy.get("checked_at") and comfy.get("queue_ok"):
                seen.append(float(comfy["checked_at"]))
            if on:
                roles.append("render")
            for task in active_by_server.get(render_name, []):
                busy_with.append({"service": "render", "what": task.get("workflow"),
                                  "stage": task.get("status"), "ref": task.get("id"),
                                  "since_utc": _iso(_ts(task.get("started_at")))})
            queue_depth += int(comfy.get("pending") or 0)
            stats = comfy.get("stats") or {}
            if stats:
                render["comfyui_version"] = stats.get("comfyui_version")
                if not gpu or gpu.get("source") != "agent":
                    gpu = {"source": "comfy", "name": stats.get("gpu_name") or server.get("gpu_name"),
                           "vram_total_mb": stats.get("vram_total_mb"), "vram_free_mb": stats.get("vram_free_mb"),
                           "error": ""}
            services["render"] = render

        # ---- LoRA sync report (render boxes): one drive's free bytes
        lora = _lora_report(box.get("lora", ""))
        if lora:
            free = lora.get("free_bytes")
            dirs = lora.get("loras_dirs") or []
            drive = str(dirs[0])[:2] if dirs else ""
            services["lora_sync"] = {"reported_age_seconds": _age(lora.get("reported_at")),
                                     "agent_version": lora.get("agent_version")}
            if drive and free is not None and not any(d.get("drive") == drive for d in disks):
                disks.append({"drive": drive, "free_gb": round(int(free) / 1024 ** 3, 1),
                              "source": "lora-sync"})
                disk_source = disk_source or "lora-sync"
            if lora.get("reported_at"):
                seen.append(float(lora["reported_at"]))

        # ---- MT Blender worker
        for worker_name, age in blender_workers.items():
            if _box_id_for(worker_name) == bid:
                roles.append("blender-worker")
                services["blender_worker"] = {"name": worker_name, "last_seen_seconds": age}

        # ---- box agent (per-drive disk, GPU, tasks, quarantine)
        if agent:
            services["fleet_agent"] = {"reported_age_seconds": agent_age,
                                       "agent_version": agent.get("agent_version"), "fresh": agent_fresh}
            if agent_fresh:
                if agent.get("received_at"):
                    seen.append(float(agent["received_at"]))
                drives = [d for d in (agent.get("drives") or []) if isinstance(d, dict)]
                if drives:
                    disks = [{"drive": d.get("drive"), "size_gb": d.get("size_gb"), "free_gb": d.get("free_gb"),
                              "used_percent": d.get("used_percent"), "label": d.get("label") or "",
                              "source": "agent"} for d in drives]
                    disk_source = "agent"
                agpu = _gpu_from_agent(agent)
                if agpu:
                    gpu = agpu
                    if agpu.get("error"):
                        blockers = [b for b in blockers
                                    if not b.startswith("ai-node blocked: cannot read free VRAM")]
                        blockers.append(f"GPU driver not responding: {agpu.get('error')}")
                trainers = int((agent.get("processes") or {}).get("trainer") or 0)
                if trainers:
                    roles.append("trainer")
                    busy_with.append({"service": "trainer", "what": "LoRA training", "count": trainers})
                if agent.get("boot_utc"):
                    services["fleet_agent"]["boot_utc"] = agent.get("boot_utc")
                if agent.get("ram_total_gb"):
                    services["fleet_agent"]["ram_free_gb"] = agent.get("ram_free_gb")
                    services["fleet_agent"]["ram_total_gb"] = agent.get("ram_total_gb")
                services["fleet_agent"]["scheduled_tasks"] = [
                    f"{t.get('name')} [{t.get('state')}]" for t in (agent.get("tasks") or [])[:30]
                    if isinstance(t, dict)]
                if agent.get("listeners"):
                    services["fleet_agent"]["listening_ports"] = agent.get("listeners")
        quarantine = []
        for item in (agent.get("quarantine") or []):
            if isinstance(item, dict) and item.get("gb"):
                quarantine.append({"path": item.get("path"), "gb": item.get("gb"),
                                   "measured_at": item.get("measured_at"),
                                   "purge": "only with the owner's OK"})

        # ---- disk verdicts
        work_drives = {d.upper() for d in box.get("work_drives") or []}
        for disk in disks:
            free_gb = disk.get("free_gb")
            if free_gb is None:
                continue
            drive = str(disk.get("drive") or "")
            disk["work_drive"] = (not work_drives) or drive.upper() in work_drives
            if not disk["work_drive"]:
                continue
            limit = LOW_DISK_SYSTEM_GB if drive.upper().startswith("C") else LOW_DISK_GB
            if float(free_gb) <= 0.05:
                blockers.append(f"disk {drive} FULL (0 GB free)")
            elif float(free_gb) < limit:
                warnings.append(f"disk {drive} low: {free_gb} GB free")

        # ---- roll-up
        if box.get("out_of_fleet"):
            state = "out_of_fleet"
        elif not any(online_parts) and online_parts:
            state = "offline"
        elif blockers and not busy_with:
            state = "blocked"
        elif busy_with:
            state = "busy"
        elif any(online_parts):
            state = "degraded" if (blockers or warnings) else "idle"
        else:
            state = "unknown"
        roles = sorted(set(roles), key=lambda r: ["converter", "render", "hunyuan", "ai-node",
                                                   "blender-worker", "trainer"].index(r)
                       if r in ("converter", "render", "hunyuan", "ai-node", "blender-worker", "trainer") else 9)
        last_seen = max(seen) if seen else None
        if state == "busy":
            line = "busy: " + "; ".join(
                f"{b.get('service')} {b.get('what')}" + (f" [{b.get('stage')}]" if b.get("stage") else "")
                for b in busy_with)
        elif state in ("blocked", "degraded", "offline"):
            line = state + ": " + "; ".join((blockers + warnings)[:3]) if (blockers or warnings) else state
        else:
            line = state
        boxes_out.append({
            "id": bid,
            "in_fleet_bool": not box.get("out_of_fleet"),
            "note_string": box.get("out_of_fleet") or box.get("note") or "",
            "roles_array": roles,
            "online_bool": bool(any(online_parts)),
            "state_string": state,
            "summary_string": line[:400],
            "busy_bool": bool(busy_with),
            "busy_with_array": busy_with,
            "queue_depth_int": int(queue_depth),
            "dispatch_object": dispatch,
            "build_object": build,
            "v3_object": v3,
            "gpu_object": gpu or {"name": box.get("gpu_hint"), "source": "hint"},
            "disks_array": disks,
            "disk_source_string": disk_source or "none",
            "quarantine_array": quarantine,
            "blockers_array": blockers,
            "warnings_array": warnings,
            "notes_array": notes,
            "last_error_string": last_error,
            "last_seen_utc": _iso(last_seen),
            "last_seen_age_seconds_int": _age(last_seen),
            "services_object": services,
        })

    in_fleet = [b for b in boxes_out if b["in_fleet_bool"]]
    converters = [b for b in in_fleet if "converter" in (b.get("services_object") or {})]
    healthy_full = [b["id"] for b in converters
                    if (b["services_object"]["converter"].get("counts_as_healthy_full_converter")
                        and b["services_object"]["converter"].get("reachable"))]
    accepting_autorig = [b["id"] for b in converters
                         if ((b["services_object"]["converter"].get("accepting") or {}).get("autorig"))]
    dispatch_on = [b["id"] for b in converters if (b.get("dispatch_object") or {}).get("autorig_endpoint_enabled")]
    ai_ready = [b["id"] for b in in_fleet
                if (b["services_object"].get("ai_node") or {}).get("accepting")
                or ((b["services_object"].get("converter") or {}).get("accepting") or {}).get("ai_vision")]
    hunyuan_ready = [b["id"] for b in in_fleet
                     if ((b["services_object"].get("converter") or {}).get("accepting") or {}).get("hunyuan")
                     or ((b["services_object"].get("hunyuan") or {}).get("accepting")
                         and (b["services_object"].get("hunyuan") or {}).get("enabled_in_pool"))]
    summary = {
        "boxes_total_int": len(boxes_out),
        "in_fleet_int": len(in_fleet),
        "online_int": sum(1 for b in in_fleet if b["online_bool"]),
        "busy_int": sum(1 for b in in_fleet if b["busy_bool"]),
        "blocked_array": [b["id"] for b in in_fleet if b["state_string"] in ("blocked", "offline")],
        "autorig_dispatch_enabled_array": dispatch_on,
        "autorig_accepting_array": accepting_autorig,
        "autorig_capable_array": [b["id"] for b in converters
                                  if (b["services_object"]["converter"].get("can_take_autorig_work"))],
        "healthy_full_converters_array": healthy_full,
        "hunyuan_ready_array": hunyuan_ready,
        "ai_ready_array": ai_ready,
        "v3_ready_array": [b["id"] for b in in_fleet if (b.get("v3_object") or {}).get("ready")],
        "v3_target_artifact_sha256": target_sha,
        "v3_target_note": str(target.get("note") or "")[:300],
        "low_disk_array": sorted({b["id"] for b in in_fleet
                                  for w in b["blockers_array"] + b["warnings_array"] if w.startswith("disk ")}),
    }
    def worker_box(worker_api: Any) -> str:
        match = re.search(r"converter-(f\d+)\.", str(worker_api or ""))
        return match.group(1) if match else (str(worker_api or "") or "none")

    counts = db.get("task_counts") or {}
    queues = {
        "autorig_tasks_object": {"created_int": int(counts.get("created") or 0),
                                 "processing_int": int(counts.get("processing") or 0),
                                 "processing_by_box_object": {
                                     worker_box(k): v for k, v in processing_by_worker.items()}},
        "renderfin_object": {"pending_int": pending_render,
                             "rendering_int": sum(len(v) for v in active_by_server.values())},
        "converter_queue_int": sum(int((b["services_object"].get("converter") or {}).get("pending_count") or 0)
                                   for b in boxes_out),
    }
    last_24h: Dict[str, Dict[str, int]] = {}
    for worker_api, status_word, count in db.get("last_24h") or []:
        last_24h.setdefault(worker_box(worker_api), {})[str(status_word)] = int(count)
    queues["autorig_last_24h_by_box_object"] = last_24h

    vps_out = {
        "host_string": "autorig.online (way-fr)",
        "release_string": vps.get("release"),
        "disk_object": vps.get("disk"),
        "new_tasks_paused_bool": vps.get("new_tasks_paused"),
        "artifact_cache_reserve_gb_float": ARTIFACT_CACHE_RESERVE_GB,
        "below_reserve_bool": vps.get("below_reserve"),
        "units_object": vps.get("units") or {},
        "load_array": vps.get("load"),
        "checked_age_seconds_int": _age(vps.get("checked_at")),
    }
    sources = {
        "renderfin_age_seconds_int": _age(renderfin.get("checked_at")),
        "renderfin_ok_bool": bool(renderfin.get("ok")),
        "db_age_seconds_int": _age(db.get("checked_at")),
        "db_ok_bool": bool(db.get("ok")),
        "mt_blender_ok_bool": bool(mt.get("ok")),
    }
    return {
        "success_bool": True,
        "schema_string": SCHEMA,
        "generated_at_utc": _iso(now),
        "server_time_unix_int": int(now),
        "snapshot_ttl_seconds_float": 2.0,
        "summary_object": summary,
        "queues_object": queues,
        "vps_object": vps_out,
        "boxes_array": boxes_out,
        "sources_object": sources,
        "rules_array": [
            "This is the one call for fleet state; do not probe boxes one by one over SSH.",
            "f5 is out of the fleet (owner order 2026-10-08).",
            "worker-4090 is the owner's PC: never start, stop or reboot it without his go-ahead.",
            "f12: ask the owner before a reboot (his programs run there).",
            "Quarantine folders (_retired_*) are purged only with the owner's OK.",
        ],
        "help_string": "GET /api/fleet?format=text for a table; GET /api/fleet/box/<id> for one box.",
    }


def render_text(snapshot: Dict[str, Any]) -> str:
    lines = [f"AutoRig fleet {snapshot.get('generated_at_utc')}  (GET /api/fleet for JSON)"]
    vps = snapshot.get("vps_object") or {}
    disk = vps.get("disk_object") or {}
    lines.append(
        f"VPS  disk {disk.get('free_gb')} GB free ({disk.get('used_percent')}%)  "
        f"new tasks paused: {vps.get('new_tasks_paused_bool')}  release {vps.get('release_string')}")
    queues = snapshot.get("queues_object") or {}
    at = queues.get("autorig_tasks_object") or {}
    rf = queues.get("renderfin_object") or {}
    lines.append(f"queues  autorig created={at.get('created_int')} processing={at.get('processing_int')}  "
                 f"renderfin pending={rf.get('pending_int')} rendering={rf.get('rendering_int')}  "
                 f"converter queue={queues.get('converter_queue_int')}")
    s = snapshot.get("summary_object") or {}
    lines.append(f"AutoRig converters able to take work: {','.join(s.get('autorig_capable_array') or []) or '-'}  "
                 f"(dispatch on: {','.join(s.get('autorig_dispatch_enabled_array') or []) or '-'})  "
                 f"hunyuan: {','.join(s.get('hunyuan_ready_array') or []) or '-'}  "
                 f"ai: {','.join(s.get('ai_ready_array') or []) or '-'}  "
                 f"V3 ready: {','.join(s.get('v3_ready_array') or []) or '-'}")
    lines.append("")
    for box in snapshot.get("boxes_array") or []:
        gpu = box.get("gpu_object") or {}
        gpu_text = gpu.get("name") or "?"
        if gpu.get("vram_total_mb"):
            gpu_text += f" {round((gpu.get('vram_free_mb') or 0) / 1024, 1)}/{round(gpu['vram_total_mb'] / 1024, 1)}G free"
        if gpu.get("error"):
            gpu_text += f" ERR {gpu.get('error')}"
        disks = " ".join(f"{d.get('drive')}{d.get('free_gb')}G" for d in box.get("disks_array") or [])
        busy = "; ".join(
            f"{b.get('service')}:{b.get('what')}" + (f"[{b.get('stage')}]" if b.get("stage") else "")
            for b in box.get("busy_with_array") or [])
        build = box.get("build_object") or {}
        version = (build.get("build_id") or build.get("server_version") or "")[:12]
        lines.append(
            f"{box['id']:<12} {box['state_string']:<12} roles={','.join(box.get('roles_array') or []) or '-'}"
            f"  q={box.get('queue_depth_int')}  gpu={gpu_text}  disk={disks or '?'}  build={version or '-'}"
            f"  v3={'ready' if (box.get('v3_object') or {}).get('ready') else ('no' if (box.get('v3_object') or {}).get('target_bool') else 'n/a')}")
        if busy:
            lines.append(f"{'':<12} busy: {busy}")
        for item in box.get("blockers_array") or []:
            lines.append(f"{'':<12} BLOCKER: {item}")
        for item in box.get("warnings_array") or []:
            lines.append(f"{'':<12} warn: {item}")
        if box.get("note_string"):
            lines.append(f"{'':<12} note: {box['note_string']}")
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------- box reports

def _authenticate(box_header: str, auth_header: str) -> Optional[str]:
    bid = _box_id_for(box_header)
    token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
    if not bid or not token:
        return None
    for path in (AGENT_KEYS, LORA_KEYS):
        data = _read_json(path, {})
        if not isinstance(data, dict):
            continue
        for name, key in data.items():
            if _box_id_for(name) == bid and isinstance(key, str) and key and hmac.compare_digest(token, key):
                return bid
    return None


def _clean_report(bid: str, body: Dict[str, Any]) -> Dict[str, Any]:
    def num(value: Any) -> Optional[float]:
        try:
            return round(float(value), 2)
        except Exception:
            return None

    drives = []
    for d in (body.get("drives") or [])[:26]:
        if isinstance(d, dict):
            drives.append({"drive": str(d.get("drive") or "")[:3], "label": str(d.get("label") or "")[:40],
                           "size_gb": num(d.get("size_gb")), "free_gb": num(d.get("free_gb")),
                           "used_percent": num(d.get("used_percent"))})
    gpu_in = body.get("gpu") if isinstance(body.get("gpu"), dict) else {}
    gpu = {"name": str(gpu_in.get("name") or "")[:80], "driver": str(gpu_in.get("driver") or "")[:20],
           "vram_total_mb": num(gpu_in.get("vram_total_mb")), "vram_used_mb": num(gpu_in.get("vram_used_mb")),
           "vram_free_mb": num(gpu_in.get("vram_free_mb")), "util_percent": num(gpu_in.get("util_percent")),
           "temp_c": num(gpu_in.get("temp_c")), "error": str(gpu_in.get("error") or "")[:160]} if gpu_in else None
    tasks = []
    for t in (body.get("tasks") or [])[:60]:
        if isinstance(t, dict):
            tasks.append({"name": str(t.get("name") or "")[:100], "state": str(t.get("state") or "")[:20],
                          "last_run": str(t.get("last_run") or "")[:30], "last_result": t.get("last_result"),
                          "expected": str(t.get("expected") or "")[:20]})
    quarantine = []
    for q in (body.get("quarantine") or [])[:20]:
        if isinstance(q, dict):
            quarantine.append({"path": str(q.get("path") or "")[:200], "gb": num(q.get("gb")),
                               "measured_at": str(q.get("measured_at") or "")[:30]})
    processes = {str(k)[:30]: int(v) for k, v in (body.get("processes") or {}).items()
                 if isinstance(v, (int, float))} if isinstance(body.get("processes"), dict) else {}
    listeners = [int(p) for p in (body.get("listeners") or [])[:40] if isinstance(p, (int, float))]
    return {
        "box": bid, "received_at": _now(),
        "agent_version": str(body.get("agent_version") or "")[:40],
        "host": str(body.get("host") or "")[:60],
        "boot_utc": str(body.get("boot_utc") or "")[:40],
        "ram_total_gb": num(body.get("ram_total_gb")), "ram_free_gb": num(body.get("ram_free_gb")),
        "drives": drives, "gpu": gpu, "tasks": tasks, "quarantine": quarantine,
        "processes": processes, "listeners": listeners,
    }


# ------------------------------------------------------------------- HTTP

COLLECTOR = Collector()


class Handler(BaseHTTPRequestHandler):
    server_version = "autorig-fleet/1"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet: nginx logs requests
        return

    def _send(self, code: int, body: bytes, ctype: str = "application/json; charset=utf-8",
              extra: Optional[Dict[str, str]] = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code: int, data: Any) -> None:
        self._send(code, json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = urllib.parse.parse_qs(parsed.query)
        with COLLECTOR.lock:
            body = COLLECTOR.snapshot_bytes
            snapshot = COLLECTOR.snapshot
            text = COLLECTOR.text
            age = time.monotonic() - COLLECTOR.snapshot_at if COLLECTOR.snapshot_at else None
        if path == "/api/fleet":
            if not snapshot:
                return self._json(503, {"success_bool": False, "error_string": "warming_up"})
            fmt = (query.get("format") or [""])[0]
            if fmt in ("text", "txt"):
                return self._send(200, text.encode("utf-8"), "text/plain; charset=utf-8")
            return self._send(200, body, extra={"X-Fleet-Snapshot-Age": f"{age:.1f}" if age else "0"})
        if path.startswith("/api/fleet/box/"):
            bid = _box_id_for(path.rsplit("/", 1)[-1])
            for box in (snapshot or {}).get("boxes_array") or []:
                if box.get("id") == bid:
                    return self._json(200, {"success_bool": True,
                                            "generated_at_utc": snapshot.get("generated_at_utc"), "box_object": box})
            return self._json(404, {"success_bool": False, "error_string": "unknown_box",
                                    "boxes_array": [b["id"] for b in BOXES]})
        if path == "/api/fleet/health":
            return self._json(200 if snapshot and age is not None and age < 30 else 503,
                              {"success_bool": bool(snapshot), "snapshot_age_seconds": age,
                               "build_error": COLLECTOR.last_build_error[-300:]})
        if path == "/api/fleet/agent.ps1":
            try:
                with open(AGENT_SCRIPT, "rb") as handle:
                    script = handle.read()
            except OSError:
                return self._json(404, {"success_bool": False, "error_string": "agent_not_deployed"})
            return self._send(200, script, "text/plain; charset=utf-8")
        return self._json(404, {"success_bool": False, "error_string": "not_found"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path.rstrip("/") != "/api/fleet/report":
            return self._json(404, {"success_bool": False, "error_string": "not_found"})
        bid = _authenticate(self.headers.get("X-AutoRig-Box", ""), self.headers.get("Authorization", ""))
        if not bid:
            return self._json(401, {"success_bool": False, "error_string": "box_unauthorised"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0 or length > 512 * 1024:
            return self._json(413, {"success_bool": False, "error_string": "bad_length"})
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8-sig"))
        except Exception:
            return self._json(400, {"success_bool": False, "error_string": "bad_json"})
        if not isinstance(body, dict):
            return self._json(400, {"success_bool": False, "error_string": "bad_json"})
        report = _clean_report(bid, body)
        _write_json_atomic(os.path.join(STATE_DIR, "boxes", f"{bid}.json"), report)
        return self._json(200, {"success_bool": True, "box_string": bid, "server_time_unix_int": int(_now())})


def main() -> None:
    os.makedirs(os.path.join(STATE_DIR, "boxes"), exist_ok=True)
    # Serve the last persisted snapshot at once; the first probe round replaces it.
    COLLECTOR.load_persisted()
    threading.Thread(target=COLLECTOR.loop, name="fleet-refresh", daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    print(f"[fleet] listening on 127.0.0.1:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
