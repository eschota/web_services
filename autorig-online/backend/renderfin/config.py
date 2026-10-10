"""Configuration for the renderfin service (env-driven, mirrors backend/config.py style)."""
from __future__ import annotations

import json
import os
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = PACKAGE_DIR / "assets"
WORKFLOWS_DIR = ASSETS_DIR / "workflows"
MASKS_DIR = ASSETS_DIR / "masks"

DATA_DIR = Path(os.getenv("RENDERFIN_DATA_DIR", "/var/autorig/renderfin"))
RENDER_DIR = DATA_DIR / "render"
DB_DIR = DATA_DIR / "db"
TMP_DIR = DATA_DIR / "tmp"
SERVERS_DIR = Path(os.getenv("RENDERFIN_SERVERS_DIR", str(DATA_DIR / "servers")))
DB_PATH = DB_DIR / "renderfin.db"

PUBLIC_BASE_URL = os.getenv(
    "RENDERFIN_PUBLIC_BASE_URL", "https://autorig.online/renderfin"
).rstrip("/")

ALLOWED_HTTP_HOSTS = {
    h.strip()
    for h in os.getenv(
        "RENDERFIN_ALLOWED_HTTP_HOSTS",
        "5.129.157.224,37.192.2.126,127.0.0.1,localhost",
    ).split(",")
    if h.strip()
}

# "user:password" for workers behind the basic-auth nginx edge (worker-4090)
WORKER_BASIC_AUTH = os.getenv("RENDERFIN_WORKER_BASIC_AUTH", "")

# A shared ComfyUI box can hold a submitted prompt behind other work for a long
# time; the wall-clock ceiling has to cover the queue wait, not just the render.
TASK_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_TASK_TIMEOUT_SECONDS", "5400"))
# Managed farm prompts have an exact, idempotent host-side preemption contract.
# A prompt which makes no observable progress for an hour is therefore recalled
# and the same durable RenderTask is returned to Pending without charging an
# attempt.  Unmanaged Comfy prompts retain the older wall-clock timeout above:
# they cannot be safely requeued after an ambiguous process-wide interrupt.
MANAGED_COMFY_NO_PROGRESS_TIMEOUT_SECONDS = float(
    os.getenv("RENDERFIN_MANAGED_COMFY_NO_PROGRESS_TIMEOUT_SECONDS", "3600")
)

# GPU memory / hang recovery for farm renders (owner, 2026-09-29: "such errors
# must be auto-retried, the box's ComfyUI restarted, and the hang fixed").
# A job that runs out of GPU memory is moved (to a bigger box when it is over
# the failed box's budget) up to OOM_MAX_RETRIES times; a job whose box stops
# answering for UNREACHABLE_REQUEUE_SECONDS, or whose prompt runs HANG_FACTOR x
# its expected time (never less than HANG_MIN_SECONDS), is moved up to
# HANG_MAX_RETRIES times. Budgets live in renderfin.gpu_budget.
OOM_RECOVERY_ENABLED = os.getenv("RENDERFIN_OOM_RECOVERY", "1").strip().lower() not in (
    "0", "false", "no", "off", ""
)
OOM_MAX_RETRIES = int(os.getenv("RENDERFIN_OOM_MAX_RETRIES", "2"))
HANG_MAX_RETRIES = int(os.getenv("RENDERFIN_HANG_MAX_RETRIES", "2"))
UNREACHABLE_REQUEUE_SECONDS = float(os.getenv("RENDERFIN_UNREACHABLE_REQUEUE_SECONDS", "300"))
HANG_FACTOR = float(os.getenv("RENDERFIN_HANG_FACTOR", "3"))
HANG_MIN_SECONDS = float(os.getenv("RENDERFIN_HANG_MIN_SECONDS", "1200"))
# After an out-of-memory, the same family is kept off that box for jobs at
# least that large for this long (other boxes first; the box is used again when
# nothing else can take the job).
OOM_CEILING_TTL_SECONDS = float(os.getenv("RENDERFIN_OOM_CEILING_TTL_SECONDS", str(6 * 3600)))
# One failed poll switches that task to short, spaced polls so a box that
# accepts connections but never answers cannot stall the whole pump (every
# poll used to wait its full 30 s timeout, once per 1.5 s tick).
POLL_FAILURE_BACKOFF_SECONDS = float(os.getenv("RENDERFIN_POLL_FAILURE_BACKOFF_SECONDS", "15"))
POLL_FAILURE_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_POLL_FAILURE_TIMEOUT_SECONDS", "8"))
BOX_VRAM_REFRESH_SECONDS = float(os.getenv("RENDERFIN_BOX_VRAM_REFRESH_SECONDS", "600"))
# Boxes whose ComfyUI renderfin may restart through ComfyUI-Manager
# (/manager/reboot). worker-4090 is the owner's own workstation and is never
# restarted by the farm, whatever this list says.
COMFY_RESTART_BOXES = {
    name.strip().lower()
    for name in os.getenv("RENDERFIN_COMFY_RESTART_BOXES", "f5,f15,Raptor").split(",")
    if name.strip()
}
NEVER_RESTART_BOXES = frozenset({"worker-4090"})
COMFY_RESTART_WAIT_SECONDS = float(os.getenv("RENDERFIN_COMFY_RESTART_WAIT_SECONDS", "240"))
COMFY_RESTART_MIN_INTERVAL_SECONDS = float(
    os.getenv("RENDERFIN_COMFY_RESTART_MIN_INTERVAL_SECONDS", "600")
)
# Idle free VRAM below this share of the card after /free means the process
# leaks or holds a broken context: restart it.
UNHEALTHY_FREE_VRAM_SHARE = float(os.getenv("RENDERFIN_UNHEALTHY_FREE_VRAM_SHARE", "0.6"))
PUMP_TICK_SECONDS = float(os.getenv("RENDERFIN_PUMP_TICK_SECONDS", "1.5"))
DISPATCH_INTERVAL_SECONDS = float(os.getenv("RENDERFIN_DISPATCH_INTERVAL_SECONDS", "5"))
STATUS_REFRESH_TICKS = int(os.getenv("RENDERFIN_STATUS_REFRESH_TICKS", "10"))
SUBMIT_FAILURE_COOLDOWN_SECONDS = float(
    os.getenv("RENDERFIN_SUBMIT_FAILURE_COOLDOWN_SECONDS", "600")
)
# How often the same stalled-dispatch explanation may be repeated. A queue
# that no box will take used to be completely silent; printing it per task per
# pass would be a line every 5s per pending task instead.
STARVATION_LOG_INTERVAL_SECONDS = float(
    os.getenv("RENDERFIN_STARVATION_LOG_INTERVAL_SECONDS", "300")
)

# Hunyuan3D image-to-3D via converter workers (POST /api-converter-glb/generate-3d).
# Each farm box provisions its OWN bearer token, so the authoritative source is a
# JSON file: [{"name": "f7", "url": "https://converter-f7...", "token": "..."}].
# RENDERFIN_HUNYUAN_WORKERS + HUNYUAN_API_TOKEN remain as a single-token fallback.
HUNYUAN_WORKERS_FILE = Path(
    os.getenv("RENDERFIN_HUNYUAN_WORKERS_FILE", "/etc/autorig-renderfin-hunyuan.json")
)
HUNYUAN_WORKERS = [
    u.strip().rstrip("/")
    for u in os.getenv("RENDERFIN_HUNYUAN_WORKERS", "").split(",")
    if u.strip()
]
HUNYUAN_API_TOKEN = os.getenv("HUNYUAN_API_TOKEN", "").strip()
_HUNYUAN_WORKERS_LAST_ERROR = ""
_HUNYUAN_WORKER_NOTICE_STATE: set[str] = set()


def hunyuan_workers_last_error() -> str:
    """Return the current farm-config resolution error, if any.

    An unreadable authoritative file is different from a deliberately absent
    farm.  Character generation must park in the central queue in that case;
    silently falling back to ComfyUI creates one local image_to_3d task per
    waiting character and defeats the Hunyuan admission controls.
    """
    return _HUNYUAN_WORKERS_LAST_ERROR


def _emit_hunyuan_worker_notices(notices: set[str]) -> None:
    """Log farm-config state transitions once, without caching the config.

    ``hunyuan_workers`` is deliberately resolved on every admission pass so a
    worker can be parked or restored without stale routing.  Emitting the same
    parked-worker notice on every one of those reads turns normal queueing into
    hundreds of journal lines.  Remember only the currently announced text;
    when a condition clears it leaves the set and will be announced again if
    it later returns.
    """
    global _HUNYUAN_WORKER_NOTICE_STATE
    for notice in sorted(notices - _HUNYUAN_WORKER_NOTICE_STATE):
        print(notice)
    _HUNYUAN_WORKER_NOTICE_STATE = set(notices)


def hunyuan_workers() -> list[dict]:
    """Resolve and de-duplicate the tiered Hunyuan worker pool.

    ``dedicated`` workers are tried first.  ``shared_converter`` workers are a
    fallback and are protected by the ordinary-conversion admission checks in
    :mod:`renderfin.hunyuan_client`.
    """
    global _HUNYUAN_WORKERS_LAST_ERROR
    _HUNYUAN_WORKERS_LAST_ERROR = ""
    workers: list[dict] = []
    physical_nodes: set[str] = set()
    notices: set[str] = set()
    try:
        if HUNYUAN_WORKERS_FILE.is_file():
            data = json.loads(HUNYUAN_WORKERS_FILE.read_text(encoding="utf-8"))
            entries = data.get("workers") if isinstance(data, dict) else data
            for entry in entries or []:
                url = str(entry.get("url") or "").strip().rstrip("/")
                token = str(entry.get("token") or "").strip() or HUNYUAN_API_TOKEN
                # A box can be parked without deleting how to reach it, so
                # putting it back is one word rather than a reconstruction.
                if entry.get("enabled") is False or entry.get("disabled") is True:
                    notices.add(
                        f"[Renderfin] hunyuan worker {entry.get('name') or url} "
                        f"is disabled in {HUNYUAN_WORKERS_FILE.name}: "
                        f"{entry.get('disabled_reason') or 'no reason given'}"
                    )
                    continue
                canary_approved = entry.get("canary_approved", True) is not False
                if not canary_approved:
                    notices.add(
                        f"[Renderfin] hunyuan worker {entry.get('name') or url} "
                        "is parked until its standard/PBR canary is approved"
                    )
                    continue
                name = str(entry.get("name") or url)
                physical_node = str(
                    entry.get("physical_resource_id_string")
                    or entry.get("physical_node")
                    or name
                ).strip().lower()
                if physical_node in physical_nodes:
                    notices.add(
                        f"[Renderfin] ignoring duplicate Hunyuan physical node "
                        f"{name} ({physical_node})"
                    )
                    continue
                if url and token:
                    physical_nodes.add(physical_node)
                    workers.append(
                        {
                            "name": name,
                            "url": url,
                            "token": token,
                            "pool": (
                                "dedicated"
                                if str(entry.get("pool") or "").strip().lower() == "dedicated"
                                else "shared_converter"
                            ),
                            "priority": int(entry.get("priority") or 100),
                            "canary_approved": canary_approved,
                            "capability_mode": str(
                                entry.get("capability_mode")
                                or entry.get("mode")
                                or "full"
                            ),
                            "physical_node": physical_node,
                            "physical_resource_id_string": str(
                                entry.get("physical_resource_id_string") or ""
                            ).strip().lower(),
                            "workload_role": str(
                                entry.get("workload_role")
                                or entry.get("reserve_role_string")
                                or ""
                            ).strip().lower(),
                        }
                    )
    except Exception as exc:  # a broken file must not take the service down
        _HUNYUAN_WORKERS_LAST_ERROR = str(exc)
        notices.add(f"[Renderfin] hunyuan workers file unreadable: {exc}")
    if workers:
        _emit_hunyuan_worker_notices(notices)
        return workers
    # Single-token fallback only makes sense for one box: farm boxes each
    # provision their own token, so pairing many URLs with one token would
    # authenticate against at most one of them.
    if HUNYUAN_API_TOKEN and len(HUNYUAN_WORKERS) == 1:
        url = HUNYUAN_WORKERS[0]
        _emit_hunyuan_worker_notices(notices)
        return [{
            "name": url,
            "url": url,
            "token": HUNYUAN_API_TOKEN,
            "pool": "shared_converter",
            "priority": 100,
            "canary_approved": True,
            "capability_mode": "full",
            "physical_node": url.lower(),
        }]
    if HUNYUAN_WORKERS and not HUNYUAN_API_TOKEN:
        notices.add("[Renderfin] RENDERFIN_HUNYUAN_WORKERS set but no token configured")
    elif len(HUNYUAN_WORKERS) > 1:
        notices.add(
            "[Renderfin] ignoring RENDERFIN_HUNYUAN_WORKERS: several boxes need "
            f"per-worker tokens in {HUNYUAN_WORKERS_FILE}"
        )
    _emit_hunyuan_worker_notices(notices)
    return []


def converter_control_workers() -> list[dict]:
    """Return authenticated converter control routes, including parked Hunyuan nodes.

    A node such as F11 can be deliberately disabled only for Hunyuan while it
    remains a healthy full converter.  Preemption is a converter concern, so
    resolving its control token through :func:`hunyuan_workers` incorrectly
    makes that full-converter task impossible to recall.  Read the same
    protected registry without applying Hunyuan admission gates.
    """
    workers: list[dict] = []
    try:
        if HUNYUAN_WORKERS_FILE.is_file():
            data = json.loads(HUNYUAN_WORKERS_FILE.read_text(encoding="utf-8"))
            entries = data.get("workers") if isinstance(data, dict) else data
            for entry in entries or []:
                if not isinstance(entry, dict):
                    continue
                url = str(entry.get("url") or "").strip().rstrip("/")
                token = str(entry.get("token") or "").strip() or HUNYUAN_API_TOKEN
                if not url or not token:
                    continue
                workers.append({
                    "name": str(entry.get("name") or url),
                    "url": url,
                    "token": token,
                    "capability_mode": str(
                        entry.get("capability_mode")
                        or entry.get("mode")
                        or "full"
                    ),
                })
    except Exception as exc:
        print(f"[Renderfin] converter control registry unreadable: {exc}")
        return []
    if workers:
        return workers
    return [
        {
            "name": worker.get("name"),
            "url": worker.get("url"),
            "token": worker.get("token"),
            "capability_mode": worker.get("capability_mode", "full"),
        }
        for worker in hunyuan_workers()
    ]
HUNYUAN_QUALITY = os.getenv("RENDERFIN_HUNYUAN_QUALITY", "standard").strip() or "standard"
HUNYUAN_POLL_SECONDS = float(os.getenv("RENDERFIN_HUNYUAN_POLL_SECONDS", "10"))
# A standard-quality generation takes ~65 min on the farm's GTX 1080 Ti boxes
# and queues behind conversion jobs, so the ceiling has to be generous.
HUNYUAN_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_HUNYUAN_TIMEOUT_SECONDS", "14400"))

# A shared converter is only borrowed when the normal AutoRig queue is empty.
# Production overrides this path in storage-host.env.
AUTORIG_QUEUE_DB_PATH = Path(
    os.getenv("RENDERFIN_AUTORIG_QUEUE_DB_PATH", "/var/autorig/autorig.db")
)
AUTORIG_QUEUE_CACHE_SECONDS = float(
    os.getenv("RENDERFIN_AUTORIG_QUEUE_CACHE_SECONDS", "5")
)

# Telegram delivery (renderfin owns delivery so results survive bot restarts)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
DELIVERY_TICK_SECONDS = float(os.getenv("RENDERFIN_DELIVERY_TICK_SECONDS", "5"))

# Turntable rendering (character_gen stage 3)
TURNTABLE_NODE = os.getenv("RENDERFIN_TURNTABLE_NODE", "node")
# Owner rule 2026-09-27: newest submission first. Jobs sent together (one X9
# batch, one list/segment run: gaps under LIFO_GROUP_SECONDS) stay one group in
# their own order; anything waiting longer than STARVE_MIN minutes goes first.
LIFO = os.getenv("RENDERFIN_LIFO", "1").strip() not in ("0", "false", "no", "")
STARVE_MINUTES = float(os.getenv("RENDERFIN_STARVE_MIN", "30"))
LIFO_GROUP_SECONDS = float(os.getenv("RENDERFIN_LIFO_GROUP_SECONDS", "10"))
# Owner rule 2026-10-11 (replaces the 2026-09-27 wipe): «перезапускай сервер не
# дожидаясь завершения графов, они должны подхватываться автоматически при
# рестарте». A restart of renderfin or autorig-storage never loses work: a
# queued job stays queued, a running one is followed on its box or put back in
# the queue under the same task id (same output URL, nothing duplicated), at
# most RESTART_RESUME_MAX times per task. RENDERFIN_RESTART_POLICY=wipe brings
# the old wipe back.
RESTART_POLICY = (os.getenv("RENDERFIN_RESTART_POLICY", "resume").strip().lower() or "resume")
WIPE_QUEUE_ON_START = RESTART_POLICY == "wipe"
RESTART_RESUME_MAX = int(os.getenv("RENDERFIN_RESTART_RESUME_MAX", "3"))
# A job that a restart cancelled anyway (the old policy, an older release) is
# brought back at the next start when it was cancelled less than this ago.
RESTART_REVIVE_WINDOW_SECONDS = float(os.getenv("RENDERFIN_RESTART_REVIVE_WINDOW_SECONDS", "3600"))
RESTART_CANCEL_REASON = "cancelled: server restarted — press Render again"
# The status a resumed job carries (i18n key render_<notice>, e.g.
# render_resumed_after_restart), and the error prefix once the cap is used up.
RESUMED_NOTICE = "resumed_after_restart"
RESTART_RESUME_EXHAUSTED = "restart_resume_exhausted"
TURNTABLE_SCRIPT = os.getenv(
    "RENDERFIN_TURNTABLE_SCRIPT",
    str(PACKAGE_DIR.parent.parent / "tools" / "renderfin" / "glb_turntable.mjs"),
)
TURNTABLE_CHROME = os.getenv("RENDERFIN_TURNTABLE_CHROME", "")
TURNTABLE_FFMPEG = os.getenv("RENDERFIN_TURNTABLE_FFMPEG", "ffmpeg")
TURNTABLE_SECONDS = float(os.getenv("RENDERFIN_TURNTABLE_SECONDS", "6"))
TURNTABLE_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_TURNTABLE_TIMEOUT_SECONDS", "600"))


def ensure_dirs() -> None:
    for p in (DATA_DIR, RENDER_DIR, DB_DIR, TMP_DIR, SERVERS_DIR, RENDER_DIR / "masks"):
        p.mkdir(parents=True, exist_ok=True)
