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
    for h in os.getenv("RENDERFIN_ALLOWED_HTTP_HOSTS", "5.129.157.224,127.0.0.1,localhost").split(",")
    if h.strip()
}

# "user:password" for workers behind the basic-auth nginx edge (worker-4090)
WORKER_BASIC_AUTH = os.getenv("RENDERFIN_WORKER_BASIC_AUTH", "")

TASK_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_TASK_TIMEOUT_SECONDS", "1800"))
PUMP_TICK_SECONDS = float(os.getenv("RENDERFIN_PUMP_TICK_SECONDS", "1.5"))
DISPATCH_INTERVAL_SECONDS = float(os.getenv("RENDERFIN_DISPATCH_INTERVAL_SECONDS", "5"))
STATUS_REFRESH_TICKS = int(os.getenv("RENDERFIN_STATUS_REFRESH_TICKS", "10"))

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


def hunyuan_workers() -> list[dict]:
    """Resolve the Hunyuan worker pool as [{name, url, token}]."""
    workers: list[dict] = []
    try:
        if HUNYUAN_WORKERS_FILE.is_file():
            data = json.loads(HUNYUAN_WORKERS_FILE.read_text(encoding="utf-8"))
            entries = data.get("workers") if isinstance(data, dict) else data
            for entry in entries or []:
                url = str(entry.get("url") or "").strip().rstrip("/")
                token = str(entry.get("token") or "").strip() or HUNYUAN_API_TOKEN
                if url and token:
                    workers.append(
                        {
                            "name": str(entry.get("name") or url),
                            "url": url,
                            "token": token,
                        }
                    )
    except Exception as exc:  # a broken file must not take the service down
        print(f"[Renderfin] hunyuan workers file unreadable: {exc}")
    if workers:
        return workers
    return [
        {"name": url, "url": url, "token": HUNYUAN_API_TOKEN}
        for url in HUNYUAN_WORKERS
        if HUNYUAN_API_TOKEN
    ]
HUNYUAN_QUALITY = os.getenv("RENDERFIN_HUNYUAN_QUALITY", "standard").strip() or "standard"
HUNYUAN_POLL_SECONDS = float(os.getenv("RENDERFIN_HUNYUAN_POLL_SECONDS", "10"))
HUNYUAN_TIMEOUT_SECONDS = float(os.getenv("RENDERFIN_HUNYUAN_TIMEOUT_SECONDS", "3600"))

# Turntable rendering (character_gen stage 3)
TURNTABLE_NODE = os.getenv("RENDERFIN_TURNTABLE_NODE", "node")
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
