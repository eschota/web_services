"""Binary FBX -> GLB on a farm box's Blender (V3 intake, 2026-10-15).

The converters no longer serve /api-converter-glb-to-fbx and assimp 5.2 on the VPS cannot read every binary FBX, so the
intake pump hands the file to the farm queue (farmjobs bundle "mt", mt/fbx_intake_farm.py).  Non-blocking and restart-safe:
start() returns a job id the pump stores on the task; poll() is called on later passes.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

FARMJOBS = os.getenv("FARMJOBS_DIR", "/srv/autorig/farmjobs")
STAGE = Path(os.getenv("AUTORIG_V3_FBX_STAGE", "/srv/autorig/data/v3-intake-farm"))
ENABLED = os.getenv("AUTORIG_V3_FBX_FARM", "1") != "0"
BOXES = [b for b in os.getenv("AUTORIG_V3_FBX_BOXES", "f1,f2,f7,f13,f5").split(",") if b]   # never worker-4090
PICKUP_S = float(os.getenv("AUTORIG_V3_FBX_PICKUP", "240"))
JOB_TIMEOUT_S = int(os.getenv("AUTORIG_V3_FBX_TIMEOUT", "600"))
PACK_MAX = 400 * 1024 * 1024
_TEXTURE_EXT = {".png", ".jpg", ".jpeg", ".tga", ".bmp", ".tif", ".tiff", ".webp", ".dds", ".exr", ".hdr"}


def available() -> bool:
    return ENABLED and Path(FARMJOBS, "farmjobs.py").is_file()


def _queue():
    if FARMJOBS not in sys.path:
        sys.path.insert(0, FARMJOBS)
    import farmjobs
    return farmjobs.Queue()


def is_binary_fbx(path: Path) -> bool:
    with Path(path).open("rb") as f:
        return f.read(18) == b"Kaydara FBX Binary"


def start(task_id: str, fbx: Path) -> str:
    """Pack the FBX with the textures beside it, enqueue the job -> job id."""
    fbx = Path(fbx)
    work = STAGE / str(task_id)
    work.mkdir(parents=True, exist_ok=True)
    pack = work / "pack.zip"
    total = 0
    with zipfile.ZipFile(pack, "w", zipfile.ZIP_STORED) as z:
        z.write(fbx, fbx.name)
        total += fbx.stat().st_size
        for p in sorted(fbx.parent.rglob("*")):
            if p.is_file() and p != fbx and p.suffix.lower() in _TEXTURE_EXT and total + p.stat().st_size <= PACK_MAX:
                total += p.stat().st_size
                z.write(p, str(p.relative_to(fbx.parent)))
    spec = {"kind": "fbx_intake", "bundle": "mt", "cmd": ["{python}", "-m", "mt.fbx_intake_farm", "{in}", "{out}"],
            "inputs": {"pack.zip": str(pack)}, "timeout": JOB_TIMEOUT_S, "priority": 20, "max_attempts": 2,
            "boxes": list(BOXES), "v3": True}
    return _queue().enqueue(spec)


def cleanup(task_id: str) -> None:
    shutil.rmtree(STAGE / str(task_id), ignore_errors=True)


def poll(job_id: str, started_at: float, now: float) -> Tuple[str, Dict[str, Any], Optional[bytes]]:
    """-> (state, info, glb): state pending | done | failed | no_mesh.  A job nobody takes in PICKUP_S is cancelled (failed)."""
    q = _queue()
    row = q.get(job_id)
    st = row["state"] if row else "gone"
    if st in ("queued", "leased"):
        if st == "queued" and now - started_at > PICKUP_S:
            q.cancel(job_id)
            return "failed", {"error": f"no farm box took the job within {int(PICKUP_S)} s"}, None
        if now - started_at > JOB_TIMEOUT_S + PICKUP_S:
            q.cancel(job_id)
            return "failed", {"error": "farm Blender timed out"}, None
        return "pending", {}, None
    if st != "done":
        return "failed", {"error": f"farm job {st}: {(row or {}).get('error') or ''}"[:240]}, None
    zp = q.result_path(job_id)
    try:
        with zipfile.ZipFile(zp) as z:
            report = json.loads(z.read("report.json").decode("utf-8"))
            info = {**report, "box": row.get("box"), "job": job_id, "job_seconds": row.get("seconds")}
            if report.get("code") == "no_mesh":
                return "no_mesh", info, None
            if not report.get("ok"):
                return "failed", info, None
            return "done", info, z.read("model.glb")
    except Exception as exc:                                    # noqa: BLE001
        return "failed", {"error": f"farm result unreadable: {exc}"[:240]}, None
