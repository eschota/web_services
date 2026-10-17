"""The backend's side of the picture normalisation stage (rig contract V3.2 §3, owner 2026-10-17: «все эти задачи
должны быть реализованы автоматически пайплайном, без ручных правок, в том числе дорисованы картинки спереди для
персонажа, если они вдруг нужны»).

The stage itself is ``mt/picture_canon.py`` in the MT tree (/srv/autorig/data/motion_transfer). It runs as its own
process, started here right after the cutout and beside the first 3D job, so the generation never waits for it:

    python -m mt.picture_canon run --token <upload token> --task <task id>

It writes into the picture's upload folder (``/srv/autorig/data/var/uploads/<token>/picture/``, nginx serves it as
``/u/<token>/picture/...``): ``view.json`` (the view, the pose, the characters, the held objects, the decisions and the
seconds), ``front.png`` / ``front_cutout.png`` (a front view redrawn from a back / side picture), ``clean.png`` (held
objects erased, the hands completed), ``props/<i>.png`` (each held object as its own picture, queued as its own prop
generation) and ``characters.json``. When the task's MT run exists it mirrors them into ``<run>/picture/`` and replays
its live events there (live_picture_view / live_picture_front / live_picture_props).

Here:
* ``kick(token, task_id)`` starts the stage once per picture (an O_EXCL marker survives backend restarts);
* ``summary(token)`` is the compact record the task page's picture viewer gets (the URLs of the redrawn front, the
  back reference, the props, the cleaned picture);
* ``generation_input(token)`` is the picture the full 3D generation should use once the stage has decided (the
  redrawn front, or the picture with the held objects removed); None while it works or when the cutout is right.

AUTORIG_PICTURE_CANON=off switches the stage off (the intake is then exactly as before).
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

UPLOAD_ROOT = Path(os.getenv("AUTORIG_PICTURE_UPLOAD_ROOT", os.getenv("UPLOAD_DIR", "/var/autorig/uploads")))
MT_ROOT = Path(os.getenv("AUTORIG_MT_ROOT", "/srv/autorig/data/motion_transfer"))
MT_PYTHON = os.getenv("AUTORIG_MT_PYTHON", "/srv/autorig/venv/bin/python3")
STALE_SECONDS = float(os.getenv("AUTORIG_PICTURE_CANON_STALE", "1800"))   # a start marker with no view.json after this
_STARTED: Dict[str, float] = {}


def enabled() -> bool:
    return os.getenv("AUTORIG_PICTURE_CANON", "on").strip().lower() not in ("off", "0", "false", "no")


def redispatch_enabled() -> bool:
    """Swap the queued full generation onto the normalised picture (AUTORIG_PICTURE_CANON_REDISPATCH=off: never)."""
    return os.getenv("AUTORIG_PICTURE_CANON_REDISPATCH", "on").strip().lower() not in ("off", "0", "false", "no")


def folder(token: str) -> Path:
    return UPLOAD_ROOT / token / "picture"


def view(token: str) -> Optional[Dict[str, Any]]:
    try:
        doc = json.loads((folder(token) / "view.json").read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError):
        return None


def kick(token: Optional[str], task_id: str) -> bool:
    """Start the stage for this picture once; True when it was started now. Never raises."""
    if not token or not enabled():
        return False
    try:
        if token in _STARTED:
            return False
        if not (UPLOAD_ROOT / token).is_dir() or not (MT_ROOT / "mt" / "picture_canon.py").is_file():
            return False
        if view(token) is not None:                            # this picture was read already (a retry, a rerun)
            _STARTED[token] = time.time()
            return False
        d = folder(token)
        d.mkdir(parents=True, exist_ok=True)
        marker = d / ".started"
        try:
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            os.write(fd, f"{time.time():.0f} {task_id}\n".encode())
            os.close(fd)
        except FileExistsError:
            # started before (this process or before a restart): only a stale start with no record is started again
            age = time.time() - marker.stat().st_mtime
            if view(token) is not None or age < STALE_SECONDS:
                _STARTED[token] = time.time()
                return False
            marker.touch()
        log = open(d / "stage.log", "ab")
        try:
            subprocess.Popen(
                ["nice", "-n", "5", MT_PYTHON, "-m", "mt.picture_canon", "run", "--token", token, "--task", str(task_id)],
                cwd=str(MT_ROOT), stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                start_new_session=True, close_fds=True)
        finally:
            log.close()
        _STARTED[token] = time.time()
        print(f"[PictureCanon] stage started for task {task_id} (picture {token})")
        return True
    except Exception as exc:                                   # noqa: BLE001 - the intake goes on without it
        print(f"[PictureCanon] could not start for {task_id}: {type(exc).__name__}: {exc}")
        return False


def _url(app_url: str, token: str, rel: Optional[str]) -> Optional[str]:
    if not rel:
        return None
    p = folder(token) / rel
    return f"{app_url}/u/{token}/picture/{rel}?v={int(p.stat().st_mtime)}" if p.is_file() else None


def generation_input(token: Optional[str], app_url: str) -> Optional[Dict[str, Any]]:
    """{kind, url} of the normalised picture for the full generation, once the stage decided on one."""
    if not token:
        return None
    v = view(token) or {}
    if v.get("status") not in ("ok", "review"):
        return None
    gi = v.get("generation_input") or {}
    kind = str(gi.get("kind") or "")
    if kind not in ("front", "clean"):
        return None
    url = _url(app_url, token, gi.get("cutout")) or _url(app_url, token, gi.get("picture"))
    return {"kind": kind, "url": url} if url else None


def summary(token: Optional[str], app_url: str) -> Optional[Dict[str, Any]]:
    """The stage as the task page's picture viewer shows it (None before it starts)."""
    if not token:
        return None
    v = view(token)
    if not v:
        return None
    front = v.get("front") or {}
    pr = v.get("props_result") or {}
    props = []
    for p in pr.get("props") or []:
        props.append({"i": p.get("i"), "name": p.get("name"), "hand": p.get("hand"), "status": p.get("status"),
                      "url": _url(app_url, token, p.get("picture")),
                      "generation": (p.get("generation") or {}).get("job")})
    return {
        "status": v.get("status"), "view": v.get("view"), "view_conf": v.get("view_conf"), "pose": v.get("pose"),
        "needs_front": v.get("needs_front"), "characters": (v.get("characters") or {}).get("detected"),
        "front": {"status": front.get("status"), "url": _url(app_url, token, front.get("picture")),
                  "back_url": _url(app_url, token, "back.png") if front else None} if front else None,
        "clean": {"status": (pr.get("clean") or {}).get("status"), "url": _url(app_url, token, "clean.png")}
        if pr.get("clean") else None,
        "props": props or None,
        "generation_input": (v.get("generation_input") or {}).get("kind"),
        "seconds": v.get("seconds"),
        "view_url": _url(app_url, token, "view.json"),
    }
