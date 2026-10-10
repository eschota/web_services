"""Downloads · V3: the task page's download mode for the V3 conveyor (owner, 2026-10-10).

«скачивание делай только новых файлов, если их нет то нужно их по запросу экспортировать, пользователю
показывать прогрессбар … все скачивания только с безлимитной подпиской»

* Files are the V3 outputs of the task's Motion Transfer run (``runs/<run>/rig/rigged.glb``, the rig the viewer
  shows, whatever rig version Rig tools made current), never the classic converter's.
* Formats (``FORMATS``): the rigged GLB with every clip, an FBX with every clip as a take, one GLB and one FBX per
  clip, and a ZIP bundle. GLB files are cut on this host in a second; FBX is exported on request by a Blender export
  worker (``deploy/v3-export-worker``, ``v3_export_blender.py``); the ZIP is packed here once its FBX exists.
* Exports are queued, deduplicated and cached per (task, format, rig content): ``<glb_cache>/<task>_v3exports/
  <sha16>/``. A new rig version is a new sha16, so an old export is never served for a new rig.
* Every download and export endpoint asks ``download_access`` first and fails closed: an export is never started
  for a caller who could not download its file. Allowed: an administrator, or the task owner with an active
  unlimited subscription (``subscription_access.user_has_active_subscription``). The cookie ``ar_view_as=free`` or
  the query ``?as=free`` (Astra's ``site_as_owner`` sends no cookies) makes an administrator a signed-in owner
  without a subscription, for UX tests; both are honoured for admin sessions only.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import struct
import time
import zipfile
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response
from sqlalchemy import select

from subscription_access import user_has_active_subscription

MT_ROOT = Path(os.getenv("AUTORIG_MT_ROOT", "/srv/autorig/data/motion_transfer"))
QUEUE_DIR = Path(os.getenv("AUTORIG_V3_EXPORT_QUEUE", "/srv/autorig/data/var/v3-exports/queue"))
WORKER_KEYS = Path(os.getenv("AUTORIG_V3_EXPORT_WORKER_KEYS", "/srv/autorig/secrets/v3-export-workers.json"))
BLENDER_SCRIPT = Path(__file__).resolve().parent / "v3_export_blender.py"
VIEW_AS_COOKIE = "ar_view_as"
INTERNAL_CLIPS = {"rig_check"}
LEASE_SECONDS = 120.0            # a taken job with no progress for this long goes back to the queue
NO_WORKER_FAIL_SECONDS = 600.0   # a queued job nobody took for this long fails (the page offers a retry)
MAX_ATTEMPTS = 3
MAX_RESULT_BYTES = 120 * 1024 * 1024
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_MT_RUN = re.compile(r"^[0-9a-f]{20}$")
_SHA16 = re.compile(r"^[0-9a-f]{16}$")
_JOB_ID = re.compile(r"^[0-9a-f]{20}$")
_FMT = re.compile(r"^(glb|fbx|zip|clip-(\d{1,2})\.(glb|fbx))$")
_NO_STORE = {"Cache-Control": "private, no-store, max-age=0", "X-Robots-Tag": "noindex"}

# id -> (kind shown in the menu, file extension, made by)
FORMATS = {
    "glb": ("glb", "glb", "here"),
    "fbx": ("fbx", "fbx", "worker"),
    "zip": ("zip", "zip", "here"),
}
WORKERS_SEEN: dict[str, float] = {}
_LOCK = asyncio.Lock()
_ZIPS: dict[str, asyncio.Task] = {}
_SHA_CACHE: dict[str, tuple[tuple[int, int, int], str]] = {}


# --------------------------------------------------------------------------- access

def plan_info(task_id: str) -> dict[str, Any]:
    from config import AUTORIG_SUBSCRIPTION_PRICE_USD, AUTORIG_SUBSCRIPTION_PRODUCT_KEY

    back = f"/task?id={task_id}"
    return {
        "product": AUTORIG_SUBSCRIPTION_PRODUCT_KEY,
        "price_usd": float(AUTORIG_SUBSCRIPTION_PRICE_USD),
        "period": "month",
        "checkout_url": (f"/buy-credits/checkout/{quote(AUTORIG_SUBSCRIPTION_PRODUCT_KEY)}"
                         f"?source=task_download&task_id={quote(task_id)}&page_url={quote(back, safe='')}"),
        "login_url": f"/auth/login?next={quote(back + '&dl=1', safe='')}",
    }


def download_access(task: Any, user: Any, request: Request, *, is_admin_email: Callable[[Optional[str]], bool],
                    anon_id: Optional[str]) -> dict[str, Any]:
    """{allowed, status, reason, ...}: the one decision every download and export endpoint takes first."""
    email = getattr(user, "email", None) if user is not None else None
    admin = bool(email and is_admin_email(email))
    query = getattr(request, "query_params", None) or {}
    view_free = bool(admin and (request.cookies.get(VIEW_AS_COOKIE) == "free" or query.get("as") == "free"))
    owner = bool((user is not None and task.owner_type == "user" and task.owner_id == email)
                 or (task.owner_type == "anon" and anon_id and task.owner_id == anon_id))
    subscribed = bool(user is not None and not view_free and user_has_active_subscription(user))
    doc = {"signed_in": user is not None, "admin": admin and not view_free, "view_as_free": view_free,
           "can_view_as": admin, "owner": owner or view_free, "subscription_active": subscribed}
    if admin and not view_free:
        return {**doc, "allowed": True, "status": 200, "reason": "admin"}
    if user is None:
        return {**doc, "allowed": False, "status": 401, "reason": "signin_required"}
    if not (owner or view_free):
        return {**doc, "allowed": False, "status": 403, "reason": "not_owner"}
    if not subscribed:
        return {**doc, "allowed": False, "status": 402, "reason": "subscription_required"}
    return {**doc, "allowed": True, "status": 200, "reason": "subscription"}


def _deny(access: dict[str, Any], task_id: str) -> None:
    from user_language import user_error_detail

    code = {"signin_required": "download_signin", "not_owner": "download_not_owner",
            "subscription_required": "download_subscription"}.get(access["reason"], "download_subscription")
    detail = user_error_detail(code)
    detail["access"] = {k: access[k] for k in ("reason", "signed_in", "owner", "subscription_active")}
    detail["plan"] = plan_info(task_id)
    raise HTTPException(status_code=int(access["status"]), detail=detail)


def _error(status: int, code: str) -> HTTPException:
    from user_language import user_error_detail

    return HTTPException(status_code=status, detail=user_error_detail(code))


# --------------------------------------------------------------------------- the V3 source

def _glb_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with path.open("rb") as fh:
            head = fh.read(20)
            if len(head) < 20 or head[:4] != b"glTF" or struct.unpack_from("<I", head, 4)[0] != 2:
                return None
            if struct.unpack_from("<I", head, 8)[0] != path.stat().st_size:
                return None
            length, kind = struct.unpack_from("<II", head, 12)
            if kind != 0x4E4F534A or length > 64 * 1024 * 1024:
                return None
            return json.loads(fh.read(length).decode("utf-8"))
    except (OSError, ValueError):
        return None


def _sha(path: Path) -> str:
    st = path.stat()
    key = (st.st_ino, st.st_size, st.st_mtime_ns)
    hit = _SHA_CACHE.get(str(path))
    if hit and hit[0] == key:
        return hit[1]
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    value = digest.hexdigest()
    _SHA_CACHE[str(path)] = (key, value)
    return value


def v3_run_of(task: Any) -> Optional[str]:
    if str(getattr(task, "pipeline_kind", "") or "").strip().lower() != "v3":
        return None
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except (TypeError, ValueError):
        return None
    session = ((settings.get("v3") or {}).get("session") or {}) if isinstance(settings, dict) else {}
    run = str(session.get("mt_run_id") or "") if isinstance(session, dict) else ""
    return run if _MT_RUN.fullmatch(run) else None


def rig_version(run_dir: Path, sha256: str) -> Optional[str]:
    """Rig tools' version id (v0, v1, …) of this exact rig file, when it made one."""
    try:
        doc = json.loads((run_dir / "rig" / "skin" / "index.json").read_text(encoding="utf-8"))
        for ver in doc.get("versions") or []:
            if isinstance(ver, dict) and ver.get("sha256") == sha256:
                return str(ver.get("id") or "")[:12] or None
    except (OSError, ValueError, AttributeError):
        pass
    return None


def source_of(task: Any) -> Optional[dict[str, Any]]:
    """The rigged GLB of the task's V3 run, its content hash, rig version and user clips (None: not ready)."""
    run = v3_run_of(task)
    if not run:
        return None
    run_dir = MT_ROOT / "runs" / run
    path = run_dir / "rig" / "rigged.glb"
    doc = _glb_json(path)
    if not doc or not doc.get("skins"):
        return None
    sha256 = _sha(path)
    clips = [str(a.get("name") or f"clip {i}")[:80] for i, a in enumerate(doc.get("animations") or [])
             if str(a.get("name") or "") not in INTERNAL_CLIPS]
    return {"run": run, "run_dir": run_dir, "path": path, "sha256": sha256, "sha16": sha256[:16],
            "version": rig_version(run_dir, sha256), "clips": clips[:50]}


# --------------------------------------------------------------------------- cache layout

def export_dir(cache_root: Path, task_id: str, sha16: str) -> Path:
    return cache_root / f"{task_id}_v3exports" / sha16


def out_name(fmt: str) -> str:
    return "bundle.zip" if fmt == "zip" else ("rigged." + fmt if fmt in ("glb", "fbx") else fmt)


def parse_fmt(fmt: str, clips: list[str]) -> Optional[dict[str, Any]]:
    m = _FMT.fullmatch(str(fmt or ""))
    if not m:
        return None
    if m.group(2) is not None:
        index = int(m.group(2))
        if index >= len(clips):
            return None
        return {"id": fmt, "ext": m.group(3), "clip": clips[index], "clip_index": index,
                "by": "worker" if m.group(3) == "fbx" else "here"}
    kind, ext, by = FORMATS[fmt]
    return {"id": fmt, "ext": ext, "clip": None, "clip_index": None, "by": by}


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _write_json(path: Path, doc: dict[str, Any]) -> None:
    _atomic_write(path, json.dumps(doc, ensure_ascii=False).encode("utf-8"))


def _read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError):
        return None


def snapshot(src: dict[str, Any], folder: Path) -> Path:
    """rigged.glb copied once into the export folder: every export of this version reads the same bytes."""
    folder.mkdir(parents=True, exist_ok=True)
    dst = folder / "rigged.glb"
    if dst.is_file() and dst.stat().st_size == src["path"].stat().st_size:
        return dst
    data = src["path"].read_bytes()
    if hashlib.sha256(data).hexdigest() != src["sha256"]:
        raise _error(409, "download_rig_changed")
    _atomic_write(dst, data)
    return dst


def split_clip(glb: Path, clip: str, dst: Path) -> None:
    """One clip of a GLB: the same container with only that animation (buffers untouched, still valid glTF)."""
    raw = glb.read_bytes()
    length = struct.unpack_from("<I", raw, 12)[0]
    doc = json.loads(raw[20:20 + length].decode("utf-8"))
    anims = [a for a in doc.get("animations") or [] if str(a.get("name") or "") == clip]
    if not anims:
        raise _error(404, "download_unknown_format")
    doc["animations"] = anims[:1]
    body = json.dumps(doc, separators=(",", ":")).encode("utf-8")
    body += b" " * ((4 - len(body) % 4) % 4)
    rest = raw[20 + length:]
    out = b"glTF" + struct.pack("<II", 2, 12 + 8 + len(body) + len(rest)) + \
        struct.pack("<II", len(body), 0x4E4F534A) + body + rest
    _atomic_write(dst, out)


# --------------------------------------------------------------------------- jobs (worker exports and zips)

def job_id_of(task_id: str, sha16: str, fmt: str) -> str:
    return hashlib.sha1(f"{task_id}|{sha16}|{fmt}".encode()).hexdigest()[:20]


def _job_path(folder: Path, fmt: str) -> Path:
    return folder / f"{fmt}.job.json"


def worker_online() -> bool:
    now = time.time()
    if any(now - t < 60 for t in WORKERS_SEEN.values()):
        return True
    try:
        st = (QUEUE_DIR.parent / "workers.json").stat()
        doc = json.loads((QUEUE_DIR.parent / "workers.json").read_text())
        return any(now - float(v) < 60 for v in doc.values()) and now - st.st_mtime < 120
    except (OSError, ValueError, AttributeError, TypeError):
        return False


def job_view(job: Optional[dict[str, Any]], folder: Path, fmt: str) -> dict[str, Any]:
    """The state the page shows: ready | missing | queued | running | failed, a real percent and a stage."""
    if (folder / out_name(fmt)).is_file():
        return {"state": "ready", "progress": 1.0, "stage": "ready",
                "bytes": (folder / out_name(fmt)).stat().st_size}
    if not job:
        return {"state": "missing", "progress": 0.0, "stage": None}
    state = job.get("state")
    now = time.time()
    if state == "queued" and now - float(job.get("queued_at") or now) > NO_WORKER_FAIL_SECONDS:
        state, job["stage"], job["error"] = "failed", "no_worker", "no export worker took the job"
    view = {"state": state if state in ("queued", "running", "failed") else "missing",
            "progress": round(float(job.get("progress") or 0), 3), "stage": job.get("stage")}
    if view["state"] == "queued" and not worker_online():
        view["stage"] = "waiting_worker"
    if view["state"] == "failed":
        view["error"] = str(job.get("error") or "")[:200]
    return view


def enqueue(folder: Path, task_id: str, src: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """A worker job for this (task, rig, format), deduplicated: an existing live job is returned as it is."""
    fmt = spec["id"]
    path = _job_path(folder, fmt)
    job = _read_json(path)
    now = time.time()
    if job and job.get("state") in ("queued", "running"):
        if job.get("state") == "queued" and now - float(job.get("queued_at") or now) > NO_WORKER_FAIL_SECONDS:
            job = None
        else:
            return job
    if job and job.get("state") == "failed" and int(job.get("attempts") or 0) >= MAX_ATTEMPTS and \
            now - float(job.get("updated_at") or 0) < 600:
        return job
    jid = job_id_of(task_id, src["sha16"], fmt)
    job = {"schema": "autorig.v3-export-job/1", "id": jid, "task_id": task_id, "sha16": src["sha16"],
           "sha256": src["sha256"], "format": fmt, "clip": spec["clip"], "state": "queued", "stage": "queued",
           "progress": 0.02, "queued_at": now, "updated_at": now,
           "attempts": int((job or {}).get("attempts") or 0) if (job or {}).get("state") == "failed" else 0,
           "folder": str(folder)}
    _write_json(path, job)
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write(QUEUE_DIR / jid, str(path).encode("utf-8"))
    return job


def _queued_jobs() -> list[tuple[Path, Path, dict[str, Any]]]:
    if not QUEUE_DIR.is_dir():
        return []
    rows = []
    for entry in sorted(QUEUE_DIR.iterdir(), key=lambda p: p.stat().st_mtime):
        if not _JOB_ID.fullmatch(entry.name):
            continue
        try:
            path = Path(entry.read_text().strip())
        except OSError:
            continue
        job = _read_json(path)
        if not job or job.get("id") != entry.name or job.get("state") not in ("queued", "running"):
            try:
                entry.unlink()
            except OSError:
                pass
            continue
        rows.append((entry, path, job))
    return rows


def take_job(worker: str) -> Optional[dict[str, Any]]:
    now = time.time()
    WORKERS_SEEN[worker] = now
    try:
        QUEUE_DIR.mkdir(parents=True, exist_ok=True)
        _write_json(QUEUE_DIR.parent / "workers.json", {k: v for k, v in WORKERS_SEEN.items()})
    except OSError:
        pass
    for entry, path, job in _queued_jobs():
        stale = job["state"] == "running" and now - float(job.get("heartbeat_at") or 0) > LEASE_SECONDS
        if job["state"] == "queued" or stale:
            if int(job.get("attempts") or 0) >= MAX_ATTEMPTS:
                job.update(state="failed", stage="failed", error="too many attempts", updated_at=now)
                _write_json(path, job)
                continue
            job.update(state="running", stage="taken", worker=worker, taken_at=now, heartbeat_at=now,
                       attempts=int(job.get("attempts") or 0) + 1, progress=max(0.05, float(job["progress"])),
                       updated_at=now, lease=secrets.token_hex(16))
            _write_json(path, job)
            return job
    return None


def find_job(jid: str) -> tuple[Optional[Path], Optional[dict[str, Any]]]:
    if not _JOB_ID.fullmatch(str(jid or "")):
        return None, None
    try:
        path = Path((QUEUE_DIR / jid).read_text().strip())
    except OSError:
        return None, None
    job = _read_json(path)
    return (path, job) if job and job.get("id") == jid else (None, None)


async def build_zip(folder: Path, task_id: str, src: dict[str, Any], slug: str) -> None:
    """GLB + FBX + one GLB per clip + a short README, packed here once the FBX exists."""
    path = _job_path(folder, "zip")
    try:
        while True:
            job = _read_json(path) or {}
            if (folder / "rigged.fbx").is_file():
                break
            fbx_job = _read_json(_job_path(folder, "fbx"))
            view = job_view(fbx_job, folder, "fbx")
            if view["state"] == "failed":
                raise RuntimeError("fbx: " + str(view.get("error") or "failed"))
            job.update(state="running", stage=view.get("stage") or "fbx",
                       progress=round(0.05 + 0.75 * float(view.get("progress") or 0), 3), updated_at=time.time())
            _write_json(path, job)
            await asyncio.sleep(2)
        job.update(stage="clips", progress=0.82, updated_at=time.time())
        _write_json(path, job)
        names = []
        for i, clip in enumerate(src["clips"]):
            dst = folder / f"clip-{i}.glb"
            if not dst.is_file():
                await asyncio.to_thread(split_clip, folder / "rigged.glb", clip, dst)
            names.append((dst, f"clips/{_slug(clip) or f'clip-{i}'}.glb"))
        job.update(stage="zip", progress=0.86, updated_at=time.time())
        _write_json(path, job)
        readme = (f"AutoRig.online · task {task_id}\n"
                  f"{slug}.glb  rigged model, every animation (glTF 2.0: Blender, three.js, Godot, Unity glTFast)\n"
                  f"{slug}.fbx  rigged model, every animation as a take (Unity, Unreal Engine, Blender, Maya)\n"
                  f"clips/      one GLB per animation\n"
                  f"https://autorig.online/task?id={task_id}\n")

        def pack() -> None:
            tmp = folder / f".bundle.{secrets.token_hex(4)}.tmp"
            members = [(folder / "rigged.glb", f"{slug}.glb"), (folder / "rigged.fbx", f"{slug}.fbx"), *names]
            total = sum(p.stat().st_size for p, _ in members) or 1
            done = 0
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                z.writestr("README.txt", readme)
                for member, arc in members:
                    with member.open("rb") as fh, z.open(arc, "w", force_zip64=True) as out:
                        for block in iter(lambda: fh.read(1 << 20), b""):
                            out.write(block)
                            done += len(block)
                            if done % (8 << 20) < (1 << 20):
                                job.update(progress=round(0.86 + 0.13 * done / total, 3), updated_at=time.time())
                                _write_json(path, job)
            os.replace(tmp, folder / "bundle.zip")

        await asyncio.to_thread(pack)
        job.update(state="done", stage="ready", progress=1.0, updated_at=time.time())
        _write_json(path, job)
    except Exception as exc:  # the page shows the failure and offers a retry
        job = _read_json(path) or {}
        job.update(state="failed", stage="failed", error=str(exc)[:300], updated_at=time.time(),
                   attempts=int(job.get("attempts") or 0) + 1)
        _write_json(path, job)
    finally:
        _ZIPS.pop(str(folder), None)


def _slug(text: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "-", str(text or "")).strip("-").lower()
    return value[:48]


def file_slug(task: Any, src: dict[str, Any]) -> str:
    title = str(getattr(task, "poster_llm_title", "") or getattr(task, "collection_member_title", "") or "")
    base = _slug(title) or f"autorig-{str(task.id)[:8]}"
    return base + (f"-{src['version']}" if src.get("version") else "")


# --------------------------------------------------------------------------- router

def build_task_downloads_v3_router(*, get_db: Callable[..., Any], get_current_user: Callable[..., Any],
                                   task_model: Any, is_admin_email: Callable[[Optional[str]], bool],
                                   glb_cache_dir: Path, effective_anon_id: Callable[[Request], Optional[str]]
                                   ) -> APIRouter:
    router = APIRouter()
    cache_root = Path(glb_cache_dir)

    async def load(task_id: str, request: Request, user: Any, db: Any) -> Any:
        from v3_viewer_routes import _can_access_task

        if not _UUID.fullmatch(str(task_id or "")):
            raise HTTPException(status_code=404, detail="Task not found")
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if task is None or not _can_access_task(task, is_public=bool(getattr(task, "is_public", False)),
                                                user=user, request=request, is_admin_email=is_admin_email):
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    def access_of(task: Any, user: Any, request: Request) -> dict[str, Any]:
        return download_access(task, user, request, is_admin_email=is_admin_email,
                               anon_id=effective_anon_id(request))

    def file_url(task_id: str, fmt: str, sha16: str) -> str:
        return f"/api/task/{task_id}/downloads-v3/{fmt}/file?v={sha16}"

    def describe(task: Any, src: Optional[dict[str, Any]], access: dict[str, Any]) -> dict[str, Any]:
        task_id = str(task.id)
        is_v3 = str(getattr(task, "pipeline_kind", "") or "").strip().lower() == "v3"
        formats = []
        if src is not None:
            folder = export_dir(cache_root, task_id, src["sha16"])
            ids = ["glb", "fbx", "zip"] + [f"clip-{i}.{ext}" for i in range(len(src["clips"]))
                                           for ext in ("glb", "fbx")]
            for fmt in ids:
                spec = parse_fmt(fmt, src["clips"])
                view = job_view(_read_json(_job_path(folder, fmt)), folder, fmt)
                if fmt in ("glb",) or (spec["clip"] is not None and spec["ext"] == "glb"):
                    view = view if view["state"] == "ready" else {"state": "instant", "progress": 0.0, "stage": None}
                row = {"id": fmt, "ext": spec["ext"], "clip": spec["clip"], **view}
                if access["allowed"] and view["state"] == "ready":
                    row["url"] = file_url(task_id, fmt, src["sha16"])
                formats.append(row)
        return {
            "schema": "autorig.task-downloads-v3/1",
            "task_id": task_id,
            "pipeline": "v3" if is_v3 else "classic",
            "available": src is not None,
            "reason": None if src is not None else ("not_ready" if is_v3 else "legacy"),
            "legacy_url": None if is_v3 else f"/task?id={task_id}&classic=1",
            "access": {k: access[k] for k in ("allowed", "status", "reason", "signed_in", "owner", "admin",
                                              "subscription_active", "view_as_free", "can_view_as")},
            "plan": plan_info(task_id),
            "rig": None if src is None else {"sha": src["sha16"], "version": src["version"], "clips": src["clips"]},
            "formats": formats,
            "exporter": {"online": worker_online()},
        }

    @router.get("/api/task/{task_id}/downloads-v3")
    async def downloads_manifest(task_id: str, request: Request, user: Any = Depends(get_current_user),
                                 db: Any = Depends(get_db)):
        task = await load(task_id, request, user, db)
        src = await asyncio.to_thread(source_of, task)
        return JSONResponse(describe(task, src, access_of(task, user, request)), headers=_NO_STORE)

    async def ensure(task: Any, src: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        task_id = str(task.id)
        folder = export_dir(cache_root, task_id, src["sha16"])
        fmt = spec["id"]
        async with _LOCK:
            await asyncio.to_thread(snapshot, src, folder)
            target = folder / out_name(fmt)
            if target.is_file():
                return job_view(None, folder, fmt)
            if spec["by"] == "here" and fmt != "zip":       # a clip GLB: cut here, now
                await asyncio.to_thread(split_clip, folder / "rigged.glb", spec["clip"], target)
                return job_view(None, folder, fmt)
            if fmt == "zip":
                fbx = parse_fmt("fbx", src["clips"])
                if not (folder / "rigged.fbx").is_file():
                    enqueue(folder, task_id, src, fbx)
                job = _read_json(_job_path(folder, "zip"))
                if not job or job.get("state") not in ("queued", "running") or str(folder) not in _ZIPS:
                    if job and job.get("state") == "failed" and int(job.get("attempts") or 0) >= MAX_ATTEMPTS \
                            and time.time() - float(job.get("updated_at") or 0) < 600:
                        return job_view(job, folder, fmt)
                    job = {"schema": "autorig.v3-export-job/1", "id": job_id_of(task_id, src["sha16"], "zip"),
                           "format": "zip", "state": "running", "stage": "fbx", "progress": 0.03,
                           "attempts": int((job or {}).get("attempts") or 0), "updated_at": time.time()}
                    _write_json(_job_path(folder, "zip"), job)
                    _ZIPS[str(folder)] = asyncio.create_task(build_zip(folder, task_id, src, file_slug(task, src)))
                return job_view(_read_json(_job_path(folder, "zip")), folder, fmt)
            job = enqueue(folder, task_id, src, spec)
            return job_view(job, folder, fmt)

    async def request_or_status(task_id: str, fmt: str, request: Request, user: Any, db: Any, *, start: bool):
        task = await load(task_id, request, user, db)
        access = access_of(task, user, request)
        if not access["allowed"]:
            _deny(access, task_id)                            # fail closed: nothing below runs
        src = await asyncio.to_thread(source_of, task)
        if src is None:
            raise _error(409, "download_not_ready")
        spec = parse_fmt(fmt, src["clips"])
        if spec is None:
            raise _error(404, "download_unknown_format")
        folder = export_dir(cache_root, str(task.id), src["sha16"])
        if start:
            view = await ensure(task, src, spec)
        else:
            view = job_view(_read_json(_job_path(folder, fmt)), folder, fmt)
            if fmt == "zip" and view["state"] in ("queued", "running") and str(folder) not in _ZIPS:
                view = await ensure(task, src, spec)          # a restart dropped the packer: pick it up again
        doc = {"format": fmt, "sha": src["sha16"], "version": src["version"], **view}
        if view["state"] == "ready":
            doc["url"] = file_url(str(task.id), fmt, src["sha16"])
        return JSONResponse(doc, headers=_NO_STORE)

    @router.post("/api/task/{task_id}/downloads-v3/{fmt}")
    async def downloads_request(task_id: str, fmt: str, request: Request, user: Any = Depends(get_current_user),
                                db: Any = Depends(get_db)):
        return await request_or_status(task_id, fmt, request, user, db, start=True)

    @router.get("/api/task/{task_id}/downloads-v3/{fmt}")
    async def downloads_status(task_id: str, fmt: str, request: Request, user: Any = Depends(get_current_user),
                               db: Any = Depends(get_db)):
        return await request_or_status(task_id, fmt, request, user, db, start=False)

    @router.get("/api/task/{task_id}/downloads-v3/{fmt}/file")
    async def downloads_file(task_id: str, fmt: str, request: Request, v: str = "",
                             user: Any = Depends(get_current_user), db: Any = Depends(get_db)):
        task = await load(task_id, request, user, db)
        access = access_of(task, user, request)
        if not access["allowed"]:
            _deny(access, task_id)
        if not _SHA16.fullmatch(v or "") or not _FMT.fullmatch(fmt or ""):
            raise _error(404, "download_unknown_format")
        folder = export_dir(cache_root, str(task.id), v)
        target = folder / out_name(fmt)
        if not target.is_file():
            raise _error(404, "download_not_ready")
        src = source_of(task)
        slug = file_slug(task, src) if src and src["sha16"] == v else f"autorig-{str(task.id)[:8]}"
        m = _FMT.fullmatch(fmt)
        name = f"{slug}.{fmt}" if m.group(2) is None else f"{slug}-clip{int(m.group(2)) + 1}.{m.group(3)}"
        if src and src["sha16"] == v and m.group(2) is not None and int(m.group(2)) < len(src["clips"]):
            name = f"{slug}-{_slug(src['clips'][int(m.group(2))]) or 'clip'}.{m.group(3)}"
        ctype = {"glb": "model/gltf-binary", "fbx": "application/octet-stream", "zip": "application/zip"}[
            name.rsplit(".", 1)[-1]]
        rel = target.resolve(strict=True).relative_to(cache_root.resolve())
        return Response(status_code=200, media_type=ctype, headers={
            "X-Accel-Redirect": "/_autorig_glb_cache/" + "/".join(quote(p, safe="") for p in rel.parts),
            "Content-Type": ctype,
            "Content-Disposition": f"attachment; filename=\"{name}\"; filename*=UTF-8''{quote(name)}",
            "Cache-Control": "private, no-store",
            "X-AutoRig-Download": "v3",
        })

    # ---------------------------------------------------------------- admin: view the page as a free user
    @router.post("/api/admin/view-as")
    async def admin_view_as(request: Request, user: Any = Depends(get_current_user)):
        if user is None or not is_admin_email(getattr(user, "email", None)):
            raise HTTPException(status_code=403, detail="admin only")
        try:
            body = await request.json()
        except ValueError:
            body = {}
        mode = str((body or {}).get("mode") or "off")
        reply = JSONResponse({"mode": "free" if mode == "free" else "off"}, headers=_NO_STORE)
        if mode == "free":
            reply.set_cookie(VIEW_AS_COOKIE, "free", max_age=12 * 3600, httponly=True, secure=True, samesite="lax")
        else:
            reply.delete_cookie(VIEW_AS_COOKIE)
        return reply

    # ---------------------------------------------------------------- export workers
    def worker_name(request: Request) -> str:
        token = request.headers.get("authorization", "")
        token = token[7:].strip() if token.lower().startswith("bearer ") else ""
        if token:
            digest = hashlib.sha256(token.encode()).hexdigest()
            try:
                keys = json.loads(WORKER_KEYS.read_text())["keys"]
            except (OSError, ValueError, KeyError, TypeError):
                keys = []
            for k in keys:
                if isinstance(k, dict) and hmac.compare_digest(str(k.get("sha256") or ""), digest):
                    return str(k.get("name") or "worker")[:40]
        raise HTTPException(status_code=401, detail="worker key required")

    def leased(request: Request, jid: str) -> tuple[Path, dict[str, Any]]:
        worker = worker_name(request)
        path, job = find_job(jid)
        if job is None or job.get("state") != "running" or job.get("worker") != worker or \
                not hmac.compare_digest(str(job.get("lease") or ""), request.headers.get("x-lease", "")):
            raise HTTPException(status_code=409, detail="job not leased to this worker")
        return path, job

    @router.get("/api/v3-export/worker/next")
    async def worker_next(request: Request):
        worker = worker_name(request)
        async with _LOCK:
            job = take_job(worker)
        if job is None:
            return Response(status_code=204)
        return JSONResponse({"id": job["id"], "lease": job["lease"], "format": job["format"],
                             "spec": {"clip": job.get("clip")}, "sha256": job["sha256"],
                             "source": f"/api/v3-export/worker/jobs/{job['id']}/source.glb",
                             "script": "/api/v3-export/worker/script"}, headers=_NO_STORE)

    @router.get("/api/v3-export/worker/script")
    async def worker_script(request: Request):
        worker_name(request)
        return PlainTextResponse(BLENDER_SCRIPT.read_text(encoding="utf-8"), headers=_NO_STORE)

    @router.get("/api/v3-export/worker/jobs/{jid}/source.glb")
    async def worker_source(jid: str, request: Request):
        path, job = leased(request, jid)
        return FileResponse(Path(job["folder"]) / "rigged.glb", media_type="model/gltf-binary", headers=_NO_STORE)

    @router.post("/api/v3-export/worker/jobs/{jid}/progress")
    async def worker_progress(jid: str, request: Request):
        path, job = leased(request, jid)
        body = await request.json()
        p = max(float(job.get("progress") or 0), min(0.97, float(body.get("progress") or 0)))
        job.update(progress=round(p, 3), stage=str(body.get("stage") or job.get("stage"))[:40],
                   heartbeat_at=time.time(), updated_at=time.time())
        _write_json(path, job)
        return {"ok": True}

    @router.put("/api/v3-export/worker/jobs/{jid}/result")
    async def worker_result(jid: str, request: Request):
        path, job = leased(request, jid)
        folder = Path(job["folder"])
        tmp = folder / f".{job['format']}.{secrets.token_hex(4)}.upload"
        size = 0
        try:
            with tmp.open("wb") as fh:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_RESULT_BYTES:
                        raise HTTPException(status_code=413, detail="result too large")
                    fh.write(chunk)
                    if size % (16 << 20) < len(chunk):
                        job.update(heartbeat_at=time.time())
                        _write_json(path, job)
            with tmp.open("rb") as fh:
                if fh.read(18) != b"Kaydara FBX Binary":
                    raise HTTPException(status_code=422, detail="not a binary FBX")
            report = request.headers.get("x-export-report", "")
            os.replace(tmp, folder / out_name(job["format"]))
        finally:
            if tmp.exists():
                tmp.unlink()
        try:
            rep = json.loads(report) if report else {}
        except ValueError:
            rep = {}
        job.update(state="done", stage="ready", progress=1.0, bytes=size, updated_at=time.time(),
                   report={k: rep.get(k) for k in ("seconds", "blender", "bones", "verify", "clips") if k in rep})
        _write_json(path, job)
        try:
            (QUEUE_DIR / jid).unlink()
        except OSError:
            pass
        return {"ok": True, "bytes": size}

    @router.post("/api/v3-export/worker/jobs/{jid}/fail")
    async def worker_fail(jid: str, request: Request):
        path, job = leased(request, jid)
        body = await request.json()
        job.update(state="failed" if int(job.get("attempts") or 0) >= MAX_ATTEMPTS else "queued",
                   stage="failed", error=str(body.get("error") or "")[:300], updated_at=time.time(),
                   queued_at=time.time(), progress=0.02)
        _write_json(path, job)
        return {"ok": True, "state": job["state"]}

    return router
