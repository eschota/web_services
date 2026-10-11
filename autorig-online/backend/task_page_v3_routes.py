"""Task page V3: live state API and the Unity viewer feed (Task page · V3, 2026-10-10).

1 task = 1 viewer.  ``/task?id=…`` in V3 mode is a thin shell around the Unity
viewer (``/api/mt/unity/test/index.html``).  This module answers two questions
for that shell, both resolved on the server from the task id alone:

* ``GET /api/task/{id}/v3-view``: where the task is (queue position, stage,
  progress) and which viewer can show it.  A task of the V3 conveyor opens the
  Motion Transfer run that Intake · V3 persisted in its Task row
  (``viewer_settings["v3"]["session"]``, read through the ``v3-shell`` contract
  of v3_runtime_mount); any other task opens its own classic outputs through
  the feed below.  A run id is never taken from the browser.
* ``/api/task-viewer/{id}/…``: an API base for the Unity viewer
  (``?api=…&run=task``) that serves the task's cached classic GLBs in the
  layout the viewer reads (``files/task/rig/rig.json``, ``rig/rigged.glb``,
  ``proj/model.glb``, ``card/card.json``) and redirects the shared assets
  (texture library, baked scenes, settings) to the Motion Transfer service.

Private tasks follow the same owner / anon_id / admin rule as the V3 viewer
routes.  The V3 binding and its projection belong to Intake · V3 and are only read.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, quote, urlsplit

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy import and_, case, func, or_, select

import task_page_live
from v3_viewer_routes import _can_access_task

log = logging.getLogger("task_page_v3")

UNITY_PAGE = "/api/mt/unity/test/index.html"
LEGACY_RUN = "task"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_MT_RUN = re.compile(r"^[0-9a-f]{20}$")
_SHARED_MT_PREFIXES = ("texlib/", "scenes/", "avatar/", "static/", "cas/")
_SHARED_MT_EXACT = ("rig-editor/capabilities",)
_TASK_STATES = {"created", "processing", "done", "error"}
_NO_STORE = {"Cache-Control": "private, no-store, max-age=0", "X-Robots-Tag": "noindex"}
BACKEND_ORIGIN = os.getenv("AUTORIG_TASK_PAGE_BACKEND_ORIGIN", "http://127.0.0.1:8200")

# (task_id, kind) -> {"running": asyncio.Task | None, "at": float, "status": int | None}
_WARM: dict[tuple[str, str], dict[str, Any]] = {}
_WARM_RETRY_SECONDS = 60.0
_WARM_TIMEOUT_SECONDS = 240.0


# --------------------------------------------------------------------------- cached classic outputs

def _valid_glb(path: Path) -> Optional[os.stat_result]:
    try:
        st = path.stat()
        if st.st_size < 12:
            return None
        with path.open("rb") as handle:
            header = handle.read(12)
    except OSError:
        return None
    if header[:4] != b"glTF" or int.from_bytes(header[4:8], "little") != 2:
        return None
    if int.from_bytes(header[8:12], "little") != st.st_size:
        return None
    return st


def cached_glb(cache_dir: Path, task_id: str, kinds: tuple[str, ...]) -> Optional[tuple[Path, os.stat_result]]:
    for kind in kinds:
        path = cache_dir / f"{task_id}_{kind}.glb"
        st = _valid_glb(path)
        if st is not None:
            return path, st
    return None


RIGGED_KINDS = ("animations_viewer", "animations")
STATIC_KINDS = ("prepared_viewer", "prepared")
MT_ROOT = Path(os.getenv("AUTORIG_MT_ROOT", "/srv/autorig/data/motion_transfer"))


def fast_rig(task_id: str) -> Optional[tuple[Path, os.stat_result, dict]]:
    """Rig path V3 (2026-10-10, «риг в пределах одной минуты»): the task's mirror run gets a fast rig at upload
    (MT mt/classic_mirror.py -> mt/rig_first.py, ~5-10 s); it feeds the viewer until the classic rig is cached."""
    try:
        binding = json.loads((MT_ROOT / "task_agents" / f"{task_id}.json").read_text(encoding="utf-8"))
        run = str(binding.get("run_id") or "")
        if not _MT_RUN.fullmatch(run):
            return None
        rig_dir = MT_ROOT / "runs" / run / "rig"
        doc = json.loads((rig_dir / "rig.json").read_text(encoding="utf-8"))
        if not isinstance(doc, dict) or doc.get("source") != "fast-v3" or not doc.get("built_at"):
            return None
        st = _valid_glb(rig_dir / "rigged.glb")
        return (rig_dir / "rigged.glb", st, doc) if st is not None else None
    except (OSError, ValueError, AttributeError):
        return None


def current_fast_rig(task_id: str) -> Optional[tuple[Path, os.stat_result, dict]]:
    """Only current-version rigs (2026-10-11): the mirror run's rig once it carries the server's rig version (a re-rig
    on open, mt/rerig_open.py) wins over the classic converter's cached rig."""
    fast = fast_rig(task_id)
    if fast is None:
        return None
    try:
        import rerig_on_open
        cur = rerig_on_open.current_version()
    except Exception:  # noqa: BLE001
        return None
    return fast if cur and fast[2].get("rig_version") == cur else None


def _accel(cache_dir: Path, path: Path, filename: str) -> Response:
    relative = path.resolve(strict=True).relative_to(cache_dir.resolve())
    uri = "/_autorig_glb_cache/" + "/".join(quote(part, safe="") for part in relative.parts)
    return Response(status_code=200, media_type="model/gltf-binary", headers={
        "X-Accel-Redirect": uri,
        "Content-Type": "model/gltf-binary",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Encoding": "identity",
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=86400",
        "X-AutoRig-Task-Viewer": "legacy",
    })


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


async def _warm_once(task_id: str, kind: str) -> Optional[int]:
    """Ask the classic endpoint for the GLB once, so it lands in the GLB cache."""
    url = f"{BACKEND_ORIGIN}/api/task/{task_id}/{kind}.glb"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_WARM_TIMEOUT_SECONDS, connect=5.0)) as client:
            async with client.stream("GET", url, headers={"Range": "bytes=0-0",
                                                          "X-AutoRig-Warmup": "task-page-v3"}) as reply:
                return reply.status_code
    except Exception as exc:  # network trouble is a failed warm-up, never a page error
        log.info("task page warm-up %s %s failed: %s", task_id, kind, exc)
        return None


def start_warmup(task_id: str, kind: str) -> str:
    """'running' | 'cooldown' | 'started' for a background cache fill of one GLB."""
    key = (task_id, kind)
    entry = _WARM.get(key)
    now = time.monotonic()
    if entry:
        running = entry.get("running")
        if running is not None and not running.done():
            return "running"
        if now - entry.get("at", 0.0) < _WARM_RETRY_SECONDS:
            return "cooldown"

    async def run() -> None:
        status = await _warm_once(task_id, kind)
        _WARM[key] = {"running": None, "at": time.monotonic(), "status": status}

    if len(_WARM) > 2048:
        for stale in [k for k, v in _WARM.items() if v.get("running") is None][:1024]:
            _WARM.pop(stale, None)
    _WARM[key] = {"running": asyncio.create_task(run()), "at": now, "status": None}
    return "started"


# --------------------------------------------------------------------------- V3 conveyor (read-only)

def _v3_projection(task: Any) -> dict[str, Any]:
    """The V3 state Intake · V3 projects into the Task row (viewer_settings["v3"])."""
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except (TypeError, ValueError):
        settings = {}
    v3 = settings.get("v3") if isinstance(settings, dict) else None
    return v3 if isinstance(v3, dict) else {}


def _v3_shell(task: Any) -> dict[str, Any]:
    """Intake's read contract (autorig.task-v3-shell/1), computed from the same row."""
    try:
        from v3_runtime_mount import _shell

        shell = _shell(task)
        if isinstance(shell, dict) and shell.get("schema") == "autorig.task-v3-shell/1":
            return shell
    except Exception as exc:  # an older release without the V3 runtime
        log.info("V3 shell contract unavailable: %s", exc)
    v3 = _v3_projection(task)
    state = str(v3.get("state") or "pending")
    progress = float(v3.get("progress") or 0) if isinstance(v3.get("progress"), (int, float)) else 0.0
    status = {"done": "done", "needs_review": "needs_review", "failed": "failed",
              "blocked_protocol": "failed"}.get(state, "created" if state.startswith("pending") else "processing")
    return {"schema": "autorig.task-v3-shell/1", "status": status, "stage": str(v3.get("stage") or state),
            "stage_id": str(v3.get("stage") or state), "progress": progress, "viewer_url": None,
            "message": "V3"}


def v3_viewer(task: Any, shell: dict[str, Any]) -> Optional[dict[str, Any]]:
    """The Motion Transfer run bound to this task, only as the persisted session names it."""
    session = _v3_projection(task).get("session")
    if not isinstance(session, dict):
        return None
    run = str(session.get("mt_run_id") or "")
    if not _MT_RUN.fullmatch(run):
        return None
    url = urlsplit(str(shell.get("viewer_url") or session.get("viewer_url") or ""))
    if url.scheme or url.netloc or url.path != UNITY_PAGE or parse_qs(url.query).get("run") != [run]:
        return None
    return {"kind": "mt-run", "page": UNITY_PAGE, "params": {"run": run},
            "rigged": shell.get("status") in ("done", "needs_review"), "revision": run}


# --------------------------------------------------------------------------- queue

async def queue_snapshot(db: Any, task_model: Any, task: Any) -> dict[str, Any]:
    """Tasks ahead in the converter queue, in task_priority's dispatch order."""
    background = case((func.lower(func.coalesce(task_model.queue_class, "")) == "background", 1), else_=0)
    mine = 1 if str(getattr(task, "queue_class", "") or "").strip().lower() == "background" else 0
    created = getattr(task, "created_at", None)
    ahead_clause = background < mine
    if created is not None:
        ahead_clause = or_(ahead_clause, and_(background == mine, or_(
            task_model.created_at < created,
            and_(task_model.created_at == created, task_model.id < task.id))))
    ahead = (await db.execute(select(func.count()).select_from(task_model).where(
        task_model.status == "created",
        task_model.pipeline_kind != "generate",
        task_model.id != task.id,
        ahead_clause,
    ))).scalar() or 0
    processing = (await db.execute(select(func.count()).select_from(task_model).where(
        task_model.status == "processing"))).scalar() or 0
    return {"ahead": int(ahead), "processing": int(processing)}


# --------------------------------------------------------------------------- measured classic durations
# Live processing · V3 (owner 2026-10-10: the card sat at «Processing 0 %»): a classic task reports outputs late (0/8
# for minutes), so until it does the card moves by the elapsed time against the median of recent classic tasks.
_TYPICAL: dict[str, Any] = {"at": 0.0, "processing": None, "queue": None, "n": 0}


def _median(values: list[float]) -> Optional[float]:
    values = sorted(v for v in values if v is not None and v > 0)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


async def classic_typical(db: Any, task_model: Any) -> dict[str, Any]:
    """Median processing and queue seconds of the last 60 finished classic rig tasks (cached for 10 minutes)."""
    now = time.monotonic()
    if _TYPICAL["processing"] is not None and now - _TYPICAL["at"] < 600:
        return _TYPICAL
    try:
        rows = (await db.execute(
            select(task_model.created_at, task_model.processing_started_at, task_model.last_progress_at)
            .where(task_model.status == "done", task_model.pipeline_kind == "rig",
                   task_model.processing_started_at.isnot(None), task_model.last_progress_at.isnot(None))
            .order_by(task_model.created_at.desc()).limit(60))).all()
    except Exception as exc:  # a failed estimate never fails the page
        log.info("classic durations unavailable: %s", exc)
        rows = []
    proc = [(r[2] - r[1]).total_seconds() for r in rows if r[1] and r[2]]
    queue = [(r[1] - r[0]).total_seconds() for r in rows if r[0] and r[1]]
    _TYPICAL.update(at=now, processing=_median(proc) or 700.0, queue=_median(queue), n=len(proc))
    return _TYPICAL


# --------------------------------------------------------------------------- state

def _task_status(task: Any) -> str:
    status = str(getattr(task, "status", "") or "created").strip().lower()
    return status if status in _TASK_STATES else "processing"


def _progress01(task: Any) -> float:
    try:
        value = float(getattr(task, "progress", 0) or 0)
    except (TypeError, ValueError):
        return 0.0
    if value != value:
        return 0.0
    return max(0.0, min(1.0, value / 100.0 if value > 1 else value))


def _seconds_since(value: Any) -> Optional[int]:
    if not isinstance(value, datetime):
        return None
    moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - moment).total_seconds()))


def _title(task: Any) -> Optional[str]:
    for field in ("poster_llm_title", "collection_member_title"):
        value = str(getattr(task, field, "") or "").strip()
        if value:
            return value[:160]
    return None


def _v3_state(task: Any, *, is_admin: bool) -> dict[str, Any]:
    """A V3 conveyor task: Intake's projection decides status, stage and run."""
    shell = _v3_shell(task)
    status = {"created": "created", "processing": "processing", "done": "done",
              "needs_review": "needs_review", "failed": "error"}.get(str(shell.get("status")), "processing")
    try:
        progress = max(0.0, min(1.0, float(shell.get("progress") or 0)))
    except (TypeError, ValueError):
        progress = 0.0
    viewer = v3_viewer(task, shell)
    return {
        "stage": str(shell.get("stage_id") or shell.get("stage") or status)[:80],
        "stage_title": str(shell.get("stage") or "")[:120] or None,       # legacy (Russian); use the keys below
        "stage_key": shell.get("stage_key"), "sub_key": shell.get("sub_key"), "stage_seconds": shell.get("stage_seconds"),
        "status": status,
        "progress": progress,
        "queue": None,
        "viewer": viewer,
        "model": {"state": "ready" if viewer else ("unavailable" if status == "error" else "pending")},
        "v3": {"state": str(_v3_projection(task).get("state") or "pending"),
               "message": str(shell.get("message") or "")[:400] or None,
               "viewer_state": shell.get("viewer_state")},
    }


async def _classic_state(task: Any, db: Any, *, task_model: Any, cache_dir: Path) -> dict[str, Any]:
    """A classic conveyor task: its own cached GLBs feed the Unity viewer."""
    task_id = str(task.id)
    status = _task_status(task)
    progress = _progress01(task)
    queue = None
    if status == "created":
        queue = await queue_snapshot(db, task_model, task)
        queue["wait_seconds"] = _seconds_since(getattr(task, "created_at", None))
    eta = None
    basis = "outputs" if progress > 0 else None
    if status == "processing":
        typical = await classic_typical(db, task_model)
        elapsed = _seconds_since(getattr(task, "processing_started_at", None))
        expected = float(typical.get("processing") or 0)
        if elapsed is not None and expected > 0:
            if progress < 0.02:                       # no outputs reported yet: move by the clock, never past 90 %
                progress, basis = min(0.9, 0.9 * elapsed / expected), "elapsed"
                eta = max(expected * 0.05, expected - elapsed)
            else:                                     # outputs arrive: their own pace, the median as a floor
                eta = max(elapsed / progress * (1 - progress), (expected - elapsed) * 0.5, 0.0)
    rigged = cached_glb(cache_dir, task_id, RIGGED_KINDS)
    static = cached_glb(cache_dir, task_id, STATIC_KINDS)
    warming = None
    # Rig within one minute (owner, 2026-10-10): the converter collects the viewer GLBs right after the
    # retarget, minutes before Unity/video/ZIP; update_task_progress stores the URL once its header is
    # valid, so the rig is warmed while the task is still processing (Converter speed · V3).
    rig_declared = bool(str(getattr(task, "viewer_animations_glb_url", None) or "").strip())
    if rigged is None and (status == "done" or (status == "processing" and rig_declared)):
        warming = start_warmup(task_id, "animations")
    if status in ("processing", "done") and rigged is None and static is None:
        prepared = start_warmup(task_id, "prepared")
        warming = warming if warming in ("running", "started") else prepared
    viewer = None
    chosen = rigged or static
    if chosen is not None:
        viewer = {"kind": "legacy", "page": UNITY_PAGE,
                  "api_path": f"/api/task-viewer/{task_id}",
                  "params": {"run": LEGACY_RUN, "agent": "0"},
                  "rigged": rigged is not None,
                  "revision": f"{'rig' if rigged is not None else 'model'}-{int(chosen[1].st_mtime)}"}
        model_state = "ready" if rigged is not None else "static"
        if status == "done" and rigged is None and warming in ("running", "started"):
            model_state = "warming"
    elif warming in ("running", "started"):
        model_state = "warming"
    elif status in ("error", "done"):
        model_state = "unavailable"
    else:
        model_state = "pending"
    current = current_fast_rig(task_id)
    if current is not None or (rigged is None and status != "error"):
        fast = current or fast_rig(task_id)
        if fast is not None:
            viewer = {"kind": "legacy", "page": UNITY_PAGE,
                      "api_path": f"/api/task-viewer/{task_id}",
                      "params": {"run": LEGACY_RUN, "agent": "0"},
                      "rigged": True, "fast_rig": True,
                      "revision": f"fast-{int(fast[1].st_mtime)}"}
            model_state = "ready"
    return {
        "stage": {"created": "queued", "processing": "processing", "done": "ready", "error": "failed"}[status],
        "stage_title": None,
        "status": status,
        "progress": progress,
        "queue": queue,
        "viewer": viewer,
        "model": {"state": model_state},
        "v3": None,
        "eta_s": round(eta) if eta is not None else None,
        "progress_basis": basis,
    }


async def build_state(task: Any, db: Any, *, task_model: Any, cache_dir: Path,
                      is_admin: bool) -> dict[str, Any]:
    task_id = str(task.id)
    v3_task = str(getattr(task, "pipeline_kind", "") or "").strip().lower() == "v3"
    part = _v3_state(task, is_admin=is_admin) if v3_task else \
        await _classic_state(task, db, task_model=task_model, cache_dir=cache_dir)
    return {
        "schema": "autorig.task-page-v3/1",
        "task_id": task_id,
        "title": _title(task),
        "status": part["status"],
        "stage": part["stage"],
        "stage_title": part["stage_title"],
        "stage_key": part.get("stage_key"),
        "sub_key": part.get("sub_key"),
        "stage_seconds": part.get("stage_seconds"),
        "progress": round(1.0 if part["status"] == "done" else part["progress"], 4),
        "eta_s": part.get("eta_s"),
        "progress_basis": part.get("progress_basis"),
        "queue": part["queue"],
        "pipeline": "v3" if v3_task else "classic",
        "v3": part["v3"],
        "viewer": part["viewer"],
        "model": part["model"],
        "links": {"classic": f"/task?id={task_id}&classic=1"},
        "error": "failed" if part["status"] == "error" else None,
        "admin": bool(is_admin),
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------- router

def build_task_page_v3_router(*, get_db: Callable[..., Any], get_current_user: Callable[..., Any],
                              task_model: Any, is_admin_email: Callable[[Optional[str]], bool],
                              glb_cache_dir: Path) -> APIRouter:
    router = APIRouter()
    cache_dir = Path(glb_cache_dir)

    async def authorized(task_id: str, request: Request, user: Any, db: Any) -> Any:
        if not _UUID.fullmatch(str(task_id or "")):
            raise HTTPException(status_code=404, detail="Task not found")
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if task is None or not _can_access_task(task, is_public=bool(getattr(task, "is_public", False)),
                                                user=user, request=request, is_admin_email=is_admin_email):
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    def admin(user: Any) -> bool:
        return bool(user is not None and is_admin_email(getattr(user, "email", None)))

    @router.get("/api/task/{task_id}/v3-view")
    async def task_v3_view(task_id: str, request: Request, user: Any = Depends(get_current_user),
                           db: Any = Depends(get_db)):
        task = await authorized(task_id, request, user, db)
        state = await build_state(task, db, task_model=task_model, cache_dir=cache_dir, is_admin=admin(user))
        try:                                     # Only current-version rigs (2026-10-11): the open re-rigs an old rig
            import rerig_on_open
            state["rig_version"] = rerig_on_open.state(task, request)
        except Exception as exc:  # noqa: BLE001 - never costs the page
            log.warning("rig version state failed for %s: %s", task_id, exc)
        return JSONResponse(state, headers=_NO_STORE)

    @router.get("/api/task/{task_id}/rig-version")
    async def task_rig_version(task_id: str, request: Request, user: Any = Depends(get_current_user),
                               db: Any = Depends(get_db)):
        task = await authorized(task_id, request, user, db)
        import rerig_on_open
        return JSONResponse(rerig_on_open.state(task, request, trigger=False), headers=_NO_STORE)

    @router.get("/api/task-page/live")
    async def task_page_live_state():
        return JSONResponse(task_page_live.describe(), headers={"Cache-Control": "no-store"})

    @router.api_route("/api/task-viewer/{task_id}/files/{run}/{rel:path}", methods=["GET", "HEAD"])
    async def task_viewer_file(task_id: str, run: str, rel: str, request: Request,
                               user: Any = Depends(get_current_user), db: Any = Depends(get_db)):
        task = await authorized(task_id, request, user, db)
        if run != LEGACY_RUN:
            raise HTTPException(status_code=404, detail="not found")
        head = request.method == "HEAD"
        if rel == "rig/rig.json":
            rigged = cached_glb(cache_dir, task_id, RIGGED_KINDS)
            fast = current_fast_rig(task_id) or (fast_rig(task_id) if rigged is None else None)
            if fast is not None:
                return JSONResponse({**fast[2], "task_id": task_id, "built_at": _iso(fast[1].st_mtime)},
                                    headers={"Cache-Control": "no-cache"})
            if rigged is None:
                raise HTTPException(status_code=404, detail="no rig yet")
            return JSONResponse({
                "schema": "autorig.task-viewer.legacy-rig/1",
                "source": "classic-task",
                "task_id": task_id,
                "built_at": _iso(rigged[1].st_mtime),
            }, headers={"Cache-Control": "no-cache"})
        if rel in ("rig/rigged.glb", "proj/model.glb"):
            rig = rel == "rig/rigged.glb"
            current = current_fast_rig(task_id) if rig else None
            if current is not None:                     # a re-rig of the current version wins over the classic rig
                return FileResponse(current[0], media_type="model/gltf-binary", headers={
                    "Cache-Control": "no-cache", "X-AutoRig-Task-Viewer": "fast-v3-current"})
            hit = cached_glb(cache_dir, task_id, RIGGED_KINDS if rig else STATIC_KINDS)
            if hit is not None:
                return _accel(cache_dir, hit[0], f"{task_id}_{'animations' if rig else 'prepared'}.glb")
            fast = fast_rig(task_id) if rig else None
            if fast is not None:
                return FileResponse(fast[0], media_type="model/gltf-binary", headers={
                    "Cache-Control": "no-cache", "X-AutoRig-Task-Viewer": "fast-v3"})
            if head:
                raise HTTPException(status_code=404, detail="not cached yet")
            kind = "animations" if rig else "prepared"
            return RedirectResponse(f"/api/task/{task_id}/{kind}.glb", status_code=307,
                                    headers={"Cache-Control": "no-store"})
        if rel == "card/card.json":
            card: dict[str, Any] = {"schema": "autorig.task-viewer.card/1", "task_id": task_id}
            title = _title(task)
            if title:
                card["name"] = title
            return JSONResponse(card, headers={"Cache-Control": "no-cache"})
        raise HTTPException(status_code=404, detail="not found")

    @router.api_route("/api/task-viewer/{task_id}/{rest:path}", methods=["GET", "HEAD", "POST"])
    async def task_viewer_shared(task_id: str, rest: str, request: Request,
                                 user: Any = Depends(get_current_user)):
        if not _UUID.fullmatch(str(task_id or "")) or ".." in rest.split("/"):
            raise HTTPException(status_code=404, detail="not found")
        query = request.url.query
        target = f"/api/mt/{rest}" + (f"?{query}" if query else "")
        if rest == "viewer/settings":
            if request.method == "POST" and not admin(user):
                # One shared settings profile belongs to the owner; a visitor's
                # toggles stay in the page they made them in.
                return JSONResponse({"ok": True, "saved": False}, headers={"Cache-Control": "no-store"})
            return RedirectResponse(target, status_code=307, headers={"Cache-Control": "no-store"})
        if request.method != "POST" and (rest in _SHARED_MT_EXACT or rest.startswith(_SHARED_MT_PREFIXES)):
            cache = "no-cache" if rest.endswith(".json") or "/" not in rest.rstrip("/") else "public, max-age=3600"
            return RedirectResponse(target, status_code=307, headers={"Cache-Control": cache})
        raise HTTPException(status_code=404, detail="not available for this task")

    return router
