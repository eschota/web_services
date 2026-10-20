"""Build 3D model by Parts (owner 2026-10-20, MT repo docs/parts-build-spec-20261020.md §3): every generated part of a
character is its own task, of the parent's owner, with a navigation link to the original.

    «каждая модель — полноценный таск со окном навигации к оригиналу, скиннинг настраивается индивидуально,
    собирается в родительской модели»

* ``POST /api/task/{parent}/parts/child`` (Bearer: the V3 dispatch token, the MT parts service autorig-parts calls it)
  ``{part, label, provider, variant, child_run, parent_run}`` -> ``{task, existing}``. The child task: same owner, the
  parent's privacy and content rating (the NSFW mode is inherited), ``pipeline_kind = "v3"``, status done (its run is
  made by mt/parts_assemble.child_run: the parent's skeleton, this part's weights, the parent's clips),
  ``poster_llm_title = "<parent title> · <part label>"``, the collection «Parts of <parent>» (one guid per parent),
  ``viewer_settings.parts = {parent_task, parent_run, part, label, provider, variant, child_run}`` and
  ``viewer_settings.v3.session = {mt_run_id, viewer_url, files_base}``. The parent gets
  ``viewer_settings.parts.children[] += {task, part, label, provider, variant, run, color}`` (the part's colour, so
  the viewer draws the children strip from v3-view alone). No credits are charged, no
  Telegram notice is sent (both notified_at stamps are set), no YouTube upload.
  **One child task per part** (the lead, 2026-10-20): a later model of the same part (another provider or variant, a
  new child run) REPLACES the run the existing child task shows (``viewer_settings.v3.session``, ``parts.child_run``;
  the earlier run goes to ``parts.history``, its files stay) and answers the same task with ``replaced: true``.
* ``POST /api/task/{parent}/parts/supersede`` (the same token) ``{tasks: [...]}``: those children leave the strip
  (``children[].superseded``) and the gallery (the child is made private, ``parts.superseded_was_public`` keeps the old
  value); their rows and files stay.
* ``GET /api/task/{id}/parts`` -> ``{parent, children[]}`` (the same block ``GET /api/task/{id}/v3-view`` carries as
  ``parts``, through :func:`parts_view`). A child task is never re-rigged on open: its rig is the parent's skeleton.
"""
from __future__ import annotations

import hmac
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

TOKEN_FILE = Path(os.getenv("AUTORIG_V3_DISPATCH_TOKEN_FILE", "/srv/autorig/secrets/v3-dispatch.token"))
MT_ROOT = Path(os.getenv("MT_ROOT", "/srv/autorig/data/motion_transfer"))
UNITY_PAGE = "/api/mt/unity/test/index.html"
PROVIDERS = ("tripo", "hunyuan_cloud", "farm_full", "farm_fast")
_RUN = re.compile(r"^[0-9a-f]{20}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_PART = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_VARIANT = re.compile(r"^[A-Za-z0-9._-]{1,40}$")
_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")
_NO_STORE = {"Cache-Control": "no-store"}
COPY_FIELDS = ("owner_type", "owner_id", "is_public", "input_type", "owner_language", "content_rating", "content_score",
               "content_classified_at", "content_classifier_version", "v3_category", "v3_subcategory")


def settings_of(task: Any) -> dict:
    try:
        s = json.loads(getattr(task, "viewer_settings", None) or "{}")
        return s if isinstance(s, dict) else {}
    except (TypeError, ValueError):
        return {}


def token_ok(request: Request) -> bool:
    auth = request.headers.get("authorization") or ""
    try:
        want = TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    return bool(want) and auth.startswith("Bearer ") and hmac.compare_digest(auth[7:].strip(), want)


def is_child(task: Any) -> bool:
    p = settings_of(task).get("parts")
    return isinstance(p, dict) and bool(p.get("parent_task"))


def parts_view(task: Any) -> Optional[dict]:
    """{parent, children[]} for the page's navigation strip, or None when the task has no parts."""
    p = settings_of(task).get("parts")
    if not isinstance(p, dict):
        return None
    parent = None
    if p.get("parent_task"):
        parent = {k: p.get(k) for k in ("parent_task", "parent_run", "part", "label", "provider", "variant", "child_run",
                                        "color")}
        parent["url"] = f"/task?id={p['parent_task']}"
    children = []
    for c in p.get("children") or []:
        if isinstance(c, dict) and _UUID.fullmatch(str(c.get("task") or "")) and not c.get("superseded"):
            children.append({**{k: c.get(k) for k in ("task", "part", "label", "provider", "variant", "run", "at",
                                                      "color")},
                             "url": f"/task?id={c['task']}"})
    if parent is None and not children:
        return None
    return {"schema": "autorig.task-parts/1", "parent": parent, "children": children}


def child_rig_version() -> dict:
    """The rig-version block of a child task: never re-rigged on open (the rig is the parent's skeleton)."""
    return {"schema": "autorig.rig-version-state/1", "pipeline": "part", "current_version": True, "new_pipeline": False,
            "rerig": {"state": "not_for_parts"}, "button": {"state": "not_for_parts"}}


def _title(task: Any, fallback: Optional[str] = None) -> str:
    """The parent's title; a V3 task without one takes the run's own words (MT build.json `what`, sent by the parts
    service), then «Task <id8>»."""
    for field in ("poster_llm_title", "collection_member_title"):
        v = str(getattr(task, field, "") or "").strip()
        if v:
            return v[:160]
    if fallback:
        return str(fallback)[:1].upper() + str(fallback)[1:160]
    return f"Task {str(task.id)[:8]}"


def _clean(body: dict) -> dict:
    part = str(body.get("part") or "")
    run = str(body.get("child_run") or "")
    prun = str(body.get("parent_run") or "")
    provider = str(body.get("provider") or "")
    variant = body.get("variant")
    if not _PART.fullmatch(part):
        raise HTTPException(400, {"error_string": "bad_part"})
    if not _RUN.fullmatch(run) or not (MT_ROOT / "runs" / run / "run.json").is_file():
        raise HTTPException(400, {"error_string": "bad_child_run"})
    if prun and not _RUN.fullmatch(prun):
        raise HTTPException(400, {"error_string": "bad_parent_run"})
    if provider not in PROVIDERS:
        raise HTTPException(400, {"error_string": "bad_provider"})
    if variant is not None and not _VARIANT.fullmatch(str(variant)):
        raise HTTPException(400, {"error_string": "bad_variant"})
    label = re.sub(r"\s+", " ", str(body.get("label") or part.replace("_", " ")))[:80]
    color = str(body.get("color") or "")
    ptitle = re.sub(r"\s+", " ", str(body.get("parent_title") or "")).strip()[:120]
    return {"part": part, "child_run": run, "parent_run": prun or None, "provider": provider,
            "variant": None if variant is None else str(variant), "label": label,
            "color": color if _COLOR.fullmatch(color) else None, "parent_title": ptitle or None}


def _session(run: str) -> dict:
    return {"mt_run_id": run, "viewer_url": f"{UNITY_PAGE}?run={run}", "files_base": f"/api/mt/files/{run}/"}


async def _replace(db: Any, parent: Any, ps: dict, parts: dict, entry: dict, child: Any, b: dict) -> Any:
    """The part's child task shows the new run: one task per part, the earlier run kept in parts.history."""
    now = datetime.utcnow()
    cs = settings_of(child)
    cp = cs.get("parts") if isinstance(cs.get("parts"), dict) else {}
    hist = list(cp.get("history") or [])
    hist.append({**{k: cp.get(k) for k in ("provider", "variant", "child_run")}, "until": now.isoformat() + "Z"})
    cp.update(provider=b["provider"], variant=b["variant"], child_run=b["child_run"], history=hist[-20:])
    if b["color"]:
        cp["color"] = b["color"]
    v3 = cs.get("v3") if isinstance(cs.get("v3"), dict) else {}
    v3["session"] = {**(v3.get("session") or {}), **_session(b["child_run"])}
    v3["updated_at"] = now.isoformat() + "Z"
    cs["v3"], cs["parts"] = v3, cp
    child.viewer_settings = json.dumps(cs, ensure_ascii=False)
    child.input_url = f"/api/mt/files/{b['child_run']}/proj/model.glb"
    child.updated_at = now
    entry.update(provider=b["provider"], variant=b["variant"], run=b["child_run"], replaced_at=now.isoformat() + "Z")
    if b["color"]:
        entry["color"] = b["color"]
    ps["parts"] = parts
    parent.viewer_settings = json.dumps(ps, ensure_ascii=False)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    print(f"[Parts build] {parent.id} part {b['part']} -> child task {child.id} now shows run {b['child_run']} "
          f"({b['provider']} {b['variant'] or ''})")
    return child


async def create_child(db: Any, task_model: Any, parent: Any, body: dict) -> tuple[Any, bool, bool]:
    """-> (child task, existing, replaced). One child task per part; one commit for the child row and the parent's
    children list."""
    b = _clean(body)
    ps = settings_of(parent)
    parts = dict(ps.get("parts")) if isinstance(ps.get("parts"), dict) else {}
    kids = [dict(c) for c in parts.get("children") or [] if isinstance(c, dict)]
    parts["children"] = kids
    for c in kids:
        if c.get("part") == b["part"] and not c.get("superseded"):
            got = (await db.execute(select(task_model).where(task_model.id == c.get("task")))).scalar_one_or_none()
            if got is None:
                continue
            if c.get("run") == b["child_run"]:
                return got, True, False
            return await _replace(db, parent, ps, parts, c, got, b), True, True
    now = datetime.utcnow()
    run = b["child_run"]
    ptitle = _title(parent, b.get("parent_title"))
    child = task_model(id=str(uuid.uuid4()))
    for f in COPY_FIELDS:
        if hasattr(task_model, f):
            setattr(child, f, getattr(parent, f, None))
    values = {
        "input_url": f"/api/mt/files/{run}/proj/model.glb", "status": "done", "pipeline_kind": "v3",
        "created_via_api": False, "queue_class": "interactive", "created_at": now, "updated_at": now,
        "processing_started_at": now, "poster_llm_title": f"{ptitle} · {b['label']}"[:256],
        "collection_guid": str(uuid.uuid5(uuid.NAMESPACE_URL, f"autorig-parts:{parent.id}")),
        "collection_title": f"Parts of {ptitle}"[:256], "collection_member_title": b["label"][:256],
        "telegram_new_notified_at": now, "telegram_done_notified_at": now, "youtube_upload_status": "skipped",
        "ready_count": 0, "total_count": 0,
        "viewer_settings": json.dumps({
            "v3": {"state": "done", "stage": "done", "progress": 1.0, "requested_intent": "part",
                   "updated_at": now.isoformat() + "Z",
                   "session": _session(run)},
            "parts": {"parent_task": str(parent.id), "parent_run": b["parent_run"], "part": b["part"],
                      "label": b["label"], "provider": b["provider"], "variant": b["variant"], "child_run": run,
                      "color": b["color"]}},
            ensure_ascii=False),
    }
    for k, v in values.items():
        if hasattr(task_model, k):
            setattr(child, k, v)
    if hasattr(task_model, "input_type") and not getattr(child, "input_type", None):
        child.input_type = "t_pose"
    db.add(child)
    parts["children"] = kids + [{
        "task": child.id, "part": b["part"], "label": b["label"], "provider": b["provider"], "variant": b["variant"],
        "run": run, "color": b["color"], "at": now.isoformat() + "Z"}]
    if b["parent_run"]:
        parts.setdefault("run", b["parent_run"])
    ps["parts"] = parts
    parent.viewer_settings = json.dumps(ps, ensure_ascii=False)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    print(f"[Parts build] {parent.id} part {b['part']} ({b['provider']} {b['variant'] or ''}) -> child task {child.id} "
          f"run {run}")
    return child, False, False


async def supersede(db: Any, task_model: Any, parent: Any, tasks: list) -> list:
    """Children leave the strip and the gallery; rows and files stay. -> the task ids marked."""
    want = {str(t) for t in tasks if _UUID.fullmatch(str(t or ""))}
    ps = settings_of(parent)
    parts = dict(ps.get("parts")) if isinstance(ps.get("parts"), dict) else {}
    kids = [dict(c) for c in parts.get("children") or [] if isinstance(c, dict)]
    now = datetime.utcnow().isoformat() + "Z"
    done = []
    for c in kids:
        if c.get("task") not in want:
            continue
        c.setdefault("superseded", now)
        child = (await db.execute(select(task_model).where(task_model.id == c["task"]))).scalar_one_or_none()
        if child is not None:
            cs = settings_of(child)
            cp = cs.get("parts") if isinstance(cs.get("parts"), dict) else {}
            if not cp.get("superseded"):
                cp["superseded"] = now
                cp["superseded_was_public"] = bool(getattr(child, "is_public", False))
            cs["parts"] = cp
            child.viewer_settings = json.dumps(cs, ensure_ascii=False)
            if hasattr(task_model, "is_public"):
                child.is_public = False                   # the gallery lists public tasks only
        done.append(c["task"])
    parts["children"] = kids
    ps["parts"] = parts
    parent.viewer_settings = json.dumps(ps, ensure_ascii=False)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    print(f"[Parts build] {parent.id} superseded children {done}")
    return done


def build_parts_build_router(*, get_db: Callable[..., Any], get_current_user: Callable[..., Any], task_model: Any,
                             is_admin_email: Callable[[Optional[str]], bool]) -> APIRouter:
    router = APIRouter()

    @router.post("/api/task/{task_id}/parts/child")
    async def parts_child(task_id: str, request: Request, db: Any = Depends(get_db)):
        if not token_ok(request):
            raise HTTPException(401, {"error_string": "unauthorized"})
        if not _UUID.fullmatch(str(task_id or "")):
            raise HTTPException(404, "Task not found")
        parent = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if parent is None:
            raise HTTPException(404, "Task not found")
        try:
            body = await request.json()
        except Exception:                                 # noqa: BLE001
            body = {}
        child, existing, replaced = await create_child(db, task_model, parent, body if isinstance(body, dict) else {})
        return JSONResponse({"task": child.id, "existing": existing, "replaced": replaced, "parent": str(parent.id),
                             "url": f"/task?id={child.id}"}, headers=_NO_STORE)

    @router.post("/api/task/{task_id}/parts/supersede")
    async def parts_supersede(task_id: str, request: Request, db: Any = Depends(get_db)):
        if not token_ok(request):
            raise HTTPException(401, {"error_string": "unauthorized"})
        if not _UUID.fullmatch(str(task_id or "")):
            raise HTTPException(404, "Task not found")
        parent = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if parent is None:
            raise HTTPException(404, "Task not found")
        try:
            body = await request.json()
        except Exception:                                 # noqa: BLE001
            body = {}
        tasks = body.get("tasks") if isinstance(body, dict) else None
        if not isinstance(tasks, list) or not tasks:
            raise HTTPException(400, {"error_string": "no_tasks"})
        done = await supersede(db, task_model, parent, tasks)
        return JSONResponse({"superseded": done, "parts": parts_view(parent)}, headers=_NO_STORE)

    @router.get("/api/task/{task_id}/parts")
    async def parts_of(task_id: str, request: Request, user: Any = Depends(get_current_user),
                       db: Any = Depends(get_db)):
        if not _UUID.fullmatch(str(task_id or "")):
            raise HTTPException(404, "Task not found")
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        from v3_viewer_routes import _can_access_task
        if task is None or not _can_access_task(task, is_public=bool(getattr(task, "is_public", False)), user=user,
                                                request=request, is_admin_email=is_admin_email):
            raise HTTPException(404, "Task not found")
        return JSONResponse(parts_view(task) or {"schema": "autorig.task-parts/1", "parent": None, "children": []},
                            headers=_NO_STORE)

    return router
