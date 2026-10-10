"""Mounted V3 task runtime of autorig-storage and its stable read API.

* :func:`start_v3_runtime` (lifespan) installs the binding table, starts the
  durable outbox runtime (backend -> Motion Transfer ``/api/mt/v3``) and the
  intake pump.  It is restart-safe: every unsettled binding is re-enqueued and
  a projection lost to a crash is replayed from the outbox.
* The projector copies each V3 state into the Task row.  ``done`` needs the
  remote accepted QA *and* this backend's own byte/hash verification of every
  artifact; a QA failure stays an explicit ``needs_review`` Task status.
* Read API for the task page (owned by the "Task page · V3" agent):

  ``GET /api/task/{id}/v3-shell`` -> ``autorig.task-v3-shell/1``
      status, stage, progress 0..1, viewer_url, viewer_state, message
  ``GET /api/task/{id}/v3`` -> ``autorig.task-v3/1``
      the binding, session, QA gates/reasons and artifacts

  Both use the task routes' access policy and answer 404 for non-V3 tasks.
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from v3_dispatch_outbox import V3DispatchOutbox, verify_dispatch_artifacts
from v3_intake import INTAKE_ROOT, intake_loop
from v3_pipeline_adapter import V3DispatchClient
from v3_task_runtime import SameTaskDbBindings, V3TaskRuntime

ORIGIN = os.getenv("AUTORIG_V3_DISPATCH_ORIGIN", "http://127.0.0.1:8251").rstrip("/")
TOKEN_FILE = Path(os.getenv("AUTORIG_V3_DISPATCH_TOKEN_FILE", "/srv/autorig/secrets/v3-dispatch.token"))
OUTBOX_PATH = Path(os.getenv("AUTORIG_V3_OUTBOX", str(INTAKE_ROOT / "dispatch-outbox.sqlite3")))
STAGE_TITLES = {
    "intake": "приём", "normalization": "нормализация", "dispatch": "постановка в V3",
    "pending_register": "регистрация источника", "pending_submit": "постановка в V3",
    "queued": "в очереди V3", "starting": "запуск", "source": "источник", "analysis": "анализ",
    "rig": "риг", "retarget": "ретаргет", "qa": "проверка качества", "publish": "публикация",
    "complete": "готово", "qa_review": "нужна проверка",
}


def _token() -> str:
    value = os.getenv("AUTORIG_V3_DISPATCH_TOKEN", "").strip()
    if value:
        return value
    try:
        return TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _settings(task) -> dict:
    try:
        value = json.loads(task.viewer_settings or "{}")
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


def _public_artifacts(manifest: Any) -> list[dict]:
    rows = manifest.get("artifacts") if isinstance(manifest, dict) else None
    out = []
    for row in rows or []:
        if isinstance(row, dict) and row.get("role"):
            out.append({"role": str(row["role"]), "url": str(row.get("public_url") or ""),
                        "sha256": str(row.get("sha256") or ""), "bytes": row.get("bytes")})
    return out


# ---------------------------------------------------------------- projection into the Task row
async def project_task(session, binding, patch, record) -> None:
    from database import Task

    task = await session.get(Task, binding.task_id)
    if task is None or str(task.pipeline_kind or "") != "v3":
        return
    remote = record.remote_status if isinstance(record.remote_status, dict) else {}
    settings = _settings(task)
    v3 = dict(settings.get("v3") or {})
    qa = remote.get("qa") if isinstance(remote.get("qa"), dict) else {}
    artifacts = _public_artifacts(remote.get("artifact_manifest"))
    session_doc = remote.get("session") or v3.get("session") or {}
    stage = record.remote_stage or record.state
    progress = 1.0 if record.state in {"done", "needs_review"} else max(0.0, min(.99, float(record.progress or 0)))
    v3.update(
        state=record.state, stage=stage, progress=round(progress, 3), attempt=binding.attempt,
        requested_intent=binding.requested_intent, dispatch_intent=binding.dispatch_intent,
        pipeline_revision=binding.pipeline_revision, source_sha256=binding.source_sha256,
        dispatch_run_id=record.run_id, session=session_doc or None,
        error=record.error if record.state in {"failed", "blocked_protocol"} else None,
        updated_at=datetime.utcnow().isoformat() + "Z",
    )
    if qa:
        v3["qa"] = {key: qa.get(key) for key in ("status", "reasons", "gates", "layers", "body_plan",
                                                 "clips", "candidate_glb_sha256") if key in qa}
    if artifacts:
        v3["artifacts"] = artifacts
    verification = remote.get("artifact_verification")
    if record.state == "done" and isinstance(verification, dict):
        v3["verified"] = {"artifact_manifest_sha256": verification.get("artifact_manifest_sha256"),
                          "status": verification.get("status")}
    settings["v3"] = v3
    now = datetime.utcnow()
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.status = patch.status
    if patch.status == "needs_review":
        reasons = "; ".join(str(r) for r in (qa.get("reasons") or [])) or "QA requires review"
        task.error_message = f"V3: нужна проверка — {reasons}"[:1000]
    else:
        task.error_message = patch.error_message
    if patch.status == "processing" and not task.processing_started_at:
        task.processing_started_at = now
    urls = [row["url"] for row in artifacts if row["url"] and row["role"] in ("rigged_glb", "rig_json", "rig_qa")]
    if patch.terminal and urls:
        task.output_urls = urls
        task.ready_urls = urls
        task.total_count = task.ready_count = len(urls)
        glb = next((row["url"] for row in artifacts if row["role"] == "rigged_glb" and row["url"]), None)
        if glb:
            task.viewer_prepared_glb_url = glb
    elif not patch.terminal:
        task.total_count = 100
        task.ready_count = int(round(progress * 100))
    task.last_progress_at = now
    task.updated_at = now


async def after_commit(binding, patch, record) -> None:
    """Terminal notifications through the existing idempotent broadcasters."""
    try:
        if record.state == "done":
            from telegram_bot import reserve_and_broadcast_task_done
            asyncio.create_task(reserve_and_broadcast_task_done(binding.task_id))
        elif record.state in {"failed", "blocked_protocol"}:
            from telegram_bot import reserve_and_broadcast_task_error
            asyncio.create_task(reserve_and_broadcast_task_error(binding.task_id))
    except Exception as exc:                                     # a notification never stops the runtime
        print(f"[V3] notification for {binding.task_id} failed: {exc}")


# ---------------------------------------------------------------- lifecycle
@dataclass
class V3RuntimeHandle:
    runtime: V3TaskRuntime
    http: httpx.AsyncClient
    stop: asyncio.Event
    intake: asyncio.Task


async def start_v3_runtime(session_factory) -> Optional[V3RuntimeHandle]:
    token = _token()
    async with session_factory() as session:
        await SameTaskDbBindings().install(session)
        await session.commit()
    if not token:
        print("[V3] dispatch token missing: V3 tasks wait in 'pending' (no legacy fallback)")
        return None
    http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
    origin = ORIGIN + "/"

    async def fetch(row):
        response = await http.get(urljoin(origin, str(row.get("url") or "").lstrip("/")),
                                  headers={"Authorization": "Bearer " + token})
        response.raise_for_status()
        return response.content

    async def verifier(status):
        return await verify_dispatch_artifacts(status, fetch)

    runtime = V3TaskRuntime(
        session_factory=session_factory, bindings=SameTaskDbBindings(),
        outbox=V3DispatchOutbox(OUTBOX_PATH), remote_factory=lambda: V3DispatchClient(ORIGIN, token, http),
        projector=project_task, artifact_verifier=verifier, workers=1, idle_seconds=2.0,
        after_commit=after_commit)
    await runtime.start()
    stop = asyncio.Event()
    intake = asyncio.create_task(intake_loop(session_factory, stop), name="v3-intake")
    print(f"[V3] runtime started: dispatch {ORIGIN}, outbox {OUTBOX_PATH}")
    return V3RuntimeHandle(runtime, http, stop, intake)


async def stop_v3_runtime(handle: Optional[V3RuntimeHandle]) -> None:
    if handle is None:
        return
    handle.stop.set()
    handle.intake.cancel()
    await asyncio.gather(handle.intake, return_exceptions=True)
    await handle.runtime.close()
    await handle.http.aclose()


# ---------------------------------------------------------------- read API
def _shell(task) -> dict:
    v3 = _settings(task).get("v3") or {}
    state = str(v3.get("state") or "pending")
    stage = str(v3.get("stage") or state)
    progress = float(v3.get("progress") or 0)
    session = v3.get("session") if isinstance(v3.get("session"), dict) else {}
    viewer = session.get("viewer_url") if session.get("mt_run_id") else None
    if state == "done":
        status, viewer_state, message = "done", "ready", "V3: риг готов и проверен."
    elif state == "needs_review":
        reasons = "; ".join(str(r) for r in ((v3.get("qa") or {}).get("reasons") or []))
        status, viewer_state = "needs_review", "ready"
        message = "V3: результат во вьювере, нужна проверка" + (f" — {reasons}" if reasons else ".")
    elif state in {"failed", "blocked_protocol"} or task.status == "error":
        status, viewer_state = "failed", "failed"
        message = "V3: обработка остановлена — " + str(v3.get("error") or task.error_message or "ошибка")[:300]
    else:
        status = "created" if state in {"pending", "normalizing", "pending_register", "pending_submit"} else "processing"
        viewer_state = "loading" if viewer else "awaiting_binding"
        message = "V3: " + STAGE_TITLES.get(stage, stage)
    return {"schema": "autorig.task-v3-shell/1", "task_id": task.id, "status": status,
            "stage": STAGE_TITLES.get(stage, stage), "stage_id": stage,
            "progress": 1.0 if status in {"done", "needs_review"} else round(min(.99, max(0.0, progress)), 3),
            "viewer_url": viewer, "viewer_state": viewer_state, "message": message}


def _detail(task) -> dict:
    v3 = dict(_settings(task).get("v3") or {})
    intake = v3.get("intake") if isinstance(v3.get("intake"), dict) else {}
    return {"schema": "autorig.task-v3/1", "task_id": task.id, "pipeline_kind": "v3",
            "task_status": task.status, **_shell(task),
            "state": v3.get("state"), "attempt": v3.get("attempt"),
            "requested_intent": v3.get("requested_intent") or intake.get("requested_intent"),
            "source": {"sha256": v3.get("source_sha256") or intake.get("source_sha256"),
                       "format": intake.get("format"), "filename": intake.get("filename"),
                       "origin": intake.get("origin")},
            "session": v3.get("session"), "qa": v3.get("qa"), "artifacts": v3.get("artifacts") or [],
            "verified": v3.get("verified"), "error": v3.get("error"), "updated_at": v3.get("updated_at"),
            "created_at": task.created_at.isoformat() + "Z" if task.created_at else None}


def build_v3_read_router(*, get_db: Callable[..., Any], get_current_user: Callable[..., Any],
                         task_model: Any, is_admin_email: Callable[[Optional[str]], bool]) -> APIRouter:
    from sqlalchemy import select

    from v3_viewer_routes import _can_access_task

    router = APIRouter()
    headers = {"Cache-Control": "private, no-store, max-age=0", "X-Robots-Tag": "noindex"}

    async def load(task_id: str, request: Request, user: Any, db: Any):
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if task is None or str(task.pipeline_kind or "") != "v3" or not _can_access_task(
                task, is_public=bool(getattr(task, "is_public", False)), user=user, request=request,
                is_admin_email=is_admin_email):
            raise HTTPException(status_code=404, detail="V3 task not found")
        return task

    @router.get("/api/task/{task_id}/v3-shell")
    async def task_v3_shell(task_id: str, request: Request, user: Any = Depends(get_current_user),
                            db: Any = Depends(get_db)):
        return JSONResponse(_shell(await load(task_id, request, user, db)), headers=headers)

    @router.get("/api/task/{task_id}/v3")
    async def task_v3(task_id: str, request: Request, user: Any = Depends(get_current_user),
                      db: Any = Depends(get_db)):
        return JSONResponse(_detail(await load(task_id, request, user, db)), headers=headers)

    return router


async def v3_retry(db, task) -> Any:
    """A new attempt of the same V3 task (same id, same exact source), never a legacy requeue."""
    bindings = SameTaskDbBindings()
    current = await bindings.latest(db, task.id)
    if current is None:
        raise HTTPException(status_code=409, detail="This V3 task has no source binding yet")
    if current.state not in {"needs_review", "failed", "blocked_protocol"}:
        raise HTTPException(status_code=409, detail=f"The current V3 attempt is still {current.state}")
    retry = await bindings.create_retry_in_transaction(db, current)
    settings = _settings(task)
    v3 = dict(settings.get("v3") or {})
    v3.update(state="pending", stage="dispatch", progress=0.0, attempt=retry.attempt, error=None,
              updated_at=datetime.utcnow().isoformat() + "Z")
    settings["v3"] = v3
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.status = "created"
    task.error_message = None
    task.telegram_done_notified_at = None
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(f"[V3] {task.id} retry -> attempt {retry.attempt}")
    return retry
