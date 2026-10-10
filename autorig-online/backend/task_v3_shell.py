"""Fail-closed resolver for the V3-only ``/task`` shell.

This module deliberately does not discover or create Motion Transfer runs.  A
caller must first authorize access to the AutoRig Task and then supply the
authoritative, persisted Task -> V3 run binding.  In particular, query-string
``run`` values are never accepted and this module never calls ``/api/mt/kit``.
"""
from __future__ import annotations

import inspect
import re
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MT_RUN = re.compile(r"^[0-9a-f]{20}$")
_DISPATCH_RUN = re.compile(r"^v3run-[0-9a-f]{32}$")
_TASK_STATES = {"created", "processing", "needs_review", "done", "error", "failed"}
_RUN_STATES = {"queued", "running", "needs_review", "done", "failed"}


class TaskV3BindingError(ValueError):
    """The persisted Task -> V3 run binding is absent or inconsistent."""


@dataclass(frozen=True)
class TaskV3Shell:
    task_id: str
    status: str
    stage: str
    progress: float
    viewer_url: Optional[str]
    viewer_state: str
    message: str

    def public(self) -> dict[str, Any]:
        return {
            "schema": "autorig.task-v3-shell/1",
            "task_id": self.task_id,
            "status": self.status,
            "stage": self.stage,
            "progress": self.progress,
            "viewer_url": self.viewer_url,
            "viewer_state": self.viewer_state,
            "message": self.message,
        }


def _task_uuid(value: Any) -> str:
    try:
        parsed = str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise TaskV3BindingError("task_id is not a canonical UUID") from exc
    if str(value) != parsed:
        raise TaskV3BindingError("task_id is not a canonical UUID")
    return parsed


def _progress(value: Any, fallback: float) -> float:
    if isinstance(value, bool):
        return fallback
    try:
        number = float(value)
    except (TypeError, ValueError):
        return fallback
    if number != number or number in (float("inf"), float("-inf")):
        return fallback
    return max(0.0, min(1.0, number))


def _task_fallback(task: Any) -> tuple[str, str, float]:
    status = str(getattr(task, "status", "") or "created").strip().lower()
    if status not in _TASK_STATES:
        status = "processing"
    raw = getattr(task, "progress", 0) or 0
    try:
        progress = max(0.0, min(1.0, float(raw) / 100.0 if float(raw) > 1 else float(raw)))
    except (TypeError, ValueError):
        progress = 0.0
    return status, status, progress


def resolve_task_v3_shell(
    task: Any,
    binding: Mapping[str, Any] | None,
    run_status: Mapping[str, Any] | None = None,
) -> TaskV3Shell:
    """Resolve public shell state from an already-authorized Task.

    ``binding`` must come from authoritative server-side storage.  Sensitive
    source refs, hashes, idempotency keys and receipt ids are validated where
    present but are never returned to the browser.
    """
    task_id = _task_uuid(getattr(task, "id", None))
    fallback_status, fallback_stage, fallback_progress = _task_fallback(task)
    if not binding:
        terminal = fallback_status in {"error", "failed"}
        return TaskV3Shell(
            task_id,
            "failed" if terminal else fallback_status,
            fallback_stage,
            fallback_progress,
            None,
            "failed" if terminal else "awaiting_binding",
            "V3 processing failed." if terminal else "Waiting for the verified V3 run binding.",
        )
    if str(binding.get("task_id") or "") != task_id:
        raise TaskV3BindingError("binding task_id mismatch")
    source_sha = str(binding.get("source_sha256") or "")
    if not _SHA256.fullmatch(source_sha):
        raise TaskV3BindingError("binding source_sha256 is invalid")
    run_id = str(binding.get("run_id") or "")
    if not (_MT_RUN.fullmatch(run_id) or _DISPATCH_RUN.fullmatch(run_id)):
        raise TaskV3BindingError("binding run_id is invalid")
    if binding.get("persisted") is not True:
        raise TaskV3BindingError("binding is not persisted")

    status_doc = run_status or {}
    if not status_doc or any(status_doc.get(key) in (None, "") for key in ("run_id", "task_id", "source_sha256")):
        return TaskV3Shell(
            task_id,
            "processing",
            "binding_verification",
            min(fallback_progress, .99),
            None,
            "awaiting_verification",
            "Waiting for source-bound V3 run status verification.",
        )
    if status_doc:
        if str(status_doc.get("run_id") or "") != run_id:
            raise TaskV3BindingError("run status identity mismatch")
        remote_task = status_doc.get("task_id")
        if str(remote_task) != task_id:
            raise TaskV3BindingError("run status task_id mismatch")
        remote_source = status_doc.get("source_sha256")
        if str(remote_source) != source_sha:
            raise TaskV3BindingError("run status source_sha256 mismatch")
    status = str(status_doc.get("status") or fallback_status).strip().lower()
    if status not in _RUN_STATES:
        status = "running" if fallback_status not in {"error", "failed"} else "failed"
    stage = str(status_doc.get("stage") or fallback_stage).strip()[:80] or "processing"
    progress = _progress(status_doc.get("progress"), fallback_progress)
    raw_done = status == "done"
    qa = status_doc.get("qa") if isinstance(status_doc.get("qa"), Mapping) else {}
    qa_accepted = bool(
        qa.get("status") == "accepted"
        and qa.get("source_sha256") == source_sha
        and _SHA256.fullmatch(str(qa.get("report_sha256") or ""))
        and str(qa.get("verifier_receipt_id") or "").strip()
    )
    if raw_done and qa_accepted:
        progress = 1.0
    elif raw_done:
        # A Motion Transfer orchestration run finishing is not an anatomical,
        # skinning or deformation quality certificate.
        status = "needs_review"
        stage = "qa_verification"
        progress = min(progress, .99)

    if _MT_RUN.fullmatch(run_id):
        publication = binding.get("viewer_publication")
        publication_ok = bool(
            isinstance(publication, Mapping)
            and publication.get("schema") == "autorig.v3.viewer-publication/1"
            and publication.get("verified") is True
            and publication.get("task_id") == task_id
            and publication.get("source_sha256") == source_sha
            and publication.get("run_id") == run_id
            and str(publication.get("serving_artifact_sha256") or "")
            and _SHA256.fullmatch(str(publication.get("serving_artifact_sha256") or ""))
            and str(publication.get("receipt_id") or "").strip()
        )
        if not publication_ok:
            return TaskV3Shell(
                task_id,
                status,
                "viewer_publication",
                min(progress, .99),
                None,
                "awaiting_viewer_publication",
                "Waiting for the verified Unity viewer publication receipt.",
            )
        viewer = f"/api/mt/unity/test/index.html?run={quote(run_id, safe='')}"
        return TaskV3Shell(
            task_id,
            status,
            stage,
            progress,
            viewer,
            "ready" if status in {"done", "needs_review"} else "loading",
            "V3 viewer is ready." if status == "done" else
            "V3 viewer is available; rig quality still needs verification." if status == "needs_review" else
            "V3 is processing the model.",
        )

    # The durable dispatch worker currently emits v3run-* ids, while the Unity
    # viewer reads the legacy 20-hex run directory.  Never truncate or rewrite
    # that identity: a verified publication mapping is required.
    terminal = status == "failed"
    return TaskV3Shell(
        task_id,
        status,
        stage,
        progress,
        None,
        "failed" if terminal else "awaiting_viewer_publication",
        "V3 processing failed." if terminal else
        "V3 run is active; waiting for its verified Unity viewer publication.",
    )


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


AuthorizedTaskLoader = Callable[[str, Request, Any, Any], Any | Awaitable[Any]]
BindingLookup = Callable[[str, Any], Mapping[str, Any] | None | Awaitable[Mapping[str, Any] | None]]
RunStatusLookup = Callable[[Mapping[str, Any]], Mapping[str, Any] | None | Awaitable[Mapping[str, Any] | None]]


def build_task_v3_shell_router(
    *,
    get_db: Callable[..., Any],
    get_current_user: Callable[..., Any],
    load_authorized_task: AuthorizedTaskLoader,
    lookup_binding: BindingLookup,
    lookup_run_status: RunStatusLookup,
) -> APIRouter:
    """Build the read-only endpoint; all identity/access lookups stay injectable."""
    router = APIRouter()

    @router.get("/api/task/{task_id}/v3-shell")
    async def task_v3_shell(
        task_id: str,
        request: Request,
        user: Any = Depends(get_current_user),
        db: Any = Depends(get_db),
    ):
        try:
            canonical = _task_uuid(task_id)
        except TaskV3BindingError:
            raise HTTPException(status_code=404, detail="Task not found") from None
        # This callback must use the same owner/admin/public policy as existing
        # Task artifact routes.  Returning None is intentionally indistinguishable
        # from a missing private Task.
        task = await _await(load_authorized_task(canonical, request, user, db))
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        binding = await _await(lookup_binding(canonical, db))
        run_status = await _await(lookup_run_status(binding)) if binding else None
        try:
            payload = resolve_task_v3_shell(task, binding, run_status).public()
        except TaskV3BindingError as exc:
            raise HTTPException(status_code=409, detail=f"V3 task binding is unavailable: {exc}") from exc
        return JSONResponse(payload, headers={
            "Cache-Control": "private, no-store, max-age=0",
            "Pragma": "no-cache",
            "X-Robots-Tag": "noindex",
        })

    return router
