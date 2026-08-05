"""Image -> 3D model -> rigged character, as one task in the normal task list.

A generation task is an ordinary row in ``tasks`` with ``pipeline_kind`` set to
``generate`` and ``input_url`` pointing at the picture the user uploaded. It is
deliberately not a separate entity: once the model exists the same row turns
into the convert task that rigs it, so every existing path - the dispatcher, the
progress page, the viewer, the downloads, the gallery - keeps working without
knowing that generation happened at all.

Route decision (per stage):

    picture -> detect -> riggable ? generation + rig : generation only

The detection decides the *route*, never the outcome. A picture that cannot be
rigged still gets its PBR model, because the user paid for a model either way.

Generation itself runs in renderfin, which already owns the Hunyuan worker pool,
its slot accounting and its stage retries. Doing it here would mean a second
queue pointed at the same GPUs, and two queues cannot keep each other from
overfilling the farm.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select

from database import Task

GENERATION_CREDITS = 3

# Stages we keep in viewer_settings["generation"], so no schema migration is
# needed for a feature that may still change shape.
GEN_STAGE_DETECT = "detect"
GEN_STAGE_MODEL = "model"          # renderfin is building the mesh
GEN_STAGE_RIGGING = "rigging"      # handed over to the convert pipeline
GEN_STAGE_DONE = "done"
GEN_STAGE_FAILED = "failed"


def generation_meta(task: Task) -> Dict[str, Any]:
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except (TypeError, ValueError):
        return {}
    meta = settings.get("generation")
    return meta if isinstance(meta, dict) else {}


def set_generation_meta(task: Task, **updates: Any) -> Dict[str, Any]:
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
        if not isinstance(settings, dict):
            settings = {}
    except (TypeError, ValueError):
        settings = {}
    meta = settings.get("generation")
    if not isinstance(meta, dict):
        meta = {}
    meta.update({k: v for k, v in updates.items() if v is not None})
    settings["generation"] = meta
    task.viewer_settings = json.dumps(settings)
    return meta


async def refund_generation_credits(db, task: Task, reason: str) -> None:
    """Give the credits back when the pipeline, not the picture, was at fault."""
    from database import User

    meta = generation_meta(task)
    if not meta or meta.get("refunded") or not meta.get("charged"):
        return
    if str(getattr(task, "owner_type", "") or "") != "user":
        return
    owner = await db.scalar(select(User).where(User.email == task.owner_id))
    if owner is None:
        return
    owner.balance_credits = int(owner.balance_credits or 0) + int(meta.get("charged") or 0)
    set_generation_meta(task, refunded=True, refund_reason=str(reason)[:200])
    print(f"[Generation] refunded {meta.get('charged')} credit(s) to {task.owner_id}: {reason}")


async def _fetch_image_bytes(url: str) -> Optional[bytes]:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        print(f"[Generation] cannot read source image {url}: {exc}")
        return None


async def _start_generation(db, task: Task) -> None:
    """Detect what the picture is, then hand it to renderfin."""
    import content_moderation
    import render_prompting

    image_bytes = await _fetch_image_bytes(task.input_url)
    if image_bytes is None:
        task.status = "error"
        task.error_message = "Source image could not be read"
        set_generation_meta(task, stage=GEN_STAGE_FAILED)
        await refund_generation_credits(db, task, "source image unreadable")
        task.updated_at = datetime.utcnow()
        await db.commit()
        return

    import asyncio

    try:
        verdict = await asyncio.to_thread(
            content_moderation.detect_character_for_generation, image_bytes
        )
    except Exception as exc:
        # Detection is an optimisation, not a gate: if it cannot run we still
        # owe the user a model, so fall back to the plain PBR route.
        print(f"[Generation] detection failed for {task.id}: {exc}")
        verdict = {"riggable": False, "reason": f"detection failed: {exc}"[:200]}

    try:
        job_id = await render_prompting.start_character_gen_from_image(
            task.input_url, source_task_id=task.id, user_name="site"
        )
    except Exception as exc:
        # Renderfin parks rather than fails when the farm is merely busy, so a
        # failure here is a real one; leave the task retryable instead of dead.
        print(f"[Generation] renderfin refused {task.id}: {exc}")
        task.error_message = f"Generation could not start: {str(exc)[:200]}"
        task.updated_at = datetime.utcnow()
        await db.commit()
        return

    set_generation_meta(
        task,
        stage=GEN_STAGE_MODEL,
        job_id=job_id,
        riggable=bool(verdict.get("riggable")),
        subject=verdict.get("subject") or "",
        pose=verdict.get("pose") or "",
        detect_reason=verdict.get("reason") or "",
    )
    task.status = "processing"
    task.processing_started_at = task.processing_started_at or datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(
        f"[Generation] task {task.id} -> renderfin job {job_id} "
        f"(riggable={bool(verdict.get('riggable'))}, pose={verdict.get('pose')})"
    )


async def _advance_generation(db, task: Task, meta: Dict[str, Any]) -> None:
    """Poll renderfin; publish the mesh, then either rig it or finish."""
    import render_prompting

    job_id = str(meta.get("job_id") or "")
    if not job_id:
        return
    try:
        job = await render_prompting.poll_character_gen(job_id)
    except Exception as exc:
        print(f"[Generation] poll failed for {task.id}: {exc}")
        return

    stage = str(job.get("stage") or "")
    glb_url = str(job.get("glb_url") or "").strip()

    if stage == "failed":
        task.status = "error"
        task.error_message = f"3D generation failed: {str(job.get('error') or '')[:200]}"
        set_generation_meta(task, stage=GEN_STAGE_FAILED)
        await refund_generation_credits(db, task, "generation failed")
        task.updated_at = datetime.utcnow()
        await db.commit()
        return

    if not glb_url:
        return  # still rendering; renderfin owns the retries and the deadline

    # The mesh exists. Show it now - the viewer falls back to this url until the
    # rigged artifacts replace it, which is what makes the task page fill in
    # progressively instead of staying blank until the very end.
    task.viewer_prepared_glb_url = glb_url
    set_generation_meta(task, glb_url=glb_url)

    if not meta.get("riggable"):
        task.status = "done"
        task.output_urls = [glb_url]
        task.ready_urls = [glb_url]
        task.ready_count = 1
        task.total_count = 1
        set_generation_meta(task, stage=GEN_STAGE_DONE)
        task.updated_at = datetime.utcnow()
        await db.commit()
        print(f"[Generation] task {task.id} finished as a model (not riggable)")
        return

    # Riggable: the same row becomes the convert task for the mesh we just made.
    # Everything downstream then treats it as an ordinary conversion.
    task.input_url = glb_url
    task.pipeline_kind = "convert"
    task.input_type = task.input_type or "t_pose"
    task.status = "created"
    task.worker_api = None
    task.worker_task_id = None
    task.processing_started_at = None
    task.source_attempt_count = 0
    task.source_next_retry_at = None
    set_generation_meta(task, stage=GEN_STAGE_RIGGING)
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(f"[Generation] task {task.id} generated {glb_url} -> queued for rigging")


async def pump_generation_tasks(db) -> None:
    """One tick of the generation pipeline. Safe to call from the worker loop."""
    result = await db.execute(
        select(Task).where(
            Task.pipeline_kind == "generate",
            Task.status.in_(("created", "processing")),
        )
    )
    for task in result.scalars().all():
        meta = generation_meta(task)
        stage = str(meta.get("stage") or "")
        try:
            if not meta or stage in ("", GEN_STAGE_DETECT):
                await _start_generation(db, task)
            elif stage == GEN_STAGE_MODEL:
                await _advance_generation(db, task, meta)
        except Exception as exc:
            print(f"[Generation] tick failed for {task.id}: {type(exc).__name__}: {exc}")
            await db.rollback()
