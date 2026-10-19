"""Image -> 3D model -> rigged character, as one task in the normal task list.

A generation task is an ordinary row in ``tasks`` with ``pipeline_kind`` set to
``generate`` and ``input_url`` pointing at the picture the user uploaded. It is
deliberately not a separate entity: once the model exists the same row turns
into the convert task that rigs it, so every existing path - the dispatcher, the
progress page, the viewer, the downloads, the gallery - keeps working without
knowing that generation happened at all.

Route decision (per stage):

    picture -> isolate (cutout) + dispatch at once, detect alongside -> generation + rig

Picture intake (owner 2026-10-16: the viewer opens at once with the picture, the picture is isolated and goes to
3D generation): the subject is cut out first (picture_isolate, seconds, on the fleet's mask service), the renderfin
job starts at once on the cutout (the original when the cutout is late or failed) and the Vision detection runs
alongside; it never blocks the job. A vehicle verdict arriving later routes the task to the vehicle conveyor and
discards the character job. V3 rigs any pose, so every generated character is rigged
(AUTORIG_GENERATION_RIG_ALL=off restores the old "not riggable -> bare model" branch).

Generation itself runs in renderfin, which already owns the Hunyuan worker pool,
its slot accounting and its stage retries. Doing it here would mean a second
queue pointed at the same GPUs, and two queues cannot keep each other from
overfilling the farm.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
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

# How long a task waits for the detection to become answerable before it starts
# anyway. Detection routes the job; when it cannot run, the honest answer is
# "not known yet", not "not riggable" - reading an outage as a verdict sent 50
# uploads down the model-only route in five minutes on 2026-08-09, each ending
# as a bare mesh with no rig and no error the owner could see.
#
# The wait is bounded because a permanently broken vision must not hold a paid
# job forever. Past the bound we route as riggable: the user paid for a rigged
# character, and if the mesh truly cannot take a rig the rig step says so and
# the model is still delivered - which is the safe direction to be wrong in.
DETECT_RETRY_SECONDS = float(os.getenv("AUTORIG_GENERATION_DETECT_RETRY", "300"))
DETECT_WAIT_CEILING_SECONDS = float(
    os.getenv("AUTORIG_GENERATION_DETECT_CEILING", "21600")  # 6 hours
)

# picture-intake 2026-10-16
GEN_STAGE_ISOLATE = "isolate"      # the cutout is being made (1-3 s on a warm mask box)
# How long the dispatch waits for the cutout before it hands Hunyuan the original (the isolation goes on and the viewer
# still gets the cutout when it lands). Target: the job starts < 10 s after the upload.
ISOLATE_WAIT_SECONDS = float(os.getenv("AUTORIG_GENERATION_ISOLATE_WAIT", "12"))   # 7 -> 12 (2026-10-19: f13 8 s)
DISPATCH_RETRY_SECONDS = float(os.getenv("AUTORIG_GENERATION_DISPATCH_RETRY", "30"))
# The mesh waits this long for the (alongside) detection before the V3 binding takes it; a vehicle verdict that
# arrives in time still routes the task to the vehicle conveyor.
DETECT_BIND_GRACE_SECONDS = float(os.getenv("AUTORIG_GENERATION_DETECT_BIND_GRACE", "180"))
ROUTE_RETRY_SECONDS = 20.0

_STARTING: set = set()                           # task ids whose start (isolate + dispatch) is running here
_DETECT: Dict[str, "asyncio.Task"] = {}          # task id -> the running Vision detection
_BG: set = set()                                 # strong references to the background tasks


def rig_all() -> bool:
    """V3 rigs any pose: every generated character is rigged (AUTORIG_GENERATION_RIG_ALL=off: the old verdict)."""
    return os.getenv("AUTORIG_GENERATION_RIG_ALL", "on").strip().lower() not in ("off", "0", "false", "no")


def _spawn(coro) -> "asyncio.Task":
    task = asyncio.create_task(coro)
    _BG.add(task)
    task.add_done_callback(_BG.discard)
    return task


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


def _merge_viewer_setting(task: Task, key: str, value: Any) -> None:
    """Write one key into viewer_settings without disturbing the rest."""
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
        if not isinstance(settings, dict):
            settings = {}
    except (TypeError, ValueError):
        settings = {}
    settings[key] = value
    task.viewer_settings = json.dumps(settings)


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


def kick_start(task_id: str) -> bool:
    """Start the task's isolate + dispatch in the background (own DB session) so no tick or request waits for it.
    False when it is already running here."""
    if task_id in _STARTING:
        return False
    _STARTING.add(task_id)
    _spawn(_start_in_session(task_id))
    return True


async def _start_in_session(task_id: str) -> None:
    try:
        from database import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
            task = await db.get(Task, task_id)
            if task is None or task.pipeline_kind != "generate" or task.status not in ("created", "processing"):
                return
            if str(generation_meta(task).get("stage") or "") not in ("", GEN_STAGE_DETECT, GEN_STAGE_ISOLATE):
                return
            await _start_generation(db, task)
    except Exception as exc:                                   # noqa: BLE001 - the next tick tries again
        print(f"[Generation] start failed for {task_id}: {type(exc).__name__}: {exc}")
    finally:
        _STARTING.discard(task_id)


def _local_source_bytes(url: str) -> Optional[bytes]:
    try:
        import picture_isolate

        token = picture_isolate.token_of(url)
        path = picture_isolate.source_path(token) if token else None
        return path.read_bytes() if path else None
    except Exception:                                          # noqa: BLE001
        return None


async def _detect_picture(url: str) -> Dict[str, Any]:
    """The Vision detection of the original picture; always a verdict dict (vision_ok False on any failure)."""
    import content_moderation

    data = _local_source_bytes(url)
    if data is None:
        data = await _fetch_image_bytes(url)
    if data is None:
        return {"riggable": False, "vision_ok": False, "reason": "source image could not be read for detection"}
    try:
        return await asyncio.to_thread(content_moderation.detect_character_for_generation, data)
    except Exception as exc:                                   # noqa: BLE001
        print(f"[Generation] detection failed: {exc}")
        return {"riggable": False, "vision_ok": False, "reason": f"detection failed: {exc}"[:200]}


def _start_detect(task: Task) -> None:
    _DETECT[str(task.id)] = _spawn(_detect_picture(str(task.input_url)))
    set_generation_meta(task, detect_started_at=time.time())


async def _discard_job(job_id: str) -> None:
    """Drop the character job of a task that went to the vehicle conveyor (renderfin cancels its GPU work)."""
    if str(job_id or "").startswith("tripo:"):
        return                                             # a Tripo task runs out on its own (it is paid already)
    import httpx
    import render_prompting

    if not job_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(f"{render_prompting.RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/discard")
    except Exception as exc:                                   # noqa: BLE001 - the result is ignored anyway
        print(f"[Generation] discard of job {job_id} failed (ignored): {exc}")


async def _start_generation(db, task: Task) -> None:
    """Cut the subject out, hand the picture to renderfin at once, run the Vision detection alongside."""
    import render_prompting
    import picture_isolate
    import vehicle_intake
    from config import APP_URL

    meta_now = generation_meta(task)
    if meta_now.get("job_id"):
        # already dispatched (a vehicle service outage reset the stage meanwhile): never a second job
        set_generation_meta(task, stage=GEN_STAGE_MODEL)
        await db.commit()
        return
    # A vehicle routing that could not reach the service is retried on its own spacing, as before.
    last_try = float(meta_now.get("detect_last_attempt") or 0)
    if last_try and (time.time() - last_try) < DETECT_RETRY_SECONDS:
        return
    last_dispatch = float(meta_now.get("dispatch_last_attempt") or 0)
    if last_dispatch and (time.time() - last_dispatch) < DISPATCH_RETRY_SECONDS:
        return

    # Vehicles V3 (owner 2026-10-15): a picture the uploader marked as a vehicle goes to the vehicle conveyor at once
    if await vehicle_intake.maybe_route(db, task, None):
        return

    # Isolate: the cutout (1-3 s warm) feeds Hunyuan; late or failed, the original goes and the cutout still lands for
    # the viewer. Never raises: a failure is the report's `error`, the generation goes on.
    token = picture_isolate.token_of(str(task.input_url))
    cutout_url = None
    iso: Dict[str, Any] = {}
    if token:
        try:
            iso = await picture_isolate.ensure(token, ISOLATE_WAIT_SECONDS) or {}
        except Exception as exc:                               # noqa: BLE001
            iso = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
        if iso.get("ok") and picture_isolate.cutout_ready(token):
            cutout_url = f"{APP_URL}/u/{token}/cutout.png"
    # Picture normalisation (rig contract V3.2 §3): the stage starts now, beside the job; its redrawn front / cleaned
    # picture is used when it is already decided (a retry), otherwise it replaces the queued job later.
    canon_in = None
    try:
        import picture_canon_bridge

        picture_canon_bridge.kick(token, str(task.id))
        canon_in = picture_canon_bridge.generation_input(token, APP_URL)
    except Exception as exc:                                   # noqa: BLE001 - the intake goes on without it
        print(f"[Generation] picture canon unavailable for {task.id}: {type(exc).__name__}: {exc}")
    image_url = (canon_in or {}).get("url") or cutout_url or str(task.input_url)

    # Tripo for the owner's own tasks (owner 2026-10-19: «если задачу делаю я — отправляй сразу в Tripo на качественную
    # генерацию»): generation_tripo decides by the task owner and the policy; a refusal falls back to renderfin.
    job_id, provider = "", "renderfin"
    try:
        import generation_tripo

        if generation_tripo.enabled_for(task):
            job_id = await generation_tripo.start(image_url, str(task.id))
            provider = "tripo"
    except Exception as exc:                                   # noqa: BLE001 - renderfin takes it
        print(f"[Generation] Tripo refused {task.id}: {type(exc).__name__}: {str(exc)[:200]} -> renderfin")
    try:
        if not job_id:
            job_id = await render_prompting.start_character_gen_from_image(
                image_url, source_task_id=task.id, user_name="site"
            )
    except Exception as exc:
        # Renderfin parks rather than fails when the farm is merely busy, so a
        # failure here is a real one; leave the task retryable instead of dead.
        print(f"[Generation] renderfin refused {task.id}: {exc}")
        task.error_message = f"Generation could not start: {str(exc)[:200]}"
        set_generation_meta(task, dispatch_last_attempt=time.time())
        task.updated_at = datetime.utcnow()
        await db.commit()
        return

    now = time.time()
    set_generation_meta(
        task,
        stage=GEN_STAGE_MODEL,
        job_id=job_id,
        dispatched_at=now,
        sub_key="waiting_3d_worker",
        sub_started_at=now,
        provider=provider,
        # V3 rigs any pose: the verdict (when it comes) only gives the category hints.
        riggable=True if rig_all() else None,
        cutout_url=cutout_url,
        dispatch_input=("picture_canon:" + canon_in["kind"]) if canon_in else ("cutout" if cutout_url else "original"),
        canon_redispatched=True if canon_in else None,
        canon_input_url=(canon_in or {}).get("url"),
        isolate_ok=bool(iso.get("ok")) if iso else None,
        isolate_error=(str(iso.get("error"))[:200] if iso and iso.get("error") else None),
        isolate_seconds=iso.get("seconds") if iso else None,
        isolate_box=iso.get("box") if iso else None,
    )
    _start_detect(task)
    # the fast tier (owner 2026-10-17: «чтобы 3д модель появлялась через 5 секунд»): Hunyuan3D-2mini beside the job.
    # Not beside a Tripo job (owner 2026-10-19: «мои модели делать Tripo платным, а не бесплатным, почему оно хуньянь
    # опять запустило?»): AUTORIG_PICTURE_TRIPO_FAST=on brings the preview back for those tasks.
    try:
        import generation_fast

        fast_wanted = provider != "tripo" or os.getenv("AUTORIG_PICTURE_TRIPO_FAST", "off").strip().lower() in ("on", "1", "true", "yes")
        fast_url = image_url
        if token and not cutout_url and not canon_in and picture_isolate.cutout_ready(token):
            fast_url = f"{APP_URL}/u/{token}/cutout.png"            # the cutout landed meanwhile (2026-10-19)
        if fast_wanted and generation_fast.enabled() and fast_url.startswith("https://"):
            set_generation_meta(task, fast=await generation_fast.request(task, fast_url))
    except Exception as exc:                                    # noqa: BLE001 - the full job is already running
        print(f"[Generation] fast tier request failed for {task.id}: {type(exc).__name__}: {exc}")
    task.error_message = None
    task.status = "processing"
    task.processing_started_at = task.processing_started_at or datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(
        f"[Generation] task {task.id} -> {provider} job {job_id} "
        f"({'cutout' if cutout_url else 'original'}"
        f"{'' if not iso else ', isolate ' + str(iso.get('seconds')) + 's on ' + str(iso.get('box'))}); detection alongside"
    )


async def _route_vehicle(db, task: Task, verdict: Dict[str, Any]) -> str:
    """A vehicle verdict arrived after the character job started. 'routed' when the vehicle conveyor took the task
    (the character job is discarded), 'continue' otherwise (the vehicle service is down: retried on its own spacing)."""
    import vehicle_intake

    job_id = str(generation_meta(task).get("job_id") or "")
    set_generation_meta(task, route_last_attempt=time.time())
    routed = await vehicle_intake.maybe_route(db, task, verdict)
    if routed and str(getattr(task, "pipeline_kind", "") or "") != "generate":
        _DETECT.pop(str(task.id), None)
        await _discard_job(job_id)
        return "routed"
    if routed:
        # the service refused and reset the stage to `detect`: the character job goes on, the routing is retried
        set_generation_meta(task, stage=GEN_STAGE_MODEL, route_last_attempt=time.time())
        await db.commit()
        return "continue"
    set_generation_meta(task, vehicle_verdict={})            # vehicle intake is off: nothing to route
    await db.commit()
    return "continue"


async def _harvest_detect(db, task: Task, meta: Dict[str, Any]) -> str:
    """Collect the alongside Vision detection into the meta. 'routed' = the task left for the vehicle conveyor."""
    import vehicle_intake

    pending = meta.get("vehicle_verdict")
    has_pending = isinstance(pending, dict) and bool(pending)
    if meta.get("detect_ok") and not has_pending:
        return "ok"
    now = time.time()
    if has_pending:
        if now - float(meta.get("route_last_attempt") or 0) < ROUTE_RETRY_SECONDS:
            return "continue"
        return await _route_vehicle(db, task, pending)
    fut = _DETECT.get(str(task.id))
    if fut is None:
        if now - float(meta.get("detect_last_attempt") or 0) >= DETECT_RETRY_SECONDS:
            _start_detect(task)                                 # after a restart, or the last answer was an outage
            await db.commit()
        return "continue"
    if not fut.done():
        return "continue"
    _DETECT.pop(str(task.id), None)
    try:
        verdict = fut.result()
    except Exception as exc:                                    # noqa: BLE001
        verdict = {"riggable": False, "vision_ok": False, "reason": f"detection failed: {exc}"[:200]}
    if not verdict.get("vision_ok"):
        # We could not ask, which is not the same as being told "no": the job is running anyway, ask again later.
        set_generation_meta(task, detect_last_attempt=now, detect_reason=str(verdict.get("reason") or "")[:200])
        await db.commit()
        return "continue"
    started = float(meta.get("detect_started_at") or now)
    updates: Dict[str, Any] = dict(
        detect_ok=True, detect_seconds=round(now - started, 1), subject=verdict.get("subject") or "",
        pose=verdict.get("pose") or "", animal_type=verdict.get("animal_type") or "",
        detect_reason=verdict.get("reason") or "")
    if not rig_all():
        updates["riggable"] = bool(verdict.get("riggable"))
    set_generation_meta(task, **updates)
    if vehicle_intake.is_vehicle(verdict):
        set_generation_meta(task, vehicle_verdict={
            "vision_ok": True, "is_character": verdict.get("is_character"),
            "subject": str(verdict.get("subject") or "")[:200], "reason": str(verdict.get("reason") or "")[:200]})
        await db.commit()
        return await _route_vehicle(db, task, verdict)
    await db.commit()
    return "ok"


def _v3_generation_stage(job: Dict[str, Any]) -> tuple:
    """Renderfin's job state as a live V3 stage and progress (the generation fills 0-30 %)."""
    import math

    stage = str(job.get("stage") or "")
    if stage == "hunyuan" and job.get("provider") == "tripo":   # the owner's tasks (generation_tripo)
        started = float(job.get("stage_started_at") or 0) or time.time()
        return "generation:tripo", .05 + .2 * (1 - math.exp(-(time.time() - started) / 90))
    if stage == "hunyuan":
        worker = str(job.get("hunyuan_worker") or "")
        if worker and job.get("hunyuan_task_id"):
            started = float(job.get("stage_started_at") or 0) or time.time()
            return f"generation:hunyuan {worker}", .05 + .2 * (1 - math.exp(-(time.time() - started) / 600))
        return "generation:waiting_3d_worker", .03
    if stage == "turntable":
        return "generation:turntable", .27
    if stage in ("ready", "submitted"):
        return "generation:ready", .29
    return "generation:detect", .02


def _sub_key(job: Dict[str, Any]) -> str:
    """The task page's sub stage of a running generation job (v3-view `sub_key`)."""
    label = _v3_generation_stage(job)[0]
    key = label.split(":", 1)[1].split()[0] if ":" in label else "waiting_3d_worker"
    return "waiting_3d_worker" if key == "detect" else key


def _note_v3_generation(task: Task, job: Dict[str, Any]) -> bool:
    """Keep a V3-routed generation row's live stage in viewer_settings.v3; True when it changed."""
    label, progress = _v3_generation_stage(job)
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
        if not isinstance(settings, dict):
            settings = {}
    except (TypeError, ValueError):
        settings = {}
    v3 = dict(settings.get("v3") or {})
    if v3.get("stage") == label and abs(float(v3.get("progress") or 0) - progress) < .005:
        return False
    v3.update(state="generating", stage=label, progress=round(max(progress, float(v3.get("progress") or 0)), 3),
              updated_at=datetime.utcnow().isoformat() + "Z")
    settings["v3"] = v3
    task.viewer_settings = json.dumps(settings)
    return True


async def _canon_redispatch(db, task: Task, meta: Dict[str, Any], job: Dict[str, Any]) -> bool:
    """The picture normalisation decided on another input (a redrawn front, the hands emptied) while the full job still
    waits for a 3D worker: the job is replaced once by one on that picture. A job already on a worker is kept; the
    decided picture is recorded (`canon_after_start`) for the upgrade path. True when the job was replaced."""
    import picture_canon_bridge
    import picture_isolate
    import render_prompting
    from config import APP_URL

    if meta.get("canon_redispatched") or not picture_canon_bridge.redispatch_enabled():
        return False
    token = picture_isolate.token_of(str(task.input_url))
    gi = picture_canon_bridge.generation_input(token, APP_URL)
    if not gi:
        return False
    if job.get("hunyuan_worker") and job.get("hunyuan_task_id"):
        if not meta.get("canon_after_start"):
            set_generation_meta(task, canon_after_start=gi["url"], canon_kind=gi["kind"])
            await db.commit()
        return False
    old = str(meta.get("job_id") or "")
    try:
        new = await render_prompting.start_character_gen_from_image(gi["url"], source_task_id=task.id, user_name="site")
    except Exception as exc:                                    # noqa: BLE001 - the old job goes on
        print(f"[Generation] picture canon re-dispatch refused for {task.id}: {exc}")
        set_generation_meta(task, canon_redispatched=True, canon_error=str(exc)[:200])
        await db.commit()
        return False
    await _discard_job(old)
    now = time.time()
    prev = list(meta.get("previous_job_ids") or []) + ([old] if old else [])
    set_generation_meta(task, job_id=new, previous_job_ids=prev, canon_redispatched=True,
                        dispatch_input="picture_canon:" + gi["kind"], canon_input_url=gi["url"], canon_kind=gi["kind"],
                        sub_key="waiting_3d_worker", sub_started_at=now)
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(f"[Generation] task {task.id}: the waiting job {old} replaced by {new} on the picture canon {gi['kind']}")
    return True


async def _advance_generation(db, task: Task, meta: Dict[str, Any]) -> None:
    """Poll renderfin; publish the mesh, then either rig it or finish."""
    import render_prompting
    from config import is_admin_email
    from v3_intake import route_enabled

    v3_route = route_enabled("generation", is_admin=task.owner_type == "user" and is_admin_email(task.owner_id),
                             explicit=bool(meta.get("v3")))

    job_id = str(meta.get("job_id") or "")
    if not job_id:
        return
    # A task that began before the picture intake has no cutout yet: make it for the viewer (idempotent, cheap).
    try:
        import picture_isolate

        _tok = picture_isolate.token_of(str(task.input_url))
        if _tok and not picture_isolate.cutout_ready(_tok):
            picture_isolate.kick(_tok)
        if _tok and picture_isolate.cutout_ready(_tok):
            import picture_canon_bridge

            picture_canon_bridge.kick(_tok, str(task.id))      # the picture normalisation (idempotent)
    except Exception:                                           # noqa: BLE001
        pass
    # The Vision detection runs alongside the job: collect it; a vehicle verdict re-routes the task.
    if await _harvest_detect(db, task, meta) == "routed":
        return
    meta = generation_meta(task)
    try:
        job = await render_prompting.poll_character_gen(job_id)
    except Exception as exc:
        print(f"[Generation] poll failed for {task.id}: {exc}")
        return

    stage = str(job.get("stage") or "")
    glb_url = str(job.get("glb_url") or "").strip()
    changed = bool(v3_route and stage != "failed" and _note_v3_generation(task, job))
    sub = "ready" if glb_url else _sub_key(job)
    if stage != "failed" and meta.get("sub_key") != sub:
        set_generation_meta(task, sub_key=sub, sub_started_at=time.time())
        changed = True
    if changed and not glb_url:
        task.updated_at = datetime.utcnow()
        await db.commit()               # the task card shows the generation's own stage, never a frozen 0 %

    if not glb_url and stage != "failed" and await _canon_redispatch(db, task, meta, job):
        return

    if stage == "failed":
        task.status = "error"
        task.error_message = f"3D generation failed: {str(job.get('error') or '')[:200]}"
        set_generation_meta(task, stage=GEN_STAGE_FAILED)
        await refund_generation_credits(db, task, "generation failed")
        task.updated_at = datetime.utcnow()
        await db.commit()
        return

    if not glb_url and v3_route:
        # the fast preview (generation_fast): bound to V3 with the low-poly mesh while the full model renders
        import generation_fast

        if await generation_fast.advance(db, task, meta, job_id, {
                "renderfin_job_id": job_id, "subject": str(meta.get("subject") or "")[:200],
                "pose": str(meta.get("pose") or "")[:120], "animal_type": str(meta.get("animal_type") or ""),
                "detect_reason": str(meta.get("detect_reason") or "")[:200]}):
            return

    if not glb_url:
        return  # still rendering; renderfin owns the retries and the deadline

    # The mesh exists. Show it now - the viewer falls back to this url until the
    # rigged artifacts replace it, which is what makes the task page fill in
    # progressively instead of staying blank until the very end.
    task.viewer_prepared_glb_url = glb_url
    set_generation_meta(task, glb_url=glb_url)

    # The alongside detection gets a bounded grace to say "vehicle" before the mesh is bound as a character; a
    # pending vehicle routing (the vehicle service was down) is waited for.
    meta = generation_meta(task)
    seen = float(meta.get("glb_seen_at") or 0)
    if not seen:
        seen = time.time()
        set_generation_meta(task, glb_seen_at=seen)
    pending_route = isinstance(meta.get("vehicle_verdict"), dict) and bool(meta.get("vehicle_verdict"))
    if pending_route or (not meta.get("detect_ok") and time.time() - seen < DETECT_BIND_GRACE_SECONDS):
        await db.commit()
        return

    if not meta.get("riggable") and not rig_all():
        task.status = "done"
        task.output_urls = [glb_url]
        task.ready_urls = [glb_url]
        task.ready_count = 1
        task.total_count = 1
        set_generation_meta(task, stage=GEN_STAGE_DONE)
        task.updated_at = datetime.utcnow()
        from artifact_cache import enqueue_artifact_cache

        await enqueue_artifact_cache(db, task)
        await db.commit()
        print(f"[Generation] task {task.id} finished as a model (not riggable)")
        return

    # V3 conveyor (2026-10-10): the generated mesh re-enters the same V3
    # conveyor as an uploaded one, bound to this row with a generation receipt.
    # A failed binding is retried on the next tick; it never becomes a legacy rig.
    from v3_intake import bind_generated_task

    if v3_route:
        set_generation_meta(task, stage=GEN_STAGE_RIGGING, conveyor="v3")
        try:
            await bind_generated_task(db, task, glb_url=glb_url, receipt_facts={
                "renderfin_job_id": job_id, "subject": str(meta.get("subject") or "")[:200],
                "pose": str(meta.get("pose") or "")[:120], "animal_type": str(meta.get("animal_type") or ""),
                "detect_reason": str(meta.get("detect_reason") or "")[:200]})
        except Exception as exc:
            await db.rollback()
            print(f"[Generation] task {task.id} V3 binding failed (retrying): {type(exc).__name__}: {exc}")
            return
        print(f"[Generation] task {task.id} generated {glb_url} -> V3 conveyor")
        return

    # Riggable: the same row becomes the convert task for the mesh we just made.
    # Everything downstream then treats it as an ordinary conversion.
    task.input_url = glb_url
    task.pipeline_kind = "convert"
    animal_type = str(meta.get("animal_type") or "").strip().lower()
    if animal_type:
        # A quadruped needs the animal skeleton, not the humanoid one. The
        # dispatcher refuses an animal task that cannot name its body type, so
        # the detection is recorded in the shape it reads - marked accepted,
        # because a picture the model already identified is as decided as a
        # human picking from the dropdown.
        task.input_type = "animal"
        _merge_viewer_setting(
            task,
            "rig_v2_animal_detection",
            {
                "animal_type": animal_type,
                "accepted": True,
                "source": "generation_vision",
                "subject": meta.get("subject") or "",
            },
        )
    else:
        task.input_type = "t_pose"
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
            if not meta or stage in ("", GEN_STAGE_DETECT, GEN_STAGE_ISOLATE):
                kick_start(str(task.id))        # isolate + dispatch run in the background, the tick never waits
            elif stage == GEN_STAGE_MODEL:
                await _advance_generation(db, task, meta)
        except Exception as exc:
            print(f"[Generation] tick failed for {task.id}: {type(exc).__name__}: {exc}")
            await db.rollback()
    # the fast tier's upgrades: the full model of a task bound with its preview (generation_fast.pump_upgrades)
    try:
        import generation_fast

        await generation_fast.pump_upgrades(db)
    except Exception as exc:                                    # noqa: BLE001
        print(f"[Generation] upgrade pump failed: {type(exc).__name__}: {exc}")
        await db.rollback()
    # Vehicles V3: the vehicle runs' stages mirrored into their task rows (the task page reads only the row)
    try:
        import vehicle_intake

        await vehicle_intake.sync_vehicle_tasks(db)
    except Exception as exc:                                     # noqa: BLE001
        print(f"[Vehicle intake] sync failed: {type(exc).__name__}: {exc}")
        await db.rollback()
