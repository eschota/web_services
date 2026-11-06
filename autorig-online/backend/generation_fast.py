"""Picture intake: a 3D model in seconds, then the full one (owner 2026-10-17, on a picture task waiting «V3 · Generating
the model · waiting for a 3D worker · 493 s»: «нельзя это на фаст мини перевести, чтобы 3д модель появлялась через 5
секунд, а потом просто улучшалась постепенно, но анализ можно делать и на лоуполи»; spec
docs/picture-intake-fast-preview-spec-20261017.md in the MT repo; contract V3.2 §2).

Every generation task (pipeline_kind generate) gets, beside its renderfin character job (Hunyuan 2.1, textured, minutes),
one gen3d job of tier fast (Hunyuan3D-2mini turbo on the Fast 3D boxes, deploy/fast3d, seconds, untextured). When that
mesh exists the row is bound to V3 with it at once (analysis, fastrig, clips run on the low-poly), meta
generation.fast records the timings, and generation.full_pending stays true; pump_upgrades() then follows the renderfin
job and, when the full model lands, binds it as the next attempt of the same task (the upgrade: a new rig version on
the full mesh; the preview's run and files stay).

    AUTORIG_PICTURE_FAST_TIER   on (default) | off       the fast tier for new and waiting picture tasks
    AUTORIG_PICTURE_UPGRADE     auto (default) | keep_preview   bind the full model when it lands, or keep the preview
    AUTORIG_PICTURE_FAST_MODEL  hunyuan_mini_turbo (default) | hunyuan_mini30
"""
from __future__ import annotations

import json
import os
import pathlib
import time
from datetime import datetime
from typing import Any, Dict, Optional

GEN3D = os.getenv("AUTORIG_GEN3D_QUEUE_URL", "http://127.0.0.1:8283")
TOKEN_FILE = pathlib.Path(os.getenv("AUTORIG_V3_DISPATCH_TOKEN_FILE", "/srv/autorig/secrets/v3-dispatch.token"))
FAST_BIND_GRACE_SECONDS = float(os.getenv("AUTORIG_PICTURE_FAST_DETECT_GRACE", "45"))


def _under_tests() -> bool:
    """A unittest / pytest process never queues real gen3d jobs (2026-10-17: a test run queued 16 jobs on the live
    queue; they were cancelled before any box took them). AUTORIG_PICTURE_FAST_TIER=force overrides."""
    import sys

    argv0 = str((sys.argv or [""])[0]).replace("\\", "/")
    return "unittest" in argv0 or "pytest" in argv0 or "autotests" in argv0


def enabled() -> bool:
    v = os.getenv("AUTORIG_PICTURE_FAST_TIER", "on").strip().lower()
    if v == "force":
        return True
    return v in ("1", "on", "true", "yes") and not _under_tests()


def upgrade_mode() -> str:
    m = os.getenv("AUTORIG_PICTURE_UPGRADE", "auto").strip().lower()
    return m if m in ("auto", "keep_preview") else "auto"


def _token() -> str:
    try:
        return TOKEN_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


async def request(task, image_url: str) -> Dict[str, Any]:
    """Queue the fast gen3d job for this picture -> the meta row {job, requested_at, image_url} or {error}."""
    import httpx

    t0 = time.time()
    body = {"image_url": image_url, "tier": "fast", "urgency": "interactive", "purpose": "prop",
            "model": os.getenv("AUTORIG_PICTURE_FAST_MODEL", "hunyuan_mini_turbo"),
            "label": f"picture {str(task.id)[:8]} fast preview", "run": str(task.id)}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{GEN3D}/api/gen3d/jobs", json=body, headers={"Authorization": f"Bearer {_token()}"})
        if r.status_code >= 400:
            return {"error": f"gen3d HTTP {r.status_code}: {r.text[:200]}", "requested_at": t0}
        j = r.json() or {}
        return {"job": j.get("id"), "requested_at": t0, "image_url": image_url, "model": j.get("model") or body["model"]}
    except Exception as exc:                                    # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"[:200], "requested_at": t0}


async def poll(job_id: str) -> Dict[str, Any]:
    import httpx

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{GEN3D}/api/gen3d/jobs/{job_id}")
    r.raise_for_status()
    return r.json() or {}


async def model_bytes(job_id: str) -> bytes:
    import httpx

    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.get(f"{GEN3D}/api/gen3d/jobs/{job_id}/model.glb")
    r.raise_for_status()
    return r.content


def public_glb(job_id: str) -> str:
    from config import APP_URL

    return f"{APP_URL}/api/gen3d/jobs/{job_id}/model.glb"


def _v3_stage(task, label: str, progress: float) -> None:
    try:
        settings = json.loads(getattr(task, "viewer_settings", None) or "{}")
        settings = settings if isinstance(settings, dict) else {}
    except (TypeError, ValueError):
        settings = {}
    v3 = dict(settings.get("v3") or {})
    v3.update(state="generating", stage=label, progress=round(max(progress, float(v3.get("progress") or 0)), 3),
              updated_at=datetime.utcnow().isoformat() + "Z")
    settings["v3"] = v3
    task.viewer_settings = json.dumps(settings)


async def advance(db, task, meta: Dict[str, Any], job_id: str, receipt_facts: Dict[str, Any]) -> bool:
    """One tick of the fast tier for a task whose full model is not there yet. True = the row is now bound to V3 with
    the fast mesh (the caller stops); False = go on with the normal path."""
    from generation_tasks import generation_meta, set_generation_meta

    if not enabled():
        return False
    fast = dict(meta.get("fast") or {})
    if not fast:                                                # waiting tasks (created before the fast tier) too
        image_url = str(meta.get("cutout_url") or task.input_url or "")
        if not image_url.startswith("https://"):
            return False
        fast = await request(task, image_url)
        set_generation_meta(task, fast=fast)
        await db.commit()
        return False
    if fast.get("error") or fast.get("bound_at") or not fast.get("job"):
        return False
    try:
        j = await poll(str(fast["job"]))
    except Exception as exc:                                    # noqa: BLE001
        fast["poll_error"] = f"{type(exc).__name__}: {exc}"[:160]
        set_generation_meta(task, fast=fast)
        await db.commit()
        return False
    status = str(j.get("status") or "")
    if status in ("failed", "cancelled"):
        fast.update(error=str(j.get("error") or status)[:200], failed_at=time.time())
        set_generation_meta(task, fast=fast)
        await db.commit()
        return False
    if status != "done":
        waited = time.time() - float(fast.get("requested_at") or time.time())
        stage = str(j.get("stage") or status)
        label = "generation:fast_preview " + ("fast worker busy" if waited > 10 and "busy" in stage else stage)[:80]
        if fast.get("stage") != stage:
            fast["stage"] = stage
            set_generation_meta(task, fast=fast)
        _v3_stage(task, label, .04)
        task.updated_at = datetime.utcnow()
        await db.commit()
        return False
    if not fast.get("done_at"):
        fast.update(done_at=time.time(), box=j.get("box"), model_seconds=j.get("model_seconds"),
                    faces=(j.get("stats") or {}).get("faces"), glb_url=public_glb(str(fast["job"])))
        set_generation_meta(task, fast=fast)
        task.viewer_prepared_glb_url = fast["glb_url"]          # the viewer shows the preview mesh at once
        await db.commit()
    # the alongside Vision detection gets a short grace to say «vehicle» (then the vehicle conveyor takes the task)
    meta = generation_meta(task)
    if isinstance(meta.get("vehicle_verdict"), dict) and meta.get("vehicle_verdict"):
        return False
    if not meta.get("detect_ok") and time.time() - float(fast.get("done_at") or 0) < FAST_BIND_GRACE_SECONDS:
        return False
    from v3_intake import bind_existing_task

    data = await model_bytes(str(fast["job"]))
    fast["bound_at"] = time.time()
    set_generation_meta(task, fast=fast, full_pending=True, stage="rigging", conveyor="v3", tier="fast")
    await bind_existing_task(db, task, data=data, origin="generation", requested_intent="generate",
                             filename=f"fast_{str(fast['job'])[:12]}.glb",
                             original={"url": task.input_url, "kind": "image"},
                             receipt_facts={**dict(receipt_facts), "glb_url": fast["glb_url"], "image_url": task.input_url,
                                            "tier": "fast", "gen3d_job": fast["job"], "model": fast.get("model")})
    print(f"[Generation] task {task.id} FAST preview {fast.get('model')} on {fast.get('box')} in "
          f"{round(fast['done_at'] - fast['requested_at'], 1)} s -> V3 (bound "
          f"{round(fast['bound_at'] - fast['requested_at'], 1)} s after the request); the full model follows")
    return True


async def _bind_upgrade(db, task, data: bytes, facts: Dict[str, Any]) -> Dict[str, Any]:
    """The full model as the next V3 attempt of the same task (as v3_textures' new source): the preview's attempt must
    be settled; it becomes superseded (its run and files stay), the new one is pending -> dispatch. One commit."""
    from sqlalchemy import text as _sql

    from v3_intake import build_binding, generation_receipt, glb_facts, store_source
    from v3_task_runtime import SETTLED_BINDING_STATES, SameTaskDbBindings

    bindings = SameTaskDbBindings()
    current = await bindings.latest(db, str(task.id))
    if current is None:
        raise ValueError("unbound")
    if current.state not in SETTLED_BINDING_STATES:
        return {"busy": True, "state": current.state}
    path, digest = store_source(data)
    if digest == current.source_sha256:
        return {"attempt": current.attempt, "same": True}
    manifest = dict(current.source_manifest or {})
    changed = await db.execute(_sql(
        "UPDATE v3_dispatch_task_bindings SET state='superseded',updated_at=CURRENT_TIMESTAMP "
        "WHERE task_id=:t AND attempt=:a AND state=:s"), {"t": task.id, "a": current.attempt, "s": current.state})
    if getattr(changed, "rowcount", 1) != 1:
        return {"busy": True, "state": "raced"}
    receipt = generation_receipt(str(task.id), digest, **{**dict(facts), "image_url": task.input_url,
                                                          "upgrade_of_attempt": current.attempt})
    binding = build_binding(str(task.id), path, digest, requested_intent=current.requested_intent,
                            origin=str(manifest.get("origin") or "generation"), filename=f"full_{digest[:12]}.glb",
                            original=manifest.get("original") or {}, facts=glb_facts(data), receipt=receipt,
                            attempt=current.attempt + 1)
    await bindings.bind_in_task_transaction(db, binding)
    try:
        settings = json.loads(task.viewer_settings or "{}")
        settings = settings if isinstance(settings, dict) else {}
    except (TypeError, ValueError):
        settings = {}
    v3 = dict(settings.get("v3") or {})
    intake = dict(v3.get("intake") or {})
    intake.update(source_sha256=digest, upgrade_of_attempt=current.attempt, tier="full")
    v3.update(state="pending", stage="dispatch", progress=0.0, attempt=current.attempt + 1, error=None, intake=intake,
              upgrade={"from_attempt": current.attempt, "at": datetime.utcnow().isoformat() + "Z"},
              updated_at=datetime.utcnow().isoformat() + "Z")
    settings["v3"] = v3
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.status = "created"
    task.error_message = None
    task.updated_at = datetime.utcnow()
    return {"attempt": current.attempt + 1, "source_sha256": digest}


async def pump_upgrades(db) -> None:
    """The tasks bound with the fast preview: follow the renderfin job; when the full model lands, bind it as the next
    attempt (AUTORIG_PICTURE_UPGRADE=auto) or keep the preview (keep_preview: the full GLB is recorded only)."""
    from sqlalchemy import select

    import render_prompting
    from database import Task
    from generation_tasks import generation_meta, set_generation_meta

    rows = (await db.execute(select(Task).where(Task.pipeline_kind == "v3",
                                                Task.viewer_settings.like('%"full_pending": true%')))).scalars().all()
    for task in rows:
        meta = generation_meta(task)
        if not meta.get("full_pending"):
            continue
        job_id = str(meta.get("job_id") or "")
        try:
            job = await render_prompting.poll_character_gen(job_id)
        except Exception as exc:                                # noqa: BLE001
            print(f"[Generation] upgrade poll failed for {task.id}: {exc}")
            continue
        stage = str(job.get("stage") or "")
        glb_url = str(job.get("glb_url") or "").strip()
        if stage == "failed" and not glb_url:
            set_generation_meta(task, full_pending=False, full_error=str(job.get("error") or "failed")[:200],
                                full_failed_at=time.time())
            await db.commit()
            print(f"[Generation] task {task.id}: the full model failed; the fast preview stays")
            continue
        if not glb_url:
            continue
        now = time.time()
        if upgrade_mode() == "keep_preview":
            set_generation_meta(task, full_pending=False, full_glb_url=glb_url, full_at=now, upgrade="kept_preview")
            await db.commit()
            continue
        try:
            from v3_intake import fetch_bytes

            data = await fetch_bytes(glb_url)
            out = await _bind_upgrade(db, task, data, {"renderfin_job_id": job_id, "glb_url": glb_url, "tier": "full"})
            if out.get("busy"):
                continue                                        # the preview's attempt is still running: next tick
            fast = meta.get("fast") or {}
            set_generation_meta(task, full_pending=False, full_glb_url=glb_url, full_at=now, upgrade="bound",
                                upgrade_attempt=out.get("attempt"), tier="full")
            task.viewer_prepared_glb_url = glb_url
            await db.commit()
            print(f"[Generation] task {task.id} UPGRADE: the full model {round(now - float(fast.get('requested_at') or now))} s "
                  f"after the fast request -> V3 attempt {out.get('attempt')}")
        except Exception as exc:                                # noqa: BLE001
            await db.rollback()
            print(f"[Generation] upgrade of {task.id} failed (retrying): {type(exc).__name__}: {exc}")
