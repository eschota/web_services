"""Vehicles V3 at intake (owner 2026-10-15: «нужно чтобы авториг умел ригать и коней и тачки»; a vehicle is never
humanoid; «до принятия решения о моделинге»).

A picture whose subject is a vehicle does not go to the character generation (renderfin T-pose -> Hunyuan3D) and never
reaches the humanoid rig. Right after the generation detection (generation_tasks._start_generation) the task becomes a
V3 task bound to a run of the vehicle conveyor (autorig-vehicle, MT repo mt/vehicle_v3_service.py, 127.0.0.1:8284):
segmentation, the wheels cut off, the split into body / suspension / wheel, the isolated render of every part, then the
run waits for the person's modelling decision. The task page opens that run in the viewer, whose «Запчасти» panel
(vehicle-parts.js) shows the parts for validation.

* Vehicle = the detection says it is not a character and its subject names a vehicle (VEHICLE_WORDS), or the
  uploader said so (generation meta ``category: vehicle``).
* Live switch ``/srv/autorig/live/config/vehicle-intake.json`` (no restart): ``{"enabled": true, "autorun": false}``.
  ``autorun: false`` makes the run without starting its stages (``hold``): an operator or an agent runs them while the
  conveyor is validated; ``true`` starts them at once. Missing file: enabled, no autorun.
* The Task row: ``pipeline_kind = "v3"``, ``status = "processing"`` (the vehicle is not finished before the modelling
  decision, and a processing task is never re-rigged on open), ``viewer_settings.v3`` with the session of the vehicle
  run; :func:`sync_vehicle_tasks` mirrors the run's stage into ``v3`` every worker tick (``needs_review`` once the parts
  wait for the person).
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

VEHICLE_URL = os.getenv("AUTORIG_VEHICLE_SERVICE_URL", "http://127.0.0.1:8284")
TOKEN_FILE = Path(os.getenv("AUTORIG_V3_DISPATCH_TOKEN_FILE", "/srv/autorig/secrets/v3-dispatch.token"))
CONFIG_FILE = Path(os.getenv("AUTORIG_VEHICLE_INTAKE_CONFIG", "/srv/autorig/live/config/vehicle-intake.json"))
MT_ROOT = Path(os.getenv("MT_ROOT", "/srv/autorig/data/motion_transfer"))
UNITY_PAGE = "/api/mt/unity/test/index.html"
_RUN = re.compile(r"^[0-9a-f]{20}$")
VEHICLE_WORDS = re.compile(
    r"\b(cars?|automobiles?|vehicles?|sedans?|saloons?|station wagons?|wagons?|hatchbacks?|coupes?|convertibles?|"
    r"pick-?ups?|pickup trucks?|trucks?|lorr(?:y|ies)|vans?|minivans?|minibus(?:es)?|bus(?:es)?|coach(?:es)?|suvs?|"
    r"jeeps?|motorhomes?|rvs?|campers?|camper vans?|caravans?|trailers?|tractors?|semi-?trucks?|tow trucks?|"
    r"wreckers?|limousines?|limos?|taxis?|cabs?|ambulances?|fire ?trucks?|fire engines?|buggies|buggy|go-?karts?|"
    r"race ?cars?|hot rods?|monster trucks?|forklifts?|bulldozers?|excavators?|dump trucks?|cement mixers?|"
    r"roadsters?|muscle cars?|sports cars?|police cars?|school bus(?:es)?)\b", re.I)
STAGE_PROGRESS = {"queued": .04, "hold": .05, "canon": .1, "cluster": .25, "masks_confirm": .35, "cut": .45,
                  "split": .6, "isolate": .75, "model_decision": 1.0}


def config() -> Dict[str, Any]:
    try:
        doc = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (OSError, ValueError):
        return {}


def enabled() -> bool:
    return bool(config().get("enabled", True))


def is_vehicle(verdict: Optional[Dict[str, Any]]) -> bool:
    """The detection (content_moderation.detect_character_for_generation) names a vehicle and no character."""
    if not isinstance(verdict, dict) or not verdict.get("vision_ok"):
        return False
    if verdict.get("is_character") is True:
        return False
    return bool(VEHICLE_WORDS.search(str(verdict.get("subject") or "")))


def _settings(task) -> Dict[str, Any]:
    try:
        s = json.loads(getattr(task, "viewer_settings", None) or "{}")
        return s if isinstance(s, dict) else {}
    except (TypeError, ValueError):
        return {}


async def _create_run(task, what: str, hold: bool) -> Dict[str, Any]:
    import httpx

    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    body = {"image_url": str(task.input_url), "task_id": str(task.id), "what": what[:200],
            "owner": f"{task.owner_type}:{task.owner_id}", "hold": bool(hold)}
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{VEHICLE_URL}/api/mt/vehicle/runs", json=body,
                              headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        return r.json()


async def maybe_route(db, task, verdict: Optional[Dict[str, Any]] = None) -> bool:
    """True when the task went to the vehicle conveyor (the caller stops); False leaves the generation as it was."""
    from generation_tasks import generation_meta, refund_generation_credits, set_generation_meta

    if not enabled():
        return False
    meta = generation_meta(task)
    hinted = str(meta.get("category") or "").strip().lower() == "vehicle"
    if not hinted and not is_vehicle(verdict):
        return False
    what = str((verdict or {}).get("subject") or meta.get("subject") or "vehicle")
    hold = not bool(config().get("autorun", False))
    try:
        doc = await _create_run(task, what, hold)
    except Exception as exc:                                     # noqa: BLE001 - the service is down: retry next tick
        print(f"[Vehicle intake] {task.id}: the vehicle service refused ({type(exc).__name__}: {exc}); retrying")
        set_generation_meta(task, stage="detect", vehicle_error=str(exc)[:200], detect_last_attempt=time.time())
        task.updated_at = datetime.utcnow()
        await db.commit()
        return True
    run = str(doc.get("run") or "")
    if not _RUN.fullmatch(run):
        print(f"[Vehicle intake] {task.id}: no run in the answer {str(doc)[:200]}")
        return False
    now = datetime.utcnow()
    set_generation_meta(task, stage="vehicle", vehicle_run=run, subject=what,
                        detect_reason=str((verdict or {}).get("reason") or "")[:200], routed="vehicle",
                        category="vehicle")
    settings = _settings(task)
    settings["v3"] = {"state": "processing", "stage": "vehicle:hold" if hold else "vehicle:canon", "progress": .05,
                      "requested_intent": "vehicle", "updated_at": now.isoformat() + "Z",
                      "session": {"mt_run_id": run, "viewer_url": f"{UNITY_PAGE}?run={run}",
                                  "files_base": f"/api/mt/files/{run}/"},
                      "vehicle": {"run": run, "subject": what, "hold": hold, "vehicle_run": run,
                                  "routed_at": now.isoformat() + "Z", "hinted": hinted}}
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.pipeline_kind = "v3"
    task.status = "processing"
    task.error_message = None
    task.processing_started_at = task.processing_started_at or now
    task.updated_at = now
    await refund_generation_credits(db, task, "vehicle: the vehicle conveyor instead of the character generation")
    await db.commit()
    print(f"[Vehicle intake] {task.id} -> vehicle run {run} ({what}; {'hold' if hold else 'autorun'})")
    return True


def _run_state(run: str) -> Optional[Dict[str, Any]]:
    d = MT_ROOT / "runs" / run
    try:
        st = json.loads((d / "run.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    v = d / "vehicle"
    vs = st.get("vehicle") if isinstance(st.get("vehicle"), dict) else {}
    stage = str(vs.get("stage") or "")
    files = [n for n, rel in (("canon", "canon.json"), ("cluster", "seg/seg.json"), ("cut", "cut.json"),
                              ("split", "split.json"), ("isolate", "isolated/isolated.json")) if (v / rel).is_file()]
    if not stage or stage in ("hold", "queued"):
        stage = {"canon": "cluster", "cluster": "cut", "cut": "split", "split": "isolate",
                 "isolate": "model_decision"}.get(files[-1], stage) if files else stage
    review = bool((v / "isolated" / "isolated.json").is_file() or
                  vs.get("waiting_for") in ("model_decision", "masks_confirm", "canon_confirm"))
    failed = [k for k, x in (vs.get("stages") or {}).items() if isinstance(x, dict) and x.get("status") == "failed"]
    return {"stage": stage or "hold", "review": review, "failed": failed, "status": st.get("status"),
            "waiting_for": vs.get("waiting_for"), "error": vs.get("error"), "done": files}


async def _pipeline(run: str) -> Optional[Dict[str, Any]]:
    """The vehicle service's one line of where the run stands (mt/vehicle_parts.py `pipeline`): stage key, progress
    0..1, accepted / total parts, running redraws. None when the service does not answer (the row stays as it is)."""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{VEHICLE_URL}/api/mt/vehicle/runs/{run}/parts")
        doc = r.json() if r.status_code == 200 else None
    except Exception:                                            # noqa: BLE001
        return None
    pipe = (doc or {}).get("pipeline")
    return pipe if isinstance(pipe, dict) and pipe.get("stage") else None


async def sync_vehicle_tasks(db) -> int:
    """Mirror each vehicle run's stage into its Task row (the task page reads only the row).

    The page shows `vehicle:<stage>` as «Тачка · <stage>» (i18n v3_stage_vehicle / v3_sub_<stage>: segmentation,
    wheels_cut, parts_split, isolate_parts, restore_parts, redraw_parts, parts_review, model_decision) with its progress;
    the task stays `processing` until the person decides the modelling (owner 2026-10-16: «я нажал и должен видеть
    прогресс»)."""
    from sqlalchemy import select

    from database import Task

    rows = (await db.execute(select(Task).where(Task.pipeline_kind == "v3", Task.status == "processing",
                                                Task.viewer_settings.like('%"vehicle_run"%')))).scalars().all()
    changed = 0
    for task in rows:
        settings = _settings(task)
        v3 = settings.get("v3") if isinstance(settings.get("v3"), dict) else {}
        run = str(((v3.get("vehicle") or {}).get("run")) or "")
        if not _RUN.fullmatch(run):
            continue
        pipe = await _pipeline(run)
        rs = _run_state(run)
        if rs is None:
            continue
        if pipe is not None:
            new = {"state": "processing", "stage": f"vehicle:{pipe['stage']}",
                   "progress": round(float(pipe.get("progress") or 0), 3), "qa": None,
                   "detail": (f"{pipe.get('accepted')}/{pipe.get('total')}" if pipe.get("total") else None)}
        elif rs["review"] or rs["failed"]:
            reasons = ["vehicle: check the parts in the viewer and decide the modelling"]
            if rs["failed"]:
                reasons.append(f"vehicle stage failed: {', '.join(rs['failed'])} {str(rs.get('error') or '')[:160]}")
            new = {"state": "needs_review", "stage": "vehicle:review", "progress": 1.0,
                   "qa": {"status": "vehicle_review", "reasons": reasons}}
        else:
            new = {"state": "processing", "stage": f"vehicle:{rs['stage']}",
                   "progress": STAGE_PROGRESS.get(rs["stage"], .05), "qa": None}
        cur = {k: v3.get(k) for k in new}
        if cur == new:
            continue
        v3.update(new)
        v3["updated_at"] = datetime.utcnow().isoformat() + "Z"
        v3.setdefault("vehicle", {})["stages_done"] = rs["done"]
        settings["v3"] = v3
        task.viewer_settings = json.dumps(settings, ensure_ascii=False)
        task.updated_at = datetime.utcnow()
        changed += 1
    if changed:
        await db.commit()
    return changed
