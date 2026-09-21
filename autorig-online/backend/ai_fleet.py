"""Fleet occupancy and how long a service actually takes.

The numbers here are measured, not configured: durations come from the render
farm's own task ledger and from the converter nodes' status, so an estimate
shown to somebody waiting is the median of what recently happened rather than a
guess that ages badly.

One cached snapshot serves every page; the pages poll it, so a busy fleet is
not hammered by a status widget.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from typing import Dict, List, Optional

import httpx
from fastapi import APIRouter

import ai_vision_api

logger = logging.getLogger(__name__)

router = APIRouter()

RENDERFIN_BASE = ai_vision_api.RENDERFIN_BASE
SNAPSHOT_TTL_SECONDS = 4.0
# Enough history to have a median that means something, short enough that a
# change in the farm shows up within a few jobs.
SAMPLE_LIMIT = 40
# Used only until a service has produced its own samples.
FALLBACK_SECONDS = {
    "vision": 12.0,
    "text": 12.0,
    "image": 100.0,
    "video": 600.0,
    "3dmodel": 900.0,
}
# Renderfin schedules by workflow token; these map onto our service ids.
IMAGE_WORKFLOWS = {"gen_image.json", "t_pose.json", "gen_image_by_z_depth.json",
                   "open_pose.json", "inpaint.json"}
MODEL_WORKFLOWS = {"image_to_3d.json"}

_snapshot: Dict[str, object] = {}
_snapshot_at = 0.0
_lock = asyncio.Lock()


def _service_of_workflow(workflow: str) -> Optional[str]:
    name = str(workflow or "").strip()
    if not name:
        return None
    if name in IMAGE_WORKFLOWS:
        return "image"
    if name in MODEL_WORKFLOWS:
        return "3dmodel"
    # Everything else the farm runs is an animation workflow.
    return "video"


def _durations_from_renderfin(tasks: List[dict]) -> Dict[str, List[float]]:
    """Real time on the card per service, from finished tasks only."""
    durations: Dict[str, List[float]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        started = float(task.get("started_at") or 0)
        finished = float(task.get("finished_at") or 0)
        if started <= 0 or finished <= started:
            continue
        service = _service_of_workflow(
            task.get("workflow") or task.get("workflow_file") or ""
        )
        if not service:
            continue
        durations.setdefault(service, []).append(finished - started)
    return durations


def _running_from_renderfin(tasks: List[dict]) -> Dict[str, int]:
    running: Dict[str, int] = {}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        state = str(task.get("status") or task.get("status_string") or "").lower()
        if state in ("done", "failed", "error", "cancelled"):
            continue
        service = _service_of_workflow(
            task.get("workflow") or task.get("workflow_file") or ""
        )
        if service:
            running[service] = running.get(service, 0) + 1
    return running


def render_node(server: Dict[str, object],
                by_task: Optional[Dict[str, dict]] = None) -> Dict[str, object]:
    """One render worker as the fleet strip needs it.

    A render worker is named `render_server_name`; reading `name` gave every
    one of them the same fallback label, so the strip showed five identical
    dots. And a worker reports `online` all the way through a render — the
    task it is holding is what says it is busy, not its status.

    That same task also says what kind of work it is: the workflow it runs is
    what separates a picture from a clip, which is what colours the dot.
    """
    state = str(server.get("status") or server.get("status_string") or "").lower()
    held = str(server.get("current_render_task") or "")
    queued = int(server.get("queue_size") or 0) > 0
    activity = ""
    task = (by_task or {}).get(held)
    if task:
        activity = _service_of_workflow(
            task.get("workflow") or task.get("workflow_file") or "") or ""
    return {
        "id": str(server.get("render_server_name") or server.get("name")
                  or server.get("id") or "render"),
        "kind": "render",
        "online": state in ("online", "busy"),
        "busy": bool(held) or queued or state == "busy",
        "activity": activity,
    }


async def _renderfin_snapshot(client: httpx.AsyncClient) -> Dict[str, object]:
    try:
        response = await client.get(RENDERFIN_BASE + "/api-render", timeout=8.0)
        if response.status_code != 200:
            return {}
        payload = response.json() or {}
    except Exception:
        return {}
    servers = payload.get("servers") or []
    tasks = payload.get("tasks") or []
    by_task = {str(task.get("task_id")): task for task in tasks
               if isinstance(task, dict) and task.get("task_id")}
    nodes = []
    for server in servers:
        if not isinstance(server, dict):
            continue
        nodes.append(render_node(server, by_task))
    return {
        "nodes": nodes,
        "durations": _durations_from_renderfin(tasks),
        "running": _running_from_renderfin(tasks),
    }


async def _converter_snapshot(client: httpx.AsyncClient) -> Dict[str, object]:
    """AI nodes: reachable, how loaded, and which models they carry."""
    workers = ai_vision_api._load_ai_workers()
    if not workers:
        return {"nodes": [], "load": 0}
    probes = await asyncio.gather(
        *(ai_vision_api._node_is_free(client, worker) for worker in workers),
        return_exceptions=True,
    )
    nodes = []
    total_load = 0
    for worker, probe in zip(workers, probes):
        node_id = ai_vision_api._node_key(worker)
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            nodes.append({"id": node_id, "kind": "ai", "online": False,
                          "busy": False, "activity": "", "activities": []})
            continue
        ok, info = probe
        load = int((info or {}).get("load") or 0) if isinstance(info, dict) else 0
        total_load += load
        activities = list((info or {}).get("activities") or []) if isinstance(info, dict) else []
        nodes.append({
            "id": node_id,
            "kind": "ai",
            "online": bool(ok),
            "busy": bool(ok and load > 0),
            # A node can hold more than one task; the strip has one dot, so it
            # shows the first and the hover panel lists the rest.
            "activity": activities[0] if activities else "",
            "activities": activities,
            "loaded_model": str((info or {}).get("loaded") or "") if isinstance(info, dict) else "",
        })
    return {"nodes": nodes, "load": total_load}


def _summarise(durations: List[float]) -> Dict[str, float]:
    ordered = sorted(durations)[-SAMPLE_LIMIT:]
    if not ordered:
        return {}
    median = statistics.median(ordered)
    index = max(0, int(len(ordered) * 0.9) - 1)
    return {
        "samples_int": len(ordered),
        "median_seconds_float": round(median, 1),
        "p90_seconds_float": round(ordered[index], 1),
    }


async def _build_snapshot() -> Dict[str, object]:
    async with httpx.AsyncClient() as client:
        render, converter = await asyncio.gather(
            _renderfin_snapshot(client), _converter_snapshot(client)
        )
    nodes = list(converter.get("nodes") or []) + list(render.get("nodes") or [])
    durations = dict(render.get("durations") or {})
    running = dict(render.get("running") or {})

    # The AI services measure themselves: the converter reports how long each
    # task took, and those are the only real numbers for vision and text.
    for service_id, samples in ai_vision_api.RECENT_DURATIONS.items():
        if samples:
            durations[service_id] = list(samples)

    services: Dict[str, Dict[str, object]] = {}
    for service_id, fallback in FALLBACK_SECONDS.items():
        summary = _summarise(durations.get(service_id) or [])
        if not summary:
            summary = {
                "samples_int": 0,
                "median_seconds_float": fallback,
                "p90_seconds_float": round(fallback * 1.8, 1),
            }
        summary["running_int"] = int(running.get(service_id) or 0)
        summary["measured_bool"] = bool(durations.get(service_id))
        services[service_id] = summary

    # AI work shares the converter nodes, so its occupancy is their load.
    ai_load = int(converter.get("load") or 0)
    for service_id in ("vision", "text"):
        services[service_id]["running_int"] = ai_load

    online = [n for n in nodes if n.get("online")]
    busy = [n for n in online if n.get("busy")]
    return {
        "success_bool": True,
        "nodes_array": nodes,
        "nodes_online_int": len(online),
        "nodes_total_int": len(nodes),
        "nodes_busy_int": len(busy),
        "services_object": services,
        "server_time_unix_int": int(time.time()),
    }


@router.get("/api/ai/fleet")
async def api_ai_fleet():
    """Who is up, who is busy, and how long each service has been taking."""
    global _snapshot, _snapshot_at
    if _snapshot and (time.monotonic() - _snapshot_at) < SNAPSHOT_TTL_SECONDS:
        return _snapshot
    async with _lock:
        if _snapshot and (time.monotonic() - _snapshot_at) < SNAPSHOT_TTL_SECONDS:
            return _snapshot
        try:
            _snapshot = await _build_snapshot()
        except Exception:
            logger.exception("Could not build the fleet snapshot")
            # A status widget must never take a page down with it.
            _snapshot = {
                "success_bool": False,
                "nodes_array": [],
                "nodes_online_int": 0,
                "nodes_total_int": 0,
                "nodes_busy_int": 0,
                "services_object": {
                    key: {"samples_int": 0, "median_seconds_float": value,
                          "p90_seconds_float": round(value * 1.8, 1),
                          "running_int": 0, "measured_bool": False}
                    for key, value in FALLBACK_SECONDS.items()
                },
                "server_time_unix_int": int(time.time()),
            }
        _snapshot_at = time.monotonic()
        return _snapshot
