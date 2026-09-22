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
    "control": 30.0,
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
    if name.startswith("gen_control_"):
        return "control"
    if name in IMAGE_WORKFLOWS or name.startswith("gen_image"):
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


def _workflow_durations(tasks: List[dict]) -> Dict[str, List[float]]:
    durations: Dict[str, List[float]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        workflow = str(task.get("workflow") or task.get("workflow_file") or "").strip()
        started = float(task.get("started_at") or 0)
        finished = float(task.get("finished_at") or 0)
        if workflow and started > 0 and finished > started:
            durations.setdefault(workflow, []).append(finished - started)
    return durations


def _queue_summary(tasks: List[dict], servers: List[dict],
                   service_durations: Dict[str, List[float]],
                   now: Optional[float] = None) -> Dict[str, object]:
    """Conservative list-scheduling ETA over workflow-compatible workers."""
    now = float(now if now is not None else time.time())
    workflow_samples = _workflow_durations(tasks)
    online = [server for server in servers if isinstance(server, dict)
              and str(server.get("status") or "").lower() in ("online", "busy")]
    availability = {str(server.get("render_server_name") or ""): 0.0
                    for server in online if server.get("render_server_name")}
    active = []
    pending = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        state = str(task.get("status") or task.get("status_string") or "").lower()
        if state in ("rendering", "running") and task.get("render_server_name"):
            active.append(task)
        elif state == "pending":
            pending.append(task)

    kinds = set()
    used_samples = set()

    def duration(task: dict) -> float:
        workflow = str(task.get("workflow") or task.get("workflow_file") or "").strip()
        samples = sorted(workflow_samples.get(workflow) or [])[-SAMPLE_LIMIT:]
        if samples:
            used_samples.update((workflow, index) for index in range(len(samples)))
            kinds.add("measured")
            return float(statistics.median(samples))
        service = _service_of_workflow(workflow) or "video"
        samples = sorted(service_durations.get(service) or [])[-SAMPLE_LIMIT:]
        if samples:
            used_samples.update((service, index) for index in range(len(samples)))
            kinds.add("measured")
            return float(statistics.median(samples))
        kinds.add("fallback")
        return float(FALLBACK_SECONDS.get(service) or 600.0)

    for task in active:
        worker = str(task.get("render_server_name") or "")
        expected = duration(task)
        elapsed = max(0.0, now - float(task.get("started_at") or now))
        availability[worker] = max(availability.get(worker, 0.0),
                                   max(0.0, expected - elapsed))

    queued = 0
    blocked = 0
    for task in sorted(pending, key=lambda item: float(item.get("created_at") or 0)):
        workflow = str(task.get("workflow") or task.get("workflow_file") or "").strip()
        eligible = [str(server.get("render_server_name") or "") for server in online
                    if workflow in (server.get("available_workflows") or [])]
        eligible = [name for name in eligible if name]
        if not eligible:
            blocked += 1
            continue
        queued += 1
        worker = min(eligible, key=lambda name: availability.get(name, 0.0))
        availability[worker] = availability.get(worker, 0.0) + duration(task)

    eta = max(availability.values(), default=0.0)
    if not active and queued == 0:
        eta_value = None
        estimate_kind = "unknown" if blocked else "empty"
    else:
        # A coarse ten-second value is more honest than fake sub-second precision.
        eta_value = float(max(0, round(eta / 10.0) * 10))
        estimate_kind = "mixed" if len(kinds) > 1 else next(iter(kinds), "unknown")
    return {
        "running_int": len(active),
        "queued_int": queued,
        "blocked_int": blocked,
        "eta_seconds_float": eta_value,
        "estimate_kind_string": estimate_kind,
        "sample_count_int": len(used_samples),
    }


def _running_from_renderfin(tasks: List[dict]) -> Dict[str, Dict[str, int]]:
    running: Dict[str, Dict[str, int]] = {}
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
            bucket = running.setdefault(service, {"active": 0, "queued": 0})
            if state in ("rendering", "running") and task.get("render_server_name"):
                bucket["active"] += 1
            else:
                bucket["queued"] += 1
    return running


def render_node(server: Dict[str, object],
                by_task: Optional[Dict[str, dict]] = None,
                by_server: Optional[Dict[str, dict]] = None,
                comfy_depth: int = 0) -> Dict[str, object]:
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
    task = (by_task or {}).get(held) or (by_server or {}).get(
        str(server.get("render_server_name") or ""))
    if task and not held:
        held = str(task.get("id") or task.get("task_id") or "")
    if task:
        activity = _service_of_workflow(
            task.get("workflow") or task.get("workflow_file") or "") or ""
    return {
        "id": str(server.get("render_server_name") or server.get("name")
                  or server.get("id") or "render"),
        "kind": "render",
        "online": state in ("online", "busy"),
        "busy": bool(held) or queued or comfy_depth > 0 or state == "busy",
        "activity": activity,
        "activities": [activity] if activity else [],
        "state": "offline" if state not in ("online", "busy") else (
            "active" if held or comfy_depth > 0 or state == "busy" else
            "queued" if queued else "idle"),
        "task_id": held,
        "task_status": str((task or {}).get("status") or ""),
        "workflow": str((task or {}).get("workflow") or
                        (task or {}).get("workflow_file") or ""),
        "assigned_worker": str(server.get("render_server_name") or ""),
        "queue_depth": int(comfy_depth),
    }


async def _comfy_queue_depth(client: httpx.AsyncClient,
                             server: Dict[str, object]) -> int:
    url = str(server.get("render_server_url") or "").rstrip("/")
    if not url:
        return 0
    try:
        response = await client.get(url + "/queue", timeout=3.0)
        if response.status_code != 200:
            return 0
        payload = response.json() or {}
        return len(payload.get("queue_running") or []) + len(payload.get("queue_pending") or [])
    except Exception:
        return 0


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
    active_tasks = [task for task in tasks if isinstance(task, dict)
                    and str(task.get("status") or "").lower()
                    not in ("done", "failed", "error", "cancelled")]
    by_task = {str(task.get("id") or task.get("task_id")): task
               for task in active_tasks if task.get("id") or task.get("task_id")}
    by_server = {str(task.get("render_server_name")): task for task in active_tasks
                 if task.get("render_server_name")}
    depths = await asyncio.gather(
        *(_comfy_queue_depth(client, server) for server in servers),
        return_exceptions=True,
    )
    nodes = []
    for server, depth in zip(servers, depths):
        if not isinstance(server, dict):
            continue
        nodes.append(render_node(server, by_task, by_server,
                                 depth if isinstance(depth, int) else 0))
    return {
        "nodes": nodes,
        "durations": _durations_from_renderfin(tasks),
        "running": _running_from_renderfin(tasks),
        "tasks": tasks,
        "servers": servers,
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
    running: Dict[str, int] = {}
    for worker, probe in zip(workers, probes):
        node_id = ai_vision_api._node_key(worker)
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            nodes.append({"id": node_id, "kind": "ai", "online": False,
                          "busy": False, "activity": "", "activities": [],
                          "state": "offline", "queue_depth": 0, "task_id": "",
                          "task_status": "", "workflow": "",
                          "assigned_worker": node_id})
            continue
        ok, info = probe
        load = int((info or {}).get("load") or 0) if isinstance(info, dict) else 0
        total_load += load
        activities = list((info or {}).get("activities") or []) if isinstance(info, dict) else []
        for activity in activities:
            service = str(activity or "")
            if service in FALLBACK_SECONDS:
                running[service] = running.get(service, 0) + 1
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
            "state": "active" if ok and load > 0 else "idle" if ok else "offline",
            "queue_depth": load,
            "task_id": "",
            "task_status": "",
            "workflow": activities[0] if activities else "",
            "assigned_worker": node_id,
        })
    return {"nodes": nodes, "load": total_load, "running": running}


def _canonical_node_id(node_id: object) -> str:
    value = str(node_id or "").strip()
    aliases = {"raptor": "ryzen-server"}
    return aliases.get(value.lower(), value.lower())


def _merge_physical_nodes(nodes: List[dict]) -> List[dict]:
    """One dot per physical GPU even when converter and Renderfin both list it."""
    merged: Dict[str, dict] = {}
    order: List[str] = []
    for source in nodes:
        if not isinstance(source, dict):
            continue
        key = _canonical_node_id(source.get("id"))
        if not key:
            continue
        if key not in merged:
            merged[key] = dict(source)
            merged[key]["id"] = "ryzen-server" if key == "ryzen-server" else str(source.get("id") or key)
            merged[key]["sources"] = [str(source.get("kind") or "")]
            order.append(key)
            continue
        target = merged[key]
        target["sources"].append(str(source.get("kind") or ""))
        target["kind"] = "mixed"
        target["online"] = bool(target.get("online") or source.get("online"))
        target["busy"] = bool(target.get("busy") or source.get("busy"))
        target["queue_depth"] = max(int(target.get("queue_depth") or 0),
                                    int(source.get("queue_depth") or 0))
        activities = list(target.get("activities") or [])
        for activity in source.get("activities") or ([source.get("activity")] if source.get("activity") else []):
            if activity and activity not in activities:
                activities.append(activity)
        target["activities"] = activities
        target["activity"] = activities[0] if activities else ""
        if source.get("task_id"):
            for field in ("task_id", "task_status", "workflow", "assigned_worker"):
                target[field] = source.get(field) or target.get(field) or ""
        target["state"] = "active" if target["busy"] else (
            "idle" if target["online"] else "offline")
    return [merged[key] for key in order]


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
    nodes = _merge_physical_nodes(
        list(converter.get("nodes") or []) + list(render.get("nodes") or []))
    durations = dict(render.get("durations") or {})
    running = dict(render.get("running") or {})
    for service_id, count in (converter.get("running") or {}).items():
        bucket = running.setdefault(service_id, {"active": 0, "queued": 0})
        bucket["active"] += int(count or 0)

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
        counts = running.get(service_id) or {}
        summary["running_int"] = int(counts.get("active") or 0)
        summary["queued_int"] = int(counts.get("queued") or 0)
        summary["measured_bool"] = bool(durations.get(service_id))
        services[service_id] = summary

    online = [n for n in nodes if n.get("online")]
    busy = [n for n in online if n.get("busy")]
    queue = _queue_summary(list(render.get("tasks") or []),
                           list(render.get("servers") or []), durations)
    return {
        "success_bool": True,
        "nodes_array": nodes,
        "nodes_online_int": len(online),
        "nodes_total_int": len(nodes),
        "nodes_busy_int": len(busy),
        "services_object": services,
        "queue_object": queue,
        "server_time_unix_int": int(time.time()),
        "snapshot_ttl_seconds_float": SNAPSHOT_TTL_SECONDS,
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
                          "running_int": 0, "queued_int": 0,
                          "measured_bool": False}
                    for key, value in FALLBACK_SECONDS.items()
                },
                "queue_object": {
                    "running_int": 0, "queued_int": 0, "blocked_int": 0,
                    "eta_seconds_float": None,
                    "estimate_kind_string": "unknown", "sample_count_int": 0,
                },
                "server_time_unix_int": int(time.time()),
                "snapshot_ttl_seconds_float": SNAPSHOT_TTL_SECONDS,
            }
        _snapshot_at = time.monotonic()
        return _snapshot
