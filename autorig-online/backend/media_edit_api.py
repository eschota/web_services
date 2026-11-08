"""Renderfin media edit: M+ "similars" (flip / rotate / crop / vertical smart crop / speed / frame) on the farm.

Owner 2026-11-07: «симиляры и апскейл на рендерфин»; design "Симиляры и апскейл M+ на Renderfin", phase B.

    POST /api/media-edit                    -> {task_id_string, status_string, output_url_string, ...}
    GET  /api/ai/render-status/{task_id}    -> queued | rendering | completed | failed | cancelled,
                                               node_string, error_string, output_url_string, output_meta
    GET  /api/media-edit/{task_id}          -> the same status
    POST /api/media-edit/{task_id}/cancel   -> cancels on the node

The work runs on the farm ai-nodes (deploy/ai-node/ai_node.py, workload `media_edit`) with ffmpeg: the node
downloads `source_url` itself (any public https host, up to 4 GB), runs ffmpeg (at most 2 jobs per node, CPU +
NVENC, next to the language model), and keeps the result until this module fetched it. The result is published
next to the render outputs (nginx `/renderfin/render/` -> /srv/autorig/data/var/renderfin/render/) under
`media-edit/<day>/<random>/result.<ext>` and deleted after 24 h. No cache: every call is a new task.

A task id is ``me.<node>.<node task id>`` and the task record is a JSON file, so a backend restart neither loses
a running job nor its published file.

Access: ``X-Autorig-Priority: mplus`` with a valid ``X-Priority-Key`` (tier mplus, ranked first on the node), or
a request from the host itself. MEDIA_EDIT_OPEN=1 admits anyone (tier generation).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import pathlib
import secrets
import shutil
import time
import urllib.parse
from collections import deque
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request

import ai_vision_api

logger = logging.getLogger(__name__)
router = APIRouter()

DATA_DIR = pathlib.Path(os.getenv("MEDIA_EDIT_DATA_DIR", "/srv/autorig/data/media-edit"))
OUTPUT_DIR = pathlib.Path(os.getenv("MEDIA_EDIT_OUTPUT_DIR", "/srv/autorig/data/var/renderfin/render/media-edit"))
OUTPUT_URL = os.getenv("MEDIA_EDIT_OUTPUT_URL", "https://autorig.online/renderfin/render/media-edit").rstrip("/")
KEEP_SECONDS = float(os.getenv("MEDIA_EDIT_KEEP_HOURS", "24")) * 3600
RECORD_KEEP_SECONDS = 7 * 24 * 3600
TASK_TIMEOUT_SECONDS = float(os.getenv("MEDIA_EDIT_TASK_TIMEOUT_SECONDS", "7200"))
NODE_LOST_SECONDS = float(os.getenv("MEDIA_EDIT_NODE_LOST_SECONDS", "900"))
MIN_FREE_BYTES = int(float(os.getenv("MEDIA_EDIT_MIN_FREE_GB", "15")) * 1024 ** 3)
OPEN_ACCESS = os.getenv("MEDIA_EDIT_OPEN", "0").strip().lower() in ("1", "true", "yes")
PROBE_FRESH_SECONDS = 3.0
PROBE_DOWN_SECONDS = 30.0
WATCH_INTERVAL_SECONDS = 3.0
MAX_SUBMIT_ATTEMPTS = 3

TERMINAL = ("completed", "failed", "cancelled")
NODE_STATE = {"Pending": "queued", "Processing": "rendering", "Completed": "completed", "Failed": "failed",
              "Cancelled": "cancelled"}


class RequestError(ValueError):
    """The request breaks the contract (HTTP 400)."""


# ------------------------------------------------------------------ validation
# Copied verbatim from deploy/ai-node/ai_node.py (parse_media_edit_request and its helpers) so the backend
# refuses exactly what a node would refuse, before a node is asked. Keep the two in step.
MEDIA_EDIT_MAX_ANCHORS = 240
MEDIA_EDIT_MAX_SIDE = 16384
IMAGE_CONTAINERS = ("jpg", "png")
VIDEO_CONTAINERS = ("mp4", "mov")


def _finite(value: object, name: str) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise RequestError(f"{name} must be a number") from None
    if number != number or number in (float("inf"), float("-inf")):
        raise RequestError(f"{name} must be finite")
    return number


def _int_field(source: Dict[str, object], key: str, name: str, low: int, high: int) -> int:
    if key not in source:
        raise RequestError(f"{name}.{key} is required")
    value = _finite(source.get(key), f"{name}.{key}")
    if value != int(value):
        raise RequestError(f"{name}.{key} must be a whole number")
    value = int(value)
    if value < low or value > high:
        raise RequestError(f"{name}.{key} must be between {low} and {high}")
    return value


def parse_media_edit_request(payload: object) -> Dict[str, object]:
    """Validate a media-edit request into the normalized spec a node runs (see ai_node.py)."""
    if not isinstance(payload, dict):
        raise RequestError("JSON object required")
    source_url = str(payload.get("source_url") or "").strip()
    if not source_url:
        raise RequestError("source_url is required")
    if len(source_url) > 4096:
        raise RequestError("source_url is too long")
    kind = str(payload.get("media_kind") or "").strip().lower()
    if kind not in ("image", "video"):
        raise RequestError("media_kind must be image or video")
    op = payload.get("operation") or {}
    if not isinstance(op, dict):
        raise RequestError("operation must be an object")
    known = {"crop", "scale", "rotate_rad", "rotation_relative_to", "hflip", "vflip", "speed",
             "cut_frame_at_seconds", "smartcrop"}
    unknown = sorted(set(op) - known)
    if unknown:
        raise RequestError(f"operation has unknown fields: {', '.join(unknown)}")
    spec: Dict[str, object] = {"source_url": source_url, "media_kind": kind}

    crop = op.get("crop")
    if crop is not None:
        if not isinstance(crop, dict):
            raise RequestError("operation.crop must be an object {w,h,x,y}")
        spec["crop"] = {"w": _int_field(crop, "w", "crop", 2, MEDIA_EDIT_MAX_SIDE),
                        "h": _int_field(crop, "h", "crop", 2, MEDIA_EDIT_MAX_SIDE),
                        "x": _int_field(crop, "x", "crop", 0, MEDIA_EDIT_MAX_SIDE) if "x" in crop else 0,
                        "y": _int_field(crop, "y", "crop", 0, MEDIA_EDIT_MAX_SIDE) if "y" in crop else 0}
    scale = op.get("scale")
    if scale is not None:
        if not isinstance(scale, dict):
            raise RequestError("operation.scale must be an object {w,h}")
        spec["scale"] = {"w": _int_field(scale, "w", "scale", 2, MEDIA_EDIT_MAX_SIDE),
                         "h": _int_field(scale, "h", "scale", 2, MEDIA_EDIT_MAX_SIDE)}
    rotate = op.get("rotate_rad")
    if rotate is not None:
        rotate = _finite(rotate, "operation.rotate_rad")
        if abs(rotate) > 2 * 3.141592653589793 + 1e-6:
            raise RequestError("operation.rotate_rad is in radians (|value| <= 2*pi)")
        spec["rotate_rad"] = rotate
    relative = str(op.get("rotation_relative_to") or "display").strip().lower()
    if relative not in ("display", "stored"):
        raise RequestError("operation.rotation_relative_to must be display or stored")
    spec["rotation_relative_to"] = relative
    for flag in ("hflip", "vflip"):
        value = op.get(flag, False)
        if not isinstance(value, bool):
            raise RequestError(f"operation.{flag} must be true or false")
        spec[flag] = value

    speed = op.get("speed")
    spec["speed"], spec["interpolate"] = 1.0, False
    if speed is not None:
        if kind != "video":
            raise RequestError("operation.speed is for video")
        if not isinstance(speed, dict):
            raise RequestError("operation.speed must be an object {factor, interpolate}")
        factor = _finite(speed.get("factor", 1.0), "operation.speed.factor")
        if factor < 0.1 or factor > 8.0:
            raise RequestError("operation.speed.factor must be between 0.1 and 8")
        interpolate = speed.get("interpolate", False)
        if not isinstance(interpolate, bool):
            raise RequestError("operation.speed.interpolate must be true or false")
        spec["speed"], spec["interpolate"] = factor, interpolate

    cut = op.get("cut_frame_at_seconds")
    spec["cut_frame_at"] = None
    if cut is not None:
        if kind != "video":
            raise RequestError("operation.cut_frame_at_seconds is for video")
        cut = _finite(cut, "operation.cut_frame_at_seconds")
        if cut < 0 or cut > 86400:
            raise RequestError("operation.cut_frame_at_seconds must be between 0 and 86400")
        spec["cut_frame_at"] = cut

    smart = op.get("smartcrop")
    if smart is not None:
        if not isinstance(smart, dict):
            raise RequestError("operation.smartcrop must be an object")
        if "crop" in spec:
            raise RequestError("operation.crop and operation.smartcrop exclude each other")
        mode = str(smart.get("mode") or "anchor_camera").strip().lower()
        if mode not in ("anchor_camera", "center"):
            raise RequestError("operation.smartcrop.mode must be anchor_camera or center")
        aspect = _finite(smart.get("target_aspect", 9.0 / 16.0), "operation.smartcrop.target_aspect")
        if aspect < 0.1 or aspect > 10:
            raise RequestError("operation.smartcrop.target_aspect must be between 0.1 and 10 (w/h)")
        units = str(smart.get("units") or "norm").strip().lower()
        if units not in ("norm", "px"):
            raise RequestError("operation.smartcrop.units must be norm or px")
        anchors_raw = smart.get("anchors") or []
        if not isinstance(anchors_raw, list):
            raise RequestError("operation.smartcrop.anchors must be a list")
        if len(anchors_raw) > MEDIA_EDIT_MAX_ANCHORS:
            raise RequestError(f"operation.smartcrop.anchors: at most {MEDIA_EDIT_MAX_ANCHORS}")
        anchors: List[Tuple[float, float, float]] = []
        if mode == "anchor_camera":
            for index, anchor in enumerate(anchors_raw):
                if not isinstance(anchor, dict):
                    raise RequestError(f"operation.smartcrop.anchors[{index}] must be an object {{t,x,y}}")
                t = _finite(anchor.get("t", 0.0), f"anchors[{index}].t")
                x = _finite(anchor.get("x"), f"anchors[{index}].x")
                y = _finite(anchor.get("y"), f"anchors[{index}].y")
                if t < 0:
                    raise RequestError(f"anchors[{index}].t must be >= 0")
                if units == "norm" and not (0.0 <= x <= 1.0 and 0.0 <= y <= 1.0):
                    raise RequestError(f"anchors[{index}]: x and y are 0..1 of the frame (units=norm)")
                if units == "px" and (x < 0 or y < 0):
                    raise RequestError(f"anchors[{index}]: x and y must be >= 0")
                anchors.append((t, x, y))
            anchors.sort(key=lambda item: item[0])
        spec["smartcrop"] = {"target_aspect": aspect, "units": units, "anchors": anchors}

    output = payload.get("output") or {}
    if not isinstance(output, dict):
        raise RequestError("output must be an object")
    container = str(output.get("container") or "auto").strip().lower()
    container = {"jpeg": "jpg"}.get(container, container)
    codec = str(output.get("codec") or "auto").strip().lower()
    keep_audio = output.get("keep_audio", False)
    if not isinstance(keep_audio, bool):
        raise RequestError("output.keep_audio must be true or false")
    if codec not in ("auto", "h264", "prores"):
        raise RequestError("output.codec must be auto, h264 or prores")
    still = kind == "image" or spec["cut_frame_at"] is not None
    if still:
        if container not in ("auto",) + IMAGE_CONTAINERS:
            raise RequestError("output.container for a picture must be jpg or png")
        if codec != "auto":
            raise RequestError("output.codec is for video output")
    else:
        if container not in ("auto",) + VIDEO_CONTAINERS:
            raise RequestError("output.container for video must be mp4 or mov (jpg/png need cut_frame_at_seconds)")
        if codec == "prores" and container == "mp4":
            raise RequestError("ProRes goes into mov")
    spec.update(container=container, codec=codec, keep_audio=bool(keep_audio) and not still,
                client_ref=str(payload.get("client_ref") or "")[:200],
                tier=str(payload.get("tier") or "").strip().lower()[:32],
                backend_task_id=str(payload.get("backend_task_id") or "")[:120])
    return spec


def validate_source_host(url: str) -> None:
    """https and a public-looking host; the node repeats the full DNS-based SSRF check before it downloads."""
    import ipaddress
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise RequestError("source_url must be an absolute https URL")
    if parsed.username or parsed.password:
        raise RequestError("source_url credentials are not allowed")
    host = parsed.hostname.lower()
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        raise RequestError("source_url host is not public")
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None                         # a name: resolved and checked by the node
    if literal is not None and not literal.is_global:
        raise RequestError("source_url host is not public")
    try:
        port = parsed.port
    except ValueError:
        raise RequestError("source_url has an invalid port") from None
    if port not in (None, 443):
        raise RequestError("source_url port must be 443")


def request_body(spec: Dict[str, object], payload: Dict[str, object], tier: str) -> Dict[str, object]:
    """What the node gets: the caller's contract fields (not the normalized spec) plus tier/ref."""
    return {"source_url": spec["source_url"], "media_kind": spec["media_kind"],
            "operation": payload.get("operation") or {}, "output": payload.get("output") or {},
            "client_ref": spec.get("client_ref") or "", "tier": tier}


# --------------------------------------------------------------------- access
def caller_tier(request: Optional[Request]) -> str:
    """'mplus' with a valid key, 'autorig'/'generation' otherwise; raises 403 for an untrusted stranger."""
    headers = request.headers if request is not None else {}
    tier = ""
    internal = not (str(headers.get("x-real-ip") or "").strip() or str(headers.get("x-forwarded-for") or "").strip())
    try:
        from renderfin import priority
        if priority.enabled():
            tier = priority.request_tier(headers)
        elif internal or priority.priority_key_valid(str(headers.get("x-priority-key") or "")):
            tier = "mplus" if str(headers.get("x-autorig-priority") or "").strip().lower() == "mplus" else ""
    except Exception:
        logger.exception("priority headers not read")
    if tier == "mplus":
        return "mplus"
    if internal:
        return "autorig"
    if OPEN_ACCESS:
        return "generation"
    raise HTTPException(status_code=403, detail={
        "error_string": "media_edit_key_required",
        "message_string": "media-edit needs X-Autorig-Priority: mplus with a valid X-Priority-Key"})


# ---------------------------------------------------------------------- records
_RECORDS: Dict[str, Dict[str, object]] = {}
_LOCKS: Dict[str, asyncio.Lock] = {}
_loaded = False


def _tasks_dir() -> pathlib.Path:
    path = DATA_DIR / "tasks"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _save(record: Dict[str, object]) -> None:
    path = _tasks_dir() / (str(record["task_id"]) + ".json")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)                     # an active record carries the source URL (retry_body)
    except OSError:
        pass
    os.replace(tmp, path)


def _load_all() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    try:
        for path in _tasks_dir().glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(record, dict) and record.get("task_id"):
                    _RECORDS[str(record["task_id"])] = record
            except Exception:
                logger.warning("media-edit: unreadable record %s", path.name)
    except OSError:
        logger.exception("media-edit: records directory %s not readable", DATA_DIR)


def _record(task_id: str) -> Optional[Dict[str, object]]:
    _load_all()
    record = _RECORDS.get(task_id)
    if record is None:
        path = _tasks_dir() / (task_id + ".json")
        if path.is_file():
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                _RECORDS[task_id] = record
            except Exception:
                return None
    return record


def _lock(task_id: str) -> asyncio.Lock:
    lock = _LOCKS.get(task_id)
    if lock is None:
        lock = _LOCKS[task_id] = asyncio.Lock()
    return lock


def parse_task_id(task_id: str) -> Tuple[str, str]:
    """'me.<node>.<node task>' -> (node, node task); ValueError otherwise."""
    prefix, _, rest = str(task_id or "").partition(".")
    node, _, node_task = rest.partition(".")
    if prefix != "me" or not node or not node_task or len(task_id) > 120:
        raise ValueError("not a media-edit task id")
    if not all(ch.isalnum() or ch == "-" for ch in node + node_task):
        raise ValueError("not a media-edit task id")
    return node, node_task


def is_media_task_id(task_id: str) -> bool:
    try:
        parse_task_id(task_id)
        return True
    except ValueError:
        return False


# ------------------------------------------------------------------- the nodes
_PROBES: Dict[str, Tuple[float, Optional[Dict[str, object]]]] = {}
_RECENT_PICKS: Dict[str, deque] = {}
BUSY_ACTIVITIES = ("conversion", "3dmodel")


def media_info(payload: Dict[str, object]) -> Optional[Dict[str, object]]:
    """The node's media_edit block from its server-status, or None when it offers no media edit."""
    block = payload.get("media_edit") if isinstance(payload, dict) else None
    if not isinstance(block, dict):
        return None
    activities = ai_vision_api._activities(payload)
    return {
        "capable": bool(block.get("capable")),
        "parallel": int(block.get("parallel") or 0),
        "running": int(block.get("running") or 0),
        "pending": int(block.get("pending") or 0),
        "free_slots": int(block.get("free_slots") or 0),
        "max_tasks": int(block.get("max_tasks") or 0),
        "nvenc": bool(block.get("nvenc")),
        "selftest_ok": bool((block.get("selftest") or {}).get("ok")),
        "selftest_seconds": (block.get("selftest") or {}).get("seconds"),
        "selftest_error": str((block.get("selftest") or {}).get("error") or "")[:200],
        "version": str(payload.get("server_version") or ""),
        "busy_with": [a for a in activities if a in BUSY_ACTIVITIES],
    }


async def _probe(client: httpx.AsyncClient, worker: Dict[str, object]) -> Optional[Dict[str, object]]:
    key = ai_vision_api._node_key(worker)
    seen = _PROBES.get(key)
    now = time.monotonic()
    if seen:
        at, info = seen
        if info is not None and now - at < PROBE_FRESH_SECONDS:
            return info
        if info is None and now - at < PROBE_DOWN_SECONDS:
            return None
    info: Optional[Dict[str, object]] = None
    try:
        response = await client.get(ai_vision_api._ai_base(worker) + "/server-status",
                                    headers={"Authorization": f"Bearer {worker['token']}"}, timeout=5.0)
        if response.status_code == 200:
            payload = response.json() or {}
            # A node that answers but predates media edit is online, just not capable.
            info = media_info(payload) or {"capable": False, "legacy": True,
                                           "version": str(payload.get("server_version") or "")}
    except Exception:
        info = None
    _PROBES[key] = (time.monotonic(), info)
    return info


def _recent(key: str, now: float) -> int:
    picks = _RECENT_PICKS.get(key)
    while picks and now - picks[0] > 10:
        picks.popleft()
    return len(picks or ())


def rank_candidates(candidates: List[Tuple[Dict[str, object], Dict[str, object]]],
                    now: Optional[float] = None) -> List[Tuple[Dict[str, object], Dict[str, object]]]:
    """Capable nodes with room, best first: a free ffmpeg slot, no conversion/3D job, least loaded per slot,
    NVENC working, then the name (stable)."""
    now = time.monotonic() if now is None else now
    usable = []
    for worker, info in candidates:
        if not info or not info.get("capable"):
            continue
        key = ai_vision_api._node_key(worker)
        queued = int(info.get("running") or 0) + int(info.get("pending") or 0) + _recent(key, now)
        if info.get("max_tasks") and queued >= int(info["max_tasks"]):
            continue
        usable.append((worker, info, queued))
    usable.sort(key=lambda item: (
        0 if int(item[1].get("free_slots") or 0) - _recent(ai_vision_api._node_key(item[0]), now) > 0 else 1,
        1 if item[1].get("busy_with") else 0,
        round(item[2] / max(1, int(item[1].get("parallel") or 1)), 3),
        0 if item[1].get("nvenc") else 1,
        ai_vision_api._node_key(item[0]),
    ))
    return [(worker, info) for worker, info, _ in usable]


async def pick_nodes(client: httpx.AsyncClient) -> List[Tuple[Dict[str, object], Dict[str, object]]]:
    workers = ai_vision_api._load_ai_workers()
    probes = await asyncio.gather(*(_probe(client, w) for w in workers), return_exceptions=True)
    pairs = [(w, p) for w, p in zip(workers, probes) if isinstance(p, dict)]
    return rank_candidates(pairs)


def _worker(node: str) -> Optional[Dict[str, object]]:
    return ai_vision_api._worker_by_key(node)


def _auth(worker: Dict[str, object]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {worker['token']}"}


# ----------------------------------------------------------------- the endpoint
def _error(status: int, code: str, message: str, **extra: object) -> HTTPException:
    return HTTPException(status_code=status, detail={"error_string": code, "message_string": message, **extra})


def _disk_free() -> int:
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(OUTPUT_DIR).free
    except OSError:
        return 0


@router.get("/api/media-edit")
async def api_media_edit_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/media-edit",
        "headers_object": {"X-Autorig-Priority": "mplus", "X-Priority-Key": "<key>"},
        "required_fields_array": ["source_url (https)", "media_kind (image|video)", "operation"],
        "example_request_object": {
            "source_url": "https://example.com/clip.mp4", "media_kind": "video",
            "operation": {"smartcrop": {"mode": "anchor_camera", "target_aspect": 0.5625,
                                        "anchors": [{"t": 0, "x": 0.3, "y": 0.5}, {"t": 8, "x": 0.7, "y": 0.5}]},
                          "speed": {"factor": 2.0, "interpolate": True}},
            "output": {"container": "mp4", "codec": "auto", "keep_audio": False},
            "client_ref": "mplus:<batch>:<iid>:<op>"},
        "operation_fields_object": {
            "crop": "{w,h,x,y} source pixels (after the source's own rotation)",
            "scale": "{w,h}", "rotate_rad": "clockwise radians; quarter turns are exact",
            "rotation_relative_to": "display (default) | stored (Freestock: minus the source rotation tag)",
            "hflip": "bool", "vflip": "bool", "speed": "{factor 0.1..8, interpolate} video",
            "cut_frame_at_seconds": "video -> one jpg/png frame",
            "smartcrop": "{mode anchor_camera|center, target_aspect w/h, units norm|px, anchors [{t,x,y}]}"},
        "status_url_string": "/api/ai/render-status/{task_id}",
        "limits_object": {"source_bytes_max_int": 4 * 1024 ** 3, "task_seconds_max_int": int(TASK_TIMEOUT_SECONDS),
                          "per_node_parallel_int": 2, "output_kept_hours_float": KEEP_SECONDS / 3600},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/media-edit")
async def api_media_edit(request: Request):
    tier = caller_tier(request)
    try:
        payload = await request.json()
    except Exception:
        raise _error(400, "invalid_request", "body is not JSON") from None
    try:
        spec = parse_media_edit_request(payload)
        validate_source_host(str(spec["source_url"]))
    except RequestError as exc:
        raise _error(400, "invalid_request", str(exc)) from None
    if _disk_free() < MIN_FREE_BYTES:
        raise _error(503, "storage_low", "the result store is short of disk space; retry later",
                     retryable_bool=True)
    return await submit(spec, payload, tier)


async def submit(spec: Dict[str, object], payload: Dict[str, object], tier: str) -> Dict[str, object]:
    body = request_body(spec, payload, tier)
    last: Optional[HTTPException] = None
    async with httpx.AsyncClient() as client:
        ranked = await pick_nodes(client)
        if not ranked:
            raise _error(503, "no_media_edit_node", "No farm node offers media edit right now; retry shortly",
                         retryable_bool=True)
        for worker, info in ranked[:MAX_SUBMIT_ATTEMPTS]:
            node = ai_vision_api._node_key(worker)
            # Reserved before the first await: a burst of requests ranks on the same cached probes, and without
            # this every one of them picked the same node (2026-11-07, 4 at once all went to f11-ai).
            _RECENT_PICKS.setdefault(node, deque()).append(time.monotonic())
            try:
                response = await client.post(ai_vision_api._ai_base(worker) + "/media-edit", json=body,
                                             headers=_auth(worker), timeout=30.0)
            except Exception:
                _PROBES[node] = (time.monotonic(), None)
                last = _error(502, "worker_unreachable", f"Farm node {node} did not accept the job")
                continue
            if response.status_code in (200, 202):
                node_task = str((response.json() or {}).get("task_id") or "")
                if not node_task:
                    last = _error(502, "worker_no_task_id", f"Farm node {node} returned no task id")
                    continue
                return _accepted(node, node_task, spec, tier, info, body)
            reason = ""
            try:
                reason = str((response.json() or {}).get("message") or (response.json() or {}).get("error") or "")
            except Exception:
                reason = response.text[:200]
            if response.status_code == 400:
                raise _error(400, "invalid_request", reason or "the node refused the request")
            _PROBES[node] = (time.monotonic(), dict(info, capable=False) if response.status_code == 503 else None)
            last = _error(503 if response.status_code == 503 else 502, "worker_busy" if response.status_code == 503
                          else "worker_rejected", f"Farm node {node}: {reason or response.status_code}",
                          retryable_bool=True)
    raise last or _error(503, "no_media_edit_node", "No farm node took the job", retryable_bool=True)


def _accepted(node: str, node_task: str, spec: Dict[str, object], tier: str,
              info: Dict[str, object], body: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    task_id = f"me.{node}.{node_task}"
    now = time.time()
    source = urllib.parse.urlparse(str(spec["source_url"]))
    record: Dict[str, object] = {
        "task_id": task_id, "node": node, "node_task_id": node_task, "status": "queued", "stage": "queued",
        "progress": 0.0, "tier": tier, "client_ref": spec.get("client_ref") or "",
        "media_kind": spec["media_kind"], "source_host": source.hostname or "",
        "created_at": now, "started_at": 0.0, "finished_at": 0.0, "last_seen_at": now,
        "output_url": "", "output_path": "", "output_meta": {}, "source_meta": {}, "error": "", "encoder": "",
        "queue_position": 0, "node_version": info.get("version") or "",
        # The node request, kept only while the task is active: one retry on another node after a failed source
        # download (2026-11-08). It holds the source URL (maybe a signed key), so it is dropped when the task ends.
        "retry_body": dict(body) if body else None, "retried_from": "", "download_attempts": 0,
    }
    _load_all()
    _RECORDS[task_id] = record
    _save(record)
    logger.info("media-edit %s -> %s (%s, tier %s, ref %s)", task_id, node, spec["media_kind"], tier or "-",
                record["client_ref"] or "-")
    return dict(public_status(record), status_url_string=f"/api/ai/render-status/{task_id}")


def public_status(record: Dict[str, object]) -> Dict[str, object]:
    state = str(record.get("status") or "queued")
    finished_at = float(record.get("finished_at") or 0)
    started_at = float(record.get("started_at") or 0)
    meta = dict(record.get("output_meta") or {})
    return {
        "success_bool": state != "failed",
        "task_id_string": record["task_id"],
        "status_string": state,
        "finished_bool": state in TERMINAL,
        "service_string": "media_edit",
        "node_string": record.get("node") or "",
        "stage_string": record.get("stage") or "",
        "progress_percent_float": round(float(record.get("progress") or 0), 1),
        "output_url_string": record.get("output_url") or "",
        "output_meta": {"w": int(meta.get("w") or 0), "h": int(meta.get("h") or 0),
                        "duration": float(meta.get("duration") or 0), "bytes": int(meta.get("bytes") or 0),
                        "fps": float(meta.get("fps") or 0), "container": meta.get("container") or "",
                        "codec": meta.get("codec") or "", "audio": bool(meta.get("audio")),
                        "download_attempts_int": int(meta.get("download_attempts_int")
                                                     or record.get("download_attempts") or 0)},
        "source_meta": dict(record.get("source_meta") or {}),
        "encoder_string": record.get("encoder") or "",
        "download_attempts_int": int(record.get("download_attempts") or 0),
        "retried_from_node_string": record.get("retried_from") or "",
        "error_string": record.get("error") or "",
        "tier_string": record.get("tier") or "",
        "client_ref_string": record.get("client_ref") or "",
        "media_kind_string": record.get("media_kind") or "",
        "queue_position_int": int(record.get("queue_position") or 0),
        "created_at_unix_float": float(record.get("created_at") or 0),
        "started_at_unix_float": started_at,
        "finished_at_unix_float": finished_at,
        "render_seconds_float": (round(float(record["node_elapsed"]), 1) if record.get("node_elapsed")
                                 else round(finished_at - started_at, 1) if finished_at and started_at else 0),
        "total_seconds_float": (round(finished_at - float(record.get("created_at") or 0), 1)
                                if finished_at else 0),
        "server_time_unix_int": int(time.time()),
    }


# ---------------------------------------------------------------- advancing
def apply_node_status(record: Dict[str, object], raw: Dict[str, object], now: Optional[float] = None) -> str:
    """Fold a node status into the record. Returns 'publish' when the output is ready to fetch."""
    now = time.time() if now is None else now
    record["last_seen_at"] = now
    node_state = str(raw.get("status") or "")
    state = NODE_STATE.get(node_state, "queued")
    record["stage"] = str(raw.get("stage") or record.get("stage") or "")
    record["progress"] = float(raw.get("progress") or 0)
    record["encoder"] = str(raw.get("encoder") or record.get("encoder") or "")
    record["queue_position"] = int(raw.get("queue_position") or 0)
    if raw.get("download_attempts"):
        record["download_attempts"] = int(raw["download_attempts"])
    if raw.get("source_meta"):
        record["source_meta"] = raw["source_meta"]
    # Backend clock, not the node's: node clocks drift (f13 ran 4 min behind, 2026-11-07); the node's own
    # elapsed_seconds is the render time.
    if state != "queued" and not record.get("started_at"):
        record["started_at"] = now
    if raw.get("elapsed_seconds"):
        record["node_elapsed"] = round(float(raw["elapsed_seconds"]), 2)
    if state == "completed":
        record["output_meta"] = dict(raw.get("output_meta") or {})
        if raw.get("output_ready"):
            record["status"], record["stage"] = "rendering", "publishing"
            record["progress"] = 99.0
            return "publish"
        record.update(status="failed", error="output_lost: the node no longer has the result",
                      finished_at=now)
        return ""
    if state in ("failed", "cancelled"):
        record.update(status=state, error=str(raw.get("error") or state)[:2000],
                      finished_at=float(raw.get("completed_at") or now))
        return ""
    record["status"] = state
    started = float(record.get("started_at") or 0)
    if started and now - started > TASK_TIMEOUT_SECONDS + 300:
        record.update(status="failed", error="timeout: the task ran longer than 2 hours", finished_at=now)
        return "cancel"
    return ""


def _output_location(record: Dict[str, object], name: str) -> Tuple[pathlib.Path, str]:
    day = time.strftime("%Y%m%d", time.gmtime(float(record.get("created_at") or time.time())))
    token = secrets.token_hex(16)
    path = OUTPUT_DIR / day / token / name
    return path, f"{OUTPUT_URL}/{day}/{token}/{name}"


async def _publish(client: httpx.AsyncClient, worker: Dict[str, object], record: Dict[str, object]) -> None:
    container = str((record.get("output_meta") or {}).get("container") or "bin")
    if container not in ("jpg", "png", "mp4", "mov"):
        container = "bin"
    path, url = _output_location(record, f"result.{container}")
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    size = 0
    try:
        async with client.stream("GET", ai_vision_api._ai_base(worker) + f"/media-edit/output/{record['node_task_id']}",
                                 headers=_auth(worker), timeout=httpx.Timeout(120.0, connect=15.0)) as response:
            if response.status_code != 200:
                raise RuntimeError(f"node answered HTTP {response.status_code} for the output")
            with open(part, "wb") as handle:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    handle.write(chunk)
                    size += len(chunk)
        expected = int((record.get("output_meta") or {}).get("bytes") or 0)
        if size == 0 or (expected and size != expected):
            raise RuntimeError(f"output truncated: {size} of {expected} bytes")
        os.replace(part, path)
    except Exception:
        try:
            part.unlink()
        except OSError:
            pass
        shutil.rmtree(path.parent, ignore_errors=True)
        raise
    record.update(status="completed", stage="done", progress=100.0, output_url=url, output_path=str(path),
                  finished_at=time.time())
    record.setdefault("output_meta", {})["bytes"] = size
    try:
        await client.post(ai_vision_api._ai_base(worker) + f"/media-edit/release/{record['node_task_id']}",
                          headers=_auth(worker), timeout=15.0)
    except Exception:
        pass                                     # the node drops it after its keep time anyway


async def advance(task_id: str) -> Optional[Dict[str, object]]:
    """Bring one record up to date with its node (publishing the result when it is ready)."""
    record = _record(task_id)
    if record is None:
        return None
    if record.get("status") in TERMINAL:
        return record
    async with _lock(task_id):
        if record.get("status") in TERMINAL:
            return record
        worker = _worker(str(record["node"]))
        now = time.time()
        if worker is None:
            record.update(status="failed", error=f"node_lost: {record['node']} is no longer configured",
                          finished_at=now)
            _save(record)
            return record
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    ai_vision_api._ai_base(worker) + f"/media-edit/status/{record['node_task_id']}",
                    headers=_auth(worker), timeout=15.0)
            except Exception:
                response = None
            if response is None or response.status_code not in (200, 404):
                if now - float(record.get("last_seen_at") or now) > NODE_LOST_SECONDS:
                    record.update(status="failed", finished_at=now,
                                  error=f"node_unreachable: {record['node']} did not answer for 15 min")
                    _save(record)
                return record
            if response.status_code == 404:
                record.update(status="failed", finished_at=now,
                              error=f"node_lost: {record['node']} restarted and lost the task; submit it again")
                _save(record)
                return record
            action = apply_node_status(record, response.json() or {}, now)
            if action == "publish":
                try:
                    await _publish(client, worker, record)
                    logger.info("media-edit %s completed on %s: %s bytes in %.1f s", task_id, record["node"],
                                (record.get("output_meta") or {}).get("bytes"),
                                float(record["finished_at"]) - float(record["created_at"]))
                except Exception as exc:
                    count = int(record.get("publish_failures") or 0) + 1
                    record["publish_failures"] = count
                    logger.warning("media-edit %s: publishing failed (%d): %s", task_id, count, exc)
                    if count >= 5:
                        record.update(status="failed", finished_at=time.time(),
                                      error=f"publish_failed: {str(exc)[:300]}")
            elif action == "cancel":
                await _cancel_on_node(client, worker, record)
            if retry_wanted(record):
                await _retry_elsewhere(client, record)
        if record.get("status") in TERMINAL:
            record.pop("retry_body", None)
        _save(record)
        return record


RETRYABLE_NODE_ERRORS = ("source_download_failed",)


def retry_wanted(record: Dict[str, object]) -> bool:
    """A node that could not fetch the source gets one more try on another box before the client sees it."""
    return (record.get("status") == "failed" and not record.get("retried_from")
            and bool(record.get("retry_body"))
            and str(record.get("error") or "").startswith(RETRYABLE_NODE_ERRORS))


async def _retry_elsewhere(client: httpx.AsyncClient, record: Dict[str, object]) -> bool:
    old = str(record["node"])
    reason = str(record.get("error") or "")[:300]
    ranked = [(w, i) for w, i in await pick_nodes(client) if ai_vision_api._node_key(w) != old]
    for worker, info in ranked[:2]:
        node = ai_vision_api._node_key(worker)
        _RECENT_PICKS.setdefault(node, deque()).append(time.monotonic())
        try:
            response = await client.post(ai_vision_api._ai_base(worker) + "/media-edit",
                                         json=record["retry_body"], headers=_auth(worker), timeout=30.0)
        except Exception:
            continue
        if response.status_code not in (200, 202):
            continue
        node_task = str((response.json() or {}).get("task_id") or "")
        if not node_task:
            continue
        record.update(node=node, node_task_id=node_task, status="queued", stage="queued", progress=0.0,
                      error="", finished_at=0.0, started_at=0.0, last_seen_at=time.time(), retried_from=old,
                      retry_reason=reason, node_version=info.get("version") or "", output_meta={},
                      queue_position=0)
        logger.warning("media-edit %s: %s could not fetch the source (%s); retried on %s",
                       record["task_id"], old, reason[:120], node)
        return True
    record["retried_from"] = old                 # tried: no second attempt
    record["error"] = reason + " (no other node took the retry)"
    return False


async def _cancel_on_node(client: httpx.AsyncClient, worker: Dict[str, object], record: Dict[str, object]) -> None:
    try:
        await client.post(ai_vision_api._ai_base(worker) + f"/media-edit/cancel/{record['node_task_id']}",
                          headers=_auth(worker), timeout=15.0)
    except Exception:
        logger.warning("media-edit %s: cancel request to %s failed", record["task_id"], record["node"])


async def status_response(task_id: str) -> Dict[str, object]:
    if not is_media_task_id(task_id):
        raise _error(400, "malformed_task_id", "task_id must look like me.<node>.<id>")
    record = await advance(task_id)
    if record is None:
        raise _error(404, "task_not_found", "No such media-edit task")
    return public_status(record)


@router.get("/api/media-edit/{task_id}")
async def api_media_edit_status(task_id: str):
    return await status_response(task_id)


@router.post("/api/media-edit/{task_id}/cancel")
async def api_media_edit_cancel(task_id: str, request: Request):
    caller_tier(request)
    if not is_media_task_id(task_id):
        raise _error(400, "malformed_task_id", "task_id must look like me.<node>.<id>")
    record = _record(task_id)
    if record is None:
        raise _error(404, "task_not_found", "No such media-edit task")
    if record.get("status") not in TERMINAL:
        async with _lock(task_id):
            worker = _worker(str(record["node"]))
            if worker is not None:
                async with httpx.AsyncClient() as client:
                    await _cancel_on_node(client, worker, record)
            record.update(status="cancelled", stage="cancelled", error="cancelled", finished_at=time.time())
            record.pop("retry_body", None)
            _save(record)
    return public_status(record)


# ------------------------------------------------------------ background work
def purge(now: Optional[float] = None) -> Tuple[int, int]:
    """Delete published files older than KEEP_SECONDS and records older than a week; (files, records)."""
    now = time.time() if now is None else now
    files = records = 0
    _load_all()
    for task_id, record in list(_RECORDS.items()):
        finished = float(record.get("finished_at") or record.get("created_at") or 0)
        path = str(record.get("output_path") or "")
        if path and now - finished > KEEP_SECONDS:
            shutil.rmtree(os.path.dirname(path), ignore_errors=True)
            record["output_path"] = ""
            record["output_url"] = ""
            record["output_expired"] = True
            _save(record)
            files += 1
        if record.get("status") in TERMINAL and now - finished > RECORD_KEEP_SECONDS:
            _RECORDS.pop(task_id, None)
            _LOCKS.pop(task_id, None)
            try:
                (_tasks_dir() / (task_id + ".json")).unlink()
            except OSError:
                pass
            records += 1
    # Orphans (a crash between download and the record write): any day folder past the keep time.
    try:
        for day in OUTPUT_DIR.iterdir():
            for token in day.iterdir() if day.is_dir() else []:
                try:
                    if now - token.stat().st_mtime > KEEP_SECONDS + 3600:
                        shutil.rmtree(token, ignore_errors=True)
                except OSError:
                    pass
            try:
                if day.is_dir() and not any(day.iterdir()):
                    day.rmdir()
            except OSError:
                pass
    except FileNotFoundError:
        pass
    return files, records


async def _watch_loop() -> None:
    last_purge = 0.0
    while True:
        try:
            _load_all()
            active = [tid for tid, r in list(_RECORDS.items()) if r.get("status") not in TERMINAL]
            for start in range(0, len(active), 8):
                await asyncio.gather(*(advance(tid) for tid in active[start:start + 8]), return_exceptions=True)
            if time.monotonic() - last_purge > 600:
                last_purge = time.monotonic()
                files, records = await asyncio.to_thread(purge)
                if files or records:
                    logger.info("media-edit purge: %d published file(s), %d record(s) removed", files, records)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("media-edit watch pass failed")
        await asyncio.sleep(WATCH_INTERVAL_SECONDS)


def start_watcher() -> Optional["asyncio.Task"]:
    """Started from main.py's lifespan: advances running tasks (publishes results) and purges old outputs."""
    if os.getenv("MEDIA_EDIT_WATCH", "1").strip().lower() in ("0", "false", "no"):
        return None
    return asyncio.create_task(_watch_loop(), name="media-edit-watch")


# ---------------------------------------------------------------------- fleet
async def fleet_object(client: httpx.AsyncClient) -> Dict[str, object]:
    """Which AI nodes offer media edit and their ffmpeg slots (for /api/ai/fleet)."""
    workers = ai_vision_api._load_ai_workers()
    probes = await asyncio.gather(*(_probe(client, w) for w in workers), return_exceptions=True)
    nodes: Dict[str, object] = {}
    capable = free = running = pending = 0
    for worker, info in zip(workers, probes):
        key = ai_vision_api._node_key(worker)
        if not isinstance(info, dict):
            nodes[key] = {"capable": False, "online": False}
            continue
        nodes[key] = dict(info, online=True)
        if info.get("capable"):
            capable += 1
            free += int(info.get("free_slots") or 0)
            running += int(info.get("running") or 0)
            pending += int(info.get("pending") or 0)
    active = sum(1 for r in _RECORDS.values() if r.get("status") not in TERMINAL)
    return {"nodes_object": nodes, "capable_nodes_int": capable, "free_slots_int": free,
            "running_int": running, "queued_int": pending, "tracked_active_int": active}
