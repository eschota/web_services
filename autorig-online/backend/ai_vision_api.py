"""Public AI Vision / text2text API backed by the converter farm.

The farm converters run the language model themselves at the `ai_vision`
workload class, so this module only has to choose a node, hand it the request
and report back. Work is never executed here.

A task id is returned as ``<node>.<worker task id>`` so a later status call can
find the node that owns it without this process keeping a routing table that a
restart would lose.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import pathlib
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

# One catalogue entry per model a caller may ask for. `worker_model` is empty
# while a node serves exactly one local model; it becomes the selector once a
# node offers more than one.
AI_MODELS: List[Dict[str, object]] = [
    {
        "id": "bonsai2-27b",
        "title": "Bonsai 2 27B",
        "description": (
            "Ternary 27B vision-language model running on the farm's own GPUs. "
            "Reads images and answers in plain text."
        ),
        "modes": ["vision", "text"],
        "context_tokens": 4096,
        "max_output_tokens": 2048,
        "hosting": "local-farm",
        "default": True,
    },
    {
        "id": "qwen35-9b-uncensored",
        "title": "Qwen3.5 9B Defiant Fable",
        "description": (
            "Uncensored 9B vision-language model on the farm's own GPUs. "
            "Answers without the refusals of a stock assistant model."
        ),
        "modes": ["vision", "text"],
        "context_tokens": 8192,
        "max_output_tokens": 2048,
        "hosting": "local-farm",
        "uncensored": True,
        "default": False,
    },
]
DEFAULT_MODEL_ID = "bonsai2-27b"

MAX_PROMPT_CHARS = 8000
MAX_INLINE_IMAGE_BYTES = 12 * 1024 * 1024
SUBMIT_TIMEOUT_SECONDS = 60.0
STATUS_TIMEOUT_SECONDS = 30.0
# A cold node loads 7 GB of weights before the first answer, so the wait a
# caller may ask us to hold for has to allow for that plus the inference.
MAX_WAIT_SECONDS = 180.0

# Completed durations, so the fleet view can show what these services actually
# take instead of a constant. Kept in memory on purpose: a rolling window of
# recent jobs is what an estimate should follow, and it costs nothing to lose.
RECENT_DURATIONS: Dict[str, deque] = {
    "vision": deque(maxlen=40),
    "text": deque(maxlen=40),
    "3dmodel": deque(maxlen=20),
}


def record_duration(service_id: str, seconds: float) -> None:
    bucket = RECENT_DURATIONS.get(service_id)
    if bucket is not None and seconds and seconds > 0:
        bucket.append(float(seconds))


class VisionRequest(BaseModel):
    prompt: str = Field(..., description="Question about the image")
    image_url: Optional[str] = Field(None, description="Public http(s) URL of the image")
    image_base64: Optional[str] = Field(None, description="Inline image, base64 or data URL")
    model: Optional[str] = Field(None, description="Model id from /api/ai/models")
    max_output_tokens: Optional[int] = Field(None, ge=1, le=8192)
    wait_seconds: Optional[float] = Field(
        None, ge=0, le=MAX_WAIT_SECONDS,
        description="Hold the response until the answer is ready, up to this long",
    )


class TextRequest(BaseModel):
    prompt: str = Field(..., description="The prompt")
    model: Optional[str] = Field(None, description="Model id from /api/ai/models")
    max_output_tokens: Optional[int] = Field(None, ge=1, le=8192)
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


def _model_entry(model_id: Optional[str]) -> Dict[str, object]:
    wanted = str(model_id or DEFAULT_MODEL_ID).strip().lower()
    for entry in AI_MODELS:
        if str(entry["id"]).lower() == wanted:
            return entry
    raise HTTPException(
        status_code=400,
        detail={
            "error_string": "unknown_model",
            "message_string": f"Unknown model '{model_id}'",
            "available_models_array": [entry["id"] for entry in AI_MODELS],
        },
    )


WORKERS_FILE = os.getenv(
    "RENDERFIN_HUNYUAN_WORKERS_FILE", "/etc/autorig-renderfin-hunyuan.json"
)


def _worker_entries() -> List[Tuple[Dict[str, object], Dict[str, object]]]:
    """(normalised worker, raw file entry) pairs, one per physical node."""
    try:
        raw = json.loads(pathlib.Path(WORKERS_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("Worker list %s does not exist", WORKERS_FILE)
        return []
    except Exception:
        logger.exception("Could not read the worker list %s", WORKERS_FILE)
        return []
    entries = raw.get("workers") if isinstance(raw, dict) else raw
    pairs = []
    seen_nodes = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip().rstrip("/")
        token = str(entry.get("token") or "").strip()
        if not url or not token:
            continue
        node = str(entry.get("physical_node") or entry.get("name") or url).strip().lower()
        if node in seen_nodes:
            continue
        seen_nodes.add(node)
        pairs.append(({
            "name": str(entry.get("name") or node),
            "url": url,
            "token": token,
            "physical_node": node,
        }, entry))
    return pairs


def _load_hunyuan_workers() -> List[Dict[str, object]]:
    """Nodes allowed to run Hunyuan, which is a narrower set than AI Vision.

    Here the file's `enabled` flag does matter: those entries are parked for
    Hunyuan-specific reasons — a bake contract failure, a torch crash, capacity
    held back for conversion — and ignoring them would send 3D work to a node
    somebody deliberately took out of 3D.
    """
    workers = []
    for entry, raw in _worker_entries():
        if raw.get("enabled") is False or raw.get("disabled") is True:
            continue
        if raw.get("canary_approved") is False:
            continue
        workers.append(entry)
    return workers


def _load_ai_workers() -> List[Dict[str, object]]:
    """Farm nodes that can answer an AI request, with their per-node token.

    The node list and tokens are shared with Hunyuan, but its `enabled` flag is
    not: a node parked for a Hunyuan bake bug still reads images perfectly well.
    Only `ai_vision_enabled: false` parks a node for AI, and whether it is
    actually free is settled by probing it, not by this file.
    """
    return [entry for entry, raw in _worker_entries()
            if raw.get("ai_vision_enabled") is not False]


def _node_key(worker: Dict[str, object]) -> str:
    """Stable, URL-safe node label used as the task id prefix."""
    name = str(worker.get("physical_node") or worker.get("name") or worker.get("url"))
    return "".join(ch if ch.isalnum() else "-" for ch in name.strip().lower()).strip("-")


def _ai_base(worker: Dict[str, object]) -> str:
    """Worker URLs are bare origins; the converter API lives under this path."""
    return str(worker["url"]).rstrip("/") + "/api-converter-glb"


async def _node_is_free(client: httpx.AsyncClient, worker: Dict[str, object]) -> Tuple[bool, Dict[str, object]]:
    """Return (reachable, {load, models, loaded}) so a node can be chosen on facts."""
    url = _ai_base(worker) + "/server-status"
    try:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=10.0,
        )
        if response.status_code != 200:
            return False, {}
        payload = response.json()
    except Exception:
        return False, {}
    # Real occupancy lives in tasks_summary; the top level has no such fields,
    # and reading them made every node look idle, so AI work queued behind a
    # twenty-minute conversion instead of going to a free node.
    summary = payload.get("tasks_summary")
    queued = 0
    active_count = 0
    if isinstance(summary, dict):
        try:
            queued = int(summary.get("queue_size") or summary.get("pending") or 0)
            active_count = int(summary.get("processing") or 0)
        except (TypeError, ValueError):
            queued, active_count = 0, 0
    catalogue = payload.get("ai_models")
    models = []
    loaded = ""
    if isinstance(catalogue, dict):
        loaded = str(catalogue.get("loaded_model") or "")
        for entry in catalogue.get("models") or []:
            if isinstance(entry, dict) and entry.get("id"):
                models.append(str(entry["id"]))
    return True, {"load": queued + active_count, "models": models, "loaded": loaded}


async def _pick_worker(
    client: httpx.AsyncClient, model_id: Optional[str] = None
) -> Dict[str, object]:
    """The least loaded reachable node that carries the requested model."""
    workers = _load_ai_workers()
    if not workers:
        raise HTTPException(
            status_code=503,
            detail={"error_string": "no_workers_configured",
                    "message_string": "No AI-capable farm nodes are configured"},
        )
    probes = await asyncio.gather(
        *(_node_is_free(client, worker) for worker in workers), return_exceptions=True
    )
    reachable = []
    for worker, probe in zip(workers, probes):
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            continue
        ok, info = probe
        if ok and isinstance(info, dict):
            reachable.append((worker, info))
    if not reachable:
        raise HTTPException(
            status_code=503,
            detail={"error_string": "no_node_available",
                    "message_string": "No farm node answered; try again shortly"},
        )
    wanted = str(model_id or "").strip()
    carrying = [(w, i) for w, i in reachable if not wanted or wanted in (i.get("models") or [])]
    if not carrying:
        # Older nodes publish no catalogue at all; treat that as "unknown, try it"
        # rather than refusing work a node may well be able to do.
        silent = [(w, i) for w, i in reachable if not (i.get("models") or [])]
        if not silent:
            raise HTTPException(status_code=503, detail={
                "error_string": "model_not_on_any_node",
                "message_string": f"No reachable node carries '{wanted}'",
                "nodes_checked_int": len(reachable)})
        carrying = silent
    # Load first, warmth only as a tie-break: swapping weights costs seconds,
    # but queueing behind a conversion costs however long that conversion runs.
    carrying.sort(key=lambda item: (
        int(item[1].get("load") or 0),
        0 if wanted and item[1].get("loaded") == wanted else 1,
    ))
    return carrying[0][0]


def _worker_by_key(key: str) -> Optional[Dict[str, object]]:
    for worker in _load_ai_workers():
        if _node_key(worker) == key:
            return worker
    return None


def _validate_prompt(raw: str) -> str:
    prompt = str(raw or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail={
            "error_string": "prompt_required",
            "message_string": "prompt must not be empty"})
    if len(prompt) > MAX_PROMPT_CHARS:
        raise HTTPException(status_code=400, detail={
            "error_string": "prompt_too_long",
            "message_string": f"prompt exceeds {MAX_PROMPT_CHARS} characters"})
    return prompt


def _decode_inline_image(raw: str) -> bytes:
    payload = str(raw or "").strip()
    if payload.startswith("data:"):
        _, _, payload = payload.partition(",")
    try:
        data = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(status_code=400, detail={
            "error_string": "image_not_base64",
            "message_string": "image_base64 is not valid base64"}) from None
    if not data:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_empty", "message_string": "image_base64 decoded to nothing"})
    if len(data) > MAX_INLINE_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail={
            "error_string": "image_too_large",
            "message_string": f"inline image exceeds {MAX_INLINE_IMAGE_BYTES} bytes"})
    return data


async def _publish_inline_image(client: httpx.AsyncClient, data: bytes) -> str:
    """Park an inline image on the public scratch store the farm can read.

    The converter only accepts a public URL, by the same SSRF rule that guards
    Hunyuan, so bytes a caller pasted have to become one first.
    """
    try:
        response = await client.post(
            "https://autorig.online/dev/api/scratch",
            files={"file": ("upload.png", data, "image/png")},
            timeout=60.0,
        )
        response.raise_for_status()
        url = str((response.json() or {}).get("url") or "").strip()
    except Exception:
        logger.exception("Could not publish an inline image for the farm")
        raise HTTPException(status_code=502, detail={
            "error_string": "image_publish_failed",
            "message_string": "Could not stage the uploaded image"}) from None
    if not url:
        raise HTTPException(status_code=502, detail={
            "error_string": "image_publish_failed",
            "message_string": "Image store returned no URL"})
    return url


async def _submit(
    client: httpx.AsyncClient,
    worker: Dict[str, object],
    path: str,
    payload: Dict[str, object],
) -> str:
    url = _ai_base(worker) + path
    try:
        response = await client.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=SUBMIT_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.exception("AI submission to %s failed", url)
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_unreachable",
            "message_string": "The farm node did not accept the request"}) from None
    if response.status_code == 503:
        raise HTTPException(status_code=503, detail={
            "error_string": "worker_busy",
            "message_string": "The farm node is not accepting AI work right now",
            "retryable_bool": True})
    if response.status_code not in (200, 202):
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_rejected",
            "message_string": f"Farm node answered HTTP {response.status_code}"})
    task_id = str((response.json() or {}).get("task_id") or "").strip()
    if not task_id:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_no_task_id",
            "message_string": "Farm node accepted the request without a task id"})
    return task_id


async def _fetch_status(
    client: httpx.AsyncClient, worker: Dict[str, object], worker_task_id: str
) -> Dict[str, object]:
    url = _ai_base(worker) + f"/ai-vision/status/{worker_task_id}"
    try:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=STATUS_TIMEOUT_SECONDS,
        )
    except Exception:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_unreachable",
            "message_string": "The farm node did not answer"}) from None
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={
            "error_string": "task_not_found", "message_string": "No such task"})
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_status_failed",
            "message_string": f"Farm node answered HTTP {response.status_code}"})
    return response.json() or {}


def _public_status(task_id: str, model_id: str, raw: Dict[str, object],
                   service_id: str = "") -> Dict[str, object]:
    status = str(raw.get("status") or "")
    finished = status in ("Completed", "Failed")
    if status == "Completed" and service_id:
        record_duration(service_id, float(raw.get("elapsed_seconds") or 0))
    return {
        "success_bool": status != "Failed",
        "task_id_string": task_id,
        "status_string": status.lower() or "pending",
        "finished_bool": finished,
        "mode_string": str(raw.get("mode") or ""),
        "model_string": model_id,
        "answer_string": str(raw.get("answer") or ""),
        "reasoning_string": str(raw.get("reasoning") or ""),
        "error_string": str(raw.get("error") or ""),
        "elapsed_seconds_float": round(float(raw.get("elapsed_seconds") or 0.0), 2),
        "stage_string": str(raw.get("current_stage") or ""),
        "server_time_unix_int": int(time.time()),
    }


async def _run(
    request_model: Dict[str, object],
    path: str,
    payload: Dict[str, object],
    wait_seconds: Optional[float],
    service_id: str = "",
) -> Dict[str, object]:
    model_id = str(request_model["id"])
    async with httpx.AsyncClient() as client:
        worker = await _pick_worker(client, model_id)
        worker_task_id = await _submit(client, worker, path, dict(payload, model=model_id))
        task_id = f"{_node_key(worker)}.{worker_task_id}"
        raw: Dict[str, object] = {"status": "Pending"}
        if wait_seconds and wait_seconds > 0:
            deadline = time.monotonic() + min(float(wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(2)
                raw = await _fetch_status(client, worker, worker_task_id)
                if str(raw.get("status")) in ("Completed", "Failed"):
                    break
        result = _public_status(task_id, model_id, raw, service_id)
        result["status_url_string"] = f"/api/ai/status/{task_id}"
        result["node_string"] = _node_key(worker)
        return result


@router.get("/api/ai/models")
async def api_ai_models():
    """Models a caller may select for /api/vision and /api/text2text."""
    return {
        "success_bool": True,
        "models_array": AI_MODELS,
        "default_model_string": DEFAULT_MODEL_ID,
        "server_time_unix_int": int(time.time()),
    }


@router.get("/api/vision")
async def api_vision_docs():
    """GET mirror documenting the POST image endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/vision",
        "required_fields_array": ["prompt", "image_url or image_base64"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds"],
        "models_url_string": "/api/ai/models",
        "example_request_object": {
            "prompt": "What is in this picture?",
            "image_url": "https://example.com/photo.png",
            "model": DEFAULT_MODEL_ID,
            "wait_seconds": 120,
        },
        "example_response_object": {
            "success_bool": True,
            "task_id_string": "f1-pc.0f1c…",
            "status_string": "completed",
            "answer_string": "A black street lamp on a magenta background.",
        },
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/vision")
async def api_vision(request: Request, body: VisionRequest):
    model = _model_entry(body.model)
    prompt = _validate_prompt(body.prompt)
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64"})
    image_url = str(body.image_url or "").strip()
    if not image_url:
        async with httpx.AsyncClient() as client:
            image_url = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
    payload: Dict[str, object] = {"prompt": prompt, "image_url": image_url}
    if body.max_output_tokens:
        payload["max_output_tokens"] = int(body.max_output_tokens)
    return await _run(model, "/ai-vision", payload, body.wait_seconds, "vision")


@router.get("/api/text2text")
async def api_text2text_docs():
    """GET mirror documenting the POST text endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/text2text",
        "required_fields_array": ["prompt"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds"],
        "models_url_string": "/api/ai/models",
        "example_request_object": {"prompt": "Name three colours.", "wait_seconds": 60},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/text2text")
async def api_text2text(request: Request, body: TextRequest):
    model = _model_entry(body.model)
    payload: Dict[str, object] = {"prompt": _validate_prompt(body.prompt)}
    if body.max_output_tokens:
        payload["max_output_tokens"] = int(body.max_output_tokens)
    return await _run(model, "/text2text", payload, body.wait_seconds, "text")


@router.get("/api/ai/status/{task_id}")
async def api_ai_status(task_id: str):
    node_key, _, worker_task_id = str(task_id).partition(".")
    if not node_key or not worker_task_id:
        raise HTTPException(status_code=400, detail={
            "error_string": "malformed_task_id",
            "message_string": "task_id must look like <node>.<id>"})
    worker = _worker_by_key(node_key)
    if worker is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "unknown_node",
            "message_string": "The node that owns this task is not configured"})
    async with httpx.AsyncClient() as client:
        raw = await _fetch_status(client, worker, worker_task_id)
    mode = str(raw.get("mode") or "")
    result = _public_status(task_id, DEFAULT_MODEL_ID, raw,
                            "vision" if mode == "vision" else "text")
    result["node_string"] = node_key
    return result

# The knobs below are the ones Renderfin's RenderPrompt actually carries. They
# are optional everywhere: a caller who sends only a prompt gets exactly the
# behaviour it had before these existed.
IMAGE_MODES = ("", "z_depth", "t_pose", "open_pose", "inpaint")
VIDEO_QUALITIES = {
    "standard": "gen_animation_by_url.json",
    "hq": "gen_animation_hq_by_url.json",
}


class ImageRequest(BaseModel):
    prompt: str = Field(..., description="What to draw")
    image_url: Optional[str] = Field(None, description="Reference image URL")
    image_base64: Optional[str] = Field(None, description="Reference image, inline")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)
    mode: Optional[str] = Field(None, description="z_depth, t_pose, open_pose or inpaint")
    negative_prompt: Optional[str] = Field(None, description="What to avoid")
    width: Optional[int] = Field(None, ge=256, le=2048)
    height: Optional[int] = Field(None, ge=256, le=2048)
    steps: Optional[int] = Field(None, ge=1, le=100)
    creativity: Optional[float] = Field(None, ge=0, le=1)
    seed: Optional[int] = Field(None, ge=0, description="0 or absent randomises")


# Renderfin runs on the same host and owns the image farm; the public service
# is a thin typed face over it so /image looks like every other service.
RENDERFIN_BASE = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")


@router.get("/api/image")
async def api_image_docs():
    """GET mirror documenting the POST image endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/image",
        "required_fields_array": ["prompt"],
        "optional_fields_array": ["image_url", "image_base64", "wait_seconds",
                                  "mode", "negative_prompt", "width", "height",
                                  "steps", "creativity", "seed"],
        "produces_string": "image",
        "example_request_object": {"prompt": "a black lamp post on magenta", "wait_seconds": 120},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/image")
async def api_image(body: ImageRequest):
    """Prompt (and optionally a reference picture) into a generated image."""
    prompt = _validate_prompt(body.prompt)
    async with httpx.AsyncClient() as client:
        reference = str(body.image_url or "").strip()
        if not reference and body.image_base64:
            reference = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64)
            )
        payload: Dict[str, object] = {"prompt": prompt}
        if reference:
            payload["image_url"] = reference
        mode = str(body.mode or "").strip().lower()
        if mode and mode in IMAGE_MODES:
            # Renderfin picks the template from `type`, not from a file name.
            payload["type"] = mode
        if body.negative_prompt and str(body.negative_prompt).strip():
            payload["negative_prompt"] = str(body.negative_prompt).strip()[:MAX_PROMPT_CHARS]
        if body.width:
            payload["main_size_width"] = int(body.width)
        if body.height:
            payload["main_size_height"] = int(body.height)
        if body.steps:
            payload["steps"] = int(body.steps)
        if body.creativity is not None:
            payload["creativity"] = float(body.creativity)
        if body.seed:
            payload["noise_seed"] = int(body.seed)
        try:
            response = await client.post(
                RENDERFIN_BASE + "/api-render", json=payload, timeout=SUBMIT_TIMEOUT_SECONDS
            )
        except Exception:
            logger.exception("Renderfin did not accept an image request")
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_unreachable",
                "message_string": "The image farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_rejected",
                "message_string": f"Image farm answered HTTP {response.status_code}"})
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        task_id = str(accepted.get("task_id") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_no_output",
                "message_string": "Image farm accepted the request without an output URL"})
        # Renderfin publishes the destination URL up front and fills it in when
        # the render lands, so readiness is the file appearing, not a status row.
        ready = False
        if body.wait_seconds and body.wait_seconds > 0:
            deadline = time.monotonic() + min(float(body.wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                try:
                    head = await client.head(output_url, timeout=15.0)
                    if head.status_code == 200:
                        ready = True
                        break
                except Exception:
                    continue
        return {
            "success_bool": True,
            "task_id_string": task_id,
            "status_string": "completed" if ready else "pending",
            "finished_bool": ready,
            "image_url_string": output_url,
            "poll_url_string": output_url,
            "server_time_unix_int": int(time.time()),
        }

class VideoRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="First frame, public URL")
    image_base64: Optional[str] = Field(None, description="First frame, inline")
    prompt: Optional[str] = Field(None, description="What should happen in the clip")
    frame_count: Optional[int] = Field(None, ge=8, le=400)
    # Giving a last frame turns the clip into a journey between two pictures;
    # passing the first frame again is how a loop is made. Renderfin prunes the
    # guide node from the workflow when this is absent, so the plain animation
    # is unchanged by its existence.
    image_url_end: Optional[str] = Field(None, description="Last frame, public URL")
    image_base64_end: Optional[str] = Field(None, description="Last frame, inline")
    quality: Optional[str] = Field(None, description="standard or hq")
    negative_prompt: Optional[str] = Field(None, description="What to avoid")
    steps: Optional[int] = Field(None, ge=1, le=100)
    creativity: Optional[float] = Field(None, ge=0, le=1)
    seed: Optional[int] = Field(None, ge=0, description="0 or absent randomises")


@router.get("/api/video")
async def api_video_docs():
    """GET mirror documenting the POST video endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/video",
        "required_fields_array": ["image_url or image_base64"],
        "optional_fields_array": ["prompt", "frame_count", "image_url_end",
                                  "image_base64_end", "quality", "negative_prompt",
                                  "steps", "creativity", "seed"],
        "produces_string": "video",
        "note_string": "A clip is animated from the frame you give it; expect minutes, not seconds.",
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/video")
async def api_video(body: VideoRequest):
    """Animate a frame into a short clip on the render farm.

    Renderfin treats a request with an image and no `type` as an animation, so
    the frame is what selects the workflow; the caller never names one.
    """
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "A video is animated from a frame; provide image_url or image_base64"})
    async with httpx.AsyncClient() as client:
        frame = str(body.image_url or "").strip()
        if not frame:
            frame = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
        payload: Dict[str, object] = {"image_url": frame}
        last_frame = str(body.image_url_end or "").strip()
        if not last_frame and body.image_base64_end:
            last_frame = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64_end)
            )
        if last_frame:
            payload["image_url_end"] = last_frame
        if body.prompt and str(body.prompt).strip():
            payload["prompt"] = _validate_prompt(body.prompt)
        if body.frame_count:
            payload["frame_count"] = int(body.frame_count)
        quality = str(body.quality or "").strip().lower()
        if quality in VIDEO_QUALITIES:
            payload["work_flow"] = VIDEO_QUALITIES[quality]
        if body.negative_prompt and str(body.negative_prompt).strip():
            payload["negative_prompt"] = str(body.negative_prompt).strip()[:MAX_PROMPT_CHARS]
        if body.steps:
            payload["steps"] = int(body.steps)
        if body.creativity is not None:
            payload["creativity"] = float(body.creativity)
        if body.seed:
            payload["noise_seed"] = int(body.seed)
        try:
            response = await client.post(
                RENDERFIN_BASE + "/api-render", json=payload,
                timeout=SUBMIT_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.exception("Renderfin did not accept a video request")
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_unreachable",
                "message_string": "The render farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_rejected",
                "message_string": f"Render farm answered HTTP {response.status_code}"})
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_no_output",
                "message_string": "Render farm accepted the request without an output URL"})
        return {
            "success_bool": True,
            "task_id_string": str(accepted.get("task_id") or ""),
            "status_string": "pending",
            "finished_bool": False,
            "video_url_string": output_url,
            "poll_url_string": output_url,
            "source_image_url_string": frame,
            "end_image_url_string": last_frame,
            "server_time_unix_int": int(time.time()),
        }

class ModelRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Picture of the subject")
    image_base64: Optional[str] = Field(None, description="Picture, inline")
    quality: Optional[str] = Field(None, description="draft, standard or high")
    background_method: Optional[str] = Field(None, description="auto, alpha or solid")


@router.get("/api/3dmodel")
async def api_3dmodel_docs():
    """GET mirror documenting the POST 3D endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/3dmodel",
        "required_fields_array": ["image_url or image_base64"],
        "optional_fields_array": ["quality", "background_method"],
        "produces_string": "model3d",
        "note_string": "Hunyuan3D on the farm; minutes, and only nodes cleared for 3D take it.",
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/3dmodel")
async def api_3dmodel(body: ModelRequest):
    """Turn a picture into a 3D model on a farm node cleared for Hunyuan."""
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64"})
    async with httpx.AsyncClient() as client:
        picture = str(body.image_url or "").strip()
        if not picture:
            picture = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
        workers = _load_hunyuan_workers()
        if not workers:
            raise HTTPException(status_code=503, detail={
                "error_string": "no_3d_node_available",
                "message_string": "No farm node is currently cleared for 3D generation"})
        probes = await asyncio.gather(
            *(_node_is_free(client, worker) for worker in workers),
            return_exceptions=True,
        )
        candidates = []
        for worker, probe in zip(workers, probes):
            if isinstance(probe, Exception) or not isinstance(probe, tuple):
                continue
            ok, info = probe
            if ok:
                candidates.append((int((info or {}).get("load") or 0), worker))
        if not candidates:
            raise HTTPException(status_code=503, detail={
                "error_string": "no_3d_node_available",
                "message_string": "No 3D-capable node answered; try again shortly"})
        candidates.sort(key=lambda item: item[0])
        worker = candidates[0][1]

        payload: Dict[str, object] = {"image_url": picture}
        if body.quality:
            payload["quality"] = str(body.quality).strip().lower()
        if body.background_method:
            payload["background_method"] = str(body.background_method).strip().lower()
        worker_task_id = await _submit(client, worker, "/generate-3d", payload)
        return {
            "success_bool": True,
            "task_id_string": f"{_node_key(worker)}.{worker_task_id}",
            "status_string": "pending",
            "finished_bool": False,
            "status_url_string": f"/api/3dmodel/status/{_node_key(worker)}.{worker_task_id}",
            "node_string": _node_key(worker),
            "source_image_url_string": picture,
            "server_time_unix_int": int(time.time()),
        }


@router.get("/api/3dmodel/status/{task_id}")
async def api_3dmodel_status(task_id: str):
    """Hunyuan has its own status path, so 3D tasks cannot share /api/ai/status."""
    node_key, _, worker_task_id = str(task_id).partition(".")
    if not node_key or not worker_task_id:
        raise HTTPException(status_code=400, detail={
            "error_string": "malformed_task_id",
            "message_string": "task_id must look like <node>.<id>"})
    worker = next((w for w in _load_hunyuan_workers()
                   if _node_key(w) == node_key), None)
    if worker is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "unknown_node",
            "message_string": "The node that owns this task is not configured for 3D"})
    url = _ai_base(worker) + f"/generate-3d/status/{worker_task_id}"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                url, headers={"Authorization": f"Bearer {worker['token']}"},
                timeout=STATUS_TIMEOUT_SECONDS,
            )
        except Exception:
            raise HTTPException(status_code=502, detail={
                "error_string": "worker_unreachable",
                "message_string": "The farm node did not answer"}) from None
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={
            "error_string": "task_not_found", "message_string": "No such task"})
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_status_failed",
            "message_string": f"Farm node answered HTTP {response.status_code}"})
    raw = response.json() or {}
    status = str(raw.get("status") or "")
    outputs = raw.get("output_urls")
    model_url = ""
    preview_url = ""
    if isinstance(outputs, dict):
        for key in ("glb", "high", "high_textured", "model", "standard"):
            value = outputs.get(key)
            if isinstance(value, str) and value.strip():
                model_url = value.strip()
                break
        for key in ("preview", "preview_front", "thumbnail"):
            value = outputs.get(key)
            if isinstance(value, str) and value.strip():
                preview_url = value.strip()
                break
    if status == "Completed":
        record_duration("3dmodel", float(raw.get("elapsed_seconds") or 0))
    return {
        "success_bool": status != "Failed",
        "task_id_string": task_id,
        "status_string": status.lower() or "pending",
        "finished_bool": status in ("Completed", "Failed"),
        "model_url_string": model_url,
        "preview_url_string": preview_url,
        "outputs_object": outputs if isinstance(outputs, dict) else {},
        "error_string": str(raw.get("error") or ""),
        "stage_string": str(raw.get("current_stage") or ""),
        "progress_int": int(raw.get("progress") or 0),
        "elapsed_seconds_float": round(float(raw.get("elapsed_seconds") or 0.0), 2),
        "node_string": node_key,
        "server_time_unix_int": int(time.time()),
    }
