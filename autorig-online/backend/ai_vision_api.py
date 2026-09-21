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
]
DEFAULT_MODEL_ID = "bonsai2-27b"

MAX_PROMPT_CHARS = 8000
MAX_INLINE_IMAGE_BYTES = 12 * 1024 * 1024
SUBMIT_TIMEOUT_SECONDS = 60.0
STATUS_TIMEOUT_SECONDS = 30.0
# A cold node loads 7 GB of weights before the first answer, so the wait a
# caller may ask us to hold for has to allow for that plus the inference.
MAX_WAIT_SECONDS = 180.0


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


def _load_ai_workers() -> List[Dict[str, object]]:
    """Farm nodes that can answer an AI request, with their per-node token.

    The node list and tokens are shared with Hunyuan, but its `enabled` flag is
    not: a node parked for a Hunyuan bake bug still reads images perfectly well.
    Only `ai_vision_enabled: false` parks a node for AI, and whether it is
    actually free is settled by probing it, not by this file.
    """
    try:
        raw = json.loads(pathlib.Path(WORKERS_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("AI worker list %s does not exist", WORKERS_FILE)
        return []
    except Exception:
        logger.exception("Could not read the AI worker list %s", WORKERS_FILE)
        return []
    entries = raw.get("workers") if isinstance(raw, dict) else raw
    workers: List[Dict[str, object]] = []
    seen_nodes = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip().rstrip("/")
        token = str(entry.get("token") or "").strip()
        if not url or not token:
            continue
        if entry.get("ai_vision_enabled") is False:
            continue
        node = str(entry.get("physical_node") or entry.get("name") or url).strip().lower()
        if node in seen_nodes:
            continue
        seen_nodes.add(node)
        workers.append({
            "name": str(entry.get("name") or node),
            "url": url,
            "token": token,
            "physical_node": node,
        })
    return workers


def _node_key(worker: Dict[str, object]) -> str:
    """Stable, URL-safe node label used as the task id prefix."""
    name = str(worker.get("physical_node") or worker.get("name") or worker.get("url"))
    return "".join(ch if ch.isalnum() else "-" for ch in name.strip().lower()).strip("-")


def _ai_base(worker: Dict[str, object]) -> str:
    """Worker URLs are bare origins; the converter API lives under this path."""
    return str(worker["url"]).rstrip("/") + "/api-converter-glb"


async def _node_is_free(client: httpx.AsyncClient, worker: Dict[str, object]) -> Tuple[bool, int]:
    """Return (reachable, queued+running) so the least loaded node can be picked."""
    url = _ai_base(worker) + "/server-status"
    try:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=10.0,
        )
        if response.status_code != 200:
            return False, 0
        payload = response.json()
    except Exception:
        return False, 0
    queued = int(payload.get("queue_size") or 0)
    active = payload.get("active_tasks")
    try:
        active_count = int(active or 0)
    except (TypeError, ValueError):
        active_count = 0
    return True, queued + active_count


async def _pick_worker(client: httpx.AsyncClient) -> Dict[str, object]:
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
    candidates = []
    for worker, probe in zip(workers, probes):
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            continue
        reachable, load = probe
        if reachable:
            candidates.append((load, worker))
    if not candidates:
        raise HTTPException(
            status_code=503,
            detail={"error_string": "no_node_available",
                    "message_string": "No farm node answered; try again shortly"},
        )
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


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


def _public_status(task_id: str, model_id: str, raw: Dict[str, object]) -> Dict[str, object]:
    status = str(raw.get("status") or "")
    finished = status in ("Completed", "Failed")
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
) -> Dict[str, object]:
    model_id = str(request_model["id"])
    async with httpx.AsyncClient() as client:
        worker = await _pick_worker(client)
        worker_task_id = await _submit(client, worker, path, payload)
        task_id = f"{_node_key(worker)}.{worker_task_id}"
        raw: Dict[str, object] = {"status": "Pending"}
        if wait_seconds and wait_seconds > 0:
            deadline = time.monotonic() + min(float(wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(2)
                raw = await _fetch_status(client, worker, worker_task_id)
                if str(raw.get("status")) in ("Completed", "Failed"):
                    break
        result = _public_status(task_id, model_id, raw)
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
    return await _run(model, "/ai-vision", payload, body.wait_seconds)


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
    return await _run(model, "/text2text", payload, body.wait_seconds)


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
    result = _public_status(task_id, DEFAULT_MODEL_ID, raw)
    result["node_string"] = node_key
    return result
