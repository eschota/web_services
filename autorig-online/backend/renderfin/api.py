"""Public HTTP surface of the renderfin service (C#-compatible /api-render)."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from .models import RenderPrompt, RenderServer

router = APIRouter(prefix="/renderfin")


def _queue(request: Request):
    return request.app.state.render_queue


def _registry(request: Request):
    return request.app.state.registry


def _chargen(request: Request):
    return request.app.state.character_gen


def _looks_like_server(body: Dict[str, Any]) -> bool:
    return bool(
        body.get("render_server_url")
        or body.get("render_operation")
        or (body.get("render_server_name") and "prompt" not in body and "image_url" not in body)
    )


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    queue = _queue(request)
    tasks = queue.all_tasks()
    return {
        "ok": True,
        "servers": len(_registry(request).all()),
        "pending": sum(1 for t in tasks if t.status == "Pending"),
        "rendering": sum(1 for t in tasks if t.status == "Rendering"),
    }


@router.get("/api-render")
async def api_render_get(request: Request) -> Dict[str, Any]:
    return {
        "servers": [s.model_dump() for s in _registry(request).all()],
        "tasks": [t.public_dict() for t in _queue(request).all_tasks()[:100]],
    }


@router.post("/api-render")
async def api_render_post(request: Request) -> Dict[str, Any]:
    try:
        body = json.loads(await request.body() or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"invalid json: {exc}")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a json object")

    if _looks_like_server(body):
        server = RenderServer(**{k: v for k, v in body.items() if k in RenderServer.model_fields})
        result = _registry(request).handle_operation(server)
        return result

    prompt = RenderPrompt(**{k: v for k, v in body.items() if k in RenderPrompt.model_fields})
    if not (prompt.prompt or "").strip() and not (prompt.image_url or "").strip():
        raise HTTPException(status_code=400, detail="prompt or image_url required")
    task = await _queue(request).enqueue(prompt)
    return {"output_url": task.output_url, "task_id": task.id}


@router.api_route("/api-render-get-task-by-url", methods=["GET", "POST"])
async def api_render_get_task_by_url(
    request: Request, url: Optional[str] = Query(default=None)
) -> Any:
    target = (url or "").strip()
    if not target and request.method == "POST":
        try:
            body = json.loads(await request.body() or b"{}")
            target = str(body.get("url") or body.get("output_url") or "").strip()
        except json.JSONDecodeError:
            target = ""
    if not target:
        raise HTTPException(status_code=400, detail="url required")
    task = _queue(request).find_by_output_url(target)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return [task.public_dict()]


class CharacterGenRequest(BaseModel):
    prompt: str
    negative_prompt: str = ""
    mask_url: str = ""
    user_name: str = "autorig-bot"
    source_task_id: str = ""


@router.post("/api-character-gen")
async def api_character_gen(request: Request, body: CharacterGenRequest) -> Dict[str, Any]:
    if not body.prompt.strip():
        raise HTTPException(status_code=400, detail="prompt required")
    job = await _chargen(request).create(
        prompt=body.prompt,
        negative_prompt=body.negative_prompt,
        mask_url=body.mask_url,
        user_name=body.user_name,
        source_task_id=body.source_task_id,
    )
    return job.public_dict()


@router.get("/api-character-gen/{job_id}")
async def api_character_gen_status(request: Request, job_id: str) -> Dict[str, Any]:
    job = _chargen(request).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.public_dict()


@router.post("/api-character-gen/{job_id}/approve-image")
async def api_character_gen_approve_image(request: Request, job_id: str) -> Dict[str, Any]:
    job, transitioned = await _chargen(request).approve_image(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    payload = job.public_dict()
    payload["transitioned"] = transitioned
    return payload


@router.post("/api-character-gen/{job_id}/resume")
async def api_character_gen_resume(request: Request, job_id: str) -> Dict[str, Any]:
    job, transitioned = await _chargen(request).resume(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    payload = job.public_dict()
    payload["transitioned"] = transitioned
    return payload


@router.post("/api-character-gen/{job_id}/regenerate-image")
async def api_character_gen_regenerate_image(request: Request, job_id: str) -> Dict[str, Any]:
    job, transitioned = await _chargen(request).regenerate_image(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    payload = job.public_dict()
    payload["transitioned"] = transitioned
    return payload


@router.post("/api-character-gen/{job_id}/discard")
async def api_character_gen_discard(request: Request, job_id: str) -> Dict[str, Any]:
    job = await _chargen(request).discard(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.public_dict()


@router.post("/api-character-gen/{job_id}/mark-submitted")
async def api_character_gen_mark_submitted(request: Request, job_id: str) -> Dict[str, Any]:
    job = await _chargen(request).mark_submitted(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.public_dict()
