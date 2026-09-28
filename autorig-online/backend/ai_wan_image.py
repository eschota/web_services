"""Wan-Animate-2 from a plain character picture (no saved Avatar), 2026-09-27.

Same workflow and limits as /api/ai/avatar-video, but the identity comes from
the picture itself (usually a Qwen keyframe: the character already placed in
the shot's first frame), so anyone can run it and nothing private is read.
"""
from __future__ import annotations

import math
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

import ai_request_cache
from ai_avatar_assets import validate_import_url
from ai_avatar_video import CHECKPOINT, MAX_PADDED_PIXELS, WORKFLOW
from ai_vision_api import RENDERFIN_BASE, SUBMIT_TIMEOUT_SECONDS
from renderfin.video_input import VideoInputError, validate_video_url

router = APIRouter()


class WanImageRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    image_url: str
    control_video_url: str
    prompt: str = Field(default="", max_length=2000)
    character: str = Field(default="", max_length=2000)
    width: int = Field(default=960, ge=256, le=2048)
    height: int = Field(default=544, ge=256, le=2048)
    frame_count: int = Field(default=97, ge=9, le=97)
    seed: int = Field(default=0, ge=0, le=9007199254740991)
    control_strength: float = Field(default=1.0, ge=0, le=1)

    @field_validator("frame_count")
    @classmethod
    def eight_k_plus_one(cls, value: int) -> int:
        if (value - 1) % 8:
            raise ValueError("frame_count must be 8k+1 in the range 9..97")
        return value

    @field_validator("width", "height")
    @classmethod
    def even(cls, value: int) -> int:
        return value - (value % 2)


@router.post("/api/ai/wan-animate")
async def wan_animate(body: WanImageRequest):
    pw, ph = math.ceil(body.width / 32) * 32, math.ceil(body.height / 32) * 32
    if pw * ph > MAX_PADDED_PIXELS:
        raise HTTPException(400, detail={"error_string": "wan_resolution_too_large",
                                         "message_string": f"{body.width}x{body.height} is above the 24 GB limit"})
    try:
        driver = validate_video_url(body.control_video_url)
    except (ValueError, VideoInputError) as error:
        raise HTTPException(400, detail=str(error)) from None
    try:
        keyframe = validate_import_url(body.image_url)
    except ValueError:
        raise HTTPException(400, detail="The character picture must be an AutoRig or Civitai address") from None
    identity = body.character.strip() or "the character in the reference image"
    payload = {
        "work_flow": WORKFLOW, "image_url": keyframe, "control_video_url": driver,
        "prompt": ("Keep " + identity + " visually consistent in every frame: same face, hair, body and outfit as "
                   "the reference image. The driving video controls motion and timing only."),
        "pose_prompt": body.prompt.strip() or "Follow only the poses, timing and motion in the driving video.",
        "main_size_width": body.width, "main_size_height": body.height, "frame_count": body.frame_count,
        "noise_seed": body.seed, "control_strength": body.control_strength, "checkpoint": CHECKPOINT,
        "steps": 6, "cfg": 1.0, "sampler": "lcm", "scheduler": "simple",
    }

    async def submit():
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(RENDERFIN_BASE + "/api-render", json=payload, timeout=SUBMIT_TIMEOUT_SECONDS)
                response.raise_for_status()
                accepted = response.json()
            except (httpx.HTTPError, ValueError):
                raise HTTPException(502, detail="The farm did not accept the Wan-Animate job") from None
        if not isinstance(accepted, dict) or not accepted.get("task_id") or not accepted.get("output_url"):
            raise HTTPException(502, detail="The farm returned an incomplete Wan-Animate job")
        return {"success_bool": True, "task_id_string": accepted["task_id"],
                "video_url_string": accepted["output_url"], "status_string": "pending", "finished_bool": False}

    return await ai_request_cache.run_cached("video", dict(payload), submit, namespace="wan-image-v1")
