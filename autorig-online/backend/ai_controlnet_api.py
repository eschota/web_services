"""Control-map extraction API backed by Renderfin/ComfyUI.

Each channel is a separate typed graph service, but they share this endpoint.
The result is an ordinary public image URL whose entity type is chosen by the
calling service (``control_pose``, ``control_depth`` or ``control_canny``).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ai_vision_api import (
    MAX_WAIT_SECONDS,
    SUBMIT_TIMEOUT_SECONDS,
    _decode_inline_image,
    _publish_inline_image,
)

logger = logging.getLogger(__name__)
router = APIRouter()

RENDERFIN_BASE = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")
CHANNEL_TYPES = {
    "pose": "control_pose",
    "depth": "control_depth",
    "canny": "control_canny",
}


class ControlNetRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Source image, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Source image as base64 or data URL")
    channel: str = Field(..., description="pose, depth or canny")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


def renderfin_payload(channel: str, image_url: str) -> Dict[str, object]:
    channel = str(channel or "").strip().lower()
    if channel not in CHANNEL_TYPES:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_control_channel",
            "message_string": "channel must be pose, depth or canny",
            "available_channels_array": sorted(CHANNEL_TYPES),
        })
    if not str(image_url or "").strip().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64",
        })
    return {
        # Renderfin requires prompt or image; extraction uses only the image.
        "image_url": str(image_url).strip(),
        "type": CHANNEL_TYPES[channel],
        "main_size_width": 960,
        "main_size_height": 540,
    }


@router.get("/api/controlnet")
async def api_controlnet_docs():
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/controlnet",
        "required_fields_array": ["image_url or image_base64", "channel"],
        "available_channels_array": sorted(CHANNEL_TYPES),
        "produces_by_channel_object": CHANNEL_TYPES,
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/controlnet")
async def api_controlnet(body: ControlNetRequest):
    async with httpx.AsyncClient() as client:
        source = str(body.image_url or "").strip()
        if not source and body.image_base64:
            source = await _publish_inline_image(client, _decode_inline_image(body.image_base64))
        payload = renderfin_payload(body.channel, source)
        try:
            response = await client.post(
                RENDERFIN_BASE + "/api-render", json=payload, timeout=SUBMIT_TIMEOUT_SECONDS
            )
        except Exception:
            logger.exception("Renderfin did not accept a ControlNet extraction request")
            raise HTTPException(status_code=502, detail={
                "error_string": "controlnet_service_unreachable",
                "message_string": "The image farm did not answer",
            }) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "controlnet_service_rejected",
                "message_string": f"Image farm answered HTTP {response.status_code}",
            })
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        task_id = str(accepted.get("task_id") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "controlnet_service_no_output",
                "message_string": "Image farm accepted the request without an output URL",
            })

        ready = False
        if body.wait_seconds and body.wait_seconds > 0:
            deadline = time.monotonic() + min(float(body.wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                try:
                    probe = await client.head(output_url, timeout=15.0)
                    if probe.status_code == 200:
                        ready = True
                        break
                except Exception:
                    continue
        channel = str(body.channel).strip().lower()
        return {
            "success_bool": True,
            "task_id_string": task_id,
            "status_string": "completed" if ready else "pending",
            "finished_bool": ready,
            "channel_string": channel,
            "entity_type_string": CHANNEL_TYPES[channel],
            "image_url_string": output_url,
            "poll_url_string": output_url,
            "server_time_unix_int": int(time.time()),
        }
