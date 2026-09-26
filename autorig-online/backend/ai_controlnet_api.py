"""Control-map extraction API backed by Renderfin/ComfyUI.

Each channel is a separate typed graph service, but they share this endpoint.
The result is an ordinary public image URL whose entity type is chosen by the
calling service (``control_pose``, ``control_depth`` or ``control_canny``).
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
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
    # Surface orientation (BAE normal preprocessor). No model on the fleet has a
    # native normal ControlNet; the map is used as a reference picture.
    "normal": "control_normal",
}


class ControlNetRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Source image, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Source image as base64 or data URL")
    channel: str = Field(..., description="pose, depth, canny or normal")
    width: Optional[int] = Field(None, ge=64, le=2048,
        description="Map width; omitted = the source image's own width")
    height: Optional[int] = Field(None, ge=64, le=2048,
        description="Map height; omitted = the source image's own height")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


# A control map is only useful at the size of the picture it was taken from:
# the image node that consumes it follows its dimensions, and a 960x540 map of
# a portrait source used to leave every downstream node at the wrong size.
DEFAULT_MAP_SIZE = (960, 540)
MAX_PROBE_BYTES = 24 * 1024 * 1024


def _clamp_map_side(value: int) -> int:
    return max(64, min(2048, int(value)))


async def probe_image_size(client: httpx.AsyncClient, url: str) -> Optional[tuple[int, int]]:
    """(width, height) of a public image, or None when it cannot be read."""
    try:
        response = await client.get(url, timeout=20.0, follow_redirects=True)
        if response.status_code != 200 or len(response.content) > MAX_PROBE_BYTES:
            return None
        from io import BytesIO
        from PIL import Image
        with Image.open(BytesIO(response.content)) as picture:
            width, height = picture.size
    except Exception:
        logger.info("Could not read the size of %s; using the default map size", url)
        return None
    if width < 1 or height < 1:
        return None
    return _clamp_map_side(width), _clamp_map_side(height)


def renderfin_payload(channel: str, image_url: str,
                     size: Optional[tuple[int, int]] = None) -> Dict[str, object]:
    channel = str(channel or "").strip().lower()
    if channel not in CHANNEL_TYPES:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_control_channel",
            "message_string": "channel must be pose, depth, canny or normal",
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
        "main_size_width": int((size or DEFAULT_MAP_SIZE)[0]),
        "main_size_height": int((size or DEFAULT_MAP_SIZE)[1]),
    }


# ------------------------------------------------ normal map -> grey shading
#
# A model handed an RGB normal map as a reference picture copies its purple and
# pink into the render (measured 2026-09-27, Qwen-Image 2.1). Lit once from the
# upper left, the same map is a neutral grey clay render that carries only the
# shapes. Only maps the farm itself made are shaded (no open proxy); the result
# is cached next to the other derived files.
RENDER_FILES = Path(os.getenv("RENDERFIN_DATA_DIR", "/srv/autorig/data/var/renderfin")) / "render"
SHADE_DIR = Path(os.getenv("AUTORIG_NORMAL_SHADE_DIR", "/srv/autorig/data/var/normal-shade"))
_RENDER_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def shade_normal_png(data: bytes) -> bytes:
    from io import BytesIO
    import numpy as np
    from PIL import Image
    with Image.open(BytesIO(data)) as picture:
        normals = np.asarray(picture.convert("RGB"), dtype=np.float32) / 127.5 - 1.0
    light = np.array([-0.4, 0.5, 0.75], dtype=np.float32)
    light /= np.linalg.norm(light)
    shade = np.clip((normals * light).sum(-1), 0.0, 1.0) * 0.85 + 0.15
    out = BytesIO()
    Image.fromarray((shade * 255).astype(np.uint8)).save(out, format="PNG")
    return out.getvalue()


@router.get("/api/ai/normal-shade/{render_id}.png")
async def api_normal_shade(render_id: str):
    if not _RENDER_ID_RE.match(render_id or ""):
        raise HTTPException(status_code=404, detail="not a farm render id")
    target = SHADE_DIR / f"{render_id}.png"
    if not target.exists():
        source = RENDER_FILES / "default_user" / f"{render_id}.png"
        if not source.is_file() or source.stat().st_size > MAX_PROBE_BYTES:
            raise HTTPException(status_code=404, detail="normal map not found")
        png = await asyncio.to_thread(shade_normal_png, source.read_bytes())
        SHADE_DIR.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(png)
        os.replace(temporary, target)
    return FileResponse(target, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})


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
    import ai_request_cache
    return await ai_request_cache.run_cached("controlnet", body.model_dump(exclude_none=True),
        lambda: _uncached_api_controlnet(body), namespace="control-maps-20260922-v1")


async def _uncached_api_controlnet(body: ControlNetRequest):
    async with httpx.AsyncClient() as client:
        source = str(body.image_url or "").strip()
        if not source and body.image_base64:
            source = await _publish_inline_image(client, _decode_inline_image(body.image_base64))
        size = None
        if body.width and body.height:
            size = (_clamp_map_side(body.width), _clamp_map_side(body.height))
        else:
            size = await probe_image_size(client, source)
        payload = renderfin_payload(body.channel, source, size)
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


# The enhancement endpoints (/api/upscale, /api/detail, /api/facefix) hang off
# this router rather than being mounted separately in main.py: they are the
# same shape — build a renderfin job, hand back the public URL it will appear
# at — and main.py is a 700 kB file several people patch at once.
from ai_enhance_api import router as _enhance_router  # noqa: E402

router.include_router(_enhance_router)

# /api/qwen-image rides here for the same reason: it is the same shape again —
# build a renderfin job, hand back the URL the picture will appear at — and it
# is already reachable because this router is mounted.
from ai_qwen_image_api import router as _qwen_image_router  # noqa: E402

router.include_router(_qwen_image_router)

# /api/music (Stable Audio 3, 2026-09-26): the same renderfin shape again.
from ai_music_api import router as _music_router  # noqa: E402

router.include_router(_music_router)
