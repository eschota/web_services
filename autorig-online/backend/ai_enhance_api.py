"""Image enhancement API backed by Renderfin/ComfyUI.

Three separate services, one shape: a picture goes in, the same picture comes
back improved, at a size this module works out rather than one the caller has
to guess.

* ``/api/upscale``  - super-resolution. ``fast`` is the ESRGAN model on its
  own; ``refine`` runs a low-creativity tiled diffusion pass over the enlarged
  picture so the invented pixels carry real detail instead of smooth edges.
* ``/api/detail``   - detail enhancement at the source size: a tiled, low
  denoise pass plus a light unsharp finish.
* ``/api/facefix``  - face repair: the face is segmented, cropped out at a far
  higher pixel density, re-rendered under a noise mask and stitched back.

What runs where is farm-dependent and deliberately explicit: every template
these endpoints ask for is scheduled with a token that only the FLUX image
boxes advertise (see ``renderfin.routing.ENHANCE_SCHEDULING_TOKEN``).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, Optional, Tuple

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

# Mirrors renderfin.routing.ENHANCE_MAX_SIDE. The backend and renderfin are
# separate processes, so this is stated rather than imported; the renderfin
# clamp is the one that actually binds, and asking for more than it allows
# would silently change the aspect ratio.
MAX_SIDE = 4096
DEFAULT_SIZE = (960, 540)
CACHE_NAMESPACE = "image-enhance-20260922-v1"

# The one ESRGAN file present on every FLUX image box. RealESRGAN_x2/x4 exist
# on f15 alone, so offering them would make the result depend on which card
# happened to be free; the catalogue lists them disabled for the same reason.
DEFAULT_UPSCALE_MODEL = "4x_NMKD-Siax_200k.pth"
INSTALLED_UPSCALE_MODELS = (DEFAULT_UPSCALE_MODEL,)


def _fit(width: int, height: int, limit: int = MAX_SIDE) -> Tuple[int, int]:
    """Largest whole size within `limit` that keeps the aspect ratio.

    Clamping each side on its own is what a naive ceiling does, and it turns a
    4x enlargement of a portrait into a squashed picture, because the delivery
    resize downstream crops to whatever was asked for.
    """
    width = max(1, int(width))
    height = max(1, int(height))
    longest = max(width, height)
    if longest > limit:
        width = max(64, round(width * limit / longest))
        height = max(64, round(height * limit / longest))
    return max(64, min(limit, width)), max(64, min(limit, height))


async def _source_size(client: httpx.AsyncClient, url: str) -> Tuple[int, int]:
    from ai_controlnet_api import probe_image_size

    size = await probe_image_size(client, url)
    return size or DEFAULT_SIZE


async def _resolve_source(client: httpx.AsyncClient, image_url: Optional[str],
                          image_base64: Optional[str]) -> str:
    source = str(image_url or "").strip()
    if not source and image_base64:
        source = await _publish_inline_image(client, _decode_inline_image(image_base64))
    if not source.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64",
        })
    return source


async def _run(service: str, payload: Dict[str, object], wait_seconds: Optional[float],
               *, produces: str = "image") -> Dict[str, object]:
    """Submit one renderfin job and answer in the shape /api/controlnet uses."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(RENDERFIN_BASE + "/api-render", json=payload,
                                         timeout=SUBMIT_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Renderfin did not accept an enhancement request")
            raise HTTPException(status_code=502, detail={
                "error_string": "enhance_service_unreachable",
                "message_string": "The image farm did not answer",
            }) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "enhance_service_rejected",
                "message_string": f"Image farm answered HTTP {response.status_code}",
            })
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        task_id = str(accepted.get("task_id") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "enhance_service_no_output",
                "message_string": "Image farm accepted the request without an output URL",
            })

        ready = False
        if wait_seconds and wait_seconds > 0:
            deadline = time.monotonic() + min(float(wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                try:
                    probe = await client.head(output_url, timeout=15.0)
                    if probe.status_code == 200:
                        ready = True
                        break
                except Exception:
                    continue
        answer = {
            "success_bool": True,
            "task_id_string": task_id,
            "status_string": "completed" if ready else "pending",
            "finished_bool": ready,
            "service_string": service,
            "entity_type_string": produces,
            "poll_url_string": output_url,
            "status_url_string": output_url,
            "server_time_unix_int": int(time.time()),
        }
        answer["video_url_string" if produces == "video" else "image_url_string"] = output_url
        return answer


# ------------------------------------------------------------------- upscale


class UpscaleRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Source image, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Source image as base64 or data URL")
    scale: int = Field(2, ge=2, le=4, description="2 or 4; the output follows the source × scale")
    model: Optional[str] = Field(None, description="Installed upscale model file name")
    mode: str = Field("fast", description="fast = ESRGAN only, refine = ESRGAN + tiled diffusion")
    prompt: Optional[str] = Field(None, description="Subject text for the refine pass")
    width: Optional[int] = Field(None, ge=64, le=MAX_SIDE, description="Explicit output width")
    height: Optional[int] = Field(None, ge=64, le=MAX_SIDE, description="Explicit output height")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


@router.get("/api/upscale")
async def api_upscale_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/upscale",
        "required_fields_array": ["image_url or image_base64"],
        "modes_array": ["fast", "refine"],
        "scales_array": [2, 4],
        "installed_models_array": list(INSTALLED_UPSCALE_MODELS),
        "max_side_int": MAX_SIDE,
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/upscale")
async def api_upscale(body: UpscaleRequest):
    import ai_request_cache
    return await ai_request_cache.run_cached(
        "upscale", body.model_dump(exclude_none=True),
        lambda: _uncached_upscale(body), namespace=CACHE_NAMESPACE)


async def _uncached_upscale(body: UpscaleRequest):
    mode = str(body.mode or "fast").strip().lower()
    if mode not in ("fast", "refine"):
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_upscale_mode",
            "message_string": "mode must be fast or refine",
            "modes_array": ["fast", "refine"],
        })
    model = str(body.model or "").strip() or DEFAULT_UPSCALE_MODEL
    if model not in INSTALLED_UPSCALE_MODELS:
        raise HTTPException(status_code=400, detail={
            "error_string": "upscale_model_not_installed",
            "message_string": f"{model} is not installed on every image worker",
            "installed_models_array": list(INSTALLED_UPSCALE_MODELS),
        })
    scale = 4 if int(body.scale or 2) >= 4 else 2
    async with httpx.AsyncClient() as client:
        source = await _resolve_source(client, body.image_url, body.image_base64)
        if body.width and body.height:
            width, height = _fit(body.width, body.height)
        else:
            source_width, source_height = await _source_size(client, source)
            width, height = _fit(source_width * scale, source_height * scale)
    payload: Dict[str, object] = {
        "image_url": source,
        "type": "upscale_refine" if mode == "refine" else "upscale_fast",
        "main_size_width": width,
        "main_size_height": height,
        "upscale_model": model,
    }
    if mode == "refine":
        payload["prompt"] = str(body.prompt or "").strip()
        # Low enough that the subject is kept and only its texture is redrawn.
        payload["creativity"] = 0.35
    return await _run("upscale", payload, body.wait_seconds)


# ------------------------------------------------------------------- detail


class DetailRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Source image, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Source image as base64 or data URL")
    strength: float = Field(0.35, ge=0, le=1, description="How much texture to invent")
    tile: bool = Field(True, description="Tile the diffusion pass; required over ~1.5 MP")
    prompt: Optional[str] = Field(None, description="Subject text, if there is one")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


@router.get("/api/detail")
async def api_detail_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/detail",
        "required_fields_array": ["image_url or image_base64"],
        "strength_range_array": [0, 1],
        "note_string": ("Detail Daemon is not installed on the farm; strength drives "
                        "the denoise of a tiled refinement pass instead"),
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/detail")
async def api_detail(body: DetailRequest):
    import ai_request_cache
    return await ai_request_cache.run_cached(
        "detail", body.model_dump(exclude_none=True),
        lambda: _uncached_detail(body), namespace=CACHE_NAMESPACE)


async def _uncached_detail(body: DetailRequest):
    async with httpx.AsyncClient() as client:
        source = await _resolve_source(client, body.image_url, body.image_base64)
        width, height = _fit(*await _source_size(client, source))
    strength = max(0.0, min(1.0, float(body.strength)))
    payload: Dict[str, object] = {
        "image_url": source,
        "type": "detail_tiled" if body.tile else "detail_plain",
        "main_size_width": width,
        "main_size_height": height,
        "prompt": str(body.prompt or "").strip(),
        # 0 would mean "leave the template's own value" downstream, so the
        # floor is deliberately above it.
        "creativity": round(0.15 + 0.45 * strength, 3),
    }
    return await _run("detail", payload, body.wait_seconds)


# ------------------------------------------------------------------ face fix


class FaceFixRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Source image, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Source image as base64 or data URL")
    fidelity: float = Field(0.6, ge=0, le=1,
                            description="1 keeps the original face, 0 rebuilds it")
    hands_eyes: bool = Field(False,
                             description="Also repair the bare skin the parser finds")
    prompt: Optional[str] = Field(None, description="Subject text, if there is one")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


@router.get("/api/facefix")
async def api_facefix_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/facefix",
        "required_fields_array": ["image_url or image_base64"],
        "fidelity_range_array": [0, 1],
        "note_string": ("CodeFormer/GFPGAN and the Impact Pack FaceDetailer are not "
                        "installed on the farm; the face is segmented, cropped at a "
                        "higher pixel density, re-rendered and stitched back"),
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/facefix")
async def api_facefix(body: FaceFixRequest):
    import ai_request_cache
    return await ai_request_cache.run_cached(
        "facefix", body.model_dump(exclude_none=True),
        lambda: _uncached_facefix(body), namespace=CACHE_NAMESPACE)


async def _uncached_facefix(body: FaceFixRequest):
    async with httpx.AsyncClient() as client:
        source = await _resolve_source(client, body.image_url, body.image_base64)
        width, height = _fit(*await _source_size(client, source))
    fidelity = max(0.0, min(1.0, float(body.fidelity)))
    payload: Dict[str, object] = {
        "image_url": source,
        "type": "face_fix_skin" if body.hands_eyes else "face_fix",
        "main_size_width": width,
        "main_size_height": height,
        "prompt": str(body.prompt or "").strip(),
        "creativity": round(0.60 - 0.45 * fidelity, 3),
    }
    return await _run("facefix", payload, body.wait_seconds)
