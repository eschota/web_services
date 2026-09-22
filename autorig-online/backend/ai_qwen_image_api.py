"""Qwen-Image: one endpoint that draws a picture or rewrites one.

Qwen-Image and Qwen-Image-Edit are the same 20B MMDiT and the same
Qwen2.5-VL-7B text encoder; what separates them is whether a picture is being
conditioned on. So this is one service with two templates rather than two
services, and the wiring of the graph — a picture on the node or not — is what
picks between them. ``mode`` exists only to override that reading.

Everything here runs as a GGUF quantisation through ComfyUI-GGUF on the image
boxes. That is the whole point: a 40 GB bf16 transformer only fits the single
24 GB card, and one card cannot render two pictures at once. A 9 GB quant runs
on the 8 GB and 12 GB boxes, so two of them render in parallel.

The checkpoint a caller names is checked against the live catalogue before it
reaches a worker, and against the resolved mode: the edit quantisation in a
generation graph would load a model trained to copy an image that is not
there.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import ai_model_catalogue
from ai_enhance_api import _resolve_source, _run, _source_size
from ai_vision_api import MAX_WAIT_SECONDS

logger = logging.getLogger(__name__)
router = APIRouter()

SERVICE_ID = "qwen_image"
# renderfin.routing.QWEN_IMAGE_WORKFLOWS keys. Stated rather than imported:
# the backend and renderfin are separate processes and only share HTTP.
TYPE_GENERATE = "qwen_image"
TYPE_EDIT = "qwen_image_edit"
MODES = ("auto", "generate", "edit")

# renderfin clamps an image render to 2048 on either side; asking for more
# would be silently cut down and change the aspect ratio of an edit.
MAX_SIDE = 2048
MIN_SIDE = 256
# Qwen-Image's own training resolution, and a size both installed quants are
# demonstrated at.
DEFAULT_SIZE = (1024, 1024)

CACHE_NAMESPACE = "qwen-image-20260922-v1"


def _round_side(value: float) -> int:
    """The requested side, kept whole and inside the ceiling.

    Deliberately not snapped to the model's own 16 px grid: renderfin already
    pads the latent to /32 and scales the result back to exactly the size that
    was asked for, so snapping here would only move the answer away from the
    request. It matters for an edit, where the size that was asked for is the
    source picture's own and any nudge reframes it.
    """
    side = int(round(float(value)))
    return max(MIN_SIDE, min(MAX_SIDE, side))


def _fit_source(width: int, height: int) -> Tuple[int, int]:
    """The source picture's own size, brought inside the ceiling whole.

    Clamping each side on its own is what squashes a tall picture: an edit
    delivered at a different aspect ratio than it went in at is a bug people
    notice immediately and cannot work around.
    """
    width = max(1, int(width))
    height = max(1, int(height))
    longest = max(width, height)
    if longest > MAX_SIDE:
        width = width * MAX_SIDE / longest
        height = height * MAX_SIDE / longest
    return _round_side(width), _round_side(height)


def resolve_mode(mode: Optional[str], has_image: bool) -> str:
    """Which of the two models this request is for.

    ``auto`` reads the graph: a picture wired in means the picture is the
    subject. ``edit`` without one is rejected rather than quietly generating,
    because a person who chose Edit is asking for their picture back.
    """
    wanted = str(mode or "auto").strip().lower() or "auto"
    if wanted not in MODES:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_qwen_image_mode",
            "message_string": "mode must be auto, generate or edit",
            "modes_array": list(MODES),
        })
    if wanted == "edit" and not has_image:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Edit mode needs a picture; wire one in or choose Generate",
        })
    if wanted == "auto":
        return "edit" if has_image else "generate"
    return wanted


def _entry_modes(entry: Dict[str, object]) -> Tuple[str, ...]:
    """Which of the two templates a catalogue entry may be loaded by.

    An entry that does not say is assumed to serve both, so a catalogue
    somebody adds a quantisation to keeps working without this file changing.
    """
    declared = entry.get("qwen_image_modes")
    if not isinstance(declared, (list, tuple)) or not declared:
        return ("generate", "edit")
    return tuple(str(value).strip().lower() for value in declared)


def validate_checkpoint(name: Optional[str], mode: str) -> str:
    """The catalogue file name to load, or "" for the template's own.

    A request names a file and that file name goes into a workflow a worker
    then loads off disk, so only names the catalogue offers are accepted.
    """
    wanted = str(name or "").strip()
    if not wanted:
        return ""
    entry = ai_model_catalogue.known_file(wanted, "checkpoint")
    if not entry or str(entry.get("family") or "").strip().lower() != "qwen_image":
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_checkpoint",
            "message_string": f"'{wanted}' is not a Qwen-Image model",
        })
    # A Qwen-Image build the farm cannot run is in the catalogue on purpose,
    # so the answer here is the reason it cannot, not "never heard of it".
    if not entry.get("usable") or SERVICE_ID not in (entry.get("services") or []):
        raise HTTPException(status_code=400, detail={
            "error_string": "checkpoint_not_usable",
            "message_string": str(entry.get("unusable_reason")
                                  or f"'{wanted}' is not installed on the image boxes"),
        })
    if mode not in _entry_modes(entry):
        raise HTTPException(status_code=400, detail={
            "error_string": "checkpoint_wrong_mode",
            "message_string": (f"'{wanted}' is a Qwen-Image {'/'.join(_entry_modes(entry))} "
                               f"model and cannot be used to {mode}"),
            "mode_string": mode,
        })
    return wanted


def installed_checkpoints(mode: Optional[str] = None) -> list:
    """What a caller may actually name, optionally for one of the two modes.

    Narrower than what the picker draws: the catalogue deliberately carries
    models the farm cannot run so the UI can grey them out with the reason,
    and this list is the opposite promise — every name in it loads.
    """
    out = []
    for entry in ai_model_catalogue.for_service(SERVICE_ID, "checkpoint"):
        if not entry.get("usable"):
            continue
        if mode and mode in MODES and mode != "auto" and mode not in _entry_modes(entry):
            continue
        out.append(entry.get("file"))
    return [name for name in out if name]


class QwenImageRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=12000,
                        description="What to draw, or what to change about the picture")
    image_url: Optional[str] = Field(None, description="Picture to edit, public http(s) URL")
    image_base64: Optional[str] = Field(None, description="Picture to edit as base64 or data URL")
    mode: str = Field("auto", description="auto, generate or edit")
    negative_prompt: Optional[str] = Field(None, max_length=12000)
    width: Optional[int] = Field(None, ge=MIN_SIDE, le=MAX_SIDE)
    height: Optional[int] = Field(None, ge=MIN_SIDE, le=MAX_SIDE)
    steps: Optional[int] = Field(None, ge=0, le=60, description="0 keeps the workflow's own")
    cfg: Optional[float] = Field(None, ge=0, le=30, description="0 keeps the workflow's own")
    seed: Optional[int] = Field(None, ge=0, le=9007199254740991)
    checkpoint: Optional[str] = Field(None, description="Installed GGUF quantisation")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)


@router.get("/api/qwen-image")
async def api_qwen_image_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/qwen-image",
        "required_fields_array": ["prompt"],
        "modes_array": list(MODES),
        "mode_note_string": ("auto edits when a picture is supplied and generates when "
                             "it is not; the two modes load different models"),
        "installed_checkpoints_object": {
            "generate": installed_checkpoints("generate"),
            "edit": installed_checkpoints("edit"),
        },
        "max_side_int": MAX_SIDE,
        "min_side_int": MIN_SIDE,
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/qwen-image")
async def api_qwen_image(body: QwenImageRequest):
    import ai_request_cache
    return await ai_request_cache.run_cached(
        SERVICE_ID, body.model_dump(exclude_none=True),
        lambda: _uncached_qwen_image(body), namespace=CACHE_NAMESPACE)


async def _uncached_qwen_image(body: QwenImageRequest):
    has_image = bool(str(body.image_url or "").strip() or str(body.image_base64 or "").strip())
    mode = resolve_mode(body.mode, has_image)
    checkpoint = validate_checkpoint(body.checkpoint, mode)
    if not checkpoint:
        # The model is named even when nobody picked one, and that is what
        # keeps the job off a box that cannot run it. Renderfin only asks a
        # worker what it holds once a file has been named; unnamed, the job is
        # eligible everywhere that advertises the scheduling token, and the
        # boxes carrying the GGUF are a strict subset of those. It changes
        # nothing about the picture: this is the file the template loads.
        installed = installed_checkpoints(mode)
        checkpoint = installed[0] if installed else ""

    source = ""
    explicit_size = bool(body.width and body.height)
    if explicit_size:
        width, height = _round_side(body.width), _round_side(body.height)
    else:
        width, height = DEFAULT_SIZE

    if mode == "edit":
        async with httpx.AsyncClient() as client:
            source = await _resolve_source(client, body.image_url, body.image_base64)
            if not explicit_size:
                # The output follows the picture that came in. A fixed default
                # would reframe every edit, and the node downstream would then
                # be working at a size nobody asked for.
                width, height = _fit_source(*await _source_size(client, source))

    payload: Dict[str, object] = {
        "prompt": str(body.prompt).strip(),
        "negative_prompt": str(body.negative_prompt or "").strip(),
        "type": TYPE_EDIT if mode == "edit" else TYPE_GENERATE,
        "main_size_width": width,
        "main_size_height": height,
    }
    if source:
        payload["image_url"] = source
    if checkpoint:
        payload["checkpoint"] = checkpoint
    if body.steps:
        payload["steps"] = int(body.steps)
    if body.cfg:
        payload["cfg"] = float(body.cfg)
    if body.seed:
        payload["noise_seed"] = int(body.seed)

    answer = await _run(SERVICE_ID, payload, body.wait_seconds)
    answer["mode_string"] = mode
    answer["width_int"] = width
    answer["height_int"] = height
    return answer
