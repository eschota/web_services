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
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import ai_model_catalogue
from ai_enhance_api import _resolve_source, _run, _source_size
from ai_vision_api import MAX_WAIT_SECONDS, _decode_inline_image, _publish_inline_image

logger = logging.getLogger(__name__)
router = APIRouter()

SERVICE_ID = "qwen_image"
# renderfin.routing.QWEN_IMAGE_WORKFLOWS keys. Stated rather than imported:
# the backend and renderfin are separate processes and only share HTTP.
TYPE_GENERATE = "qwen_image"
TYPE_EDIT = "qwen_image_edit"
# Several pictures in one edit (renderfin.multiref): image1..image3 of
# TextEncodeQwenImageEditPlus, which is the node's own ceiling.
TYPE_EDIT_MULTI = "qwen_image_edit_multi"
MAX_REFERENCE_IMAGES = 3
MODES = ("auto", "generate", "edit")

# renderfin clamps an image render to 2048 on either side; asking for more
# would be silently cut down and change the aspect ratio of an edit.
MAX_SIDE = 2048
MIN_SIDE = 256
# Qwen-Image's own training resolution, and a size both installed quants are
# demonstrated at.
DEFAULT_SIZE = (1024, 1024)

CACHE_NAMESPACE = "qwen-image-20260926-v3"

# Qwen-Image-2.1 + Viggle turbo (2026-09-26). A catalogue entry that declares
# "qwen_image_generation": "2.1" is rendered by the 2.1 templates: one int8
# transformer for both modes, Viggle's 6-step LoRA, no CFG, no negative. Its
# sampling is fixed, so steps/cfg/negative_prompt are not forwarded to it.
TYPE21_GENERATE = "qwen_image21"
TYPE21_EDIT = "qwen_image21_edit"
TYPE21_EDIT_MULTI = "qwen_image21_edit_multi"

# One edit model on the farm (owner, 2026-09-26): every edit runs on the
# Qwen-Image 2.1 turbo. The old edit files stay installed on the boxes for
# rollback but are out of the catalogue; a request that still names one is
# redirected to the default and told so, instead of failing.
RETIRED_EDIT_CHECKPOINTS = frozenset({
    "qwen-image-edit-2511-Q3_K_S.gguf",
    "qwen-image-edit-2511",
    "qwen-image-edit-2512-Q3_K_S.gguf",
})


def default_edit_checkpoint() -> str:
    for entry in ai_model_catalogue.for_service(SERVICE_ID, "checkpoint"):
        if entry.get("usable") and entry.get("qwen_image_default") and "edit" in _entry_modes(entry):
            return str(entry.get("file") or "")
    return ""


def redirect_retired_checkpoint(name: Optional[str], mode: str) -> Tuple[Optional[str], str]:
    """(checkpoint to use, deprecation note or "")."""
    wanted = str(name or "").strip()
    if not wanted:
        return name, ""
    retired = wanted in RETIRED_EDIT_CHECKPOINTS
    if not retired and mode == "edit":
        entry = ai_model_catalogue.known_file(wanted, "checkpoint") or {}
        # A generate-only Qwen file named for an edit: same answer.
        retired = bool(entry) and str(entry.get("qwen_image_generation") or "") != "2.1"
    if not retired:
        return name, ""
    target = default_edit_checkpoint()
    note = (f"checkpoint '{wanted}' is retired for editing since 2026-09-26; "
            f"the request ran on '{target or 'the default'}' (Qwen-Image 2.1 turbo). "
            "Leave checkpoint empty.")
    logger.warning("qwen-image deprecation: %s", note)
    return (target or None), note


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


def is_generation21(name: str) -> bool:
    entry = ai_model_catalogue.known_file(str(name or ""), "checkpoint") or {}
    return str(entry.get("qwen_image_generation") or "") == "2.1"


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
        out.append(entry)
    # The entry marked qwen_image_default comes first: that is the file an
    # unnamed request loads.
    out.sort(key=lambda entry: 0 if entry.get("qwen_image_default") else 1)
    return [entry.get("file") for entry in out if entry.get("file")]


# Owner rule 2026-09-27: an edit needs no text. The standing instruction is
# always applied (editable per node as `system_prompt`); the user's text, if
# any, follows it. {images} expands to "image 1, image 2, ..." for the pictures
# actually wired in.
QWEN_SYSTEM_PROMPT_DEFAULT = (
    "Remix {images} into one coherent image: unify the style and lighting, "
    "combine the subjects and the story of all inputs; image 1 is the base scene and composition."
)
QWEN_SINGLE_VARIATION = (
    "Re-render image 1 as a clean, style-consistent variation: keep the subject, "
    "composition, colours and lighting."
)


def compose_prompt(user_text: str, system_prompt: Optional[str], picture_count: int) -> str:
    """The prompt Qwen-Image gets: standing instruction + the user's text."""
    user_text = str(user_text or "").strip()
    if picture_count <= 0:
        return user_text
    standing = str(system_prompt if system_prompt is not None else QWEN_SYSTEM_PROMPT_DEFAULT).strip()
    if picture_count == 1 and not user_text and standing == QWEN_SYSTEM_PROMPT_DEFAULT:
        return QWEN_SINGLE_VARIATION
    names = ", ".join(f"image {index}" for index in range(1, picture_count + 1))
    standing = standing.replace("{images}", names)
    return (standing + " " + user_text).strip()


class QwenImageRequest(BaseModel):
    prompt: str = Field("", max_length=12000,
                        description=("What to draw, or what to change about the picture. May be empty "
                                     "when at least one picture is wired in (the standing instruction remixes them)"))
    system_prompt: Optional[str] = Field(None, max_length=12000,
                                         description="Standing instruction applied before the prompt; {images} = image 1..N")
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
    reference_image_urls: Optional[List[str]] = Field(
        None, description=("More pictures after image_url, in order (image 2, image 3); "
                           "3 in all. A video URL stands for its first frame"))


def extra_references(body: "QwenImageRequest") -> List[str]:
    """The pictures after the first, checked before anything is fetched."""
    extras = [str(item or "").strip() for item in (body.reference_image_urls or [])]
    if not extras:
        return []
    if any(not item.startswith(("http://", "https://", "data:")) for item in extras):
        raise HTTPException(status_code=400, detail={
            "error_string": "bad_reference_image",
            "message_string": "Reference pictures must be http(s) URLs or data URLs"})
    total = len(extras) + (1 if (body.image_url or body.image_base64) else 0)
    if total > MAX_REFERENCE_IMAGES:
        raise HTTPException(status_code=400, detail={
            "error_string": "too_many_reference_images",
            "message_string": (f"Qwen-Image-Edit takes at most {MAX_REFERENCE_IMAGES} "
                               f"pictures; {total} were wired in"),
            "max_int": MAX_REFERENCE_IMAGES})
    return extras


@router.get("/api/qwen-image")
async def api_qwen_image_docs():
    return {
        "status_string": "ok", "method_string": "POST", "url_string": "/api/qwen-image",
        "required_fields_array": ["prompt (optional with at least one picture)"],
        "system_prompt_default_string": QWEN_SYSTEM_PROMPT_DEFAULT,
        "modes_array": list(MODES),
        "mode_note_string": ("auto edits when a picture is supplied and generates when "
                             "it is not; the two modes load different models"),
        "installed_checkpoints_object": {
            "generate": installed_checkpoints("generate"),
            "edit": installed_checkpoints("edit"),
        },
        "max_side_int": MAX_SIDE,
        "min_side_int": MIN_SIDE,
        "max_reference_images_int": MAX_REFERENCE_IMAGES,
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/qwen-image")
async def api_qwen_image(body: QwenImageRequest):
    import ai_request_cache
    return await ai_request_cache.run_cached(
        SERVICE_ID, body.model_dump(exclude_none=True),
        lambda: _uncached_qwen_image(body), namespace=CACHE_NAMESPACE)


async def _uncached_qwen_image(body: QwenImageRequest):
    extras = extra_references(body)
    has_image = bool(str(body.image_url or "").strip() or str(body.image_base64 or "").strip()
                     or extras)
    mode = resolve_mode(body.mode, has_image)
    if mode == "generate":
        extras = []
        if not str(body.prompt or "").strip():
            raise HTTPException(status_code=400, detail={
                "error_string": "prompt_required",
                "message_string": "Say what to draw, or wire in a picture to remix"})
    requested, deprecation = redirect_retired_checkpoint(body.checkpoint, mode)
    checkpoint = validate_checkpoint(requested, mode)
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

    pictures: List[str] = []
    if mode == "edit":
        import ai_multiref
        async with httpx.AsyncClient() as client:
            if body.image_url or body.image_base64:
                source = await _resolve_source(
                    client, await ai_multiref.as_picture(str(body.image_url or ""), client),
                    body.image_base64)
                pictures.append(source)
            for item in extras:
                if item.startswith("data:"):
                    item = await _publish_inline_image(client, _decode_inline_image(item))
                else:
                    item = await ai_multiref.as_picture(item, client)
                pictures.append(item)
            source = pictures[0]
            if not explicit_size:
                # The output follows the picture that came in. A fixed default
                # would reframe every edit, and the node downstream would then
                # be working at a size nobody asked for.
                width, height = _fit_source(*await _source_size(client, source))

    turbo21 = is_generation21(checkpoint)
    final_prompt = compose_prompt(body.prompt, body.system_prompt,
                                  len(pictures) if mode == "edit" else 0)
    payload: Dict[str, object] = {
        "prompt": final_prompt,
        "negative_prompt": "" if turbo21 else str(body.negative_prompt or "").strip(),
        "type": ((TYPE21_EDIT if turbo21 else TYPE_EDIT) if mode == "edit"
                 else (TYPE21_GENERATE if turbo21 else TYPE_GENERATE)),
        "main_size_width": width,
        "main_size_height": height,
    }
    if len(pictures) > 1:
        # The output follows image 1, the one the prompt edits; the others are
        # what it borrows from.
        payload["type"] = TYPE21_EDIT_MULTI if turbo21 else TYPE_EDIT_MULTI
        payload["reference_image_urls"] = pictures
    elif source:
        payload["image_url"] = source
    if checkpoint:
        payload["checkpoint"] = checkpoint
    if body.steps and not turbo21:
        payload["steps"] = int(body.steps)
    if body.cfg and not turbo21:
        payload["cfg"] = float(body.cfg)
    if body.seed:
        payload["noise_seed"] = int(body.seed)

    answer = await _run(SERVICE_ID, payload, body.wait_seconds)
    answer["mode_string"] = mode
    answer["prompt_string"] = final_prompt
    answer["width_int"] = width
    answer["height_int"] = height
    answer["checkpoint_string"] = checkpoint
    if deprecation:
        answer["deprecation_string"] = deprecation
    return answer
