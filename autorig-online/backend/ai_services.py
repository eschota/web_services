"""One typed catalogue for the AI services under autorig.online.

Every service declares what it accepts and what it produces in the same small
vocabulary of entity types, so an answer from one service can be handed to any
service that accepts that type without either side knowing about the other.

An entity is `{type, value, mime?, label?, origin?}`. `value` is text for
`text`, and an http(s) URL for everything else, which is what makes a handoff
free: nothing is copied, the next service is simply given the address.

The catalogue is the single source of truth for the pages, so a service that
is not wired yet says so here rather than being quietly missing from the UI.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter

router = APIRouter()

# ---------------------------------------------------------------- entity types

TEXT = "text"
IMAGE = "image"
VIDEO = "video"
MODEL3D = "model3d"

ENTITY_TYPES: List[Dict[str, object]] = [
    {
        "id": TEXT,
        "title": "Text",
        "carries": "the string itself",
        "icon": "📝",
    },
    {
        "id": IMAGE,
        "title": "Image",
        "carries": "a public URL to a PNG, JPEG or WebP",
        "icon": "🖼️",
    },
    {
        "id": VIDEO,
        "title": "Video",
        "carries": "a public URL to an MP4",
        "icon": "🎬",
    },
    {
        "id": MODEL3D,
        "title": "3D model",
        "carries": "a public URL to a GLB or FBX",
        "icon": "🧊",
    },
]

# --------------------------------------------------------------------- services

# `status` is either "live" (wired end to end) or "planned" (declared so the
# handoff UI can show where a result could go, but not yet callable). A page
# shows a planned service greyed out instead of pretending it works.
SERVICES: List[Dict[str, object]] = [
    {
        "id": "vision",
        "title": "Vision",
        "path": "/vision",
        "api": "/api/vision",
        "summary": "Ask a question about an image and get an answer in text.",
        "status": "live",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Image to look at"},
            # Not required as a wire: the question is usually a fixed sentence,
            # and needing a whole node to hold it was the commonest way to end
            # up submitting a request with no prompt at all.
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What to ask about it"},
        ],
        "outputs": [
            {"type": TEXT, "field": "answer_string", "title": "Answer"},
        ],
        "models_url": "/api/ai/models",
    },
    {
        "id": "text",
        "title": "Text",
        "path": "/text",
        "api": "/api/text2text",
        "summary": "Send a prompt to a language model and get text back.",
        "status": "live",
        "inputs": [
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "Instruction"},
            # The material to work on, kept apart from the instruction so a
            # text coming from another service can be processed rather than
            # merely passed to the model as the whole request.
            {"type": TEXT, "field": "input", "required": False,
             "title": "Text to work on"},
        ],
        "outputs": [
            {"type": TEXT, "field": "answer_string", "title": "Answer"},
        ],
        "models_url": "/api/ai/models",
    },
    {
        "id": "image",
        "title": "Image",
        "path": "/image",
        "api": "/api/image",
        "summary": "Generate a picture from a prompt, optionally guided by a reference image.",
        "status": "live",
        "inputs": [
            {"type": TEXT, "field": "prompt", "required": True,
             "title": "What to draw"},
            {"type": IMAGE, "field": "image", "required": False,
             "title": "Reference image"},
        ],
        "outputs": [
            {"type": IMAGE, "field": "image_url_string", "title": "Picture"},
        ],
    },
    {
        "id": "video",
        "title": "Video",
        "path": "/video",
        "api": "/api/video",
        "summary": "Animate a picture into a short clip. Minutes, not seconds.",
        # The farm's workers advertise the animation workflow, and Renderfin
        # picks it from the frame alone, so a picture is all this needs.
        "status": "live",
        "slow": True,
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "First frame"},
            # Optional, and the reason a loop is possible at all: give the last
            # frame and the clip travels to it; give the first frame again and
            # it comes back to where it started.
            {"type": IMAGE, "field": "image_url_end", "required": False,
             "title": "Last frame"},
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What should happen"},
        ],
        "outputs": [
            {"type": VIDEO, "field": "video_url_string", "title": "Clip"},
        ],
    },
    {
        "id": "3dmodel",
        "title": "3D model",
        "path": "/3dmodel",
        "api": "/api/3dmodel",
        "summary": "Turn a picture into a 3D model. Hunyuan3D on the farm's own GPUs.",
        # Runs on the converter nodes' own Hunyuan3D, which is installed and
        # idle there; `/api/generate/from-image` is the separate account-and-
        # credits product flow and is not what this uses.
        "status": "live",
        "slow": True,
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Picture of the subject"},
        ],
        "outputs": [
            {"type": MODEL3D, "field": "model_url_string", "title": "3D model"},
            {"type": IMAGE, "field": "preview_url_string", "title": "Preview"},
        ],
    },
]


# ------------------------------------------------------------------ parameters

# What a caller may tune beyond the typed inputs. These are declared here, in
# the same catalogue, so a graph editor can draw the controls for a service it
# has never heard of; every one of them is optional and maps onto a field the
# API already accepts.
#
# Only knobs the farm genuinely has are listed. There is no LoRA picker because
# there are no LoRAs on these workers: the equivalent choice is the animation
# workflow the workers advertise, which is what `quality` selects.
PARAMS: Dict[str, List[Dict[str, object]]] = {
    "vision": [
        {"name": "model", "title": "Model", "type": "select", "source": "ai_models",
         "default": "bonsai2-27b"},
        # Typed on the node when nothing is wired in; a wired question wins.
        {"name": "prompt", "title": "Question", "type": "textarea",
         "default": "Describe this picture.",
         "help": "What to ask about the image"},
        # Zero means automatic, and automatic is per model: a model that
        # reasons before answering needs a far bigger budget than one that
        # does not, and a single number for both starves one of them.
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "text": [
        {"name": "model", "title": "Model", "type": "select", "source": "ai_models",
         "default": "bonsai2-27b"},
        # The instruction can be typed here instead of needing a node of its
        # own; a wired instruction wins over this one when both are present.
        {"name": "prompt", "title": "Instruction", "type": "textarea", "default": "",
         "help": "What to do with the text coming in, e.g. 'Summarise in one sentence'"},
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "image": [
        # Drawn as a picture list, not a text dropdown: a model is recognised
        # by what it produces, and the file name says nothing.
        {"name": "checkpoint", "title": "Model", "type": "model",
         "source": "checkpoints", "default": "",
         "help": "Leave empty for the workflow's own model"},
        {"name": "lora", "title": "Style (LoRA)", "type": "model",
         "source": "loras", "default": ""},
        {"name": "lora_strength", "title": "Style strength", "type": "range",
         "min": 0, "max": 1.5, "step": 0.05, "default": 0,
         "help": "0 leaves the workflow's own strength"},
        {"name": "mode", "title": "Mode", "type": "select", "default": "",
         "options": [
             {"value": "", "title": "Plain"},
             {"value": "z_depth", "title": "From depth"},
             {"value": "t_pose", "title": "T-pose"},
             {"value": "open_pose", "title": "Open pose"},
             {"value": "inpaint", "title": "Inpaint"},
         ],
         "help": "The reference picture is read differently in each mode"},
        {"name": "negative_prompt", "title": "Avoid", "type": "text", "default": ""},
        {"name": "width", "title": "Width", "type": "select", "default": 1024,
         "options": [{"value": 768, "title": "768"}, {"value": 1024, "title": "1024"},
                     {"value": 1280, "title": "1280"}, {"value": 1536, "title": "1536"}]},
        {"name": "height", "title": "Height", "type": "select", "default": 1024,
         "options": [{"value": 768, "title": "768"}, {"value": 1024, "title": "1024"},
                     {"value": 1280, "title": "1280"}, {"value": 1536, "title": "1536"}]},
        # The range starts at zero because zero is a real choice here: it
        # means "leave the workflow's own value alone". A minimum of 4 would
        # be silently clamped up by the browser and every render would go out
        # with four steps, which ruins the picture.
        {"name": "steps", "title": "Steps", "type": "range", "min": 0, "max": 60,
         "step": 1, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "creativity", "title": "Creativity", "type": "range", "min": 0,
         "max": 1, "step": 0.05, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 2147483647,
         "step": 1, "default": 0, "help": "0 gives a different picture each run"},
    ],
    "video": [
        {"name": "checkpoint", "title": "Model", "type": "model",
         "source": "checkpoints", "default": "",
         "help": "Leave empty for the workflow's own model"},
        {"name": "lora", "title": "Style (LoRA)", "type": "model",
         "source": "loras", "default": ""},
        {"name": "lora_strength", "title": "Style strength", "type": "range",
         "min": 0, "max": 1.5, "step": 0.05, "default": 0,
         "help": "0 leaves the workflow's own strength"},
        {"name": "quality", "title": "Workflow", "type": "select", "default": "standard",
         "options": [
             {"value": "standard", "title": "Standard — gen_animation_by_url"},
             {"value": "hq", "title": "High quality — slower, fewer nodes take it"},
         ],
         "help": "These are the animation workflows the render workers advertise"},
        {"name": "frame_count", "title": "Frames", "type": "range", "min": 24, "max": 400,
         "step": 8, "default": 96, "help": "About 24 frames to the second"},
        {"name": "negative_prompt", "title": "Avoid", "type": "text", "default": ""},
        # The range starts at zero because zero is a real choice here: it
        # means "leave the workflow's own value alone". A minimum of 4 would
        # be silently clamped up by the browser and every render would go out
        # with four steps, which ruins the picture.
        {"name": "steps", "title": "Steps", "type": "range", "min": 0, "max": 60,
         "step": 1, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "creativity", "title": "Creativity", "type": "range", "min": 0,
         "max": 1, "step": 0.05, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 2147483647,
         "step": 1, "default": 0},
    ],
    "3dmodel": [
        {"name": "quality", "title": "Quality", "type": "select", "default": "standard",
         "options": [{"value": "fast", "title": "Fast"},
                     {"value": "standard", "title": "Standard"},
                     {"value": "high", "title": "High"}]},
        {"name": "background_method", "title": "Cut out the subject", "type": "select",
         "default": "auto",
         "options": [{"value": "auto", "title": "Automatic"},
                     {"value": "rembg", "title": "rembg"},
                     {"value": "none", "title": "Leave the background"}],
         "help": "Hunyuan fails outright when nothing survives the cut-out"},
    ],
}


def params_for(service_id: str) -> List[Dict[str, object]]:
    return PARAMS.get(service_id) or []


def service(service_id: str) -> Optional[Dict[str, object]]:
    for entry in SERVICES:
        if entry["id"] == service_id:
            return entry
    return None


def accepted_types(entry: Dict[str, object]) -> List[str]:
    return [str(item["type"]) for item in entry.get("inputs") or []]


def produced_types(entry: Dict[str, object]) -> List[str]:
    return [str(item["type"]) for item in entry.get("outputs") or []]


def targets_for(entity_type: str) -> List[Dict[str, str]]:
    """Services that can take this entity as an input, for the handoff menu.

    A planned service is included and flagged: knowing where a result is meant
    to go is useful before the destination is callable.
    """
    targets = []
    for entry in SERVICES:
        for item in entry.get("inputs") or []:
            if str(item["type"]) != entity_type:
                continue
            targets.append({
                "service_id": str(entry["id"]),
                "title": str(entry["title"]),
                "path": str(entry["path"]),
                "field": str(item["field"]),
                "status": str(entry["status"]),
                "input_title": str(item.get("title") or item["field"]),
            })
            break
    return targets


@router.get("/api/ai/services")
async def api_ai_services():
    """The typed catalogue the pages build themselves from."""
    return {
        "success_bool": True,
        "entity_types_array": ENTITY_TYPES,
        "services_array": [
            dict(entry, accepts_array=accepted_types(entry),
                 produces_array=produced_types(entry),
                 params_array=params_for(str(entry["id"])))
            for entry in SERVICES
        ],
        "handoff_object": {
            entity["id"]: targets_for(str(entity["id"]))
            for entity in ENTITY_TYPES
        },
        "server_time_unix_int": int(time.time()),
    }
