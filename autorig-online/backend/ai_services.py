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
            {"type": TEXT, "field": "prompt", "required": True,
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
            {"type": TEXT, "field": "prompt", "required": True, "title": "Prompt"},
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
        "api": "/api/generate/from-image",
        "summary": "Turn a picture into a rigged 3D character.",
        # The backend exists and works, but it spends credits and needs a
        # signed-in account, so it needs a page of its own before it can be
        # offered here. Planned until that page exists, so the nav never links
        # somewhere that answers 404.
        "status": "planned",
        "requires_account": True,
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Picture of the character"},
        ],
        "outputs": [
            {"type": MODEL3D, "field": "model_url_string", "title": "Rigged model"},
            {"type": IMAGE, "field": "preview_url_string", "title": "Preview"},
        ],
    },
]


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
                 produces_array=produced_types(entry))
            for entry in SERVICES
        ],
        "handoff_object": {
            entity["id"]: targets_for(str(entity["id"]))
            for entity in ENTITY_TYPES
        },
        "server_time_unix_int": int(time.time()),
    }
