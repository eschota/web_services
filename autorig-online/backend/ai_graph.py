"""Saved node graphs and the built-in templates behind `autorig.online/nodes`.

A graph is a plain description of which services are wired to which: nothing
here runs anything. The browser walks the graph and calls the same public
`/api/*` endpoints a person would, so a composition can never do more than the
services already allow, and a service gains nothing to maintain by being
reachable from the editor.

Graphs are stored as one small JSON file each, outside the release tree, so a
deploy neither carries them nor loses them.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pathlib
import re
import time
import uuid
from typing import Dict, List, Optional

from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator, model_validator

import ai_services

logger = logging.getLogger(__name__)

router = APIRouter()

GRAPH_DIR = pathlib.Path(
    os.getenv("AUTORIG_AI_GRAPH_DIR", "/srv/autorig/data/var/ai-graphs")
)
MAX_NODES = 200
MAX_GRAPH_BYTES = 1024 * 1024
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{4,64}$")

# A node is either a service call or a value the person supplies. Keeping the
# two apart means a graph always says where its raw material came from instead
# of hiding a literal inside a service's parameters.
NODE_INPUT = "input"
NODE_SERVICE = "service"


class GraphNode(BaseModel):
    id: str
    kind: str = NODE_SERVICE
    service: Optional[str] = None
    entity_type: Optional[str] = None
    value: Optional[str] = None
    x: float = 0
    y: float = 0
    params: Dict[str, object] = Field(default_factory=dict)


class GraphLink(BaseModel):
    from_node: str = Field(..., alias="from")
    output: str = ""
    to_node: str = Field(..., alias="to")
    input: str = ""

    model_config = {"populate_by_name": True}


class HistoryEntry(BaseModel):
    """One prior completed value, deliberately without farm task metadata."""

    type: str = Field(..., min_length=1, max_length=32)
    value: str = Field(..., min_length=1, max_length=65536)
    input_reference_url: str = Field("", max_length=4096)
    created_at: float = Field(0, ge=0)

    @model_validator(mode="after")
    def validate_media(self):
        allowed = {str(item["id"]) for item in ai_services.ENTITY_TYPES}
        if self.type not in allowed:
            raise ValueError(f"unknown history entity type '{self.type}'")
        if self.value.startswith(("data:", "blob:")):
            raise ValueError("history cannot contain inline or temporary media")
        if self.type != ai_services.TEXT:
            if len(self.value) > 4096 or not self.value.startswith(("http://", "https://")):
                raise ValueError("media history values must be public http(s) URLs")
        if self.input_reference_url and not self.input_reference_url.startswith(
            ("http://", "https://")
        ):
            raise ValueError("history input_reference_url must be a public http(s) URL")
        return self


MAX_RESULT_OUTPUTS = 24

# Avatar builds from explicit sources mark their assets private
# (ai_avatar_build). Saved graphs are public, so those addresses are never
# stored in results: the page that ran the build still shows them live.
_AVATAR_ASSET_RE = re.compile(r"/api/ai/avatar-assets/([a-f0-9]{32})/")


def _private_asset_url(value: object) -> bool:
    match = _AVATAR_ASSET_RE.search(str(value or ""))
    if not match:
        return False
    root = pathlib.Path(os.getenv("AUTORIG_AI_AVATAR_ASSET_DIR", "/srv/autorig/data/var/ai-avatar-assets"))
    try:
        meta = json.loads((root / "assets" / match.group(1) / "metadata.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return bool(isinstance(meta, dict) and meta.get("private"))


class NodeResult(BaseModel):
    """What one node produced, or is still producing.

    Stored with the graph so a deep link shows the run and not just the
    wiring. `task_id` is what lets a reopened link carry on watching a render
    that is still going rather than showing a permanently half-finished page.
    """

    status: str = "done"          # running | done | failed | skipped
    type: str = ""                # entity type of `value`
    value: str = ""               # the text, or the address of the file
    task_id: str = ""
    error: str = ""
    started_at: float = 0
    input_reference_url: str = Field("", max_length=4096)
    history: List[HistoryEntry] = Field(default_factory=list, max_length=5)
    # A node with several outputs (the Avatar builder: the Avatar, eight
    # views, a sheet, a description) keeps each one by its output field, so a
    # reopened link can feed every socket and not only the first.
    outputs: Dict[str, str] = Field(default_factory=dict)
    # X9 mode (2026-09-27): nine seeds of one node, {seed, status, value, error}
    # each, and which one is the node's output (`pick`).
    x9: List[Dict[str, object]] = Field(default_factory=list, max_length=9)
    pick: int = -1
    x9sig: str = Field("", max_length=200000)

    @field_validator("input_reference_url")
    @classmethod
    def validate_input_reference_url(cls, value: str) -> str:
        value = str(value or "")
        if value and not value.startswith(("http://", "https://")):
            raise ValueError("input_reference_url must be a public http(s) URL")
        return value

    @model_validator(mode="after")
    def drop_private_media(self):
        if _private_asset_url(self.value):
            self.value = ""
        if _private_asset_url(self.input_reference_url):
            self.input_reference_url = ""
        self.outputs = {key: ("" if _private_asset_url(item) else item) for key, item in self.outputs.items()}
        self.history = [entry for entry in self.history if not _private_asset_url(entry.value)]
        return self

    @field_validator("outputs")
    @classmethod
    def validate_outputs(cls, value: Dict[str, str]) -> Dict[str, str]:
        if len(value) > MAX_RESULT_OUTPUTS:
            raise ValueError(f"at most {MAX_RESULT_OUTPUTS} outputs per node")
        clean: Dict[str, str] = {}
        for key, item in value.items():
            key, item = str(key), str(item or "")
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key):
                raise ValueError(f"'{key}' is not an output field name")
            if item.startswith(("data:", "blob:")) or len(item) > 8192:
                raise ValueError("outputs hold addresses or short text, not inline media")
            clean[key] = item
        return clean


# Render quality: the factor every width/height is multiplied by at submit time.
RENDER_QUALITIES = {"preview": 0.25, "fast": 0.5, "normal": 1.0, "highquality": 2.0}
# What a newly created graph renders at when its body names no quality.
DEFAULT_RENDER_QUALITY = "preview"


class Graph(BaseModel):
    name: str = "Untitled"
    # Empty keeps every legacy graph's content-derived id unchanged. A
    # duplicate receives a fresh value so its otherwise identical snapshot has
    # an independent deep link and subsequent result updates stay on it.
    instance_id: str = Field("", max_length=64)
    comparison_anchor_id: str = Field("", max_length=64)
    # One scale for every width/height the graph sends (the editor applies it
    # at submit time; node params keep the base size). "normal" is left out
    # of a graph's identity, so every graph saved before this existed keeps
    # its id. A NEW graph that does not say otherwise is a draft (preview,
    # every size / 4): the owner tests in draft first (2026-09-26). A PUT
    # that omits the field keeps the stored graph's value (see update).
    render_quality: str = Field(DEFAULT_RENDER_QUALITY, max_length=16)
    nodes: List[GraphNode] = Field(default_factory=list)
    links: List[GraphLink] = Field(default_factory=list)
    # Keyed by node id. Never part of what makes a graph's identity: a rerun
    # must update the same link, not mint a new one.
    results: Dict[str, NodeResult] = Field(default_factory=dict)
    # Branch isolation (Ctrl+O): {"target": node id, "prior": {node id: was
    # bypassed}}. Absent when nothing is isolated; never part of identity.
    isolation: Optional[Dict[str, object]] = None

    @field_validator("instance_id")
    @classmethod
    def validate_instance_id(cls, value: str) -> str:
        value = str(value or "")
        if value and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
            raise ValueError("instance_id may contain letters, digits, - and _")
        return value

    @field_validator("render_quality")
    @classmethod
    def validate_render_quality(cls, value: str) -> str:
        value = str(value or "normal").strip().lower()
        if value not in RENDER_QUALITIES:
            raise ValueError("render_quality is one of " + ", ".join(RENDER_QUALITIES))
        return value

    @field_validator("comparison_anchor_id")
    @classmethod
    def validate_comparison_anchor_id(cls, value: str) -> str:
        value = str(value or "")
        if value and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
            raise ValueError("comparison_anchor_id may contain letters, digits, - and _")
        return value


class DuplicateGraphRequest(BaseModel):
    current_graph: Graph = Field(..., alias="currentGraph")

    model_config = {"populate_by_name": True}


# ----------------------------------------------------------------- templates

def _template_simple_text_to_video_by_vision() -> Dict[str, object]:
    """The reference composition: one picture in, four things out.

    Vision reads the picture into a prompt, that prompt draws a new picture,
    and the new picture becomes a plain clip, a looping clip, and a 3D model.
    The loop is made by feeding the same frame in as the last frame, which is
    the only difference between the two video nodes.
    """
    return {
        "id": "SimpleTextToVideoByVision",
        "title": "SimpleTextToVideoByVision",
        "summary": (
            "A picture is described by Vision, the description draws a new "
            "picture, and that picture becomes a clip, a looping clip and a "
            "3D model."
        ),
        "graph": {
            "name": "SimpleTextToVideoByVision",
            "nodes": [
                {"id": "source", "kind": NODE_INPUT, "entity_type": ai_services.IMAGE,
                 "value": "", "x": 40, "y": 260},
                {"id": "ask", "kind": NODE_INPUT, "entity_type": ai_services.TEXT,
                 "value": ("Describe this picture as a single vivid prompt for an "
                           "image generator. Subject, lighting, mood, lens. "
                           "No preamble."),
                 "x": 40, "y": 60},
                # Not the default 27B: it is served with a 4096-token context,
                # and a farm-sized 1024x1024 picture is about 4140 tokens on
                # its own, so the request is refused before the prompt is even
                # counted. The 9B is served with 8192 and takes the same frame.
                {"id": "vision", "kind": NODE_SERVICE, "service": "vision",
                 "x": 380, "y": 140,
                 "params": {"model": "qwen35-9b-uncensored", "max_output_tokens": 512}},
                {"id": "draw", "kind": NODE_SERVICE, "service": "image",
                 "x": 720, "y": 140,
                 "params": {"width": 960, "height": 540, "mode": ""}},
                {"id": "clip", "kind": NODE_SERVICE, "service": "video",
                 "x": 1080, "y": 20,
                 "params": {"frame_count": 97}},
                {"id": "loop", "kind": NODE_SERVICE, "service": "video",
                 "x": 1080, "y": 300,
                 "params": {"frame_count": 97}},
                {"id": "solid", "kind": NODE_SERVICE, "service": "3dmodel",
                 "x": 1080, "y": 580,
                 "params": {"quality": "standard", "background_method": "auto"}},
            ],
            "links": [
                {"from": "source", "output": "value", "to": "vision", "input": "image"},
                {"from": "ask", "output": "value", "to": "vision", "input": "prompt"},
                {"from": "vision", "output": "answer_string", "to": "draw", "input": "prompt"},
                {"from": "draw", "output": "image_url_string", "to": "clip", "input": "image"},
                {"from": "draw", "output": "image_url_string", "to": "loop", "input": "image"},
                # Same frame at both ends: that is what makes the clip loop.
                {"from": "draw", "output": "image_url_string", "to": "loop",
                 "input": "image_url_end"},
                {"from": "draw", "output": "image_url_string", "to": "solid", "input": "image"},
            ],
        },
    }


def _template_avatar_video_motion() -> Dict[str, object]:
    """A private saved Avatar reenacts the action from a user-owned video.

    The source person's identity is deliberately excluded by the Vision
    instruction. The first frame supplies scene geometry and exact frame-zero
    pose, while the storyboard supplies only chronological motion verbs. Nothing in this built-
    in graph identifies a real Avatar or embeds a private source URL.
    """
    action_prompt = (
        "Read the storyboard chronologically. Output ONLY short chronological "
        "verb phrases separated by semicolons, for example: opens mouth; tilts "
        "head; raises both hands. Describe actions, pose transitions, movement "
        "direction, and camera movement only. Do not describe any subject, "
        "identity, face, hair, body, clothing, appearance, scene, mood, style, "
        "lighting, or cinematic qualities. Do not write a caption, generator "
        "prompt, sentence, introduction, or explanation."
    )
    first_frame_instruction = (
        "Replace the principal actor in the scene reference with the saved "
        "Avatar. The Avatar's canonical hair and wardrobe override all source "
        "garments and headwear; preserve any headwear that belongs to the saved "
        "Avatar profile. Preserve only the exact first-frame body pose, head "
        "angle, hand and finger positions, framing, camera angle, background, "
        "and scene props. This is frame "
        "zero: do not anticipate, begin, or advance any later action."
    )
    return {
        "id": "SavedAvatarVideoMotion",
        "title": "Avatar reenacts a video · LTX Pose",
        "summary": (
            "Choose a saved Avatar and a driving video. The scene and action "
            "are reconstructed without copying the source person's identity."
        ),
        "graph": {
            "name": "Saved Avatar video motion",
            "nodes": [
                {"id": "source_video", "kind": NODE_INPUT,
                 "entity_type": ai_services.VIDEO, "value": "", "x": 40, "y": 180},
                {"id": "saved_avatar", "kind": NODE_INPUT,
                 "entity_type": ai_services.AVATAR, "value": "", "x": 40, "y": 500},
                {"id": "first_frame_instruction", "kind": NODE_INPUT,
                 "entity_type": ai_services.TEXT, "value": first_frame_instruction,
                 "x": 680, "y": 40},
                {"id": "first_frame", "kind": NODE_SERVICE,
                 "service": "video_frame", "x": 350, "y": 40, "params": {}},
                {"id": "storyboard", "kind": NODE_SERVICE,
                 "service": "video_storyboard", "x": 350, "y": 300, "params": {}},
                {"id": "action_vision", "kind": NODE_SERVICE,
                 "service": "vision", "x": 680, "y": 300,
                 "params": {"model": "qwen35-9b-uncensored", "prompt": action_prompt,
                            "max_output_tokens": 1024}},
                {"id": "avatar_scene", "kind": NODE_SERVICE,
                 "service": "avatar_image", "x": 1010, "y": 100,
                 "params": {"width": 960, "height": 540, "seed": 0}},
                {"id": "motion_video", "kind": NODE_SERVICE,
                 "service": "video_control", "x": 1360, "y": 180,
                 "params": {"width": 960, "height": 540, "frame_count": 97,
                            "control_channel": "pose", "control_strength": 0.85,
                            "seed": 0}},
            ],
            "links": [
                {"from": "source_video", "output": "value",
                 "to": "first_frame", "input": "video_url"},
                {"from": "source_video", "output": "value",
                 "to": "storyboard", "input": "video_url"},
                {"from": "storyboard", "output": "image_url_string",
                 "to": "action_vision", "input": "image"},
                {"from": "saved_avatar", "output": "value",
                 "to": "avatar_scene", "input": "avatar"},
                {"from": "first_frame", "output": "image_url_string",
                 "to": "avatar_scene", "input": "image"},
                {"from": "first_frame_instruction", "output": "value",
                 "to": "avatar_scene", "input": "prompt"},
                {"from": "avatar_scene", "output": "image_url_string",
                 "to": "motion_video", "input": "image"},
                {"from": "source_video", "output": "value",
                 "to": "motion_video", "input": "control_video_url"},
                {"from": "action_vision", "output": "answer_string",
                 "to": "motion_video", "input": "prompt"},
            ],
        },
    }


def _template_saved_avatar_wan_motion() -> Dict[str, object]:
    """The same identity-safe preparation, finished by Wan-Animate-2."""
    template = json.loads(json.dumps(_template_avatar_video_motion()))
    template["id"] = "SavedAvatarWanMotion"
    template["title"] = "Avatar reenacts a video · Wan-Animate-2"
    template["summary"] = (
        "Stable Avatar + driving video; catalogue-fixed 6-step "
        "Wan-Animate-2 motion transfer."
    )
    graph = template["graph"]
    graph["name"] = "Saved Avatar Wan-Animate-2 motion"
    final = next(node for node in graph["nodes"] if node["id"] == "motion_video")
    final["service"] = "avatar_video"
    final["params"] = {
        "width": 960, "height": 540, "frame_count": 97,
        "control_strength": 1, "seed": 0,
    }
    graph["links"].append({
        "from": "saved_avatar", "output": "value",
        "to": "motion_video", "input": "avatar",
    })
    return template


def templates() -> List[Dict[str, object]]:
    return [
        _template_simple_text_to_video_by_vision(),
        _template_avatar_video_motion(),
        _template_saved_avatar_wan_motion(),
    ]


# ------------------------------------------------------------------ validation

def validate(graph: Graph) -> None:
    """Reject a graph the runner could not execute, and say why.

    Checked here rather than only in the browser because a deep link is a URL
    somebody can hand to anybody, and a saved graph that cannot run is a worse
    thing to share than a refused save.
    """
    if len(graph.nodes) > MAX_NODES:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_too_large",
            "message_string": f"A graph may hold at most {MAX_NODES} nodes"})
    if not graph.nodes:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_empty",
            "message_string": "A graph needs at least one node"})

    by_id: Dict[str, GraphNode] = {}
    for node in graph.nodes:
        if node.id in by_id:
            raise HTTPException(status_code=400, detail={
                "error_string": "duplicate_node_id",
                "message_string": f"Two nodes share the id '{node.id}'"})
        by_id[node.id] = node
        if node.kind == NODE_SERVICE:
            entry = ai_services.service(str(node.service or ""))
            if not entry:
                raise HTTPException(status_code=400, detail={
                    "error_string": "unknown_service",
                    "message_string": f"No service called '{node.service}'"})
        elif node.kind == NODE_INPUT:
            if node.entity_type not in (t["id"] for t in ai_services.ENTITY_TYPES):
                raise HTTPException(status_code=400, detail={
                    "error_string": "unknown_entity_type",
                    "message_string": f"No entity type '{node.entity_type}'"})
        else:
            raise HTTPException(status_code=400, detail={
                "error_string": "unknown_node_kind",
                "message_string": f"A node is either '{NODE_SERVICE}' or '{NODE_INPUT}'"})

    if graph.comparison_anchor_id and graph.comparison_anchor_id not in by_id:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_comparison_anchor",
            "message_string": (
                f"The comparison anchor '{graph.comparison_anchor_id}' is not a graph node"
            ),
        })

    for link in graph.links:
        source = by_id.get(link.from_node)
        target = by_id.get(link.to_node)
        if not source or not target:
            raise HTTPException(status_code=400, detail={
                "error_string": "dangling_link",
                "message_string": f"A link names a node that is not in the graph"})
        if target.kind != NODE_SERVICE:
            raise HTTPException(status_code=400, detail={
                "error_string": "link_into_input",
                "message_string": "An input node takes nothing; it only produces"})
        produced = _output_type(source, link.output)
        accepted = _input_type(target, link.input)
        also = _input_also_accepts(target, link.input)
        media_fits = produced == ai_services.MEDIA and (
            accepted in (ai_services.IMAGE, ai_services.VIDEO)
            or ai_services.IMAGE in also or ai_services.VIDEO in also)
        # A control map is a raster: any picture socket takes it as a reference.
        raster_fits = str(produced or "").startswith("control_") and (
            accepted == ai_services.IMAGE or ai_services.IMAGE in also)
        if produced is None or accepted is None or (
                produced != accepted and produced not in also and not media_fits
                and not raster_fits):
            raise HTTPException(status_code=400, detail={
                "error_string": "type_mismatch",
                "message_string": (
                    f"'{link.from_node}.{link.output}' produces {produced or 'nothing'}, "
                    f"but '{link.to_node}.{link.input}' takes {accepted or 'nothing'}")})

    _reject_cycles(graph, by_id)


LEGACY_MEDIA_INPUTS = (ai_services.IMAGE, ai_services.VIDEO)


_MOJIBAKE_MARKERS = ("В·", "вЂ", "Р’", "Г—", "в†", "В ")


def repair_mojibake(text):
    """UTF-8 text once (or twice) misread as cp1251 back to what was typed.

    "Vision В· person" -> "Vision · person". Graphs written through
    Windows shells picked this up (2026-09-27). Only strings carrying the
    telltale pairs are touched, and only when the round trip decodes cleanly,
    so real Russian text is never altered.
    """
    if not isinstance(text, str) or not any(marker in text for marker in _MOJIBAKE_MARKERS):
        return text
    fixed = text
    for _ in range(3):
        if not any(marker in fixed for marker in _MOJIBAKE_MARKERS):
            break
        try:
            fixed = fixed.encode("cp1251").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
    return fixed


def _repair_tree(value):
    if isinstance(value, str):
        return repair_mojibake(value)
    if isinstance(value, list):
        return [_repair_tree(item) for item in value]
    if isinstance(value, dict):
        return {key: _repair_tree(item) for key, item in value.items()}
    return value


def migrate_media_inputs(graph) -> None:
    """Old Image-in / Video-in nodes become the universal Media node in place.

    Works on a Graph or a plain dict; ids, values and links are untouched.
    Also repairs cp1251 mojibake in the name, labels, params and input values.
    """
    if isinstance(graph, dict):
        if isinstance(graph.get("name"), str):
            graph["name"] = repair_mojibake(graph["name"])
    elif isinstance(getattr(graph, "name", None), str):
        graph.name = repair_mojibake(graph.name)
    nodes = graph.get("nodes") if isinstance(graph, dict) else getattr(graph, "nodes", None)
    for node in nodes or []:
        if isinstance(node, dict):
            if isinstance(node.get("params"), dict):
                node["params"] = _repair_tree(node["params"])
            if isinstance(node.get("value"), str):
                node["value"] = repair_mojibake(node["value"])
        else:
            if isinstance(getattr(node, "params", None), dict):
                node.params = _repair_tree(node.params)
            if isinstance(getattr(node, "value", None), str):
                node.value = repair_mojibake(node.value)
    for node in nodes or []:
        if isinstance(node, dict):
            if node.get("kind") == NODE_INPUT and node.get("entity_type") in LEGACY_MEDIA_INPUTS:
                node["entity_type"] = ai_services.MEDIA
        elif getattr(node, "kind", None) == NODE_INPUT and node.entity_type in LEGACY_MEDIA_INPUTS:
            node.entity_type = ai_services.MEDIA


def _output_type(node: GraphNode, field: str) -> Optional[str]:
    if node.kind == NODE_INPUT:
        return node.entity_type if field in ("", "value") else None
    entry = ai_services.service(str(node.service or "")) or {}
    for item in entry.get("outputs") or []:
        if str(item["field"]) == field:
            return str(item["type"])
    return None


def _input_type(node: GraphNode, field: str) -> Optional[str]:
    entry = ai_services.service(str(node.service or "")) or {}
    for item in entry.get("inputs") or []:
        if str(item["field"]) == field:
            return str(item["type"])
    return None


def _input_also_accepts(node: GraphNode, field: str) -> List[str]:
    """Other types a socket takes; a reference socket reads a video's first frame."""
    entry = ai_services.service(str(node.service or "")) or {}
    for item in entry.get("inputs") or []:
        if str(item["field"]) == field:
            return [str(value) for value in (item.get("also_accepts") or [])]
    return []


def _reject_cycles(graph: Graph, by_id: Dict[str, GraphNode]) -> None:
    """A composition is a pipeline; a loop in it would never finish."""
    edges: Dict[str, List[str]] = {node_id: [] for node_id in by_id}
    for link in graph.links:
        edges[link.from_node].append(link.to_node)
    state: Dict[str, int] = {}

    def walk(node_id: str) -> None:
        if state.get(node_id) == 2:
            return
        if state.get(node_id) == 1:
            raise HTTPException(status_code=400, detail={
                "error_string": "graph_has_a_cycle",
                "message_string": f"The wiring loops back on '{node_id}'"})
        state[node_id] = 1
        for nxt in edges.get(node_id, ()):
            walk(nxt)
        state[node_id] = 2

    for node_id in by_id:
        walk(node_id)


# -------------------------------------------------------------------- storage

def _path_for(graph_id: str) -> pathlib.Path:
    if not SAFE_ID_RE.match(graph_id):
        raise HTTPException(status_code=400, detail={
            "error_string": "bad_graph_id",
            "message_string": "A graph id is 4-64 characters of letters, digits, - or _"})
    return GRAPH_DIR / f"{graph_id}.json"


# Superseded copies are moved here rather than deleted: the library no longer
# lists them, but a link somebody already shared keeps opening.
ARCHIVE_SUBDIR = "archive"


def _stored_path(graph_id: str) -> pathlib.Path:
    """Where a saved graph lives now: the store, else its archive."""
    path = _path_for(graph_id)
    if not path.exists():
        archived = GRAPH_DIR / ARCHIVE_SUBDIR / path.name
        if archived.exists():
            return archived
    return path


def _new_id(payload: str) -> str:
    """Short, content-derived, and stable: saving the same graph twice gives
    the same link instead of littering the store with copies."""
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:12]


def _identity_payload(body: Dict[str, object]) -> str:
    """Serialize graph identity without results or an empty legacy instance.

    ``Graph.model_dump`` includes default fields. Excluding the empty instance
    explicitly prevents introducing this feature from changing every existing
    graph id on its next save.
    """
    identity = {
        key: value
        for key, value in body.items()
        if key not in ("results", "isolation") and not (
            key in {"instance_id", "comparison_anchor_id"} and not value
        ) and not (key == "render_quality" and value in ("", "normal"))
    }
    # Pydantic preserves an incoming integer ``0`` for a float field on the
    # first model dump, while JSON load + revalidation emits ``0.0``. New
    # instance-aware graphs normalize only their canvas coordinates so opening
    # and saving the same copy cannot move its link. Legacy identity remains
    # byte-for-byte compatible with the pre-instance serializer above.
    if identity.get("instance_id"):
        nodes = identity.get("nodes") or []
        node_ids = {
            str(node.get("id")): f"node-{index}"
            for index, node in enumerate(nodes)
            if isinstance(node, dict)
        }
        identity["nodes"] = [
            {
                **node,
                "id": node_ids.get(str(node.get("id")), str(node.get("id"))),
                "x": float(node.get("x") or 0),
                "y": float(node.get("y") or 0),
            }
            if isinstance(node, dict) else node
            for node in nodes
        ]
        identity["links"] = [
            {
                **link,
                "from": node_ids.get(str(link.get("from")), str(link.get("from"))),
                "to": node_ids.get(str(link.get("to")), str(link.get("to"))),
            }
            if isinstance(link, dict) else link
            for link in (identity.get("links") or [])
        ]
        anchor = str(identity.get("comparison_anchor_id") or "")
        if anchor:
            identity["comparison_anchor_id"] = node_ids.get(anchor, anchor)
    return json.dumps(identity, ensure_ascii=False, sort_keys=True)


@router.get("/api/ai/graph/templates")
async def api_graph_templates():
    """Compositions that ship with the editor."""
    listed = templates()
    for template in listed:
        migrate_media_inputs(template.get("graph") or {})
    return {
        "success_bool": True,
        "templates_array": listed,
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/ai/graphs")
async def api_graph_save(graph: Graph):
    """Store a graph and hand back the link that reopens it."""
    migrate_media_inputs(graph)
    validate(graph)
    body = graph.model_dump(by_alias=True)
    # The link names the composition, not the run: saving after a render must
    # land on the same link so the one already shared stays the right one.
    identity = _identity_payload(body)
    payload = json.dumps(body, ensure_ascii=False, sort_keys=True)
    if len(payload.encode("utf-8")) > MAX_GRAPH_BYTES:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_too_large",
            "message_string": "The graph is larger than the store accepts"})
    graph_id = _new_id(identity)
    now = int(time.time())
    saved_at = now
    existing = _read_stored(_path_for(graph_id))
    if existing is not None:
        # The id is derived from content, but a document that has since been
        # edited in place (PUT below) no longer holds that content. Landing on
        # it again would silently replace somebody's newer graph, so this
        # snapshot gets an id of its own instead.
        if _identity_payload(existing.get("graph") or {}) != identity:
            graph_id = _new_id(identity + "|" + uuid.uuid4().hex)
            existing = None
        else:
            saved_at = int(existing.get("saved_at_unix_int") or now)
    try:
        GRAPH_DIR.mkdir(parents=True, exist_ok=True)
        stored = {"id": graph_id, "saved_at_unix_int": saved_at, "graph": body,
                  "revision_int": int((existing or {}).get("revision_int") or 0) + 1}
        if existing is not None:
            stored["updated_at_unix_int"] = now
        _path_for(graph_id).write_text(
            json.dumps(stored, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Could not write graph %s", graph_id)
        raise HTTPException(status_code=500, detail={
            "error_string": "graph_not_saved",
            "message_string": "The graph store did not accept the file"}) from None
    return {
        "success_bool": True,
        "graph_id_string": graph_id,
        "deep_link_string": f"/nodes?g={graph_id}",
        "revision_int": stored["revision_int"],
        "server_time_unix_int": int(time.time()),
    }


def _migrated(graph: Dict[str, object]) -> Dict[str, object]:
    migrate_media_inputs(graph)
    return graph


def _read_stored(path: pathlib.Path) -> Optional[Dict[str, object]]:
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        logger.exception("Could not read graph %s", path.name)
        raise HTTPException(status_code=500, detail={
            "error_string": "graph_unreadable",
            "message_string": "The stored graph could not be read"}) from None
    return stored if isinstance(stored, dict) else None


def _node_signature(node: Dict[str, object]) -> str:
    """Everything that makes a node's result valid, minus where it is drawn."""
    return json.dumps({key: value for key, value in node.items() if key not in {"x", "y"}},
                      ensure_ascii=False, sort_keys=True)


def _carry_results(previous: Dict[str, object], body: Dict[str, object]) -> Dict[str, object]:
    """Results the stored copy has and the incoming one does not, kept safely.

    Another tab may have finished a node since this one last looked. Drawflow
    renumbers nodes on reload, so a stored result is carried over only onto a
    node that is the same in every respect but its position; anything else
    belongs to a node that no longer exists.
    """
    incoming = dict(body.get("results") or {})
    old_graph = previous.get("graph") or {}
    old_results = old_graph.get("results") or {}
    old_nodes = {str(node.get("id")): node for node in old_graph.get("nodes") or []
                 if isinstance(node, dict)}
    for node in body.get("nodes") or []:
        key = str(node.get("id"))
        if key in incoming or key not in old_results or key not in old_nodes:
            continue
        if _node_signature(old_nodes[key]) == _node_signature(node):
            incoming[key] = old_results[key]
    return incoming


@router.put("/api/ai/graphs/{graph_id}")
async def api_graph_update(graph_id: str, graph: Graph, request: Request):
    """Save an edited graph under the link it already has.

    Ordinary saving is an edit of one document, not a new library entry: the
    content-derived id only names a graph the first time it is stored, and a
    legacy link carries on under the id it was shared with. Last write wins
    between two tabs, except that results the other tab finished are kept.
    Only /duplicate makes a copy.
    """
    for template in templates():
        if str(template["id"]) == graph_id:
            raise HTTPException(status_code=409, detail={
                "error_string": "template_is_read_only",
                "message_string": "A built-in composition is saved as a new graph"})
    migrate_media_inputs(graph)
    validate(graph)
    path = _stored_path(graph_id)
    previous = _read_stored(path)
    if previous is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "graph_not_found",
            "message_string": f"No graph saved as '{graph_id}'"})
    # A tab saves with the revision it opened. If anyone saved since, this tab
    # holds an older graph and would overwrite (and drop) their nodes.
    current_revision = int(previous.get("revision_int") or 0)
    base = str(request.headers.get("x-graph-revision") or "").strip()
    if not base and "/nodes" in str(request.headers.get("referer") or ""):
        # An editor tab opened before the revision guard existed: it holds a
        # canvas of unknown age, so it may not overwrite the stored graph.
        raise HTTPException(status_code=428, detail={
            "error_string": "graph_stale",
            "message_string": "This tab runs an older editor; reload the page (your graph was changed elsewhere)",
            "revision_int": current_revision})
    if base.isdigit() and int(base) != current_revision:
        raise HTTPException(status_code=409, detail={
            "error_string": "graph_stale",
            "message_string": "This graph was changed in another tab or by an agent; reload to get the latest version",
            "revision_int": current_revision})
    body = graph.model_dump(by_alias=True)
    if "render_quality" not in graph.model_fields_set:
        # An edit that does not mention the quality leaves it as it was;
        # the draft default is for new graphs only.
        body["render_quality"] = str(
            (previous.get("graph") or {}).get("render_quality") or "normal")
    body["results"] = _carry_results(previous, body)
    payload = json.dumps(body, ensure_ascii=False, sort_keys=True)
    if len(payload.encode("utf-8")) > MAX_GRAPH_BYTES:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_too_large",
            "message_string": "The graph is larger than the store accepts"})
    now = int(time.time())
    stored = {key: value for key, value in previous.items() if key != "graph"}
    stored.update({"id": graph_id, "graph": body, "updated_at_unix_int": now,
                   "revision_int": current_revision + 1})
    stored.setdefault("saved_at_unix_int", now)
    try:
        path.write_text(json.dumps(stored, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logger.exception("Could not update graph %s", graph_id)
        raise HTTPException(status_code=500, detail={
            "error_string": "graph_not_saved",
            "message_string": "The graph store did not accept the file"}) from None
    return {
        "success_bool": True,
        "graph_id_string": graph_id,
        "deep_link_string": f"/nodes?g={graph_id}",
        "updated_bool": True,
        "revision_int": current_revision + 1,
        "server_time_unix_int": now,
    }


@router.post("/api/ai/graphs/duplicate")
async def api_graph_duplicate(body: DuplicateGraphRequest):
    """Persist an independent snapshot without altering its source graph.

    Results are intentionally retained. A copied running task therefore keeps
    watching the already accepted farm task rather than submitting it again;
    changing a node's parameters remains the browser runner's normal signal to
    invalidate that node's previous result.
    """
    duplicate = body.current_graph.model_copy(deep=True)
    previous_instance = duplicate.instance_id
    while True:
        duplicate.instance_id = str(uuid.uuid4())
        if duplicate.instance_id != previous_instance:
            break
    saved = await api_graph_save(duplicate)
    return {
        **saved,
        "graph_object": duplicate.model_dump(by_alias=True),
        "source_unchanged_bool": True,
    }


@router.put("/api/ai/graphs/{graph_id}/results")
async def api_graph_results(graph_id: str, results: Dict[str, NodeResult]):
    """Record what a run has produced so far, without touching the wiring.

    Written as the run goes rather than once at the end: a clip takes minutes,
    and a link shared while it renders should show it arriving.
    """
    path = _stored_path(graph_id)
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail={
            "error_string": "graph_not_found",
            "message_string": f"No graph saved as '{graph_id}'"}) from None
    except Exception:
        logger.exception("Could not read graph %s", graph_id)
        raise HTTPException(status_code=500, detail={
            "error_string": "graph_unreadable",
            "message_string": "The stored graph could not be read"}) from None
    graph = stored.get("graph") or {}
    known = {str(node.get("id")) for node in graph.get("nodes") or []}
    unknown = set(results) - known
    if unknown:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_node",
            "message_string": f"The graph has no node called '{sorted(unknown)[0]}'"})
    previous_results = graph.get("results") or {}
    merged_results: Dict[str, object] = {}
    for key, incoming in results.items():
        previous_raw = previous_results.get(key) or {}
        try:
            previous = NodeResult.model_validate(previous_raw)
        except Exception:
            previous = NodeResult()

        previous_completed = previous.status.lower() in {"done", "completed"}
        previous_changed = (
            previous.type,
            previous.value,
            previous.input_reference_url,
        ) != (
            incoming.type,
            incoming.value,
            incoming.input_reference_url,
        )
        allowed_types = {str(item["id"]) for item in ai_services.ENTITY_TYPES}
        archived: Optional[HistoryEntry] = None
        if (
            previous_completed
            and previous_changed
            and previous.type in allowed_types
            and previous.value
        ):
            archived = HistoryEntry(
                type=previous.type,
                value=previous.value,
                input_reference_url=previous.input_reference_url,
                created_at=time.time(),
            )
        # Newest first everywhere. The browser already archives the visible
        # result before publishing a running replacement, while this endpoint
        # also archives the previously stored completed result. Those copies
        # have different clocks, so timestamp cannot be part of their identity.
        candidates = ([archived] if archived else []) + [
            *incoming.history, *previous.history
        ]
        candidates.sort(key=lambda entry: entry.created_at, reverse=True)
        history: List[HistoryEntry] = []
        seen_history = set()
        current_media_value = (
            incoming.value
            if incoming.status.lower() in {"done", "completed", "stale"}
            and incoming.type != ai_services.TEXT
            else ""
        )
        for entry in candidates:
            if current_media_value and entry.value == current_media_value:
                continue
            # ControlNet outputs are images. The browser normalises their
            # archived type to `image`, while an older server result still says
            # `control_canny`/`control_pose`/`control_depth`. The URL is the
            # visual identity; type, reference and clocks must not duplicate it.
            signature = (
                (entry.type, entry.value)
                if entry.type == ai_services.TEXT
                else ("media", entry.value)
            )
            if signature in seen_history:
                continue
            history.append(entry)
            seen_history.add(signature)
            if len(history) == 5:
                break
        incoming.history = history
        merged_results[key] = incoming.model_dump()
    graph["results"] = merged_results
    serialized_graph = json.dumps(graph, ensure_ascii=False, sort_keys=True)
    if len(serialized_graph.encode("utf-8")) > MAX_GRAPH_BYTES:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_too_large",
            "message_string": "The graph and its result history are larger than the store accepts",
        })
    stored["graph"] = graph
    stored["results_at_unix_int"] = int(time.time())
    try:
        path.write_text(json.dumps(stored, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logger.exception("Could not write results for graph %s", graph_id)
        raise HTTPException(status_code=500, detail={
            "error_string": "results_not_saved",
            "message_string": "The graph store did not accept the update"}) from None
    return {"success_bool": True, "graph_id_string": graph_id,
            "server_time_unix_int": int(time.time())}


# --------------------------------------------------------------- the library

# `/workflows` shows every saved composition at once, so one request reads the
# whole store. The summaries are therefore kept in memory and rebuilt only for
# the files whose mtime or size moved; nothing here writes, because the store
# belongs to the editor and a gallery that repaired it would be a second
# author of the same files.
LIBRARY_LIMIT_DEFAULT = 60
LIBRARY_LIMIT_MAX = 200
PREVIEW_TEXT_LIMIT = 300
_DONE_STATUSES = {"done", "completed"}

_library_index: Dict[str, object] = {"key": None, "rows": []}
_library_summaries: Dict[tuple, Dict[str, object]] = {}


def _is_public_url(value: str) -> bool:
    return str(value or "").startswith(("http://", "https://"))


def _library_signature(directory: pathlib.Path) -> Optional[tuple]:
    """What the store looks like right now, cheaply enough to ask every time."""
    try:
        directory_mtime = directory.stat().st_mtime_ns
    except OSError:
        return None
    entries = []
    for path in sorted(directory.glob("*.json")):
        try:
            info = path.stat()
        except OSError:
            continue
        entries.append((path.name, info.st_mtime_ns, info.st_size))
    return (str(directory), directory_mtime, tuple(entries))


def _topological_order(node_ids: List[str], links: List[Dict[str, object]]) -> List[str]:
    """Execution order, which is also the order a person reads the canvas in.

    Ties are broken by position in the stored graph so the same file always
    yields the same previews. A stored graph should never contain a cycle, but
    if one somehow does, the nodes it traps are appended rather than dropped.
    """
    position = {node_id: index for index, node_id in enumerate(node_ids)}
    indegree = {node_id: 0 for node_id in node_ids}
    edges: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}
    for link in links:
        source = str(link.get("from") or "")
        target = str(link.get("to") or "")
        if source not in indegree or target not in indegree or source == target:
            continue
        edges[source].append(target)
        indegree[target] += 1
    ready = sorted(position[node_id] for node_id in node_ids if not indegree[node_id])
    order: List[str] = []
    while ready:
        index = ready.pop(0)
        node_id = node_ids[index]
        order.append(node_id)
        for nxt in edges[node_id]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append(position[nxt])
                ready.sort()
    if len(order) < len(node_ids):
        seen = set(order)
        order.extend(node_id for node_id in node_ids if node_id not in seen)
    return order


def _preview(entity_type: str, value: str, node_id: str, label: str) -> Dict[str, object]:
    """One tile on a card: a type, something to show, and nothing else.

    Deliberately carries no task id, no error text and no history — a listing
    is a public-facing index of what exists, not a window into a run.
    """
    text = str(value or "")
    if not _is_public_url(text):
        text = text.strip()
        if len(text) > PREVIEW_TEXT_LIMIT:
            text = text[:PREVIEW_TEXT_LIMIT - 1].rstrip() + "…"
    return {
        "type_string": str(entity_type or ""),
        "value_string": text,
        "node_id_string": str(node_id),
        "label_string": label,
    }


def _preview_input(
    order: List[str],
    by_id: Dict[str, Dict[str, object]],
    results: Dict[str, object],
) -> Optional[Dict[str, object]]:
    """What went in: the picture or clip the person supplied, if there was one."""
    for wanted in (ai_services.IMAGE, ai_services.VIDEO):
        for node_id in order:
            node = by_id.get(node_id) or {}
            if str(node.get("kind") or NODE_SERVICE) != NODE_INPUT:
                continue
            if str(node.get("entity_type") or "") != wanted:
                continue
            value = str(node.get("value") or "").strip()
            if _is_public_url(value):
                return _preview(wanted, value, node_id, "input")
    # A graph can begin with a prompt rather than a picture. Then the first
    # thing it made is the closest honest stand-in for its raw material.
    for node_id in order:
        record = results.get(node_id)
        if not isinstance(record, dict):
            continue
        if str(record.get("status") or "").strip().lower() not in _DONE_STATUSES:
            continue
        value = str(record.get("value") or "").strip()
        if _is_public_url(value):
            return _preview(str(record.get("type") or ""), value, node_id, "input")
    return None


def _preview_output(
    order: List[str],
    by_id: Dict[str, Dict[str, object]],
    results: Dict[str, object],
    has_outgoing: set,
) -> Optional[Dict[str, object]]:
    """What came out: the last editable node that produced anything at all.

    Sinks win over mid-pipeline nodes because a sink is what the composition
    was built to reach; a finished result wins over a stale one because stale
    means the wiring has moved on since it was made.
    """
    best: Optional[Dict[str, object]] = None
    best_key: Optional[tuple] = None
    for index, node_id in enumerate(order):
        node = by_id.get(node_id) or {}
        if str(node.get("kind") or NODE_SERVICE) != NODE_SERVICE:
            continue
        record = results.get(node_id)
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "").strip().lower()
        if status in _DONE_STATUSES:
            rank = 2
        elif status == "stale":
            rank = 1
        else:
            continue
        value = str(record.get("value") or "").strip()
        if not value:
            continue
        key = (rank, 0 if node_id in has_outgoing else 1, index)
        if best_key is None or key > best_key:
            best_key = key
            best = _preview(str(record.get("type") or ""), value, node_id, "output")
    return best


def _summarize(path: pathlib.Path) -> Optional[Dict[str, object]]:
    """One stored file reduced to the card that stands for it."""
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Skipping unreadable graph %s in the library", path.name)
        return None
    if not isinstance(stored, dict):
        return None
    graph = stored.get("graph")
    if not isinstance(graph, dict):
        graph = {}
    nodes = [node for node in (graph.get("nodes") or []) if isinstance(node, dict)]
    links = [link for link in (graph.get("links") or []) if isinstance(link, dict)]
    stored_results = graph.get("results")
    results: Dict[str, object] = (
        {str(key): value for key, value in stored_results.items()}
        if isinstance(stored_results, dict) else {}
    )

    services: Dict[str, int] = {}
    for node in nodes:
        if str(node.get("kind") or NODE_SERVICE) == NODE_INPUT:
            name = f"input:{str(node.get('entity_type') or 'unknown')}"
        else:
            name = str(node.get("service") or "unknown")
        services[name] = services.get(name, 0) + 1

    result_counts: Dict[str, int] = {}
    for record in results.values():
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "").strip().lower() or "unknown"
        if status == "completed":
            status = "done"
        result_counts[status] = result_counts.get(status, 0) + 1

    node_ids = [str(node.get("id")) for node in nodes]
    by_id = {str(node.get("id")): node for node in nodes}
    has_outgoing = {str(link.get("from") or "") for link in links}
    order = _topological_order(node_ids, links)

    graph_id = str(stored.get("id") or path.stem)
    try:
        saved_at = int(stored.get("saved_at_unix_int") or 0)
    except (TypeError, ValueError):
        saved_at = 0
    try:
        results_at = int(stored.get("results_at_unix_int") or 0)
    except (TypeError, ValueError):
        results_at = 0
    try:
        updated_at = int(stored.get("updated_at_unix_int") or 0)
    except (TypeError, ValueError):
        updated_at = 0
    return {
        "graph_id_string": graph_id,
        "name_string": str(graph.get("name") or "Untitled"),
        "deep_link_string": f"/nodes?g={graph_id}",
        "saved_at_unix_int": saved_at,
        "results_at_unix_int": results_at,
        "updated_at_unix_int": updated_at,
        "node_count_int": len(nodes),
        "link_count_int": len(links),
        "services_object": services,
        "result_counts_object": result_counts,
        "preview_a_object": _preview_input(order, by_id, results),
        "preview_b_object": _preview_output(order, by_id, results, has_outgoing),
    }


def _library_rows() -> List[Dict[str, object]]:
    """Every saved composition as a card, newest first."""
    directory = GRAPH_DIR
    signature = _library_signature(directory)
    if signature is None:
        return []
    if _library_index.get("key") == signature:
        return list(_library_index.get("rows") or [])
    rows: List[Dict[str, object]] = []
    fresh: Dict[tuple, Dict[str, object]] = {}
    for name, mtime, size in signature[2]:
        key = (str(directory / name), mtime, size)
        row = _library_summaries.get(key)
        if row is None:
            row = _summarize(directory / name)
            if row is None:
                continue
        fresh[key] = row
        rows.append(row)
    rows.sort(
        key=lambda row: (
            max(int(row["results_at_unix_int"] or 0),
                int(row.get("updated_at_unix_int") or 0),
                int(row["saved_at_unix_int"] or 0)),
            str(row["graph_id_string"]),
        ),
        reverse=True,
    )
    _library_summaries.clear()
    _library_summaries.update(fresh)
    _library_index["key"] = signature
    _library_index["rows"] = rows
    return list(rows)


@router.get("/api/ai/graphs")
async def api_graph_library(
    limit: int = LIBRARY_LIMIT_DEFAULT,
    offset: int = 0,
    q: str = "",
):
    """The saved compositions, newest first, for the workflow library."""
    limit = max(1, min(LIBRARY_LIMIT_MAX, int(limit or LIBRARY_LIMIT_DEFAULT)))
    offset = max(0, int(offset or 0))
    needle = str(q or "").strip().lower()
    rows = _library_rows()
    if needle:
        rows = [row for row in rows if needle in str(row["name_string"]).lower()]
    return {
        "success_bool": True,
        "total_int": len(rows),
        "limit_int": limit,
        "offset_int": offset,
        "graphs_array": rows[offset:offset + limit],
        "server_time_unix_int": int(time.time()),
    }


# ------------------------------------------------------------ named links
#
# /nodes/<slug> opens a saved graph by a readable name. The alias table is a
# small JSON file next to the graphs ({slug: graph_id}); a slug that is not in
# it falls back to a saved graph whose name slugifies to it (newest wins).

ALIAS_FILE = "aliases.json"
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,78}[a-z0-9]$")


def _slugify(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")[:80]


def _aliases() -> Dict[str, str]:
    try:
        data = json.loads((GRAPH_DIR / ALIAS_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("Could not read graph aliases")
        return {}
    return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}


def _resolve_slug(slug: str) -> Optional[str]:
    slug = _slugify(slug)
    if not slug:
        return None
    graph_id = _aliases().get(slug)
    if graph_id:
        return graph_id
    for row in _library_rows():
        if _slugify(row.get("name_string")) == slug:
            return str(row["graph_id_string"])
    return None


class GraphAliasRequest(BaseModel):
    slug: str = Field(..., max_length=80)


@router.put("/api/ai/graphs/{graph_id}/alias")
async def api_graph_alias(graph_id: str, body: GraphAliasRequest):
    """Give a saved graph a readable link, /nodes/<slug>.

    First come, first served: a slug that already names another graph is
    refused (409) rather than silently re-pointed.
    """
    slug = _slugify(body.slug)
    if not SLUG_RE.match(slug):
        raise HTTPException(status_code=400, detail={
            "error_string": "bad_slug",
            "message_string": "A slug is 3-80 characters of a-z, 0-9 and -"})
    if _read_stored(_stored_path(graph_id)) is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "graph_not_found",
            "message_string": f"No graph saved as '{graph_id}'"})
    aliases = _aliases()
    if aliases.get(slug) not in (None, graph_id):
        raise HTTPException(status_code=409, detail={
            "error_string": "slug_taken",
            "message_string": f"'{slug}' already opens another graph"})
    aliases[slug] = graph_id
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    tmp = GRAPH_DIR / (ALIAS_FILE + ".tmp")
    tmp.write_text(json.dumps(aliases, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, GRAPH_DIR / ALIAS_FILE)
    return {"success_bool": True, "slug_string": slug, "graph_id_string": graph_id,
            "deep_link_string": f"/nodes/{slug}", "server_time_unix_int": int(time.time())}


@router.get("/nodes/{slug}")
async def nodes_named_link(slug: str, request: Request):
    graph_id = _resolve_slug(slug)
    if not graph_id:
        raise HTTPException(status_code=404, detail={
            "error_string": "graph_not_found",
            "message_string": f"No graph is called '{slug}'"})
    query = {"g": graph_id}
    quality = request.query_params.get("q")
    if quality in RENDER_QUALITIES:
        query["q"] = quality
    return RedirectResponse("/nodes?" + urlencode(query), status_code=302)


@router.get("/api/ai/graphs/{graph_id}")
async def api_graph_load(graph_id: str):
    """Reopen a saved graph, or a template if the id names one."""
    for template in templates():
        if str(template["id"]) == graph_id:
            migrate_media_inputs(template["graph"])
            return {"success_bool": True, "graph_id_string": graph_id,
                    "graph_object": template["graph"],
                    "template_bool": True,
                    "server_time_unix_int": int(time.time())}
    path = _stored_path(graph_id)
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail={
            "error_string": "graph_not_found",
            "message_string": f"No graph saved as '{graph_id}'"}) from None
    except Exception:
        logger.exception("Could not read graph %s", graph_id)
        raise HTTPException(status_code=500, detail={
            "error_string": "graph_unreadable",
            "message_string": "The stored graph could not be read"}) from None
    return {
        "success_bool": True,
        "graph_id_string": graph_id,
        "graph_object": _migrated(stored.get("graph") or {}),
        "template_bool": False,
        "saved_at_unix_int": int(stored.get("saved_at_unix_int") or 0),
        "revision_int": int(stored.get("revision_int") or 0),
        "server_time_unix_int": int(time.time()),
    }


# --------------------------------------------------------------- cancellation

class CancelRequest(BaseModel):
    task_ids: List[str] = Field(default_factory=list)


@router.post("/api/ai/cancel")
async def api_cancel(body: CancelRequest):
    """Stand down work that has not started, and leave alone work that has.

    A render still waiting for a card is pure waste once nobody wants it, and
    the queue is shared, so dropping it gives the capacity back to somebody
    else. A job already on a card has spent real GPU minutes and its output is
    usually still wanted, so it is left to finish — and the converter offers no
    way to stop one anyway, which is the honest reason its tasks are only
    counted here rather than cancelled.
    """
    import httpx

    import ai_vision_api

    cancelled = 0
    running = 0
    unknown = 0
    async with httpx.AsyncClient() as client:
        for task_id in body.task_ids[:64]:
            task_id = str(task_id).strip()
            if not task_id:
                continue
            if "." in task_id:
                # `<node>.<id>` is a converter task: AI or Hunyuan.
                running += 1
                continue
            try:
                response = await client.post(
                    ai_vision_api.RENDERFIN_BASE + "/api-render/cancel-if-pending",
                    json={"task_id": task_id}, timeout=15.0)
                payload = response.json() if response.status_code == 200 else {}
            except Exception:
                logger.warning("Could not reach the render queue to cancel %s", task_id)
                unknown += 1
                continue
            if payload.get("cancelled"):
                cancelled += 1
            elif payload.get("status") == "unknown":
                unknown += 1
            else:
                running += 1
    return {
        "success_bool": True,
        "cancelled_int": cancelled,
        "running_int": running,
        "unknown_int": unknown,
        "note_string": "Jobs already on a card are left to finish",
        "server_time_unix_int": int(time.time()),
    }


# -------------------------------------------------------------- cache purging

# Where the render farm writes what it produces. Compositions share this tree
# with the rest of the site, so nothing is deleted because of where it lives:
# a file is only removed when a stored composition says it produced it.
RENDER_OUTPUT_DIR = pathlib.Path(
    os.getenv("AUTORIG_AI_OUTPUT_DIR", "/srv/autorig/data/var/renderfin/render")
)
RENDER_URL_MARKER = "/renderfin/render/"


def _local_path_for(url: str) -> Optional[pathlib.Path]:
    """The file behind a render URL, or nothing if it is not ours to touch.

    Resolved and then checked to be inside the render tree, so a stored value
    cannot walk out of it — the results are written by a browser and a graph
    can be handed to anybody.
    """
    text = str(url or "").strip()
    marker = text.find(RENDER_URL_MARKER)
    if marker < 0:
        return None
    relative = text[marker + len(RENDER_URL_MARKER):].split("?")[0].split("#")[0]
    if not relative:
        return None
    try:
        candidate = (RENDER_OUTPUT_DIR / relative).resolve()
        root = RENDER_OUTPUT_DIR.resolve()
    except Exception:
        return None
    if root not in candidate.parents:
        return None
    return candidate


@router.delete("/api/ai/cache")
async def api_purge_cache():
    """Delete the saved compositions and the files they produced.

    Really deletes: the point of the button is to get the disk back, so a
    tidy-up that only forgot the index would be worse than none. It deletes
    only files a stored composition claims as its own output — the render tree
    holds work from the rest of the site too, and none of that is ours.

    The farm nodes clean up after themselves, so nothing is asked of them.
    """
    graphs = 0
    removed = 0
    freed = 0
    seen: set = set()
    if GRAPH_DIR.is_dir():
        for path in sorted(GRAPH_DIR.glob("*.json")):
            try:
                stored = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                stored = {}
            results = ((stored.get("graph") or {}).get("results") or {})
            for record in results.values():
                target = _local_path_for((record or {}).get("value") or "")
                if target is None or target in seen:
                    continue
                seen.add(target)
                try:
                    if target.is_file():
                        size = target.stat().st_size
                        target.unlink()
                        removed += 1
                        freed += size
                except Exception:
                    logger.warning("Could not remove %s", target)
            try:
                path.unlink()
                graphs += 1
            except Exception:
                logger.warning("Could not remove graph %s", path)
    logger.info("AI cache purge: %s compositions, %s files, %s bytes",
                graphs, removed, freed)
    return {
        "success_bool": True,
        "graphs_removed_int": graphs,
        "files_removed_int": removed,
        "bytes_freed_int": freed,
        "server_time_unix_int": int(time.time()),
    }
