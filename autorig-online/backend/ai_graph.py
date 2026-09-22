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

from fastapi import APIRouter, HTTPException
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

    @field_validator("input_reference_url")
    @classmethod
    def validate_input_reference_url(cls, value: str) -> str:
        value = str(value or "")
        if value and not value.startswith(("http://", "https://")):
            raise ValueError("input_reference_url must be a public http(s) URL")
        return value


class Graph(BaseModel):
    name: str = "Untitled"
    # Empty keeps every legacy graph's content-derived id unchanged. A
    # duplicate receives a fresh value so its otherwise identical snapshot has
    # an independent deep link and subsequent result updates stay on it.
    instance_id: str = Field("", max_length=64)
    comparison_anchor_id: str = Field("", max_length=64)
    nodes: List[GraphNode] = Field(default_factory=list)
    links: List[GraphLink] = Field(default_factory=list)
    # Keyed by node id. Never part of what makes a graph's identity: a rerun
    # must update the same link, not mint a new one.
    results: Dict[str, NodeResult] = Field(default_factory=dict)

    @field_validator("instance_id")
    @classmethod
    def validate_instance_id(cls, value: str) -> str:
        value = str(value or "")
        if value and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
            raise ValueError("instance_id may contain letters, digits, - and _")
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
        if produced is None or accepted is None or produced != accepted:
            raise HTTPException(status_code=400, detail={
                "error_string": "type_mismatch",
                "message_string": (
                    f"'{link.from_node}.{link.output}' produces {produced or 'nothing'}, "
                    f"but '{link.to_node}.{link.input}' takes {accepted or 'nothing'}")})

    _reject_cycles(graph, by_id)


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
        if key != "results" and not (
            key in {"instance_id", "comparison_anchor_id"} and not value
        )
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
    return {
        "success_bool": True,
        "templates_array": templates(),
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/ai/graphs")
async def api_graph_save(graph: Graph):
    """Store a graph and hand back the link that reopens it."""
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
    try:
        GRAPH_DIR.mkdir(parents=True, exist_ok=True)
        _path_for(graph_id).write_text(
            json.dumps({"id": graph_id, "saved_at_unix_int": int(time.time()),
                        "graph": body}, ensure_ascii=False, indent=2),
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
        "server_time_unix_int": int(time.time()),
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
    path = _path_for(graph_id)
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


@router.get("/api/ai/graphs/{graph_id}")
async def api_graph_load(graph_id: str):
    """Reopen a saved graph, or a template if the id names one."""
    for template in templates():
        if str(template["id"]) == graph_id:
            return {"success_bool": True, "graph_id_string": graph_id,
                    "graph_object": template["graph"],
                    "template_bool": True,
                    "server_time_unix_int": int(time.time())}
    path = _path_for(graph_id)
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
        "graph_object": stored.get("graph") or {},
        "template_bool": False,
        "saved_at_unix_int": int(stored.get("saved_at_unix_int") or 0),
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
