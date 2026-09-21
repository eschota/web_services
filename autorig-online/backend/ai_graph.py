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
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import ai_services

logger = logging.getLogger(__name__)

router = APIRouter()

GRAPH_DIR = pathlib.Path(
    os.getenv("AUTORIG_AI_GRAPH_DIR", "/srv/autorig/data/var/ai-graphs")
)
MAX_NODES = 60
MAX_GRAPH_BYTES = 256 * 1024
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


class Graph(BaseModel):
    name: str = "Untitled"
    nodes: List[GraphNode] = Field(default_factory=list)
    links: List[GraphLink] = Field(default_factory=list)


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
                 "params": {"width": 1024, "height": 1024, "mode": ""}},
                {"id": "clip", "kind": NODE_SERVICE, "service": "video",
                 "x": 1080, "y": 20,
                 "params": {"quality": "standard", "frame_count": 96}},
                {"id": "loop", "kind": NODE_SERVICE, "service": "video",
                 "x": 1080, "y": 300,
                 "params": {"quality": "standard", "frame_count": 96}},
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


def templates() -> List[Dict[str, object]]:
    return [_template_simple_text_to_video_by_vision()]


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
    payload = json.dumps(body, ensure_ascii=False, sort_keys=True)
    if len(payload.encode("utf-8")) > MAX_GRAPH_BYTES:
        raise HTTPException(status_code=400, detail={
            "error_string": "graph_too_large",
            "message_string": "The graph is larger than the store accepts"})
    graph_id = _new_id(payload)
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
