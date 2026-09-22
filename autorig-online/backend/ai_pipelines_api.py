"""Which computer can run which pipeline, and what a day of it actually cost.

The farm is a set of boxes that each advertise the workflow templates they
carry, and a pipeline only runs where its template — and the model that
template loads — is really present. That relationship is normally invisible:
a job is dispatched, it lands somewhere, and the only trace is a row in the
render ledger. This module turns it into a table: pipelines down the side,
computers across the top, and in every cell what the last 24 hours cost there.

Everything here is read-only and measured. Capability comes from what the boxes
themselves advertise (the registry they write) together with the model
catalogue's validated-worker lists; timings come from the render ledger and the
AI request cache. Nothing is invented: a pipeline that ran nowhere in the
window says so rather than showing a plausible number.

The same capability rules answer a second question, on the node editor: how
many computers could take this job right now. `capacity_object` is that
answer, and it is deliberately the same code path, so the badge on a node and
the column on the matrix can never disagree.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import sqlite3
import statistics
import time
from typing import Dict, Iterable, List, Optional, Tuple

import httpx
from fastapi import APIRouter

import ai_vision_api

logger = logging.getLogger(__name__)

router = APIRouter()

# A day is the window the page promises. Long enough to cover a night of
# batch work, short enough that a box fixed this morning stops being judged
# by what it did yesterday.
WINDOW_SECONDS = 86400

# The ledger holds a few thousand rows and the boxes are probed over HTTP, so
# the whole computation is done once and shared for a few seconds.
CACHE_TTL_SECONDS = 15.0

# Enhancement templates are dispatched under a different name from the one
# they run: the farm boxes never advertised `upscale_fast.json`, so scheduling
# matches the canny-control token, which exactly the four FLUX image boxes
# carry. Imported from the router when it is importable so there is one list.
try:  # pragma: no cover - exercised by the live service
    from renderfin.routing import (  # type: ignore
        ENHANCE_SCHEDULING_TOKEN,
        ENHANCE_WORKFLOWS,
        QWEN_IMAGE_SCHEDULING_TOKEN,
        QWEN_IMAGE_WORKFLOWS,
    )
except Exception:  # a page must not fall over because a package moved
    ENHANCE_SCHEDULING_TOKEN = "gen_image_control_canny.json"
    ENHANCE_WORKFLOWS = {
        "upscale_fast": "upscale_fast.json",
        "upscale_refine": "upscale_refine.json",
        "detail_tiled": "detail_tiled.json",
        "detail_plain": "detail_plain.json",
        "face_fix": "face_fix.json",
        "face_fix_skin": "face_fix_skin.json",
    }
    QWEN_IMAGE_SCHEDULING_TOKEN = ENHANCE_SCHEDULING_TOKEN
    QWEN_IMAGE_WORKFLOWS = {
        "qwen_image": "qwen_image_generate.json",
        "qwen_image_edit": "qwen_image_edit.json",
    }

ENHANCE_FILES = frozenset(ENHANCE_WORKFLOWS.values())
# The Qwen-Image pair rides the same canny token: the four image boxes
# advertise it, and which of them holds a GGUF is settled per model file.
QWEN_IMAGE_FILES = frozenset(QWEN_IMAGE_WORKFLOWS.values())

# One physical box, two names: the converter registry calls the Ryzen machine
# by its host name and the render registry by its farm name. The fleet strip
# already merges them; the matrix has to agree or the same GPU gets two
# columns, one of which is always empty.
COMPUTER_ALIASES = {"ryzen-server": "raptor"}

# Services that are not a ComfyUI workflow at all.
HUNYUAN_SERVICES = {"3dmodel"}
# Which AI mode each language-model service needs a node to carry.
AI_SERVICE_MODES = {"vision": "vision", "text": "text",
                    "avatar_from_image": "vision"}
# Frame extraction is ffmpeg on this host: no GPU box is involved, so counting
# farm computers for it would be a wrong answer rather than a missing one.
LOCAL_SERVICES = {"video_frame", "video_storyboard"}

# A typed image request is scheduled as plain `gen_image.json` and only then
# resolved to its real template — a quirk carried over from the C# scheduler.
# Matching these file names against a box's advertised list would find nothing
# and paint four working pipelines as running nowhere.
TYPED_IMAGE_TOKENS = {
    "t_pose.json": "gen_image.json",
    "open_pose.json": "gen_image.json",
    "inpaint.json": "gen_image.json",
    "gen_image_by_z_depth.json": "gen_image.json",
}

# The workflow token each service is scheduled under when nothing overrides it.
# A checkpoint chosen on the node replaces this with the checkpoint's own
# workflow, which is what `checkpoints_object` below is for.
SERVICE_TOKENS = {
    "image": "gen_image.json",
    "video": "gen_animation_by_url.json",
    "avatar_image": "gen_image_flux2_avatar.json",
    "avatar_video": "gen_video_wan_animate2_by_url.json",
    "video_control": "gen_video_ltx23_control_by_url.json",
    "control_pose": "gen_control_pose.json",
    "control_depth": "gen_control_depth.json",
    "control_canny": "gen_control_canny.json",
    "upscale": ENHANCE_SCHEDULING_TOKEN,
    "detail_enhance": ENHANCE_SCHEDULING_TOKEN,
    "face_fix": ENHANCE_SCHEDULING_TOKEN,
    "qwen_image": QWEN_IMAGE_SCHEDULING_TOKEN,
}

# Services scheduled under a token that names more boxes than can serve them:
# the canny token is carried by every image box, but a Qwen-Image job only
# lands where the chosen GGUF is. For these the answer with no model chosen
# is the union of the answers per model, not the token's own box list.
BORROWED_TOKEN_SERVICES = frozenset({"qwen_image"})

# Templates a service reaches through one of its own settings rather than by
# default: the Image node's modes and control channels, the Video node's
# motion guide. Without these a third of the matrix would name no service at
# all and read as farm plumbing nobody ordered.
SERVICE_EXTRA_TOKENS = {
    "image": ("t_pose.json", "open_pose.json", "inpaint.json",
              "gen_image_by_z_depth.json",
              "gen_image_control_pose.json", "gen_image_control_depth.json",
              "gen_image_control_canny.json",
              "gen_image_flux2_klein_edit.json"),
    "video_control": ("gen_video_ltx23_pose_by_url.json",
                      "gen_video_ltx23_depth_by_url.json"),
    "avatar_image": ("gen_image_flux2_avatar.json",),
}

# Templates of retired model families (Pony/SDXL, 2026-09-23). The files stay
# in the tree so old code paths and tests still load, but no box advertises
# them and the matrix should not list them as pipelines running nowhere.
RETIRED_TOKENS = frozenset({
    "gen_image_sdxl.json", "gen_image_sdxl_edit.json",
    "gen_image_sdxl_control_pose.json", "gen_image_sdxl_control_depth.json",
    "gen_image_sdxl_control_canny.json",
})

# Rows that are not workflows. They still belong on the matrix: a reader
# wanting to know where 3D generation happens should not have to know that
# Hunyuan is not a ComfyUI template.
NATIVE_PIPELINES = [
    {"id": "hunyuan_3d", "kind": "hunyuan", "title": "Hunyuan3D · image to 3D",
     "group": "3D", "services": ["3dmodel"]},
    {"id": "ai_vision", "kind": "ai", "title": "Vision · image to text",
     "group": "Language", "services": ["vision", "avatar_from_image"]},
    {"id": "ai_text", "kind": "ai", "title": "Text · language model",
     "group": "Language", "services": ["text"]},
]

# How a workflow file is grouped on the page. First match wins.
PIPELINE_GROUPS = (
    ("gen_control_", "Control maps"),
    ("upscale_", "Enhance"),
    ("detail_", "Enhance"),
    ("face_fix", "Enhance"),
    ("gen_video_", "Video"),
    ("gen_animation_", "Video"),
    ("gen_image_flux2_avatar", "Avatars"),
    ("gen_image", "Image"),
    ("t_pose", "Image"),
    ("open_pose", "Image"),
    ("inpaint", "Image"),
    ("image_to_3d", "3D"),
)


# --------------------------------------------------------------- where things live

def _existing(*candidates: object) -> Optional[pathlib.Path]:
    for candidate in candidates:
        if not candidate:
            continue
        path = pathlib.Path(str(candidate))
        if path.exists():
            return path
    return None


def renderfin_db_path() -> Optional[pathlib.Path]:
    """The render ledger, wherever this host keeps it."""
    data_dir = os.getenv("RENDERFIN_DATA_DIR")
    return _existing(
        os.getenv("AUTORIG_RENDERFIN_DB"),
        (pathlib.Path(data_dir) / "db" / "renderfin.db") if data_dir else None,
        "/srv/autorig/data/var/renderfin/db/renderfin.db",
        "/var/autorig/renderfin/db/renderfin.db",
    )


def servers_dir() -> Optional[pathlib.Path]:
    """Where the render boxes write what they are and what they carry."""
    data_dir = os.getenv("RENDERFIN_DATA_DIR")
    return _existing(
        os.getenv("RENDERFIN_SERVERS_DIR"),
        (pathlib.Path(data_dir) / "servers") if data_dir else None,
        "/var/autorig/renderfin/servers",
        "/srv/autorig/data/var/renderfin/servers",
    )


def workflows_dir() -> Optional[pathlib.Path]:
    """The templates this deployment can dispatch at all."""
    return _existing(
        os.getenv("RENDERFIN_WORKFLOWS_DIR"),
        pathlib.Path(__file__).resolve().parent / "renderfin" / "assets" / "workflows",
    )


def ai_cache_path() -> Optional[pathlib.Path]:
    """Task metadata for the services that are not on the render ledger."""
    return _existing(
        os.getenv("AUTORIG_AI_REQUEST_CACHE_DB"),
        "/srv/autorig/data/var/ai-request-cache.sqlite3",
    )


# ------------------------------------------------------------------------- naming

def canonical_computer(name: object) -> str:
    """One key per physical box, whatever registry the name came from."""
    value = str(name or "").strip().lower()
    return COMPUTER_ALIASES.get(value, value)


def scheduling_token(workflow: object) -> str:
    """The name scheduling matches against a box's advertised workflows.

    For almost every template that is the file name itself. The enhancement
    and Qwen-Image templates are the exception and the reason this function
    exists: they are dispatched under the canny-control token because that is
    the token the boxes carrying ESRGAN, TiledDiffusion and the GGUF loader
    advertise.
    """
    name = str(workflow or "").strip()
    if name in ENHANCE_FILES:
        return ENHANCE_SCHEDULING_TOKEN
    if name in QWEN_IMAGE_FILES:
        return QWEN_IMAGE_SCHEDULING_TOKEN
    return TYPED_IMAGE_TOKENS.get(name, name)


def pipeline_group(token: str) -> str:
    for prefix, group in PIPELINE_GROUPS:
        if token.startswith(prefix):
            return group
    return "Other"


def pipeline_title(token: str) -> str:
    """A file name read as words, with the noise trimmed."""
    stem = token[:-5] if token.endswith(".json") else token
    for prefix in ("gen_video_", "gen_image_", "gen_animation_", "gen_control_", "gen_"):
        if stem.startswith(prefix):
            stem = stem[len(prefix):]
            break
    stem = stem.replace("_by_url", "").replace("_", " ").strip()
    return stem or token


# ------------------------------------------------------------------------ readers

def load_servers(path: Optional[pathlib.Path] = None) -> List[Dict[str, object]]:
    """The render boxes as they describe themselves, newest heartbeat included."""
    directory = path or servers_dir()
    if not directory or not directory.is_dir():
        return []
    servers = []
    for entry in sorted(directory.glob("*.json")):
        try:
            data = json.loads(entry.read_text(encoding="utf-8"))
        except Exception:
            # A half-written or hand-edited file must not empty the page.
            logger.warning("Could not read render server file %s", entry.name)
            continue
        if isinstance(data, dict) and data.get("render_server_name"):
            servers.append(data)
    return servers


def load_converters() -> List[Dict[str, object]]:
    """Converter boxes, with the two flags that decide what they may take.

    Reads the same file the dispatcher reads, and deliberately keeps nothing
    but the identity and those flags: the file also holds each box's bearer
    token, which has no business leaving this process.
    """
    converters = []
    for worker, raw in ai_vision_api._worker_entries():
        hunyuan_ok = (raw.get("enabled") is not False
                      and raw.get("disabled") is not True
                      and raw.get("canary_approved") is not False)
        converters.append({
            "name": str(worker.get("name") or ""),
            "node": canonical_computer(worker.get("physical_node")),
            "hunyuan_bool": bool(hunyuan_ok),
            "ai_bool": raw.get("ai_vision_enabled") is not False,
            "mode_string": str(raw.get("capability_mode") or raw.get("mode") or "full"),
            "parked_reason_string": str(raw.get("disabled_reason") or ""),
        })
    return converters


def catalogue_entries() -> List[Dict[str, object]]:
    try:
        import ai_model_catalogue

        return list(ai_model_catalogue.entries())
    except Exception:
        logger.exception("Could not read the model catalogue")
        return []


_RENDER_COLUMNS = (
    "json_extract(payload,'$.workflow_file')",
    "json_extract(payload,'$.server_name')",
    "json_extract(payload,'$.status')",
    "json_extract(payload,'$.started_at')",
    "json_extract(payload,'$.finished_at')",
    "json_extract(payload,'$.prompt.checkpoint')",
    "json_extract(payload,'$.prompt.type')",
)


def read_render_jobs(since: float,
                     path: Optional[pathlib.Path] = None
                     ) -> Tuple[List[Dict[str, object]], str]:
    """Finished and failed render work since `since`, straight off the ledger.

    Only the seven fields the matrix needs are pulled out in SQL. The payload
    also carries the user's prompt, and a status page has no reason to load
    somebody's prompt into memory, let alone cache it.
    """
    database = path or renderfin_db_path()
    if not database:
        return [], "the render ledger is not on this host"
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        return [], f"the render ledger could not be opened: {exc}"
    jobs: List[Dict[str, object]] = []
    try:
        connection.execute("PRAGMA query_only = 1")
        rows = connection.execute(
            "SELECT " + ", ".join(_RENDER_COLUMNS) + ", created_at "
            "FROM render_tasks WHERE created_at >= ?", (float(since),)
        ).fetchall()
    except sqlite3.Error as exc:
        connection.close()
        return [], f"the render ledger could not be read: {exc}"
    connection.close()
    for workflow, server, status, started, finished, checkpoint, ptype, created in rows:
        jobs.append({
            "workflow": str(workflow or ""),
            "computer": canonical_computer(server),
            "status": str(status or ""),
            "started_at": float(started or 0),
            "finished_at": float(finished or 0),
            "checkpoint": str(checkpoint or ""),
            "type": str(ptype or ""),
            "created_at": float(created or 0),
        })
    return jobs, ""


def read_ai_jobs(since: float,
                 path: Optional[pathlib.Path] = None
                 ) -> Tuple[List[Dict[str, object]], str]:
    """Jobs for the services that never touch the render ledger.

    Vision, text and Hunyuan are dispatched straight at a converter box, so
    the only durable record of one is the request cache: which service, which
    node answered, and how long the node said it took. Failures are dropped
    from that cache by design, so a count here is a count of completed work.
    """
    database = path or ai_cache_path()
    if not database:
        return [], ""
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        return [], f"the AI request cache could not be opened: {exc}"
    jobs: List[Dict[str, object]] = []
    try:
        connection.execute("PRAGMA query_only = 1")
        rows = connection.execute(
            "SELECT service, state, task_id, response_json, created_at, updated_at "
            "FROM ai_request_cache WHERE created_at >= ?", (float(since),)
        ).fetchall()
    except sqlite3.Error as exc:
        connection.close()
        return [], f"the AI request cache could not be read: {exc}"
    connection.close()
    for service, state, task_id, response, created, updated in rows:
        try:
            payload = json.loads(response or "{}")
        except Exception:
            payload = {}
        node = str(payload.get("node_string") or "")
        if not node and task_id and "." in str(task_id):
            node = str(task_id).split(".", 1)[0]
        elapsed = payload.get("elapsed_seconds_float")
        jobs.append({
            "service": str(service or ""),
            "computer": canonical_computer(node),
            "state": str(state or ""),
            "elapsed_seconds": float(elapsed) if isinstance(elapsed, (int, float)) else 0.0,
            "created_at": float(created or 0),
            "updated_at": float(updated or 0),
        })
    return jobs, ""


# -------------------------------------------------------------------- capability

def server_workflows(server: Dict[str, object]) -> List[str]:
    return [str(item) for item in (server.get("available_workflows") or [])]


def server_advertises(server: Dict[str, object], token: str) -> bool:
    return bool(token) and token in server_workflows(server)


def checkpoint_workers(entry: Dict[str, object]) -> Optional[set]:
    """Boxes a checkpoint was validated on, or None when nobody said.

    An absent list is not an empty one: several catalogue entries predate the
    validation pass, and treating those as "nowhere" would black out rows that
    demonstrably run. Only a declared list narrows a pipeline.
    """
    declared = entry.get("validated_workers")
    if not isinstance(declared, (list, tuple, set)) or not declared:
        return None
    return {canonical_computer(name) for name in declared}


def checkpoints_for_token(catalogue: Iterable[Dict[str, object]],
                          token: str) -> List[Dict[str, object]]:
    """Catalogue checkpoints whose own workflow is this pipeline."""
    return [entry for entry in catalogue
            if str(entry.get("kind") or "") == "checkpoint"
            and str(entry.get("workflow") or "") == token
            and entry.get("usable")]


def _display_names(servers: List[Dict[str, object]],
                   converters: List[Dict[str, object]],
                   extra: Iterable[str] = ()) -> Dict[str, str]:
    """Canonical key -> the name a person would recognise.

    The render registry's spelling wins: that is the name on the ledger, in
    the fleet strip and in every error message somebody has already read.
    """
    names: Dict[str, str] = {}
    for converter in converters:
        key = canonical_computer(converter.get("node"))
        if key:
            names.setdefault(key, str(converter.get("name") or key))
    for server in servers:
        key = canonical_computer(server.get("render_server_name"))
        if key:
            names[key] = str(server.get("render_server_name") or key)
    for name in extra:
        key = canonical_computer(name)
        if key:
            names.setdefault(key, str(name))
    return names


def _ai_modes(models: Iterable[str]) -> set:
    """Which AI modes a node can serve, from the models it reports carrying."""
    wanted = {str(entry.get("id")): set(entry.get("modes") or [])
              for entry in ai_vision_api.AI_MODELS}
    modes: set = set()
    for model in models or []:
        modes |= wanted.get(str(model), set())
    return modes


# --------------------------------------------------------- capacity for the editor

def capacity_object(nodes: List[Dict[str, object]],
                    servers: List[Dict[str, object]],
                    *,
                    ai_models_by_node: Optional[Dict[str, List[str]]] = None,
                    converters: Optional[List[Dict[str, object]]] = None,
                    catalogue: Optional[List[Dict[str, object]]] = None,
                    ) -> Dict[str, Dict[str, object]]:
    """How many computers could take each service right now, and how many are free.

    `nodes` is the fleet's own view of who is up and who is busy; `servers` is
    what the render boxes advertise. Capability is the intersection: a box has
    to be reachable *and* carry the template, and for a checkpoint-bound
    choice it has to carry the model file too.
    """
    converters = converters if converters is not None else load_converters()
    catalogue = catalogue if catalogue is not None else catalogue_entries()
    models_by_node = {canonical_computer(key): list(value or [])
                      for key, value in (ai_models_by_node or {}).items()}

    online: Dict[str, bool] = {}
    busy: Dict[str, bool] = {}
    for node in nodes or []:
        key = canonical_computer(node.get("id"))
        if not key:
            continue
        online[key] = bool(online.get(key)) or bool(node.get("online"))
        busy[key] = bool(busy.get(key)) or bool(node.get("busy"))
        carried = node.get("models")
        if carried and key not in models_by_node:
            models_by_node[key] = [str(item) for item in carried]

    by_computer = {canonical_computer(server.get("render_server_name")): server
                   for server in servers or []
                   if server.get("render_server_name")}
    names = _display_names(list(servers or []), converters, online.keys())

    def label(key: str) -> str:
        return names.get(key, key)

    def summarise(keys: Iterable[str], token: str = "") -> Dict[str, object]:
        ordered = sorted({key for key in keys if key}, key=lambda k: label(k).lower())
        idle = [key for key in ordered if not busy.get(key)]
        return {
            "token_string": token,
            "total_int": len(ordered),
            "idle_int": len(idle),
            "computers_array": [label(key) for key in ordered],
            "idle_array": [label(key) for key in idle],
        }

    def comfy_capable(token: str, allowed: Optional[set] = None) -> List[str]:
        capable = []
        for key, server in by_computer.items():
            if not server_advertises(server, token):
                continue
            # A box that is not answering cannot take work now, whatever its
            # file still says it carries.
            if not online.get(key, str(server.get("status") or "").lower()
                              in ("online", "busy")):
                continue
            if allowed is not None and key not in allowed:
                continue
            capable.append(key)
        return capable

    capacity: Dict[str, Dict[str, object]] = {}

    for service_id, token in SERVICE_TOKENS.items():
        entry = summarise(comfy_capable(token), token)
        entry["kind_string"] = "comfy"
        checkpoints: Dict[str, object] = {}
        # Only the services that expose a model picker can move off their
        # default token; the rest run one template by construction.
        if service_id in ("image", "video", "qwen_image"):
            for model in catalogue:
                if str(model.get("kind") or "") != "checkpoint" or not model.get("usable"):
                    continue
                if service_id not in (model.get("services") or []):
                    continue
                model_token = str(model.get("workflow") or "") or token
                # Matched the way scheduling matches: a Qwen template is
                # advertised by nobody, the canny token it rides under is.
                summary = summarise(
                    comfy_capable(scheduling_token(model_token), checkpoint_workers(model)),
                    model_token)
                summary["title_string"] = str(model.get("title") or model.get("file") or "")
                checkpoints[str(model.get("file"))] = summary
        if service_id in BORROWED_TOKEN_SERVICES and checkpoints:
            held = {key for key in by_computer
                    if any(label(key) in (item.get("computers_array") or [])
                           for item in checkpoints.values())}
            entry.update(summarise(held, token))
        entry["checkpoints_object"] = checkpoints
        capacity[service_id] = entry

    hunyuan_nodes = {converter["node"] for converter in converters
                     if converter.get("hunyuan_bool")}
    for service_id in HUNYUAN_SERVICES:
        entry = summarise([key for key in hunyuan_nodes if online.get(key)])
        entry["kind_string"] = "hunyuan"
        entry["checkpoints_object"] = {}
        capacity[service_id] = entry

    ai_nodes = {converter["node"] for converter in converters if converter.get("ai_bool")}
    for service_id, mode in AI_SERVICE_MODES.items():
        capable = [key for key in ai_nodes
                   if online.get(key) and mode in _ai_modes(models_by_node.get(key) or [])]
        entry = summarise(capable)
        entry["kind_string"] = "ai"
        entry["checkpoints_object"] = {}
        capacity[service_id] = entry

    for service_id in LOCAL_SERVICES:
        capacity[service_id] = {
            "kind_string": "local", "token_string": "",
            "total_int": 1, "idle_int": 1,
            "computers_array": ["this server"], "idle_array": ["this server"],
            "checkpoints_object": {},
        }

    return capacity


# ------------------------------------------------------------------- the matrix

def _summary(durations: List[float]) -> Dict[str, object]:
    if not durations:
        return {"avg_seconds_float": None, "median_seconds_float": None}
    return {
        "avg_seconds_float": round(sum(durations) / len(durations), 1),
        "median_seconds_float": round(statistics.median(durations), 1),
    }


def _worth_sending(cells: Dict[str, Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    """Drop the cells that say nothing.

    A box that cannot run a pipeline and never tried is a dash on the page,
    and a dash does not need three hundred bytes of JSON. With five boxes and
    thirty pipelines the empty cells are most of the answer.
    """
    kept = {}
    for key, cell in cells.items():
        if not cell["capable_bool"] and not cell["jobs_int"]:
            continue
        kept[key] = {name: value for name, value in cell.items()
                     if value is not None and value != []}
    return kept


def _blank_cell() -> Dict[str, object]:
    return {"jobs_int": 0, "ok_int": 0, "failed_int": 0, "running_int": 0,
            "avg_seconds_float": None, "median_seconds_float": None,
            "last_ok_unix_int": 0, "capable_bool": False, "ready_bool": False,
            "model_ready_bool": True, "checkpoints_array": [],
            "ran_checkpoints_array": []}


def build_matrix(*, now: Optional[float] = None,
                 render_jobs: Optional[List[Dict[str, object]]] = None,
                 ai_jobs: Optional[List[Dict[str, object]]] = None,
                 servers: Optional[List[Dict[str, object]]] = None,
                 converters: Optional[List[Dict[str, object]]] = None,
                 catalogue: Optional[List[Dict[str, object]]] = None,
                 node_state: Optional[Dict[str, Dict[str, object]]] = None,
                 ai_models: Optional[Dict[str, List[str]]] = None,
                 workflow_files: Optional[List[str]] = None,
                 notes: Optional[List[str]] = None,
                 ) -> Dict[str, object]:
    """The whole table, from data that has already been read.

    Pure on purpose: every source is an argument, so the shape of the answer
    can be tested against a temporary ledger and a handful of fake registry
    files instead of against whatever the farm happens to be doing.
    """
    now = float(now if now is not None else time.time())
    since = now - WINDOW_SECONDS
    notes = list(notes or [])
    servers = list(servers if servers is not None else load_servers())
    converters = list(converters if converters is not None else load_converters())
    catalogue = list(catalogue if catalogue is not None else catalogue_entries())
    node_state = dict(node_state or {})
    ai_models = {canonical_computer(key): list(value or [])
                 for key, value in (ai_models or {}).items()}
    if render_jobs is None:
        render_jobs, problem = read_render_jobs(since)
        if problem:
            notes.append(problem)
    if ai_jobs is None:
        ai_jobs, problem = read_ai_jobs(since)
        if problem:
            notes.append(problem)
    if workflow_files is None:
        directory = workflows_dir()
        workflow_files = sorted(path.name for path in directory.glob("*.json")) \
            if directory and directory.is_dir() else []

    by_computer = {canonical_computer(server.get("render_server_name")): server
                   for server in servers if server.get("render_server_name")}
    seen_computers = {job["computer"] for job in render_jobs if job.get("computer")}
    seen_computers |= {job["computer"] for job in ai_jobs if job.get("computer")}
    names = _display_names(servers, converters, seen_computers)

    # ---------------------------------------------------------------- columns
    columns: List[Dict[str, object]] = []
    for key in sorted(set(by_computer) | {c["node"] for c in converters} | seen_computers,
                      key=lambda k: names.get(k, k).lower()):
        server = by_computer.get(key)
        converter = next((c for c in converters if c["node"] == key), None)
        state = node_state.get(key) or {}
        registry_online = str((server or {}).get("status") or "").lower() in ("online", "busy")
        online = bool(state.get("online")) if "online" in state else registry_online
        busy = bool(state["busy"]) if "busy" in state else bool(
            (server or {}).get("current_render_task")
            or int((server or {}).get("queue_size") or 0) > 0)
        roles = []
        if server:
            roles.append("render")
        if converter and converter.get("hunyuan_bool"):
            roles.append("hunyuan")
        if converter and converter.get("ai_bool"):
            roles.append("ai")
        columns.append({
            "id": key,
            "title": names.get(key, key),
            "gpu_string": str((server or {}).get("gpu_name") or ""),
            "online_bool": online,
            "busy_bool": busy,
            "queue_int": int((server or {}).get("queue_size") or 0),
            "workflows_int": len(server_workflows(server or {})),
            "roles_array": roles,
            "parked_reason_string": str((converter or {}).get("parked_reason_string") or ""),
            "state_string": ("offline" if not online else "busy" if busy else "idle"),
            "updated_string": str((server or {}).get("date_update") or ""),
        })

    # ------------------------------------------------------------------- rows
    tokens = {job["workflow"] for job in render_jobs if job.get("workflow")}
    tokens |= set(workflow_files)
    for server in servers:
        tokens |= set(server_workflows(server))
    for entry in catalogue:
        if entry.get("workflow"):
            tokens.add(str(entry["workflow"]))
    tokens = {token for token in tokens if token and token not in RETIRED_TOKENS}

    # Which services can land on which token, so a row says what it is for.
    services_by_token: Dict[str, List[str]] = {}
    for service_id, token in SERVICE_TOKENS.items():
        services_by_token.setdefault(token, []).append(service_id)
    for service_id, extra in SERVICE_EXTRA_TOKENS.items():
        for token in extra:
            bucket = services_by_token.setdefault(token, [])
            if service_id not in bucket:
                bucket.append(service_id)
    for entry in catalogue:
        token = str(entry.get("workflow") or "")
        if not token:
            continue
        for service_id in entry.get("services") or []:
            bucket = services_by_token.setdefault(token, [])
            if service_id not in bucket:
                bucket.append(str(service_id))
    # The enhancement templates are dispatched under the canny token but are
    # their own pipelines; naming the service on each row is what tells a
    # reader that "face_fix.json" is the Face fixer node.
    for ptype, workflow in ENHANCE_WORKFLOWS.items():
        service_id = ("upscale" if ptype.startswith("upscale")
                      else "detail_enhance" if ptype.startswith("detail") else "face_fix")
        bucket = services_by_token.setdefault(workflow, [])
        if service_id not in bucket:
            bucket.append(service_id)
    for workflow in QWEN_IMAGE_WORKFLOWS.values():
        bucket = services_by_token.setdefault(workflow, [])
        if "qwen_image" not in bucket:
            bucket.append("qwen_image")

    rows: List[Dict[str, object]] = []
    render_by_token: Dict[str, List[Dict[str, object]]] = {}
    for job in render_jobs:
        render_by_token.setdefault(job["workflow"], []).append(job)

    for token in sorted(tokens, key=lambda name: (pipeline_group(name), name)):
        jobs = render_by_token.get(token) or []
        checkpoints = checkpoints_for_token(catalogue, token)
        dispatch = scheduling_token(token)
        cells: Dict[str, Dict[str, object]] = {}
        for column in columns:
            key = str(column["id"])
            server = by_computer.get(key)
            cell = _blank_cell()
            if server is not None:
                cell["capable_bool"] = server_advertises(server, dispatch)
                cell["ready_bool"] = bool(cell["capable_bool"]
                                          and column["online_bool"])
            if checkpoints:
                held = [entry for entry in checkpoints
                        if key in (checkpoint_workers(entry) or {key})]
                cell["model_ready_bool"] = bool(held)
                cell["checkpoints_array"] = [str(entry.get("file")) for entry in held]
            cells[key] = cell
        unassigned = _blank_cell()
        durations: List[float] = []
        ok = failed = 0
        for job in jobs:
            cell = cells.get(job["computer"], unassigned)
            cell["jobs_int"] += 1
            done = job["status"].lower() == "done"
            if done:
                cell["ok_int"] += 1
                ok += 1
                cell["last_ok_unix_int"] = max(int(cell["last_ok_unix_int"]),
                                               int(job["finished_at"] or 0))
            elif job["status"].lower() in ("error", "failed", "cancelled"):
                cell["failed_int"] += 1
                failed += 1
            else:
                # Pending or on the card: a job with no time yet is not a job
                # that took no time.
                cell["running_int"] += 1
            started, finished = job["started_at"], job["finished_at"]
            if done and started > 0 and finished > started:
                cell.setdefault("_durations", []).append(finished - started)
                durations.append(finished - started)
            if job.get("checkpoint") and job["checkpoint"] not in cell["ran_checkpoints_array"]:
                cell["ran_checkpoints_array"].append(job["checkpoint"])
        for cell in list(cells.values()) + [unassigned]:
            cell.update(_summary(cell.pop("_durations", [])))
            # Evidence outranks the catalogue: a box that finished one of these
            # in the last day demonstrably has the model, whatever the
            # validation list was last told.
            if cell["ok_int"]:
                cell["model_ready_bool"] = True
        rows.append({
            "id": token,
            "kind_string": "comfy",
            "title": pipeline_title(token),
            "token_string": token,
            "dispatch_token_string": dispatch,
            "group_string": pipeline_group(token),
            "services_array": sorted(services_by_token.get(token) or []),
            "checkpoints_array": [str(entry.get("file")) for entry in checkpoints],
            "jobs_int": len(jobs), "ok_int": ok, "failed_int": failed,
            "unassigned_object": unassigned if unassigned["jobs_int"] else None,
            "computers_object": _worth_sending(cells),
            **_summary(durations),
        })

    # ------------------------------------------------- rows that are not workflows
    ai_by_service: Dict[str, List[Dict[str, object]]] = {}
    for job in ai_jobs:
        ai_by_service.setdefault(job["service"], []).append(job)

    for native in NATIVE_PIPELINES:
        services = list(native["services"])
        if native["kind"] == "hunyuan":
            capable_keys = {c["node"] for c in converters if c.get("hunyuan_bool")}
        else:
            # A converter parked for 3D still reads pictures, but one that
            # carries no language model at all cannot answer either way — and
            # a box we cannot reach carries nothing we know of.
            wanted = "vision" if native["id"] == "ai_vision" else "text"
            capable_keys = {c["node"] for c in converters if c.get("ai_bool")
                            and wanted in _ai_modes(ai_models.get(c["node"]) or [])}
        # Every service that lands on this pipeline contributes its jobs: an
        # Avatar built from an image is a Vision job wearing another name.
        sources = [job for service in services
                   for job in (ai_by_service.get(service) or [])]
        cells = {}
        for column in columns:
            key = str(column["id"])
            cell = _blank_cell()
            cell["capable_bool"] = key in capable_keys
            cell["ready_bool"] = bool(cell["capable_bool"] and column["online_bool"])
            cells[key] = cell
        unassigned = _blank_cell()
        durations = []
        ok = 0
        for job in sources:
            cell = cells.get(job["computer"], unassigned)
            cell["jobs_int"] += 1
            cell["ok_int"] += 1
            ok += 1
            cell["last_ok_unix_int"] = max(int(cell["last_ok_unix_int"]),
                                           int(job.get("updated_at") or 0))
            if job.get("elapsed_seconds"):
                cell.setdefault("_durations", []).append(float(job["elapsed_seconds"]))
                durations.append(float(job["elapsed_seconds"]))
        for cell in list(cells.values()) + [unassigned]:
            cell.update(_summary(cell.pop("_durations", [])))
        rows.append({
            "id": native["id"], "kind_string": native["kind"],
            "title": native["title"], "token_string": "",
            "dispatch_token_string": "",
            "group_string": native["group"], "services_array": services,
            "checkpoints_array": [],
            "jobs_int": len(sources), "ok_int": ok, "failed_int": 0,
            "unassigned_object": unassigned if unassigned["jobs_int"] else None,
            "computers_object": _worth_sending(cells),
            **_summary(durations),
        })

    online_columns = [column for column in columns if column["online_bool"]]
    return {
        "success_bool": True,
        "window_seconds_int": WINDOW_SECONDS,
        "window_from_unix_int": int(since),
        "computers_array": columns,
        "pipelines_array": rows,
        "totals_object": {
            "pipelines_int": len(rows),
            "computers_total_int": len(columns),
            "computers_online_int": len(online_columns),
            "computers_busy_int": len([c for c in online_columns if c["busy_bool"]]),
            # Counted off the rows, not off the two sources: an image job is
            # on the render ledger *and* in the request cache, and adding the
            # two would report half again as much work as the farm did.
            "jobs_int": sum(int(row["jobs_int"]) for row in rows),
            "ok_int": sum(int(row["ok_int"]) for row in rows),
            "failed_int": sum(int(row["failed_int"]) for row in rows),
        },
        "notes_array": notes,
        "server_time_unix_int": int(now),
    }


# ------------------------------------------------------------------ the endpoint

_snapshot: Dict[str, object] = {}
_snapshot_at = 0.0
_lock = asyncio.Lock()


async def _node_state() -> Tuple[Dict[str, Dict[str, object]], Dict[str, List[str]]]:
    """Who is answering right now, asked of the boxes themselves.

    The registry says what a box carries; only a probe says whether it is
    there. Converter boxes are asked directly because they are the only
    source for the AI rows; render boxes take their state from the heartbeat
    they write, which is what the dispatcher trusts too.
    """
    state: Dict[str, Dict[str, object]] = {}
    models: Dict[str, List[str]] = {}
    workers = ai_vision_api._load_ai_workers()
    if not workers:
        return state, models
    try:
        async with httpx.AsyncClient() as client:
            probes = await asyncio.gather(
                *(ai_vision_api._node_is_free(client, worker) for worker in workers),
                return_exceptions=True,
            )
    except Exception:
        logger.exception("Could not probe the converter boxes")
        return state, models
    for worker, probe in zip(workers, probes):
        key = canonical_computer(worker.get("physical_node") or worker.get("name"))
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            state[key] = {"online": False, "busy": False}
            continue
        reachable, info = probe
        info = info if isinstance(info, dict) else {}
        state[key] = {"online": bool(reachable),
                      "busy": bool(reachable and int(info.get("load") or 0) > 0)}
        models[key] = [str(item) for item in (info.get("models") or [])]
    return state, models


@router.get("/api/ai/pipelines")
async def api_ai_pipelines():
    """Every pipeline against every computer, measured over the last day."""
    global _snapshot, _snapshot_at
    if _snapshot and (time.monotonic() - _snapshot_at) < CACHE_TTL_SECONDS:
        return _snapshot
    async with _lock:
        if _snapshot and (time.monotonic() - _snapshot_at) < CACHE_TTL_SECONDS:
            return _snapshot
        try:
            servers = load_servers()
            converters = load_converters()
            state, models = await _node_state()
            # A render box that is holding a task is busy even when its own
            # heartbeat still says "online"; the ledger is what knows.
            jobs, problem = read_render_jobs(time.time() - WINDOW_SECONDS)
            for job in jobs:
                if job["status"].lower() == "rendering" and job["computer"]:
                    state.setdefault(job["computer"], {})["busy"] = True
            snapshot = build_matrix(
                render_jobs=jobs, servers=servers, converters=converters,
                node_state=state, ai_models=models,
                notes=[problem] if problem else [])
            snapshot["capacity_object"] = capacity_object(
                [{"id": key, "online": value.get("online"), "busy": value.get("busy")}
                 for key, value in state.items()]
                + [{"id": str(server.get("render_server_name")),
                    "online": str(server.get("status") or "").lower() in ("online", "busy"),
                    "busy": bool(server.get("current_render_task"))}
                   for server in servers],
                servers, ai_models_by_node=models, converters=converters)
            _snapshot = snapshot
        except Exception:
            logger.exception("Could not build the pipeline matrix")
            # A status page is never a reason to return a 500 to a browser
            # that is polling it every thirty seconds.
            _snapshot = {
                "success_bool": False,
                "window_seconds_int": WINDOW_SECONDS,
                "computers_array": [], "pipelines_array": [],
                "totals_object": {}, "capacity_object": {},
                "notes_array": ["the matrix could not be built"],
                "server_time_unix_int": int(time.time()),
            }
        _snapshot_at = time.monotonic()
        return _snapshot
