"""Public AI Vision / text2text API backed by the converter farm.

The farm converters run the language model themselves at the `ai_vision`
workload class, so this module only has to choose a node, hand it the request
and report back. Work is never executed here.

A task id is returned as ``<node>.<worker task id>`` so a later status call can
find the node that owns it without this process keeping a routing table that a
restart would lose.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import pathlib
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/ai/render-status/{task_id}")
async def api_render_task_status(task_id: str):
    """Actual queue state and assigned worker, independent of output-file timing."""
    import re
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", task_id):
        raise HTTPException(status_code=400, detail="Invalid render task id")
    async with httpx.AsyncClient() as client:
        response = await client.get(RENDERFIN_BASE + "/api-render/tasks/" + task_id,
                                    timeout=10.0)
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Render task not found")
    response.raise_for_status()
    row = response.json()
    raw = str(row.get("status_string") or row.get("status") or "").lower()
    state = {"pending": "queued", "rendering": "rendering", "done": "completed",
             "error": "failed", "cancelled": "cancelled"}.get(raw, raw or "unknown")
    result = {"success_bool": True, "task_id_string": task_id, "status_string": state,
            "finished_bool": state in {"completed", "failed", "cancelled"},
            "node_string": row.get("render_server_name") or "",
            "workflow_string": row.get("workflow_file") or row.get("workflow") or "",
            "output_url_string": row.get("output_url_string") or row.get("output_url") or "",
            "error_string": row.get("error_string") or row.get("error") or "",
            "started_at_unix_float": row.get("started_at") or 0,
            "created_at_unix_float": row.get("created_at") or 0,
            # Where in the line this job is, so "queued" can say how long a
            # wait it is. Zero while it is not waiting: running, or finished.
            "queue_position_int": int(row.get("queue_position_int") or 0),
            "queue_length_int": int(row.get("queue_length_int") or 0)}
    import ai_request_cache
    await ai_request_cache.anote_result(task_id, state, result)
    return result


@router.delete("/api/ai/request-cache")
async def api_clear_request_cache():
    import ai_request_cache
    count = await asyncio.to_thread(ai_request_cache.clear)
    return {"success_bool": True, "entries_removed_int": count}


# One catalogue entry per model a caller may ask for. `worker_model` is empty
# while a node serves exactly one local model; it becomes the selector once a
# node offers more than one.
AI_MODELS: List[Dict[str, object]] = [
    {
        "id": "bonsai2-27b",
        "graph_agent_supported": True,
        # Verified system role; 4k window, thinks before answering, so keep
        # graph edits small (a few operations or clone variants per request).
        "graph_agent_instruction_role": "system",
        "unlimited_output_supported": True,
        "title": "Bonsai 2 27B",
        "description": (
            "Ternary 27B vision-language model running on the farm's own GPUs. "
            "Reads images and answers in plain text."
        ),
        "modes": ["vision", "text"],
        "context_tokens": 4096,
        "max_output_tokens": 2048,
        # This one reasons before it answers, and the reasoning is charged to
        # the same budget. A budget sized for a plain answer is spent before
        # the answer starts, which is what "the whole budget went to reasoning"
        # was: not a broken model, a budget set for the wrong kind of model.
        "reasons_first": True,
        "default_output_tokens": 2048,
        "hosting": "local-farm",
        "default": True,
    },
    {
        "id": "qwen35-9b-uncensored",
        # Its system role failed the conflict canary, so the graph editor sends
        # its standing instructions in the prompt instead. With reasoning off
        # and an 8k window it completed ten-variant edits the 27B could not.
        "graph_agent_supported": True,
        "graph_agent_instruction_role": "prompt",
        "graph_agent_default": True,
        "unlimited_output_supported": True,
        "title": "Qwen3.5 9B Defiant Fable",
        "description": (
            "Uncensored 9B vision-language model on the farm's own GPUs. "
            "Answers without the refusals of a stock assistant model."
        ),
        "modes": ["vision", "text"],
        "context_tokens": 8192,
        "max_output_tokens": 2048,
        "hosting": "local-farm",
        "uncensored": True,
        # Served with reasoning off, so a modest budget is all it needs.
        "reasons_first": False,
        "default_output_tokens": 1024,
        "default": False,
    },
]
DEFAULT_MODEL_ID = "bonsai2-27b"

MAX_PROMPT_CHARS = 8000
MAX_INLINE_IMAGE_BYTES = 12 * 1024 * 1024
SUBMIT_TIMEOUT_SECONDS = 60.0
STATUS_TIMEOUT_SECONDS = 30.0
# A cold node loads 7 GB of weights before the first answer, so the wait a
# caller may ask us to hold for has to allow for that plus the inference.
MAX_WAIT_SECONDS = 180.0

# Completed durations, so the fleet view can show what these services actually
# take instead of a constant. Kept in memory on purpose: a rolling window of
# recent jobs is what an estimate should follow, and it costs nothing to lose.
RECENT_DURATIONS: Dict[str, deque] = {
    "vision": deque(maxlen=40),
    "text": deque(maxlen=40),
    "3dmodel": deque(maxlen=20),
}


def record_duration(service_id: str, seconds: float) -> None:
    bucket = RECENT_DURATIONS.get(service_id)
    if bucket is not None and seconds and seconds > 0:
        bucket.append(float(seconds))


class VisionRequest(BaseModel):
    prompt: str = Field(..., description="Question about the image")
    image_url: Optional[str] = Field(None, description="Public http(s) URL of the image")
    image_base64: Optional[str] = Field(None, description="Inline image, base64 or data URL")
    # A video is read by turning it into a picture first: either its opening
    # frame or a labelled sheet of frames spanning the whole clip. The models
    # on the farm read images, so this is what "the Vision node accepts video"
    # means in practice — and doing it here keeps it one request for a caller.
    video_url: Optional[str] = Field(
        None, max_length=2048,
        description="Public https URL of a video to read instead of an image")
    video_mode: str = Field(
        "storyboard",
        description="storyboard (frames from start to end) or frame (first frame only)")
    system_prompt: Optional[str] = Field(
        None, max_length=4000,
        description="Standing instructions for the model, kept out of the answer")
    structured: bool = Field(
        False,
        description="Ask for one JSON object {\"output_text\": …} and return only its text")
    model: Optional[str] = Field(None, description="Model id from /api/ai/models")

    @field_validator("video_url")
    @classmethod
    def validate_video_source(cls, value):
        if value is None or not str(value).strip():
            return None
        from renderfin.video_input import VideoInputError, validate_video_url
        try:
            return validate_video_url(str(value))
        except VideoInputError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("video_mode")
    @classmethod
    def validate_video_mode(cls, value):
        mode = str(value or "storyboard").strip().lower()
        if mode not in ("storyboard", "frame"):
            raise ValueError("video_mode must be 'storyboard' or 'frame'")
        return mode
    max_output_tokens: Optional[int] = Field(None, ge=-1,
        description="-1 removes the output-token cap; positive values request a budget")

    @field_validator("max_output_tokens")
    @classmethod
    def validate_output_tokens(cls, value):
        if value == 0:
            raise ValueError("Use -1 for no output-token cap, or a positive budget")
        return value
    wait_seconds: Optional[float] = Field(
        None, ge=0, le=MAX_WAIT_SECONDS,
        description="Hold the response until the answer is ready, up to this long",
    )


class TextRequest(BaseModel):
    prompt: str = Field("", description="What to do; may be empty if `input` is given")
    system_prompt: Optional[str] = Field(
        None, max_length=4000,
        description="Standing instructions sent as a separate system-role message")
    structured: bool = Field(
        False,
        description="Ask for one JSON object {\"output_text\": …} and return only its text")
    # The text to work on, kept apart from the instruction. Sending the two
    # already glued together works, but then a caller who has a document and a
    # standing instruction has to do the gluing, and every caller does it
    # slightly differently.
    input: Optional[str] = Field(None, description="Text the instruction applies to")
    model: Optional[str] = Field(None, description="Model id from /api/ai/models")
    max_output_tokens: Optional[int] = Field(None, ge=-1,
        description="-1 removes the output-token cap; positive values request a budget")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)

    @field_validator("max_output_tokens")
    @classmethod
    def validate_output_tokens(cls, value):
        if value == 0:
            raise ValueError("Use -1 for no output-token cap, or a positive budget")
        return value

    def combined_prompt(self) -> str:
        """Instruction first, then the material, with a marker between them.

        A plain blank line is not enough: a long document runs into the
        instruction and the model answers about the wrong half.
        """
        instruction = str(self.prompt or "").strip()
        material = str(self.input or "").strip()
        if not material:
            return instruction
        if not instruction:
            return material
        return instruction + "\n\n--- text ---\n" + material


def _model_entry(model_id: Optional[str]) -> Dict[str, object]:
    wanted = str(model_id or DEFAULT_MODEL_ID).strip().lower()
    for entry in AI_MODELS:
        if str(entry["id"]).lower() == wanted:
            return entry
    raise HTTPException(
        status_code=400,
        detail={
            "error_string": "unknown_model",
            "message_string": f"Unknown model '{model_id}'",
            "available_models_array": [entry["id"] for entry in AI_MODELS],
        },
    )


WORKERS_FILE = os.getenv(
    "RENDERFIN_HUNYUAN_WORKERS_FILE", "/etc/autorig-renderfin-hunyuan.json"
)


def _worker_entries() -> List[Tuple[Dict[str, object], Dict[str, object]]]:
    """(normalised worker, raw file entry) pairs, one per physical node."""
    try:
        raw = json.loads(pathlib.Path(WORKERS_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("Worker list %s does not exist", WORKERS_FILE)
        return []
    except Exception:
        logger.exception("Could not read the worker list %s", WORKERS_FILE)
        return []
    entries = raw.get("workers") if isinstance(raw, dict) else raw
    pairs = []
    seen_nodes = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip().rstrip("/")
        token = str(entry.get("token") or "").strip()
        if not url or not token:
            continue
        node = str(entry.get("physical_node") or entry.get("name") or url).strip().lower()
        if node in seen_nodes:
            continue
        seen_nodes.add(node)
        pairs.append(({
            "name": str(entry.get("name") or node),
            "url": url,
            "token": token,
            "physical_node": node,
        }, entry))
    return pairs


def _load_hunyuan_workers() -> List[Dict[str, object]]:
    """Nodes allowed to run Hunyuan, which is a narrower set than AI Vision.

    Here the file's `enabled` flag does matter: those entries are parked for
    Hunyuan-specific reasons — a bake contract failure, a torch crash, capacity
    held back for conversion — and ignoring them would send 3D work to a node
    somebody deliberately took out of 3D.
    """
    workers = []
    for entry, raw in _worker_entries():
        if raw.get("enabled") is False or raw.get("disabled") is True:
            continue
        if raw.get("canary_approved") is False:
            continue
        workers.append(entry)
    return workers


def _load_ai_workers() -> List[Dict[str, object]]:
    """Farm nodes that can answer an AI request, with their per-node token.

    The node list and tokens are shared with Hunyuan, but its `enabled` flag is
    not: a node parked for a Hunyuan bake bug still reads images perfectly well.
    Only `ai_vision_enabled: false` parks a node for AI, and whether it is
    actually free is settled by probing it, not by this file.
    """
    return [entry for entry, raw in _worker_entries()
            if raw.get("ai_vision_enabled") is not False]


def _node_key(worker: Dict[str, object]) -> str:
    """Stable, URL-safe node label used as the task id prefix."""
    name = str(worker.get("physical_node") or worker.get("name") or worker.get("url"))
    return "".join(ch if ch.isalnum() else "-" for ch in name.strip().lower()).strip("-")


def _ai_base(worker: Dict[str, object]) -> str:
    """Worker URLs are bare origins; the converter API lives under this path."""
    return str(worker["url"]).rstrip("/") + "/api-converter-glb"


async def _node_is_free(client: httpx.AsyncClient, worker: Dict[str, object]) -> Tuple[bool, Dict[str, object]]:
    """Return (reachable, {load, models, loaded}) so a node can be chosen on facts."""
    url = _ai_base(worker) + "/server-status"
    try:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=10.0,
        )
        if response.status_code != 200:
            return False, {}
        payload = response.json()
    except Exception:
        return False, {}
    # Real occupancy lives in tasks_summary; the top level has no such fields,
    # and reading them made every node look idle, so AI work queued behind a
    # twenty-minute conversion instead of going to a free node.
    summary = payload.get("tasks_summary")
    queued = 0
    active_count = 0
    if isinstance(summary, dict):
        try:
            queued = int(summary.get("queue_size") or summary.get("pending") or 0)
            active_count = int(summary.get("processing") or 0)
        except (TypeError, ValueError):
            queued, active_count = 0, 0
    catalogue = payload.get("ai_models")
    models = []
    loaded = ""
    if isinstance(catalogue, dict):
        loaded = str(catalogue.get("loaded_model") or "")
        for entry in catalogue.get("models") or []:
            if isinstance(entry, dict) and entry.get("id"):
                models.append(str(entry["id"]))
    return True, {"load": queued + active_count, "models": models, "loaded": loaded,
                  "system_prompt_supported": bool(isinstance(catalogue, dict) and
                                                   catalogue.get("system_prompt_supported") is True),
                  "system_prompt_models": (catalogue.get("system_prompt_models") or [])
                                           if isinstance(catalogue, dict) else [],
                  "unlimited_output_supported": bool(isinstance(catalogue, dict) and
                                                       catalogue.get("unlimited_output_supported") is True),
                  "activities": _activities(payload)}


# What a node is actually doing, in the vocabulary the services use. A node
# reports every task it is processing with a `workload_class`, and the AI tasks
# carry a `mode` saying whether a picture was involved, so a busy dot can say
# which kind of work is on the card rather than only that there is some.
WORKLOAD_ACTIVITY = {
    "hunyuan": "3dmodel",
    "comfy": "image",
    "glb": "conversion",
    "conversion": "conversion",
    "autorig": "conversion",
    "autorig_interactive": "conversion",
}


def _activities(payload: Dict[str, object]) -> List[str]:
    found: List[str] = []
    for task in payload.get("processing_tasks") or []:
        if not isinstance(task, dict):
            continue
        workload = str(task.get("workload_class") or "").strip().lower()
        if workload == "ai_vision":
            # One queue serves both. A node that reports the mode gets the
            # finer colour; one that does not is shown as language-model work
            # rather than guessed at, because guessing puts a wrong label on
            # somebody's dot.
            mode = str(task.get("mode") or task.get("ai_mode") or "").strip().lower()
            found.append(mode if mode in ("vision", "text") else "ai")
            continue
        activity = WORKLOAD_ACTIVITY.get(workload)
        if not activity and workload:
            activity = "conversion"
        if activity:
            found.append(activity)
    return found


async def _pick_worker(
    client: httpx.AsyncClient, model_id: Optional[str] = None,
    *, require_system_prompt: bool = False, require_unlimited_output: bool = False,
    with_info: bool = False, exclude: Optional[set] = None,
):
    """The least loaded reachable node that actually serves the requested model.

    With `with_info` the node's probe record comes back alongside it, so the
    caller can say which model the node really serves instead of echoing what
    was asked for.
    """
    workers = _load_ai_workers()
    # Used to step past a node that just refused: it is still reachable and
    # would be picked again, and picking it again is how one node's bad minute
    # becomes the caller's failed render.
    if exclude:
        workers = [w for w in workers if _node_key(w) not in exclude] or workers
    if not workers:
        raise HTTPException(
            status_code=503,
            detail={"error_string": "no_workers_configured",
                    "message_string": "No AI-capable farm nodes are configured"},
        )
    probes = await asyncio.gather(
        *(_node_is_free(client, worker) for worker in workers), return_exceptions=True
    )
    reachable = []
    for worker, probe in zip(workers, probes):
        if isinstance(probe, Exception) or not isinstance(probe, tuple):
            continue
        ok, info = probe
        if ok and isinstance(info, dict):
            reachable.append((worker, info))
    if not reachable:
        raise HTTPException(
            status_code=503,
            detail={"error_string": "no_node_available",
                    "message_string": "No farm node answered; try again shortly"},
        )
    wanted = str(model_id or "").strip()
    if require_unlimited_output:
        reachable = [(worker, info) for worker, info in reachable
                     if info.get("unlimited_output_supported") is True]
        if not reachable:
            raise HTTPException(status_code=503, detail={
                "error_string": "unlimited_output_not_supported",
                "message_string": "No available text worker supports uncapped output yet"})
    if require_system_prompt:
        reachable = [(worker, info) for worker, info in reachable
                     if info.get("system_prompt_supported") is True and
                     (not wanted or wanted in (info.get("system_prompt_models") or []))]
        if not reachable:
            raise HTTPException(status_code=503, detail={
                "error_string": "system_prompt_not_supported",
                "message_string": f"No available text worker has verified system instructions for '{wanted}'"})
    carrying = [(w, i) for w, i in reachable if not wanted or wanted in (i.get("models") or [])]
    if not carrying:
        # A node that publishes no catalogue at all is only worth guessing at
        # for the farm default, which every build carries. Guessing for any
        # other model is how a request for Qwen came back as Bonsai: the node
        # answers with whatever weights it happens to hold, the caller is told
        # it got what it asked for, and a 10-second turn becomes a 50-second
        # one in the wrong voice. A busy node that really serves the model is
        # the better answer, and there is a queue for exactly that.
        silent = ([(w, i) for w, i in reachable if not (i.get("models") or [])]
                  if (not wanted or wanted == DEFAULT_MODEL_ID) else [])
        if not silent:
            served: List[str] = []
            for _, info in reachable:
                for name in info.get("models") or []:
                    if name not in served:
                        served.append(str(name))
            raise HTTPException(status_code=503, detail={
                "error_string": "model_unavailable",
                "message_string": f"No reachable node serves '{wanted}'",
                "requested_model_string": wanted,
                "served_models_array": sorted(served),
                "nodes_checked_int": len(reachable)})
        carrying = silent
    # Load first, warmth only as a tie-break: swapping weights costs seconds,
    # but queueing behind a conversion costs however long that conversion runs.
    carrying.sort(key=lambda item: (
        int(item[1].get("load") or 0),
        0 if wanted and item[1].get("loaded") == wanted else 1,
    ))
    worker, info = carrying[0]
    if with_info:
        return worker, info
    return worker


def _worker_by_key(key: str) -> Optional[Dict[str, object]]:
    for worker in _load_ai_workers():
        if _node_key(worker) == key:
            return worker
    return None


def _output_budget(model: Dict[str, object], asked: Optional[int]) -> int:
    """How many tokens the answer may take.

    Absent or zero means automatic, and automatic is per model rather than one
    number for all: a model that reasons before answering spends the same
    budget on the reasoning, so the figure that suits a plain answer leaves
    nothing for the answer itself.
    """
    if asked == -1:
        return -1
    if asked and int(asked) > 0:
        return int(asked)
    return int(model.get("default_output_tokens") or 1024)


# Owner rule 2026-09-27: every generator runs without text when it has
# pictures. The user's text wins; these fill in only when it is empty.
DEFAULT_REMIX_PROMPT = ("Remix {images} into one coherent image: unify the style and lighting, "
                        "combine the subjects and the story of all inputs; image 1 is the base "
                        "scene and composition.")
DEFAULT_VARIATION_PROMPT = ("A clean, style-consistent variation of the reference picture: keep the "
                            "subject, composition, colours and lighting.")
DEFAULT_STRUCTURE_PROMPT = ("A detailed, natural, well-lit photograph that follows the given "
                            "structure map exactly.")
DEFAULT_ANIMATE_PROMPT = ("Animate the picture naturally: subtle, realistic motion that fits the "
                          "scene; keep the subject, style and framing.")
DEFAULT_TRANSITION_PROMPT = ("A smooth, natural transition from the first frame to the last frame; "
                             "keep the subject and style consistent.")


def default_image_prompt(body) -> str:
    """The prompt an image request gets when its own is empty ('' = nothing to go on)."""
    pictures = [str(item or "").strip() for item in
                [getattr(body, "image_url", None)] + list(getattr(body, "reference_image_urls", None) or [])]
    count = len([item for item in pictures if item]) + (1 if getattr(body, "image_base64", None) else 0)
    if count >= 2:
        names = ", ".join(f"image {index}" for index in range(1, count + 1))
        return DEFAULT_REMIX_PROMPT.replace("{images}", names)
    if count == 1:
        return DEFAULT_VARIATION_PROMPT
    if any(getattr(body, name, None) for name in ("control_pose", "control_depth", "control_canny")):
        return DEFAULT_STRUCTURE_PROMPT
    return ""


def _validate_prompt(raw: str) -> str:
    prompt = str(raw or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail={
            "error_string": "prompt_required",
            "message_string": "prompt must not be empty"})
    if len(prompt) > MAX_PROMPT_CHARS:
        raise HTTPException(status_code=400, detail={
            "error_string": "prompt_too_long",
            "message_string": f"prompt exceeds {MAX_PROMPT_CHARS} characters"})
    return prompt


# --------------------------------------------------- standing instructions

STRUCTURED_OUTPUT_INSTRUCTION = (
    "Reply with exactly one JSON object and nothing else: "
    '{"output_text": "..."}. '
    "Put the whole answer inside output_text as plain text. "
    "No markdown, no code fence, no explanation before or after the object."
)


def _default_system_prompt() -> str:
    import ai_services
    return ai_services.SYSTEM_PROMPT_DEFAULT


def _standing_instruction(system_prompt: Optional[str], structured: bool) -> str:
    """What the model is told before it is told what to do.

    A structured request always carries an instruction, because the JSON shape
    is the whole point of it; an unstructured one carries only what the caller
    sent, so the plain /vision and /text pages behave exactly as before.
    """
    text = str(system_prompt or "").strip()
    if not structured:
        return text
    return (text or _default_system_prompt()).strip() + "\n" + STRUCTURED_OUTPUT_INSTRUCTION


def _instruction_role(model: Dict[str, object]) -> str:
    """Where a model's standing instructions are actually obeyed.

    Only a worker that passed the system-role canary is asked to carry them in
    a system message. For the rest they go at the top of the prompt, which is
    what the graph agent already does — and it matters more than losing an
    instruction would: `_pick_worker(require_system_prompt=True)` refuses every
    node that has not verified the role for that model, so sending a system
    prompt for Qwen does not degrade the answer, it fails the request.
    """
    role = str(model.get("system_prompt_role")
               or model.get("graph_agent_instruction_role")
               or "system").strip().lower()
    return "prompt" if role == "prompt" else "system"


def _apply_standing_instruction(model: Dict[str, object],
                                payload: Dict[str, object],
                                instruction: str) -> Dict[str, object]:
    if not instruction:
        return payload
    prompt = str(payload.get("prompt") or "")
    # One bound on the whole request, whichever way the instruction travels.
    _validate_prompt(instruction + "\n" + prompt)
    if _instruction_role(model) == "system":
        payload["system_prompt"] = instruction
    else:
        payload["prompt"] = instruction + "\n\n" + prompt
    return payload


def _extract_output_text(answer: str) -> Tuple[str, bool]:
    """Unwrap one `{"output_text": …}` object, tolerating what models add.

    Returns (text, was_structured). A model that answers in plain text, or
    fences its JSON, or writes a sentence after it, all end up in the same
    place: the caller gets the answer and never the wrapper. When nothing
    parses the raw answer is returned unchanged, so a model that ignores the
    instruction still produces a usable node result rather than an empty one.
    """
    text = str(answer or "")
    candidate = text.strip()
    if "```" in candidate:
        parts = candidate.split("```")
        if len(parts) >= 3:
            inner = parts[1]
            first, newline, rest = inner.partition("\n")
            # ```json on its own line is a language tag, not content.
            if newline and (not first.strip() or first.strip().isalnum()):
                inner = rest
            candidate = inner.strip() or candidate
    decoder = json.JSONDecoder()
    index = candidate.find("{")
    while index >= 0:
        try:
            value, _end = decoder.raw_decode(candidate[index:])
        except ValueError:
            index = candidate.find("{", index + 1)
            continue
        if isinstance(value, dict) and isinstance(value.get("output_text"), str):
            return value["output_text"].strip(), True
        index = candidate.find("{", index + 1)
    return text, False


def _validate_model_choice(service_id: str, checkpoint: Optional[str],
                           lora: Optional[str]) -> Dict[str, object]:
    """Accept only files the catalogue offers for this service.

    A name here ends up in a workflow that a worker then loads off its own
    disk, so an unchecked string would let a caller point a GPU at an arbitrary
    path. Checking against the catalogue also means a model the farm cannot run
    is refused with a reason instead of failing minutes later on the card.
    """
    import ai_model_catalogue
    import ai_model_defaults

    chosen: Dict[str, object] = {}
    selected: Dict[str, Dict[str, object]] = {}
    for name, kind in ((checkpoint, "checkpoint"), (lora, "lora")):
        original = str(name or "").strip()
        if not original:
            continue
        wanted = ai_model_defaults.canonical_file(original)
        entry = (ai_model_catalogue.known_file(wanted, kind)
                 or ai_model_catalogue.known_file(original, kind))
        if entry is None:
            raise HTTPException(status_code=400, detail={
                "error_string": "unknown_" + kind,
                "message_string": f"No {kind} called '{wanted}' is in the catalogue"})
        if not entry.get("usable"):
            raise HTTPException(status_code=400, detail={
                "error_string": kind + "_not_runnable",
                "message_string": str(entry.get("unusable_reason")
                                      or "The farm has no workflow that loads this")})
        if service_id not in (entry.get("services") or []):
            raise HTTPException(status_code=400, detail={
                "error_string": kind + "_wrong_service",
                "message_string": (f"'{wanted}' is for "
                                   f"{', '.join(entry.get('services') or []) or 'nothing here'}, "
                                   f"not {service_id}")})
        chosen[kind] = (wanted if wanted != original
                        else str(entry.get("file") or wanted))
        selected[kind] = entry
    if selected:
        if not ai_model_defaults.compatible(selected.get("checkpoint"), selected.get("lora")):
            raise HTTPException(status_code=400, detail={
                "error_string": "incompatible_model_pair",
                "message_string": "The checkpoint and LoRA use different model families"})
    return chosen


def _effective_model_settings(service_id: str, checkpoint: Optional[str],
                              lora: Optional[str], explicit: Dict[str, object],
                              use_default: bool = True,
                              mode: str = "", control_channel: str = "",
                              ) -> tuple[Dict[str, object], str]:
    """Validate a selection and merge its attributed recommendations."""
    import ai_model_catalogue
    import ai_model_defaults

    mode = str(mode or "").strip().lower()
    control_channel = str(control_channel or "").strip().lower()
    entries = ai_model_catalogue.entries()
    required_default_family = ""
    if service_id == "image" and mode in LEGACY_IMAGE_MODES:
        required_default_family = LEGACY_IMAGE_MODE_FAMILY
    elif service_id == "image" and control_channel and not checkpoint and not lora:
        # Pony/SDXL left the farm on 2026-09-23; Z-Image's Fun ControlNet
        # Union patch carries pose/depth/canny now.
        required_default_family = LEGACY_IMAGE_MODE_FAMILY

    if required_default_family and not checkpoint and not lora:
        family_default = ai_model_defaults.family_default_checkpoint(
            entries, {"family": required_default_family}, service_id)
        if family_default is None:
            raise HTTPException(status_code=400, detail={
                "error_string": "model_family_default_missing",
                "message_string": (
                    f"No validated {required_default_family} checkpoint is configured "
                    "for this image mode")})
        checkpoint = str(family_default.get("file") or "") or None

    if (not required_default_family and not use_default and not checkpoint
            and not lora and service_id == "image"):
        legacy_base = ai_model_defaults.family_default_checkpoint(
            entries, {"family": LEGACY_IMAGE_MODE_FAMILY}, "image")
        checkpoint = str((legacy_base or {}).get("file") or "") or None
    if not checkpoint and lora:
        # Validate the adapter first, then resolve only a catalogue-declared
        # base for its architecture. Falling through to a workflow's embedded
        # loader made the same saved node run Schnell on one worker and another
        # Flux.1 base on a different worker.
        lora_choice = _validate_model_choice(service_id, None, lora)
        lora_entry = ai_model_catalogue.known_file(
            str(lora_choice.get("lora") or ""), "lora")
        family_default = ai_model_defaults.family_default_checkpoint(
            entries, lora_entry, service_id)
        if family_default is None:
            raise HTTPException(status_code=400, detail={
                "error_string": "checkpoint_required_for_lora",
                "message_string": (
                    "Select a compatible checkpoint for this LoRA; the catalogue "
                    "does not declare an automatic base model for its family")})
        checkpoint = str(family_default.get("file") or "") or None
    if use_default and not checkpoint and not lora:
        default_entry = next((entry for entry in entries
                              if entry.get("kind") == "checkpoint"
                              and (entry.get("default") is True
                                   or service_id in (entry.get("default_for_services") or []))
                              and entry.get("usable")
                              and service_id in (entry.get("services") or [])), None)
        checkpoint = str((default_entry or {}).get("file") or "") or None
    chosen = _validate_model_choice(service_id, checkpoint, lora)
    checkpoint_entry = ai_model_catalogue.known_file(chosen.get("checkpoint", ""), "checkpoint")
    lora_entry = ai_model_catalogue.known_file(chosen.get("lora", ""), "lora")
    selected_entry = checkpoint_entry or lora_entry
    selected_family = ai_model_defaults.model_family(selected_entry)
    if mode in LEGACY_IMAGE_MODES and selected_family != LEGACY_IMAGE_MODE_FAMILY:
        raise HTTPException(status_code=400, detail={
            "error_string": "mode_model_incompatible",
            "message_string": f"{mode} requires a Z-Image checkpoint"})
    if control_channel:
        try:
            ai_model_defaults.control_workflow(selected_family, control_channel)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={
                "error_string": "control_model_incompatible",
                "message_string": str(exc)}) from None
    try:
        effective = ai_model_defaults.resolve(checkpoint_entry, lora_entry, explicit)
    except ValueError as exc:
        compatible_pair = ai_model_defaults.compatible(checkpoint_entry, lora_entry)
        raise HTTPException(status_code=400, detail={
            "error_string": ("invalid_sampling_settings" if compatible_pair
                             else "incompatible_model_pair"),
            "message_string": str(exc)}) from None
    if mode in LEGACY_IMAGE_MODES:
        # These templates carry their own tuned sampling (the inpaint and
        # T-pose graphs keep their own control strength and step counts).
        # Model-level Auto values must not flatten every stage to one preset.
        # A caller's explicit knob is still forwarded and remains authoritative.
        for key in ("steps", "cfg", "sampler", "scheduler", "clip_skip"):
            if explicit.get(key) in (None, ""):
                effective.pop(key, None)
    payload = dict(chosen)
    payload.update(effective)
    return payload, ai_model_defaults.add_triggers("", (checkpoint_entry, lora_entry))


def _render_model_profile(service_id: str, checkpoint: Optional[str],
                          lora: Optional[str], *, mode: str = "",
                          has_control: bool = False) -> List[Dict[str, object]]:
    """Catalogue material whose policy can change this automatic request."""
    import ai_model_catalogue
    import ai_model_defaults

    entries = ai_model_catalogue.entries()
    selected = {str(checkpoint or ""), str(lora or "")}
    families = set()
    if str(mode or "").strip().lower() in LEGACY_IMAGE_MODES:
        families.add(LEGACY_IMAGE_MODE_FAMILY)
    elif has_control and not checkpoint and not lora:
        families.add(LEGACY_IMAGE_MODE_FAMILY)
    if lora:
        lora_entry = ai_model_catalogue.known_file(str(lora), "lora")
        family = ai_model_defaults.model_family(lora_entry)
        if family:
            families.add(family)
    fields = ("file", "base", "version", "workflow", "recommended",
              "sampling_policy", "triggers", "source_version_id", "sha256")
    return [
        {key: entry.get(key) for key in fields}
        for entry in entries
        if (
            entry.get("file") in selected
            or service_id in (entry.get("default_for_services") or [])
            or (families.intersection(entry.get("default_for_families") or []))
        )
    ]


@router.get("/api/ai/model-settings")
async def api_ai_model_settings(service: str, checkpoint: Optional[str] = None,
                                lora: Optional[str] = None, control_channel: str = "", mode: str = ""):
    """Resolved catalogue defaults used when a model selection changes."""
    service_id = str(service or "").strip().lower()
    if service_id not in ("image", "video"):
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_service",
            "message_string": "service must be image or video"})
    effective, trigger_prefix = _effective_model_settings(
        service_id, checkpoint, lora, {},
        use_default=not (service_id == "image" and bool(control_channel or mode)),
        mode=mode, control_channel=control_channel)
    effective.setdefault("main_size_width", 960)
    effective.setdefault("main_size_height", 540)
    import ai_model_catalogue
    selected_checkpoint = ai_model_catalogue.known_file(
        str(effective.get("checkpoint") or ""), "checkpoint") or {}
    return {
        "success_bool": True,
        "service_string": service_id,
        "checkpoint_string": str(effective.get("checkpoint") or ""),
        "lora_string": str(effective.get("lora") or ""),
        "trigger_prefix_string": trigger_prefix,
        "effective_params_object": effective,
        "sampling_policy_object": selected_checkpoint.get("sampling_policy") or {},
        "server_time_unix_int": int(time.time()),
    }


def _decode_inline_image(raw: str) -> bytes:
    payload = str(raw or "").strip()
    if payload.startswith("data:"):
        _, _, payload = payload.partition(",")
    try:
        data = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(status_code=400, detail={
            "error_string": "image_not_base64",
            "message_string": "image_base64 is not valid base64"}) from None
    if not data:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_empty", "message_string": "image_base64 decoded to nothing"})
    if len(data) > MAX_INLINE_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail={
            "error_string": "image_too_large",
            "message_string": f"inline image exceeds {MAX_INLINE_IMAGE_BYTES} bytes"})
    return data


async def _publish_inline_image(client: httpx.AsyncClient, data: bytes) -> str:
    """Park an inline image on the public scratch store the farm can read.

    The converter only accepts a public URL, by the same SSRF rule that guards
    Hunyuan, so bytes a caller pasted have to become one first.
    """
    try:
        response = await client.post(
            "https://autorig.online/dev/api/scratch",
            files={"file": ("upload.png", data, "image/png")},
            timeout=60.0,
        )
        response.raise_for_status()
        url = str((response.json() or {}).get("url") or "").strip()
    except Exception:
        logger.exception("Could not publish an inline image for the farm")
        raise HTTPException(status_code=502, detail={
            "error_string": "image_publish_failed",
            "message_string": "Could not stage the uploaded image"}) from None
    if not url:
        raise HTTPException(status_code=502, detail={
            "error_string": "image_publish_failed",
            "message_string": "Image store returned no URL"})
    return url


async def _submit(
    client: httpx.AsyncClient,
    worker: Dict[str, object],
    path: str,
    payload: Dict[str, object],
) -> str:
    url = _ai_base(worker) + path
    try:
        response = await client.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=SUBMIT_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.exception("AI submission to %s failed", url)
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_unreachable",
            "message_string": "The farm node did not accept the request"}) from None
    if response.status_code == 503:
        raise HTTPException(status_code=503, detail={
            "error_string": "worker_busy",
            "message_string": "The farm node is not accepting AI work right now",
            "retryable_bool": True})
    if response.status_code not in (200, 202):
        # The node usually says why, and its reason is the useful part: a node
        # holding a maintenance claim during a deploy refuses work on purpose,
        # and "HTTP 400" alone reads like a bug in the request.
        reason = ""
        try:
            body = response.json() or {}
            # The sentence first, the code second: "invalid_request" alone
            # reads like a malformed body when the node actually said its DNS
            # timed out.
            reason = str(body.get("message") or body.get("error")
                         or body.get("detail") or "").strip()
        except Exception:
            reason = response.text.strip()[:200]
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_rejected",
            "message_string": (f"Farm node {_node_key(worker)} refused the job: {reason}"
                               if reason else
                               f"Farm node {_node_key(worker)} answered HTTP "
                               f"{response.status_code}")})
    task_id = str((response.json() or {}).get("task_id") or "").strip()
    if not task_id:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_no_task_id",
            "message_string": "Farm node accepted the request without a task id"})
    return task_id


async def _fetch_status(
    client: httpx.AsyncClient, worker: Dict[str, object], worker_task_id: str
) -> Dict[str, object]:
    url = _ai_base(worker) + f"/ai-vision/status/{worker_task_id}"
    try:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {worker['token']}"},
            timeout=STATUS_TIMEOUT_SECONDS,
        )
    except Exception:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_unreachable",
            "message_string": "The farm node did not answer"}) from None
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={
            "error_string": "task_not_found", "message_string": "No such task"})
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_status_failed",
            "message_string": f"Farm node answered HTTP {response.status_code}"})
    return response.json() or {}


def _public_status(task_id: str, model_id: str, raw: Dict[str, object],
                   service_id: str = "", *,
                   served_model: str = "") -> Dict[str, object]:
    status = str(raw.get("status") or "")
    finished = status in ("Completed", "Failed")
    if status == "Completed" and service_id:
        record_duration(service_id, float(raw.get("elapsed_seconds") or 0))
    # Unwrapped here rather than at the endpoint that submitted the work: a
    # graph node gets its task id back immediately and collects the answer from
    # /api/ai/status, so unwrapping only on submission would hand the wrapper
    # to every caller that does not block. Keyed on the shape, not on remembered
    # state, so a restart between submit and poll changes nothing.
    answer = str(raw.get("answer") or "")
    output_text, was_structured = _extract_output_text(answer)
    return {
        "success_bool": status != "Failed",
        "task_id_string": task_id,
        "status_string": status.lower() or "pending",
        "finished_bool": finished,
        "mode_string": str(raw.get("mode") or ""),
        "model_string": model_id,
        # What the node really ran, when that is known: the accept response
        # knows it from the node's catalogue, a poll from the node's own
        # report. Empty means "not established yet", never "the default".
        "served_model_string": str(served_model or raw.get("model") or ""),
        "answer_string": output_text,
        "raw_answer_string": answer,
        "structured_answer_bool": was_structured,
        "reasoning_string": str(raw.get("reasoning") or ""),
        "error_string": str(raw.get("error") or ""),
        "elapsed_seconds_float": round(float(raw.get("elapsed_seconds") or 0.0), 2),
        "stage_string": str(raw.get("current_stage") or ""),
        "server_time_unix_int": int(time.time()),
    }


def _folded_system_prompt(payload: Dict[str, object],
                          refusal: HTTPException) -> Optional[Dict[str, object]]:
    """Put the standing instruction at the top of the prompt instead.

    One worker build on the farm has passed the system-role canary. When that
    node is offline the choice is between an answer given the instruction the
    way every other model on the farm gets it, and no answer at all — and a
    node showing "no worker has verified system instructions" is the worse of
    the two by a distance. Only that one refusal is caught: a missing model or
    an unreachable farm still fails, because folding would not fix either.
    """
    detail = refusal.detail if isinstance(refusal.detail, dict) else {}
    if str(detail.get("error_string") or "") != "system_prompt_not_supported":
        return None
    instruction = str(payload.get("system_prompt") or "")
    if not instruction:
        return None
    folded = dict(payload)
    folded.pop("system_prompt", None)
    folded["prompt"] = instruction + "\n\n" + str(payload.get("prompt") or "")
    return folded


# Faults that are about the node's moment rather than the request. Matched on
# the node's own wording because the converter reports them all under one code.
TRANSIENT_REFUSALS = (
    "host resolution",
    "timed out",
    "timeout",
    "temporarily",
    "connection reset",
    "connection aborted",
    "maintenance",
)


def _refusal_reason(exc: HTTPException) -> str:
    detail = exc.detail if isinstance(exc.detail, dict) else {}
    return str(detail.get("message_string") or detail.get("error_string") or exc.detail)


def _refusal_is_transient(exc: HTTPException) -> bool:
    detail = exc.detail if isinstance(exc.detail, dict) else {}
    if str(detail.get("error_string") or "") not in ("worker_rejected", "worker_unreachable"):
        return False
    reason = _refusal_reason(exc).lower()
    return any(mark in reason for mark in TRANSIENT_REFUSALS)


async def _run(
    request_model: Dict[str, object],
    path: str,
    payload: Dict[str, object],
    wait_seconds: Optional[float],
    service_id: str = "",
) -> Dict[str, object]:
    model_id = str(request_model["id"])
    async with httpx.AsyncClient() as client:
        requirements = {}
        if payload.get("system_prompt"):
            requirements["require_system_prompt"] = True
        if payload.get("max_output_tokens") == -1:
            requirements["require_unlimited_output"] = True
        try:
            picked = await _pick_worker(client, model_id, with_info=True, **requirements)
        except HTTPException as exc:
            folded = _folded_system_prompt(payload, exc)
            if folded is None:
                raise
            payload = folded
            requirements.pop("require_system_prompt", None)
            picked = await _pick_worker(client, model_id, with_info=True, **requirements)
        # A test double, or any caller predating `with_info`, hands back the
        # bare node; then the served model is simply not known yet and the
        # status poll reports it.
        worker, node_info = picked if isinstance(picked, tuple) else (picked, {})
        served_model = model_id if model_id in (node_info.get("models") or []) else ""
        try:
            worker_task_id = await _submit(client, worker, path, dict(payload, model=model_id))
        except HTTPException as exc:
            # A node whose network blinked refuses the job and the next node
            # takes it without anyone noticing. Only transient faults are
            # retried: a genuinely bad request would be refused everywhere and
            # retrying it would just cost a second node its time.
            if not _refusal_is_transient(exc):
                raise
            logger.warning("Node %s refused a %s job (%s); trying another node",
                           _node_key(worker), service_id or path, _refusal_reason(exc))
            second = await _pick_worker(client, model_id, with_info=True,
                                        exclude={_node_key(worker)}, **requirements)
            worker, node_info = second if isinstance(second, tuple) else (second, {})
            served_model = model_id if model_id in (node_info.get("models") or []) else ""
            worker_task_id = await _submit(client, worker, path, dict(payload, model=model_id))
        task_id = f"{_node_key(worker)}.{worker_task_id}"
        raw: Dict[str, object] = {"status": "Pending"}
        if wait_seconds and wait_seconds > 0:
            deadline = time.monotonic() + min(float(wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(2)
                raw = await _fetch_status(client, worker, worker_task_id)
                if str(raw.get("status")) in ("Completed", "Failed"):
                    break
        result = _public_status(task_id, model_id, raw, service_id,
                                served_model=served_model)
        result["status_url_string"] = f"/api/ai/status/{task_id}"
        result["node_string"] = _node_key(worker)
        return result


@router.get("/api/ai/models")
async def api_ai_models():
    """Models a caller may select for /api/vision and /api/text2text."""
    return {
        "success_bool": True,
        "models_array": AI_MODELS,
        "default_model_string": DEFAULT_MODEL_ID,
        "server_time_unix_int": int(time.time()),
    }


@router.get("/api/vision")
async def api_vision_docs():
    """GET mirror documenting the POST image endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/vision",
        "required_fields_array": ["prompt", "image_url, image_base64 or video_url"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds",
                                  "video_url", "video_mode", "system_prompt", "structured"],
        "video_mode_array": ["storyboard", "frame"],
        "structured_string": (
            "true asks the model for one {\"output_text\": …} object and returns "
            "its text as answer_string, with the model's own reply kept in "
            "raw_answer_string"),
        "models_url_string": "/api/ai/models",
        "example_request_object": {
            "prompt": "What is in this picture?",
            "image_url": "https://example.com/photo.png",
            "model": DEFAULT_MODEL_ID,
            "wait_seconds": 120,
        },
        "example_response_object": {
            "success_bool": True,
            "task_id_string": "f1-pc.0f1c…",
            "status_string": "completed",
            "answer_string": "A black street lamp on a magenta background.",
        },
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/vision")
async def api_vision(request: Request, body: VisionRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
    if "vision" in ("vision", "text"):
        model = _model_entry(body.model)
        payload["model"] = model["id"]
        payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    import hashlib
    if "vision" in ("vision", "text"):
        profile = _model_entry(body.model)
    else:
        import ai_model_catalogue
        selected = {str(body.checkpoint or ""), str(body.lora or "")}
        profile = [{key: entry.get(key) for key in ("file", "base", "version", "workflow", "recommended", "sampling_policy", "triggers", "source_version_id", "sha256")}
                   for entry in ai_model_catalogue.entries()
                   if entry.get("file") in selected or "vision" in (entry.get("default_for_services") or [])]
    payload["profile_hash"] = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    # Namespace bumped with the structured answer: a cached reply from before
    # the JSON contract is the wrapper text, and replaying it would put the
    # model's preamble back into every node that asked for a clean prompt.
    return await ai_request_cache.run_cached("vision", payload,
        lambda: _uncached_api_vision(request, body), namespace="ai-structured-20260922-v1")


async def _uncached_api_vision(request: Request, body: VisionRequest):
    model = _model_entry(body.model)
    prompt = _validate_prompt(body.prompt)
    if not body.image_url and not body.image_base64 and not body.video_url:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url, image_base64 or video_url"})
    image_url = str(body.image_url or "").strip()
    video_note = ""
    if not image_url and not body.image_base64:
        # A video reaches the model as a picture of the video. Which picture is
        # the whole difference between "what is in this shot" and "what happens
        # in this clip", so the mode is a node setting rather than a constant.
        import ai_video_reference
        view = "first_frame" if body.video_mode == "frame" else "contact_sheet"
        reference = await ai_video_reference.reference_for_video(
            str(body.video_url or ""), view)
        image_url = str(reference.get("image_url_string") or "")
        if not image_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "video_reference_failed",
                "message_string": "The video could not be turned into a picture"})
        video_note = str(reference.get("prompt_prefix_string") or "")
    elif not image_url:
        async with httpx.AsyncClient() as client:
            image_url = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
    if video_note:
        # Without this the model sees a collage and describes the tiles; with
        # it, it sees one video and describes what happens over its length.
        prompt = _validate_prompt(video_note + "\n\n" + prompt)
    payload: Dict[str, object] = {"prompt": prompt, "image_url": image_url}
    payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    _apply_standing_instruction(
        model, payload, _standing_instruction(body.system_prompt, body.structured))
    return await _run(model, "/ai-vision", payload, body.wait_seconds, "vision")


@router.get("/api/text2text")
async def api_text2text_docs():
    """GET mirror documenting the POST text endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/text2text",
        "required_fields_array": ["prompt"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds",
                                  "input", "system_prompt", "structured"],
        "models_url_string": "/api/ai/models",
        "example_request_object": {"prompt": "Name three colours.", "wait_seconds": 60},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/text2text")
async def api_text2text(request: Request, body: TextRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
    if "text" in ("vision", "text"):
        model = _model_entry(body.model)
        payload["model"] = model["id"]
        payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    import hashlib
    if "text" in ("vision", "text"):
        profile = _model_entry(body.model)
    else:
        import ai_model_catalogue
        selected = {str(body.checkpoint or ""), str(body.lora or "")}
        profile = [{key: entry.get(key) for key in ("file", "base", "version", "workflow", "recommended", "sampling_policy", "triggers", "source_version_id", "sha256")}
                   for entry in ai_model_catalogue.entries()
                   if entry.get("file") in selected or "text" in (entry.get("default_for_services") or [])]
    payload["profile_hash"] = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    return await ai_request_cache.run_cached("text", payload,
        lambda: _uncached_api_text2text(request, body), namespace="ai-structured-20260922-v1")


async def _uncached_api_text2text(request: Request, body: TextRequest):
    model = _model_entry(body.model)
    payload: Dict[str, object] = {"prompt": _validate_prompt(body.combined_prompt())}
    # Keep the same total request bound, and send the instructions by whichever
    # route this model has actually been shown to obey.
    _apply_standing_instruction(
        model, payload, _standing_instruction(body.system_prompt, body.structured))
    payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    return await _run(model, "/text2text", payload, body.wait_seconds, "text")


@router.get("/api/ai/status/{task_id}")
async def api_ai_status(task_id: str):
    node_key, _, worker_task_id = str(task_id).partition(".")
    if not node_key or not worker_task_id:
        raise HTTPException(status_code=400, detail={
            "error_string": "malformed_task_id",
            "message_string": "task_id must look like <node>.<id>"})
    worker = _worker_by_key(node_key)
    if worker is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "unknown_node",
            "message_string": "The node that owns this task is not configured"})
    async with httpx.AsyncClient() as client:
        raw = await _fetch_status(client, worker, worker_task_id)
    mode = str(raw.get("mode") or "")
    actual_model = str(raw.get("model") or raw.get("requested_model") or "")
    result = _public_status(task_id, actual_model, raw,
                            "vision" if mode == "vision" else "text")
    result["node_string"] = node_key
    import ai_request_cache
    await ai_request_cache.anote_result(task_id, str(result.get("status_string") or ""), result)
    return result

# The knobs below are the ones Renderfin's RenderPrompt actually carries. They
# are optional everywhere: a caller who sends only a prompt gets exactly the
# behaviour it had before these existed.
IMAGE_MODES = ("", "z_depth", "t_pose", "open_pose", "inpaint")
VIDEO_QUALITIES = {
    "standard": "gen_animation_by_url.json",
    "hq": "gen_animation_hq_by_url.json",
}
# Typed image modes with their own graphs (depth from a sphere, T-pose from a
# skeleton, pose from a photo, background extraction). FLUX.1 ran them until
# 2026-09-23; they now run on Z-Image Turbo with the Fun ControlNet Union patch,
# which also gives inpaint a model again (FLUX.1 Fill Dev was never installed).
LEGACY_IMAGE_MODES = {"z_depth", "t_pose", "open_pose", "inpaint"}
LEGACY_IMAGE_MODE_FAMILY = "zimage"
# Pose / depth / canny video control runs LTX-2.5 with the Union-Control
# IC-LoRA. The 2.3 name is accepted from nodes saved before the migration.
VIDEO_CONTROL_CHECKPOINT = "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors"
VIDEO_CONTROL_CHECKPOINTS = (VIDEO_CONTROL_CHECKPOINT, "ltx-2.3-22b-distilled-1.1")
LTX25_HQ_WORKFLOW = "gen_animation_ltx25_hq_by_url.json"


def clamp_video_frames(entry, frames: int) -> int:
    """Snap a requested length into the checkpoint's trained frame range.

    MiniMax H3 is trained on 124-362 frames (about 5-15 s at 24 fps); 393
    frames ran 7x slower per step on the 4090 and is outside what the model
    has seen. The catalogue entry's `frame_range` carries the limits.
    """
    limits = (entry or {}).get("frame_range")
    if not (isinstance(limits, (list, tuple)) and len(limits) == 2):
        return frames
    low, high = int(limits[0]), int(limits[1])
    return max(low, min(high, int(frames)))


def _video_quality_workflow(quality: str, family: str,
                            model_workflow: str = "") -> str:
    """Which template a quality label may substitute, if any.

    `quality` predates model selection: it chose between the two animation
    templates when nothing else could. A checkpoint that names its own
    validated workflow is the stronger statement, so the label may no longer
    swap the template under it — a Standard label is simply that model's
    standard, and a label that asks for a *different* template is a conflict
    the caller has to see rather than a silent model substitution.
    """
    quality = str(quality or "").strip().lower()
    family = str(family or "").strip().lower()
    model_workflow = str(model_workflow or "").strip()
    if family == "ltx25":
        # LTX-2.5 has a real second tier: the official two-stage graph
        # (half-size pass, x2 latent upscale, three-step refine).
        return LTX25_HQ_WORKFLOW if quality == "hq" else ""
    if quality == "hq" and family == "ltx23":
        raise HTTPException(status_code=400, detail={
            "error_string": "unsupported_video_quality",
            "message_string": (
                "High quality is not a separate validated workflow for modern "
                "LTX 2.3; use Standard, which keeps the selected model's workflow")})
    if not model_workflow:
        return str(VIDEO_QUALITIES.get(quality) or "") if family != "ltx23" else ""
    wanted = str(VIDEO_QUALITIES.get(quality) or "")
    if wanted and wanted != model_workflow:
        raise HTTPException(status_code=400, detail={
            "error_string": "unsupported_video_quality",
            "message_string": (
                f"The selected checkpoint is validated on {model_workflow}; "
                f"'{quality}' would run {wanted} instead. Choose the checkpoint "
                "that belongs to that tier, or leave quality unset")})
    return ""


LoraStackValue = Union[str, List[Union[str, Dict[str, Any]]]]


def _lora_error(code: str, message: str, **extra: object) -> HTTPException:
    detail: Dict[str, object] = {"error_string": code, "message_string": message}
    detail.update(extra)
    return HTTPException(status_code=400, detail=detail)


def _lora_stack_request(service_id: str, prompt: Optional[str], stack_value: object,
                        single_lora: Optional[str]):
    """Cut `<lora:...>` tags out of the prompt and resolve them with the stack.

    Returns (clean prompt, resolved stack, strength override for the single
    `lora`). Precedence and grammar are documented in ai_lora_prompt. An
    unknown or not-yet-installed LoRA is a 400 that names it, never a render
    that quietly left it out.
    """
    import ai_lora_prompt
    import ai_model_catalogue

    text = str(prompt or "")
    try:
        clean, prompt_refs = ai_lora_prompt.parse_prompt(text)
        stack_refs = ai_lora_prompt.parse_stack(stack_value)
    except ai_lora_prompt.LoraSyntaxError as exc:
        raise _lora_error("lora_syntax", str(exc)) from None
    if not prompt_refs and not stack_refs:
        return text, [], None
    loras = [entry for entry in ai_model_catalogue.entries() if entry.get("kind") == "lora"]
    try:
        stack, override = ai_lora_prompt.build_stack(
            stack_refs=stack_refs, prompt_refs=prompt_refs, loras=loras,
            single_lora=str(single_lora or "").strip())
    except ai_lora_prompt.LoraResolutionError as exc:
        raise _lora_error("unknown_lora", str(exc), lora_string=exc.name,
                          suggestions_array=exc.suggestions) from None
    except ai_lora_prompt.LoraSyntaxError as exc:
        raise _lora_error("lora_syntax", str(exc)) from None
    for item in stack:
        entry = item.entry
        if entry.get("no_model"):
            raise _lora_error(
                "lora_has_no_model",
                f"LoRA '{item.file}' is for {entry.get('base') or entry.get('family')}, "
                "and no model on the farm loads it", lora_string=item.file)
        if not entry.get("usable"):
            raise _lora_error(
                "lora_not_ready",
                f"LoRA '{item.file}' is not on any render computer yet: "
                f"{entry.get('unusable_reason') or 'still downloading'}",
                lora_string=item.file)
        if service_id not in (entry.get("services") or []):
            raise _lora_error(
                "lora_wrong_service",
                f"LoRA '{item.file}' is for {', '.join(entry.get('services') or []) or 'nothing'}, "
                f"not {service_id}", lora_string=item.file)
    return clean, stack, override


def _lora_dispatch_gate(payload: Dict[str, object]) -> None:
    """Refuse a LoRA render no render computer can take yet.

    Renderfin only dispatches to a box that advertises the workflow and holds
    every LoRA; with no such box the task would sit in Pending with nobody
    told. The usual case is a LoRA still downloading to the one box that runs
    its model, and the caller is told exactly that.
    """
    names = [str(payload.get("lora") or "")]
    names += [str(item.get("name") or "") for item in (payload.get("loras") or [])
              if isinstance(item, dict)]
    names = [name for name in names if name]
    workflow = payload.get("work_flow")
    if not names or not isinstance(workflow, str) or not workflow:
        return
    import ai_lora_manager
    ok, reason, _boxes = ai_lora_manager.dispatch_check(workflow, names)
    if not ok:
        raise HTTPException(status_code=409, detail={
            "error_string": "lora_not_on_render_computer",
            "message_string": reason, "work_flow_string": workflow})


def _stack_default_checkpoint(service_id: str, stack) -> Optional[str]:
    """The family default checkpoint for a stack given without one."""
    import ai_model_catalogue
    import ai_model_defaults

    if not stack:
        return None
    entry = ai_model_defaults.family_default_checkpoint(
        ai_model_catalogue.entries(), stack[0].entry, service_id)
    if entry is None:
        raise _lora_error(
            "checkpoint_required_for_lora",
            f"Select a checkpoint for LoRA '{stack[0].file}'; the catalogue declares "
            "no automatic base model for its family")
    return str(entry.get("file") or "") or None


def _check_stack_family(stack, checkpoint_file: str) -> None:
    import ai_model_catalogue
    import ai_model_defaults

    checkpoint = ai_model_catalogue.known_file(str(checkpoint_file or ""), "checkpoint")
    for item in stack:
        if not ai_model_defaults.compatible(checkpoint, item.entry):
            raise _lora_error(
                "incompatible_model_pair",
                f"LoRA '{item.file}' ({item.entry.get('base') or item.entry.get('family')}) "
                f"does not fit checkpoint '{checkpoint_file}' "
                f"({(checkpoint or {}).get('base') or (checkpoint or {}).get('family')})",
                lora_string=item.file)


def _stack_profile(service_id: str, prompt: Optional[str], stack_value: object,
                   single_lora: Optional[str]) -> List[Dict[str, object]]:
    """Catalogue facts of the stacked LoRAs, for the request-cache key."""
    try:
        _clean, stack, _override = _lora_stack_request(service_id, prompt, stack_value, single_lora)
    except HTTPException:
        return []
    return [{key: item.entry.get(key) for key in ("file", "sha256", "source_version_id")}
            for item in stack]


class ImageRequest(BaseModel):
    # Defaulted rather than required so an empty one reaches `_validate_prompt`
    # and comes back as "prompt must not be empty" instead of FastAPI's
    # "Field required", which reads like the caller used the wrong field name.
    prompt: str = Field("", description="What to draw; <lora:NAME:WEIGHT> tags pick LoRAs")
    image_url: Optional[str] = Field(None, description="Reference image URL")
    image_base64: Optional[str] = Field(None, description="Reference image, inline")
    wait_seconds: Optional[float] = Field(None, ge=0, le=MAX_WAIT_SECONDS)
    mode: Optional[str] = Field(None, description="z_depth, t_pose, open_pose or inpaint")
    negative_prompt: Optional[str] = Field(None, description="What to avoid")
    width: Optional[int] = Field(None, ge=256, le=2048)
    height: Optional[int] = Field(None, ge=256, le=2048)
    steps: Optional[int] = Field(None, ge=1, le=100)
    cfg: Optional[float] = Field(None, ge=0, le=30)
    sampler: Optional[str] = None
    scheduler: Optional[str] = None
    creativity: Optional[float] = Field(None, ge=0, le=1)
    seed: Optional[int] = Field(None, ge=0, description="0 or absent randomises")
    checkpoint: Optional[str] = Field(None, description="Model file from /api/ai/model-catalogue")
    lora: Optional[str] = Field(None, description="LoRA file from /api/ai/model-catalogue")
    lora_strength: Optional[float] = Field(None, ge=0, le=2)
    loras: Optional[LoraStackValue] = Field(
        None, description="LoRA stack: [{name, strength, strength_clip}] or '<lora:NAME:W> ...'")
    clip_skip: Optional[int] = Field(None, ge=1, le=12)
    control_pose: Optional[str] = Field(None, description="Precomputed pose control-map URL")
    control_depth: Optional[str] = Field(None, description="Precomputed depth control-map URL")
    control_canny: Optional[str] = Field(None, description="Precomputed canny control-map URL")
    control_strength: float = Field(0.8, ge=0, le=2)
    control_start: float = Field(0.0, ge=0, le=1)
    control_end: float = Field(1.0, ge=0, le=1)
    reference_image_urls: Optional[List[str]] = Field(
        None, description=("More pictures after image_url, in order: image 1 is image_url, "
                           "image 2 the first entry here; 3 in all. Since 2026-09-26 "
                           "these edits run on Qwen-Image 2.1 turbo. "
                           "A video URL stands for its first frame"))
    internal_pipeline: Optional[str] = Field(
        None, description="Internal callers only (avatar build): keep the named model for an edit")


# Several pictures composed into one (renderfin.multiref). Only FLUX.2 klein
# takes them natively on this node; Qwen-Image-Edit 2511 has its own node.
MULTIREF_TYPE = "image_multiref"
MULTIREF_WORKFLOW = "gen_image_flux2_klein_multiref.json"
MULTIREF_FAMILIES = frozenset({"flux2"})
MULTIREF_MAX_IMAGES = 4


def _multiref_inputs(body: "ImageRequest") -> List[str]:
    """The extra pictures, validated before anything is published or rendered."""
    extras = [str(item or "").strip() for item in (body.reference_image_urls or [])]
    if not extras:
        return []
    if any(not item for item in extras):
        raise HTTPException(status_code=400, detail={
            "error_string": "empty_reference_image",
            "message_string": "reference_image_urls has an empty entry"})
    for item in extras:
        if not item.startswith(("http://", "https://", "data:")):
            raise HTTPException(status_code=400, detail={
                "error_string": "bad_reference_image",
                "message_string": "Reference pictures must be http(s) URLs or data URLs"})
    total = len(extras) + (1 if (body.image_url or body.image_base64) else 0)
    if total > MULTIREF_MAX_IMAGES:
        raise HTTPException(status_code=400, detail={
            "error_string": "too_many_reference_images",
            "message_string": (f"FLUX.2 klein takes at most {MULTIREF_MAX_IMAGES} pictures; "
                               f"{total} were wired in"),
            "max_int": MULTIREF_MAX_IMAGES})
    if body.control_pose or body.control_depth or body.control_canny or body.mode:
        raise HTTPException(status_code=400, detail={
            "error_string": "multi_reference_exclusive",
            "message_string": ("Several reference pictures cannot be combined with a "
                               "ControlNet map or an image mode")})
    return extras


def _multiref_default_checkpoint() -> str:
    """The installed checkpoint that composes several pictures."""
    import ai_model_catalogue
    import ai_model_defaults

    for entry in ai_model_catalogue.entries():
        if (entry.get("kind") == "checkpoint" and entry.get("usable")
                and "image" in (entry.get("services") or [])
                and ai_model_defaults.model_family(entry) in MULTIREF_FAMILIES):
            return str(entry.get("file") or "")
    raise HTTPException(status_code=503, detail={
        "error_string": "multi_reference_model_missing",
        "message_string": "No FLUX.2 klein checkpoint is installed on the image boxes"})


# Renderfin runs on the same host and owns the image farm; the public service
# is a thin typed face over it so /image looks like every other service.
RENDERFIN_BASE = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")


@router.get("/api/image")
async def api_image_docs():
    """GET mirror documenting the POST image endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/image",
        "required_fields_array": ["prompt"],
        "optional_fields_array": ["image_url", "image_base64", "wait_seconds",
                                  "mode", "negative_prompt", "width", "height",
                                  "steps", "cfg", "sampler", "scheduler",
                                  "creativity", "seed", "checkpoint", "lora",
                                  "lora_strength", "loras", "clip_skip",
                                  "control_pose", "control_depth",
                                  "control_canny", "reference_image_urls"],
        "multi_reference_object": {
            "max_images_int": 3,
            "families_array": sorted(MULTIREF_FAMILIES),
            "edit_model_string": "Qwen-Image 2.1 turbo (since 2026-09-26 every picture edit and multi-picture edit is redirected to POST /api/qwen-image)",
            "note_string": ("image_url is image 1, reference_image_urls follow in order; "
                            "refer to them as image 1, image 2... in the prompt. "
                            "A video URL stands for its first frame"),
        },
        "produces_string": "image",
        "example_request_object": {"prompt": "a black lamp post on magenta", "wait_seconds": 120},
        "server_time_unix_int": int(time.time()),
    }


def _retired_edit_reason(body: "ImageRequest") -> str:
    """Why this /api/image request is an edit that now belongs to Qwen 2.1, or "".

    One edit model on the farm (owner, 2026-09-26): instruction edits and
    multi-picture edits run on Qwen-Image 2.1 turbo only. FLUX.2 klein stays a
    text-to-image model here; its edit and multi-reference templates stay on
    the boxes for rollback and for the avatar pipeline, which names itself.
    """
    if str(body.internal_pipeline or "").strip():
        return ""
    if body.reference_image_urls and any(str(item or "").strip() for item in body.reference_image_urls):
        return "several pictures"
    has_picture = bool(str(body.image_url or "").strip() or str(body.image_base64 or "").strip())
    if not has_picture or body.mode or body.control_pose or body.control_depth or body.control_canny:
        return ""
    import ai_model_catalogue
    import ai_model_defaults
    selected = (ai_model_catalogue.known_file(str(body.checkpoint or ""), "checkpoint")
                or ai_model_catalogue.known_file(str(body.lora or ""), "lora"))
    if selected and ai_model_defaults.model_family(selected) in MULTIREF_FAMILIES:
        return "a FLUX.2 klein picture edit"
    return ""


async def _redirected_edit(body: "ImageRequest", reason: str) -> Dict[str, object]:
    import ai_qwen_image_api
    extras = [str(item or "").strip() for item in (body.reference_image_urls or [])
              if str(item or "").strip()]
    total = len(extras) + (1 if (body.image_url or body.image_base64) else 0)
    if total > ai_qwen_image_api.MAX_REFERENCE_IMAGES:
        raise HTTPException(status_code=400, detail={
            "error_string": "too_many_reference_images",
            "message_string": (f"Edits run on Qwen-Image 2.1 turbo, which takes at most "
                               f"{ai_qwen_image_api.MAX_REFERENCE_IMAGES} pictures; "
                               f"{total} were wired in"),
            "max_int": ai_qwen_image_api.MAX_REFERENCE_IMAGES})
    image_url = str(body.image_url or "").strip() or None
    if not image_url and not body.image_base64 and extras:
        image_url, extras = extras[0], extras[1:]
    request = ai_qwen_image_api.QwenImageRequest(
        # Empty text is fine: Qwen-Image applies its remix/variation default.
        prompt=str(body.prompt or "").strip(), image_url=image_url,
        image_base64=body.image_base64, mode="edit",
        width=body.width, height=body.height, seed=body.seed or None,
        wait_seconds=body.wait_seconds, reference_image_urls=extras or None)
    note = (f"/api/image: {reason} is retired since 2026-09-26; the edit ran on "
            "Qwen-Image 2.1 turbo (POST /api/qwen-image)")
    logger.warning("image edit deprecation: %s (checkpoint=%s)", note, body.checkpoint)
    answer = dict(await ai_qwen_image_api.api_qwen_image(request))
    answer["deprecation_string"] = note
    answer.setdefault("poll_url_string", answer.get("image_url_string"))
    answer["effective_params_object"] = {
        "checkpoint": answer.get("checkpoint_string"),
        "work_flow": ("qwen_image21_edit_multi.json" if extras else "qwen_image21_edit.json"),
        "service": "qwen_image",
        "main_size_width": answer.get("width_int"), "main_size_height": answer.get("height_int"),
        "prompt": request.prompt, "noise_seed": body.seed or 0,
    }
    return answer


@router.post("/api/image")
async def api_image(body: ImageRequest):
    import ai_request_cache
    retired_edit = _retired_edit_reason(body)
    if retired_edit:
        return await _redirected_edit(body, retired_edit)
    payload = body.model_dump(exclude_none=True)
    payload.pop("internal_pipeline", None)
    if "image" in ("vision", "text"):
        model = _model_entry(body.model)
        payload["model"] = model["id"]
        payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    import hashlib
    if "image" in ("vision", "text"):
        profile = _model_entry(body.model)
    else:
        profile = _render_model_profile(
            "image", body.checkpoint, body.lora, mode=str(body.mode or ""),
            has_control=bool(body.control_pose or body.control_depth or body.control_canny))
        profile = profile + _stack_profile("image", body.prompt, body.loras, body.lora)
    payload["profile_hash"] = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    return await ai_request_cache.run_cached("image", payload,
        lambda: _uncached_api_image(body), namespace="ai-exact-models-20260922-v4")


async def _uncached_api_image(body: ImageRequest):
    """Prompt (and optionally a reference picture) into a generated image."""
    if not str(body.prompt or "").strip():
        fallback = default_image_prompt(body)
        if not fallback:
            raise HTTPException(status_code=400, detail={
                "error_string": "prompt_required",
                "message_string": "Nothing to draw: connect a prompt (Text / Vision) or a picture"})
        body.prompt = fallback
    prompt = _validate_prompt(body.prompt)
    prompt, lora_stack, single_strength = _lora_stack_request(
        "image", prompt, body.loras, body.lora)
    prompt = _validate_prompt(prompt)
    stack_checkpoint = body.checkpoint
    if lora_stack and not body.checkpoint and not body.lora:
        stack_checkpoint = _stack_default_checkpoint("image", lora_stack)
    extra_references = _multiref_inputs(body)
    if extra_references and not stack_checkpoint and not body.lora:
        stack_checkpoint = _multiref_default_checkpoint()
    controls = [(name, str(value or "").strip()) for name, value in (
        ("pose", body.control_pose), ("depth", body.control_depth),
        ("canny", body.control_canny)) if str(value or "").strip()]
    if len(controls) > 1:
        raise HTTPException(status_code=400, detail={
            "error_string": "multiple_control_channels_unsupported",
            "message_string": "Choose one of control_pose, control_depth or control_canny"})
    async with httpx.AsyncClient() as client:
        import ai_multiref
        # A video wired into the picture socket stands for its first frame.
        reference = await ai_multiref.as_picture(str(body.image_url or "").strip(), client)
        if not reference and body.image_base64:
            reference = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64)
            )
        mode = str(body.mode or "").strip().lower()
        model_payload, trigger_prefix = _effective_model_settings(
            "image", stack_checkpoint, body.lora, {
                "steps": body.steps, "cfg": body.cfg,
                "sampler": body.sampler, "scheduler": body.scheduler,
                "lora_strength": (single_strength if single_strength is not None
                                  else body.lora_strength),
                "clip_skip": body.clip_skip,
            }, use_default=not (controls or mode), mode=mode,
            control_channel=controls[0][0] if controls else "")
        if lora_stack:
            _check_stack_family(lora_stack, str(model_payload.get("checkpoint") or ""))
            model_payload["loras"] = [item.as_payload() for item in lora_stack]
        control_workflow = ""
        if controls:
            import ai_model_catalogue
            import ai_model_defaults
            selected = (ai_model_catalogue.known_file(
                str(model_payload.get("checkpoint") or ""), "checkpoint")
                or ai_model_catalogue.known_file(
                    str(model_payload.get("lora") or ""), "lora"))
            try:
                control_workflow = ai_model_defaults.control_workflow(
                    ai_model_defaults.model_family(selected), controls[0][0])
            except ValueError as exc:
                raise HTTPException(status_code=400, detail={
                    "error_string": "control_model_incompatible",
                    "message_string": str(exc)}) from None
        if trigger_prefix:
            import ai_model_defaults
            prompt = ai_model_defaults.add_triggers(prompt, [{"triggers": [part.strip() for part in trigger_prefix.split(",")]}])
        payload: Dict[str, object] = {
            "prompt": prompt, "main_size_width": int(body.width or 960),
            "main_size_height": int(body.height or 540),
        }
        if reference:
            payload["image_url"] = reference
        if controls:
            channel, control_url = controls[0]
            payload["image_url"] = control_url
            payload["type"] = f"image_control_{channel}"
        if not controls and mode and mode in IMAGE_MODES:
            # Renderfin picks the template from `type`, not from a file name.
            payload["type"] = mode
        elif not controls and reference:
            payload["type"] = "image"
        if body.negative_prompt and str(body.negative_prompt).strip():
            payload["negative_prompt"] = str(body.negative_prompt).strip()[:MAX_PROMPT_CHARS]
        payload.update(model_payload)
        if reference and not controls and not mode:
            import ai_model_catalogue
            import ai_model_defaults
            selected = ai_model_catalogue.known_file(str(model_payload.get("checkpoint") or ""), "checkpoint")
            family = ai_model_defaults.model_family(selected)
            if family == "flux2":
                payload["work_flow"] = "gen_image_flux2_klein_edit.json"
            elif family in {"pony", "sdxl"}:
                payload["work_flow"] = "gen_image_sdxl_edit.json"
        if controls:
            channel = controls[0][0]
            payload["type"] = f"image_control_{channel}"
            payload["work_flow"] = control_workflow
            payload.update(control_strength=body.control_strength, control_start=body.control_start, control_end=body.control_end)
        if extra_references:
            import ai_model_catalogue
            import ai_model_defaults
            selected = (ai_model_catalogue.known_file(
                str(model_payload.get("checkpoint") or ""), "checkpoint")
                or ai_model_catalogue.known_file(str(model_payload.get("lora") or ""), "lora"))
            if ai_model_defaults.model_family(selected) not in MULTIREF_FAMILIES:
                name = (selected or {}).get("base") or model_payload.get("checkpoint") or "This model"
                raise HTTPException(status_code=400, detail={
                    "error_string": "model_takes_no_references",
                    "message_string": (f"{name} cannot take several pictures; choose FLUX.2 "
                                       "klein 4B, or use the Qwen-Image node (up to 3)")})
            pictures = [reference] if reference else []
            for item in extra_references:
                if item.startswith("data:"):
                    item = await _publish_inline_image(client, _decode_inline_image(item))
                else:
                    item = await ai_multiref.as_picture(item, client)
                pictures.append(item)
            payload.pop("image_url", None)
            payload["type"] = MULTIREF_TYPE
            payload["work_flow"] = MULTIREF_WORKFLOW
            payload["reference_image_urls"] = pictures
        if body.creativity is not None:
            payload["creativity"] = float(body.creativity)
        if body.seed:
            payload["noise_seed"] = int(body.seed)
        _lora_dispatch_gate(payload)
        try:
            response = await client.post(
                RENDERFIN_BASE + "/api-render", json=payload, timeout=SUBMIT_TIMEOUT_SECONDS
            )
        except Exception:
            logger.exception("Renderfin did not accept an image request")
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_unreachable",
                "message_string": "The image farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_rejected",
                "message_string": f"Image farm answered HTTP {response.status_code}"})
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        task_id = str(accepted.get("task_id") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "image_service_no_output",
                "message_string": "Image farm accepted the request without an output URL"})
        # Renderfin publishes the destination URL up front and fills it in when
        # the render lands, so readiness is the file appearing, not a status row.
        ready = False
        if body.wait_seconds and body.wait_seconds > 0:
            deadline = time.monotonic() + min(float(body.wait_seconds), MAX_WAIT_SECONDS)
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                try:
                    head = await client.head(output_url, timeout=15.0)
                    if head.status_code == 200:
                        ready = True
                        break
                except Exception:
                    continue
        return {
            "success_bool": True,
            "task_id_string": task_id,
            "status_string": "completed" if ready else "pending",
            "finished_bool": ready,
            "image_url_string": output_url,
            "poll_url_string": output_url,
            "effective_params_object": {k: payload[k] for k in (
                "main_size_width", "main_size_height", "steps", "cfg", "sampler",
                "scheduler", "clip_skip", "checkpoint", "lora", "lora_strength", "loras",
                "work_flow", "prompt", "noise_seed", "reference_image_urls")
                if k in payload},
            "server_time_unix_int": int(time.time()),
        }

class VideoRequest(BaseModel):
    control_video_url: Optional[str] = Field(None, description="Driving MP4 for whole-sequence motion guidance")
    control_channel: Optional[str] = Field(None, pattern="^(canny|pose|depth)$")
    control_strength: float = Field(0.8, ge=0, le=1)
    image_url: Optional[str] = Field(None, description="First frame, public URL")
    image_base64: Optional[str] = Field(None, description="First frame, inline")
    prompt: Optional[str] = Field(None, description="What should happen in the clip")
    frame_count: Optional[int] = Field(None, ge=8, le=400)
    # Giving a last frame turns the clip into a journey between two pictures;
    # passing the first frame again is how a loop is made. Renderfin prunes the
    # guide node from the workflow when this is absent, so the plain animation
    # is unchanged by its existence.
    image_url_end: Optional[str] = Field(None, description="Last frame, public URL")
    image_base64_end: Optional[str] = Field(None, description="Last frame, inline")
    quality: Optional[str] = Field(None, description="standard or hq")
    width: Optional[int] = Field(None, ge=256, le=2048)
    height: Optional[int] = Field(None, ge=256, le=2048)
    checkpoint: Optional[str] = Field(None, description="Model file from /api/ai/model-catalogue")
    lora: Optional[str] = Field(None, description="LoRA file from /api/ai/model-catalogue")
    lora_strength: Optional[float] = Field(None, ge=0, le=2)
    loras: Optional[LoraStackValue] = Field(
        None, description="LoRA stack: [{name, strength, strength_clip}] or '<lora:NAME:W> ...'")
    negative_prompt: Optional[str] = Field(None, description="What to avoid")
    steps: Optional[int] = Field(None, ge=1, le=100)
    cfg: Optional[float] = Field(None, ge=0, le=30)
    sampler: Optional[str] = None
    scheduler: Optional[str] = None
    creativity: Optional[float] = Field(None, ge=0, le=1)
    seed: Optional[int] = Field(None, ge=0, description="0 or absent randomises")


@router.get("/api/video")
async def api_video_docs():
    """GET mirror documenting the POST video endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/video",
        "required_fields_array": ["image_url or image_base64"],
        "optional_fields_array": ["prompt", "frame_count", "image_url_end",
                                  "image_base64_end", "quality", "negative_prompt",
                                  "steps", "creativity", "seed"],
        "produces_string": "video",
        "note_string": "A clip is animated from the frame you give it; expect minutes, not seconds.",
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/ai/video", include_in_schema=False)
@router.post("/api/video")
async def api_video(body: VideoRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
    if "video" in ("vision", "text"):
        model = _model_entry(body.model)
        payload["model"] = model["id"]
        payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    import hashlib
    if "video" in ("vision", "text"):
        profile = _model_entry(body.model)
    else:
        profile = _render_model_profile("video", body.checkpoint, body.lora)
        profile = profile + _stack_profile("video", body.prompt, body.loras, body.lora)
    payload["profile_hash"] = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    namespace = ("ai-video-control-latent-crop-20260922-v1"
                 if body.control_video_url else "ai-video-exact-models-20260922-v5")
    return await ai_request_cache.run_cached("video", payload,
        lambda: _uncached_api_video(body), namespace=namespace)


async def _uncached_api_video(body: VideoRequest):
    """Animate a frame into a short clip on the render farm.

    Renderfin treats a request with an image and no `type` as an animation, so
    the frame is what selects the workflow; the caller never names one.
    """
    if bool(body.control_video_url) != bool(body.control_channel):
        raise HTTPException(400, detail="Choose both a driving video and its control channel")
    if body.control_video_url:
        import ai_services
        if (ai_services.service('video_control') or {}).get('status') != 'live':
            raise HTTPException(503, detail="Video motion transfer is still completing its render checks")
        from renderfin.video_input import validate_video_url, VideoInputError
        try:
            validate_video_url(body.control_video_url)
        except (ValueError, VideoInputError) as error:
            raise HTTPException(400, detail=str(error)) from None
        if body.lora:
            raise HTTPException(400, detail="Video control uses its dedicated Union adapter; remove the style LoRA")
    if not body.image_url and not body.image_base64 and (body.image_url_end or body.image_base64_end):
        # One picture wired to the Last-frame socket only: animate from it.
        body.image_url, body.image_base64 = body.image_url_end, body.image_base64_end
        body.image_url_end = body.image_base64_end = None
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "A video is animated from a frame; provide image_url or image_base64"})
    async with httpx.AsyncClient() as client:
        frame = str(body.image_url or "").strip()
        if not frame:
            frame = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
        video_prompt, lora_stack, single_strength = _lora_stack_request(
            "video", body.prompt, body.loras, body.lora)
        video_checkpoint = body.checkpoint
        if lora_stack and not body.checkpoint and not body.lora:
            video_checkpoint = _stack_default_checkpoint("video", lora_stack)
        model_payload, trigger_prefix = _effective_model_settings(
            "video", video_checkpoint, body.lora, {
                "steps": body.steps, "cfg": body.cfg,
                "sampler": body.sampler, "scheduler": body.scheduler,
                "lora_strength": (single_strength if single_strength is not None
                                  else body.lora_strength),
            })
        if lora_stack:
            _check_stack_family(lora_stack, str(model_payload.get("checkpoint") or ""))
            model_payload["loras"] = [item.as_payload() for item in lora_stack]
        payload: Dict[str, object] = {
            "image_url": frame, "main_size_width": int(body.width or 960),
            "main_size_height": int(body.height or 540),
            "frame_count": int(body.frame_count or 97),
        }
        last_frame = str(body.image_url_end or "").strip()
        if not last_frame and body.image_base64_end:
            last_frame = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64_end)
            )
        if last_frame:
            payload["image_url_end"] = last_frame
        if not (video_prompt and str(video_prompt).strip()):
            video_prompt = DEFAULT_TRANSITION_PROMPT if last_frame else DEFAULT_ANIMATE_PROMPT
        if video_prompt and str(video_prompt).strip():
            rendered_prompt = _validate_prompt(video_prompt)
            import ai_model_defaults
            payload["prompt"] = ai_model_defaults.add_triggers(rendered_prompt, [{"triggers": [part.strip() for part in trigger_prefix.split(",")]}])
        if body.frame_count:
            payload["frame_count"] = int(body.frame_count)
        payload.update(model_payload)
        quality = str(body.quality or "").strip().lower()
        import ai_model_catalogue
        import ai_model_defaults
        selected_video_model = (
            ai_model_catalogue.known_file(str(payload.get("checkpoint") or ""), "checkpoint")
            or ai_model_catalogue.known_file(str(payload.get("lora") or ""), "lora"))
        video_family = ai_model_defaults.model_family(selected_video_model)
        payload["frame_count"] = clamp_video_frames(
            selected_video_model, int(payload.get("frame_count") or 97))
        quality_workflow = _video_quality_workflow(
            quality, video_family, str(payload.get("work_flow") or ""))
        if quality_workflow:
            payload["work_flow"] = quality_workflow
        if body.negative_prompt and str(body.negative_prompt).strip():
            payload["negative_prompt"] = str(body.negative_prompt).strip()[:MAX_PROMPT_CHARS]
        if body.control_video_url:
            if not any(name in str(payload.get('checkpoint', '')) for name in VIDEO_CONTROL_CHECKPOINTS):
                raise HTTPException(400, detail="Video control requires the LTX-2.5 distilled model")
            # The control templates are LTX-2.5 graphs carrying the 2.3
            # Union-Control IC-LoRA; a saved node that still names the 2.3
            # transformer is rendered on the model those graphs were built for.
            payload['checkpoint'] = VIDEO_CONTROL_CHECKPOINT
            payload['control_video_url'] = body.control_video_url
            payload['control_strength'] = body.control_strength
            payload['work_flow'] = {
                'canny': 'gen_video_ltx23_control_by_url.json',
                'pose': 'gen_video_ltx23_pose_by_url.json',
                'depth': 'gen_video_ltx23_depth_by_url.json',
            }[body.control_channel]
        if body.creativity is not None:
            payload["creativity"] = float(body.creativity)
        if body.seed:
            payload["noise_seed"] = int(body.seed)
        _lora_dispatch_gate(payload)
        try:
            response = await client.post(
                RENDERFIN_BASE + "/api-render", json=payload,
                timeout=SUBMIT_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.exception("Renderfin did not accept a video request")
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_unreachable",
                "message_string": "The render farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_rejected",
                "message_string": f"Render farm answered HTTP {response.status_code}"})
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "").strip()
        if not output_url:
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_no_output",
                "message_string": "Render farm accepted the request without an output URL"})
        return {
            "success_bool": True,
            "task_id_string": str(accepted.get("task_id") or ""),
            "status_string": "pending",
            "finished_bool": False,
            "video_url_string": output_url,
            "poll_url_string": output_url,
            "source_image_url_string": frame,
            "end_image_url_string": last_frame,
            "effective_params_object": {k: payload[k] for k in (
                "main_size_width", "main_size_height", "steps", "cfg", "sampler",
                "scheduler", "clip_skip", "checkpoint", "lora", "lora_strength", "loras",
                "work_flow", "prompt", "noise_seed")
                if k in payload},
            "server_time_unix_int": int(time.time()),
        }

class ModelRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Picture of the subject")
    image_base64: Optional[str] = Field(None, description="Picture, inline")
    quality: Optional[str] = Field(None, description="draft, standard or high")
    background_method: Optional[str] = Field(None, description="auto, alpha or solid")


@router.get("/api/3dmodel")
async def api_3dmodel_docs():
    """GET mirror documenting the POST 3D endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/3dmodel",
        "required_fields_array": ["image_url or image_base64"],
        "optional_fields_array": ["quality", "background_method"],
        "produces_string": "model3d",
        "note_string": "Hunyuan3D on the farm; minutes, and only nodes cleared for 3D take it.",
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/3dmodel")
async def api_3dmodel(body: ModelRequest):
    """Turn a picture into a 3D model on a farm node cleared for Hunyuan."""
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64"})
    async with httpx.AsyncClient() as client:
        picture = str(body.image_url or "").strip()
        if not picture:
            picture = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
        workers = _load_hunyuan_workers()
        if not workers:
            raise HTTPException(status_code=503, detail={
                "error_string": "no_3d_node_available",
                "message_string": "No farm node is currently cleared for 3D generation"})
        probes = await asyncio.gather(
            *(_node_is_free(client, worker) for worker in workers),
            return_exceptions=True,
        )
        candidates = []
        for worker, probe in zip(workers, probes):
            if isinstance(probe, Exception) or not isinstance(probe, tuple):
                continue
            ok, info = probe
            if ok:
                candidates.append((int((info or {}).get("load") or 0), worker))
        if not candidates:
            raise HTTPException(status_code=503, detail={
                "error_string": "no_3d_node_available",
                "message_string": "No 3D-capable node answered; try again shortly"})
        candidates.sort(key=lambda item: item[0])
        worker = candidates[0][1]

        payload: Dict[str, object] = {"image_url": picture}
        if body.quality:
            payload["quality"] = str(body.quality).strip().lower()
        if body.background_method:
            payload["background_method"] = str(body.background_method).strip().lower()
        worker_task_id = await _submit(client, worker, "/generate-3d", payload)
        return {
            "success_bool": True,
            "task_id_string": f"{_node_key(worker)}.{worker_task_id}",
            "status_string": "pending",
            "finished_bool": False,
            "status_url_string": f"/api/3dmodel/status/{_node_key(worker)}.{worker_task_id}",
            "node_string": _node_key(worker),
            "source_image_url_string": picture,
            "server_time_unix_int": int(time.time()),
        }


@router.get("/api/3dmodel/status/{task_id}")
async def api_3dmodel_status(task_id: str):
    """Hunyuan has its own status path, so 3D tasks cannot share /api/ai/status."""
    node_key, _, worker_task_id = str(task_id).partition(".")
    if not node_key or not worker_task_id:
        raise HTTPException(status_code=400, detail={
            "error_string": "malformed_task_id",
            "message_string": "task_id must look like <node>.<id>"})
    worker = next((w for w in _load_hunyuan_workers()
                   if _node_key(w) == node_key), None)
    if worker is None:
        raise HTTPException(status_code=404, detail={
            "error_string": "unknown_node",
            "message_string": "The node that owns this task is not configured for 3D"})
    url = _ai_base(worker) + f"/generate-3d/status/{worker_task_id}"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                url, headers={"Authorization": f"Bearer {worker['token']}"},
                timeout=STATUS_TIMEOUT_SECONDS,
            )
        except Exception:
            raise HTTPException(status_code=502, detail={
                "error_string": "worker_unreachable",
                "message_string": "The farm node did not answer"}) from None
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={
            "error_string": "task_not_found", "message_string": "No such task"})
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail={
            "error_string": "worker_status_failed",
            "message_string": f"Farm node answered HTTP {response.status_code}"})
    raw = response.json() or {}
    status = str(raw.get("status") or "")
    outputs = raw.get("output_urls")
    model_url = ""
    preview_url = ""
    if isinstance(outputs, dict):
        for key in ("glb", "high", "high_textured", "model", "standard"):
            value = outputs.get(key)
            if isinstance(value, str) and value.strip():
                model_url = value.strip()
                break
        for key in ("preview", "preview_front", "thumbnail"):
            value = outputs.get(key)
            if isinstance(value, str) and value.strip():
                preview_url = value.strip()
                break
    if status == "Completed":
        record_duration("3dmodel", float(raw.get("elapsed_seconds") or 0))
    return {
        "success_bool": status != "Failed",
        "task_id_string": task_id,
        "status_string": status.lower() or "pending",
        "finished_bool": status in ("Completed", "Failed"),
        "model_url_string": model_url,
        "preview_url_string": preview_url,
        "outputs_object": outputs if isinstance(outputs, dict) else {},
        "error_string": str(raw.get("error") or ""),
        "stage_string": str(raw.get("current_stage") or ""),
        "progress_int": int(raw.get("progress") or 0),
        "elapsed_seconds_float": round(float(raw.get("elapsed_seconds") or 0.0), 2),
        "node_string": node_key,
        "server_time_unix_int": int(time.time()),
    }
