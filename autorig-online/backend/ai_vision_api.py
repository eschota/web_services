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
from typing import Dict, List, Optional, Tuple

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
            "created_at_unix_float": row.get("created_at") or 0}
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
        "graph_agent_supported": False,
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
    model: Optional[str] = Field(None, description="Model id from /api/ai/models")
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
) -> Dict[str, object]:
    """The least loaded reachable node that carries the requested model."""
    workers = _load_ai_workers()
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
        # Older nodes publish no catalogue at all; treat that as "unknown, try it"
        # rather than refusing work a node may well be able to do.
        silent = [(w, i) for w, i in reachable if not (i.get("models") or [])]
        if not silent:
            raise HTTPException(status_code=503, detail={
                "error_string": "model_not_on_any_node",
                "message_string": f"No reachable node carries '{wanted}'",
                "nodes_checked_int": len(reachable)})
        carrying = silent
    # Load first, warmth only as a tie-break: swapping weights costs seconds,
    # but queueing behind a conversion costs however long that conversion runs.
    carrying.sort(key=lambda item: (
        int(item[1].get("load") or 0),
        0 if wanted and item[1].get("loaded") == wanted else 1,
    ))
    return carrying[0][0]


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
    if service_id == "image" and mode == "inpaint":
        raise HTTPException(status_code=400, detail={
            "error_string": "unsupported_image_mode",
            "message_string": (
                "Inpaint needs the FLUX.1 Fill Dev checkpoint, which is not "
                "installed or validated; choose Plain or another available mode")})

    entries = ai_model_catalogue.entries()
    required_default_family = ""
    if service_id == "image" and mode in LEGACY_FLUX_IMAGE_MODES:
        required_default_family = "flux"
    elif service_id == "image" and control_channel and not checkpoint and not lora:
        required_default_family = "pony"

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
            entries, {"family": "flux"}, "image")
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
    if mode in LEGACY_FLUX_IMAGE_MODES and selected_family != "flux":
        raise HTTPException(status_code=400, detail={
            "error_string": "mode_model_incompatible",
            "message_string": f"{mode} requires a validated FLUX.1 checkpoint"})
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
    if mode in LEGACY_FLUX_IMAGE_MODES:
        # These audited templates contain more than one sampling stage (the
        # T-pose refiner intentionally differs from its Schnell base stage).
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
    if str(mode or "").strip().lower() in LEGACY_FLUX_IMAGE_MODES:
        families.add("flux")
    elif has_control and not checkpoint and not lora:
        families.add("pony")
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
            reason = str(body.get("error") or body.get("message")
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
                   service_id: str = "") -> Dict[str, object]:
    status = str(raw.get("status") or "")
    finished = status in ("Completed", "Failed")
    if status == "Completed" and service_id:
        record_duration(service_id, float(raw.get("elapsed_seconds") or 0))
    return {
        "success_bool": status != "Failed",
        "task_id_string": task_id,
        "status_string": status.lower() or "pending",
        "finished_bool": finished,
        "mode_string": str(raw.get("mode") or ""),
        "model_string": model_id,
        "answer_string": str(raw.get("answer") or ""),
        "reasoning_string": str(raw.get("reasoning") or ""),
        "error_string": str(raw.get("error") or ""),
        "elapsed_seconds_float": round(float(raw.get("elapsed_seconds") or 0.0), 2),
        "stage_string": str(raw.get("current_stage") or ""),
        "server_time_unix_int": int(time.time()),
    }


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
        worker = await _pick_worker(client, model_id, **requirements)
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
        result = _public_status(task_id, model_id, raw, service_id)
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
        "required_fields_array": ["prompt", "image_url or image_base64"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds"],
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
    return await ai_request_cache.run_cached("vision", payload,
        lambda: _uncached_api_vision(request, body), namespace="ai-workflows-20260922-v1")


async def _uncached_api_vision(request: Request, body: VisionRequest):
    model = _model_entry(body.model)
    prompt = _validate_prompt(body.prompt)
    if not body.image_url and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Provide image_url or image_base64"})
    image_url = str(body.image_url or "").strip()
    if not image_url:
        async with httpx.AsyncClient() as client:
            image_url = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64 or "")
            )
    payload: Dict[str, object] = {"prompt": prompt, "image_url": image_url}
    payload["max_output_tokens"] = _output_budget(model, body.max_output_tokens)
    return await _run(model, "/ai-vision", payload, body.wait_seconds, "vision")


@router.get("/api/text2text")
async def api_text2text_docs():
    """GET mirror documenting the POST text endpoint."""
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/text2text",
        "required_fields_array": ["prompt"],
        "optional_fields_array": ["model", "max_output_tokens", "wait_seconds"],
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
        lambda: _uncached_api_text2text(request, body), namespace="ai-workflows-20260922-v1")


async def _uncached_api_text2text(request: Request, body: TextRequest):
    model = _model_entry(body.model)
    payload: Dict[str, object] = {"prompt": _validate_prompt(body.combined_prompt())}
    system_prompt = str(body.system_prompt or "").strip()
    if system_prompt:
        # Keep the same total request bound, but never concatenate the system
        # instructions into user data on their way to the inference worker.
        _validate_prompt(system_prompt + "\n" + str(payload["prompt"]))
        payload["system_prompt"] = system_prompt
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
    result = _public_status(task_id, DEFAULT_MODEL_ID, raw,
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
LEGACY_FLUX_IMAGE_MODES = {"z_depth", "t_pose", "open_pose"}


def _video_quality_workflow(quality: str, family: str) -> str:
    quality = str(quality or "").strip().lower()
    family = str(family or "").strip().lower()
    if quality == "hq" and family == "ltx23":
        raise HTTPException(status_code=400, detail={
            "error_string": "unsupported_video_quality",
            "message_string": (
                "High quality is not a separate validated workflow for modern "
                "LTX 2.3; use Standard, which keeps the selected model's workflow")})
    return str(VIDEO_QUALITIES.get(quality) or "") if family != "ltx23" else ""


class ImageRequest(BaseModel):
    prompt: str = Field(..., description="What to draw")
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
    control_pose: Optional[str] = Field(None, description="Precomputed pose control-map URL")
    control_depth: Optional[str] = Field(None, description="Precomputed depth control-map URL")
    control_canny: Optional[str] = Field(None, description="Precomputed canny control-map URL")
    control_strength: float = Field(0.8, ge=0, le=2)
    control_start: float = Field(0.0, ge=0, le=1)
    control_end: float = Field(1.0, ge=0, le=1)


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
                                  "lora_strength", "control_pose", "control_depth",
                                  "control_canny"],
        "produces_string": "image",
        "example_request_object": {"prompt": "a black lamp post on magenta", "wait_seconds": 120},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/image")
async def api_image(body: ImageRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
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
    payload["profile_hash"] = hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()
    return await ai_request_cache.run_cached("image", payload,
        lambda: _uncached_api_image(body), namespace="ai-exact-models-20260922-v4")


async def _uncached_api_image(body: ImageRequest):
    """Prompt (and optionally a reference picture) into a generated image."""
    prompt = _validate_prompt(body.prompt)
    controls = [(name, str(value or "").strip()) for name, value in (
        ("pose", body.control_pose), ("depth", body.control_depth),
        ("canny", body.control_canny)) if str(value or "").strip()]
    if len(controls) > 1:
        raise HTTPException(status_code=400, detail={
            "error_string": "multiple_control_channels_unsupported",
            "message_string": "Choose one of control_pose, control_depth or control_canny"})
    async with httpx.AsyncClient() as client:
        reference = str(body.image_url or "").strip()
        if not reference and body.image_base64:
            reference = await _publish_inline_image(
                client, _decode_inline_image(body.image_base64)
            )
        mode = str(body.mode or "").strip().lower()
        model_payload, trigger_prefix = _effective_model_settings(
            "image", body.checkpoint, body.lora, {
                "steps": body.steps, "cfg": body.cfg,
                "sampler": body.sampler, "scheduler": body.scheduler,
                "lora_strength": body.lora_strength,
            }, use_default=not (controls or mode), mode=mode,
            control_channel=controls[0][0] if controls else "")
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
        if body.creativity is not None:
            payload["creativity"] = float(body.creativity)
        if body.seed:
            payload["noise_seed"] = int(body.seed)
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
                "scheduler", "clip_skip", "checkpoint", "lora", "lora_strength", "work_flow")
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
        model_payload, trigger_prefix = _effective_model_settings(
            "video", body.checkpoint, body.lora, {
                "steps": body.steps, "cfg": body.cfg,
                "sampler": body.sampler, "scheduler": body.scheduler,
                "lora_strength": body.lora_strength,
            })
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
        if body.prompt and str(body.prompt).strip():
            rendered_prompt = _validate_prompt(body.prompt)
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
        quality_workflow = _video_quality_workflow(quality, video_family)
        if quality_workflow:
            payload["work_flow"] = quality_workflow
        if body.negative_prompt and str(body.negative_prompt).strip():
            payload["negative_prompt"] = str(body.negative_prompt).strip()[:MAX_PROMPT_CHARS]
        if body.control_video_url:
            if 'ltx-2.3-22b-distilled-1.1' not in str(payload.get('checkpoint', '')):
                raise HTTPException(400, detail="Video control requires the LTX-2.3 distilled 1.1 model")
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
                "scheduler", "clip_skip", "checkpoint", "lora", "lora_strength", "work_flow")
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
