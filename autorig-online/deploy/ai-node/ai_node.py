#!/usr/bin/env python3
"""AutoRig AI node: the backend's Vision/Text LLM jobs on a render box, no converter.

The backend (autorig-online/backend/ai_vision_api.py) routes `/api/vision` and
`/api/text2text` to "AI nodes" listed in the renderfin workers file. Until now
only the GLB converter (webserver_converter_glb.py + bonsai_adapter.py, converter
main 60101f4) answered that contract, so a box that runs ComfyUI and no
converter could not take LLM work. This file answers the same contract on its
own, with nothing but the Python standard library.

One process serves exactly one pinned model through one llama-server child. The
box's GPU belongs to ComfyUI first:

* ComfyUI is polled every `comfy_poll_seconds` (3 s). While it has a prompt
  running or queued the node reports a high load (queue_size 99) so the backend
  prefers other nodes, refuses new work with 503, and stops an idle
  llama-server at once to hand the VRAM back.
* The model is loaded only when ComfyUI is idle and nvidia-smi reports at least
  the model's need plus a margin free; otherwise a submission gets 503.
* After the last task the model stays warm for `keepalive_seconds` (120 s).
* One task runs at a time; `max_tasks` (2) accepted-but-unfinished tasks at
  most, the rest get 503.

The request handling, llama-server launch line, chat request and answer/
reasoning split are copied from the converter's bonsai_adapter.py and
webserver_converter_glb.py at main 60101f4; each copied piece names its source.

Run:  python ai_node.py --config config.json
      python ai_node.py --config config.json --check
"""

from __future__ import annotations

import argparse
import base64
import hmac
import ipaddress
import json
import logging
import logging.handlers
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional, Tuple

VERSION = "ai-node-20261108-media-edit-retry"
API_PREFIX = "/api-converter-glb"
WORKLOAD_AI_VISION = "ai_vision"

# Same bounds as the converter (bonsai_adapter.py 60101f4, lines 32-35) and the
# backend (ai_vision_api.py MAX_PROMPT_CHARS).
DEFAULT_START_TIMEOUT_SECONDS = 300
DEFAULT_REQUEST_TIMEOUT_SECONDS = 900
MAX_PROMPT_CHARS = 8000
MAX_IMAGE_BYTES = 16 * 1024 * 1024
# A base64 data: URI of a 16 MiB image plus the prompt fits comfortably.
MAX_BODY_BYTES = 24 * 1024 * 1024
# Reported as tasks_summary.queue_size while this node would refuse new work.
BUSY_QUEUE_SIZE = 99

CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
IS_WINDOWS = os.name == "nt"

logger = logging.getLogger("ai_node")

# Copied from bonsai_adapter.py DEFAULT_MODELS (60101f4, lines 127-150): the two
# models the farm serves. A config that names one of these ids inherits these
# values for any key it leaves out.
KNOWN_MODELS: Dict[str, Dict[str, object]] = {
    "bonsai2-27b": {
        "title": "Bonsai 2 27B",
        "weights": "bonsai2-PQ2_0.gguf",
        "mmproj": "mmproj-Q8_0.gguf",
        "context_tokens": 4096,
        "uncensored": False,
        "max_output_tokens": 2048,
        "reasoning": "auto",
    },
    "qwen35-9b-uncensored": {
        "title": "Qwen3.5 9B Defiant Fable (uncensored)",
        "weights": "qwen35-9b-uncensored-Q4_K_M.gguf",
        "mmproj": "qwen35-9b-uncensored-mmproj-BF16.gguf",
        "context_tokens": 8192,
        "uncensored": True,
        "max_output_tokens": 4096,
        # Converter note: with thinking on it burned the whole budget on
        # reasoning and never answered; with it off it answers in seconds.
        "reasoning": "off",
    },
    # 2026-11-06: the A/B candidate for M+ keywording (owner-approved, installed by the owner):
    # unsloth/Qwen3.5-4B-GGUF Qwen3.5-4B-Q4_K_M.gguf (2.74 GB) + mmproj-F16.gguf (0.67 GB), kept in their own
    # folder under models_dir because the projector's file name is generic.
    "qwen35-4b": {
        "title": "Qwen3.5 4B",
        "weights": os.path.join("qwen35-4b", "Qwen3.5-4B-Q4_K_M.gguf"),
        "mmproj": os.path.join("qwen35-4b", "mmproj-F16.gguf"),
        "context_tokens": 4096,
        "uncensored": False,
        "max_output_tokens": 2048,
        "reasoning": "off",
    },
}

# Keys one model profile may set; everything else in the config is node-wide.
MODEL_KEYS = ("model_id", "title", "weights", "mmproj", "context_tokens", "max_output_tokens", "reasoning",
              "uncensored", "system_prompt_supported", "vram_need_mb", "extra_llama_args")

# The converter hardcodes system_prompt_models=["bonsai2-27b"] (bonsai_adapter
# 60101f4, line 283): only that model passed the system-role canary.
SYSTEM_ROLE_VERIFIED_MODELS = ("bonsai2-27b",)

DEFAULT_CONFIG: Dict[str, object] = {
    "node_name": "",
    "listen_host": "127.0.0.1",
    "listen_port": 5480,
    "token_file": "token.txt",
    "model_id": "",
    "title": None,
    "weights": None,
    "mmproj": None,
    "models_dir": None,
    "context_tokens": None,
    "max_output_tokens": None,
    "reasoning": None,
    "uncensored": None,
    "system_prompt_supported": None,
    "vram_need_mb": None,
    "vram_overhead_mb": 1536,
    "vram_margin_mb": 1536,
    "ngl": 99,
    "llama_server": "",
    "llama_port": 8092,
    "extra_llama_args": [],
    # More than one model on one node (2026-11-06, the 4B A/B). Each entry is a model profile: model_id plus any of
    # MODEL_KEYS (weights, mmproj, context_tokens, reasoning, vram_need_mb, its own extra_llama_args appended after
    # the node-wide ones, ...); a known model_id fills in the rest. One model is loaded at a time: a task for another
    # model waits until the running one finishes, then the node swaps the weights (stop + load, ~10-30 s). The
    # top-level model_id (or, without one, the first entry) is the primary model. Empty = one pinned model, as before.
    "models": [],
    "llama_log_file": "",
    "cuda_visible_devices": "",
    "nvidia_smi": "nvidia-smi",
    "gpu_index": 0,
    "comfy_url": "http://127.0.0.1:8188",
    "comfy_poll_seconds": 3.0,
    "comfy_unreachable_is_busy": False,
    # A render box whose ComfyUI keeps its models in VRAM after a job: when ComfyUI is idle and the VRAM is short,
    # ask it to free its cache (POST /free) instead of refusing the job (2026-10-07). False keeps the old policy.
    "free_comfy_when_idle": False,
    # A box whose converter runs Hunyuan / conversions (f7): its local server-status counts as a second GPU owner
    # (any processing or queued converter task = busy, like a ComfyUI prompt). Empty = not watched.
    "converter_status_url": "",
    "converter_token_file": "",
    # Two nodes on one card (worker-4090, 2026-10-11: the 9B pool node and the film director's 27B): the node that
    # needs the whole card writes `gpu_lease_hold_file` while it waits for, loads or holds its model; a node that
    # lists that file in `gpu_lease_yield_files` treats a fresh lease like a busy ComfyUI (refuses new work, unloads
    # when idle, a running answer finishes). Empty = no lease, the old behaviour.
    "gpu_lease_hold_file": "",
    "gpu_lease_yield_files": [],
    "gpu_lease_fresh_seconds": 60,
    # worker-4090's GPU gate (C:\AI\HY3D2\fleet-adapter\gate.py, control 127.0.0.1:18778): renderfin reaches ComfyUI
    # through it, and while someone holds a gate lease renderfin sends no NEW render to the card. The holder above
    # also takes a gate lease under `gpu_gate_owner`, so the card can drain and the model load. Empty = not used.
    "gpu_gate_url": "",
    "gpu_gate_owner": "",
    # The gate lease lasts while tasks wait or run, plus this grace after the last one (a caller's next call of the same
    # batch finds the model warm); a model idling in keepalive does not keep renders off the card.
    "gpu_gate_grace_seconds": 30,
    # The owner's games (worker-4090, 2026-10-11: cs2 held 4.3 GB while the 27B waited): while one runs the node loads
    # nothing, refuses new work, unloads an idle model and holds no gate. Executable names, plus a file with one per line
    # (the 3D adapter's games.txt). Empty = not watched.
    "owner_games": [],
    "owner_games_file": "",
    # A model that does not fit whole (owner 2026-10-11: «он должен брать мозг пока идёт игра» - cs2 and the desktop
    # leave the 27B at 128k some GB short): llama.cpp's --fit puts as many layers on the GPU as fit, leaving
    # fit_target_mb free, and the rest in RAM - slower, the same context. Only when at least fit_min_vram_mb (+ the
    # margin) are free once ComfyUI is put away. False = wait for the whole need.
    "fit_when_short": False,
    "fit_min_vram_mb": 0,
    "fit_target_mb": 1536,
    # The converter/3D adapter answers 503 when it refuses NEW 3D jobs - also because an LLM took the VRAM it needs to
    # start Hunyuan. False = a 503 is not «the GPU is in use» (the VRAM check still guards the load).
    "converter_503_is_busy": True,
    # The owner's priority (2026-10-11: «приоритет с отменой других задач именно для задач с режиссёром или другими
    # личностями на этой ноде»): a request with "priority": "owner" is admitted while ComfyUI renders and goes first;
    # owner_preempt_comfy deletes ComfyUI's queued prompts and interrupts the running one (renderfin requeues each on
    # another box, same task id); for owner_warm_seconds after an owner request the gate and the model stay held.
    "owner_priority": False,
    "owner_preempt_comfy": False,
    "owner_warm_seconds": 600,
    # Characters of system_prompt + prompt one request may carry. The backend's own limit is 8000; a node that only a
    # long-context caller talks to directly (the film director) raises it with its context_tokens.
    "max_prompt_chars": MAX_PROMPT_CHARS,
    "keepalive_seconds": 120,
    "max_tasks": 2,
    # Tasks generating at the same time (2026-11-06, throughput for big M+ batches). llama-server needs as many
    # slots: e.g. ["-np", "2", "-kvu"] in extra_llama_args (-kvu keeps one shared KV of context_tokens, so one long
    # request still gets the whole window). Loads and model swaps stay one at a time. 1 = one task at a time.
    "parallel_tasks": 1,
    "comfy_wait_seconds": 300,
    "start_timeout_seconds": DEFAULT_START_TIMEOUT_SECONDS,
    "request_timeout_seconds": DEFAULT_REQUEST_TIMEOUT_SECONDS,
    "task_retention_seconds": 6 * 3600,
    "allow_private_image_urls": False,
    # Media edit (2026-11-07, M+ similars on Renderfin): ffmpeg jobs next to the model, CPU + NVENC only (no model
    # lock, no VRAM check). Offered only after the startup selftest (2 s synthetic clip) passed.
    "media_edit_enabled": True,
    "media_edit_parallel": 2,
    "media_edit_max_tasks": 16,
    "ffmpeg_exe": "",                     # empty = %LOCALAPPDATA%\Freestock\tools\bin\ffmpeg.exe, then PATH
    "ffprobe_exe": "",                    # empty = next to ffmpeg, then PATH, else `ffmpeg -i` is parsed
    "media_edit_work_dir": "media-edit-work",
    "media_edit_timeout_seconds": 2 * 3600,
    "media_edit_max_source_bytes": 4 * 1024 ** 3,
    "media_edit_output_keep_seconds": 6 * 3600,
    "media_edit_nvenc": True,
    "media_edit_selftest_on_start": True,
    "media_edit_allow_private_urls": False,   # test only
    "log_file": "ai-node.log",
    "log_level": "INFO",
}


class NodeError(RuntimeError):
    """Fails one task without taking the node down (converter: BonsaiError)."""


class RequestError(ValueError):
    """Caller payload is invalid; rejected before it queues (BonsaiRequestError)."""


class ComfyBusy(NodeError):
    """ComfyUI took the GPU while the model was loading."""


# --------------------------------------------------------------------- config
class Config:
    """Effective settings: DEFAULT_CONFIG, the model's known values, the file."""

    def __init__(self, raw: Dict[str, object], base_dir: str):
        unknown = sorted(set(raw) - set(DEFAULT_CONFIG) - {"_comment"})
        self.unknown_keys = unknown
        merged = dict(DEFAULT_CONFIG)
        merged.update({k: v for k, v in raw.items() if k in DEFAULT_CONFIG})
        self.base_dir = base_dir
        self.node_name = str(merged["node_name"] or socket.gethostname()).strip()
        self.listen_host = str(merged["listen_host"] or "127.0.0.1")
        self.listen_port = int(merged["listen_port"])
        self.token_file = self._path(merged["token_file"]) if merged["token_file"] else ""
        self.models_dir = self._path(merged["models_dir"]) if merged.get("models_dir") else (
            os.path.join(base_dir, "models"))
        # Node-wide llama-server flags; a profile's own extra_llama_args come after them.
        self.node_extra_llama_args = [str(a) for a in (merged.get("extra_llama_args") or [])]
        self.profiles: Dict[str, Dict[str, object]] = {}
        entries: List[Dict[str, object]] = []
        if str(merged.get("model_id") or "").strip():
            entries.append({k: merged.get(k) for k in MODEL_KEYS if k != "extra_llama_args"})
        extra_models = merged.get("models") or []
        if not isinstance(extra_models, list) or not all(isinstance(e, dict) for e in extra_models):
            raise ValueError("config: models must be a list of objects")
        entries.extend(extra_models)
        if not entries:
            raise ValueError("config: model_id is required")
        for entry in entries:
            profile = self._resolve_profile(entry)
            key = str(profile["model_id"]).lower()
            if key in self.profiles:
                raise ValueError(f"config: model {profile['model_id']} is listed twice")
            self.profiles[key] = profile
        self.primary_model_id = str(next(iter(self.profiles.values()))["model_id"])
        self.select_model(self.primary_model_id)
        self.vram_overhead_mb = int(merged["vram_overhead_mb"])
        self.vram_margin_mb = int(merged["vram_margin_mb"])
        self.ngl = int(merged["ngl"])
        self.llama_server = self._argv(merged["llama_server"])
        if not self.llama_server:
            raise ValueError("config: llama_server is required")
        self.llama_port = int(merged["llama_port"])
        self.llama_log_file = (self._path(merged["llama_log_file"])
                               if merged.get("llama_log_file") else "")
        self.cuda_visible_devices = str(merged.get("cuda_visible_devices") or "").strip()
        self.nvidia_smi = self._argv(merged["nvidia_smi"], resolve=False)
        self.gpu_index = int(merged["gpu_index"])
        self.comfy_url = str(merged.get("comfy_url") or "").strip().rstrip("/")
        self.comfy_poll_seconds = max(0.2, float(merged["comfy_poll_seconds"]))
        self.comfy_unreachable_is_busy = bool(merged["comfy_unreachable_is_busy"])
        self.free_comfy_when_idle = bool(merged.get("free_comfy_when_idle"))
        self.converter_status_url = str(merged.get("converter_status_url") or "").strip()
        self.converter_token_file = (self._path(merged["converter_token_file"])
                                     if merged.get("converter_token_file") else "")
        self.gpu_lease_hold_file = (self._path(merged["gpu_lease_hold_file"])
                                    if merged.get("gpu_lease_hold_file") else "")
        self.gpu_lease_yield_files = [self._path(p) for p in (merged.get("gpu_lease_yield_files") or []) if p]
        self.gpu_lease_fresh_seconds = max(10.0, float(merged.get("gpu_lease_fresh_seconds") or 60))
        self.gpu_gate_url = str(merged.get("gpu_gate_url") or "").strip().rstrip("/")
        self.gpu_gate_owner = str(merged.get("gpu_gate_owner") or "").strip() or self.node_name
        self.gpu_gate_grace_seconds = max(0.0, float(merged.get("gpu_gate_grace_seconds") or 0))
        self.owner_games = [str(g).strip().lower() for g in (merged.get("owner_games") or []) if str(g).strip()]
        self.owner_games_file = (self._path(merged["owner_games_file"]) if merged.get("owner_games_file") else "")
        self.fit_when_short = bool(merged.get("fit_when_short"))
        self.fit_min_vram_mb = max(0, int(merged.get("fit_min_vram_mb") or 0))
        self.fit_target_mb = max(256, int(merged.get("fit_target_mb") or 1536))
        self.converter_503_is_busy = bool(merged.get("converter_503_is_busy", True))
        self.owner_priority = bool(merged.get("owner_priority"))
        self.owner_preempt_comfy = bool(merged.get("owner_preempt_comfy"))
        self.owner_warm_seconds = max(0.0, float(merged.get("owner_warm_seconds") or 0))
        self.max_prompt_chars = max(1000, int(merged.get("max_prompt_chars") or MAX_PROMPT_CHARS))
        self.keepalive_seconds = int(merged["keepalive_seconds"])
        self.max_tasks = max(1, int(merged["max_tasks"]))
        self.parallel_tasks = max(1, min(8, int(merged.get("parallel_tasks") or 1)))
        self.comfy_wait_seconds = max(0.0, float(merged["comfy_wait_seconds"]))
        self.start_timeout_seconds = int(merged["start_timeout_seconds"])
        self.request_timeout_seconds = int(merged["request_timeout_seconds"])
        self.task_retention_seconds = int(merged["task_retention_seconds"])
        self.allow_private_image_urls = bool(merged["allow_private_image_urls"])
        self.media_edit_enabled = bool(merged.get("media_edit_enabled", True))
        self.media_edit_parallel = max(1, min(8, int(merged.get("media_edit_parallel") or 2)))
        self.media_edit_max_tasks = max(self.media_edit_parallel, int(merged.get("media_edit_max_tasks") or 16))
        self.ffmpeg_exe = self._path(merged["ffmpeg_exe"]) if merged.get("ffmpeg_exe") else ""
        self.ffprobe_exe = self._path(merged["ffprobe_exe"]) if merged.get("ffprobe_exe") else ""
        self.media_edit_work_dir = self._path(merged.get("media_edit_work_dir") or "media-edit-work")
        self.media_edit_timeout_seconds = max(60.0, float(merged.get("media_edit_timeout_seconds")
                                                          or MEDIA_EDIT_TIMEOUT_SECONDS))
        self.media_edit_max_source_bytes = max(1, int(merged.get("media_edit_max_source_bytes")
                                                      or MEDIA_EDIT_MAX_SOURCE_BYTES))
        self.media_edit_output_keep_seconds = max(60.0, float(merged.get("media_edit_output_keep_seconds")
                                                              or 6 * 3600))
        self.media_edit_nvenc = bool(merged.get("media_edit_nvenc", True))
        self.media_edit_selftest_on_start = bool(merged.get("media_edit_selftest_on_start", True))
        self.media_edit_allow_private_urls = bool(merged.get("media_edit_allow_private_urls"))
        self.log_file = self._path(merged["log_file"]) if merged.get("log_file") else ""
        self.log_level = str(merged.get("log_level") or "INFO").upper()

    def _resolve_profile(self, entry: Dict[str, object]) -> Dict[str, object]:
        """One model profile: the entry, then the known model's values for what it leaves out."""
        unknown = sorted(set(entry) - set(MODEL_KEYS) - {"_comment"})
        if unknown:
            raise ValueError(f"config: unknown model keys {unknown}")
        model_id = str(entry.get("model_id") or "").strip()
        if not model_id:
            raise ValueError("config: every model needs a model_id")
        merged = {k: entry.get(k) for k in MODEL_KEYS}
        for key, value in KNOWN_MODELS.get(model_id.lower(), {}).items():
            if merged.get(key) in (None, ""):
                merged[key] = value
        weights = str(merged.get("weights") or "").strip()
        if not weights:
            raise ValueError(f"config: weights is required for a model the node does not know ({model_id})")
        mmproj = str(merged.get("mmproj") or "").strip()
        sp = merged.get("system_prompt_supported")
        return {
            "model_id": model_id,
            "title": str(merged.get("title") or model_id),
            "weights": weights if os.path.isabs(weights) else os.path.join(self.models_dir, weights),
            "mmproj": mmproj if (not mmproj or os.path.isabs(mmproj)) else os.path.join(self.models_dir, mmproj),
            "context_tokens": int(merged.get("context_tokens") or 4096),
            "max_output_tokens": int(merged.get("max_output_tokens") or 2048),
            "reasoning": str(merged.get("reasoning") or "auto").strip().lower(),
            "uncensored": bool(merged.get("uncensored")),
            "system_prompt_supported": (model_id.lower() in SYSTEM_ROLE_VERIFIED_MODELS
                                        if sp is None else bool(sp)),
            "vram_need_mb": int(merged["vram_need_mb"]) if merged.get("vram_need_mb") else 0,
            "extra_llama_args": [str(a) for a in (merged.get("extra_llama_args") or [])],
        }

    def select_model(self, model_id: str) -> None:
        """Make `model_id` the profile the launch line, budgets and catalogue use."""
        profile = self.profiles[str(model_id).lower()]
        self.model_id = str(profile["model_id"])
        self.title = str(profile["title"])
        self.weights = str(profile["weights"])
        self.mmproj = str(profile["mmproj"])
        self.context_tokens = int(profile["context_tokens"])
        self.max_output_tokens = int(profile["max_output_tokens"])
        self.reasoning = str(profile["reasoning"])
        self.uncensored = bool(profile["uncensored"])
        self.system_prompt_supported = bool(profile["system_prompt_supported"])
        self.vram_need_mb = int(profile["vram_need_mb"])
        self.extra_llama_args = list(self.node_extra_llama_args) + list(profile["extra_llama_args"])

    def profile(self, model_id: str) -> Optional[Dict[str, object]]:
        return self.profiles.get(str(model_id or "").strip().lower())

    def profile_installed(self, model_id: str) -> bool:
        profile = self.profile(model_id)
        return bool(profile) and self.binary_installed() and os.path.isfile(str(profile["weights"]))

    def profile_need_mb(self, model_id: str) -> int:
        """VRAM a profile takes once loaded: its vram_need_mb, or its files + vram_overhead_mb."""
        profile = self.profile(model_id) or {}
        if int(profile.get("vram_need_mb") or 0) > 0:
            return int(profile["vram_need_mb"])
        total = sum(os.path.getsize(str(p)) for p in (profile.get("weights"), profile.get("mmproj"))
                    if p and os.path.isfile(str(p)))
        return int(total / (1024 * 1024)) + self.vram_overhead_mb

    def installed_model_ids(self) -> List[str]:
        return [str(p["model_id"]) for p in self.profiles.values() if self.profile_installed(str(p["model_id"]))]

    def _path(self, value: object) -> str:
        text = str(value or "").strip()
        return text if os.path.isabs(text) else os.path.join(self.base_dir, text)

    def _argv(self, value: object, *, resolve: bool = True) -> List[str]:
        if isinstance(value, list):
            return [str(part) for part in value]
        text = str(value or "").strip()
        if not text:
            return []
        if resolve and (os.sep in text or "/" in text) and not os.path.isabs(text):
            text = self._path(text)
        return [text]

    # ------------------------------------------------------------ model facts
    @property
    def llama_base_url(self) -> str:
        return f"http://127.0.0.1:{self.llama_port}"

    def binary_installed(self) -> bool:
        # A prefix like [python, fake.py] is a test double; its first element
        # is an interpreter on PATH, so only an explicit path is checked.
        exe = self.llama_server[0]
        if os.path.isabs(exe) or os.sep in exe or "/" in exe:
            return os.path.isfile(exe)
        return True

    def installed(self) -> bool:
        """Converter rule: the binary and the weights file exist (lines 250-261)."""
        return self.binary_installed() and os.path.isfile(self.weights)

    def vision_installed(self) -> bool:
        return bool(self.mmproj) and os.path.isfile(self.mmproj)

    def min_need_mb(self) -> int:
        """The least VRAM a load starts with: fit_min_vram_mb with fit_when_short, else the whole need."""
        if self.fit_when_short and self.fit_min_vram_mb > 0:
            return min(self.fit_min_vram_mb, self.need_mb())
        return self.need_mb()

    def need_mb(self) -> int:
        """VRAM the model takes once loaded: configured, or files + overhead."""
        if self.vram_need_mb > 0:
            return self.vram_need_mb
        total = 0
        for path in (self.weights, self.mmproj if self.vision_installed() else ""):
            if path and os.path.isfile(path):
                total += os.path.getsize(path)
        return int(total / (1024 * 1024)) + self.vram_overhead_mb


def load_config(path: str) -> Config:
    with open(path, encoding="utf-8-sig") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("config must be a JSON object")
    return Config(raw, os.path.dirname(os.path.abspath(path)))


# ---------------------------------------------------- validation (converter)
def validate_bearer_header(header: str, configured_token: str) -> Tuple[bool, str]:
    """Copied from autorig_hunyuan/adapter.py validate_bearer_header.

    Validates a Bearer token without leaking whether a submitted prefix matched.
    """
    token = str(configured_token or "")
    if not token:
        return False, "token_not_configured"
    value = str(header or "").strip()
    if not value.lower().startswith("bearer "):
        return False, "unauthorized"
    supplied = value[7:].strip()
    if not supplied or not hmac.compare_digest(supplied.encode("utf-8"), token.encode("utf-8")):
        return False, "unauthorized"
    return True, "ok"


def validate_prompt(raw: object, limit: int = MAX_PROMPT_CHARS) -> str:
    """Copied from bonsai_adapter.validate_prompt (60101f4, line 216); `limit` is the node's max_prompt_chars."""
    prompt = str(raw or "").strip()
    if not prompt:
        raise RequestError("prompt is required")
    if len(prompt) > limit:
        raise RequestError(f"prompt exceeds {limit} characters")
    return prompt


def validate_temperature(raw: object) -> Optional[float]:
    """An optional sampling temperature, 0-1.5; absent = the node's default (0.3)."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise RequestError("temperature must be a number") from None
    if not 0.0 <= value <= 1.5:
        raise RequestError("temperature must be between 0 and 1.5")
    return value


def validate_max_tokens(raw: object, ceiling: int) -> int:
    """Copied from bonsai_adapter.validate_max_tokens (60101f4, line 225)."""
    if raw is None or str(raw).strip() == "":
        return int(ceiling)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise RequestError("max_output_tokens must be an integer") from None
    if value == -1:
        return -1
    if value < 1:
        raise RequestError("max_output_tokens must be positive or -1 for unlimited")
    return min(value, int(ceiling))


def _resolve_host_addresses(host: str, port: int, timeout_seconds: float = 10.0) -> set:
    """Bounded DNS lookup (autorig_hunyuan/adapter.py _resolve_host_addresses)."""
    result: "queue.Queue[Tuple[str, object]]" = queue.Queue(maxsize=1)

    def resolver() -> None:
        try:
            result.put(("ok", socket.getaddrinfo(host, port)), block=False)
        except Exception as exc:  # noqa: BLE001 - reported to the caller below
            try:
                result.put(("error", exc), block=False)
            except queue.Full:
                pass

    threading.Thread(target=resolver, daemon=True, name="ImageDNS").start()
    try:
        kind, payload = result.get(timeout=max(0.1, float(timeout_seconds)))
    except queue.Empty:
        raise RequestError("image_url host resolution timed out") from None
    if kind == "error":
        raise RequestError(f"image_url host cannot be resolved: {payload}")
    return {item[4][0] for item in payload}  # type: ignore[index]


def validate_public_image_url(url: str, *, allow_private: bool = False) -> None:
    """SSRF guard copied from autorig_hunyuan/adapter.py validate_public_image_url.

    `allow_private` exists only for the self-test, which serves its image from
    127.0.0.1; production configs leave it false.
    """
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname
    if parsed.scheme.lower() not in {"http", "https"} or not host:
        raise RequestError("image_url must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password:
        raise RequestError("image_url credentials are not allowed")
    try:
        port = parsed.port
    except ValueError as exc:
        raise RequestError("image_url has an invalid port") from exc
    if allow_private:
        return
    if port and port not in {80, 443}:
        raise RequestError("image_url port must be 80 or 443")
    if host.lower() == "localhost":
        raise RequestError("image_url host is not public")
    addresses = _resolve_host_addresses(
        host, port or (443 if parsed.scheme.lower() == "https" else 80))
    for address in addresses:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
        if not ip.is_global:
            raise RequestError("image_url host is not public")


def decode_data_image(url: str) -> Tuple[bytes, str]:
    """`data:image/...;base64,...` -> (bytes, media type).

    Not in the converter: the backend publishes inline images as URLs before
    it calls a node. Accepted here so an image can reach the node without a
    network fetch; held to the same size and content-type rules.
    """
    header, sep, payload = url[5:].partition(",")
    if not sep:
        raise RequestError("data: image_url has no payload")
    if not header.lower().endswith(";base64"):
        raise RequestError("data: image_url must be base64")
    media_type = (header[:-7] or "image/png").strip().lower()
    if not media_type.startswith("image/"):
        raise RequestError(f"image_unexpected_content_type: {media_type}")
    if len(payload) > (MAX_IMAGE_BYTES * 4) // 3 + 8:
        raise RequestError("image exceeds the inline size limit")
    try:
        data = base64.b64decode(payload)
    except (ValueError, TypeError):
        raise RequestError("data: image_url is not valid base64") from None
    if not data:
        raise RequestError("image_download_empty")
    if len(data) > MAX_IMAGE_BYTES:
        raise RequestError("image exceeds the inline size limit")
    return data, media_type


def validate_image_url(url: str, *, allow_private: bool = False) -> None:
    if url.lower().startswith("data:"):
        decode_data_image(url)
        return
    validate_public_image_url(url, allow_private=allow_private)


class _GuardedRedirect(urllib.request.HTTPRedirectHandler):
    """Re-run the SSRF guard on every redirect target.

    Addition over the converter, whose plain urlopen would follow a public URL
    that redirects to a private address.
    """

    def __init__(self, allow_private: bool):
        super().__init__()
        self.allow_private = allow_private

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        validate_public_image_url(newurl, allow_private=self.allow_private)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def fetch_task_image(image_url: str, *, allow_private: bool = False) -> Tuple[bytes, str]:
    """Download one caller image (webserver_converter_glb._fetch_ai_task_image, 60101f4)."""
    if image_url.lower().startswith("data:"):
        try:
            return decode_data_image(image_url)
        except RequestError as exc:
            raise NodeError(str(exc)) from None
    try:
        validate_public_image_url(str(image_url), allow_private=allow_private)
    except RequestError as exc:
        raise NodeError(f"image_download_failed: {exc}") from None
    opener = urllib.request.build_opener(_GuardedRedirect(allow_private))
    request_obj = urllib.request.Request(
        str(image_url), headers={"User-Agent": "AutoRig-AIVision/1"})
    try:
        with opener.open(request_obj, timeout=60) as response:
            media_type = (response.headers.get_content_type() or "image/png").strip().lower()
            # Read one byte past the cap so an oversized body is detected
            # instead of being silently truncated into a corrupt image.
            data = response.read(MAX_IMAGE_BYTES + 1)
    except (urllib.error.URLError, OSError, RequestError) as exc:
        raise NodeError(f"image_download_failed: {exc}") from None
    if len(data) > MAX_IMAGE_BYTES:
        raise NodeError("image_exceeds_size_limit")
    if not data:
        raise NodeError("image_download_empty")
    if not media_type.startswith("image/"):
        raise NodeError(f"image_unexpected_content_type: {media_type}")
    return data, media_type


# ------------------------------------------------------------- GPU / ComfyUI
def query_vram(cfg: Config) -> Dict[str, object]:
    """Free/used/total MiB of the configured GPU from nvidia-smi."""
    argv = list(cfg.nvidia_smi) + [
        "-i", str(cfg.gpu_index),
        "--query-gpu=memory.total,memory.used,memory.free",
        "--format=csv,noheader,nounits",
    ]
    checked_at = time.time()
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=15,
                             creationflags=CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "error": f"nvidia_smi_failed: {exc}", "checked_at": checked_at}
    if out.returncode != 0:
        return {"ok": False, "error": f"nvidia_smi_exit_{out.returncode}",
                "checked_at": checked_at}
    for line in (out.stdout or "").splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3:
            try:
                total, used, free = (int(float(p)) for p in parts[:3])
            except ValueError:
                continue
            return {"ok": True, "total_mb": total, "used_mb": used, "free_mb": free,
                    "checked_at": checked_at}
    return {"ok": False, "error": "nvidia_smi_unparsed", "checked_at": checked_at}


def _get_json(url: str, timeout: float) -> object:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json(url: str, body: object, timeout: float) -> None:
    request = urllib.request.Request(url, method="POST", data=json.dumps(body).encode("utf-8"),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


def probe_comfy(cfg: Config) -> Dict[str, object]:
    """Is ComfyUI running or holding a prompt?

    GET /prompt answers {"exec_info": {"queue_remaining": running + pending}}
    and stays small; GET /queue carries every queued workflow, so it is only
    the fallback for a ComfyUI that does not answer /prompt.
    """
    state = _probe_comfy_only(cfg)
    if cfg.converter_status_url:
        state = dict(state, watched=True)
        try:
            token = ""
            if cfg.converter_token_file:
                with open(cfg.converter_token_file, encoding="utf-8-sig") as handle:
                    token = handle.read().strip()
            request = urllib.request.Request(cfg.converter_status_url,
                                             headers={"Authorization": "Bearer " + token} if token else {})
            with urllib.request.urlopen(request, timeout=5.0) as response:
                payload = json.loads(response.read().decode("utf-8", "replace"))
            summary = payload.get("tasks_summary") or {}
            tasks = int(summary.get("processing") or 0) + int(summary.get("queue_size") or summary.get("pending") or 0)
            state["converter_tasks"] = tasks
            if tasks > 0:              # Hunyuan / a conversion holds or is about to take the GPU
                state.update(busy=True, online=True, queue_remaining=int(state.get("queue_remaining") or 0) + tasks)
        except urllib.error.HTTPError as exc:
            state["converter_error"] = f"HTTP {exc.code}"
            if exc.code != 503 or cfg.converter_503_is_busy:
                state["busy"] = True   # an unreadable converter is treated as busy: Hunyuan comes first
        except (urllib.error.URLError, OSError, ValueError, AttributeError) as exc:
            state["converter_error"] = str(exc)[:200]
            state["busy"] = True       # an unreadable converter is treated as busy: Hunyuan comes first
    lease = fresh_gpu_lease(cfg)
    if lease:                          # another node on this card holds it (gpu_lease_yield_files)
        state = dict(state, watched=True, busy=True, online=True, gpu_lease=lease,
                     queue_remaining=int(state.get("queue_remaining") or 0) + 1)
    return state


_GAME_CACHE: Dict[str, object] = {"at": 0.0, "name": ""}


def running_game(cfg: Config) -> str:
    """The owner's game that is running ('' = none): owner_games + owner_games_file, checked every 5 s (tasklist)."""
    names = set(cfg.owner_games)
    if cfg.owner_games_file:
        try:
            with open(cfg.owner_games_file, encoding="utf-8") as handle:
                names |= {line.strip().lower() for line in handle if line.strip() and not line.lstrip().startswith("#")}
        except OSError:
            pass
    if not names:
        return ""
    if time.monotonic() - float(_GAME_CACHE["at"]) < 5:
        return str(_GAME_CACHE["name"])
    found = ""
    try:
        if IS_WINDOWS:
            out = subprocess.run(["tasklist", "/FO", "CSV", "/NH"], capture_output=True, text=True, timeout=15,
                                 creationflags=CREATE_NO_WINDOW).stdout
            running = {line.split(",", 1)[0].strip().strip('"').lower() for line in out.splitlines() if line.strip()}
        else:
            out = subprocess.run(["ps", "-eo", "comm="], capture_output=True, text=True, timeout=15).stdout
            running = {os.path.basename(line.strip()).lower() for line in out.splitlines() if line.strip()}
        found = next((n for n in sorted(names) if n in running), "")
    except (OSError, subprocess.SubprocessError):
        found = ""
    _GAME_CACHE.update(at=time.monotonic(), name=found)
    return found


def fresh_gpu_lease(cfg: Config) -> str:
    """The node name in a fresh lease file this node yields to, or ''."""
    for path in cfg.gpu_lease_yield_files:
        try:
            age = time.time() - os.path.getmtime(path)
        except OSError:
            continue
        if age <= cfg.gpu_lease_fresh_seconds:
            try:
                with open(path, encoding="utf-8") as handle:
                    return str(json.load(handle).get("node") or path)
            except (OSError, ValueError, AttributeError):
                return path
    return ""


def _probe_comfy_only(cfg: Config) -> Dict[str, object]:
    checked_at = time.time()
    if not cfg.comfy_url:
        return {"watched": False, "online": False, "busy": False, "running": 0,
                "pending": 0, "queue_remaining": 0, "checked_at": checked_at}
    state: Dict[str, object] = {"watched": True, "online": False, "running": 0,
                                "pending": 0, "queue_remaining": 0, "error": "",
                                "checked_at": checked_at}
    try:
        payload = _get_json(cfg.comfy_url + "/prompt", 2.5)
        remaining = payload.get("exec_info", {}).get("queue_remaining") if isinstance(
            payload, dict) else None
        if isinstance(remaining, int):
            state.update(online=True, queue_remaining=remaining, source="prompt")
    except (urllib.error.URLError, OSError, ValueError, AttributeError) as exc:
        state["error"] = str(exc)
    if not state["online"]:
        try:
            payload = _get_json(cfg.comfy_url + "/queue", 5.0)
            if isinstance(payload, dict):
                running = len(payload.get("queue_running") or [])
                pending = len(payload.get("queue_pending") or [])
                state.update(online=True, running=running, pending=pending,
                             queue_remaining=running + pending, source="queue", error="")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            state["error"] = str(exc)
    if state["online"]:
        state["busy"] = int(state["queue_remaining"]) > 0
    else:
        state["busy"] = bool(cfg.comfy_unreachable_is_busy)
    return state


# ------------------------------------------------------- process utilities
def _windows_kill_on_close_job(process: subprocess.Popen):
    """Put the child in a job that the OS kills when this process goes away.

    If ai_node is killed (Task Scheduler "End", a crash) the job handle closes
    and llama-server dies with it, so no orphan keeps the weights on VRAM.
    Best effort: returns the job handle, or None.
    """
    if not IS_WINDOWS:
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

        class BASIC_LIMIT(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                        ("PerJobUserTimeLimit", ctypes.c_int64),
                        ("LimitFlags", wintypes.DWORD),
                        ("MinimumWorkingSetSize", ctypes.c_size_t),
                        ("MaximumWorkingSetSize", ctypes.c_size_t),
                        ("ActiveProcessLimit", wintypes.DWORD),
                        ("Affinity", ctypes.c_size_t),
                        ("PriorityClass", wintypes.DWORD),
                        ("SchedulingClass", wintypes.DWORD)]

        class EXTENDED_LIMIT(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", BASIC_LIMIT),
                        ("IoInfo", IO_COUNTERS),
                        ("ProcessMemoryLimit", ctypes.c_size_t),
                        ("JobMemoryLimit", ctypes.c_size_t),
                        ("PeakProcessMemoryUsed", ctypes.c_size_t),
                        ("PeakJobMemoryUsed", ctypes.c_size_t)]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = EXTENDED_LIMIT()
        info.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info),
                                                ctypes.sizeof(info)):
            kernel32.CloseHandle(job)
            return None
        handle = int(getattr(process, "_handle", 0) or 0)
        if not handle or not kernel32.AssignProcessToJobObject(job, handle):
            kernel32.CloseHandle(job)
            return None
        return (kernel32, job)
    except Exception:  # noqa: BLE001 - optional hardening
        logger.warning("Could not put llama-server in a kill-on-close job", exc_info=True)
        return None


def _close_job(job) -> None:
    if job:
        try:
            kernel32, handle = job
            kernel32.CloseHandle(handle)
        except Exception:  # noqa: BLE001
            pass


def _processes_with_port_flag(port: int) -> List[Tuple[int, str]]:
    """(pid, command line) of processes whose command line names `--port <port>`."""
    found: List[Tuple[int, str]] = []
    if IS_WINDOWS:
        script = (
            "$ProgressPreference='SilentlyContinue';"
            "[Console]::OutputEncoding=[Text.Encoding]::UTF8;"
            "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "
            f"'*--port*{int(port)}*' }} | ForEach-Object {{ [pscustomobject]@{{"
            "pid=$_.ProcessId; cmd=$_.CommandLine } } | ConvertTo-Json -Compress"
        )
        encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
        try:
            out = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
                capture_output=True, encoding="utf-8", errors="replace", timeout=60,
                creationflags=CREATE_NO_WINDOW)
            text = (out.stdout or "").strip()
            items = json.loads(text) if text else []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                found.append((int(item.get("pid") or 0), str(item.get("cmd") or "")))
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            logger.warning("Could not list processes: %s", exc)
        return found
    for entry in os.listdir("/proc") if os.path.isdir("/proc") else []:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as handle:
                cmd = handle.read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if "--port" in cmd:
            found.append((int(entry), cmd))
    return found


def _kill_pid(pid: int) -> None:
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True,
                       timeout=30, creationflags=CREATE_NO_WINDOW)
    else:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass


# ------------------------------------------------------------- llama-server
class LlamaServer:
    """The one llama-server child serving this node's pinned model.

    Process handling follows BonsaiAdapter (bonsai_adapter.py 60101f4): the
    same launch line, /health readiness poll, terminate-wait-kill stop. The
    differences are ComfyUI awareness (a load is abandoned when ComfyUI starts)
    and a single model, so there is never a swap.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._process: Optional[subprocess.Popen] = None
        self._job = None
        self._log_handle = None
        self._loaded_model_id = ""
        self._lock = threading.RLock()
        self._in_use = 0
        self.last_used = 0.0
        self.loaded_at = 0.0
        self.loads = 0
        self.last_error = ""
        self.last_stop_reason = ""
        self.fit = False                       # this load lets llama.cpp --fit the layers (part of them in RAM)

    # ------------------------------------------------------------- state
    def running(self) -> bool:
        with self._lock:
            return self._process is not None and self._process.poll() is None

    def loaded_model_id(self) -> str:
        with self._lock:
            return self._loaded_model_id if self.running() else ""

    def pid(self) -> int:
        with self._lock:
            return self._process.pid if self.running() else 0

    def acquire(self) -> None:
        with self._lock:
            self._in_use += 1

    def release(self) -> None:
        with self._lock:
            self._in_use = max(0, self._in_use - 1)
            self.last_used = time.monotonic()

    def in_use(self) -> bool:
        with self._lock:
            return self._in_use > 0

    def users(self) -> int:
        """Tasks holding the server right now (parallel_tasks > 1 lets several share it)."""
        with self._lock:
            return self._in_use

    def idle_seconds(self) -> float:
        with self._lock:
            if not self.running() or self._in_use:
                return 0.0
            return max(0.0, time.monotonic() - self.last_used)

    # ----------------------------------------------------------- process
    def answering(self, timeout: float = 3.0) -> bool:
        """bonsai_adapter._answering: /health answers 200 once the weights are up."""
        try:
            with urllib.request.urlopen(self.cfg.llama_base_url + "/health",
                                        timeout=timeout) as response:
                return response.status == 200
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def _port_open(self) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", self.cfg.llama_port), timeout=1.0):
                return True
        except OSError:
            return False

    def launch_argv(self) -> List[str]:
        """bonsai_adapter._launch_argv (60101f4, lines 311-328), plus extra args."""
        cfg = self.cfg
        argv = list(cfg.llama_server) + ["-m", cfg.weights]
        if cfg.vision_installed():
            argv += ["--mmproj", cfg.mmproj]
        if cfg.reasoning in ("on", "off"):
            argv += ["--reasoning", cfg.reasoning]
        argv += [
            "--alias", cfg.model_id,
            "-ngl", "auto" if self.fit else str(cfg.ngl),
            "-c", str(cfg.context_tokens),
            "--host", "127.0.0.1",
            "--port", str(cfg.llama_port),
            "--jinja",
        ]
        argv += list(cfg.extra_llama_args)
        if self.fit:
            argv += ["--fit", "on", "--fit-target", str(cfg.fit_target_mb)]
        return argv

    def _is_ours(self, cmdline: str) -> bool:
        """A command line this node would have launched: our weights, port and alias."""
        text = os.path.normcase(cmdline)
        port = re.search(r'--port\s+"?%d"?(\s|$)' % self.cfg.llama_port, cmdline)
        if not port:
            return False
        for profile in self.cfg.profiles.values():     # any of our models (a swap may have left the other one)
            weights = str(profile["weights"])
            if os.path.normcase(os.path.abspath(weights)) not in text and os.path.normcase(weights) not in text:
                continue
            if re.search(r'--alias\s+"?%s"?(\s|$)' % re.escape(str(profile["model_id"])), cmdline):
                return True
        return False

    def release_stray(self) -> int:
        """Stop a llama-server a previous ai_node run left on our port.

        Counterpart of BonsaiAdapter.adopt_or_release_stray, narrowed: only a
        process whose command line carries our weights, our --port and our
        --alias is stopped. Anything else on the port is left alone and the
        load fails with llama_port_in_use.
        """
        stopped = 0
        own_pid = os.getpid()
        child = self.pid()
        for pid, cmdline in _processes_with_port_flag(self.cfg.llama_port):
            if pid in (0, own_pid, child) or not self._is_ours(cmdline):
                continue
            logger.warning("Stopping orphaned llama-server pid %s left on port %s",
                           pid, self.cfg.llama_port)
            _kill_pid(pid)
            stopped += 1
        if stopped:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and self._port_open():
                time.sleep(0.5)
        return stopped

    def start(self, abort_if: Callable[[], bool]) -> None:
        """Start the model and wait until it answers (BonsaiAdapter.ensure_started).

        `abort_if` is checked while the weights load; when it says ComfyUI has
        taken the GPU the half-loaded server is stopped and ComfyBusy raised.
        """
        cfg = self.cfg
        if not cfg.installed():
            raise NodeError("runtime_not_installed")
        with self._lock:
            if self.running() and self._loaded_model_id == cfg.model_id and self.answering():
                self.last_used = time.monotonic()
                return
            self._stop_locked("restart")
            if self._port_open():
                self.release_stray()
                if self._port_open():
                    raise NodeError(
                        f"llama_port_in_use: something else listens on 127.0.0.1:"
                        f"{cfg.llama_port}; set another llama_port")
            env = dict(os.environ)
            if cfg.cuda_visible_devices:
                env["CUDA_VISIBLE_DEVICES"] = cfg.cuda_visible_devices
            output = subprocess.DEVNULL
            if cfg.llama_log_file:
                try:
                    self._log_handle = open(cfg.llama_log_file, "wb")
                    output = self._log_handle
                except OSError:
                    logger.warning("Cannot open llama_log_file; discarding its output")
            exe_dir = os.path.dirname(cfg.llama_server[0])
            try:
                self._process = subprocess.Popen(
                    self.launch_argv(),
                    stdout=output,
                    stderr=subprocess.STDOUT if output is not subprocess.DEVNULL
                    else subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    cwd=exe_dir if exe_dir and os.path.isdir(exe_dir) else None,
                    env=env,
                    creationflags=CREATE_NO_WINDOW,
                )
            except OSError as exc:
                self._process = None
                raise NodeError(f"server_start_failed: {exc}") from None
            self._job = _windows_kill_on_close_job(self._process)
            process = self._process
            logger.info("llama-server pid %s starting %s on port %s",
                        process.pid, cfg.model_id, cfg.llama_port)
        deadline = time.monotonic() + cfg.start_timeout_seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                with self._lock:
                    self._stop_locked("exited during start")
                raise NodeError("server_exited_during_start")
            if abort_if():
                with self._lock:
                    self._stop_locked("comfyui_busy during load")
                raise ComfyBusy("comfyui_busy")
            if self.answering():
                with self._lock:
                    self._loaded_model_id = cfg.model_id
                    self.last_used = time.monotonic()
                    self.loaded_at = time.time()
                    self.loads += 1
                logger.info("llama-server pid %s is serving %s", process.pid, cfg.model_id)
                return
            time.sleep(0.5)
        with self._lock:
            self._stop_locked("start timeout")
        raise NodeError("server_start_timeout")

    def _stop_locked(self, reason: str) -> None:
        """bonsai_adapter._stop_locked: terminate, wait 20 s, kill."""
        process, self._process = self._process, None
        self._loaded_model_id = ""
        job, self._job = self._job, None
        if process is not None and process.poll() is None:
            logger.info("Stopping llama-server pid %s (%s)", process.pid, reason)
            self.last_stop_reason = reason
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.error("llama-server did not exit after kill")
        _close_job(job)
        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except OSError:
                pass
            self._log_handle = None

    def stop(self, reason: str) -> bool:
        with self._lock:
            was_running = self.running()
            self._stop_locked(reason)
            return was_running

    def stop_if_idle(self, reason: str, min_idle: float = 0.0) -> bool:
        """Stop the server unless a task is using it or it is younger than min_idle."""
        with self._lock:
            if self._in_use or not self.running():
                return False
            if min_idle > 0 and time.monotonic() - self.last_used < min_idle:
                return False
            self._stop_locked(reason)
            return True

    def reap(self) -> None:
        """Notice a server that died on its own."""
        with self._lock:
            if self._process is not None and self._process.poll() is not None:
                code = self._process.returncode
                logger.warning("llama-server exited by itself with code %s", code)
                self.last_error = f"llama_server_exited: {code}"
                self._stop_locked("exited")

    # -------------------------------------------------------- completion
    def run_completion(
        self,
        prompt: str,
        *,
        system_prompt: str = "",
        image_bytes: Optional[bytes] = None,
        image_media_type: str = "image/png",
        max_output_tokens: Optional[int] = None,
        temperature: float = 0.3,
    ) -> Tuple[str, str, Dict[str, object]]:
        """Copied from BonsaiAdapter.run_completion (60101f4, lines 447-526).

        Returns (answer, reasoning, usage). The caller has already started the
        server, so ensure_started is not repeated here.
        """
        cfg = self.cfg
        if image_bytes is not None:
            if len(image_bytes) > MAX_IMAGE_BYTES:
                raise RequestError("image exceeds the inline size limit")
            if not cfg.vision_installed():
                raise NodeError("vision_projector_not_installed")
        if image_bytes is None:
            content: object = prompt
        else:
            encoded = base64.b64encode(image_bytes).decode("ascii")
            content = [
                {"type": "text", "text": prompt},
                {"type": "image_url",
                 "image_url": {"url": f"data:{image_media_type};base64,{encoded}"}},
            ]
        messages = []
        clean_system_prompt = str(system_prompt or "").strip()
        if clean_system_prompt:
            messages.append({"role": "system", "content": clean_system_prompt})
        messages.append({"role": "user", "content": content})
        requested_max_tokens = (cfg.max_output_tokens if max_output_tokens is None
                                else int(max_output_tokens))
        body = {
            "messages": messages,
            "max_tokens": requested_max_tokens,
            "temperature": float(temperature),
        }
        request = urllib.request.Request(
            cfg.llama_base_url + "/v1/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=cfg.request_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise NodeError(f"model_http_{exc.code}: {detail}") from None
        except (urllib.error.URLError, OSError) as exc:
            raise NodeError(f"model_unreachable: {exc}") from None
        except ValueError:
            raise NodeError("model_returned_invalid_json") from None
        self.last_used = time.monotonic()
        try:
            message = payload["choices"][0]["message"]
        except (KeyError, IndexError, TypeError):
            raise NodeError("model_returned_no_choice") from None
        answer = str(message.get("content") or "").strip()
        reasoning = str(message.get("reasoning_content") or "")
        if not answer:
            # A thinking model that ran out of budget mid-thought returns
            # reasoning and no answer. Say so, with the number to raise.
            budget = int(max_output_tokens or cfg.max_output_tokens)
            raise NodeError(
                "model_spent_its_budget_thinking: the whole "
                f"max_output_tokens={budget} went to reasoning "
                f"({len(reasoning)} characters) before an answer began; "
                "raise max_output_tokens and retry"
            )
        usage = dict(payload.get("usage") or {})
        usage["requested_max_tokens"] = requested_max_tokens
        return answer, reasoning, usage


# ---------------------------------------------------------------------- tasks
class AiTask:
    """One language-model request (converter AiVisionTask, trimmed)."""

    def __init__(self, *, mode: str, prompt: str, system_prompt: str, image_url: str,
                 model: str, requested_model: str, max_output_tokens: int,
                 queue_class: str, backend_task_id: str, temperature: Optional[float] = None,
                 priority: str = ""):
        self.task_id = str(uuid.uuid4())
        self.temperature = temperature
        self.status = "Pending"
        self.mode = mode
        self.prompt = prompt
        self.system_prompt = system_prompt
        self.image_url = image_url
        self.model = model
        self.requested_model = requested_model
        self.max_output_tokens = int(max_output_tokens)
        self.progress = 0
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.error: Optional[str] = None
        self.answer = ""
        self.reasoning = ""
        self.model_usage: Optional[Dict[str, object]] = None
        self.current_stage = "queued"
        self.wait_reason = ""
        self.queue_class = queue_class
        self.backend_task_id = backend_task_id
        self.priority = priority                # "owner": the owner waits for this answer
        self.workload_class = WORKLOAD_AI_VISION

    def active(self) -> bool:
        return self.status in ("Pending", "Processing")

    def to_dict(self) -> Dict[str, object]:
        """Same fields the converter's AiVisionTask.to_dict publishes, minus the
        system prompt (as there)."""
        image_url = self.image_url
        if image_url.lower().startswith("data:"):
            image_url = image_url.split(",", 1)[0] + ",<inline>"
        now = time.time()
        return {
            "task_id": self.task_id,
            "status": self.status,
            "mode": self.mode,
            "prompt": self.prompt,
            "image_url": image_url,
            "model": self.model,
            "requested_model": self.requested_model,
            "max_output_tokens": self.max_output_tokens,
            "progress": self.progress,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "answer": self.answer,
            "reasoning": self.reasoning,
            "model_usage": self.model_usage,
            "backend_task_id": self.backend_task_id,
            "queue_class": self.queue_class,
            "priority": self.priority,
            "current_stage": self.current_stage,
            "wait_reason": self.wait_reason,
            "workload_class": self.workload_class,
            "elapsed_seconds": (max(0.0, (self.completed_at or now) - self.started_at)
                                if self.started_at else 0.0),
        }

    def brief(self) -> Dict[str, object]:
        now = time.time()
        return {
            "task_id": self.task_id,
            "workload_class": self.workload_class,
            "mode": self.mode,
            "status": self.status,
            "current_stage": self.current_stage,
            "model": self.model,
            "running_time": (now - self.started_at) if self.started_at else 0.0,
        }


# ----------------------------------------------------------------- media edit
# M+ "similars" (flip / rotate / crop / vertical smart crop / speed / frame) on the farm (2026-11-07, design
# "Симиляры и апскейл M+ на Renderfin", phase B). The ffmpeg logic is a port of Freestock's media-edit executor
# (freestock_task_queue_plugin/manager.py: _raster_filters ~L6611, _video_filters ~L7026, smart crop ~L6953,
# encoders ~L7908, _execute_video_task ~L8018, cut frame ~L7142). ffmpeg is CPU work (plus NVENC): it never takes
# the model lock, runs next to llama-server, at most `media_edit_parallel` at a time.
WORKLOAD_MEDIA_EDIT = "media_edit"
MEDIA_EDIT_MAX_SOURCE_BYTES = 4 * 1024 ** 3
MEDIA_EDIT_TIMEOUT_SECONDS = 2 * 3600
MEDIA_EDIT_MAX_BODY_BYTES = 256 * 1024
MEDIA_EDIT_MAX_ANCHORS = 240
MEDIA_EDIT_MAX_SIDE = 16384
# Freestock VIDEO_MINTERPOLATE_MAX_SOURCE_BYTES_INT: motion interpolation only for sources up to this size.
MINTERPOLATE_MAX_SOURCE_BYTES = 512 * 1024 * 1024
IMAGE_CONTAINERS = ("jpg", "png")
VIDEO_CONTAINERS = ("mp4", "mov")
MEDIA_EDIT_TIER_RANK = {"mplus": 0, "autorig": 1}
# Freestock _ffmpeg_error_is_legacy_h264_nvenc_fallback, plus the session-limit / init errors of consumer cards.
NVENC_FALLBACK_MARKERS = (
    "10 bit encode not supported",
    "No capable devices found",
    "No NVENC capable devices found",
    "Cannot load nvcuda.dll",
    "Driver does not support the required nvenc API version.",
    "Cannot load libcuda.so",
    "Unrecognized option 'rc:v'",
    "Error splitting the argument list: Option not found",
    "Unknown encoder 'h264_nvenc'",
    "Frame Dimension less than the minimum supported value.",
    "OpenEncodeSessionEx failed",
    "InitializeEncoder failed",
    "Error while opening encoder",
)
CONTENT_TYPES = {"jpg": "image/jpeg", "png": "image/png", "mp4": "video/mp4", "mov": "video/quicktime"}


class MediaCancelled(NodeError):
    """The backend cancelled the task (or the node is shutting down)."""


def _finite(value: object, name: str) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise RequestError(f"{name} must be a number") from None
    if number != number or number in (float("inf"), float("-inf")):
        raise RequestError(f"{name} must be finite")
    return number


def _int_field(source: Dict[str, object], key: str, name: str, low: int, high: int) -> int:
    if key not in source:
        raise RequestError(f"{name}.{key} is required")
    value = _finite(source.get(key), f"{name}.{key}")
    if value != int(value):
        raise RequestError(f"{name}.{key} must be a whole number")
    value = int(value)
    if value < low or value > high:
        raise RequestError(f"{name}.{key} must be between {low} and {high}")
    return value


def parse_media_edit_request(payload: object) -> Dict[str, object]:
    """Validate a media-edit request (contract: backend /api/media-edit) into a normalized spec.

    The backend validates the same contract before it calls a node; the node checks again because it is the
    one running ffmpeg on what it is told.
    """
    if not isinstance(payload, dict):
        raise RequestError("JSON object required")
    source_url = str(payload.get("source_url") or "").strip()
    if not source_url:
        raise RequestError("source_url is required")
    if len(source_url) > 4096:
        raise RequestError("source_url is too long")
    kind = str(payload.get("media_kind") or "").strip().lower()
    if kind not in ("image", "video"):
        raise RequestError("media_kind must be image or video")
    op = payload.get("operation") or {}
    if not isinstance(op, dict):
        raise RequestError("operation must be an object")
    known = {"crop", "scale", "rotate_rad", "rotation_relative_to", "hflip", "vflip", "speed",
             "cut_frame_at_seconds", "smartcrop"}
    unknown = sorted(set(op) - known)
    if unknown:
        raise RequestError(f"operation has unknown fields: {', '.join(unknown)}")
    spec: Dict[str, object] = {"source_url": source_url, "media_kind": kind}

    crop = op.get("crop")
    if crop is not None:
        if not isinstance(crop, dict):
            raise RequestError("operation.crop must be an object {w,h,x,y}")
        spec["crop"] = {"w": _int_field(crop, "w", "crop", 2, MEDIA_EDIT_MAX_SIDE),
                        "h": _int_field(crop, "h", "crop", 2, MEDIA_EDIT_MAX_SIDE),
                        "x": _int_field(crop, "x", "crop", 0, MEDIA_EDIT_MAX_SIDE) if "x" in crop else 0,
                        "y": _int_field(crop, "y", "crop", 0, MEDIA_EDIT_MAX_SIDE) if "y" in crop else 0}
    scale = op.get("scale")
    if scale is not None:
        if not isinstance(scale, dict):
            raise RequestError("operation.scale must be an object {w,h}")
        spec["scale"] = {"w": _int_field(scale, "w", "scale", 2, MEDIA_EDIT_MAX_SIDE),
                         "h": _int_field(scale, "h", "scale", 2, MEDIA_EDIT_MAX_SIDE)}
    rotate = op.get("rotate_rad")
    if rotate is not None:
        rotate = _finite(rotate, "operation.rotate_rad")
        if abs(rotate) > 2 * 3.141592653589793 + 1e-6:
            raise RequestError("operation.rotate_rad is in radians (|value| <= 2*pi)")
        spec["rotate_rad"] = rotate
    relative = str(op.get("rotation_relative_to") or "display").strip().lower()
    if relative not in ("display", "stored"):
        raise RequestError("operation.rotation_relative_to must be display or stored")
    spec["rotation_relative_to"] = relative
    for flag in ("hflip", "vflip"):
        value = op.get(flag, False)
        if not isinstance(value, bool):
            raise RequestError(f"operation.{flag} must be true or false")
        spec[flag] = value

    speed = op.get("speed")
    spec["speed"], spec["interpolate"] = 1.0, False
    if speed is not None:
        if kind != "video":
            raise RequestError("operation.speed is for video")
        if not isinstance(speed, dict):
            raise RequestError("operation.speed must be an object {factor, interpolate}")
        factor = _finite(speed.get("factor", 1.0), "operation.speed.factor")
        if factor < 0.1 or factor > 8.0:
            raise RequestError("operation.speed.factor must be between 0.1 and 8")
        interpolate = speed.get("interpolate", False)
        if not isinstance(interpolate, bool):
            raise RequestError("operation.speed.interpolate must be true or false")
        spec["speed"], spec["interpolate"] = factor, interpolate

    cut = op.get("cut_frame_at_seconds")
    spec["cut_frame_at"] = None
    if cut is not None:
        if kind != "video":
            raise RequestError("operation.cut_frame_at_seconds is for video")
        cut = _finite(cut, "operation.cut_frame_at_seconds")
        if cut < 0 or cut > 86400:
            raise RequestError("operation.cut_frame_at_seconds must be between 0 and 86400")
        spec["cut_frame_at"] = cut

    smart = op.get("smartcrop")
    if smart is not None:
        if not isinstance(smart, dict):
            raise RequestError("operation.smartcrop must be an object")
        if "crop" in spec:
            raise RequestError("operation.crop and operation.smartcrop exclude each other")
        mode = str(smart.get("mode") or "anchor_camera").strip().lower()
        if mode not in ("anchor_camera", "center"):
            raise RequestError("operation.smartcrop.mode must be anchor_camera or center")
        aspect = _finite(smart.get("target_aspect", 9.0 / 16.0), "operation.smartcrop.target_aspect")
        if aspect < 0.1 or aspect > 10:
            raise RequestError("operation.smartcrop.target_aspect must be between 0.1 and 10 (w/h)")
        units = str(smart.get("units") or "norm").strip().lower()
        if units not in ("norm", "px"):
            raise RequestError("operation.smartcrop.units must be norm or px")
        anchors_raw = smart.get("anchors") or []
        if not isinstance(anchors_raw, list):
            raise RequestError("operation.smartcrop.anchors must be a list")
        if len(anchors_raw) > MEDIA_EDIT_MAX_ANCHORS:
            raise RequestError(f"operation.smartcrop.anchors: at most {MEDIA_EDIT_MAX_ANCHORS}")
        anchors: List[Tuple[float, float, float]] = []
        if mode == "anchor_camera":
            for index, anchor in enumerate(anchors_raw):
                if not isinstance(anchor, dict):
                    raise RequestError(f"operation.smartcrop.anchors[{index}] must be an object {{t,x,y}}")
                t = _finite(anchor.get("t", 0.0), f"anchors[{index}].t")
                x = _finite(anchor.get("x"), f"anchors[{index}].x")
                y = _finite(anchor.get("y"), f"anchors[{index}].y")
                if t < 0:
                    raise RequestError(f"anchors[{index}].t must be >= 0")
                if units == "norm" and not (0.0 <= x <= 1.0 and 0.0 <= y <= 1.0):
                    raise RequestError(f"anchors[{index}]: x and y are 0..1 of the frame (units=norm)")
                if units == "px" and (x < 0 or y < 0):
                    raise RequestError(f"anchors[{index}]: x and y must be >= 0")
                anchors.append((t, x, y))
            anchors.sort(key=lambda item: item[0])
        spec["smartcrop"] = {"target_aspect": aspect, "units": units, "anchors": anchors}

    output = payload.get("output") or {}
    if not isinstance(output, dict):
        raise RequestError("output must be an object")
    container = str(output.get("container") or "auto").strip().lower()
    container = {"jpeg": "jpg"}.get(container, container)
    codec = str(output.get("codec") or "auto").strip().lower()
    keep_audio = output.get("keep_audio", False)
    if not isinstance(keep_audio, bool):
        raise RequestError("output.keep_audio must be true or false")
    if codec not in ("auto", "h264", "prores"):
        raise RequestError("output.codec must be auto, h264 or prores")
    still = kind == "image" or spec["cut_frame_at"] is not None
    if still:
        if container not in ("auto",) + IMAGE_CONTAINERS:
            raise RequestError("output.container for a picture must be jpg or png")
        if codec != "auto":
            raise RequestError("output.codec is for video output")
    else:
        if container not in ("auto",) + VIDEO_CONTAINERS:
            raise RequestError("output.container for video must be mp4 or mov (jpg/png need cut_frame_at_seconds)")
        if codec == "prores" and container == "mp4":
            raise RequestError("ProRes goes into mov")
    spec.update(container=container, codec=codec, keep_audio=bool(keep_audio) and not still,
                client_ref=str(payload.get("client_ref") or "")[:200],
                tier=str(payload.get("tier") or "").strip().lower()[:32],
                backend_task_id=str(payload.get("backend_task_id") or "")[:120])
    return spec


def validate_media_source_url(url: str, *, allow_private: bool = False) -> None:
    """https only (any public host); the SSRF guard of the image path for the rest."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https" and not allow_private:
        raise RequestError("source_url must be https")
    try:
        validate_public_image_url(url, allow_private=allow_private)
    except RequestError as exc:
        raise RequestError(str(exc).replace("image_url", "source_url")) from None


class _MediaRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, allow_private: bool):
        super().__init__()
        self.allow_private = allow_private

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        validate_media_source_url(newurl, allow_private=self.allow_private)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _source_suffix(url: str, content_type: str) -> str:
    path = urllib.parse.urlparse(url).path.lower()
    for ext in (".mp4", ".mov", ".m4v", ".mkv", ".webm", ".avi", ".jpg", ".jpeg", ".png", ".webp", ".tif",
                ".tiff", ".bmp", ".gif"):
        if path.endswith(ext):
            return ext
    by_type = {"video/mp4": ".mp4", "video/quicktime": ".mov", "video/webm": ".webm", "image/jpeg": ".jpg",
               "image/png": ".png", "image/webp": ".webp", "image/tiff": ".tif", "video/x-matroska": ".mkv"}
    return by_type.get(content_type, ".bin")


MEDIA_DOWNLOAD_ATTEMPTS = 4
MEDIA_DOWNLOAD_BACKOFF = (2.0, 5.0, 10.0)
MEDIA_CONNECT_TIMEOUT = 20.0
MEDIA_READ_TIMEOUT = 120.0


def _set_read_timeout(response, seconds: float) -> None:
    """urllib opens with one timeout (the connect); reads then get their own (best effort)."""
    for path in (("fp", "raw", "_sock"), ("fp", "_sock"), ("fp", "fp", "raw", "_sock")):
        obj = response
        try:
            for name in path:
                obj = getattr(obj, name)
            obj.settimeout(seconds)
            return
        except Exception:  # noqa: BLE001 - another shape of response object
            continue


def _download_fault(exc: BaseException) -> Tuple[bool, str]:
    """(retryable, text). 5xx/429, timeouts, resets and dropped connections are retried; 4xx are not."""
    import http.client
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code >= 500 or code == 429, f"HTTP {code}"
    reason = getattr(exc, "reason", None)
    transient = (TimeoutError, socket.timeout, ConnectionError, http.client.HTTPException, OSError)
    if isinstance(exc, transient) or isinstance(reason, transient):
        return True, str(exc) or exc.__class__.__name__
    return False, str(exc) or exc.__class__.__name__


def download_media_source(url: str, work_dir: str, *, max_bytes: int, cancel: threading.Event,
                          deadline: float, on_progress: Optional[Callable[[float], None]] = None,
                          allow_private: bool = False, stats: Optional[Dict[str, object]] = None,
                          attempts: int = MEDIA_DOWNLOAD_ATTEMPTS,
                          backoff: Tuple[float, ...] = MEDIA_DOWNLOAD_BACKOFF,
                          connect_timeout: float = MEDIA_CONNECT_TIMEOUT,
                          read_timeout: float = MEDIA_READ_TIMEOUT) -> Tuple[str, int]:
    """Stream the source to work_dir/source.<ext>; (path, bytes). Never logs the URL (it may carry a key).

    2026-11-08 (two M+ JPEGs failed with WinError 10060 on f7 and raptor while ~100 others went through): up to
    `attempts` tries with `backoff` pauses on connect/read timeouts, resets and 5xx (never on 4xx); connect
    timeout 20 s, read timeout 120 s; after a partial read the next try asks for the rest with Range and appends
    when the server answers 206 from that offset, otherwise starts over. stats["attempts"] = tries used.
    """
    try:
        validate_media_source_url(url, allow_private=allow_private)
    except RequestError as exc:
        raise NodeError(f"source_download_failed: {exc}") from None
    opener = urllib.request.build_opener(_MediaRedirect(allow_private))
    stats = stats if stats is not None else {}
    attempts = max(1, int(attempts))
    path = ""
    done = 0
    total = 0
    last = ""

    def pause(attempt: int) -> None:
        if backoff:
            cancel.wait(backoff[min(attempt - 1, len(backoff) - 1)])
        if cancel.is_set():
            raise MediaCancelled("cancelled")

    for attempt in range(1, attempts + 1):
        stats["attempts"] = attempt
        if cancel.is_set():
            raise MediaCancelled("cancelled")
        if time.time() > deadline:
            raise NodeError("timeout: the source download took too long")
        headers = {"User-Agent": "AutoRig-MediaEdit/1"}
        if done > 0:
            headers["Range"] = f"bytes={done}-"
        try:
            response = opener.open(urllib.request.Request(url, headers=headers), timeout=connect_timeout)
        except RequestError as exc:
            raise NodeError(f"source_download_failed: {exc}") from None
        except Exception as exc:  # noqa: BLE001 - URLError, HTTPError, OSError, http.client faults
            retry, last = _download_fault(exc)
            if not retry or attempt >= attempts:
                raise NodeError(f"source_download_failed: {last} (attempt {attempt} of {attempts})") from None
            logger.warning("media-edit source download attempt %d failed (%s); retrying", attempt, last)
            pause(attempt)
            continue
        failed = ""
        with response:
            _set_read_timeout(response, read_timeout)
            status = int(getattr(response, "status", 200) or 200)
            content_type = (response.headers.get_content_type() or "").strip().lower()
            try:
                length = int(response.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            resumed = False
            if done > 0 and status == 206:
                match = re.match(r"bytes (\d+)-\d+/(\d+|\*)", str(response.headers.get("Content-Range") or ""))
                if match and int(match.group(1)) == done:
                    resumed = True
                    if match.group(2) != "*":
                        total = int(match.group(2))
            if not resumed:
                done = 0
                total = length
            if total > max_bytes:
                raise NodeError(f"source_too_large: {total} bytes > {max_bytes}")
            if not path:
                path = os.path.join(work_dir, "source" + _source_suffix(url, content_type))
            stats["resumed"] = bool(stats.get("resumed")) or resumed
            with open(path, "ab" if resumed else "wb") as handle:
                while True:
                    if cancel.is_set():
                        raise MediaCancelled("cancelled")
                    if time.time() > deadline:
                        raise NodeError("timeout: the source download took too long")
                    try:
                        chunk = response.read(1024 * 1024)
                    except Exception as exc:  # noqa: BLE001 - timeouts, resets, IncompleteRead
                        retry, failed = _download_fault(exc)
                        if not retry:
                            raise NodeError(f"source_download_failed: {failed}") from None
                        break
                    if not chunk:
                        break
                    done += len(chunk)
                    if done > max_bytes:
                        raise NodeError(f"source_too_large: more than {max_bytes} bytes")
                    handle.write(chunk)
                    if on_progress and total:
                        on_progress(min(1.0, done / total))
        if not failed and total and done < total:
            failed = f"got {done} of {total} bytes"
        if not failed:
            if done == 0:
                raise NodeError("source_download_empty")
            return path, done
        last = failed
        if attempt >= attempts:
            raise NodeError(f"source_download_failed: {last} (attempt {attempt} of {attempts})")
        logger.warning("media-edit source download attempt %d broke off after %d bytes (%s); retrying",
                       attempt, done, last)
        pause(attempt)
    raise NodeError(f"source_download_failed: {last}")


# ---- ffmpeg discovery and probing
def find_ffmpeg(configured: str = "", configured_probe: str = "") -> Tuple[str, str]:
    """(ffmpeg, ffprobe or ''): config, the Freestock node's bundled ffmpeg, then PATH."""
    import glob
    import shutil
    exe = ".exe" if IS_WINDOWS else ""
    def candidates():
        if configured:
            yield configured
            return
        local = os.environ.get("LOCALAPPDATA") or ""
        roots = [os.path.join(local, "Freestock")] if local else []
        if IS_WINDOWS:
            roots += [r for r in sorted(glob.glob(r"C:\Users\*\AppData\Local\Freestock")) if r not in roots]
        for root in roots:
            yield os.path.join(root, "tools", "bin", "ffmpeg" + exe)
        yield shutil.which("ffmpeg") or ""
        for root in roots:      # a deeper bundle, searched only when nothing above exists
            yield from sorted(glob.glob(os.path.join(root, "WEBSERVER", "**", "ffmpeg" + exe), recursive=True))

    ffmpeg = next((c for c in candidates() if c and os.path.isfile(c)), "")
    probe = ""
    if configured_probe and os.path.isfile(configured_probe):
        probe = configured_probe
    elif ffmpeg:
        sibling = os.path.join(os.path.dirname(ffmpeg), "ffprobe" + exe)
        probe = sibling if os.path.isfile(sibling) else (shutil.which("ffprobe") or "")
    return ffmpeg, probe


def _rate(value: object) -> float:
    text = str(value or "").strip()
    if "/" in text:
        num, _, den = text.partition("/")
        try:
            return 0.0 if float(den) == 0 else float(num) / float(den)
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _probe_from_ffprobe(data: Dict[str, object]) -> Dict[str, object]:
    streams = [s for s in (data.get("streams") or []) if isinstance(s, dict)]
    fmt = data.get("format") if isinstance(data.get("format"), dict) else {}
    video = next((s for s in streams if str(s.get("codec_type")) == "video"), {})
    audio = next((s for s in streams if str(s.get("codec_type")) == "audio"), {})
    # Freestock _source_rotation_radians_from_probe_payload: side data rotations summed, else the rotate tag.
    rotation = None
    for side in video.get("side_data_list") or []:
        if isinstance(side, dict) and "rotation" in side:
            try:
                rotation = (rotation or 0.0) + float(side["rotation"])
            except (TypeError, ValueError):
                pass
    if rotation is None:
        for tags in (video.get("tags"), fmt.get("tags")):
            if isinstance(tags, dict) and "rotate" in tags:
                try:
                    rotation = float(tags["rotate"])
                except (TypeError, ValueError):
                    pass
                break
    duration = 0.0
    for value in (video.get("duration"), fmt.get("duration")):
        try:
            duration = float(value or 0)
        except (TypeError, ValueError):
            duration = 0.0
        if duration > 0:
            break
    bit_rate = ""
    for value in (video.get("bit_rate"),):
        try:
            if float(value or 0) > 0:
                bit_rate = str(int(float(value)))
        except (TypeError, ValueError):
            pass
    fps = _rate(video.get("avg_frame_rate")) or _rate(video.get("r_frame_rate"))
    return {
        "codec": str(video.get("codec_name") or "").lower(),
        "codec_tag": str(video.get("codec_tag_string") or "").lower(),
        "pix_fmt": str(video.get("pix_fmt") or ""),
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "fps": round(fps, 4),
        "duration": duration,
        "bit_rate": bit_rate,
        "rotation_deg": float(rotation or 0.0),
        "has_video": bool(video),
        "has_audio": bool(audio),
        "audio_codec": str(audio.get("codec_name") or ""),
        "format_name": str(fmt.get("format_name") or ""),
    }


def _probe_from_banner(text: str) -> Dict[str, object]:
    """`ffmpeg -i` output for a box without ffprobe (Freestock's bundle ships only ffmpeg.exe)."""
    info: Dict[str, object] = {"codec": "", "codec_tag": "", "pix_fmt": "", "width": 0, "height": 0, "fps": 0.0,
                               "duration": 0.0, "bit_rate": "", "rotation_deg": 0.0, "has_video": False,
                               "has_audio": False, "audio_codec": "", "format_name": ""}
    match = re.search(r"Input #0, ([^,]+(?:,[^,\s]+)*), from", text)
    if match:
        info["format_name"] = match.group(1).strip()
    match = re.search(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)", text)
    if match:
        info["duration"] = int(match.group(1)) * 3600 + int(match.group(2)) * 60 + float(match.group(3))
    for line in text.splitlines():
        if "Video:" in line and not info["has_video"]:
            info["has_video"] = True
            rest = line.split("Video:", 1)[1].strip()
            info["codec"] = rest.split()[0].strip(",").lower() if rest else ""
            tag = re.search(r"\(([0-9A-Za-z]{4}) / 0x[0-9A-Fa-f]+\)", rest)
            if tag:
                info["codec_tag"] = tag.group(1).lower()
            size = re.search(r"\b(\d{2,5})x(\d{2,5})\b", rest)
            if size:
                info["width"], info["height"] = int(size.group(1)), int(size.group(2))
            pix = re.search(r"\b(yuv\w+|rgb\w+|bgr\w+|gray\w*|nv12|p010\w*)\b", rest)
            if pix:
                info["pix_fmt"] = pix.group(1)
            fps = re.search(r"([\d.]+) fps", rest)
            if fps:
                info["fps"] = float(fps.group(1))
            rate = re.search(r"(\d+) kb/s", rest)
            if rate:
                info["bit_rate"] = str(int(rate.group(1)) * 1000)
        elif "Audio:" in line and not info["has_audio"]:
            info["has_audio"] = True
            rest = line.split("Audio:", 1)[1].strip()
            info["audio_codec"] = rest.split()[0].strip(",").lower() if rest else ""
        rot = re.search(r"rotation of (-?[\d.]+) degrees", line)
        if rot:
            info["rotation_deg"] = float(rot.group(1))
    return info


def probe_media(ffmpeg: str, ffprobe: str, path: str) -> Dict[str, object]:
    if ffprobe:
        try:
            done = subprocess.run([ffprobe, "-hide_banner", "-v", "quiet", "-print_format", "json",
                                   "-show_streams", "-show_format", path], capture_output=True, text=True,
                                  encoding="utf-8", errors="replace", timeout=60, creationflags=CREATE_NO_WINDOW)
            if done.returncode == 0:
                data = json.loads(done.stdout or "{}")
                if isinstance(data, dict):
                    return _probe_from_ffprobe(data)
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
    try:
        done = subprocess.run([ffmpeg, "-hide_banner", "-nostdin", "-i", path], capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=60, creationflags=CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError) as exc:
        raise NodeError(f"probe_failed: {exc}") from None
    return _probe_from_banner(done.stderr or "")


def display_size(info: Dict[str, object]) -> Tuple[int, int]:
    """The frame size the filters see: ffmpeg autorotates, so a quarter-turn source swaps its sides."""
    width, height = int(info.get("width") or 0), int(info.get("height") or 0)
    if round(abs(float(info.get("rotation_deg") or 0.0))) % 180 == 90:
        return height, width
    return width, height


# ---- filters (Freestock port)
def _fmt(value: float) -> str:
    """Freestock _smartcrop_fmt_float."""
    if abs(value - round(value)) < 0.000001:
        return str(int(round(value)))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _smartcrop_rect(width: int, height: int, aspect: float, center: Optional[Tuple[float, float]]) -> Dict[str, float]:
    """Freestock _smartcrop_rect: the largest target-aspect window, centred on the subject, inside the frame."""
    source_aspect = float(width) / float(height)
    cx = center[0] if center else width / 2.0
    cy = center[1] if center else height / 2.0
    if source_aspect >= aspect:
        h = float(height)
        w = min(float(width), h * aspect)
        return {"x": min(max(cx - w / 2.0, 0.0), max(0.0, width - w)), "y": 0.0, "w": w, "h": h}
    w = float(width)
    h = min(float(height), w / aspect)
    return {"x": 0.0, "y": min(max(cy - h / 2.0, 0.0), max(0.0, height - h)), "w": w, "h": h}


def _smooth_segment(v0: float, v1: float, t0: float, t1: float) -> str:
    """Freestock _smartcrop_smooth_expr between two anchors: smoothstep from v0 at t0 to v1 at t1."""
    delta = v1 - v0
    if abs(delta) < 0.000001:
        return _fmt(v0)
    span = max(0.001, t1 - t0)
    p = f"min(max((t-{_fmt(t0)})/{_fmt(span)}\\,0)\\,1)"
    return f"{_fmt(v0)}+{_fmt(delta)}*({p}*{p}*(3-2*{p}))"


def _piecewise(points: List[Tuple[float, float]]) -> str:
    if not points:
        return "0"
    if len(points) == 1 or all(abs(v - points[0][1]) < 0.000001 for _, v in points):
        return _fmt(points[0][1])
    expr = _fmt(points[-1][1])
    for index in range(len(points) - 2, -1, -1):
        t0, v0 = points[index]
        t1, v1 = points[index + 1]
        expr = f"if(lt(t\\,{_fmt(t1)})\\,{_smooth_segment(v0, v1, t0, t1)}\\,{expr})"
    return expr


def smartcrop_rects(smart: Dict[str, object], width: int, height: int) -> List[Tuple[float, Dict[str, float]]]:
    """(t, rect) per anchor in source pixels; one centred rect when there are no anchors."""
    aspect = float(smart.get("target_aspect") or 9.0 / 16.0)
    anchors = list(smart.get("anchors") or [])
    if not anchors:
        return [(0.0, _smartcrop_rect(width, height, aspect, None))]
    norm = str(smart.get("units") or "norm") == "norm"
    rects = []
    for t, x, y in anchors:
        center = (x * width, y * height) if norm else (min(float(x), width), min(float(y), height))
        rects.append((float(t), _smartcrop_rect(width, height, aspect, center)))
    return rects


def smartcrop_filter(smart: Dict[str, object], width: int, height: int, at: Optional[float] = None) -> str:
    """crop=w:h:x(t):y(t). Freestock eased between two Detectron anchors (start/end); here between any number
    the caller computed, in the source timeline (the crop runs before any speed change). With `at` (a cut
    frame) the window is fixed where it is at that moment."""
    if width <= 0 or height <= 0:
        raise NodeError("smartcrop: the source size is unknown")
    rects = smartcrop_rects(smart, width, height)
    w = max(2, int(round(rects[0][1]["w"])) // 2 * 2)
    h = max(2, int(round(rects[0][1]["h"])) // 2 * 2)
    xs = [(t, min(r["x"], float(width - w))) for t, r in rects]
    ys = [(t, min(r["y"], float(height - h))) for t, r in rects]
    if at is not None:
        x_expr = _fmt(_value_at(xs, at))
        y_expr = _fmt(_value_at(ys, at))
    else:
        x_expr, y_expr = _piecewise(xs), _piecewise(ys)
    return f"crop={w}:{h}:{x_expr}:{y_expr}"


def _value_at(points: List[Tuple[float, float]], at: float) -> float:
    """The eased value of _piecewise at time `at` (for a cut frame)."""
    if at <= points[0][0] or len(points) == 1:
        return points[0][1]
    for (t0, v0), (t1, v1) in zip(points, points[1:]):
        if at < t1:
            p = min(max((at - t0) / max(0.001, t1 - t0), 0.0), 1.0)
            return v0 + (v1 - v0) * p * p * (3 - 2 * p)
    return points[-1][1]


def _rotation_filters(radians: float) -> List[str]:
    """Clockwise rotation (ffmpeg's rotate convention). Quarter turns are exact transposes (Freestock used
    rotate=…:ow=rotw:oh=roth for every angle, which resamples and can leave a 1 px border); any other angle
    keeps Freestock's rotate filter."""
    import math
    turn = radians / (math.pi / 2)
    nearest = round(turn)
    if abs(turn - nearest) < 0.0005:
        quarter = int(nearest) % 4
        return {0: [], 1: ["transpose=clock"], 2: ["hflip", "vflip"], 3: ["transpose=cclock"]}[quarter]
    text = f"{radians:.10f}".rstrip("0").rstrip(".")
    return [f"rotate={text}:ow=rotw({text}):oh=roth({text})"]


def _normalized(radians: float) -> float:
    import math
    while radians > math.pi:
        radians -= 2 * math.pi
    while radians < -math.pi:
        radians += 2 * math.pi
    return radians


def effective_rotation(spec: Dict[str, object], info: Dict[str, object]) -> float:
    """rotate_rad as asked. rotation_relative_to=stored is Freestock's video rule (_video_filters): the
    source's own rotation metadata is subtracted, for callers that computed the angle on the stored frame."""
    import math
    if spec.get("rotate_rad") is None:
        return 0.0
    radians = float(spec["rotate_rad"])
    if spec.get("media_kind") == "video" and spec.get("rotation_relative_to") == "stored":
        radians = _normalized(radians - math.radians(float(info.get("rotation_deg") or 0.0)))
    return radians


def media_filters(spec: Dict[str, object], info: Dict[str, object], *, source_bytes: int = 0,
                  at: Optional[float] = None) -> List[str]:
    """The -vf chain. Order: crop / smart crop on the source frame, scale, flips, rotation, then speed
    (setpts, optional minterpolate). Freestock put setpts first; cropping first keeps smart-crop anchor times
    on the source timeline and interpolates fewer pixels."""
    width, height = display_size(info)
    filters: List[str] = []
    if spec.get("smartcrop") is not None:
        filters.append(smartcrop_filter(spec["smartcrop"], width, height, at))  # type: ignore[arg-type]
    elif spec.get("crop"):
        crop = spec["crop"]  # type: ignore[assignment]
        w, h = int(crop["w"]), int(crop["h"])  # type: ignore[index]
        x, y = int(crop["x"]), int(crop["y"])  # type: ignore[index]
        if width and height and (x + w > width or y + h > height):
            raise NodeError(f"crop {w}x{h}+{x}+{y} is outside the {width}x{height} frame")
        filters.append(f"crop={w - (w % 2)}:{h - (h % 2)}:{x}:{y}")
    if spec.get("scale"):
        scale = spec["scale"]  # type: ignore[assignment]
        filters.append(f"scale={max(2, int(scale['w']) // 2 * 2)}:{max(2, int(scale['h']) // 2 * 2)}")  # type: ignore[index]
    if spec.get("hflip"):
        filters.append("hflip")
    if spec.get("vflip"):
        filters.append("vflip")
    filters += _rotation_filters(effective_rotation(spec, info))
    speed = float(spec.get("speed") or 1.0)
    if at is None and spec.get("media_kind") == "video" and abs(speed - 1.0) > 0.001:
        filters.append(f"setpts={1.0 / speed:.6f}*PTS")
        if spec.get("interpolate") and minterpolate_allowed(info, source_bytes):
            fps = float(info.get("fps") or 0.0)
            target = max(fps, fps * speed) if fps > 0 else 60.0
            filters.append(f"minterpolate='mi_mode=mci:mc_mode=aobmc:vsbmc=1:fps={target:g}'")
    return filters


def minterpolate_allowed(info: Dict[str, object], source_bytes: int) -> bool:
    """Freestock _video_minterpolate_allowed_bool: <= 512 MB, <= 30 fps, <= 4K."""
    if source_bytes > MINTERPOLATE_MAX_SOURCE_BYTES:
        return False
    if float(info.get("fps") or 0.0) > 30.001:
        return False
    if int(info.get("width") or 0) * int(info.get("height") or 0) > 3840 * 2160:
        return False
    return True


def output_frame_rate(spec: Dict[str, object], info: Dict[str, object]) -> float:
    """Freestock _source_video_output_frame_rate_float: a 30 fps clip at x2 is written at 60 fps."""
    speed = float(spec.get("speed") or 1.0)
    fps = float(info.get("fps") or 0.0)
    if 29.9 <= fps <= 30.0 and abs(speed - 2.0) <= 0.001:
        return max(fps, fps * speed)
    return 0.0


def audio_filters(speed: float) -> List[str]:
    """Freestock _audio_filters: atempo chain (each step 0.5..2)."""
    if speed <= 0 or abs(speed - 1.0) <= 0.001:
        return []
    parts: List[str] = []
    remaining = speed
    while remaining > 2.0:
        parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        parts.append("atempo=0.5")
        remaining /= 0.5
    parts.append(f"atempo={remaining:.6f}")
    return parts


# ---- encoders (Freestock port)
PRORES_PROFILE_BY_TAG = {"apco": 0, "ocpa": 0, "apcs": 1, "scpa": 1, "apcn": 2, "ncpa": 2, "apch": 3,
                         "hcpa": 3, "ap4h": 4, "h4pa": 4, "ap4x": 5}


def prores_args(info: Dict[str, object]) -> List[str]:
    """Freestock _legacy_prores_output_args: prores_ks q4 in the source's profile, 10-bit 4:2:2 or 4:4:4."""
    profile = PRORES_PROFILE_BY_TAG.get(str(info.get("codec_tag") or ""), 3)
    pix_fmt = "yuv422p10le" if profile <= 3 else "yuv444p10le"
    return ["-c:v", "prores_ks", "-qscale:v", "4", "-pix_fmt", pix_fmt, "-profile:v", str(profile)]


def nvenc_args(info: Dict[str, object]) -> List[str]:
    """Freestock _legacy_h264_nvenc_output_args: p7 hq, CBR at the source bitrate when known."""
    args = ["-c:v", "h264_nvenc", "-preset:v", "p7", "-tune:v", "hq"]
    if info.get("bit_rate"):
        args += ["-rc:v", "cbr", "-b:v", str(info["bit_rate"])]
    return args


def x264_args(info: Dict[str, object]) -> List[str]:
    """Freestock _legacy_h264_fallback_output_args (encoder "h264" = libx264): slow, film, source bitrate."""
    args = ["-c:v", "libx264", "-preset:v", "slow", "-tune:v", "film"]
    if info.get("bit_rate"):
        args += ["-b:v", str(info["bit_rate"])]
    return args


def resolve_container(spec: Dict[str, object], info: Dict[str, object], source_path: str) -> str:
    container = str(spec.get("container") or "auto")
    if container != "auto":
        return container
    if spec.get("media_kind") == "image":
        return "png" if str(info.get("codec") or "") in ("png", "tiff", "bmp") else "jpg"
    if spec.get("cut_frame_at") is not None:
        return "jpg"
    if spec.get("codec") == "prores" or (spec.get("codec") == "auto" and info.get("codec") == "prores"):
        return "mov"
    return "mov" if source_path.lower().endswith(".mov") else "mp4"


def build_media_commands(ffmpeg: str, spec: Dict[str, object], info: Dict[str, object], source_path: str,
                         output_path: str, *, source_bytes: int = 0,
                         use_nvenc: bool = True) -> Tuple[List[str], Optional[List[str]], str]:
    """(command, libx264 fallback or None, encoder label) for one task."""
    head = [ffmpeg, "-hide_banner", "-nostdin", "-y", "-progress", "pipe:1", "-nostats"]
    container = os.path.splitext(output_path)[1].lstrip(".").lower()
    if spec.get("media_kind") == "image":
        # Freestock _execute_raster_task.
        filters = media_filters(spec, info)
        command = head + ["-i", source_path]
        if filters:
            command += ["-vf", ",".join(filters)]
        command += ["-frames:v", "1"]
        if container == "jpg":
            command += ["-q:v", "2"]
        command += ["-update", "1", output_path]
        return command, None, "image"
    at = spec.get("cut_frame_at")
    if at is not None:
        # Freestock _legacy_cutframe_jpeg_command: seek before the input, one mjpeg q2 frame.
        filters = media_filters(spec, info, at=float(at))  # type: ignore[arg-type]
        command = head + ["-ss", _fmt(float(at)), "-i", source_path]  # type: ignore[arg-type]
        if filters:
            command += ["-vf", ",".join(filters)]
        if container == "jpg":
            command += ["-c:v", "mjpeg", "-f", "image2", "-fps_mode", "passthrough", "-qscale:v", "2"]
        command += ["-update", "1", "-frames:v", "1", "-an", output_path]
        return command, None, "frame"
    filters = media_filters(spec, info, source_bytes=source_bytes)
    command = head + ["-i", source_path, "-map", "0:v:0"]
    if spec.get("keep_audio") and info.get("has_audio"):
        command += ["-map", "0:a:0?"]
    if filters:
        command += ["-vf", ",".join(filters)]
    prores = spec.get("codec") == "prores" or (spec.get("codec") == "auto" and info.get("codec") == "prores")
    if prores:
        codec_args, fallback_args, label = prores_args(info), None, "prores_ks"
    elif use_nvenc:
        codec_args, fallback_args, label = nvenc_args(info), x264_args(info), "h264_nvenc"
    else:
        codec_args, fallback_args, label = x264_args(info), None, "libx264"
    tail: List[str] = []
    rate = output_frame_rate(spec, info)
    if rate > 0:
        tail += ["-r", f"{rate:g}"]
    if spec.get("keep_audio") and info.get("has_audio"):
        speed_filters = audio_filters(float(spec.get("speed") or 1.0))
        tail += (["-af", ",".join(speed_filters)] if speed_filters else ["-c:a", "copy"])
    else:
        tail += ["-an"]
    if container == "mp4":
        tail += ["-movflags", "+faststart"]
    tail += [output_path]
    fallback = (command + fallback_args + tail) if fallback_args else None
    return command + codec_args + tail, fallback, label


def expected_output_seconds(spec: Dict[str, object], info: Dict[str, object]) -> float:
    if spec.get("media_kind") != "video" or spec.get("cut_frame_at") is not None:
        return 0.0
    return float(info.get("duration") or 0.0) / max(0.1, float(spec.get("speed") or 1.0))


def run_ffmpeg(command: List[str], *, expected_seconds: float, cancel: threading.Event, deadline: float,
               on_progress: Optional[Callable[[float], None]] = None,
               on_process: Optional[Callable[[Optional[subprocess.Popen]], None]] = None) -> str:
    """Run ffmpeg with -progress pipe:1, report the fraction done; raise NodeError with the stderr tail."""
    import collections
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   stdin=subprocess.DEVNULL, creationflags=CREATE_NO_WINDOW)
    except OSError as exc:
        raise NodeError(f"ffmpeg_not_startable: {exc}") from None
    if on_process:
        on_process(process)
    tail: "collections.deque[str]" = collections.deque(maxlen=60)

    def read_stderr() -> None:
        for raw in iter(process.stderr.readline, b""):  # type: ignore[union-attr]
            tail.append(raw.decode("utf-8", "replace").rstrip())

    reader = threading.Thread(target=read_stderr, daemon=True, name="ffmpeg-stderr")
    reader.start()

    def watchdog() -> None:
        while process.poll() is None:
            if cancel.is_set() or time.time() > deadline:
                _kill_process_tree(process)
                return
            time.sleep(0.5)

    threading.Thread(target=watchdog, daemon=True, name="ffmpeg-watchdog").start()
    try:
        for raw in iter(process.stdout.readline, b""):  # type: ignore[union-attr]
            line = raw.decode("ascii", "replace").strip()
            if line.startswith(("out_time_us=", "out_time_ms=")) and expected_seconds > 0 and on_progress:
                try:
                    micro = int(line.split("=", 1)[1])
                except ValueError:
                    continue
                on_progress(min(1.0, max(0.0, micro / 1e6 / expected_seconds)))
        process.wait()
    finally:
        if on_process:
            on_process(None)
    reader.join(timeout=5)
    if cancel.is_set():
        raise MediaCancelled("cancelled")
    if time.time() > deadline and process.returncode != 0:
        raise NodeError("timeout: ffmpeg ran past the task limit")
    text = "\n".join(tail)
    if process.returncode != 0:
        raise NodeError("ffmpeg_failed: " + (text[-1500:] if text else f"exit {process.returncode}"))
    return text


def _kill_process_tree(process: subprocess.Popen) -> None:
    """ffmpeg (and whatever wrapper started it) - a killed wrapper alone would leave the pipe open."""
    try:
        if IS_WINDOWS:
            _kill_pid(process.pid)
        process.kill()
    except (OSError, subprocess.SubprocessError):
        pass


def nvenc_fallback_wanted(error: str) -> bool:
    return any(marker in error for marker in NVENC_FALLBACK_MARKERS)


class MediaTask:
    """One media-edit job: download, ffmpeg, output file kept until the backend fetched it."""

    def __init__(self, spec: Dict[str, object]):
        self.task_id = str(uuid.uuid4())
        self.spec = spec
        self.status = "Pending"
        self.stage = "queued"
        self.progress = 0.0
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.error = ""
        self.encoder = ""
        self.output_path = ""
        self.output_meta: Dict[str, object] = {}
        self.source_meta: Dict[str, object] = {}
        self.source_bytes = 0
        self.download_attempts = 0
        self.released = False
        self.cancel = threading.Event()
        self.process: Optional[subprocess.Popen] = None
        self.work_dir = ""
        self.workload_class = WORKLOAD_MEDIA_EDIT

    def active(self) -> bool:
        return self.status in ("Pending", "Processing")

    def rank(self) -> Tuple[int, float]:
        return (MEDIA_EDIT_TIER_RANK.get(str(self.spec.get("tier") or ""), 2), self.created_at)

    def to_dict(self) -> Dict[str, object]:
        now = time.time()
        return {
            "task_id": self.task_id,
            "workload_class": WORKLOAD_MEDIA_EDIT,
            "status": self.status,
            "stage": self.stage,
            "progress": round(self.progress, 1),
            "media_kind": self.spec.get("media_kind"),
            "tier": self.spec.get("tier") or "",
            "client_ref": self.spec.get("client_ref") or "",
            "backend_task_id": self.spec.get("backend_task_id") or "",
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "elapsed_seconds": (round(max(0.0, (self.completed_at or now) - self.started_at), 2)
                                if self.started_at else 0.0),
            "error": self.error,
            "encoder": self.encoder,
            "output_ready": bool(self.status == "Completed" and self.output_path and not self.released
                                 and os.path.isfile(self.output_path)),
            "output_name": os.path.basename(self.output_path) if self.output_path else "",
            "output_meta": dict(self.output_meta),
            "source_meta": dict(self.source_meta),
            "download_attempts": self.download_attempts,
        }


def execute_media_task(task: MediaTask, *, ffmpeg: str, ffprobe: str, max_source_bytes: int,
                       timeout_seconds: float, allow_private: bool, use_nvenc: bool,
                       local_source: str = "") -> None:
    """Run one task in task.work_dir (exists). Fills output_path/meta; raises NodeError / MediaCancelled."""
    spec = task.spec
    deadline = (task.started_at or time.time()) + timeout_seconds

    def set_progress(low: float, high: float) -> Callable[[float], None]:
        def update(fraction: float) -> None:
            task.progress = max(task.progress, low + (high - low) * fraction)
        return update

    if local_source:
        source_path, size = local_source, os.path.getsize(local_source)
    else:
        task.stage = "downloading"
        download: Dict[str, object] = {}
        try:
            source_path, size = download_media_source(
                str(spec["source_url"]), task.work_dir, max_bytes=max_source_bytes, cancel=task.cancel,
                deadline=deadline, on_progress=set_progress(0.0, 20.0), allow_private=allow_private,
                stats=download)
        finally:
            task.download_attempts = int(download.get("attempts") or 0)
    task.source_bytes = size
    task.progress = max(task.progress, 20.0)
    task.stage = "probing"
    info = probe_media(ffmpeg, ffprobe, source_path)
    width, height = display_size(info)
    task.source_meta = {"w": width, "h": height, "duration": round(float(info.get("duration") or 0.0), 3),
                        "fps": info.get("fps"), "codec": info.get("codec"), "bytes": size,
                        "audio": bool(info.get("has_audio")), "rotation_deg": info.get("rotation_deg")}
    if spec.get("media_kind") == "video" and not info.get("has_video"):
        raise NodeError("source_not_video: ffmpeg found no video stream")
    if not width or not height:
        raise NodeError("source_unreadable: ffmpeg could not read the picture size")
    at = spec.get("cut_frame_at")
    if at is not None and float(info.get("duration") or 0) and float(at) > float(info["duration"]):  # type: ignore[arg-type]
        raise NodeError(f"cut_frame_beyond_end: {at} s > {info['duration']} s")
    container = resolve_container(spec, info, source_path)
    output_path = os.path.join(task.work_dir, "result." + container)
    command, fallback, label = build_media_commands(ffmpeg, spec, info, source_path, output_path,
                                                    source_bytes=size, use_nvenc=use_nvenc)
    task.stage = "encoding"
    task.encoder = label
    expected = expected_output_seconds(spec, info)

    def keep_process(process: Optional[subprocess.Popen]) -> None:
        task.process = process

    try:
        run_ffmpeg(command, expected_seconds=expected, cancel=task.cancel, deadline=deadline,
                   on_progress=set_progress(20.0, 99.0), on_process=keep_process)
    except MediaCancelled:
        raise
    except NodeError as exc:
        if fallback is None or not nvenc_fallback_wanted(str(exc)):
            raise
        logger.warning("Media task %s: NVENC refused (%s); encoding with libx264", task.task_id,
                       str(exc).splitlines()[-1][:160] if str(exc) else "")
        task.encoder = "libx264 (nvenc fallback)"
        run_ffmpeg(fallback, expected_seconds=expected, cancel=task.cancel, deadline=deadline,
                   on_progress=set_progress(20.0, 99.0), on_process=keep_process)
    if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
        raise NodeError("ffmpeg_no_output: ffmpeg finished without writing a file")
    task.stage = "finishing"
    out = probe_media(ffmpeg, ffprobe, output_path)
    out_w, out_h = display_size(out)
    task.output_path = output_path
    task.output_meta = {"w": out_w, "h": out_h, "duration": round(float(out.get("duration") or 0.0), 3)
                        if spec.get("media_kind") == "video" and at is None else 0.0,
                        "bytes": os.path.getsize(output_path),
                        "fps": out.get("fps") if spec.get("media_kind") == "video" and at is None else 0,
                        "codec": out.get("codec"), "container": container,
                        "audio": bool(out.get("has_audio")),
                        "download_attempts_int": task.download_attempts}


def make_selftest_clip(ffmpeg: str, path: str) -> None:
    """2 s 640x360 30 fps clip with a tone (libx264, else mpeg4) - lavfi, no files needed."""
    base = [ffmpeg, "-hide_banner", "-nostdin", "-y", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30:duration=2",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=2"]
    for codec in (["-c:v", "libx264", "-pix_fmt", "yuv420p", "-b:v", "1500k"], ["-c:v", "mpeg4", "-b:v", "1500k"]):
        done = subprocess.run(base + codec + ["-c:a", "aac", "-shortest", path], capture_output=True,
                              timeout=120, creationflags=CREATE_NO_WINDOW)
        if done.returncode == 0 and os.path.isfile(path) and os.path.getsize(path) > 0:
            return
    raise NodeError("selftest: ffmpeg could not generate the synthetic clip: "
                    + (done.stderr or b"").decode("utf-8", "replace")[-400:])


def run_media_selftest(ffmpeg: str, ffprobe: str, work_root: str, use_nvenc: bool = True) -> Dict[str, object]:
    """2-second synthetic clip through flip + crop + speed x2 (+ a cut frame): validates ffmpeg and NVENC."""
    import shutil
    started = time.time()
    result: Dict[str, object] = {"ok": False, "at": started, "ffmpeg": ffmpeg, "ffprobe": ffprobe,
                                 "nvenc": False, "encoder": "", "seconds": 0.0, "error": "", "version": ""}
    if not ffmpeg:
        result["error"] = "ffmpeg not found (config ffmpeg_exe, the Freestock bundle or PATH)"
        return result
    work = os.path.join(work_root, "selftest-" + uuid.uuid4().hex[:8])
    os.makedirs(work, exist_ok=True)
    try:
        version = subprocess.run([ffmpeg, "-hide_banner", "-version"], capture_output=True, text=True,
                                 timeout=30, creationflags=CREATE_NO_WINDOW)
        result["version"] = (version.stdout or "").splitlines()[0][:120] if version.stdout else ""
        source = os.path.join(work, "synthetic.mp4")
        make_selftest_clip(ffmpeg, source)
        spec = parse_media_edit_request({
            "source_url": "https://selftest.invalid/synthetic.mp4", "media_kind": "video",
            "operation": {"hflip": True, "crop": {"w": 320, "h": 360, "x": 160, "y": 0},
                          "speed": {"factor": 2.0, "interpolate": True}},
            "output": {"container": "mp4"}})
        task = MediaTask(spec)
        task.work_dir = work
        task.started_at = time.time()
        execute_media_task(task, ffmpeg=ffmpeg, ffprobe=ffprobe, max_source_bytes=MEDIA_EDIT_MAX_SOURCE_BYTES,
                           timeout_seconds=300, allow_private=True, use_nvenc=use_nvenc, local_source=source)
        meta = task.output_meta
        problems = []
        if (meta.get("w"), meta.get("h")) != (320, 360):
            problems.append(f"size {meta.get('w')}x{meta.get('h')} != 320x360")
        if abs(float(meta.get("duration") or 0) - 1.0) > 0.3:
            problems.append(f"duration {meta.get('duration')} s != ~1 s")
        if abs(float(meta.get("fps") or 0) - 60.0) > 0.5:
            problems.append(f"fps {meta.get('fps')} != 60")
        result["encoder"] = task.encoder
        result["nvenc"] = task.encoder == "h264_nvenc"
        result["output_meta"] = meta
        frame = MediaTask(parse_media_edit_request({
            "source_url": "https://selftest.invalid/synthetic.mp4", "media_kind": "video",
            "operation": {"cut_frame_at_seconds": 1.0, "rotate_rad": 1.5707963}, "output": {"container": "jpg"}}))
        frame.work_dir = os.path.join(work, "frame")
        os.makedirs(frame.work_dir, exist_ok=True)
        frame.started_at = time.time()
        execute_media_task(frame, ffmpeg=ffmpeg, ffprobe=ffprobe, max_source_bytes=MEDIA_EDIT_MAX_SOURCE_BYTES,
                           timeout_seconds=120, allow_private=True, use_nvenc=use_nvenc, local_source=source)
        if (frame.output_meta.get("w"), frame.output_meta.get("h")) != (360, 640):
            problems.append(f"frame {frame.output_meta.get('w')}x{frame.output_meta.get('h')} != 360x640")
        result["ok"] = not problems
        result["error"] = "; ".join(problems)
    except Exception as exc:  # noqa: BLE001 - reported, never raised
        result["error"] = str(exc)[-600:]
    finally:
        result["seconds"] = round(time.time() - started, 2)
        shutil.rmtree(work, ignore_errors=True)
    return result


class MediaEditor:
    """The node's media-edit queue: `parallel` ffmpeg jobs at a time, M+ first, outputs kept until released."""

    def __init__(self, cfg: "Config"):
        self.cfg = cfg
        self.tasks: Dict[str, MediaTask] = {}
        self.lock = threading.RLock()
        self.wake = threading.Condition(self.lock)
        self.stopping = threading.Event()
        self.ffmpeg, self.ffprobe = find_ffmpeg(cfg.ffmpeg_exe, cfg.ffprobe_exe)
        self.selftest: Dict[str, object] = {}
        self.selftest_running = False
        self.completed = 0
        self.failed = 0
        self.work_root = cfg.media_edit_work_dir

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        import shutil
        os.makedirs(self.work_root, exist_ok=True)
        # Leftovers of a previous run: nothing can ask for them any more.
        for name in os.listdir(self.work_root):
            shutil.rmtree(os.path.join(self.work_root, name), ignore_errors=True)
        for index in range(self.cfg.media_edit_parallel):
            threading.Thread(target=self.worker_loop, name=f"media-worker-{index}", daemon=True).start()
        if self.cfg.media_edit_selftest_on_start:
            threading.Thread(target=self.run_selftest, name="media-selftest", daemon=True).start()

    def capable(self) -> bool:
        return bool(self.cfg.media_edit_enabled and self.ffmpeg and self.selftest.get("ok"))

    def run_selftest(self) -> Dict[str, object]:
        with self.lock:
            if self.selftest_running:
                return dict(self.selftest, running=True)
            self.selftest_running = True
        try:
            if not self.ffmpeg:
                self.ffmpeg, self.ffprobe = find_ffmpeg(self.cfg.ffmpeg_exe, self.cfg.ffprobe_exe)
            result = run_media_selftest(self.ffmpeg, self.ffprobe, self.work_root, self.cfg.media_edit_nvenc)
            if result.get("ok"):
                logger.info("media-edit selftest passed in %.1f s (encoder %s, ffmpeg %s)", result["seconds"],
                            result["encoder"], self.ffmpeg)
            else:
                logger.warning("media-edit selftest FAILED in %.1f s: %s (ffmpeg %s); media_edit not offered",
                               result["seconds"], result["error"], self.ffmpeg or "not found")
            with self.lock:
                self.selftest = result
            return dict(result)
        finally:
            with self.lock:
                self.selftest_running = False

    def shutdown(self) -> None:
        self.stopping.set()
        with self.lock:
            for task in self.tasks.values():
                task.cancel.set()
            self.wake.notify_all()

    # ------------------------------------------------------------- submit
    def submit(self, payload: object) -> Tuple[int, Dict[str, object]]:
        if not self.cfg.media_edit_enabled:
            return 503, {"error": "media_edit_disabled", "message": "media_edit_enabled is false on this node"}
        try:
            spec = parse_media_edit_request(payload)
            validate_media_source_url(str(spec["source_url"]), allow_private=self.cfg.media_edit_allow_private_urls)
        except RequestError as exc:
            return 400, {"error": "invalid_request", "message": str(exc)}
        if not self.capable():
            return 503, {"error": "media_edit_unavailable", "retryable": True,
                         "message": "ffmpeg selftest has not passed on this node: "
                                    + str(self.selftest.get("error") or "not run yet")}
        with self.lock:
            active = sum(1 for t in self.tasks.values() if t.active())
            if active >= self.cfg.media_edit_max_tasks:
                return 503, {"error": "media_edit_queue_full", "retryable": True,
                             "message": f"{active} media-edit task(s) already accepted on this node"}
            task = MediaTask(spec)
            self.tasks[task.task_id] = task
            self.wake.notify()
        logger.info("Accepted media-edit task %s (%s, tier %s, ref %s)", task.task_id, spec["media_kind"],
                    spec.get("tier") or "-", spec.get("client_ref") or "-")
        return 202, {"task_id": task.task_id, "status": "Pending", "workload_class": WORKLOAD_MEDIA_EDIT}

    def status(self, task_id: str) -> Optional[Dict[str, object]]:
        with self.lock:
            task = self.tasks.get(task_id)
            if task is None:
                return None
            body = task.to_dict()
            if task.status == "Pending":
                waiting = sorted((t for t in self.tasks.values() if t.status == "Pending"), key=MediaTask.rank)
                body["queue_position"] = 1 + waiting.index(task)
            return body

    def cancel(self, task_id: str) -> Optional[Dict[str, object]]:
        with self.lock:
            task = self.tasks.get(task_id)
            if task is None:
                return None
            if task.status == "Pending":
                task.status, task.stage, task.error = "Cancelled", "cancelled", "cancelled"
                task.completed_at = time.time()
            elif task.status == "Processing":
                task.cancel.set()
                process = task.process
                if process is not None:
                    _kill_process_tree(process)
            return task.to_dict()

    def output_file(self, task_id: str) -> Optional[str]:
        with self.lock:
            task = self.tasks.get(task_id)
            if task and task.status == "Completed" and not task.released and os.path.isfile(task.output_path):
                return task.output_path
        return None

    def release(self, task_id: str) -> bool:
        import shutil
        with self.lock:
            task = self.tasks.get(task_id)
            if task is None or task.active():
                return False
            task.released = True
            work = task.work_dir
        if work:
            shutil.rmtree(work, ignore_errors=True)
        return True

    # ------------------------------------------------------------- worker
    def _next(self) -> Optional[MediaTask]:
        with self.lock:
            while not self.stopping.is_set():
                waiting = [t for t in self.tasks.values() if t.status == "Pending"]
                if waiting:
                    task = min(waiting, key=MediaTask.rank)
                    task.status, task.stage, task.started_at = "Processing", "starting", time.time()
                    return task
                self.wake.wait(timeout=1.0)
        return None

    def worker_loop(self) -> None:
        import shutil
        while not self.stopping.is_set():
            task = self._next()
            if task is None:
                return
            task.work_dir = os.path.join(self.work_root, task.task_id)
            try:
                os.makedirs(task.work_dir, exist_ok=True)
                execute_media_task(task, ffmpeg=self.ffmpeg, ffprobe=self.ffprobe,
                                   max_source_bytes=self.cfg.media_edit_max_source_bytes,
                                   timeout_seconds=self.cfg.media_edit_timeout_seconds,
                                   allow_private=self.cfg.media_edit_allow_private_urls,
                                   use_nvenc=self.cfg.media_edit_nvenc)
                # Only the result stays: a 4 GB source is not kept while the backend fetches the output.
                for name in os.listdir(task.work_dir):
                    path = os.path.join(task.work_dir, name)
                    if path != task.output_path:
                        if os.path.isdir(path):
                            shutil.rmtree(path, ignore_errors=True)
                        else:
                            try:
                                os.remove(path)
                            except OSError:
                                pass
                with self.lock:
                    task.status, task.stage, task.progress = "Completed", "done", 100.0
                    task.completed_at = time.time()
                    self.completed += 1
                logger.info("Media task %s completed in %.1f s (%s, %s bytes)", task.task_id,
                            task.completed_at - (task.started_at or task.completed_at), task.encoder,
                            task.output_meta.get("bytes"))
            except MediaCancelled:
                with self.lock:
                    task.status, task.stage, task.error = "Cancelled", "cancelled", "cancelled"
                    task.completed_at = time.time()
                shutil.rmtree(task.work_dir, ignore_errors=True)
                logger.info("Media task %s cancelled", task.task_id)
            except Exception as exc:  # noqa: BLE001 - every failure fails the task
                with self.lock:
                    task.status, task.stage = "Failed", "failed"
                    task.error = str(exc)[-1800:] or exc.__class__.__name__
                    task.completed_at = time.time()
                    self.failed += 1
                shutil.rmtree(task.work_dir, ignore_errors=True)
                logger.warning("Media task %s failed: %s", task.task_id, str(exc).splitlines()[0][:300]
                               if str(exc) else exc.__class__.__name__)
            finally:
                task.process = None

    def prune(self) -> None:
        """Outputs nobody fetched go after media_edit_output_keep_seconds; finished records after 6 h."""
        import shutil
        now = time.time()
        drop = []
        with self.lock:
            for task in self.tasks.values():
                if task.active() or not task.completed_at:
                    continue
                if not task.released and now - task.completed_at > self.cfg.media_edit_output_keep_seconds:
                    task.released = True
                    if task.work_dir:
                        shutil.rmtree(task.work_dir, ignore_errors=True)
                if now - task.completed_at > max(self.cfg.task_retention_seconds,
                                                 self.cfg.media_edit_output_keep_seconds):
                    drop.append(task.task_id)
            for task_id in drop:
                self.tasks.pop(task_id, None)

    def summary(self) -> Dict[str, object]:
        with self.lock:
            running = [t for t in self.tasks.values() if t.status == "Processing"]
            pending = [t for t in self.tasks.values() if t.status == "Pending"]
            selftest = {k: v for k, v in self.selftest.items() if k != "output_meta"}
            return {
                "enabled": bool(self.cfg.media_edit_enabled),
                "capable": self.capable(),
                "parallel": self.cfg.media_edit_parallel,
                "running": len(running),
                "pending": len(pending),
                "free_slots": max(0, self.cfg.media_edit_parallel - len(running)),
                "max_tasks": self.cfg.media_edit_max_tasks,
                "completed": self.completed,
                "failed": self.failed,
                "ffmpeg": self.ffmpeg,
                "ffprobe": self.ffprobe,
                "nvenc": bool(self.selftest.get("nvenc")),
                "selftest": selftest,
                "selftest_running": self.selftest_running,
                "tasks": [t.to_dict() for t in sorted(running + pending, key=MediaTask.rank)][:20],
            }


# ----------------------------------------------------------------------- node
class Node:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.llama = LlamaServer(cfg)
        self.tasks: Dict[str, AiTask] = {}
        self.lock = threading.RLock()
        self._model_lock = threading.Lock()     # parallel_tasks: one model check / load / swap at a time
        self.work: "queue.Queue[str]" = queue.Queue()
        self.work_owner: "queue.Queue[str]" = queue.Queue()   # the owner's requests, taken first
        self._comfy_preempted_at = 0.0
        self._last_owner = -1e9
        self.running_task_id = ""
        self.comfy: Dict[str, object] = {"watched": bool(cfg.comfy_url), "online": False,
                                          "busy": False, "checked_at": 0.0}
        self.vram: Dict[str, object] = {"ok": False, "error": "not_checked", "checked_at": 0.0}
        self._token_cache: Tuple[float, str] = (-1.0, "")
        self.stopping = threading.Event()
        self.completed = 0
        self.failed = 0
        self.swaps = 0                          # model swaps on a node with a `models` list
        self.reload_pending = False             # POST /ai-reload while a task generated: stop after it
        self.reloads = 0
        self._comfy_freed_at = 0.0
        self._gate_wanted = False              # set by _ensure_model: waiting for ComfyUI to drain, or loading
        self._last_use = -1e9
        self.media = MediaEditor(cfg)           # ffmpeg jobs (2026-11-07): own queue, no model lock

    # --------------------------------------------------------------- token
    def token(self) -> str:
        """The bearer token, re-read when the token file changes. Never logged."""
        path = self.cfg.token_file
        if path:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                mtime = -2.0
            if mtime != self._token_cache[0]:
                value = ""
                if mtime >= 0:
                    try:
                        with open(path, encoding="utf-8-sig") as handle:
                            value = handle.read().strip()
                    except OSError:
                        value = ""
                self._token_cache = (mtime, value)
            if self._token_cache[1]:
                return self._token_cache[1]
        return str(os.getenv("AI_NODE_TOKEN") or "").strip()

    # ------------------------------------------------------------- probes
    def refresh_comfy(self) -> Dict[str, object]:
        state = probe_comfy(self.cfg)
        with self.lock:
            self.comfy = state
        return state

    def refresh_vram(self) -> Dict[str, object]:
        state = query_vram(self.cfg)
        with self.lock:
            self.vram = state
        return state

    def comfy_busy(self) -> bool:
        with self.lock:
            return bool(self.comfy.get("busy"))

    def can_free_comfy(self, comfy: Dict[str, object]) -> bool:
        """free_comfy_when_idle and ComfyUI is online with nothing running or queued."""
        return bool(self.cfg.free_comfy_when_idle and self.cfg.comfy_url and comfy.get("online")
                    and not comfy.get("busy") and not int(comfy.get("queue_remaining") or 0))

    def owner_warm(self) -> bool:
        """Within owner_warm_seconds of the owner's last request: the gate and the model stay held."""
        return time.monotonic() - self._last_owner < self.cfg.owner_warm_seconds

    def preempt_comfy(self) -> bool:
        """For the owner's request: ComfyUI's queued prompts are deleted and the running one interrupted; renderfin
        sends each back to Pending on another box under the same task id. At most every 15 s."""
        if not self.cfg.comfy_url or time.monotonic() - self._comfy_preempted_at < 15:
            return False
        self._comfy_preempted_at = time.monotonic()
        base = self.cfg.comfy_url
        try:
            queued = _get_json(base + "/queue", 5.0)
            ids = ([item[1] for item in (queued.get("queue_pending") or [])
                    if isinstance(item, (list, tuple)) and len(item) > 1] if isinstance(queued, dict) else [])
            if ids:
                _post_json(base + "/queue", {"delete": ids}, 10.0)
            _post_json(base + "/interrupt", {}, 10.0)
            logger.info("Owner priority: ComfyUI interrupted, %d queued prompt(s) deleted (renderfin requeues them)",
                        len(ids))
            return True
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning("ComfyUI preemption for the owner failed: %s", exc)
            return False

    def free_comfy(self) -> bool:
        """Ask the idle ComfyUI to unload its models and free its VRAM; at most once per 30 s."""
        if time.monotonic() - self._comfy_freed_at < 30:
            return False
        self._comfy_freed_at = time.monotonic()
        try:
            request = urllib.request.Request(
                self.cfg.comfy_url + "/free", method="POST",
                data=json.dumps({"unload_models": True, "free_memory": True}).encode("utf-8"),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=15.0) as response:
                response.read()
            logger.info("Asked the idle ComfyUI to free its VRAM for an AI task")
            return True
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning("ComfyUI /free failed: %s", exc)
            return False

    def hold_gate(self, on: bool) -> None:
        """Take / renew (every 20 s, TTL 60 s) or release this node's lease on the GPU gate."""
        url = self.cfg.gpu_gate_url
        if not url:
            return
        now = time.monotonic()
        held = getattr(self, "_gate_held", False)
        if on and held and now - getattr(self, "_gate_at", 0.0) < 20:
            return
        if not on and not held:
            return
        body = ({"owner": self.cfg.gpu_gate_owner, "ttl": 60, "reason": f"{self.cfg.model_id} needs the card"}
                if on else {"owner": self.cfg.gpu_gate_owner})
        try:
            request = urllib.request.Request(url + ("/lease" if on else "/release"), method="POST",
                                             data=json.dumps(body).encode("utf-8"),
                                             headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=5.0) as response:
                response.read()
            if on and not held:
                logger.info("GPU gate lease taken: renderfin sends no new render to this card")
            elif not on:
                logger.info("GPU gate lease released")
            self._gate_held, self._gate_at = on, now
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning("GPU gate %s failed: %s", "lease" if on else "release", exc)
            if not on:
                self._gate_held = False   # the gate lets an unrenewed lease expire within its TTL anyway

    def _active_tasks(self) -> List[AiTask]:
        return [t for t in self.tasks.values() if t.active()]

    def hold_lease(self, on: bool, gate: Optional[bool] = None) -> None:
        """gpu_lease_hold_file: written (fresh mtime) while this node waits for, loads or holds its model, removed
        once it has let the card go, so a yielding node on the same card stays out of the way meanwhile. With
        gpu_gate_url the same span holds a lease on the card's GPU gate (renderfin sends no new render meanwhile)."""
        self.hold_gate(on if gate is None else gate)
        path = self.cfg.gpu_lease_hold_file
        if not path:
            return
        try:
            if on:
                tmp = f"{path}.tmp{os.getpid()}"
                with open(tmp, "w", encoding="utf-8") as handle:
                    json.dump({"node": self.cfg.node_name, "model": self.cfg.model_id, "pid": os.getpid(),
                               "ts": time.time(), "need_mb": self.cfg.need_mb()}, handle)
                os.replace(tmp, path)
            elif os.path.exists(path):
                os.remove(path)
        except OSError as exc:
            logger.warning("GPU lease file %s: %s", path, exc)

    def blocker(self, model_id: str = "") -> Optional[Tuple[str, str]]:
        """Why a new submission would be refused right now, from the last probes."""
        cfg = self.cfg
        if not cfg.installed():
            return ("runtime_not_installed", "llama-server or the weights are missing")
        with self.lock:
            comfy = dict(self.comfy)
            vram = dict(self.vram)
            active = len(self._active_tasks())
        game = running_game(cfg)
        if game:
            return ("owner_game", f"the owner is playing ({game}): the GPU is his")
        if comfy.get("busy"):
            if comfy.get("gpu_lease"):
                return ("gpu_leased", f"{comfy.get('gpu_lease')} holds this card (gpu_lease_yield_files)")
            if comfy.get("online"):
                return ("gpu_busy_comfyui",
                        f"ComfyUI has {comfy.get('queue_remaining', 0)} prompt(s) running "
                        "or queued; the GPU is ComfyUI's first")
            return ("gpu_busy_comfyui", "ComfyUI is unreachable and treated as busy")
        if active >= cfg.max_tasks:
            return ("queue_full", f"{active} AI task(s) already accepted on this node")
        if not self.llama.loaded_model_id() and active == 0:
            if not vram.get("ok"):
                return ("vram_unknown", f"cannot read free VRAM: {vram.get('error')}")
            # A node with several models checks the model the request asked for, not the selected one.
            model_need = (cfg.profile_need_mb(model_id) if model_id and model_id.lower() != cfg.model_id.lower()
                          else cfg.min_need_mb())
            need = model_need + cfg.vram_margin_mb
            if int(vram.get("free_mb") or 0) < need and not self.can_free_comfy(comfy):
                return ("insufficient_vram",
                        f"{vram.get('free_mb')} MiB free, model needs {model_need} MiB "
                        f"+ {cfg.vram_margin_mb} MiB margin")
        return None

    def load_blocker(self) -> Optional[str]:
        """Fresh check before llama-server is started: ComfyUI idle, VRAM free."""
        comfy = self.refresh_comfy()
        if comfy.get("busy"):
            return "comfyui_busy"
        vram = self.refresh_vram()
        if not vram.get("ok"):
            return f"vram_unknown ({vram.get('error')})"
        need = self.cfg.need_mb() + self.cfg.vram_margin_mb
        if int(vram.get("free_mb") or 0) < need and self.can_free_comfy(comfy) and self.free_comfy():
            for _ in range(10):                   # ComfyUI hands the memory back within a few seconds
                time.sleep(1.0)
                vram = self.refresh_vram()
                if not vram.get("ok") or int(vram.get("free_mb") or 0) >= need:
                    break
            if not vram.get("ok"):
                return f"vram_unknown ({vram.get('error')})"
        if int(vram.get("free_mb") or 0) < need:
            short = (f"insufficient_vram ({vram.get('free_mb')} MiB free < "
                     f"{self.cfg.need_mb()} + {self.cfg.vram_margin_mb} MiB)")
            if self.cfg.fit_when_short and int(vram.get("free_mb") or 0) >= \
                    self.cfg.min_need_mb() + self.cfg.vram_margin_mb:
                self.llama.fit = True              # the game / the desktop keep some GB: part of the layers in RAM
                logger.info("%s: loading with --fit, part of the layers in RAM", short)
                return None
            return short
        self.llama.fit = False
        return None

    # ------------------------------------------------------------- submit
    def submit(self, mode: str, payload: object, url_root: str) -> Tuple[int, Dict[str, object]]:
        """Admission for both endpoints (converter _submit_ai_task, 60101f4)."""
        cfg = self.cfg
        if not isinstance(payload, dict):
            return 400, {"error": "invalid_request", "message": "JSON object required"}
        requested_model = str(payload.get("model") or "").strip()
        # No model named = the primary one. A node with a `models` list serves
        # each listed model that is installed; one model is loaded at a time.
        profile = cfg.profile(requested_model or cfg.primary_model_id)
        if profile is None or not cfg.profile_installed(str(profile["model_id"])):
            # An unknown id and a model this node does not carry are the same
            # thing to a caller: it cannot be served here, so say which can.
            return 503, {"error": "runtime_not_installed",
                         "requested_model": requested_model,
                         "available_models": cfg.installed_model_ids()}
        task_model = str(profile["model_id"])
        try:
            prompt = validate_prompt(payload.get("prompt"), cfg.max_prompt_chars)
            system_prompt = str(payload.get("system_prompt") or "").strip()
            if len(system_prompt) + len(prompt) > cfg.max_prompt_chars:
                raise RequestError(
                    f"system_prompt and prompt exceed {cfg.max_prompt_chars} characters")
            max_output_tokens = validate_max_tokens(
                payload.get("max_output_tokens"), int(profile["max_output_tokens"]))
            temperature = validate_temperature(payload.get("temperature"))
            image_url = ""
            if mode == "vision":
                image_url = str(payload.get("image_url") or "").strip()
                if not image_url:
                    raise RequestError("image_url is required")
                validate_image_url(image_url, allow_private=cfg.allow_private_image_urls)
        except (RequestError, ValueError) as exc:
            return 400, {"error": "invalid_request", "message": str(exc)}

        # Fresh probes outside the lock: a /prompt call and nvidia-smi.
        self.refresh_comfy()
        if not self.llama.loaded_model_id():
            self.refresh_vram()
        owner = cfg.owner_priority and str(payload.get("priority") or "").strip().lower() == "owner"
        with self.lock:
            blocked = self.blocker(task_model)
            if blocked and owner and blocked[0] in ("gpu_busy_comfyui", "gpu_leased", "insufficient_vram"):
                blocked = None                 # the owner's request takes the card
            if blocked:
                code, message = blocked
                return 503, {"error": code, "message": message, "retryable": True,
                             "status": "capacity_wait"}
            task = AiTask(
                mode=mode, prompt=prompt, system_prompt=system_prompt, image_url=image_url,
                model=task_model, requested_model=requested_model,
                max_output_tokens=max_output_tokens,
                queue_class=str(payload.get("queue_class") or "interactive"),
                backend_task_id=str(payload.get("backend_task_id") or ""),
                temperature=temperature,
                priority="owner" if owner else "",
            )
            self.tasks[task.task_id] = task
            if owner:
                self._last_owner = time.monotonic()      # the gate is held from now on: no new render here
                self._gate_wanted = True
            (self.work_owner if owner else self.work).put(task.task_id)
        logger.info("Accepted %s task %s (prompt %d chars, system %d chars, max_tokens %s)",
                    mode, task.task_id, len(prompt), len(system_prompt), max_output_tokens)
        return 202, {
            "task_id": task.task_id,
            "status": "Pending",
            "status_url": url_root.rstrip("/") + f"{API_PREFIX}/ai-vision/status/{task.task_id}",
            "mode": mode,
            "model": task_model,
            "queue_class": task.queue_class,
            "workload_class": WORKLOAD_AI_VISION,
            "worker_process_hint": task.task_id,
        }

    def task_status(self, task_id: str) -> Optional[Dict[str, object]]:
        with self.lock:
            task = self.tasks.get(task_id)
            return task.to_dict() if task else None

    # ------------------------------------------------------------- worker
    def worker_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                task_id = self.work_owner.get_nowait()        # the owner's requests first
            except queue.Empty:
                try:
                    task_id = self.work.get(timeout=1.0)
                except queue.Empty:
                    continue
            with self.lock:
                task = self.tasks.get(task_id)
                if task is None or task.status != "Pending":
                    continue
                task.status = "Processing"
                task.current_stage = "Processing"
                task.started_at = time.time()
                self.running_task_id = task_id
            self.llama.acquire()
            try:
                self._process(task)
                with self.lock:
                    task.status = "Completed"
                    task.current_stage = "Completed"
                    task.progress = 100
                    task.completed_at = time.time()
                    self.completed += 1
                logger.info("Task %s completed in %.1f s", task.task_id,
                            task.completed_at - (task.started_at or task.completed_at))
            except Exception as exc:  # noqa: BLE001 - every failure fails the task
                with self.lock:
                    task.status = "Failed"
                    task.current_stage = "Failed"
                    task.error = str(exc)
                    task.completed_at = time.time()
                    self.failed += 1
                logger.warning("Task %s failed: %s", task.task_id, exc)
            finally:
                self.llama.release()
                with self.lock:
                    self.running_task_id = ""
                if task.priority == "owner":
                    self._last_owner = time.monotonic()  # the conversation goes on: keep the card warm
                if self.reload_pending and self.llama.stop_if_idle("reload requested (garbage answers)"):
                    self.reload_pending = False
                    self.reloads += 1
                    logger.warning("llama-server stopped after task %s for the requested reload", task.task_id)
                # Hand the card back now when ComfyUI wants it or when there is
                # no keepalive; otherwise the monitor unloads after keepalive.
                if not self.owner_warm() and (self.comfy_busy() or self.cfg.keepalive_seconds <= 0):
                    if self.llama.stop_if_idle(
                            "comfyui_busy" if self.comfy_busy() else "keepalive 0"):
                        logger.info("Model unloaded after task %s", task.task_id)

    def _process(self, task: AiTask) -> None:
        """process_ai_vision_task (60101f4) with a ComfyUI-aware model load."""
        task.current_stage = "loading_model"
        image_bytes = None
        media_type = "image/png"
        if task.mode == "vision":
            task.current_stage = "downloading_image"
            image_bytes, media_type = fetch_task_image(
                task.image_url, allow_private=self.cfg.allow_private_image_urls)
        # One task at a time picks / checks / loads the model; with parallel_tasks > 1
        # the others wait here and then find it answering. Generation runs outside.
        with self._model_lock:
            self._select_task_model(task)
            if task.mode == "vision" and not self.cfg.vision_installed():
                raise NodeError("vision_projector_not_installed")
            self._ensure_model(task)
        task.current_stage = "generating"
        task.progress = 10
        answer, reasoning, usage = self.llama.run_completion(
            task.prompt,
            system_prompt=task.system_prompt,
            image_bytes=image_bytes,
            image_media_type=media_type,
            max_output_tokens=task.max_output_tokens or None,
            **({"temperature": task.temperature} if task.temperature is not None else {}),
        )
        task.answer = answer
        task.reasoning = reasoning
        task.model_usage = usage

    def request_reload(self, reason: str = "") -> Dict[str, object]:
        """POST /ai-reload: restart llama-server (the backend asks after garbage answers like "????").

        The server is stopped now when no task is generating, otherwise right after
        the running task; the next task loads the model fresh. Nothing is queued
        or lost: pending tasks simply load it.
        """
        label = "reload requested" + (f" ({reason})" if reason else "")
        if not self.llama.running():
            self.reload_pending = False
            return {"reload": "not_loaded"}
        if self.llama.stop_if_idle(label):
            self.reload_pending = False
            self.reloads += 1
            logger.warning("llama-server stopped: %s", label)
            return {"reload": "stopped"}
        self.reload_pending = True
        logger.warning("llama-server will stop after the running task: %s", label)
        return {"reload": "after_running_task"}

    def _select_task_model(self, task: AiTask) -> None:
        """A node with several models: put the task's model in place. Called under _model_lock; with
        parallel_tasks > 1 it first waits until no other task is generating on the loaded model (every
        running task holds the server, this one included). Same model = nothing to do."""
        cfg = self.cfg
        wanted = str(task.model or cfg.primary_model_id)
        if wanted.lower() == cfg.model_id.lower():
            return
        while self.llama.users() > 1 and self.llama.running():
            task.current_stage = "waiting_for_model_swap"
            time.sleep(0.5)
        if self.llama.running():
            self.llama.stop(f"swap {cfg.model_id} -> {wanted}")
        with self.lock:
            cfg.select_model(wanted)
        self.swaps += 1
        logger.info("Model swap for task %s: now %s", task.task_id, cfg.model_id)

    def _ensure_model(self, task: AiTask) -> None:
        """Have the model answering, but only while ComfyUI leaves the GPU alone.

        A queued task that finds ComfyUI busy (or the VRAM taken) waits up to
        comfy_wait_seconds, then fails with gpu_unavailable.
        """
        deadline = time.monotonic() + self.cfg.comfy_wait_seconds
        while True:
            game = running_game(self.cfg)
            if game:                           # the owner plays: no model, no gate
                self._gate_wanted = False
                self.hold_lease(True, gate=False)
                if self.llama.running() and not self.llama.in_use():
                    self.llama.stop(f"owner_game {game}")
                reason = f"owner_game ({game})"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise NodeError(f"gpu_unavailable: {reason}; waited {self.cfg.comfy_wait_seconds:.0f} s")
                task.current_stage = "waiting_for_gpu"
                task.wait_reason = reason
                time.sleep(min(5.0, max(0.1, remaining)))
                continue
            self._gate_wanted = True           # waiting for ComfyUI to drain, or loading: renderfin sends nothing new
            self.hold_lease(True)
            if self.refresh_comfy().get("busy"):
                if self.cfg.owner_preempt_comfy and (task.priority == "owner" or not self.work_owner.empty()):
                    # the owner waits: ComfyUI's prompts go back to renderfin's queue, another box renders them
                    self.preempt_comfy()
                    reason = "comfyui_preempted_for_owner"
                else:
                    # ComfyUI first: even a warm model is put away before the next
                    # queued task would hold the card next to a running prompt -
                    # but never under another task still generating (parallel_tasks).
                    if self.llama.running() and self.llama.users() <= 1:
                        self.llama.stop("comfyui_busy before a queued task")
                    reason = "comfyui_busy"
            else:
                if self.llama.loaded_model_id():
                    others = self.llama.users() > 1
                    if self.llama.answering(timeout=10.0 if others else 3.0):
                        task.wait_reason = ""
                        return
                    if others:                 # busy serving another task: wait, do not kill it
                        task.current_stage = "waiting_for_gpu"
                        task.wait_reason = "model busy with another task"
                        time.sleep(1.0)
                        continue
                    # Alive but silent: free its VRAM before measuring.
                    self.llama.stop("not answering")
                reason = self.load_blocker()
                if reason is not None and not self.comfy_busy():
                    # ComfyUI is idle and freed, yet the VRAM is short (the desktop, another app): renders may run
                    self._gate_wanted = False
                    self.hold_lease(True, gate=False)
                if reason is None:
                    task.current_stage = "loading_model"
                    task.wait_reason = ""
                    try:
                        # The monitor refreshes ComfyUI's state every poll.
                        self.llama.start(abort_if=self.comfy_busy)
                        return
                    except ComfyBusy:
                        reason = "comfyui_busy"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise NodeError(
                    f"gpu_unavailable: {reason}; waited {self.cfg.comfy_wait_seconds:.0f} s "
                    "for the GPU")
            task.current_stage = "waiting_for_gpu"
            task.wait_reason = reason
            time.sleep(min(self.cfg.comfy_poll_seconds, max(0.1, remaining)))

    # ------------------------------------------------------------ monitor
    def monitor_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                comfy = self.refresh_comfy()
                self.llama.reap()
                # Free VRAM only gates a cold load; skip nvidia-smi while warm.
                if not self.llama.running():
                    self.refresh_vram()
                if self.owner_warm():
                    pass                       # the owner's conversation: the model stays, ComfyUI gets nothing new
                elif comfy.get("busy"):
                    if self.llama.stop_if_idle("comfyui_busy"):
                        logger.info("ComfyUI is busy; model unloaded to free VRAM")
                elif self.cfg.keepalive_seconds > 0:
                    if self.llama.stop_if_idle("keepalive expired",
                                               min_idle=self.cfg.keepalive_seconds):
                        logger.info("Model unloaded after %s s idle", self.cfg.keepalive_seconds)
                with self.lock:
                    wanted = bool(self._active_tasks())
                if self.llama.in_use():
                    self._last_use = time.monotonic()
                if not wanted:
                    self._gate_wanted = False
                game = running_game(self.cfg)
                if game and self.llama.stop_if_idle(f"owner_game {game}"):
                    logger.info("The owner plays %s: model unloaded", game)
                recent = time.monotonic() - self._last_use < self.cfg.gpu_gate_grace_seconds
                warm = self.owner_warm()
                gate = not game and ((wanted and self._gate_wanted) or self.llama.in_use() or recent or warm)
                self.hold_lease(wanted or self.llama.running() or warm, gate=gate)
                self._prune()
                self.media.prune()
            except Exception:  # noqa: BLE001 - the monitor must keep running
                logger.exception("Monitor pass failed")
            self.stopping.wait(self.cfg.comfy_poll_seconds)

    def _prune(self) -> None:
        cutoff = time.time() - self.cfg.task_retention_seconds
        with self.lock:
            finished = sorted((t for t in self.tasks.values() if not t.active()),
                              key=lambda t: t.completed_at or 0)
            drop = [t.task_id for t in finished if (t.completed_at or 0) < cutoff]
            excess = len(finished) - len(drop) - 1000
            if excess > 0:
                drop += [t.task_id for t in finished if t.task_id not in drop][:excess]
            for task_id in drop:
                self.tasks.pop(task_id, None)

    # -------------------------------------------------------------- status
    def server_status(self) -> Dict[str, object]:
        cfg = self.cfg
        blocked = self.blocker()
        with self.lock:
            processing = [t for t in self.tasks.values() if t.status == "Processing"]
            pending = sorted((t for t in self.tasks.values() if t.status == "Pending"),
                             key=lambda t: t.created_at)
            comfy = dict(self.comfy)
            vram = dict(self.vram)
            completed, failed = self.completed, self.failed
        loaded = self.llama.loaded_model_id()
        installed = cfg.installed()
        accepting = blocked is None
        models = []
        for model_id in cfg.installed_model_ids():          # one entry per installed profile, primary first
            profile = cfg.profile(model_id) or {}
            mmproj = str(profile.get("mmproj") or "")
            models.append({
                "id": model_id,
                "title": profile.get("title") or model_id,
                "vision": bool(mmproj) and os.path.isfile(mmproj),
                "uncensored": bool(profile.get("uncensored")),
                "context_tokens": int(profile.get("context_tokens") or 0),
                "system_prompt_supported": bool(profile.get("system_prompt_supported")),
                "unlimited_output_supported": True,
            })
        installed = installed or bool(models)
        return {
            "status": "ok",
            "service": "autorig-ai-node",
            "server_version": VERSION,
            "physical_node": cfg.node_name,
            "hostname": socket.gethostname(),
            "timestamp": time.time(),
            "gpu_mode": "ai_node",
            "comfy_online": bool(comfy.get("online")),
            "comfy_running": int(comfy.get("running") or 0),
            "comfy_pending": int(comfy.get("pending") or 0),
            "accepting_ai_vision": accepting,
            "accepting_media_edit": self.media.capable(),
            "capabilities": ["ai_vision"] + (["media_edit"] if self.media.capable() else []),
            "accepting_autorig": False,
            "accepting_hunyuan": False,
            # Same shape as BonsaiAdapter.status() (60101f4, lines 275-299).
            "ai_models": {
                "enabled": True,
                "installed": installed,
                "running": self.llama.running(),
                "loaded_model": loaded,
                "system_prompt_supported": True,
                "system_prompt_models": [m["id"] for m in models if m["system_prompt_supported"]],
                "unlimited_output_supported": True,
                "base_url": cfg.llama_base_url,
                "keepalive_seconds": int(cfg.keepalive_seconds),
                "models": models,
            },
            "tasks_summary": {
                "processing": len(processing),
                "pending": len(pending),
                "stuck": 0,
                "failed": failed,
                "completed": completed,
                "preempted": 0,
                # The backend sorts nodes by queue_size + processing. A node
                # that would refuse right now reports a load no idle node has.
                "queue_size": len(pending) if accepting else BUSY_QUEUE_SIZE,
            },
            "processing_tasks": [t.brief() for t in processing],
            "pending_tasks": [t.brief() for t in pending],
            # Kept apart from tasks_summary / processing_tasks: ffmpeg does not hold the GPU, so it must not make
            # the backend's Vision picker think the model is busy.
            "media_edit": self.media.summary(),
            "ai_node": {
                "blocker": blocked[0] if blocked else "",
                "blocker_message": blocked[1] if blocked else "",
                "comfy": comfy,
                "vram": vram,
                "vram_need_mb": cfg.need_mb(),
                "vram_margin_mb": cfg.vram_margin_mb,
                "llama_pid": self.llama.pid(),
                "llama_port": cfg.llama_port,
                "loaded_at": self.llama.loaded_at if loaded else None,
                "idle_seconds": round(self.llama.idle_seconds(), 1),
                "loads": self.llama.loads,
                "selected_model": cfg.model_id,
                "model_swaps": self.swaps,
                "reloads": self.reloads,
                "reload_pending": self.reload_pending,
                "max_tasks": cfg.max_tasks,
                "max_prompt_chars": cfg.max_prompt_chars,
                "context_tokens": cfg.context_tokens,
                "fit_when_short": cfg.fit_when_short,
                "owner_priority": cfg.owner_priority,
                "owner_warm_left": round(max(0.0, cfg.owner_warm_seconds - (time.monotonic() - self._last_owner)), 1),
                "owner_queue": self.work_owner.qsize(),
                "llama_fit": bool(self.llama.fit and loaded),
                "gpu_lease_hold_file": cfg.gpu_lease_hold_file,
                "gpu_lease_yield_to": comfy.get("gpu_lease") or "",
                "owner_game": running_game(cfg),
                "last_error": self.llama.last_error,
                "last_stop_reason": self.llama.last_stop_reason,
            },
        }

    def shutdown(self) -> None:
        self.stopping.set()
        self.media.shutdown()
        self.llama.stop("ai_node shutting down")
        self.hold_lease(False)


# ----------------------------------------------------------------------- HTTP
class AiNodeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    # On Windows SO_REUSEADDR would let a second instance bind the same port.
    allow_reuse_address = False

    def __init__(self, address, handler, node: Node):
        super().__init__(address, handler)
        self.node = node

    def handle_error(self, request, client_address):
        # A caller that hung up mid-response is not worth a traceback.
        logger.debug("Connection from %s failed", client_address, exc_info=True)


class Handler(BaseHTTPRequestHandler):
    server_version = "AutoRigAINode/1"
    protocol_version = "HTTP/1.0"

    @property
    def node(self) -> Node:
        return self.server.node  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # noqa: D401 - quiet; no headers logged
        logger.debug("%s %s", self.address_string(), fmt % args)

    def _send(self, code: int, payload: Dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        ok, reason = validate_bearer_header(self.headers.get("Authorization", ""),
                                            self.node.token())
        if ok:
            return True
        if reason == "token_not_configured":
            self._send(503, {"error": reason, "message": "AI node token is not configured"})
        else:
            self._send(401, {"error": "unauthorized"})
        return False

    def _path(self) -> str:
        return urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"

    def _url_root(self) -> str:
        host = self.headers.get("Host") or f"127.0.0.1:{self.node.cfg.listen_port}"
        return f"http://{host}/"

    def do_GET(self):  # noqa: N802
        path = self._path()
        try:
            if path == "/healthz":
                self._send(200, {"status": "ok", "service": "autorig-ai-node"})
                return
            if path == API_PREFIX + "/server-status":
                if self._authorized():
                    self._send(200, self.node.server_status())
                return
            if path.startswith(API_PREFIX + "/media-edit") or path == API_PREFIX + "/media-edit-selftest":
                if self._authorized():
                    self._media_get(path)
                return
            for prefix in (API_PREFIX + "/ai-vision/status/", API_PREFIX + "/text2text/status/"):
                if path.startswith(prefix):
                    if not self._authorized():
                        return
                    status = self.node.task_status(path[len(prefix):])
                    if status is None:
                        self._send(404, {"error": "Task not found"})
                    else:
                        self._send(200, status)
                    return
            self._send(404, {"error": "not_found"})
        except Exception as exc:  # noqa: BLE001
            logger.exception("GET %s failed", path)
            self._send(500, {"error": "internal_error", "message": str(exc)})

    def do_POST(self):  # noqa: N802
        path = self._path()
        modes = {API_PREFIX + "/ai-vision": "vision", API_PREFIX + "/text2text": "text"}
        try:
            if path == API_PREFIX + "/ai-reload":
                if not self._authorized():
                    return
                try:
                    length = min(int(self.headers.get("Content-Length") or 0), 4096)
                except ValueError:
                    length = 0
                raw = self.rfile.read(length) if length > 0 else b""
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                except (UnicodeDecodeError, ValueError):
                    payload = {}
                if not isinstance(payload, dict) or payload.get("reload") is not True:
                    self._send(400, {"error": "invalid_request", "message": 'send {"reload": true}'})
                    return
                self._send(200, dict(self.node.request_reload(str(payload.get("reason") or "")[:80]),
                                     version=VERSION))
                return
            if path.startswith(API_PREFIX + "/media-edit"):
                if self._authorized():
                    self._media_post(path)
                return
            if path not in modes:
                self._send(404, {"error": "not_found"})
                return
            if not self._authorized():
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if length < 0 or length > MAX_BODY_BYTES:
                self._send(413, {"error": "invalid_request",
                                 "message": "request body too large or unsized"})
                return
            raw = self.rfile.read(length) if length else b""
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except (UnicodeDecodeError, ValueError):
                self._send(400, {"error": "invalid_request", "message": "body is not JSON"})
                return
            code, body = self.node.submit(modes[path], payload, self._url_root())
            self._send(code, body)
        except Exception as exc:  # noqa: BLE001
            logger.exception("POST %s failed", path)
            self._send(500, {"error": "internal_error", "message": str(exc)})


    # ---------------------------------------------------------- media edit
    def _media_get(self, path: str) -> None:
        media = self.node.media
        if path == API_PREFIX + "/media-edit-selftest":
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            if query.get("run") == ["1"] or not media.selftest:
                self._send(200, dict(media.run_selftest(), version=VERSION))
            else:
                self._send(200, dict(media.selftest, version=VERSION, running=media.selftest_running))
            return
        status_prefix = API_PREFIX + "/media-edit/status/"
        output_prefix = API_PREFIX + "/media-edit/output/"
        if path.startswith(status_prefix):
            body = media.status(path[len(status_prefix):])
            if body is None:
                self._send(404, {"error": "Task not found"})
            else:
                self._send(200, body)
            return
        if path.startswith(output_prefix):
            file_path = media.output_file(path[len(output_prefix):])
            if not file_path:
                self._send(404, {"error": "output_not_available"})
                return
            ext = os.path.splitext(file_path)[1].lstrip(".").lower()
            size = os.path.getsize(file_path)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPES.get(ext, "application/octet-stream"))
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition", f'attachment; filename="result.{ext}"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with open(file_path, "rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            return
        if path == API_PREFIX + "/media-edit":
            self._send(200, media.summary())
            return
        self._send(404, {"error": "not_found"})

    def _media_post(self, path: str) -> None:
        media = self.node.media
        if path == API_PREFIX + "/media-edit-selftest":
            self._send(200, dict(media.run_selftest(), version=VERSION))
            return
        for prefix, action in ((API_PREFIX + "/media-edit/cancel/", media.cancel),
                               (API_PREFIX + "/media-edit/release/", media.release)):
            if path.startswith(prefix):
                result = action(path[len(prefix):])
                if result is None or result is False:
                    self._send(404, {"error": "Task not found"})
                else:
                    self._send(200, result if isinstance(result, dict) else {"released": True})
                return
        if path != API_PREFIX + "/media-edit":
            self._send(404, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if length < 0 or length > MEDIA_EDIT_MAX_BODY_BYTES:
            self._send(413, {"error": "invalid_request", "message": "request body too large or unsized"})
            return
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, ValueError):
            self._send(400, {"error": "invalid_request", "message": "body is not JSON"})
            return
        code, body = media.submit(payload)
        self._send(code, body)


# ----------------------------------------------------------------------- main
def _setup_logging(cfg: Config) -> None:
    level = getattr(logging, cfg.log_level, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    if cfg.log_file:
        handler = logging.handlers.RotatingFileHandler(
            cfg.log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        handler.setFormatter(fmt)
        root.addHandler(handler)
    if sys.stderr is not None:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(fmt)
        root.addHandler(stream)


def run_check(cfg: Config) -> int:
    """--check: print what the node would do, without loading anything."""
    node = Node(cfg)
    report = {
        "config_unknown_keys": cfg.unknown_keys,
        "model_id": cfg.model_id,
        "llama_server": cfg.llama_server[0],
        "llama_server_exists": cfg.binary_installed(),
        "weights": cfg.weights,
        "weights_exists": os.path.isfile(cfg.weights),
        "mmproj": cfg.mmproj,
        "mmproj_exists": cfg.vision_installed(),
        "vram_need_mb": cfg.need_mb(),
        "vram_margin_mb": cfg.vram_margin_mb,
        "token_configured": bool(node.token()),
        "comfy": node.refresh_comfy(),
        "vram": node.refresh_vram(),
        "llama_port_busy": node.llama._port_open(),
        "launch_argv": node.llama.launch_argv(),
        "media_edit": {"enabled": cfg.media_edit_enabled, "ffmpeg": node.media.ffmpeg,
                       "ffprobe": node.media.ffprobe, "parallel": cfg.media_edit_parallel,
                       "work_dir": cfg.media_edit_work_dir},
    }
    if len(cfg.profiles) > 1:
        primary = cfg.model_id
        report["models"] = {}
        for profile in list(cfg.profiles.values()):
            cfg.select_model(str(profile["model_id"]))
            report["models"][cfg.model_id] = {
                "weights": cfg.weights, "weights_exists": os.path.isfile(cfg.weights),
                "mmproj": cfg.mmproj, "mmproj_exists": cfg.vision_installed(),
                "vram_need_mb": cfg.need_mb(), "launch_argv": node.llama.launch_argv()}
        cfg.select_model(primary)
    report["would_accept"] = node.blocker() is None
    report["blocker"] = node.blocker()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if cfg.installed() and report["token_configured"] else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="AutoRig AI node")
    parser.add_argument("--config", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.json"))
    parser.add_argument("--check", action="store_true",
                        help="print the effective setup and exit")
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    if args.check:
        return run_check(cfg)
    _setup_logging(cfg)
    if cfg.unknown_keys:
        logger.warning("Ignoring unknown config keys: %s", ", ".join(cfg.unknown_keys))
    node = Node(cfg)
    if not node.token():
        logger.warning("No token configured: every API call will get 503 token_not_configured")
    # A predecessor's server on our port would otherwise keep the VRAM forever.
    if node.llama._port_open():
        node.llama.release_stray()
    node.refresh_comfy()
    node.refresh_vram()
    try:
        server = AiNodeHTTPServer((cfg.listen_host, cfg.listen_port), Handler, node)
    except OSError as exc:
        logger.error("Cannot listen on %s:%s: %s", cfg.listen_host, cfg.listen_port, exc)
        return 2
    for index in range(cfg.parallel_tasks):
        threading.Thread(target=node.worker_loop, name=f"ai-worker-{index}", daemon=True).start()
    try:
        node.media.start()
    except OSError as exc:
        logger.error("media-edit not started (work dir %s): %s", cfg.media_edit_work_dir, exc)
    threading.Thread(target=node.monitor_loop, name="ai-monitor", daemon=True).start()
    serve = threading.Thread(target=server.serve_forever, name="http", daemon=True)
    serve.start()
    logger.info("%s serving %s on %s:%s (llama port %s, ComfyUI %s, need %s MiB + %s MiB)",
                VERSION, cfg.model_id, cfg.listen_host, cfg.listen_port, cfg.llama_port,
                cfg.comfy_url or "not watched", cfg.need_mb(), cfg.vram_margin_mb)

    def _on_signal(signum, _frame):
        logger.info("Signal %s: shutting down", signum)
        node.stopping.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            try:
                signal.signal(getattr(signal, name), _on_signal)
            except (OSError, ValueError):
                pass
    try:
        while not node.stopping.wait(0.5):
            pass
    except KeyboardInterrupt:
        pass
    node.shutdown()
    server.shutdown()
    server.server_close()
    logger.info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
