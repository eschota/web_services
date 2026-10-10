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

VERSION = "ai-node-20261011b"
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
}

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
    # Characters of system_prompt + prompt one request may carry. The backend's own limit is 8000; a node that only a
    # long-context caller talks to directly (the film director) raises it with its context_tokens.
    "max_prompt_chars": MAX_PROMPT_CHARS,
    "keepalive_seconds": 120,
    "max_tasks": 2,
    "comfy_wait_seconds": 300,
    "start_timeout_seconds": DEFAULT_START_TIMEOUT_SECONDS,
    "request_timeout_seconds": DEFAULT_REQUEST_TIMEOUT_SECONDS,
    "task_retention_seconds": 6 * 3600,
    "allow_private_image_urls": False,
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
        model_id = str(merged.get("model_id") or "").strip()
        if not model_id:
            raise ValueError("config: model_id is required")
        known = KNOWN_MODELS.get(model_id.lower(), {})
        for key, value in known.items():
            if merged.get(key) in (None, ""):
                merged[key] = value
        self.base_dir = base_dir
        self.node_name = str(merged["node_name"] or socket.gethostname()).strip()
        self.listen_host = str(merged["listen_host"] or "127.0.0.1")
        self.listen_port = int(merged["listen_port"])
        self.token_file = self._path(merged["token_file"]) if merged["token_file"] else ""
        self.model_id = model_id
        self.title = str(merged.get("title") or model_id)
        models_dir = self._path(merged["models_dir"]) if merged.get("models_dir") else (
            os.path.join(base_dir, "models"))
        weights = str(merged.get("weights") or "").strip()
        if not weights:
            raise ValueError("config: weights is required for a model the node does not know")
        self.weights = weights if os.path.isabs(weights) else os.path.join(models_dir, weights)
        mmproj = str(merged.get("mmproj") or "").strip()
        self.mmproj = (mmproj if (not mmproj or os.path.isabs(mmproj))
                       else os.path.join(models_dir, mmproj))
        self.context_tokens = int(merged.get("context_tokens") or 4096)
        self.max_output_tokens = int(merged.get("max_output_tokens") or 2048)
        self.reasoning = str(merged.get("reasoning") or "auto").strip().lower()
        self.uncensored = bool(merged.get("uncensored"))
        sp = merged.get("system_prompt_supported")
        self.system_prompt_supported = (model_id.lower() in SYSTEM_ROLE_VERIFIED_MODELS
                                        if sp is None else bool(sp))
        self.vram_need_mb = int(merged["vram_need_mb"]) if merged.get("vram_need_mb") else 0
        self.vram_overhead_mb = int(merged["vram_overhead_mb"])
        self.vram_margin_mb = int(merged["vram_margin_mb"])
        self.ngl = int(merged["ngl"])
        self.llama_server = self._argv(merged["llama_server"])
        if not self.llama_server:
            raise ValueError("config: llama_server is required")
        self.llama_port = int(merged["llama_port"])
        self.extra_llama_args = [str(a) for a in (merged.get("extra_llama_args") or [])]
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
        self.max_prompt_chars = max(1000, int(merged.get("max_prompt_chars") or MAX_PROMPT_CHARS))
        self.keepalive_seconds = int(merged["keepalive_seconds"])
        self.max_tasks = max(1, int(merged["max_tasks"]))
        self.comfy_wait_seconds = max(0.0, float(merged["comfy_wait_seconds"]))
        self.start_timeout_seconds = int(merged["start_timeout_seconds"])
        self.request_timeout_seconds = int(merged["request_timeout_seconds"])
        self.task_retention_seconds = int(merged["task_retention_seconds"])
        self.allow_private_image_urls = bool(merged["allow_private_image_urls"])
        self.log_file = self._path(merged["log_file"]) if merged.get("log_file") else ""
        self.log_level = str(merged.get("log_level") or "INFO").upper()

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
        if os.path.normcase(os.path.abspath(self.cfg.weights)) not in text and \
                os.path.normcase(self.cfg.weights) not in text:
            return False
        port = re.search(r'--port\s+"?%d"?(\s|$)' % self.cfg.llama_port, cmdline)
        alias = re.search(r'--alias\s+"?%s"?(\s|$)' % re.escape(self.cfg.model_id), cmdline)
        return bool(port and alias)

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
            if self.running() and self._loaded_model_id and self.answering():
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
                 queue_class: str, backend_task_id: str, temperature: Optional[float] = None):
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


# ----------------------------------------------------------------------- node
class Node:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.llama = LlamaServer(cfg)
        self.tasks: Dict[str, AiTask] = {}
        self.lock = threading.RLock()
        self.work: "queue.Queue[str]" = queue.Queue()
        self.running_task_id = ""
        self.comfy: Dict[str, object] = {"watched": bool(cfg.comfy_url), "online": False,
                                          "busy": False, "checked_at": 0.0}
        self.vram: Dict[str, object] = {"ok": False, "error": "not_checked", "checked_at": 0.0}
        self._token_cache: Tuple[float, str] = (-1.0, "")
        self.stopping = threading.Event()
        self.completed = 0
        self.failed = 0
        self._comfy_freed_at = 0.0
        self._gate_wanted = False              # set by _ensure_model: waiting for ComfyUI to drain, or loading
        self._last_use = -1e9

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

    def blocker(self) -> Optional[Tuple[str, str]]:
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
            need = cfg.min_need_mb() + cfg.vram_margin_mb
            if int(vram.get("free_mb") or 0) < need and not self.can_free_comfy(comfy):
                return ("insufficient_vram",
                        f"{vram.get('free_mb')} MiB free, model needs {cfg.min_need_mb()} MiB "
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
        if (requested_model and requested_model.lower() != cfg.model_id.lower()) or \
                not cfg.installed():
            # An unknown id and a model this node does not carry are the same
            # thing to a caller: it cannot be served here, so say which can.
            return 503, {"error": "runtime_not_installed",
                         "requested_model": requested_model,
                         "available_models": [cfg.model_id] if cfg.installed() else []}
        try:
            prompt = validate_prompt(payload.get("prompt"), cfg.max_prompt_chars)
            system_prompt = str(payload.get("system_prompt") or "").strip()
            if len(system_prompt) + len(prompt) > cfg.max_prompt_chars:
                raise RequestError(
                    f"system_prompt and prompt exceed {cfg.max_prompt_chars} characters")
            max_output_tokens = validate_max_tokens(
                payload.get("max_output_tokens"), cfg.max_output_tokens)
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
        with self.lock:
            blocked = self.blocker()
            if blocked:
                code, message = blocked
                return 503, {"error": code, "message": message, "retryable": True,
                             "status": "capacity_wait"}
            task = AiTask(
                mode=mode, prompt=prompt, system_prompt=system_prompt, image_url=image_url,
                model=cfg.model_id, requested_model=requested_model,
                max_output_tokens=max_output_tokens,
                queue_class=str(payload.get("queue_class") or "interactive"),
                backend_task_id=str(payload.get("backend_task_id") or ""),
                temperature=temperature,
            )
            self.tasks[task.task_id] = task
            self.work.put(task.task_id)
        logger.info("Accepted %s task %s (prompt %d chars, system %d chars, max_tokens %s)",
                    mode, task.task_id, len(prompt), len(system_prompt), max_output_tokens)
        return 202, {
            "task_id": task.task_id,
            "status": "Pending",
            "status_url": url_root.rstrip("/") + f"{API_PREFIX}/ai-vision/status/{task.task_id}",
            "mode": mode,
            "model": cfg.model_id,
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
                # Hand the card back now when ComfyUI wants it or when there is
                # no keepalive; otherwise the monitor unloads after keepalive.
                if self.comfy_busy() or self.cfg.keepalive_seconds <= 0:
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
            if not self.cfg.vision_installed():
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
                # ComfyUI first: even a warm model is put away before the next
                # queued task would hold the card next to a running prompt.
                if self.llama.running():
                    self.llama.stop("comfyui_busy before a queued task")
                reason = "comfyui_busy"
            else:
                if self.llama.loaded_model_id():
                    if self.llama.answering():
                        task.wait_reason = ""
                        return
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
                if comfy.get("busy"):
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
                gate = not game and ((wanted and self._gate_wanted) or self.llama.in_use() or recent)
                self.hold_lease(wanted or self.llama.running(), gate=gate)
                self._prune()
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
        if installed:
            models.append({
                "id": cfg.model_id,
                "title": cfg.title,
                "vision": cfg.vision_installed(),
                "uncensored": cfg.uncensored,
                "context_tokens": cfg.context_tokens,
                "system_prompt_supported": cfg.system_prompt_supported,
                "unlimited_output_supported": True,
            })
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
            "accepting_autorig": False,
            "accepting_hunyuan": False,
            # Same shape as BonsaiAdapter.status() (60101f4, lines 275-299).
            "ai_models": {
                "enabled": True,
                "installed": installed,
                "running": self.llama.running(),
                "loaded_model": loaded,
                "system_prompt_supported": True,
                "system_prompt_models": [cfg.model_id] if cfg.system_prompt_supported else [],
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
                "max_tasks": cfg.max_tasks,
                "max_prompt_chars": cfg.max_prompt_chars,
                "context_tokens": cfg.context_tokens,
                "fit_when_short": cfg.fit_when_short,
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
    }
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
    threading.Thread(target=node.worker_loop, name="ai-worker", daemon=True).start()
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
