"""Worker-local model inventory checks used before render dispatch."""
from __future__ import annotations

import asyncio
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

import httpx

from . import comfy_adapter
from .models import RenderPrompt, RenderServer

_CACHE_TTL = 30.0
_cache: Dict[Tuple[str, str], Tuple[float, Set[str]]] = {}


def _names(payload: object, class_name: str, input_name: str) -> Set[str]:
    if not isinstance(payload, dict):
        return set()
    try:
        spec = payload[class_name]["input"]["required"][input_name]
        values = spec[0]
        if values == "COMBO" and len(spec) > 1 and isinstance(spec[1], dict):
            values = spec[1].get("options") or []
    except (KeyError, IndexError, TypeError):
        return set()
    if not isinstance(values, (list, tuple, set)):
        return set()
    return {str(value) for value in values if isinstance(value, str)}


async def _slot(client: httpx.AsyncClient, server: RenderServer,
                class_name: str, input_name: str) -> Set[str]:
    key = (server.render_server_name, class_name)
    cached = _cache.get(key)
    now = time.monotonic()
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]
    base = comfy_adapter._validate_server_url(server.render_server_url)
    response = await client.get(
        f"{base}/object_info/{class_name}", timeout=10.0,
        auth=comfy_adapter._auth_for(server),
    )
    response.raise_for_status()
    result = _names(response.json(), class_name, input_name)
    _cache[key] = (now, result)
    return result


async def _optional_slot(client: httpx.AsyncClient, server: RenderServer,
                         class_name: str, input_name: str) -> Set[str]:
    """Names from a loader a worker is allowed not to have at all.

    ComfyUI-GGUF is a custom node package: on a box without it,
    /object_info/UnetLoaderGGUF is a 404. That is a fact about that box's node
    set, not an unreadable inventory, and treating it as a failure would
    fail-close every ordinary checkpoint on every box that has no GGUF loader.
    The empty answer is cached like any other so the 404 is asked for once per
    TTL rather than once per dispatch pass.
    """
    key = (server.render_server_name, class_name)
    try:
        return await _slot(client, server, class_name, input_name)
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        _cache[key] = (time.monotonic(), set())
        return set()


def _aliases() -> Dict[str, str]:
    try:
        import ai_model_defaults  # the backend package, when importable
        return dict(getattr(ai_model_defaults, "MODEL_FILE_ALIASES", {}) or {})
    except Exception:
        return {}


def equivalent_names(name: str) -> List[str]:
    """The same model file under every name a box may keep it as.

    Civitai and Hugging Face publish identical bytes under different names
    (CyberRealistic Pony v18 is `cyberrealisticPony_v180Coreshift_2764472` on
    one and `CyberRealisticPony_V18.0_F16` on the other). A box holding either
    can serve the request; `local_name` then picks the one it has.
    """
    name = str(name or "").strip()
    if not name:
        return []
    names = [name]
    for alias, canonical in _aliases().items():
        if name == canonical and alias not in names:
            names.append(alias)
        elif name == alias and canonical not in names:
            names.append(canonical)
    return names


def requested_loras(prompt: RenderPrompt) -> List[str]:
    names: List[str] = []
    single = str(getattr(prompt, "lora", "") or "").strip()
    if single:
        names.append(single)
    for item in getattr(prompt, "loras", None) or []:
        name = str(getattr(item, "name", None) or (item.get("name") if isinstance(item, dict) else "") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def local_name(server: RenderServer, kind: str, name: str) -> str:
    """The name this server keeps `name` under, from the last inventory read."""
    name = str(name or "").strip()
    if not name or kind != "checkpoint":
        return name
    present: Set[str] = set()
    for class_name in ("CheckpointLoaderSimple", "UNETLoader", "UnetLoaderGGUF"):
        cached = _cache.get((server.render_server_name, class_name))
        if cached:
            present |= cached[1]
    for candidate in equivalent_names(name):
        if candidate in present:
            return candidate
    return name


async def can_load(client: httpx.AsyncClient, server: RenderServer,
                   prompt: RenderPrompt) -> bool:
    """Fail closed when a selected file is absent or inventory is unreadable.

    Every LoRA of a stack must be on the box: a render that loaded some of
    them would be a different picture, not a degraded one.
    """
    checkpoint = str(prompt.checkpoint or "").strip()
    loras = requested_loras(prompt)
    upscale = str(getattr(prompt, "upscale_model", "") or "").strip()
    if upscale:
        # Upscale models are not on every image box (RealESRGAN_x2 was added
        # to f5/f15/Raptor on 2026-09-26): send the job only where it is.
        try:
            if upscale not in await _slot(client, server, "UpscaleModelLoader", "model_name"):
                return False
        except Exception:
            return False
    if not checkpoint and not loras:
        return True
    try:
        checkpoint_names, unet_names, gguf_names, lora_names, model_lora_names = await asyncio.gather(
            _slot(client, server, "CheckpointLoaderSimple", "ckpt_name")
            if checkpoint else _empty(),
            _slot(client, server, "UNETLoader", "unet_name")
            if checkpoint else _empty(),
            # A .gguf quantisation is listed by ComfyUI-GGUF's own loader and
            # by nothing else: UNETLoader filters the same folder by the
            # extensions core ComfyUI can read.
            _optional_slot(client, server, "UnetLoaderGGUF", "unet_name")
            if checkpoint else _empty(),
            _slot(client, server, "LoraLoader", "lora_name")
            if loras else _empty(),
            _slot(client, server, "LoraLoaderModelOnly", "lora_name")
            if loras else _empty(),
        )
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return False
    models = checkpoint_names | unet_names | gguf_names
    available_loras = lora_names | model_lora_names
    return ((not checkpoint
             or any(name in models for name in equivalent_names(checkpoint)))
            and all(name in available_loras for name in loras))


async def eligible_names(client: httpx.AsyncClient,
                         servers: Iterable[RenderServer],
                         prompt: RenderPrompt) -> Set[str]:
    servers = list(servers)
    verdicts = await asyncio.gather(
        *(can_load(client, server, prompt) for server in servers),
        return_exceptions=True,
    )
    return {server.render_server_name for server, verdict in zip(servers, verdicts)
            if verdict is True}


async def _empty() -> Set[str]:
    return set()
