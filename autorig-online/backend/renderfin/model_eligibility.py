"""Worker-local model inventory checks used before render dispatch."""
from __future__ import annotations

import asyncio
import time
from typing import Dict, Iterable, Optional, Set, Tuple

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


async def can_load(client: httpx.AsyncClient, server: RenderServer,
                   prompt: RenderPrompt) -> bool:
    """Fail closed when a selected file is absent or inventory is unreadable."""
    checkpoint = str(prompt.checkpoint or "").strip()
    lora = str(prompt.lora or "").strip()
    if not checkpoint and not lora:
        return True
    try:
        checkpoint_names, unet_names, lora_names, model_lora_names = await asyncio.gather(
            _slot(client, server, "CheckpointLoaderSimple", "ckpt_name")
            if checkpoint else _empty(),
            _slot(client, server, "UNETLoader", "unet_name")
            if checkpoint else _empty(),
            _slot(client, server, "LoraLoader", "lora_name")
            if lora else _empty(),
            _slot(client, server, "LoraLoaderModelOnly", "lora_name")
            if lora else _empty(),
        )
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return False
    return ((not checkpoint or checkpoint in checkpoint_names | unet_names)
            and (not lora or lora in lora_names | model_lora_names))


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
