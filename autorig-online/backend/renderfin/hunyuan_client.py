"""Client for the converter workers' Hunyuan3D 2.1 image-to-3D API.

Contract: R:\\3d\\HUNYUAN_IMAGE_TO_3D_API.md —
POST {worker}/api-converter-glb/generate-3d  (Bearer HUNYUAN_API_TOKEN)
GET  {worker}/api-converter-glb/generate-3d/status/{task_id}
Status: Pending/Downloading/GeneratingShape/GeneratingPBR/Packaging/Completed/Failed
Completed payload carries output_urls: {model, previews, report}.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from . import config


class HunyuanClientError(RuntimeError):
    pass


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {config.HUNYUAN_API_TOKEN}"}


def is_configured() -> bool:
    return bool(config.HUNYUAN_API_TOKEN and config.HUNYUAN_WORKERS)


async def _worker_hunyuan_state(client: httpx.AsyncClient, worker: str) -> Optional[Dict[str, Any]]:
    try:
        resp = await client.get(f"{worker}/api-converter-glb/server-status", timeout=10.0)
        if resp.status_code != 200:
            return None
        data = resp.json()
        hunyuan = data.get("hunyuan")
        return hunyuan if isinstance(hunyuan, dict) else None
    except Exception:
        return None


async def pick_worker(client: httpx.AsyncClient) -> str:
    """Prefer an idle enabled worker; fall back to the first enabled one."""
    enabled: List[str] = []
    for worker in config.HUNYUAN_WORKERS:
        state = await _worker_hunyuan_state(client, worker)
        if not state or not state.get("enabled") or not state.get("installed"):
            continue
        if str(state.get("service_state") or "") == "idle":
            return worker
        enabled.append(worker)
    if enabled:
        return enabled[0]
    raise HunyuanClientError(
        f"no enabled Hunyuan worker among {', '.join(config.HUNYUAN_WORKERS)}"
    )


async def submit(
    client: httpx.AsyncClient,
    *,
    image_url: str,
    seed: Optional[int] = None,
    quality: Optional[str] = None,
    background_method: str = "auto",
) -> Tuple[str, str]:
    """Create a generation task. Returns (worker, status_url)."""
    worker = await pick_worker(client)
    body: Dict[str, Any] = {
        "image_url": image_url,
        "quality": quality or config.HUNYUAN_QUALITY,
        "background_method": background_method,
    }
    if seed:
        body["seed"] = int(seed) & 0xFFFFFFFF
    resp = await client.post(
        f"{worker}/api-converter-glb/generate-3d",
        json=body,
        headers=_headers(),
        timeout=30.0,
    )
    if resp.status_code not in (200, 202):
        raise HunyuanClientError(
            f"generate-3d on {worker} failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    payload = resp.json()
    status_url = str(payload.get("status_url") or "")
    task_id = str(payload.get("task_id") or "")
    if not status_url:
        if not task_id:
            raise HunyuanClientError(f"generate-3d returned no status_url/task_id: {payload}")
        status_url = f"{worker}/api-converter-glb/generate-3d/status/{task_id}"
    return worker, status_url


async def wait_for_model(
    client: httpx.AsyncClient,
    status_url: str,
    *,
    timeout: Optional[float] = None,
    on_progress=None,
) -> Dict[str, Any]:
    """Poll until Completed; returns the final status payload (with output_urls)."""
    deadline = time.time() + (timeout or config.HUNYUAN_TIMEOUT_SECONDS)
    last_status = ""
    while time.time() < deadline:
        try:
            resp = await client.get(status_url, headers=_headers(), timeout=30.0)
        except Exception as exc:
            print(f"[Renderfin][Hunyuan] status poll error: {exc}")
            await asyncio.sleep(config.HUNYUAN_POLL_SECONDS)
            continue
        if resp.status_code == 200:
            payload = resp.json()
            status = str(payload.get("status") or "")
            if status != last_status:
                last_status = status
                if on_progress:
                    try:
                        on_progress(status, payload)
                    except Exception:
                        pass
            if status == "Completed":
                outputs = payload.get("output_urls") or {}
                if not outputs.get("model"):
                    raise HunyuanClientError(f"Completed without model url: {payload}")
                return payload
            if status == "Failed":
                raise HunyuanClientError(
                    f"generation failed: {payload.get('error') or 'unknown error'}"
                )
        await asyncio.sleep(config.HUNYUAN_POLL_SECONDS)
    raise HunyuanClientError("generation timed out")


async def download_model(client: httpx.AsyncClient, model_url: str) -> bytes:
    resp = await client.get(model_url, headers=_headers(), timeout=300.0, follow_redirects=True)
    if resp.status_code != 200:
        raise HunyuanClientError(f"model download failed: HTTP {resp.status_code} {model_url}")
    if len(resp.content) < 1024:
        raise HunyuanClientError(f"model download suspiciously small: {len(resp.content)} bytes")
    return resp.content
