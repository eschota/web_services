"""Client for the converter workers' Hunyuan3D 2.1 image-to-3D API.

Contract: R:\\3d\\HUNYUAN_IMAGE_TO_3D_API.md —
POST {worker}/api-converter-glb/generate-3d  (Bearer token, provisioned per box)
GET  {worker}/api-converter-glb/generate-3d/status/{task_id}
Status: Pending/Downloading/GeneratingShape/GeneratingPBR/Packaging/Completed/Failed
Completed payload carries output_urls: {model, previews, report}.

Every farm box has its OWN bearer token, so a worker travels as
{name, url, token} and is never authenticated with a shared secret.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx

from . import config


class TaskVanished(RuntimeError):
    """The worker forgot a task it had accepted.

    Distinct from a generation failure: the box restarted, so there is nothing
    to report and nothing to fix - the work is resubmitted.
    """


class NoWorkerAvailable(RuntimeError):
    """The whole 3D fleet is unusable right now.

    Distinct from HunyuanClientError because it says nothing about the job: no
    number of retries fixes it and retrying costs nothing, so the pipeline
    waits it out instead of spending the job's attempts and giving up.
    """


class HunyuanClientError(RuntimeError):
    pass


def workers() -> List[Dict[str, str]]:
    return config.hunyuan_workers()


def is_configured() -> bool:
    return bool(workers())


def worker_by_name(name: str) -> Optional[Dict[str, str]]:
    for worker in workers():
        if worker["name"] == name:
            return worker
    return None


def worker_for_url(url: str) -> Optional[Dict[str, str]]:
    """Find the worker owning a status/artifact URL (used when resuming a job)."""
    url = (url or "").strip()
    for worker in workers():
        if url.startswith(worker["url"]):
            return worker
    return None


def _headers(worker: Dict[str, str]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {worker['token']}"}


async def server_status(
    client: httpx.AsyncClient, worker: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    try:
        resp = await client.get(
            f"{worker['url']}/api-converter-glb/server-status", timeout=12.0
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def worker_state(
    client: httpx.AsyncClient, worker: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    data = await server_status(client, worker)
    if not data:
        return None
    hunyuan = data.get("hunyuan")
    return hunyuan if isinstance(hunyuan, dict) else None


def _load_score(status: Dict[str, Any], hunyuan: Dict[str, Any]) -> int:
    """Lower is better. Hunyuan shares the box's single-consumer queue with
    Blender/OCConvert jobs, so box-level depth matters more than the hunyuan
    sub-queue alone."""
    summary = status.get("tasks_summary")
    depth = 0
    if isinstance(summary, dict):
        try:
            depth = int(summary.get("queue_size") or 0) + int(summary.get("processing") or 0)
        except Exception:
            depth = 0
    else:
        try:
            depth = int(hunyuan.get("queue_size") or 0)
        except Exception:
            depth = 0
    if str(hunyuan.get("service_state") or "") != "idle":
        depth += 1
    return depth


async def pick_worker(client: httpx.AsyncClient) -> Dict[str, str]:
    """Pick the enabled worker with the shallowest overall queue."""
    pool = workers()
    if not pool:
        raise NoWorkerAvailable("no Hunyuan workers configured")
    candidates: List[Tuple[int, int, Dict[str, str]]] = []
    for index, worker in enumerate(pool):
        status = await server_status(client, worker)
        if not status:
            continue
        hunyuan = status.get("hunyuan")
        if not isinstance(hunyuan, dict):
            continue
        if not hunyuan.get("enabled") or not hunyuan.get("installed"):
            continue
        candidates.append((_load_score(status, hunyuan), index, worker))
    if not candidates:
        raise NoWorkerAvailable(
            "no enabled Hunyuan worker among " + ", ".join(w["name"] for w in pool)
        )
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


async def submit(
    client: httpx.AsyncClient,
    *,
    image_url: str,
    seed: Optional[int] = None,
    quality: Optional[str] = None,
    background_method: str = "auto",
) -> Tuple[Dict[str, str], str]:
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
        f"{worker['url']}/api-converter-glb/generate-3d",
        json=body,
        headers=_headers(worker),
        timeout=30.0,
    )
    if resp.status_code in (401, 403):
        # The box re-provisions its token on restart, so ours goes stale
        # without anything being wrong with the job. Same class as an empty
        # fleet: wait for the credentials to be fixed rather than burning the
        # job's attempts and reporting a failure the user cannot act on.
        raise NoWorkerAvailable(
            f"{worker['name']} rejected our token (HTTP {resp.status_code})"
        )
    if resp.status_code not in (200, 202):
        raise HunyuanClientError(
            f"generate-3d on {worker['name']} failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    payload = resp.json()
    task_id = str(payload.get("task_id") or "")
    if task_id:
        # The worker builds status_url from the Host header and drops the port,
        # so it can point at an unrelated service. Always re-base on the worker.
        status_url = f"{worker['url']}/api-converter-glb/generate-3d/status/{task_id}"
    else:
        advertised = str(payload.get("status_url") or "")
        path = urlsplit(advertised).path if advertised else ""
        if not path:
            raise HunyuanClientError(f"generate-3d returned no status_url/task_id: {payload}")
        status_url = f"{worker['url']}{path}"
    return worker, status_url


async def wait_for_model(
    client: httpx.AsyncClient,
    worker: Dict[str, str],
    status_url: str,
    *,
    timeout: Optional[float] = None,
    on_progress=None,
) -> Dict[str, Any]:
    """Poll until Completed; returns the final status payload (with output_urls)."""
    deadline = time.time() + (timeout or config.HUNYUAN_TIMEOUT_SECONDS)
    last_status = ""
    misses = 0
    while time.time() < deadline:
        try:
            resp = await client.get(status_url, headers=_headers(worker), timeout=30.0)
        except Exception as exc:
            print(f"[Renderfin][Hunyuan] status poll error: {exc}")
            await asyncio.sleep(config.HUNYUAN_POLL_SECONDS)
            continue
        if resp.status_code == 200:
            payload = resp.json()
            status = str(payload.get("status") or "")
            if status != last_status:
                last_status = status
                print(f"[Renderfin][Hunyuan] {worker['name']}: {status}")
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
                    f"generation failed on {worker['name']}: "
                    f"{payload.get('error') or 'unknown error'}"
                )
        elif resp.status_code == 404:
            # Tolerate a transient miss (worker restart window) before giving up.
            misses += 1
            if misses >= 5:
                # The box lost its task registry - it reboots without shutting
                # down cleanly - so the work is simply gone. Nothing about the
                # job caused this and nothing about the job can fix it: it is
                # resubmitted rather than charged an attempt.
                raise TaskVanished(f"task vanished on {worker['name']} (HTTP 404)")
        await asyncio.sleep(config.HUNYUAN_POLL_SECONDS)
    raise HunyuanClientError("generation timed out")


async def download_model(
    client: httpx.AsyncClient, worker: Dict[str, str], model_url: str
) -> bytes:
    resp = await client.get(
        model_url, headers=_headers(worker), timeout=300.0, follow_redirects=True
    )
    if resp.status_code != 200:
        raise HunyuanClientError(f"model download failed: HTTP {resp.status_code} {model_url}")
    if len(resp.content) < 1024:
        raise HunyuanClientError(f"model download suspiciously small: {len(resp.content)} bytes")
    return resp.content
