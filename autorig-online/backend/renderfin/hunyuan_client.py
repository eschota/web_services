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
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx

from . import config


# How many generations of ours a single box may hold at once. The converter
# runs one at a time, so anything above one only buys an opaque backlog.
WORKER_INFLIGHT_CAP = int(os.getenv("RENDERFIN_HUNYUAN_INFLIGHT_CAP", "1"))

# The same boxes also run ordinary rig/convert tasks and the Flux T-pose
# renders. A batch of a hundred generations would otherwise occupy every one of
# them, and the regular queue - which has a real per-task deadline once it is
# dispatched - starts timing out while the fleet is nominally healthy. Keep this
# many boxes out of generation's reach so the rest of the service keeps moving;
# generations queue on our side instead, which costs them nothing.
RESERVED_FOR_OTHER_WORK = int(os.getenv("RENDERFIN_HUNYUAN_RESERVED_WORKERS", "1"))


class TaskVanished(RuntimeError):
    """The worker forgot a task it had accepted.

    Distinct from a generation failure: the box restarted, so there is nothing
    to report and nothing to fix - the work is resubmitted.
    """


class WorkerUnreachable(RuntimeError):
    """We lost the route to the box while it was holding our generation.

    Not a verdict on the task - the box may well still be running it - but we
    cannot follow it any more, and a job that keeps polling an address that
    refuses connections also keeps that worker's only slot. That is how a
    tunnel outage turns into a stopped queue instead of a slow one: the whole
    fleet ends up "busy" with generations nobody can see.

    So the handle is dropped and the work is resubmitted. If the original
    generation is still alive when the route comes back, the box refuses the
    second one for being at capacity and the job simply waits - which is the
    safe direction to be wrong in.
    """


class WorkerInputFetchError(WorkerUnreachable):
    """One worker accepted our route but cannot resolve the public input host."""

    def __init__(self, worker_name: str, message: str):
        super().__init__(message)
        self.worker_name = worker_name


# Poll failures are counted, not tolerated forever: at a 10s poll this is five
# minutes, long enough to sit out a tunnel restart and short enough that a real
# outage frees the slot instead of pinning it for the whole 4h ceiling.
MAX_TRANSPORT_MISSES = int(os.getenv("RENDERFIN_HUNYUAN_TRANSPORT_MISSES", "30"))


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


async def pick_worker(
    client: httpx.AsyncClient,
    in_flight: Optional[Dict[str, int]] = None,
    excluded: Optional[set[str]] = None,
) -> Dict[str, str]:
    """Pick the enabled worker with the shallowest overall queue.

    `in_flight` is how many of OUR jobs each worker is already holding. A box
    that runs one generation at a time gains nothing from being handed more,
    and a job waiting in its queue is invisible to us, so a worker at its cap
    is not a candidate and the job waits on our side instead.
    """
    pool = workers()
    if not pool:
        raise NoWorkerAvailable("no Hunyuan workers configured")
    in_flight = in_flight or {}
    excluded = excluded or set()
    at_capacity: List[str] = []
    # Boxes already carrying one of our generations. Spreading onto a fresh box
    # is what eats the fleet, so that is what the reserve limits - a box already
    # generating may keep doing so.
    busy_with_ours = {name for name, count in in_flight.items() if count > 0}
    max_generating = max(1, len(pool) - RESERVED_FOR_OTHER_WORK)
    reserve_reached = len(busy_with_ours) >= max_generating
    reserved_skipped: List[str] = []
    candidates: List[Tuple[int, int, Dict[str, str]]] = []
    for index, worker in enumerate(pool):
        if worker["name"] in excluded:
            continue
        if in_flight.get(worker["name"], 0) >= WORKER_INFLIGHT_CAP:
            at_capacity.append(worker["name"])
            continue
        if reserve_reached and worker["name"] not in busy_with_ours:
            reserved_skipped.append(worker["name"])
            continue
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
        if reserved_skipped:
            # Deliberate, not a fault: the same wait path as a full pool, so the
            # job keeps its attempts and its stage clock while it holds off.
            raise NoWorkerAvailable(
                "every Hunyuan worker is at capacity: holding "
                + ", ".join(reserved_skipped)
                + " free for rig/convert work"
            )
        if at_capacity:
            # not a fault: every box is busy with work of ours. Waiting for a
            # slot is the correct outcome and must not spend an attempt.
            raise NoWorkerAvailable(
                "every Hunyuan worker is at capacity: " + ", ".join(at_capacity)
            )
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
    in_flight: Optional[Dict[str, int]] = None,
    excluded: Optional[set[str]] = None,
) -> Tuple[Dict[str, str], str]:
    """Create a generation task. Returns (worker, status_url)."""
    worker = await pick_worker(client, in_flight, excluded)
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
    response_text = resp.text[:1000]
    if resp.status_code == 400 and (
        "image_url host cannot be resolved" in response_text.lower()
        or "getaddrinfo failed" in response_text.lower()
    ):
        raise WorkerInputFetchError(
            worker["name"],
            f"{worker['name']} cannot resolve the input image host: {response_text[:300]}",
        )
    if resp.status_code not in (200, 202):
        raise HunyuanClientError(
            f"generate-3d on {worker['name']} failed: HTTP {resp.status_code} {response_text[:300]}"
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
    unreachable = 0
    while time.time() < deadline:
        try:
            resp = await client.get(status_url, headers=_headers(worker), timeout=30.0)
        except Exception as exc:
            unreachable += 1
            print(
                f"[Renderfin][Hunyuan] status poll error ({unreachable}/"
                f"{MAX_TRANSPORT_MISSES}): {exc}"
            )
            if unreachable >= MAX_TRANSPORT_MISSES:
                raise WorkerUnreachable(
                    f"lost the route to {worker['name']} while it held our "
                    f"generation ({exc})"
                )
            await asyncio.sleep(config.HUNYUAN_POLL_SECONDS)
            continue
        unreachable = 0
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


def _rebase_on_worker(worker: Dict[str, str], url: str) -> str:
    """Point a worker-advertised URL back at the worker's own origin."""
    parts = urlsplit(url or "")
    if not parts.path:
        return ""
    tail = parts.path + (f"?{parts.query}" if parts.query else "")
    return f"{worker['url']}{tail}"


_DOWNLOAD_ATTEMPTS = 3
_DOWNLOAD_RETRY_SECONDS = 5.0


async def download_model(
    client: httpx.AsyncClient, worker: Dict[str, str], model_url: str
) -> bytes:
    candidates = [u for u in (_rebase_on_worker(worker, model_url), model_url) if u]
    if not candidates:
        raise HunyuanClientError(f"model download failed: no usable url ({model_url!r})")
    last_error = ""
    for attempt in range(1, _DOWNLOAD_ATTEMPTS + 1):
        for url in candidates:
            try:
                resp = await client.get(
                    url, headers=_headers(worker), timeout=300.0, follow_redirects=True
                )
            except Exception as exc:
                last_error = f"model download failed: {exc} {url}"
                continue
            if resp.status_code != 200:
                last_error = f"model download failed: HTTP {resp.status_code} {url}"
                continue
            if len(resp.content) < 1024:
                last_error = (
                    f"model download suspiciously small: {len(resp.content)} bytes"
                )
                continue
            return resp.content
        if attempt < _DOWNLOAD_ATTEMPTS:
            print(
                f"[Renderfin][Hunyuan] {worker['name']}: {last_error}; "
                f"retrying ({attempt}/{_DOWNLOAD_ATTEMPTS})"
            )
            await asyncio.sleep(_DOWNLOAD_RETRY_SECONDS)
    raise HunyuanClientError(last_error)
