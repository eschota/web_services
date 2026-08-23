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
import uuid
import os
import sqlite3
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

_ORDINARY_QUEUE_CACHE: Tuple[float, bool] = (0.0, False)
_ORDINARY_ACTIVE_STATES = {
    "created",
    "queued",
    "pending",
    "assigned",
    "starting",
    "processing",
}


class TaskVanished(RuntimeError):
    """The worker forgot a task it had accepted.

    Distinct from a generation failure: the box restarted, so there is nothing
    to report and nothing to fix - the work is resubmitted.
    """


class TaskPreempted(RuntimeError):
    """A background generation yielded its GPU slot to interactive work."""


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


def ordinary_conversion_waiting(*, force_refresh: bool = False) -> bool:
    """Return True while the normal AutoRig queue needs converter capacity.

    A missing optional database means this protection is not configured (the
    legacy VPS layout).  An existing but unreadable database fails closed so a
    broken probe cannot make shared converters look free.
    """
    global _ORDINARY_QUEUE_CACHE
    now = time.monotonic()
    cached_at, cached_value = _ORDINARY_QUEUE_CACHE
    if not force_refresh and now - cached_at < max(0.1, config.AUTORIG_QUEUE_CACHE_SECONDS):
        return cached_value
    path = config.AUTORIG_QUEUE_DB_PATH
    if not path.is_file():
        _ORDINARY_QUEUE_CACHE = (now, False)
        return False
    try:
        connection = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro",
            uri=True,
            timeout=0.5,
        )
        try:
            try:
                rows = connection.execute(
                    "SELECT lower(status), count(*) FROM tasks "
                    "WHERE lower(coalesce(queue_class, 'interactive')) = 'interactive' "
                    "GROUP BY lower(status)"
                ).fetchall()
            except sqlite3.OperationalError as exc:
                if "queue_class" not in str(exc).lower():
                    raise
                # Rolling-upgrade compatibility: before the additive migration,
                # every existing task is interactive by definition.
                rows = connection.execute(
                    "SELECT lower(status), count(*) FROM tasks GROUP BY lower(status)"
                ).fetchall()
        finally:
            connection.close()
        waiting = any(str(status or "") in _ORDINARY_ACTIVE_STATES and int(count) > 0
                      for status, count in rows)
        _ORDINARY_QUEUE_CACHE = (now, waiting)
        return waiting
    except Exception as exc:
        print(f"[Renderfin][Hunyuan] ordinary queue probe failed closed: {exc}")
        _ORDINARY_QUEUE_CACHE = (now, True)
        return True


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


def _worker_name_for_binding(
    worker_url: str, pool: List[Dict[str, Any]]
) -> Optional[str]:
    """Resolve an AutoRig worker binding to the matching Hunyuan registry row.

    AutoRig may persist either the public converter hostname or the storage-host
    tunnel URL.  Matching both hostname labels and the configured host/port
    keeps the reserve calculation independent of which route dispatched it.
    """
    raw = str(worker_url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return None
    host = str(parsed.hostname or "").strip().lower()
    first_label = host.split(".", 1)[0]
    public_name = (
        first_label.split("converter-", 1)[1]
        if first_label.startswith("converter-")
        else first_label
    )
    for worker in pool:
        name = str(worker.get("name") or "").strip()
        if name and public_name == name.lower():
            return name
        try:
            candidate = urlsplit(str(worker.get("url") or ""))
        except ValueError:
            continue
        if (
            host
            and host == str(candidate.hostname or "").strip().lower()
            and parsed.port == candidate.port
        ):
            return name or None
    return None


def background_conversion_workers(
    pool: Optional[List[Dict[str, Any]]] = None,
) -> Optional[set[str]]:
    """Return full workers occupied by persisted background AutoRig tasks.

    A missing optional database is the legacy layout and contributes no rows.
    An existing but unreadable or incompatible database fails closed by
    returning ``None``: background Hunyuan must not borrow a shared converter
    when the reserve cannot be proved.
    """
    path = config.AUTORIG_QUEUE_DB_PATH
    if not path.is_file():
        return set()
    registry = list(pool if pool is not None else workers())
    try:
        connection = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro",
            uri=True,
            timeout=0.5,
        )
        try:
            rows = connection.execute(
                "SELECT worker_api FROM tasks "
                "WHERE lower(status) = 'processing' "
                "AND lower(coalesce(queue_class, 'interactive')) = "
                "'collection_background' "
                "AND worker_api IS NOT NULL"
            ).fetchall()
        finally:
            connection.close()
    except Exception as exc:
        print(
            "[Renderfin][Hunyuan] background occupancy probe failed closed: "
            f"{exc}"
        )
        return None
    occupied: set[str] = set()
    for (binding,) in rows:
        name = _worker_name_for_binding(str(binding or ""), registry)
        if name:
            occupied.add(name)
    return occupied


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
    *,
    queue_class: str = "collection_background",
) -> Dict[str, Any]:
    """Pick dedicated capacity first, then an empty protected converter.

    No worker receives a local Hunyuan backlog.  Shared converters are eligible
    only when the ordinary AutoRig queue is empty, their complete box queue is
    empty, and at least ``RESERVED_FOR_OTHER_WORK`` other idle shared workers
    remain available for conversion.
    """
    pool = workers()
    if not pool:
        raise NoWorkerAvailable("no Hunyuan workers configured")
    in_flight = in_flight or {}
    excluded = excluded or set()
    background = str(queue_class or "").strip().lower() == "collection_background"
    at_capacity: List[str] = []
    unreachable: List[str] = []
    dedicated: List[Tuple[int, int, int, Dict[str, Any]]] = []
    shared_idle: List[Tuple[int, int, Dict[str, Any]]] = []
    shared_full_statuses: Dict[str, Dict[str, Any]] = {}
    for index, worker in enumerate(pool):
        if worker["name"] in excluded:
            continue
        status = await server_status(client, worker)
        if not status:
            unreachable.append(worker["name"])
            continue
        hunyuan = status.get("hunyuan")
        if not isinstance(hunyuan, dict):
            continue
        if not hunyuan.get("enabled") or not hunyuan.get("installed"):
            continue
        worker_pool = str(worker.get("pool") or "shared_converter")
        if worker_pool != "dedicated" and _status_is_full_converter(status):
            shared_full_statuses[str(worker["name"])] = status
        if in_flight.get(worker["name"], 0) >= WORKER_INFLIGHT_CAP:
            at_capacity.append(worker["name"])
            continue
        score = _load_score(status, hunyuan)
        priority = int(worker.get("priority") or 100)
        accepting_flag = status.get("accepting_hunyuan")
        if worker_pool == "dedicated":
            # New render nodes expose an atomic arbiter admission flag.  F12 is
            # dedicated too but predates that field, so its queue/service state
            # remains the compatibility signal.
            if accepting_flag is False:
                at_capacity.append(worker["name"])
                continue
            if accepting_flag is None and score > 0:
                at_capacity.append(worker["name"])
                continue
            dedicated.append((score, priority, index, worker))
            continue
        if str(worker["name"]) not in shared_full_statuses and background:
            # Background work is admitted only on workers that expose the
            # deployed full-converter/preemption capability contract.
            continue
        # A shared converter is borrowed only while its complete queue is idle;
        # the central queue keeps the Hunyuan job instead of hiding it locally.
        if score == 0 and accepting_flag is not False:
            shared_idle.append((priority, index, worker))

    if dedicated:
        dedicated.sort(key=lambda item: (item[0], item[1], item[2]))
        return dedicated[0][3]

    if ordinary_conversion_waiting(force_refresh=background):
        raise NoWorkerAvailable(
            "dedicated Hunyuan pool has no capacity; shared fallback paused for ordinary conversion"
        )

    if shared_full_statuses and set(shared_full_statuses).issubset(set(at_capacity)):
        raise NoWorkerAvailable(
            "every Hunyuan worker is at capacity: " + ", ".join(at_capacity)
        )

    reserve = max(0, RESERVED_FOR_OTHER_WORK) if background else 0
    if background:
        persisted_occupied = background_conversion_workers(pool)
        if persisted_occupied is None:
            raise NoWorkerAvailable(
                "shared Hunyuan fallback paused: background full-worker occupancy is unknown"
            )
        background_occupied = {
            name for name in persisted_occupied if name in shared_full_statuses
        }
        background_occupied.update(
            name
            for name, count in in_flight.items()
            if int(count or 0) > 0 and name in shared_full_statuses
        )
        for name, status in shared_full_statuses.items():
            slots = _status_slot_items(status)
            if slots is None:
                raise NoWorkerAvailable(
                    "shared Hunyuan fallback paused: exact slot telemetry is "
                    f"unavailable on {name}"
                )
            if any(
                str(item.get("queue_class") or "").strip().lower()
                == "collection_background"
                for item in slots
            ):
                background_occupied.add(name)
        background_limit = max(0, len(shared_full_statuses) - reserve)
        if len(background_occupied) >= background_limit:
            raise NoWorkerAvailable(
                "shared Hunyuan fallback paused: background work already occupies "
                f"{len(background_occupied)}/{len(shared_full_statuses)} healthy "
                f"full converters (reserve={reserve})"
            )
    if shared_idle:
        shared_idle.sort(key=lambda item: (item[0], item[1]))
        return shared_idle[0][2]
    if at_capacity:
        raise NoWorkerAvailable(
            "every Hunyuan worker is at capacity: " + ", ".join(at_capacity)
        )
    if unreachable:
        raise NoWorkerAvailable(
            "no reachable Hunyuan worker among " + ", ".join(unreachable)
        )
    raise NoWorkerAvailable(
        "no enabled Hunyuan worker among " + ", ".join(w["name"] for w in pool)
    )


async def submit(
    client: httpx.AsyncClient,
    *,
    image_url: str,
    seed: Optional[int] = None,
    quality: Optional[str] = None,
    background_method: str = "auto",
    backend_task_id: str = "",
    queue_class: str = "interactive",
    in_flight: Optional[Dict[str, int]] = None,
    excluded: Optional[set[str]] = None,
) -> Tuple[Dict[str, str], str]:
    """Create a generation task. Returns (worker, status_url)."""
    try:
        worker = await pick_worker(
            client,
            in_flight,
            excluded,
            queue_class=queue_class,
        )
    except NoWorkerAvailable:
        if str(queue_class or "").strip().lower() != "collection_background":
            released = await preempt_background_hunyuan(client)
            if released:
                adjusted_in_flight = dict(in_flight or {})
                released_name = str(released.get("name") or "")
                if released_name in adjusted_in_flight:
                    remaining = max(0, int(adjusted_in_flight[released_name]) - 1)
                    if remaining:
                        adjusted_in_flight[released_name] = remaining
                    else:
                        adjusted_in_flight.pop(released_name, None)
                worker = await pick_worker(
                    client,
                    adjusted_in_flight,
                    excluded,
                    queue_class=queue_class,
                )
            else:
                raise
        else:
            raise
    body: Dict[str, Any] = {
        "image_url": image_url,
        "quality": quality or config.HUNYUAN_QUALITY,
        "background_method": background_method,
        "backend_task_id": str(backend_task_id or ""),
        "queue_class": (
            "collection_background"
            if str(queue_class or "").strip().lower() == "collection_background"
            else "interactive"
        ),
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
    if resp.status_code in (409, 423, 429, 503):
        try:
            rejection = resp.json()
        except ValueError:
            rejection = {}
        error = str(rejection.get("error") or "").strip().lower()
        if rejection.get("retryable") is True or error in {
            "gpu_busy_comfy",
            "gpu_leased",
            "worker_capacity",
            "gpu_arbiter_unavailable",
            "gpu_arbiter_failure",
        }:
            raise NoWorkerAvailable(
                f"{worker['name']} is temporarily unavailable: "
                f"{error or response_text[:200]}"
            )
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


def _status_slot_items(status: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Return active and accepted-pending identities, or fail closed."""
    combined: List[Dict[str, Any]] = []
    for key in ("processing_tasks", "pending_tasks"):
        if key not in status:
            return None
        value = status.get(key)
        if isinstance(value, dict):
            value = list(value.values())
        if not isinstance(value, list) or any(
            not isinstance(item, dict) for item in value
        ):
            return None
        combined.extend(value)
    return combined


def _processing_items(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Compatibility name: recall scans both running and accepted-pending."""
    return _status_slot_items(status) or []


def _status_proves_hunyuan_idle(status: Dict[str, Any], task_id: str) -> bool:
    """Fail closed unless the worker explicitly reports a zero-length queue."""
    if not isinstance(status, dict):
        return False
    raw = _status_slot_items(status)
    if raw is None:
        return False
    if any(str(item.get("task_id") or "") == task_id for item in raw):
        return False
    summary = status.get("tasks_summary")
    if not isinstance(summary, dict):
        return False
    counters: List[int] = []
    for key in ("processing", "pending", "queue_size"):
        value = summary.get(key)
        if isinstance(value, bool):
            return False
        if isinstance(value, float) and not value.is_integer():
            return False
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return False
        if parsed < 0:
            return False
        counters.append(parsed)
    return not raw and counters == [0, 0, 0]


def _status_is_full_converter(status: Dict[str, Any]) -> bool:
    """Fail closed when a Hunyuan-only node is presented as a shared worker."""
    capabilities = status.get("capabilities")
    flags = status.get("feature_flags")
    if not isinstance(capabilities, dict) or not isinstance(flags, dict):
        return False
    return (
        str(capabilities.get("mode") or "").strip().lower() == "full"
        and capabilities.get("legacy_conversion") is True
        and str(flags.get("converter_capability_mode") or "").strip().lower()
        == "full"
        and flags.get("legacy_conversion_enabled") is True
    )


async def _preempt_hunyuan_candidate(
    client: httpx.AsyncClient,
    candidate: Tuple[float, float, str, Dict[str, Any], Dict[str, Any]],
    *,
    deadline: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    _progress, _running, task_id, worker, item = candidate
    deadline = float(deadline or (time.monotonic() + 60.0))
    request_id = str(uuid.uuid4())
    post_timeout = min(15.0, deadline - time.monotonic())
    if post_timeout <= 0:
        return None
    response = await asyncio.wait_for(
        client.post(
            f"{worker['url']}/api-converter-glb/control/tasks/{task_id}/preempt",
            json={
                "backend_task_id": str(item.get("backend_task_id") or ""),
                "preemption_request_id": request_id,
            },
            headers=_headers(worker),
            timeout=post_timeout,
        ),
        timeout=post_timeout,
    )
    if response.status_code not in (200, 202):
        return None
    status_url = f"{worker['url']}/api-converter-glb/generate-3d/status/{task_id}"
    while time.monotonic() < deadline:
        status_timeout = min(12.0, deadline - time.monotonic())
        if status_timeout <= 0:
            break
        task_response = await asyncio.wait_for(
            client.get(
                status_url, headers=_headers(worker), timeout=status_timeout
            ),
            timeout=status_timeout,
        )
        if task_response.status_code == 200:
            payload = task_response.json()
            if str(payload.get("status") or "") in {"Completed", "Preempted"}:
                proof_timeout = min(12.0, deadline - time.monotonic())
                if proof_timeout <= 0:
                    break
                try:
                    current = await asyncio.wait_for(
                        server_status(client, worker), timeout=proof_timeout
                    )
                except asyncio.TimeoutError:
                    current = None
                if current and _status_proves_hunyuan_idle(current, task_id):
                    if str(payload.get("status") or "") == "Preempted":
                        print(
                            f"[Renderfin][Hunyuan] preempted background task {task_id} "
                            f"on {worker['name']} for interactive work"
                        )
                    return worker
        remaining = deadline - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(min(1.0, remaining))
    return None


async def preempt_background_hunyuan_many(
    client: httpx.AsyncClient,
    *,
    limit: int,
    shared_full_converter_only: bool = False,
) -> List[Dict[str, Any]]:
    """Free up to ``limit`` compatible Hunyuan slots in one 60-second window.

    Full conversion tasks are deliberately not recalled here: their AutoRig DB
    row is owned by the full-converter scheduler.  A shared worker is eligible
    only when its active task is explicitly a Hunyuan generation.
    """
    deadline = time.monotonic() + 60.0
    candidates: List[Tuple[float, float, str, Dict[str, Any], Dict[str, Any]]] = []
    pool = [
        worker
        for worker in workers()
        if not (
            shared_full_converter_only
            and (
                str(worker.get("pool") or "") == "dedicated"
                or str(worker.get("capability_mode") or "").strip().lower()
                != "full"
            )
        )
    ]

    async def probe(worker: Dict[str, Any]):
        timeout = min(12.0, deadline - time.monotonic())
        if timeout <= 0:
            return worker, None
        try:
            status = await asyncio.wait_for(
                server_status(client, worker), timeout=timeout
            )
        except asyncio.TimeoutError:
            status = None
        return worker, status

    probes = await asyncio.gather(*(probe(worker) for worker in pool))
    for worker, status in probes:
        if shared_full_converter_only and str(worker.get("pool") or "") == "dedicated":
            continue
        if not status:
            continue
        if shared_full_converter_only and not _status_is_full_converter(status):
            continue
        flags = status.get("feature_flags") if isinstance(status.get("feature_flags"), dict) else {}
        if flags.get("collection_preemption_v1") is not True:
            continue
        for item in _processing_items(status):
            task_type = str(item.get("type") or "")
            if worker.get("pool") != "dedicated" and task_type != "HunyuanGenerationTask":
                continue
            if str(item.get("queue_class") or "") != "collection_background":
                continue
            if item.get("preemptible") is not True:
                continue
            task_id = str(item.get("task_id") or "")
            backend_task_id = str(item.get("backend_task_id") or "")
            if not task_id or not backend_task_id:
                continue
            try:
                progress = float(item.get("progress_percent") or 0)
            except (TypeError, ValueError):
                progress = 0.0
            try:
                running = float(item.get("running_time") or 0)
            except (TypeError, ValueError):
                running = 0.0
            candidates.append((progress, running, task_id, worker, item))
    if not candidates:
        return []
    selected = sorted(
        candidates, key=lambda value: (value[0], value[1], value[2])
    )[: max(0, int(limit or 0))]
    results = await asyncio.gather(
        *(
            _preempt_hunyuan_candidate(client, candidate, deadline=deadline)
            for candidate in selected
        ),
        return_exceptions=True,
    )
    return [result for result in results if isinstance(result, dict)]


async def preempt_background_hunyuan(
    client: httpx.AsyncClient,
    *,
    shared_full_converter_only: bool = False,
) -> Optional[Dict[str, Any]]:
    released = await preempt_background_hunyuan_many(
        client,
        limit=1,
        shared_full_converter_only=shared_full_converter_only,
    )
    return released[0] if released else None


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
            if status == "Preempted":
                raise TaskPreempted(
                    f"background Hunyuan task preempted on {worker['name']}"
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
