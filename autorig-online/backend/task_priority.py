"""Priority and safe-preemption policy for the full converter fleet.

Interactive work is the default.  Only automatic Renderfin collection submits
may opt into ``collection_background``.  The helpers in this module keep the
policy testable and keep worker-control credentials out of task payloads and
process arguments.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

import httpx
from sqlalchemy import case, func, or_, select, update


QUEUE_CLASS_INTERACTIVE = "interactive"
QUEUE_CLASS_BACKGROUND = "collection_background"
PREEMPTION_NONE = "none"
PREEMPTION_REQUESTED = "requested"
PREEMPTION_STOPPING = "stopping"
PREEMPT_COOLDOWN_SECONDS = int(os.getenv("AUTORIG_COLLECTION_PREEMPT_COOLDOWN", "300"))
PREEMPT_DEADLINE_SECONDS = max(
    1, min(60, int(os.getenv("AUTORIG_COLLECTION_PREEMPT_DEADLINE", "60")))
)
PREEMPTION_ENABLED = str(
    os.getenv("AUTORIG_COLLECTION_PREEMPTION_ENABLED", "0")
).strip().lower() in {"1", "true", "yes", "on"}

_METRICS: Dict[str, float] = {
    "preemption_requested": 0,
    "preemption_succeeded": 0,
    "preemption_failed": 0,
    "preemption_resumed": 0,
    "preemption_latency_seconds_total": 0.0,
}


def normalize_queue_class(value: Any) -> str:
    return (
        QUEUE_CLASS_BACKGROUND
        if str(value or "").strip().lower() == QUEUE_CLASS_BACKGROUND
        else QUEUE_CLASS_INTERACTIVE
    )


def is_background(value: Any) -> bool:
    return normalize_queue_class(value) == QUEUE_CLASS_BACKGROUND


def preemption_in_progress(task: Any) -> bool:
    return str(getattr(task, "preemption_state", PREEMPTION_NONE) or PREEMPTION_NONE) in {
        PREEMPTION_REQUESTED,
        PREEMPTION_STOPPING,
    }


def dispatch_sort_key(task: Any) -> tuple:
    """Interactive FIFO first; background FIFO second."""
    return (
        1 if is_background(getattr(task, "queue_class", None)) else 0,
        getattr(task, "created_at", None) or datetime.min,
        str(getattr(task, "id", "") or ""),
    )


def dispatch_queue_statement(task_model: Any, dispatch_now: datetime, *, limit: int = 500):
    """Build the globally ordered, eligible converter queue query.

    Ordering belongs in SQL before LIMIT.  Sorting an arbitrary limited subset
    can hide every interactive row behind an older collection backlog.
    """
    return (
        select(task_model)
        .where(
            task_model.status == "created",
            task_model.pipeline_kind != "generate",
            or_(
                task_model.source_next_retry_at.is_(None),
                task_model.source_next_retry_at <= dispatch_now,
            ),
            or_(
                task_model.dispatch_not_before.is_(None),
                task_model.dispatch_not_before <= dispatch_now,
            ),
            or_(
                task_model.preemption_state.is_(None),
                task_model.preemption_state == PREEMPTION_NONE,
            ),
        )
        .order_by(
            case((task_model.queue_class == QUEUE_CLASS_BACKGROUND, 1), else_=0),
            task_model.created_at.asc(),
            task_model.id.asc(),
        )
        .limit(max(1, int(limit)))
    )


def victim_sort_key(task: Any) -> tuple:
    """Least progress, latest start, stable task id."""
    ready = int(getattr(task, "ready_count", 0) or 0)
    total = int(getattr(task, "total_count", 0) or 0)
    progress = (ready / total) if total > 0 else 0.0
    started = getattr(task, "processing_started_at", None)
    started_ts = started.timestamp() if isinstance(started, datetime) else 0.0
    return (progress, -started_ts, str(getattr(task, "id", "") or ""))


def select_preemption_victims(tasks: Iterable[Any], count: int) -> List[Any]:
    candidates = [
        task
        for task in tasks
        if is_background(getattr(task, "queue_class", None))
        and str(getattr(task, "status", "") or "").lower() == "processing"
        and str(getattr(task, "preemption_state", PREEMPTION_NONE) or PREEMPTION_NONE)
        == PREEMPTION_NONE
    ]
    return sorted(candidates, key=victim_sort_key)[: max(0, int(count or 0))]


def background_dispatch_budget(free_workers: Sequence[Any], interactive_waiting: int) -> int:
    """Keep one healthy full-converter slot unused by background work."""
    if int(interactive_waiting or 0) > 0:
        return 0
    return max(0, len(free_workers) - 1)


async def dispatch_fifo_candidate(candidates: List[Any], attempt) -> bool:
    """Dispatch one task, preserving the head on worker-transient rejection."""
    while candidates:
        task = candidates.pop(0)
        source_attempts_before = int(getattr(task, "source_attempt_count", 0) or 0)
        try:
            started_task, dispatch_error = await attempt(task)
        except Exception as exc:
            print(f"[Priority] Error dispatching task {task.id}: {exc}")
            candidates.insert(0, task)
            return False
        if started_task.status == "processing":
            return True
        source_attempts_after = int(
            getattr(started_task, "source_attempt_count", 0) or 0
        )
        if started_task.status == "error" or source_attempts_after > source_attempts_before:
            print(
                f"[Priority] Skipping task {task.id} after task-specific "
                "dispatch rejection"
            )
            continue
        if dispatch_error:
            candidates.insert(0, task)
            return False
    return False


def worker_supports_preemption(worker: Any) -> bool:
    flags = getattr(worker, "feature_flags", None)
    return bool(isinstance(flags, dict) and flags.get("collection_preemption_v1") is True)


def metrics_snapshot(*, interactive_queued: int = 0, background_queued: int = 0,
                     interactive_active: int = 0, background_active: int = 0,
                     reserved_full_slots: int = 0) -> Dict[str, Any]:
    result = dict(_METRICS)
    result.update({
        "interactive_queued": int(interactive_queued),
        "background_queued": int(background_queued),
        "interactive_active": int(interactive_active),
        "background_active": int(background_active),
        "reserved_full_slots": int(reserved_full_slots),
        "preemption_enabled": PREEMPTION_ENABLED,
    })
    successes = int(result["preemption_succeeded"])
    result["preemption_latency_seconds_avg"] = (
        float(result["preemption_latency_seconds_total"]) / successes if successes else 0.0
    )
    return result


def _worker_name_from_full_url(worker_url: str) -> str:
    host = (urlparse(worker_url or "").hostname or "").lower()
    if host.startswith("converter-"):
        return host.split("converter-", 1)[1].split(".", 1)[0]
    return ""


def _control_worker(worker_url: str) -> Optional[Dict[str, Any]]:
    """Resolve the secret-bearing internal control route by physical worker name."""
    name = _worker_name_from_full_url(worker_url)
    if not name:
        return None
    try:
        from renderfin import config as renderfin_config

        for worker in renderfin_config.hunyuan_workers():
            if str(worker.get("name") or "").strip().lower() == name:
                return worker
    except Exception as exc:
        print(f"[Priority] Cannot resolve control worker {name}: {exc}")
    return None


def _nonnegative_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _status_slot_items(payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Return exact active and locally pending identities, or fail closed."""
    if not isinstance(payload, dict):
        return None
    combined: List[Dict[str, Any]] = []
    for key in ("processing_tasks", "pending_tasks"):
        if key not in payload:
            return None
        raw = payload.get(key)
        if isinstance(raw, dict):
            raw = list(raw.values())
        if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
            return None
        combined.extend(raw)
    return combined


def _status_is_slot_empty(payload: Dict[str, Any], worker_task_id: str) -> bool:
    """Require explicit, well-formed zero-queue telemetry before requeue."""
    active = _status_slot_items(payload)
    if active is None:
        return False
    for item in active:
        if not isinstance(item, dict):
            return False
        if str(item.get("task_id") or "") == worker_task_id:
            return False
    summary = payload.get("tasks_summary")
    if not isinstance(summary, dict):
        return False
    counters = [_nonnegative_int(summary.get(key)) for key in ("processing", "pending", "queue_size")]
    if any(value is None for value in counters):
        return False
    return not active and counters == [0, 0, 0]


async def _cas_requeue_preempted_task(
    db: Any,
    task_model: Any,
    *,
    task_id: str,
    worker_api: str,
    worker_task_id: str,
    request_id: str,
    now: datetime,
) -> bool:
    """Atomically clear only the exact worker attempt that was recalled."""
    statement = (
        update(task_model)
        .where(
            task_model.id == task_id,
            task_model.status == "processing",
            task_model.worker_api == worker_api,
            task_model.worker_task_id == worker_task_id,
            task_model.preemption_request_id == request_id,
            task_model.preemption_state.in_((
                PREEMPTION_REQUESTED,
                PREEMPTION_STOPPING,
            )),
        )
        .values(
            status="created",
            worker_api=None,
            worker_task_id=None,
            progress_page=None,
            guid=None,
            _output_urls="[]",
            _ready_urls="[]",
            ready_count=0,
            total_count=0,
            video_ready=False,
            video_url=None,
            fbx_glb_output_url=None,
            fbx_glb_model_name=None,
            fbx_glb_ready=False,
            fbx_glb_error=None,
            viewer_prepared_glb_url=None,
            viewer_animations_glb_url=None,
            error_message=None,
            processing_started_at=None,
            last_progress_at=None,
            preemption_state=PREEMPTION_NONE,
            preemption_count=func.coalesce(task_model.preemption_count, 0) + 1,
            preempted_at=now,
            dispatch_not_before=now + timedelta(seconds=PREEMPT_COOLDOWN_SECONDS),
            preemption_request_id=None,
            preemption_worker_boot_id=None,
            updated_at=now,
        )
    )
    result = await db.execute(statement)
    if int(result.rowcount or 0) != 1:
        await db.rollback()
        return False
    await db.commit()
    return True


async def _worker_task_status(
    client: httpx.AsyncClient,
    worker: Dict[str, Any],
    worker_task_id: str,
    *,
    deadline: Optional[float] = None,
) -> tuple[str, Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {worker['token']}"}
    urls = (
        f"{worker['url']}/api-converter-glb/status/{worker_task_id}",
        f"{worker['url']}/api-converter-glb/generate-3d/status/{worker_task_id}",
    )
    last: Dict[str, Any] = {}
    for url in urls:
        timeout = 12.0
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
            if timeout <= 0:
                raise TimeoutError("preemption deadline expired during status probe")
        response = await asyncio.wait_for(
            client.get(url, headers=headers, timeout=timeout), timeout=timeout
        )
        if response.status_code == 404:
            continue
        if response.status_code != 200:
            return "", {"http_status": response.status_code}
        data = response.json()
        if isinstance(data, dict):
            last = data
            return str(data.get("status") or ""), data
    return "", last


async def preempt_background_task(task_id: str) -> bool:
    """Recall one full-conversion task and requeue the same DB row after proof."""
    if not PREEMPTION_ENABLED:
        return False

    from database import AsyncSessionLocal, Task
    from workers import clear_worker_quarantine, quarantine_worker

    started = time.monotonic()
    deadline = started + PREEMPT_DEADLINE_SECONDS
    async with AsyncSessionLocal() as db:
        task = await db.get(Task, task_id)
        if task is None or task.status != "processing" or not is_background(task.queue_class):
            return False
        worker = _control_worker(task.worker_api or "")
        if not worker or not task.worker_task_id:
            return False
        request_id = task.preemption_request_id or str(uuid.uuid4())
        task.preemption_request_id = request_id
        if task.preemption_state not in (PREEMPTION_REQUESTED, PREEMPTION_STOPPING):
            task.preemption_state = PREEMPTION_REQUESTED
        stored_boot_id = str(task.preemption_worker_boot_id or "").strip()
        await db.commit()
        worker_task_id = str(task.worker_task_id)
        backend_task_id = str(task.id)
        worker_api = str(task.worker_api or "")

    _METRICS["preemption_requested"] += 1
    headers = {"Authorization": f"Bearer {worker['token']}"}
    body = {
        "backend_task_id": backend_task_id,
        "preemption_request_id": request_id,
    }
    control_url = (
        f"{worker['url']}/api-converter-glb/control/tasks/{worker_task_id}/preempt"
    )
    terminal_status = ""
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            initial_timeout = min(12.0, deadline - time.monotonic())
            if initial_timeout <= 0:
                raise TimeoutError("preemption deadline expired before worker probe")
            initial_server = await asyncio.wait_for(
                client.get(
                    f"{worker['url']}/api-converter-glb/server-status",
                    timeout=initial_timeout,
                ),
                timeout=initial_timeout,
            )
            if initial_server.status_code != 200:
                raise RuntimeError("cannot capture worker boot identity before preemption")
            initial_payload = initial_server.json()
            current_boot_id = str(initial_payload.get("process_boot_id") or "").strip()
            initial_items = _status_slot_items(initial_payload)
            bound_task_visible = bool(
                isinstance(initial_items, list)
                and any(
                    isinstance(item, dict)
                    and str(item.get("task_id") or "") == worker_task_id
                    for item in initial_items
                )
            )
            initial_boot_id = stored_boot_id or current_boot_id
            already_released = False

            if stored_boot_id:
                if (
                    current_boot_id
                    and current_boot_id != stored_boot_id
                    and _status_is_slot_empty(initial_payload, worker_task_id)
                ):
                    terminal_status = "PreemptedAfterWorkerReboot"
                    already_released = True
                elif not bound_task_visible:
                    previous_status, _ = await _worker_task_status(
                        client, worker, worker_task_id, deadline=deadline
                    )
                    if previous_status == "Completed":
                        async with AsyncSessionLocal() as db:
                            await db.execute(
                                update(Task)
                                .where(
                                    Task.id == task_id,
                                    Task.worker_api == worker_api,
                                    Task.worker_task_id == worker_task_id,
                                    Task.preemption_request_id == request_id,
                                )
                                .values(
                                    preemption_state=PREEMPTION_NONE,
                                    preemption_request_id=None,
                                    preemption_worker_boot_id=None,
                                )
                            )
                            await db.commit()
                        return False
                    if (
                        previous_status == "Preempted"
                        and _status_is_slot_empty(initial_payload, worker_task_id)
                    ):
                        terminal_status = "Preempted"
                        already_released = True
                    else:
                        raise RuntimeError(
                            "persisted preemption has neither its bound task nor release proof"
                        )
            elif not current_boot_id or not bound_task_visible:
                raise RuntimeError("worker did not prove the bound task and boot identity")
            else:
                # Persist the immutable process identity before sending the
                # cancellation request.  A backend crash after the POST can
                # then distinguish a lost registry from a live orphan.
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "processing",
                            Task.worker_api == worker_api,
                            Task.worker_task_id == worker_task_id,
                            Task.preemption_request_id == request_id,
                            Task.preemption_state.in_((
                                PREEMPTION_REQUESTED,
                                PREEMPTION_STOPPING,
                            )),
                        )
                        .values(preemption_worker_boot_id=current_boot_id)
                    )
                    await db.commit()
                    if int(result.rowcount or 0) != 1:
                        return False

            if not already_released:
                post_timeout = min(15.0, deadline - time.monotonic())
                if post_timeout <= 0:
                    raise TimeoutError("preemption deadline expired before control request")
                response = await asyncio.wait_for(
                    client.post(
                        control_url,
                        json=body,
                        headers=headers,
                        timeout=post_timeout,
                    ),
                    timeout=post_timeout,
                )
                if response.status_code == 409:
                    try:
                        rejection = response.json()
                    except ValueError:
                        rejection = {}
                    if str(rejection.get("error") or "") == "task_already_completed":
                        async with AsyncSessionLocal() as db:
                            await db.execute(
                                update(Task)
                                .where(
                                    Task.id == task_id,
                                    Task.worker_api == worker_api,
                                    Task.worker_task_id == worker_task_id,
                                    Task.preemption_request_id == request_id,
                                )
                                .values(
                                    preemption_state=PREEMPTION_NONE,
                                    preemption_request_id=None,
                                    preemption_worker_boot_id=None,
                                )
                            )
                            await db.commit()
                        return False
                    raise RuntimeError(
                        f"preempt control rejected identity/state: {response.text[:200]}"
                    )
                if response.status_code not in (200, 202):
                    raise RuntimeError(
                        f"preempt control HTTP {response.status_code}: {response.text[:200]}"
                    )

                async with AsyncSessionLocal() as db:
                    await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "processing",
                            Task.worker_api == worker_api,
                            Task.worker_task_id == worker_task_id,
                            Task.preemption_request_id == request_id,
                            Task.preemption_state == PREEMPTION_REQUESTED,
                        )
                        .values(preemption_state=PREEMPTION_STOPPING)
                    )
                    await db.commit()

            released_confirmed = already_released
            while not released_confirmed and time.monotonic() < deadline:
                status, _payload = await _worker_task_status(
                    client, worker, worker_task_id, deadline=deadline
                )
                terminal_status = status
                if status == "Completed":
                    # Natural completion wins.  The regular synchronizer will
                    # persist outputs and must never launch a duplicate attempt.
                    async with AsyncSessionLocal() as db:
                        await db.execute(
                            update(Task)
                            .where(
                                Task.id == task_id,
                                Task.worker_api == worker_api,
                                Task.worker_task_id == worker_task_id,
                                Task.preemption_request_id == request_id,
                                Task.preemption_state.in_((
                                    PREEMPTION_REQUESTED,
                                    PREEMPTION_STOPPING,
                                )),
                            )
                            .values(
                                preemption_state=PREEMPTION_NONE,
                                preemption_request_id=None,
                                preemption_worker_boot_id=None,
                            )
                        )
                        await db.commit()
                    return False
                if status == "Preempted":
                    status_timeout = min(12.0, deadline - time.monotonic())
                    if status_timeout <= 0:
                        break
                    server = await asyncio.wait_for(
                        client.get(
                            f"{worker['url']}/api-converter-glb/server-status",
                            timeout=status_timeout,
                        ),
                        timeout=status_timeout,
                    )
                    if server.status_code == 200 and _status_is_slot_empty(server.json(), worker_task_id):
                        released_confirmed = True
                        break
                if not status:
                    # A worker reboot loses its in-memory task registry, but a
                    # changed immutable boot id plus an explicitly empty slot
                    # proves that the old process tree cannot still be alive.
                    status_timeout = min(12.0, deadline - time.monotonic())
                    if status_timeout <= 0:
                        break
                    server = await asyncio.wait_for(
                        client.get(
                            f"{worker['url']}/api-converter-glb/server-status",
                            timeout=status_timeout,
                        ),
                        timeout=status_timeout,
                    )
                    if server.status_code == 200:
                        current_payload = server.json()
                        current_boot_id = str(
                            current_payload.get("process_boot_id") or ""
                        ).strip()
                        if (
                            current_boot_id
                            and current_boot_id != initial_boot_id
                            and _status_is_slot_empty(current_payload, worker_task_id)
                        ):
                            terminal_status = "PreemptedAfterWorkerReboot"
                            released_confirmed = True
                            break
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    await asyncio.sleep(min(1.0, remaining))
            if not released_confirmed:
                raise TimeoutError(f"worker slot not released in {PREEMPT_DEADLINE_SECONDS}s")

        async with AsyncSessionLocal() as db:
            now = datetime.utcnow()
            requeued = await _cas_requeue_preempted_task(
                db,
                Task,
                task_id=task_id,
                worker_api=worker_api,
                worker_task_id=worker_task_id,
                request_id=request_id,
                now=now,
            )
            if not requeued:
                # Natural completion or an operator action wins. Clear the
                # marker only on the same completed request; never erase a new
                # worker binding established by a concurrent action.
                await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == "done",
                        Task.preemption_request_id == request_id,
                    )
                    .values(
                        preemption_state=PREEMPTION_NONE,
                        preemption_request_id=None,
                        preemption_worker_boot_id=None,
                        updated_at=now,
                    )
                )
                await db.commit()
                return False
        elapsed = time.monotonic() - started
        _METRICS["preemption_succeeded"] += 1
        _METRICS["preemption_resumed"] += 1
        _METRICS["preemption_latency_seconds_total"] += elapsed
        clear_worker_quarantine(worker_api)
        print(f"[Priority] Preempted background task {task_id} in {elapsed:.1f}s; same row requeued")
        return True
    except Exception as exc:
        _METRICS["preemption_failed"] += 1
        quarantine_worker(worker_api, reason=f"preemption_failed:{str(exc)[:120]}")
        print(
            f"[Priority] Preemption failed for {task_id} on {worker_api}; "
            f"binding retained and worker quarantined: {exc}"
        )
        return False


async def recover_incomplete_preemptions() -> int:
    """Replay persisted idempotent requests after a backend restart."""
    if not PREEMPTION_ENABLED:
        return 0
    from database import AsyncSessionLocal, Task

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Task.id).where(
                Task.status == "processing",
                Task.queue_class == QUEUE_CLASS_BACKGROUND,
                Task.preemption_state.in_((PREEMPTION_REQUESTED, PREEMPTION_STOPPING)),
            )
        )
        task_ids = list(result.scalars().all())
    if not task_ids:
        return 0
    results = await asyncio.gather(
        *(preempt_background_task(task_id) for task_id in task_ids),
        return_exceptions=True,
    )
    return sum(result is True for result in results)


def renderfin_submitted_task_ids() -> set[str]:
    """Read only AutoRig task ids emitted by automatic Renderfin collection jobs."""
    try:
        from renderfin import config as renderfin_config

        path = renderfin_config.DB_PATH
        if not path.is_file():
            return set()
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=2.0)
        try:
            rows = connection.execute("SELECT payload FROM chargen_jobs").fetchall()
        finally:
            connection.close()
    except Exception as exc:
        print(f"[Priority] Renderfin backfill probe skipped: {exc}")
        return set()
    result: set[str] = set()
    for (raw,) in rows:
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        task_id = str(payload.get("submitted_task_id") or "").strip()
        if task_id and str(payload.get("collection_guid") or "").strip():
            result.add(task_id)
    return result


async def backfill_active_collection_tasks() -> int:
    """Classify only active auto-submits; never rewrite done/manual tasks."""
    from database import AsyncSessionLocal, Task

    ids = renderfin_submitted_task_ids()
    if not ids:
        return 0
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Task).where(
                Task.id.in_(ids),
                Task.status.in_(("created", "processing")),
                Task.queue_class != QUEUE_CLASS_BACKGROUND,
            )
        )
        tasks = list(result.scalars().all())
        for task in tasks:
            task.queue_class = QUEUE_CLASS_BACKGROUND
        if tasks:
            await db.commit()
        return len(tasks)
