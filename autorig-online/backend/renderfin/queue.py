"""Render task queue: sqlite persistence + single async pump (port of C# TaskQueue)."""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
import math
import os
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

import aiosqlite
import httpx

from . import (
    comfy_adapter,
    config,
    image_quality,
    routing,
    templating,
    workload_lease,
)
from .models import (
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
    RenderTask,
)
from .registry import ServerRegistry

# Where a box whose queue we could not read sorts. Above any plausible real
# backlog, so an unreachable box is never mistaken for an idle one - but still
# finite, so it stays usable when every box is unreadable.
_UNKNOWN_DEPTH = 10_000


class ManagedComfyCleanupPending(RuntimeError):
    """Host registration may exist; keep lease/binding until exact cleanup."""


def _artifact_owned_by_task(artifact: Dict[str, str], task_id: str) -> bool:
    """Only accept SaveImage outputs carrying this logical task prefix."""

    filename = PurePosixPath(
        str(artifact.get("filename") or "").replace("\\", "/")
    ).name.lower()
    owner = str(task_id or "").strip().lower()
    return bool(
        owner
        and (
            filename == owner
            or filename.startswith(owner + "_")
            or filename.startswith(owner + ".")
        )
    )


def _artifact_contract_error(
    task: RenderTask, machine_code: str, message: str, artifacts: List[Dict[str, str]]
) -> image_quality.RenderArtifactQualityError:
    return image_quality.RenderArtifactQualityError(
        machine_code,
        message,
        {
            "schema": "renderfin.render_artifact_contract.v1",
            "passed": False,
            "task_id": task.id,
            "prompt_type": str(task.prompt.type or ""),
            "artifacts": [
                {
                    "filename": str(item.get("filename") or ""),
                    "subfolder": str(item.get("subfolder") or ""),
                    "type": str(item.get("type") or ""),
                }
                for item in artifacts
            ],
        },
    )


def _host_terminal_outcome(
    payload: Dict[str, Any],
    task: Optional[RenderTask] = None,
    *,
    action: str = "host_control",
) -> str:
    """Return only a terminal receipt that belongs to this exact task.

    The task-less form is retained for status-normalization tests. Every queue
    control path supplies ``task`` so an authenticated-but-mismatched host
    response is retryable and cannot mutate a different logical prompt.
    """

    if task is None:
        return workload_lease.host_comfy_terminal_outcome(payload)
    return workload_lease.validate_host_comfy_terminal_receipt(
        payload,
        action=action,
        prompt_id=task.comfy_prompt_id,
        logical_task_id=task.id,
        lease_id=task.workload_lease_id,
        request_id=task.workload_request_id,
    )


def _finite_number(value: Any) -> Optional[float]:
    """Parse a finite number without accepting booleans or NaN/Infinity."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _host_managed_progress(
    payload: Dict[str, Any], task: RenderTask, *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Normalize an authenticated exact host stale/progress observation.

    Host releases may add fields over time.  Accept a small set of aliases but
    never let a malformed or mismatched response reset the watchdog or recall a
    different prompt.  Volatile heartbeat/expiry timestamps deliberately do
    not participate in the progress signature.
    """
    if not isinstance(payload, dict):
        return {}
    if not workload_lease.host_comfy_receipt_matches(
        payload,
        prompt_id=task.comfy_prompt_id,
        logical_task_id=task.id,
        lease_id=task.workload_lease_id,
        request_id=task.workload_request_id,
    ):
        return {}
    entry = workload_lease.host_comfy_receipt_entry(payload)

    def first(*keys: str) -> str:
        for key in keys:
            value = entry.get(key)
            if value is None:
                value = payload.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    progress = entry.get("progress_by_key")
    progress = progress if isinstance(progress, dict) else entry
    state = first("state", "state_string", "status", "status_string").lower()
    stage = str(
        progress.get("current_stage_string")
        or progress.get("current_stage")
        or progress.get("stage_string")
        or progress.get("stage")
        or ""
    ).strip()[:160]
    marker = str(
        progress.get("progress_marker_string")
        or progress.get("progress_marker")
        or ""
    ).strip()[:240]
    percent = _finite_number(
        progress.get("progress_percent")
        if "progress_percent" in progress
        else progress.get("progress_percent_float")
    )
    if percent is not None and not 0 <= percent <= 100:
        percent = None
    signature_parts = {
        "state": state if state not in {"stale", "artifact_pending"} else "",
        "stage": stage,
        "marker": marker,
    }
    signature = json.dumps(signature_parts, sort_keys=True, separators=(",", ":"))
    if not any(signature_parts.values()):
        signature = ""

    current = float(now if now is not None else time.time())
    host_progress_at = _finite_number(
        progress.get("last_progress_at")
        if "last_progress_at" in progress
        else progress.get("last_progress_at_utc_timestamp")
    )
    # A host clock far in the future could postpone recovery indefinitely.  An
    # observation before this exact prompt started is equally non-authoritative.
    if host_progress_at is not None and not (
        max(0.0, float(task.started_at or 0) - 60.0)
        <= host_progress_at
        <= current + 60.0
    ):
        host_progress_at = None
    no_progress_seconds = _finite_number(
        progress.get("no_progress_seconds")
        if "no_progress_seconds" in progress
        else progress.get("stale_for_seconds")
    )
    if no_progress_seconds is not None and no_progress_seconds < 0:
        no_progress_seconds = None
    stale_at = _finite_number(entry.get("stale_at"))
    stale_bool = entry.get("stale_bool") is True or entry.get("stale") is True
    return {
        "state": state,
        "stage": stage,
        "marker": marker,
        "signature": signature,
        "percent": percent,
        "last_progress_at": host_progress_at,
        "no_progress_seconds": no_progress_seconds,
        "stale": bool(state == "stale" or stale_bool),
        "stale_at": stale_at,
    }

_SCHEMA = """
CREATE TABLE IF NOT EXISTS render_tasks (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_render_tasks_status ON render_tasks(status);
"""


def _jpeg_sibling(png_path: Path) -> None:
    """Write a quality-85 JPEG next to a PNG artifact (C# parity)."""
    try:
        from PIL import Image

        with Image.open(png_path) as img:
            rgb = img.convert("RGB")
            rgb.save(png_path.with_suffix(".jpg"), "JPEG", quality=85)
    except Exception as exc:
        print(f"[Renderfin][Queue] jpeg sibling failed for {png_path.name}: {exc}")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        if os.name == "nt":
            return
        raise workload_lease.HostComfyArtifactWait(
            "central_managed_comfy_directory_fsync_open_failed", 2
        ) from exc
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if os.name != "nt":
                raise workload_lease.HostComfyArtifactWait(
                    "central_managed_comfy_directory_fsync_failed", 2
                ) from exc
    finally:
        os.close(descriptor)


def _atomic_fsync_bytes(path: Path, data: bytes) -> tuple[str, int]:
    """Persist task-owned bytes before an exact host artifact ACK."""

    if not data or len(data) > 512 * 1024 * 1024:
        raise workload_lease.HostComfyArtifactWait(
            "central_managed_comfy_artifact_size_invalid", 2
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{uuid.uuid4().hex}.managed-comfy-part"
    )
    digest = hashlib.sha256(data).hexdigest()
    try:
        with temporary.open("xb") as sink:
            sink.write(data)
            sink.flush()
            os.fsync(sink.fileno())
        os.replace(str(temporary), str(path))
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    if not workload_lease.verify_central_artifact(
        path, expected_sha256=digest, expected_size=len(data)
    ):
        raise workload_lease.HostComfyArtifactWait(
            "central_managed_comfy_artifact_verify_failed", 2
        )
    return digest, len(data)


def _managed_artifact_relative_path(artifact: Dict[str, str]) -> str:
    """Build the canonical host output-relative path accepted by spool v1."""

    if str(artifact.get("type") or "output").strip().lower() != "output":
        raise workload_lease.HostComfyArtifactWait(
            "managed_comfy_artifact_not_output", 10
        )
    filename = str(artifact.get("filename") or "").strip().replace("\\", "/")
    subfolder = str(artifact.get("subfolder") or "").strip().replace("\\", "/")
    raw = "/".join(part for part in (subfolder.strip("/"), filename) if part)
    relative = PurePosixPath(raw)
    if (
        not raw
        or relative.is_absolute()
        or ":" in raw
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}
    ):
        raise workload_lease.HostComfyArtifactWait(
            "managed_comfy_artifact_path_not_allowlisted", 10
        )
    return relative.as_posix()


def _bundle_receipt_id(task: RenderTask) -> str:
    """Return a deterministic receipt for every centrally durable bundle file."""

    payload = {
        "protocol": workload_lease.MANAGED_COMFY_ARTIFACT_SPOOL_PROTOCOL,
        "prompt_id": task.comfy_prompt_id,
        "logical_task_id": task.id,
        "lease_id": task.workload_lease_id,
        "request_id": task.workload_request_id,
        "primary_sha256": task.artifact_sha256,
        "primary_size_int": task.managed_comfy_artifact_size_int,
        "isolated_sha256": task.managed_comfy_isolated_sha256,
        "isolated_size_int": task.managed_comfy_isolated_size_int,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"renderfin_bundle_v1_{digest}"


class RenderQueue:
    def __init__(
        self,
        registry: ServerRegistry,
        *,
        db_path: Optional[Path] = None,
        client: Optional[httpx.AsyncClient] = None,
    ):
        self.registry = registry
        self.db_path = Path(db_path or config.DB_PATH)
        self._db: Optional[aiosqlite.Connection] = None
        self._client = client
        self._own_client = client is None
        self._tasks: Dict[str, RenderTask] = {}
        self._pump_task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._last_dispatch = 0.0
        self._tick_count = 0
        self._finishers: Dict[str, asyncio.Task] = {}
        self._download_slots = asyncio.Semaphore(3)
        # A node that accepted health probes but failed the actual upload or
        # prompt submission must not be selected again on the next refresh.
        # Without a cooldown, one broken disk/proxy can spend all three task
        # attempts while healthy renderers sit unused.
        self._server_submit_cooldowns: Dict[str, float] = {}

    # ---------- lifecycle ----------

    async def start(self) -> None:
        config.ensure_dirs()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=10000")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        if self._client is None:
            self._client = httpx.AsyncClient(follow_redirects=True)
        await self._resurrect()
        await self._reconcile_terminal_leases()
        self._stopped.clear()
        self._pump_task = asyncio.create_task(self._pump())

    async def stop(self) -> None:
        self._stopped.set()
        for finisher in list(self._finishers.values()):
            finisher.cancel()
        self._finishers.clear()
        if self._pump_task:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except (asyncio.CancelledError, Exception):
                pass
            self._pump_task = None
        if self._db:
            await self._db.close()
            self._db = None
        if self._own_client and self._client:
            await self._client.aclose()
            self._client = None

    async def _resurrect(self) -> None:
        assert self._db is not None
        # active tasks restart from Pending; recent finished tasks are loaded
        # read-only so in-flight character_gen jobs can resume against them
        day_ago = time.time() - 86400
        async with self._db.execute(
            "SELECT payload FROM render_tasks WHERE status IN (?, ?) OR created_at > ?",
            (TASK_PENDING, TASK_RENDERING, day_ago),
        ) as cur:
            rows = await cur.fetchall()
        for (payload,) in rows:
            try:
                task = RenderTask(**json.loads(payload))
            except Exception as exc:
                print(f"[Renderfin][Queue] resurrect skip: {exc}")
                continue
            if task.status == TASK_RENDERING:
                # Keep following a render that is still identifiable on a known
                # server instead of burning GPU time on a fresh submit.
                still_known = bool(
                    task.server_name
                    and task.comfy_prompt_id
                    and (
                        self.registry.get(task.server_name) is not None
                        or (
                            task.managed_prompt
                            and task.workload_lease_id
                        )
                    )
                )
                if still_known:
                    # Keep the ORIGINAL clock. Restarting it hands a render a
                    # fresh 90-minute window on every service restart, so a
                    # render that is genuinely stuck never times out: it holds
                    # its box in _busy_servers forever, and the character_gen
                    # job re-attaches to it (its status is Rendering, not
                    # Error) and spends an attempt per window until it dies.
                    # One job was killed exactly this way on 2026-08-03.
                    task.started_at = task.started_at or time.time()
                else:
                    task.status = TASK_PENDING
                    task.server_name = ""
                    task.comfy_prompt_id = ""
                await self._persist(task)
            self._tasks[task.id] = task
        if self._tasks:
            print(f"[Renderfin][Queue] resurrected {len(self._tasks)} task(s)")

    # ---------- public API ----------

    async def enqueue(
        self,
        prompt: RenderPrompt,
        *,
        queue_class: str = "interactive",
        logical_owner_task_id: str = "",
    ) -> RenderTask:
        token = routing.scheduling_token(prompt)
        workflow_file, forced = routing.resolve_workflow_file(prompt)
        ext = routing.output_extension(prompt)
        normalized_queue = (
            "collection_background"
            if str(queue_class or "").strip().lower() == "collection_background"
            else "interactive"
        )
        task = RenderTask(
            prompt=prompt,
            workflow=token,
            workflow_file=workflow_file,
            output_ext=ext,
            queue_class=normalized_queue,
            logical_owner_task_id=str(logical_owner_task_id or ""),
            workload_class=(
                "collection_background"
                if normalized_queue == "collection_background"
                else "comfy"
            ),
        )
        task.output_url = (
            f"{config.PUBLIC_BASE_URL}/render/{prompt.user_name}/{task.id}{ext}"
        )
        self._tasks[task.id] = task
        await self._persist(task)
        return task

    def get(self, task_id: str) -> Optional[RenderTask]:
        return self._tasks.get(task_id)

    def find_by_output_url(self, output_url: str) -> Optional[RenderTask]:
        output_url = (output_url or "").strip()
        for task in self._tasks.values():
            if task.output_url == output_url:
                return task
        return None

    def all_tasks(self) -> List[RenderTask]:
        return sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)

    async def cancel(self, task_id: str, *, reason: str = "cancelled") -> bool:
        """Stop a queued/running task and best-effort interrupt the worker."""
        task = self._tasks.get(task_id)
        if task is None or task.status in (TASK_DONE, TASK_ERROR):
            return False
        if task.status == TASK_RENDERING and task.server_name and self._client is not None:
            server = self.registry.get(task.server_name)
            if server is not None:
                try:
                    await comfy_adapter.interrupt(self._client, server)
                except Exception as exc:
                    print(f"[Renderfin][Queue] interrupt {task.server_name} failed: {exc}")
        await self._fail(task, reason)
        return True

    async def wait_for(self, task_id: str, timeout: float = 1800) -> RenderTask:
        """Convenience for in-process callers (character_gen)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            task = self._tasks.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task.status in (TASK_DONE, TASK_ERROR):
                return task
            await asyncio.sleep(2.0)
        raise TimeoutError(f"render task {task_id} timed out")

    # ---------- persistence ----------

    async def _persist(self, task: RenderTask) -> None:
        if self._db is None:
            return
        await self._db.execute(
            "INSERT INTO render_tasks(id, payload, status, created_at) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, status=excluded.status",
            (task.id, task.model_dump_json(), task.status, task.created_at),
        )
        await self._db.commit()

    # ---------- pump ----------

    async def _pump(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[Renderfin][Queue] pump error: {exc}")
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=config.PUMP_TICK_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    async def tick(self) -> None:
        """One scheduler pass (kept separate for tests)."""
        self._tick_count += 1
        if self._tick_count % 200 == 0:
            counts: Dict[str, int] = {}
            for t in self._tasks.values():
                counts[t.status] = counts.get(t.status, 0) + 1
            print(f"[Renderfin][Queue] heartbeat tick={self._tick_count} tasks={counts}")
        if self._tick_count % config.STATUS_REFRESH_TICKS == 1:
            await self._refresh_servers()
        if self._tick_count % 10 == 1:
            await self._reconcile_terminal_leases()
        now = time.time()
        if now - self._last_dispatch >= config.DISPATCH_INTERVAL_SECONDS:
            # dispatch in parallel: keep going while there are pending tasks
            # AND free capable servers (one in-flight task per server)
            while await self._dispatch_one():
                pass
            self._last_dispatch = now
        await self._poll_rendering()

    @staticmethod
    def _uses_managed_artifact_spool(
        task: RenderTask, server: RenderServer
    ) -> bool:
        # Once a durable handshake has started, status-probe drift cannot send
        # the task back through legacy /view + /complete.
        return bool(task.managed_comfy_artifact_spool_state) or bool(
            task.managed_prompt
            and task.workload_lease_id
            and getattr(
                server, "managed_comfy_artifact_spool_required_bool", False
            )
        )

    @staticmethod
    def _managed_bundle_is_durable(task: RenderTask) -> bool:
        if not (
            task.output_path
            and workload_lease.verify_central_artifact(
                Path(task.output_path),
                expected_sha256=task.artifact_sha256,
                expected_size=task.managed_comfy_artifact_size_int,
            )
        ):
            return False
        ptype = str(task.prompt.type or "").strip().lower()
        if ptype in {"t_pose", "t_poses"}:
            return bool(
                task.managed_comfy_isolated_output_path
                and workload_lease.verify_central_artifact(
                    Path(task.managed_comfy_isolated_output_path),
                    expected_sha256=task.managed_comfy_isolated_sha256,
                    expected_size=task.managed_comfy_isolated_size_int,
                )
            )
        return True

    async def _ack_managed_artifact(
        self, task: RenderTask, server: RenderServer
    ) -> Dict[str, Any]:
        assert self._client is not None
        if not self._managed_bundle_is_durable(task):
            raise workload_lease.HostComfyArtifactWait(
                "central_managed_comfy_bundle_not_durable", 2
            )
        expected_receipt = _bundle_receipt_id(task)
        if (
            task.managed_comfy_central_persistence_receipt_id_string
            != expected_receipt
        ):
            raise workload_lease.HostComfyArtifactWait(
                "central_managed_comfy_bundle_receipt_mismatch", 2
            )
        return await workload_lease.host_comfy_ack_artifact(
            self._client,
            server=server,
            prompt_id=task.comfy_prompt_id,
            logical_task_id=task.id,
            lease_id=task.workload_lease_id,
            request_id=task.workload_request_id,
            artifact_sha256=task.artifact_sha256,
            artifact_size_int=task.managed_comfy_artifact_size_int,
            central_persistence_receipt_id_string=expected_receipt,
        )

    async def _reconcile_terminal_leases(self) -> None:
        """Finish a lost-response completion handshake after restart."""
        if self._client is None:
            return
        for task in list(self._tasks.values()):
            if not (
                task.status == TASK_DONE
                and task.artifact_sha256
                and task.managed_prompt
                and task.workload_lease_id
                and task.server_name
                and task.comfy_prompt_id
            ):
                continue
            server = self.registry.get(task.server_name)
            if server is None:
                continue
            try:
                if self._uses_managed_artifact_spool(task, server):
                    result = await self._ack_managed_artifact(task, server)
                    if _host_terminal_outcome(
                        result, task, action="ack"
                    ) != "completed":
                        continue
                else:
                    result = await workload_lease.host_comfy_control(
                        self._client,
                        server=server,
                        action="complete",
                        prompt_id=task.comfy_prompt_id,
                        logical_task_id=task.id,
                        lease_id=task.workload_lease_id,
                        request_id=task.workload_request_id,
                        artifact_sha256=task.artifact_sha256,
                    )
                    if _host_terminal_outcome(
                        result, task, action="complete"
                    ) != "completed":
                        continue
                await self._release_workload(task, outcome="completed")
            except Exception as exc:
                print(
                    f"[Renderfin][Queue] terminal lease reconciliation "
                    f"deferred for {task.id}: {exc}"
                )

    async def _refresh_servers(self) -> None:
        assert self._client is not None
        for server in self.registry.all():
            online = await comfy_adapter.check_server_online(self._client, server)
            identity_ok = await workload_lease.refresh_managed_identity(
                self._client, server
            )
            new_status = "online" if online and identity_ok else "offline"
            if server.status != new_status or workload_lease.managed_server(server):
                server.status = new_status
                self.registry.save(server)

    def _busy_servers(self) -> Dict[str, str]:
        busy: Dict[str, str] = {}
        for task in self._tasks.values():
            if task.status == TASK_RENDERING and task.server_name:
                busy[task.server_name] = task.id
        return busy

    def _pick_server(
        self,
        token: str,
        depths: Optional[Dict[str, int]] = None,
        task: Optional[RenderTask] = None,
    ) -> Optional[RenderServer]:
        """Least-loaded box first, fastest box to break the tie.

        Sorting on average_render_time alone picks the box that is *historically*
        quickest, which is not the box that will finish first: these ComfyUI
        machines also serve renderfin.com, and that backlog is invisible to our
        own dispatch records. One t_pose render was handed to a box with fifteen
        other prompts queued while another sat completely idle, and the job it
        belonged to burned all three of its attempts on render timeouts.

        A box we could not ask sorts as unknown rather than empty, so a probe
        failure can never make a box look like the attractive choice.
        """
        busy = self._busy_servers()
        depths = depths or {}
        now = time.time()
        self._server_submit_cooldowns = {
            name: until
            for name, until in self._server_submit_cooldowns.items()
            if until > now
        }
        candidates = [
            s
            for s in self.registry.all()
            if s.status == "online"
            and routing.server_can_run(s, token)
            and s.render_server_name not in busy
            and self._server_submit_cooldowns.get(s.render_server_name, 0) <= now
        ]
        if task is not None and task.workload_lease_id and task.workload_physical_resource_id:
            bound = [
                server
                for server in candidates
                if workload_lease.server_identity(server)[1]
                == task.workload_physical_resource_id
            ]
            return bound[0] if bound else None
        workload_class = str(
            getattr(task, "workload_class", "") or "comfy"
        )
        candidates.sort(
            key=lambda s: (
                workload_lease.server_role_rank(
                    workload_class,
                    getattr(s, "reserve_role_string", "shared"),
                ),
                depths.get(s.render_server_name, _UNKNOWN_DEPTH),
                s.average_render_time or 1e9,
            )
        )
        return candidates[0] if candidates else None

    async def _queue_depths(self) -> Dict[str, int]:
        """Ask every online box how much work it is already sitting on."""
        servers = [s for s in self.registry.all() if s.status == "online"]
        if not servers:
            return {}
        depths: Dict[str, int] = {}
        results = await asyncio.gather(
            *(comfy_adapter.queue_depth(self._client, s) for s in servers),
            return_exceptions=True,
        )
        for server, depth in zip(servers, results):
            if isinstance(depth, int):
                depths[server.render_server_name] = depth
        return depths

    async def _dispatch_one(self) -> bool:
        pending = sorted(
            (t for t in self._tasks.values() if t.status == TASK_PENDING),
            key=lambda t: t.created_at,
        )
        if not pending:
            return False
        depths = await self._queue_depths()
        for task in pending:
            server = self._pick_server(task.workflow, depths, task)
            if server is None:
                continue
            try:
                await self._ensure_workload_lease(task, server)
                await self._submit_task(task, server)
                return True
            except comfy_adapter.ComfyCapacityWait as exc:
                # The render node atomically switched to Hunyuan after our
                # queue probe. Keep the render pending without charging a
                # submit failure; the next server refresh re-enables it after
                # ComfyUI is restored.
                print(
                    f"[Renderfin][Queue] capacity wait {task.id} on "
                    f"{server.render_server_name}: {exc}"
                )
                await self._persist(task)
                server.status = "offline"
                self.registry.save(server)
                return False
            except workload_lease.WorkloadCapacityWait as exc:
                # Central admission wait: keep request id/FIFO position and do
                # not start render timeout or spend submit_failures.
                task.workload_lease_state = "waiting"
                task.started_at = 0
                await self._persist(task)
                print(
                    f"[Renderfin][Queue] workload wait {task.id} on "
                    f"{server.render_server_name}: {exc.status}"
                )
                return False
            except ManagedComfyCleanupPending as exc:
                # Unknown network result after host registration is fail-closed:
                # keep the exact binding/lease and let the poller or TTL
                # watchdog prove cleanup. No retry/deadline is consumed.
                print(f"[Renderfin][Queue] managed cleanup pending {task.id}: {exc}")
                await self._persist(task)
                return False
            except (ValueError, KeyError) as exc:
                # bad workflow/template/prompt: the task is broken, not the box
                print(f"[Renderfin][Queue] task {task.id} rejected: {exc}")
                await self._fail(task, f"invalid render request: {exc}")
                continue
            except Exception as exc:
                print(
                    f"[Renderfin][Queue] submit {task.id} to "
                    f"{server.render_server_name} failed: {exc}"
                )
                await self._release_workload(task, outcome="released", retry=True)
                task.submit_failures += 1
                self._server_submit_cooldowns[server.render_server_name] = (
                    time.time() + config.SUBMIT_FAILURE_COOLDOWN_SECONDS
                )
                if task.submit_failures >= 3:
                    await self._fail(task, f"submit failed 3x: {exc}")
                else:
                    await self._persist(task)
                server.status = "render_error"
                self.registry.save(server)
                continue
        return False

    async def _ensure_workload_lease(
        self, task: RenderTask, server: RenderServer
    ) -> Dict[str, Any]:
        assert self._client is not None
        if not workload_lease.enabled() or not workload_lease.managed_server(server):
            task.managed_prompt = False
            task.host_comfy_registered = False
            return {}
        if task.workload_lease_id:
            try:
                await workload_lease.heartbeat(
                    self._client,
                    lease_id=task.workload_lease_id,
                    owner_task_id=task.id,
                    request_id=task.workload_request_id,
                    server=server,
                )
            except workload_lease.WorkloadPreempted:
                await self._release_workload(task, outcome="preempted", retry=True)
                raise workload_lease.WorkloadCapacityWait("preemption_requested", 2)
            task.workload_lease_state = "active"
            task.workload_heartbeat_at = time.time()
            await self._persist(task)
            return {
                "lease_id_string": task.workload_lease_id,
                "request_id_string": task.workload_request_id,
                "physical_resource_id_string": task.workload_physical_resource_id,
                "node_id_string": task.workload_node_id,
            }
        lease = await workload_lease.acquire(
            self._client,
            server=server,
            workload_class=task.workload_class,
            owner_task_id=task.id,
            request_id=task.workload_request_id,
            metadata={
                "logical_owner_task_id": task.logical_owner_task_id,
                "workflow": task.workflow,
                "queue_class": task.queue_class,
                "managed_prompt": True,
            },
        )
        if not lease:
            task.managed_prompt = False
            return {}
        task.workload_lease_id = str(lease.get("lease_id_string") or "")
        task.workload_physical_resource_id = str(
            lease.get("physical_resource_id_string") or ""
        )
        task.workload_node_id = str(lease.get("node_id_string") or "")
        task.workload_lease_state = "active"
        task.workload_heartbeat_at = time.time()
        task.managed_prompt = True
        # Commit before POST /prompt. A restart resumes this exact lease.
        await self._persist(task)
        return lease

    async def _release_workload(
        self,
        task: RenderTask,
        *,
        outcome: str,
        retry: bool = False,
    ) -> None:
        assert self._client is not None
        lease_id = str(task.workload_lease_id or "")
        request_id = str(task.workload_request_id or "")
        if retry:
            old_prompt_id = str(task.comfy_prompt_id or "")
            if old_prompt_id and old_prompt_id not in task.retired_comfy_prompt_ids:
                task.retired_comfy_prompt_ids.append(old_prompt_id)
            task.status = TASK_PENDING
            task.server_name = ""
            task.comfy_prompt_id = ""
            task.started_at = 0
            task.finished_at = 0
            task.workload_request_id = f"rf_{uuid.uuid4().hex}"
            task.workload_lease_id = ""
            task.workload_physical_resource_id = ""
            task.workload_node_id = ""
            task.workload_lease_state = "waiting"
            task.workload_heartbeat_at = 0
            task.managed_prompt = False
            task.managed_comfy_progress_signature = ""
            task.managed_comfy_progress_percent = -1.0
            task.managed_comfy_last_progress_at = 0
            task.managed_comfy_host_stale_at = 0
            task.managed_comfy_watchdog_requested_at = 0
            task.artifact_sha256 = ""
            task.output_path = ""
            task.extra_outputs = {}
            task.managed_comfy_artifact_spool_state = ""
            task.managed_comfy_artifact_relative_path_string = ""
            task.managed_comfy_artifact_size_int = 0
            task.managed_comfy_artifact_spool_protocol_string = ""
            task.managed_comfy_central_persistence_receipt_id_string = ""
            task.managed_comfy_isolated_output_path = ""
            task.managed_comfy_isolated_sha256 = ""
            task.managed_comfy_isolated_size_int = 0
            # This commit retires the old prompt/binding before its central
            # lease is released. A crash can therefore delay capacity via TTL,
            # but can never make old and new prompts run concurrently.
            await self._persist(task)
        release_confirmed = not lease_id and not request_id
        if lease_id:
            try:
                await workload_lease.release(
                    self._client,
                    lease_id=lease_id,
                    owner_task_id=task.id,
                    request_id=request_id,
                    outcome=outcome,
                )
                release_confirmed = True
            except Exception as exc:
                print(f"[Renderfin][Queue] lease release {task.id} deferred to TTL: {exc}")
        elif retry is False and request_id:
            try:
                await workload_lease.cancel_waiter(
                    self._client,
                    request_id=request_id,
                    owner_task_id=task.id,
                )
                release_confirmed = True
            except Exception as exc:
                print(f"[Renderfin][Queue] waiter cancel {task.id} deferred: {exc}")
        if not retry and release_confirmed:
            task.workload_lease_state = outcome
            if outcome in {"completed", "preempted", "released"}:
                task.host_comfy_registered = False
                task.workload_lease_id = ""
                task.workload_physical_resource_id = ""
                task.workload_node_id = ""
                task.workload_heartbeat_at = 0
        await self._persist(task)

    async def _managed_watchdog_reason(
        self,
        task: RenderTask,
        host_status: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Persist real progress and return a reason when exact recall is due."""
        now = time.time()
        timeout = max(
            60.0, float(config.MANAGED_COMFY_NO_PROGRESS_TIMEOUT_SECONDS or 3600)
        )
        dirty = False
        observation = _host_managed_progress(host_status or {}, task, now=now)
        if observation:
            stale_at = observation.get("stale_at")
            if (
                observation.get("stale")
                and isinstance(stale_at, (int, float))
                and max(0.0, float(task.started_at or 0) - 60.0)
                <= float(stale_at)
                <= now + 60.0
                and float(stale_at) > task.managed_comfy_host_stale_at
            ):
                task.managed_comfy_host_stale_at = float(stale_at)
                dirty = True

            signature = str(observation.get("signature") or "")
            previous_signature = task.managed_comfy_progress_signature
            advanced = False
            if signature and signature != previous_signature:
                task.managed_comfy_progress_signature = signature
                dirty = True
                # An initial observation after a service upgrade is not proof
                # that progress happened now.  A later state/stage/marker
                # change is.  Percentage >0 and an explicit host timestamp are
                # handled independently below.
                if previous_signature and (
                    str(observation.get("state") or "")
                    in {"running", "rendering", "executing", "processing"}
                    or bool(observation.get("stage"))
                    or bool(observation.get("marker"))
                ):
                    advanced = True

            percent = observation.get("percent")
            if isinstance(percent, (int, float)):
                percent = float(percent)
                if percent > task.managed_comfy_progress_percent:
                    if percent > 0:
                        advanced = True
                    task.managed_comfy_progress_percent = percent
                    dirty = True

            host_progress_at = observation.get("last_progress_at")
            if isinstance(host_progress_at, (int, float)) and float(
                host_progress_at
            ) > task.managed_comfy_last_progress_at:
                task.managed_comfy_last_progress_at = float(host_progress_at)
                dirty = True
                advanced = False  # the authoritative host time already won
            elif advanced:
                task.managed_comfy_last_progress_at = now
                dirty = True

        if not task.managed_comfy_last_progress_at and task.started_at:
            # Establish the durable baseline without manufacturing progress.
            task.managed_comfy_last_progress_at = float(task.started_at)
            dirty = True
        if dirty:
            await self._persist(task)

        if observation.get("stale"):
            return "host_reported_stale"
        host_no_progress = observation.get("no_progress_seconds")
        if isinstance(host_no_progress, (int, float)) and float(
            host_no_progress
        ) >= timeout:
            return f"host_no_progress_{float(host_no_progress):.0f}s"
        baseline = float(
            task.managed_comfy_last_progress_at or task.started_at or 0
        )
        if baseline and now - baseline >= timeout:
            return f"central_no_progress_{now - baseline:.0f}s"
        return ""

    async def _recall_managed_prompt(
        self,
        task: RenderTask,
        server: RenderServer,
        *,
        reason: str,
    ) -> None:
        """Exactly stop one managed prompt and retry the same logical task.

        A missing/ambiguous acknowledgement keeps the old binding fail-closed.
        Host Completed always wins and follows the old prompt to its artifact.
        """
        old_prompt_id = str(task.comfy_prompt_id or "")
        now = time.time()
        if (
            task.managed_comfy_watchdog_requested_at
            and now - task.managed_comfy_watchdog_requested_at < 15.0
        ):
            return
        task.managed_comfy_watchdog_requested_at = now
        await self._persist(task)
        try:
            host_result = await workload_lease.host_comfy_control(
                self._client,
                server=server,
                action="preempt",
                prompt_id=old_prompt_id,
                logical_task_id=task.id,
                lease_id=task.workload_lease_id,
                request_id=task.workload_request_id,
            )
            host_outcome = _host_terminal_outcome(
                host_result, task, action="preempt"
            )
        except Exception as exc:
            print(
                f"[Renderfin][Queue] managed watchdog proof deferred for "
                f"{task.id}/{old_prompt_id}: {exc}"
            )
            return

        # The authenticated bridge is necessary but not sufficient proof: its
        # terminal receipt must echo all four identities for this exact prompt.
        if not _host_managed_progress(host_result, task):
            print(
                f"[Renderfin][Queue] managed watchdog exact receipt missing for "
                f"{task.id}/{old_prompt_id}: {host_result}"
            )
            return
        if host_outcome == "completed":
            try:
                state, entry = await comfy_adapter.poll_history(
                    self._client, server, old_prompt_id
                )
                if state not in {"success", "completed"}:
                    print(
                        f"[Renderfin][Queue] watchdog Completed for {task.id} "
                        f"but history is {state}; retaining exact binding"
                    )
                    return
                await self._finish(
                    task, server, entry or {}, skip_lease_heartbeat=True
                )
            except Exception as exc:
                # Completion is authoritative; artifact transport failure must
                # not turn it into a second render or spend a retry.
                print(
                    f"[Renderfin][Queue] watchdog completed artifact deferred "
                    f"for {task.id}: {exc}"
                )
            return
        if host_outcome not in {"preempted", "released"}:
            print(
                f"[Renderfin][Queue] managed watchdog outcome ambiguous for "
                f"{task.id}/{old_prompt_id}: {host_result}"
            )
            return

        await self._release_workload(task, outcome="preempted", retry=True)
        print(
            f"[Renderfin][Queue] managed watchdog recalled {old_prompt_id} "
            f"({reason}); same task {task.id} returned Pending attempt-neutrally"
        )

    async def _submit_task(self, task: RenderTask, server: RenderServer) -> None:
        assert self._client is not None
        prompt = task.prompt
        workflow_file, forced = routing.resolve_workflow_file(prompt)
        runtime_name = routing.resolve_runtime_workflow(server, workflow_file)
        template_path = config.WORKFLOWS_DIR / runtime_name
        if not template_path.is_file():
            raise comfy_adapter.ComfyAdapterError(f"workflow template missing: {runtime_name}")
        template_text = template_path.read_text(encoding="utf-8")

        image_filename = ""
        if (prompt.image_url or "").strip():
            name, data = await comfy_adapter.download_input_image(self._client, prompt.image_url)
            image_filename = await comfy_adapter.upload_image(self._client, server, name, data)

        if forced:
            width, height = forced
        elif routing.is_image_request(prompt):
            width, height = routing.clamp_image_dims(prompt.main_size_width, prompt.main_size_height)
        else:
            width, height = routing.clamp_video_dims(prompt.main_size_width, prompt.main_size_height)

        workflow = templating.render_workflow_text(
            template_text,
            width=width,
            height=height,
            prompt=prompt.prompt,
            negative_prompt=prompt.negative_prompt,
            image_filename=image_filename,
            output_prefix=task.id,
            workflow_type=prompt.type,
            seed=prompt.noise_seed or None,
        )
        prompt_id = task.comfy_prompt_id or str(uuid.uuid4())
        task.comfy_prompt_id = prompt_id
        task.server_name = server.render_server_name
        task.status = TASK_RENDERING
        task.started_at = time.time()
        if task.managed_prompt and task.workload_lease_id:
            task.managed_comfy_progress_signature = json.dumps(
                {"state": "submitted", "stage": "", "marker": ""},
                sort_keys=True,
                separators=(",", ":"),
            )
            task.managed_comfy_progress_percent = -1.0
            task.managed_comfy_last_progress_at = task.started_at
            task.managed_comfy_host_stale_at = 0
            task.managed_comfy_watchdog_requested_at = 0
        # Durable identity first: the host registration and Comfy submit both
        # use this caller-supplied prompt_id. A crash never leaves an accepted
        # prompt that the Renderfin DB cannot identify.
        await self._persist(task)
        managed_identity = (
            {
                "logical_task_id": task.id,
                "lease_id": task.workload_lease_id,
                "request_id": task.workload_request_id,
            }
            if task.managed_prompt and task.workload_lease_id
            else None
        )
        if managed_identity:
            try:
                expected_submission_sha256 = (
                    comfy_adapter.managed_submission_sha256(
                        workflow,
                        managed_identity=managed_identity,
                        prompt_id=prompt_id,
                    )
                )
                registration = await workload_lease.host_comfy_control(
                    self._client,
                    server=server,
                    action="register",
                    prompt_id=prompt_id,
                    logical_task_id=task.id,
                    lease_id=task.workload_lease_id,
                    request_id=task.workload_request_id,
                    expected_canonical_submission_sha256=(
                        expected_submission_sha256
                    ),
                )
                registration_outcome = _host_terminal_outcome(
                    registration, task, action="register"
                )
            except workload_lease.HostComfyReceiptMismatch as exc:
                # The host may have processed register, but its response cannot
                # prove which prompt reached a terminal boundary. Preserve the
                # exact binding and retry the idempotent registration.
                task.started_at = 0
                task.managed_comfy_last_progress_at = 0
                await self._persist(task)
                raise ManagedComfyCleanupPending(str(exc)) from exc
            except workload_lease.WorkloadCapacityWait:
                # A known retryable rejection means no host registration was
                # granted. Retire the preallocated binding before releasing
                # central admission; attempts and stage clock remain neutral.
                await self._release_workload(
                    task, outcome="released", retry=True
                )
                raise
            except Exception as exc:
                # A timeout may have reached the host even though way-fr did
                # not receive the response. Preserve exact identity/lease and
                # let reconciliation prove whether registration exists.
                task.started_at = 0
                task.managed_comfy_last_progress_at = 0
                await self._persist(task)
                raise ManagedComfyCleanupPending(
                    f"host register outcome unknown: {exc}"
                ) from exc
            if registration_outcome in {"preempted", "released"}:
                await self._release_workload(
                    task, outcome="preempted", retry=True
                )
                raise workload_lease.WorkloadCapacityWait(
                    "managed_prompt_already_preempted", 1
                )
            if registration_outcome == "completed":
                # Never resubmit a terminal prompt id. Follow its durable
                # history/artifact path; completion will win central release.
                task.host_comfy_registered = True
                task.started_at = task.started_at or time.time()
                await self._persist(task)
                return
            task.host_comfy_registered = True
            await self._persist(task)
        try:
            returned_prompt_id = await comfy_adapter.submit(
                self._client,
                server,
                workflow,
                managed_identity=managed_identity,
                prompt_id=prompt_id,
            )
        except Exception as submit_exc:
            if managed_identity:
                try:
                    cleanup = await workload_lease.host_comfy_control(
                        self._client,
                        server=server,
                        action="preempt",
                        prompt_id=prompt_id,
                        logical_task_id=task.id,
                        lease_id=task.workload_lease_id,
                        request_id=task.workload_request_id,
                    )
                    if _host_terminal_outcome(
                        cleanup, task, action="preempt"
                    ) not in {
                        "preempted",
                        "released",
                    }:
                        raise RuntimeError(f"ambiguous cleanup outcome: {cleanup}")
                    task.host_comfy_registered = False
                    if isinstance(submit_exc, comfy_adapter.ComfyCapacityWait):
                        await self._release_workload(
                            task, outcome="released", retry=True
                        )
                except Exception as cleanup_exc:
                    print(
                        f"[Renderfin][Queue] host registration cleanup {task.id} "
                        f"deferred: {cleanup_exc}"
                    )
                    raise ManagedComfyCleanupPending(str(cleanup_exc)) from submit_exc
            raise submit_exc
        task.comfy_prompt_id = returned_prompt_id
        self._server_submit_cooldowns.pop(server.render_server_name, None)
        await self._persist(task)
        print(f"[Renderfin][Queue] task {task.id} -> {server.render_server_name} ({runtime_name})")

    async def _poll_rendering(self) -> None:
        assert self._client is not None
        for task in list(self._tasks.values()):
            if task.status != TASK_RENDERING:
                continue
            # Once a completed history entry is being downloaded, that
            # finisher exclusively owns lease heartbeat/terminal transition.
            # The regular poller must not concurrently preempt the same task.
            if task.id in self._finishers and not self._finishers[task.id].done():
                continue
            if task.workload_lease_id:
                server = self.registry.get(task.server_name)
                if server is None:
                    # A missing registry/tunnel is not proof that the managed
                    # host stopped. Preserve exact prompt+lease fail-closed;
                    # never clear/requeue onto a second physical GPU.
                    print(
                        f"[Renderfin][Queue] managed server {task.server_name} "
                        f"missing for {task.id}; preserving exact binding"
                    )
                    continue
                if task.managed_comfy_artifact_spool_state in {
                    "prepared",
                    "staged",
                    "central_persisted",
                    "acknowledged",
                }:
                    # Recovery no longer depends on Comfy /history once the
                    # exact primary path and any required isolated companion
                    # are durably recorded. Retry the same stage/GET/ACK
                    # identity directly; do not let artifact_spooled heartbeat
                    # gating hide the recovery path.
                    self._finishers[task.id] = asyncio.create_task(
                        self._finish_guarded(task, server, {})
                    )
                    continue
                if task.managed_prompt and not task.host_comfy_registered:
                    try:
                        # The earlier register response was unknown. Retry the
                        # idempotent register and caller-supplied prompt_id;
                        # started_at stays zero until this succeeds.
                        await self._submit_task(task, server)
                    except (
                        workload_lease.WorkloadCapacityWait,
                        comfy_adapter.ComfyCapacityWait,
                        ManagedComfyCleanupPending,
                    ) as exc:
                        print(
                            f"[Renderfin][Queue] managed submit recovery "
                            f"waiting for {task.id}: {exc}"
                        )
                    except Exception as exc:
                        print(
                            f"[Renderfin][Queue] managed submit recovery failed "
                            f"closed for {task.id}: {exc}"
                        )
                    continue
                host_heartbeat: Dict[str, Any] = {}
                host_completion_proven = False
                try:
                    await workload_lease.heartbeat(
                        self._client,
                        lease_id=task.workload_lease_id,
                        owner_task_id=task.id,
                        request_id=task.workload_request_id,
                        server=server,
                    )
                    host_heartbeat = await workload_lease.host_comfy_control(
                        self._client,
                        server=server,
                        action="heartbeat",
                        prompt_id=task.comfy_prompt_id,
                        logical_task_id=task.id,
                        lease_id=task.workload_lease_id,
                        request_id=task.workload_request_id,
                    )
                    host_heartbeat_outcome = _host_terminal_outcome(
                        host_heartbeat, task, action="heartbeat"
                    )
                    if host_heartbeat_outcome in {"preempted", "released"}:
                        old_prompt_id = task.comfy_prompt_id
                        await self._release_workload(
                            task, outcome="preempted", retry=True
                        )
                        print(
                            f"[Renderfin][Queue] host TTL/watchdog retired "
                            f"{old_prompt_id}; same task {task.id} returned Pending"
                        )
                        continue
                    if host_heartbeat_outcome == "completed":
                        host_completion_proven = True
                    task.workload_heartbeat_at = time.time()
                except (
                    workload_lease.WorkloadPreempted,
                    workload_lease.WorkloadLeaseTerminal,
                ):
                    if not task.managed_prompt:
                        print(
                            f"[Renderfin][Queue] fail-closed preempt proof missing "
                            f"for {task.id}/{task.comfy_prompt_id}"
                        )
                        continue
                    old_prompt_id = task.comfy_prompt_id
                    try:
                        host_result = await workload_lease.host_comfy_control(
                            self._client,
                            server=server,
                            action="preempt",
                            prompt_id=old_prompt_id,
                            logical_task_id=task.id,
                            lease_id=task.workload_lease_id,
                            request_id=task.workload_request_id,
                        )
                        host_outcome = _host_terminal_outcome(
                            host_result, task, action="preempt"
                        )
                    except Exception as exc:
                        print(
                            f"[Renderfin][Queue] fail-closed host preempt proof "
                            f"missing for {task.id}/{old_prompt_id}: {exc}"
                        )
                        continue
                    if host_outcome == "completed":
                        # The prompt crossed its terminal boundary before the
                        # stop request. Continue to history/artifact download;
                        # durable completion will release central admission.
                        host_completion_proven = True
                    elif host_outcome in {"preempted", "released"}:
                        await self._release_workload(
                            task, outcome="preempted", retry=True
                        )
                        print(
                            f"[Renderfin][Queue] centrally preempted {task.id}; "
                            f"old prompt {old_prompt_id} will never be polled/reposted"
                        )
                        continue
                    else:
                        print(
                            f"[Renderfin][Queue] fail-closed ambiguous host "
                            f"preempt outcome for {task.id}: {host_result}"
                        )
                        continue
                except workload_lease.WorkloadCapacityWait as exc:
                    print(f"[Renderfin][Queue] lease heartbeat wait {task.id}: {exc.status}")
                    watchdog_reason = await self._managed_watchdog_reason(task)
                    if watchdog_reason:
                        await self._recall_managed_prompt(
                            task, server, reason=watchdog_reason
                        )
                    continue
                except Exception as exc:
                    print(f"[Renderfin][Queue] lease heartbeat failed {task.id}: {exc}")
                    watchdog_reason = await self._managed_watchdog_reason(task)
                    if watchdog_reason:
                        await self._recall_managed_prompt(
                            task, server, reason=watchdog_reason
                        )
                    continue
                if task.managed_prompt and not host_completion_proven:
                    watchdog_reason = await self._managed_watchdog_reason(
                        task, host_heartbeat
                    )
                    if watchdog_reason:
                        await self._recall_managed_prompt(
                            task, server, reason=watchdog_reason
                        )
                        continue
            if (
                not (task.managed_prompt and task.workload_lease_id)
                and time.time() - task.started_at > config.TASK_TIMEOUT_SECONDS
            ):
                await self._fail(task, "render timeout")
                continue
            server = self.registry.get(task.server_name)
            if server is None:
                await self._fail(task, f"server {task.server_name} vanished")
                continue
            try:
                state, entry = await comfy_adapter.poll_history(
                    self._client, server, task.comfy_prompt_id
                )
            except Exception as exc:
                print(f"[Renderfin][Queue] poll {task.id} failed: {exc!r}")
                continue
            if state == "unknown":
                # the worker has no record of this prompt: either it is queued
                # but not started, or the worker forgot it (restart/crash).
                # Only /queue distinguishes the two, and a forgotten prompt
                # would otherwise hold this server for the whole timeout.
                try:
                    still_queued = await comfy_adapter.queue_contains(
                        self._client, server, task.comfy_prompt_id
                    )
                except Exception as exc:
                    print(f"[Renderfin][Queue] queue check {task.id} failed: {exc!r}")
                    continue
                if still_queued:
                    continue
                if task.managed_prompt and task.workload_lease_id:
                    try:
                        host_result = await workload_lease.host_comfy_control(
                            self._client,
                            server=server,
                            action="preempt",
                            prompt_id=task.comfy_prompt_id,
                            logical_task_id=task.id,
                            lease_id=task.workload_lease_id,
                            request_id=task.workload_request_id,
                        )
                        host_outcome = _host_terminal_outcome(
                            host_result, task, action="preempt"
                        )
                    except Exception as exc:
                        print(
                            f"[Renderfin][Queue] vanished prompt {task.id} "
                            f"not retired without host proof: {exc}"
                        )
                        continue
                    if host_outcome == "completed":
                        # History may lag the host terminal ledger. Keep the
                        # exact old identity and retry; never create hidden work.
                        continue
                    if host_outcome not in {"preempted", "released"}:
                        print(
                            f"[Renderfin][Queue] vanished prompt {task.id} has "
                            f"ambiguous host outcome: {host_result}"
                        )
                        continue
                    old_prompt_id = task.comfy_prompt_id
                    await self._release_workload(
                        task, outcome="preempted", retry=True
                    )
                    print(
                        f"[Renderfin][Queue] vanished managed prompt {old_prompt_id} "
                        f"retired; same task {task.id} returned Pending"
                    )
                    continue
                print(
                    f"[Renderfin][Queue] task {task.id} vanished from "
                    f"{task.server_name}; requeueing"
                )
                task.status = TASK_PENDING
                task.server_name = ""
                task.comfy_prompt_id = ""
                task.started_at = 0
                await self._persist(task)
                continue
            if state == "pending":
                continue
            if state == "error":
                err = ""
                if entry:
                    err = json.dumps(entry.get("status", {}))[:500]
                await self._fail(task, f"comfy error: {err}")
                continue
            # Finish (download artifacts) off the pump so a slow transfer cannot
            # stall dispatch or status polling for every other task.
            if task.id in self._finishers and not self._finishers[task.id].done():
                continue
            self._finishers[task.id] = asyncio.create_task(
                self._finish_guarded(task, server, entry or {})
            )

    async def _validate_tpose_bundle_bytes(
        self,
        task: RenderTask,
        server: RenderServer,
        *,
        primary_data: bytes,
        isolated_data: bytes,
        primary_artifact: Optional[Dict[str, str]] = None,
        isolated_artifact: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        assert self._client is not None
        reference_data: Optional[bytes] = None
        try:
            _reference_name, reference_data = await comfy_adapter.download_input_image(
                self._client, task.prompt.image_url
            )
            return image_quality.validate_tpose_bundle(
                primary_data,
                isolated_data,
                reference_data,
            )
        except image_quality.RenderArtifactQualityError as exc:
            report = dict(exc.report)
            report["context"] = {
                "task_id": task.id,
                "server_name": server.render_server_name,
                "prompt_id": task.comfy_prompt_id,
                "primary_artifact": dict(primary_artifact or {}),
                "isolated_artifact": dict(isolated_artifact or {}),
            }
            try:
                archived = image_quality.archive_rejected_bundle(
                    config.DATA_DIR / "rejected" / "tpose" / task.id,
                    primary_bytes=primary_data,
                    isolated_bytes=isolated_data,
                    reference_bytes=reference_data,
                    report=report,
                    label=server.render_server_name or "unknown-server",
                )
                print(
                    f"[Renderfin][Queue] rejected T-pose bundle {task.id} "
                    f"archived at {archived}"
                )
            except Exception as archive_exc:
                # Archiving is evidence preservation, not permission to accept
                # bytes that already failed the quality contract.
                print(
                    f"[Renderfin][Queue] rejected bundle archive failed for "
                    f"{task.id}: {archive_exc}"
                )
            raise
        except Exception as exc:
            raise image_quality.RenderArtifactQualityError(
                "control_mask_reference_unavailable",
                "the exact T-pose control input could not be loaded for validation",
                {
                    "schema": "renderfin.tpose_bundle_quality.v1",
                    "passed": False,
                    "task_id": task.id,
                    "server_name": server.render_server_name,
                    "error_type": type(exc).__name__,
                },
            ) from exc

    async def _reject_artifact_quality(
        self,
        task: RenderTask,
        server: RenderServer,
        exc: image_quality.RenderArtifactQualityError,
    ) -> None:
        """Retire one exact bad Comfy result and quarantine its producer."""

        cooldown = max(
            60.0,
            float(os.getenv("RENDERFIN_RENDER_QUALITY_COOLDOWN_SECONDS", "3600")),
        )
        self._server_submit_cooldowns[server.render_server_name] = (
            time.time() + cooldown
        )
        server.status = "render_quality_error"
        self.registry.save(server)
        error = (
            f"render artifact quality rejected on {server.render_server_name}: "
            f"{exc}"
        )

        host_outcome = ""
        if task.managed_prompt and task.workload_lease_id:
            if (
                task.managed_comfy_artifact_spool_state == "central_persisted"
                and self._managed_bundle_is_durable(task)
            ):
                acknowledgement = await self._ack_managed_artifact(task, server)
                host_outcome = _host_terminal_outcome(
                    acknowledgement, task, action="ack"
                )
                if host_outcome != "completed":
                    raise workload_lease.HostComfyArtifactWait(
                        "bad_artifact_archive_ack_not_completed", 2
                    )
                task.managed_comfy_artifact_spool_state = "acknowledged"
            else:
                terminal = await workload_lease.host_comfy_control(
                    self._client,
                    server=server,
                    action="preempt",
                    prompt_id=task.comfy_prompt_id,
                    logical_task_id=task.id,
                    lease_id=task.workload_lease_id,
                    request_id=task.workload_request_id,
                )
                host_outcome = _host_terminal_outcome(
                    terminal, task, action="preempt"
                )
                if host_outcome not in {"completed", "preempted", "released"}:
                    raise workload_lease.HostComfyArtifactWait(
                        "bad_artifact_terminal_proof_pending", 2
                    )

        task.status = TASK_ERROR
        task.error = error[:1000]
        task.finished_at = time.time()
        await self._persist(task)
        if task.workload_lease_id:
            await self._release_workload(
                task,
                outcome=("completed" if host_outcome == "completed" else "preempted"),
            )
        print(
            f"[Renderfin][Queue] task {task.id} rejected by quality gate; "
            f"{server.render_server_name} cooled down for {int(cooldown)}s"
        )

    async def _finish_guarded(self, task: RenderTask, server: RenderServer, entry: dict) -> None:
        try:
            async with self._download_slots:
                await self._finish(task, server, entry)
        except asyncio.CancelledError:
            raise
        except image_quality.RenderArtifactQualityError as exc:
            try:
                await self._reject_artifact_quality(task, server, exc)
            except Exception as reject_exc:
                # Keep the exact managed binding when terminal proof/ACK is
                # temporarily unavailable. The next poll retries this same
                # artifact and never admits a duplicate prompt.
                print(
                    f"[Renderfin][Queue] quality rejection deferred for "
                    f"{task.id}: {reject_exc}"
                )
        except (
            comfy_adapter.ComfyCapacityWait,
            workload_lease.WorkloadCapacityWait,
        ) as exc:
            # The prompt has already completed and its history entry is durable,
            # but a shared node may grant Hunyuan the GPU lease before we fetch
            # /view.  Keep following the same prompt: once Comfy is restored the
            # next poll will retry the artifact without spending a render attempt
            # or holding the character job in a long fleet-error cooldown.
            print(f"[Renderfin][Queue] task {task.id} artifact gated; retrying: {exc}")
        except (
            workload_lease.WorkloadPreempted,
            workload_lease.WorkloadLeaseTerminal,
        ):
            server = self.registry.get(task.server_name)
            if server is None or not task.managed_prompt:
                print(f"[Renderfin][Queue] fail-closed artifact preempt for {task.id}")
                return
            try:
                old_prompt_id = task.comfy_prompt_id
                host_result = await workload_lease.host_comfy_control(
                    self._client,
                    server=server,
                    action="preempt",
                    prompt_id=old_prompt_id,
                    logical_task_id=task.id,
                    lease_id=task.workload_lease_id,
                    request_id=task.workload_request_id,
                )
                host_outcome = _host_terminal_outcome(
                    host_result, task, action="preempt"
                )
                if host_outcome == "completed":
                    await self._finish(
                        task, server, entry, skip_lease_heartbeat=True
                    )
                elif host_outcome in {"preempted", "released"}:
                    await self._release_workload(
                        task, outcome="preempted", retry=True
                    )
                    print(
                        f"[Renderfin][Queue] artifact-stage preempted {task.id}; "
                        f"retired prompt {old_prompt_id}"
                    )
                else:
                    print(
                        f"[Renderfin][Queue] artifact preempt outcome ambiguous: "
                        f"{host_result}"
                    )
            except Exception as exc:
                print(f"[Renderfin][Queue] artifact preempt proof deferred: {exc}")
        except Exception as exc:
            await self._fail(task, f"artifact download failed: {exc}")
        finally:
            self._finishers.pop(task.id, None)

    async def _finish(
        self,
        task: RenderTask,
        server: RenderServer,
        entry: dict,
        *,
        skip_lease_heartbeat: bool = False,
    ) -> None:
        assert self._client is not None
        if self._uses_managed_artifact_spool(task, server):
            await self._finish_managed_spooled(task, server, entry)
            return
        preferred = ""
        ptype = (task.prompt.type or "").strip().lower()
        if ptype in ("t_pose", "t_poses", "inpaint"):
            preferred = "_Isolated_"
        artifacts = comfy_adapter.resolve_artifacts(
            entry, output_ext=task.output_ext, preferred_fragment=preferred
        )
        if not artifacts:
            raise _artifact_contract_error(
                task,
                "real_output_artifact_missing",
                "history contains no non-temporary output artifact",
                [],
            )
        history_artifacts = artifacts
        if ptype in {"t_pose", "t_poses"}:
            artifacts = [
                artifact
                for artifact in history_artifacts
                if _artifact_owned_by_task(artifact, task.id)
            ]
            if not artifacts:
                raise _artifact_contract_error(
                    task,
                    "task_owned_artifact_missing",
                    "history contains no T-pose output owned by this logical task",
                    history_artifacts,
                )

        user_dir = config.RENDER_DIR / task.prompt.user_name
        user_dir.mkdir(parents=True, exist_ok=True)

        # Primary artifact: for t_pose the primary is the FULL render (no
        # _Isolated_ fragment) at output_url; the isolated one is stored as an
        # extra output. For inpaint the C# primary IS the isolated file.
        primary = artifacts[0]
        isolated: List[Dict[str, str]] = []
        if ptype in ("t_pose", "t_poses"):
            non_isolated = [
                a for a in artifacts if "_isolated_" not in a.get("filename", "").lower()
            ]
            isolated = [
                a for a in artifacts if "_isolated_" in a.get("filename", "").lower()
            ]
            if not non_isolated or not isolated:
                raise _artifact_contract_error(
                    task,
                    "tpose_output_bundle_incomplete",
                    "T-pose output requires task-owned FULL and _Isolated_ images",
                    artifacts,
                )
            primary = non_isolated[0]

        if task.workload_lease_id and not skip_lease_heartbeat:
            await workload_lease.heartbeat(
                self._client,
                lease_id=task.workload_lease_id,
                owner_task_id=task.id,
                request_id=task.workload_request_id,
                server=server,
                ttl_seconds=900,
            )
            host_heartbeat = await workload_lease.host_comfy_control(
                self._client,
                server=server,
                action="heartbeat",
                prompt_id=task.comfy_prompt_id,
                logical_task_id=task.id,
                lease_id=task.workload_lease_id,
                request_id=task.workload_request_id,
                ttl_seconds=900,
            )
            _host_terminal_outcome(host_heartbeat, task, action="heartbeat")
        data = await comfy_adapter.download_artifact(self._client, server, primary)
        iso_data: Optional[bytes] = None
        if ptype in ("t_pose", "t_poses"):
            iso_data = await comfy_adapter.download_artifact(
                self._client, server, isolated[0]
            )
            await self._validate_tpose_bundle_bytes(
                task,
                server,
                primary_data=data,
                isolated_data=iso_data,
                primary_artifact=primary,
                isolated_artifact=isolated[0],
            )

        out_path = user_dir / f"{task.id}{task.output_ext}"
        _atomic_fsync_bytes(out_path, data)
        task.output_path = str(out_path)
        if task.output_ext == ".png":
            _jpeg_sibling(out_path)

        if ptype in ("t_pose", "t_poses"):
            assert iso_data is not None
            iso_path = user_dir / f"{task.id}_Isolated.png"
            _atomic_fsync_bytes(iso_path, iso_data)
            task.extra_outputs["isolated"] = (
                f"{config.PUBLIC_BASE_URL}/render/{task.prompt.user_name}/{task.id}_Isolated.png"
            )

        elapsed = time.time() - task.started_at
        server.average_render_time = (
            elapsed
            if not server.average_render_time
            else (server.average_render_time * 0.7 + elapsed * 0.3)
        )
        self.registry.save(server)

        task.status = TASK_DONE
        task.finished_at = time.time()
        task.artifact_sha256 = hashlib.sha256(data).hexdigest()
        # Artifact and terminal state are durable before either the host-local
        # or central lease is released. Completion therefore wins a concurrent
        # preemption/restart race without re-rendering the logical task.
        await self._persist(task)
        if task.workload_lease_id:
            try:
                completion = await workload_lease.host_comfy_control(
                    self._client,
                    server=server,
                    action="complete",
                    prompt_id=task.comfy_prompt_id,
                    logical_task_id=task.id,
                    lease_id=task.workload_lease_id,
                    request_id=task.workload_request_id,
                    artifact_sha256=task.artifact_sha256,
                )
                if _host_terminal_outcome(
                    completion, task, action="complete"
                ) != "completed":
                    raise workload_lease.WorkloadCapacityWait(
                        "host_comfy_complete_receipt_pending", 2
                    )
                await self._release_workload(task, outcome="completed")
            except Exception as exc:
                print(
                    f"[Renderfin][Queue] completed {task.id}; lease release "
                    f"deferred to watchdog/TTL: {exc}"
                )
        print(f"[Renderfin][Queue] task {task.id} done in {elapsed:.0f}s -> {task.output_url}")

    async def _finish_managed_spooled(
        self,
        task: RenderTask,
        server: RenderServer,
        entry: Dict[str, Any],
    ) -> None:
        """Complete the exact host spool v1 handshake without GPU ambiguity.

        For t-pose, `_Isolated_` must be centrally fsynced first because the
        v1 host spool intentionally accepts one artifact per four-part prompt
        identity. The FULL primary is then staged, streamed to a central
        fsynced file and ACKed with a deterministic receipt covering both.
        """

        assert self._client is not None
        state = str(task.managed_comfy_artifact_spool_state or "").strip()
        allowed_states = {
            "",
            "prepared",
            "staged",
            "central_persisted",
            "acknowledged",
        }
        if state not in allowed_states:
            raise workload_lease.HostComfyArtifactWait(
                "central_managed_comfy_spool_state_invalid", 2
            )
        ptype = str(task.prompt.type or "").strip().lower()
        user_dir = config.RENDER_DIR / task.prompt.user_name
        user_dir.mkdir(parents=True, exist_ok=True)
        out_path = user_dir / f"{task.id}{task.output_ext}"

        if not task.managed_comfy_artifact_relative_path_string:
            preferred = "_Isolated_" if ptype in {"t_pose", "t_poses", "inpaint"} else ""
            artifacts = comfy_adapter.resolve_artifacts(
                entry,
                output_ext=task.output_ext,
                preferred_fragment=preferred,
            )
            if not artifacts:
                raise _artifact_contract_error(
                    task,
                    "real_output_artifact_missing",
                    "managed history contains no non-temporary output artifact",
                    [],
                )
            history_artifacts = artifacts
            if ptype in {"t_pose", "t_poses"}:
                artifacts = [
                    artifact
                    for artifact in history_artifacts
                    if _artifact_owned_by_task(artifact, task.id)
                ]
                if not artifacts:
                    raise _artifact_contract_error(
                        task,
                        "task_owned_artifact_missing",
                        "managed history contains no T-pose output owned by this logical task",
                        history_artifacts,
                    )
            primary = artifacts[0]
            if ptype in {"t_pose", "t_poses"}:
                non_isolated = [
                    artifact
                    for artifact in artifacts
                    if "_isolated_"
                    not in str(artifact.get("filename") or "").lower()
                ]
                isolated = [
                    artifact
                    for artifact in artifacts
                    if "_isolated_"
                    in str(artifact.get("filename") or "").lower()
                ]
                if not non_isolated or not isolated:
                    raise _artifact_contract_error(
                        task,
                        "tpose_output_bundle_incomplete",
                        "managed T-pose output requires task-owned FULL and _Isolated_ images",
                        artifacts,
                    )
                primary = non_isolated[0]
                # The singular spool cannot safely detach until this required
                # companion exists durably on central storage.
                isolated_data = await comfy_adapter.download_artifact(
                    self._client, server, isolated[0]
                )
                isolated_path = user_dir / f"{task.id}_Isolated.png"
                isolated_sha, isolated_size = _atomic_fsync_bytes(
                    isolated_path, isolated_data
                )
                task.managed_comfy_isolated_output_path = str(isolated_path)
                task.managed_comfy_isolated_sha256 = isolated_sha
                task.managed_comfy_isolated_size_int = isolated_size
                task.extra_outputs["isolated"] = (
                    f"{config.PUBLIC_BASE_URL}/render/"
                    f"{task.prompt.user_name}/{task.id}_Isolated.png"
                )
            task.managed_comfy_artifact_relative_path_string = (
                _managed_artifact_relative_path(primary)
            )
            task.managed_comfy_artifact_spool_state = "prepared"
            # Persist path provenance and the isolated checksum before stage:
            # a crash after host detach can resume without /view.
            await self._persist(task)
            state = "prepared"

        if ptype in {"t_pose", "t_poses"}:
            if not (
                task.managed_comfy_isolated_output_path
                and workload_lease.verify_central_artifact(
                    Path(task.managed_comfy_isolated_output_path),
                    expected_sha256=task.managed_comfy_isolated_sha256,
                    expected_size=task.managed_comfy_isolated_size_int,
                )
            ):
                raise workload_lease.HostComfyArtifactWait(
                    "central_managed_comfy_isolated_not_durable", 2
                )
            task.extra_outputs["isolated"] = (
                f"{config.PUBLIC_BASE_URL}/render/"
                f"{task.prompt.user_name}/{task.id}_Isolated.png"
            )

        if state == "prepared":
            staged = await workload_lease.host_comfy_stage_artifact(
                self._client,
                server=server,
                prompt_id=task.comfy_prompt_id,
                logical_task_id=task.id,
                lease_id=task.workload_lease_id,
                request_id=task.workload_request_id,
                artifact_relative_path_string=(
                    task.managed_comfy_artifact_relative_path_string
                ),
            )
            stage_outcome = _host_terminal_outcome(
                staged, task, action="stage"
            )
            if stage_outcome == "completed":
                # Completed wins and the old binding remains authoritative,
                # but ACK can only have happened after a durable local record.
                # With no such record here, fail closed instead of inventing
                # persistence or re-rendering.
                raise workload_lease.HostComfyArtifactWait(
                    "host_comfy_stage_completed_without_central_receipt", 2
                )
            task.artifact_sha256 = str(
                staged.get("artifact_sha256") or ""
            ).strip().lower()
            task.managed_comfy_artifact_size_int = int(
                staged.get("artifact_size_int") or 0
            )
            task.managed_comfy_artifact_spool_protocol_string = str(
                staged.get("artifact_spool_protocol_string") or ""
            ).strip()
            task.managed_comfy_artifact_spool_state = "staged"
            await self._persist(task)
            state = "staged"

        if state == "staged":
            await workload_lease.host_comfy_download_artifact(
                self._client,
                server=server,
                prompt_id=task.comfy_prompt_id,
                logical_task_id=task.id,
                lease_id=task.workload_lease_id,
                request_id=task.workload_request_id,
                destination_path=out_path,
                expected_sha256=task.artifact_sha256,
                expected_size_int=task.managed_comfy_artifact_size_int,
            )
            task.output_path = str(out_path)
            if task.output_ext == ".png" and ptype not in {"t_pose", "t_poses"}:
                _jpeg_sibling(out_path)
            task.managed_comfy_central_persistence_receipt_id_string = (
                _bundle_receipt_id(task)
            )
            task.managed_comfy_artifact_spool_state = "central_persisted"
            # The fsynced bundle checksums and receipt must survive a process
            # crash before the host is allowed to tombstone/delete its bytes.
            await self._persist(task)
            state = "central_persisted"

        if state in {"central_persisted", "acknowledged"} and ptype in {
            "t_pose",
            "t_poses",
        }:
            primary_data = out_path.read_bytes()
            isolated_path = Path(task.managed_comfy_isolated_output_path)
            isolated_data = isolated_path.read_bytes()
            await self._validate_tpose_bundle_bytes(
                task,
                server,
                primary_data=primary_data,
                isolated_data=isolated_data,
                primary_artifact={
                    "filename": PurePosixPath(
                        task.managed_comfy_artifact_relative_path_string
                    ).name,
                    "subfolder": str(
                        PurePosixPath(
                            task.managed_comfy_artifact_relative_path_string
                        ).parent
                    ),
                    "type": "output",
                },
                isolated_artifact={
                    "filename": isolated_path.name,
                    "type": "central",
                },
            )
            if task.output_ext == ".png":
                _jpeg_sibling(out_path)

        if state == "central_persisted":
            acknowledgement = await self._ack_managed_artifact(task, server)
            if _host_terminal_outcome(
                acknowledgement, task, action="ack"
            ) != "completed":
                raise workload_lease.HostComfyArtifactWait(
                    "host_comfy_artifact_ack_not_completed", 2
                )
            task.managed_comfy_artifact_spool_state = "acknowledged"
            state = "acknowledged"

        if state != "acknowledged" or not self._managed_bundle_is_durable(task):
            raise workload_lease.HostComfyArtifactWait(
                "central_managed_comfy_bundle_completion_unproven", 2
            )

        elapsed = max(0.0, time.time() - float(task.started_at or time.time()))
        server.average_render_time = (
            elapsed
            if not server.average_render_time
            else (server.average_render_time * 0.7 + elapsed * 0.3)
        )
        self.registry.save(server)
        task.status = TASK_DONE
        task.finished_at = time.time()
        await self._persist(task)
        try:
            await self._release_workload(task, outcome="completed")
        except Exception as exc:
            print(
                f"[Renderfin][Queue] spooled completion {task.id}; central "
                f"lease release deferred to reconciliation/TTL: {exc}"
            )
        print(
            f"[Renderfin][Queue] task {task.id} spooled bundle done in "
            f"{elapsed:.0f}s -> {task.output_url}"
        )

    async def _fail(self, task: RenderTask, error: str) -> None:
        if task.managed_prompt and task.workload_lease_id:
            if self._client is None or not task.server_name or not task.comfy_prompt_id:
                # An incomplete local control identity is itself ambiguous.  It
                # cannot authorize releasing the central lease or forgetting a
                # potentially live managed prompt.
                print(
                    f"[Renderfin][Queue] managed failure proof deferred for "
                    f"{task.id}: exact host control identity is unavailable"
                )
                await self._persist(task)
                return
            server = self.registry.get(task.server_name)
            if server is None:
                # Losing the registry/tunnel is not proof that the host prompt
                # stopped.  Preserve the exact binding so restart/recovery can
                # reconcile it without admitting duplicate GPU work.
                print(
                    f"[Renderfin][Queue] managed failure proof deferred for "
                    f"{task.id}: server {task.server_name} is unavailable"
                )
                await self._persist(task)
                return
            try:
                host_result = await workload_lease.host_comfy_control(
                    self._client,
                    server=server,
                    action="preempt",
                    prompt_id=task.comfy_prompt_id,
                    logical_task_id=task.id,
                    lease_id=task.workload_lease_id,
                    request_id=task.workload_request_id,
                )
                host_outcome = _host_terminal_outcome(
                    host_result, task, action="preempt"
                )
            except Exception as exc:
                # A timeout, bridge failure, or ambiguous response cannot
                # authorize a DB Error transition or central lease release.
                # The next poll retries the same exact prompt identity.
                print(
                    f"[Renderfin][Queue] managed failure proof deferred for "
                    f"{task.id}: {exc}"
                )
                await self._persist(task)
                return

            if host_outcome == "completed":
                # Completion wins the timeout/error race.  Keep the old prompt
                # and lease, obtain its history entry, then use the normal
                # download + checksum + durable Done + complete/release path.
                try:
                    state, entry = await comfy_adapter.poll_history(
                        self._client, server, task.comfy_prompt_id
                    )
                    if state not in {"success", "completed"}:
                        print(
                            f"[Renderfin][Queue] completed managed prompt "
                            f"{task.id} history is not ready ({state}); retaining binding"
                        )
                        await self._persist(task)
                        return
                    await self._finish(
                        task,
                        server,
                        entry or {},
                        skip_lease_heartbeat=True,
                    )
                except Exception as exc:
                    # Host completion is already proven.  Artifact retrieval
                    # may be temporarily gated or incomplete, but it must never
                    # turn the logical task into Error or cause a re-render.
                    print(
                        f"[Renderfin][Queue] completed managed prompt "
                        f"{task.id} artifact completion deferred: {exc}"
                    )
                    await self._persist(task)
                return

            if host_outcome not in {"preempted", "released"}:
                print(
                    f"[Renderfin][Queue] managed failure proof ambiguous for "
                    f"{task.id}: {host_result}"
                )
                await self._persist(task)
                return

            # Exact host Preempted/Released proof is the only managed failure
            # result that permits the logical task to become Error.  Release
            # central admission only after that proof, using the matching
            # terminal outcome rather than a generic release.
            task.status = TASK_ERROR
            task.error = error[:1000]
            task.finished_at = time.time()
            await self._persist(task)
            await self._release_workload(task, outcome="preempted")
            print(f"[Renderfin][Queue] task {task.id} FAILED after exact preempt: {error[:200]}")
            return

        task.status = TASK_ERROR
        task.error = error[:1000]
        task.finished_at = time.time()
        await self._persist(task)
        if self._client is not None:
            await self._release_workload(task, outcome="released")
        print(f"[Renderfin][Queue] task {task.id} FAILED: {error[:200]}")
