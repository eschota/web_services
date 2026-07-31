"""Render task queue: sqlite persistence + single async pump (port of C# TaskQueue)."""
from __future__ import annotations

import asyncio
import io
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import aiosqlite
import httpx

from . import comfy_adapter, config, routing, templating
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
        self._stopped.clear()
        self._pump_task = asyncio.create_task(self._pump())

    async def stop(self) -> None:
        self._stopped.set()
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
        async with self._db.execute(
            "SELECT payload FROM render_tasks WHERE status IN (?, ?)",
            (TASK_PENDING, TASK_RENDERING),
        ) as cur:
            rows = await cur.fetchall()
        for (payload,) in rows:
            try:
                task = RenderTask(**json.loads(payload))
            except Exception as exc:
                print(f"[Renderfin][Queue] resurrect skip: {exc}")
                continue
            if task.status == TASK_RENDERING:
                task.status = TASK_PENDING
                task.server_name = ""
                task.comfy_prompt_id = ""
            self._tasks[task.id] = task
            await self._persist(task)
        if self._tasks:
            print(f"[Renderfin][Queue] resurrected {len(self._tasks)} task(s)")

    # ---------- public API ----------

    async def enqueue(self, prompt: RenderPrompt) -> RenderTask:
        token = routing.scheduling_token(prompt)
        workflow_file, forced = routing.resolve_workflow_file(prompt)
        ext = routing.output_extension(prompt)
        task = RenderTask(prompt=prompt, workflow=token, workflow_file=workflow_file, output_ext=ext)
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
        if self._tick_count % config.STATUS_REFRESH_TICKS == 1:
            await self._refresh_servers()
        now = time.time()
        if now - self._last_dispatch >= config.DISPATCH_INTERVAL_SECONDS:
            dispatched = await self._dispatch_one()
            if dispatched:
                self._last_dispatch = now
        await self._poll_rendering()

    async def _refresh_servers(self) -> None:
        assert self._client is not None
        for server in self.registry.all():
            online = await comfy_adapter.check_server_online(self._client, server)
            new_status = "online" if online else "offline"
            if server.status != new_status:
                server.status = new_status
                self.registry.save(server)

    def _busy_servers(self) -> Dict[str, str]:
        busy: Dict[str, str] = {}
        for task in self._tasks.values():
            if task.status == TASK_RENDERING and task.server_name:
                busy[task.server_name] = task.id
        return busy

    def _pick_server(self, token: str) -> Optional[RenderServer]:
        busy = self._busy_servers()
        candidates = [
            s
            for s in self.registry.all()
            if s.status == "online"
            and routing.server_can_run(s, token)
            and s.render_server_name not in busy
        ]
        candidates.sort(key=lambda s: s.average_render_time or 1e9)
        return candidates[0] if candidates else None

    async def _dispatch_one(self) -> bool:
        pending = sorted(
            (t for t in self._tasks.values() if t.status == TASK_PENDING),
            key=lambda t: t.created_at,
        )
        for task in pending:
            server = self._pick_server(task.workflow)
            if server is None:
                continue
            try:
                await self._submit_task(task, server)
                return True
            except Exception as exc:
                print(f"[Renderfin][Queue] submit {task.id} to {server.render_server_name} failed: {exc}")
                server.status = "render_error"
                self.registry.save(server)
                continue
        return False

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
        prompt_id = await comfy_adapter.submit(self._client, server, workflow)
        task.comfy_prompt_id = prompt_id
        task.server_name = server.render_server_name
        task.status = TASK_RENDERING
        task.started_at = time.time()
        await self._persist(task)
        print(f"[Renderfin][Queue] task {task.id} -> {server.render_server_name} ({runtime_name})")

    async def _poll_rendering(self) -> None:
        assert self._client is not None
        for task in list(self._tasks.values()):
            if task.status != TASK_RENDERING:
                continue
            if time.time() - task.started_at > config.TASK_TIMEOUT_SECONDS:
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
                print(f"[Renderfin][Queue] poll {task.id} failed: {exc}")
                continue
            if state == "pending":
                continue
            if state == "error":
                err = ""
                if entry:
                    err = json.dumps(entry.get("status", {}))[:500]
                await self._fail(task, f"comfy error: {err}")
                continue
            try:
                await self._finish(task, server, entry or {})
            except Exception as exc:
                await self._fail(task, f"artifact download failed: {exc}")

    async def _finish(self, task: RenderTask, server: RenderServer, entry: dict) -> None:
        assert self._client is not None
        preferred = ""
        ptype = (task.prompt.type or "").strip().lower()
        if ptype in ("t_pose", "t_poses", "inpaint"):
            preferred = "_Isolated_"
        artifacts = comfy_adapter.resolve_artifacts(
            entry, output_ext=task.output_ext, preferred_fragment=preferred
        )
        if not artifacts:
            raise comfy_adapter.ComfyAdapterError("no artifacts in history outputs")

        user_dir = config.RENDER_DIR / task.prompt.user_name
        user_dir.mkdir(parents=True, exist_ok=True)

        # Primary artifact: for t_pose the primary is the FULL render (no
        # _Isolated_ fragment) at output_url; the isolated one is stored as an
        # extra output. For inpaint the C# primary IS the isolated file.
        primary = artifacts[0]
        if ptype in ("t_pose", "t_poses"):
            non_isolated = [
                a for a in artifacts if "_isolated_" not in a.get("filename", "").lower()
            ]
            if non_isolated:
                primary = non_isolated[0]

        data = await comfy_adapter.download_artifact(self._client, server, primary)
        out_path = user_dir / f"{task.id}{task.output_ext}"
        out_path.write_bytes(data)
        task.output_path = str(out_path)
        if task.output_ext == ".png":
            _jpeg_sibling(out_path)

        if ptype in ("t_pose", "t_poses"):
            isolated = [
                a for a in artifacts if "_isolated_" in a.get("filename", "").lower()
            ]
            if isolated:
                iso_data = await comfy_adapter.download_artifact(self._client, server, isolated[0])
                iso_path = user_dir / f"{task.id}_Isolated.png"
                iso_path.write_bytes(iso_data)
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
        await self._persist(task)
        print(f"[Renderfin][Queue] task {task.id} done in {elapsed:.0f}s -> {task.output_url}")

    async def _fail(self, task: RenderTask, error: str) -> None:
        task.status = TASK_ERROR
        task.error = error[:1000]
        task.finished_at = time.time()
        await self._persist(task)
        print(f"[Renderfin][Queue] task {task.id} FAILED: {error[:200]}")
