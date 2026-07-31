"""Composite character-generation pipeline orchestrator.

Stages: flux_render (t_pose image + isolated alpha) -> hunyuan (image_to_3d GLB)
-> turntable (6s orbit mp4) -> ready. Stage state is persisted to sqlite so the
service can resume interrupted jobs after a restart.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Optional

import aiosqlite
import httpx

from . import config, hunyuan_client, turntable
from .models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_SUBMITTED,
    CHARGEN_STAGE_TURNTABLE,
    TASK_DONE,
    CharacterGenJob,
    RenderPrompt,
)
from .queue import RenderQueue

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chargen_jobs (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    stage TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chargen_stage ON chargen_jobs(stage);
"""

_ACTIVE_STAGES = (CHARGEN_STAGE_FLUX, CHARGEN_STAGE_HUNYUAN, CHARGEN_STAGE_TURNTABLE)

FLUX_STAGE_TIMEOUT = 1800
HUNYUAN_STAGE_TIMEOUT = 1800


class CharacterGenManager:
    def __init__(self, queue: RenderQueue, *, db_path: Optional[Path] = None):
        self.queue = queue
        self.db_path = Path(db_path or config.DB_PATH)
        self._db: Optional[aiosqlite.Connection] = None
        self._jobs: Dict[str, CharacterGenJob] = {}
        self._runners: Dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=10000")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        await self._load()
        for job in self._jobs.values():
            if job.stage in _ACTIVE_STAGES:
                print(f"[Renderfin][CharGen] resuming job {job.id} at stage {job.stage}")
                self._spawn(job)

    async def stop(self) -> None:
        for runner in self._runners.values():
            runner.cancel()
        for runner in list(self._runners.values()):
            try:
                await runner
            except (asyncio.CancelledError, Exception):
                pass
        self._runners.clear()
        if self._db:
            await self._db.close()
            self._db = None

    async def _load(self) -> None:
        assert self._db is not None
        async with self._db.execute("SELECT payload FROM chargen_jobs") as cur:
            rows = await cur.fetchall()
        for (payload,) in rows:
            try:
                job = CharacterGenJob(**json.loads(payload))
                self._jobs[job.id] = job
            except Exception as exc:
                print(f"[Renderfin][CharGen] load skip: {exc}")

    async def _persist(self, job: CharacterGenJob) -> None:
        job.updated_at = time.time()
        if self._db is None:
            return
        await self._db.execute(
            "INSERT INTO chargen_jobs(id, payload, stage, created_at) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, stage=excluded.stage",
            (job.id, job.model_dump_json(), job.stage, job.created_at),
        )
        await self._db.commit()

    # ---------- public API ----------

    async def create(
        self,
        *,
        prompt: str,
        negative_prompt: str = "",
        mask_url: str = "",
        user_name: str = "autorig-bot",
        source_task_id: str = "",
    ) -> CharacterGenJob:
        mask_url = (mask_url or "").strip() or f"{config.PUBLIC_BASE_URL}/render/masks/t_pose.jpg"
        job = CharacterGenJob(
            prompt=prompt,
            negative_prompt=negative_prompt,
            mask_url=mask_url,
            user_name=user_name,
            source_task_id=source_task_id,
        )
        self._jobs[job.id] = job
        await self._persist(job)
        self._spawn(job)
        return job

    def get(self, job_id: str) -> Optional[CharacterGenJob]:
        return self._jobs.get(job_id)

    async def discard(self, job_id: str) -> Optional[CharacterGenJob]:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        runner = self._runners.pop(job_id, None)
        if runner:
            runner.cancel()
        job.stage = CHARGEN_STAGE_DISCARDED
        await self._persist(job)
        self._cleanup_artifacts(job)
        return job

    async def mark_submitted(self, job_id: str) -> Optional[CharacterGenJob]:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        job.stage = CHARGEN_STAGE_SUBMITTED
        await self._persist(job)
        return job

    async def approve_image(self, job_id: str):
        """Human approved the Flux render: continue to the 3D stage.

        Returns (job, transitioned) — transitioned is False when the job was
        not awaiting approval (double-press, wrong stage), so callers can skip
        spawning a duplicate watcher.
        """
        job = self._jobs.get(job_id)
        if job is None:
            return None, False
        if job.stage != CHARGEN_STAGE_AWAITING_IMAGE:
            return job, False
        job.stage = CHARGEN_STAGE_HUNYUAN
        job.error = ""
        await self._persist(job)
        self._spawn(job)
        return job, True

    async def regenerate_image(self, job_id: str):
        """Re-run the Flux stage with a fresh seed (same prompt/mask)."""
        job = self._jobs.get(job_id)
        if job is None:
            return None, False
        if job.stage not in (CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED):
            return job, False
        job.flux_task_id = ""
        job.image_url = ""
        job.isolated_url = ""
        job.error = ""
        job.stage = CHARGEN_STAGE_FLUX
        await self._persist(job)
        self._spawn(job)
        return job, True

    # ---------- pipeline ----------

    def _spawn(self, job: CharacterGenJob) -> None:
        if job.id in self._runners and not self._runners[job.id].done():
            return
        self._runners[job.id] = asyncio.create_task(self._run(job))

    async def _run(self, job: CharacterGenJob) -> None:
        try:
            if job.stage == CHARGEN_STAGE_FLUX:
                await self._stage_flux(job)
            if job.stage == CHARGEN_STAGE_AWAITING_IMAGE:
                # paused for human validation of the Flux render; approve_image
                # or regenerate_image resumes the pipeline
                return
            if job.stage == CHARGEN_STAGE_HUNYUAN:
                await self._stage_hunyuan(job)
            if job.stage == CHARGEN_STAGE_TURNTABLE:
                await self._stage_turntable(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job.error = str(exc)[:1000]
            job.stage = CHARGEN_STAGE_FAILED
            await self._persist(job)
            print(f"[Renderfin][CharGen] job {job.id} FAILED: {exc}")
        finally:
            self._runners.pop(job.id, None)

    async def _await_render(self, task_id: str, timeout: float):
        task = await self.queue.wait_for(task_id, timeout=timeout)
        if task.status != TASK_DONE:
            raise RuntimeError(f"render task {task_id} failed: {task.error or task.status}")
        return task

    async def _stage_flux(self, job: CharacterGenJob) -> None:
        if not job.flux_task_id or self.queue.get(job.flux_task_id) is None:
            task = await self.queue.enqueue(
                RenderPrompt(
                    prompt=job.prompt,
                    negative_prompt=job.negative_prompt,
                    image_url=job.mask_url,
                    type="t_pose",
                    user_name=job.user_name,
                )
            )
            job.flux_task_id = task.id
            await self._persist(job)
        task = await self._await_render(job.flux_task_id, FLUX_STAGE_TIMEOUT)
        job.image_url = task.output_url
        job.isolated_url = task.extra_outputs.get("isolated") or task.output_url
        job.stage = CHARGEN_STAGE_AWAITING_IMAGE
        await self._persist(job)
        print(f"[Renderfin][CharGen] job {job.id} flux done, awaiting approval -> {job.image_url}")

    async def _stage_hunyuan(self, job: CharacterGenJob) -> None:
        if hunyuan_client.is_configured():
            await self._stage_hunyuan_converter(job)
        else:
            await self._stage_hunyuan_comfy(job)
        job.stage = CHARGEN_STAGE_TURNTABLE
        await self._persist(job)
        print(f"[Renderfin][CharGen] job {job.id} hunyuan done -> {job.glb_url}")

    async def _stage_hunyuan_converter(self, job: CharacterGenJob) -> None:
        """Preferred path: the converter workers' Hunyuan3D 2.1 PBR API (F2/F7/F13)."""
        async with httpx.AsyncClient(follow_redirects=True) as client:
            if not job.hunyuan_task_id or not job.hunyuan_task_id.startswith("http"):
                worker, status_url = await hunyuan_client.submit(
                    client, image_url=job.isolated_url
                )
                # store the status_url so a service restart can resume polling
                job.hunyuan_task_id = status_url
                await self._persist(job)
                print(f"[Renderfin][CharGen] job {job.id} hunyuan on {worker}")
            payload = await hunyuan_client.wait_for_model(client, job.hunyuan_task_id)
            model_url = str((payload.get("output_urls") or {}).get("model"))
            data = await hunyuan_client.download_model(client, model_url)
        user_dir = config.RENDER_DIR / job.user_name
        user_dir.mkdir(parents=True, exist_ok=True)
        glb_path = user_dir / f"{job.id}.glb"
        glb_path.write_bytes(data)
        job.glb_url = f"{config.PUBLIC_BASE_URL}/render/{job.user_name}/{job.id}.glb"

    async def _stage_hunyuan_comfy(self, job: CharacterGenJob) -> None:
        """Fallback: ComfyUI image_to_3d workflow via the render queue."""
        if not job.hunyuan_task_id or self.queue.get(job.hunyuan_task_id) is None:
            task = await self.queue.enqueue(
                RenderPrompt(
                    image_url=job.isolated_url,
                    type="image_to_3d",
                    user_name=job.user_name,
                )
            )
            job.hunyuan_task_id = task.id
            await self._persist(job)
        task = await self._await_render(job.hunyuan_task_id, HUNYUAN_STAGE_TIMEOUT)
        job.glb_url = task.output_url

    async def _stage_turntable(self, job: CharacterGenJob) -> None:
        glb_task = self.queue.get(job.hunyuan_task_id)
        glb_path: Optional[Path] = None
        if glb_task and glb_task.output_path:
            glb_path = Path(glb_task.output_path)
        if glb_path is None or not glb_path.is_file():
            # fall back to the public-url disk mapping
            prefix = f"{config.PUBLIC_BASE_URL}/render/"
            if job.glb_url.startswith(prefix):
                glb_path = config.RENDER_DIR / job.glb_url[len(prefix):]
        if glb_path is None or not glb_path.is_file():
            raise RuntimeError("generated glb not found on disk")

        out_path = config.RENDER_DIR / job.user_name / f"{job.id}_turntable.mp4"
        await turntable.render_turntable(glb_path, out_path)
        job.video_url = (
            f"{config.PUBLIC_BASE_URL}/render/{job.user_name}/{job.id}_turntable.mp4"
        )
        job.stage = CHARGEN_STAGE_READY
        await self._persist(job)
        print(f"[Renderfin][CharGen] job {job.id} READY -> {job.video_url}")

    def _cleanup_artifacts(self, job: CharacterGenJob) -> None:
        prefix = f"{config.PUBLIC_BASE_URL}/render/"
        for url in (job.image_url, job.isolated_url, job.glb_url, job.video_url):
            if not url or not url.startswith(prefix):
                continue
            path = config.RENDER_DIR / url[len(prefix):]
            try:
                if path.is_file():
                    path.unlink()
                sibling = path.with_suffix(".jpg")
                if path.suffix == ".png" and sibling.is_file():
                    sibling.unlink()
            except Exception as exc:
                print(f"[Renderfin][CharGen] cleanup {path}: {exc}")
