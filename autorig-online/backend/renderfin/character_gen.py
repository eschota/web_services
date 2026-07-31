"""Composite character-generation pipeline orchestrator.

Stages: flux_render (t_pose image + isolated alpha) -> hunyuan (image_to_3d GLB)
-> turntable (6s orbit mp4) -> ready. Stage state is persisted to sqlite so the
service can resume interrupted jobs after a restart.
"""
from __future__ import annotations

import asyncio
import json
import os
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
    TASK_ERROR,
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

# Stage waits include time spent queued behind other renders on a shared
# worker, so they are deliberately generous and env-tunable.
FLUX_STAGE_TIMEOUT = float(os.getenv("RENDERFIN_CHARGEN_FLUX_TIMEOUT", "3600"))
HUNYUAN_STAGE_TIMEOUT = float(os.getenv("RENDERFIN_CHARGEN_HUNYUAN_TIMEOUT", "16200"))

# Automatic stage recovery: how many times a stage retries itself before the
# job is reported as failed, and how long to wait between attempts.
MAX_STAGE_ATTEMPTS = int(os.getenv("RENDERFIN_CHARGEN_STAGE_ATTEMPTS", "3"))
RETRY_BACKOFF_SECONDS = (30.0, 120.0, 600.0)
RETRY_TICK_SECONDS = float(os.getenv("RENDERFIN_CHARGEN_RETRY_TICK", "15"))


class CharacterGenManager:
    def __init__(self, queue: RenderQueue, *, db_path: Optional[Path] = None):
        self.queue = queue
        self.db_path = Path(db_path or config.DB_PATH)
        self._db: Optional[aiosqlite.Connection] = None
        self._jobs: Dict[str, CharacterGenJob] = {}
        self._runners: Dict[str, asyncio.Task] = {}
        self._retry_task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=10000")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        await self._load()
        for job in self._jobs.values():
            if job.stage in _ACTIVE_STAGES and not job.retry_at:
                print(f"[Renderfin][CharGen] resuming job {job.id} at stage {job.stage}")
                self._spawn(job)
        self._retry_task = asyncio.create_task(self._retry_loop())

    async def _retry_loop(self) -> None:
        """Re-spawn stages whose retry delay has elapsed (survives restarts)."""
        while not self._stopped.is_set():
            try:
                now = time.time()
                for job in list(self._jobs.values()):
                    if job.stage not in _ACTIVE_STAGES or not job.retry_at:
                        continue
                    if job.retry_at > now:
                        continue
                    job.retry_at = 0
                    await self._persist(job)
                    print(
                        f"[Renderfin][CharGen] retrying job {job.id} at stage {job.stage}"
                    )
                    self._spawn(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[Renderfin][CharGen] retry loop error: {exc}")
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=RETRY_TICK_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        self._stopped.set()
        if self._retry_task:
            self._retry_task.cancel()
            try:
                await self._retry_task
            except (asyncio.CancelledError, Exception):
                pass
            self._retry_task = None
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
        telegram_chat_id: int = 0,
    ) -> CharacterGenJob:
        mask_url = (mask_url or "").strip() or f"{config.PUBLIC_BASE_URL}/render/masks/t_pose.jpg"
        job = CharacterGenJob(
            prompt=prompt,
            negative_prompt=negative_prompt,
            mask_url=mask_url,
            user_name=user_name,
            source_task_id=source_task_id,
            telegram_chat_id=int(telegram_chat_id or 0),
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
        # stop the GPU work too, otherwise the render keeps running and would
        # write back into files we are about to delete
        for task_id in (job.flux_task_id, job.hunyuan_task_id):
            if task_id and not task_id.startswith("http"):
                await self.queue.cancel(task_id, reason="job discarded")
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

    async def set_telegram_context(
        self,
        job_id: str,
        *,
        chat_id: int = 0,
        message_id: int = 0,
        status_message_id: int = 0,
    ):
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if chat_id:
            job.telegram_chat_id = int(chat_id)
        if message_id:
            job.telegram_message_id = int(message_id)
        if status_message_id:
            job.telegram_status_message_id = int(status_message_id)
        await self._persist(job)
        return job

    async def mark_delivered(
        self,
        job_id: str,
        kind: str,
        marker: str,
        *,
        message_id: int = 0,
        clear_status_message: bool = False,
    ):
        """Record that a result was handed to Telegram (idempotency marker)."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        delivered = dict(job.delivered or {})
        delivered[kind] = marker or ""
        job.delivered = delivered
        if message_id:
            job.telegram_message_id = int(message_id)
        if clear_status_message:
            job.telegram_status_message_id = 0
        await self._persist(job)
        return job

    def all_jobs(self) -> list:
        return list(self._jobs.values())

    def active_jobs(self) -> list:
        """Jobs a client may still be waiting on (used to re-attach watchers)."""
        watchable = set(_ACTIVE_STAGES) | {CHARGEN_STAGE_AWAITING_IMAGE}
        return [j for j in self._jobs.values() if j.stage in watchable]

    async def resume(self, job_id: str):
        """Retry a failed job from its furthest completed stage, reusing any
        still-alive render task (unlike regenerate, no new render is enqueued)."""
        job = self._jobs.get(job_id)
        if job is None:
            return None, False
        if job.stage != CHARGEN_STAGE_FAILED:
            return job, False
        if job.video_url:
            job.stage = CHARGEN_STAGE_READY
            await self._persist(job)
            return job, True
        if job.glb_url:
            job.stage = CHARGEN_STAGE_TURNTABLE
        elif job.isolated_url:
            job.stage = CHARGEN_STAGE_HUNYUAN
            # the previous 3D attempt is the reason we are here: never re-poll it
            job.hunyuan_task_id = ""
            job.hunyuan_worker = ""
        else:
            job.stage = CHARGEN_STAGE_FLUX
            # keep following the previous render only if it is still alive
            previous = self.queue.get(job.flux_task_id) if job.flux_task_id else None
            if previous is None or previous.status == TASK_ERROR:
                job.flux_task_id = ""
        job.error = ""
        job.retry_at = 0
        job.attempts = {}
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
        # a regenerated image invalidates everything downstream
        job.hunyuan_task_id = ""
        job.hunyuan_worker = ""
        job.glb_url = ""
        job.video_url = ""
        job.error = ""
        job.warning = ""
        job.retry_at = 0
        job.attempts = {}
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
            await self._handle_stage_error(job, exc)
        finally:
            self._runners.pop(job.id, None)

    async def _handle_stage_error(self, job: CharacterGenJob, exc: Exception) -> None:
        """Retry the failed stage automatically before bothering the user.

        Most failures here are infrastructure (a busy/rebooting farm box, a
        chrome or ffmpeg hiccup, a network blip), and the user asked for the
        result regardless — so the pipeline heals itself and only reports a
        failure once the retries are exhausted.
        """
        stage = job.stage
        attempts = dict(job.attempts or {})
        attempts[stage] = attempts.get(stage, 0) + 1
        job.attempts = attempts
        job.last_error = str(exc)[:1000]
        count = attempts[stage]

        if count >= MAX_STAGE_ATTEMPTS:
            job.error = job.last_error
            job.stage = CHARGEN_STAGE_FAILED
            job.retry_at = 0
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id} FAILED after {count} "
                f"attempts at {stage}: {exc}"
            )
            return

        # drop references to the dead attempt so the retry starts clean
        if stage == CHARGEN_STAGE_HUNYUAN:
            job.hunyuan_task_id = ""
            job.hunyuan_worker = ""
        elif stage == CHARGEN_STAGE_FLUX:
            previous = self.queue.get(job.flux_task_id) if job.flux_task_id else None
            if previous is None or previous.status == TASK_ERROR:
                job.flux_task_id = ""

        delay = RETRY_BACKOFF_SECONDS[min(count - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
        job.retry_at = time.time() + delay
        job.error = ""  # not a terminal failure: nothing to deliver yet
        await self._persist(job)
        print(
            f"[Renderfin][CharGen] job {job.id} stage {stage} attempt {count} "
            f"failed ({exc}); retrying in {int(delay)}s"
        )

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
        isolated = task.extra_outputs.get("isolated")
        if not isolated:
            # Hunyuan works far better on a matted character; say so instead of
            # silently feeding it the full frame.
            job.warning = "RMBG isolated render missing; using the full frame for 3D"
            print(f"[Renderfin][CharGen] job {job.id} WARNING: {job.warning}")
        job.isolated_url = isolated or task.output_url
        job.stage = CHARGEN_STAGE_AWAITING_IMAGE
        await self._persist(job)
        print(f"[Renderfin][CharGen] job {job.id} flux done, awaiting approval -> {job.image_url}")

    async def _stage_hunyuan(self, job: CharacterGenJob) -> None:
        pool = hunyuan_client.workers()
        if pool:
            print(
                f"[Renderfin][CharGen] job {job.id} 3D via converter API: "
                + ", ".join(w["name"] for w in pool)
            )
            await self._stage_hunyuan_converter(job)
        else:
            print(
                f"[Renderfin][CharGen] job {job.id} 3D via ComfyUI fallback "
                "(no Hunyuan workers configured)"
            )
            await self._stage_hunyuan_comfy(job)
        job.stage = CHARGEN_STAGE_TURNTABLE
        await self._persist(job)
        print(f"[Renderfin][CharGen] job {job.id} hunyuan done -> {job.glb_url}")

    async def _stage_hunyuan_converter(self, job: CharacterGenJob) -> None:
        """Preferred path: the converter workers' Hunyuan3D 2.1 PBR API (per-box token)."""
        async with httpx.AsyncClient(follow_redirects=True) as client:
            worker = None
            if job.hunyuan_task_id.startswith("http"):
                # resuming: the owning worker is derivable from the stored status url
                worker = hunyuan_client.worker_for_url(job.hunyuan_task_id)
                if worker is None:
                    print(
                        f"[Renderfin][CharGen] job {job.id}: worker for "
                        f"{job.hunyuan_task_id} is gone, resubmitting"
                    )
                    job.hunyuan_task_id = ""
            if worker is None:
                worker, status_url = await hunyuan_client.submit(
                    client, image_url=job.isolated_url
                )
                # store the status_url so a service restart can resume polling
                job.hunyuan_task_id = status_url
                job.hunyuan_worker = worker["name"]
                await self._persist(job)
                print(f"[Renderfin][CharGen] job {job.id} hunyuan on {worker['name']}")
            payload = await hunyuan_client.wait_for_model(
                client, worker, job.hunyuan_task_id
            )
            model_url = str((payload.get("output_urls") or {}).get("model"))
            data = await hunyuan_client.download_model(client, worker, model_url)
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
