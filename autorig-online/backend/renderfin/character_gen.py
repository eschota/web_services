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
from typing import Any, Dict, List, Optional

import aiosqlite
import httpx

from fleet_admission import fleet_admission_lock

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
    SentMessage,
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

# A stage waits on a queue task, so its ceiling MUST exceed the queue's own
# ceiling - otherwise the stage gives up while the task keeps holding a worker,
# and the job fails with a timeout the queue would still have honoured.
_STAGE_SLACK_SECONDS = 600.0
FLUX_STAGE_TIMEOUT = float(
    os.getenv("RENDERFIN_CHARGEN_FLUX_TIMEOUT", "")
    or config.TASK_TIMEOUT_SECONDS + _STAGE_SLACK_SECONDS
)
HUNYUAN_STAGE_TIMEOUT = float(
    os.getenv("RENDERFIN_CHARGEN_HUNYUAN_TIMEOUT", "")
    or config.HUNYUAN_TIMEOUT_SECONDS + _STAGE_SLACK_SECONDS
)

# Automatic stage recovery: how many times a stage retries itself before the
# job is reported as failed, and how long to wait between attempts.
MAX_STAGE_ATTEMPTS = int(os.getenv("RENDERFIN_CHARGEN_STAGE_ATTEMPTS", "3"))
RETRY_BACKOFF_SECONDS = (30.0, 120.0, 600.0)
# An empty 3D fleet says nothing about the job, so waiting is free and giving
# up is wrong: the user pressed the button and is owed the result whenever the
# farm comes back. These waits do not count against the job's attempts.
FLEET_WAIT_SECONDS = float(os.getenv("RENDERFIN_CHARGEN_FLEET_WAIT", "300"))
# Waiting for a busy box to free up is ordinary queueing, so it is re-checked
# far more often than a farm that is actually down.
SLOT_WAIT_SECONDS = float(os.getenv("RENDERFIN_CHARGEN_SLOT_WAIT", "60"))
# Admission-control responses describe a healthy queue, not a dead fleet. The
# wording comes from the central FIFO, shared-converter reserve and worker GPU
# gate, so matching only "at capacity" can leave idle dedicated cards waiting
# for the five-minute outage poll.
_HUNYUAN_CAPACITY_WAIT_MARKERS = (
    "at capacity",
    "no capacity",
    "higher-priority hunyuan job",
    "shared hunyuan fallback paused",
    "gpu_busy_comfy",
    "gpu_leased",
    "worker_capacity",
)
# Some failures arrive only AFTER a full 3D generation has been paid for, so
# re-checking every five minutes would burn a GPU-hour to rediscover the same
# broken post-processor. They still must not fail the job.
FARM_BREAKAGE_WAIT_SECONDS = float(
    os.getenv("RENDERFIN_CHARGEN_FARM_BREAKAGE_WAIT", "1800")
)
INPUT_FETCH_WORKER_COOLDOWN_SECONDS = float(
    os.getenv("RENDERFIN_HUNYUAN_INPUT_FETCH_COOLDOWN", "3600")
)
FARM_WORKER_COOLDOWN_SECONDS = float(
    os.getenv("RENDERFIN_HUNYUAN_FARM_WORKER_COOLDOWN", "1800")
)
VRAM_WORKER_COOLDOWN_SECONDS = float(
    os.getenv("RENDERFIN_HUNYUAN_VRAM_WORKER_COOLDOWN", "300")
)
# Known farm-side pipeline breakages: the generation itself succeeded and the
# box then failed to finish its own post-processing. Nothing about the job is
# wrong, so retrying the job harder cannot help and giving up is not allowed.
_FARM_BREAKAGE_MARKERS = (
    # The box gave up on its own two-hour task ceiling. That is a property of
    # how long the farm currently takes per generation, not of the job, so
    # spending the job's attempts on it would kill work that is fine the
    # moment the farm gets faster.
    "timed out after",
    "generation timed out",
    # The box was busy, not broken: its own gate refused to start a
    # generation that would not fit alongside what it is already running.
    # Waiting for the card to free up is the whole remedy.
    "vram gate failed",
    "vertex-pbr manifest is missing",
    "vertex-pbr manifest contract",
    "blender vertex-pbr pipeline failed",
)


def _is_farm_breakage(text: str) -> bool:
    low = (text or "").lower()
    return any(marker in low for marker in _FARM_BREAKAGE_MARKERS)


def _is_hunyuan_capacity_wait(text: str) -> bool:
    """Return true for healthy admission/slot waits that merit a fast poll."""
    low = (text or "").lower()
    return any(marker in low for marker in _HUNYUAN_CAPACITY_WAIT_MARKERS)


def _is_collection_infrastructure_failure(
    job: CharacterGenJob, text: str
) -> bool:
    """Recognise transient farm faults only for automatic collection work.

    A manual generation must still surface a repeatable bad input or workflow
    after its normal retry budget. Collection members, however, are explicitly
    background work and must survive renderer disk pressure, leased GPUs,
    worker restarts and transient 5xx responses without terminating the whole
    collection.
    """
    if not job.collection_guid or job.queue_class != "collection_background":
        return False
    low = (text or "").lower()
    if "upload/image failed:" in low and any(
        code in low for code in ("http 500", "http 502", "http 503", "http 504")
    ):
        return True
    # Artifact transfer is downstream of a successfully completed render. An
    # empty exception string is common for a disconnected HTTP stream, so the
    # prefix itself is enough evidence that the collection member should be
    # retried instead of terminated.
    if "artifact download failed:" in low:
        return True
    if "generate-3d on " in low and any(
        code in low for code in ("http 500", "http 502", "http 503", "http 504")
    ):
        return True
    # A worker can accept the task and then fail while fetching the owned
    # autorig.online input URL. Requests reports that transport failure inside
    # the worker's terminal error instead of as an HTTP status. It is a box or
    # route fault, not a verdict on the model, so a background collection must
    # rotate/retry instead of becoming terminal after three identical misses.
    if "generation failed on " in low and any(
        marker in low
        for marker in (
            "connectionpool(",
            "read timed out",
            "connect timeout",
            "connection reset",
            "remote end closed connection",
        )
    ):
        return True
    return any(
        marker in low
        for marker in (
            "hunyuan worker exited with code",
            "render timeout",
            "render task ",
        )
    ) and ("timeout" in low or "timed out" in low or "hunyuan worker exited" in low)


# Jobs that were failed by an empty fleet before it was treated as a wait.
# They are indistinguishable from a real failure only by their message, so it
# is matched here and they are revived rather than left for a human.
_FLEET_ERROR_MARKERS = (
    "timed out after",
    "generation timed out",
    "vram gate failed",
    "task vanished on",
    "vertex-pbr manifest is missing",
    "blender vertex-pbr pipeline failed",
    "lost the route to",
    "no enabled hunyuan worker",
    "no hunyuan workers configured",
    "rejected our token",
    # jobs failed by a stale token before it was treated as a wait
    '"error":"unauthorized"',
    "http 401",
    "http 403",
    "image_url host cannot be resolved",
    "getaddrinfo failed",
)


def _failed_on_empty_fleet(job: CharacterGenJob) -> bool:
    """Only the terminal reason counts.

    last_error is deliberately not consulted: it survives a revival, so a job
    that later fails for a real reason would keep matching and be revived
    forever.
    """
    return any(marker in (job.error or "").lower() for marker in _FLEET_ERROR_MARKERS)


def _failed_on_recoverable_infrastructure(job: CharacterGenJob) -> bool:
    return _failed_on_empty_fleet(job) or _is_collection_infrastructure_failure(
        job, job.error
    )


RETRY_TICK_SECONDS = float(os.getenv("RENDERFIN_CHARGEN_RETRY_TICK", "15"))


class CharacterGenManager:
    def __init__(self, queue: RenderQueue, *, db_path: Optional[Path] = None):
        self.queue = queue
        self.db_path = Path(db_path or config.DB_PATH)
        self._db: Optional[aiosqlite.Connection] = None
        self._jobs: Dict[str, CharacterGenJob] = {}
        self._runners: Dict[str, asyncio.Task] = {}
        # Serialises submission so a slot count cannot be read stale.
        self._submit_lock = asyncio.Lock()
        # Worker-side DNS is a box property, not a model property. Persisted
        # per-job cooldowns protect retries after a restart; this shared map
        # prevents every member of a newly queued collection from first
        # rediscovering the same broken resolver.
        self._input_fetch_worker_cooldowns: Dict[str, float] = {}
        # Farm faults are worker properties, not model properties. Without a
        # shared cooldown every waiting collection member immediately probes
        # the same just-failed box and turns one crash/VRAM conflict into a
        # fleet-wide failure burst.
        self._farm_worker_cooldowns: Dict[str, float] = {}
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
        await self._backfill_seq()
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
                    if (
                        job.stage == CHARGEN_STAGE_FAILED
                        and _failed_on_recoverable_infrastructure(job)
                    ):
                        # the farm, not the job, was broken: put it back in the
                        # pipeline. If the fleet is still empty it parks again,
                        # so this costs one check per FLEET_WAIT_SECONDS.
                        print(
                            f"[Renderfin][CharGen] reviving job {job.id}: it was "
                            f"failed by recoverable farm infrastructure"
                        )
                        await self.resume(job.id)
                        continue
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
        priority_backfill: List[CharacterGenJob] = []
        async with self._db.execute("SELECT payload FROM chargen_jobs") as cur:
            rows = await cur.fetchall()
        for (payload,) in rows:
            try:
                raw = json.loads(payload)
                legacy_collection = bool(
                    isinstance(raw, dict)
                    and raw.get("collection_guid")
                    and "queue_class" not in raw
                )
                if legacy_collection:
                    raw["queue_class"] = "collection_background"
                job = CharacterGenJob(**raw)
                self._jobs[job.id] = job
                if legacy_collection:
                    priority_backfill.append(job)
            except Exception as exc:
                print(f"[Renderfin][CharGen] load skip: {exc}")
        if priority_backfill:
            await self._persist_many(priority_backfill)
            print(
                f"[Renderfin][CharGen] priority backfill: {len(priority_backfill)} "
                "legacy collection job(s) marked collection_background"
            )

    async def _backfill_seq(self) -> None:
        """Number the jobs that predate the running counter, oldest first."""
        unnumbered = sorted(
            (j for j in self._jobs.values() if not j.seq), key=lambda j: j.created_at
        )
        if not unnumbered:
            return
        nxt = max((int(j.seq or 0) for j in self._jobs.values()), default=0) + 1
        for job in unnumbered:
            job.seq = nxt
            nxt += 1
            await self._persist(job)
        print(f"[Renderfin][CharGen] numbered {len(unnumbered)} existing job(s)")

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

    async def _persist_many(self, jobs: List[CharacterGenJob]) -> None:
        """Write a collection in one sqlite transaction."""
        now = time.time()
        for job in jobs:
            job.updated_at = now
        if self._db is None:
            return
        await self._db.executemany(
            "INSERT INTO chargen_jobs(id, payload, stage, created_at) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, stage=excluded.stage",
            [
                (job.id, job.model_dump_json(), job.stage, job.created_at)
                for job in jobs
            ],
        )
        await self._db.commit()

    # ---------- public API ----------

    async def create(
        self,
        *,
        prompt: str,
        prompt_b: str = "",
        negative_prompt: str = "",
        mask_url: str = "",
        mask_url_b: str = "",
        user_name: str = "autorig-bot",
        source_task_id: str = "",
        telegram_chat_id: int = 0,
    ) -> CharacterGenJob:
        mask_url = (mask_url or "").strip() or f"{config.PUBLIC_BASE_URL}/render/masks/t_pose.jpg"
        job = CharacterGenJob(
            seq=self._next_seq(),
            prompt=prompt,
            prompt_b=prompt_b,
            negative_prompt=negative_prompt,
            mask_url=mask_url,
            mask_url_b=(mask_url_b or "").strip(),
            user_name=user_name,
            source_task_id=source_task_id,
            telegram_chat_id=int(telegram_chat_id or 0),
        )
        self._jobs[job.id] = job
        await self._persist(job)
        self._spawn(job)
        return job

    async def create_collection(
        self,
        *,
        collection_guid: str,
        collection_title: str,
        collection_description: str,
        collection_tags: List[str],
        members: List[Dict[str, Any]],
        user_name: str = "autorig-bot",
        source_task_id: str = "",
        telegram_chat_id: int = 0,
        telegram_status_message_id: int = 0,
    ) -> List[CharacterGenJob]:
        """Persist and start a pre-validated collection as 15 ordinary jobs.

        The ordinary queue, retry and worker-slot accounting remains the sole
        execution path. A collection is grouping metadata, never a bypass
        around the capacity controls that protect normal conversion work.
        """
        size = len(members)
        jobs: List[CharacterGenJob] = []
        for position, member in enumerate(members, start=1):
            mask_url = str(member.get("mask_url") or "").strip() or (
                f"{config.PUBLIC_BASE_URL}/render/masks/t_pose.jpg"
            )
            job = CharacterGenJob(
                seq=self._next_seq(),
                prompt=str(member.get("prompt") or ""),
                prompt_b=str(member.get("prompt_b") or ""),
                negative_prompt=str(member.get("negative_prompt") or ""),
                mask_url=mask_url,
                mask_url_b=str(member.get("mask_url_b") or "").strip(),
                user_name=user_name,
                source_task_id=source_task_id,
                queue_class="collection_background",
                telegram_chat_id=int(telegram_chat_id or 0),
                telegram_status_message_id=int(telegram_status_message_id or 0),
                collection_guid=collection_guid,
                collection_title=collection_title,
                collection_description=collection_description,
                collection_tags=list(collection_tags or []),
                collection_index=int(member.get("index") or position),
                collection_size=size,
                collection_member_title=str(member.get("title") or ""),
            )
            self._jobs[job.id] = job
            jobs.append(job)

        try:
            await self._persist_many(jobs)
        except Exception:
            if self._db is not None:
                await self._db.rollback()
            for job in jobs:
                self._jobs.pop(job.id, None)
            raise
        for job in jobs:
            self._spawn(job)
        print(
            f"[Renderfin][CharGen] collection {collection_guid} started: "
            f"{len(jobs)} jobs"
        )
        return jobs

    async def create_from_image(
        self,
        *,
        image_url: str,
        user_name: str = "autorig-bot",
        source_task_id: str = "",
    ) -> CharacterGenJob:
        """Start at the 3D stage from a picture the user already has.

        The Flux stages exist to invent a T-pose render, and the approval stage
        exists to choose between two invented ones. When the picture is supplied
        there is nothing to invent and nothing to choose, so the job enters at
        ``hunyuan`` with that picture standing in for the alpha-isolated render.
        Everything downstream is the same code the prompt-driven jobs run: the
        same worker pool and slot accounting, the same stage retries, the same
        turntable - which is what keeps site generations and bot generations in
        one queue instead of two that can each overfill the farm.
        """
        job = CharacterGenJob(
            seq=self._next_seq(),
            user_name=user_name,
            source_task_id=source_task_id,
            stage=CHARGEN_STAGE_HUNYUAN,
            image_url=image_url,
            isolated_url=image_url,
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

    async def mark_submitted(
        self, job_id: str, task_id: str = ""
    ) -> Optional[CharacterGenJob]:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        job.stage = CHARGEN_STAGE_SUBMITTED
        if task_id:
            job.submitted_task_id = str(task_id)
        await self._persist(job)
        return job

    def job_for_task(self, task_id: str) -> Optional[CharacterGenJob]:
        """The job that produced this conversion task, if any."""
        task_id = str(task_id or "")
        if not task_id:
            return None
        for job in self._jobs.values():
            if job.submitted_task_id == task_id:
                return job
        return None

    async def approve_image(self, job_id: str, variant: str = "a"):
        """Human picked a variant: continue to the 3D stage with that image.

        Returns (job, transitioned) — transitioned is False when the job was
        not awaiting approval (double-press, wrong stage), so callers can skip
        spawning a duplicate watcher.
        """
        job = self._jobs.get(job_id)
        if job is None:
            return None, False
        if job.stage != CHARGEN_STAGE_AWAITING_IMAGE:
            return job, False
        variant = (variant or "a").strip().lower()
        if variant == "b" and job.isolated_url_b:
            job.chosen_variant = "b"
            job.image_url = job.image_url_b
            job.isolated_url = job.isolated_url_b
        else:
            job.chosen_variant = "a"
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

    async def record_messages(self, job_id: str, message_ids, at: float = 0.0, kind: str = ""):
        """Remember messages this job put in the chat, so it can remove them."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        stamp = at or time.time()
        known = {m.id for m in job.telegram_messages}
        job.telegram_messages = list(job.telegram_messages) + [
            SentMessage(id=int(mid), at=stamp, kind=kind)
            for mid in message_ids
            if int(mid or 0) and int(mid) not in known
        ]
        await self._persist(job)
        return job

    async def set_status_message(self, job_id: str, message_id: int):
        """Remember the job's progress line so later stages can edit it."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        job.telegram_status_message_id = int(message_id or 0)
        await self._persist(job)
        return job

    async def set_telegram_messages(self, job_id: str, messages, *, undeletable=None):
        """Replace the tracked message list (used after a cleanup sweep)."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        job.telegram_messages = list(messages)
        if undeletable is not None:
            job.telegram_undeletable = list(undeletable)
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

    async def kick_parked(self) -> int:
        """Stop waiting: re-run every parked stage right now.

        Parked jobs are waiting out a condition on the farm — an empty pool, a
        rejected token, a broken post-processor — with a delay chosen for a
        farm that is down. Once it is fixed there is nothing left to wait for,
        and sitting out the rest of a half-hour backoff is pure delay.
        """
        kicked = 0
        for job in list(self._jobs.values()):
            if job.stage not in _ACTIVE_STAGES or not job.retry_at:
                continue
            job.retry_at = 0
            await self._persist(job)
            self._spawn(job)
            kicked += 1
        if kicked:
            print(f"[Renderfin][CharGen] kicked {kicked} parked job(s)")
        return kicked

    async def refund_attempts(self) -> int:
        """Clear the attempt debt of jobs that are still alive.

        revive_failed forgives a farm outage only once a job has already died
        of it. A job that survived carries the same debt and nothing ever pays
        it back: two of the three attempts it is allowed can be gone before
        anything was ever wrong with the job itself, so the next unrelated
        hiccup kills it. Refund on the same grounds, while it still helps.
        """
        refunded = 0
        for job in list(self._jobs.values()):
            if job.attempts_refunded or job.stage not in _ACTIVE_STAGES:
                continue
            if not (job.attempts or {}).get(job.stage):
                continue
            attempts = dict(job.attempts)
            attempts.pop(job.stage, None)
            job.attempts = attempts
            job.attempts_refunded = True
            await self._persist(job)
            refunded += 1
        if refunded:
            print(f"[Renderfin][CharGen] refunded attempts on {refunded} job(s)")
        return refunded

    async def revive_failed(self) -> int:
        """Put failed jobs back in the pipeline with a fresh attempt budget.

        Their attempts were spent on a farm that was broken for every one of
        them, so charging them for it would leave work permanently dead that
        nothing was ever wrong with.
        """
        revived = 0
        for job in list(self._jobs.values()):
            if job.stage != CHARGEN_STAGE_FAILED:
                continue
            _, ok = await self.resume(job.id)
            revived += 1 if ok else 0
        if revived:
            print(f"[Renderfin][CharGen] revived {revived} failed job(s)")
        return revived

    def in_flight_by_worker(self) -> Dict[str, int]:
        """How many generations each box is holding for us right now.

        Read from persisted state. That is only safe because submission holds
        _submit_lock until the job is written down; without it thirty jobs
        woken at the same moment all see the same idle worker.
        """
        counts: Dict[str, int] = {}
        for job in self._jobs.values():
            if (
                job.stage == CHARGEN_STAGE_HUNYUAN
                and job.hunyuan_task_id
                and job.hunyuan_worker
            ):
                counts[job.hunyuan_worker] = counts.get(job.hunyuan_worker, 0) + 1
        return counts

    def _hunyuan_admission_candidates(
        self, current: CharacterGenJob, *, now: Optional[float] = None
    ) -> List[CharacterGenJob]:
        """Eligible central waiters ordered as interactive FIFO then background FIFO."""
        current_time = float(now if now is not None else time.time())
        candidates: List[CharacterGenJob] = []
        for candidate in self._jobs.values():
            if candidate.stage != CHARGEN_STAGE_HUNYUAN or candidate.hunyuan_task_id:
                continue
            if float(candidate.dispatch_not_before or 0) > current_time:
                continue
            eligible_now = not candidate.retry_at or float(candidate.retry_at) <= current_time
            if not (
                candidate.id == current.id
                or eligible_now
                or bool(candidate.hunyuan_waiting_for_capacity)
            ):
                continue
            candidates.append(candidate)
        return sorted(
            candidates,
            key=lambda candidate: (
                1
                if str(candidate.queue_class or "").strip().lower()
                == "collection_background"
                else 0,
                int(candidate.seq or 0),
                float(candidate.created_at or 0),
                candidate.id,
            ),
        )

    async def _require_hunyuan_admission(self, job: CharacterGenJob) -> None:
        """Admit only the head job while the shared submission lock is held."""
        candidates = self._hunyuan_admission_candidates(job)
        if not candidates or candidates[0].id == job.id:
            return
        head = candidates[0]
        if head.hunyuan_waiting_for_capacity and float(head.dispatch_not_before or 0) <= time.time():
            # A free slot may have appeared before its bounded retry tick. Wake
            # it now so background work cannot steal the newly available GPU.
            # The original durable retry remains valid if the process exits
            # before this opportunistic in-memory wake finishes.
            head.retry_at = 0
            self._spawn(head)
        raise hunyuan_client.NoWorkerAvailable(
            f"higher-priority Hunyuan job {head.id} is ahead of {job.id}"
        )

    def _next_seq(self) -> int:
        """Running number over every job ever created (gaps are fine)."""
        return max((int(j.seq or 0) for j in self._jobs.values()), default=0) + 1

    def stats(self) -> Dict[str, int]:
        """Throughput of the last 24h against the 24h before it."""
        now = time.time()
        day = 24 * 3600
        current = sum(1 for j in self._jobs.values() if j.created_at >= now - day)
        previous = sum(
            1 for j in self._jobs.values()
            if now - 2 * day <= j.created_at < now - day
        )
        done = sum(1 for j in self._jobs.values() if j.stage == CHARGEN_STAGE_SUBMITTED)
        return {
            "total": len(self._jobs),
            "current_24h": current,
            "previous_24h": previous,
            "delta_24h": current - previous,
            "done": done,
            "done_24h": sum(
                1 for j in self._jobs.values()
                if j.stage == CHARGEN_STAGE_SUBMITTED and j.updated_at >= now - day
            ),
            "failed": sum(1 for j in self._jobs.values() if j.stage == CHARGEN_STAGE_FAILED),
        }

    def all_jobs(self) -> list:
        return list(self._jobs.values())

    def active_jobs(self) -> list:
        """Jobs a client may still be waiting on (used to re-attach watchers)."""
        # READY is included on purpose: the bot auto-submits those, so a job
        # sitting at ready is unfinished work someone has to pick up.
        watchable = set(_ACTIVE_STAGES) | {
            CHARGEN_STAGE_AWAITING_IMAGE,
            CHARGEN_STAGE_READY,
        }
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
        job.last_error = ""
        job.retry_at = 0
        job.attempts = {}
        # an explicit resume is a fresh attempt, so it earns a fresh window
        job.stage_started_at = 0
        job.timed_stage = ""
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
        job.flux_task_id_b = ""
        job.image_url = ""
        job.isolated_url = ""
        job.image_url_b = ""
        job.isolated_url_b = ""
        job.chosen_variant = ""
        # a regenerated image invalidates everything downstream
        job.hunyuan_task_id = ""
        job.hunyuan_worker = ""
        job.glb_url = ""
        job.video_url = ""
        job.error = ""
        job.warning = ""
        job.retry_at = 0
        job.attempts = {}
        job.stage_started_at = 0
        job.timed_stage = ""
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
        job.last_error = str(exc)[:1000]
        job.hunyuan_waiting_for_capacity = False

        if _is_farm_breakage(str(exc)) or _is_collection_infrastructure_failure(
            job, str(exc)
        ):
            # a card that is merely busy frees up in minutes; a broken
            # post-processor does not, and re-checking it costs a GPU hour
            wait = (
                SLOT_WAIT_SECONDS
                if "vram gate" in str(exc).lower()
                else FARM_BREAKAGE_WAIT_SECONDS
            )
            # The generation is over and it failed; the handle points at a
            # finished task. Keeping it makes the retry re-read the same
            # failure forever AND counts the job against that worker's slot,
            # so a box ends up "holding" dozens of jobs it is not running.
            failed_worker = job.hunyuan_worker
            if stage == CHARGEN_STAGE_HUNYUAN:
                if failed_worker:
                    worker_wait = (
                        VRAM_WORKER_COOLDOWN_SECONDS
                        if "vram gate" in str(exc).lower()
                        else FARM_WORKER_COOLDOWN_SECONDS
                    )
                    cooldown_until = time.time() + worker_wait
                    cooldowns = dict(job.hunyuan_worker_cooldowns or {})
                    cooldowns[failed_worker] = cooldown_until
                    job.hunyuan_worker_cooldowns = cooldowns
                    self._farm_worker_cooldowns[failed_worker] = cooldown_until
                job.hunyuan_task_id = ""
                job.hunyuan_worker = ""
            elif stage == CHARGEN_STAGE_FLUX:
                # Only discard terminal/missing render handles. A rare stage
                # timeout while Comfy still owns the prompt must not create a
                # duplicate or orphan process; the next pass will observe its
                # eventual terminal state.
                for attr in ("flux_task_id", "flux_task_id_b"):
                    task_id = getattr(job, attr)
                    previous = self.queue.get(task_id) if task_id else None
                    if previous is None or previous.status == TASK_ERROR:
                        setattr(job, attr, "")
            job.retry_at = time.time() + wait
            job.stage_started_at = 0
            job.timed_stage = ""
            job.error = ""
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id} parked on a farm-side "
                f"condition ({exc}); re-checking in {int(wait)}s"
            )
            return

        if isinstance(exc, hunyuan_client.WorkerInputFetchError):
            # The same owned input succeeds on other boxes. Cool down this
            # box for the job so the least-loaded picker does not select its
            # broken DNS resolver again on every retry.
            cooldowns = dict(job.hunyuan_worker_cooldowns or {})
            cooldown_until = time.time() + INPUT_FETCH_WORKER_COOLDOWN_SECONDS
            cooldowns[exc.worker_name] = cooldown_until
            job.hunyuan_worker_cooldowns = cooldowns
            self._input_fetch_worker_cooldowns[exc.worker_name] = cooldown_until
            job.hunyuan_task_id = ""
            job.hunyuan_worker = ""
            job.retry_at = time.time() + RETRY_BACKOFF_SECONDS[0]
            job.stage_started_at = 0
            job.timed_stage = ""
            job.error = ""
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id}: {exc.worker_name} cannot "
                "resolve the input host; rotating to another worker"
            )
            return

        if isinstance(exc, (
            hunyuan_client.TaskVanished,
            hunyuan_client.TaskPreempted,
            hunyuan_client.WorkerUnreachable,
        )):
            # drop the dead handle so the stage submits again, and do not spend
            # an attempt: a crashing box would otherwise exhaust every job
            job.hunyuan_task_id = ""
            job.hunyuan_worker = ""
            if isinstance(exc, hunyuan_client.TaskPreempted):
                cooldown = 300.0
                job.preemption_count = int(job.preemption_count or 0) + 1
                job.preempted_at = time.time()
                job.dispatch_not_before = job.preempted_at + cooldown
                job.retry_at = job.dispatch_not_before
            else:
                job.retry_at = time.time() + FLEET_WAIT_SECONDS
            job.stage_started_at = 0
            job.timed_stage = ""
            job.error = ""
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id} lost its 3D task ({exc}); "
                "resubmitting"
            )
            return

        if isinstance(exc, hunyuan_client.NoWorkerAvailable):
            # a full pool is a queue, not an outage: look again soon
            wait = (
                SLOT_WAIT_SECONDS
                if _is_hunyuan_capacity_wait(str(exc))
                else FLEET_WAIT_SECONDS
            )
            # Not this job's fault and not fixable by retrying harder: park it
            # in place and keep checking. Attempts are untouched, and the stage
            # clock is pushed along so waiting for the farm cannot time it out.
            job.hunyuan_waiting_for_capacity = True
            job.retry_at = time.time() + wait
            job.stage_started_at = 0
            job.timed_stage = ""
            job.error = ""
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id} waiting for a 3D worker "
                f"({exc}); re-checking in {int(wait)}s"
            )
            return

        attempts = dict(job.attempts or {})
        attempts[stage] = attempts.get(stage, 0) + 1
        job.attempts = attempts
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

        job.stage_started_at = 0
        job.timed_stage = ""
        delay = RETRY_BACKOFF_SECONDS[min(count - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
        job.retry_at = time.time() + delay
        job.error = ""  # not a terminal failure: nothing to deliver yet
        await self._persist(job)
        print(
            f"[Renderfin][CharGen] job {job.id} stage {stage} attempt {count} "
            f"failed ({exc}); retrying in {int(delay)}s"
        )

    def _stage_budget(self, job: CharacterGenJob, ceiling: float) -> float:
        """Seconds this stage has left.

        The clock starts when the stage is entered and is persisted with the
        job, so a service restart resumes the same window instead of handing a
        stuck job a fresh one. Without this a job that a farm box has silently
        dropped never reaches its retry path: every restart re-enters the stage
        and grants another full ceiling.
        """
        now = time.time()
        if job.timed_stage != job.stage or not job.stage_started_at:
            job.timed_stage = job.stage
            job.stage_started_at = now
        return max(0.0, ceiling - (now - job.stage_started_at))

    async def _persisted_stage_budget(
        self, job: CharacterGenJob, ceiling: float
    ) -> float:
        """Persist a newly-started deadline before entering a long wait."""
        previous = (job.timed_stage, job.stage_started_at)
        budget = self._stage_budget(job, ceiling)
        if (job.timed_stage, job.stage_started_at) != previous:
            await self._persist(job)
        return budget

    async def _await_render(self, task_id: str, timeout: float):
        task = await self.queue.wait_for(task_id, timeout=timeout)
        if task.status != TASK_DONE:
            raise RuntimeError(f"render task {task_id} failed: {task.error or task.status}")
        return task

    async def _enqueue_flux(self, job: CharacterGenJob, prompt: str, mask_url: str = ""):
        return await self.queue.enqueue(
            RenderPrompt(
                prompt=prompt,
                negative_prompt=job.negative_prompt,
                image_url=mask_url or job.mask_url,
                type="t_pose",
                user_name=job.user_name,
            )
        )

    async def _stage_flux(self, job: CharacterGenJob) -> None:
        """Render both style variants so the user picks the better 3D base.

        The two renders are queued together and go to different workers when
        the farm has capacity, so two variants cost roughly one render of
        wall-clock time.
        """
        if not job.flux_task_id or self.queue.get(job.flux_task_id) is None:
            task = await self._enqueue_flux(job, job.prompt)
            job.flux_task_id = task.id
            await self._persist(job)
        if job.prompt_b and (
            not job.flux_task_id_b or self.queue.get(job.flux_task_id_b) is None
        ):
            task_b = await self._enqueue_flux(job, job.prompt_b, job.mask_url_b)
            job.flux_task_id_b = task_b.id
            await self._persist(job)

        task = await self._await_render(
            job.flux_task_id,
            await self._persisted_stage_budget(job, FLUX_STAGE_TIMEOUT),
        )
        job.image_url = task.output_url
        isolated = task.extra_outputs.get("isolated")
        if not isolated:
            # Hunyuan works far better on a matted character; say so instead of
            # silently feeding it the full frame.
            job.warning = "RMBG isolated render missing; using the full frame for 3D"
            print(f"[Renderfin][CharGen] job {job.id} WARNING: {job.warning}")
        job.isolated_url = isolated or task.output_url

        if job.flux_task_id_b:
            # a failed second variant must not sink the job: one image is enough
            try:
                task_b = await self._await_render(
                    job.flux_task_id_b,
                    await self._persisted_stage_budget(job, FLUX_STAGE_TIMEOUT),
                )
                job.image_url_b = task_b.output_url
                job.isolated_url_b = (
                    task_b.extra_outputs.get("isolated") or task_b.output_url
                )
            except Exception as exc:
                print(f"[Renderfin][CharGen] job {job.id} variant B failed: {exc}")
                job.flux_task_id_b = ""

        if not job.image_url_b:
            # Only one render survived, so the approval stage has nothing to
            # ask: its whole purpose is choosing between two invented T-poses.
            # The same reasoning create_from_image already applies - nothing to
            # invent, nothing to choose, enter at hunyuan - and the cost of not
            # applying it here was real: every single-variant job sat waiting
            # for a button nobody had a reason to press, two of them for eleven
            # and eight hours, and each one also spent a card in a chat the
            # operator asked to keep to decisions and results.
            job.chosen_variant = "a"
            job.stage = CHARGEN_STAGE_HUNYUAN
            await self._persist(job)
            print(
                f"[Renderfin][CharGen] job {job.id} flux done (1 variant, "
                f"nothing to choose) -> 3D directly"
            )
            return

        job.stage = CHARGEN_STAGE_AWAITING_IMAGE
        await self._persist(job)
        print(
            f"[Renderfin][CharGen] job {job.id} flux done (2 variants), "
            f"awaiting approval -> {job.image_url}"
        )

    async def _stage_hunyuan(self, job: CharacterGenJob) -> None:
        pool = hunyuan_client.workers()
        if pool:
            print(
                f"[Renderfin][CharGen] job {job.id} 3D via converter API: "
                + ", ".join(w["name"] for w in pool)
            )
            await self._stage_hunyuan_converter(job)
        elif config.hunyuan_workers_last_error():
            raise hunyuan_client.NoWorkerAvailable(
                "Hunyuan worker configuration is temporarily unavailable; "
                "keeping the job in the central queue"
            )
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
                # The lock is held until the job is written down, so the
                # slot count the next job reads already includes this one.
                # A separate "claimed but not yet persisted" counter is the
                # obvious alternative and it leaks: any path that skips its
                # release marks a worker busy forever, which is exactly what
                # happened - an idle box reported at capacity with no job.
                async with self._submit_lock, fleet_admission_lock():
                    await self._require_hunyuan_admission(job)
                    worker, status_url = await hunyuan_client.submit(
                        client,
                        image_url=job.isolated_url,
                        backend_task_id=job.id,
                        queue_class=job.queue_class,
                        in_flight=self.in_flight_by_worker(),
                        excluded={
                            name
                            for name, until in {
                                **self._farm_worker_cooldowns,
                                **self._input_fetch_worker_cooldowns,
                                **(job.hunyuan_worker_cooldowns or {}),
                            }.items()
                            if float(until or 0) > time.time()
                        },
                    )
                    # store the status_url so a service restart can resume polling
                    job.hunyuan_task_id = status_url
                    job.hunyuan_worker = worker["name"]
                    job.hunyuan_waiting_for_capacity = False
                    await self._persist(job)
                print(f"[Renderfin][CharGen] job {job.id} hunyuan on {worker['name']}")
            payload = await hunyuan_client.wait_for_model(
                client,
                worker,
                job.hunyuan_task_id,
                timeout=await self._persisted_stage_budget(
                    job, HUNYUAN_STAGE_TIMEOUT
                ),
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
        task = await self._await_render(
            job.hunyuan_task_id,
            await self._persisted_stage_budget(job, HUNYUAN_STAGE_TIMEOUT),
        )
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
