from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from .comfy import (
    ComfyAnimationClient,
    ComfySubmission,
    ComfyWorker,
    apply_workflow_bindings,
    deterministic_prompt_id,
)
from .specs import AnimationFittingSpecs, load_animation_fitting_specs
from .storage import FfmpegFrameExtractor, ImmutableArtifactStore, StoredArtifact, WorkerBusyError


@dataclass(frozen=True)
class CandidatePolicy:
    initial_count: int = 8
    top_k: int = 3
    retry_batch_count: int = 8
    max_count: int = 16

    @classmethod
    def from_specs(cls, specs: AnimationFittingSpecs) -> "CandidatePolicy":
        raw = specs.qa.candidate_policy
        return cls(
            initial_count=raw.initial_count,
            top_k=raw.top_k,
            retry_batch_count=raw.retry_batch_count,
            max_count=raw.max_count,
        )


@dataclass(frozen=True)
class CandidatePlan:
    action_id: str
    candidate_index: int
    seed: int
    candidate_id: str


@dataclass(frozen=True)
class CandidateAssessment:
    candidate_id: str
    candidate_index: int
    accepted_bool: bool
    ranking_score_float: float
    failed_gates: Tuple[str, ...]


@dataclass(frozen=True)
class CandidateRunResult:
    job_id: str
    action_id: str
    candidate_index: int
    seed: int
    prompt_id: str
    worker_base_url: str
    workflow_name: str
    workflow_fingerprint: str
    raw_video: StoredArtifact
    frames: Tuple[StoredArtifact, ...]


class AnimationFittingOrchestrator:
    def __init__(
        self,
        artifact_store: ImmutableArtifactStore,
        *,
        specs: Optional[AnimationFittingSpecs] = None,
        frame_extractor: Optional[FfmpegFrameExtractor] = None,
        client_factory: Optional[Callable[[ComfyWorker], Any]] = None,
    ) -> None:
        self.specs = specs or load_animation_fitting_specs()
        self.policy = CandidatePolicy.from_specs(self.specs)
        self.artifact_store = artifact_store
        self.frame_extractor = frame_extractor or FfmpegFrameExtractor()
        self.client_factory = client_factory or (lambda worker: ComfyAnimationClient(worker))

    def initial_candidate_plan(self, task_id: str, action_id: str) -> Tuple[CandidatePlan, ...]:
        return self._candidate_plans(task_id, action_id, range(self.policy.initial_count))

    def next_candidate_plan(
        self,
        task_id: str,
        action_id: str,
        existing: Sequence[CandidateAssessment],
    ) -> Tuple[CandidatePlan, ...]:
        self.specs.action(action_id)
        accepted_count = sum(1 for item in existing if item.accepted_bool)
        used = {int(item.candidate_index) for item in existing}
        if accepted_count >= self.policy.top_k or len(used) >= self.policy.max_count:
            return ()
        target_count = self.policy.initial_count if len(used) < self.policy.initial_count else min(
            self.policy.max_count,
            len(used) + self.policy.retry_batch_count,
        )
        indices = []
        candidate_index = 0
        while len(used) + len(indices) < target_count and candidate_index < self.policy.max_count:
            if candidate_index not in used:
                indices.append(candidate_index)
            candidate_index += 1
        return self._candidate_plans(task_id, action_id, indices)

    def assess_candidate(
        self,
        candidate: CandidatePlan,
        metrics: Mapping[str, object],
    ) -> CandidateAssessment:
        action = self.specs.action(candidate.action_id)
        required_gates = list(self.specs.qa.hard_gate_metric_keys)
        if action.is_loop:
            required_gates.extend(self.specs.qa.loop_hard_gate_metric_keys)
        failed = tuple(key for key in required_gates if metrics.get(key) is not True)

        weights = dict(self.specs.qa.ranking_weights)
        if not action.is_loop:
            weights.pop("loop_seam_float", None)
        weight_total = sum(weights.values()) or 1.0
        score = 0.0
        for key, weight in weights.items():
            value = metrics.get(key, self.specs.qa.missing_metric_score)
            try:
                normalized = max(0.0, min(1.0, float(value)))
            except (TypeError, ValueError):
                normalized = self.specs.qa.missing_metric_score
            score += normalized * weight
        score = score / weight_total
        return CandidateAssessment(
            candidate_id=candidate.candidate_id,
            candidate_index=candidate.candidate_index,
            accepted_bool=not failed,
            ranking_score_float=round(score, 8),
            failed_gates=failed,
        )

    def top_candidates(self, assessments: Sequence[CandidateAssessment]) -> Tuple[CandidateAssessment, ...]:
        accepted = [item for item in assessments if item.accepted_bool]
        accepted.sort(key=lambda item: (-item.ranking_score_float, item.candidate_index, item.candidate_id))
        return tuple(accepted[: self.policy.top_k])

    async def run_candidate(
        self,
        *,
        task_id: str,
        action_id: str,
        candidate_index: int,
        species: str,
        reference_frame_path: Path,
        worker: ComfyWorker,
        motion_notes: str = "",
    ) -> CandidateRunResult:
        task_token = _safe_task_token(task_id)
        action = self.specs.action(action_id)
        if not 0 <= int(candidate_index) < self.policy.max_count:
            raise ValueError(f"candidate_index must be 0..{self.policy.max_count - 1}")
        workflow_profile = self.specs.workflow_for_action(action_id)
        if worker.workflow_name != workflow_profile.workflow_name:
            raise ValueError(
                f"Worker workflow {worker.workflow_name} does not match {workflow_profile.workflow_name}"
            )
        if worker.expected_workflow_fingerprint != workflow_profile.workflow_fingerprint:
            raise ValueError(
                f"Worker fingerprint does not match pinned {workflow_profile.workflow_fingerprint}"
            )
        frame_path = Path(reference_frame_path).resolve()
        frame_bytes = await asyncio.to_thread(frame_path.read_bytes)
        if not frame_bytes:
            raise ValueError("reference_frame_path is empty")
        source_sha256 = hashlib.sha256(frame_bytes).hexdigest()
        plan = self._candidate_plans(task_token, action_id, [int(candidate_index)])[0]
        positive_prompt = action.render_positive_prompt(species, motion_notes)

        identity_object = {
            "schema": "autorig.animation-fitting-job-identity.v1",
            "task_id_string": task_token,
            "action_id_string": action_id,
            "candidate_index_int": plan.candidate_index,
            "seed_int": plan.seed,
            "source_sha256_string": source_sha256,
            "worker_id_string": worker.worker_id,
            "worker_base_url_string": worker.base_url,
            "workflow_name_string": worker.workflow_name,
            "workflow_fingerprint_string": worker.expected_workflow_fingerprint,
            "prompt_schema_string": self.specs.prompt_schema,
            "workflow_schema_string": self.specs.workflow_schema,
            "generation_mode_string": action.generation_mode,
            "frame_count_int": action.frame_count,
            "input_fps_int": action.input_fps,
            "output_fps_int": action.output_fps,
            "positive_prompt_sha256_string": hashlib.sha256(
                positive_prompt.encode("utf-8")
            ).hexdigest(),
            "negative_prompt_sha256_string": hashlib.sha256(
                action.common_negative_prompt.encode("utf-8")
            ).hexdigest(),
        }
        identity_json = json.dumps(identity_object, sort_keys=True, separators=(",", ":"))
        job_id = hashlib.sha256(identity_json.encode("utf-8")).hexdigest()
        idempotency_key = f"autorig-animation-fitting:{job_id}"
        planned_prompt_id = deterministic_prompt_id(idempotency_key)
        output_prefix = (
            f"animation_fitting/{task_token}/{action_id}/"
            f"candidate_{plan.candidate_index:02d}_{job_id[:12]}"
        )
        client = self.client_factory(worker)
        raw_video: Optional[StoredArtifact] = None
        workflow_fingerprint = worker.expected_workflow_fingerprint
        try:
            api_prompt, workflow_fingerprint = await client.fetch_api_workflow()
            # worker_id is the configured GPU identity.  Keeping the lease keyed
            # to it prevents two aliases/routes for the same board from running
            # concurrent diffusion jobs.
            with self.artifact_store.worker_lease(worker.worker_id, owner_id=job_id):
                queue_load = await client.queue_load()
                if queue_load > 0:
                    raise WorkerBusyError(
                        f"Comfy worker {worker.worker_id} already has "
                        f"{queue_load} queued or running task(s)"
                    )
                uploaded = await client.upload_reference_image(frame_path)
                prompt = apply_workflow_bindings(
                    api_prompt,
                    workflow_profile,
                    uploaded_start_image=uploaded,
                    positive_prompt=positive_prompt,
                    negative_prompt=action.common_negative_prompt,
                    frame_count=action.frame_count,
                    seed=plan.seed,
                    output_prefix=output_prefix,
                )
                self.artifact_store.append_job_state(
                    job_id,
                    {
                        **identity_object,
                        "status_string": "submitting",
                        "generation_mode_string": action.generation_mode,
                        "input_fps_int": action.input_fps,
                        "output_fps_int": action.output_fps,
                        "frame_count_int": action.frame_count,
                        "prompt_id_string": planned_prompt_id,
                        "output_prefix_string": output_prefix,
                        "positive_prompt_string": positive_prompt,
                        "negative_prompt_string": action.common_negative_prompt,
                    },
                )
                submission: ComfySubmission = await client.submit(prompt, idempotency_key)
                self.artifact_store.append_job_state(
                    job_id,
                    {
                        **identity_object,
                        "status_string": "rendering",
                        "prompt_id_string": submission.prompt_id,
                        "resumed_existing_bool": submission.resumed_existing_bool,
                    },
                )
                _, output_file = await client.wait_for_output(submission.prompt_id)
                if not output_file.filename.lower().endswith(".mp4"):
                    raise ValueError(
                        f"Animation fitting requires immutable raw MP4 output, got {output_file.filename}"
                    )
                video_bytes = await client.download_output(output_file)
                raw_video = self.artifact_store.store_raw_video(video_bytes)
                frames = tuple(
                    await asyncio.to_thread(
                        self.frame_extractor.extract_and_store,
                        raw_video,
                        self.artifact_store,
                        expected_frame_count=action.frame_count,
                    )
                )
                self.artifact_store.append_job_state(
                    job_id,
                    {
                        **identity_object,
                        "status_string": "completed",
                        "prompt_id_string": submission.prompt_id,
                        "raw_video_sha256_string": raw_video.sha256,
                        "raw_video_path_string": str(raw_video.path),
                        "frame_count_int": len(frames),
                        "frame_sha256_array": [frame.sha256 for frame in frames],
                        "backend_output_object": {
                            "filename_string": output_file.filename,
                            "subfolder_string": output_file.subfolder,
                            "type_string": output_file.file_type,
                        },
                    },
                )
                return CandidateRunResult(
                    job_id=job_id,
                    action_id=action_id,
                    candidate_index=plan.candidate_index,
                    seed=plan.seed,
                    prompt_id=submission.prompt_id,
                    worker_base_url=worker.base_url,
                    workflow_name=worker.workflow_name,
                    workflow_fingerprint=workflow_fingerprint,
                    raw_video=raw_video,
                    frames=frames,
                )
        except Exception as exc:
            failure: Dict[str, Any] = {
                **identity_object,
                "status_string": "failed",
                "prompt_id_string": planned_prompt_id,
                "error_type_string": type(exc).__name__,
                "error_string": str(exc)[:3000],
            }
            if raw_video is not None:
                failure["raw_video_sha256_string"] = raw_video.sha256
                failure["raw_video_path_string"] = str(raw_video.path)
            self.artifact_store.append_job_state(job_id, failure)
            raise
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                await close()

    def _candidate_plans(
        self,
        task_id: str,
        action_id: str,
        indices: Sequence[int],
    ) -> Tuple[CandidatePlan, ...]:
        task_token = _safe_task_token(task_id)
        self.specs.action(action_id)
        result = []
        for raw_index in indices:
            index = int(raw_index)
            if not 0 <= index < self.policy.max_count:
                raise ValueError(f"candidate index must be 0..{self.policy.max_count - 1}")
            seed_material = f"{task_token}\n{action_id}\n{index}\n{self.specs.prompt_schema}"
            digest = hashlib.sha256(seed_material.encode("utf-8")).hexdigest()
            seed = int(digest[:16], 16) & ((1 << 63) - 1)
            candidate_id = f"{action_id}-c{index:02d}-{digest[16:28]}"
            result.append(
                CandidatePlan(
                    action_id=action_id,
                    candidate_index=index,
                    seed=seed,
                    candidate_id=candidate_id,
                )
            )
        return tuple(result)


def _safe_task_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value or "").strip())[:120]
    if not token:
        raise ValueError("task_id is required")
    return token
