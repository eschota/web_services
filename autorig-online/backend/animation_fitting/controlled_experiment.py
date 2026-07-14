from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .comfy import (
    ComfyAnimationClient,
    ComfyOutputFile,
    ComfySubmission,
    ComfyWorker,
    apply_workflow_bindings,
    deterministic_prompt_id,
    worker_from_environment,
)
from .specs import WorkflowProfile, load_animation_fitting_specs
from .storage import FfmpegFrameExtractor, ImmutableArtifactStore, StoredArtifact, WorkerBusyError


EXPERIMENT_SCHEMA = "autorig.animation-fitting-experiment.v1"
EXPECTED_EXPERIMENT_ID = "horse_walk_prompt_v2_semantic_reference_guide_080_v1"
V5_EXPERIMENT_ID = (
    "horse_walk_prompt_v5_semantic_chronological_av_"
    "seed_3794990487858656905_guide_065_v1"
)
V6_EXPERIMENT_ID = (
    "horse_walk_prompt_v6_semantic_chronological_av_"
    "seed_3794990487858656905_guide_075_v1"
)
V7_EXPERIMENT_ID = (
    "horse_walk_prompt_v7_semantic_chronological_av_"
    "seed_4891025524393280044_guide_065_v1"
)
SUPPORTED_EXPERIMENT_IDS = frozenset({
    EXPECTED_EXPERIMENT_ID,
    "horse_walk_prompt_v3_semantic_staggered_beats_guide_065_v1",
    "horse_walk_prompt_v4_semantic_seed_7721404986102443281_guide_055_v1",
    V5_EXPERIMENT_ID,
    V6_EXPERIMENT_ID,
    V7_EXPERIMENT_ID,
})
RESULT_SCHEMA = "autorig.animation-fitting-controlled-result.v1"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class ControlledExperimentError(RuntimeError):
    """Raised when a controlled generation would violate its immutable contract."""


@dataclass(frozen=True)
class ControlledExperimentPlan:
    experiment_id: str
    experiment_path: Path
    experiment_sha256: str
    reference_bundle: Path
    reference_image: Path
    reference_sha256: str
    positive_prompt: str
    negative_prompt: str
    frame_count: int
    input_fps: int
    output_fps: int
    seed: int
    workflow_name: str
    workflow_fingerprint: str
    start_guide_strength: float
    end_guide_strength: float
    artifact_root: Path


@dataclass(frozen=True)
class ControlledExperimentResult:
    job_id: str
    prompt_id: str
    raw_video: StoredArtifact
    frames: Sequence[StoredArtifact]
    resumed_existing_result: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": RESULT_SCHEMA,
            "job_id_string": self.job_id,
            "prompt_id_string": self.prompt_id,
            "raw_video_path_string": str(self.raw_video.path),
            "raw_video_sha256_string": self.raw_video.sha256,
            "raw_video_bytes_int": self.raw_video.size_bytes,
            "frame_count_int": len(self.frames),
            "frame_paths_array": [str(frame.path) for frame in self.frames],
            "frame_sha256_array": [frame.sha256 for frame in self.frames],
            "resumed_existing_result_bool": self.resumed_existing_result,
            "approval_state_string": "generated_not_approved",
            "send_to_skeletal_fitting_bool": False,
        }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlledExperimentError(f"Cannot read JSON contract {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ControlledExperimentError(f"JSON contract must be an object: {path}")
    return parsed


def _sha256(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise ControlledExperimentError(f"Cannot hash {path}: {exc}") from exc


def _require_sha(value: object, label: str) -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise ControlledExperimentError(f"{label} must be a lowercase SHA-256 digest")
    return digest


def _require_exact_sha(path: Path, expected: object, label: str) -> str:
    digest = _require_sha(expected, label)
    actual = _sha256(path)
    if actual != digest:
        raise ControlledExperimentError(
            f"{label} mismatch for {path}: expected {digest}, got {actual}"
        )
    return actual


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ControlledExperimentError(f"{label} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be a positive integer") from exc
    if result <= 0:
        raise ControlledExperimentError(f"{label} must be a positive integer")
    return result


def _guide_strength(value: object, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be numeric") from exc
    if not 0 <= result <= 1:
        raise ControlledExperimentError(f"{label} must be in [0, 1]")
    return result


def _verify_reference_manifest(bundle: Path, reference: Mapping[str, Any]) -> None:
    manifest_path = bundle / str(reference.get("immutable_manifest_filename_string") or "")
    _require_exact_sha(
        manifest_path,
        reference.get("immutable_manifest_sha256_string"),
        "reference immutable manifest SHA-256",
    )
    manifest = _read_json(manifest_path)
    if manifest.get("schema") != "autorig-ltx-semantic-reference-output.v1":
        raise ControlledExperimentError("reference immutable manifest schema is invalid")
    rows = manifest.get("files")
    if not isinstance(rows, list) or int(manifest.get("file_count", -1)) != len(rows) or len(rows) != 2:
        raise ControlledExperimentError("reference immutable manifest must contain exactly two files")
    expected_names = {
        str(reference.get("reference_png_filename_string") or ""),
        str(reference.get("derivation_manifest_filename_string") or ""),
    }
    if {row.get("filename") for row in rows if isinstance(row, dict)} != expected_names:
        raise ControlledExperimentError("reference immutable manifest file inventory is not exact")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ControlledExperimentError(f"reference manifest row {index} is invalid")
        filename = str(row.get("filename") or "")
        if not filename or Path(filename).name != filename:
            raise ControlledExperimentError("reference manifest filenames must be simple names")
        path = bundle / filename
        digest = _require_exact_sha(path, row.get("sha256"), f"reference file {filename} SHA-256")
        if path.stat().st_size != _positive_int(row.get("bytes"), f"reference file {filename} bytes"):
            raise ControlledExperimentError(f"reference file {filename} byte size mismatch")
        if filename == reference.get("reference_png_filename_string") and digest != reference.get(
            "reference_png_sha256_string"
        ):
            raise ControlledExperimentError("reference PNG disagrees with experiment contract")
        if filename == reference.get("derivation_manifest_filename_string") and digest != reference.get(
            "derivation_manifest_sha256_string"
        ):
            raise ControlledExperimentError("reference derivation manifest disagrees with experiment contract")


def load_controlled_plan(
    *,
    experiment_path: Path,
    authorization: str,
    reference_bundle: Path,
    artifact_root: Path,
) -> ControlledExperimentPlan:
    path = Path(experiment_path).resolve()
    experiment = _read_json(path)
    if experiment.get("schema") != EXPERIMENT_SCHEMA:
        raise ControlledExperimentError(f"experiment schema must be {EXPERIMENT_SCHEMA}")
    experiment_id = str(experiment.get("experiment_id_string") or "")
    if experiment_id not in SUPPORTED_EXPERIMENT_IDS:
        raise ControlledExperimentError(
            "controlled runner does not allow this experiment id: "
            f"{experiment_id!r}; supported={sorted(SUPPORTED_EXPERIMENT_IDS)}"
        )
    if authorization != experiment_id:
        raise ControlledExperimentError(
            f"explicit --authorize-experiment {experiment_id} is required"
        )
    authorization_object = experiment.get("generation_authorization_object")
    if not isinstance(authorization_object, dict) or authorization_object.get("authorized_bool") is not False:
        raise ControlledExperimentError(
            "immutable experiment must remain prepared/unapproved; runtime CLI authorization is recorded separately"
        )
    if experiment.get("approved_bool") is not False:
        raise ControlledExperimentError("controlled experiment must not be pre-approved")
    if experiment.get("generation_mode_string") != "loop":
        raise ControlledExperimentError("controlled semantic experiment must use loop generation")

    frame_count = _positive_int(experiment.get("frame_count_int"), "frame_count_int")
    if (frame_count - 1) % 8:
        raise ControlledExperimentError("frame_count_int must satisfy 8n+1")
    input_fps = _positive_int(experiment.get("input_fps_int"), "input_fps_int")
    output_fps = _positive_int(experiment.get("output_fps_int"), "output_fps_int")
    seed = _positive_int(experiment.get("seed_int"), "seed_int")
    positive_prompt = str(experiment.get("positive_prompt_string") or "")
    negative_prompt = str(experiment.get("negative_prompt_string") or "")
    if not positive_prompt or not negative_prompt:
        raise ControlledExperimentError("positive and negative prompts must be non-empty")

    variants = experiment.get("variants_array")
    if not isinstance(variants, list) or len(variants) != 1 or not isinstance(variants[0], dict):
        raise ControlledExperimentError("controlled experiment must contain exactly one variant")
    start_strength = _guide_strength(
        variants[0].get("start_guide_strength_float"), "start guide strength"
    )
    end_strength = _guide_strength(
        variants[0].get("end_guide_strength_float"), "end guide strength"
    )
    if start_strength != end_strength:
        raise ControlledExperimentError(
            "controlled loop experiment must use the same immutable start/end guide strength"
        )

    workflow = experiment.get("workflow_object")
    if not isinstance(workflow, dict):
        raise ControlledExperimentError("workflow_object is required")
    workflow_name = str(workflow.get("workflow_name_string") or "")
    workflow_fingerprint = _require_sha(
        workflow.get("workflow_fingerprint_sha256_string"), "workflow fingerprint"
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    if (
        workflow_name != profile.workflow_name
        or workflow_fingerprint != profile.workflow_fingerprint
        or input_fps != profile.input_fps
        or output_fps != profile.output_fps
    ):
        raise ControlledExperimentError("experiment workflow/FPS does not match the pinned loop profile")

    reference = experiment.get("reference_object")
    if not isinstance(reference, dict):
        raise ControlledExperimentError("reference_object is required")
    bundle = Path(reference_bundle).resolve()
    if bundle.name != reference.get("bundle_id_string") or not bundle.is_dir():
        raise ControlledExperimentError(
            f"reference bundle must be the existing {reference.get('bundle_id_string')} directory"
        )
    _verify_reference_manifest(bundle, reference)
    image = bundle / str(reference.get("reference_png_filename_string") or "")
    reference_sha = _require_exact_sha(
        image, reference.get("reference_png_sha256_string"), "reference PNG SHA-256"
    )
    derivation = _read_json(
        bundle / str(reference.get("derivation_manifest_filename_string") or "")
    )
    if derivation.get("schema") != "autorig-ltx-semantic-reference-derivation.v1":
        raise ControlledExperimentError("reference derivation schema is invalid")
    semantic_profile = derivation.get("semantic_profile")
    if not isinstance(semantic_profile, dict) or (
        semantic_profile.get("profile_id") != reference.get("semantic_profile_id_string")
        or semantic_profile.get("sha256") != reference.get("semantic_profile_sha256_string")
    ):
        raise ControlledExperimentError("semantic profile provenance disagrees with experiment contract")

    return ControlledExperimentPlan(
        experiment_id=experiment_id,
        experiment_path=path,
        experiment_sha256=_sha256(path),
        reference_bundle=bundle,
        reference_image=image,
        reference_sha256=reference_sha,
        positive_prompt=positive_prompt,
        negative_prompt=negative_prompt,
        frame_count=frame_count,
        input_fps=input_fps,
        output_fps=output_fps,
        seed=seed,
        workflow_name=workflow_name,
        workflow_fingerprint=workflow_fingerprint,
        start_guide_strength=start_strength,
        end_guide_strength=end_strength,
        artifact_root=Path(artifact_root).resolve(),
    )


def patch_guide_strengths(
    prompt: Mapping[str, Any], *, start_strength: float, end_strength: float
) -> dict[str, Any]:
    # apply_workflow_bindings already deep-copies the pinned prompt, so mutating
    # this owned result cannot change the immutable workflow source.
    result = dict(prompt)
    matches: dict[int, list[dict[str, Any]]] = {0: [], -1: []}
    for node in result.values():
        if not isinstance(node, dict) or node.get("class_type") != "LTXVAddGuide":
            continue
        inputs = node.get("inputs")
        if isinstance(inputs, dict) and inputs.get("frame_idx") in matches:
            matches[int(inputs["frame_idx"])].append(inputs)
    if len(matches[0]) != 1 or len(matches[-1]) != 1:
        raise ControlledExperimentError(
            "pinned loop workflow must have exactly one frame-0 and one frame-N-1 guide"
        )
    matches[0][0]["strength"] = _guide_strength(start_strength, "start guide strength")
    matches[-1][0]["strength"] = _guide_strength(end_strength, "end guide strength")
    return result


def _job_identity(plan: ControlledExperimentPlan, worker: ComfyWorker) -> tuple[dict[str, Any], str, str]:
    identity = {
        "schema": "autorig.animation-fitting-controlled-job-identity.v1",
        "experiment_id_string": plan.experiment_id,
        "experiment_sha256_string": plan.experiment_sha256,
        "runtime_authorization_string": f"explicit_cli:{plan.experiment_id}",
        "reference_sha256_string": plan.reference_sha256,
        "positive_prompt_sha256_string": hashlib.sha256(plan.positive_prompt.encode()).hexdigest(),
        "negative_prompt_sha256_string": hashlib.sha256(plan.negative_prompt.encode()).hexdigest(),
        "seed_int": plan.seed,
        "frame_count_int": plan.frame_count,
        "input_fps_int": plan.input_fps,
        "output_fps_int": plan.output_fps,
        "start_guide_strength_float": plan.start_guide_strength,
        "end_guide_strength_float": plan.end_guide_strength,
        "worker_id_string": worker.worker_id,
        "worker_base_url_string": worker.base_url,
        "workflow_name_string": worker.workflow_name,
        "workflow_fingerprint_string": worker.expected_workflow_fingerprint,
        "approval_state_string": "generated_not_approved",
        "send_to_skeletal_fitting_bool": False,
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    job_id = hashlib.sha256(canonical.encode()).hexdigest()
    idempotency_key = f"autorig-controlled-animation-fitting:{job_id}"
    return identity, job_id, idempotency_key


def _stored(path: Path, digest: str) -> StoredArtifact:
    resolved = Path(path).resolve()
    if not resolved.is_file() or _sha256(resolved) != digest:
        raise ControlledExperimentError(f"stored artifact is missing or corrupt: {resolved}")
    return StoredArtifact(sha256=digest, path=resolved, size_bytes=resolved.stat().st_size)


def _resume_completed(
    store: ImmutableArtifactStore, job_id: str, frame_count: int
) -> Optional[ControlledExperimentResult]:
    state = store.latest_job_state(job_id)
    if not state or state.get("status_string") != "completed":
        return None
    raw = _stored(
        Path(str(state.get("raw_video_path_string") or "")),
        _require_sha(state.get("raw_video_sha256_string"), "stored video SHA-256"),
    )
    frame_paths = state.get("frame_paths_array")
    frame_hashes = state.get("frame_sha256_array")
    if not isinstance(frame_paths, list) or not isinstance(frame_hashes, list) or (
        len(frame_paths) != frame_count or len(frame_hashes) != frame_count
    ):
        raise ControlledExperimentError("completed state frame inventory is incomplete")
    frames = tuple(
        _stored(Path(str(path)), _require_sha(digest, f"stored frame {index} SHA-256"))
        for index, (path, digest) in enumerate(zip(frame_paths, frame_hashes))
    )
    return ControlledExperimentResult(
        job_id=job_id,
        prompt_id=str(state.get("prompt_id_string") or ""),
        raw_video=raw,
        frames=frames,
        resumed_existing_result=True,
    )


async def run_controlled_experiment(
    plan: ControlledExperimentPlan,
    *,
    worker: Optional[ComfyWorker] = None,
    client_factory: Callable[[ComfyWorker], Any] = ComfyAnimationClient,
    frame_extractor: Optional[FfmpegFrameExtractor] = None,
) -> ControlledExperimentResult:
    selected_worker = worker or worker_from_environment("loop")
    if (
        selected_worker.workflow_name != plan.workflow_name
        or selected_worker.expected_workflow_fingerprint != plan.workflow_fingerprint
    ):
        raise ControlledExperimentError("worker does not match the experiment's pinned workflow")
    store = ImmutableArtifactStore(plan.artifact_root)
    store.ensure()
    identity, job_id, idempotency_key = _job_identity(plan, selected_worker)
    resumed = _resume_completed(store, job_id, plan.frame_count)
    if resumed:
        return resumed

    extractor = frame_extractor or FfmpegFrameExtractor(
        os.getenv("AUTORIG_FFMPEG_PATH", "ffmpeg")
    )
    planned_prompt_id = deterministic_prompt_id(idempotency_key)
    client = client_factory(selected_worker)
    try:
        api_prompt, fingerprint = await client.fetch_api_workflow()
        if fingerprint != plan.workflow_fingerprint:
            raise ControlledExperimentError("live workflow fingerprint changed after worker validation")
        with store.worker_lease(selected_worker.worker_id, owner_id=job_id):
            queue_load = await client.queue_load()
            if queue_load:
                raise WorkerBusyError(
                    f"Comfy worker {selected_worker.worker_id} has {queue_load} queued/running task(s)"
                )
            uploaded = await client.upload_reference_image(plan.reference_image)
            profile: WorkflowProfile = load_animation_fitting_specs().workflows["loop"]
            prompt = apply_workflow_bindings(
                api_prompt,
                profile,
                uploaded_start_image=uploaded,
                positive_prompt=plan.positive_prompt,
                negative_prompt=plan.negative_prompt,
                frame_count=plan.frame_count,
                seed=plan.seed,
                output_prefix=f"animation_fitting/controlled/{plan.experiment_id}/{job_id[:16]}",
            )
            prompt = patch_guide_strengths(
                prompt,
                start_strength=plan.start_guide_strength,
                end_strength=plan.end_guide_strength,
            )
            store.append_job_state(job_id, {
                **identity,
                "status_string": "submitting",
                "prompt_id_string": planned_prompt_id,
                "positive_prompt_string": plan.positive_prompt,
                "negative_prompt_string": plan.negative_prompt,
            })
            submission: ComfySubmission = await client.submit(prompt, idempotency_key)
            store.append_job_state(job_id, {
                **identity,
                "status_string": "rendering",
                "prompt_id_string": submission.prompt_id,
                "resumed_existing_prompt_bool": submission.resumed_existing_bool,
            })
            _, output = await client.wait_for_output(submission.prompt_id)
            if not isinstance(output, ComfyOutputFile):
                raise ControlledExperimentError("Comfy returned an invalid output contract")
            if not output.filename.lower().endswith(".mp4"):
                raise ControlledExperimentError("controlled experiment requires MP4 output")
            video = store.store_raw_video(await client.download_output(output))
            frames = tuple(await asyncio.to_thread(
                extractor.extract_and_store,
                video,
                store,
                expected_frame_count=plan.frame_count,
            ))
            store.append_job_state(job_id, {
                **identity,
                "status_string": "completed",
                "prompt_id_string": submission.prompt_id,
                "raw_video_path_string": str(video.path),
                "raw_video_sha256_string": video.sha256,
                "raw_video_bytes_int": video.size_bytes,
                "frame_count_int": len(frames),
                "frame_paths_array": [str(frame.path) for frame in frames],
                "frame_sha256_array": [frame.sha256 for frame in frames],
                "backend_output_object": {
                    "filename_string": output.filename,
                    "subfolder_string": output.subfolder,
                    "type_string": output.file_type,
                },
            })
            return ControlledExperimentResult(
                job_id=job_id,
                prompt_id=submission.prompt_id,
                raw_video=video,
                frames=frames,
                resumed_existing_result=False,
            )
    except Exception as exc:
        store.append_job_state(job_id, {
            **identity,
            "status_string": "failed",
            "prompt_id_string": planned_prompt_id,
            "error_type_string": type(exc).__name__,
            "error_string": str(exc)[:3000],
        })
        raise
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            await close()


def _default_spec_path() -> Path:
    return Path(__file__).resolve().parent / "specs" / "experiments" / (
        "horse_walk_prompt_v2_semantic_reference_guide_080.v1.json"
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one explicitly authorized, allowlisted immutable semantic Horse Walk LTX experiment."
    )
    parser.add_argument("--authorize-experiment", required=True)
    parser.add_argument("--experiment", type=Path, default=_default_spec_path())
    parser.add_argument("--reference-bundle", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--ffmpeg", default=os.getenv("AUTORIG_FFMPEG_PATH", "ffmpeg"))
    return parser


async def _main_async(arguments: argparse.Namespace) -> dict[str, Any]:
    plan = load_controlled_plan(
        experiment_path=arguments.experiment,
        authorization=arguments.authorize_experiment,
        reference_bundle=arguments.reference_bundle,
        artifact_root=arguments.artifact_root,
    )
    result = await run_controlled_experiment(
        plan,
        frame_extractor=FfmpegFrameExtractor(arguments.ffmpeg),
    )
    return result.to_dict()


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    result = asyncio.run(_main_async(arguments))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
