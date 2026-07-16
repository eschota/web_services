"""Immutable server validation and human review for browser fitting uploads.

The upload capture in :mod:`animation_fitting_candidate_ingest` is deliberately
untrusted.  This module is the next, still database-neutral, boundary:

* it re-opens an immutable upload by content identity rather than a caller path;
* a server-owned resolver binds the real task model and skeleton bytes;
* a trusted QA runner recomputes fixed-camera/deformation evidence;
* a human can review only that server evidence; and
* PASS emits a deterministic, content-pinned package descriptor.

No ``AnimalAnimationCandidate``, approved-clip, artifact, or activation row is
created here.  Route/auth wiring intentionally lives elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import Any, Awaitable, BinaryIO, Callable, Dict, Mapping, Union
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from animal_animation_library import (
    AnimationLibraryError,
    normalize_rig_type,
    normalize_sha256,
    taxonomy_clip,
    validate_visual_phase_gate,
)
from animation_fitting_candidate_ingest import (
    BUNDLE_SCHEMA,
    BrowserCandidatePlanTrust,
    CONTROLLED_GENERATION_V2_KEYS,
    CONTROLLED_RECEIPT_SCHEMA_V2,
    MAX_TOTAL_INGEST_BYTES,
    PHASES,
    parse_browser_candidate_plan,
    _normalize_worker_url,
)
from config import ANIMATION_FITTING_JOBS_ROOT
from database import AnimalAnimationFittingJob, AnimalAnimationLibraryVersion, Task


SERVER_VALIDATION_SCHEMA = "autorig.browser-animation-server-validation.v1"
HUMAN_REVIEW_SCHEMA = "autorig.browser-animation-human-review.v1"
PACKAGE_DESCRIPTOR_SCHEMA = "autorig.browser-animation-package-descriptor.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
PACKAGE_NAMESPACE = uuid.UUID("e1d5c947-36f3-5faf-a8af-3ed668b30fb0")

MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_METRICS_BYTES = 8 * 1024 * 1024
MAX_EVIDENCE_BYTES = 192 * 1024 * 1024
MAX_TASK_ARTIFACT_BYTES = 1024 * 1024 * 1024
MAX_RUNTIME_ARTIFACT_BYTES = 1024 * 1024 * 1024
MAX_RECEIPT_BYTES = 2 * 1024 * 1024
THREE_RUNTIME_REVISION = "160"
SERVER_EVIDENCE_NAMES = (
    "camera-settings.json",
    "deformation-report.json",
    "fixed-camera-preview.mp4",
    "phase-start.png",
    "phase-middle.png",
    "phase-three_quarter.png",
)
UPLOAD_ARTIFACT_NAMES = (
    "camera-settings.json",
    "deformation-report.json",
    "fitted-animation.json",
    "fixed-camera-preview.mp4",
    "phase-middle.png",
    "phase-start.png",
    "phase-three_quarter.png",
    "source-video.mp4",
    "three-clip.json",
    "visual-phase-qa.json",
)
SERVER_VALIDATION_FILES = (
    *SERVER_EVIDENCE_NAMES,
    "server-qa-metrics.json",
    "server-validation-receipt.json",
)


class CandidateReviewError(AnimationLibraryError):
    """A fail-closed candidate validation or review failure."""


ArtifactSource = Union[bytes, bytearray, memoryview, BinaryIO]


@dataclass(frozen=True)
class TaskArtifactRequest:
    """Server-derived lookup key passed to the trusted artifact resolver."""

    task_id: str
    task_guid: str


@dataclass(frozen=True)
class TrustedTaskArtifacts:
    """Actual server-owned source artifacts; hashes are computed by this module."""

    model_path: Path
    skeleton_path: Path


@dataclass(frozen=True)
class TrustedQARuntime:
    """Server-configured executables and the pinned Three.js runtime."""

    node_path: Path
    chrome_path: Path
    ffmpeg_path: Path
    ffprobe_path: Path
    three_module_path: Path
    three_revision: str
    three_expected_sha256: str


@dataclass(frozen=True)
class TrustedQARunContext:
    """Pinned, server-resolved inputs supplied to the trusted QA implementation."""

    job_id: str
    library_revision: str
    rig_type: str
    semantic_id: str
    candidate_identity_sha256: str
    candidate_directory: Path
    three_clip_path: Path
    source_video_path: Path
    task_model_path: Path
    task_skeleton_path: Path
    runtime: TrustedQARuntime
    runtime_pins: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class TrustedQAEvidence:
    """Fresh evidence returned by trusted server code, never by the uploader."""

    runner_name: str
    runner_revision: str
    metrics: Mapping[str, Any]
    artifacts: Mapping[str, ArtifactSource]


@dataclass(frozen=True)
class HumanReviewDecision:
    decision: str
    reviewer_id: str
    reviewed_at: str
    reason: str | None = None


@dataclass(frozen=True)
class ImmutableReceipt:
    identity_sha256: str
    directory: Path
    receipt_path: Path
    receipt_sha256: str
    receipt: Dict[str, Any]
    created: bool
    package_descriptor_path: Path | None = None
    package_descriptor_sha256: str | None = None
    package_id: str | None = None


TaskArtifactResolver = Callable[
    [TaskArtifactRequest],
    TrustedTaskArtifacts | Awaitable[TrustedTaskArtifacts],
]
TrustedQARunner = Callable[
    [TrustedQARunContext],
    TrustedQAEvidence | Awaitable[TrustedQAEvidence],
]


@dataclass(frozen=True)
class _BundleSnapshot:
    directory: Path
    manifest: Dict[str, Any]
    manifest_pin: Dict[str, Any]
    artifacts: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class _LifecycleSnapshot:
    binding: Dict[str, Any]
    binding_sha256: str
    task_request: TaskArtifactRequest


def _error(message: str, status_code: int = 409) -> CandidateReviewError:
    return CandidateReviewError(message, status_code=status_code)


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise _error("receipt input is not finite canonical JSON", 400) from exc


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _strict_object(payload: bytes, field: str) -> Dict[str, Any]:
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _error(f"{field} is not strict UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise _error(f"{field} must be a JSON object")
    # Round-tripping here rejects values that cannot be represented by the
    # immutable canonical receipt encoder.
    _canonical_json(value)
    return value


def _sha(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(result):
        raise _error(f"{field} must be a lowercase SHA-256")
    return result


def _uuid(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not UUID_RE.fullmatch(result):
        raise _error(f"{field} must be a canonical UUID")
    return result


def _exact_object(value: Any, field: str, keys: tuple[str, ...]) -> Dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(keys):
        raise _error(f"{field} must contain exactly: {', '.join(keys)}")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _error(f"{field} must be a positive integer")
    return value


def _positive_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _error(f"{field} must be a positive finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise _error(f"{field} must be a positive finite number")
    return result


def _reject_symlink_chain(path: Path, field: str) -> None:
    current = path
    while True:
        if current.exists() and current.is_symlink():
            raise _error(f"{field} must not traverse a symlink")
        parent = current.parent
        if parent == current:
            break
        current = parent


def _root(path_value: str) -> Path:
    lexical = Path(os.path.abspath(os.fspath(path_value)))
    if not lexical.exists() or not lexical.is_dir():
        raise _error("ANIMATION_FITTING_JOBS_ROOT is missing")
    _reject_symlink_chain(lexical, "ANIMATION_FITTING_JOBS_ROOT")
    resolved = lexical.resolve(strict=True)
    if resolved != lexical:
        raise _error("ANIMATION_FITTING_JOBS_ROOT must not use aliases or symlinks")
    return resolved


def _secure_existing_directory(root: Path, path: Path, field: str) -> Path:
    lexical = Path(os.path.abspath(os.fspath(path)))
    try:
        relative = lexical.relative_to(root)
    except ValueError as exc:
        raise _error(f"{field} escapes ANIMATION_FITTING_JOBS_ROOT") from exc
    current = root
    for part in relative.parts:
        if part in ("", ".", ".."):
            raise _error(f"{field} contains traversal")
        current = current / part
        if current.is_symlink():
            raise _error(f"{field} must not traverse a symlink")
        if not current.exists() or not current.is_dir():
            raise _error(f"{field} is missing")
        resolved = current.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            raise _error(f"{field} escapes ANIMATION_FITTING_JOBS_ROOT")
    return lexical


def _secure_directory_chain(root: Path, path: Path, *, create: bool) -> Path:
    lexical = Path(os.path.abspath(os.fspath(path)))
    try:
        relative = lexical.relative_to(root)
    except ValueError as exc:
        raise _error("receipt directory escapes ANIMATION_FITTING_JOBS_ROOT") from exc
    current = root
    for part in relative.parts:
        if part in ("", ".", ".."):
            raise _error("receipt directory contains traversal")
        current = current / part
        if current.is_symlink():
            raise _error("receipt directory must not traverse a symlink")
        if not current.exists():
            if not create:
                raise _error("receipt directory is missing")
            try:
                current.mkdir()
            except FileExistsError:
                pass
        if current.is_symlink() or not current.is_dir():
            raise _error("receipt ancestor is not a real directory")
        resolved = current.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            raise _error("receipt directory escapes ANIMATION_FITTING_JOBS_ROOT")
    return lexical


def _open_regular_no_follow(path: Path):
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise _error(f"{path.name} is not a regular file")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _hash_regular_file(path_value: Path, field: str, maximum: int) -> Dict[str, Any]:
    path = Path(os.path.abspath(os.fspath(path_value)))
    _reject_symlink_chain(path, field)
    if not path.exists() or path.is_symlink():
        raise _error(f"{field} is missing or symlinked")
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
        raise _error(f"{field} must be a non-empty regular file")
    if before.st_size > maximum:
        raise _error(f"{field} exceeds the server size limit", 413)
    digest = hashlib.sha256()
    total = 0
    with _open_regular_no_follow(path) as handle:
        while chunk := handle.read(1024 * 1024):
            total += len(chunk)
            if total > maximum:
                raise _error(f"{field} exceeds the server size limit", 413)
            digest.update(chunk)
    after = os.lstat(path)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise _error(f"{field} changed while it was hashed")
    return {"filename": path.name, "bytes": total, "sha256": digest.hexdigest()}


def _read_bounded_file(path: Path, field: str, maximum: int) -> bytes:
    pin = _hash_regular_file(path, field, maximum)
    with _open_regular_no_follow(path) as handle:
        payload = handle.read(pin["bytes"] + 1)
    if len(payload) != pin["bytes"] or _sha256(payload) != pin["sha256"]:
        raise _error(f"{field} changed while it was read")
    return payload


def _pin_payload(payload: bytes, filename: str) -> Dict[str, Any]:
    return {"filename": filename, "bytes": len(payload), "sha256": _sha256(payload)}


def _validate_pin(value: Any, filename: str, field: str) -> Dict[str, Any]:
    pin = _exact_object(value, field, ("filename", "bytes", "sha256"))
    if pin["filename"] != filename:
        raise _error(f"{field}.filename does not match its inventory key")
    size = _positive_int(pin["bytes"], f"{field}.bytes")
    digest = _sha(pin["sha256"], f"{field}.sha256")
    return {"filename": filename, "bytes": size, "sha256": digest}


def _verify_file_pin(path: Path, expected: Mapping[str, Any], field: str) -> None:
    actual = _hash_regular_file(path, field, max(expected["bytes"], 1))
    if actual["bytes"] != expected["bytes"] or actual["sha256"] != expected["sha256"]:
        raise _error(f"{field} differs from its immutable SHA binding")


def _load_bundle(root: Path, job_id: str, identity: str) -> _BundleSnapshot:
    identity = _sha(identity, "candidate_identity_sha256")
    directory = _secure_existing_directory(
        root,
        root / job_id / "browser-candidates" / identity[:2] / identity,
        "browser candidate bundle",
    )
    manifest_path = directory / "candidate-manifest.json"
    manifest_bytes = _read_bounded_file(
        manifest_path, "candidate-manifest.json", MAX_MANIFEST_BYTES
    )
    manifest = _strict_object(manifest_bytes, "candidate-manifest.json")
    if manifest_bytes != _canonical_json(manifest) + b"\n":
        raise _error("candidate-manifest.json is not canonical")
    _exact_object(
        manifest,
        "candidate-manifest.json",
        (
            "schema",
            "library",
            "fitting_job",
            "source_task",
            "candidate",
            "controlled_generation",
            "artifacts",
            "identity_sha256",
        ),
    )
    if manifest.get("schema") != BUNDLE_SCHEMA or manifest.get("identity_sha256") != identity:
        raise _error("candidate manifest schema or identity is invalid")
    unsigned = dict(manifest)
    unsigned.pop("identity_sha256", None)
    if _sha256(_canonical_json(unsigned)) != identity:
        raise _error("candidate manifest content does not match its identity")
    artifacts_value = manifest.get("artifacts")
    if not isinstance(artifacts_value, dict) or set(artifacts_value) != set(
        UPLOAD_ARTIFACT_NAMES
    ):
        raise _error("candidate bundle artifact inventory is invalid")
    artifacts = {
        name: _validate_pin(artifacts_value[name], name, f"artifacts.{name}")
        for name in UPLOAD_ARTIFACT_NAMES
    }
    if sum(pin["bytes"] for pin in artifacts.values()) > MAX_TOTAL_INGEST_BYTES:
        raise _error("candidate bundle exceeds the server size limit", 413)
    entries = tuple(directory.iterdir())
    expected_names = sorted((*UPLOAD_ARTIFACT_NAMES, "candidate-manifest.json"))
    if sorted(path.name for path in entries) != expected_names:
        raise _error("candidate bundle inventory drifted after ingestion")
    for path in entries:
        if path.is_symlink() or not path.is_file():
            raise _error("candidate bundle contains a symlink or non-file")
    for name, pin in artifacts.items():
        _verify_file_pin(directory / name, pin, f"candidate artifact {name}")
    candidate = manifest.get("candidate")
    if not isinstance(candidate, dict):
        raise _error("candidate manifest state is missing")
    validation = candidate.get("server_validation")
    if (
        candidate.get("review_state") != "uploaded_pending_server_validation"
        or candidate.get("uploaded_qa_assertions_trusted") is not False
        or not isinstance(validation, dict)
        or validation.get("status") != "pending"
        or validation.get("required")
        != [
            "task_model_sha256_binding",
            "task_skeleton_sha256_binding",
            "media_decode_and_phase_extraction",
            "deformation_recompute",
            "visual_review",
        ]
    ):
        raise _error("uploaded candidate is not in the untrusted pending state")
    return _BundleSnapshot(
        directory=directory,
        manifest=manifest,
        manifest_pin=_pin_payload(manifest_bytes, "candidate-manifest.json"),
        artifacts=artifacts,
    )


def _parse_job_config(value: str) -> Dict[str, Any]:
    payload = (value or "{}").encode("utf-8")
    if len(payload) > MAX_MANIFEST_BYTES:
        raise _error("fitting job config exceeds the server size limit")
    return _strict_object(payload, "fitting job config")


async def _load_lifecycle(
    db: AsyncSession,
    job_id: str,
    bundle: _BundleSnapshot,
    *,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> _LifecycleSnapshot:
    job = (
        await db.execute(
            select(AnimalAnimationFittingJob).where(AnimalAnimationFittingJob.id == job_id)
        )
    ).scalar_one_or_none()
    if job is None:
        raise _error("Animation fitting job not found", 404)
    version = (
        await db.execute(
            select(AnimalAnimationLibraryVersion).where(
                AnimalAnimationLibraryVersion.id == job.library_version_id
            )
        )
    ).scalar_one_or_none()
    if version is None:
        raise _error("Animation library version not found", 404)
    manifest = bundle.manifest
    library_manifest = manifest.get("library") or {}
    fitting_manifest = manifest.get("fitting_job") or {}
    task_manifest = manifest.get("source_task") or {}
    candidate_manifest = manifest.get("candidate") or {}
    if fitting_manifest.get("id") != job.id:
        raise _error("candidate bundle is not bound to the requested fitting job")
    rig_type = normalize_rig_type(job.rig_type)
    semantic_id = str(job.semantic_id or "").strip().lower()
    taxonomy_clip(semantic_id)
    if (
        job.status != "review"
        or version.status != "draft"
        or normalize_rig_type(version.rig_type) != rig_type
        or library_manifest.get("version_id") != version.id
        or library_manifest.get("revision") != version.revision
        or library_manifest.get("rig_type") != rig_type
        or fitting_manifest.get("semantic_id") != semantic_id
        or fitting_manifest.get("workflow_name") != job.workflow_name
        or fitting_manifest.get("workflow_fingerprint") != job.workflow_fingerprint
    ):
        raise _error("fitting job or draft library lifecycle differs from the bundle")
    skeleton_sha = normalize_sha256(
        version.template_skeleton_sha256, "template_skeleton_sha256"
    )
    if library_manifest.get("template_skeleton_sha256") != skeleton_sha:
        raise _error("candidate library skeleton binding drifted")
    task_id = _uuid(task_manifest.get("id"), "source_task.id")
    task_guid = _uuid(task_manifest.get("guid"), "source_task.guid")
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    if (
        task is None
        or str(task.guid or "").strip().lower() != task_guid
        or task.status != "done"
        or str(task.input_type or "").strip().lower() != "animal"
    ):
        raise _error("source task is not the exact completed animal task")
    config = _parse_job_config(job.config_json)
    binding, planned_slots = parse_browser_candidate_plan(
        config,
        semantic_id=semantic_id,
        candidate_limit=int(job.candidate_limit),
        candidate_target=int(job.candidate_target),
        workflow_name=job.workflow_name,
        workflow_fingerprint=job.workflow_fingerprint,
        prompt_id=job.prompt_id,
        worker_url=job.worker_url,
        trusted_plan_inputs=trusted_plan_inputs,
        trusted_store_root=fitting_jobs_root,
    )
    if (
        _uuid(binding.get("task_id"), "binding.task_id") != task_id
        or _uuid(binding.get("task_guid"), "binding.task_guid") != task_guid
    ):
        raise _error("fitting job task binding drifted")
    seed = candidate_manifest.get("seed")
    candidate_index = candidate_manifest.get("candidate_index")
    slot_matches = [
        slot
        for slot in planned_slots
        if slot["candidate_index"] == candidate_index and slot["seed"] == seed
    ]
    if (
        isinstance(seed, bool)
        or not isinstance(seed, int)
        or isinstance(candidate_index, bool)
        or not isinstance(candidate_index, int)
        or len(slot_matches) != 1
    ):
        raise _error("fitting job candidate seed binding drifted")
    candidate_slot = slot_matches[0]
    source_model_sha = normalize_sha256(
        binding.get("source_model_sha256"), "source_model_sha256"
    )
    source_skeleton_sha = normalize_sha256(
        binding.get("source_skeleton_sha256"), "source_skeleton_sha256"
    )
    binding_frame_count = _positive_int(
        binding.get("frame_count"), "binding.frame_count"
    )
    binding_input_fps = None
    candidate_input_fps = None
    if "input_fps" in binding:
        binding_input_fps = _positive_int(
            binding.get("input_fps"), "binding.input_fps"
        )
        candidate_input_fps = _positive_int(
            candidate_manifest.get("input_fps"), "candidate.input_fps"
        )
        if binding_input_fps != 24:
            raise _error("fitting job input_fps is not the canonical 24 fps timing")
    binding_fps = _positive_number(binding.get("output_fps"), "binding.output_fps")
    candidate_frame_count = _positive_int(
        candidate_manifest.get("frame_count"), "candidate.frame_count"
    )
    candidate_fps = _positive_number(candidate_manifest.get("fps"), "candidate.fps")
    if (
        source_model_sha != candidate_manifest.get("source_model_sha256")
        or source_skeleton_sha != candidate_manifest.get("source_skeleton_sha256")
        or source_skeleton_sha != skeleton_sha
        or binding.get("source_rig_type") != candidate_manifest.get("source_rig_type")
        or binding_frame_count != candidate_frame_count
        or binding_input_fps != candidate_input_fps
        or binding_fps != candidate_fps
    ):
        raise _error("fitting job model, skeleton, rig, or timing binding drifted")
    source_video = _exact_object(
        candidate_slot.get("source_video"),
        "candidate_slot.source_video",
        ("path", "sha256", "bytes"),
    )
    source_video_pin = bundle.artifacts["source-video.mp4"]
    if (
        _sha(source_video.get("sha256"), "binding.source_video.sha256")
        != source_video_pin["sha256"]
        or source_video.get("bytes") != source_video_pin["bytes"]
    ):
        raise _error("fitting job source video SHA binding drifted")
    verified_generation = candidate_slot.get("verified_generation")
    is_v2 = (
        isinstance(verified_generation, Mapping)
        and verified_generation.get("schema") == CONTROLLED_RECEIPT_SCHEMA_V2
    )
    controlled_keys = CONTROLLED_GENERATION_V2_KEYS if is_v2 else (
        "job_id",
        "prompt_id",
        "experiment_id",
        "experiment_sha256",
        "workflow_fingerprint_sha256",
        *(
            ("worker_id", "worker_base_url")
            if verified_generation is not None
            else ()
        ),
    )
    controlled = _exact_object(
        candidate_slot.get("controlled_generation"),
        "candidate_slot.controlled_generation",
        controlled_keys,
    )
    if controlled != manifest.get("controlled_generation"):
        raise _error("controlled generation provenance drifted")
    if normalize_sha256(
        controlled.get("workflow_fingerprint_sha256"),
        "controlled_generation.workflow_fingerprint_sha256",
    ) != normalize_sha256(job.workflow_fingerprint, "job.workflow_fingerprint"):
        raise _error("controlled generation workflow binding drifted")
    if verified_generation is not None and (
        not str(controlled.get("worker_id") or "").strip()
        or _normalize_worker_url(
            controlled.get("worker_base_url"),
            "controlled_generation.worker_base_url",
        )
        != _normalize_worker_url(job.worker_url, "job.worker_url")
    ):
        raise _error("controlled generation worker binding drifted")
    lifecycle_binding = {
        "job": {
            "id": job.id,
            "status": job.status,
            "library_version_id": version.id,
            "rig_type": rig_type,
            "semantic_id": semantic_id,
            "workflow_name": job.workflow_name,
            "workflow_fingerprint": job.workflow_fingerprint,
        },
        "library": {
            "revision": version.revision,
            "status": version.status,
            "template_skeleton_sha256": skeleton_sha,
            "qa_profile_revision": version.qa_profile_revision,
        },
        "task": {
            "id": task_id,
            "guid": task_guid,
            "status": task.status,
            "input_type": str(task.input_type).lower(),
        },
        "ingest_binding": binding,
    }
    return _LifecycleSnapshot(
        binding=lifecycle_binding,
        binding_sha256=_sha256(_canonical_json(lifecycle_binding)),
        task_request=TaskArtifactRequest(task_id=task_id, task_guid=task_guid),
    )


async def _await_result(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def _resolve_task_artifacts(
    resolver: TaskArtifactResolver, request: TaskArtifactRequest
) -> tuple[TrustedTaskArtifacts, Dict[str, Dict[str, Any]]]:
    resolved = await _await_result(resolver(request))
    if not isinstance(resolved, TrustedTaskArtifacts):
        raise _error("trusted task artifact resolver returned an invalid result")
    model_path = Path(os.path.abspath(os.fspath(resolved.model_path)))
    skeleton_path = Path(os.path.abspath(os.fspath(resolved.skeleton_path)))
    pins = {
        "task_model": _hash_regular_file(
            model_path, "trusted task model", MAX_TASK_ARTIFACT_BYTES
        ),
        "task_skeleton": _hash_regular_file(
            skeleton_path, "trusted task skeleton", MAX_TASK_ARTIFACT_BYTES
        ),
    }
    return TrustedTaskArtifacts(model_path, skeleton_path), pins


def _resolve_qa_runtime(
    runtime: TrustedQARuntime,
) -> tuple[TrustedQARuntime, Dict[str, Any]]:
    if not isinstance(runtime, TrustedQARuntime):
        raise _error("trusted QA runtime descriptor is invalid")
    revision = str(runtime.three_revision or "").strip()
    if revision != THREE_RUNTIME_REVISION:
        raise _error(f"trusted QA Three.js revision must be {THREE_RUNTIME_REVISION}")
    expected_three_sha = _sha(
        runtime.three_expected_sha256, "trusted QA Three.js expected SHA"
    )
    resolved = TrustedQARuntime(
        node_path=Path(os.path.abspath(os.fspath(runtime.node_path))),
        chrome_path=Path(os.path.abspath(os.fspath(runtime.chrome_path))),
        ffmpeg_path=Path(os.path.abspath(os.fspath(runtime.ffmpeg_path))),
        ffprobe_path=Path(os.path.abspath(os.fspath(runtime.ffprobe_path))),
        three_module_path=Path(
            os.path.abspath(os.fspath(runtime.three_module_path))
        ),
        three_revision=revision,
        three_expected_sha256=expected_three_sha,
    )
    pins = {
        "node": _hash_regular_file(
            resolved.node_path, "trusted QA Node runtime", MAX_RUNTIME_ARTIFACT_BYTES
        ),
        "chrome": _hash_regular_file(
            resolved.chrome_path,
            "trusted QA Chrome runtime",
            MAX_RUNTIME_ARTIFACT_BYTES,
        ),
        "ffmpeg": _hash_regular_file(
            resolved.ffmpeg_path,
            "trusted QA ffmpeg runtime",
            MAX_RUNTIME_ARTIFACT_BYTES,
        ),
        "ffprobe": _hash_regular_file(
            resolved.ffprobe_path,
            "trusted QA ffprobe runtime",
            MAX_RUNTIME_ARTIFACT_BYTES,
        ),
        "three_module": _hash_regular_file(
            resolved.three_module_path,
            "trusted QA Three.js runtime",
            MAX_RUNTIME_ARTIFACT_BYTES,
        ),
    }
    if pins["three_module"]["sha256"] != expected_three_sha:
        raise _error("trusted QA Three.js runtime differs from its expected SHA")
    return resolved, {
        "three_revision": revision,
        "three_expected_sha256": expected_three_sha,
        "artifacts": pins,
    }


def _read_artifact(source: ArtifactSource, field: str, maximum: int) -> bytes:
    if isinstance(source, (bytes, bytearray, memoryview)):
        payload = bytes(source)
    elif hasattr(source, "read"):
        payload = source.read(maximum + 1)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise _error(f"{field} stream did not return bytes")
        payload = bytes(payload)
    else:
        raise _error(f"{field} must be bytes or a binary stream")
    if not payload:
        raise _error(f"{field} is empty")
    if len(payload) > maximum:
        raise _error(f"{field} exceeds the server size limit", 413)
    return payload


def _normalize_qa_evidence(
    result: TrustedQAEvidence,
    *,
    bundle: _BundleSnapshot,
    rig_type: str,
    semantic_id: str,
) -> tuple[Dict[str, Any], bytes, Dict[str, bytes], Dict[str, Dict[str, Any]]]:
    if not isinstance(result, TrustedQAEvidence):
        raise _error("trusted QA runner returned an invalid result")
    runner_name = str(result.runner_name or "").strip()
    runner_revision = str(result.runner_revision or "").strip()
    if not runner_name or len(runner_name) > 128 or not runner_revision or len(runner_revision) > 128:
        raise _error("trusted QA runner identity is invalid")
    if set(result.artifacts) != set(SERVER_EVIDENCE_NAMES):
        raise _error("trusted QA runner returned an invalid evidence inventory")
    evidence = {
        name: _read_artifact(
            result.artifacts[name], f"trusted QA {name}", MAX_EVIDENCE_BYTES
        )
        for name in SERVER_EVIDENCE_NAMES
    }
    if sum(len(payload) for payload in evidence.values()) > MAX_EVIDENCE_BYTES:
        raise _error("trusted QA evidence set exceeds the server size limit", 413)
    if not evidence["fixed-camera-preview.mp4"][4:8] == b"ftyp" and not evidence[
        "fixed-camera-preview.mp4"
    ].startswith(b"\x1aE\xdf\xa3"):
        raise _error("trusted QA preview is not a recognized video container")
    for phase in PHASES:
        if not evidence[f"phase-{phase}.png"].startswith(b"\x89PNG\r\n\x1a\n"):
            raise _error(f"trusted QA {phase} frame is not PNG")
    # JSON evidence is parsed now so malformed server-runner output cannot be
    # hidden behind a correct-looking digest.
    _strict_object(evidence["camera-settings.json"], "trusted camera settings")
    _strict_object(evidence["deformation-report.json"], "trusted deformation report")
    metrics_bytes = _canonical_json(dict(result.metrics)) + b"\n"
    if len(metrics_bytes) > MAX_METRICS_BYTES:
        raise _error("trusted QA metrics exceed the server size limit", 413)
    metrics = _strict_object(metrics_bytes, "trusted QA metrics")
    pins = {name: _pin_payload(payload, name) for name, payload in evidence.items()}
    gate = metrics.get("visual_phase_gate")
    if not isinstance(gate, dict):
        raise _error("trusted QA metrics are missing visual_phase_gate")
    if gate.get("decision") is not None or gate.get("reviewer") != {
        "id": None,
        "reviewed_at": None,
    }:
        raise _error("trusted QA evidence must be unreviewed")
    if (
        gate.get("rig_type") != rig_type
        or gate.get("semantic_id") != semantic_id
        or gate.get("fitted_clip_sha256")
        != bundle.artifacts["three-clip.json"]["sha256"]
    ):
        raise _error("trusted QA evidence is bound to a different candidate")
    camera = gate.get("camera") or {}
    separation = gate.get("coincident_rest_vertex_separation") or {}
    if camera.get("settings_sha256") != pins["camera-settings.json"]["sha256"]:
        raise _error("trusted QA camera SHA does not bind the server evidence")
    if separation.get("report_sha256") != pins["deformation-report.json"]["sha256"]:
        raise _error("trusted QA deformation SHA does not bind the server evidence")
    frames = gate.get("frames")
    frame_count = bundle.manifest["candidate"].get("frame_count")
    if isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count <= 1:
        raise _error("candidate frame_count is invalid")
    expected_indices = (0, (frame_count - 1) // 2, math.floor((frame_count - 1) * 0.75))
    if not isinstance(frames, list) or len(frames) != len(PHASES):
        raise _error("trusted QA evidence must contain all fixed phases")
    for phase, frame_index, row in zip(PHASES, expected_indices, frames):
        if (
            not isinstance(row, dict)
            or row.get("phase") != phase
            or row.get("frame_index") != frame_index
            or row.get("sha256") != pins[f"phase-{phase}.png"]["sha256"]
        ):
            raise _error(f"trusted QA {phase} frame does not bind server evidence")
    # Exercise the same release gate used by library approval, but on a deep
    # server-owned copy.  The real reviewer identity is added only at review.
    probe = json.loads(metrics_bytes)
    probe["visual_phase_gate"]["decision"] = "PASS"
    probe["visual_phase_gate"]["reviewer"] = {
        "id": "server-validation-probe",
        "reviewed_at": "1970-01-01T00:00:00Z",
    }
    validate_visual_phase_gate(
        probe,
        expected_rig_type=rig_type,
        expected_semantic_id=semantic_id,
        expected_fitted_clip_sha256=bundle.artifacts["three-clip.json"]["sha256"],
    )
    return (
        {"name": runner_name, "revision": runner_revision},
        metrics_bytes,
        evidence,
        pins,
    )


def _write_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _verify_directory(directory: Path, files: Mapping[str, bytes], field: str) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise _error(f"{field} path is not a real directory")
    entries = tuple(directory.iterdir())
    if sorted(path.name for path in entries) != sorted(files):
        raise _error(f"{field} immutable inventory collision")
    for path in entries:
        if path.is_symlink() or not path.is_file():
            raise _error(f"{field} contains a symlink or non-file")
    for name, expected in files.items():
        payload = _read_bounded_file(directory / name, f"{field} {name}", len(expected))
        if payload != expected:
            raise _error(f"{field} content-addressed identity collision")


def _publish_files(root: Path, target: Path, files: Mapping[str, bytes]) -> bool:
    parent = _secure_directory_chain(root, target.parent, create=True)
    if target.is_symlink():
        raise _error("content-addressed receipt target is a symlink")
    if target.exists():
        _verify_directory(target, files, "receipt")
        return False
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=str(parent)))
    try:
        for name, payload in files.items():
            _write_file(staging / name, payload)
        try:
            staging.rename(target)
        except OSError:
            if not target.is_dir():
                raise
            _verify_directory(target, files, "receipt")
            shutil.rmtree(staging)
            return False
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return True


async def _recheck_all(
    db: AsyncSession,
    *,
    root: Path,
    job_id: str,
    candidate_identity: str,
    expected_lifecycle_sha: str,
    expected_task_pins: Mapping[str, Mapping[str, Any]],
    resolver: TaskArtifactResolver,
    qa_runtime: TrustedQARuntime | None = None,
    expected_runtime_pins: Mapping[str, Any] | None = None,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> tuple[_BundleSnapshot, _LifecycleSnapshot, TrustedTaskArtifacts]:
    await db.rollback()
    bundle = _load_bundle(root, job_id, candidate_identity)
    lifecycle = await _load_lifecycle(
        db,
        job_id,
        bundle,
        fitting_jobs_root=str(root),
        trusted_plan_inputs=trusted_plan_inputs,
    )
    if lifecycle.binding_sha256 != expected_lifecycle_sha:
        raise _error("job, library, or task lifecycle changed before publication")
    task_artifacts, task_pins = await _resolve_task_artifacts(
        resolver, lifecycle.task_request
    )
    if task_pins != expected_task_pins:
        raise _error("trusted task artifacts changed before publication")
    if qa_runtime is not None or expected_runtime_pins is not None:
        if qa_runtime is None or expected_runtime_pins is None:
            raise _error("trusted QA runtime recheck is incomplete")
        _, runtime_pins = _resolve_qa_runtime(qa_runtime)
        if runtime_pins != expected_runtime_pins:
            raise _error("trusted QA runtime changed before publication")
    return bundle, lifecycle, task_artifacts


async def create_server_validation_receipt(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_identity_sha256: str,
    task_artifact_resolver: TaskArtifactResolver,
    qa_runner: TrustedQARunner,
    qa_runtime: TrustedQARuntime,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableReceipt:
    """Recompute and immutably pin trusted server evidence for one upload."""
    if db.new or db.dirty or db.deleted:
        raise _error("server validation requires a clean database session")
    job_id = _uuid(job_id, "job_id")
    candidate_identity = _sha(
        candidate_identity_sha256, "candidate_identity_sha256"
    )
    root = _root(fitting_jobs_root)
    bundle = _load_bundle(root, job_id, candidate_identity)
    lifecycle = await _load_lifecycle(
        db,
        job_id,
        bundle,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    task_artifacts, task_pins = await _resolve_task_artifacts(
        task_artifact_resolver, lifecycle.task_request
    )
    resolved_runtime, runtime_pins = _resolve_qa_runtime(qa_runtime)
    candidate = bundle.manifest["candidate"]
    if task_pins["task_model"]["sha256"] != candidate.get("source_model_sha256"):
        raise _error("actual task model SHA does not match the candidate binding")
    if task_pins["task_skeleton"]["sha256"] != candidate.get(
        "source_skeleton_sha256"
    ):
        raise _error("actual task skeleton SHA does not match the candidate binding")
    context = TrustedQARunContext(
        job_id=job_id,
        library_revision=bundle.manifest["library"]["revision"],
        rig_type=bundle.manifest["library"]["rig_type"],
        semantic_id=bundle.manifest["fitting_job"]["semantic_id"],
        candidate_identity_sha256=candidate_identity,
        candidate_directory=bundle.directory,
        three_clip_path=bundle.directory / "three-clip.json",
        source_video_path=bundle.directory / "source-video.mp4",
        task_model_path=task_artifacts.model_path,
        task_skeleton_path=task_artifacts.skeleton_path,
        runtime=resolved_runtime,
        runtime_pins=runtime_pins["artifacts"],
    )
    qa_result = await _await_result(qa_runner(context))
    runner, metrics_bytes, evidence, evidence_pins = _normalize_qa_evidence(
        qa_result,
        bundle=bundle,
        rig_type=context.rig_type,
        semantic_id=context.semantic_id,
    )
    metrics_pin = _pin_payload(metrics_bytes, "server-qa-metrics.json")
    validation_binding = {
        "schema": SERVER_VALIDATION_SCHEMA,
        "candidate": {
            "identity_sha256": candidate_identity,
            "manifest": bundle.manifest_pin,
            "uploaded_qa_assertions_trusted": False,
            "uploaded_qa_artifact_used": False,
        },
        "lifecycle": {
            "binding_sha256": lifecycle.binding_sha256,
            "job_id": job_id,
            "library_version_id": bundle.manifest["library"]["version_id"],
            "library_revision": context.library_revision,
            "rig_type": context.rig_type,
            "semantic_id": context.semantic_id,
            "source_task": bundle.manifest["source_task"],
        },
        "task_artifacts": task_pins,
        "candidate_artifacts": {
            "three_clip": bundle.artifacts["three-clip.json"],
            "source_video": bundle.artifacts["source-video.mp4"],
        },
        "trusted_qa": {
            "runner": runner,
            "runtime": runtime_pins,
            "status": "PASS",
            "metrics": metrics_pin,
            "evidence": evidence_pins,
        },
    }
    identity = _sha256(_canonical_json(validation_binding))
    receipt = {**validation_binding, "identity_sha256": identity}
    receipt_bytes = _canonical_json(receipt) + b"\n"
    if len(receipt_bytes) > MAX_RECEIPT_BYTES:
        raise _error("server validation receipt exceeds the server size limit")
    files = {
        **evidence,
        "server-qa-metrics.json": metrics_bytes,
        "server-validation-receipt.json": receipt_bytes,
    }
    # Runner execution is intentionally outside the final publication window.
    # Re-open every immutable and database binding before the rename.
    bundle, lifecycle, _ = await _recheck_all(
        db,
        root=root,
        job_id=job_id,
        candidate_identity=candidate_identity,
        expected_lifecycle_sha=lifecycle.binding_sha256,
        expected_task_pins=task_pins,
        resolver=task_artifact_resolver,
        qa_runtime=resolved_runtime,
        expected_runtime_pins=runtime_pins,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    target = (
        root
        / job_id
        / "browser-candidate-reviews"
        / candidate_identity[:2]
        / candidate_identity
        / "server-validations"
        / identity
    )
    created = _publish_files(root, target, files)
    return ImmutableReceipt(
        identity_sha256=identity,
        directory=target,
        receipt_path=target / "server-validation-receipt.json",
        receipt_sha256=_sha256(receipt_bytes),
        receipt=receipt,
        created=created,
    )


def _load_server_validation(
    root: Path,
    job_id: str,
    candidate_identity: str,
    validation_identity: str,
) -> tuple[Path, Dict[str, Any], Dict[str, Any], bytes, Dict[str, bytes]]:
    directory = _secure_existing_directory(
        root,
        root
        / job_id
        / "browser-candidate-reviews"
        / candidate_identity[:2]
        / candidate_identity
        / "server-validations"
        / validation_identity,
        "server validation receipt",
    )
    entries = tuple(directory.iterdir())
    if sorted(path.name for path in entries) != sorted(SERVER_VALIDATION_FILES):
        raise _error("server validation receipt inventory drifted")
    if any(path.is_symlink() or not path.is_file() for path in entries):
        raise _error("server validation receipt contains a symlink or non-file")
    receipt_bytes = _read_bounded_file(
        directory / "server-validation-receipt.json",
        "server-validation-receipt.json",
        MAX_RECEIPT_BYTES,
    )
    receipt = _strict_object(receipt_bytes, "server-validation-receipt.json")
    if receipt_bytes != _canonical_json(receipt) + b"\n":
        raise _error("server validation receipt is not canonical")
    if (
        receipt.get("schema") != SERVER_VALIDATION_SCHEMA
        or receipt.get("identity_sha256") != validation_identity
    ):
        raise _error("server validation receipt identity is invalid")
    unsigned = dict(receipt)
    unsigned.pop("identity_sha256", None)
    if _sha256(_canonical_json(unsigned)) != validation_identity:
        raise _error("server validation receipt content identity is invalid")
    if receipt.get("candidate", {}).get("identity_sha256") != candidate_identity:
        raise _error("server validation receipt belongs to another candidate")
    trusted_qa = receipt.get("trusted_qa") or {}
    if (
        trusted_qa.get("status") != "PASS"
        or receipt.get("candidate", {}).get("uploaded_qa_assertions_trusted") is not False
        or receipt.get("candidate", {}).get("uploaded_qa_artifact_used") is not False
    ):
        raise _error("server validation receipt is not a trusted PASS")
    runtime = _exact_object(
        trusted_qa.get("runtime"),
        "trusted_qa.runtime",
        ("three_revision", "three_expected_sha256", "artifacts"),
    )
    if runtime["three_revision"] != THREE_RUNTIME_REVISION:
        raise _error("server validation Three.js revision is invalid")
    expected_three_sha = _sha(
        runtime["three_expected_sha256"],
        "trusted_qa.runtime.three_expected_sha256",
    )
    runtime_artifacts = runtime.get("artifacts")
    if not isinstance(runtime_artifacts, dict) or set(runtime_artifacts) != {
        "node",
        "chrome",
        "ffmpeg",
        "ffprobe",
        "three_module",
    }:
        raise _error("server validation runtime inventory is invalid")
    for name, value in runtime_artifacts.items():
        if not isinstance(value, dict):
            raise _error(f"server validation runtime {name} pin is invalid")
        filename = str(value.get("filename") or "")
        if not filename or Path(filename).name != filename:
            raise _error(f"server validation runtime {name} filename is invalid")
        _validate_pin(value, filename, f"trusted_qa.runtime.artifacts.{name}")
    if runtime_artifacts["three_module"]["sha256"] != expected_three_sha:
        raise _error("server validation Three.js runtime SHA is invalid")
    metrics_pin = _validate_pin(
        trusted_qa.get("metrics"), "server-qa-metrics.json", "trusted_qa.metrics"
    )
    metrics_bytes = _read_bounded_file(
        directory / "server-qa-metrics.json",
        "server-qa-metrics.json",
        metrics_pin["bytes"],
    )
    if _sha256(metrics_bytes) != metrics_pin["sha256"]:
        raise _error("server QA metrics SHA drifted")
    metrics = _strict_object(metrics_bytes, "server-qa-metrics.json")
    evidence_value = trusted_qa.get("evidence")
    if not isinstance(evidence_value, dict) or set(evidence_value) != set(
        SERVER_EVIDENCE_NAMES
    ):
        raise _error("server validation evidence inventory is invalid")
    evidence: Dict[str, bytes] = {}
    evidence_pins: Dict[str, Dict[str, Any]] = {}
    for name in SERVER_EVIDENCE_NAMES:
        pin = _validate_pin(evidence_value[name], name, f"trusted_qa.evidence.{name}")
        payload = _read_bounded_file(directory / name, f"server evidence {name}", pin["bytes"])
        if _sha256(payload) != pin["sha256"]:
            raise _error(f"server evidence {name} SHA drifted")
        evidence[name] = payload
        evidence_pins[name] = pin
    gate = metrics.get("visual_phase_gate")
    if (
        not isinstance(gate, dict)
        or gate.get("decision") is not None
        or gate.get("reviewer") != {"id": None, "reviewed_at": None}
    ):
        raise _error("server validation metrics are not an unreviewed evidence set")
    if (gate.get("camera") or {}).get("settings_sha256") != evidence_pins[
        "camera-settings.json"
    ]["sha256"]:
        raise _error("server validation camera evidence binding drifted")
    if (gate.get("coincident_rest_vertex_separation") or {}).get(
        "report_sha256"
    ) != evidence_pins["deformation-report.json"]["sha256"]:
        raise _error("server validation deformation evidence binding drifted")
    frames = gate.get("frames")
    if not isinstance(frames, list) or len(frames) != len(PHASES):
        raise _error("server validation phase evidence inventory drifted")
    for phase, row in zip(PHASES, frames):
        if (
            not isinstance(row, dict)
            or row.get("phase") != phase
            or row.get("sha256") != evidence_pins[f"phase-{phase}.png"]["sha256"]
        ):
            raise _error(f"server validation {phase} evidence binding drifted")
    return directory, receipt, metrics, receipt_bytes, evidence


def _review_value(review: HumanReviewDecision) -> Dict[str, Any]:
    if not isinstance(review, HumanReviewDecision):
        raise _error("human review decision is invalid", 400)
    decision = str(review.decision or "").strip().upper()
    if decision not in ("PASS", "HOLD", "REJECT"):
        raise _error("human review decision must be PASS, HOLD, or REJECT", 400)
    reviewer_id = str(review.reviewer_id or "")
    if (
        not reviewer_id.strip()
        or reviewer_id != reviewer_id.strip()
        or len(reviewer_id) > 320
    ):
        raise _error("human reviewer id is invalid", 400)
    reviewed_at = str(review.reviewed_at or "").strip()
    # Reuse the release validator for exact timestamp parsing on PASS.  For
    # HOLD/REJECT, perform an equivalent timezone check here.
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(reviewed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _error("human reviewed_at must be an ISO-8601 timestamp", 400) from exc
    if parsed.utcoffset() is None:
        raise _error("human reviewed_at must include a timezone", 400)
    reason = review.reason
    if reason is not None:
        reason = str(reason)
        if reason != reason.strip() or len(reason) > 2000 or not reason:
            raise _error("human review reason is invalid", 400)
    if decision in ("HOLD", "REJECT") and reason is None:
        raise _error("HOLD/REJECT human review requires a reason", 400)
    return {
        "decision": decision,
        "reviewer_id": reviewer_id,
        "reviewed_at": reviewed_at,
        "reason": reason,
    }


async def create_human_review_receipt(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_identity_sha256: str,
    server_validation_identity_sha256: str,
    review: HumanReviewDecision,
    task_artifact_resolver: TaskArtifactResolver,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableReceipt:
    """Pin a human decision under the shared candidate publication lock.

    The import is deliberately local: candidate selection imports the immutable
    review readers from this module.  At call time both modules are initialized,
    and the shared lock serializes this publication with OPEN/FINAL selection,
    candidate admission, and generation closure.
    """
    from animation_fitting_candidate_selection import (
        async_candidate_publication_lock,
    )

    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        return await _create_human_review_receipt_locked(
            db,
            job_id=job_id,
            candidate_identity_sha256=candidate_identity_sha256,
            server_validation_identity_sha256=server_validation_identity_sha256,
            review=review,
            task_artifact_resolver=task_artifact_resolver,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )


async def _create_human_review_receipt_locked(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_identity_sha256: str,
    server_validation_identity_sha256: str,
    review: HumanReviewDecision,
    task_artifact_resolver: TaskArtifactResolver,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableReceipt:
    """Build and publish a human review while the publication lock is held."""
    if db.new or db.dirty or db.deleted:
        raise _error("human review requires a clean database session")
    job_id = _uuid(job_id, "job_id")
    candidate_identity = _sha(
        candidate_identity_sha256, "candidate_identity_sha256"
    )
    validation_identity = _sha(
        server_validation_identity_sha256, "server_validation_identity_sha256"
    )
    review_value = _review_value(review)
    root = _root(fitting_jobs_root)
    bundle = _load_bundle(root, job_id, candidate_identity)
    lifecycle = await _load_lifecycle(
        db,
        job_id,
        bundle,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    _, validation, metrics, validation_bytes, _ = _load_server_validation(
        root, job_id, candidate_identity, validation_identity
    )
    if validation.get("lifecycle", {}).get("binding_sha256") != lifecycle.binding_sha256:
        raise _error("server validation lifecycle binding is stale")
    if validation.get("candidate", {}).get("manifest") != bundle.manifest_pin:
        raise _error("server validation candidate manifest binding is stale")
    if validation.get("candidate_artifacts") != {
        "three_clip": bundle.artifacts["three-clip.json"],
        "source_video": bundle.artifacts["source-video.mp4"],
    }:
        raise _error("server validation candidate artifact binding is stale")
    task_artifacts, task_pins = await _resolve_task_artifacts(
        task_artifact_resolver, lifecycle.task_request
    )
    if validation.get("task_artifacts") != task_pins:
        raise _error("server validation task artifact binding is stale")
    reviewed_metrics = json.loads(_canonical_json(metrics))
    reviewed_metrics["visual_phase_gate"]["decision"] = review_value["decision"]
    reviewed_metrics["visual_phase_gate"]["reviewer"] = {
        "id": review_value["reviewer_id"],
        "reviewed_at": review_value["reviewed_at"],
    }
    if review_value["decision"] == "PASS":
        validate_visual_phase_gate(
            reviewed_metrics,
            expected_rig_type=bundle.manifest["library"]["rig_type"],
            expected_semantic_id=bundle.manifest["fitting_job"]["semantic_id"],
            expected_fitted_clip_sha256=bundle.artifacts["three-clip.json"]["sha256"],
        )
    review_binding = {
        "schema": HUMAN_REVIEW_SCHEMA,
        "candidate": {
            "identity_sha256": candidate_identity,
            "manifest": bundle.manifest_pin,
        },
        "server_validation": {
            "identity_sha256": validation_identity,
            "receipt": _pin_payload(
                validation_bytes, "server-validation-receipt.json"
            ),
            "trusted_qa_metrics": validation["trusted_qa"]["metrics"],
        },
        "lifecycle_binding_sha256": lifecycle.binding_sha256,
        "review": review_value,
    }
    identity = _sha256(_canonical_json(review_binding))
    receipt = {**review_binding, "identity_sha256": identity}
    receipt_bytes = _canonical_json(receipt) + b"\n"
    receipt_pin = _pin_payload(receipt_bytes, "human-review-receipt.json")
    descriptor_bytes = None
    package_id = None
    descriptor_pin = None
    if review_value["decision"] == "PASS":
        candidate_id = str(
            uuid.uuid5(
                PACKAGE_NAMESPACE,
                (
                    f"{job_id}:{bundle.manifest['candidate']['seed']}:"
                    f"{bundle.manifest_pin['sha256']}"
                ),
            )
        )
        package_id = candidate_id
        descriptor = {
            "schema": PACKAGE_DESCRIPTOR_SCHEMA,
            "package_id": package_id,
            "candidate_id": candidate_id,
            "candidate_bundle_sha256": bundle.manifest_pin["sha256"],
            "human_review_sha256": receipt_pin["sha256"],
            "semantic_id": bundle.manifest["fitting_job"]["semantic_id"],
            "clip": bundle.artifacts["three-clip.json"],
            "review_identity_sha256": identity,
            "library": bundle.manifest["library"],
            "fitting_job": bundle.manifest["fitting_job"],
            "source_task": bundle.manifest["source_task"],
            "candidate_identity_sha256": candidate_identity,
            "server_validation_identity_sha256": validation_identity,
            "review": review_value,
            "pins": {
                "candidate_manifest": bundle.manifest_pin,
                "three_clip": bundle.artifacts["three-clip.json"],
                "task_model": task_pins["task_model"],
                "task_skeleton": task_pins["task_skeleton"],
                "server_validation_receipt": _pin_payload(
                    validation_bytes, "server-validation-receipt.json"
                ),
                "server_qa_metrics": validation["trusted_qa"]["metrics"],
                "human_review_receipt": receipt_pin,
            },
        }
        descriptor_bytes = _canonical_json(descriptor) + b"\n"
        descriptor_pin = _pin_payload(descriptor_bytes, "package-descriptor.json")
    files = {"human-review-receipt.json": receipt_bytes}
    if descriptor_bytes is not None:
        files["package-descriptor.json"] = descriptor_bytes
    # Recheck all server-owned bindings after the reviewer decision has been
    # validated and immediately before immutable publication.
    bundle, lifecycle, task_artifacts = await _recheck_all(
        db,
        root=root,
        job_id=job_id,
        candidate_identity=candidate_identity,
        expected_lifecycle_sha=lifecycle.binding_sha256,
        expected_task_pins=task_pins,
        resolver=task_artifact_resolver,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    _load_server_validation(root, job_id, candidate_identity, validation_identity)
    target = (
        root
        / job_id
        / "browser-candidate-reviews"
        / candidate_identity[:2]
        / candidate_identity
        / "human-review"
    )
    created = _publish_files(root, target, files)
    return ImmutableReceipt(
        identity_sha256=identity,
        directory=target,
        receipt_path=target / "human-review-receipt.json",
        receipt_sha256=_sha256(receipt_bytes),
        receipt=receipt,
        created=created,
        package_descriptor_path=(
            target / "package-descriptor.json" if descriptor_bytes is not None else None
        ),
        package_descriptor_sha256=(
            descriptor_pin["sha256"] if descriptor_pin is not None else None
        ),
        package_id=package_id,
    )
