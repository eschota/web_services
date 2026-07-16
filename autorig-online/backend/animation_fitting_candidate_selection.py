"""Server-owned admission and deterministic selection for browser candidates.

The browser upload boundary is intentionally untrusted.  This module assigns a
candidate to a server-planned slot, pins the trusted server-validation outcome,
and publishes immutable OPEN/FINAL selection receipts.  No caller can provide
rank, rank score, or ranking metrics.

The file lock in this module is the common publication lock.  Candidate ingest
route wiring must call :func:`assert_candidate_publication_open` while holding
that lock before publishing a new bundle.  Admission, generation closure, and
FINAL publication already use the same lock here.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Dict, Iterator, Mapping, Sequence
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from animal_animation_library import (
    AnimationLibraryError,
    normalize_rig_type,
    taxonomy_clip,
    validate_visual_phase_gate,
)
from animation_fitting.specs import SPEC_ROOT
from animation_fitting_candidate_ingest import (
    BrowserCandidatePlanTrust,
    derive_browser_candidate_seed,
    parse_browser_candidate_plan,
)
from animation_fitting_candidate_review import (
    MAX_MANIFEST_BYTES,
    MAX_RECEIPT_BYTES,
    SERVER_VALIDATION_SCHEMA,
    _canonical_json,
    _exact_object,
    _load_bundle,
    _load_server_validation,
    _pin_payload,
    _publish_files,
    _read_bounded_file,
    _root,
    _secure_directory_chain,
    _sha,
    _sha256,
    _strict_object,
    _uuid,
)
from config import ANIMATION_FITTING_JOBS_ROOT
from database import (
    AnimalAnimationCandidate,
    AnimalAnimationFittingJob,
    AnimalAnimationLibraryVersion,
    Task,
)


CANDIDATE_ADMISSION_SCHEMA = "autorig.browser-animation-candidate-admission.v1"
CANDIDATE_OUTCOME_SCHEMA = "autorig.browser-animation-candidate-outcome.v1"
CANDIDATE_GENERATION_CLOSURE_SCHEMA = (
    "autorig.browser-animation-candidate-generation-closure.v1"
)
CANDIDATE_SELECTION_SCHEMA = "autorig.browser-animation-candidate-selection.v1"
SELECTION_CONFIG_SCHEMA = "autorig.browser-animation-candidate-selection-config.v1"
TOP_K = 3
SCORE_ROUND_DIGITS = 8
LOCK_TIMEOUT_SECONDS = 30.0
MAX_ADMISSION_BYTES = 2 * 1024 * 1024
SAFE_FAILURE_RE = re.compile(r"^[a-z][a-z0-9_.-]{2,127}$")

BACKEND_ROOT = Path(__file__).resolve().parent
TAXONOMY_PATH = BACKEND_ROOT / "animal_animation_taxonomy.v1.json"
QA_PROFILE_PATH = SPEC_ROOT / "qa_profile.v1.json"


class CandidateSelectionError(AnimationLibraryError):
    """Fail-closed admission, outcome, snapshot, or finalization failure."""


@dataclass(frozen=True)
class ImmutableCandidateSelectionReceipt:
    identity_sha256: str
    directory: Path
    receipt_path: Path
    receipt_sha256: str
    receipt: Dict[str, Any]
    created: bool


@dataclass(frozen=True)
class ImmutableCandidateAdmission:
    identity_sha256: str
    directory: Path
    receipt_path: Path
    receipt_sha256: str
    receipt: Dict[str, Any]
    created: bool


@dataclass(frozen=True)
class _JobSnapshot:
    job: AnimalAnimationFittingJob
    version: AnimalAnimationLibraryVersion
    mode: str
    task_id: str
    planned_slots: Mapping[int, Mapping[str, Any]]
    lifecycle: Dict[str, Any]
    lifecycle_identity_sha256: str
    human_review_lifecycle_binding_sha256: str


@dataclass(frozen=True)
class _Admission:
    directory: Path
    receipt: Dict[str, Any]
    receipt_bytes: bytes
    receipt_pin: Dict[str, Any]


@dataclass(frozen=True)
class _Outcome:
    directory: Path
    receipt: Dict[str, Any]
    receipt_bytes: bytes
    receipt_pin: Dict[str, Any]


def _error(message: str, status_code: int = 409) -> CandidateSelectionError:
    return CandidateSelectionError(message, status_code=status_code)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _error(f"{field} must be a positive integer")
    return value


def _candidate_index(value: Any, limit: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < limit:
        raise _error(f"candidate_index must be in 0..{limit - 1}", 400)
    return value


def _read_json_file(path: Path, field: str, maximum: int) -> tuple[Dict[str, Any], bytes]:
    payload = _read_bounded_file(path, field, maximum)
    value = _strict_object(payload, field)
    if payload != _canonical_json(value) + b"\n":
        raise _error(f"{field} is not canonical JSON")
    return value, payload


def _identity_receipt(value: Mapping[str, Any], schema: str, field: str) -> str:
    if value.get("schema") != schema:
        raise _error(f"{field} schema is invalid")
    identity = _sha(value.get("identity_sha256"), f"{field}.identity_sha256")
    unsigned = dict(value)
    unsigned.pop("identity_sha256", None)
    if _sha256(_canonical_json(unsigned)) != identity:
        raise _error(f"{field} content identity is invalid")
    return identity


def derive_candidate_seed(task_id: str, semantic_id: str, candidate_index: int) -> int:
    """Use the same stable seed contract as ``AnimationFittingOrchestrator``."""
    return derive_browser_candidate_seed(task_id, semantic_id, candidate_index)


def _strict_config(value: str) -> Dict[str, Any]:
    payload = (value or "{}").encode("utf-8")
    if len(payload) > MAX_MANIFEST_BYTES:
        raise _error("fitting job config exceeds the size limit")
    return _strict_object(payload, "fitting job config")


async def _load_job(
    db: AsyncSession,
    job_id: str,
    *,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> _JobSnapshot:
    if db.new or db.dirty or db.deleted:
        raise _error("candidate selection requires a clean database session")
    job_id = _uuid(job_id, "job_id")
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
    rig_type = normalize_rig_type(job.rig_type)
    semantic_id = str(job.semantic_id or "").strip().lower()
    taxonomy_clip(semantic_id)
    if (
        job.status != "review"
        or version.status != "draft"
        or normalize_rig_type(version.rig_type) != rig_type
    ):
        raise _error("candidate selection requires the exact review job and draft library")
    target = _positive_int(job.candidate_target, "candidate_target")
    limit = _positive_int(job.candidate_limit, "candidate_limit")
    if target > limit or limit > 16:
        raise _error("fitting job candidate policy is invalid")
    config = _strict_config(job.config_json)
    ingest, planned_slot_rows = parse_browser_candidate_plan(
        config,
        semantic_id=semantic_id,
        candidate_limit=limit,
        candidate_target=target,
        workflow_name=job.workflow_name,
        workflow_fingerprint=job.workflow_fingerprint,
        prompt_id=job.prompt_id,
        worker_url=job.worker_url,
        trusted_plan_inputs=trusted_plan_inputs,
        trusted_store_root=fitting_jobs_root,
    )
    planned_slots = {
        int(row["candidate_index"]): dict(row) for row in planned_slot_rows
    }
    selection = config.get("browser_candidate_selection")
    mode = (
        "legacy_upload_only"
        if ingest.get("schema") == "autorig.browser-animation-candidate-job-binding.v1"
        else "production"
    )
    if selection is not None:
        selection = _exact_object(
            selection,
            "browser_candidate_selection",
            ("schema", "mode"),
        )
        if selection.get("schema") != SELECTION_CONFIG_SCHEMA:
            raise _error("browser candidate selection config schema is invalid")
        mode = str(selection.get("mode") or "")
    if mode not in (
        "production",
        "canary_single_candidate",
        "legacy_upload_only",
    ):
        raise _error("browser candidate selection mode is invalid")
    if mode == "production" and ingest.get("schema") != "autorig.browser-animation-candidate-job-binding.v2":
        raise _error("production selection requires the canonical V2 server plan")
    if mode == "canary_single_candidate" and (target, limit) != (1, 1):
        raise _error("canary_single_candidate requires target=1 and limit=1")
    task_id = _uuid(ingest.get("task_id"), "browser_candidate_ingest.task_id")
    task_guid = _uuid(ingest.get("task_guid"), "browser_candidate_ingest.task_guid")
    task = (
        await db.execute(select(Task).where(Task.id == task_id))
    ).scalar_one_or_none()
    if (
        task is None
        or str(task.guid or "").strip().lower() != task_guid
        or task.status != "done"
        or str(task.input_type or "").strip().lower() != "animal"
    ):
        raise _error("server-owned plan source task is not the exact completed animal task")
    human_review_lifecycle = {
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
            "template_skeleton_sha256": _sha(
                version.template_skeleton_sha256, "template_skeleton_sha256"
            ),
            "qa_profile_revision": version.qa_profile_revision,
        },
        "task": {
            "id": task_id,
            "guid": task_guid,
            "status": task.status,
            "input_type": str(task.input_type).lower(),
        },
        "ingest_binding": ingest,
    }
    human_review_lifecycle_sha = _sha256(_canonical_json(human_review_lifecycle))
    lifecycle = {
        "job": {
            "id": job.id,
            "status": job.status,
            "library_version_id": version.id,
            "rig_type": rig_type,
            "semantic_id": semantic_id,
            "workflow_name": job.workflow_name,
            "workflow_fingerprint": job.workflow_fingerprint,
            "worker_url": job.worker_url,
            "prompt_id": job.prompt_id,
            "candidate_target": target,
            "candidate_limit": limit,
        },
        "library": {
            "revision": version.revision,
            "status": version.status,
            "template_skeleton_sha256": _sha(
                version.template_skeleton_sha256, "template_skeleton_sha256"
            ),
            "qa_profile_revision": str(version.qa_profile_revision or "").strip(),
        },
        "selection": {
            "mode": mode,
            "task_id": task_id,
            "human_review_lifecycle_binding_sha256": human_review_lifecycle_sha,
            "planned_slots": [
                {"candidate_index": index, "seed": planned_slots[index]["seed"]}
                for index in sorted(planned_slots)
            ],
        },
        "config_sha256": _sha256(_canonical_json(config)),
    }
    if not lifecycle["library"]["qa_profile_revision"]:
        raise _error("library QA profile revision is missing")
    return _JobSnapshot(
        job=job,
        version=version,
        mode=mode,
        task_id=task_id,
        planned_slots=planned_slots,
        lifecycle=lifecycle,
        lifecycle_identity_sha256=_sha256(_canonical_json(lifecycle)),
        human_review_lifecycle_binding_sha256=human_review_lifecycle_sha,
    )


def _selection_root(root: Path, job_id: str, *, create: bool) -> Path:
    return _secure_directory_chain(
        root, root / job_id / "browser-candidate-selection", create=create
    )


def _try_lock(handle) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def _unlock(handle) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def candidate_publication_lock(
    *,
    job_id: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    timeout_seconds: float = LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Cross-process lock shared by admission, closure, and FINAL publication."""
    job_id = _uuid(job_id, "job_id")
    root = _root(fitting_jobs_root)
    selection_root = _selection_root(root, job_id, create=True)
    lock_path = selection_root / ".publication.lock"
    if lock_path.is_symlink():
        raise _error("candidate publication lock must not be a symlink")
    handle = open(lock_path, "a+b")
    try:
        if lock_path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        deadline = time.monotonic() + max(0.05, float(timeout_seconds))
        while not _try_lock(handle):
            if time.monotonic() >= deadline:
                raise _error("candidate publication lock timed out", 503)
            time.sleep(0.025)
        try:
            yield
        finally:
            _unlock(handle)
    finally:
        handle.close()


@asynccontextmanager
async def async_candidate_publication_lock(
    *,
    job_id: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    timeout_seconds: float = LOCK_TIMEOUT_SECONDS,
):
    """Event-loop-safe nonblocking cross-process publication lock."""
    job_id = _uuid(job_id, "job_id")
    root = _root(fitting_jobs_root)
    selection_root = _selection_root(root, job_id, create=True)
    lock_path = selection_root / ".publication.lock"
    if lock_path.is_symlink():
        raise _error("candidate publication lock must not be a symlink")
    handle = open(lock_path, "a+b")
    try:
        if lock_path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        deadline = time.monotonic() + max(0.05, float(timeout_seconds))
        while not _try_lock(handle):
            if time.monotonic() >= deadline:
                raise _error("candidate publication lock timed out", 503)
            await asyncio.sleep(0.025)
        try:
            yield
        finally:
            _unlock(handle)
    finally:
        handle.close()


def _final_path(root: Path, job_id: str) -> Path:
    return root / job_id / "browser-candidate-selection" / "final"


def _closure_path(root: Path, job_id: str) -> Path:
    return root / job_id / "browser-candidate-selection" / "generation-closure"


def assert_candidate_publication_open(
    *, job_id: str, fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT
) -> None:
    """Fail closed when generation is closed or a FINAL receipt already exists.

    The caller must hold :func:`candidate_publication_lock` for the complete
    bundle publication window.
    """
    job_id = _uuid(job_id, "job_id")
    root = _root(fitting_jobs_root)
    if _final_path(root, job_id).exists():
        raise _error("candidate selection is FINAL; late candidates are forbidden")
    if _closure_path(root, job_id).exists():
        raise _error("candidate generation is closed; late candidates are forbidden")


def _manifest_matches_job(
    bundle,
    snapshot: _JobSnapshot,
    expected_index: int,
    expected_slot: Mapping[str, Any],
) -> None:
    manifest = bundle.manifest
    library = manifest.get("library") or {}
    fitting = manifest.get("fitting_job") or {}
    candidate = manifest.get("candidate") or {}
    source_task = manifest.get("source_task") or {}
    controlled = manifest.get("controlled_generation") or {}
    expected_seed = expected_slot["seed"]
    source_video = bundle.artifacts.get("source-video.mp4") or {}
    planned_video = expected_slot.get("source_video") or {}
    lifecycle = snapshot.lifecycle
    if (
        library.get("version_id") != snapshot.version.id
        or library.get("revision") != snapshot.version.revision
        or library.get("rig_type") != lifecycle["job"]["rig_type"]
        or library.get("template_skeleton_sha256")
        != lifecycle["library"]["template_skeleton_sha256"]
        or fitting.get("id") != snapshot.job.id
        or fitting.get("semantic_id") != snapshot.job.semantic_id
        or fitting.get("workflow_name") != snapshot.job.workflow_name
        or fitting.get("workflow_fingerprint") != snapshot.job.workflow_fingerprint
        or source_task.get("id") != snapshot.task_id
        or controlled.get("workflow_fingerprint_sha256")
        != snapshot.job.workflow_fingerprint
        or controlled != expected_slot.get("controlled_generation")
        or source_video.get("sha256") != planned_video.get("sha256")
        or source_video.get("bytes") != planned_video.get("bytes")
        or candidate.get("candidate_index") != expected_index
        or candidate.get("seed") != expected_seed
    ):
        raise _error("candidate manifest differs from the server-planned job slot")


def _receipt_directory(root: Path, job_id: str, family: str, index: int) -> Path:
    return root / job_id / "browser-candidate-selection" / family / f"{index:02d}"


def _publish(root: Path, target: Path, files: Mapping[str, bytes]) -> bool:
    try:
        return _publish_files(root, target, files)
    except CandidateSelectionError:
        raise
    except AnimationLibraryError as exc:
        raise _error(str(exc), getattr(exc, "status_code", 409)) from exc


def _load_admission(root: Path, job_id: str, index: int) -> _Admission:
    directory = _receipt_directory(root, job_id, "admissions", index)
    value, payload = _read_json_file(
        directory / "admission.json", "candidate admission", MAX_ADMISSION_BYTES
    )
    _identity_receipt(value, CANDIDATE_ADMISSION_SCHEMA, "candidate admission")
    if value.get("job_id") != job_id or value.get("candidate_index") != index:
        raise _error("candidate admission job/index binding is invalid")
    _sha(
        value.get("human_review_lifecycle_binding_sha256"),
        "candidate admission human_review_lifecycle_binding_sha256",
    )
    return _Admission(
        directory=directory,
        receipt=value,
        receipt_bytes=payload,
        receipt_pin=_pin_payload(payload, "admission.json"),
    )


def _scan_admissions(root: Path, job_id: str, limit: int) -> list[_Admission]:
    base = root / job_id / "browser-candidate-selection" / "admissions"
    if not base.exists():
        return []
    if base.is_symlink() or not base.is_dir():
        raise _error("candidate admissions directory is invalid")
    result = []
    for entry in sorted(base.iterdir(), key=lambda path: path.name):
        if entry.is_symlink() or not entry.is_dir() or not re.fullmatch(r"\d{2}", entry.name):
            raise _error("candidate admission inventory contains an invalid entry")
        index = int(entry.name)
        if index >= limit or sorted(path.name for path in entry.iterdir()) != ["admission.json"]:
            raise _error("candidate admission inventory is invalid")
        result.append(_load_admission(root, job_id, index))
    indices = [row.receipt["candidate_index"] for row in result]
    seeds = [row.receipt["seed"] for row in result]
    identities = [row.receipt["candidate_identity_sha256"] for row in result]
    if len(indices) != len(set(indices)) or len(seeds) != len(set(seeds)) or len(identities) != len(set(identities)):
        raise _error("candidate admission inventory has duplicate index, seed, or identity")
    return result


def _scan_bundle_identities(root: Path, job_id: str) -> set[str]:
    base = root / job_id / "browser-candidates"
    if not base.exists():
        return set()
    if base.is_symlink() or not base.is_dir():
        raise _error("browser candidate bundle root is invalid")
    result: set[str] = set()
    for shard in base.iterdir():
        if shard.is_symlink() or not shard.is_dir() or not re.fullmatch(r"[0-9a-f]{2}", shard.name):
            raise _error("browser candidate bundle shard inventory is invalid")
        for directory in shard.iterdir():
            if directory.is_symlink() or not directory.is_dir():
                raise _error("browser candidate bundle inventory is invalid")
            identity = _sha(directory.name, "candidate bundle directory identity")
            if identity[:2] != shard.name or identity in result:
                raise _error("browser candidate bundle sharding or uniqueness is invalid")
            result.add(identity)
    return result


def _reconcile_orphaned_bundles_locked(
    root: Path, snapshot: _JobSnapshot
) -> list[_Admission]:
    """Recover a valid rename-before-admission crash under the publication lock."""
    admissions = _scan_admissions(
        root, snapshot.job.id, snapshot.job.candidate_limit
    )
    admitted = {
        row.receipt["candidate_identity_sha256"] for row in admissions
    }
    for identity in sorted(_scan_bundle_identities(root, snapshot.job.id) - admitted):
        bundle = _load_bundle(root, snapshot.job.id, identity)
        candidate = bundle.manifest.get("candidate") or {}
        index = candidate.get("candidate_index")
        if isinstance(index, bool) or not isinstance(index, int):
            raise _error("orphan candidate bundle has no valid planned index")
        _admit_browser_candidate_locked(
            root=root,
            snapshot=snapshot,
            candidate_index=index,
            candidate_identity_sha256=identity,
        )
    return _scan_admissions(root, snapshot.job.id, snapshot.job.candidate_limit)


async def admit_browser_candidate(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_index: int,
    candidate_identity_sha256: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateAdmission:
    """Bind an immutable upload to a server-planned index and derived seed."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    index = _candidate_index(candidate_index, snapshot.job.candidate_limit)
    identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        assert_candidate_publication_open(job_id=job_id, fitting_jobs_root=fitting_jobs_root)
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        return _admit_browser_candidate_locked(
            root=root,
            snapshot=snapshot,
            candidate_index=index,
            candidate_identity_sha256=identity,
        )


def _admit_browser_candidate_locked(
    *,
    root: Path,
    snapshot: _JobSnapshot,
    candidate_index: int,
    candidate_identity_sha256: str,
) -> ImmutableCandidateAdmission:
    """Publish admission while the caller holds ``candidate_publication_lock``."""
    index = _candidate_index(candidate_index, snapshot.job.candidate_limit)
    identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    expected_slot = snapshot.planned_slots.get(index)
    if expected_slot is None:
        raise _error("candidate index is not present in the immutable slot plan")
    expected_seed = expected_slot["seed"]
    bundle = _load_bundle(root, snapshot.job.id, identity)
    _manifest_matches_job(bundle, snapshot, index, expected_slot)
    existing = _scan_admissions(root, snapshot.job.id, snapshot.job.candidate_limit)
    for row in existing:
        if row.receipt["candidate_identity_sha256"] == identity:
            if row.receipt["candidate_index"] != index:
                raise _error("candidate identity is already admitted in another slot")
            return ImmutableCandidateAdmission(
                identity_sha256=row.receipt["identity_sha256"],
                directory=row.directory,
                receipt_path=row.directory / "admission.json",
                receipt_sha256=row.receipt_pin["sha256"],
                receipt=row.receipt,
                created=False,
            )
    binding = {
        "schema": CANDIDATE_ADMISSION_SCHEMA,
        "job_id": snapshot.job.id,
        "candidate_index": index,
        "seed": expected_seed,
        "candidate_identity_sha256": identity,
        "candidate_manifest": bundle.manifest_pin,
        "lifecycle_identity_sha256": snapshot.lifecycle_identity_sha256,
        "human_review_lifecycle_binding_sha256": (
            snapshot.human_review_lifecycle_binding_sha256
        ),
    }
    receipt_identity = _sha256(_canonical_json(binding))
    receipt = {**binding, "identity_sha256": receipt_identity}
    payload = _canonical_json(receipt) + b"\n"
    target = _receipt_directory(root, snapshot.job.id, "admissions", index)
    created = _publish(root, target, {"admission.json": payload})
    return ImmutableCandidateAdmission(
        identity_sha256=receipt_identity,
        directory=target,
        receipt_path=target / "admission.json",
        receipt_sha256=_sha256(payload),
        receipt=receipt,
        created=created,
    )


def _validation_for_admission(
    root: Path, job_id: str, admission: _Admission, validation_identity: str
) -> tuple[Dict[str, Any], Dict[str, Any], bytes]:
    candidate_identity = admission.receipt["candidate_identity_sha256"]
    _, validation, metrics, validation_bytes, _ = _load_server_validation(
        root, job_id, candidate_identity, validation_identity
    )
    if validation.get("schema") != SERVER_VALIDATION_SCHEMA:
        raise _error("candidate server validation schema is invalid")
    if validation.get("candidate", {}).get("manifest") != admission.receipt["candidate_manifest"]:
        raise _error("server validation does not bind the admitted candidate manifest")
    lifecycle = validation.get("lifecycle") or {}
    if (
        lifecycle.get("job_id") != job_id
        or lifecycle.get("binding_sha256")
        != admission.receipt["human_review_lifecycle_binding_sha256"]
    ):
        raise _error("server validation belongs to another fitting job")
    return validation, metrics, validation_bytes


async def record_candidate_validation_outcome(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_identity_sha256: str,
    server_validation_identity_sha256: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateAdmission:
    """Pin a trusted PASS validation; uploaded metrics/rank are never accepted."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    validation_identity = _sha(
        server_validation_identity_sha256, "server_validation_identity_sha256"
    )
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        if _final_path(root, job_id).exists():
            raise _error("candidate selection is FINAL; outcomes are immutable")
        admissions = _scan_admissions(root, job_id, snapshot.job.candidate_limit)
        admission = next(
            (row for row in admissions if row.receipt["candidate_identity_sha256"] == identity),
            None,
        )
        if admission is None:
            raise _error("candidate must be admitted before recording its outcome")
        validation, _, validation_bytes = _validation_for_admission(
            root, job_id, admission, validation_identity
        )
        lifecycle = validation.get("lifecycle") or {}
        if (
            lifecycle.get("library_version_id") != snapshot.version.id
            or lifecycle.get("library_revision") != snapshot.version.revision
            or lifecycle.get("rig_type") != snapshot.job.rig_type
            or lifecycle.get("semantic_id") != snapshot.job.semantic_id
        ):
            raise _error("server validation lifecycle differs from the active job")
        binding = {
            "schema": CANDIDATE_OUTCOME_SCHEMA,
            "job_id": job_id,
            "candidate_index": admission.receipt["candidate_index"],
            "seed": admission.receipt["seed"],
            "candidate_identity_sha256": identity,
            "admission": admission.receipt_pin,
            "status": "VALIDATED_PASS",
            "server_validation": {
                "identity_sha256": validation_identity,
                "receipt": _pin_payload(
                    validation_bytes, "server-validation-receipt.json"
                ),
                "metrics": validation["trusted_qa"]["metrics"],
            },
            "failure": None,
        }
        receipt_identity = _sha256(_canonical_json(binding))
        receipt = {**binding, "identity_sha256": receipt_identity}
        payload = _canonical_json(receipt) + b"\n"
        target = _receipt_directory(
            root, job_id, "outcomes", admission.receipt["candidate_index"]
        )
        created = _publish(root, target, {"outcome.json": payload})
        return ImmutableCandidateAdmission(
            identity_sha256=receipt_identity,
            directory=target,
            receipt_path=target / "outcome.json",
            receipt_sha256=_sha256(payload),
            receipt=receipt,
            created=created,
        )


async def record_candidate_failure_outcome(
    db: AsyncSession,
    *,
    job_id: str,
    candidate_identity_sha256: str,
    failure_code: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateAdmission:
    """Pin a server-authored terminal failure without accepting client metrics."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    code = str(failure_code or "").strip().lower()
    if not SAFE_FAILURE_RE.fullmatch(code):
        raise _error("failure_code is invalid", 400)
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        if _final_path(root, job_id).exists():
            raise _error("candidate selection is FINAL; outcomes are immutable")
        admission = next(
            (
                row
                for row in _scan_admissions(root, job_id, snapshot.job.candidate_limit)
                if row.receipt["candidate_identity_sha256"] == identity
            ),
            None,
        )
        if admission is None:
            raise _error("candidate must be admitted before recording its outcome")
        binding = {
            "schema": CANDIDATE_OUTCOME_SCHEMA,
            "job_id": job_id,
            "candidate_index": admission.receipt["candidate_index"],
            "seed": admission.receipt["seed"],
            "candidate_identity_sha256": identity,
            "admission": admission.receipt_pin,
            "status": "VALIDATED_FAIL",
            "server_validation": None,
            "failure": {"code": code},
        }
        receipt_identity = _sha256(_canonical_json(binding))
        receipt = {**binding, "identity_sha256": receipt_identity}
        payload = _canonical_json(receipt) + b"\n"
        target = _receipt_directory(
            root, job_id, "outcomes", admission.receipt["candidate_index"]
        )
        created = _publish(root, target, {"outcome.json": payload})
        return ImmutableCandidateAdmission(
            identity_sha256=receipt_identity,
            directory=target,
            receipt_path=target / "outcome.json",
            receipt_sha256=_sha256(payload),
            receipt=receipt,
            created=created,
        )


def _load_outcome(root: Path, job_id: str, admission: _Admission) -> _Outcome | None:
    index = admission.receipt["candidate_index"]
    directory = _receipt_directory(root, job_id, "outcomes", index)
    if not directory.exists():
        return None
    if sorted(path.name for path in directory.iterdir()) != ["outcome.json"]:
        raise _error("candidate outcome inventory is invalid")
    value, payload = _read_json_file(
        directory / "outcome.json", "candidate outcome", MAX_ADMISSION_BYTES
    )
    _identity_receipt(value, CANDIDATE_OUTCOME_SCHEMA, "candidate outcome")
    if (
        value.get("job_id") != job_id
        or value.get("candidate_index") != index
        or value.get("seed") != admission.receipt["seed"]
        or value.get("candidate_identity_sha256")
        != admission.receipt["candidate_identity_sha256"]
        or value.get("admission") != admission.receipt_pin
    ):
        raise _error("candidate outcome admission binding is invalid")
    status = value.get("status")
    if status == "VALIDATED_PASS":
        server = _exact_object(
            value.get("server_validation"),
            "candidate outcome server_validation",
            ("identity_sha256", "receipt", "metrics"),
        )
        validation_identity = _sha(
            server.get("identity_sha256"), "server_validation.identity_sha256"
        )
        validation, _, validation_bytes = _validation_for_admission(
            root, job_id, admission, validation_identity
        )
        if (
            server.get("receipt")
            != _pin_payload(validation_bytes, "server-validation-receipt.json")
            or server.get("metrics") != validation["trusted_qa"]["metrics"]
            or value.get("failure") is not None
        ):
            raise _error("candidate outcome trusted validation pins drifted")
    elif status == "VALIDATED_FAIL":
        failure = value.get("failure")
        if (
            value.get("server_validation") is not None
            or not isinstance(failure, dict)
            or set(failure) != {"code"}
            or not SAFE_FAILURE_RE.fullmatch(str(failure.get("code") or ""))
        ):
            raise _error("candidate failure outcome is invalid")
    else:
        raise _error("candidate outcome status is invalid")
    return _Outcome(
        directory=directory,
        receipt=value,
        receipt_bytes=payload,
        receipt_pin=_pin_payload(payload, "outcome.json"),
    )


def _contracts() -> Dict[str, Any]:
    taxonomy_bytes = TAXONOMY_PATH.read_bytes()
    qa_bytes = QA_PROFILE_PATH.read_bytes()
    taxonomy = json.loads(taxonomy_bytes.decode("utf-8-sig"))
    qa = json.loads(qa_bytes.decode("utf-8-sig"))
    weights = qa.get("ranking_weights_object")
    hard = qa.get("hard_gate_metric_keys_array")
    loop_hard = qa.get("loop_hard_gate_metric_keys_array")
    if (
        taxonomy.get("schema") != "animal-animation-taxonomy.v1"
        or not isinstance(taxonomy.get("revision"), str)
        or qa.get("schema") != "autorig.animation-fitting-qa.v1"
        or not isinstance(weights, dict)
        or not isinstance(hard, list)
        or not isinstance(loop_hard, list)
    ):
        raise _error("taxonomy or QA ranking contract is invalid")
    parsed_weights = {str(key): float(value) for key, value in weights.items()}
    if (
        any(not math.isfinite(value) or value <= 0 for value in parsed_weights.values())
        or abs(sum(parsed_weights.values()) - 1.0) > 1e-6
    ):
        raise _error("QA ranking weights are invalid")
    return {
        "taxonomy": {
            "schema": taxonomy["schema"],
            "revision": taxonomy["revision"],
            "filename": TAXONOMY_PATH.name,
            "bytes": len(taxonomy_bytes),
            "sha256": _sha256(taxonomy_bytes),
        },
        "qa_profile": {
            "schema": qa["schema"],
            "calibration_state": str(qa.get("calibration_state_string") or ""),
            "filename": QA_PROFILE_PATH.name,
            "bytes": len(qa_bytes),
            "sha256": _sha256(qa_bytes),
        },
        "ranking": {
            "top_k": TOP_K,
            "hard_gate_metric_keys": [str(item) for item in hard],
            "loop_hard_gate_metric_keys": [str(item) for item in loop_hard],
            "weights": parsed_weights,
            "missing_metric_policy": "reject",
            "score_round_digits": SCORE_ROUND_DIGITS,
            "tie_break": [
                "score_desc",
                "candidate_index_asc",
                "candidate_identity_sha256_asc",
            ],
        },
    }


def _rank_metrics(
    metrics: Mapping[str, Any], *, is_loop: bool, ranking: Mapping[str, Any]
) -> Dict[str, Any]:
    gates = list(ranking["hard_gate_metric_keys"])
    if is_loop:
        gates.extend(ranking["loop_hard_gate_metric_keys"])
    failed_gates = [key for key in gates if metrics.get(key) is not True]
    missing_gates = [key for key in gates if key not in metrics]
    weights = dict(ranking["weights"])
    if not is_loop:
        weights.pop("loop_seam_float", None)
    missing_metrics = []
    components: Dict[str, Any] = {}
    weight_total = sum(weights.values()) or 1.0
    score = 0.0
    for key, weight in weights.items():
        value = metrics.get(key)
        if (
            key not in metrics
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0.0 <= float(value) <= 1.0
        ):
            missing_metrics.append(key)
            continue
        normalized_weight = float(weight) / weight_total
        contribution = float(value) * normalized_weight
        components[key] = {
            "value": float(value),
            "weight": round(normalized_weight, 12),
            "contribution": round(contribution, 12),
        }
        score += contribution
    complete = not missing_gates and not missing_metrics
    return {
        "eligible": complete and not failed_gates,
        "failed_gates": failed_gates,
        "missing_metric_keys": sorted(set((*missing_gates, *missing_metrics))),
        "components": components,
        "score": round(score, SCORE_ROUND_DIGITS) if complete else None,
        "rank": None,
        "provisional_order": None,
    }


def _closure(root: Path, job_id: str, admissions: Sequence[_Admission]) -> Dict[str, Any] | None:
    directory = _closure_path(root, job_id)
    if not directory.exists():
        return None
    if sorted(path.name for path in directory.iterdir()) != ["generation-closure.json"]:
        raise _error("candidate generation closure inventory is invalid")
    value, _ = _read_json_file(
        directory / "generation-closure.json",
        "candidate generation closure",
        MAX_ADMISSION_BYTES,
    )
    _identity_receipt(
        value, CANDIDATE_GENERATION_CLOSURE_SCHEMA, "candidate generation closure"
    )
    expected = [row.receipt_pin for row in admissions]
    lifecycle = value.get("lifecycle")
    if (
        not isinstance(lifecycle, dict)
        or _sha256(_canonical_json(lifecycle))
        != value.get("lifecycle_identity_sha256")
    ):
        raise _error("candidate generation closure lifecycle pin drifted")
    if value.get("job_id") != job_id or value.get("admissions") != expected:
        raise _error("candidate generation closure admission inventory drifted")
    return value


async def close_candidate_generation(
    db: AsyncSession,
    *,
    job_id: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateAdmission:
    """Immutably declare that no more candidate slots will be admitted."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        if _final_path(root, job_id).exists():
            raise _error("candidate selection is already FINAL")
        admissions = _reconcile_orphaned_bundles_locked(root, snapshot)
        bundle_ids = _scan_bundle_identities(root, job_id)
        admitted_ids = {row.receipt["candidate_identity_sha256"] for row in admissions}
        if bundle_ids != admitted_ids:
            raise _error("all uploaded bundles must be server-admitted before closure")
        binding = {
            "schema": CANDIDATE_GENERATION_CLOSURE_SCHEMA,
            "job_id": job_id,
            "lifecycle_identity_sha256": snapshot.lifecycle_identity_sha256,
            "lifecycle": snapshot.lifecycle,
            "admissions": [row.receipt_pin for row in admissions],
        }
        identity = _sha256(_canonical_json(binding))
        receipt = {**binding, "identity_sha256": identity}
        payload = _canonical_json(receipt) + b"\n"
        target = _closure_path(root, job_id)
        created = _publish(root, target, {"generation-closure.json": payload})
        return ImmutableCandidateAdmission(
            identity_sha256=identity,
            directory=target,
            receipt_path=target / "generation-closure.json",
            receipt_sha256=_sha256(payload),
            receipt=receipt,
            created=created,
        )


def _build_receipt(
    root: Path,
    snapshot: _JobSnapshot,
    *,
    state: str,
    finalized_by: str | None = None,
) -> Dict[str, Any]:
    contracts = _contracts()
    admissions = _scan_admissions(root, snapshot.job.id, snapshot.job.candidate_limit)
    bundle_ids = _scan_bundle_identities(root, snapshot.job.id)
    admitted_ids = {row.receipt["candidate_identity_sha256"] for row in admissions}
    if bundle_ids != admitted_ids:
        raise _error("selection inventory contains uploaded but unadmitted candidates")
    closure = _closure(root, snapshot.job.id, admissions)
    action = taxonomy_clip(snapshot.job.semantic_id)
    candidates = []
    eligible = []
    terminal_count = 0
    pending_count = 0
    for admission in admissions:
        index = admission.receipt["candidate_index"]
        identity = admission.receipt["candidate_identity_sha256"]
        expected_slot = snapshot.planned_slots.get(index)
        if expected_slot is None:
            raise _error("candidate admission references an unplanned slot")
        expected_seed = expected_slot["seed"]
        if (
            admission.receipt["seed"] != expected_seed
            or admission.receipt["lifecycle_identity_sha256"]
            != snapshot.lifecycle_identity_sha256
            or admission.receipt["human_review_lifecycle_binding_sha256"]
            != snapshot.human_review_lifecycle_binding_sha256
        ):
            raise _error("candidate admission is stale relative to the fitting job")
        bundle = _load_bundle(root, snapshot.job.id, identity)
        _manifest_matches_job(bundle, snapshot, index, expected_slot)
        if bundle.manifest_pin != admission.receipt["candidate_manifest"]:
            raise _error("candidate admission manifest pin drifted")
        outcome = _load_outcome(root, snapshot.job.id, admission)
        if outcome is None:
            pending_count += 1
            server_outcome = {
                "status": "PENDING",
                "receipt": None,
                "validation_identity_sha256": None,
                "validation_receipt": None,
                "metrics": None,
                "failure": None,
            }
            ranking = {
                "eligible": False,
                "failed_gates": [],
                "missing_metric_keys": [],
                "components": {},
                "score": None,
                "rank": None,
                "provisional_order": None,
            }
        else:
            terminal_count += 1
            outcome_value = outcome.receipt
            status = outcome_value["status"]
            server = outcome_value.get("server_validation")
            if status == "VALIDATED_PASS":
                validation_identity = server["identity_sha256"]
                _, metrics, _ = _validation_for_admission(
                    root, snapshot.job.id, admission, validation_identity
                )
                ranking = _rank_metrics(
                    metrics,
                    is_loop=bool(action.get("loop")),
                    ranking=contracts["ranking"],
                )
            else:
                validation_identity = None
                ranking = {
                    "eligible": False,
                    "failed_gates": [outcome_value["failure"]["code"]],
                    "missing_metric_keys": [],
                    "components": {},
                    "score": None,
                    "rank": None,
                    "provisional_order": None,
                }
            server_outcome = {
                "status": status,
                "receipt": outcome.receipt_pin,
                "validation_identity_sha256": validation_identity,
                "validation_receipt": server.get("receipt") if server else None,
                "metrics": server.get("metrics") if server else None,
                "failure": outcome_value.get("failure"),
            }
        row = {
            "candidate_index": index,
            "seed": expected_seed,
            "candidate_identity_sha256": identity,
            "candidate_manifest": bundle.manifest_pin,
            "admission": admission.receipt_pin,
            "human_review_lifecycle_binding_sha256": admission.receipt[
                "human_review_lifecycle_binding_sha256"
            ],
            "server_outcome": server_outcome,
            "human_review": None,
            "ranking": ranking,
        }
        candidates.append(row)
        if ranking["eligible"]:
            eligible.append(row)
    candidates.sort(key=lambda row: row["candidate_index"])
    eligible.sort(
        key=lambda row: (
            -row["ranking"]["score"],
            row["candidate_index"],
            row["candidate_identity_sha256"],
        )
    )
    for order, row in enumerate(eligible, 1):
        row["ranking"]["provisional_order"] = order
    # OPEN receipts deliberately pin the complete immutable review state.  A
    # review arriving after an operator captured OPEN therefore changes the
    # OPEN identity and makes expected_snapshot CAS fail closed at FINAL.
    for row in candidates:
        if row["server_outcome"]["status"] == "VALIDATED_PASS":
            row["human_review"] = _human_review_summary(
                root, snapshot.job.id, row
            )
    required_eligible = 1 if snapshot.mode == "canary_single_candidate" else TOP_K
    admitted_count = len(admissions)
    target_satisfied = admitted_count >= snapshot.job.candidate_target
    top_k_satisfied = len(eligible) >= required_eligible
    generation_closed = closure is not None
    selected = (
        eligible[:1]
        if state == "FINAL" and snapshot.mode == "canary_single_candidate"
        else []
    )
    if state == "FINAL":
        if admitted_count > snapshot.job.candidate_limit:
            raise _error("candidate_limit is exceeded")
        if not target_satisfied:
            raise _error("candidate_target is not satisfied")
        if pending_count:
            raise _error("pending candidates block FINAL selection")
        if not top_k_satisfied:
            raise _error("insufficient eligible candidates block FINAL selection")
        if not generation_closed:
            raise _error("server generation closure is required for FINAL selection")
        missing = sorted(
            {
                key
                for row in candidates
                if row["server_outcome"]["status"] == "VALIDATED_PASS"
                for key in row["ranking"]["missing_metric_keys"]
            }
        )
        if missing:
            raise _error(
                "trusted ranking metrics are incomplete: " + ", ".join(missing)
            )
        if (
            snapshot.mode == "production"
            and "provisional" in contracts["qa_profile"]["calibration_state"].lower()
        ):
            raise _error("provisional QA calibration blocks production FINAL")
        if snapshot.mode == "production":
            selected = []
            for row in eligible:
                review_summary = row["human_review"]
                if review_summary is None:
                    raise _error(
                        "pending human review blocks deterministic FINAL selection"
                    )
                decision = review_summary["decision"]
                if decision == "PASS":
                    selected.append(row)
                if len(selected) == TOP_K:
                    break
            if len(selected) != TOP_K:
                raise _error("fewer than three immutable human PASS reviews")
            for rank, row in enumerate(selected, 1):
                row["ranking"]["rank"] = rank
            top_k_satisfied = True
        elif selected:
            selected[0]["ranking"]["rank"] = 1
    candidate_set = [
        {
            "candidate_index": row["candidate_index"],
            "seed": row["seed"],
            "candidate_identity_sha256": row["candidate_identity_sha256"],
            "admission": row["admission"],
            "outcome": row["server_outcome"]["receipt"],
            "human_review": row["human_review"],
        }
        for row in candidates
    ]
    job_value = {
        "id": snapshot.job.id,
        "library_version_id": snapshot.version.id,
        "library_revision": snapshot.version.revision,
        "rig_type": snapshot.job.rig_type,
        "semantic_id": snapshot.job.semantic_id,
        "workflow_name": snapshot.job.workflow_name,
        "workflow_fingerprint": snapshot.job.workflow_fingerprint,
        "worker_url": snapshot.job.worker_url,
        "prompt_id": snapshot.job.prompt_id,
        "candidate_target": snapshot.job.candidate_target,
        "candidate_limit": snapshot.job.candidate_limit,
        "lifecycle_identity_sha256": snapshot.lifecycle_identity_sha256,
        "human_review_lifecycle_binding_sha256": (
            snapshot.human_review_lifecycle_binding_sha256
        ),
    }
    comparative = state == "FINAL" and snapshot.mode == "production"
    production_eligible = comparative and all(
        row["human_review"] is not None for row in selected
    )
    finalization_reason = (
        "open_generation"
        if state == "OPEN"
        else (
            "canary_single_candidate"
            if snapshot.mode == "canary_single_candidate"
            else "target_and_top_k_satisfied"
        )
    )
    binding = {
        "schema": CANDIDATE_SELECTION_SCHEMA,
        "state": state,
        "mode": snapshot.mode,
        "job": job_value,
        "contracts": contracts,
        "inventory": {
            "candidate_set_sha256": _sha256(_canonical_json(candidate_set)),
            "admitted_count": admitted_count,
            "terminal_count": terminal_count,
            "eligible_count": len(eligible),
            "pending_count": pending_count,
            "candidate_target_satisfied": target_satisfied,
            "top_k_satisfied": top_k_satisfied,
            "generation_closed": generation_closed,
            "generation_closure_identity_sha256": (
                closure["identity_sha256"] if closure else None
            ),
        },
        "candidates": candidates,
        "selection": {
            "top_candidate_identity_sha256": (
                selected[0]["candidate_identity_sha256"]
                if state == "FINAL" and selected
                else None
            ),
            "top_k_candidate_identity_sha256": (
                [row["candidate_identity_sha256"] for row in selected]
                if state == "FINAL"
                else []
            ),
            "provisional_order_candidate_identity_sha256": [
                row["candidate_identity_sha256"] for row in eligible
            ],
            "comparative_selection": comparative,
            "production_eligible": production_eligible,
            "finalization_reason": finalization_reason,
            "finalized_by": finalized_by if state == "FINAL" else None,
        },
    }
    identity = _sha256(_canonical_json(binding))
    return {**binding, "identity_sha256": identity}


async def create_candidate_selection_snapshot(
    db: AsyncSession,
    *,
    job_id: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateSelectionReceipt:
    """Publish a content-addressed OPEN view of the complete admitted inventory."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        if _final_path(root, job_id).exists():
            raise _error("candidate selection is already FINAL")
        receipt = _build_receipt(root, snapshot, state="OPEN")
        payload = _canonical_json(receipt) + b"\n"
        target = (
            root
            / job_id
            / "browser-candidate-selection"
            / "snapshots"
            / receipt["identity_sha256"]
        )
        created = _publish(root, target, {"selection-receipt.json": payload})
        return ImmutableCandidateSelectionReceipt(
            identity_sha256=receipt["identity_sha256"],
            directory=target,
            receipt_path=target / "selection-receipt.json",
            receipt_sha256=_sha256(payload),
            receipt=receipt,
            created=created,
        )


async def finalize_candidate_selection(
    db: AsyncSession,
    *,
    job_id: str,
    admin_email: str,
    expected_snapshot_identity_sha256: str | None = None,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> ImmutableCandidateSelectionReceipt:
    """Publish exactly one FINAL receipt after all fail-closed gates pass."""
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    root = _root(fitting_jobs_root)
    admin = str(admin_email or "")
    if not admin.strip() or admin != admin.strip() or len(admin) > 320:
        raise _error("admin_email is invalid", 400)
    expected = (
        _sha(expected_snapshot_identity_sha256, "expected_snapshot_identity_sha256")
        if expected_snapshot_identity_sha256 is not None
        else None
    )
    async with async_candidate_publication_lock(
        job_id=job_id, fitting_jobs_root=fitting_jobs_root
    ):
        await db.rollback()
        snapshot = await _load_job(
            db,
            job_id,
            fitting_jobs_root=fitting_jobs_root,
            trusted_plan_inputs=trusted_plan_inputs,
        )
        final_path = _final_path(root, job_id)
        if final_path.exists():
            receipt = verify_candidate_selection_receipt(
                job_id=job_id,
                selection_identity_sha256=_read_json_file(
                    final_path / "selection-receipt.json",
                    "FINAL candidate selection",
                    MAX_RECEIPT_BYTES,
                )[0]["identity_sha256"],
                fitting_jobs_root=fitting_jobs_root,
            )
            if receipt["selection"]["finalized_by"] != admin:
                raise _error("FINAL selection is already pinned to another admin")
            payload = _read_bounded_file(
                final_path / "selection-receipt.json",
                "FINAL candidate selection",
                MAX_RECEIPT_BYTES,
            )
            return ImmutableCandidateSelectionReceipt(
                identity_sha256=receipt["identity_sha256"],
                directory=final_path,
                receipt_path=final_path / "selection-receipt.json",
                receipt_sha256=_sha256(payload),
                receipt=receipt,
                created=False,
            )
        open_receipt = _build_receipt(root, snapshot, state="OPEN")
        if expected is not None and open_receipt["identity_sha256"] != expected:
            raise _error("OPEN snapshot changed before FINAL publication")
        receipt = _build_receipt(root, snapshot, state="FINAL", finalized_by=admin)
        payload = _canonical_json(receipt) + b"\n"
        created = _publish(root, final_path, {"selection-receipt.json": payload})
        return ImmutableCandidateSelectionReceipt(
            identity_sha256=receipt["identity_sha256"],
            directory=final_path,
            receipt_path=final_path / "selection-receipt.json",
            receipt_sha256=_sha256(payload),
            receipt=receipt,
            created=created,
        )


def _recompute_receipt_semantics(
    value: Mapping[str, Any], root: Path, job_id: str
) -> tuple[list[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """Rebuild all rank/counter/selection fields from immutable leaf receipts."""
    contracts = value["contracts"]
    job = value.get("job") or {}
    state = value.get("state")
    mode = value.get("mode")
    action = taxonomy_clip(job.get("semantic_id"))
    source_rows = value.get("candidates")
    if not isinstance(source_rows, list):
        raise _error("candidate selection inventory is invalid")
    rows: list[Dict[str, Any]] = []
    eligible: list[Dict[str, Any]] = []
    terminal_count = 0
    pending_count = 0
    for source_row in source_rows:
        index = source_row.get("candidate_index")
        admission = _load_admission(root, job_id, index)
        identity = admission.receipt["candidate_identity_sha256"]
        bundle = _load_bundle(root, job_id, identity)
        outcome = _load_outcome(root, job_id, admission)
        if outcome is None:
            status = "PENDING"
            pending_count += 1
            server_outcome = {
                "status": "PENDING",
                "receipt": None,
                "validation_identity_sha256": None,
                "validation_receipt": None,
                "metrics": None,
                "failure": None,
            }
            ranking = {
                "eligible": False,
                "failed_gates": [],
                "missing_metric_keys": [],
                "components": {},
                "score": None,
                "rank": None,
                "provisional_order": None,
            }
        else:
            status = outcome.receipt.get("status")
            terminal_count += 1
            outcome_value = outcome.receipt
            server = outcome_value.get("server_validation")
            if status == "VALIDATED_PASS":
                validation_identity = server["identity_sha256"]
                _, metrics, _ = _validation_for_admission(
                    root, job_id, admission, validation_identity
                )
                ranking = _rank_metrics(
                    metrics,
                    is_loop=bool(action.get("loop")),
                    ranking=contracts["ranking"],
                )
            elif status == "VALIDATED_FAIL":
                validation_identity = None
                ranking = {
                    "eligible": False,
                    "failed_gates": [outcome_value["failure"]["code"]],
                    "missing_metric_keys": [],
                    "components": {},
                    "score": None,
                    "rank": None,
                    "provisional_order": None,
                }
            else:
                raise _error("candidate selection server outcome status is invalid")
            server_outcome = {
                "status": status,
                "receipt": outcome.receipt_pin,
                "validation_identity_sha256": validation_identity,
                "validation_receipt": server.get("receipt") if server else None,
                "metrics": server.get("metrics") if server else None,
                "failure": outcome_value.get("failure"),
            }
        row = {
            "candidate_index": index,
            "seed": admission.receipt["seed"],
            "candidate_identity_sha256": identity,
            "candidate_manifest": bundle.manifest_pin,
            "admission": admission.receipt_pin,
            "human_review_lifecycle_binding_sha256": admission.receipt[
                "human_review_lifecycle_binding_sha256"
            ],
            "server_outcome": server_outcome,
            "human_review": None,
            "ranking": ranking,
        }
        rows.append(row)
        if ranking["eligible"]:
            eligible.append(row)
    rows.sort(key=lambda row: row["candidate_index"])
    eligible.sort(
        key=lambda row: (
            -row["ranking"]["score"],
            row["candidate_index"],
            row["candidate_identity_sha256"],
        )
    )
    for order, row in enumerate(eligible, 1):
        row["ranking"]["provisional_order"] = order
    for row in rows:
        if row["server_outcome"]["status"] == "VALIDATED_PASS":
            row["human_review"] = _human_review_summary(root, job_id, row)
    selected: list[Dict[str, Any]] = []
    if state == "FINAL" and mode == "production":
        for row in eligible:
            summary = row["human_review"]
            if summary is None:
                raise _error("FINAL receipt references a pending human review")
            decision = summary["decision"]
            if decision == "PASS":
                selected.append(row)
            if len(selected) == TOP_K:
                break
        if len(selected) != TOP_K:
            raise _error("FINAL receipt has fewer than three human PASS reviews")
        for rank, row in enumerate(selected, 1):
            row["ranking"]["rank"] = rank
    elif state == "FINAL" and mode == "canary_single_candidate":
        selected = eligible[:1]
        if selected:
            selected[0]["ranking"]["rank"] = 1
    candidate_set = [
        {
            "candidate_index": row["candidate_index"],
            "seed": row["seed"],
            "candidate_identity_sha256": row["candidate_identity_sha256"],
            "admission": row["admission"],
            "outcome": row["server_outcome"]["receipt"],
            "human_review": row["human_review"],
        }
        for row in rows
    ]
    all_admissions = _scan_admissions(root, job_id, 16)
    admission_identities = {
        row.receipt["candidate_identity_sha256"] for row in all_admissions
    }
    row_identities = {row["candidate_identity_sha256"] for row in rows}
    bundle_identities = _scan_bundle_identities(root, job_id)
    if row_identities != admission_identities or bundle_identities != admission_identities:
        raise _error(
            "candidate selection rows, admissions, and immutable bundle inventory differ"
        )
    closure = _closure(root, job_id, all_admissions)
    closed = closure is not None
    if closure is not None:
        lifecycle = closure["lifecycle"]
        lifecycle_job = lifecycle.get("job") or {}
        lifecycle_library = lifecycle.get("library") or {}
        lifecycle_selection = lifecycle.get("selection") or {}
        expected_job_fields = {
            "id": lifecycle_job.get("id"),
            "library_version_id": lifecycle_job.get("library_version_id"),
            "library_revision": lifecycle_library.get("revision"),
            "rig_type": lifecycle_job.get("rig_type"),
            "semantic_id": lifecycle_job.get("semantic_id"),
            "workflow_name": lifecycle_job.get("workflow_name"),
            "workflow_fingerprint": lifecycle_job.get("workflow_fingerprint"),
            "worker_url": lifecycle_job.get("worker_url"),
            "prompt_id": lifecycle_job.get("prompt_id"),
            "candidate_target": lifecycle_job.get("candidate_target"),
            "candidate_limit": lifecycle_job.get("candidate_limit"),
            "lifecycle_identity_sha256": closure[
                "lifecycle_identity_sha256"
            ],
            "human_review_lifecycle_binding_sha256": lifecycle_selection.get(
                "human_review_lifecycle_binding_sha256"
            ),
        }
        if job != expected_job_fields or mode != lifecycle_selection.get("mode"):
            raise _error("candidate selection job differs from immutable closure lifecycle")
        target = _positive_int(
            lifecycle_job.get("candidate_target"), "closure candidate_target"
        )
        limit = _positive_int(
            lifecycle_job.get("candidate_limit"), "closure candidate_limit"
        )
    else:
        target = _positive_int(job.get("candidate_target"), "receipt candidate_target")
        limit = _positive_int(job.get("candidate_limit"), "receipt candidate_limit")
    required_eligible = 1 if mode == "canary_single_candidate" else TOP_K
    if len(all_admissions) > limit:
        raise _error("immutable admission inventory exceeds candidate_limit")
    closure_identity = closure["identity_sha256"] if closure is not None else None
    inventory = {
        "candidate_set_sha256": _sha256(_canonical_json(candidate_set)),
        "admitted_count": len(rows),
        "terminal_count": terminal_count,
        "eligible_count": len(eligible),
        "pending_count": pending_count,
        "candidate_target_satisfied": len(rows) >= target,
        "top_k_satisfied": (
            len(selected) == TOP_K
            if state == "FINAL" and mode == "production"
            else len(eligible) >= required_eligible
        ),
        "generation_closed": closed,
        "generation_closure_identity_sha256": closure_identity,
    }
    comparative = state == "FINAL" and mode == "production"
    selection = {
        "top_candidate_identity_sha256": (
            selected[0]["candidate_identity_sha256"]
            if state == "FINAL" and selected
            else None
        ),
        "top_k_candidate_identity_sha256": (
            [row["candidate_identity_sha256"] for row in selected]
            if state == "FINAL"
            else []
        ),
        "provisional_order_candidate_identity_sha256": [
            row["candidate_identity_sha256"] for row in eligible
        ],
        "comparative_selection": comparative,
        "production_eligible": comparative and len(selected) == TOP_K,
        "finalization_reason": (
            "open_generation"
            if state == "OPEN"
            else (
                "canary_single_candidate"
                if mode == "canary_single_candidate"
                else "target_and_top_k_satisfied"
            )
        ),
        "finalized_by": (
            (value.get("selection") or {}).get("finalized_by")
            if state == "FINAL"
            else None
        ),
    }
    if state == "FINAL":
        if not closed:
            raise _error("FINAL selection requires immutable generation closure")
        if inventory["candidate_target_satisfied"] is not True:
            raise _error("FINAL selection does not satisfy candidate_target")
        if pending_count:
            raise _error("FINAL selection contains pending candidate outcomes")
        missing = sorted(
            {
                key
                for row in rows
                if row["server_outcome"]["status"] == "VALIDATED_PASS"
                for key in row["ranking"]["missing_metric_keys"]
            }
        )
        if missing:
            raise _error(
                "FINAL selection trusted ranking metrics are incomplete: "
                + ", ".join(missing)
            )
        if mode == "production":
            if "provisional" in contracts["qa_profile"][
                "calibration_state"
            ].lower():
                raise _error("provisional QA calibration blocks production FINAL")
            if len(selected) != TOP_K or selection["production_eligible"] is not True:
                raise _error("production FINAL requires three immutable human PASS reviews")
        elif mode == "canary_single_candidate":
            if (
                target != 1
                or limit != 1
                or len(selected) != 1
                or selection["production_eligible"] is not False
            ):
                raise _error("canary FINAL invariants are invalid")
        else:
            raise _error("FINAL selection mode is invalid")
    return rows, inventory, selection


def verify_candidate_selection_receipt(
    *,
    job_id: str,
    selection_identity_sha256: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
) -> Dict[str, Any]:
    """Re-open receipt plus every manifest/validation/metric pin it references."""
    job_id = _uuid(job_id, "job_id")
    identity = _sha(selection_identity_sha256, "selection_identity_sha256")
    root = _root(fitting_jobs_root)
    final = _final_path(root, job_id)
    paths = [
        final,
        root
        / job_id
        / "browser-candidate-selection"
        / "snapshots"
        / identity,
    ]
    directory = None
    value = None
    payload = None
    for candidate_path in paths:
        receipt_path = candidate_path / "selection-receipt.json"
        if receipt_path.exists():
            candidate_value, candidate_payload = _read_json_file(
                receipt_path, "candidate selection receipt", MAX_RECEIPT_BYTES
            )
            if candidate_value.get("identity_sha256") == identity:
                directory = candidate_path
                value = candidate_value
                payload = candidate_payload
                break
    if directory is None or value is None or payload is None:
        raise _error("candidate selection receipt not found", 404)
    _identity_receipt(value, CANDIDATE_SELECTION_SCHEMA, "candidate selection receipt")
    if value.get("job", {}).get("id") != job_id:
        raise _error("candidate selection receipt belongs to another job")
    current_contracts = _contracts()
    if value.get("contracts") != current_contracts:
        raise _error("candidate selection taxonomy or QA policy pin drifted")
    rows = value.get("candidates")
    if not isinstance(rows, list):
        raise _error("candidate selection inventory is invalid")
    seen_indices: set[int] = set()
    seen_seeds: set[int] = set()
    seen_identities: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise _error("candidate selection row is invalid")
        index = row.get("candidate_index")
        seed = row.get("seed")
        candidate_identity = _sha(
            row.get("candidate_identity_sha256"), "candidate identity"
        )
        if index in seen_indices or seed in seen_seeds or candidate_identity in seen_identities:
            raise _error("candidate selection has duplicate index, seed, or identity")
        seen_indices.add(index)
        seen_seeds.add(seed)
        seen_identities.add(candidate_identity)
        admission = _load_admission(root, job_id, index)
        bundle = _load_bundle(root, job_id, candidate_identity)
        if (
            row.get("admission") != admission.receipt_pin
            or row.get("candidate_manifest") != bundle.manifest_pin
            or admission.receipt["candidate_identity_sha256"] != candidate_identity
            or row.get("human_review_lifecycle_binding_sha256")
            != admission.receipt["human_review_lifecycle_binding_sha256"]
        ):
            raise _error("candidate selection admission/manifest pin drifted")
        outcome = _load_outcome(root, job_id, admission)
        pinned_outcome = row.get("server_outcome", {}).get("receipt")
        expected_outcome = outcome.receipt_pin if outcome is not None else None
        if pinned_outcome != expected_outcome:
            raise _error("candidate selection outcome pin drifted")
    state = value.get("state")
    current_identities = _scan_bundle_identities(root, job_id)
    all_admissions = _scan_admissions(root, job_id, 16)
    admission_identities = {
        row.receipt["candidate_identity_sha256"] for row in all_admissions
    }
    if (
        seen_identities != admission_identities
        or current_identities != admission_identities
    ):
        raise _error(
            "candidate selection rows, admissions, and immutable bundle inventory differ"
        )
    if state == "FINAL":
        if directory != final:
            raise _error("FINAL selection is not in the canonical final slot")
        if (
            value.get("selection", {}).get("production_eligible")
            and value.get("mode") != "production"
        ):
            raise _error("non-production selection cannot be production eligible")
    elif state != "OPEN":
        raise _error("candidate selection state is invalid")
    expected_rows, expected_inventory, expected_selection = _recompute_receipt_semantics(
        value, root, job_id
    )
    if (
        value.get("candidates") != expected_rows
        or value.get("inventory") != expected_inventory
        or value.get("selection") != expected_selection
    ):
        raise _error("candidate selection semantic content is not server-derived")
    return value


def assert_production_selection(
    *,
    job_id: str,
    selection_identity_sha256: str,
    candidate_identity_sha256: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
) -> Dict[str, Any]:
    """Approval/activation adapter gate: only a FINAL production top-3 may pass."""
    receipt = verify_candidate_selection_receipt(
        job_id=job_id,
        selection_identity_sha256=selection_identity_sha256,
        fitting_jobs_root=fitting_jobs_root,
    )
    candidate_identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    selection = receipt.get("selection") or {}
    if (
        receipt.get("state") != "FINAL"
        or receipt.get("mode") != "production"
        or selection.get("comparative_selection") is not True
        or selection.get("production_eligible") is not True
        or candidate_identity not in selection.get("top_k_candidate_identity_sha256", [])
    ):
        raise _error("candidate is not in a production-eligible FINAL top-3")
    return receipt


def _load_human_review_decision(
    root: Path,
    job_id: str,
    row: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]] | None:
    candidate_identity = row["candidate_identity_sha256"]
    directory = (
        root
        / job_id
        / "browser-candidate-reviews"
        / candidate_identity[:2]
        / candidate_identity
        / "human-review"
    )
    if not directory.exists():
        return None
    names = sorted(path.name for path in directory.iterdir())
    if "human-review-receipt.json" not in names:
        raise _error("human review directory has no immutable receipt")
    review, review_bytes = _read_json_file(
        directory / "human-review-receipt.json",
        "human review receipt",
        MAX_RECEIPT_BYTES,
    )
    from animation_fitting_candidate_review import (
        HUMAN_REVIEW_SCHEMA,
    )

    _identity_receipt(review, HUMAN_REVIEW_SCHEMA, "human review receipt")
    review_pin = _pin_payload(review_bytes, "human-review-receipt.json")
    outcome = row.get("server_outcome") or {}
    decision = review.get("review", {}).get("decision")
    expected_lifecycle = _sha(
        row.get("human_review_lifecycle_binding_sha256"),
        "candidate human review lifecycle binding",
    )
    expected_names = (
        ["human-review-receipt.json", "package-descriptor.json"]
        if decision == "PASS"
        else ["human-review-receipt.json"]
    )
    if (
        decision not in ("PASS", "HOLD", "REJECT")
        or names != expected_names
        or review.get("candidate", {}).get("identity_sha256") != candidate_identity
        or review.get("candidate", {}).get("manifest") != row.get("candidate_manifest")
        or review.get("server_validation", {}).get("identity_sha256")
        != outcome.get("validation_identity_sha256")
        or review.get("server_validation", {}).get("receipt")
        != outcome.get("validation_receipt")
        or review.get("server_validation", {}).get("trusted_qa_metrics")
        != outcome.get("metrics")
        or review.get("lifecycle_binding_sha256") != expected_lifecycle
    ):
        raise _error("human review receipt does not bind the selected trusted evidence")
    return review, review_pin


def _human_review_summary(
    root: Path, job_id: str, row: Mapping[str, Any]
) -> Dict[str, Any] | None:
    loaded = _load_human_review_decision(root, job_id, row)
    if loaded is None:
        return None
    review, review_pin = loaded
    decision = review["review"]["decision"]
    summary = {
        "identity_sha256": review["identity_sha256"],
        "receipt": review_pin,
        "decision": decision,
        "package_descriptor": None,
        "candidate_id": None,
        "reviewer": {
            "id": review["review"]["reviewer_id"],
            "reviewed_at": review["review"]["reviewed_at"],
        },
    }
    if decision == "PASS":
        _, descriptor, descriptor_pin = _load_pass_review_descriptor(
            root, job_id, row
        )
        summary["package_descriptor"] = descriptor_pin
        summary["candidate_id"] = descriptor["candidate_id"]
    return summary


def _load_pass_review_descriptor(
    root: Path,
    job_id: str,
    row: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    loaded = _load_human_review_decision(root, job_id, row)
    if loaded is None or loaded[0].get("review", {}).get("decision") != "PASS":
        raise _error("selected candidate has no immutable PASS review package")
    review, review_pin = loaded
    candidate_identity = row["candidate_identity_sha256"]
    directory = (
        root
        / job_id
        / "browser-candidate-reviews"
        / candidate_identity[:2]
        / candidate_identity
        / "human-review"
    )
    from animation_fitting_candidate_review import (
        PACKAGE_NAMESPACE,
        PACKAGE_DESCRIPTOR_SCHEMA,
    )

    descriptor, descriptor_bytes = _read_json_file(
        directory / "package-descriptor.json",
        "package descriptor",
        MAX_RECEIPT_BYTES,
    )
    if descriptor.get("schema") != PACKAGE_DESCRIPTOR_SCHEMA:
        raise _error("package descriptor schema is invalid")
    descriptor_pin = _pin_payload(descriptor_bytes, "package-descriptor.json")
    bundle = _load_bundle(root, job_id, candidate_identity)
    package_id = _uuid(descriptor.get("candidate_id"), "package candidate_id")
    expected_package_id = str(
        uuid.uuid5(
            PACKAGE_NAMESPACE,
            (
                f"{job_id}:{row['seed']}:"
                f"{row['candidate_manifest']['sha256']}"
            ),
        )
    )
    outcome = row.get("server_outcome") or {}
    if (
        package_id != expected_package_id
        or review.get("candidate", {}).get("identity_sha256") != candidate_identity
        or review.get("candidate", {}).get("manifest") != row.get("candidate_manifest")
        or review.get("review", {}).get("decision") != "PASS"
        or review.get("server_validation", {}).get("identity_sha256")
        != outcome.get("validation_identity_sha256")
        or review.get("server_validation", {}).get("receipt")
        != outcome.get("validation_receipt")
        or review.get("server_validation", {}).get("trusted_qa_metrics")
        != outcome.get("metrics")
        or descriptor.get("package_id") != package_id
        or descriptor.get("candidate_identity_sha256") != candidate_identity
        or descriptor.get("server_validation_identity_sha256")
        != outcome.get("validation_identity_sha256")
        or descriptor.get("review_identity_sha256")
        != review.get("identity_sha256")
        or descriptor.get("candidate_bundle_sha256")
        != row["candidate_manifest"]["sha256"]
        or descriptor.get("human_review_sha256") != review_pin["sha256"]
        or descriptor.get("pins", {}).get("candidate_manifest")
        != row["candidate_manifest"]
        or descriptor.get("pins", {}).get("human_review_receipt") != review_pin
        or descriptor.get("pins", {}).get("server_validation_receipt")
        != outcome.get("validation_receipt")
        or descriptor.get("pins", {}).get("server_qa_metrics")
        != outcome.get("metrics")
        or descriptor.get("clip") != bundle.artifacts["three-clip.json"]
        or descriptor.get("fitting_job", {}).get("id") != job_id
    ):
        raise _error("human review package does not bind the selected trusted evidence")
    return review, descriptor, descriptor_pin


async def materialize_selected_candidates(
    db: AsyncSession,
    *,
    job_id: str,
    selection_identity_sha256: str,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> tuple[AnimalAnimationCandidate, ...]:
    """Persist FINAL top-3 using only immutable descriptor UUIDs and receipt ranks.

    This deliberately does not call the legacy client-payload candidate creator.
    All selected candidates must already have an immutable human PASS package.
    """
    selection_identity = _sha(
        selection_identity_sha256, "selection_identity_sha256"
    )
    receipt = verify_candidate_selection_receipt(
        job_id=job_id,
        selection_identity_sha256=selection_identity,
        fitting_jobs_root=fitting_jobs_root,
    )
    if (
        receipt.get("state") != "FINAL"
        or receipt.get("mode") != "production"
        or receipt.get("selection", {}).get("production_eligible") is not True
    ):
        raise _error("only a production-eligible FINAL selection can be materialized")
    snapshot = await _load_job(
        db,
        job_id,
        fitting_jobs_root=fitting_jobs_root,
        trusted_plan_inputs=trusted_plan_inputs,
    )
    if receipt.get("job", {}).get("lifecycle_identity_sha256") != snapshot.lifecycle_identity_sha256:
        raise _error("FINAL selection lifecycle is stale")
    root = _root(fitting_jobs_root)
    selected_ids = receipt["selection"]["top_k_candidate_identity_sha256"]
    rows_by_identity = {
        row["candidate_identity_sha256"]: row for row in receipt["candidates"]
    }
    if len(selected_ids) != TOP_K or len(set(selected_ids)) != TOP_K:
        raise _error("FINAL production selection must contain exactly three candidates")
    materialized: list[AnimalAnimationCandidate] = []
    for candidate_identity in selected_ids:
        row = rows_by_identity.get(candidate_identity)
        if row is None or row.get("ranking", {}).get("rank") not in (1, 2, 3):
            raise _error("FINAL selected candidate rank is invalid")
        admission = _load_admission(root, job_id, row["candidate_index"])
        outcome = _load_outcome(root, job_id, admission)
        if outcome is None or outcome.receipt.get("status") != "VALIDATED_PASS":
            raise _error("FINAL selected candidate has no trusted PASS outcome")
        _, metrics, _ = _validation_for_admission(
            root,
            job_id,
            admission,
            outcome.receipt["server_validation"]["identity_sha256"],
        )
        review, descriptor, descriptor_pin = _load_pass_review_descriptor(
            root, job_id, row
        )
        bundle = _load_bundle(root, job_id, candidate_identity)
        reviewed_metrics = json.loads(_canonical_json(metrics))
        reviewed_metrics["visual_phase_gate"]["decision"] = "PASS"
        reviewed_metrics["visual_phase_gate"]["reviewer"] = {
            "id": review["review"]["reviewer_id"],
            "reviewed_at": review["review"]["reviewed_at"],
        }
        validate_visual_phase_gate(
            reviewed_metrics,
            expected_rig_type=receipt["job"]["rig_type"],
            expected_semantic_id=receipt["job"]["semantic_id"],
            expected_fitted_clip_sha256=bundle.artifacts["three-clip.json"][
                "sha256"
            ],
        )
        candidate_state = bundle.manifest.get("candidate") or {}
        duration = candidate_state.get("duration_seconds")
        fps = candidate_state.get("fps")
        if (
            isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(float(duration))
            or float(duration) <= 0
            or isinstance(fps, bool)
            or not isinstance(fps, (int, float))
            or not math.isfinite(float(fps))
            or float(fps) <= 0
        ):
            raise _error("selected candidate timing is invalid")
        candidate_id = descriptor["candidate_id"]
        rank = row["ranking"]["rank"]
        rank_score = row["ranking"]["score"]
        human_review_path = (
            root
            / job_id
            / "browser-candidate-reviews"
            / candidate_identity[:2]
            / candidate_identity
            / "human-review"
            / "human-review-receipt.json"
        )
        provenance = {
            "schema": "autorig.browser-animation-candidate-materialization.v1",
            "selection_identity_sha256": selection_identity,
            "candidate_identity_sha256": candidate_identity,
            "candidate_manifest": bundle.manifest_pin,
            "server_validation_identity_sha256": outcome.receipt[
                "server_validation"
            ]["identity_sha256"],
            "human_review_identity_sha256": review["identity_sha256"],
            "human_review_receipt": _pin_payload(
                human_review_path.read_bytes(), "human-review-receipt.json"
            ),
            "package_descriptor": descriptor_pin,
            "rank_source": "immutable_selection_receipt",
        }
        expected = {
            "job_id": job_id,
            "seed": row["seed"],
            "status": "qa_complete",
            "raw_video_path": str(bundle.directory / "source-video.mp4"),
            "fitted_clip_path": str(bundle.directory / "three-clip.json"),
            "fitted_clip_sha256": bundle.artifacts["three-clip.json"]["sha256"],
            "fitted_clip_format": "threejs-animation-json.v1",
            "candidate_bundle_sha256": bundle.manifest_pin["sha256"],
            "human_review_sha256": provenance["human_review_receipt"]["sha256"],
            "duration": float(duration),
            "fps": float(fps),
            "root_motion_available": False,
            "metrics_json": _canonical_json(reviewed_metrics).decode("utf-8"),
            "provenance_json": _canonical_json(provenance).decode("utf-8"),
            "rank_score": float(rank_score),
            "rank": int(rank),
            "qa_passed": True,
        }
        existing = (
            await db.execute(
                select(AnimalAnimationCandidate).where(
                    AnimalAnimationCandidate.id == candidate_id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            actual = {name: getattr(existing, name) for name in expected}
            if actual != expected:
                raise _error("immutable review candidate_id is already bound differently")
            materialized.append(existing)
            continue
        conflicts = (
            await db.execute(
                select(AnimalAnimationCandidate).where(
                    AnimalAnimationCandidate.job_id == job_id,
                    (
                        (AnimalAnimationCandidate.seed == row["seed"])
                        | (AnimalAnimationCandidate.rank == rank)
                    ),
                )
            )
        ).scalars().all()
        if conflicts:
            raise _error("candidate seed or receipt-derived rank is already occupied")
        candidate = AnimalAnimationCandidate(
            id=candidate_id,
            raw_video_url=None,
            decoded_frames_path=None,
            fitted_clip_url=None,
            **expected,
        )
        db.add(candidate)
        materialized.append(candidate)
    await db.commit()
    for candidate in materialized:
        await db.refresh(candidate)
    return tuple(materialized)
