"""Resolve production browser-candidate trust exclusively from server state.

``AnimalAnimationFittingJob.config_json`` is compared with the result, but is
never used to discover a reference, controlled generation, retry grant, or
closure.  The resolver walks the server-owned immutable artifact store,
rehashes every input, reconstructs V2 receipts, asks
``animation_fitting_candidate_job_plan`` to rebuild the canonical plan, and
only then compares the complete plan with the database row.

The module intentionally has no route or SQLAlchemy dependency.  A route must
pass ORM rows loaded in its own transaction; mappings are accepted to keep the
boundary independently testable.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time
from typing import Any, Dict, Mapping, Sequence

import animation_fitting_candidate_job_plan as job_plan


MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_REFERENCE_BYTES = 128 * 1024 * 1024
MAX_VIDEO_BYTES = 512 * 1024 * 1024
MAX_CONTROLLED_JOB_DIRECTORIES = 100_000
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
STATE_FILE_RE = re.compile(r"^[0-9]{6}\.json$")
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")

_STATE_RUNTIME_KEYS = {
    "sequence_int",
    "recorded_at_unix_float",
    "status_string",
    "prompt_id_string",
    "resumed_existing_prompt_bool",
    "positive_prompt_string",
    "negative_prompt_string",
    "raw_video_path_string",
    "raw_video_sha256_string",
    "raw_video_bytes_int",
    "frame_paths_array",
    "frame_sha256_array",
    "backend_output_object",
    "error_type_string",
    "error_string",
}


class AnimationFittingPlanTrustError(ValueError):
    """A server-owned trust input is absent, mutable, or contradictory."""


@dataclass(frozen=True)
class ResolvedProductionCandidatePlan:
    """Canonical production plan plus the independently resolved trust set."""

    plan: job_plan.BrowserCandidateJobPlan
    trusted_task: Mapping[str, Any]
    reference_manifest: Mapping[str, Any]
    job_set_manifest: Mapping[str, Any]
    latest_states: tuple[Mapping[str, Any], ...]
    retry_authorization: Mapping[str, Any] | None
    verified_receipts: tuple[Mapping[str, Any], ...]

    def as_ingest_trust(self):
        """Create the ingest DTO lazily, without a module-level DB import."""

        from animation_fitting_candidate_ingest import BrowserCandidatePlanTrust

        return BrowserCandidatePlanTrust(
            reference_manifest=self.reference_manifest,
            latest_states=self.latest_states,
            retry_authorization=self.retry_authorization,
            job_set_manifest=self.job_set_manifest,
        )


@dataclass(frozen=True)
class PublishedControlledJobSetManifest:
    """One create-exclusive producer receipt for a V3 fitting batch."""

    content: Mapping[str, Any]
    pin: Mapping[str, Any]
    path: Path
    created: bool


@dataclass(frozen=True)
class _FileStamp:
    path: Path
    device: int
    inode: int
    size: int
    mtime_ns: int
    sha256: str
    maximum: int


@dataclass(frozen=True)
class _DirectoryStamp:
    path: Path
    device: int
    inode: int
    mtime_ns: int
    entries: tuple[tuple[str, int, int, int], ...]


class _StoreAudit:
    def __init__(self, root_input: str | Path) -> None:
        candidate = Path(root_input)
        if candidate.is_symlink() or not candidate.is_dir():
            raise _error("controlled artifact store root must be a real directory")
        self.root = candidate.resolve(strict=True)
        if self.root.is_symlink() or not self.root.is_dir():
            raise _error("controlled artifact store root must not be a symlink")
        self._files: Dict[Path, _FileStamp] = {}
        self._directories: Dict[Path, _DirectoryStamp] = {}
        self._absent_paths: set[Path] = set()

    def relative(self, value: Any, field: str) -> tuple[str, Path]:
        text = str(value or "")
        if "\\" in text:
            raise _error(f"{field} must be a canonical POSIX relative path")
        relative = PurePosixPath(text)
        if (
            not text
            or relative.is_absolute()
            or any(part in {"", ".", ".."} for part in relative.parts)
            or ":" in relative.parts[0]
        ):
            raise _error(f"{field} escapes the controlled artifact store")
        path = self.root.joinpath(*relative.parts)
        self._assert_chain(path, field, missing_may_be_absent=True)
        if path.parent.exists():
            try:
                resolved_parent = path.parent.resolve(strict=True)
            except OSError as exc:
                raise _error(f"{field} parent is not safely resolvable") from exc
            if (
                resolved_parent != self.root
                and self.root not in resolved_parent.parents
            ):
                raise _error(f"{field} escapes the controlled artifact store")
        return relative.as_posix(), path

    def directory(
        self, relative: str, field: str, *, required: bool = True
    ) -> Path | None:
        _, path = self.relative(relative, field)
        if not path.exists():
            if required:
                raise _error(f"{field} is missing")
            self._absent_paths.add(path)
            return None
        self._assert_chain(path, field)
        info = os.lstat(path)
        if not stat.S_ISDIR(info.st_mode):
            raise _error(f"{field} is not a real directory")
        entries = []
        with os.scandir(path) as iterator:
            for entry in iterator:
                entry_info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(entry_info.st_mode):
                    raise _error(f"{field} contains a symlink")
                entries.append(
                    (
                        entry.name,
                        entry_info.st_mode,
                        entry_info.st_size,
                        entry_info.st_mtime_ns,
                    )
                )
        self._directories[path] = _DirectoryStamp(
            path=path,
            device=info.st_dev,
            inode=info.st_ino,
            mtime_ns=info.st_mtime_ns,
            entries=tuple(sorted(entries)),
        )
        return path

    def exists(self, relative: str, field: str) -> bool:
        _, path = self.relative(relative, field)
        if not path.exists():
            self._absent_paths.add(path)
            return False
        self._assert_chain(path, field)
        return True

    def read_relative(self, relative: Any, field: str, maximum: int) -> bytes:
        canonical, path = self.relative(relative, field)
        del canonical
        return self._read(path, field, maximum)

    def read_absolute_or_relative(
        self, value: Any, expected_relative: str, field: str, maximum: int
    ) -> bytes:
        canonical, expected = self.relative(expected_relative, field)
        raw = Path(str(value or ""))
        if raw.is_absolute():
            self._assert_chain(raw, field)
            try:
                resolved = raw.resolve(strict=True)
            except OSError as exc:
                raise _error(f"{field} is missing") from exc
            if resolved != expected.resolve(strict=True):
                raise _error(f"{field} does not resolve to {canonical}")
        elif PurePosixPath(str(value or "")).as_posix() != canonical:
            raise _error(f"{field} does not use its canonical relative path")
        return self._read(expected, field, maximum)

    def json_relative(self, relative: Any, field: str) -> tuple[Dict[str, Any], bytes]:
        payload = self.read_relative(relative, field, MAX_JSON_BYTES)
        return _json_object(payload, field), payload

    def verify_unchanged(self) -> None:
        for path in tuple(self._absent_paths):
            if os.path.lexists(path):
                raise _error("controlled artifact appeared during trust resolution")
        for stamp in tuple(self._files.values()):
            current = self._read_once(stamp.path, "trusted file recheck", stamp.maximum)
            if (
                current[1:5] != (stamp.device, stamp.inode, stamp.size, stamp.mtime_ns)
                or _sha(current[0]) != stamp.sha256
            ):
                raise _error("controlled artifact changed during trust resolution")
        for stamp in tuple(self._directories.values()):
            info = os.lstat(stamp.path)
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise _error("controlled directory changed during trust resolution")
            entries = []
            with os.scandir(stamp.path) as iterator:
                for entry in iterator:
                    entry_info = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(entry_info.st_mode):
                        raise _error("controlled directory gained a symlink")
                    entries.append(
                        (
                            entry.name,
                            entry_info.st_mode,
                            entry_info.st_size,
                            entry_info.st_mtime_ns,
                        )
                    )
            if (info.st_dev, info.st_ino) != (stamp.device, stamp.inode) or tuple(
                sorted(entries)
            ) != stamp.entries:
                raise _error(
                    "controlled directory inventory changed during trust resolution"
                )

    def _read(self, path: Path, field: str, maximum: int) -> bytes:
        payload, device, inode, size, mtime_ns = self._read_once(path, field, maximum)
        stamp = _FileStamp(
            path=path,
            device=device,
            inode=inode,
            size=size,
            mtime_ns=mtime_ns,
            sha256=_sha(payload),
            maximum=maximum,
        )
        previous = self._files.get(path)
        if previous is not None and previous != stamp:
            raise _error(f"{field} changed during trust resolution")
        self._files[path] = stamp
        return payload

    @staticmethod
    def _read_once(
        path: Path, field: str, maximum: int
    ) -> tuple[bytes, int, int, int, int]:
        if path.is_symlink():
            raise _error(f"{field} must not be a symlink")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise _error(f"{field} is missing or not safely readable") from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                raise _error(f"{field} must be a non-empty regular file")
            if before.st_size > maximum:
                raise _error(f"{field} exceeds the server-owned size limit")
            chunks = []
            remaining = before.st_size + 1
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        payload = b"".join(chunks)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if len(payload) != before.st_size or identity_before != identity_after:
            raise _error(f"{field} changed while it was read")
        return payload, *identity_before

    def _assert_chain(
        self, path: Path, field: str, *, missing_may_be_absent: bool = False
    ) -> None:
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise _error(f"{field} escapes the controlled artifact store") from exc
        current = self.root
        for index, part in enumerate(relative.parts):
            current = current / part
            try:
                info = os.lstat(current)
            except OSError as exc:
                if missing_may_be_absent:
                    return
                raise _error(f"{field} is missing") from exc
            if stat.S_ISLNK(info.st_mode):
                raise _error(f"{field} traverses a symlink")


def _error(message: str) -> AnimationFittingPlanTrustError:
    return AnimationFittingPlanTrustError(message)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_object(payload: bytes, field: str) -> Dict[str, Any]:
    def reject_duplicate(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicate)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _error(f"{field} is not strict UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise _error(f"{field} must be a JSON object")
    return value


def _canonical(value: Any) -> bytes:
    return job_plan.canonical_json_bytes(value)


def _record(value: Any, field: str) -> Any:
    if isinstance(value, Mapping):
        if field not in value:
            raise _error(f"database record is missing {field}")
        return value[field]
    if not hasattr(value, field):
        raise _error(f"database record is missing {field}")
    return getattr(value, field)


def _uuid(value: Any, field: str) -> str:
    import uuid

    try:
        parsed = uuid.UUID(str(value or ""))
    except (ValueError, TypeError, AttributeError) as exc:
        raise _error(f"{field} is not a canonical UUID") from exc
    canonical = str(parsed)
    if str(value).lower() != canonical:
        raise _error(f"{field} is not a canonical UUID")
    return canonical


def _digest(value: Any, field: str) -> str:
    text = str(value or "")
    if not SHA256_RE.fullmatch(text):
        raise _error(f"{field} is not a lowercase SHA-256")
    return text


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _error(f"{field} must be a positive integer")
    return value


def _pin(path: str, payload: bytes) -> Dict[str, Any]:
    return {"path": path, "sha256": _sha(payload), "bytes": len(payload)}


def _secure_create_directories(root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise _error("publisher directory escapes the controlled store") from exc
    current = root
    for part in relative.parts:
        if part in ("", ".", ".."):
            raise _error("publisher directory contains an unsafe path component")
        current = current / part
        try:
            current.mkdir()
        except FileExistsError:
            pass
        if current.is_symlink() or not current.is_dir():
            raise _error("publisher directory chain contains a symlink or non-directory")


@contextmanager
def _job_set_publish_lock(root: Path, namespace: str):
    lock_root = root / ".locks" / "job-sets"
    _secure_create_directories(root, lock_root)
    lock_path = lock_root / f"{namespace}.lock"
    handle = lock_path.open("a+b")
    acquired = False
    deadline = time.monotonic() + 30.0
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        while not acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    raise _error("timed out acquiring the job-set publisher lock")
                time.sleep(0.025)
        yield
    finally:
        if acquired:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _discover_reference(
    audit: _StoreAudit, task_record: Any
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    task_id = _uuid(_record(task_record, "id"), "task.id")
    task_guid = _uuid(_record(task_record, "guid"), "task.guid")
    if (
        str(_record(task_record, "status") or "") != "done"
        or str(_record(task_record, "input_type") or "").lower() != "animal"
    ):
        raise _error("source task must be a completed Animal task")
    relative_dir = f"reference-manifests/{task_id}"
    directory = audit.directory(relative_dir, "reference manifest directory")
    assert directory is not None
    candidates = []
    for path in sorted(directory.iterdir(), key=lambda value: value.name):
        if not re.fullmatch(r"[0-9a-f]{64}\.json", path.name):
            raise _error("reference manifest directory contains an unexpected entry")
        relative = f"{relative_dir}/{path.name}"
        content, payload = audit.json_relative(relative, "reference manifest")
        if payload != _canonical(content) or path.stem != _sha(payload):
            raise _error("reference manifest is not canonical content-addressed JSON")
        if content.get("task_id") == task_id and content.get("task_guid") == task_guid:
            candidates.append((relative, content, payload))
    if len(candidates) != 1:
        raise _error("exactly one canonical task/GUID reference manifest is required")
    relative, content, payload = candidates[0]
    required = {
        "schema",
        "task_id",
        "task_guid",
        "source_rig_type",
        "species",
        "source_model_sha256",
        "source_skeleton_sha256",
        "actionless",
        "geometry_uv_normals_mutated",
        "reference_artifact",
    }
    if set(content) != required:
        raise _error("reference manifest content shape is not canonical")
    rig = str(content.get("source_rig_type") or "")
    species = job_plan.CANONICAL_SOURCE_RIG_SPECIES.get(rig.upper())
    if (
        content.get("schema") != job_plan.REFERENCE_MANIFEST_SCHEMA
        or content.get("species") != species
        or content.get("actionless") is not True
        or content.get("geometry_uv_normals_mutated") is not False
    ):
        raise _error("reference manifest is not an actionless canonical rig reference")
    model_sha = _digest(content.get("source_model_sha256"), "source model SHA-256")
    skeleton_sha = _digest(
        content.get("source_skeleton_sha256"), "source skeleton SHA-256"
    )
    artifact = content.get("reference_artifact")
    if not isinstance(artifact, Mapping) or set(artifact) != {
        "path",
        "sha256",
        "bytes",
    }:
        raise _error("reference artifact pin is invalid")
    expected_reference_path = f"references/{task_id}/reference_rgb.png"
    if artifact.get("path") != expected_reference_path:
        raise _error("reference artifact does not use the canonical task path")
    reference_payload = audit.read_relative(
        expected_reference_path, "reference RGB", MAX_REFERENCE_BYTES
    )
    if artifact.get("sha256") != _sha(reference_payload) or artifact.get(
        "bytes"
    ) != len(reference_payload):
        raise _error("reference RGB differs from its immutable manifest pin")
    trusted_task = {
        "schema": job_plan.TRUSTED_TASK_PINS_SCHEMA,
        "task_id": task_id,
        "task_guid": task_guid,
        "status": "done",
        "input_type": "animal",
        "source_rig_type": rig,
        "source_model_sha256": model_sha,
        "source_skeleton_sha256": skeleton_sha,
    }
    wrapper = {"content": content, "pin": _pin(relative, payload)}
    return trusted_task, wrapper


def _canonical_action_context(semantic_id: str, species: str):
    try:
        clip, output_fps = job_plan._load_taxonomy_clip(semantic_id)
        prompt, workflow = job_plan._canonical_prompt_and_workflow_contract(
            semantic_id=semantic_id, clip=clip, species=species
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise _error(f"fitting job action contract is invalid: {exc}") from exc
    return clip, output_fps, prompt, workflow


def _state_identity_job_id(state: Mapping[str, Any]) -> str:
    identity = {
        key: value for key, value in state.items() if key not in _STATE_RUNTIME_KEYS
    }
    if not identity or identity.get("schema") != job_plan.CONTROLLED_STATE_SCHEMA:
        raise _error("controlled state identity schema is invalid")
    return _sha(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _experiment_wrapper(
    audit: _StoreAudit, experiment_id: str, expected_sha: str
) -> Dict[str, Any]:
    if not SAFE_TOKEN_RE.fullmatch(experiment_id):
        raise _error("controlled state experiment ID is unsafe")
    relative = f"animation_fitting/specs/experiments/{experiment_id}.json"
    content, payload = audit.json_relative(relative, "controlled experiment spec")
    if payload != _canonical(content) or _sha(payload) != expected_sha:
        raise _error("controlled experiment spec differs from its state pin")
    return {"content": content, "pin": _pin(relative, payload)}


def _candidate_index(task_id: str, semantic_id: str, seed: Any) -> int:
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise _error("controlled state seed is invalid")
    matches = [
        index
        for index in range(job_plan.PRODUCTION_CANDIDATE_LIMIT)
        if job_plan._derive_canonical_seed(task_id, semantic_id, index) == seed
    ]
    if len(matches) != 1:
        raise _error("controlled state seed is not a canonical candidate seed")
    return matches[0]


def _state_receipt(
    audit: _StoreAudit,
    *,
    state: Mapping[str, Any],
    state_payload: bytes,
    state_relative: str,
    job_id: str,
    sequence: int,
    semantic_id: str,
    trusted_task: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    clip: Mapping[str, Any],
    output_fps: int,
    prompt_contract: Mapping[str, Any],
    workflow_contract: Mapping[str, Any],
) -> tuple[int, Dict[str, Any], Dict[str, Any]]:
    if state.get("status_string") != "completed":
        raise _error("latest controlled state is not completed")
    if state.get("sequence_int") != sequence:
        raise _error("controlled state sequence differs from its filename")
    if _state_identity_job_id(state) != job_id:
        raise _error("controlled state identity does not match its job directory")
    reference_sha = reference_manifest["content"]["reference_artifact"]["sha256"]
    if state.get("reference_sha256_string") != reference_sha:
        raise _error("controlled state belongs to another reference")
    experiment_id = str(state.get("experiment_id_string") or "")
    experiment_sha = _digest(
        state.get("experiment_sha256_string"), "controlled experiment SHA-256"
    )
    experiment = _experiment_wrapper(audit, experiment_id, experiment_sha)
    content = experiment["content"]
    if content.get("base_action_id_string") != semantic_id:
        raise _error(
            "controlled experiment semantic action differs from the fitting job"
        )
    mode = "loop" if clip.get("loop") is True else "one_shot"
    expected_timing = (
        int(clip["frame_profile"]),
        24,
        output_fps,
    )
    if (
        content.get("generation_mode_string") != mode
        or content.get("species_string") != prompt_contract["species"]
        or (
            content.get("frame_count_int"),
            content.get("input_fps_int"),
            content.get("output_fps_int"),
        )
        != expected_timing
        or content.get("reference_object")
        != {
            "immutable_manifest_sha256_string": reference_manifest["pin"]["sha256"],
            "source_model_sha256_string": trusted_task["source_model_sha256"],
        }
        or content.get("workflow_object")
        != {
            "workflow_name_string": workflow_contract["workflow_name"],
            "workflow_fingerprint_sha256_string": workflow_contract[
                "workflow_fingerprint_sha256"
            ],
        }
    ):
        raise _error(
            "controlled experiment spec does not match canonical task/action pins"
        )
    seed = state.get("seed_int")
    index = _candidate_index(trusted_task["task_id"], semantic_id, seed)
    if content.get("seed_int") != seed:
        raise _error("controlled experiment seed differs from the completed state")
    positive = str(content.get("positive_prompt_string") or "")
    negative = str(content.get("negative_prompt_string") or "")
    if (
        _sha(positive.encode("utf-8")) != prompt_contract["positive_prompt_sha256"]
        or _sha(negative.encode("utf-8")) != prompt_contract["negative_prompt_sha256"]
        or state.get("positive_prompt_sha256_string")
        != prompt_contract["positive_prompt_sha256"]
        or state.get("negative_prompt_sha256_string")
        != prompt_contract["negative_prompt_sha256"]
    ):
        raise _error("controlled state prompt binding differs from canonical prompts")
    if (
        state.get("frame_count_int") != expected_timing[0]
        or state.get("input_fps_int") != expected_timing[1]
        or state.get("output_fps_int") != expected_timing[2]
        or state.get("worker_id_string") is None
        or state.get("worker_base_url_string") is None
        or state.get("workflow_name_string") != workflow_contract["workflow_name"]
        or state.get("workflow_fingerprint_string")
        != workflow_contract["workflow_fingerprint_sha256"]
    ):
        raise _error("controlled state worker/workflow/timing binding drifted")
    raw_sha = _digest(state.get("raw_video_sha256_string"), "raw video SHA-256")
    raw_relative = f"raw/{raw_sha[:2]}/{raw_sha}.mp4"
    raw_payload = audit.read_absolute_or_relative(
        state.get("raw_video_path_string"), raw_relative, "raw video", MAX_VIDEO_BYTES
    )
    if (
        state.get("raw_video_bytes_int") != len(raw_payload)
        or _sha(raw_payload) != raw_sha
    ):
        raise _error("raw video differs from its completed state pin")
    prompt_id = str(state.get("prompt_id_string") or "")
    if prompt_id != job_plan.derive_controlled_prompt_id(job_id):
        raise _error("completed state prompt ID is not derived from its controlled job")
    latest = {
        "schema": job_plan.TRUSTED_LATEST_STATE_SCHEMA,
        "status": "completed",
        "latest": True,
        "job_id": job_id,
        "state_schema": job_plan.CONTROLLED_STATE_SCHEMA,
        "sequence": sequence,
        "filename": f"{sequence:06d}.json",
        "pin": _pin(state_relative, state_payload),
    }
    receipt = {
        "schema": job_plan.CONTROLLED_RECEIPT_SCHEMA_V2,
        "status": "completed",
        "candidate_index": index,
        "seed": seed,
        "job_id": job_id,
        "prompt_id": prompt_id,
        "semantic_id": semantic_id,
        "generation_mode": mode,
        "task": dict(trusted_task),
        "prompt_contract": dict(prompt_contract),
        "reference_manifest": dict(reference_manifest),
        "experiment_id": experiment_id,
        "experiment_sha256": experiment_sha,
        "experiment_spec": experiment,
        "worker_id": str(state["worker_id_string"]),
        "worker_base_url": str(state["worker_base_url_string"]),
        "workflow_name": str(state["workflow_name_string"]),
        "workflow_fingerprint_sha256": str(state["workflow_fingerprint_string"]),
        "frame_count": expected_timing[0],
        "input_fps": expected_timing[1],
        "output_fps": expected_timing[2],
        "source_video": _pin(raw_relative, raw_payload),
    }
    return index, latest, receipt


def _discover_job_set_manifest(
    audit: _StoreAudit,
    *,
    trusted_task: Mapping[str, Any],
    semantic_id: str,
    fitting_job_record: Any,
) -> Dict[str, Any]:
    fitting_job_id = _uuid(_record(fitting_job_record, "id"), "fitting_job.id")
    directory_relative = (
        f"job-sets/{trusted_task['task_id']}/{semantic_id}/{fitting_job_id}"
    )
    directory = audit.directory(directory_relative, "controlled job-set directory")
    assert directory is not None
    entries = sorted(directory.iterdir(), key=lambda path: path.name)
    if len(entries) != 1 or not re.fullmatch(r"[0-9a-f]{64}\.json", entries[0].name):
        raise _error(
            "exactly one namespaced content-addressed job-set manifest is required"
        )
    relative = f"{directory_relative}/{entries[0].name}"
    content, payload = audit.json_relative(relative, "controlled job-set manifest")
    if (
        payload != _canonical(content)
        or entries[0].stem != _sha(payload)
        or content.get("schema") != job_plan.CONTROLLED_JOB_SET_MANIFEST_SCHEMA
        or content.get("task_id") != trusted_task["task_id"]
        or content.get("task_guid") != trusted_task["task_guid"]
        or content.get("semantic_id") != semantic_id
        or content.get("fitting_job_id") != fitting_job_id
    ):
        raise _error("controlled job-set manifest namespace/content binding is invalid")
    return {"content": content, "pin": _pin(relative, payload)}


def _discover_controlled_receipts(
    audit: _StoreAudit,
    *,
    semantic_id: str,
    trusted_task: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    job_set_manifest: Mapping[str, Any],
) -> tuple[tuple[Mapping[str, Any], ...], tuple[Mapping[str, Any], ...]]:
    species = str(reference_manifest["content"]["species"])
    clip, output_fps, prompt_contract, workflow_contract = _canonical_action_context(
        semantic_id, species
    )
    controlled_jobs = job_set_manifest["content"].get("controlled_jobs")
    batch_start = job_set_manifest["content"].get("batch_start")
    expected_count = 8 if batch_start == 0 else 16 if batch_start == 8 else None
    if (
        expected_count is None
        or not isinstance(controlled_jobs, list)
        or len(controlled_jobs) != expected_count
    ):
        raise _error("controlled job-set batch inventory is invalid")
    by_index: Dict[int, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for offset, descriptor in enumerate(controlled_jobs):
        if (
            not isinstance(descriptor, Mapping)
            or set(descriptor) != {"candidate_index", "job_id"}
            or descriptor.get("candidate_index") != offset
        ):
            raise _error("controlled job-set indices must be exact and contiguous")
        job_id = _digest(descriptor.get("job_id"), "controlled job ID")
        relative_dir = f"jobs/{job_id}"
        directory = audit.directory(relative_dir, "controlled job state directory")
        assert directory is not None
        entries = sorted(directory.iterdir(), key=lambda value: value.name)
        if not entries or any(
            not STATE_FILE_RE.fullmatch(path.name) for path in entries
        ):
            raise _error("controlled job state inventory is invalid")
        latest_path = entries[-1]
        state_relative = f"{relative_dir}/{latest_path.name}"
        state, state_payload = audit.json_relative(
            state_relative, "latest controlled job state"
        )
        index, latest, receipt = _state_receipt(
            audit,
            state=state,
            state_payload=state_payload,
            state_relative=state_relative,
            job_id=job_id,
            sequence=int(latest_path.stem),
            semantic_id=semantic_id,
            trusted_task=trusted_task,
            reference_manifest=reference_manifest,
            clip=clip,
            output_fps=output_fps,
            prompt_contract=prompt_contract,
            workflow_contract=workflow_contract,
        )
        if index != offset or index in by_index:
            raise _error("controlled state differs from its namespaced job-set slot")
        by_index[index] = (latest, receipt)
    latest_states = tuple(by_index[index][0] for index in range(expected_count))
    receipts = tuple(by_index[index][1] for index in range(expected_count))
    return latest_states, receipts


def _identity_json(value: Mapping[str, Any], expected_schema: str, field: str) -> str:
    if value.get("schema") != expected_schema:
        raise _error(f"{field} schema is invalid")
    identity = _digest(value.get("identity_sha256"), f"{field} identity")
    unsigned = dict(value)
    unsigned.pop("identity_sha256", None)
    if _sha(_canonical(unsigned)) != identity:
        raise _error(f"{field} content identity drifted")
    return identity


def _read_pinned_json(
    audit: _StoreAudit, pin: Any, field: str
) -> tuple[Dict[str, Any], bytes]:
    if not isinstance(pin, Mapping) or set(pin) != {"path", "sha256", "bytes"}:
        raise _error(f"{field} pin is invalid")
    payload = audit.read_relative(pin.get("path"), field, MAX_JSON_BYTES)
    if pin.get("sha256") != _sha(payload) or pin.get("bytes") != len(payload):
        raise _error(f"{field} differs from its immutable pin")
    return _json_object(payload, field), payload


def _verify_zero_pass_parent_selection(
    audit: _StoreAudit,
    *,
    parent_id: str,
    authorization: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Independently prove that the immutable first batch had zero gate passes."""

    identity = _digest(
        authorization.get("parent_selection_identity_sha256"),
        "parent selection identity",
    )
    pin = authorization.get("parent_selection_receipt")
    expected_path = (
        f"{parent_id}/browser-candidate-selection/snapshots/{identity}/"
        "selection-receipt.json"
    )
    if not isinstance(pin, Mapping) or pin.get("path") != expected_path:
        raise _error("retry authorization parent selection path is invalid")
    selection, payload = _read_pinned_json(
        audit, pin, "parent zero-pass selection receipt"
    )
    if payload != _canonical(selection) + b"\n":
        raise _error("parent zero-pass selection receipt is not canonical JSON")
    if (
        set(selection)
        != {
            "schema",
            "state",
            "mode",
            "job",
            "contracts",
            "inventory",
            "candidates",
            "selection",
            "identity_sha256",
        }
        or _identity_json(
            selection,
            "autorig.browser-animation-candidate-selection.v1",
            "parent zero-pass selection receipt",
        )
        != identity
        or selection.get("state") != "OPEN"
        or selection.get("mode") != "production"
    ):
        raise _error("retry authorization parent selection identity is invalid")
    job = selection.get("job")
    inventory = selection.get("inventory")
    selected = selection.get("selection")
    candidates = selection.get("candidates")
    if (
        not isinstance(job, Mapping)
        or job.get("id") != parent_id
        or job.get("candidate_target") != 8
        or job.get("candidate_limit") != 8
        or job.get("adaptive_top_k") is not True
        or not isinstance(inventory, Mapping)
        or inventory.get("admitted_count") != 8
        or inventory.get("terminal_count") != 8
        or inventory.get("eligible_count") != 0
        or inventory.get("pending_count") != 0
        or inventory.get("candidate_target_satisfied") is not True
        or inventory.get("top_k_satisfied") is not False
        or inventory.get("generation_closed") is not True
        or inventory.get("generation_closure_identity_sha256")
        != authorization.get("parent_generation_closure_identity_sha256")
        or not isinstance(selected, Mapping)
        or selected.get("top_candidate_identity_sha256") is not None
        or selected.get("top_k_candidate_identity_sha256") != []
        or selected.get("production_eligible") is not False
        or not isinstance(candidates, list)
        or len(candidates) != 8
    ):
        raise _error("parent selection does not prove a closed zero-pass first batch")
    outcome_pins = authorization.get("first_batch_outcomes")
    if not isinstance(outcome_pins, list) or len(outcome_pins) != 8:
        raise _error("retry authorization must pin all eight parent outcomes")
    for index, (row, outcome_pin) in enumerate(zip(candidates, outcome_pins)):
        if not isinstance(row, Mapping) or not isinstance(outcome_pin, Mapping):
            raise _error("parent zero-pass selection candidate inventory is invalid")
        server_outcome = row.get("server_outcome")
        ranking = row.get("ranking")
        receipt_pin = (
            server_outcome.get("receipt")
            if isinstance(server_outcome, Mapping)
            else None
        )
        if (
            row.get("candidate_index") != index
            or not isinstance(server_outcome, Mapping)
            or server_outcome.get("status") != "VALIDATED_FAIL"
            or not isinstance(receipt_pin, Mapping)
            or receipt_pin.get("filename") != "outcome.json"
            or receipt_pin.get("sha256") != outcome_pin.get("sha256")
            or receipt_pin.get("bytes") != outcome_pin.get("bytes")
            or not isinstance(ranking, Mapping)
            or ranking.get("eligible") is not False
        ):
            raise _error(
                "parent selection contradicts the claimed zero-pass first batch"
            )
    return selection


def _discover_retry_authorization(
    audit: _StoreAudit,
    *,
    semantic_id: str,
    trusted_task: Mapping[str, Any],
    latest_states: Sequence[Mapping[str, Any]],
    job_set_manifest: Mapping[str, Any],
    fitting_job_record: Any,
    parent_fitting_job_record: Any | None,
    parent_resolution: ResolvedProductionCandidatePlan | None,
) -> Mapping[str, Any] | None:
    job_set = job_set_manifest["content"]
    batch_start = job_set.get("batch_start")
    if batch_start == 0:
        if job_set.get("parent") is not None:
            raise _error("immutable first job-set must not carry successor state")
        return None
    parent = job_set.get("parent")
    if (
        batch_start != 8
        or not isinstance(parent, Mapping)
        or parent_fitting_job_record is None
        or parent_resolution is None
    ):
        raise _error("successor job-set requires its independently resolved parent")
    parent_id = _uuid(_record(parent_fitting_job_record, "id"), "parent fitting job.id")
    successor_id = _uuid(_record(fitting_job_record, "id"), "successor fitting job.id")
    if (
        parent_id == successor_id
        or parent.get("fitting_job_id") != parent_id
        or parent.get("job_set_manifest_sha256")
        != parent_resolution.job_set_manifest["pin"]["sha256"]
    ):
        raise _error("successor job-set parent identity is invalid")
    auth_sha = _digest(parent.get("retry_authorization_sha256"), "retry authorization")
    relative = (
        f"retry-authorizations/{trusted_task['task_id']}/{semantic_id}/"
        f"{parent_id}/{successor_id}/{auth_sha}.json"
    )
    authorization, payload = audit.json_relative(relative, "retry authorization")
    if payload != _canonical(authorization) or _sha(payload) != auth_sha:
        raise _error("retry authorization is not canonical content-addressed JSON")
    expected_keys = {
        "schema",
        "status",
        "task_id",
        "task_guid",
        "semantic_id",
        "parent_fitting_job_id",
        "successor_fitting_job_id",
        "parent_job_set_manifest_sha256",
        "parent_config_sha256",
        "parent_lifecycle_identity_sha256",
        "parent_generation_closure",
        "parent_generation_closure_identity_sha256",
        "parent_selection_receipt",
        "parent_selection_identity_sha256",
        "first_batch_candidate_indices",
        "first_batch_latest_state_sha256s",
        "first_batch_outcomes",
        "first_batch_eligible_candidate_indices",
        "first_batch_outcome",
        "authorized_candidate_indices",
    }
    parent_config = _json_object(
        str(_record(parent_fitting_job_record, "config_json") or "").encode("utf-8"),
        "parent fitting job config",
    )
    expected_state_hashes = [state["pin"]["sha256"] for state in latest_states[:8]]
    if (
        set(authorization) != expected_keys
        or authorization.get("schema") != job_plan.RETRY_AUTHORIZATION_SCHEMA_V2
        or authorization.get("status") != "authorized"
        or authorization.get("task_id") != trusted_task["task_id"]
        or authorization.get("task_guid") != trusted_task["task_guid"]
        or authorization.get("semantic_id") != semantic_id
        or authorization.get("parent_fitting_job_id") != parent_id
        or authorization.get("successor_fitting_job_id") != successor_id
        or authorization.get("parent_job_set_manifest_sha256")
        != parent_resolution.job_set_manifest["pin"]["sha256"]
        or authorization.get("parent_config_sha256") != _sha(_canonical(parent_config))
        or authorization.get("first_batch_candidate_indices") != list(range(8))
        or authorization.get("first_batch_latest_state_sha256s")
        != expected_state_hashes
        or authorization.get("first_batch_eligible_candidate_indices") != []
        or authorization.get("first_batch_outcome") != "no_candidate_passed"
        or authorization.get("authorized_candidate_indices") != list(range(8, 16))
    ):
        raise _error("retry authorization does not prove the exact zero-pass parent")
    _verify_zero_pass_parent_selection(
        audit,
        parent_id=parent_id,
        authorization=authorization,
    )
    closure, _ = _read_pinned_json(
        audit,
        authorization.get("parent_generation_closure"),
        "parent generation closure",
    )
    closure_identity = _identity_json(
        closure,
        "autorig.browser-animation-candidate-generation-closure.v1",
        "parent generation closure",
    )
    if (
        closure.get("job_id") != parent_id
        or len(closure.get("admissions") or []) != 8
        or closure_identity
        != authorization.get("parent_generation_closure_identity_sha256")
        or closure_identity != parent.get("generation_closure_identity_sha256")
    ):
        raise _error("retry authorization generation closure binding drifted")
    outcome_pins = authorization.get("first_batch_outcomes")
    if not isinstance(outcome_pins, list) or len(outcome_pins) != 8:
        raise _error("retry authorization must pin all eight parent outcomes")
    for index, pin in enumerate(outcome_pins):
        expected_path = (
            f"{parent_id}/browser-candidate-selection/outcomes/{index:02d}/outcome.json"
        )
        if not isinstance(pin, Mapping) or pin.get("path") != expected_path:
            raise _error("retry authorization outcome path is not parent namespaced")
        outcome, _ = _read_pinned_json(audit, pin, f"parent outcome {index}")
        _identity_json(
            outcome,
            "autorig.browser-animation-candidate-outcome.v1",
            f"parent outcome {index}",
        )
        if (
            outcome.get("job_id") != parent_id
            or outcome.get("candidate_index") != index
            or outcome.get("status") not in {"VALIDATED_PASS", "VALIDATED_FAIL"}
        ):
            raise _error("retry authorization parent outcome binding drifted")
    expected_parent = {
        "fitting_job_id": parent_id,
        "job_set_manifest_sha256": authorization[
            "parent_job_set_manifest_sha256"
        ],
        "config_sha256": authorization["parent_config_sha256"],
        "lifecycle_identity_sha256": authorization[
            "parent_lifecycle_identity_sha256"
        ],
        "generation_closure_identity_sha256": authorization[
            "parent_generation_closure_identity_sha256"
        ],
        "retry_authorization_sha256": auth_sha,
    }
    if dict(parent) != expected_parent:
        raise _error("successor job-set parent binding differs from authorization")
    return {"content": authorization, "pin": _pin(relative, payload)}


def _database_request(
    fitting_job: Any, job_set_manifest: Mapping[str, Any]
) -> Mapping[str, Any]:
    semantic_id = str(_record(fitting_job, "semantic_id") or "")
    target = _record(fitting_job, "candidate_target")
    limit = _record(fitting_job, "candidate_limit")
    return {
        "schema": job_plan.BATCH_PLAN_REQUEST_SCHEMA,
        "semantic_id": semantic_id,
        "candidate_target": target,
        "candidate_limit": limit,
        "batch_start": job_set_manifest["content"].get("batch_start"),
    }


def _validate_database_job(
    fitting_job: Any,
    *,
    plan: job_plan.BrowserCandidateJobPlan,
    trusted_task: Mapping[str, Any],
) -> None:
    try:
        config = _json_object(
            str(_record(fitting_job, "config_json") or "").encode("utf-8"),
            "fitting job config_json",
        )
    except UnicodeEncodeError as exc:
        raise _error("fitting job config_json is not UTF-8") from exc
    expected = {
        "semantic_id": plan.semantic_id,
        "workflow_name": plan.workflow_name,
        "workflow_fingerprint": plan.workflow_fingerprint,
        "worker_url": plan.worker_base_url,
        "prompt_id": plan.prompt_id,
        "candidate_target": plan.candidate_target,
        "candidate_limit": plan.candidate_limit,
    }
    actual = {key: _record(fitting_job, key) for key in expected}
    if actual != expected or config != plan.config:
        raise _error(
            "fitting job DB/config differs from the canonical server-owned plan"
        )
    rig_type = str(_record(fitting_job, "rig_type") or "").strip().lower()
    expected_species = job_plan.CANONICAL_SOURCE_RIG_SPECIES.get(
        str(trusted_task["source_rig_type"]).upper()
    )
    if rig_type != expected_species:
        raise _error("fitting job rig_type differs from the canonical source rig")


def _build_and_validate_plan(
    *,
    fitting_job: Any,
    trusted_task: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    job_set_manifest: Mapping[str, Any],
    latest_states: Sequence[Mapping[str, Any]],
    retry_authorization: Mapping[str, Any] | None,
    receipts: Sequence[Mapping[str, Any]],
) -> job_plan.BrowserCandidateJobPlan:
    try:
        plan = job_plan.build_production_browser_candidate_batch_job_plan(
            _database_request(fitting_job, job_set_manifest),
            fitting_job_id=_record(fitting_job, "id"),
            trusted_task=trusted_task,
            trusted_reference_manifest=reference_manifest,
            trusted_job_set_manifest=job_set_manifest,
            trusted_latest_states=latest_states,
            trusted_retry_authorization=retry_authorization,
            verified_receipts=receipts,
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise _error(
            f"canonical production candidate plan cannot be rebuilt: {exc}"
        ) from exc
    _validate_database_job(fitting_job, plan=plan, trusted_task=trusted_task)
    return plan


def publish_controlled_job_set_manifest(
    *,
    store_root: str | Path,
    task_record: Any,
    fitting_job_record: Any,
    controlled_results: Sequence[Any],
    parent_fitting_job_record: Any | None = None,
) -> PublishedControlledJobSetManifest:
    """Publish one immutable V3 job-set from server-owned producer results.

    The caller cannot supply manifest content, candidate indices, reference
    pins, retry pins, or parent bindings.  Those are reconstructed from the
    completed ``ControlledExperimentResult`` objects, DB rows, and canonical
    artifact namespaces under one cross-process publisher lock.
    """

    from animation_fitting.controlled_experiment import ControlledExperimentResult

    audit = _StoreAudit(store_root)
    trusted_task, reference_manifest = _discover_reference(audit, task_record)
    fitting_job_id = _uuid(_record(fitting_job_record, "id"), "fitting job.id")
    semantic_id = str(_record(fitting_job_record, "semantic_id") or "").strip()
    target = _record(fitting_job_record, "candidate_target")
    limit = _record(fitting_job_record, "candidate_limit")
    if target != 8 or limit not in (8, 16):
        raise _error("V3 job-set publisher requires target 8 and limit 8 or 16")
    batch_start = 0 if limit == 8 else 8
    results = tuple(controlled_results)
    expected_count = 8 if batch_start == 0 else 16
    if len(results) != expected_count or any(
        not isinstance(result, ControlledExperimentResult) for result in results
    ):
        raise _error(
            "job-set publisher requires the exact server ControlledExperimentResult inventory"
        )
    controlled_jobs = []
    seen_job_ids = set()
    for index, result in enumerate(results):
        job_id = _digest(result.job_id, f"controlled result {index} job ID")
        if job_id in seen_job_ids:
            raise _error("job-set publisher received duplicate controlled jobs")
        seen_job_ids.add(job_id)
        controlled_jobs.append({"candidate_index": index, "job_id": job_id})

    parent = None
    parent_resolution = None
    if batch_start == 0:
        if parent_fitting_job_record is not None:
            raise _error("immutable first job-set must not receive a parent DB row")
    else:
        if parent_fitting_job_record is None:
            raise _error("successor job-set publisher requires its parent DB row")
        parent_id = _uuid(
            _record(parent_fitting_job_record, "id"), "parent fitting job.id"
        )
        if parent_id == fitting_job_id:
            raise _error("successor fitting job must differ from its parent")
        parent_resolution = resolve_production_candidate_plan(
            store_root=store_root,
            task_record=task_record,
            fitting_job_record=parent_fitting_job_record,
        )
        parent_jobs = parent_resolution.job_set_manifest["content"][
            "controlled_jobs"
        ]
        if controlled_jobs[:8] != parent_jobs:
            raise _error(
                "successor producer results do not preserve the immutable first batch"
            )
        auth_dir_relative = (
            f"retry-authorizations/{trusted_task['task_id']}/{semantic_id}/"
            f"{parent_id}/{fitting_job_id}"
        )
        auth_dir = audit.directory(
            auth_dir_relative, "successor retry authorization directory"
        )
        assert auth_dir is not None
        entries = sorted(auth_dir.iterdir(), key=lambda path: path.name)
        if len(entries) != 1 or not re.fullmatch(
            r"[0-9a-f]{64}\.json", entries[0].name
        ):
            raise _error(
                "exactly one content-addressed successor authorization is required"
            )
        auth_relative = f"{auth_dir_relative}/{entries[0].name}"
        auth_content, auth_payload = audit.json_relative(
            auth_relative, "successor retry authorization"
        )
        if (
            auth_payload != _canonical(auth_content)
            or entries[0].stem != _sha(auth_payload)
            or auth_content.get("parent_fitting_job_id") != parent_id
            or auth_content.get("successor_fitting_job_id") != fitting_job_id
        ):
            raise _error("successor retry authorization namespace/content drifted")
        parent = {
            "fitting_job_id": parent_id,
            "job_set_manifest_sha256": parent_resolution.job_set_manifest["pin"][
                "sha256"
            ],
            "config_sha256": auth_content.get("parent_config_sha256"),
            "lifecycle_identity_sha256": auth_content.get(
                "parent_lifecycle_identity_sha256"
            ),
            "generation_closure_identity_sha256": auth_content.get(
                "parent_generation_closure_identity_sha256"
            ),
            "retry_authorization_sha256": entries[0].stem,
        }

    content = {
        "schema": job_plan.CONTROLLED_JOB_SET_MANIFEST_SCHEMA,
        "task_id": trusted_task["task_id"],
        "task_guid": trusted_task["task_guid"],
        "semantic_id": semantic_id,
        "fitting_job_id": fitting_job_id,
        "batch_start": batch_start,
        "candidate_target": 8,
        "candidate_limit": limit,
        "reference_manifest_sha256": reference_manifest["pin"]["sha256"],
        "controlled_jobs": controlled_jobs,
        "parent": parent,
    }
    payload = _canonical(content)
    digest = _sha(payload)
    relative = (
        f"job-sets/{trusted_task['task_id']}/{semantic_id}/"
        f"{fitting_job_id}/{digest}.json"
    )
    wrapper = {"content": content, "pin": _pin(relative, payload)}
    latest_states, receipts = _discover_controlled_receipts(
        audit,
        semantic_id=semantic_id,
        trusted_task=trusted_task,
        reference_manifest=reference_manifest,
        job_set_manifest=wrapper,
    )
    if batch_start == 8:
        assert parent_resolution is not None
        _discover_retry_authorization(
            audit,
            semantic_id=semantic_id,
            trusted_task=trusted_task,
            latest_states=latest_states,
            job_set_manifest=wrapper,
            fitting_job_record=fitting_job_record,
            parent_fitting_job_record=parent_fitting_job_record,
            parent_resolution=parent_resolution,
        )
    # Validate the producer receipts before making the namespace immutable.
    if len(receipts) != expected_count:
        raise _error("job-set producer receipt inventory is incomplete")
    audit.verify_unchanged()

    root = audit.root
    target = root / PurePosixPath(relative)
    target_dir = target.parent
    namespace = f"{trusted_task['task_id']}-{semantic_id}-{fitting_job_id}"
    with _job_set_publish_lock(root, namespace):
        _secure_create_directories(root, target_dir.parent)
        if target_dir.exists():
            if target_dir.is_symlink() or not target_dir.is_dir():
                raise _error("job-set namespace is a symlink or non-directory")
            entries = sorted(target_dir.iterdir(), key=lambda path: path.name)
            if (
                len(entries) != 1
                or entries[0].name != target.name
                or entries[0].is_symlink()
                or not entries[0].is_file()
                or entries[0].read_bytes() != payload
            ):
                raise _error("job-set namespace is already pinned to different bytes")
            created = False
        else:
            target_dir.mkdir()
            try:
                with target.open("xb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            except FileExistsError as exc:
                raise _error("job-set manifest create-exclusive publication raced") from exc
            created = True
    return PublishedControlledJobSetManifest(
        content=content,
        pin=wrapper["pin"],
        path=target,
        created=created,
    )


def resolve_production_candidate_plan(
    *,
    store_root: str | Path,
    task_record: Any,
    fitting_job_record: Any,
    parent_fitting_job_record: Any | None = None,
) -> ResolvedProductionCandidatePlan:
    """Resolve and validate one production 8-or-16 candidate plan.

    The only discovery inputs are the two server-loaded DB rows and the
    server-owned artifact root.  In particular, job IDs, reference paths,
    receipt descriptors, retry grants, and closures are never selected from
    ``config_json``.
    """

    audit = _StoreAudit(store_root)
    trusted_task, reference_manifest = _discover_reference(audit, task_record)
    semantic_id = str(_record(fitting_job_record, "semantic_id") or "")
    job_set_manifest = _discover_job_set_manifest(
        audit,
        trusted_task=trusted_task,
        semantic_id=semantic_id,
        fitting_job_record=fitting_job_record,
    )
    parent_resolution = None
    if job_set_manifest["content"].get("batch_start") == 8:
        if parent_fitting_job_record is None:
            raise _error("successor fitting job requires its server-loaded parent DB row")
        parent_resolution = resolve_production_candidate_plan(
            store_root=store_root,
            task_record=task_record,
            fitting_job_record=parent_fitting_job_record,
        )
    latest_states, receipts = _discover_controlled_receipts(
        audit,
        semantic_id=semantic_id,
        trusted_task=trusted_task,
        reference_manifest=reference_manifest,
        job_set_manifest=job_set_manifest,
    )
    retry_authorization = _discover_retry_authorization(
        audit,
        semantic_id=semantic_id,
        trusted_task=trusted_task,
        latest_states=latest_states,
        job_set_manifest=job_set_manifest,
        fitting_job_record=fitting_job_record,
        parent_fitting_job_record=parent_fitting_job_record,
        parent_resolution=parent_resolution,
    )
    plan = _build_and_validate_plan(
        fitting_job=fitting_job_record,
        trusted_task=trusted_task,
        reference_manifest=reference_manifest,
        job_set_manifest=job_set_manifest,
        latest_states=latest_states,
        retry_authorization=retry_authorization,
        receipts=receipts,
    )
    # The final inventory/file recheck closes scan/read/build TOCTOU windows,
    # including a newly appended higher state sequence.
    audit.verify_unchanged()
    return ResolvedProductionCandidatePlan(
        plan=plan,
        trusted_task=trusted_task,
        reference_manifest=reference_manifest,
        job_set_manifest=job_set_manifest,
        latest_states=tuple(latest_states),
        retry_authorization=retry_authorization,
        verified_receipts=tuple(receipts),
    )
