"""Bridge server-approved browser clips into the Blender export motion contract.

The bridge performs no fitting and never invokes Blender.  It resolves each
clip from the server-owned canonical FINAL candidate-selection slot, reopens
the immutable PASS review package and candidate bundle, and only then converts
the exact Three.js clip into local matrices.  Caller-provided approval hashes
are comparison data, never approval authority.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np

if __package__:
    from .math3d import flatten_matrix, quaternion_xyzw_from_matrix
    from .motion_export_contract import load_motion
    from .package_browser_animation_glb import (
        _joint_nodes,
        _load_inputs,
        _parse_glb,
        _validate_clip,
    )
    from .rig import _load_bones
else:  # pragma: no cover - direct script boundary
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from animation_fitting.math3d import flatten_matrix, quaternion_xyzw_from_matrix
    from animation_fitting.motion_export_contract import load_motion
    from animation_fitting.package_browser_animation_glb import (
        _joint_nodes,
        _load_inputs,
        _parse_glb,
        _validate_clip,
    )
    from animation_fitting.rig import _load_bones


RESULT_SCHEMA = "autorig.browser-animation-motion-bridge-result.v2"
MOTION_SCHEMA = "autorig-fitted-animation.v1"
TRANSFORM_SCHEMA = "autorig-fitted-transform-contract.v1"
SELECTION_SCHEMA = "autorig.browser-animation-candidate-selection.v1"
CANDIDATE_SCHEMA = "autorig.browser-animation-candidate-bundle.v1"
HUMAN_REVIEW_SCHEMA = "autorig.browser-animation-human-review.v1"
PACKAGE_DESCRIPTOR_SCHEMA = "autorig.browser-animation-package-descriptor.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
MATRIX_TOLERANCE = 1e-6
# Blender's glTF exporter changes world coordinates from Blender Z-up to glTF
# Y-up while retaining the joint-local bone basis.  A complete exported
# skeleton is therefore either in the source matrix space or under this one
# code-owned left-handed world-basis conversion.  Selection is asset-wide;
# individual joints can never choose different modes.
BLENDER_TO_GLTF_Y_UP = np.asarray(
    (
        (1.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, -1.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    ),
    dtype=np.float64,
)
TIMELINE_TOLERANCE = 2e-6
OUTPUT_FPS = 30
LOOP_POSITION_CLOSURE_TOLERANCE = 1e-4
LOOP_ROTATION_CLOSURE_RADIANS = 1e-4
MAX_JSON_BYTES = 64 * 1024 * 1024
FILE_ATTRIBUTE_REPARSE_POINT = 0x400


class BridgeError(RuntimeError):
    """Fail-closed browser-motion bridge violation."""


@dataclass(frozen=True)
class Snapshot:
    path: Path
    data: bytes
    size: int
    sha256: str

    def descriptor(self) -> dict[str, Any]:
        return {"path": str(self.path), "bytes": self.size, "sha256": self.sha256}

    def filename_pin(self) -> dict[str, Any]:
        return {
            "filename": self.path.name,
            "bytes": self.size,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class ApprovalEvidence:
    semantic_id: str
    job_id: str
    selection_identity_sha256: str
    candidate_identity_sha256: str
    candidate_id: str
    selection_receipt: Snapshot
    candidate_manifest: Snapshot
    human_review_receipt: Snapshot
    package_descriptor: Snapshot
    browser_clip: Snapshot
    candidate_bundle_sha256: str
    human_review_sha256: str
    library_revision: str
    rig_type: str
    source_task: Mapping[str, str]
    task_model_pin: Mapping[str, Any]
    task_skeleton_pin: Mapping[str, Any]

    def result_descriptor(self) -> dict[str, Any]:
        return {
            "selection_job_id": self.job_id,
            "selection_identity_sha256": self.selection_identity_sha256,
            "selection_receipt": self.selection_receipt.descriptor(),
            "candidate_identity_sha256": self.candidate_identity_sha256,
            "candidate_id": self.candidate_id,
            "candidate_manifest": self.candidate_manifest.descriptor(),
            "candidate_bundle_sha256": self.candidate_bundle_sha256,
            "human_review_receipt": self.human_review_receipt.descriptor(),
            "human_review_sha256": self.human_review_sha256,
            "package_descriptor": self.package_descriptor.descriptor(),
            "browser_clip": self.browser_clip.descriptor(),
            "source_task": dict(self.source_task),
            "task_model": dict(self.task_model_pin),
            "task_skeleton": dict(self.task_skeleton_pin),
        }


def _canonical(value: Mapping[str, Any], *, newline: bool = True) -> bytes:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return payload + (b"\n" if newline else b"")


def _sha(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BridgeError(f"{field} must be a lowercase SHA-256")
    return value


def _uuid(value: Any, field: str) -> str:
    if not isinstance(value, str) or not UUID_RE.fullmatch(value):
        raise BridgeError(f"{field} must be a canonical lowercase UUID")
    return value


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise BridgeError(f"{field} must be an object")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BridgeError(f"{field} must be a positive integer")
    return value


def _is_reparse(metadata: os.stat_result) -> bool:
    return bool(
        getattr(metadata, "st_file_attributes", 0) & FILE_ATTRIBUTE_REPARSE_POINT
    )


def _lexical_absolute(path: str | Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _reject_symlink_chain(path: Path, field: str) -> None:
    current = _lexical_absolute(path)
    chain = [current]
    chain.extend(current.parents)
    for component in reversed(chain):
        try:
            metadata = os.lstat(component)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise BridgeError(f"{field} path is unavailable: {exc}") from exc
        if stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
            raise BridgeError(f"{field} path contains a symlink/reparse component")


def _secure_root(path: str | Path, field: str) -> Path:
    lexical = _lexical_absolute(path)
    _reject_symlink_chain(lexical, field)
    try:
        resolved = lexical.resolve(strict=True)
    except OSError as exc:
        raise BridgeError(f"{field} is unavailable: {exc}") from exc
    if not resolved.is_dir():
        raise BridgeError(f"{field} must be an existing real directory")
    return resolved


def _secure_file(path: str | Path, field: str, *, root: Path | None = None) -> Path:
    lexical = _lexical_absolute(path)
    if root is not None:
        root_lexical = _lexical_absolute(root)
        try:
            lexical.relative_to(root_lexical)
        except ValueError as exc:
            raise BridgeError(f"{field} escapes its server-owned root") from exc
    _reject_symlink_chain(lexical, field)
    try:
        resolved = lexical.resolve(strict=True)
    except OSError as exc:
        raise BridgeError(f"{field} is unavailable: {exc}") from exc
    if root is not None:
        try:
            resolved.relative_to(root.resolve(strict=True))
        except ValueError as exc:
            raise BridgeError(
                f"{field} resolves outside its server-owned root"
            ) from exc
    if not resolved.is_file():
        raise BridgeError(f"{field} must be a regular file")
    return resolved


def _snapshot(
    path: str | Path,
    field: str,
    *,
    root: Path | None = None,
    maximum: int | None = None,
) -> Snapshot:
    source = _secure_file(path, field, root=root)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
        with os.fdopen(descriptor, "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                raise BridgeError(f"{field} must be a non-empty regular file")
            if maximum is not None and before.st_size > maximum:
                raise BridgeError(f"{field} exceeds its {maximum}-byte limit")
            data = stream.read((maximum + 1) if maximum is not None else -1)
            after = os.fstat(stream.fileno())
    except BridgeError:
        raise
    except OSError as exc:
        raise BridgeError(f"{field} could not be read: {exc}") from exc
    if maximum is not None and len(data) > maximum:
        raise BridgeError(f"{field} exceeds its {maximum}-byte limit")
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or len(data) != before.st_size:
        raise BridgeError(f"{field} changed while read")
    return Snapshot(source, data, len(data), hashlib.sha256(data).hexdigest())


def _json(
    snapshot: Snapshot, field: str, *, canonical: bool = False
) -> Mapping[str, Any]:
    try:
        value = _object(json.loads(snapshot.data.decode("utf-8-sig")), field)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BridgeError(f"{field} is invalid JSON: {exc}") from exc
    if canonical and snapshot.data != _canonical(value):
        raise BridgeError(f"{field} must use canonical JSON bytes")
    return value


def _validate_filename_pin(
    value: Any,
    snapshot: Snapshot,
    field: str,
    *,
    expected_filename: str | None = None,
) -> None:
    pin = _object(value, field)
    if set(pin) != {"filename", "bytes", "sha256"}:
        raise BridgeError(f"{field} must contain filename/bytes/sha256")
    filename = expected_filename or snapshot.path.name
    if (
        pin.get("filename") != filename
        or pin.get("bytes") != snapshot.size
        or pin.get("sha256") != snapshot.sha256
    ):
        raise BridgeError(f"{field} does not match server-owned bytes")


def _same_content_pin(value: Any, snapshot: Snapshot, field: str) -> None:
    pin = _object(value, field)
    if pin.get("bytes") != snapshot.size or pin.get("sha256") != snapshot.sha256:
        raise BridgeError(f"{field} does not match server-owned bytes")


def _identity_json(value: Mapping[str, Any], schema: str, field: str) -> str:
    if value.get("schema") != schema:
        raise BridgeError(f"{field} schema is invalid")
    identity = _sha(value.get("identity_sha256"), f"{field}.identity_sha256")
    unsigned = dict(value)
    unsigned.pop("identity_sha256", None)
    if hashlib.sha256(_canonical(unsigned, newline=False)).hexdigest() != identity:
        raise BridgeError(f"{field} content identity is invalid")
    return identity


def _resolve_final_approval(
    *, fitting_jobs_root: str | Path, job_id: str, semantic_id: str
) -> ApprovalEvidence:
    """Resolve one canonical FINAL rank-1 PASS package from server-owned storage."""

    root = _secure_root(fitting_jobs_root, "fitting_jobs_root")
    job = _uuid(job_id, f"{semantic_id} approval job_id")
    job_root = root / job
    selection_path = (
        job_root / "browser-candidate-selection" / "final" / "selection-receipt.json"
    )
    selection_snapshot = _snapshot(
        selection_path,
        f"{semantic_id} FINAL selection receipt",
        root=root,
        maximum=MAX_JSON_BYTES,
    )
    selection = _json(
        selection_snapshot, f"{semantic_id} FINAL selection receipt", canonical=True
    )
    selection_identity = _identity_json(
        selection, SELECTION_SCHEMA, f"{semantic_id} FINAL selection receipt"
    )
    job_value = _object(selection.get("job"), f"{semantic_id} selection.job")
    selected = _object(selection.get("selection"), f"{semantic_id} selection.selection")
    top_identity = _sha(
        selected.get("top_candidate_identity_sha256"),
        f"{semantic_id} selection top candidate",
    )
    top_k = selected.get("top_k_candidate_identity_sha256")
    if isinstance(top_k, list):
        for index, identity_value in enumerate(top_k):
            _sha(identity_value, f"{semantic_id} selection top_k[{index}]")
    if (
        selection.get("state") != "FINAL"
        or selection.get("mode") != "production"
        or job_value.get("id") != job
        or job_value.get("semantic_id") != semantic_id
        or selected.get("comparative_selection") is not True
        or selected.get("production_eligible") is not True
        or not isinstance(top_k, list)
        or len(top_k) != 3
        or len(set(top_k)) != 3
        or top_k[0] != top_identity
    ):
        raise BridgeError(
            f"{semantic_id} approval is not the canonical production FINAL rank-1 selection"
        )
    rows = selection.get("candidates")
    if not isinstance(rows, list):
        raise BridgeError(f"{semantic_id} FINAL candidate inventory is invalid")
    for rank, identity_value in enumerate(top_k, 1):
        ranked = [
            candidate_row
            for candidate_row in rows
            if isinstance(candidate_row, dict)
            and candidate_row.get("candidate_identity_sha256") == identity_value
        ]
        if (
            len(ranked) != 1
            or ranked[0].get("ranking", {}).get("eligible") is not True
            or ranked[0].get("ranking", {}).get("rank") != rank
            or ranked[0].get("human_review", {}).get("decision") != "PASS"
        ):
            raise BridgeError(
                f"{semantic_id} FINAL top-{rank} row is missing its immutable PASS rank"
            )
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("candidate_identity_sha256") == top_identity
    ]
    if len(matches) != 1:
        raise BridgeError(f"{semantic_id} FINAL rank-1 row is missing or duplicated")
    row = matches[0]
    review_summary = _object(
        row.get("human_review"), f"{semantic_id} FINAL human review summary"
    )
    ranking = _object(row.get("ranking"), f"{semantic_id} FINAL ranking")
    candidate_id = _uuid(
        review_summary.get("candidate_id"), f"{semantic_id} approved candidate_id"
    )
    if (
        ranking.get("eligible") is not True
        or ranking.get("rank") != 1
        or review_summary.get("decision") != "PASS"
    ):
        raise BridgeError(f"{semantic_id} FINAL rank-1 candidate has no human PASS")

    candidate_dir = job_root / "browser-candidates" / top_identity[:2] / top_identity
    manifest_snapshot = _snapshot(
        candidate_dir / "candidate-manifest.json",
        f"{semantic_id} candidate manifest",
        root=root,
        maximum=MAX_JSON_BYTES,
    )
    manifest = _json(
        manifest_snapshot, f"{semantic_id} candidate manifest", canonical=True
    )
    manifest_identity = _identity_json(
        manifest, CANDIDATE_SCHEMA, f"{semantic_id} candidate manifest"
    )
    if manifest_identity != top_identity:
        raise BridgeError(f"{semantic_id} candidate identity differs from FINAL")
    _validate_filename_pin(
        row.get("candidate_manifest"),
        manifest_snapshot,
        f"{semantic_id} FINAL candidate_manifest",
        expected_filename="candidate-manifest.json",
    )
    library = _object(manifest.get("library"), f"{semantic_id} candidate library")
    fitting_job = _object(
        manifest.get("fitting_job"), f"{semantic_id} candidate fitting_job"
    )
    source_task = _object(
        manifest.get("source_task"), f"{semantic_id} candidate source_task"
    )
    candidate = _object(manifest.get("candidate"), f"{semantic_id} candidate")
    artifacts = _object(manifest.get("artifacts"), f"{semantic_id} artifacts")
    if (
        fitting_job.get("id") != job
        or fitting_job.get("semantic_id") != semantic_id
        or job_value.get("library_revision") != library.get("revision")
        or job_value.get("rig_type") != library.get("rig_type")
        or candidate.get("source_model_sha256") is None
        or candidate.get("source_skeleton_sha256") is None
    ):
        raise BridgeError(f"{semantic_id} candidate/job/library binding is invalid")
    task_id = _uuid(source_task.get("id"), f"{semantic_id} source task id")
    task_guid = _uuid(source_task.get("guid"), f"{semantic_id} source task guid")

    clip_snapshot = _snapshot(
        candidate_dir / "three-clip.json",
        f"{semantic_id} approved browser clip",
        root=root,
        maximum=MAX_JSON_BYTES,
    )
    _validate_filename_pin(
        artifacts.get("three-clip.json"),
        clip_snapshot,
        f"{semantic_id} candidate three-clip pin",
        expected_filename="three-clip.json",
    )

    review_dir = (
        job_root
        / "browser-candidate-reviews"
        / top_identity[:2]
        / top_identity
        / "human-review"
    )
    review_snapshot = _snapshot(
        review_dir / "human-review-receipt.json",
        f"{semantic_id} human PASS receipt",
        root=root,
        maximum=MAX_JSON_BYTES,
    )
    review = _json(review_snapshot, f"{semantic_id} human PASS receipt", canonical=True)
    review_identity = _identity_json(
        review, HUMAN_REVIEW_SCHEMA, f"{semantic_id} human PASS receipt"
    )
    _validate_filename_pin(
        review_summary.get("receipt"),
        review_snapshot,
        f"{semantic_id} FINAL human review receipt",
        expected_filename="human-review-receipt.json",
    )
    if (
        review.get("review", {}).get("decision") != "PASS"
        or review.get("candidate", {}).get("identity_sha256") != top_identity
        or review.get("candidate", {}).get("manifest") != row.get("candidate_manifest")
        or review_summary.get("identity_sha256") != review_identity
    ):
        raise BridgeError(f"{semantic_id} human PASS receipt binding is invalid")

    descriptor_snapshot = _snapshot(
        review_dir / "package-descriptor.json",
        f"{semantic_id} PASS package descriptor",
        root=root,
        maximum=MAX_JSON_BYTES,
    )
    descriptor = _json(
        descriptor_snapshot, f"{semantic_id} PASS package descriptor", canonical=True
    )
    if descriptor.get("schema") != PACKAGE_DESCRIPTOR_SCHEMA:
        raise BridgeError(f"{semantic_id} PASS package descriptor schema is invalid")
    _validate_filename_pin(
        review_summary.get("package_descriptor"),
        descriptor_snapshot,
        f"{semantic_id} FINAL package descriptor",
        expected_filename="package-descriptor.json",
    )
    pins = _object(descriptor.get("pins"), f"{semantic_id} package pins")
    task_model_pin = _object(pins.get("task_model"), f"{semantic_id} task model pin")
    task_skeleton_pin = _object(
        pins.get("task_skeleton"), f"{semantic_id} task skeleton pin"
    )
    for pin_value, label in (
        (task_model_pin, "task_model"),
        (task_skeleton_pin, "task_skeleton"),
    ):
        if set(pin_value) != {"filename", "bytes", "sha256"}:
            raise BridgeError(f"{semantic_id} package {label} pin is invalid")
        _positive_int(pin_value.get("bytes"), f"{semantic_id} {label}.bytes")
        _sha(pin_value.get("sha256"), f"{semantic_id} {label}.sha256")
    if (
        descriptor.get("package_id") != candidate_id
        or descriptor.get("candidate_id") != candidate_id
        or descriptor.get("candidate_identity_sha256") != top_identity
        or descriptor.get("review_identity_sha256") != review_identity
        or descriptor.get("candidate_bundle_sha256") != manifest_snapshot.sha256
        or descriptor.get("human_review_sha256") != review_snapshot.sha256
        or descriptor.get("semantic_id") != semantic_id
        or descriptor.get("clip") != artifacts.get("three-clip.json")
        or descriptor.get("library") != library
        or descriptor.get("fitting_job") != fitting_job
        or descriptor.get("source_task") != source_task
        or pins.get("candidate_manifest") != row.get("candidate_manifest")
        or pins.get("three_clip") != artifacts.get("three-clip.json")
        or pins.get("human_review_receipt") != review_summary.get("receipt")
        or task_model_pin.get("sha256") != candidate.get("source_model_sha256")
        or task_skeleton_pin.get("sha256") != candidate.get("source_skeleton_sha256")
        or task_skeleton_pin.get("sha256") != library.get("template_skeleton_sha256")
    ):
        raise BridgeError(
            f"{semantic_id} PASS package does not bind FINAL/candidate/clip/task evidence"
        )
    return ApprovalEvidence(
        semantic_id=semantic_id,
        job_id=job,
        selection_identity_sha256=selection_identity,
        candidate_identity_sha256=top_identity,
        candidate_id=candidate_id,
        selection_receipt=selection_snapshot,
        candidate_manifest=manifest_snapshot,
        human_review_receipt=review_snapshot,
        package_descriptor=descriptor_snapshot,
        browser_clip=clip_snapshot,
        candidate_bundle_sha256=manifest_snapshot.sha256,
        human_review_sha256=review_snapshot.sha256,
        library_revision=str(library["revision"]),
        rig_type=str(library["rig_type"]),
        source_task={"id": task_id, "guid": task_guid},
        task_model_pin=dict(task_model_pin),
        task_skeleton_pin=dict(task_skeleton_pin),
    )


def _quaternion_matrix(values: Sequence[float], field: str) -> np.ndarray:
    quaternion = np.asarray(values, dtype=np.float64)
    if quaternion.shape != (4,) or not np.all(np.isfinite(quaternion)):
        raise BridgeError(f"{field} must contain four finite values")
    norm = float(np.linalg.norm(quaternion))
    if abs(norm - 1.0) > 1e-3:
        raise BridgeError(f"{field} must be normalized")
    x, y, z, w = quaternion / norm
    return np.asarray(
        (
            (1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)),
            (2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)),
            (2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)),
        ),
        dtype=np.float64,
    )


def _decompose_rest(
    matrix: np.ndarray, field: str
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    source = np.asarray(matrix, dtype=np.float64)
    translation = source[:3, 3].copy()
    linear = source[:3, :3]
    scale = np.linalg.norm(linear, axis=0)
    if np.any(scale <= 1e-10) or not np.all(np.isfinite(scale)):
        raise BridgeError(f"{field} has singular rest scale")
    rotation = linear / scale[np.newaxis, :]
    if np.linalg.det(rotation) < 0:
        scale[-1] *= -1
        rotation[:, -1] *= -1
    if not np.allclose(rotation.T @ rotation, np.eye(3), atol=MATRIX_TOLERANCE, rtol=0):
        raise BridgeError(f"{field} contains unsupported shear")
    rebuilt = np.eye(4)
    rebuilt[:3, :3] = rotation @ np.diag(scale)
    rebuilt[:3, 3] = translation
    if not np.allclose(rebuilt, source, atol=MATRIX_TOLERANCE, rtol=0):
        raise BridgeError(f"{field} cannot be represented as Three.js TRS")
    return translation, rotation, scale


def _compose(
    position: Sequence[float],
    quaternion: Sequence[float],
    scale: np.ndarray,
    field: str,
) -> np.ndarray:
    translation = np.asarray(position, dtype=np.float64)
    if translation.shape != (3,) or not np.all(np.isfinite(translation)):
        raise BridgeError(f"{field}.position must contain three finite values")
    result = np.eye(4, dtype=np.float64)
    result[:3, :3] = _quaternion_matrix(quaternion, f"{field}.quaternion") @ np.diag(
        scale
    )
    result[:3, 3] = translation
    return result


def _rotation_quaternion(rotation: np.ndarray) -> list[float]:
    matrix = np.eye(4)
    matrix[:3, :3] = rotation
    return quaternion_xyzw_from_matrix(matrix)


def _rotation_distance(left: np.ndarray, right: np.ndarray) -> float:
    q1 = np.asarray(quaternion_xyzw_from_matrix(left), dtype=np.float64)
    q2 = np.asarray(quaternion_xyzw_from_matrix(right), dtype=np.float64)
    dot = min(1.0, max(0.0, abs(float(np.dot(q1, q2)))))
    return 2.0 * math.acos(dot)


def _uniform_timeline(
    times: Sequence[float], semantic_id: str, expected_frame_count: int
) -> float:
    values = np.asarray(times, dtype=np.float64)
    if (
        values.ndim != 1
        or len(values) != expected_frame_count
        or not np.all(np.isfinite(values))
    ):
        raise BridgeError(
            f"browser clip {semantic_id} must contain exactly {expected_frame_count} frames"
        )
    expected = np.arange(expected_frame_count, dtype=np.float64) / OUTPUT_FPS
    if not np.allclose(values, expected, atol=TIMELINE_TOLERANCE, rtol=0):
        raise BridgeError(
            f"browser clip {semantic_id} must use exact {OUTPUT_FPS} FPS frame-profile timing"
        )
    return float(OUTPUT_FPS)


def _track_frames(track: Any) -> tuple[tuple[float, ...], ...]:
    width = int(track.item_size)
    values = tuple(float(value) for value in track.values)
    return tuple(
        values[index : index + width] for index in range(0, len(values), width)
    )


def _node_local_matrix(node_value: Any, field: str) -> np.ndarray:
    node = _object(node_value, field)
    has_matrix = "matrix" in node
    has_trs = any(name in node for name in ("translation", "rotation", "scale"))
    if has_matrix and has_trs:
        raise BridgeError(f"{field} mixes matrix and TRS")
    if has_matrix:
        values = np.asarray(node["matrix"], dtype=np.float64)
        if values.shape != (16,) or not np.all(np.isfinite(values)):
            raise BridgeError(f"{field}.matrix must contain 16 finite values")
        matrix = values.reshape(4, 4).T
    else:
        translation = node.get("translation", [0.0, 0.0, 0.0])
        rotation = node.get("rotation", [0.0, 0.0, 0.0, 1.0])
        scale = np.asarray(node.get("scale", [1.0, 1.0, 1.0]), dtype=np.float64)
        if (
            scale.shape != (3,)
            or not np.all(np.isfinite(scale))
            or np.any(abs(scale) < 1e-10)
        ):
            raise BridgeError(f"{field}.scale is invalid")
        matrix = _compose(translation, rotation, scale, field)
    if not np.allclose(matrix[3], [0, 0, 0, 1], atol=1e-7, rtol=0):
        raise BridgeError(f"{field} is not affine")
    return matrix


def _skeleton_fingerprint(
    armature_name: str,
    armature_world: np.ndarray,
    bones: Mapping[str, Any],
    bone_order: Sequence[str],
) -> str:
    payload = {
        "armature_name": armature_name,
        "armature_world": [round(float(value), 12) for value in armature_world.flat],
        "bones": [
            {
                "name": name,
                "parent": bones[name].parent,
                "helper": bool(bones[name].helper),
                "use_deform": bool(bones[name].use_deform),
                "rest_world": [
                    round(float(value), 12) for value in bones[name].rest_world.flat
                ],
            }
            for name in bone_order
        ],
    }
    return hashlib.sha256(_canonical(payload, newline=False)).hexdigest()


def _validate_glb_skeleton(
    gltf: Mapping[str, Any],
    *,
    armature_name: str,
    armature_world: np.ndarray,
    bones: Mapping[str, Any],
    bone_order: Sequence[str],
    field: str,
) -> str:
    """Validate every joint, including helper/non-deform and multiple roots."""

    nodes = gltf.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise BridgeError(f"{field} contains no nodes")
    joints = _joint_nodes(gltf)
    if set(joints) != set(bone_order):
        missing = sorted(set(bone_order).difference(joints))
        extra = sorted(set(joints).difference(bone_order))
        raise BridgeError(
            f"{field} joint inventory differs from skeleton; missing={missing}, extra={extra}"
        )
    parent_by_index: dict[int, int] = {}
    for parent_index, node_value in enumerate(nodes):
        node = _object(node_value, f"{field}.nodes[{parent_index}]")
        children = node.get("children", [])
        if not isinstance(children, list):
            raise BridgeError(f"{field}.nodes[{parent_index}].children is invalid")
        for raw_child in children:
            if (
                isinstance(raw_child, bool)
                or not isinstance(raw_child, int)
                or not 0 <= raw_child < len(nodes)
            ):
                raise BridgeError(f"{field} child node index is invalid")
            if raw_child in parent_by_index:
                raise BridgeError(f"{field} node has multiple parents")
            parent_by_index[raw_child] = parent_index
    local = {
        index: _node_local_matrix(value, f"{field}.nodes[{index}]")
        for index, value in enumerate(nodes)
    }
    world: dict[int, np.ndarray] = {}
    visiting: set[int] = set()

    def resolve(index: int) -> np.ndarray:
        if index in world:
            return world[index]
        if index in visiting:
            raise BridgeError(f"{field} node hierarchy contains a cycle")
        visiting.add(index)
        parent = parent_by_index.get(index)
        result = local[index] if parent is None else resolve(parent) @ local[index]
        visiting.remove(index)
        world[index] = result
        return result

    joint_index_set = set(joints.values())
    for name in bone_order:
        node_index = joints[name]
        cursor = parent_by_index.get(node_index)
        joint_parent_name = None
        while cursor is not None:
            if cursor in joint_index_set:
                joint_parent_name = next(
                    candidate for candidate, index in joints.items() if index == cursor
                )
                break
            cursor = parent_by_index.get(cursor)
        bone = bones[name]
        if joint_parent_name != bone.parent:
            raise BridgeError(
                f"{field} joint parent mismatch for {name}: {joint_parent_name!r} != {bone.parent!r}"
            )
    rest_modes = {
        "direct": lambda matrix: matrix,
        "blender_to_gltf_y_up": lambda matrix: BLENDER_TO_GLTF_Y_UP @ matrix,
    }
    valid_modes = set(rest_modes)
    for name in bone_order:
        actual = resolve(joints[name])
        bone = bones[name]
        valid_modes = {
            mode
            for mode in valid_modes
            if np.allclose(
                actual,
                rest_modes[mode](bone.rest_world),
                atol=MATRIX_TOLERANCE,
                rtol=0,
            )
        }
        if not valid_modes:
            raise BridgeError(
                f"{field} rest matrix mismatch for joint {name}; "
                "skeleton must use one asset-wide direct or Blender-to-glTF Y-up mode"
            )
    return _skeleton_fingerprint(armature_name, armature_world, bones, bone_order)


def _motion_payload(
    *,
    semantic_id: str,
    loop: bool,
    expected_frame_count: int,
    clip: Any,
    skeleton_pin: Mapping[str, Any],
    package_input_pin: Mapping[str, Any],
    approval: ApprovalEvidence,
    source_asset_identity_sha256: str,
    skeleton_fingerprint_sha256: str,
    armature_name: str,
    armature_world: np.ndarray,
    bones: Mapping[str, Any],
    bone_order: Sequence[str],
) -> dict[str, Any]:
    timeline = clip.tracks[0].times
    fps = _uniform_timeline(timeline, semantic_id, expected_frame_count)
    frame_count = len(timeline)
    tracks: dict[tuple[str, str], tuple[tuple[float, ...], ...]] = {}
    for track in clip.tracks:
        if track.bone_name not in bones:
            raise BridgeError(
                f"browser clip {semantic_id} references unknown skeleton bone {track.bone_name}"
            )
        key = (track.bone_name, track.target_path)
        if key in tracks:
            raise BridgeError(
                f"browser clip {semantic_id} repeats {track.bone_name}.{track.target_path}"
            )
        frames = _track_frames(track)
        if len(frames) != frame_count:
            raise BridgeError(f"browser clip {semantic_id} track frame count mismatch")
        tracks[key] = frames
    inverse_armature = np.linalg.inv(armature_world)
    rest: dict[str, tuple[np.ndarray, list[float], np.ndarray]] = {}
    roots: list[str] = []
    for name in bone_order:
        bone = bones[name]
        relative = (
            inverse_armature @ bone.rest_local
            if bone.parent is None
            else bone.rest_local
        )
        position, rotation, scale = _decompose_rest(relative, f"skeleton bone {name}")
        rest[name] = (position, _rotation_quaternion(rotation), scale)
        if bone.parent is None:
            roots.append(name)
    translation_bones = set(roots)
    translation_bones.update(name for name, target in tracks if target == "translation")
    frames: list[dict[str, Any]] = []
    matrices: list[dict[str, np.ndarray]] = []
    for frame_index in range(frame_count):
        frame_bones: dict[str, Any] = {}
        frame_matrices: dict[str, np.ndarray] = {}
        for name in bone_order:
            bone = bones[name]
            rest_position, rest_quaternion, rest_scale = rest[name]
            positions = tracks.get((name, "translation"))
            quaternions = tracks.get((name, "rotation"))
            position = positions[frame_index] if positions else rest_position
            quaternion = quaternions[frame_index] if quaternions else rest_quaternion
            relative = _compose(
                position, quaternion, rest_scale, f"{semantic_id}.{name}[{frame_index}]"
            )
            local = armature_world @ relative if bone.parent is None else relative
            frame_matrices[name] = local
            frame_bones[name] = {
                "parent": bone.parent,
                "local_matrix": flatten_matrix(local),
                "local_translation": [float(value) for value in local[:3, 3]],
                "local_rotation_xyzw": quaternion_xyzw_from_matrix(local),
            }
        frames.append({"frame": frame_index, "bones": frame_bones})
        matrices.append(frame_matrices)
    if loop:
        for name in bone_order:
            first = matrices[0][name]
            last = matrices[-1][name]
            position_error = float(np.linalg.norm(first[:3, 3] - last[:3, 3]))
            rotation_error = _rotation_distance(first, last)
            if position_error > LOOP_POSITION_CLOSURE_TOLERANCE:
                raise BridgeError(
                    f"loop clip {semantic_id} position closure failed for {name}: {position_error:.9g}"
                )
            if rotation_error > LOOP_ROTATION_CLOSURE_RADIANS:
                raise BridgeError(
                    f"loop clip {semantic_id} rotation closure failed for {name}: {rotation_error:.9g} rad"
                )
    approval_descriptor = approval.result_descriptor()
    return {
        "schema": MOTION_SCHEMA,
        "semantic_action_id": semantic_id,
        "frame_count": frame_count,
        "fps": fps,
        "loop": loop,
        "transform_contract": {
            "schema": TRANSFORM_SCHEMA,
            "source_armature_name": armature_name,
            "source_armature_world_matrix": flatten_matrix(armature_world),
            "root_local_matrix_space": "WORLD",
            "child_local_matrix_space": "PARENT_BONE",
            "rotation_channel": "QUATERNION",
            "scale_animation": False,
            "translation_policy": {
                "mode": "explicit_bones",
                "bones": [name for name in bone_order if name in translation_bones],
            },
        },
        "browser_export_provenance": {
            "browser_only": True,
            "blender_used": False,
            "fitting_performed": False,
            "source_clip_schema": "THREE.AnimationClip.toJSON",
            "skeleton": dict(skeleton_pin),
            "glb_package_input": dict(package_input_pin),
            "approval": approval_descriptor,
            "candidate_id": approval.candidate_id,
            "candidate_bundle_sha256": approval.candidate_bundle_sha256,
            "human_review_sha256": approval.human_review_sha256,
            "source_asset_identity_sha256": source_asset_identity_sha256,
            "skeleton_fingerprint_sha256": skeleton_fingerprint_sha256,
            "timing_contract": {
                "output_fps": OUTPUT_FPS,
                "frame_profile": expected_frame_count,
                "loop": loop,
                "position_closure_tolerance": LOOP_POSITION_CLOSURE_TOLERANCE,
                "rotation_closure_tolerance_radians": LOOP_ROTATION_CLOSURE_RADIANS,
            },
        },
        "frames": frames,
    }


def bridge_browser_animation_motions(
    *,
    package_input: str | Path,
    package_input_sha256: str,
    skeleton: str | Path,
    skeleton_sha256: str,
    fitting_jobs_root: str | Path,
    approval_job_ids: Mapping[str, str],
    output_dir: str | Path,
) -> dict[str, Any]:
    package_sha = _sha(package_input_sha256, "package_input_sha256")
    skeleton_sha = _sha(skeleton_sha256, "skeleton_sha256")
    skeleton_snapshot = _snapshot(
        skeleton, "actionless skeleton", maximum=MAX_JSON_BYTES
    )
    if skeleton_snapshot.sha256 != skeleton_sha:
        raise BridgeError("skeleton SHA-256 mismatch")
    try:
        package = _load_inputs(package_input, package_sha)
    except Exception as exc:
        raise BridgeError(f"browser package input validation failed: {exc}") from exc
    if package.template_skeleton_sha256 != skeleton_snapshot.sha256:
        raise BridgeError(
            "skeleton SHA-256 must equal package template_skeleton_sha256"
        )
    if set(approval_job_ids) != set(package.clip_ids):
        raise BridgeError(
            "approval_job_ids must contain exactly the canonical 30 semantic IDs"
        )
    if len(set(approval_job_ids.values())) != len(package.clip_ids):
        raise BridgeError("approval_job_ids must not reuse a fitting job")
    approvals = tuple(
        _resolve_final_approval(
            fitting_jobs_root=fitting_jobs_root,
            job_id=approval_job_ids[semantic_id],
            semantic_id=semantic_id,
        )
        for semantic_id in package.clip_ids
    )
    source_tasks = {
        tuple(sorted(approval.source_task.items())) for approval in approvals
    }
    task_models = {
        (approval.task_model_pin["bytes"], approval.task_model_pin["sha256"])
        for approval in approvals
    }
    task_skeletons = {
        (approval.task_skeleton_pin["bytes"], approval.task_skeleton_pin["sha256"])
        for approval in approvals
    }
    if len(source_tasks) != 1 or len(task_models) != 1 or len(task_skeletons) != 1:
        raise BridgeError(
            "all 30 FINAL approvals must target one exact server-owned source asset"
        )
    source_task = dict(source_tasks.pop())
    first_approval = approvals[0]
    if (
        first_approval.task_model_pin["bytes"] != package.source.size
        or first_approval.task_model_pin["sha256"] != package.source.sha256
    ):
        raise BridgeError("package source GLB differs from FINAL-approved task model")
    if (
        first_approval.task_skeleton_pin["bytes"] != skeleton_snapshot.size
        or first_approval.task_skeleton_pin["sha256"] != skeleton_snapshot.sha256
    ):
        raise BridgeError(
            "actionless skeleton differs from FINAL-approved task skeleton"
        )
    for approved_input, approval in zip(package.clips, approvals, strict=True):
        if (
            approval.library_revision != package.library_revision
            or approval.rig_type != package.rig_type
            or approved_input.semantic_id != approval.semantic_id
            or approved_input.candidate_id != approval.candidate_id
            or approved_input.candidate_bundle_sha256
            != approval.candidate_bundle_sha256
            or approved_input.human_review_sha256 != approval.human_review_sha256
            or approved_input.source.size != approval.browser_clip.size
            or approved_input.source.sha256 != approval.browser_clip.sha256
        ):
            raise BridgeError(
                f"package clip/approval binding mismatch for {approved_input.semantic_id}"
            )

    destination = _lexical_absolute(output_dir)
    _reject_symlink_chain(destination.parent, "motion bridge output parent")
    if destination.exists() or destination.is_symlink():
        raise BridgeError(f"motion bridge output collision: {destination}")
    if not destination.parent.is_dir():
        raise BridgeError(
            f"motion bridge output parent does not exist: {destination.parent}"
        )
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        skeleton_copy = staging / ".skeleton.snapshot.json"
        skeleton_copy.write_bytes(skeleton_snapshot.data)
        try:
            source_gltf, _ = _parse_glb(package.source)
            joints = _joint_nodes(source_gltf)
            browser_clips = [_validate_clip(source, joints) for source in package.clips]
            armature_name, armature_world, bones, bone_order = _load_bones(
                skeleton_copy
            )
        except Exception as exc:
            raise BridgeError(
                f"browser motion bridge input validation failed: {exc}"
            ) from exc
        skeleton_fingerprint = _validate_glb_skeleton(
            source_gltf,
            armature_name=armature_name,
            armature_world=armature_world,
            bones=bones,
            bone_order=bone_order,
            field="package source GLB",
        )
        taxonomy_value = json.loads(package.taxonomy.data.decode("utf-8-sig"))
        if taxonomy_value.get("output_fps") != OUTPUT_FPS:
            raise BridgeError(f"canonical taxonomy output_fps must be {OUTPUT_FPS}")
        taxonomy_rows = taxonomy_value.get("clips")
        if not isinstance(taxonomy_rows, list) or len(taxonomy_rows) != 30:
            raise BridgeError("canonical taxonomy clip inventory is invalid")
        taxonomy_by_id = {row["id"]: row for row in taxonomy_rows}
        asset_identity_payload = {
            "source_task": source_task,
            "task_model": dict(first_approval.task_model_pin),
            "task_skeleton": dict(first_approval.task_skeleton_pin),
            "template_skeleton_sha256": skeleton_snapshot.sha256,
            "rig_type": package.rig_type,
            "orientation": package.orientation,
        }
        source_asset_identity = hashlib.sha256(
            _canonical(asset_identity_payload, newline=False)
        ).hexdigest()
        package_pin = {
            "path": str(package.manifest_snapshot.path),
            "bytes": package.manifest_snapshot.size,
            "sha256": package.manifest_snapshot.sha256,
        }
        skeleton_pin = skeleton_snapshot.descriptor()
        rows: list[dict[str, Any]] = []
        for order, (approved, clip, approval) in enumerate(
            zip(package.clips, browser_clips, approvals, strict=True), 1
        ):
            taxonomy_row = taxonomy_by_id[approved.semantic_id]
            frame_profile = _positive_int(
                taxonomy_row.get("frame_profile"),
                f"{approved.semantic_id} frame_profile",
            )
            loop = taxonomy_row.get("loop")
            if not isinstance(loop, bool):
                raise BridgeError(f"{approved.semantic_id} loop contract is invalid")
            payload = _motion_payload(
                semantic_id=approved.semantic_id,
                loop=loop,
                expected_frame_count=frame_profile,
                clip=clip,
                skeleton_pin=skeleton_pin,
                package_input_pin=package_pin,
                approval=approval,
                source_asset_identity_sha256=source_asset_identity,
                skeleton_fingerprint_sha256=skeleton_fingerprint,
                armature_name=armature_name,
                armature_world=armature_world,
                bones=bones,
                bone_order=bone_order,
            )
            output = staging / f"{order:02d}-{approved.semantic_id}.motion.json"
            output.write_bytes(_canonical(payload))
            validated = load_motion(output)
            if (
                validated.raw.get("semantic_action_id") != approved.semantic_id
                or validated.frame_count != frame_profile
                or validated.fps != OUTPUT_FPS
                or validated.loop is not loop
            ):
                raise BridgeError(
                    f"derived motion contract mismatch for {approved.semantic_id}"
                )
            output_snapshot = _snapshot(
                output, f"{approved.semantic_id} derived motion"
            )
            rows.append(
                {
                    "order": order,
                    "semantic_id": approved.semantic_id,
                    "motion": {
                        **output_snapshot.descriptor(),
                        "path": str(destination / output.name),
                    },
                    "approval": approval.result_descriptor(),
                    "frame_profile": frame_profile,
                    "output_fps": OUTPUT_FPS,
                    "loop": loop,
                }
            )
        skeleton_copy.unlink()
        result = {
            "schema": RESULT_SCHEMA,
            "library_revision": package.library_revision,
            "rig_type": package.rig_type,
            "orientation": package.orientation,
            "template_skeleton_sha256": package.template_skeleton_sha256,
            "taxonomy": package.taxonomy.descriptor(),
            "glb_package_input": package_pin,
            "source_glb": package.source.descriptor(),
            "skeleton": skeleton_pin,
            "source_asset": {
                **asset_identity_payload,
                "identity_sha256": source_asset_identity,
            },
            "skeleton_fingerprint_sha256": skeleton_fingerprint,
            "browser_only": True,
            "blender_used": False,
            "fitting_performed": False,
            "clip_count": 30,
            "clips": rows,
            "publication": {
                "atomic_directory": True,
                "completion_marker": "motion-bridge-result.json",
                "manifest_written_last": True,
            },
        }
        manifest = staging / "motion-bridge-result.json"
        manifest.write_bytes(_canonical(result))
        # Every approval and source artifact is re-resolved immediately before
        # atomic visibility.  A changed canonical FINAL slot or PASS package
        # cannot be bridged from an earlier caller snapshot.
        refreshed = tuple(
            _resolve_final_approval(
                fitting_jobs_root=fitting_jobs_root,
                job_id=approval.job_id,
                semantic_id=approval.semantic_id,
            )
            for approval in approvals
        )
        if refreshed != approvals:
            raise BridgeError(
                "server-owned FINAL approval evidence changed during bridge"
            )
        current_skeleton = _snapshot(
            skeleton_snapshot.path, "actionless skeleton", maximum=MAX_JSON_BYTES
        )
        if current_skeleton != skeleton_snapshot:
            raise BridgeError("actionless skeleton changed during bridge")
        try:
            os.rename(staging, destination)
        except FileExistsError as exc:
            raise BridgeError(f"motion bridge output collision: {destination}") from exc
        return {
            **result,
            "clips": [
                {
                    **row,
                    "motion": _snapshot(
                        Path(row["motion"]["path"]),
                        f"{row['semantic_id']} published motion",
                    ).descriptor(),
                }
                for row in rows
            ],
            "result_manifest": _snapshot(
                destination / manifest.name, "motion bridge result"
            ).descriptor(),
        }
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _approval_jobs(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        semantic_id, separator, job_id = value.partition("=")
        if not separator or not semantic_id or semantic_id in result:
            raise BridgeError(
                "--approval-job must be repeated as unique semantic_id=job_uuid"
            )
        result[semantic_id] = _uuid(job_id, f"approval job for {semantic_id}")
    return result


def _args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-input", required=True)
    parser.add_argument("--package-input-sha256", required=True)
    parser.add_argument("--skeleton", required=True)
    parser.add_argument("--skeleton-sha256", required=True)
    parser.add_argument("--fitting-jobs-root", required=True)
    parser.add_argument("--approval-job", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _args(argv)
    try:
        result = bridge_browser_animation_motions(
            package_input=args.package_input,
            package_input_sha256=args.package_input_sha256,
            skeleton=args.skeleton,
            skeleton_sha256=args.skeleton_sha256,
            fitting_jobs_root=args.fitting_jobs_root,
            approval_job_ids=_approval_jobs(args.approval_job),
            output_dir=args.output_dir,
        )
    except Exception as exc:
        print(
            json.dumps(
                {"error_type": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
