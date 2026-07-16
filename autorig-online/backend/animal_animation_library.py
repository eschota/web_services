"""Canonical animal animation taxonomy, manifests, and durable library workflow.

This module intentionally contains no FastAPI globals.  ``main.py`` owns HTTP
routing while this module owns validation and transactional state changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlparse
import uuid

from pydantic import BaseModel, Field
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import ANIMATION_FITTING_JOBS_ROOT, ANIMATION_LIBRARY_ROOT
from database import (
    AnimalAnimationApprovedClip,
    AnimalAnimationCandidate,
    AnimalAnimationFittingJob,
    AnimalAnimationLibraryActivation,
    AnimalAnimationLibraryArtifact,
    AnimalAnimationLibraryVersion,
)


BACKEND_DIR = Path(__file__).resolve().parent
TAXONOMY_PATH = BACKEND_DIR / "animal_animation_taxonomy.v1.json"
MANIFEST_V1_SCHEMA_PATH = BACKEND_DIR / "animal_animation_manifest.v1.schema.json"
MANIFEST_V2_SCHEMA_PATH = BACKEND_DIR / "animal_animation_manifest.v2.schema.json"
MANIFEST_V1_SCHEMA_ID = "animal-animation-manifest.v1"
MANIFEST_V2_SCHEMA_ID = "animal-animation-manifest.v2"
# Backward-compatible names imported by existing callers.
MANIFEST_SCHEMA_PATH = MANIFEST_V1_SCHEMA_PATH
MANIFEST_SCHEMA_ID = MANIFEST_V1_SCHEMA_ID
CLIP_ARTIFACT_FORMAT_FBX = "fbx"
CLIP_ARTIFACT_FORMAT_THREEJS_JSON = "threejs-animation-json.v1"
SUPPORTED_CLIP_ARTIFACT_FORMATS = (
    CLIP_ARTIFACT_FORMAT_FBX,
    CLIP_ARTIFACT_FORMAT_THREEJS_JSON,
)
PACKAGE_RESULT_SCHEMA_ID = "autorig.browser-animation-glb-package-result.v1"
PACKAGE_RESULT_MAX_BYTES = 8 * 1024 * 1024
VISUAL_PHASE_QA_SCHEMA_ID = "autorig.animation-visual-phase-qa.v1"
VISUAL_PHASE_QA_VERSION = 1
VISUAL_PHASE_REQUIRED_PHASES = ("start", "middle", "three_quarter")
COINCIDENT_REST_VERTEX_MAX_THRESHOLD_M_BY_RIG = {
    # Provisional Horse calibration: the visually accepted Andalusian comparator
    # peaks around 0.032 m, while the rejected Akhal-Teke opens to 0.178 m.
    # Human fixed-camera phase review remains mandatory below this numeric cap.
    "horse": 0.04,
}
COINCIDENT_REST_VERTEX_MIN_SAMPLE_COUNT = 5
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path.name} must contain a JSON object")
    return payload


TAXONOMY: Dict[str, Any] = _load_json(TAXONOMY_PATH)
ANIMAL_RIG_TYPES = tuple(str(value) for value in TAXONOMY.get("rig_types", []))
ANIMAL_ORIENTATIONS = tuple(str(value) for value in TAXONOMY.get("orientations", []))
ANIMAL_CLIPS = tuple(dict(value) for value in TAXONOMY.get("clips", []))
ANIMAL_CLIP_IDS = tuple(str(value.get("id")) for value in ANIMAL_CLIPS)
ANIMAL_POSE_IDS = tuple(str(value.get("id")) for value in TAXONOMY.get("poses", []))
_CLIP_BY_ID = {str(value["id"]): value for value in ANIMAL_CLIPS}


class AnimationLibraryError(ValueError):
    def __init__(self, message: str, *, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _validate_taxonomy() -> None:
    if TAXONOMY.get("schema") != "animal-animation-taxonomy.v1":
        raise RuntimeError("Unsupported animal animation taxonomy schema")
    if len(ANIMAL_RIG_TYPES) != 12 or len(set(ANIMAL_RIG_TYPES)) != 12:
        raise RuntimeError("Animal taxonomy must define 12 unique rig types")
    if ANIMAL_ORIENTATIONS != ("front", "back"):
        raise RuntimeError("Animal taxonomy orientations must be front/back")
    if len(ANIMAL_CLIP_IDS) != 30 or len(set(ANIMAL_CLIP_IDS)) != 30:
        raise RuntimeError("Animal taxonomy must define exactly 30 unique clips")
    if "default_pose" in ANIMAL_CLIP_IDS:
        raise RuntimeError("default_pose is a pose, not one of the 30 clips")
    expected_orders = list(range(1, 31))
    actual_orders = [int(value.get("order", 0)) for value in ANIMAL_CLIPS]
    if actual_orders != expected_orders:
        raise RuntimeError("Animal taxonomy clip order must be exactly 1..30")
    for clip in ANIMAL_CLIPS:
        if clip.get("start_pose_id") not in ANIMAL_POSE_IDS or clip.get("end_pose_id") not in ANIMAL_POSE_IDS:
            raise RuntimeError(f"Unknown pose in taxonomy clip {clip.get('id')}")
        frame_profile = int(clip.get("frame_profile", 0))
        if frame_profile not in (33, 49, 65, 97) or (frame_profile - 1) % 8:
            raise RuntimeError(f"Invalid 8n+1 frame profile for {clip.get('id')}")


_validate_taxonomy()


def taxonomy_clip(semantic_id: str) -> dict:
    clip = _CLIP_BY_ID.get(str(semantic_id or "").strip().lower())
    if not clip:
        raise AnimationLibraryError("Unknown animal animation semantic ID")
    return dict(clip)


def _normalise_action_id(value: Any) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def canonical_animation_id(value: Any, rig_type: Optional[str] = None) -> Optional[str]:
    """Return a canonical ID using only exact IDs or declared legacy aliases."""
    normalized = _normalise_action_id(value)
    rig = str(rig_type or "").strip().lower()
    if rig and normalized.startswith(f"{rig}_"):
        normalized = normalized[len(rig) + 1 :]
    if normalized in _CLIP_BY_ID:
        return normalized
    for clip in ANIMAL_CLIPS:
        aliases = {_normalise_action_id(alias) for alias in clip.get("legacy_aliases", [])}
        if normalized in aliases:
            return str(clip["id"])
    return None


def normalize_rig_type(value: Any) -> str:
    rig_type = str(value or "").strip().lower()
    if rig_type not in ANIMAL_RIG_TYPES:
        raise AnimationLibraryError("Invalid animal rig type")
    return rig_type


def normalize_orientation(value: Any) -> str:
    orientation = str(value or "").strip().lower()
    if orientation not in ANIMAL_ORIENTATIONS:
        raise AnimationLibraryError("Invalid animal orientation")
    return orientation


def normalize_revision(value: Any) -> str:
    revision = str(value or "").strip().lower()
    if not REVISION_RE.fullmatch(revision):
        raise AnimationLibraryError("Invalid library revision")
    return revision


def normalize_sha256(value: Any, field_name: str = "sha256") -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise AnimationLibraryError(f"{field_name} must be a 64-character lowercase SHA-256")
    return digest


def normalize_clip_artifact_format(value: Any) -> str:
    clip_format = str(value or "").strip().lower()
    if clip_format not in SUPPORTED_CLIP_ARTIFACT_FORMATS:
        raise AnimationLibraryError("Unsupported fitted clip artifact format")
    return clip_format


def _normalize_provenance_pins(
    candidate_bundle_sha256: Any,
    human_review_sha256: Any,
    *,
    required: bool,
) -> tuple[Optional[str], Optional[str]]:
    bundle_value = str(candidate_bundle_sha256 or "").strip()
    review_value = str(human_review_sha256 or "").strip()
    if not bundle_value and not review_value and not required:
        return None, None
    if not bundle_value or not review_value:
        raise AnimationLibraryError(
            "candidate_bundle_sha256 and human_review_sha256 must be provided together"
        )
    return (
        normalize_sha256(bundle_value, "candidate_bundle_sha256"),
        normalize_sha256(review_value, "human_review_sha256"),
    )


@dataclass(frozen=True)
class ClipArtifactContract:
    format: str
    url: Optional[str]
    path: Optional[str]
    sha256: str
    candidate_bundle_sha256: Optional[str]
    human_review_sha256: Optional[str]


def resolve_candidate_clip_artifact(candidate: AnimalAnimationCandidate) -> ClipArtifactContract:
    """Return the authoritative generic clip contract for a candidate row."""
    clip_format = normalize_clip_artifact_format(
        getattr(candidate, "fitted_clip_format", None) or CLIP_ARTIFACT_FORMAT_FBX
    )
    url = str(candidate.fitted_clip_url or "").strip() or None
    path = str(candidate.fitted_clip_path or "").strip() or None
    if not url and not path:
        raise AnimationLibraryError("A fitted clip URL or path is required", status_code=409)
    bundle_sha, review_sha = _normalize_provenance_pins(
        getattr(candidate, "candidate_bundle_sha256", None),
        getattr(candidate, "human_review_sha256", None),
        required=clip_format != CLIP_ARTIFACT_FORMAT_FBX,
    )
    return ClipArtifactContract(
        format=clip_format,
        url=url,
        path=path,
        sha256=normalize_sha256(candidate.fitted_clip_sha256, "fitted_clip_sha256"),
        candidate_bundle_sha256=bundle_sha,
        human_review_sha256=review_sha,
    )


def resolve_approved_clip_artifact(approved: AnimalAnimationApprovedClip) -> ClipArtifactContract:
    """Resolve generic fields, falling back to legacy FBX columns for old rows."""
    clip_format = normalize_clip_artifact_format(
        getattr(approved, "clip_artifact_format", None) or CLIP_ARTIFACT_FORMAT_FBX
    )
    url = (
        str(getattr(approved, "clip_artifact_url", None) or approved.fbx_url or "").strip()
        or None
    )
    path = (
        str(getattr(approved, "clip_artifact_path", None) or approved.fbx_path or "").strip()
        or None
    )
    sha_value = getattr(approved, "clip_artifact_sha256", None) or approved.fbx_sha256
    if not url and not path:
        raise AnimationLibraryError("Approved clip URL or path is missing", status_code=409)
    bundle_sha, review_sha = _normalize_provenance_pins(
        getattr(approved, "candidate_bundle_sha256", None),
        getattr(approved, "human_review_sha256", None),
        required=clip_format != CLIP_ARTIFACT_FORMAT_FBX,
    )
    return ClipArtifactContract(
        format=clip_format,
        url=url,
        path=path,
        sha256=normalize_sha256(sha_value, "clip_artifact_sha256"),
        candidate_bundle_sha256=bundle_sha,
        human_review_sha256=review_sha,
    )


def bind_approved_clip_artifact(
    approved: AnimalAnimationApprovedClip,
    candidate: AnimalAnimationCandidate,
) -> ClipArtifactContract:
    """Copy a validated candidate contract into generic and legacy columns."""
    clip = resolve_candidate_clip_artifact(candidate)
    approved.clip_artifact_format = clip.format
    approved.clip_artifact_url = clip.url
    approved.clip_artifact_path = clip.path
    approved.clip_artifact_sha256 = clip.sha256
    approved.candidate_bundle_sha256 = clip.candidate_bundle_sha256
    approved.human_review_sha256 = clip.human_review_sha256
    # Legacy columns intentionally remain mirrors until their public API is
    # versioned away.  clip_artifact_format tells old FBX bytes from browser JSON bytes.
    approved.fbx_url = clip.url
    approved.fbx_path = clip.path
    approved.fbx_sha256 = clip.sha256
    return clip


def canonical_approved_clip_provenance(
    candidate: AnimalAnimationCandidate,
    clip: Optional[ClipArtifactContract] = None,
) -> dict:
    """Bind browser candidate provenance to its server-owned row identity."""
    contract = clip or resolve_candidate_clip_artifact(candidate)
    payload = _json_value(candidate.provenance_json, None)
    if not isinstance(payload, dict):
        raise AnimationLibraryError("Candidate provenance must be an object", status_code=409)
    if not contract.candidate_bundle_sha256 or not contract.human_review_sha256:
        return payload
    required = {
        "candidate_id": candidate.id,
        "candidate_bundle_sha256": contract.candidate_bundle_sha256,
        "human_review_sha256": contract.human_review_sha256,
    }
    for field_name, expected_value in required.items():
        existing_value = payload.get(field_name)
        if existing_value is not None and existing_value != expected_value:
            raise AnimationLibraryError(
                f"Candidate provenance {field_name} conflicts with the stored candidate",
                status_code=409,
            )
        payload[field_name] = expected_value
    return payload


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def manifest_sha256(payload: dict) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _validate_http_url(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise AnimationLibraryError(f"{field_name} must be an absolute HTTP(S) URL")
    url = str(value or "").strip()
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise AnimationLibraryError(f"{field_name} must be an absolute HTTP(S) URL")
    return url


def _require_exact_object_keys(value: Any, field_name: str, expected: Sequence[str]) -> dict:
    if not isinstance(value, dict):
        raise AnimationLibraryError(f"{field_name} must be an object", status_code=409)
    expected_keys = set(expected)
    actual_keys = set(value)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        unexpected = sorted(actual_keys - expected_keys)
        detail = []
        if missing:
            detail.append(f"missing {', '.join(missing)}")
        if unexpected:
            detail.append(f"unexpected {', '.join(unexpected)}")
        raise AnimationLibraryError(
            f"{field_name} has invalid fields ({'; '.join(detail)})",
            status_code=409,
        )
    return value


def _validate_visual_phase_sha256(value: Any, field_name: str) -> str:
    try:
        return normalize_sha256(value, field_name)
    except AnimationLibraryError as exc:
        raise AnimationLibraryError(str(exc), status_code=409) from exc


def _validate_visual_phase_url(value: Any, field_name: str) -> str:
    try:
        return _validate_http_url(value, field_name)
    except AnimationLibraryError as exc:
        raise AnimationLibraryError(str(exc), status_code=409) from exc


def validate_visual_phase_gate(
    metrics: Any,
    *,
    expected_rig_type: str,
    expected_semantic_id: str,
    expected_fitted_clip_sha256: str,
) -> dict:
    """Validate immutable, fixed-camera visual evidence for a fitted clip.

    Machine QA and numeric deformation thresholds remain separate.  This gate
    is intentionally fail-closed so a candidate cannot be approved or a
    library activated from numeric metrics alone.
    """
    if not isinstance(metrics, dict):
        raise AnimationLibraryError("Candidate metrics must be an object", status_code=409)
    metrics_object = metrics
    gate = _require_exact_object_keys(
        metrics_object.get("visual_phase_gate"),
        "metrics.visual_phase_gate",
        (
            "schema",
            "version",
            "rig_type",
            "semantic_id",
            "fitted_clip_sha256",
            "decision",
            "camera",
            "coincident_rest_vertex_separation",
            "required_phases",
            "frames",
            "reviewer",
        ),
    )
    if (
        gate["schema"] != VISUAL_PHASE_QA_SCHEMA_ID
        or not isinstance(gate["version"], int)
        or isinstance(gate["version"], bool)
    ):
        raise AnimationLibraryError("Unsupported visual phase QA schema/version", status_code=409)
    if gate["version"] != VISUAL_PHASE_QA_VERSION:
        raise AnimationLibraryError("Unsupported visual phase QA schema/version", status_code=409)

    rig_type = normalize_rig_type(expected_rig_type)
    semantic_id = str(expected_semantic_id or "").strip().lower()
    taxonomy_clip(semantic_id)
    fitted_clip_sha256 = _validate_visual_phase_sha256(expected_fitted_clip_sha256, "fitted_clip_sha256")
    if gate["rig_type"] != rig_type:
        raise AnimationLibraryError("Visual phase QA rig_type does not match the candidate", status_code=409)
    if gate["semantic_id"] != semantic_id:
        raise AnimationLibraryError("Visual phase QA semantic_id does not match the candidate", status_code=409)
    if gate["fitted_clip_sha256"] != fitted_clip_sha256:
        raise AnimationLibraryError(
            "Visual phase QA fitted_clip_sha256 does not match the candidate",
            status_code=409,
        )
    if gate["decision"] != "PASS":
        raise AnimationLibraryError("Visual phase QA decision must be PASS", status_code=409)

    camera = _require_exact_object_keys(
        gate["camera"],
        "metrics.visual_phase_gate.camera",
        ("static", "projection", "view", "root_motion_locked", "settings_sha256"),
    )
    if camera["static"] is not True:
        raise AnimationLibraryError("Visual phase QA camera must be static", status_code=409)
    if camera["root_motion_locked"] is not True:
        raise AnimationLibraryError("Visual phase QA must use root-motion lock", status_code=409)
    if camera["projection"] not in ("orthographic", "perspective"):
        raise AnimationLibraryError("Visual phase QA camera projection is invalid", status_code=409)
    view = camera["view"]
    if not isinstance(view, str) or not view.strip() or len(view) > 64 or view != view.strip():
        raise AnimationLibraryError("Visual phase QA camera view is invalid", status_code=409)
    _validate_visual_phase_sha256(camera["settings_sha256"], "visual phase camera settings_sha256")

    separation = _require_exact_object_keys(
        gate["coincident_rest_vertex_separation"],
        "metrics.visual_phase_gate.coincident_rest_vertex_separation",
        (
            "measured",
            "pass",
            "threshold_m",
            "max_separation_m",
            "sample_count",
            "group_count",
            "report_url",
            "report_sha256",
        ),
    )
    if separation["measured"] is not True:
        raise AnimationLibraryError("Coincident-rest-vertex separation must be measured", status_code=409)
    if separation["pass"] is not True:
        raise AnimationLibraryError("Coincident-rest-vertex separation gate did not pass", status_code=409)
    threshold_m = separation["threshold_m"]
    max_separation_m = separation["max_separation_m"]
    for value, field_name in (
        (threshold_m, "threshold_m"),
        (max_separation_m, "max_separation_m"),
    ):
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            raise AnimationLibraryError(
                f"Coincident-rest-vertex {field_name} must be a finite number",
                status_code=409,
            )
    server_threshold_m = COINCIDENT_REST_VERTEX_MAX_THRESHOLD_M_BY_RIG.get(rig_type)
    if server_threshold_m is None:
        raise AnimationLibraryError(
            "Coincident-rest-vertex release threshold is not calibrated for this rig type",
            status_code=409,
        )
    if float(threshold_m) <= 0 or float(threshold_m) > server_threshold_m:
        raise AnimationLibraryError(
            "Coincident-rest-vertex threshold_m exceeds the server release limit",
            status_code=409,
        )
    if float(max_separation_m) < 0 or float(max_separation_m) > float(threshold_m):
        raise AnimationLibraryError(
            "Coincident-rest-vertex max_separation_m exceeds threshold_m",
            status_code=409,
        )
    sample_count = separation["sample_count"]
    group_count = separation["group_count"]
    if (
        not isinstance(sample_count, int)
        or isinstance(sample_count, bool)
        or sample_count < COINCIDENT_REST_VERTEX_MIN_SAMPLE_COUNT
        or not isinstance(group_count, int)
        or isinstance(group_count, bool)
        or group_count < 0
    ):
        raise AnimationLibraryError(
            "Coincident-rest-vertex sample/group counts are invalid",
            status_code=409,
        )
    if group_count == 0 and float(max_separation_m) != 0:
        raise AnimationLibraryError(
            "Coincident-rest-vertex zero groups require zero separation",
            status_code=409,
        )
    _validate_visual_phase_url(separation["report_url"], "coincident-rest-vertex report_url")
    _validate_visual_phase_sha256(
        separation["report_sha256"],
        "coincident-rest-vertex report_sha256",
    )

    required_phases = gate["required_phases"]
    if required_phases != list(VISUAL_PHASE_REQUIRED_PHASES):
        raise AnimationLibraryError(
            "Visual phase QA required_phases must be start, middle, three_quarter",
            status_code=409,
        )
    frames = gate["frames"]
    if not isinstance(frames, list) or len(frames) != len(VISUAL_PHASE_REQUIRED_PHASES):
        raise AnimationLibraryError("Visual phase QA must contain all required frames", status_code=409)
    frame_indices = []
    for expected_phase, frame in zip(VISUAL_PHASE_REQUIRED_PHASES, frames):
        frame_object = _require_exact_object_keys(
            frame,
            f"metrics.visual_phase_gate.frames.{expected_phase}",
            ("phase", "frame_index", "evidence_url", "sha256"),
        )
        if frame_object["phase"] != expected_phase:
            raise AnimationLibraryError("Visual phase QA frame phases are incomplete or unordered", status_code=409)
        frame_index = frame_object["frame_index"]
        if not isinstance(frame_index, int) or isinstance(frame_index, bool) or frame_index < 0:
            raise AnimationLibraryError("Visual phase QA frame_index must be a non-negative integer", status_code=409)
        frame_indices.append(frame_index)
        _validate_visual_phase_url(frame_object["evidence_url"], "visual phase evidence_url")
        _validate_visual_phase_sha256(frame_object["sha256"], "visual phase frame sha256")
    if frame_indices != sorted(set(frame_indices)):
        raise AnimationLibraryError("Visual phase QA frame indices must be unique and increasing", status_code=409)

    reviewer = _require_exact_object_keys(
        gate["reviewer"],
        "metrics.visual_phase_gate.reviewer",
        ("id", "reviewed_at"),
    )
    reviewer_id = reviewer["id"]
    if (
        not isinstance(reviewer_id, str)
        or not reviewer_id.strip()
        or len(reviewer_id) > 320
        or reviewer_id != reviewer_id.strip()
    ):
        raise AnimationLibraryError("Visual phase QA reviewer id is invalid", status_code=409)
    reviewed_at = reviewer["reviewed_at"]
    if not isinstance(reviewed_at, str):
        raise AnimationLibraryError("Visual phase QA reviewed_at must be an ISO-8601 timestamp", status_code=409)
    try:
        parsed_reviewed_at = datetime.fromisoformat(reviewed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AnimationLibraryError(
            "Visual phase QA reviewed_at must be an ISO-8601 timestamp",
            status_code=409,
        ) from exc
    if parsed_reviewed_at.utcoffset() is None:
        raise AnimationLibraryError("Visual phase QA reviewed_at must include a timezone", status_code=409)
    return gate


def _validated_runtime_path(value: Any, root: str, field_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise AnimationLibraryError(f"{field_name} is required")
    root_path = Path(root).expanduser().resolve(strict=False)
    path = Path(raw).expanduser().resolve(strict=False)
    try:
        path.relative_to(root_path)
    except ValueError as exc:
        raise AnimationLibraryError(f"{field_name} must stay under {root_path}") from exc
    return str(path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_pinned_json_file(
    path_value: str,
    expected_sha256: str,
    field_name: str,
    *,
    max_bytes: int = PACKAGE_RESULT_MAX_BYTES,
) -> dict:
    path = Path(path_value)
    if not path.is_file():
        raise AnimationLibraryError(f"{field_name} does not exist", status_code=409)
    size = path.stat().st_size
    if size <= 0 or size > max_bytes:
        raise AnimationLibraryError(f"{field_name} has an invalid size", status_code=409)
    payload_bytes = path.read_bytes()
    if len(payload_bytes) != size:
        raise AnimationLibraryError(f"{field_name} changed while being read", status_code=409)
    if hashlib.sha256(payload_bytes).hexdigest() != expected_sha256:
        raise AnimationLibraryError(f"{field_name} SHA-256 mismatch", status_code=409)
    try:
        payload = json.loads(payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnimationLibraryError(f"{field_name} is not valid JSON", status_code=409) from exc
    if not isinstance(payload, dict):
        raise AnimationLibraryError(f"{field_name} must contain a JSON object", status_code=409)
    return payload


def _validate_glb_path(path_value: str, expected_sha256: str) -> None:
    path = Path(path_value)
    if not path.is_file():
        raise AnimationLibraryError(f"Animation GLB does not exist: {path}", status_code=409)
    size = path.stat().st_size
    if size < 12:
        raise AnimationLibraryError("Animation GLB is truncated", status_code=409)
    with path.open("rb") as stream:
        header = stream.read(12)
    if header[:4] != b"glTF" or int.from_bytes(header[4:8], "little") not in (1, 2):
        raise AnimationLibraryError("Animation artifact is not a valid GLB", status_code=409)
    if int.from_bytes(header[8:12], "little") != size:
        raise AnimationLibraryError("Animation GLB declared length does not match file size", status_code=409)
    if _file_sha256(path) != expected_sha256:
        raise AnimationLibraryError("Animation GLB SHA-256 mismatch", status_code=409)


def _read_glb_json(path: Path) -> dict:
    with path.open("rb") as stream:
        header = stream.read(12)
        if len(header) != 12 or header[:4] != b"glTF":
            raise AnimationLibraryError("Animation artifact is not a GLB", status_code=409)
        while True:
            chunk_header = stream.read(8)
            if not chunk_header:
                break
            if len(chunk_header) != 8:
                raise AnimationLibraryError("Animation GLB chunk header is truncated", status_code=409)
            chunk_length = int.from_bytes(chunk_header[:4], "little")
            chunk_type = int.from_bytes(chunk_header[4:8], "little")
            chunk_data = stream.read(chunk_length)
            if len(chunk_data) != chunk_length:
                raise AnimationLibraryError("Animation GLB chunk is truncated", status_code=409)
            if chunk_type == 0x4E4F534A:  # JSON
                try:
                    payload = json.loads(chunk_data.rstrip(b" \t\r\n\x00").decode("utf-8"))
                except Exception as exc:
                    raise AnimationLibraryError("Animation GLB JSON chunk is invalid", status_code=409) from exc
                if not isinstance(payload, dict):
                    raise AnimationLibraryError("Animation GLB JSON root must be an object", status_code=409)
                return payload
    raise AnimationLibraryError("Animation GLB has no JSON chunk", status_code=409)


def validate_glb_animation_contract(path_value: str | Path, manifest: dict) -> None:
    """Verify that the actual GLB carries the manifest's 30 named moving clips."""
    path = Path(path_value)
    gltf = _read_glb_json(path)
    if not isinstance(gltf.get("meshes"), list) or not gltf["meshes"]:
        raise AnimationLibraryError("Animation GLB must contain a mesh", status_code=409)
    if not isinstance(gltf.get("skins"), list) or not gltf["skins"]:
        raise AnimationLibraryError("Animation GLB must contain a skin", status_code=409)
    for mesh in gltf["meshes"]:
        for primitive in (mesh.get("primitives") or []) if isinstance(mesh, dict) else []:
            attributes = primitive.get("attributes") if isinstance(primitive, dict) else {}
            if isinstance(attributes, dict) and ("JOINTS_1" in attributes or "WEIGHTS_1" in attributes):
                raise AnimationLibraryError("Animation GLB exceeds four skin influences", status_code=409)

    animations = gltf.get("animations")
    if not isinstance(animations, list) or len(animations) != 30:
        raise AnimationLibraryError("Animation GLB must contain exactly 30 animations", status_code=409)
    names = [item.get("name") if isinstance(item, dict) else None for item in animations]
    if names != list(ANIMAL_CLIP_IDS) or any(name == "Animation" for name in names):
        raise AnimationLibraryError("Animation GLB clip names/order do not match the manifest", status_code=409)
    accessors = gltf.get("accessors")
    if not isinstance(accessors, list):
        raise AnimationLibraryError("Animation GLB has no animation time accessors", status_code=409)
    manifest_clips = manifest.get("clips") if isinstance(manifest, dict) else None
    if not isinstance(manifest_clips, list) or len(manifest_clips) != 30:
        raise AnimationLibraryError("Animation manifest must be validated before GLB comparison", status_code=409)
    for animation, manifest_clip in zip(animations, manifest_clips):
        samplers = animation.get("samplers") if isinstance(animation, dict) else None
        if not isinstance(samplers, list) or not samplers:
            raise AnimationLibraryError(f"GLB clip {animation.get('name')} has no moving sampler", status_code=409)
        duration = 0.0
        for sampler in samplers:
            accessor_index = sampler.get("input") if isinstance(sampler, dict) else None
            if isinstance(accessor_index, bool) or not isinstance(accessor_index, int):
                raise AnimationLibraryError("Animation sampler has no valid input accessor", status_code=409)
            if accessor_index < 0 or accessor_index >= len(accessors):
                raise AnimationLibraryError("Animation sampler input accessor is out of range", status_code=409)
            accessor = accessors[accessor_index]
            maximum = accessor.get("max") if isinstance(accessor, dict) else None
            minimum = accessor.get("min") if isinstance(accessor, dict) else None
            if isinstance(maximum, list) and maximum:
                try:
                    start_time = float(minimum[0]) if isinstance(minimum, list) and minimum else 0.0
                    duration = max(duration, float(maximum[0]) - start_time)
                except (TypeError, ValueError):
                    pass
        if duration <= 0:
            raise AnimationLibraryError(f"GLB clip {animation.get('name')} has zero duration", status_code=409)
        if abs(duration - float(manifest_clip["duration"])) > max(1e-4, 1.0 / float(manifest_clip["fps"])):
            raise AnimationLibraryError(
                f"GLB duration differs from manifest clip {animation.get('name')}",
                status_code=409,
            )


def _validate_animation_manifest_v1(
    manifest: Any,
    *,
    expected_revision: Optional[str] = None,
    expected_rig_type: Optional[str] = None,
    expected_orientation: Optional[str] = None,
    expected_artifact_sha256: Optional[str] = None,
    expected_template_skeleton_sha256: Optional[str] = None,
) -> dict:
    """Validate the unchanged, FBX-only v1 manifest contract."""
    if not isinstance(manifest, dict):
        raise AnimationLibraryError("Animation manifest must be an object")
    payload = json.loads(json.dumps(manifest))
    allowed_top_level = {
        "schema",
        "library_revision",
        "rig_type",
        "orientation",
        "template_skeleton_sha256",
        "artifact_sha256",
        "clips",
        "poses",
    }
    if set(payload) != allowed_top_level:
        raise AnimationLibraryError("Animation manifest top-level fields do not match v1 schema")
    if payload.get("schema") != MANIFEST_SCHEMA_ID:
        raise AnimationLibraryError("Unsupported animation manifest schema")
    for field_name in (
        "library_revision",
        "rig_type",
        "orientation",
        "template_skeleton_sha256",
        "artifact_sha256",
    ):
        if not isinstance(payload.get(field_name), str):
            raise AnimationLibraryError(f"{field_name} must be a string")

    revision = normalize_revision(payload.get("library_revision"))
    rig_type = normalize_rig_type(payload.get("rig_type"))
    orientation = normalize_orientation(payload.get("orientation"))
    skeleton_sha = normalize_sha256(payload.get("template_skeleton_sha256"), "template_skeleton_sha256")
    artifact_sha = normalize_sha256(payload.get("artifact_sha256"), "artifact_sha256")
    if expected_revision and revision != normalize_revision(expected_revision):
        raise AnimationLibraryError("Manifest library revision does not match the selected revision", status_code=409)
    if expected_rig_type and rig_type != normalize_rig_type(expected_rig_type):
        raise AnimationLibraryError("Manifest rig type does not match the selected variant", status_code=409)
    if expected_orientation and orientation != normalize_orientation(expected_orientation):
        raise AnimationLibraryError("Manifest orientation does not match the selected variant", status_code=409)
    if expected_artifact_sha256 and artifact_sha != normalize_sha256(expected_artifact_sha256, "artifact_sha256"):
        raise AnimationLibraryError("Manifest artifact SHA-256 does not match matrix metadata", status_code=409)
    if expected_template_skeleton_sha256 and skeleton_sha != normalize_sha256(
        expected_template_skeleton_sha256, "template_skeleton_sha256"
    ):
        raise AnimationLibraryError("Manifest template skeleton SHA-256 does not match the library", status_code=409)

    poses = payload.get("poses")
    if not isinstance(poses, list):
        raise AnimationLibraryError("Manifest poses must be an array")
    if any(not isinstance(item, dict) or not isinstance(item.get("id"), str) for item in poses):
        raise AnimationLibraryError("Every manifest pose must have a string ID")
    pose_ids = [item["id"] for item in poses]
    if len(pose_ids) != len(set(pose_ids)) or not set(ANIMAL_POSE_IDS).issubset(set(pose_ids)):
        raise AnimationLibraryError("Manifest must contain unique required pose IDs")

    clips = payload.get("clips")
    if not isinstance(clips, list) or len(clips) != 30:
        raise AnimationLibraryError("Manifest must contain exactly 30 clips")
    if any(not isinstance(item, dict) for item in clips):
        raise AnimationLibraryError("Every manifest clip must be an object")
    if [item.get("id") for item in clips] != list(ANIMAL_CLIP_IDS):
        raise AnimationLibraryError("Manifest clips must match canonical IDs and order exactly")
    for item, canonical in zip(clips, ANIMAL_CLIPS):
        if str(item.get("category") or "") != canonical["category"]:
            raise AnimationLibraryError(f"Category mismatch for {canonical['id']}")
        if isinstance(item.get("order"), bool) or not isinstance(item.get("order"), int):
            raise AnimationLibraryError(f"Order must be an integer for {canonical['id']}")
        if item["order"] != int(canonical["order"]):
            raise AnimationLibraryError(f"Order mismatch for {canonical['id']}")
        if item.get("loop") is not bool(canonical["loop"]):
            raise AnimationLibraryError(f"Loop policy mismatch for {canonical['id']}")
        if str(item.get("start_pose_id") or "") != canonical["start_pose_id"]:
            raise AnimationLibraryError(f"Start pose mismatch for {canonical['id']}")
        if str(item.get("end_pose_id") or "") != canonical["end_pose_id"]:
            raise AnimationLibraryError(f"End pose mismatch for {canonical['id']}")
        if (
            isinstance(item.get("duration"), bool)
            or not isinstance(item.get("duration"), (int, float))
            or isinstance(item.get("fps"), bool)
            or not isinstance(item.get("fps"), (int, float))
        ):
            raise AnimationLibraryError(f"Invalid duration/fps for {canonical['id']}")
        duration = float(item["duration"])
        fps = float(item["fps"])
        if duration <= 0 or fps <= 0:
            raise AnimationLibraryError(f"Duration/fps must be positive for {canonical['id']}")
        if not isinstance(item.get("root_motion_available"), bool):
            raise AnimationLibraryError(f"root_motion_available must be boolean for {canonical['id']}")
        if not isinstance(item.get("qa_profile_revision"), str) or not item["qa_profile_revision"].strip():
            raise AnimationLibraryError(f"qa_profile_revision is required for {canonical['id']}")
        if not isinstance(item.get("provenance"), dict):
            raise AnimationLibraryError(f"provenance is required for {canonical['id']}")
        if not isinstance(item.get("fbx_url"), str) or not item["fbx_url"].strip():
            raise AnimationLibraryError(f"fbx_url is required for {canonical['id']}")

    payload["library_revision"] = revision
    payload["rig_type"] = rig_type
    payload["orientation"] = orientation
    payload["template_skeleton_sha256"] = skeleton_sha
    payload["artifact_sha256"] = artifact_sha
    return payload


def _validate_animation_manifest_v2(
    manifest: Any,
    *,
    expected_revision: Optional[str] = None,
    expected_rig_type: Optional[str] = None,
    expected_orientation: Optional[str] = None,
    expected_artifact_sha256: Optional[str] = None,
    expected_template_skeleton_sha256: Optional[str] = None,
    expected_package_result_sha256: Optional[str] = None,
) -> dict:
    """Validate browser-native clip artifacts and their immutable pins."""
    if not isinstance(manifest, dict):
        raise AnimationLibraryError("Animation manifest must be an object")
    payload = json.loads(json.dumps(manifest))
    allowed_top_level = {
        "schema",
        "library_revision",
        "rig_type",
        "orientation",
        "template_skeleton_sha256",
        "artifact_sha256",
        "package_result_sha256",
        "clips",
        "poses",
    }
    if set(payload) != allowed_top_level:
        raise AnimationLibraryError("Animation manifest top-level fields do not match v2 schema")
    if payload.get("schema") != MANIFEST_V2_SCHEMA_ID:
        raise AnimationLibraryError("Unsupported animation manifest schema")
    for field_name in (
        "library_revision",
        "rig_type",
        "orientation",
        "template_skeleton_sha256",
        "artifact_sha256",
        "package_result_sha256",
    ):
        if not isinstance(payload.get(field_name), str):
            raise AnimationLibraryError(f"{field_name} must be a string")

    revision = normalize_revision(payload["library_revision"])
    rig_type = normalize_rig_type(payload["rig_type"])
    orientation = normalize_orientation(payload["orientation"])
    skeleton_sha = normalize_sha256(
        payload["template_skeleton_sha256"], "template_skeleton_sha256"
    )
    artifact_sha = normalize_sha256(payload["artifact_sha256"], "artifact_sha256")
    package_result_sha = normalize_sha256(
        payload["package_result_sha256"], "package_result_sha256"
    )
    if expected_revision and revision != normalize_revision(expected_revision):
        raise AnimationLibraryError(
            "Manifest library revision does not match the selected revision",
            status_code=409,
        )
    if expected_rig_type and rig_type != normalize_rig_type(expected_rig_type):
        raise AnimationLibraryError(
            "Manifest rig type does not match the selected variant", status_code=409
        )
    if expected_orientation and orientation != normalize_orientation(expected_orientation):
        raise AnimationLibraryError(
            "Manifest orientation does not match the selected variant", status_code=409
        )
    if expected_artifact_sha256 and artifact_sha != normalize_sha256(
        expected_artifact_sha256, "artifact_sha256"
    ):
        raise AnimationLibraryError(
            "Manifest artifact SHA-256 does not match matrix metadata", status_code=409
        )
    if expected_template_skeleton_sha256 and skeleton_sha != normalize_sha256(
        expected_template_skeleton_sha256, "template_skeleton_sha256"
    ):
        raise AnimationLibraryError(
            "Manifest template skeleton SHA-256 does not match the library",
            status_code=409,
        )
    if expected_package_result_sha256 and package_result_sha != normalize_sha256(
        expected_package_result_sha256, "package_result_sha256"
    ):
        raise AnimationLibraryError(
            "Manifest package-result SHA-256 does not match artifact metadata",
            status_code=409,
        )

    poses = payload.get("poses")
    if not isinstance(poses, list):
        raise AnimationLibraryError("Manifest poses must be an array")
    if any(not isinstance(item, dict) or not isinstance(item.get("id"), str) for item in poses):
        raise AnimationLibraryError("Every manifest pose must have a string ID")
    pose_ids = [item["id"] for item in poses]
    if len(pose_ids) != len(set(pose_ids)) or not set(ANIMAL_POSE_IDS).issubset(set(pose_ids)):
        raise AnimationLibraryError("Manifest must contain unique required pose IDs")

    required_clip_keys = {
        "id",
        "category",
        "order",
        "loop",
        "duration",
        "fps",
        "start_pose_id",
        "end_pose_id",
        "root_motion_available",
        "qa_profile_revision",
        "provenance",
        "clip_artifact",
    }
    clips = payload.get("clips")
    if not isinstance(clips, list) or len(clips) != 30:
        raise AnimationLibraryError("Manifest must contain exactly 30 clips")
    if any(not isinstance(item, dict) for item in clips):
        raise AnimationLibraryError("Every manifest clip must be an object")
    if [item.get("id") for item in clips] != list(ANIMAL_CLIP_IDS):
        raise AnimationLibraryError("Manifest clips must match canonical IDs and order exactly")
    clip_hashes: set[str] = set()
    candidate_ids: set[str] = set()
    candidate_bundle_hashes: set[str] = set()
    human_review_hashes: set[str] = set()
    for item, canonical in zip(clips, ANIMAL_CLIPS):
        if set(item) != required_clip_keys:
            raise AnimationLibraryError(f"Clip fields do not match v2 schema for {canonical['id']}")
        if item["category"] != canonical["category"]:
            raise AnimationLibraryError(f"Category mismatch for {canonical['id']}")
        if isinstance(item["order"], bool) or not isinstance(item["order"], int):
            raise AnimationLibraryError(f"Order must be an integer for {canonical['id']}")
        if item["order"] != int(canonical["order"]):
            raise AnimationLibraryError(f"Order mismatch for {canonical['id']}")
        if item["loop"] is not bool(canonical["loop"]):
            raise AnimationLibraryError(f"Loop policy mismatch for {canonical['id']}")
        if item["start_pose_id"] != canonical["start_pose_id"]:
            raise AnimationLibraryError(f"Start pose mismatch for {canonical['id']}")
        if item["end_pose_id"] != canonical["end_pose_id"]:
            raise AnimationLibraryError(f"End pose mismatch for {canonical['id']}")
        if (
            isinstance(item["duration"], bool)
            or not isinstance(item["duration"], (int, float))
            or isinstance(item["fps"], bool)
            or not isinstance(item["fps"], (int, float))
        ):
            raise AnimationLibraryError(f"Invalid duration/fps for {canonical['id']}")
        duration = float(item["duration"])
        fps = float(item["fps"])
        if not math.isfinite(duration) or not math.isfinite(fps) or duration <= 0 or fps <= 0:
            raise AnimationLibraryError(f"Duration/fps must be positive for {canonical['id']}")
        if not isinstance(item["root_motion_available"], bool):
            raise AnimationLibraryError(
                f"root_motion_available must be boolean for {canonical['id']}"
            )
        if (
            not isinstance(item["qa_profile_revision"], str)
            or not item["qa_profile_revision"].strip()
        ):
            raise AnimationLibraryError(f"qa_profile_revision is required for {canonical['id']}")

        clip_artifact = item["clip_artifact"]
        if not isinstance(clip_artifact, dict) or set(clip_artifact) not in (
            {"format", "sha256"},
            {"format", "url", "sha256"},
        ):
            raise AnimationLibraryError(
                f"clip_artifact fields are invalid for {canonical['id']}"
            )
        clip_artifact["format"] = normalize_clip_artifact_format(clip_artifact["format"])
        if "url" in clip_artifact:
            clip_artifact["url"] = _validate_http_url(
                clip_artifact["url"], f"clips.{canonical['id']}.clip_artifact.url"
            )
        clip_artifact["sha256"] = normalize_sha256(
            clip_artifact["sha256"], f"clips.{canonical['id']}.clip_artifact.sha256"
        )
        if clip_artifact["sha256"] in clip_hashes:
            raise AnimationLibraryError("Manifest repeats a fitted clip SHA-256")
        clip_hashes.add(clip_artifact["sha256"])

        provenance = item["provenance"]
        if not isinstance(provenance, dict):
            raise AnimationLibraryError(f"provenance is required for {canonical['id']}")
        for key in ("candidate_id", "candidate_bundle_sha256", "human_review_sha256"):
            if key not in provenance:
                raise AnimationLibraryError(
                    f"provenance.{key} is required for {canonical['id']}"
                )
        candidate_id = provenance["candidate_id"]
        try:
            canonical_candidate_id = str(uuid.UUID(str(candidate_id)))
        except (ValueError, AttributeError, TypeError) as exc:
            raise AnimationLibraryError(
                f"provenance.candidate_id is invalid for {canonical['id']}"
            ) from exc
        if candidate_id != canonical_candidate_id:
            raise AnimationLibraryError(
                f"provenance.candidate_id must be canonical for {canonical['id']}"
            )
        if canonical_candidate_id in candidate_ids:
            raise AnimationLibraryError("Manifest repeats a candidate_id")
        candidate_ids.add(canonical_candidate_id)
        provenance["candidate_bundle_sha256"], provenance["human_review_sha256"] = (
            _normalize_provenance_pins(
                provenance["candidate_bundle_sha256"],
                provenance["human_review_sha256"],
                required=True,
            )
        )
        if provenance["candidate_bundle_sha256"] in candidate_bundle_hashes:
            raise AnimationLibraryError("Manifest repeats a candidate bundle SHA-256")
        if provenance["human_review_sha256"] in human_review_hashes:
            raise AnimationLibraryError("Manifest repeats a human review SHA-256")
        candidate_bundle_hashes.add(provenance["candidate_bundle_sha256"])
        human_review_hashes.add(provenance["human_review_sha256"])

    payload["library_revision"] = revision
    payload["rig_type"] = rig_type
    payload["orientation"] = orientation
    payload["template_skeleton_sha256"] = skeleton_sha
    payload["artifact_sha256"] = artifact_sha
    payload["package_result_sha256"] = package_result_sha
    return payload


def validate_animation_manifest(
    manifest: Any,
    *,
    expected_revision: Optional[str] = None,
    expected_rig_type: Optional[str] = None,
    expected_orientation: Optional[str] = None,
    expected_artifact_sha256: Optional[str] = None,
    expected_template_skeleton_sha256: Optional[str] = None,
    expected_package_result_sha256: Optional[str] = None,
) -> dict:
    """Dispatch to an explicitly supported manifest schema, failing closed."""
    if not isinstance(manifest, dict):
        raise AnimationLibraryError("Animation manifest must be an object")
    schema = manifest.get("schema")
    common = {
        "expected_revision": expected_revision,
        "expected_rig_type": expected_rig_type,
        "expected_orientation": expected_orientation,
        "expected_artifact_sha256": expected_artifact_sha256,
        "expected_template_skeleton_sha256": expected_template_skeleton_sha256,
    }
    if schema == MANIFEST_V1_SCHEMA_ID:
        if expected_package_result_sha256:
            raise AnimationLibraryError(
                "A v1 manifest cannot bind a package-result artifact", status_code=409
            )
        return _validate_animation_manifest_v1(manifest, **common)
    if schema == MANIFEST_V2_SCHEMA_ID:
        return _validate_animation_manifest_v2(
            manifest,
            **common,
            expected_package_result_sha256=expected_package_result_sha256,
        )
    raise AnimationLibraryError("Unsupported animation manifest schema")


@dataclass(frozen=True)
class MatrixAnimationArtifact:
    rig_type: str
    orientation: str
    library_revision: str
    animation_manifest_url: str
    animation_glb_url: str
    animation_clip_count: int
    animation_glb_sha256: str
    animation_manifest_sha256: Optional[str] = None


def parse_matrix_animation_artifact(
    row: Any,
    *,
    rig_type: str,
    orientation: str,
    expected_revision: Optional[str] = None,
) -> MatrixAnimationArtifact:
    """Parse only the authoritative v1 matrix fields; no basename guessing."""
    if not isinstance(row, dict):
        raise AnimationLibraryError("Animal variant matrix row is missing", status_code=404)
    rig = normalize_rig_type(rig_type)
    orient = normalize_orientation(orientation)
    if not isinstance(row.get("animation_library_revision"), str):
        raise AnimationLibraryError("animation_library_revision is missing from variant matrix", status_code=404)
    revision = normalize_revision(row.get("animation_library_revision"))
    if expected_revision and revision != normalize_revision(expected_revision):
        raise AnimationLibraryError("Task variant was built with a different animation library revision", status_code=409)
    clip_count = row.get("animation_clip_count")
    if isinstance(clip_count, bool) or not isinstance(clip_count, int):
        raise AnimationLibraryError("animation_clip_count is missing from variant matrix", status_code=404)
    if clip_count != 30:
        raise AnimationLibraryError("Animal variant matrix must declare exactly 30 clips", status_code=409)
    raw_manifest_digest = row.get("animation_manifest_sha256")
    if raw_manifest_digest is not None and not isinstance(raw_manifest_digest, str):
        raise AnimationLibraryError("animation_manifest_sha256 must be a string")
    manifest_digest = str(raw_manifest_digest or "").strip().lower() or None
    if manifest_digest:
        manifest_digest = normalize_sha256(manifest_digest, "animation_manifest_sha256")
    if not isinstance(row.get("animation_glb_sha256"), str):
        raise AnimationLibraryError("animation_glb_sha256 is missing from variant matrix", status_code=404)
    return MatrixAnimationArtifact(
        rig_type=rig,
        orientation=orient,
        library_revision=revision,
        animation_manifest_url=_validate_http_url(row.get("animation_manifest_url"), "animation_manifest_url"),
        animation_glb_url=_validate_http_url(row.get("animation_glb_url"), "animation_glb_url"),
        animation_clip_count=clip_count,
        animation_glb_sha256=normalize_sha256(row.get("animation_glb_sha256"), "animation_glb_sha256"),
        animation_manifest_sha256=manifest_digest,
    )


class AnimationLibraryCreateRequest(BaseModel):
    rig_type: str
    revision: str
    template_skeleton_sha256: str
    qa_profile_revision: str = Field(min_length=1, max_length=128)
    notes: Optional[str] = Field(default=None, max_length=4000)


class AnimationLibraryArtifactPutRequest(BaseModel):
    manifest: Dict[str, Any]
    manifest_sha256: Optional[str] = None
    animation_glb_url: Optional[str] = None
    animation_glb_path: str
    artifact_sha256: str
    package_zip_url: Optional[str] = None
    package_zip_path: Optional[str] = None
    package_result_path: Optional[str] = None
    package_result_sha256: Optional[str] = None


class AnimationFittingJobCreateRequest(BaseModel):
    rig_type: str
    semantic_id: str
    library_revision: str
    workflow_name: str = Field(min_length=1, max_length=128)
    workflow_fingerprint: str = Field(min_length=1, max_length=128)
    worker_url: str
    prompt: str = Field(min_length=1, max_length=12000)
    prompt_id: Optional[str] = Field(default=None, max_length=128)
    candidate_target: int = Field(default=8, ge=1, le=16)
    candidate_limit: int = Field(default=16, ge=1, le=16)
    config: Dict[str, Any] = Field(default_factory=dict)


class AnimationCandidateCreateRequest(BaseModel):
    seed: int
    status: str = Field(default="qa_complete", max_length=32)
    raw_video_url: Optional[str] = None
    raw_video_path: Optional[str] = None
    decoded_frames_path: Optional[str] = None
    fitted_clip_url: Optional[str] = None
    fitted_clip_path: Optional[str] = None
    fitted_clip_sha256: str
    fitted_clip_format: str = CLIP_ARTIFACT_FORMAT_FBX
    candidate_bundle_sha256: Optional[str] = None
    human_review_sha256: Optional[str] = None
    duration: float = Field(gt=0)
    fps: float = Field(gt=0)
    root_motion_available: bool = False
    metrics: Dict[str, Any] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)
    rank_score: Optional[float] = None
    rank: Optional[int] = Field(default=None, ge=1, le=16)
    qa_passed: bool = False


class AnimationCandidateDecisionRequest(BaseModel):
    decision: str
    reason: Optional[str] = Field(default=None, max_length=4000)


class AnimationLibraryRollbackRequest(BaseModel):
    target_revision: Optional[str] = None


def _json_text(payload: Any) -> str:
    return canonical_json_bytes(payload).decode("utf-8")


def _json_value(payload: Optional[str], default: Any) -> Any:
    try:
        return json.loads(payload or "")
    except Exception:
        return default


async def find_library_version(db: AsyncSession, rig_type: str, revision: str) -> AnimalAnimationLibraryVersion:
    rig = normalize_rig_type(rig_type)
    rev = normalize_revision(revision)
    result = await db.execute(
        select(AnimalAnimationLibraryVersion).where(
            AnimalAnimationLibraryVersion.rig_type == rig,
            AnimalAnimationLibraryVersion.revision == rev,
        )
    )
    version = result.scalar_one_or_none()
    if not version:
        raise AnimationLibraryError("Animation library revision not found", status_code=404)
    return version


async def create_library_version(
    db: AsyncSession,
    request: AnimationLibraryCreateRequest,
    *,
    admin_email: str,
) -> AnimalAnimationLibraryVersion:
    rig = normalize_rig_type(request.rig_type)
    revision = normalize_revision(request.revision)
    skeleton_sha = normalize_sha256(request.template_skeleton_sha256, "template_skeleton_sha256")
    existing = await db.execute(
        select(AnimalAnimationLibraryVersion.id).where(
            AnimalAnimationLibraryVersion.rig_type == rig,
            AnimalAnimationLibraryVersion.revision == revision,
        )
    )
    if existing.scalar_one_or_none() is not None:
        raise AnimationLibraryError("Animation library revision already exists", status_code=409)
    version = AnimalAnimationLibraryVersion(
        rig_type=rig,
        revision=revision,
        status="draft",
        template_skeleton_sha256=skeleton_sha,
        qa_profile_revision=str(request.qa_profile_revision).strip(),
        notes=request.notes,
        created_by=admin_email,
    )
    db.add(version)
    await db.commit()
    await db.refresh(version)
    return version


async def put_library_artifact(
    db: AsyncSession,
    *,
    rig_type: str,
    revision: str,
    orientation: str,
    request: AnimationLibraryArtifactPutRequest,
    library_root: str = ANIMATION_LIBRARY_ROOT,
) -> AnimalAnimationLibraryArtifact:
    version = await find_library_version(db, rig_type, revision)
    if version.status != "draft":
        raise AnimationLibraryError("Published library revisions are immutable", status_code=409)
    orient = normalize_orientation(orientation)
    artifact_sha = normalize_sha256(request.artifact_sha256, "artifact_sha256")
    manifest_schema = request.manifest.get("schema") if isinstance(request.manifest, dict) else None
    package_result_path = None
    package_result_sha = None
    if manifest_schema == MANIFEST_V2_SCHEMA_ID:
        if not request.package_result_path or not request.package_result_sha256:
            raise AnimationLibraryError(
                "A v2 artifact requires package_result_path and package_result_sha256"
            )
        package_result_sha = normalize_sha256(
            request.package_result_sha256, "package_result_sha256"
        )
        package_result_path = _validated_runtime_path(
            request.package_result_path, library_root, "package_result_path"
        )
        package_result = _read_pinned_json_file(
            package_result_path, package_result_sha, "package_result_path"
        )
        if package_result.get("schema") != PACKAGE_RESULT_SCHEMA_ID:
            raise AnimationLibraryError("Unsupported package-result schema", status_code=409)
    elif request.package_result_path or request.package_result_sha256:
        raise AnimationLibraryError("Only a v2 manifest may bind a package-result artifact")
    manifest = validate_animation_manifest(
        request.manifest,
        expected_revision=version.revision,
        expected_rig_type=version.rig_type,
        expected_orientation=orient,
        expected_artifact_sha256=artifact_sha,
        expected_template_skeleton_sha256=version.template_skeleton_sha256,
        expected_package_result_sha256=package_result_sha,
    )
    calculated_manifest_sha = manifest_sha256(manifest)
    if request.manifest_sha256 and normalize_sha256(request.manifest_sha256, "manifest_sha256") != calculated_manifest_sha:
        raise AnimationLibraryError("Manifest SHA-256 mismatch")
    glb_path = _validated_runtime_path(request.animation_glb_path, library_root, "animation_glb_path")
    glb_url = _validate_http_url(request.animation_glb_url, "animation_glb_url") if request.animation_glb_url else None
    zip_path = None
    if request.package_zip_path:
        zip_path = _validated_runtime_path(request.package_zip_path, library_root, "package_zip_path")
    zip_url = _validate_http_url(request.package_zip_url, "package_zip_url") if request.package_zip_url else None

    result = await db.execute(
        select(AnimalAnimationLibraryArtifact).where(
            AnimalAnimationLibraryArtifact.library_version_id == version.id,
            AnimalAnimationLibraryArtifact.orientation == orient,
        )
    )
    artifact = result.scalar_one_or_none()
    if artifact is None:
        artifact = AnimalAnimationLibraryArtifact(library_version_id=version.id, orientation=orient)
        db.add(artifact)
    artifact.manifest_json = _json_text(manifest)
    artifact.manifest_sha256 = calculated_manifest_sha
    artifact.animation_glb_url = glb_url
    artifact.animation_glb_path = glb_path
    artifact.artifact_sha256 = artifact_sha
    artifact.animation_clip_count = 30
    artifact.package_zip_url = zip_url
    artifact.package_zip_path = zip_path
    artifact.package_result_path = package_result_path
    artifact.package_result_sha256 = package_result_sha
    artifact.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(artifact)
    return artifact


async def create_fitting_job(
    db: AsyncSession,
    request: AnimationFittingJobCreateRequest,
    *,
    admin_email: str,
) -> AnimalAnimationFittingJob:
    version = await find_library_version(db, request.rig_type, request.library_revision)
    if version.status != "draft":
        raise AnimationLibraryError("Fitting jobs can only target a draft library", status_code=409)
    clip = taxonomy_clip(request.semantic_id)
    if request.candidate_target > request.candidate_limit:
        raise AnimationLibraryError("candidate_target cannot exceed candidate_limit")
    worker_url = _validate_http_url(request.worker_url, "worker_url")
    prompt_id = str(request.prompt_id or "").strip()
    if not prompt_id:
        source = "\n".join(
            (version.revision, version.rig_type, clip["id"], request.workflow_fingerprint, request.prompt)
        ).encode("utf-8")
        prompt_id = hashlib.sha256(source).hexdigest()
    elif len(prompt_id) > 128:
        raise AnimationLibraryError("prompt_id is too long")
    duplicate = await db.execute(
        select(AnimalAnimationFittingJob).where(AnimalAnimationFittingJob.prompt_id == prompt_id)
    )
    existing = duplicate.scalar_one_or_none()
    if existing:
        expected = (
            version.id,
            version.rig_type,
            clip["id"],
            str(request.workflow_name).strip(),
            str(request.workflow_fingerprint).strip(),
            worker_url,
            request.prompt.strip(),
            request.candidate_target,
            request.candidate_limit,
            _json_text(request.config),
        )
        actual = (
            existing.library_version_id,
            existing.rig_type,
            existing.semantic_id,
            existing.workflow_name,
            existing.workflow_fingerprint,
            existing.worker_url,
            existing.prompt,
            existing.candidate_target,
            existing.candidate_limit,
            existing.config_json,
        )
        if actual != expected:
            raise AnimationLibraryError("prompt_id is already pinned to a different fitting job", status_code=409)
        return existing
    job = AnimalAnimationFittingJob(
        id=str(uuid.uuid4()),
        library_version_id=version.id,
        rig_type=version.rig_type,
        semantic_id=clip["id"],
        status="queued",
        workflow_name=str(request.workflow_name).strip(),
        workflow_fingerprint=str(request.workflow_fingerprint).strip(),
        worker_url=worker_url,
        prompt_id=prompt_id,
        prompt=request.prompt.strip(),
        candidate_target=request.candidate_target,
        candidate_limit=request.candidate_limit,
        config_json=_json_text(request.config),
        created_by=admin_email,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job


async def add_fitting_candidate(
    db: AsyncSession,
    *,
    job_id: str,
    request: AnimationCandidateCreateRequest,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
) -> AnimalAnimationCandidate:
    result = await db.execute(select(AnimalAnimationFittingJob).where(AnimalAnimationFittingJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise AnimationLibraryError("Fitting job not found", status_code=404)
    fitted_sha = normalize_sha256(request.fitted_clip_sha256, "fitted_clip_sha256")
    fitted_format = normalize_clip_artifact_format(request.fitted_clip_format)
    candidate_bundle_sha, human_review_sha = _normalize_provenance_pins(
        request.candidate_bundle_sha256,
        request.human_review_sha256,
        required=fitted_format != CLIP_ARTIFACT_FORMAT_FBX,
    )
    if not request.fitted_clip_url and not request.fitted_clip_path:
        raise AnimationLibraryError("A fitted clip URL or path is required")
    if fitted_format == CLIP_ARTIFACT_FORMAT_THREEJS_JSON and not request.fitted_clip_path:
        raise AnimationLibraryError(
            "A browser clip requires a server-pinned fitted_clip_path"
        )
    raw_video_url = _validate_http_url(request.raw_video_url, "raw_video_url") if request.raw_video_url else None
    fitted_clip_url = _validate_http_url(request.fitted_clip_url, "fitted_clip_url") if request.fitted_clip_url else None
    raw_video_path = (
        _validated_runtime_path(request.raw_video_path, fitting_jobs_root, "raw_video_path")
        if request.raw_video_path else None
    )
    decoded_frames_path = (
        _validated_runtime_path(request.decoded_frames_path, fitting_jobs_root, "decoded_frames_path")
        if request.decoded_frames_path else None
    )
    fitted_clip_path = (
        _validated_runtime_path(request.fitted_clip_path, fitting_jobs_root, "fitted_clip_path")
        if request.fitted_clip_path else None
    )
    if raw_video_path and not Path(raw_video_path).is_file():
        raise AnimationLibraryError("raw_video_path does not exist", status_code=409)
    if decoded_frames_path and not Path(decoded_frames_path).is_dir():
        raise AnimationLibraryError("decoded_frames_path does not exist", status_code=409)
    if fitted_clip_path:
        if not Path(fitted_clip_path).is_file():
            raise AnimationLibraryError("fitted_clip_path does not exist", status_code=409)
        if _file_sha256(Path(fitted_clip_path)) != fitted_sha:
            raise AnimationLibraryError("Fitted clip SHA-256 mismatch", status_code=409)
    duplicate = await db.execute(
        select(AnimalAnimationCandidate).where(
            AnimalAnimationCandidate.job_id == job.id,
            AnimalAnimationCandidate.seed == request.seed,
        )
    )
    existing = duplicate.scalar_one_or_none()
    if existing:
        expected = (
            raw_video_url,
            raw_video_path,
            decoded_frames_path,
            fitted_clip_url,
            fitted_clip_path,
            fitted_sha,
            fitted_format,
            candidate_bundle_sha,
            human_review_sha,
            float(request.duration),
            float(request.fps),
            bool(request.root_motion_available),
            _json_text(request.metrics),
            _json_text(request.provenance),
            request.rank_score,
            request.rank,
            bool(request.qa_passed),
        )
        actual = (
            existing.raw_video_url,
            existing.raw_video_path,
            existing.decoded_frames_path,
            existing.fitted_clip_url,
            existing.fitted_clip_path,
            existing.fitted_clip_sha256,
            existing.fitted_clip_format or CLIP_ARTIFACT_FORMAT_FBX,
            existing.candidate_bundle_sha256,
            existing.human_review_sha256,
            float(existing.duration or 0),
            float(existing.fps or 0),
            bool(existing.root_motion_available),
            existing.metrics_json,
            existing.provenance_json,
            existing.rank_score,
            existing.rank,
            bool(existing.qa_passed),
        )
        if actual != expected:
            raise AnimationLibraryError("Candidate seed is already pinned to different artifacts", status_code=409)
        return existing
    if request.rank is not None:
        ranked_result = await db.execute(
            select(AnimalAnimationCandidate.id).where(
                AnimalAnimationCandidate.job_id == job.id,
                AnimalAnimationCandidate.rank == request.rank,
            )
        )
        if ranked_result.scalar_one_or_none() is not None:
            raise AnimationLibraryError("Candidate rank is already assigned in this job", status_code=409)
    count_result = await db.execute(
        select(AnimalAnimationCandidate.id).where(AnimalAnimationCandidate.job_id == job.id)
    )
    current_count = len(count_result.scalars().all())
    if current_count >= int(job.candidate_limit):
        raise AnimationLibraryError("Candidate limit reached", status_code=409)
    candidate = AnimalAnimationCandidate(
        id=str(uuid.uuid4()),
        job_id=job.id,
        seed=request.seed,
        status=request.status,
        raw_video_url=raw_video_url,
        raw_video_path=raw_video_path,
        decoded_frames_path=decoded_frames_path,
        fitted_clip_url=fitted_clip_url,
        fitted_clip_path=fitted_clip_path,
        fitted_clip_sha256=fitted_sha,
        fitted_clip_format=fitted_format,
        candidate_bundle_sha256=candidate_bundle_sha,
        human_review_sha256=human_review_sha,
        duration=request.duration,
        fps=request.fps,
        root_motion_available=request.root_motion_available,
        metrics_json=_json_text(request.metrics),
        provenance_json=_json_text(request.provenance),
        rank_score=request.rank_score,
        rank=request.rank,
        qa_passed=request.qa_passed,
    )
    db.add(candidate)
    job.candidates_attempted = current_count + 1
    job.status = "review" if request.qa_passed else "generating"
    job.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(candidate)
    return candidate


async def decide_fitting_candidate(
    db: AsyncSession,
    *,
    candidate_id: str,
    request: AnimationCandidateDecisionRequest,
    admin_email: str,
) -> AnimalAnimationCandidate:
    decision = str(request.decision or "").strip().lower()
    if decision not in ("approve", "reject"):
        raise AnimationLibraryError("decision must be approve or reject")
    candidate_result = await db.execute(
        select(AnimalAnimationCandidate).where(AnimalAnimationCandidate.id == candidate_id)
    )
    candidate = candidate_result.scalar_one_or_none()
    if not candidate:
        raise AnimationLibraryError("Animation candidate not found", status_code=404)
    job_result = await db.execute(
        select(AnimalAnimationFittingJob).where(AnimalAnimationFittingJob.id == candidate.job_id)
    )
    job = job_result.scalar_one()
    version_result = await db.execute(
        select(AnimalAnimationLibraryVersion).where(AnimalAnimationLibraryVersion.id == job.library_version_id)
    )
    version = version_result.scalar_one()
    if version.status != "draft":
        raise AnimationLibraryError("Published library revisions are immutable", status_code=409)
    now = datetime.utcnow()

    existing_result = await db.execute(
        select(AnimalAnimationApprovedClip).where(
            AnimalAnimationApprovedClip.library_version_id == version.id,
            AnimalAnimationApprovedClip.semantic_id == job.semantic_id,
        )
    )
    approved = existing_result.scalar_one_or_none()
    if decision == "reject":
        if approved and approved.candidate_id == candidate.id:
            raise AnimationLibraryError("Replace the approved clip before rejecting it", status_code=409)
        candidate.decision = "rejected"
        candidate.decision_reason = request.reason
        candidate.reviewed_by = admin_email
        candidate.reviewed_at = now
        candidate.status = "rejected"
        await db.commit()
        await db.refresh(candidate)
        return candidate

    if not candidate.qa_passed:
        raise AnimationLibraryError("Candidate did not pass automatic QA", status_code=409)
    if candidate.rank not in (1, 2, 3):
        raise AnimationLibraryError("Only an automatically ranked top-3 candidate can be approved", status_code=409)
    if not candidate.fitted_clip_sha256 or not candidate.duration or not candidate.fps:
        raise AnimationLibraryError("Candidate fitted clip metadata is incomplete", status_code=409)
    candidate_clip = resolve_candidate_clip_artifact(candidate)
    validate_visual_phase_gate(
        _json_value(candidate.metrics_json, None),
        expected_rig_type=job.rig_type,
        expected_semantic_id=job.semantic_id,
        expected_fitted_clip_sha256=candidate_clip.sha256,
    )
    clip = taxonomy_clip(job.semantic_id)
    if approved is None:
        approved = AnimalAnimationApprovedClip(
            library_version_id=version.id,
            semantic_id=job.semantic_id,
        )
        db.add(approved)
    elif approved.candidate_id != candidate.id:
        old_result = await db.execute(
            select(AnimalAnimationCandidate).where(AnimalAnimationCandidate.id == approved.candidate_id)
        )
        old_candidate = old_result.scalar_one_or_none()
        if old_candidate:
            old_candidate.decision = "rejected"
            old_candidate.decision_reason = f"Replaced by candidate {candidate.id}"
            old_candidate.status = "rejected"
            old_candidate.reviewed_by = admin_email
            old_candidate.reviewed_at = now
    approved.candidate_id = candidate.id
    approved.category = clip["category"]
    approved.clip_order = clip["order"]
    approved.loop = clip["loop"]
    approved.duration = float(candidate.duration)
    approved.fps = float(candidate.fps)
    approved.start_pose_id = clip["start_pose_id"]
    approved.end_pose_id = clip["end_pose_id"]
    approved.root_motion_available = bool(candidate.root_motion_available)
    approved.qa_profile_revision = version.qa_profile_revision
    bind_approved_clip_artifact(approved, candidate)
    approved.metrics_json = candidate.metrics_json
    if candidate_clip.candidate_bundle_sha256 and candidate_clip.human_review_sha256:
        approved.provenance_json = _json_text(
            canonical_approved_clip_provenance(candidate, candidate_clip)
        )
    else:
        # Preserve the legacy v1 FBX behavior and bytes exactly.
        approved.provenance_json = candidate.provenance_json
    approved.approved_by = admin_email
    approved.approved_at = now
    candidate.decision = "approved"
    candidate.decision_reason = request.reason
    candidate.reviewed_by = admin_email
    candidate.reviewed_at = now
    candidate.status = "approved"
    job.status = "approved"
    job.completed_at = now
    job.updated_at = now
    await db.commit()
    await db.refresh(candidate)
    return candidate


async def activation_for_task(
    db: AsyncSession,
    *,
    rig_type: str,
    task_created_at: datetime,
) -> Optional[tuple[AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion]]:
    """Resolve the revision active at task creation; never silently backfill older tasks."""
    rig = normalize_rig_type(rig_type)
    result = await db.execute(
        select(AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion)
        .join(
            AnimalAnimationLibraryVersion,
            AnimalAnimationLibraryVersion.id == AnimalAnimationLibraryActivation.library_version_id,
        )
        .where(
            AnimalAnimationLibraryActivation.rig_type == rig,
            AnimalAnimationLibraryActivation.activated_at <= task_created_at,
            or_(
                AnimalAnimationLibraryActivation.deactivated_at.is_(None),
                AnimalAnimationLibraryActivation.deactivated_at > task_created_at,
            ),
        )
        .order_by(AnimalAnimationLibraryActivation.activated_at.desc())
        .limit(1)
    )
    row = result.first()
    return (row[0], row[1]) if row else None


async def activations_for_task(
    db: AsyncSession,
    *,
    task_created_at: datetime,
) -> Dict[str, tuple[AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion]]:
    """Resolve all species revisions in one query for the variant matrix response."""
    result = await db.execute(
        select(AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion)
        .join(
            AnimalAnimationLibraryVersion,
            AnimalAnimationLibraryVersion.id == AnimalAnimationLibraryActivation.library_version_id,
        )
        .where(
            AnimalAnimationLibraryActivation.activated_at <= task_created_at,
            or_(
                AnimalAnimationLibraryActivation.deactivated_at.is_(None),
                AnimalAnimationLibraryActivation.deactivated_at > task_created_at,
            ),
        )
        .order_by(AnimalAnimationLibraryActivation.activated_at.desc())
    )
    out: Dict[str, tuple[AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion]] = {}
    for activation, version in result.all():
        if version.rig_type in ANIMAL_RIG_TYPES and version.rig_type not in out:
            out[version.rig_type] = (activation, version)
    return out


async def current_activation(
    db: AsyncSession,
    rig_type: str,
) -> Optional[tuple[AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion]]:
    rig = normalize_rig_type(rig_type)
    result = await db.execute(
        select(AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion)
        .join(
            AnimalAnimationLibraryVersion,
            AnimalAnimationLibraryVersion.id == AnimalAnimationLibraryActivation.library_version_id,
        )
        .where(
            AnimalAnimationLibraryActivation.rig_type == rig,
            AnimalAnimationLibraryActivation.deactivated_at.is_(None),
        )
        .order_by(AnimalAnimationLibraryActivation.activated_at.desc())
        .limit(1)
    )
    row = result.first()
    return (row[0], row[1]) if row else None


def _validate_package_snapshot_descriptor(value: Any, field_name: str) -> dict:
    descriptor = _require_exact_object_keys(
        value, field_name, ("path", "bytes", "sha256")
    )
    path_value = descriptor["path"]
    if (
        not isinstance(path_value, str)
        or not path_value.strip()
        or path_value != path_value.strip()
        or len(path_value) > 4096
    ):
        raise AnimationLibraryError(f"{field_name}.path is invalid", status_code=409)
    byte_count = descriptor["bytes"]
    if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count <= 0:
        raise AnimationLibraryError(f"{field_name}.bytes is invalid", status_code=409)
    descriptor["sha256"] = normalize_sha256(
        descriptor["sha256"], f"{field_name}.sha256"
    )
    return descriptor


def validate_package_result_contract(
    package_result: Any,
    *,
    version: AnimalAnimationLibraryVersion,
    artifact: AnimalAnimationLibraryArtifact,
    approved: Sequence[AnimalAnimationApprovedClip],
) -> dict:
    """Cross-pin the browser packager completion marker to stored approvals."""
    package_result = _require_exact_object_keys(
        package_result,
        "package_result",
        (
            "schema",
            "library_revision",
            "rig_type",
            "orientation",
            "template_skeleton_sha256",
            "taxonomy",
            "source",
            "input_manifest",
            "clips",
            "output",
            "source_bin_prefix_bytes",
            "animation_count",
        ),
    )
    if package_result.get("schema") != PACKAGE_RESULT_SCHEMA_ID:
        raise AnimationLibraryError("Unsupported package-result schema", status_code=409)
    if normalize_revision(package_result.get("library_revision")) != version.revision:
        raise AnimationLibraryError("Package-result revision mismatch", status_code=409)
    if normalize_rig_type(package_result.get("rig_type")) != version.rig_type:
        raise AnimationLibraryError("Package-result rig type mismatch", status_code=409)
    if normalize_orientation(package_result.get("orientation")) != artifact.orientation:
        raise AnimationLibraryError("Package-result orientation mismatch", status_code=409)
    if normalize_sha256(
        package_result.get("template_skeleton_sha256"),
        "package_result.template_skeleton_sha256",
    ) != normalize_sha256(version.template_skeleton_sha256, "template_skeleton_sha256"):
        raise AnimationLibraryError("Package-result skeleton pin mismatch", status_code=409)
    animation_count = package_result.get("animation_count")
    if isinstance(animation_count, bool) or animation_count != 30:
        raise AnimationLibraryError("Package result must declare exactly 30 animations", status_code=409)
    taxonomy = _validate_package_snapshot_descriptor(
        package_result["taxonomy"], "package_result.taxonomy"
    )
    checked_taxonomy_bytes = TAXONOMY_PATH.read_bytes()
    if taxonomy["bytes"] != len(checked_taxonomy_bytes) or taxonomy["sha256"] != hashlib.sha256(
        checked_taxonomy_bytes
    ).hexdigest():
        raise AnimationLibraryError(
            "Package-result taxonomy pin differs from the checked-in taxonomy",
            status_code=409,
        )
    _validate_package_snapshot_descriptor(package_result["source"], "package_result.source")
    _validate_package_snapshot_descriptor(
        package_result["input_manifest"], "package_result.input_manifest"
    )
    output = _validate_package_snapshot_descriptor(
        package_result["output"], "package_result.output"
    )
    artifact_path = Path(str(artifact.animation_glb_path or ""))
    if not artifact_path.is_file():
        raise AnimationLibraryError("Stored animation GLB is missing", status_code=409)
    if output["sha256"] != normalize_sha256(artifact.artifact_sha256, "artifact_sha256"):
        raise AnimationLibraryError("Package-result GLB pin mismatch", status_code=409)
    if output["bytes"] != artifact_path.stat().st_size:
        raise AnimationLibraryError("Package-result GLB byte count mismatch", status_code=409)
    source_bin_prefix_bytes = package_result["source_bin_prefix_bytes"]
    if (
        isinstance(source_bin_prefix_bytes, bool)
        or not isinstance(source_bin_prefix_bytes, int)
        or source_bin_prefix_bytes <= 0
        or source_bin_prefix_bytes >= output["bytes"]
    ):
        raise AnimationLibraryError(
            "Package-result source_bin_prefix_bytes is invalid", status_code=409
        )

    rows = package_result.get("clips")
    if not isinstance(rows, list) or len(rows) != 30:
        raise AnimationLibraryError("Package result must contain exactly 30 clip pins", status_code=409)
    if [row.get("semantic_id") if isinstance(row, dict) else None for row in rows] != list(
        ANIMAL_CLIP_IDS
    ):
        raise AnimationLibraryError("Package-result clips are incomplete or unordered", status_code=409)
    for row, approved_clip in zip(rows, approved):
        clip = resolve_approved_clip_artifact(approved_clip)
        row = _require_exact_object_keys(
            row,
            f"package_result.clips.{approved_clip.semantic_id}",
            (
                "semantic_id",
                "path",
                "bytes",
                "sha256",
                "duration",
                "track_count",
                "candidate_id",
                "candidate_bundle_sha256",
                "human_review_sha256",
            ),
        )
        _validate_package_snapshot_descriptor(
            {key: row[key] for key in ("path", "bytes", "sha256")},
            f"package_result.clips.{approved_clip.semantic_id}",
        )
        duration = row["duration"]
        if (
            isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(float(duration))
            or float(duration) <= 0
            or abs(float(duration) - float(approved_clip.duration)) > 1e-6
        ):
            raise AnimationLibraryError(
                f"Package-result duration differs for {approved_clip.semantic_id}",
                status_code=409,
            )
        track_count = row["track_count"]
        if isinstance(track_count, bool) or not isinstance(track_count, int) or track_count <= 0:
            raise AnimationLibraryError(
                f"Package-result track_count is invalid for {approved_clip.semantic_id}",
                status_code=409,
            )
        expected_values = {
            "sha256": clip.sha256,
            "candidate_id": approved_clip.candidate_id,
            "candidate_bundle_sha256": clip.candidate_bundle_sha256,
            "human_review_sha256": clip.human_review_sha256,
        }
        for field_name, expected in expected_values.items():
            actual = row.get(field_name)
            if field_name.endswith("sha256"):
                actual = normalize_sha256(actual, f"package_result.clips.{field_name}")
            if actual != expected:
                raise AnimationLibraryError(
                    f"Package-result {field_name} differs for {approved_clip.semantic_id}",
                    status_code=409,
                )
    return package_result


async def _validate_activation_contents(
    db: AsyncSession,
    version: AnimalAnimationLibraryVersion,
    *,
    library_root: str,
) -> None:
    approved_result = await db.execute(
        select(AnimalAnimationApprovedClip)
        .where(AnimalAnimationApprovedClip.library_version_id == version.id)
        .order_by(AnimalAnimationApprovedClip.clip_order.asc())
    )
    approved = list(approved_result.scalars().all())
    if [row.semantic_id for row in approved] != list(ANIMAL_CLIP_IDS):
        raise AnimationLibraryError("All 30 canonical clips must be approved before activation", status_code=409)
    for approved_clip in approved:
        approved_artifact = resolve_approved_clip_artifact(approved_clip)
        validate_visual_phase_gate(
            _json_value(approved_clip.metrics_json, None),
            expected_rig_type=version.rig_type,
            expected_semantic_id=approved_clip.semantic_id,
            expected_fitted_clip_sha256=approved_artifact.sha256,
        )
    artifact_result = await db.execute(
        select(AnimalAnimationLibraryArtifact).where(
            AnimalAnimationLibraryArtifact.library_version_id == version.id
        )
    )
    artifacts = {row.orientation: row for row in artifact_result.scalars().all()}
    if set(artifacts) != set(ANIMAL_ORIENTATIONS):
        raise AnimationLibraryError("Both front and back artifacts are required before activation", status_code=409)
    for orientation in ANIMAL_ORIENTATIONS:
        artifact = artifacts[orientation]
        manifest = validate_animation_manifest(
            _json_value(artifact.manifest_json, {}),
            expected_revision=version.revision,
            expected_rig_type=version.rig_type,
            expected_orientation=orientation,
            expected_artifact_sha256=artifact.artifact_sha256,
            expected_template_skeleton_sha256=version.template_skeleton_sha256,
            expected_package_result_sha256=artifact.package_result_sha256,
        )
        if artifact.animation_clip_count != 30 or manifest_sha256(manifest) != artifact.manifest_sha256:
            raise AnimationLibraryError("Stored manifest metadata is inconsistent", status_code=409)
        required_clip_format = (
            CLIP_ARTIFACT_FORMAT_FBX
            if manifest["schema"] == MANIFEST_V1_SCHEMA_ID
            else CLIP_ARTIFACT_FORMAT_THREEJS_JSON
        )
        for manifest_clip, approved_clip in zip(manifest["clips"], approved):
            approved_artifact = resolve_approved_clip_artifact(approved_clip)
            if approved_artifact.format != required_clip_format:
                raise AnimationLibraryError(
                    f"{manifest['schema']} cannot activate {approved_artifact.format} clips",
                    status_code=409,
                )
            if (
                approved_clip.qa_profile_revision != version.qa_profile_revision
                or manifest_clip["qa_profile_revision"] != approved_clip.qa_profile_revision
            ):
                raise AnimationLibraryError(
                    f"QA profile revision differs for {approved_clip.semantic_id}",
                    status_code=409,
                )
            if abs(float(manifest_clip["duration"]) - float(approved_clip.duration)) > 1e-6:
                raise AnimationLibraryError(
                    f"Manifest duration differs from approved clip {approved_clip.semantic_id}",
                    status_code=409,
                )
            if abs(float(manifest_clip["fps"]) - float(approved_clip.fps)) > 1e-6:
                raise AnimationLibraryError(
                    f"Manifest FPS differs from approved clip {approved_clip.semantic_id}",
                    status_code=409,
                )
            if bool(manifest_clip["root_motion_available"]) != bool(approved_clip.root_motion_available):
                raise AnimationLibraryError(
                    f"Manifest root-motion flag differs from approved clip {approved_clip.semantic_id}",
                    status_code=409,
                )
            provenance = manifest_clip.get("provenance") or {}
            if str(provenance.get("candidate_id") or "") != approved_clip.candidate_id:
                raise AnimationLibraryError(
                    f"Manifest provenance differs from approved clip {approved_clip.semantic_id}",
                    status_code=409,
                )
            if manifest["schema"] == MANIFEST_V2_SCHEMA_ID:
                clip_artifact = manifest_clip["clip_artifact"]
                expected_clip_values = {
                    "format": approved_artifact.format,
                    "sha256": approved_artifact.sha256,
                }
                for field_name, expected_value in expected_clip_values.items():
                    if clip_artifact[field_name] != expected_value:
                        raise AnimationLibraryError(
                            f"Manifest clip artifact differs for {approved_clip.semantic_id}",
                            status_code=409,
                        )
                if "url" in clip_artifact and clip_artifact["url"] != approved_artifact.url:
                    raise AnimationLibraryError(
                        f"Manifest clip artifact URL differs for {approved_clip.semantic_id}",
                        status_code=409,
                    )
                if (
                    provenance.get("candidate_bundle_sha256")
                    != approved_artifact.candidate_bundle_sha256
                    or provenance.get("human_review_sha256")
                    != approved_artifact.human_review_sha256
                ):
                    raise AnimationLibraryError(
                        f"Manifest provenance pins differ for {approved_clip.semantic_id}",
                        status_code=409,
                    )
                stored_provenance = _json_value(approved_clip.provenance_json, None)
                if not isinstance(stored_provenance, dict) or canonical_json_bytes(
                    provenance
                ) != canonical_json_bytes(stored_provenance):
                    raise AnimationLibraryError(
                        f"Manifest full provenance differs for {approved_clip.semantic_id}",
                        status_code=409,
                    )
        if manifest["schema"] == MANIFEST_V2_SCHEMA_ID:
            if not artifact.package_result_path or not artifact.package_result_sha256:
                raise AnimationLibraryError(
                    "A v2 artifact is missing its package-result pin", status_code=409
                )
            result_path = _validated_runtime_path(
                artifact.package_result_path, library_root, "package_result_path"
            )
            package_result = _read_pinned_json_file(
                result_path,
                normalize_sha256(artifact.package_result_sha256, "package_result_sha256"),
                "package_result_path",
            )
            validate_package_result_contract(
                package_result,
                version=version,
                artifact=artifact,
                approved=approved,
            )
        elif artifact.package_result_path or artifact.package_result_sha256:
            raise AnimationLibraryError(
                "A v1 artifact cannot bind package-result metadata", status_code=409
            )
        path = _validated_runtime_path(artifact.animation_glb_path, library_root, "animation_glb_path")
        _validate_glb_path(path, artifact.artifact_sha256)
        validate_glb_animation_contract(path, manifest)


async def activate_library_version(
    db: AsyncSession,
    *,
    rig_type: str,
    revision: str,
    admin_email: str,
    reason: str = "activate",
    library_root: str = ANIMATION_LIBRARY_ROOT,
) -> AnimalAnimationLibraryActivation:
    version = await find_library_version(db, rig_type, revision)
    await _validate_activation_contents(db, version, library_root=library_root)
    current = await current_activation(db, version.rig_type)
    if current and current[1].id == version.id:
        return current[0]
    now = datetime.utcnow()
    if current:
        current[0].deactivated_at = now
    activation = AnimalAnimationLibraryActivation(
        rig_type=version.rig_type,
        library_version_id=version.id,
        activated_at=now,
        activated_by=admin_email,
        reason="rollback" if reason == "rollback" else "activate",
    )
    db.add(activation)
    version.status = "published"
    version.published_at = version.published_at or now
    version.updated_at = now
    await db.commit()
    await db.refresh(activation)
    return activation


async def rollback_library_version(
    db: AsyncSession,
    *,
    rig_type: str,
    target_revision: Optional[str],
    admin_email: str,
    library_root: str = ANIMATION_LIBRARY_ROOT,
) -> AnimalAnimationLibraryActivation:
    rig = normalize_rig_type(rig_type)
    current = await current_activation(db, rig)
    if not current:
        raise AnimationLibraryError("No active animation library to roll back", status_code=409)
    if target_revision:
        target = await find_library_version(db, rig, target_revision)
    else:
        previous_result = await db.execute(
            select(AnimalAnimationLibraryActivation, AnimalAnimationLibraryVersion)
            .join(
                AnimalAnimationLibraryVersion,
                AnimalAnimationLibraryVersion.id == AnimalAnimationLibraryActivation.library_version_id,
            )
            .where(
                AnimalAnimationLibraryActivation.rig_type == rig,
                AnimalAnimationLibraryActivation.id != current[0].id,
                AnimalAnimationLibraryActivation.activated_at < current[0].activated_at,
            )
            .order_by(AnimalAnimationLibraryActivation.activated_at.desc())
            .limit(1)
        )
        row = previous_result.first()
        if not row:
            raise AnimationLibraryError("No previous animation library revision is available", status_code=409)
        target = row[1]
    if target.id == current[1].id:
        raise AnimationLibraryError("Target revision is already active", status_code=409)
    return await activate_library_version(
        db,
        rig_type=rig,
        revision=target.revision,
        admin_email=admin_email,
        reason="rollback",
        library_root=library_root,
    )


def serialize_library_version(
    version: AnimalAnimationLibraryVersion,
    *,
    artifacts: Sequence[AnimalAnimationLibraryArtifact] = (),
    approved_clip_count: int = 0,
    active: bool = False,
) -> dict:
    return {
        "id": version.id,
        "rig_type": version.rig_type,
        "revision": version.revision,
        "status": version.status,
        "active": active,
        "template_skeleton_sha256": version.template_skeleton_sha256,
        "qa_profile_revision": version.qa_profile_revision,
        "notes": version.notes,
        "approved_clip_count": approved_clip_count,
        "created_by": version.created_by,
        "created_at": version.created_at,
        "published_at": version.published_at,
        "artifacts": [
            {
                "orientation": item.orientation,
                "manifest_sha256": item.manifest_sha256,
                "animation_glb_url": item.animation_glb_url,
                "artifact_sha256": item.artifact_sha256,
                "animation_clip_count": item.animation_clip_count,
                "package_zip_url": item.package_zip_url,
                "package_result_sha256": item.package_result_sha256,
            }
            for item in sorted(artifacts, key=lambda row: row.orientation)
        ],
    }


def serialize_candidate(candidate: AnimalAnimationCandidate) -> dict:
    return {
        "id": candidate.id,
        "job_id": candidate.job_id,
        "seed": candidate.seed,
        "status": candidate.status,
        "raw_video_url": candidate.raw_video_url,
        "fitted_clip_url": candidate.fitted_clip_url,
        "fitted_clip_format": candidate.fitted_clip_format or CLIP_ARTIFACT_FORMAT_FBX,
        "fitted_clip_sha256": candidate.fitted_clip_sha256,
        "candidate_bundle_sha256": candidate.candidate_bundle_sha256,
        "human_review_sha256": candidate.human_review_sha256,
        "duration": candidate.duration,
        "fps": candidate.fps,
        "root_motion_available": candidate.root_motion_available,
        "metrics": _json_value(candidate.metrics_json, {}),
        "provenance": _json_value(candidate.provenance_json, {}),
        "rank_score": candidate.rank_score,
        "rank": candidate.rank,
        "qa_passed": candidate.qa_passed,
        "decision": candidate.decision,
        "decision_reason": candidate.decision_reason,
        "reviewed_by": candidate.reviewed_by,
        "reviewed_at": candidate.reviewed_at,
        "created_at": candidate.created_at,
    }


def serialize_fitting_job(job: AnimalAnimationFittingJob, candidates: Sequence[AnimalAnimationCandidate] = ()) -> dict:
    return {
        "id": job.id,
        "library_version_id": job.library_version_id,
        "rig_type": job.rig_type,
        "semantic_id": job.semantic_id,
        "status": job.status,
        "workflow_name": job.workflow_name,
        "workflow_fingerprint": job.workflow_fingerprint,
        "worker_url": job.worker_url,
        "prompt_id": job.prompt_id,
        "prompt": job.prompt,
        "candidate_target": job.candidate_target,
        "candidate_limit": job.candidate_limit,
        "candidates_attempted": job.candidates_attempted,
        "config": _json_value(job.config_json, {}),
        "error": job.error,
        "created_by": job.created_by,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "completed_at": job.completed_at,
        "candidates": [serialize_candidate(item) for item in candidates],
    }


async def get_fitting_job_payload(db: AsyncSession, job_id: str) -> dict:
    job_result = await db.execute(select(AnimalAnimationFittingJob).where(AnimalAnimationFittingJob.id == job_id))
    job = job_result.scalar_one_or_none()
    if not job:
        raise AnimationLibraryError("Fitting job not found", status_code=404)
    candidates_result = await db.execute(
        select(AnimalAnimationCandidate)
        .where(AnimalAnimationCandidate.job_id == job.id)
        .order_by(AnimalAnimationCandidate.rank.asc(), AnimalAnimationCandidate.created_at.asc())
    )
    return serialize_fitting_job(job, list(candidates_result.scalars().all()))
