"""Validation and serialization for realtime task bone corrections."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional


SCHEMA_VERSION = 1
MAX_PAYLOAD_BYTES = 512 * 1024
MAX_GLOBAL_BONES = 512
MAX_CLIPS = 128
MAX_BONES_PER_CLIP = 512
MAX_BONE_PATH_LENGTH = 1024
MAX_CLIP_ID_LENGTH = 160


class AnimationCorrectionValidationError(ValueError):
    pass


def _finite_number(value: Any, *, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool):
        raise AnimationCorrectionValidationError(f"{field} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AnimationCorrectionValidationError(f"{field} must be a number") from exc
    if not math.isfinite(number) or number < minimum or number > maximum:
        raise AnimationCorrectionValidationError(
            f"{field} must be between {minimum:g} and {maximum:g}"
        )
    return number


def _vector3(
    value: Any,
    *,
    field: str,
    minimum: float,
    maximum: float,
) -> list[float]:
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise AnimationCorrectionValidationError(f"{field} must contain exactly 3 numbers")
    return [
        _finite_number(item, field=f"{field}[{index}]", minimum=minimum, maximum=maximum)
        for index, item in enumerate(value)
    ]


def _normalize_bone_path(value: Any) -> str:
    path = str(value or "").strip()
    if not path or len(path) > MAX_BONE_PATH_LENGTH:
        raise AnimationCorrectionValidationError("bonePath is empty or too long")
    if any(ord(char) < 32 for char in path):
        raise AnimationCorrectionValidationError("bonePath contains control characters")
    return path


def _normalize_clip_id(value: Any) -> str:
    clip_id = str(value or "").strip()
    if not clip_id or len(clip_id) > MAX_CLIP_ID_LENGTH:
        raise AnimationCorrectionValidationError("clip id is empty or too long")
    if any(ord(char) < 32 for char in clip_id):
        raise AnimationCorrectionValidationError("clip id contains control characters")
    return clip_id


def normalize_correction(value: Any, *, field: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AnimationCorrectionValidationError(f"{field} must be an object")
    return {
        "rotationDeg": _vector3(
            value.get("rotationDeg", [0, 0, 0]),
            field=f"{field}.rotationDeg",
            minimum=-180,
            maximum=180,
        ),
        "positionPct": _vector3(
            value.get("positionPct", [0, 0, 0]),
            field=f"{field}.positionPct",
            minimum=-100,
            maximum=100,
        ),
        "motionScale": _finite_number(
            value.get("motionScale", 1),
            field=f"{field}.motionScale",
            minimum=0,
            maximum=2,
        ),
        "enabled": bool(value.get("enabled", True)),
    }


def _normalize_bone_map(value: Any, *, field: str, maximum: int) -> Dict[str, Dict[str, Any]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise AnimationCorrectionValidationError(f"{field} must be an object")
    if len(value) > maximum:
        raise AnimationCorrectionValidationError(f"{field} contains too many bones")
    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_path, raw_correction in value.items():
        path = _normalize_bone_path(raw_path)
        normalized[path] = normalize_correction(raw_correction, field=f"{field}.{path}")
    return normalized


def validate_animation_corrections(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise AnimationCorrectionValidationError("corrections payload must be an object")
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AnimationCorrectionValidationError("corrections payload is not valid JSON") from exc
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise AnimationCorrectionValidationError("corrections payload is too large")

    try:
        version = int(payload.get("schemaVersion", SCHEMA_VERSION))
    except (TypeError, ValueError) as exc:
        raise AnimationCorrectionValidationError("schemaVersion must be 1") from exc
    if version != SCHEMA_VERSION:
        raise AnimationCorrectionValidationError("unsupported corrections schemaVersion")

    raw_clips = payload.get("clips") or {}
    if not isinstance(raw_clips, Mapping):
        raise AnimationCorrectionValidationError("clips must be an object")
    if len(raw_clips) > MAX_CLIPS:
        raise AnimationCorrectionValidationError("clips contains too many entries")

    clips: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for raw_clip_id, raw_bones in raw_clips.items():
        clip_id = _normalize_clip_id(raw_clip_id)
        clips[clip_id] = _normalize_bone_map(
            raw_bones,
            field=f"clips.{clip_id}",
            maximum=MAX_BONES_PER_CLIP,
        )

    skeleton_signature = str(payload.get("skeletonSignature") or "").strip()
    if len(skeleton_signature) > 128:
        raise AnimationCorrectionValidationError("skeletonSignature is too long")

    return {
        "schemaVersion": SCHEMA_VERSION,
        "skeletonSignature": skeleton_signature,
        "global": _normalize_bone_map(
            payload.get("global") or {},
            field="global",
            maximum=MAX_GLOBAL_BONES,
        ),
        "clips": clips,
    }


def canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def load_json_object(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
