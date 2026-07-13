from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import struct
from typing import Any, Dict, Optional

import numpy as np

from .errors import ContractError
from .math3d import quaternion_xyzw_from_matrix


MOTION_SCHEMA = "autorig-fitted-animation.v1"
TRANSFORM_SCHEMA = "autorig-fitted-transform-contract.v1"
TARGET_SCHEMA = "autorig-motion-target.v1"
ASSET_BUNDLE_SCHEMA = "autorig-fitted-asset-bundle.v1"
ACTION_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
GENERIC_ACTION_NAMES = {"action", "animation", "anim", "take_001", "mixamo_com"}


def sha256_file(path: str | Path) -> str:
    source = Path(path)
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json_object(path: str | Path, *, label: str) -> dict:
    source = Path(path).resolve()
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid {label} JSON {source}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ContractError(f"{label} JSON must be an object: {source}")
    return payload


def validate_action_id(value: str) -> str:
    if not isinstance(value, str) or not ACTION_ID_RE.fullmatch(value):
        raise ContractError(
            "semantic action ID must match ^[a-z][a-z0-9_]{1,63}$"
        )
    if value in GENERIC_ACTION_NAMES:
        raise ContractError(f"Generic action name is forbidden: {value}")
    return value


def _finite_float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be a finite number") from exc
    if not math.isfinite(result):
        raise ContractError(f"{field} must be a finite number")
    return result


def matrix4(values: Any, field: str) -> np.ndarray:
    try:
        matrix = np.asarray(values, dtype=np.float64).reshape(4, 4)
    except Exception as exc:
        raise ContractError(f"{field} must contain exactly 16 numeric values") from exc
    if not np.all(np.isfinite(matrix)):
        raise ContractError(f"{field} contains non-finite values")
    if not np.allclose(matrix[3], (0.0, 0.0, 0.0, 1.0), atol=1e-7, rtol=0.0):
        raise ContractError(f"{field} is not an affine 4x4 matrix")
    if abs(float(np.linalg.det(matrix))) <= 1e-12:
        raise ContractError(f"{field} must be invertible")
    return matrix


@dataclass(frozen=True)
class MotionBone:
    name: str
    parent: Optional[str]
    local_matrix: np.ndarray


@dataclass(frozen=True)
class MotionFrame:
    frame: int
    bones: Dict[str, MotionBone]


@dataclass(frozen=True)
class MotionClip:
    path: Path
    sha256: str
    raw: dict
    frame_count: int
    fps: float
    loop: bool
    armature_name: str
    armature_world: np.ndarray
    bone_names: tuple[str, ...]
    parent_by_bone: Dict[str, Optional[str]]
    translation_bones: tuple[str, ...]
    frames: tuple[MotionFrame, ...]


def load_motion(path: str | Path) -> MotionClip:
    source = Path(path).resolve()
    payload = read_json_object(source, label="motion")
    if payload.get("schema") != MOTION_SCHEMA:
        raise ContractError(f"Unsupported motion schema; expected {MOTION_SCHEMA}")
    contract = payload.get("transform_contract")
    if not isinstance(contract, dict) or contract.get("schema") != TRANSFORM_SCHEMA:
        raise ContractError(f"Motion requires transform_contract schema {TRANSFORM_SCHEMA}")
    expected = {
        "root_local_matrix_space": "WORLD",
        "child_local_matrix_space": "PARENT_BONE",
        "rotation_channel": "QUATERNION",
        "scale_animation": False,
    }
    for field, expected_value in expected.items():
        if contract.get(field) != expected_value:
            raise ContractError(
                f"Unsupported transform contract {field}={contract.get(field)!r}; expected {expected_value!r}"
            )
    armature_name = contract.get("source_armature_name")
    if not isinstance(armature_name, str) or not armature_name:
        raise ContractError("transform_contract.source_armature_name is required")
    armature_world = matrix4(
        contract.get("source_armature_world_matrix"),
        "transform_contract.source_armature_world_matrix",
    )
    translation_policy = contract.get("translation_policy")
    if not isinstance(translation_policy, dict):
        raise ContractError("transform_contract.translation_policy is required")
    translation_mode = translation_policy.get("mode")
    if translation_mode not in {"root_only", "explicit_bones"}:
        raise ContractError("translation_policy.mode must be root_only or explicit_bones")
    translation_values = translation_policy.get("bones")
    if not isinstance(translation_values, list) or any(
        not isinstance(name, str) or not name for name in translation_values
    ):
        raise ContractError("translation_policy.bones must contain non-empty bone names")
    if len(set(translation_values)) != len(translation_values):
        raise ContractError("translation_policy.bones must be unique")

    frame_count = payload.get("frame_count")
    if isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 2:
        raise ContractError("motion.frame_count must be an integer >= 2")
    fps = _finite_float(payload.get("fps"), "motion.fps")
    if fps <= 0.0:
        raise ContractError("motion.fps must be positive")
    loop = payload.get("loop")
    if not isinstance(loop, bool):
        raise ContractError("motion.loop must be boolean")
    raw_frames = payload.get("frames")
    if not isinstance(raw_frames, list) or len(raw_frames) != frame_count:
        raise ContractError("motion.frames length must equal frame_count")

    frames: list[MotionFrame] = []
    bone_names: Optional[tuple[str, ...]] = None
    parent_by_bone: Dict[str, Optional[str]] = {}
    baseline_translation: Dict[str, np.ndarray] = {}
    baseline_scale: Dict[str, np.ndarray] = {}
    for expected_frame, raw_frame in enumerate(raw_frames):
        if not isinstance(raw_frame, dict) or raw_frame.get("frame") != expected_frame:
            raise ContractError(f"motion frame indices must be contiguous from 0; expected {expected_frame}")
        raw_bones = raw_frame.get("bones")
        if not isinstance(raw_bones, dict) or not raw_bones:
            raise ContractError(f"motion.frames[{expected_frame}].bones must be a non-empty object")
        current_names = tuple(raw_bones)
        if any(not isinstance(name, str) or not name for name in current_names):
            raise ContractError("motion bone names must be non-empty strings")
        if bone_names is None:
            bone_names = current_names
        elif set(current_names) != set(bone_names):
            raise ContractError(f"motion frame {expected_frame} does not contain the exact bone set")
        parsed: Dict[str, MotionBone] = {}
        for name in bone_names:
            raw_bone = raw_bones.get(name)
            if not isinstance(raw_bone, dict):
                raise ContractError(f"motion frame {expected_frame} bone {name} is malformed")
            parent = raw_bone.get("parent")
            if parent is not None and not isinstance(parent, str):
                raise ContractError(f"motion bone {name}.parent must be a string or null")
            if expected_frame == 0:
                parent_by_bone[name] = parent
            elif parent_by_bone[name] != parent:
                raise ContractError(f"motion bone {name} changes parent at frame {expected_frame}")
            matrix = matrix4(
                raw_bone.get("local_matrix"),
                f"motion.frames[{expected_frame}].bones[{name}].local_matrix",
            )
            if "local_translation" in raw_bone:
                declared_translation = np.asarray(raw_bone.get("local_translation"), dtype=np.float64)
                if declared_translation.shape != (3,) or not np.all(np.isfinite(declared_translation)):
                    raise ContractError(f"motion bone {name} has invalid local_translation")
                if not np.allclose(declared_translation, matrix[:3, 3], atol=1e-6, rtol=0.0):
                    raise ContractError(f"motion bone {name} local_translation disagrees with local_matrix")
            if "local_rotation_xyzw" in raw_bone:
                declared_quaternion = np.asarray(raw_bone.get("local_rotation_xyzw"), dtype=np.float64)
                if declared_quaternion.shape != (4,) or not np.all(np.isfinite(declared_quaternion)):
                    raise ContractError(f"motion bone {name} has invalid local_rotation_xyzw")
                norm = float(np.linalg.norm(declared_quaternion))
                if abs(norm - 1.0) > 1e-5:
                    raise ContractError(f"motion bone {name} quaternion is not normalized")
                actual = np.asarray(quaternion_xyzw_from_matrix(matrix), dtype=np.float64)
                if min(np.linalg.norm(actual - declared_quaternion), np.linalg.norm(actual + declared_quaternion)) > 1e-4:
                    raise ContractError(f"motion bone {name} quaternion disagrees with local_matrix")
            singular_values = np.linalg.svd(matrix[:3, :3], compute_uv=False)
            if expected_frame == 0:
                baseline_translation[name] = matrix[:3, 3].copy()
                baseline_scale[name] = singular_values
            elif not np.allclose(singular_values, baseline_scale[name], atol=1e-5, rtol=0.0):
                raise ContractError(f"motion bone {name} contains scale animation, forbidden by contract")
            parsed[name] = MotionBone(name=name, parent=parent, local_matrix=matrix)
        frames.append(MotionFrame(frame=expected_frame, bones=parsed))

    assert bone_names is not None
    unknown_parents = {parent for parent in parent_by_bone.values() if parent is not None}.difference(bone_names)
    if unknown_parents:
        raise ContractError(f"motion references unknown parent bones: {sorted(unknown_parents)}")
    roots = tuple(name for name in bone_names if parent_by_bone[name] is None)
    if not roots:
        raise ContractError("motion hierarchy contains no root bone")
    unresolved = set(bone_names)
    resolved: set[str] = set()
    while unresolved:
        progress = {
            name
            for name in unresolved
            if parent_by_bone[name] is None or parent_by_bone[name] in resolved
        }
        if not progress:
            raise ContractError("motion hierarchy contains a parent cycle")
        resolved.update(progress)
        unresolved.difference_update(progress)
    translation_bones = tuple(translation_values)
    unknown_translation = set(translation_bones).difference(bone_names)
    if unknown_translation:
        raise ContractError(f"translation_policy references unknown bones: {sorted(unknown_translation)}")
    if translation_mode == "root_only" and set(translation_bones) != set(roots):
        raise ContractError(
            f"root_only translation_policy must name exactly root bones {list(roots)}"
        )
    for frame in frames[1:]:
        for name in bone_names:
            if name in translation_bones:
                continue
            if not np.allclose(
                frame.bones[name].local_matrix[:3, 3],
                baseline_translation[name],
                atol=1e-5,
                rtol=0.0,
            ):
                raise ContractError(
                    f"motion bone {name} translates at frame {frame.frame} without translation_policy permission"
                )
    return MotionClip(
        path=source,
        sha256=sha256_file(source),
        raw=payload,
        frame_count=frame_count,
        fps=fps,
        loop=loop,
        armature_name=armature_name,
        armature_world=armature_world,
        bone_names=bone_names,
        parent_by_bone=parent_by_bone,
        translation_bones=translation_bones,
        frames=tuple(frames),
    )


@dataclass(frozen=True)
class TargetSpec:
    path: Optional[Path]
    sha256: Optional[str]
    source_sha256: Optional[str]
    armature_name: str
    armature_data_name: Optional[str]
    bone_names: Optional[tuple[str, ...]]
    bone_parents: Optional[Dict[str, Optional[str]]]


def load_target_spec(
    *,
    manifest_path: Optional[str | Path],
    armature_name: Optional[str],
) -> TargetSpec:
    if bool(manifest_path) == bool(armature_name):
        raise ContractError("Provide exactly one of target manifest or target armature name")
    if armature_name:
        if not isinstance(armature_name, str) or not armature_name:
            raise ContractError("target armature name must be non-empty")
        return TargetSpec(None, None, None, armature_name, None, None, None)
    assert manifest_path is not None
    source = Path(manifest_path).resolve()
    payload = read_json_object(source, label="target manifest")
    if payload.get("schema") != TARGET_SCHEMA:
        raise ContractError(f"Unsupported target manifest schema; expected {TARGET_SCHEMA}")
    name = payload.get("armature_name")
    source_sha = payload.get("source_sha256")
    if not isinstance(name, str) or not name:
        raise ContractError("target manifest armature_name is required")
    if not isinstance(source_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", source_sha):
        raise ContractError("target manifest source_sha256 must be lowercase SHA-256")
    data_name = payload.get("armature_data_name")
    if data_name is not None and (not isinstance(data_name, str) or not data_name):
        raise ContractError("target manifest armature_data_name must be non-empty when present")
    raw_names = payload.get("bone_names")
    names = None
    if raw_names is not None:
        if not isinstance(raw_names, list) or any(not isinstance(value, str) or not value for value in raw_names):
            raise ContractError("target manifest bone_names must be non-empty strings")
        if len(set(raw_names)) != len(raw_names):
            raise ContractError("target manifest bone_names must be unique")
        names = tuple(raw_names)
    raw_parents = payload.get("bone_parents")
    parents = None
    if raw_parents is not None:
        if not isinstance(raw_parents, dict) or any(
            not isinstance(key, str)
            or not key
            or (value is not None and (not isinstance(value, str) or not value))
            for key, value in raw_parents.items()
        ):
            raise ContractError("target manifest bone_parents must map names to string/null")
        parents = dict(raw_parents)
    return TargetSpec(source, sha256_file(source), source_sha, name, data_name, names, parents)


def validate_target_source(spec: TargetSpec, source_sha256: str) -> None:
    if spec.source_sha256 is not None and spec.source_sha256 != source_sha256:
        raise ContractError("target manifest source_sha256 does not match the canonical source file")


def parse_glb_validation(
    path: str | Path,
    *,
    action_id: str,
    expected_duration: float,
    duration_tolerance: float,
) -> dict:
    source = Path(path)
    data = source.read_bytes()
    if len(data) < 20 or data[:4] != b"glTF":
        raise ContractError(f"Exported GLB has an invalid header: {source}")
    magic, version, total_length = struct.unpack_from("<4sII", data, 0)
    if magic != b"glTF" or version != 2 or total_length != len(data):
        raise ContractError(f"Exported GLB header/length is invalid: {source}")
    offset = 12
    document = None
    while offset + 8 <= len(data):
        chunk_length, chunk_type = struct.unpack_from("<II", data, offset)
        offset += 8
        chunk = data[offset : offset + chunk_length]
        offset += chunk_length
        if chunk_type == 0x4E4F534A:
            try:
                document = json.loads(chunk.rstrip(b" \t\r\n\x00").decode("utf-8"))
            except Exception as exc:
                raise ContractError(f"Exported GLB JSON is invalid: {exc}") from exc
    if not isinstance(document, dict):
        raise ContractError("Exported GLB contains no JSON chunk")
    animations = document.get("animations")
    if not isinstance(animations, list) or len(animations) != 1:
        raise ContractError("Exported GLB must contain exactly one animation")
    animation = animations[0]
    if animation.get("name") != action_id:
        raise ContractError(
            f"Exported GLB animation is {animation.get('name')!r}, expected {action_id!r}"
        )
    if not document.get("meshes") or not document.get("skins"):
        raise ContractError("Exported GLB must contain mesh and skin data")
    for mesh in document["meshes"]:
        for primitive in mesh.get("primitives") or []:
            attributes = primitive.get("attributes") or {}
            if "JOINTS_1" in attributes or "WEIGHTS_1" in attributes:
                raise ContractError("Exported GLB contains more than four skin influences")
    accessors = document.get("accessors") or []
    times: list[tuple[float, float]] = []
    for sampler in animation.get("samplers") or []:
        input_index = sampler.get("input")
        if not isinstance(input_index, int) or input_index < 0 or input_index >= len(accessors):
            raise ContractError("Exported GLB animation has an invalid time accessor")
        accessor = accessors[input_index]
        minimum, maximum = accessor.get("min"), accessor.get("max")
        if not isinstance(minimum, list) or not isinstance(maximum, list) or len(minimum) != 1 or len(maximum) != 1:
            raise ContractError("Exported GLB time accessor lacks min/max")
        times.append((float(minimum[0]), float(maximum[0])))
    if not times:
        raise ContractError("Exported GLB animation contains no samplers")
    start = min(value[0] for value in times)
    end = max(value[1] for value in times)
    duration = end - start
    if abs(duration - expected_duration) > duration_tolerance:
        raise ContractError(
            f"Exported GLB duration {duration:.9g}s does not match expected {expected_duration:.9g}s"
        )
    return {
        "animation_count": 1,
        "animation_name": action_id,
        "mesh_count": len(document["meshes"]),
        "skin_count": len(document["skins"]),
        "duration_seconds": duration,
        "has_secondary_joint_sets": False,
    }


def artifact_record(path: str | Path) -> dict:
    source = Path(path)
    return {
        "filename": source.name,
        "sha256": sha256_file(source),
        "bytes": source.stat().st_size,
    }
