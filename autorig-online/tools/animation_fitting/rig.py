from __future__ import annotations

from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Dict, Iterable, Optional

import numpy as np

from .errors import ContractError
from .math3d import (
    matrix4,
    normalize_vector,
    rotation_matrix_rotvec,
    rotation_matrix_xyz,
    transform_point,
    translation_matrix,
)


BUNDLE_SCHEMA = "autorig-actionless-fitting-bundle.v1"
IMMUTABLE_MANIFEST_FILENAME = "immutable_manifest.json"
IMMUTABLE_MANIFEST_SCHEMAS = {
    "autorig-fitting-immutable-bundle.v1",
    "autorig-fitting-immutable-copy.v1",
}
REQUIRED_ARTIFACT_KEYS = {
    "rgb",
    "mask",
    "skeleton",
    "skin_weights",
    "surface_anchors",
}
REQUIRED_V2_ARTIFACT_KEYS = {
    "camera_z",
    "depth",
    "face_id",
    "surface_topology",
}
REQUIRED_V3_ARTIFACT_KEYS = {"ltx_semantic"}
SEMANTIC_OUTPUT_LABELS = {
    "fore_near",
    "fore_far",
    "hind_near",
    "hind_far",
}


def _matrix(values: Any, *, field: str) -> np.ndarray:
    try:
        result = matrix4([] if values is None else values, field=field)
    except (TypeError, ValueError) as exc:
        raise ContractError(str(exc)) from exc
    if abs(float(np.linalg.det(result))) <= 1e-12:
        raise ContractError(f"{field} must be invertible")
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                return json.load(stream)
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid JSON artifact {path}: {exc}") from exc


@dataclass(frozen=True)
class JointBounds:
    lower: np.ndarray
    upper: np.ndarray
    constrained_axes: tuple[bool, bool, bool]

    @property
    def has_constraint(self) -> bool:
        return any(self.constrained_axes)


@dataclass(frozen=True)
class Bone:
    name: str
    parent: Optional[str]
    helper: bool
    use_deform: bool
    rest_local: np.ndarray
    rest_world: np.ndarray
    length: float
    joint_bounds: JointBounds


@dataclass(frozen=True)
class AnchorInfluence:
    bone: str
    weight: float
    bone_local: np.ndarray


@dataclass(frozen=True)
class Anchor:
    id: str
    bone: str
    vertex_id: int
    rest_world: np.ndarray
    skin_weight: float
    influences: tuple[AnchorInfluence, ...]


@dataclass(frozen=True)
class _SkinWeight:
    bone: str
    raw_weight: float
    weight: float


@dataclass(frozen=True)
class Camera:
    width: int
    height: int
    fx: float
    fy: float
    cx: float
    cy: float
    world_to_camera: np.ndarray

    def project(self, point_world: np.ndarray) -> tuple[np.ndarray, float]:
        point_camera = transform_point(self.world_to_camera, point_world)
        depth = -float(point_camera[2])
        if depth <= 1e-9:
            return np.asarray((np.nan, np.nan), dtype=np.float64), depth
        x = self.fx * float(point_camera[0]) / depth + self.cx
        y = self.cy - self.fy * float(point_camera[1]) / depth
        return np.asarray((x, y), dtype=np.float64), depth


@dataclass
class RigBundle:
    root: Path
    metadata_path: Path
    metadata_sha256: str
    immutable_manifest_path: Path
    immutable_manifest_sha256: str
    metadata: dict
    artifacts: Dict[str, Path]
    armature_name: str
    armature_world: np.ndarray
    bones: Dict[str, Bone]
    bone_order: tuple[str, ...]
    anchors: Dict[str, Anchor]
    camera: Camera
    ground_normal: np.ndarray
    ground_height: float

    def select_active_bones(
        self,
        anchor_ids: Iterable[str],
        *,
        explicit: Optional[Iterable[str]] = None,
        require_joint_limits: bool = True,
    ) -> tuple[str, ...]:
        if explicit is not None:
            requested = {str(name) for name in explicit}
            unknown = requested.difference(self.bones)
            if unknown:
                raise ContractError(f"Unknown active bones: {sorted(unknown)}")
            invalid = [
                name
                for name in requested
                if self.bones[name].parent is None
                or self.bones[name].helper
                or not self.bones[name].use_deform
            ]
            if invalid:
                raise ContractError(
                    "Active joints must be non-root deform bones and not helpers: "
                    + ", ".join(sorted(invalid))
                )
        else:
            requested: set[str] = set()
            for anchor_id in anchor_ids:
                anchor = self.anchors.get(anchor_id)
                if not anchor:
                    raise ContractError(f"Observation references unknown anchor: {anchor_id}")
                for influence in anchor.influences:
                    name: Optional[str] = influence.bone
                    while name:
                        bone = self.bones[name]
                        if bone.parent is not None and bone.use_deform and not bone.helper:
                            requested.add(name)
                        name = bone.parent

        ordered = tuple(name for name in self.bone_order if name in requested)
        if require_joint_limits:
            missing = [name for name in ordered if not self.bones[name].joint_bounds.has_constraint]
            if missing:
                raise ContractError(
                    "Optimized bones have no local LIMIT_ROTATION contract: "
                    + ", ".join(missing)
                    + ". Add constraints in the source rig or explicitly use --allow-unbounded-joints."
                )
        return ordered

    def forward_kinematics(
        self,
        root_translation: np.ndarray,
        root_rotvec: np.ndarray,
        joint_eulers: Dict[str, np.ndarray],
    ) -> tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
        root_delta = translation_matrix(root_translation) @ rotation_matrix_rotvec(root_rotvec)
        world: Dict[str, np.ndarray] = {}
        local: Dict[str, np.ndarray] = {}
        for name in self.bone_order:
            bone = self.bones[name]
            rotation_delta = rotation_matrix_xyz(joint_eulers.get(name, np.zeros(3, dtype=np.float64)))
            local_matrix = bone.rest_local @ rotation_delta
            if bone.parent is None:
                world[name] = root_delta @ local_matrix
                local[name] = world[name]
            else:
                world[name] = world[bone.parent] @ local_matrix
                local[name] = local_matrix
        return world, local

    def anchor_positions(self, world_matrices: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        positions: Dict[str, np.ndarray] = {}
        for anchor_id, anchor in self.anchors.items():
            position = np.zeros(3, dtype=np.float64)
            for influence in anchor.influences:
                matrix = world_matrices.get(influence.bone)
                if matrix is None:
                    raise ContractError(
                        f"Missing posed world matrix for anchor {anchor_id} influence {influence.bone}"
                    )
                position += influence.weight * transform_point(matrix, influence.bone_local)
            positions[anchor_id] = position
        return positions


def _relative_artifact(bundle_root: Path, filename: Any, *, field: str) -> tuple[str, Path]:
    if not isinstance(filename, str) or not filename.strip():
        raise ContractError(f"{field} must be a non-empty relative filename")
    root = bundle_root.resolve()
    path = (root / filename).resolve()
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ContractError(f"{field} escapes the fitting bundle root: {filename}") from exc
    if path == root or not path.is_file():
        raise ContractError(f"{field} does not reference a bundle file: {filename}")
    return relative.as_posix(), path


def _load_immutable_manifest(
    bundle_root: Path,
) -> tuple[Path, str, Dict[str, dict[str, Any]]]:
    raw_manifest_path = bundle_root / IMMUTABLE_MANIFEST_FILENAME
    if not raw_manifest_path.is_file():
        manifest_path = raw_manifest_path.resolve()
        raise ContractError(f"Missing mandatory immutable manifest: {manifest_path}")
    _, manifest_path = _relative_artifact(
        bundle_root,
        IMMUTABLE_MANIFEST_FILENAME,
        field=IMMUTABLE_MANIFEST_FILENAME,
    )
    payload = _read_json(manifest_path)
    schema = payload.get("schema") if isinstance(payload, dict) else None
    if schema not in IMMUTABLE_MANIFEST_SCHEMAS:
        raise ContractError(
            "Unsupported immutable manifest schema; expected one of "
            + ", ".join(sorted(IMMUTABLE_MANIFEST_SCHEMAS))
        )
    rows = payload.get("files")
    if not isinstance(rows, list) or not rows:
        raise ContractError("immutable_manifest.json contains no files")
    verified: Dict[str, dict[str, Any]] = {}
    casefolded: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ContractError(f"immutable manifest row {index} must be an object")
        relative, path = _relative_artifact(
            bundle_root,
            row.get("filename"),
            field=f"immutable manifest row {index}.filename",
        )
        identity = relative.casefold()
        if identity in casefolded:
            raise ContractError(f"Duplicate immutable manifest filename: {relative}")
        casefolded.add(identity)
        expected_sha = row.get("sha256")
        expected_bytes = row.get("bytes")
        if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha):
            raise ContractError(f"Immutable manifest SHA-256 is invalid for {relative}")
        if isinstance(expected_bytes, bool) or not isinstance(expected_bytes, int) or expected_bytes < 0:
            raise ContractError(f"Immutable manifest byte size is invalid for {relative}")
        actual_bytes = path.stat().st_size
        actual_sha = _sha256(path)
        if actual_bytes != expected_bytes:
            raise ContractError(
                f"Immutable artifact size mismatch for {relative}: "
                f"expected {expected_bytes}, got {actual_bytes}"
            )
        if actual_sha != expected_sha.lower():
            raise ContractError(f"Immutable artifact SHA-256 mismatch for {relative}")
        verified[relative] = {
            "path": path,
            "bytes": actual_bytes,
            "sha256": actual_sha,
        }
    if "fitting_bundle.json" not in verified:
        raise ContractError("immutable_manifest.json must cover fitting_bundle.json")
    return manifest_path, _sha256(manifest_path), verified


def _artifact_path(
    bundle_root: Path,
    metadata: dict,
    manifest_files: Dict[str, dict[str, Any]],
    key: str,
) -> Path:
    artifacts = metadata.get("artifacts")
    record = artifacts.get(key) if isinstance(artifacts, dict) else None
    if not isinstance(record, dict):
        raise ContractError(f"Bundle artifact is missing: {key}")
    relative, path = _relative_artifact(
        bundle_root,
        record.get("filename"),
        field=f"artifacts.{key}.filename",
    )
    expected_sha = record.get("sha256")
    expected_bytes = record.get("bytes")
    if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha):
        raise ContractError(f"Bundle artifact SHA-256 is invalid: {key}")
    if isinstance(expected_bytes, bool) or not isinstance(expected_bytes, int) or expected_bytes < 0:
        raise ContractError(f"Bundle artifact byte size is invalid: {key}")
    immutable = manifest_files.get(relative)
    if immutable is None:
        raise ContractError(f"Immutable manifest does not cover bundle artifact {key}: {relative}")
    if immutable["sha256"] != expected_sha.lower() or immutable["bytes"] != expected_bytes:
        raise ContractError(f"Bundle metadata/immutable manifest disagree for artifact {key}")
    return path


def _joint_bounds(raw: Any, bone_name: str) -> JointBounds:
    lower = np.full(3, -np.inf, dtype=np.float64)
    upper = np.full(3, np.inf, dtype=np.float64)
    constrained = [False, False, False]
    constraints = raw if isinstance(raw, list) else []
    for constraint in constraints:
        if not isinstance(constraint, dict) or constraint.get("type") != "LIMIT_ROTATION":
            continue
        uses = [bool(constraint.get(f"use_limit_{axis}")) for axis in "xyz"]
        if any(uses) and str(constraint.get("space") or "").upper() != "LOCAL":
            raise ContractError(
                f"Bone {bone_name} has LIMIT_ROTATION in unsupported space {constraint.get('space')!r}; LOCAL is required"
            )
        mins = constraint.get("min")
        maxs = constraint.get("max")
        if not isinstance(mins, list) or not isinstance(maxs, list) or len(mins) != 3 or len(maxs) != 3:
            raise ContractError(f"Bone {bone_name} has malformed LIMIT_ROTATION values")
        for axis in range(3):
            if not uses[axis]:
                continue
            minimum, maximum = float(mins[axis]), float(maxs[axis])
            if not np.isfinite(minimum) or not np.isfinite(maximum) or minimum > maximum:
                raise ContractError(f"Bone {bone_name} has invalid joint bounds on axis {axis}")
            lower[axis] = max(lower[axis], minimum)
            upper[axis] = min(upper[axis], maximum)
            constrained[axis] = True
    if np.any(lower > upper):
        raise ContractError(f"Bone {bone_name} has contradictory joint constraints")
    return JointBounds(lower=lower, upper=upper, constrained_axes=tuple(constrained))


def _load_bones(
    skeleton_path: Path,
) -> tuple[str, np.ndarray, Dict[str, Bone], tuple[str, ...]]:
    payload = _read_json(skeleton_path)
    armatures = payload.get("armatures") if isinstance(payload, dict) else None
    if not isinstance(armatures, list) or len(armatures) != 1:
        raise ContractError("Fitting v1 requires exactly one armature in skeleton.json")
    armature = armatures[0]
    armature_name = armature.get("name")
    if not isinstance(armature_name, str) or not armature_name:
        raise ContractError("skeleton.json armature requires a non-empty name")
    armature_world = _matrix(armature.get("matrix_world"), field="armature.matrix_world")
    raw_bones = armature.get("bones")
    if not isinstance(raw_bones, list) or not raw_bones:
        raise ContractError("skeleton.json contains no bones")
    raw_by_name: Dict[str, dict] = {}
    for raw in raw_bones:
        name = raw.get("name") if isinstance(raw, dict) else None
        if not isinstance(name, str) or not name or name in raw_by_name:
            raise ContractError("Bone names must be non-empty and unique")
        raw_by_name[name] = raw
    pending = dict(raw_by_name)
    bones: Dict[str, Bone] = {}
    order: list[str] = []
    while pending:
        progressed = False
        for name, raw in list(pending.items()):
            parent = raw.get("parent")
            if parent is not None and parent not in raw_by_name:
                raise ContractError(f"Bone {name} references missing parent {parent}")
            if parent is not None and parent not in bones:
                continue
            relative = _matrix(
                raw.get("parent_relative_matrix"), field=f"bone {name}.parent_relative_matrix"
            )
            rest_local = armature_world @ relative if parent is None else relative
            rest_world = rest_local if parent is None else bones[parent].rest_world @ rest_local
            try:
                length = float(raw.get("length"))
            except (TypeError, ValueError) as exc:
                raise ContractError(f"Bone {name} has invalid length") from exc
            if not np.isfinite(length) or length < 0:
                raise ContractError(f"Bone {name} has invalid length")
            bones[name] = Bone(
                name=name,
                parent=parent,
                helper=bool(raw.get("helper")),
                use_deform=bool(raw.get("use_deform", not raw.get("helper"))),
                rest_local=rest_local,
                rest_world=rest_world,
                length=length,
                joint_bounds=_joint_bounds(raw.get("joint_limits"), name),
            )
            order.append(name)
            del pending[name]
            progressed = True
        if not progressed:
            raise ContractError("Bone hierarchy contains a cycle")
    return armature_name, armature_world, bones, tuple(order)


def _load_skin_weights(path: Path, bones: Dict[str, Bone]) -> Dict[int, tuple[_SkinWeight, ...]]:
    payload = _read_json(path)
    rows = payload.get("vertices") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ContractError("skin_weights.json.gz contains no vertices")
    result: Dict[int, tuple[_SkinWeight, ...]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("vertex_id"), int):
            raise ContractError("Skin vertex rows require integer vertex_id")
        vertex_id = row["vertex_id"]
        if vertex_id in result:
            raise ContractError(f"Duplicate skin vertex_id: {vertex_id}")
        weights = row.get("weights")
        if not isinstance(weights, list) or not weights or len(weights) > 4:
            raise ContractError(f"Vertex {vertex_id} must have between one and four skin weights")
        raw_weights: list[tuple[str, float]] = []
        seen_bones: set[str] = set()
        for item in weights:
            bone = item.get("bone") if isinstance(item, dict) else None
            weight = item.get("weight") if isinstance(item, dict) else None
            if bone not in bones:
                raise ContractError(f"Vertex {vertex_id} references unknown bone {bone!r}")
            if bone in seen_bones:
                raise ContractError(f"Vertex {vertex_id} has duplicate skin weight for bone {bone}")
            try:
                numeric = float(weight)
            except (TypeError, ValueError) as exc:
                raise ContractError(f"Vertex {vertex_id} has invalid skin weight") from exc
            if not np.isfinite(numeric) or numeric < 0:
                raise ContractError(f"Vertex {vertex_id} has invalid skin weight")
            seen_bones.add(bone)
            raw_weights.append((bone, numeric))
        weight_sum = sum(weight for _, weight in raw_weights)
        if not np.isfinite(weight_sum) or weight_sum <= 0.0:
            raise ContractError(f"Vertex {vertex_id} skin weights must have a positive finite sum")
        result[vertex_id] = tuple(
            _SkinWeight(bone=bone, raw_weight=weight, weight=weight / weight_sum)
            for bone, weight in raw_weights
        )
    return result


def _load_anchors(
    path: Path,
    bones: Dict[str, Bone],
    skin: Dict[int, tuple[_SkinWeight, ...]],
) -> Dict[str, Anchor]:
    payload = _read_json(path)
    groups = payload.get("bones") if isinstance(payload, dict) else None
    if not isinstance(groups, list) or not groups:
        raise ContractError("surface_anchors.json contains no anchors")
    anchors: Dict[str, Anchor] = {}
    for group in groups:
        bone_name = group.get("bone") if isinstance(group, dict) else None
        if bone_name not in bones:
            raise ContractError(f"Surface anchors reference unknown bone {bone_name!r}")
        points = group.get("points")
        if not isinstance(points, list):
            raise ContractError(f"Surface anchor group {bone_name} has no points")
        for point in points:
            if not isinstance(point, dict) or not isinstance(point.get("vertex_id"), int):
                raise ContractError(f"Surface anchor on {bone_name} needs integer vertex_id")
            vertex_id = point["vertex_id"]
            anchor_id = str(point.get("id") or f"{bone_name}:{vertex_id}")
            if anchor_id in anchors:
                raise ContractError(f"Duplicate anchor ID: {anchor_id}")
            vertex_weights = skin.get(vertex_id)
            primary_weight = next(
                (influence for influence in vertex_weights or () if influence.bone == bone_name),
                None,
            )
            if primary_weight is None:
                raise ContractError(f"Anchor {anchor_id} is not weighted to bone {bone_name}")
            world = np.asarray(point.get("world"), dtype=np.float64)
            if world.shape != (3,) or not np.all(np.isfinite(world)):
                raise ContractError(f"Anchor {anchor_id} has invalid world coordinates")
            try:
                declared_weight = float(point.get("weight", primary_weight.raw_weight))
            except (TypeError, ValueError) as exc:
                raise ContractError(f"Anchor {anchor_id} has invalid declared skin weight") from exc
            actual_weight = primary_weight.raw_weight
            if not np.isfinite(declared_weight) or declared_weight < 0.0:
                raise ContractError(f"Anchor {anchor_id} has invalid declared skin weight")
            if abs(declared_weight - actual_weight) > 1e-6:
                raise ContractError(f"Anchor {anchor_id} weight does not match skin_weights.json.gz")
            influences = tuple(
                AnchorInfluence(
                    bone=influence.bone,
                    weight=influence.weight,
                    bone_local=transform_point(
                        np.linalg.inv(bones[influence.bone].rest_world),
                        world,
                    ),
                )
                for influence in vertex_weights
            )
            anchors[anchor_id] = Anchor(
                id=anchor_id,
                bone=bone_name,
                vertex_id=vertex_id,
                rest_world=world,
                skin_weight=actual_weight,
                influences=influences,
            )
    return anchors


def _load_camera(metadata: dict) -> Camera:
    camera = metadata.get("camera")
    if not isinstance(camera, dict):
        raise ContractError("Bundle camera metadata is missing")
    resolution = camera.get("resolution")
    intrinsics = camera.get("intrinsics")
    if not isinstance(resolution, list) or len(resolution) != 2 or not isinstance(intrinsics, dict):
        raise ContractError("Bundle camera resolution/intrinsics are invalid")
    width, height = int(resolution[0]), int(resolution[1])
    values = [float(intrinsics.get(key)) for key in ("fx", "fy", "cx", "cy")]
    if width <= 0 or height <= 0 or not all(np.isfinite(values)) or values[0] <= 0 or values[1] <= 0:
        raise ContractError("Bundle camera intrinsics are invalid")
    return Camera(
        width=width,
        height=height,
        fx=values[0],
        fy=values[1],
        cx=values[2],
        cy=values[3],
        world_to_camera=_matrix(camera.get("world_to_camera"), field="camera.world_to_camera"),
    )


def _validate_camera_z_artifact(metadata: dict, path: Path, camera: Camera) -> None:
    camera_metadata = metadata.get("camera")
    contract = (
        camera_metadata.get("camera_z_contract") if isinstance(camera_metadata, dict) else None
    )
    if not isinstance(contract, dict):
        raise ContractError("v2 fitting bundle camera_z_contract metadata is missing")
    expected = {
        "mode": "positive_camera_z",
        "dtype": "float32",
        "invalid": "NaN",
        "shape": [camera.height, camera.width],
    }
    for key, value in expected.items():
        if contract.get(key) != value:
            raise ContractError(
                f"v2 fitting bundle camera_z_contract.{key} must be {value!r}"
            )
    try:
        camera_z = np.load(path, allow_pickle=False)
    except Exception as exc:
        raise ContractError(f"Cannot read camera-Z artifact {path}: {exc}") from exc
    if camera_z.dtype != np.float32 or camera_z.shape != (camera.height, camera.width):
        raise ContractError(
            f"camera-Z artifact must be float32 [{camera.height}, {camera.width}], "
            f"got {camera_z.dtype} {camera_z.shape}"
        )
    finite = camera_z[np.isfinite(camera_z)]
    invalid = ~np.isfinite(camera_z)
    if finite.size == 0 or np.any(finite <= 0.0):
        raise ContractError("camera-Z artifact contains no finite positive depth")
    if not np.all(np.isnan(camera_z[invalid])):
        raise ContractError("camera-Z invalid pixels must be NaN, not infinity")
    if int(contract.get("valid_pixels", -1)) != int(finite.size):
        raise ContractError("camera-Z valid_pixels metadata does not match the artifact")
    for key, actual in (
        ("minimum", float(np.min(finite))),
        ("median", float(np.median(finite))),
        ("maximum", float(np.max(finite))),
    ):
        try:
            declared = float(contract.get(key))
        except (TypeError, ValueError) as exc:
            raise ContractError(f"camera-Z {key} metadata is invalid") from exc
        if not np.isfinite(declared) or not np.isclose(declared, actual, rtol=1e-6, atol=1e-6):
            raise ContractError(f"camera-Z {key} metadata does not match the artifact")


def _validate_semantic_ltx_artifact(metadata: dict, path: Path, camera: Camera) -> None:
    contract = metadata.get("semantic_ltx_reference")
    if not isinstance(contract, dict) or contract.get("schema") != (
        "autorig-ltx-semantic-reference.v1"
    ):
        raise ContractError("v3 fitting bundle semantic_ltx_reference metadata is missing")
    if contract.get("resolution") != [camera.width, camera.height]:
        raise ContractError("semantic LTX resolution does not match the canonical camera")
    profile = contract.get("profile")
    if not isinstance(profile, dict):
        raise ContractError("semantic LTX profile provenance is missing")
    profile_sha = profile.get("sha256")
    if not isinstance(profile_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", profile_sha):
        raise ContractError("semantic LTX profile SHA-256 is invalid")
    camera_metadata = metadata.get("camera")
    if not isinstance(camera_metadata, dict):
        raise ContractError("semantic LTX canonical camera metadata is missing")
    core_camera_keys = (
        "name",
        "resolution",
        "lens_mm",
        "sensor_width_mm",
        "intrinsics",
        "camera_to_world",
        "world_to_camera",
    )
    camera_contract = {key: camera_metadata.get(key) for key in core_camera_keys}
    camera_sha = hashlib.sha256(
        json.dumps(
            camera_contract,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if contract.get("camera_contract_sha256") != camera_sha:
        raise ContractError("semantic LTX camera contract SHA-256 does not match metadata")
    artifacts = metadata.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ContractError("semantic LTX artifact metadata is missing")
    for link_key, artifact_key in (("canonical_rgb", "rgb"), ("canonical_mask", "mask")):
        linked = contract.get(link_key)
        artifact = artifacts.get(artifact_key)
        if not isinstance(linked, dict) or not isinstance(artifact, dict):
            raise ContractError(f"semantic LTX {link_key} linkage is missing")
        expected = {
            "filename": artifact.get("filename"),
            "sha256": artifact.get("sha256"),
            "bytes": artifact.get("bytes"),
        }
        if linked != expected:
            raise ContractError(f"semantic LTX {link_key} linkage disagrees with artifacts")
    limb_groups = contract.get("limb_groups")
    if not isinstance(limb_groups, dict) or set(limb_groups) != {
        "fore_left",
        "fore_right",
        "hind_left",
        "hind_right",
    }:
        raise ContractError("semantic LTX requires four explicit anatomical limb groups")
    claimed: set[str] = set()
    for group, bones in limb_groups.items():
        if (
            not isinstance(bones, list)
            or not bones
            or any(not isinstance(bone, str) or not bone for bone in bones)
            or claimed.intersection(bones)
        ):
            raise ContractError(f"semantic LTX limb group is invalid or overlapping: {group}")
        claimed.update(bones)
    classification = contract.get("classification")
    assignments = (
        classification.get("near_far_assignment")
        if isinstance(classification, dict)
        else None
    )
    if not isinstance(assignments, dict) or set(assignments) != {"fore", "hind"}:
        raise ContractError("semantic LTX near/far camera assignment is missing")
    for anatomy, assignment in assignments.items():
        if not isinstance(assignment, dict):
            raise ContractError(f"semantic LTX {anatomy} camera assignment is invalid")
        near = assignment.get("near_source_group")
        far = assignment.get("far_source_group")
        near_z = assignment.get("near_camera_z")
        far_z = assignment.get("far_camera_z")
        if (
            near not in {f"{anatomy}_left", f"{anatomy}_right"}
            or far not in {f"{anatomy}_left", f"{anatomy}_right"}
            or near == far
        ):
            raise ContractError(f"semantic LTX {anatomy} source-group assignment is invalid")
        try:
            near_depth = float(near_z)
            far_depth = float(far_z)
        except (TypeError, ValueError) as exc:
            raise ContractError(f"semantic LTX {anatomy} camera depths are invalid") from exc
        if (
            not np.isfinite(near_depth)
            or not np.isfinite(far_depth)
            or near_depth <= 0.0
            or near_depth >= far_depth
        ):
            raise ContractError(f"semantic LTX {anatomy} near/far camera depths are invalid")
    pixels = contract.get("pixels")
    gates = contract.get("gates")
    if not isinstance(pixels, dict) or not isinstance(gates, dict):
        raise ContractError("semantic LTX pixel/gate contract is missing")
    counts = pixels.get("output_label_pixel_counts")
    fractions = pixels.get("output_label_mask_fractions")
    if (
        not isinstance(counts, dict)
        or set(counts) != SEMANTIC_OUTPUT_LABELS
        or not isinstance(fractions, dict)
        or set(fractions) != SEMANTIC_OUTPUT_LABELS
    ):
        raise ContractError("semantic LTX must measure all four output labels")
    try:
        minimum_pixels = int(gates.get("minimum_pixels_per_output_label"))
        minimum_fraction = float(gates.get("minimum_pixel_fraction_of_mask"))
        maximum_mismatch = int(gates.get("maximum_mask_mismatch_pixels"))
        mismatch = int(pixels.get("semantic_mask_mismatch_pixels"))
    except (TypeError, ValueError) as exc:
        raise ContractError("semantic LTX pixel gates are invalid") from exc
    for label in SEMANTIC_OUTPUT_LABELS:
        if (
            isinstance(counts[label], bool)
            or int(counts[label]) < minimum_pixels
            or not np.isfinite(float(fractions[label]))
            or float(fractions[label]) < minimum_fraction
        ):
            raise ContractError(f"semantic LTX label fails its pixel gate: {label}")
    if mismatch < 0 or mismatch > maximum_mismatch:
        raise ContractError("semantic LTX mask mismatch exceeds its configured gate")
    if contract.get("restoration_verified") is not True or contract.get("render_order") != (
        "after_reference_rgb_before_face_id_override"
    ):
        raise ContractError("semantic LTX render/restoration contract is invalid")
    try:
        from PIL import Image

        with Image.open(path) as image:
            if image.format != "PNG" or image.size != (camera.width, camera.height):
                raise ContractError(
                    "semantic LTX artifact must be a same-resolution lossless PNG"
                )
            image.verify()
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(f"Cannot validate semantic LTX PNG {path}: {exc}") from exc


def load_rig_bundle(bundle_dir: str | Path) -> RigBundle:
    root = Path(bundle_dir).resolve()
    if not root.is_dir():
        raise ContractError(f"Fitting bundle directory does not exist: {root}")
    manifest_path, manifest_sha256, manifest_files = _load_immutable_manifest(root)
    metadata_record = manifest_files["fitting_bundle.json"]
    metadata_path = metadata_record["path"]
    metadata = _read_json(metadata_path)
    if not isinstance(metadata, dict) or metadata.get("schema") != BUNDLE_SCHEMA:
        raise ContractError(f"Unsupported fitting bundle schema; expected {BUNDLE_SCHEMA}")
    actionless = metadata.get("actionless")
    if not isinstance(actionless, dict) or actionless.get("actionless") is not True:
        raise ContractError("Fitting bundle is not actionless")
    raw_artifacts = metadata.get("artifacts")
    if not isinstance(raw_artifacts, dict):
        raise ContractError("Fitting bundle artifacts object is missing")
    required_artifacts = set(REQUIRED_ARTIFACT_KEYS)
    revision = metadata.get("revision")
    if revision in {"autorig_actionless_bundle_v2", "autorig_actionless_bundle_v3"}:
        required_artifacts.update(REQUIRED_V2_ARTIFACT_KEYS)
        counts = metadata.get("counts")
        if not isinstance(counts, dict) or counts.get("armatures") != 1:
            raise ContractError("v2 fitting bundle metadata must declare exactly one armature")
    if revision == "autorig_actionless_bundle_v3":
        required_artifacts.update(REQUIRED_V3_ARTIFACT_KEYS)
    missing_artifacts = required_artifacts.difference(raw_artifacts)
    if missing_artifacts:
        raise ContractError(f"Fitting bundle is missing required artifacts: {sorted(missing_artifacts)}")
    artifact_paths: Dict[str, Path] = {}
    artifact_relative_paths: set[str] = set()
    for key in sorted(raw_artifacts):
        if not isinstance(key, str) or not key:
            raise ContractError("Fitting bundle artifact keys must be non-empty strings")
        path = _artifact_path(root, metadata, manifest_files, key)
        relative = path.relative_to(root).as_posix()
        if relative in artifact_relative_paths:
            raise ContractError(f"More than one artifact key references {relative}")
        artifact_relative_paths.add(relative)
        artifact_paths[key] = path
    expected_manifest_files = {"fitting_bundle.json", *artifact_relative_paths}
    manifest_names = set(manifest_files)
    if manifest_names != expected_manifest_files:
        raise ContractError(
            "immutable_manifest.json must cover exactly fitting_bundle.json and metadata artifacts; "
            f"missing={sorted(expected_manifest_files - manifest_names)}, "
            f"unexpected={sorted(manifest_names - expected_manifest_files)}"
        )
    skeleton_path = artifact_paths["skeleton"]
    skin_path = artifact_paths["skin_weights"]
    anchors_path = artifact_paths["surface_anchors"]
    camera = _load_camera(metadata)
    if revision in {"autorig_actionless_bundle_v2", "autorig_actionless_bundle_v3"}:
        _validate_camera_z_artifact(metadata, artifact_paths["camera_z"], camera)
    if revision == "autorig_actionless_bundle_v3":
        _validate_semantic_ltx_artifact(metadata, artifact_paths["ltx_semantic"], camera)
    armature_name, armature_world, bones, bone_order = _load_bones(skeleton_path)
    skin = _load_skin_weights(skin_path, bones)
    anchors = _load_anchors(anchors_path, bones, skin)
    plane = metadata.get("ground_plane")
    if not isinstance(plane, dict):
        raise ContractError("Bundle ground_plane metadata is missing")
    try:
        ground_normal = normalize_vector(plane.get("normal") or [], field="ground_plane.normal")
        ground_height = float(plane.get("height"))
    except (TypeError, ValueError) as exc:
        raise ContractError(f"Bundle ground plane is invalid: {exc}") from exc
    if not np.isfinite(ground_height):
        raise ContractError("Bundle ground plane height must be finite")
    return RigBundle(
        root=root,
        metadata_path=metadata_path,
        metadata_sha256=metadata_record["sha256"],
        immutable_manifest_path=manifest_path,
        immutable_manifest_sha256=manifest_sha256,
        metadata=metadata,
        artifacts=artifact_paths,
        armature_name=armature_name,
        armature_world=armature_world,
        bones=bones,
        bone_order=bone_order,
        anchors=anchors,
        camera=camera,
        ground_normal=ground_normal,
        ground_height=ground_height,
    )
