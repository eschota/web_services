from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

import numpy as np


PROFILE_SCHEMA = "autorig-semantic-ltx-profile.v1"
SOURCE_GROUP_KEYS = (
    "fore_left",
    "fore_right",
    "hind_left",
    "hind_right",
)
OUTPUT_LABEL_KEYS = (
    "fore_near",
    "fore_far",
    "hind_near",
    "hind_far",
)
PALETTE_KEYS = ("body", *OUTPUT_LABEL_KEYS)
GATE_KEYS = {
    "minimum_face_limb_weight",
    "minimum_face_dominance",
    "minimum_faces_per_source_group",
    "minimum_group_weight_mass",
    "minimum_near_far_depth_separation",
    "minimum_pixels_per_output_label",
    "minimum_pixel_fraction_of_mask",
    "maximum_mask_mismatch_pixels",
    "mask_threshold",
    "pixel_color_tolerance",
    "minimum_palette_distance",
}


class SemanticLtxContractError(ValueError):
    """Raised when a semantic reference cannot be derived deterministically."""


@dataclass(frozen=True)
class SemanticLtxProfile:
    path: Path
    sha256: str
    profile_id: str
    source: dict[str, Any]
    limb_groups: dict[str, tuple[str, ...]]
    palette: dict[str, tuple[float, float, float]]
    gates: dict[str, float | int]


@dataclass(frozen=True)
class SemanticLtxPlan:
    face_labels: dict[int, str]
    source_groups: dict[int, str]
    contract: dict[str, Any]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SemanticLtxContractError(f"{field} must be an object")
    return value


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise SemanticLtxContractError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SemanticLtxContractError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise SemanticLtxContractError(f"{field} must be a finite number")
    return number


def _positive_integer(value: Any, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SemanticLtxContractError(f"{field} must be an integer")
    minimum = 0 if allow_zero else 1
    if value < minimum:
        raise SemanticLtxContractError(f"{field} must be at least {minimum}")
    return value


def load_semantic_ltx_profile(path: str | Path) -> SemanticLtxProfile:
    source_path = Path(path).resolve()
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SemanticLtxContractError(f"Invalid semantic LTX profile {source_path}: {exc}") from exc
    root = _object(payload, "profile")
    if root.get("schema") != PROFILE_SCHEMA:
        raise SemanticLtxContractError(
            f"Unsupported semantic LTX profile schema; expected {PROFILE_SCHEMA}"
        )
    profile_id = root.get("profile_id")
    if not isinstance(profile_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]+", profile_id):
        raise SemanticLtxContractError("profile_id must be a stable lowercase identifier")

    source = _object(root.get("source"), "source")
    required_source = {
        "rig_type",
        "filename",
        "sha256",
        "armature_name",
        "mesh_names",
    }
    if set(source) != required_source:
        raise SemanticLtxContractError(
            f"source keys must be exactly {sorted(required_source)}"
        )
    for key in ("rig_type", "filename", "armature_name"):
        if not isinstance(source[key], str) or not source[key]:
            raise SemanticLtxContractError(f"source.{key} must be a non-empty string")
    if not isinstance(source["sha256"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", source["sha256"]
    ):
        raise SemanticLtxContractError("source.sha256 must be lowercase SHA-256")
    mesh_names = source["mesh_names"]
    if (
        not isinstance(mesh_names, list)
        or not mesh_names
        or any(not isinstance(name, str) or not name for name in mesh_names)
        or len(set(mesh_names)) != len(mesh_names)
    ):
        raise SemanticLtxContractError("source.mesh_names must contain unique names")

    raw_groups = _object(root.get("limb_groups"), "limb_groups")
    if set(raw_groups) != set(SOURCE_GROUP_KEYS):
        raise SemanticLtxContractError(
            f"limb_groups must be exactly {list(SOURCE_GROUP_KEYS)}"
        )
    limb_groups: dict[str, tuple[str, ...]] = {}
    claimed_bones: set[str] = set()
    for key in SOURCE_GROUP_KEYS:
        record = _object(raw_groups[key], f"limb_groups.{key}")
        expected_anatomy, expected_side = key.split("_", 1)
        if record.get("anatomy") != expected_anatomy or record.get("side") != expected_side:
            raise SemanticLtxContractError(
                f"limb_groups.{key} must explicitly declare anatomy={expected_anatomy} "
                f"and side={expected_side}"
            )
        if set(record) != {"anatomy", "side", "bones"}:
            raise SemanticLtxContractError(
                f"limb_groups.{key} keys must be anatomy, side and bones"
            )
        bones = record.get("bones")
        if (
            not isinstance(bones, list)
            or not bones
            or any(not isinstance(name, str) or not name for name in bones)
            or len(set(bones)) != len(bones)
        ):
            raise SemanticLtxContractError(f"limb_groups.{key}.bones must be unique names")
        overlap = claimed_bones.intersection(bones)
        if overlap:
            raise SemanticLtxContractError(
                f"Semantic limb bones may belong to only one group: {sorted(overlap)}"
            )
        claimed_bones.update(bones)
        limb_groups[key] = tuple(bones)

    raw_palette = _object(root.get("palette_linear"), "palette_linear")
    if set(raw_palette) != set(PALETTE_KEYS):
        raise SemanticLtxContractError(
            f"palette_linear must be exactly {list(PALETTE_KEYS)}"
        )
    palette: dict[str, tuple[float, float, float]] = {}
    for key in PALETTE_KEYS:
        raw_color = raw_palette[key]
        if not isinstance(raw_color, list) or len(raw_color) != 3:
            raise SemanticLtxContractError(f"palette_linear.{key} must be RGB")
        color = tuple(
            _finite_number(channel, f"palette_linear.{key}[{index}]")
            for index, channel in enumerate(raw_color)
        )
        if any(channel < 0.0 or channel > 1.0 for channel in color):
            raise SemanticLtxContractError(f"palette_linear.{key} must stay inside [0, 1]")
        palette[key] = color

    raw_gates = _object(root.get("gates"), "gates")
    if set(raw_gates) != GATE_KEYS:
        raise SemanticLtxContractError(f"gates must be exactly {sorted(GATE_KEYS)}")
    gates: dict[str, float | int] = {}
    for key in (
        "minimum_faces_per_source_group",
        "minimum_pixels_per_output_label",
    ):
        gates[key] = _positive_integer(raw_gates[key], f"gates.{key}")
    gates["maximum_mask_mismatch_pixels"] = _positive_integer(
        raw_gates["maximum_mask_mismatch_pixels"],
        "gates.maximum_mask_mismatch_pixels",
        allow_zero=True,
    )
    for key in GATE_KEYS.difference(gates):
        gates[key] = _finite_number(raw_gates[key], f"gates.{key}")
    for key in (
        "minimum_face_limb_weight",
        "minimum_face_dominance",
        "minimum_pixel_fraction_of_mask",
        "mask_threshold",
        "pixel_color_tolerance",
    ):
        value = float(gates[key])
        if value <= 0.0 or value > 1.0:
            raise SemanticLtxContractError(f"gates.{key} must be inside (0, 1]")
    for key in (
        "minimum_group_weight_mass",
        "minimum_near_far_depth_separation",
        "minimum_palette_distance",
    ):
        if float(gates[key]) <= 0.0:
            raise SemanticLtxContractError(f"gates.{key} must be positive")

    minimum_palette_distance = float(gates["minimum_palette_distance"])
    for index, left in enumerate(PALETTE_KEYS):
        for right in PALETTE_KEYS[index + 1 :]:
            distance = float(
                np.linalg.norm(np.asarray(palette[left]) - np.asarray(palette[right]))
            )
            if distance < minimum_palette_distance:
                raise SemanticLtxContractError(
                    f"Palette colors {left}/{right} are too similar: {distance:.6g}"
                )

    normalized_source = dict(source)
    normalized_source["mesh_names"] = tuple(mesh_names)
    return SemanticLtxProfile(
        path=source_path,
        sha256=_sha256(source_path),
        profile_id=profile_id,
        source=normalized_source,
        limb_groups=limb_groups,
        palette=palette,
        gates=gates,
    )


def validate_semantic_profile_source(
    profile: SemanticLtxProfile,
    *,
    rig_type: str,
    filename: str,
    source_sha256: str,
    armature_name: str,
    mesh_names: Sequence[str],
) -> None:
    actual = {
        "rig_type": rig_type,
        "filename": filename,
        "sha256": source_sha256,
        "armature_name": armature_name,
        "mesh_names": tuple(sorted(mesh_names)),
    }
    expected = {
        **profile.source,
        "mesh_names": tuple(sorted(profile.source["mesh_names"])),
    }
    mismatches = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if actual[key] != expected[key]
    }
    if mismatches:
        raise SemanticLtxContractError(
            "Semantic profile/source identity mismatch: "
            + json.dumps(mismatches, ensure_ascii=False, sort_keys=True)
        )


def _weighted_median(rows: list[tuple[float, float, int]]) -> float:
    ordered = sorted(rows, key=lambda item: (item[0], item[2]))
    total = sum(weight for _, weight, _ in ordered)
    midpoint = total * 0.5
    cumulative = 0.0
    for value, weight, _ in ordered:
        cumulative += weight
        if cumulative >= midpoint:
            return value
    raise SemanticLtxContractError("Cannot calculate a weighted median")


def build_semantic_ltx_plan(
    profile: SemanticLtxProfile,
    *,
    skin_rows: Sequence[Mapping[str, Any]],
    topology_rows: Sequence[Mapping[str, Any]],
    available_bones: Sequence[str],
    world_to_camera: Sequence[float] | np.ndarray,
) -> SemanticLtxPlan:
    available = set(available_bones)
    required = {bone for bones in profile.limb_groups.values() for bone in bones}
    missing = sorted(required.difference(available))
    if missing:
        raise SemanticLtxContractError(
            "Semantic profile references missing deform bones: " + ", ".join(missing)
        )
    matrix = np.asarray(world_to_camera, dtype=np.float64)
    if matrix.size != 16:
        raise SemanticLtxContractError("world_to_camera must contain 16 values")
    matrix = matrix.reshape(4, 4)
    if not np.all(np.isfinite(matrix)) or abs(float(np.linalg.det(matrix))) <= 1e-12:
        raise SemanticLtxContractError("world_to_camera must be a finite invertible matrix")

    bone_to_group = {
        bone: group for group, bones in profile.limb_groups.items() for bone in bones
    }
    vertex_scores: dict[int, dict[str, float]] = {}
    depth_rows: dict[str, list[tuple[float, float, int]]] = {
        key: [] for key in SOURCE_GROUP_KEYS
    }
    group_mass = {key: 0.0 for key in SOURCE_GROUP_KEYS}
    for raw in skin_rows:
        vertex_id = raw.get("vertex_id")
        if isinstance(vertex_id, bool) or not isinstance(vertex_id, int) or vertex_id in vertex_scores:
            raise SemanticLtxContractError("Skin rows require unique integer vertex_id values")
        world = np.asarray(raw.get("world"), dtype=np.float64)
        if world.shape != (3,) or not np.all(np.isfinite(world)):
            raise SemanticLtxContractError(f"Skin vertex {vertex_id} has invalid world coordinates")
        weights = raw.get("weights")
        if not isinstance(weights, list) or not weights:
            raise SemanticLtxContractError(f"Skin vertex {vertex_id} has no weights")
        scores = {key: 0.0 for key in SOURCE_GROUP_KEYS}
        total = 0.0
        for item in weights:
            if not isinstance(item, dict):
                raise SemanticLtxContractError(f"Skin vertex {vertex_id} has an invalid weight")
            bone = item.get("bone")
            weight = _finite_number(item.get("weight"), f"vertex {vertex_id} weight")
            if weight < 0.0:
                raise SemanticLtxContractError(f"Skin vertex {vertex_id} has a negative weight")
            total += weight
            group = bone_to_group.get(bone)
            if group is not None:
                scores[group] += weight
        if total <= 1e-12:
            raise SemanticLtxContractError(f"Skin vertex {vertex_id} has zero-sum weights")
        scores = {key: value / total for key, value in scores.items()}
        vertex_scores[vertex_id] = scores
        camera = matrix @ np.asarray((world[0], world[1], world[2], 1.0))
        if abs(float(camera[3])) <= 1e-12:
            raise SemanticLtxContractError(f"Skin vertex {vertex_id} has invalid camera W")
        camera_z = -float(camera[2] / camera[3])
        for group, weight in scores.items():
            if weight <= 0.0:
                continue
            if not math.isfinite(camera_z) or camera_z <= 0.0:
                raise SemanticLtxContractError(
                    f"Semantic limb vertex {vertex_id} is not in front of the canonical camera"
                )
            depth_rows[group].append((camera_z, weight, vertex_id))
            group_mass[group] += weight

    minimum_mass = float(profile.gates["minimum_group_weight_mass"])
    group_depths: dict[str, float] = {}
    for group in SOURCE_GROUP_KEYS:
        if group_mass[group] < minimum_mass or not depth_rows[group]:
            raise SemanticLtxContractError(
                f"Semantic source group {group} has insufficient skin-weight mass: "
                f"{group_mass[group]:.9g} < {minimum_mass:.9g}"
            )
        group_depths[group] = _weighted_median(depth_rows[group])

    assignments: dict[str, dict[str, Any]] = {}
    source_to_output: dict[str, str] = {}
    minimum_separation = float(profile.gates["minimum_near_far_depth_separation"])
    for anatomy in ("fore", "hind"):
        left = f"{anatomy}_left"
        right = f"{anatomy}_right"
        separation = abs(group_depths[left] - group_depths[right])
        if separation < minimum_separation:
            raise SemanticLtxContractError(
                f"Canonical camera cannot resolve {anatomy} near/far: "
                f"separation={separation:.9g} < {minimum_separation:.9g}"
            )
        near, far = (
            (left, right)
            if group_depths[left] < group_depths[right]
            else (right, left)
        )
        source_to_output[near] = f"{anatomy}_near"
        source_to_output[far] = f"{anatomy}_far"
        assignments[anatomy] = {
            "near_source_group": near,
            "far_source_group": far,
            "near_camera_z": group_depths[near],
            "far_camera_z": group_depths[far],
            "separation": separation,
            "statistic": "skin_weighted_median_positive_camera_z",
        }

    minimum_limb_weight = float(profile.gates["minimum_face_limb_weight"])
    minimum_dominance = float(profile.gates["minimum_face_dominance"])
    face_labels: dict[int, str] = {}
    source_groups: dict[int, str] = {}
    source_counts = {key: 0 for key in SOURCE_GROUP_KEYS}
    ambiguous: list[dict[str, Any]] = []
    seen_faces: set[int] = set()
    minimum_observed_dominance = 1.0
    minimum_observed_limb_weight = 1.0
    for raw in topology_rows:
        face_id = raw.get("face_id")
        vertex_ids = raw.get("vertex_ids")
        if (
            isinstance(face_id, bool)
            or not isinstance(face_id, int)
            or face_id <= 0
            or face_id in seen_faces
        ):
            raise SemanticLtxContractError("Topology rows require unique positive face_id values")
        if not isinstance(vertex_ids, list) or len(vertex_ids) < 3:
            raise SemanticLtxContractError(f"Topology face {face_id} has invalid vertex_ids")
        seen_faces.add(face_id)
        try:
            rows = [vertex_scores[int(vertex_id)] for vertex_id in vertex_ids]
        except (KeyError, TypeError, ValueError) as exc:
            raise SemanticLtxContractError(
                f"Topology face {face_id} references a missing skin vertex"
            ) from exc
        scores = {
            group: sum(row[group] for row in rows) / len(rows)
            for group in SOURCE_GROUP_KEYS
        }
        limb_weight = sum(scores.values())
        if limb_weight < minimum_limb_weight:
            continue
        dominant_group = max(SOURCE_GROUP_KEYS, key=lambda group: (scores[group], group))
        dominance = scores[dominant_group] / limb_weight
        minimum_observed_dominance = min(minimum_observed_dominance, dominance)
        minimum_observed_limb_weight = min(minimum_observed_limb_weight, limb_weight)
        if dominance < minimum_dominance:
            ambiguous.append(
                {
                    "face_id": face_id,
                    "dominance": dominance,
                    "limb_weight": limb_weight,
                    "scores": scores,
                }
            )
            continue
        source_groups[face_id] = dominant_group
        face_labels[face_id] = source_to_output[dominant_group]
        source_counts[dominant_group] += 1
    if ambiguous:
        preview = ", ".join(
            f"{row['face_id']}:{row['dominance']:.6g}" for row in ambiguous[:12]
        )
        raise SemanticLtxContractError(
            f"Semantic limb faces fail dominance gate {minimum_dominance:.6g}: {preview}"
        )
    minimum_faces = int(profile.gates["minimum_faces_per_source_group"])
    insufficient = {
        group: count for group, count in source_counts.items() if count < minimum_faces
    }
    if insufficient:
        raise SemanticLtxContractError(
            "Semantic source groups have insufficient classified faces: "
            + json.dumps(insufficient, sort_keys=True)
        )
    output_counts = {
        label: sum(1 for value in face_labels.values() if value == label)
        for label in OUTPUT_LABEL_KEYS
    }
    return SemanticLtxPlan(
        face_labels=face_labels,
        source_groups=source_groups,
        contract={
            "depth_space": "positive_camera_z",
            "near_far_assignment": assignments,
            "source_group_camera_z": group_depths,
            "source_group_weight_mass": group_mass,
            "source_group_face_counts": source_counts,
            "output_label_face_counts": output_counts,
            "classified_limb_faces": len(face_labels),
            "body_faces": len(seen_faces) - len(face_labels),
            "total_faces": len(seen_faces),
            "minimum_observed_face_dominance": minimum_observed_dominance,
            "minimum_observed_face_limb_weight": minimum_observed_limb_weight,
        },
    )


def decode_semantic_label_masks(
    profile: SemanticLtxProfile,
    overlay_rgb: np.ndarray,
    overlay_alpha: np.ndarray,
) -> dict[str, np.ndarray]:
    rgb = np.asarray(overlay_rgb, dtype=np.float64)
    alpha = np.asarray(overlay_alpha, dtype=np.float64)
    if rgb.ndim != 3 or rgb.shape[2] != 3 or alpha.shape != rgb.shape[:2]:
        raise SemanticLtxContractError("Semantic overlay RGB/alpha dimensions are invalid")
    if not np.all(np.isfinite(rgb)) or not np.all(np.isfinite(alpha)):
        raise SemanticLtxContractError("Semantic overlay contains non-finite values")
    linear_colors = np.asarray([profile.palette[label] for label in OUTPUT_LABEL_KEYS])
    colors = np.where(
        linear_colors <= 0.0031308,
        12.92 * linear_colors,
        1.055 * np.power(linear_colors, 1.0 / 2.4) - 0.055,
    )
    distances = np.linalg.norm(rgb[:, :, None, :] - colors[None, None, :, :], axis=3)
    nearest = np.argmin(distances, axis=2)
    nearest_distance = np.min(distances, axis=2)
    foreground = alpha >= float(profile.gates["mask_threshold"])
    confident = nearest_distance <= float(profile.gates["pixel_color_tolerance"])
    return {
        label: foreground & confident & (nearest == index)
        for index, label in enumerate(OUTPUT_LABEL_KEYS)
    }


def validate_semantic_pixel_contract(
    profile: SemanticLtxProfile,
    *,
    overlay_alpha: np.ndarray,
    canonical_mask: np.ndarray,
    label_masks: Mapping[str, np.ndarray],
) -> dict[str, Any]:
    alpha = np.asarray(overlay_alpha, dtype=np.float64)
    mask = np.asarray(canonical_mask, dtype=bool)
    if alpha.shape != mask.shape:
        raise SemanticLtxContractError(
            f"Semantic overlay/mask dimensions differ: {alpha.shape} vs {mask.shape}"
        )
    semantic_mask = alpha >= float(profile.gates["mask_threshold"])
    mismatch = int(np.count_nonzero(semantic_mask != mask))
    maximum_mismatch = int(profile.gates["maximum_mask_mismatch_pixels"])
    if mismatch > maximum_mismatch:
        raise SemanticLtxContractError(
            f"Semantic overlay does not use the canonical mask: {mismatch} mismatch pixels"
        )
    mask_pixels = int(np.count_nonzero(mask))
    if mask_pixels <= 0:
        raise SemanticLtxContractError("Canonical mask has no foreground pixels")
    minimum_pixels = int(profile.gates["minimum_pixels_per_output_label"])
    minimum_fraction = float(profile.gates["minimum_pixel_fraction_of_mask"])
    counts: dict[str, int] = {}
    fractions: dict[str, float] = {}
    occupied = np.zeros(mask.shape, dtype=bool)
    for label in OUTPUT_LABEL_KEYS:
        if label not in label_masks:
            raise SemanticLtxContractError(f"Missing semantic label mask: {label}")
        label_mask = np.asarray(label_masks[label], dtype=bool)
        if label_mask.shape != mask.shape:
            raise SemanticLtxContractError(f"Semantic label mask shape differs: {label}")
        if np.any(label_mask & ~semantic_mask):
            raise SemanticLtxContractError(
                f"Semantic label {label} contains pixels outside the animal mask"
            )
        if np.any(occupied & label_mask):
            raise SemanticLtxContractError("Semantic output label masks overlap")
        occupied |= label_mask
        count = int(np.count_nonzero(label_mask))
        fraction = count / mask_pixels
        if count < minimum_pixels or fraction < minimum_fraction:
            raise SemanticLtxContractError(
                f"Semantic label {label} has insufficient pixels: "
                f"count={count}, fraction={fraction:.9g}"
            )
        counts[label] = count
        fractions[label] = fraction
    return {
        "mask_threshold": float(profile.gates["mask_threshold"]),
        "mask_foreground_pixels": mask_pixels,
        "semantic_mask_mismatch_pixels": mismatch,
        "output_label_pixel_counts": counts,
        "output_label_mask_fractions": fractions,
    }
