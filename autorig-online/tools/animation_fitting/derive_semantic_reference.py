from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
from PIL import Image
from scipy.ndimage import distance_transform_edt

from .errors import ContractError, DependencyUnavailableError
from .rig import RigBundle, load_rig_bundle
from .semantic_ltx_reference import (
    OUTPUT_LABEL_KEYS,
    SemanticLtxContractError,
    SemanticLtxPlan,
    SemanticLtxProfile,
    build_semantic_ltx_plan,
    load_semantic_ltx_profile,
    validate_semantic_pixel_contract,
    validate_semantic_profile_source,
)


DERIVATION_SCHEMA = "autorig-ltx-semantic-reference-derivation.v1"
IMMUTABLE_OUTPUT_SCHEMA = "autorig-ltx-semantic-reference-output.v1"
DERIVATION_REVISION = "offline_face_id_skin_weights_v2"
SEMANTIC_FILENAME = "reference_ltx_semantic.png"
DERIVATION_MANIFEST_FILENAME = "semantic_reference.json"
IMMUTABLE_MANIFEST_FILENAME = "immutable_manifest.json"
LABEL_ORDER = ("body", *OUTPUT_LABEL_KEYS)
FACE_ID_QUANTIZATION_TOLERANCE_BYTES = 0.51
MAXIMUM_FACE_ID_FILL_FRACTION = 0.005
MAXIMUM_FACE_ID_FILL_DISTANCE_PIXELS = 1.5
MAXIMUM_TOPOLOGY_RASTER_FILL_FRACTION = 0.03
MAXIMUM_TOPOLOGY_RASTER_FILL_DISTANCE_PIXELS = 1.5
MINIMUM_FACE_ID_PROJECTION_EXACT_AGREEMENT = 0.75
MINIMUM_FACE_ID_PROJECTION_LABEL_AGREEMENT = 0.90


@dataclass(frozen=True)
class FaceIdDecodeResult:
    label_indices: np.ndarray
    foreground_mask: np.ndarray
    label_masks: dict[str, np.ndarray]
    decoded_face_ids: np.ndarray
    direct_mask: np.ndarray
    stats: dict[str, Any]


@dataclass(frozen=True)
class TopologyRasterResult:
    label_indices: np.ndarray
    foreground_mask: np.ndarray
    label_masks: dict[str, np.ndarray]
    face_ids: np.ndarray
    direct_mask: np.ndarray
    stats: dict[str, Any]


@dataclass(frozen=True)
class SemanticReferenceResult:
    output_dir: Path
    semantic_path: Path
    derivation_manifest_path: Path
    immutable_manifest_path: Path
    semantic_sha256: str
    immutable_manifest_sha256: str


def sha256_file(path: str | Path) -> str:
    source = Path(path)
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_contract_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def snapshot_source_files(paths: Sequence[str | Path]) -> dict[Path, dict[str, Any]]:
    snapshot: dict[Path, dict[str, Any]] = {}
    casefolded: set[str] = set()
    for raw_path in paths:
        path = Path(raw_path).resolve()
        identity = str(path).casefold()
        if identity in casefolded:
            raise SemanticLtxContractError(f"Source artifact collision: {path}")
        casefolded.add(identity)
        if not path.is_file():
            raise SemanticLtxContractError(f"Source artifact is missing: {path}")
        snapshot[path] = {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    return snapshot


def verify_source_snapshot(snapshot: Mapping[Path, Mapping[str, Any]]) -> None:
    for path, expected in snapshot.items():
        if not path.is_file():
            raise SemanticLtxContractError(f"Source artifact disappeared during derivation: {path}")
        actual_bytes = path.stat().st_size
        if actual_bytes != expected["bytes"]:
            raise SemanticLtxContractError(
                f"Source artifact size changed during derivation: {path}"
            )
        if sha256_file(path) != expected["sha256"]:
            raise SemanticLtxContractError(
                f"Source artifact SHA-256 changed during derivation: {path}"
            )


def encode_face_id_rgb(face_id: int) -> np.ndarray:
    if isinstance(face_id, bool) or not isinstance(face_id, int) or not (1 <= face_id < 1 << 24):
        raise SemanticLtxContractError("face_id must be an integer inside [1, 2^24)")
    return np.asarray(
        (
            float(face_id & 255) / 255.0,
            float((face_id >> 8) & 255) / 255.0,
            float((face_id >> 16) & 255) / 255.0,
        ),
        dtype=np.float64,
    )


def _validated_topology_face_ids(face_ids: Sequence[int]) -> tuple[int, ...]:
    values: list[int] = []
    seen: set[int] = set()
    for raw_value in face_ids:
        if isinstance(raw_value, bool) or not isinstance(raw_value, int) or raw_value <= 0:
            raise SemanticLtxContractError("Topology face IDs must be positive integers")
        if raw_value in seen:
            raise SemanticLtxContractError(
                f"Topology face-ID collision: face_id {raw_value} occurs more than once"
            )
        seen.add(raw_value)
        values.append(raw_value)
    if not values:
        raise SemanticLtxContractError("Topology contains no face IDs")
    expected = tuple(range(1, len(values) + 1))
    if tuple(sorted(values)) != expected:
        raise SemanticLtxContractError(
            "Topology face IDs must be contiguous from 1 through the declared face count"
        )
    return tuple(values)


def decode_face_id_labels(
    face_id_rgb: np.ndarray,
    mask_alpha: np.ndarray,
    *,
    topology_face_ids: Sequence[int],
    face_labels: Mapping[int, str],
    mask_threshold: float,
    quantization_tolerance_bytes: float = FACE_ID_QUANTIZATION_TOLERANCE_BYTES,
    maximum_fill_fraction: float = MAXIMUM_FACE_ID_FILL_FRACTION,
    maximum_fill_distance_pixels: float = MAXIMUM_FACE_ID_FILL_DISTANCE_PIXELS,
) -> FaceIdDecodeResult:
    rgb = np.asarray(face_id_rgb, dtype=np.float64)
    alpha = np.asarray(mask_alpha, dtype=np.float64)
    if rgb.ndim != 3 or rgb.shape[2] != 3 or alpha.shape != rgb.shape[:2]:
        raise SemanticLtxContractError("face-ID RGB and canonical mask dimensions differ")
    if not np.all(np.isfinite(rgb)) or not np.all(np.isfinite(alpha)):
        raise SemanticLtxContractError("face-ID RGB and canonical mask must be finite")
    if np.any(rgb < -1e-6) or np.any(rgb > 1.000001):
        raise SemanticLtxContractError("face-ID EXR channels must stay inside [0, 1]")
    if np.any(alpha < 0.0) or np.any(alpha > 1.0):
        raise SemanticLtxContractError("canonical mask alpha must stay inside [0, 1]")
    if not 0.0 < mask_threshold <= 1.0:
        raise SemanticLtxContractError("mask_threshold must be inside (0, 1]")
    if not 0.0 < quantization_tolerance_bytes <= 0.51:
        raise SemanticLtxContractError(
            "face-ID quantization tolerance must be inside (0, 0.51] bytes"
        )
    if not 0.0 <= maximum_fill_fraction <= 1.0:
        raise SemanticLtxContractError("maximum_fill_fraction must be inside [0, 1]")
    if maximum_fill_distance_pixels <= 0.0:
        raise SemanticLtxContractError("maximum_fill_distance_pixels must be positive")

    topology_ids = _validated_topology_face_ids(topology_face_ids)
    topology_set = set(topology_ids)
    unknown_labels = sorted(set(face_labels).difference(topology_set))
    if unknown_labels:
        raise SemanticLtxContractError(
            f"Semantic face labels reference unknown topology IDs: {unknown_labels[:12]}"
        )
    invalid_labels = sorted(set(face_labels.values()).difference(OUTPUT_LABEL_KEYS))
    if invalid_labels:
        raise SemanticLtxContractError(
            f"Semantic face labels contain unsupported labels: {invalid_labels}"
        )

    foreground = alpha >= mask_threshold
    foreground_pixels = int(np.count_nonzero(foreground))
    if foreground_pixels <= 0:
        raise SemanticLtxContractError("canonical mask has no foreground pixels")

    # Blender's emission pass is coverage-premultiplied at silhouette pixels.
    # The independently rendered canonical mask has the same fixed camera and
    # sample pattern, so unpremultiplication restores the 24-bit face code.
    unpremultiplied = np.zeros_like(rgb)
    unpremultiplied[foreground] = (
        rgb[foreground] / alpha[foreground, None]
    )
    if np.any(unpremultiplied[foreground] < -1e-5) or np.any(
        unpremultiplied[foreground] > 1.005
    ):
        raise SemanticLtxContractError(
            "face-ID EXR cannot be unpremultiplied with the canonical mask"
        )
    scaled = unpremultiplied * 255.0
    rounded = np.rint(scaled)
    residual = np.max(np.abs(scaled - rounded), axis=2)
    bytes_rgb = rounded.astype(np.int64)
    byte_range = np.all((bytes_rgb >= 0) & (bytes_rgb <= 255), axis=2)
    decoded_ids = (
        bytes_rgb[:, :, 0]
        + (bytes_rgb[:, :, 1] << 8)
        + (bytes_rgb[:, :, 2] << 16)
    )
    topology_lookup = np.zeros(max(topology_ids) + 1, dtype=bool)
    topology_lookup[np.asarray(topology_ids, dtype=np.int64)] = True
    in_topology = (
        (decoded_ids >= 0)
        & (decoded_ids < topology_lookup.size)
        & topology_lookup[np.clip(decoded_ids, 0, topology_lookup.size - 1)]
    )
    direct = (
        foreground
        & byte_range
        & (residual <= quantization_tolerance_bytes)
        & in_topology
    )
    direct_pixels = int(np.count_nonzero(direct))
    if direct_pixels <= 0:
        raise SemanticLtxContractError("face-ID EXR contains no directly decodable pixels")

    fill_mask = foreground & ~direct
    filled_pixels = int(np.count_nonzero(fill_mask))
    fill_fraction = filled_pixels / foreground_pixels
    if fill_fraction > maximum_fill_fraction:
        raise SemanticLtxContractError(
            "face-ID undecodable pixel fraction exceeds the fill gate: "
            f"{fill_fraction:.9g} > {maximum_fill_fraction:.9g}"
        )

    label_indices = np.full(alpha.shape, -1, dtype=np.int16)
    direct_ids = decoded_ids[direct]
    direct_labels = np.zeros(direct_ids.shape, dtype=np.int16)
    for label_index, label in enumerate(LABEL_ORDER[1:], start=1):
        labelled_faces = np.fromiter(
            (face_id for face_id, value in face_labels.items() if value == label),
            dtype=np.int64,
        )
        if labelled_faces.size:
            direct_labels[np.isin(direct_ids, labelled_faces)] = label_index
    label_indices[direct] = direct_labels

    maximum_fill_distance = 0.0
    if filled_pixels:
        distances, nearest = distance_transform_edt(~direct, return_indices=True)
        maximum_fill_distance = float(np.max(distances[fill_mask]))
        if maximum_fill_distance > maximum_fill_distance_pixels:
            raise SemanticLtxContractError(
                "face-ID undecodable pixels exceed the nearest-label distance gate: "
                f"{maximum_fill_distance:.9g} > {maximum_fill_distance_pixels:.9g}"
            )
        label_indices[fill_mask] = label_indices[
            nearest[0][fill_mask], nearest[1][fill_mask]
        ]
    if np.any(label_indices[foreground] < 0):
        raise SemanticLtxContractError("face-ID fill left foreground pixels unlabeled")

    label_masks = {
        label: foreground & (label_indices == label_index)
        for label_index, label in enumerate(LABEL_ORDER[1:], start=1)
    }
    decoded_unique = sorted(int(value) for value in np.unique(decoded_ids[direct]))
    missing_decoded = sorted(topology_set.difference(decoded_unique))
    nonintegral = foreground & (residual > quantization_tolerance_bytes)
    out_of_topology = foreground & byte_range & ~in_topology
    stats = {
        "encoding": "uint24_little_endian_rgb",
        "coverage_unpremultiply": "reference_face_id_rgb_divided_by_reference_mask_alpha",
        "mask_threshold": mask_threshold,
        "quantization_tolerance_bytes": quantization_tolerance_bytes,
        "topology_face_count": len(topology_ids),
        "foreground_pixels": foreground_pixels,
        "directly_decoded_pixels": direct_pixels,
        "filled_pixels": filled_pixels,
        "fill_fraction": fill_fraction,
        "maximum_fill_fraction_gate": maximum_fill_fraction,
        "maximum_fill_distance_pixels": maximum_fill_distance,
        "maximum_fill_distance_gate_pixels": maximum_fill_distance_pixels,
        "nonintegral_foreground_pixels": int(np.count_nonzero(nonintegral)),
        "out_of_topology_foreground_pixels": int(np.count_nonzero(out_of_topology)),
        "decoded_unique_face_count": len(decoded_unique),
        "decoded_face_id_minimum": min(decoded_unique),
        "decoded_face_id_maximum": max(decoded_unique),
        "topology_faces_without_direct_pixels": missing_decoded,
    }
    return FaceIdDecodeResult(
        label_indices=label_indices,
        foreground_mask=foreground,
        label_masks=label_masks,
        decoded_face_ids=decoded_ids,
        direct_mask=direct,
        stats=stats,
    )


def rasterize_topology_labels(
    *,
    skin_rows: Sequence[Mapping[str, Any]],
    topology_rows: Sequence[Mapping[str, Any]],
    camera: Mapping[str, Any],
    canonical_mask: np.ndarray,
    face_labels: Mapping[int, str],
    maximum_fill_fraction: float = MAXIMUM_TOPOLOGY_RASTER_FILL_FRACTION,
    maximum_fill_distance_pixels: float = MAXIMUM_TOPOLOGY_RASTER_FILL_DISTANCE_PIXELS,
) -> TopologyRasterResult:
    mask = np.asarray(canonical_mask, dtype=bool)
    if mask.ndim != 2 or not np.any(mask):
        raise SemanticLtxContractError("canonical mask must be a non-empty 2D array")
    height, width = mask.shape
    resolution = camera.get("resolution")
    if resolution != [width, height]:
        raise SemanticLtxContractError(
            "camera resolution and canonical mask dimensions differ"
        )
    intrinsics = camera.get("intrinsics")
    if not isinstance(intrinsics, dict):
        raise SemanticLtxContractError("camera intrinsics are missing")
    try:
        declared_fx = float(intrinsics["fx"])
        declared_fy = float(intrinsics["fy"])
        cx = float(intrinsics["cx"])
        cy = float(intrinsics["cy"])
        lens_mm = float(camera["lens_mm"])
        sensor_width_mm = float(camera["sensor_width_mm"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SemanticLtxContractError("camera calibration is invalid") from exc
    calibration = np.asarray(
        (declared_fx, declared_fy, cx, cy, lens_mm, sensor_width_mm),
        dtype=np.float64,
    )
    if not np.all(np.isfinite(calibration)) or np.any(calibration[:2] <= 0.0):
        raise SemanticLtxContractError("camera calibration must be finite and positive")
    if lens_mm <= 0.0 or sensor_width_mm <= 0.0:
        raise SemanticLtxContractError("camera lens and sensor width must be positive")
    horizontal_focal = lens_mm * width / sensor_width_mm
    if not math.isclose(declared_fx, horizontal_focal, rel_tol=1e-6, abs_tol=1e-5):
        raise SemanticLtxContractError(
            "camera fx disagrees with lens/sensor-width calibration"
        )
    # Blender renders this actionless bundle with square pixels and horizontal
    # sensor fit.  The legacy bundle's angle_y-derived `fy` is not the actual
    # pixel focal length (the silhouette proves the horizontal focal applies
    # on both axes).  Alignment with the independent face-ID pass is gated
    # below, so a different camera convention fails closed.
    effective_fx = declared_fx
    effective_fy = declared_fx
    matrix = np.asarray(camera.get("world_to_camera"), dtype=np.float64)
    if matrix.size != 16:
        raise SemanticLtxContractError("camera world_to_camera must contain 16 values")
    matrix = matrix.reshape(4, 4)
    if not np.all(np.isfinite(matrix)) or abs(float(np.linalg.det(matrix))) <= 1e-12:
        raise SemanticLtxContractError("camera world_to_camera must be finite and invertible")

    vertex_world: dict[int, np.ndarray] = {}
    for row in skin_rows:
        vertex_id = row.get("vertex_id")
        world = np.asarray(row.get("world"), dtype=np.float64)
        if (
            isinstance(vertex_id, bool)
            or not isinstance(vertex_id, int)
            or vertex_id < 0
            or vertex_id in vertex_world
            or world.shape != (3,)
            or not np.all(np.isfinite(world))
        ):
            raise SemanticLtxContractError(
                "skin rows require unique non-negative IDs and finite world positions"
            )
        vertex_world[vertex_id] = world
    if not vertex_world:
        raise SemanticLtxContractError("skin rows contain no vertices")
    ordered_vertex_ids = sorted(vertex_world)
    if ordered_vertex_ids != list(range(len(vertex_world))):
        raise SemanticLtxContractError("skin vertex IDs must be contiguous from zero")
    world = np.asarray([vertex_world[index] for index in ordered_vertex_ids])
    homogeneous = np.concatenate(
        (world, np.ones((world.shape[0], 1), dtype=np.float64)), axis=1
    )
    camera_points = (matrix @ homogeneous.T).T
    camera_w = camera_points[:, 3]
    if np.any(np.abs(camera_w) <= 1e-12):
        raise SemanticLtxContractError("projected skin vertices contain invalid camera W")
    camera_xyz = camera_points[:, :3] / camera_w[:, None]
    depths = -camera_xyz[:, 2]
    if np.any(~np.isfinite(depths)) or np.any(depths <= 0.0):
        raise SemanticLtxContractError("all topology vertices must be in front of the camera")
    projected_x = effective_fx * camera_xyz[:, 0] / depths + cx
    projected_y = cy - effective_fy * camera_xyz[:, 1] / depths

    topology_face_ids = [row.get("face_id") for row in topology_rows]
    _validated_topology_face_ids(topology_face_ids)
    depth_buffer = np.full((height, width), np.inf, dtype=np.float64)
    face_buffer = np.zeros((height, width), dtype=np.int32)
    triangle_count = 0
    degenerate_triangles = 0
    for row in topology_rows:
        face_id = int(row["face_id"])
        vertex_ids = row.get("vertex_ids")
        if (
            not isinstance(vertex_ids, list)
            or len(vertex_ids) < 3
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value not in vertex_world
                for value in vertex_ids
            )
        ):
            raise SemanticLtxContractError(
                f"Topology face {face_id} has invalid vertex IDs"
            )
        for corner in range(1, len(vertex_ids) - 1):
            triangle_count += 1
            indices = np.asarray(
                (vertex_ids[0], vertex_ids[corner], vertex_ids[corner + 1]),
                dtype=np.int64,
            )
            xs = projected_x[indices]
            ys = projected_y[indices]
            triangle_depths = depths[indices]
            minimum_x = max(0, int(np.floor(np.min(xs))))
            maximum_x = min(width - 1, int(np.ceil(np.max(xs))))
            minimum_y = max(0, int(np.floor(np.min(ys))))
            maximum_y = min(height - 1, int(np.ceil(np.max(ys))))
            if minimum_x > maximum_x or minimum_y > maximum_y:
                continue
            denominator = (
                (ys[1] - ys[2]) * (xs[0] - xs[2])
                + (xs[2] - xs[1]) * (ys[0] - ys[2])
            )
            if abs(float(denominator)) <= 1e-12:
                degenerate_triangles += 1
                continue
            sample_x, sample_y = np.meshgrid(
                np.arange(minimum_x, maximum_x + 1, dtype=np.float64) + 0.5,
                np.arange(minimum_y, maximum_y + 1, dtype=np.float64) + 0.5,
            )
            barycentric_0 = (
                (ys[1] - ys[2]) * (sample_x - xs[2])
                + (xs[2] - xs[1]) * (sample_y - ys[2])
            ) / denominator
            barycentric_1 = (
                (ys[2] - ys[0]) * (sample_x - xs[2])
                + (xs[0] - xs[2]) * (sample_y - ys[2])
            ) / denominator
            barycentric_2 = 1.0 - barycentric_0 - barycentric_1
            inside = (
                (barycentric_0 >= -1e-8)
                & (barycentric_1 >= -1e-8)
                & (barycentric_2 >= -1e-8)
            )
            raster_depth = 1.0 / (
                barycentric_0 / triangle_depths[0]
                + barycentric_1 / triangle_depths[1]
                + barycentric_2 / triangle_depths[2]
            )
            depth_region = depth_buffer[
                minimum_y : maximum_y + 1,
                minimum_x : maximum_x + 1,
            ]
            face_region = face_buffer[
                minimum_y : maximum_y + 1,
                minimum_x : maximum_x + 1,
            ]
            closer = raster_depth < depth_region - 1e-8
            tied_lower_face = (
                np.abs(raster_depth - depth_region) <= 1e-8
            ) & ((face_region == 0) | (face_id < face_region))
            update = inside & (closer | tied_lower_face)
            depth_region[update] = raster_depth[update]
            face_region[update] = face_id
    if triangle_count <= 0 or degenerate_triangles:
        raise SemanticLtxContractError(
            f"Topology rasterization found {degenerate_triangles} degenerate triangles"
        )

    raster_mask = face_buffer > 0
    outside_mask_pixels = int(np.count_nonzero(raster_mask & ~mask))
    if outside_mask_pixels:
        raise SemanticLtxContractError(
            "Topology projection escaped the canonical mask: "
            f"{outside_mask_pixels} pixels"
        )
    direct_mask = mask & raster_mask
    missing_mask = mask & ~direct_mask
    mask_pixels = int(np.count_nonzero(mask))
    filled_pixels = int(np.count_nonzero(missing_mask))
    fill_fraction = filled_pixels / mask_pixels
    if fill_fraction > maximum_fill_fraction:
        raise SemanticLtxContractError(
            "Topology raster fill fraction exceeds its gate: "
            f"{fill_fraction:.9g} > {maximum_fill_fraction:.9g}"
        )

    label_indices = np.full(mask.shape, -1, dtype=np.int16)
    label_indices[direct_mask] = 0
    for label_index, label in enumerate(LABEL_ORDER[1:], start=1):
        labelled_faces = np.fromiter(
            (face_id for face_id, value in face_labels.items() if value == label),
            dtype=np.int64,
        )
        if labelled_faces.size:
            label_indices[direct_mask & np.isin(face_buffer, labelled_faces)] = label_index
    maximum_fill_distance = 0.0
    if filled_pixels:
        distances, nearest = distance_transform_edt(~direct_mask, return_indices=True)
        maximum_fill_distance = float(np.max(distances[missing_mask]))
        if maximum_fill_distance > maximum_fill_distance_pixels:
            raise SemanticLtxContractError(
                "Topology raster fill distance exceeds its gate: "
                f"{maximum_fill_distance:.9g} > {maximum_fill_distance_pixels:.9g}"
            )
        label_indices[missing_mask] = label_indices[
            nearest[0][missing_mask], nearest[1][missing_mask]
        ]
    if np.any(label_indices[mask] < 0):
        raise SemanticLtxContractError("Topology raster fill left foreground pixels unlabeled")
    label_masks = {
        label: mask & (label_indices == label_index)
        for label_index, label in enumerate(LABEL_ORDER[1:], start=1)
    }
    stats = {
        "projection": "perspective_square_pixel_horizontal_sensor_fit",
        "triangulation": "deterministic_polygon_fan",
        "triangle_count": triangle_count,
        "degenerate_triangle_count": degenerate_triangles,
        "declared_fx": declared_fx,
        "declared_fy": declared_fy,
        "effective_fx": effective_fx,
        "effective_fy": effective_fy,
        "pixel_center_offset": 0.5,
        "z_buffer": "perspective_correct_reciprocal_positive_camera_z",
        "direct_raster_pixels": int(np.count_nonzero(direct_mask)),
        "outside_canonical_mask_pixels": outside_mask_pixels,
        "filled_pixels": filled_pixels,
        "fill_fraction": fill_fraction,
        "maximum_fill_fraction_gate": maximum_fill_fraction,
        "maximum_fill_distance_pixels": maximum_fill_distance,
        "maximum_fill_distance_gate_pixels": maximum_fill_distance_pixels,
        "visible_face_count": int(np.unique(face_buffer[direct_mask]).size),
    }
    return TopologyRasterResult(
        label_indices=label_indices,
        foreground_mask=mask,
        label_masks=label_masks,
        face_ids=face_buffer,
        direct_mask=direct_mask,
        stats=stats,
    )


def validate_face_id_projection_alignment(
    decoded: FaceIdDecodeResult,
    raster: TopologyRasterResult,
) -> dict[str, Any]:
    if decoded.foreground_mask.shape != raster.foreground_mask.shape or not np.array_equal(
        decoded.foreground_mask, raster.foreground_mask
    ):
        raise SemanticLtxContractError("face-ID decode and topology raster masks differ")
    comparison = decoded.direct_mask & raster.direct_mask
    pixels = int(np.count_nonzero(comparison))
    if pixels <= 0:
        raise SemanticLtxContractError("face-ID/topology projection has no comparison pixels")
    exact_matches = int(
        np.count_nonzero(
            comparison & (decoded.decoded_face_ids == raster.face_ids)
        )
    )
    label_matches = int(
        np.count_nonzero(
            comparison & (decoded.label_indices == raster.label_indices)
        )
    )
    exact_fraction = exact_matches / pixels
    label_fraction = label_matches / pixels
    if exact_fraction < MINIMUM_FACE_ID_PROJECTION_EXACT_AGREEMENT:
        raise SemanticLtxContractError(
            "face-ID/topology exact agreement fails its gate: "
            f"{exact_fraction:.9g} < {MINIMUM_FACE_ID_PROJECTION_EXACT_AGREEMENT:.9g}"
        )
    if label_fraction < MINIMUM_FACE_ID_PROJECTION_LABEL_AGREEMENT:
        raise SemanticLtxContractError(
            "face-ID/topology semantic-label agreement fails its gate: "
            f"{label_fraction:.9g} < {MINIMUM_FACE_ID_PROJECTION_LABEL_AGREEMENT:.9g}"
        )
    return {
        "comparison_pixels": pixels,
        "exact_face_id_matches": exact_matches,
        "exact_face_id_agreement_fraction": exact_fraction,
        "minimum_exact_face_id_agreement_gate": (
            MINIMUM_FACE_ID_PROJECTION_EXACT_AGREEMENT
        ),
        "semantic_label_matches": label_matches,
        "semantic_label_agreement_fraction": label_fraction,
        "minimum_semantic_label_agreement_gate": (
            MINIMUM_FACE_ID_PROJECTION_LABEL_AGREEMENT
        ),
        "accepted": True,
    }


def _load_json(path: Path) -> Any:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                return json.load(stream)
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SemanticLtxContractError(f"Cannot decode source artifact {path}: {exc}") from exc


def _load_rgb_png(path: Path, resolution: tuple[int, int]) -> np.ndarray:
    try:
        with Image.open(path) as image:
            if image.format != "PNG" or image.size != resolution:
                raise SemanticLtxContractError(
                    f"{path.name} must be a {resolution[0]}x{resolution[1]} PNG"
                )
            image.verify()
        with Image.open(path) as image:
            return np.asarray(image.convert("RGB"), dtype=np.uint8).copy()
    except SemanticLtxContractError:
        raise
    except Exception as exc:
        raise SemanticLtxContractError(f"Cannot decode PNG {path}: {exc}") from exc


def _load_mask_png(path: Path, resolution: tuple[int, int]) -> np.ndarray:
    try:
        with Image.open(path) as image:
            if image.format != "PNG" or image.size != resolution:
                raise SemanticLtxContractError(
                    f"{path.name} must be a {resolution[0]}x{resolution[1]} PNG"
                )
            return np.asarray(image.convert("L"), dtype=np.float64) / 255.0
    except SemanticLtxContractError:
        raise
    except Exception as exc:
        raise SemanticLtxContractError(f"Cannot decode mask PNG {path}: {exc}") from exc


def _load_face_id_exr(path: Path, resolution: tuple[int, int]) -> np.ndarray:
    os.environ.setdefault("OPENCV_IO_ENABLE_OPENEXR", "1")
    try:
        import cv2
    except Exception as exc:
        raise DependencyUnavailableError(
            "opencv-python-headless==4.11.0.86 is required to decode reference_face_id.exr"
        ) from exc
    try:
        decoded = cv2.imread(str(path), cv2.IMREAD_UNCHANGED)
    except Exception as exc:
        raise SemanticLtxContractError(f"Cannot decode face-ID EXR {path}: {exc}") from exc
    if decoded is None or decoded.ndim != 3 or decoded.shape[2] not in {3, 4}:
        raise SemanticLtxContractError(f"face-ID EXR must decode as HxWx3/4: {path}")
    if decoded.shape[:2] != (resolution[1], resolution[0]):
        raise SemanticLtxContractError(
            f"face-ID EXR resolution differs from canonical camera: {decoded.shape[:2]}"
        )
    # OpenCV exposes EXR color channels in BGR(A) order.
    return np.asarray(decoded[:, :, :3][:, :, ::-1], dtype=np.float64)


def _linear_to_srgb8(colors: np.ndarray) -> np.ndarray:
    linear = np.clip(np.asarray(colors, dtype=np.float64), 0.0, 1.0)
    srgb = np.where(
        linear <= 0.0031308,
        12.92 * linear,
        1.055 * np.power(linear, 1.0 / 2.4) - 0.055,
    )
    return np.rint(np.clip(srgb, 0.0, 1.0) * 255.0).astype(np.uint8)


def compose_semantic_reference(
    canonical_rgb: np.ndarray,
    decoded: FaceIdDecodeResult | TopologyRasterResult,
    profile: SemanticLtxProfile,
) -> tuple[np.ndarray, dict[str, Any]]:
    source = np.asarray(canonical_rgb, dtype=np.uint8)
    if source.ndim != 3 or source.shape[2] != 3 or source.shape[:2] != decoded.foreground_mask.shape:
        raise SemanticLtxContractError("canonical RGB and decoded face-ID dimensions differ")
    pixel_contract = validate_semantic_pixel_contract(
        profile,
        overlay_alpha=decoded.foreground_mask.astype(np.float64),
        canonical_mask=decoded.foreground_mask,
        label_masks=decoded.label_masks,
    )
    palette = _linear_to_srgb8(
        np.asarray([profile.palette[label] for label in LABEL_ORDER], dtype=np.float64)
    )
    output = source.copy()
    output[decoded.foreground_mask] = palette[
        decoded.label_indices[decoded.foreground_mask]
    ]
    if not np.array_equal(
        output[~decoded.foreground_mask], source[~decoded.foreground_mask]
    ):
        raise SemanticLtxContractError("semantic composition changed canonical background pixels")
    for label_index, label in enumerate(LABEL_ORDER[1:], start=1):
        mask = decoded.label_masks[label]
        if not np.all(output[mask] == palette[label_index]):
            raise SemanticLtxContractError(f"semantic output palette mismatch for {label}")
    return output, {
        **pixel_contract,
        "palette_srgb8": {
            label: [int(channel) for channel in palette[index]]
            for index, label in enumerate(LABEL_ORDER)
        },
        "background_pixels_preserved_exactly": True,
    }


def _require_artifacts(rig: RigBundle) -> dict[str, Path]:
    required = {
        "rgb",
        "mask",
        "face_id",
        "skeleton",
        "skin_weights",
        "surface_topology",
    }
    missing = sorted(required.difference(rig.artifacts))
    if missing:
        raise SemanticLtxContractError(
            f"Actionless bundle lacks semantic derivation artifacts: {missing}"
        )
    paths = {key: rig.artifacts[key].resolve() for key in sorted(required)}
    if len({str(path).casefold() for path in paths.values()}) != len(paths):
        raise SemanticLtxContractError("Bundle artifact path collision")
    return paths


def _source_identity(
    rig: RigBundle,
    profile: SemanticLtxProfile,
    skin_rows: Sequence[Mapping[str, Any]],
    topology_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[str], list[str]]:
    source = rig.metadata.get("source")
    if not isinstance(source, dict):
        raise SemanticLtxContractError("Bundle source identity is missing")
    skin_meshes = sorted(
        {
            row.get("object")
            for row in skin_rows
            if isinstance(row, dict) and isinstance(row.get("object"), str)
        }
    )
    topology_meshes = sorted(
        {
            row.get("object")
            for row in topology_rows
            if isinstance(row, dict) and isinstance(row.get("object"), str)
        }
    )
    if not skin_meshes or skin_meshes != topology_meshes:
        raise SemanticLtxContractError(
            "skin_weights and surface_topology mesh identities differ"
        )
    validate_semantic_profile_source(
        profile,
        rig_type=str(source.get("rig_type") or ""),
        filename=str(source.get("filename") or ""),
        source_sha256=str(source.get("sha256") or ""),
        armature_name=rig.armature_name,
        mesh_names=skin_meshes,
    )
    available_bones = [
        name for name in rig.bone_order if rig.bones[name].use_deform
    ]
    return skin_meshes, available_bones


def _artifact_record(path: Path, root: Path | None = None) -> dict[str, Any]:
    filename = path.name if root is None else path.resolve().relative_to(root.resolve()).as_posix()
    return {
        "filename": filename,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _write_png(path: Path, pixels: np.ndarray) -> None:
    Image.fromarray(np.asarray(pixels, dtype=np.uint8), mode="RGB").save(
        path,
        format="PNG",
        optimize=False,
        compress_level=9,
    )


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _source_artifact_contract(
    rig: RigBundle,
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    metadata_artifacts = rig.metadata.get("artifacts")
    if not isinstance(metadata_artifacts, dict):
        raise SemanticLtxContractError("Bundle artifact metadata is missing")
    result: dict[str, Any] = {}
    for key, path in sorted(paths.items()):
        declared = metadata_artifacts.get(key)
        if not isinstance(declared, dict):
            raise SemanticLtxContractError(f"Bundle artifact declaration is missing: {key}")
        actual = _artifact_record(path)
        if (
            declared.get("filename") != actual["filename"]
            or declared.get("bytes") != actual["bytes"]
            or str(declared.get("sha256") or "").lower() != actual["sha256"]
        ):
            raise SemanticLtxContractError(
                f"Bundle artifact declaration changed after immutable verification: {key}"
            )
        result[key] = actual
    return result


def derive_semantic_reference(
    bundle_dir: str | Path,
    profile_path: str | Path,
    output_dir: str | Path,
) -> SemanticReferenceResult:
    bundle_root = Path(bundle_dir).resolve()
    profile_file = Path(profile_path).resolve()
    target = Path(output_dir).resolve()
    if target == bundle_root or bundle_root in target.parents:
        raise SemanticLtxContractError(
            "Output directory must not be inside the immutable source bundle: "
            f"{target}"
        )
    if target.exists():
        raise SemanticLtxContractError(
            f"Output directory collision; immutable output must not already exist: {target}"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    rig = load_rig_bundle(bundle_root)
    profile = load_semantic_ltx_profile(profile_file)
    paths = _require_artifacts(rig)
    source_files = [
        rig.metadata_path,
        rig.immutable_manifest_path,
        profile.path,
        *paths.values(),
    ]
    source_snapshot = snapshot_source_files(source_files)

    skin_payload = _load_json(paths["skin_weights"])
    topology_payload = _load_json(paths["surface_topology"])
    if not isinstance(skin_payload, dict) or set(skin_payload) != {"vertices"}:
        raise SemanticLtxContractError("skin_weights root must contain only vertices")
    if not isinstance(topology_payload, dict) or set(topology_payload) != {"faces"}:
        raise SemanticLtxContractError("surface_topology root must contain only faces")
    skin_rows = skin_payload["vertices"]
    topology_rows = topology_payload["faces"]
    if not isinstance(skin_rows, list) or not isinstance(topology_rows, list):
        raise SemanticLtxContractError("skin/topology records must be arrays")
    counts = rig.metadata.get("counts")
    if not isinstance(counts, dict):
        raise SemanticLtxContractError("Bundle counts are missing")
    if len(skin_rows) != counts.get("vertices") or len(topology_rows) != counts.get("faces"):
        raise SemanticLtxContractError("skin/topology rows disagree with bundle counts")

    mesh_names, available_bones = _source_identity(
        rig,
        profile,
        skin_rows,
        topology_rows,
    )
    plan: SemanticLtxPlan = build_semantic_ltx_plan(
        profile,
        skin_rows=skin_rows,
        topology_rows=topology_rows,
        available_bones=available_bones,
        world_to_camera=rig.camera.world_to_camera,
    )
    topology_face_ids = [row.get("face_id") for row in topology_rows]
    _validated_topology_face_ids(topology_face_ids)
    resolution = (rig.camera.width, rig.camera.height)
    canonical_rgb = _load_rgb_png(paths["rgb"], resolution)
    mask_alpha = _load_mask_png(paths["mask"], resolution)
    face_id_rgb = _load_face_id_exr(paths["face_id"], resolution)
    decoded = decode_face_id_labels(
        face_id_rgb,
        mask_alpha,
        topology_face_ids=topology_face_ids,
        face_labels=plan.face_labels,
        mask_threshold=float(profile.gates["mask_threshold"]),
    )
    topology_raster = rasterize_topology_labels(
        skin_rows=skin_rows,
        topology_rows=topology_rows,
        camera=rig.metadata["camera"],
        canonical_mask=decoded.foreground_mask,
        face_labels=plan.face_labels,
    )
    alignment_contract = validate_face_id_projection_alignment(
        decoded,
        topology_raster,
    )
    semantic_rgb, pixel_contract = compose_semantic_reference(
        canonical_rgb,
        topology_raster,
        profile,
    )
    source_artifacts = _source_artifact_contract(rig, paths)
    verify_source_snapshot(source_snapshot)

    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{target.name}.staging-",
            dir=str(target.parent),
        )
    ).resolve()
    try:
        if staging.parent != target.parent or not staging.name.startswith(f".{target.name}.staging-"):
            raise SemanticLtxContractError("Unsafe semantic-reference staging directory")
        semantic_path = staging / SEMANTIC_FILENAME
        _write_png(semantic_path, semantic_rgb)
        reloaded = _load_rgb_png(semantic_path, resolution)
        if not np.array_equal(reloaded, semantic_rgb):
            raise SemanticLtxContractError("Lossless semantic PNG verification failed")
        semantic_record = _artifact_record(semantic_path, staging)
        source = rig.metadata["source"]
        derivation_manifest = staging / DERIVATION_MANIFEST_FILENAME
        _write_json(
            derivation_manifest,
            {
                "schema": DERIVATION_SCHEMA,
                "revision": DERIVATION_REVISION,
                "created_at_utc": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
                "source_bundle": {
                    "bundle_id": bundle_root.name,
                    "source_model": {
                        "filename": source.get("filename"),
                        "sha256": source.get("sha256"),
                        "rig_type": source.get("rig_type"),
                        "species": source.get("species"),
                    },
                    "fitting_bundle": _artifact_record(rig.metadata_path),
                    "immutable_manifest": _artifact_record(
                        rig.immutable_manifest_path
                    ),
                    "mesh_names": mesh_names,
                    "armature_name": rig.armature_name,
                    "artifacts": source_artifacts,
                    "source_immutability_verified_before_and_after": True,
                },
                "semantic_profile": {
                    "profile_id": profile.profile_id,
                    "filename": profile.path.name,
                    "bytes": profile.path.stat().st_size,
                    "sha256": profile.sha256,
                    "limb_groups": {
                        key: list(value)
                        for key, value in profile.limb_groups.items()
                    },
                    "palette_linear": {
                        key: list(value) for key, value in profile.palette.items()
                    },
                    "gates": dict(profile.gates),
                },
                "camera": {
                    "resolution": [rig.camera.width, rig.camera.height],
                    "contract_sha256": json_contract_sha256(
                        rig.metadata["camera"]
                    ),
                },
                "classification": plan.contract,
                "face_id_decode": decoded.stats,
                "topology_raster": topology_raster.stats,
                "face_id_projection_alignment": alignment_contract,
                "pixels": pixel_contract,
                "composition": (
                    "rig_semantic_palette_inside_canonical_mask_over_unchanged_"
                    "canonical_rgb_background"
                ),
                "method": (
                    "profile_bone_groups_plus_skin_weights_plus_topology_plus_"
                    "canonical_camera_plus_face_id_raster"
                ),
                "manual_painting_used": False,
                "source_blend_required": False,
                "geometry_uv_normals_mutated": False,
                "scope": {
                    "motion_conditioning_r_and_d_only": True,
                    "topology_or_skin_repair_claimed": False,
                    "ltx_generation_authorized": False,
                },
                "output": semantic_record,
            },
        )
        immutable_manifest = staging / IMMUTABLE_MANIFEST_FILENAME
        output_rows = [
            _artifact_record(semantic_path, staging),
            _artifact_record(derivation_manifest, staging),
        ]
        output_rows.sort(key=lambda row: row["filename"])
        _write_json(
            immutable_manifest,
            {
                "schema": IMMUTABLE_OUTPUT_SCHEMA,
                "revision": DERIVATION_REVISION,
                "file_count": len(output_rows),
                "total_bytes": sum(int(row["bytes"]) for row in output_rows),
                "files": output_rows,
            },
        )
        verify_source_snapshot(source_snapshot)
        if target.exists():
            raise SemanticLtxContractError(
                f"Output directory collision during immutable publish: {target}"
            )
        staging.rename(target)
    except BaseException:
        if staging.exists() and staging.parent == target.parent and staging.name.startswith(
            f".{target.name}.staging-"
        ):
            shutil.rmtree(staging)
        raise

    semantic_path = target / SEMANTIC_FILENAME
    derivation_manifest = target / DERIVATION_MANIFEST_FILENAME
    immutable_manifest = target / IMMUTABLE_MANIFEST_FILENAME
    return SemanticReferenceResult(
        output_dir=target,
        semantic_path=semantic_path,
        derivation_manifest_path=derivation_manifest,
        immutable_manifest_path=immutable_manifest,
        semantic_sha256=sha256_file(semantic_path),
        immutable_manifest_sha256=sha256_file(immutable_manifest),
    )


def derive_semantic_reference_cli(
    bundle_dir: str | Path,
    profile_path: str | Path,
    output_dir: str | Path,
) -> SemanticReferenceResult:
    try:
        return derive_semantic_reference(bundle_dir, profile_path, output_dir)
    except (ContractError, DependencyUnavailableError, SemanticLtxContractError):
        raise
    except Exception as exc:
        raise SemanticLtxContractError(f"Semantic reference derivation failed: {exc}") from exc
