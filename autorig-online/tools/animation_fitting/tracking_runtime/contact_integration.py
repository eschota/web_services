from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import math
from typing import Any, Mapping

import numpy as np

from ..contact_profile import AnimalContactProfile
from ..errors import ContractError
from ..rig import RigBundle
from .contact_solver import (
    ContactInferenceConfig,
    DepthCalibrationConfig,
    DepthCalibrationResult,
    HoofEvidence,
    calibrate_relative_depth_to_camera_z,
    infer_circular_walk_contacts,
    solve_virtual_ground_path,
)


@dataclass(frozen=True)
class ContactRuntimeResult:
    contacts: tuple[dict[str, Any], ...]
    camera_z: np.ndarray
    hoof_positions: Mapping[str, np.ndarray]
    virtual_ground_increments: np.ndarray
    virtual_ground_root_path: np.ndarray
    provenance: dict[str, Any]
    qa: dict[str, Any]


def characteristic_height(rig: RigBundle) -> float:
    normal = np.asarray(rig.ground_normal, dtype=np.float64)
    normal /= float(np.linalg.norm(normal))
    heights = [
        float(np.dot(normal, anchor.rest_world) - rig.ground_height)
        for anchor in rig.anchors.values()
    ]
    if not heights:
        raise ContractError(
            "Cannot derive characteristic height without surface anchors"
        )
    height = float(max(heights))
    if not math.isfinite(height) or height <= 1e-6:
        raise ContractError(
            f"Immutable bundle characteristic height is invalid: {height}"
        )
    return height


def calibrate_bundle_camera_z(
    rig: RigBundle,
    relative_depth: np.ndarray,
    canonical_mask: np.ndarray,
    *,
    config: DepthCalibrationConfig | None = None,
) -> DepthCalibrationResult:
    reference_path = rig.artifacts.get("camera_z")
    if reference_path is None:
        raise ContractError(
            "Metric camera-Z calibration requires an immutable camera_z reference artifact"
        )
    try:
        reference = np.load(reference_path, allow_pickle=False)
    except Exception as exc:
        raise ContractError(
            f"Cannot load immutable reference camera-Z {reference_path}: {exc}"
        ) from exc
    relative = np.asarray(relative_depth)
    if relative.ndim not in (2, 3):
        raise ContractError(
            "relative_depth must have shape [height, width] or [frames, height, width]"
        )
    if reference.ndim != 2:
        raise ContractError("Immutable reference camera-Z must be two-dimensional")
    target_shape = relative.shape[-2:]
    if any(dimension < 1 for dimension in target_shape):
        raise ContractError("relative_depth image dimensions must be positive")
    if reference.shape != target_shape:
        source_height, source_width = reference.shape
        target_height, target_width = target_shape
        if abs((source_width / source_height) - (target_width / target_height)) > 0.01:
            raise ContractError(
                "Reference/video camera-Z aspect ratios differ: "
                f"{source_width}x{source_height} vs {target_width}x{target_height}"
            )
        try:
            import cv2
        except ImportError as exc:
            raise ContractError(
                "OpenCV is required to resize immutable camera-Z"
            ) from exc
        valid = np.isfinite(reference).astype(np.float32)
        numerator = cv2.resize(
            np.nan_to_num(reference, nan=0.0).astype(np.float32) * valid,
            (target_width, target_height),
            interpolation=cv2.INTER_LINEAR,
        )
        denominator = cv2.resize(
            valid,
            (target_width, target_height),
            interpolation=cv2.INTER_LINEAR,
        )
        reference = np.where(
            denominator >= 0.999,
            numerator / np.maximum(denominator, 1e-12),
            np.nan,
        ).astype(np.float32)
    return calibrate_relative_depth_to_camera_z(
        relative,
        reference,
        canonical_mask,
        characteristic_height=characteristic_height(rig),
        config=config,
    )


def _sample_camera_z(
    camera_z: np.ndarray,
    mask: np.ndarray,
    x: float,
    y: float,
    *,
    radius: int = 2,
) -> float | None:
    if not math.isfinite(x) or not math.isfinite(y):
        return None
    height, width = camera_z.shape
    cx, cy = int(round(x)), int(round(y))
    x0, x1 = max(0, cx - radius), min(width, cx + radius + 1)
    y0, y1 = max(0, cy - radius), min(height, cy + radius + 1)
    if x0 >= x1 or y0 >= y1:
        return None
    values = camera_z[y0:y1, x0:x1]
    valid = mask[y0:y1, x0:x1] & np.isfinite(values) & (values > 0.0)
    if not np.any(valid):
        return None
    return float(np.median(values[valid]))


def _unproject(
    rig: RigBundle,
    x: float,
    y: float,
    camera_z: float,
    *,
    image_width: int,
    image_height: int,
) -> np.ndarray:
    scale_x = image_width / rig.camera.width
    scale_y = image_height / rig.camera.height
    fx = rig.camera.fx * scale_x
    fy = rig.camera.fy * scale_y
    cx = rig.camera.cx * scale_x
    cy = rig.camera.cy * scale_y
    point_camera = np.asarray(
        (
            (x - cx) * camera_z / fx,
            (cy - y) * camera_z / fy,
            -camera_z,
            1.0,
        ),
        dtype=np.float64,
    )
    point_world = np.linalg.inv(rig.camera.world_to_camera) @ point_camera
    if abs(float(point_world[3])) <= 1e-12:
        raise ContractError("Camera unprojection produced invalid homogeneous W")
    result = point_world[:3] / point_world[3]
    if not np.all(np.isfinite(result)):
        raise ContractError("Camera unprojection produced non-finite world coordinates")
    return result


def _mask_bottom_gap(mask: np.ndarray, x: float, y: float, *, radius: int = 2) -> float:
    height, width = mask.shape
    center = int(round(x))
    columns = range(max(0, center - radius), min(width, center + radius + 1))
    bottoms = []
    for column in columns:
        rows = np.flatnonzero(mask[:, column])
        if rows.size:
            bottoms.append(int(rows[-1]))
    if not bottoms:
        return float("nan")
    return float(max(bottoms) - y)


def _circular_missing_runs(valid: np.ndarray) -> tuple[tuple[int, ...], ...]:
    missing = ~np.asarray(valid, dtype=bool)
    count = len(missing)
    if not np.any(missing):
        return ()
    if np.all(missing):
        return (tuple(range(count)),)
    start = next(index for index in range(count) if not missing[index])
    runs: list[tuple[int, ...]] = []
    current: list[int] = []
    for step in range(1, count + 1):
        index = (start + step) % count
        if missing[index]:
            current.append(index)
        elif current:
            runs.append(tuple(current))
            current = []
    return tuple(runs)


def _fill_short_circular_gaps(
    values: np.ndarray,
    valid: np.ndarray,
    *,
    maximum_gap: int,
    field: str,
) -> np.ndarray:
    result = np.asarray(values, dtype=np.float64).copy()
    good = (
        np.asarray(valid, dtype=bool) & np.all(np.isfinite(result), axis=1)
        if result.ndim == 2
        else np.asarray(valid, dtype=bool) & np.isfinite(result)
    )
    runs = _circular_missing_runs(good)
    if not runs:
        return result
    longest = max(len(run) for run in runs)
    if longest > maximum_gap:
        raise ContractError(
            f"{field} has an unsupported circular occlusion gap of {longest} frames"
        )
    count = len(result)
    for run in runs:
        previous = (run[0] - 1) % count
        following = (run[-1] + 1) % count
        if not good[previous] or not good[following]:
            raise ContractError(f"{field} cannot interpolate a circular gap")
        denominator = len(run) + 1
        for offset, frame in enumerate(run, start=1):
            alpha = offset / denominator
            result[frame] = (1.0 - alpha) * result[previous] + alpha * result[following]
    if not np.all(np.isfinite(result)):
        raise ContractError(f"{field} interpolation left non-finite values")
    return result


def _mask_bbox_heights(masks: np.ndarray) -> np.ndarray:
    heights = []
    for frame, mask in enumerate(masks):
        rows = np.flatnonzero(np.any(mask, axis=1))
        if rows.size == 0:
            raise ContractError(f"Frame {frame} has an empty silhouette mask")
        heights.append(float(rows[-1] - rows[0] + 1))
    return np.asarray(heights, dtype=np.float64)


def infer_contact_runtime(
    *,
    rig: RigBundle,
    profile: AnimalContactProfile,
    camera_z: np.ndarray,
    points_xy: np.ndarray,
    visible: np.ndarray,
    confidence: np.ndarray,
    masks: np.ndarray,
    anchor_ids: tuple[str, ...],
    fps: float,
    contact_config: ContactInferenceConfig | None = None,
) -> ContactRuntimeResult:
    depth = np.asarray(camera_z, dtype=np.float32)
    points = np.asarray(points_xy, dtype=np.float64)
    seen = np.asarray(visible, dtype=bool)
    scores = np.asarray(confidence, dtype=np.float64)
    silhouettes = np.asarray(masks, dtype=bool)
    if points.ndim != 3:
        raise ContractError("Contact points must have shape [frames, tracks, 2]")
    frame_count = points.shape[0]
    unique_count = profile.loop_unique_frames
    if frame_count != unique_count + 1:
        raise ContractError(
            f"Contact profile {profile.profile_id} requires {unique_count + 1} loop frames "
            f"including the duplicated endpoint; got {frame_count}"
        )
    expected_track_shape = (frame_count, len(anchor_ids))
    if (
        points.shape != (*expected_track_shape, 2)
        or seen.shape != expected_track_shape
        or scores.shape != expected_track_shape
    ):
        raise ContractError("Contact tracking arrays have inconsistent dimensions")
    if len(set(anchor_ids)) != len(anchor_ids):
        raise ContractError("Contact anchor_ids must be unique")
    if (
        depth.ndim != 3
        or depth.shape[0] != frame_count
        or depth.shape[1] < 1
        or depth.shape[2] < 1
    ):
        raise ContractError(
            "Calibrated camera-Z must have shape [frames, height, width]"
        )
    if silhouettes.shape != depth.shape:
        raise ContractError("Contact silhouette/camera-Z dimensions differ")
    track_index = {anchor_id: index for index, anchor_id in enumerate(anchor_ids)}
    missing_tracks = sorted(set(profile.priority_anchor_ids).difference(track_index))
    if missing_tracks:
        raise ContractError(
            f"Contact priority anchors were not tracked: {missing_tracks}"
        )

    normal = np.asarray(rig.ground_normal, dtype=np.float64)
    normal /= float(np.linalg.norm(normal))
    bbox_heights = _mask_bbox_heights(silhouettes[:unique_count])
    height_scale = characteristic_height(rig)
    evidence: dict[str, HoofEvidence] = {}
    hoof_positions: dict[str, np.ndarray] = {}
    for foot_id in profile.foot_order:
        foot = profile.feet[foot_id]
        heights = np.full(unique_count, np.nan, dtype=np.float64)
        gaps = np.full(unique_count, np.nan, dtype=np.float64)
        positions = np.full((unique_count, 3), np.nan, dtype=np.float64)
        visible_counts = np.zeros(unique_count, dtype=np.int64)
        frame_confidence = np.zeros(unique_count, dtype=np.float64)
        for frame in range(unique_count):
            world_rows: list[np.ndarray] = []
            gap_rows: list[float] = []
            confidence_rows: list[float] = []
            for anchor_id in foot.anchor_ids:
                index = track_index[anchor_id]
                if not seen[frame, index] or not math.isfinite(scores[frame, index]):
                    continue
                x, y = points[frame, index]
                camera_depth = _sample_camera_z(
                    depth[frame], silhouettes[frame], float(x), float(y)
                )
                if camera_depth is None:
                    continue
                world_rows.append(
                    _unproject(
                        rig,
                        float(x),
                        float(y),
                        camera_depth,
                        image_width=depth.shape[2],
                        image_height=depth.shape[1],
                    )
                )
                gap_rows.append(
                    _mask_bottom_gap(silhouettes[frame], float(x), float(y))
                )
                confidence_rows.append(float(np.clip(scores[frame, index], 0.0, 1.0)))
            visible_counts[frame] = len(world_rows)
            if world_rows:
                world = np.stack(world_rows)
                positions[frame] = np.median(world, axis=0)
                signed = world @ normal - rig.ground_height
                heights[frame] = float(np.median(signed))
                finite_gaps = np.asarray(gap_rows, dtype=np.float64)
                finite_gaps = finite_gaps[np.isfinite(finite_gaps)]
                if finite_gaps.size:
                    gaps[frame] = float(np.median(finite_gaps))
                frame_confidence[frame] = float(np.median(confidence_rows))
        reliable = visible_counts >= 2
        filled_heights = _fill_short_circular_gaps(
            heights,
            reliable,
            maximum_gap=3,
            field=f"{foot_id}.height_world",
        )
        filled_positions = _fill_short_circular_gaps(
            positions,
            reliable,
            maximum_gap=3,
            field=f"{foot_id}.hoof_positions",
        )
        gap_valid = reliable & np.isfinite(gaps)
        if np.any(gap_valid):
            filled_gaps = _fill_short_circular_gaps(
                gaps,
                gap_valid,
                maximum_gap=3,
                field=f"{foot_id}.silhouette_bottom_gap_px",
            )
        else:
            filled_gaps = np.full(unique_count, np.nan, dtype=np.float64)
        speed = (np.roll(filled_heights, -1) - np.roll(filled_heights, 1)) * (
            float(fps) * 0.5
        )
        evidence[foot_id] = HoofEvidence(
            foot_id=foot_id,
            height_world=filled_heights,
            vertical_speed_world_per_second=speed,
            silhouette_bottom_gap_px=filled_gaps,
            mask_bbox_height_px=bbox_heights,
            visible_anchor_count=visible_counts,
            confidence=frame_confidence,
            total_anchor_count=4,
        )
        hoof_positions[foot_id] = filled_positions

    cfg = contact_config or ContactInferenceConfig()
    cfg = replace(cfg, unique_frame_count=unique_count)
    schedule = infer_circular_walk_contacts(
        evidence,
        foot_order=profile.foot_order,
        characteristic_height=height_scale,
        config=cfg,
    )
    virtual_ground = solve_virtual_ground_path(
        hoof_positions,
        schedule,
        ground_normal=normal,
        forward_axis=profile.forward_axis_world,
        characteristic_height=height_scale,
        fps=fps,
        require_root_motion=False,
    )
    contacts: list[dict[str, Any]] = []
    for foot_id in profile.foot_order:
        phase = schedule.phase_by_foot[foot_id]
        frames = np.flatnonzero(phase.contact).astype(int).tolist()
        if phase.contact[0]:
            frames.append(unique_count)
        positive_weights = phase.weights[phase.weights > 0.0]
        weight = float(np.median(positive_weights)) if positive_weights.size else 1.0
        for anchor_id in profile.feet[foot_id].anchor_ids:
            contacts.append(
                {
                    "anchor_id": anchor_id,
                    "frames": sorted(frames),
                    "ground_height": float(rig.ground_height),
                    "weight": max(weight, 1e-6),
                }
            )
    provenance = {
        "contact_profile": {
            "schema": "autorig-animal-contact-profile.v1",
            "profile_id": profile.profile_id,
            "revision": profile.revision,
            "sha256": profile.sha256,
            "action_id": profile.action_id,
            "root_motion_policy": profile.root_motion_policy,
        },
        "contact_schedule": schedule.provenance,
        "virtual_ground": virtual_ground.provenance,
        "canonical_output_motion": "in_place",
        "derived_root_motion_available": virtual_ground.root_motion_observable,
    }
    qa = {
        "contact_schedule": schedule.qa,
        "virtual_ground": virtual_ground.qa,
        "thresholds": {
            "contact": asdict(cfg),
        },
    }
    return ContactRuntimeResult(
        contacts=tuple(contacts),
        camera_z=depth,
        hoof_positions=hoof_positions,
        virtual_ground_increments=virtual_ground.increments,
        virtual_ground_root_path=virtual_ground.root_path,
        provenance=provenance,
        qa=qa,
    )
