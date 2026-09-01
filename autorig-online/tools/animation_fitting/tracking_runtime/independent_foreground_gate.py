from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

import numpy as np

from ..errors import ContractError, DependencyUnavailableError
from ..rig import load_rig_bundle
from .core import (
    REFERENCE_GEOMETRY_ASPECT_STRICT,
    REFERENCE_GEOMETRY_CENTER_CROP,
    REFERENCE_GEOMETRY_MODES,
    _artifact_path,
    _reference_geometry_transform,
    load_video,
    select_anchor_seeds,
)
from .models import IndependentForegroundBackend, VideoFrames
from .runtime_lock import sha256_file


SCHEMA = "autorig.animation-fitting.independent-foreground-gate.v1"
MANIFEST_SCHEMA = "autorig.animation-fitting.independent-foreground-gate-bundle.v1"


@dataclass(frozen=True)
class IndependentForegroundGateConfig:
    motion_quantile: float = 0.85
    min_motion_component_fraction: float = 0.003
    prompt_margin_fraction: float = 0.04
    min_backend_score: float = 0.80
    min_mask_fraction: float = 0.02
    max_mask_fraction: float = 0.35
    min_silhouette_iou: float = 0.85
    min_canonical_recall: float = 0.90
    min_independent_precision: float = 0.90
    max_boundary_p95_px: float = 6.0
    max_centroid_shift_diagonal: float = 0.02
    min_seed_inside_exact_ratio: float = 0.80
    min_seed_inside_tolerant_ratio: float = 0.95
    seed_tolerance_px: int = 2
    canonical_mask_threshold_uint8: int = 128

    def validate(self) -> None:
        for name in (
            "motion_quantile",
            "min_motion_component_fraction",
            "prompt_margin_fraction",
            "min_backend_score",
            "min_mask_fraction",
            "max_mask_fraction",
            "min_silhouette_iou",
            "min_canonical_recall",
            "min_independent_precision",
            "max_centroid_shift_diagonal",
            "min_seed_inside_exact_ratio",
            "min_seed_inside_tolerant_ratio",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0 or value > 1.0:
                raise ContractError(f"{name} must be inside [0, 1]")
        if self.max_mask_fraction <= self.min_mask_fraction:
            raise ContractError("max_mask_fraction must exceed min_mask_fraction")
        if not math.isfinite(self.max_boundary_p95_px) or self.max_boundary_p95_px <= 0:
            raise ContractError("max_boundary_p95_px must be positive")
        if (
            isinstance(self.seed_tolerance_px, bool)
            or not isinstance(self.seed_tolerance_px, int)
            or self.seed_tolerance_px < 0
            or self.seed_tolerance_px > 16
        ):
            raise ContractError("seed_tolerance_px must be an integer inside [0, 16]")
        if (
            isinstance(self.canonical_mask_threshold_uint8, bool)
            or not isinstance(self.canonical_mask_threshold_uint8, int)
            or not 1 <= self.canonical_mask_threshold_uint8 <= 255
        ):
            raise ContractError("canonical_mask_threshold_uint8 must be inside [1, 255]")


@dataclass(frozen=True)
class MotionPrompt:
    box_xyxy: np.ndarray
    saliency: np.ndarray
    component_mask: np.ndarray
    provenance: dict[str, Any]


@dataclass(frozen=True)
class GateRunResult:
    gate_json: Path
    decision: str


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def derive_video_motion_prompt(
    video: VideoFrames,
    config: IndependentForegroundGateConfig,
) -> MotionPrompt:
    """Derive a SAM prompt from video pixels, without canonical inputs."""

    config.validate()
    try:
        import cv2
    except ImportError as exc:
        raise DependencyUnavailableError("OpenCV is required for the foreground gate") from exc
    frames = np.asarray(video.frames_bgr)
    if (
        frames.dtype != np.uint8
        or frames.ndim != 4
        or frames.shape[-1] != 3
        or frames.shape[0] < 8
    ):
        raise ContractError("Independent foreground motion prompt requires >=8 HxWx3 uint8 frames")
    height, width = frames.shape[1:3]
    gray = np.stack([cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) for frame in frames])
    magnitudes = []
    farneback = {
        "pyr_scale": 0.5,
        "levels": 3,
        "winsize": 15,
        "iterations": 3,
        "poly_n": 5,
        "poly_sigma": 1.2,
        "flags": 0,
    }
    for previous, current in zip(gray[:-1], gray[1:]):
        flow = cv2.calcOpticalFlowFarneback(
            previous,
            current,
            None,
            farneback["pyr_scale"],
            farneback["levels"],
            farneback["winsize"],
            farneback["iterations"],
            farneback["poly_n"],
            farneback["poly_sigma"],
            farneback["flags"],
        )
        magnitudes.append(np.linalg.norm(flow, axis=2))
    saliency = np.max(np.stack(magnitudes), axis=0).astype(np.float32, copy=False)
    border = max(2, int(round(0.025 * min(width, height))))
    if width <= 2 * border or height <= 2 * border:
        raise ContractError("Video is too small for an independent motion prompt")
    interior = saliency[border:-border, border:-border]
    threshold = float(np.quantile(interior, config.motion_quantile))
    if not math.isfinite(threshold) or threshold <= 1e-6:
        raise ContractError("Video has no independently measurable foreground motion")
    motion = (saliency >= threshold).astype(np.uint8)
    motion[:border, :] = 0
    motion[-border:, :] = 0
    motion[:, :border] = 0
    motion[:, -border:] = 0
    kernel_size = max(3, 2 * int(round(min(width, height) / 80.0)) + 1)
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_size, kernel_size))
    motion = cv2.morphologyEx(motion, cv2.MORPH_CLOSE, kernel, iterations=2)
    motion = cv2.dilate(motion, kernel, iterations=1)
    component_count, labels, stats, centroids = cv2.connectedComponentsWithStats(
        motion, connectivity=8
    )
    minimum_area = max(
        32,
        int(round(config.min_motion_component_fraction * width * height)),
    )
    selected_components = []
    for component in range(1, component_count):
        area = int(stats[component, cv2.CC_STAT_AREA])
        centroid_x, centroid_y = centroids[component]
        if (
            area >= minimum_area
            and 0.08 * width <= centroid_x <= 0.92 * width
            and 0.05 * height <= centroid_y <= 0.95 * height
        ):
            selected_components.append(component)
    if not selected_components:
        raise ContractError("No central video-motion component passed the independent prompt gate")
    selected_mask = np.isin(labels, selected_components)
    rows, columns = np.nonzero(selected_mask)
    x0, x1 = int(columns.min()), int(columns.max())
    y0, y1 = int(rows.min()), int(rows.max())
    margin_x = max(4, int(round(config.prompt_margin_fraction * width)))
    margin_y = max(4, int(round(config.prompt_margin_fraction * height)))
    box = np.asarray(
        (
            max(0, x0 - margin_x),
            max(0, y0 - margin_y),
            min(width - 1, x1 + margin_x),
            min(height - 1, y1 + margin_y),
        ),
        dtype=np.float32,
    )
    if not (box[0] < box[2] and box[1] < box[3]):
        raise ContractError("Independent video-motion prompt box is degenerate")
    return MotionPrompt(
        box_xyxy=box,
        saliency=saliency,
        component_mask=selected_mask,
        provenance={
            "schema": "autorig.video-pixel-motion-prompt.v1",
            "input": "decoded_video_pixels_only",
            "canonical_geometry_used": False,
            "canonical_mask_used": False,
            "semantic_seeds_used": False,
            "tracker_output_used": False,
            "frame_count": int(frames.shape[0]),
            "resolution": [width, height],
            "optical_flow": {
                "implementation": "opencv_farneback_adjacent_frames",
                "parameters": farneback,
                "temporal_reduction": "per_pixel_maximum_magnitude",
            },
            "motion_quantile": config.motion_quantile,
            "motion_threshold_px": threshold,
            "border_exclusion_px": border,
            "morphology_kernel_px": kernel_size,
            "minimum_component_area_px": minimum_area,
            "selected_component_ids": selected_components,
            "raw_union_bbox_xyxy": [x0, y0, x1, y1],
            "prompt_margin_xy_px": [margin_x, margin_y],
            "prompt_box_xyxy": [float(value) for value in box],
        },
    )


def _bbox(mask: np.ndarray) -> list[int]:
    rows, columns = np.nonzero(mask)
    return [int(columns.min()), int(rows.min()), int(columns.max()), int(rows.max())]


def evaluate_independent_foreground(
    *,
    independent_mask: np.ndarray,
    backend_score: float,
    canonical_mask: np.ndarray,
    semantic_points_xy: np.ndarray,
    config: IndependentForegroundGateConfig,
) -> tuple[dict[str, Any], list[str]]:
    """Compare a previously frozen image-only mask to canonical geometry."""

    config.validate()
    try:
        from scipy.ndimage import binary_dilation, binary_erosion, distance_transform_edt
    except ImportError as exc:
        raise DependencyUnavailableError("SciPy is required for the foreground gate") from exc
    independent = np.asarray(independent_mask, dtype=bool)
    canonical = np.asarray(canonical_mask, dtype=bool)
    points = np.asarray(semantic_points_xy, dtype=np.float64)
    if independent.ndim != 2 or canonical.shape != independent.shape:
        raise ContractError("Independent and canonical masks must have the same 2D shape")
    if not np.any(independent) or not np.any(canonical):
        raise ContractError("Independent and canonical masks must both be non-empty")
    if points.ndim != 2 or points.shape[1] != 2 or not np.all(np.isfinite(points)):
        raise ContractError("Semantic points must be finite Nx2 coordinates")
    if not math.isfinite(float(backend_score)):
        raise ContractError("Independent foreground backend score must be finite")
    height, width = independent.shape
    intersection = int(np.sum(independent & canonical))
    union = int(np.sum(independent | canonical))
    independent_area = int(np.sum(independent))
    canonical_area = int(np.sum(canonical))
    iou = intersection / max(1, union)
    canonical_recall = intersection / canonical_area
    independent_precision = intersection / independent_area
    independent_boundary = independent & ~binary_erosion(independent)
    canonical_boundary = canonical & ~binary_erosion(canonical)
    independent_to_canonical = distance_transform_edt(~canonical_boundary)[
        independent_boundary
    ]
    canonical_to_independent = distance_transform_edt(~independent_boundary)[
        canonical_boundary
    ]
    boundary_p95 = {
        "independent_to_canonical": float(np.percentile(independent_to_canonical, 95)),
        "canonical_to_independent": float(np.percentile(canonical_to_independent, 95)),
    }
    independent_rows, independent_columns = np.nonzero(independent)
    canonical_rows, canonical_columns = np.nonzero(canonical)
    centroid_shift_px = math.hypot(
        float(np.mean(independent_columns) - np.mean(canonical_columns)),
        float(np.mean(independent_rows) - np.mean(canonical_rows)),
    )
    diagonal = math.hypot(width, height)
    rounded = np.rint(points).astype(np.int64)
    inside_frame = (
        (rounded[:, 0] >= 0)
        & (rounded[:, 0] < width)
        & (rounded[:, 1] >= 0)
        & (rounded[:, 1] < height)
    )
    exact = np.zeros(len(points), dtype=bool)
    exact[inside_frame] = independent[
        rounded[inside_frame, 1], rounded[inside_frame, 0]
    ]
    tolerant_mask = (
        binary_dilation(independent, iterations=config.seed_tolerance_px)
        if config.seed_tolerance_px > 0
        else independent
    )
    tolerant = np.zeros(len(points), dtype=bool)
    tolerant[inside_frame] = tolerant_mask[
        rounded[inside_frame, 1], rounded[inside_frame, 0]
    ]
    border_contact_pixels = int(
        np.sum(independent[0])
        + np.sum(independent[-1])
        + np.sum(independent[1:-1, 0])
        + np.sum(independent[1:-1, -1])
    )
    metrics = {
        "backend_score": float(backend_score),
        "resolution": [width, height],
        "independent_mask": {
            "area_pixels": independent_area,
            "area_fraction": independent_area / (width * height),
            "bbox_xyxy": _bbox(independent),
            "border_contact_pixels": border_contact_pixels,
        },
        "canonical_high_confidence_mask": {
            "area_pixels": canonical_area,
            "area_fraction": canonical_area / (width * height),
            "bbox_xyxy": _bbox(canonical),
        },
        "silhouette": {
            "intersection_pixels": intersection,
            "union_pixels": union,
            "iou": iou,
            "canonical_recall": canonical_recall,
            "independent_precision": independent_precision,
            "boundary_p95_px": boundary_p95,
            "centroid_shift_px": centroid_shift_px,
            "centroid_shift_diagonal": centroid_shift_px / diagonal,
        },
        "semantic_seeds": {
            "count": int(len(points)),
            "inside_exact_count": int(np.sum(exact)),
            "inside_exact_ratio": float(np.mean(exact)) if len(exact) else 0.0,
            "tolerance_px": config.seed_tolerance_px,
            "inside_tolerant_count": int(np.sum(tolerant)),
            "inside_tolerant_ratio": float(np.mean(tolerant)) if len(tolerant) else 0.0,
        },
    }
    failures: list[str] = []
    if backend_score < config.min_backend_score:
        failures.append("independent_backend_score")
    fraction = independent_area / (width * height)
    if fraction < config.min_mask_fraction or fraction > config.max_mask_fraction:
        failures.append("independent_mask_area")
    if border_contact_pixels:
        failures.append("independent_mask_border_contact")
    if iou < config.min_silhouette_iou:
        failures.append("independent_silhouette_iou")
    if canonical_recall < config.min_canonical_recall:
        failures.append("independent_canonical_recall")
    if independent_precision < config.min_independent_precision:
        failures.append("independent_precision")
    if max(boundary_p95.values()) > config.max_boundary_p95_px:
        failures.append("independent_boundary_distance")
    if centroid_shift_px / diagonal > config.max_centroid_shift_diagonal:
        failures.append("independent_centroid_shift")
    if metrics["semantic_seeds"]["inside_exact_ratio"] < config.min_seed_inside_exact_ratio:
        failures.append("independent_seed_exact_membership")
    if (
        metrics["semantic_seeds"]["inside_tolerant_ratio"]
        < config.min_seed_inside_tolerant_ratio
    ):
        failures.append("independent_seed_tolerant_membership")
    return metrics, failures


def _high_confidence_canonical_mask(
    bundle: str | Path,
    *,
    geometry: dict[str, Any],
    width: int,
    height: int,
    threshold: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    try:
        import cv2
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "OpenCV and Pillow are required for canonical mask comparison"
        ) from exc
    rig = load_rig_bundle(bundle)
    path = _artifact_path(rig, "mask")
    try:
        with Image.open(path) as image:
            source = np.asarray(image.convert("L"), dtype=np.uint8)
    except Exception as exc:
        raise ContractError(f"Cannot load canonical mask {path}: {exc}") from exc
    crop = geometry["crop_pixels"]
    cropped = source[
        crop["y"] : crop["y"] + crop["height"],
        crop["x"] : crop["x"] + crop["width"],
    ]
    transformed = cv2.resize(cropped, (width, height), interpolation=cv2.INTER_NEAREST)
    mask = transformed >= threshold
    if not np.any(mask):
        raise ContractError("High-confidence canonical mask is empty")
    return mask, {
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "threshold_contract": f"uint8_greater_than_or_equal_{threshold}",
        "geometry_transform": geometry,
    }


def _file_record(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def run_independent_foreground_gate(
    *,
    video: str | Path,
    bundle: str | Path,
    output_dir: str | Path,
    backend: IndependentForegroundBackend,
    reference_geometry_mode: str = REFERENCE_GEOMETRY_ASPECT_STRICT,
    config: IndependentForegroundGateConfig | None = None,
    ffprobe: str | None = None,
) -> GateRunResult:
    cfg = config or IndependentForegroundGateConfig()
    cfg.validate()
    if reference_geometry_mode not in REFERENCE_GEOMETRY_MODES:
        raise ContractError(f"Unsupported reference geometry mode: {reference_geometry_mode}")
    destination = Path(output_dir).resolve()
    if destination.exists():
        raise ContractError(f"Independent foreground output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_video = load_video(video, ffprobe=ffprobe)
    seeds = select_anchor_seeds(bundle)
    _, _, transformed_points, geometry = _reference_geometry_transform(
        seeds,
        width=source_video.width,
        height=source_video.height,
        mode=reference_geometry_mode,
    )
    motion_prompt = derive_video_motion_prompt(source_video, cfg)
    image_rgb = source_video.frames_bgr[0, :, :, ::-1].copy()
    independent_result = backend.segment(image_rgb, motion_prompt.box_xyxy)
    independent_mask = np.asarray(independent_result.mask, dtype=bool)
    if independent_mask.shape != (source_video.height, source_video.width):
        raise ContractError("Independent foreground backend returned the wrong mask shape")
    canonical_mask, canonical_provenance = _high_confidence_canonical_mask(
        bundle,
        geometry=geometry,
        width=source_video.width,
        height=source_video.height,
        threshold=cfg.canonical_mask_threshold_uint8,
    )
    metrics, failures = evaluate_independent_foreground(
        independent_mask=independent_mask,
        backend_score=independent_result.score,
        canonical_mask=canonical_mask,
        semantic_points_xy=transformed_points,
        config=cfg,
    )
    decision = "PASS" if not failures else "FAIL"
    staging = Path(
        tempfile.mkdtemp(prefix=destination.name + ".tmp-", dir=destination.parent)
    )
    try:
        try:
            import cv2
            from PIL import Image
        except ImportError as exc:
            raise DependencyUnavailableError(
                "OpenCV and Pillow are required to publish foreground-gate evidence"
            ) from exc
        independent_path = staging / "independent_foreground_mask.png"
        canonical_path = staging / "canonical_high_confidence_mask.png"
        motion_path = staging / "video_motion_components.png"
        saliency_path = staging / "video_motion_saliency.png"
        overlay_path = staging / "independent_foreground_overlay.png"
        Image.fromarray(independent_mask.astype(np.uint8) * 255, mode="L").save(
            independent_path, optimize=True
        )
        Image.fromarray(canonical_mask.astype(np.uint8) * 255, mode="L").save(
            canonical_path, optimize=True
        )
        Image.fromarray(
            motion_prompt.component_mask.astype(np.uint8) * 255, mode="L"
        ).save(motion_path, optimize=True)
        saliency_scale = max(float(np.percentile(motion_prompt.saliency, 99.5)), 1e-6)
        Image.fromarray(
            np.clip(motion_prompt.saliency / saliency_scale * 255.0, 0, 255).astype(
                np.uint8
            ),
            mode="L",
        ).save(saliency_path, optimize=True)
        overlay = source_video.frames_bgr[0].copy()
        overlay[independent_mask] = (
            0.55 * overlay[independent_mask]
            + 0.45 * np.asarray((50, 220, 50), dtype=np.float64)
        ).astype(np.uint8)
        x0, y0, x1, y1 = np.rint(motion_prompt.box_xyxy).astype(int)
        cv2.rectangle(overlay, (x0, y0), (x1, y1), (0, 0, 255), 1)
        if not cv2.imwrite(str(overlay_path), overlay):
            raise ContractError(f"Cannot write independent foreground overlay {overlay_path}")
        gate_payload = {
            "schema": SCHEMA,
            "decision": decision,
            "fitting_allowed": decision == "PASS",
            "animation_quality_approved": False,
            "human_review_required": True,
            "failures": failures,
            "independence_contract": {
                "segmentation_input": "decoded_video_pixels_only",
                "prompt_derivation": "video_pixel_optical_flow_only",
                "mask_selection": "maximum_sam_predicted_iou_only",
                "canonical_geometry_used_before_mask_freeze": False,
                "canonical_mask_used_before_mask_freeze": False,
                "semantic_seeds_used_before_mask_freeze": False,
                "canonical_comparison_stage": "after_independent_mask_freeze",
            },
            "source": {
                "video": str(source_video.source),
                "sha256": source_video.source_sha256,
                "bytes": source_video.source.stat().st_size,
                "frame_count": source_video.frame_count,
                "fps": source_video.fps,
                "resolution": [source_video.width, source_video.height],
            },
            "canonical": {
                "bundle": str(Path(bundle).resolve()),
                "bundle_sha256": seeds.bundle_sha256,
                "immutable_manifest_sha256": seeds.immutable_manifest_sha256,
                "mask": canonical_provenance,
            },
            "motion_prompt": motion_prompt.provenance,
            "backend": independent_result.provenance,
            "metrics": metrics,
            "thresholds": asdict(cfg),
        }
        gate_path = staging / "gate.json"
        _write_json(gate_path, gate_payload)
        evidence_paths = [
            gate_path,
            independent_path,
            canonical_path,
            motion_path,
            saliency_path,
            overlay_path,
        ]
        manifest_path = staging / "manifest.json"
        _write_json(
            manifest_path,
            {
                "schema": MANIFEST_SCHEMA,
                "decision": decision,
                "source_video_sha256": source_video.source_sha256,
                "bundle_sha256": seeds.bundle_sha256,
                "files": [
                    _file_record(path, staging)
                    for path in sorted(evidence_paths, key=lambda item: item.name)
                ],
            },
        )
        os.replace(staging, destination)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return GateRunResult(destination / "gate.json", decision)
