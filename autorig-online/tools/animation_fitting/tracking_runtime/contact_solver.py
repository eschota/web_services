from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, Mapping, Sequence

import numpy as np

from ..errors import ContractError, DependencyUnavailableError


DEPTH_CALIBRATION_SCHEMA = "autorig-camera-z-calibration.v1"
CONTACT_SCHEDULE_SCHEMA = "autorig-horse-walk-contact-schedule.v1"
VIRTUAL_GROUND_SCHEMA = "autorig-virtual-ground-path.v1"


@dataclass(frozen=True)
class DepthCalibrationConfig:
    min_valid_pixels: int = 10_000
    min_foreground_coverage: float = 0.90
    min_abs_spearman: float = 0.75
    max_median_abs_error_height: float = 0.02
    max_p95_abs_error_height: float = 0.06
    reciprocal_epsilon: float = 1e-6
    max_irls_iterations: int = 24

    def validate(self) -> None:
        if (
            isinstance(self.min_valid_pixels, bool)
            or not isinstance(self.min_valid_pixels, int)
            or self.min_valid_pixels < 2
        ):
            raise ContractError("min_valid_pixels must be an integer of at least two")
        for name in (
            "min_foreground_coverage",
            "min_abs_spearman",
            "max_median_abs_error_height",
            "max_p95_abs_error_height",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ContractError(f"{name} must be finite and positive")
        if self.min_foreground_coverage > 1.0 or self.min_abs_spearman > 1.0:
            raise ContractError("coverage and Spearman thresholds must not exceed one")
        if self.max_p95_abs_error_height < self.max_median_abs_error_height:
            raise ContractError(
                "p95 error threshold must be at least the median threshold"
            )
        if not math.isfinite(self.reciprocal_epsilon) or self.reciprocal_epsilon <= 0.0:
            raise ContractError("reciprocal_epsilon must be finite and positive")
        if (
            isinstance(self.max_irls_iterations, bool)
            or not isinstance(self.max_irls_iterations, int)
            or self.max_irls_iterations < 1
        ):
            raise ContractError("max_irls_iterations must be a positive integer")


@dataclass(frozen=True)
class DepthCalibrationResult:
    mode: str
    scale: float
    offset: float
    camera_z: np.ndarray
    provenance: dict[str, Any]


@dataclass(frozen=True)
class _CalibrationCandidate:
    mode: str
    scale: float
    offset: float
    median_abs_error: float
    p95_abs_error: float
    abs_spearman: float
    valid_pixels: int
    foreground_coverage: float
    irls_iterations: int

    def to_dict(self, characteristic_height: float) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "scale": self.scale,
            "offset": self.offset,
            "valid_pixels": self.valid_pixels,
            "foreground_coverage": self.foreground_coverage,
            "abs_spearman": self.abs_spearman,
            "median_abs_error_world": self.median_abs_error,
            "p95_abs_error_world": self.p95_abs_error,
            "median_abs_error_height": self.median_abs_error / characteristic_height,
            "p95_abs_error_height": self.p95_abs_error / characteristic_height,
            "irls_iterations": self.irls_iterations,
        }


def _finite_positive(value: float, field: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0.0:
        raise ContractError(f"{field} must be finite and positive")
    return number


def _robust_affine_fit(
    feature: np.ndarray,
    target: np.ndarray,
    *,
    characteristic_height: float,
    max_iterations: int,
) -> tuple[float, float, int]:
    design = np.column_stack((feature, np.ones(feature.size, dtype=np.float64)))
    if float(np.ptp(feature)) <= max(1e-12, 1e-9 * float(np.max(np.abs(feature)))):
        raise ContractError("Relative-depth calibration feature is constant")
    weights = np.ones(feature.size, dtype=np.float64)
    coefficients = np.linalg.lstsq(design, target, rcond=None)[0]
    iterations = 0
    for iterations in range(1, max_iterations + 1):
        predicted = design @ coefficients
        residual = predicted - target
        centered = residual - float(np.median(residual))
        robust_sigma = 1.4826 * float(np.median(np.abs(centered)))
        delta = max(1.345 * robust_sigma, characteristic_height * 1e-6)
        absolute = np.abs(residual)
        new_weights = np.ones_like(absolute)
        outside = absolute > delta
        new_weights[outside] = delta / absolute[outside]
        weighted_design = design * np.sqrt(new_weights)[:, None]
        weighted_target = target * np.sqrt(new_weights)
        updated = np.linalg.lstsq(weighted_design, weighted_target, rcond=None)[0]
        if float(np.linalg.norm(updated - coefficients)) <= 1e-12 * (
            1.0 + float(np.linalg.norm(coefficients))
        ):
            coefficients = updated
            weights = new_weights
            break
        coefficients = updated
        weights = new_weights
    if not np.all(np.isfinite(coefficients)) or abs(float(coefficients[0])) <= 1e-12:
        raise ContractError("Relative-depth affine calibration is degenerate")
    if not np.all(np.isfinite(weights)):
        raise ContractError("Relative-depth IRLS produced non-finite weights")
    return float(coefficients[0]), float(coefficients[1]), iterations


def _abs_spearman(left: np.ndarray, right: np.ndarray) -> float:
    try:
        from scipy.stats import spearmanr
    except ImportError as exc:
        raise DependencyUnavailableError(
            "SciPy is required for camera-Z calibration rank QA"
        ) from exc
    correlation = float(spearmanr(left, right).statistic)
    return abs(correlation) if math.isfinite(correlation) else 0.0


def _calibration_feature(values: np.ndarray, mode: str, epsilon: float) -> np.ndarray:
    if mode == "affine":
        return np.asarray(values, dtype=np.float64)
    if mode == "reciprocal_affine":
        result = np.full(values.shape, np.nan, dtype=np.float64)
        valid = np.isfinite(values) & (np.abs(values) > epsilon)
        result[valid] = 1.0 / values[valid]
        return result
    raise ContractError(f"Unsupported relative-depth calibration mode: {mode}")


def _fit_calibration_candidate(
    relative_first: np.ndarray,
    reference_camera_z: np.ndarray,
    foreground: np.ndarray,
    *,
    mode: str,
    characteristic_height: float,
    config: DepthCalibrationConfig,
) -> _CalibrationCandidate:
    feature_image = _calibration_feature(
        relative_first, mode, config.reciprocal_epsilon
    )
    valid = (
        foreground
        & np.isfinite(feature_image)
        & np.isfinite(reference_camera_z)
        & (reference_camera_z > 0.0)
    )
    valid_pixels = int(np.sum(valid))
    foreground_pixels = int(np.sum(foreground))
    coverage = valid_pixels / max(1, foreground_pixels)
    if valid_pixels < config.min_valid_pixels:
        raise ContractError(
            f"{mode} camera-Z calibration has {valid_pixels} valid pixels; "
            f"requires {config.min_valid_pixels}"
        )
    feature = feature_image[valid].astype(np.float64, copy=False)
    target = reference_camera_z[valid].astype(np.float64, copy=False)
    scale, offset, iterations = _robust_affine_fit(
        feature,
        target,
        characteristic_height=characteristic_height,
        max_iterations=config.max_irls_iterations,
    )
    predicted = feature * scale + offset
    absolute_error = np.abs(predicted - target)
    return _CalibrationCandidate(
        mode=mode,
        scale=scale,
        offset=offset,
        median_abs_error=float(np.median(absolute_error)),
        p95_abs_error=float(np.percentile(absolute_error, 95)),
        abs_spearman=_abs_spearman(feature, target),
        valid_pixels=valid_pixels,
        foreground_coverage=coverage,
        irls_iterations=iterations,
    )


def calibrate_relative_depth_to_camera_z(
    relative_depth: np.ndarray,
    reference_camera_z: np.ndarray,
    canonical_mask: np.ndarray,
    *,
    characteristic_height: float,
    config: DepthCalibrationConfig | None = None,
) -> DepthCalibrationResult:
    """Select and apply a fail-closed affine or reciprocal camera-Z calibration.

    The first relative-depth frame is aligned to the exact actionless camera-Z
    artifact. The selected transform is then applied unchanged to every frame.
    """

    cfg = config or DepthCalibrationConfig()
    cfg.validate()
    height_scale = _finite_positive(characteristic_height, "characteristic_height")
    relative = np.asarray(relative_depth)
    reference = np.asarray(reference_camera_z, dtype=np.float64)
    foreground = np.asarray(canonical_mask)
    if relative.ndim == 2:
        relative = relative[None, ...]
    if relative.ndim != 3 or relative.shape[0] < 1:
        raise ContractError("relative_depth must have shape [frames, height, width]")
    if reference.ndim != 2 or foreground.ndim != 2:
        raise ContractError("reference_camera_z and canonical_mask must be 2D")
    if relative.shape[1:] != reference.shape or reference.shape != foreground.shape:
        raise ContractError(
            "Relative depth, reference camera-Z, and mask dimensions differ"
        )
    if foreground.dtype != np.bool_:
        foreground = foreground.astype(bool)
    if not np.any(foreground):
        raise ContractError("Canonical calibration mask is empty")

    candidates: list[_CalibrationCandidate] = []
    failures: dict[str, str] = {}
    for mode in ("affine", "reciprocal_affine"):
        try:
            candidates.append(
                _fit_calibration_candidate(
                    np.asarray(relative[0], dtype=np.float64),
                    reference,
                    foreground,
                    mode=mode,
                    characteristic_height=height_scale,
                    config=cfg,
                )
            )
        except ContractError as exc:
            failures[mode] = str(exc)
    if not candidates:
        raise ContractError(f"No camera-Z calibration model is valid: {failures}")
    candidates.sort(
        key=lambda item: (
            item.median_abs_error,
            item.p95_abs_error,
            item.mode,
        )
    )
    selected = candidates[0]
    selected_metrics = selected.to_dict(height_scale)
    rejected_gates: list[str] = []
    if selected.valid_pixels < cfg.min_valid_pixels:
        rejected_gates.append("valid_pixels")
    if selected.foreground_coverage < cfg.min_foreground_coverage:
        rejected_gates.append("foreground_coverage")
    if selected.abs_spearman < cfg.min_abs_spearman:
        rejected_gates.append("abs_spearman")
    if selected.median_abs_error / height_scale > cfg.max_median_abs_error_height:
        rejected_gates.append("median_abs_error")
    if selected.p95_abs_error / height_scale > cfg.max_p95_abs_error_height:
        rejected_gates.append("p95_abs_error")
    if rejected_gates:
        raise ContractError(
            "Camera-Z calibration QA rejected the selected model: "
            + ", ".join(rejected_gates)
            + f"; metrics={selected_metrics}"
        )

    feature = _calibration_feature(
        np.asarray(relative, dtype=np.float64),
        selected.mode,
        cfg.reciprocal_epsilon,
    )
    camera_z = feature * selected.scale + selected.offset
    valid_output = np.isfinite(camera_z) & (camera_z > 0.0)
    valid_pixels_by_frame = np.sum(valid_output, axis=(1, 2), dtype=np.int64)
    sparse_frames = np.flatnonzero(valid_pixels_by_frame < cfg.min_valid_pixels)
    if sparse_frames.size:
        details = {
            int(frame): int(valid_pixels_by_frame[frame]) for frame in sparse_frames
        }
        raise ContractError(
            "Calibrated camera-Z frames lost required valid-pixel coverage: "
            f"{details}; requires {cfg.min_valid_pixels} per frame"
        )
    camera_z = np.where(valid_output, camera_z, np.nan).astype(np.float32)
    first_valid = foreground & np.isfinite(camera_z[0])
    output_coverage = float(np.sum(first_valid) / np.sum(foreground))
    if output_coverage < cfg.min_foreground_coverage:
        raise ContractError(
            "Calibrated camera-Z output lost foreground coverage: "
            f"{output_coverage:.6f} < {cfg.min_foreground_coverage:.6f}"
        )
    provenance = {
        "schema": DEPTH_CALIBRATION_SCHEMA,
        "selected": selected_metrics,
        "candidates": [item.to_dict(height_scale) for item in candidates],
        "candidate_failures": failures,
        "thresholds": asdict(cfg),
        "frame_count": int(relative.shape[0]),
        "shape": [int(reference.shape[0]), int(reference.shape[1])],
        "characteristic_height": height_scale,
        "first_frame_output_coverage": output_coverage,
        "valid_pixels_by_frame": [int(value) for value in valid_pixels_by_frame],
    }
    return DepthCalibrationResult(
        mode=selected.mode,
        scale=selected.scale,
        offset=selected.offset,
        camera_z=camera_z,
        provenance=provenance,
    )


@dataclass(frozen=True)
class HoofEvidence:
    foot_id: str
    height_world: np.ndarray
    vertical_speed_world_per_second: np.ndarray
    silhouette_bottom_gap_px: np.ndarray
    mask_bbox_height_px: np.ndarray
    visible_anchor_count: np.ndarray
    confidence: np.ndarray
    total_anchor_count: int = 4


@dataclass(frozen=True)
class ContactInferenceConfig:
    unique_frame_count: int = 48
    min_visible_anchors: int = 2
    min_confidence: float = 0.50
    max_occlusion_gap_frames: int = 3
    max_carried_fraction: float = 0.10
    contact_height_height: float = 0.02
    swing_height_height: float = 0.04
    contact_vertical_speed_height_per_second: float = 0.15
    swing_vertical_speed_height_per_second: float = 0.30
    contact_bottom_gap_bbox: float = 0.025
    swing_bottom_gap_bbox: float = 0.05
    min_duty_factor: float = 0.50
    max_duty_factor: float = 0.80
    min_touchdown_phase: float = 0.15
    max_touchdown_phase: float = 0.35
    max_four_support_frames: int = 2
    max_emission_disagreement_fraction: float = 0.10
    max_candidates_per_foot: int = 12
    perturbation_runs: int = 16
    min_stable_runs: int = 14
    max_boundary_mad_frames: float = 1.0
    min_contact_jaccard: float = 0.90

    def validate(self) -> None:
        integer_names = (
            "unique_frame_count",
            "min_visible_anchors",
            "max_occlusion_gap_frames",
            "max_four_support_frames",
            "max_candidates_per_foot",
            "perturbation_runs",
            "min_stable_runs",
        )
        for name in integer_names:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractError(f"{name} must be a non-negative integer")
        if self.unique_frame_count < 4 or self.min_visible_anchors < 1:
            raise ContractError("Contact inference frame/visibility bounds are invalid")
        if self.max_candidates_per_foot < 1 or self.perturbation_runs < 1:
            raise ContractError(
                "Contact candidate and perturbation counts must be positive"
            )
        if self.min_stable_runs > self.perturbation_runs:
            raise ContractError("min_stable_runs cannot exceed perturbation_runs")
        for name in (
            "min_confidence",
            "max_carried_fraction",
            "contact_height_height",
            "swing_height_height",
            "contact_vertical_speed_height_per_second",
            "swing_vertical_speed_height_per_second",
            "contact_bottom_gap_bbox",
            "swing_bottom_gap_bbox",
            "min_duty_factor",
            "max_duty_factor",
            "min_touchdown_phase",
            "max_touchdown_phase",
            "max_emission_disagreement_fraction",
            "max_boundary_mad_frames",
            "min_contact_jaccard",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ContractError(f"{name} must be finite and non-negative")
        if self.min_confidence > 1.0 or self.max_carried_fraction > 1.0:
            raise ContractError(
                "Confidence and carried-fraction thresholds must not exceed one"
            )
        if (
            self.min_contact_jaccard > 1.0
            or self.max_emission_disagreement_fraction > 1.0
        ):
            raise ContractError("Contact ratios must not exceed one")
        if not 0.0 < self.min_duty_factor <= self.max_duty_factor < 1.0:
            raise ContractError("Duty-factor bounds must lie inside (0, 1)")
        if not 0.0 < self.min_touchdown_phase <= self.max_touchdown_phase < 1.0:
            raise ContractError("Touchdown phase bounds must lie inside (0, 1)")
        if self.contact_height_height >= self.swing_height_height:
            raise ContractError(
                "Contact height threshold must be below swing height threshold"
            )
        if (
            self.contact_vertical_speed_height_per_second
            >= self.swing_vertical_speed_height_per_second
        ):
            raise ContractError(
                "Contact speed threshold must be below swing speed threshold"
            )
        if self.contact_bottom_gap_bbox >= self.swing_bottom_gap_bbox:
            raise ContractError(
                "Contact bottom-gap threshold must be below swing threshold"
            )


@dataclass(frozen=True)
class ContactPhase:
    foot_id: str
    touchdown_frame: int
    liftoff_frame: int
    contact: np.ndarray
    observed: np.ndarray
    weights: np.ndarray


@dataclass(frozen=True)
class ContactSchedule:
    foot_order: tuple[str, ...]
    phases: tuple[ContactPhase, ...]
    unique_frame_count: int
    score: float
    qa: dict[str, Any]
    provenance: dict[str, Any]

    @property
    def phase_by_foot(self) -> dict[str, ContactPhase]:
        return {phase.foot_id: phase for phase in self.phases}


@dataclass(frozen=True)
class _EvidenceState:
    reliable: np.ndarray
    strong_contact: np.ndarray
    strong_swing: np.ndarray
    confidence: np.ndarray
    missing_runs: tuple[tuple[int, ...], ...]


@dataclass(frozen=True)
class _ContactCandidate:
    start: int
    length: int
    contact: np.ndarray
    score: float
    disagreement_fraction: float

    @property
    def end(self) -> int:
        return int((self.start + self.length) % len(self.contact))


def _circular_true_runs(mask: np.ndarray) -> tuple[tuple[int, ...], ...]:
    values = np.asarray(mask, dtype=bool)
    count = len(values)
    if count == 0 or not np.any(values):
        return ()
    if np.all(values):
        return (tuple(range(count)),)
    start = next(index for index in range(count) if not values[index])
    runs: list[tuple[int, ...]] = []
    current: list[int] = []
    for step in range(1, count + 1):
        index = (start + step) % count
        if values[index]:
            current.append(index)
        elif current:
            runs.append(tuple(current))
            current = []
    if current:
        runs.append(tuple(current))
    return tuple(runs)


def _validate_hoof_evidence(
    evidence: HoofEvidence,
    config: ContactInferenceConfig,
) -> None:
    if not isinstance(evidence.foot_id, str) or not evidence.foot_id:
        raise ContractError("Hoof evidence requires a non-empty foot_id")
    if evidence.total_anchor_count != 4:
        raise ContractError(
            f"{evidence.foot_id} must aggregate exactly four contact anchors"
        )
    arrays = {
        "height_world": np.asarray(evidence.height_world),
        "vertical_speed_world_per_second": np.asarray(
            evidence.vertical_speed_world_per_second
        ),
        "silhouette_bottom_gap_px": np.asarray(evidence.silhouette_bottom_gap_px),
        "mask_bbox_height_px": np.asarray(evidence.mask_bbox_height_px),
        "visible_anchor_count": np.asarray(evidence.visible_anchor_count),
        "confidence": np.asarray(evidence.confidence),
    }
    expected = (config.unique_frame_count,)
    for name, array in arrays.items():
        if array.shape != expected:
            raise ContractError(
                f"{evidence.foot_id}.{name} must have shape {expected}, got {array.shape}"
            )
    bbox = arrays["mask_bbox_height_px"].astype(np.float64, copy=False)
    confidence = arrays["confidence"].astype(np.float64, copy=False)
    visible = arrays["visible_anchor_count"]
    if not np.all(np.isfinite(bbox)) or np.any(bbox <= 0.0):
        raise ContractError(
            f"{evidence.foot_id} mask bbox heights must be finite and positive"
        )
    if not np.all(np.isfinite(confidence)) or np.any(
        (confidence < 0.0) | (confidence > 1.0)
    ):
        raise ContractError(f"{evidence.foot_id} confidence must stay inside [0, 1]")
    if not np.issubdtype(visible.dtype, np.integer):
        if not np.all(np.isfinite(visible)) or not np.all(visible == np.rint(visible)):
            raise ContractError(
                f"{evidence.foot_id} visible-anchor counts must be integers"
            )
    if np.any((visible < 0) | (visible > evidence.total_anchor_count)):
        raise ContractError(
            f"{evidence.foot_id} visible-anchor counts are outside [0, 4]"
        )


def _evidence_state(
    evidence: HoofEvidence,
    *,
    characteristic_height: float,
    config: ContactInferenceConfig,
    height_threshold_scale: float = 1.0,
    speed_threshold_scale: float = 1.0,
    gap_threshold_scale: float = 1.0,
    height_bias: float = 0.0,
) -> _EvidenceState:
    height = np.asarray(evidence.height_world, dtype=np.float64) + height_bias
    speed = np.asarray(evidence.vertical_speed_world_per_second, dtype=np.float64)
    gap = np.asarray(evidence.silhouette_bottom_gap_px, dtype=np.float64)
    bbox = np.asarray(evidence.mask_bbox_height_px, dtype=np.float64)
    visible = np.asarray(evidence.visible_anchor_count, dtype=np.int64)
    confidence = np.asarray(evidence.confidence, dtype=np.float64)
    reliable = (
        (visible >= config.min_visible_anchors)
        & (confidence >= config.min_confidence)
        & np.isfinite(height)
        & np.isfinite(speed)
    )
    contact_gap = config.contact_bottom_gap_bbox * gap_threshold_scale * bbox
    swing_gap = config.swing_bottom_gap_bbox * gap_threshold_scale * bbox
    gap_contact = ~np.isfinite(gap) | (gap <= contact_gap)
    gap_swing = np.isfinite(gap) & (gap >= swing_gap)
    strong_contact = (
        reliable
        & (
            height
            <= config.contact_height_height
            * height_threshold_scale
            * characteristic_height
        )
        & (
            np.abs(speed)
            <= config.contact_vertical_speed_height_per_second
            * speed_threshold_scale
            * characteristic_height
        )
        & gap_contact
    )
    strong_swing = reliable & (
        (
            height
            >= config.swing_height_height
            * height_threshold_scale
            * characteristic_height
        )
        | (
            np.abs(speed)
            >= config.swing_vertical_speed_height_per_second
            * speed_threshold_scale
            * characteristic_height
        )
        | gap_swing
    )
    missing_runs = _circular_true_runs(~reliable)
    return _EvidenceState(
        reliable=reliable,
        strong_contact=strong_contact,
        strong_swing=strong_swing,
        confidence=confidence,
        missing_runs=missing_runs,
    )


def _boundary_supported(state: _EvidenceState, start: int, end: int) -> bool:
    count = len(state.reliable)
    contact_window = ((start - 1) % count, start, (start + 1) % count)
    swing_window = ((end - 1) % count, end, (end + 1) % count)
    return bool(
        any(state.strong_contact[index] for index in contact_window)
        and any(state.strong_swing[index] for index in swing_window)
        and state.reliable[start]
        and state.reliable[end]
    )


def _candidate_respects_occlusion(candidate: np.ndarray, state: _EvidenceState) -> bool:
    count = len(candidate)
    for run in state.missing_runs:
        before = (run[0] - 1) % count
        after = (run[-1] + 1) % count
        if candidate[before] != candidate[after]:
            return False
        if any(candidate[index] != candidate[before] for index in run):
            return False
    return True


def _foot_candidates(
    state: _EvidenceState,
    config: ContactInferenceConfig,
) -> list[_ContactCandidate]:
    count = config.unique_frame_count
    min_length = int(math.ceil(config.min_duty_factor * count))
    max_length = int(math.floor(config.max_duty_factor * count))
    strong_count = int(np.sum(state.strong_contact) + np.sum(state.strong_swing))
    if strong_count < max(4, count // 4):
        raise ContractError(
            "Contact evidence has too few strong contact/swing observations"
        )
    candidates: list[_ContactCandidate] = []
    for start in range(count):
        for length in range(min_length, max_length + 1):
            contact = np.zeros(count, dtype=bool)
            contact[(start + np.arange(length)) % count] = True
            end = (start + length) % count
            if not _boundary_supported(state, start, end):
                continue
            if not _candidate_respects_occlusion(contact, state):
                continue
            mismatches = (contact & state.strong_swing) | (
                ~contact & state.strong_contact
            )
            disagreement = float(np.sum(mismatches) / strong_count)
            if disagreement > config.max_emission_disagreement_fraction:
                continue
            unknown = ~(state.strong_contact | state.strong_swing)
            score = 4.0 * float(np.sum(mismatches)) + float(np.sum(unknown))
            candidates.append(
                _ContactCandidate(
                    start=start,
                    length=length,
                    contact=contact,
                    score=score,
                    disagreement_fraction=disagreement,
                )
            )
    candidates.sort(key=lambda item: (item.score, item.start, item.length))
    return candidates[: config.max_candidates_per_foot]


def _phase_order_valid(
    candidates: Sequence[_ContactCandidate],
    config: ContactInferenceConfig,
) -> bool:
    count = config.unique_frame_count
    for index, candidate in enumerate(candidates):
        following = candidates[(index + 1) % len(candidates)]
        interval = ((following.start - candidate.start) % count) / count
        if (
            interval < config.min_touchdown_phase
            or interval > config.max_touchdown_phase
        ):
            return False
    return True


def _support_valid(
    candidates: Sequence[_ContactCandidate],
    config: ContactInferenceConfig,
) -> bool:
    support = np.sum(np.stack([item.contact for item in candidates]), axis=0)
    if int(np.min(support)) < 2:
        return False
    if np.any((support < 2) | (support > 4)):
        return False
    return int(np.sum(support == 4)) <= config.max_four_support_frames


def _solve_contact_combination(
    states: Mapping[str, _EvidenceState],
    foot_order: tuple[str, ...],
    config: ContactInferenceConfig,
) -> tuple[tuple[_ContactCandidate, ...], float]:
    candidates_by_foot = [_foot_candidates(states[foot], config) for foot in foot_order]
    missing = [foot for foot, rows in zip(foot_order, candidates_by_foot) if not rows]
    if missing:
        raise ContractError(f"No valid circular contact phase for feet: {missing}")
    best: tuple[_ContactCandidate, ...] | None = None
    best_key: tuple[Any, ...] | None = None
    count = config.unique_frame_count

    def ordered(first: _ContactCandidate, second: _ContactCandidate) -> bool:
        interval = ((second.start - first.start) % count) / count
        return config.min_touchdown_phase <= interval <= config.max_touchdown_phase

    # Four nested loops keep the exact Cartesian result while pruning partial
    # combinations as soon as their cyclic touchdown order is impossible.
    first_rows, second_rows, third_rows, fourth_rows = candidates_by_foot
    for first in first_rows:
        for second in second_rows:
            if not ordered(first, second):
                continue
            support_two = first.contact.astype(np.int8) + second.contact
            for third in third_rows:
                if not ordered(second, third):
                    continue
                support_three = support_two + third.contact
                for fourth in fourth_rows:
                    if not ordered(third, fourth) or not ordered(fourth, first):
                        continue
                    support = support_three + fourth.contact
                    if int(np.min(support)) < 2:
                        continue
                    if int(np.sum(support == 4)) > config.max_four_support_frames:
                        continue
                    combination = (first, second, third, fourth)
                    score = float(sum(item.score for item in combination))
                    key = (
                        score,
                        tuple(item.start for item in combination),
                        tuple(item.length for item in combination),
                    )
                    if best_key is None or key < best_key:
                        best = combination
                        best_key = key
    if best is None or best_key is None:
        raise ContractError(
            "No four-foot circular walk schedule satisfies order/support contracts"
        )
    return best, float(best_key[0])


def _circular_distance(left: int, right: int, count: int) -> int:
    direct = abs(int(left) - int(right))
    return min(direct, count - direct)


def _jaccard(left: np.ndarray, right: np.ndarray) -> float:
    union = int(np.sum(left | right))
    return 1.0 if union == 0 else float(np.sum(left & right) / union)


def infer_circular_walk_contacts(
    evidence_by_foot: Mapping[str, HoofEvidence],
    *,
    foot_order: Sequence[str],
    characteristic_height: float,
    config: ContactInferenceConfig | None = None,
) -> ContactSchedule:
    """Infer one deterministic four-beat walk contact cycle from hoof evidence."""

    cfg = config or ContactInferenceConfig()
    cfg.validate()
    height_scale = _finite_positive(characteristic_height, "characteristic_height")
    order = tuple(str(value) for value in foot_order)
    if len(order) != 4 or len(set(order)) != 4 or any(not value for value in order):
        raise ContractError("foot_order must contain four unique non-empty foot IDs")
    if set(evidence_by_foot) != set(order):
        raise ContractError(
            "Evidence feet must exactly match foot_order; "
            f"missing={sorted(set(order) - set(evidence_by_foot))}, "
            f"extra={sorted(set(evidence_by_foot) - set(order))}"
        )
    states: dict[str, _EvidenceState] = {}
    for foot in order:
        evidence = evidence_by_foot[foot]
        if evidence.foot_id != foot:
            raise ContractError(
                f"Evidence key {foot!r} disagrees with foot_id {evidence.foot_id!r}"
            )
        _validate_hoof_evidence(evidence, cfg)
        state = _evidence_state(
            evidence,
            characteristic_height=height_scale,
            config=cfg,
        )
        missing_count = int(np.sum(~state.reliable))
        if any(len(run) > cfg.max_occlusion_gap_frames for run in state.missing_runs):
            raise ContractError(
                f"{foot} has an occlusion gap longer than the allowed contract"
            )
        if missing_count / cfg.unique_frame_count > cfg.max_carried_fraction:
            raise ContractError(f"{foot} exceeds the carried-occlusion frame fraction")
        states[foot] = state

    selected, score = _solve_contact_combination(states, order, cfg)
    perturbation_results: list[tuple[_ContactCandidate, ...]] = []
    perturbation_failures: list[dict[str, Any]] = []
    for run in range(cfg.perturbation_runs):
        height_threshold_scale = 0.90 if run & 1 else 1.10
        speed_threshold_scale = 0.90 if run & 2 else 1.10
        gap_threshold_scale = 0.90 if run & 4 else 1.10
        height_bias = (-0.002 if run & 8 else 0.002) * height_scale
        perturbed_states = {
            foot: _evidence_state(
                evidence_by_foot[foot],
                characteristic_height=height_scale,
                config=cfg,
                height_threshold_scale=height_threshold_scale,
                speed_threshold_scale=speed_threshold_scale,
                gap_threshold_scale=gap_threshold_scale,
                height_bias=height_bias,
            )
            for foot in order
        }
        try:
            combination, _ = _solve_contact_combination(perturbed_states, order, cfg)
            perturbation_results.append(combination)
        except ContractError as exc:
            perturbation_failures.append({"run": run, "reason": str(exc)})
    if len(perturbation_results) < cfg.min_stable_runs:
        raise ContractError(
            "Contact perturbation QA is unstable: "
            f"{len(perturbation_results)}/{cfg.perturbation_runs} successful"
        )

    stability: dict[str, Any] = {}
    for foot_index, foot in enumerate(order):
        base = selected[foot_index]
        touchdown_errors = [
            _circular_distance(
                item[foot_index].start, base.start, cfg.unique_frame_count
            )
            for item in perturbation_results
        ]
        liftoff_errors = [
            _circular_distance(item[foot_index].end, base.end, cfg.unique_frame_count)
            for item in perturbation_results
        ]
        jaccards = [
            _jaccard(item[foot_index].contact, base.contact)
            for item in perturbation_results
        ]
        touchdown_mad = float(np.median(touchdown_errors))
        liftoff_mad = float(np.median(liftoff_errors))
        median_jaccard = float(np.median(jaccards))
        if (
            touchdown_mad > cfg.max_boundary_mad_frames
            or liftoff_mad > cfg.max_boundary_mad_frames
            or median_jaccard < cfg.min_contact_jaccard
        ):
            raise ContractError(
                f"Contact perturbation QA rejected {foot}: "
                f"touchdown_mad={touchdown_mad}, liftoff_mad={liftoff_mad}, "
                f"jaccard={median_jaccard}"
            )
        stability[foot] = {
            "touchdown_mad_frames": touchdown_mad,
            "liftoff_mad_frames": liftoff_mad,
            "median_contact_jaccard": median_jaccard,
        }

    phases: list[ContactPhase] = []
    support = np.sum(np.stack([item.contact for item in selected]), axis=0)
    for foot, candidate in zip(order, selected):
        state = states[foot]
        observed = state.reliable.copy()
        weights = np.where(candidate.contact & observed, state.confidence, 0.0)
        phases.append(
            ContactPhase(
                foot_id=foot,
                touchdown_frame=candidate.start,
                liftoff_frame=candidate.end,
                contact=candidate.contact.copy(),
                observed=observed,
                weights=weights.astype(np.float64, copy=False),
            )
        )
    qa = {
        "decision": "accepted_contact_schedule",
        "support": {
            "minimum": int(np.min(support)),
            "median": float(np.median(support)),
            "maximum": int(np.max(support)),
            "four_support_frames": int(np.sum(support == 4)),
        },
        "perturbations": {
            "successful_runs": len(perturbation_results),
            "total_runs": cfg.perturbation_runs,
            "failures": perturbation_failures,
            "per_foot": stability,
        },
        "per_foot": {
            foot: {
                "touchdown_frame": candidate.start,
                "liftoff_frame": candidate.end,
                "duty_factor": candidate.length / cfg.unique_frame_count,
                "emission_disagreement_fraction": candidate.disagreement_fraction,
                "observed_fraction": float(np.mean(states[foot].reliable)),
            }
            for foot, candidate in zip(order, selected)
        },
    }
    provenance = {
        "schema": CONTACT_SCHEDULE_SCHEMA,
        "algorithm": "deterministic-circular-four-foot-hsmm-enumeration-v1",
        "foot_order": list(order),
        "unique_frame_count": cfg.unique_frame_count,
        "characteristic_height": height_scale,
        "thresholds": asdict(cfg),
    }
    return ContactSchedule(
        foot_order=order,
        phases=tuple(phases),
        unique_frame_count=cfg.unique_frame_count,
        score=score,
        qa=qa,
        provenance=provenance,
    )


@dataclass(frozen=True)
class VirtualGroundConfig:
    smoothing_weight: float = 0.05
    huber_delta_height_per_frame: float = 0.003
    max_consensus_p95_height_per_frame: float = 0.003
    max_cumulative_stance_drift_height: float = 0.01
    min_cycle_displacement_height: float = 0.15
    min_per_foot_stance_sweep_height: float = 0.03
    min_observable_feet: int = 3
    min_directional_agreement: float = 0.80
    max_lateral_cycle_drift_height: float = 0.02
    max_speed_coefficient_of_variation: float = 0.35

    def validate(self) -> None:
        for name in (
            "smoothing_weight",
            "huber_delta_height_per_frame",
            "max_consensus_p95_height_per_frame",
            "max_cumulative_stance_drift_height",
            "min_cycle_displacement_height",
            "min_per_foot_stance_sweep_height",
            "min_directional_agreement",
            "max_lateral_cycle_drift_height",
            "max_speed_coefficient_of_variation",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ContractError(f"{name} must be finite and non-negative")
        if self.min_directional_agreement > 1.0:
            raise ContractError("min_directional_agreement must not exceed one")
        if self.huber_delta_height_per_frame <= 0.0:
            raise ContractError(
                "huber_delta_height_per_frame must be finite and positive"
            )
        if (
            isinstance(self.min_observable_feet, bool)
            or not isinstance(self.min_observable_feet, int)
            or not 1 <= self.min_observable_feet <= 4
        ):
            raise ContractError("min_observable_feet must be inside [1, 4]")


@dataclass(frozen=True)
class VirtualGroundResult:
    increments: np.ndarray
    root_path: np.ndarray
    cycle_displacement: np.ndarray
    root_motion_observable: bool
    qa: dict[str, Any]
    provenance: dict[str, Any]


def _normalize(values: np.ndarray, field: str) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float64)
    if vector.shape != (3,) or not np.all(np.isfinite(vector)):
        raise ContractError(f"{field} must contain three finite values")
    length = float(np.linalg.norm(vector))
    if length <= 1e-12:
        raise ContractError(f"{field} must be non-zero")
    return vector / length


def _robust_consensus(
    proposals: np.ndarray,
    weights: np.ndarray,
    *,
    delta: float,
) -> tuple[np.ndarray, float]:
    normalized = weights / float(np.sum(weights))
    estimate = np.sum(proposals * normalized[:, None], axis=0)
    for _ in range(12):
        residual = np.linalg.norm(proposals - estimate, axis=1)
        huber = np.ones_like(residual)
        outside = residual > delta
        huber[outside] = delta / residual[outside]
        combined = weights * huber
        updated = np.sum(proposals * combined[:, None], axis=0) / float(
            np.sum(combined)
        )
        if float(np.linalg.norm(updated - estimate)) <= 1e-12:
            estimate = updated
            break
        estimate = updated
    return estimate, float(np.sum(weights))


def _smooth_increments(
    raw: np.ndarray,
    weights: np.ndarray,
    smoothing_weight: float,
    *,
    forward: np.ndarray,
    cycle_displacement_prior: float | None,
) -> np.ndarray:
    count = len(raw)
    matrix = np.diag(weights)
    rhs = weights[:, None] * raw
    if smoothing_weight:
        for index in range(count):
            following = (index + 1) % count
            matrix[index, index] += smoothing_weight
            matrix[following, following] += smoothing_weight
            matrix[index, following] -= smoothing_weight
            matrix[following, index] -= smoothing_weight
    if np.linalg.matrix_rank(matrix) != count:
        raise ContractError("Virtual-ground temporal system is singular")
    result = np.column_stack(
        [np.linalg.solve(matrix, rhs[:, axis]) for axis in range(3)]
    )
    if cycle_displacement_prior is not None:
        prior = _finite_positive(cycle_displacement_prior, "cycle_displacement_prior")
        response = np.linalg.solve(matrix, np.ones(count, dtype=np.float64))
        denominator = float(np.sum(response))
        if denominator <= 1e-12:
            raise ContractError("Virtual-ground prior constraint is singular")
        current = float(np.dot(np.sum(result, axis=0), forward))
        correction = (prior - current) / denominator
        result += response[:, None] * correction * forward[None, :]
    return result


def _maximum_stance_drift(
    positions: np.ndarray,
    root_path: np.ndarray,
    contact: np.ndarray,
) -> float:
    count = len(contact)
    extended_contact = np.concatenate((contact, contact))
    extended_positions = np.concatenate((positions, positions), axis=0)
    extended_root = np.concatenate(
        (root_path[:-1], root_path[:-1] + root_path[-1]), axis=0
    )
    runs = _circular_true_runs(contact)
    maximum = 0.0
    for run in runs:
        start = run[0]
        length = len(run)
        indices = np.arange(start, start + length)
        world = extended_root[indices] + extended_positions[indices]
        drift = np.linalg.norm(world - world[0], axis=1)
        maximum = max(maximum, float(np.max(drift)))
    if len(extended_contact) != 2 * count:
        raise ContractError("Internal circular contact extension failed")
    return maximum


def solve_virtual_ground_path(
    hoof_positions_by_foot: Mapping[str, np.ndarray],
    schedule: ContactSchedule,
    *,
    ground_normal: np.ndarray,
    forward_axis: np.ndarray,
    characteristic_height: float,
    fps: float,
    require_root_motion: bool = False,
    cycle_displacement_prior: float | None = None,
    config: VirtualGroundConfig | None = None,
) -> VirtualGroundResult:
    """Solve the latent controller path that cancels stance-hoof displacement."""

    cfg = config or VirtualGroundConfig()
    cfg.validate()
    height_scale = _finite_positive(characteristic_height, "characteristic_height")
    frame_rate = _finite_positive(fps, "fps")
    if not isinstance(require_root_motion, bool):
        raise ContractError("require_root_motion must be boolean")
    normal = _normalize(ground_normal, "ground_normal")
    forward_raw = np.asarray(forward_axis, dtype=np.float64)
    forward_tangent = forward_raw - normal * float(np.dot(normal, forward_raw))
    forward = _normalize(forward_tangent, "ground-tangent forward_axis")
    lateral = _normalize(np.cross(normal, forward), "ground lateral axis")
    projector = np.eye(3, dtype=np.float64) - np.outer(normal, normal)
    count = schedule.unique_frame_count
    phases = schedule.phase_by_foot
    if set(hoof_positions_by_foot) != set(schedule.foot_order):
        raise ContractError(
            "Hoof-position feet must exactly match the contact schedule"
        )
    positions: dict[str, np.ndarray] = {}
    for foot in schedule.foot_order:
        array = np.asarray(hoof_positions_by_foot[foot], dtype=np.float64)
        if array.shape != (count, 3) or not np.all(np.isfinite(array)):
            raise ContractError(f"{foot} hoof positions must be finite [{count}, 3]")
        positions[foot] = array

    raw = np.zeros((count, 3), dtype=np.float64)
    edge_weights = np.zeros(count, dtype=np.float64)
    proposal_rows: list[tuple[int, str, np.ndarray, float]] = []
    per_foot_forward: dict[str, float] = {foot: 0.0 for foot in schedule.foot_order}
    direction_weights = 0.0
    positive_direction_weights = 0.0
    delta = cfg.huber_delta_height_per_frame * height_scale
    for frame in range(count):
        following = (frame + 1) % count
        proposals: list[np.ndarray] = []
        weights: list[float] = []
        metadata: list[tuple[str, np.ndarray, float]] = []
        for foot in schedule.foot_order:
            phase = phases[foot]
            if not (phase.contact[frame] and phase.contact[following]):
                continue
            weight = float(min(phase.weights[frame], phase.weights[following]))
            if weight <= 0.0:
                continue
            motion = positions[foot][following] - positions[foot][frame]
            proposal = -(projector @ motion)
            proposals.append(proposal)
            weights.append(weight)
            metadata.append((foot, proposal, weight))
            forward_value = float(np.dot(proposal, forward))
            per_foot_forward[foot] += forward_value
            direction_weights += weight
            if forward_value > 0.0:
                positive_direction_weights += weight
        if not proposals:
            raise ContractError(
                f"Virtual-ground edge {frame}->{following} has no observed stance hoof"
            )
        proposal_array = np.stack(proposals)
        weight_array = np.asarray(weights, dtype=np.float64)
        raw[frame], edge_weights[frame] = _robust_consensus(
            proposal_array,
            weight_array,
            delta=delta,
        )
        for foot, proposal, weight in metadata:
            proposal_rows.append((frame, foot, proposal, weight))

    increments = _smooth_increments(
        raw,
        edge_weights,
        cfg.smoothing_weight,
        forward=forward,
        cycle_displacement_prior=cycle_displacement_prior,
    )
    increments = (projector @ increments.T).T
    root_path = np.zeros((count + 1, 3), dtype=np.float64)
    root_path[1:] = np.cumsum(increments, axis=0)
    cycle_displacement = root_path[-1]
    consensus_errors = [
        float(np.linalg.norm(increments[frame] - proposal))
        for frame, _, proposal, _ in proposal_rows
    ]
    consensus_p95 = float(np.percentile(consensus_errors, 95))
    if consensus_p95 / height_scale > cfg.max_consensus_p95_height_per_frame:
        raise ContractError(
            "Virtual-ground stance consensus failed: "
            f"p95/H={consensus_p95 / height_scale:.6f}"
        )
    stance_drift = {
        foot: _maximum_stance_drift(positions[foot], root_path, phases[foot].contact)
        for foot in schedule.foot_order
    }
    maximum_stance_drift = max(stance_drift.values())
    if maximum_stance_drift / height_scale > cfg.max_cumulative_stance_drift_height:
        raise ContractError(
            "Virtual-ground cumulative stance drift failed: "
            f"max/H={maximum_stance_drift / height_scale:.6f}"
        )
    forward_displacement = float(np.dot(cycle_displacement, forward))
    lateral_displacement = float(np.dot(cycle_displacement, lateral))
    directional_agreement = positive_direction_weights / max(direction_weights, 1e-12)
    observable_feet = sum(
        value / height_scale >= cfg.min_per_foot_stance_sweep_height
        for value in per_foot_forward.values()
    )
    forward_speeds = np.maximum(0.0, increments @ forward) * frame_rate
    positive_speeds = forward_speeds[forward_speeds > height_scale * 1e-9]
    speed_cv = (
        float(np.std(positive_speeds) / np.mean(positive_speeds))
        if positive_speeds.size
        else float("inf")
    )
    extracted_observable = bool(
        forward_displacement / height_scale >= cfg.min_cycle_displacement_height
        and observable_feet >= cfg.min_observable_feet
        and directional_agreement >= cfg.min_directional_agreement
        and abs(lateral_displacement) / height_scale
        <= cfg.max_lateral_cycle_drift_height
        and speed_cv <= cfg.max_speed_coefficient_of_variation
    )
    mode = "authored_prior" if cycle_displacement_prior is not None else "extracted"
    root_motion_observable = (
        extracted_observable or cycle_displacement_prior is not None
    )
    if require_root_motion and not root_motion_observable:
        raise ContractError(
            "root_motion_unobservable: no speed/stride prior and the in-place stance sweep "
            f"does not identify forward displacement; D/H={forward_displacement / height_scale:.6f}, "
            f"observable_feet={observable_feet}, direction={directional_agreement:.6f}"
        )

    qa = {
        "decision": "accepted_virtual_ground_path",
        "root_motion_observable": root_motion_observable,
        "mode": mode,
        "forward_cycle_displacement_world": forward_displacement,
        "forward_cycle_displacement_height": forward_displacement / height_scale,
        "lateral_cycle_displacement_world": lateral_displacement,
        "lateral_cycle_displacement_height": lateral_displacement / height_scale,
        "directional_agreement": directional_agreement,
        "observable_feet": observable_feet,
        "per_foot_forward_stance_sweep_world": per_foot_forward,
        "consensus_error_p95_world_per_frame": consensus_p95,
        "consensus_error_p95_height_per_frame": consensus_p95 / height_scale,
        "maximum_stance_drift_world": maximum_stance_drift,
        "maximum_stance_drift_height": maximum_stance_drift / height_scale,
        "per_foot_stance_drift_world": stance_drift,
        "positive_forward_speed_cv": speed_cv if math.isfinite(speed_cv) else None,
    }
    provenance = {
        "schema": VIRTUAL_GROUND_SCHEMA,
        "algorithm": "robust-contact-consensus-circular-smoothing-v1",
        "thresholds": asdict(cfg),
        "frame_count": count,
        "fps": frame_rate,
        "characteristic_height": height_scale,
        "cycle_displacement_prior": cycle_displacement_prior,
    }
    return VirtualGroundResult(
        increments=increments,
        root_path=root_path,
        cycle_displacement=cycle_displacement,
        root_motion_observable=root_motion_observable,
        qa=qa,
        provenance=provenance,
    )
