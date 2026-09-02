from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from animation_fitting.errors import ContractError
from animation_fitting.tracking_runtime.contact_solver import (
    ContactInferenceConfig,
    DepthCalibrationConfig,
    HoofEvidence,
    VirtualGroundConfig,
    calibrate_relative_depth_to_camera_z,
    infer_circular_walk_contacts,
    solve_virtual_ground_path,
)


HORSE_HEIGHT = 2.469202561500424
FOOT_ORDER = ("hind_near", "fore_near", "hind_far", "fore_far")
TOUCHDOWNS = (0, 12, 24, 36)
CONTACT_LENGTH = 30


def _depth_config(**overrides: object) -> DepthCalibrationConfig:
    return replace(
        DepthCalibrationConfig(
            min_valid_pixels=500,
            min_foreground_coverage=0.95,
            min_abs_spearman=0.95,
            max_median_abs_error_height=0.005,
            max_p95_abs_error_height=0.02,
        ),
        **overrides,
    )


def test_camera_z_calibration_selects_affine_and_applies_all_frames() -> None:
    yy, xx = np.mgrid[0:32, 0:40]
    reference = 3.0 + 0.025 * xx + 0.018 * yy
    relative_first = (reference - 1.2) / 2.3
    relative = np.stack((relative_first, relative_first + 0.05, relative_first + 0.10))
    mask = np.ones(reference.shape, dtype=bool)

    result = calibrate_relative_depth_to_camera_z(
        relative,
        reference.astype(np.float32),
        mask,
        characteristic_height=HORSE_HEIGHT,
        config=_depth_config(),
    )

    assert result.mode == "affine"
    assert result.scale == pytest.approx(2.3, abs=1e-8)
    assert result.offset == pytest.approx(1.2, abs=1e-8)
    assert result.camera_z.dtype == np.float32
    assert result.camera_z.shape == (3, 32, 40)
    assert np.max(np.abs(result.camera_z[0] - reference)) < 1e-5
    assert np.median(result.camera_z[1] - result.camera_z[0]) == pytest.approx(0.115)
    assert result.provenance["selected"]["abs_spearman"] > 0.999


def test_camera_z_calibration_selects_reciprocal_affine() -> None:
    yy, xx = np.mgrid[0:36, 0:42]
    reference = 2.2 + 0.035 * xx + 0.021 * yy
    relative_first = 1.0 / ((reference - 0.45) / 1.7)
    relative = np.stack((relative_first, relative_first * 0.98))
    mask = np.ones(reference.shape, dtype=bool)

    result = calibrate_relative_depth_to_camera_z(
        relative,
        reference.astype(np.float32),
        mask,
        characteristic_height=HORSE_HEIGHT,
        config=_depth_config(),
    )

    assert result.mode == "reciprocal_affine"
    assert result.scale == pytest.approx(1.7, abs=1e-7)
    assert result.offset == pytest.approx(0.45, abs=1e-7)
    assert np.max(np.abs(result.camera_z[0] - reference)) < 1e-5
    candidate_modes = [row["mode"] for row in result.provenance["candidates"]]
    assert candidate_modes[0] == "reciprocal_affine"


def test_camera_z_calibration_rejects_sparse_later_frame() -> None:
    yy, xx = np.mgrid[0:32, 0:40]
    reference = 2.2 + 0.035 * xx + 0.021 * yy
    relative_first = 1.0 / ((reference - 0.45) / 1.7)
    relative = np.stack((relative_first, np.zeros_like(relative_first)))

    with pytest.raises(ContractError, match="frames lost required valid-pixel"):
        calibrate_relative_depth_to_camera_z(
            relative,
            reference.astype(np.float32),
            np.ones(reference.shape, dtype=bool),
            characteristic_height=HORSE_HEIGHT,
            config=_depth_config(),
        )


def _hoof_local_fixture() -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Reference plane, exact relative depth, mask and four hoof-like anchors."""

    yy, xx = np.mgrid[0:240, 0:320]
    reference = 3.0 + 0.004 * xx + 0.003 * yy
    relative = (reference - 1.2) / 2.3
    mask = np.ones(reference.shape, dtype=bool)
    anchors = np.asarray(
        [[60.0, 210.0], [120.0, 214.0], [200.0, 208.0], [260.0, 212.0]],
        dtype=np.float64,
    )
    return reference, relative, mask, anchors


def _distance_to_anchors(shape: tuple[int, int], anchors: np.ndarray) -> np.ndarray:
    yy, xx = np.mgrid[0 : shape[0], 0 : shape[1]]
    distance = np.full(shape, np.inf, dtype=np.float64)
    for x, y in anchors:
        distance = np.minimum(distance, np.hypot(xx - x, yy - y))
    return distance


def test_camera_z_calibration_hoof_local_qa_gates_only_anchor_neighborhoods() -> None:
    reference, relative, mask, anchors = _hoof_local_fixture()
    # Zero-mean sinusoidal body distortion away from every hoof neighborhood:
    # the global median/p95 blow past the gates while hooves stay accurate.
    distance = _distance_to_anchors(reference.shape, anchors)
    yy, xx = np.mgrid[0:240, 0:320]
    corrupted = relative + np.where(
        distance > 14.0, 0.03 * np.sin(0.9 * xx + 0.7 * yy), 0.0
    )

    result = calibrate_relative_depth_to_camera_z(
        corrupted[None, ...],
        reference.astype(np.float32),
        mask,
        characteristic_height=HORSE_HEIGHT,
        config=_depth_config(min_abs_spearman=0.90),
        qa_anchor_points_xy=anchors,
    )

    assert result.mode == "affine"
    assert result.scale == pytest.approx(2.3, abs=5e-2)
    assert result.offset == pytest.approx(1.2, abs=5e-2)
    selected = result.provenance["selected"]
    assert selected["error_qa_region"]["mode"] == "contact_anchor_local"
    assert selected["error_qa_region"]["anchor_count"] == 4
    assert selected["error_qa_region"]["pixels"] >= 100
    # ~2.5% of the frame diagonal (240x320 -> diagonal 400 -> 10 px).
    assert selected["error_qa_region"]["radius_px"] == pytest.approx(10.0)
    assert selected["median_abs_error_height"] <= 0.005
    assert selected["p95_abs_error_height"] <= 0.02
    assert selected["global_median_abs_error_height"] > 0.005
    assert selected["global_median_abs_error_world"] > selected["median_abs_error_world"]


def test_camera_z_calibration_hoof_local_qa_rejects_wrong_hoof_depth() -> None:
    reference, relative, mask, anchors = _hoof_local_fixture()
    # One-sided bias only inside the hoof neighborhoods: globally negligible,
    # locally fatal for contact inference, so QA must fail closed.
    distance = _distance_to_anchors(reference.shape, anchors)
    corrupted = relative + np.where(distance <= 14.0, 0.05, 0.0)

    with pytest.raises(ContractError, match="Camera-Z calibration QA rejected"):
        calibrate_relative_depth_to_camera_z(
            corrupted[None, ...],
            reference.astype(np.float32),
            mask,
            characteristic_height=HORSE_HEIGHT,
            config=_depth_config(),
            qa_anchor_points_xy=anchors,
        )


def test_camera_z_calibration_hoof_local_qa_requires_region_pixels() -> None:
    reference, relative, mask, _ = _hoof_local_fixture()
    outside = np.asarray([[-500.0, -500.0]], dtype=np.float64)

    with pytest.raises(ContractError, match="No camera-Z calibration model is valid"):
        calibrate_relative_depth_to_camera_z(
            relative[None, ...],
            reference.astype(np.float32),
            mask,
            characteristic_height=HORSE_HEIGHT,
            config=_depth_config(),
            qa_anchor_points_xy=outside,
        )


def test_camera_z_calibration_rejects_non_finite_anchor_projections() -> None:
    reference, relative, mask, anchors = _hoof_local_fixture()
    anchors = anchors.copy()
    anchors[0, 0] = np.nan

    with pytest.raises(ContractError, match="qa_anchor_points_xy"):
        calibrate_relative_depth_to_camera_z(
            relative[None, ...],
            reference.astype(np.float32),
            mask,
            characteristic_height=HORSE_HEIGHT,
            config=_depth_config(),
            qa_anchor_points_xy=anchors,
        )


def test_camera_z_calibration_empty_anchor_set_falls_back_to_global_qa() -> None:
    reference, relative, mask, _ = _hoof_local_fixture()

    result = calibrate_relative_depth_to_camera_z(
        relative[None, ...],
        reference.astype(np.float32),
        mask,
        characteristic_height=HORSE_HEIGHT,
        config=_depth_config(),
        qa_anchor_points_xy=np.zeros((0, 2), dtype=np.float64),
    )

    selected = result.provenance["selected"]
    assert selected["error_qa_region"]["mode"] == "global_foreground"
    assert selected["median_abs_error_world"] == pytest.approx(
        selected["global_median_abs_error_world"]
    )


def test_camera_z_calibration_rejects_uncorrelated_depth() -> None:
    yy, xx = np.mgrid[0:32, 0:40]
    reference = 2.5 + 0.02 * xx + 0.01 * yy
    relative = (
        np.random.default_rng(42)
        .permutation(reference.reshape(-1))
        .reshape(reference.shape)
    )

    with pytest.raises(ContractError, match="Camera-Z calibration QA rejected"):
        calibrate_relative_depth_to_camera_z(
            relative[None, ...],
            reference,
            np.ones(reference.shape, dtype=bool),
            characteristic_height=HORSE_HEIGHT,
            config=_depth_config(min_abs_spearman=0.90),
        )


def _circular_contact(start: int, length: int = CONTACT_LENGTH) -> np.ndarray:
    result = np.zeros(48, dtype=bool)
    result[(start + np.arange(length)) % 48] = True
    return result


def _walk_evidence(
    *,
    touchdown_by_foot: dict[str, int] | None = None,
    occlusions: dict[str, tuple[int, ...]] | None = None,
) -> dict[str, HoofEvidence]:
    touchdowns = touchdown_by_foot or dict(zip(FOOT_ORDER, TOUCHDOWNS))
    occluded = occlusions or {}
    result: dict[str, HoofEvidence] = {}
    for foot in FOOT_ORDER:
        contact = _circular_contact(touchdowns[foot])
        bbox = np.full(48, 300.0, dtype=np.float64)
        visible = np.full(48, 4, dtype=np.int64)
        confidence = np.full(48, 0.95, dtype=np.float64)
        for frame in occluded.get(foot, ()):
            visible[frame] = 0
            confidence[frame] = 0.0
        result[foot] = HoofEvidence(
            foot_id=foot,
            height_world=np.where(contact, 0.005 * HORSE_HEIGHT, 0.08 * HORSE_HEIGHT),
            vertical_speed_world_per_second=np.zeros(48, dtype=np.float64),
            silhouette_bottom_gap_px=np.where(contact, 2.0, 24.0),
            mask_bbox_height_px=bbox,
            visible_anchor_count=visible,
            confidence=confidence,
        )
    return result


def _contact_config(**overrides: object) -> ContactInferenceConfig:
    return replace(ContactInferenceConfig(), **overrides)


def test_circular_walk_contact_inference_bridges_short_in_stance_occlusion() -> None:
    evidence = _walk_evidence(occlusions={"hind_near": (8, 9, 10)})

    schedule = infer_circular_walk_contacts(
        evidence,
        foot_order=FOOT_ORDER,
        characteristic_height=HORSE_HEIGHT,
        config=_contact_config(),
    )

    assert schedule.unique_frame_count == 48
    assert schedule.foot_order == FOOT_ORDER
    assert schedule.qa["support"]["minimum"] == 2
    assert schedule.qa["support"]["maximum"] == 3
    assert schedule.qa["perturbations"]["successful_runs"] >= 14
    for foot, touchdown in zip(FOOT_ORDER, TOUCHDOWNS):
        phase = schedule.phase_by_foot[foot]
        assert phase.touchdown_frame == touchdown
        assert phase.liftoff_frame == (touchdown + CONTACT_LENGTH) % 48
        assert int(np.sum(phase.contact)) == CONTACT_LENGTH
    hind = schedule.phase_by_foot["hind_near"]
    assert np.all(hind.contact[[8, 9, 10]])
    assert not np.any(hind.observed[[8, 9, 10]])
    assert np.all(hind.weights[[8, 9, 10]] == 0.0)


def test_circular_walk_contact_inference_rejects_long_occlusion() -> None:
    evidence = _walk_evidence(occlusions={"hind_near": (8, 9, 10, 11)})

    with pytest.raises(ContractError, match="occlusion gap"):
        infer_circular_walk_contacts(
            evidence,
            foot_order=FOOT_ORDER,
            characteristic_height=HORSE_HEIGHT,
        )


def test_circular_walk_contact_inference_rejects_wrong_footfall_order() -> None:
    wrong = {
        "hind_near": 0,
        "fore_near": 24,
        "hind_far": 12,
        "fore_far": 36,
    }

    with pytest.raises(ContractError, match="circular walk schedule|contact phase"):
        infer_circular_walk_contacts(
            _walk_evidence(touchdown_by_foot=wrong),
            foot_order=FOOT_ORDER,
            characteristic_height=HORSE_HEIGHT,
        )


def _periodic_hoof_positions(
    contact: np.ndarray,
    *,
    forward_step: float,
    lateral_offset: float,
) -> np.ndarray:
    contact_edges = contact & np.roll(contact, -1)
    stance_edges = int(np.sum(contact_edges))
    swing_edges = len(contact) - stance_edges
    deltas = np.zeros((len(contact), 3), dtype=np.float64)
    deltas[contact_edges, 0] = -forward_step
    deltas[~contact_edges, 0] = forward_step * stance_edges / swing_edges
    positions = np.zeros((len(contact), 3), dtype=np.float64)
    positions[0, 1] = lateral_offset
    for frame in range(len(contact) - 1):
        positions[frame + 1] = positions[frame] + deltas[frame]
    assert positions[0, 0] - positions[-1, 0] == pytest.approx(deltas[-1, 0])
    return positions


def _stable_schedule() -> object:
    return infer_circular_walk_contacts(
        _walk_evidence(),
        foot_order=FOOT_ORDER,
        characteristic_height=HORSE_HEIGHT,
    )


def test_virtual_ground_recovers_observable_controller_displacement() -> None:
    schedule = _stable_schedule()
    expected_cycle_displacement = 0.40 * HORSE_HEIGHT
    forward_step = expected_cycle_displacement / 48.0
    positions = {
        foot: _periodic_hoof_positions(
            schedule.phase_by_foot[foot].contact,
            forward_step=forward_step,
            lateral_offset=index * 0.15,
        )
        for index, foot in enumerate(FOOT_ORDER)
    }

    result = solve_virtual_ground_path(
        positions,
        schedule,
        ground_normal=np.asarray((0.0, 0.0, 1.0)),
        forward_axis=np.asarray((1.0, 0.0, 0.0)),
        characteristic_height=HORSE_HEIGHT,
        fps=30.0,
        require_root_motion=True,
    )

    assert result.root_motion_observable is True
    assert result.root_path.shape == (49, 3)
    assert result.cycle_displacement[0] == pytest.approx(
        expected_cycle_displacement, rel=1e-8
    )
    assert np.max(np.abs(result.increments[:, 0] - forward_step)) < 1e-9
    assert result.qa["observable_feet"] == 4
    assert result.qa["consensus_error_p95_height_per_frame"] < 1e-10
    assert result.qa["maximum_stance_drift_height"] < 1e-10


def test_virtual_ground_fails_closed_when_root_motion_is_unobservable() -> None:
    schedule = _stable_schedule()
    positions = {
        foot: np.tile(np.asarray((0.0, index * 0.15, 0.0)), (48, 1))
        for index, foot in enumerate(FOOT_ORDER)
    }

    diagnostic = solve_virtual_ground_path(
        positions,
        schedule,
        ground_normal=np.asarray((0.0, 0.0, 1.0)),
        forward_axis=np.asarray((1.0, 0.0, 0.0)),
        characteristic_height=HORSE_HEIGHT,
        fps=30.0,
        require_root_motion=False,
    )
    assert diagnostic.root_motion_observable is False
    assert diagnostic.qa["forward_cycle_displacement_height"] == pytest.approx(0.0)

    with pytest.raises(ContractError, match="root_motion_unobservable"):
        solve_virtual_ground_path(
            positions,
            schedule,
            ground_normal=np.asarray((0.0, 0.0, 1.0)),
            forward_axis=np.asarray((1.0, 0.0, 0.0)),
            characteristic_height=HORSE_HEIGHT,
            fps=30.0,
            require_root_motion=True,
        )

    with pytest.raises(ContractError, match="huber_delta_height_per_frame"):
        solve_virtual_ground_path(
            positions,
            schedule,
            ground_normal=np.asarray((0.0, 0.0, 1.0)),
            forward_axis=np.asarray((1.0, 0.0, 0.0)),
            characteristic_height=HORSE_HEIGHT,
            fps=30.0,
            config=replace(
                VirtualGroundConfig(),
                huber_delta_height_per_frame=0.0,
            ),
        )
