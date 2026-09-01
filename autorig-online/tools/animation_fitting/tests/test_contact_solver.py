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
