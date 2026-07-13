from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from animation_fitting.contact_profile import load_contact_profile
from animation_fitting.rig import Anchor, Camera, RigBundle
from animation_fitting.tracking_runtime.contact_integration import (
    calibrate_bundle_camera_z,
    infer_contact_runtime,
)
from animation_fitting.tracking_runtime.contact_solver import DepthCalibrationConfig


HORSE_HEIGHT = 2.469202561500424
PROFILE_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "contact_profiles"
    / "horse_2.walk_forward.v1.json"
)


def _rig(tmp_path: Path, *, width: int = 320, height: int = 240) -> RigBundle:
    profile = load_contact_profile(PROFILE_PATH)
    world_to_camera = np.asarray(
        (
            (1.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 1.0, 0.0),
            (0.0, 1.0, 0.0, -8.0),
            (0.0, 0.0, 0.0, 1.0),
        ),
        dtype=np.float64,
    )
    anchors = {}
    for foot_index, foot_id in enumerate(profile.foot_order):
        foot = profile.feet[foot_id]
        for anchor_index, (vertex_id, anchor_id) in enumerate(
            zip(foot.vertex_ids, foot.anchor_ids)
        ):
            anchors[anchor_id] = Anchor(
                id=anchor_id,
                bone=foot.bone,
                vertex_id=vertex_id,
                rest_world=np.asarray(
                    (
                        -0.3 + foot_index * 0.2 + anchor_index * 0.005,
                        0.0,
                        0.0,
                    ),
                    dtype=np.float64,
                ),
                skin_weight=1.0,
                influences=(),
            )
    anchors["body:999"] = Anchor(
        id="body:999",
        bone="body",
        vertex_id=999,
        rest_world=np.asarray((0.0, 0.0, HORSE_HEIGHT), dtype=np.float64),
        skin_weight=1.0,
        influences=(),
    )
    return RigBundle(
        root=tmp_path,
        metadata_path=tmp_path / "fitting_bundle.json",
        metadata_sha256="a" * 64,
        immutable_manifest_path=tmp_path / "immutable_manifest.json",
        immutable_manifest_sha256="b" * 64,
        metadata={"source": {"rig_type": "HORSE_2"}},
        artifacts={},
        armature_name="SyntheticHorse",
        armature_world=np.eye(4),
        bones={},
        bone_order=(),
        anchors=anchors,
        camera=Camera(
            width=width,
            height=height,
            fx=200.0,
            fy=800.0,
            cx=width * 0.5,
            cy=220.0,
            world_to_camera=world_to_camera,
        ),
        ground_normal=np.asarray((0.0, 0.0, 1.0), dtype=np.float64),
        ground_height=0.0,
    )


def _contact(start: int, length: int = 30) -> np.ndarray:
    result = np.zeros(48, dtype=bool)
    result[(start + np.arange(length)) % 48] = True
    return result


@pytest.mark.parametrize(
    ("render_width", "render_height"),
    ((320, 240), (160, 120)),
)
def test_contact_runtime_emits_sixteen_anchor_constraints_and_in_place_ground(
    tmp_path: Path,
    render_width: int,
    render_height: int,
) -> None:
    rig = _rig(tmp_path)
    profile = load_contact_profile(PROFILE_PATH)
    anchor_ids = profile.priority_anchor_ids
    points = np.zeros((49, len(anchor_ids), 2), dtype=np.float32)
    visible = np.ones((49, len(anchor_ids)), dtype=bool)
    confidence = np.full((49, len(anchor_ids)), 0.95, dtype=np.float32)
    masks = np.zeros((49, render_height, render_width), dtype=bool)
    scale_x = render_width / rig.camera.width
    scale_y = render_height / rig.camera.height
    masks[
        :,
        max(1, int(round(20 * scale_y))) : min(
            render_height, int(round(222 * scale_y))
        ),
        max(1, int(round(20 * scale_x))) : min(render_width, int(round(301 * scale_x))),
    ] = True
    camera_z = np.full(masks.shape, 8.0, dtype=np.float32)
    fx = rig.camera.fx * scale_x
    fy = rig.camera.fy * scale_y
    cx = rig.camera.cx * scale_x
    cy = rig.camera.cy * scale_y
    touchdown = dict(zip(profile.foot_order, (0, 12, 24, 36)))
    expected_heights = {}
    for foot_index, foot_id in enumerate(profile.foot_order):
        contact = _contact(touchdown[foot_id])
        height_world = np.where(
            contact,
            0.005 * HORSE_HEIGHT,
            0.08 * HORSE_HEIGHT,
        )
        expected_heights[foot_id] = height_world
        for anchor_offset, anchor_id in enumerate(profile.feet[foot_id].anchor_ids):
            track = anchor_ids.index(anchor_id)
            x_world = -0.3 + foot_index * 0.2 + anchor_offset * 0.005
            points[:48, track, 0] = fx * x_world / 8.0 + cx
            points[:48, track, 1] = cy - fy * height_world / 8.0
            points[48, track] = points[0, track]

    result = infer_contact_runtime(
        rig=rig,
        profile=profile,
        camera_z=camera_z,
        points_xy=points,
        visible=visible,
        confidence=confidence,
        masks=masks,
        anchor_ids=anchor_ids,
        fps=24.0,
    )

    assert len(result.contacts) == 16
    assert {row["anchor_id"] for row in result.contacts} == set(anchor_ids)
    assert all(row["frames"] for row in result.contacts)
    assert all((48 in row["frames"]) == (0 in row["frames"]) for row in result.contacts)
    assert result.virtual_ground_increments.shape == (48, 3)
    assert result.virtual_ground_root_path.shape == (49, 3)
    assert np.max(np.abs(result.virtual_ground_root_path)) < 1e-10
    assert result.provenance["canonical_output_motion"] == "in_place"
    assert result.provenance["derived_root_motion_available"] is False
    assert result.qa["contact_schedule"]["perturbations"]["successful_runs"] >= 14
    assert all(
        np.all(np.isfinite(positions)) for positions in result.hoof_positions.values()
    )
    for foot_index, foot_id in enumerate(profile.foot_order):
        positions = result.hoof_positions[foot_id]
        expected_x = -0.3 + foot_index * 0.2 + 0.0075
        assert np.max(np.abs(positions[:, 0] - expected_x)) < 1e-5
        assert np.max(np.abs(positions[:, 1])) < 1e-8
        assert np.max(np.abs(positions[:, 2] - expected_heights[foot_id])) < 1e-5


def test_bundle_camera_z_calibration_resizes_immutable_reference(
    tmp_path: Path,
) -> None:
    import cv2

    rig = _rig(tmp_path, width=160, height=100)
    yy, xx = np.mgrid[0:100, 0:160]
    reference = (4.0 + 0.01 * xx + 0.006 * yy).astype(np.float32)
    reference_path = tmp_path / "reference_camera_z.npy"
    np.save(reference_path, reference)
    rig.artifacts["camera_z"] = reference_path
    resized = cv2.resize(reference, (80, 50), interpolation=cv2.INTER_LINEAR)
    relative = (resized - 1.2) / 2.3

    result = calibrate_bundle_camera_z(
        rig,
        relative,
        np.ones(resized.shape, dtype=bool),
        config=DepthCalibrationConfig(min_valid_pixels=1_000),
    )

    assert result.mode == "affine"
    assert result.scale == pytest.approx(2.3, abs=1e-6)
    assert result.offset == pytest.approx(1.2, abs=1e-6)
    assert result.camera_z.shape == (1, 50, 80)
    assert np.max(np.abs(result.camera_z[0] - resized)) < 1e-5
