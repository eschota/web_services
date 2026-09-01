from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from animation_fitting.errors import ContractError
from animation_fitting.optimizer import (
    _camera_contract,
    _observation_camera,
    _project_safe,
    _validate_contracts,
)
from animation_fitting.rig import Camera


def _camera() -> Camera:
    return Camera(
        width=320,
        height=240,
        fx=260.0,
        fy=250.0,
        cx=157.5,
        cy=118.25,
        world_to_camera=np.eye(4, dtype=np.float64),
    )


def test_same_aspect_observations_use_exact_uniformly_scaled_intrinsics() -> None:
    source = _camera()
    scaled = _observation_camera(source, width=160, height=120)

    assert scaled.width == 160
    assert scaled.height == 120
    assert scaled.fx == source.fx * 0.5
    assert scaled.fy == source.fy * 0.5
    assert scaled.cx == source.cx * 0.5
    assert scaled.cy == source.cy * 0.5
    assert scaled.world_to_camera is source.world_to_camera

    point = np.asarray((1.0, 2.0, -8.0), dtype=np.float64)
    source_xy, source_depth, source_behind = _project_safe(source, point)
    scaled_xy, scaled_depth, scaled_behind = _project_safe(scaled, point)
    assert np.array_equal(scaled_xy, source_xy * 0.5)
    assert scaled_depth == source_depth
    assert scaled_behind == source_behind

    contract = _camera_contract(source, scaled)
    assert contract["scale"] == 0.5
    assert contract["source_intrinsics"] == {
        "fx": 260.0,
        "fy": 250.0,
        "cx": 157.5,
        "cy": 118.25,
    }
    assert contract["intrinsics"] == {
        "fx": 130.0,
        "fy": 125.0,
        "cx": 78.75,
        "cy": 59.125,
    }


def test_optimizer_contract_accepts_same_aspect_scaled_observations() -> None:
    source = _camera()
    rig = SimpleNamespace(camera=source, anchors={})
    observations = SimpleNamespace(
        width=640,
        height=480,
        track_by_anchor={},
        contacts=(),
        frame_count=2,
    )

    scaled = _validate_contracts(rig, observations)

    assert scaled.fx == source.fx * 2.0
    assert scaled.fy == source.fy * 2.0


@pytest.mark.parametrize(("width", "height"), ((160, 119), (321, 240)))
def test_optimizer_contract_rejects_arbitrary_aspect_or_intrinsics(
    width: int,
    height: int,
) -> None:
    with pytest.raises(ContractError, match="arbitrary intrinsics are not allowed"):
        _observation_camera(_camera(), width=width, height=height)
