import numpy as np
import pytest
from animation_fitting.spine_surface_retention import (
    compare_dorsal_curves, dorsal_angles_degrees, dorsal_landmarks,
)


def surface():
    # Four longitudinal cross-sections, including a non-flat dorsal surface.
    tails = np.array([[0, y, 0] for y in (0., 1., 2., 3.)])
    points = np.array([[x, y, z + .04 * y * y]
                       for y in tails[:, 1] for x in np.linspace(-.1, .1, 11)
                       for z in np.linspace(0, 1, 11)])
    return points, tails


def test_landmarks_and_curves_ignore_model_scale_and_global_rigid_motion():
    points, tails = surface()
    marks = dorsal_landmarks(points, tails)
    for scale in (.01, 1., 100.):
        scaled = dorsal_landmarks(points * scale, tails * scale)
        assert all(np.array_equal(a, b) for a, b in zip(marks, scaled))
    rotation = np.array([[0, -1, 0], [1, 0, 0], [0, 0, 1.]])
    assert dorsal_angles_degrees(points @ rotation.T + [3, 4, 5], marks) == pytest.approx(
        dorsal_angles_degrees(points, marks), abs=1e-10)


def test_curve_gate_rejects_lost_articulation_despite_similar_mean_shape():
    full = np.array([[5., 10.], [7., 11.], [5., 10.]])
    assert compare_dorsal_curves(full, full + .05)['passed']
    frozen = np.repeat(full[:1], len(full), axis=0)
    report = compare_dorsal_curves(full, frozen)
    assert not report['passed'] and report['max_angle_error_degrees'] == 2.


def test_surface_validation_rejects_missing_or_degenerate_measurements():
    points, tails = surface()
    with pytest.raises(ValueError): dorsal_landmarks(points[:3], tails)
    with pytest.raises(ValueError): dorsal_landmarks(points, np.zeros_like(tails))
    with pytest.raises(ValueError): dorsal_angles_degrees(points, [[0], [0], [0]])
    with pytest.raises(ValueError): compare_dorsal_curves([[1]], [[float('nan')]])
    with pytest.raises(ValueError): compare_dorsal_curves([[1]], [[1]], 0)


def test_landmarks_reject_shuffled_stations_instead_of_crossing_the_back():
    points, tails = surface()
    with pytest.raises(ValueError, match='strictly ordered'):
        dorsal_landmarks(points, tails[[0, 2, 1, 3]])
