"""Unit tests of regen helpers: angular coverage, sector runs, attach
semantics, label smoothing / island absorption, welding."""

import numpy as np
import pytest

pytest.importorskip("scipy")
trimesh = pytest.importorskip("trimesh")

from regen.chains import _longest_run, angles_around, angular_coverage, attach_semantic  # noqa: E402
from regen.geometry import weld  # noqa: E402
from regen.landmarks import compute_landmarks  # noqa: E402
from regen.segment import absorb_islands, smooth_labels  # noqa: E402
from regen.synthetic import make_body  # noqa: E402


def _ring(start_deg, span_deg, n=720):
    theta = np.radians(start_deg + np.linspace(0.0, span_deg, n, endpoint=False))
    return np.stack([np.sin(theta), np.zeros(n), np.cos(theta)], axis=1)


def test_angles_start_at_the_front_and_turn_to_the_left():
    pts = np.array([[0, 0, 1.0], [1.0, 0, 0], [0, 0, -1.0], [-1.0, 0, 0]])
    assert np.allclose(angles_around(pts, np.zeros(2), 1), [0, 90, 180, 270])
    # facing -Z: the front is -Z and the left is -X
    assert np.allclose(angles_around(pts, np.zeros(2), -1), [180, 270, 0, 90])


def test_chain_angles_turn_left_for_a_character_facing_minus_z():
    # joints are rebuilt from (radius, angle) with the same convention
    import math

    facing = -1
    theta = 90.0  # the character's left
    x = facing * math.sin(math.radians(theta))
    z = facing * math.cos(math.radians(theta))
    assert (x, round(z, 12)) == (-1.0, 0.0)
    assert angles_around(np.array([[x, 0.0, z]]), np.zeros(2), facing)[0] == pytest.approx(theta)


def test_angular_coverage_of_rings_and_arcs():
    full = angles_around(_ring(0, 360), np.zeros(2), 1)
    assert angular_coverage(full, 5.0, 2) == (360.0, 0.0, 360.0)
    back = angles_around(_ring(105, 150), np.zeros(2), 1)
    cov, start, length = angular_coverage(back, 5.0, 2)
    assert cov == pytest.approx(150, abs=5) and start == pytest.approx(105, abs=5) and length == pytest.approx(150, abs=5)
    wrapped = angles_around(_ring(300, 120), np.zeros(2), 1)
    cov, start, length = angular_coverage(wrapped, 5.0, 2)
    assert start == pytest.approx(300, abs=5) and length == pytest.approx(120, abs=5)


def test_longest_run_of_sectors():
    assert _longest_run([0, 1, 2, 3], 4, circular=True) == [0, 1, 2, 3]
    assert _longest_run([6, 7, 0, 1, 3], 8, circular=True) == [6, 7, 0, 1]
    assert _longest_run([0, 1, 3, 4, 5], 6, circular=False) == [3, 4, 5]
    assert _longest_run([], 5, circular=False) == []


@pytest.fixture(scope="module")
def landmarks():
    body = make_body(0.8)
    return compute_landmarks(np.asarray(body.vertices), np.asarray(body.faces))


@pytest.mark.parametrize(
    "point, kind, expected",
    [
        ((0.0, 0.95, 0.0), "cloth", "head"),
        ((0.3, 0.9, 0.0), "hair", "head"),
        ((0.30, 0.745, 0.0), "cloth", "upper_arm_l"),
        ((-0.30, 0.745, 0.0), "cloth", "upper_arm_r"),
        ((0.0, 0.52, 0.0), "cloth", "hips"),
        ((0.16, 0.50, 0.0), "cloth", "hips"),  # a sash on the hip is not an arm
        ((0.0, 0.62, -0.09), "cloth", "spine"),
        ((0.0, 0.76, -0.12), "cloth", "chest"),
        ((0.075, 0.30, 0.0), "cloth", "upper_leg_l"),
        ((-0.075, 0.30, 0.0), "cloth", "upper_leg_r"),
    ],
)
def test_attach_semantics(landmarks, point, kind, expected):
    assert attach_semantic(kind, np.asarray(point), landmarks) == expected


def _grid(n=30):
    mesh = trimesh.creation.box(extents=(1, 1, 1))
    mesh = mesh.subdivide().subdivide().subdivide().subdivide()
    return weld(mesh.vertices, mesh.faces)


def test_small_islands_are_absorbed_and_isolated_pieces_kept():
    topo = _grid()
    n = topo.n_vertices
    labels = np.zeros(n, dtype=np.uint8)
    # a big cloth region (the top face) and a 1-ring speck of hair inside it
    top = topo.vertices[:, 1] > 0.49
    labels[top] = 2
    speck = int(np.flatnonzero(top & (np.abs(topo.vertices[:, 0]) < 0.05) & (np.abs(topo.vertices[:, 2]) < 0.05))[0])
    labels[speck] = 1
    out = absorb_islands(labels, topo, min_size=10)
    assert out[speck] == 2
    assert np.array_equal(out[top], np.full(int(top.sum()), 2))
    # a separate mesh piece with no border keeps its label
    other = weld(np.asarray(topo.vertices) + 5.0, topo.faces)
    both = weld(np.concatenate([topo.vertices, other.vertices]), np.concatenate([topo.faces, other.faces + n]))
    labels2 = np.concatenate([np.zeros(n, np.uint8), np.full(other.n_vertices, 1, np.uint8)])
    assert np.array_equal(absorb_islands(labels2, both, min_size=10 ** 6), labels2)


def test_majority_smoothing_removes_single_vertex_noise():
    topo = _grid()
    rng = np.random.default_rng(0)
    labels = np.zeros(topo.n_vertices, dtype=np.uint8)
    noisy = rng.choice(topo.n_vertices, size=topo.n_vertices // 50, replace=False)
    labels[noisy] = 2
    out = smooth_labels(labels, topo, iterations=2)
    assert np.mean(out == 0) > 0.99


def test_weld_merges_seam_duplicates_only():
    mesh = trimesh.creation.icosphere(2)
    corner = mesh.faces.reshape(-1)
    split = weld(mesh.vertices[corner], np.arange(len(corner)).reshape(-1, 3))
    assert split.n_vertices == len(mesh.vertices)
    assert len(split.edges[0]) == len(mesh.edges_unique)
    near = mesh.vertices.copy()
    near[0] += 1e-3  # a real (not seam) neighbour stays separate
    assert weld(np.concatenate([mesh.vertices, near[:1]]), mesh.faces).n_vertices == len(mesh.vertices) + 1
