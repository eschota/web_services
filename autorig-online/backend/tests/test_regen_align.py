"""regen alignment, landmarks and failure modes on weird inputs."""

import numpy as np
import pytest

pytest.importorskip("scipy")
trimesh = pytest.importorskip("trimesh")

from regen import DecompositionError, RegenConfig  # noqa: E402
from regen.align import Similarity, align_body_to_dressed, umeyama  # noqa: E402
from regen.decompose import decompose  # noqa: E402
from regen.geometry import weld  # noqa: E402
from regen.landmarks import compute_landmarks  # noqa: E402
from regen.mesh_io import save_trimesh_glb  # noqa: E402
from regen.synthetic import make_body, make_pair, write_pair  # noqa: E402


def test_umeyama_recovers_an_exact_similarity():
    rng = np.random.default_rng(0)
    src = rng.normal(size=(200, 3))
    angle = 0.3
    rot = np.array([[np.cos(angle), 0, np.sin(angle)], [0, 1, 0], [-np.sin(angle), 0, np.cos(angle)]])
    dst = 1.7 * src @ rot.T + [0.1, -2.0, 3.0]
    s, r, t = umeyama(src, dst)
    assert s == pytest.approx(1.7)
    assert np.allclose(r, rot)
    assert np.allclose(t, [0.1, -2.0, 3.0])
    sim = Similarity.from_srt(s, r, t)
    assert np.allclose(sim.inverse().apply(sim.apply(src)), src)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_alignment_recovers_random_similarities(seed):
    pair = make_pair(seed=seed, resolution=0.7, cape=seed == 3)
    body = weld(pair.body_moved.vertices, pair.body_moved.faces)
    dressed = weld(pair.dressed.vertices, pair.dressed.faces)
    sim = align_body_to_dressed(body, dressed, config=RegenConfig())
    true = pair.body_to_dressed
    true_scale = float(np.cbrt(np.linalg.det(true[:3, :3])))
    assert abs(sim.scale / true_scale - 1.0) < 0.02
    pts = np.asarray(pair.body_moved.vertices)
    err = np.linalg.norm(pts @ true[:3, :3].T + true[:3, 3] - sim.apply(pts), axis=1)
    assert err.mean() < 0.01  # height is 1
    assert np.linalg.norm((pts @ true[:3, :3].T + true[:3, 3]).mean(0) - sim.apply(pts).mean(0)) < 0.01
    assert sim.rotation_deg < 5.0


def test_landmarks_on_the_true_body():
    body = make_body(0.8)
    lm = compute_landmarks(np.asarray(body.vertices), np.asarray(body.faces))
    assert lm.height == pytest.approx(1.0, abs=0.01)
    assert lm.facing == 1 and lm.facing_confident
    assert lm.neck[1] == pytest.approx(0.8, abs=0.03)
    assert lm.head_radius == pytest.approx(0.1, abs=0.015)
    assert lm.crotch_y == pytest.approx(0.435, abs=0.03)
    assert lm.arm_span == pytest.approx(1.06, abs=0.03)
    assert lm.side(0.3) == "l" and lm.side(-0.3) == "r"


def test_landmarks_follow_a_character_facing_minus_z():
    body = make_body(0.8)
    flipped = np.asarray(body.vertices) * [-1.0, 1.0, -1.0]  # turned 180 degrees around Y
    lm = compute_landmarks(flipped, np.asarray(body.faces))
    assert lm.facing == -1
    assert lm.side(-0.3) == "l"
    assert lm.shoulder_l[0] < 0.0


# ------------------------------------------------------------------ failures


def _write(path, mesh):
    save_trimesh_glb(path, [("mesh", mesh)])
    return path


def test_cube_is_not_a_character(tmp_path):
    cube = trimesh.creation.box(extents=(1.0, 1.0, 1.0)).subdivide().subdivide()
    a = _write(tmp_path / "a.glb", cube)
    b = _write(tmp_path / "b.glb", cube)
    with pytest.raises(DecompositionError, match="T-pose"):
        decompose(a, b, tmp_path / "out")


def test_sphere_against_a_character_fails_cleanly(tmp_path):
    pair = make_pair(seed=1, resolution=0.6)
    dressed, _ = write_pair(pair, tmp_path)
    sphere = _write(tmp_path / "sphere.glb", trimesh.creation.icosphere(3))
    with pytest.raises(DecompositionError):
        decompose(dressed, sphere, tmp_path / "out")


def test_a_pose_is_rejected(tmp_path):
    body = make_body(0.6, arm_angle_deg=50.0)
    a = _write(tmp_path / "a.glb", body)
    b = _write(tmp_path / "b.glb", body)
    with pytest.raises(DecompositionError, match="T-pose"):
        decompose(a, b, tmp_path / "out")


def test_mismatched_body_reports_a_high_residual(tmp_path):
    pair = make_pair(seed=4, resolution=0.6)
    dressed, _ = write_pair(pair, tmp_path)
    rng = np.random.default_rng(0)
    noisy = pair.body_moved.copy()
    noisy.vertices = np.asarray(noisy.vertices) + rng.normal(scale=0.06 * pair.scale, size=noisy.vertices.shape)
    body = _write(tmp_path / "noisy.glb", noisy)
    with pytest.raises(DecompositionError, match="alignment residual too high"):
        decompose(dressed, body, tmp_path / "out")


def test_unreadable_and_empty_files(tmp_path):
    garbage = tmp_path / "garbage.glb"
    garbage.write_bytes(b"glTF\x02\x00\x00\x00not really a glb")
    pair = make_pair(seed=1, resolution=0.6)
    dressed, body = write_pair(pair, tmp_path)
    with pytest.raises(DecompositionError):
        decompose(garbage, body, tmp_path / "out")
    text = tmp_path / "mesh.txt"
    text.write_text("hello")
    with pytest.raises(DecompositionError, match="unsupported"):
        decompose(dressed, text, tmp_path / "out")
    lines = trimesh.load_path(np.array([[0, 0, 0], [0, 1, 0], [1, 1, 0]], dtype=float))
    only_lines = tmp_path / "lines.glb"
    only_lines.write_bytes(trimesh.Scene(lines).export(file_type="glb"))
    with pytest.raises(DecompositionError, match="no triangles"):
        decompose(dressed, only_lines, tmp_path / "out")
