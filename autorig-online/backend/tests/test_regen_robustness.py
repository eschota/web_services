"""Inputs shaped like real Hunyuan output: every vertex split along UV seams,
a different normalisation scale, a character facing -Z, and footwear that is
looser than the bare-footed base body."""

import math

import numpy as np
import pytest

pytest.importorskip("scipy")
trimesh = pytest.importorskip("trimesh")

from scipy.spatial import cKDTree  # noqa: E402

from regen.decompose import decompose  # noqa: E402
from regen.mesh_io import save_trimesh_glb  # noqa: E402
from regen.synthetic import TAGS, make_pair, random_similarity  # noqa: E402

SCALE = 1.7


def _global(points):
    """Turn 180 degrees about Y, scale by SCALE, move."""
    rot = np.array([[-1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, -1.0]])
    return SCALE * np.asarray(points) @ rot.T + np.array([0.3, -0.2, 0.5])


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    root = tmp_path_factory.mktemp("regen_robust")
    pair = make_pair(seed=9, resolution=0.8, boots=True, move_body=False)
    faces = np.asarray(pair.dressed.faces)
    # every face gets its own three vertices: the worst case of UV-seam splits
    corner = faces.reshape(-1)
    dressed = trimesh.Trimesh(
        _global(np.asarray(pair.dressed.vertices)[corner]), np.arange(len(corner)).reshape(-1, 3), process=False
    )
    tags = pair.dressed_tags[corner]
    s, r, t, _ = random_similarity(np.random.default_rng(99))
    body = trimesh.Trimesh(s * _global(pair.body.vertices) @ r.T + t, np.asarray(pair.body.faces), process=False)
    save_trimesh_glb(root / "dressed.glb", [("dressed", dressed)])
    save_trimesh_glb(root / "body.glb", [("body", body)])
    result = decompose(root / "dressed.glb", root / "body.glb", root / "out")
    labels = np.load(result.labels_path)
    _, idx = cKDTree(np.asarray(dressed.vertices)).query(labels["positions"])
    return result, labels, tags[idx]


def _share(labels, tags, label, *names):
    mask = np.isin(tags, [TAGS.index(n) for n in names])
    return float(np.mean(labels["labels"][mask] == label))


def test_seams_are_welded_for_topology(run):
    result, _, _ = run
    inputs = result.document["inputs"]
    assert inputs["dressed_welded_vertices"] < 0.3 * inputs["dressed_vertices"]


def test_scale_and_facing_are_measured(run):
    result, _, _ = run
    lm = result.document["landmarks"]
    assert lm["height"] == pytest.approx(SCALE, rel=0.02)
    assert lm["front_axis"] == "-Z" and lm["left_axis"] == "-X"
    assert result.document["height"] == pytest.approx(SCALE, rel=0.02)


def test_labels_survive_seams_scale_and_facing(run):
    _, labels, tags = run
    assert _share(labels, tags, 2, "skirt") >= 0.90
    assert _share(labels, tags, 1, "hair_cap", "ponytail") >= 0.90
    assert _share(labels, tags, 0, "shirt", "skin") >= 0.95


def test_groups_survive_seams_scale_and_facing(run):
    result, _, _ = run
    groups = {g["name"]: g for g in result.document["groups"]}
    assert groups["hair"]["connection"] == "none" and groups["hair"]["attach"] == "head"
    loops = [g for g in groups.values() if g["connection"] == "loop"]
    assert len(loops) == 1 and loops[0]["attach"] == "hips" and len(loops[0]["chains"]) == 8
    # chain lengths scale with the character
    assert np.mean([c["length"] for c in loops[0]["chains"]]) == pytest.approx(0.27 * SCALE, rel=0.1)
    # angular order is measured from the character's front toward its left,
    # which for a character facing -Z means from -Z toward -X
    ax, az = loops[0]["axis"]
    roots = np.array([c["joints"][0] for c in loops[0]["chains"]])
    angles = np.degrees(np.arctan2(-(roots[:, 0] - ax), -(roots[:, 2] - az))) % 360.0
    steps = np.diff(np.append(angles, angles[0])) % 360.0
    assert np.all((steps > 20) & (steps < 70))
    assert angles[0] < 25 or angles[0] > 335  # the first chain hangs at the front
    assert roots[0][2] < az  # ... which is -Z here
    assert roots[2][0] < ax  # the third chain (90 degrees) is on the character's left: -X


def test_boots_are_rigid_and_sided(run):
    result, labels, tags = run
    boot = tags == TAGS.index("boot")
    part_ids = np.unique(labels["part_index"][boot & (labels["part_index"] >= 0)])
    assert len(part_ids) >= 2
    parts = result.document["parts"]
    boot_parts = []
    for pid in part_ids:
        share = np.mean(tags[labels["part_index"] == pid] == TAGS.index("boot"))
        if share > 0.9:
            boot_parts.append(parts[pid])
    assert len(boot_parts) == 2
    grouped = {g["part"] for g in result.document["groups"]}
    sides = set()
    for part in boot_parts:
        assert part["rigid"] is True and part["name"] not in grouped
        assert part["attach"] in ("upper_leg_l", "upper_leg_r")
        sides.add(part["attach"])
        # the character faces -Z, so its left is -X: the left boot has the smaller X
        centre_x = 0.5 * (part["bbox_min"][0] + part["bbox_max"][0])
        expect = "upper_leg_l" if centre_x < result.document["landmarks"]["center"][0] else "upper_leg_r"
        assert part["attach"] == expect
    assert sides == {"upper_leg_l", "upper_leg_r"}
    assert not math.isnan(result.document["diagnostics"]["align_rms"])


def test_nothing_loose_means_no_parts(tmp_path):
    from regen.synthetic import make_body

    body = make_body(0.6)
    save_trimesh_glb(tmp_path / "a.glb", [("a", body)])
    save_trimesh_glb(tmp_path / "b.glb", [("b", body)])
    result = decompose(tmp_path / "a.glb", tmp_path / "b.glb", tmp_path / "out")
    assert result.document["parts"] == [] and result.document["groups"] == []
    counts = result.document["diagnostics"]["label_counts"]
    assert counts["hair"] == 0 and counts["cloth"] == 0
    for name in ("decomposition.json", "labels.npz", "weights.npz", "parts.glb"):
        assert (tmp_path / "out" / name).is_file()


def test_a_failed_run_leaves_no_stale_result(tmp_path):
    from regen import DecompositionError

    out = tmp_path / "out"
    out.mkdir()
    (out / "decomposition.json").write_text('{"stale": true}')
    (out / "labels.npz").write_bytes(b"old")
    cube = trimesh.creation.box().subdivide().subdivide()
    save_trimesh_glb(tmp_path / "cube.glb", [("cube", cube)])
    with pytest.raises(DecompositionError):
        decompose(tmp_path / "cube.glb", tmp_path / "cube.glb", out)
    assert not (out / "decomposition.json").exists()
    assert not (out / "labels.npz").exists()
