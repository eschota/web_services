"""The optional front-view hair mask overrides the geometric hair heuristic
where it can see the surface, and the CLI accepts it as an image."""

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("scipy")
pytest.importorskip("trimesh")
Image = pytest.importorskip("PIL.Image")

from scipy import ndimage  # noqa: E402
from scipy.spatial import cKDTree  # noqa: E402

from regen.decompose import decompose  # noqa: E402
from regen.segment import HairMask  # noqa: E402
from regen.synthetic import TAGS, make_pair, write_pair  # noqa: E402

BACKEND = Path(__file__).resolve().parents[1]


def front_view_hair_mask(pair, size=160):
    """Ground-truth front view: the front-most tag at each pixel, hair = True.
    The dressed mesh's X/Y bounds span the whole image."""
    v = np.asarray(pair.dressed.vertices)
    lo, hi = v.min(axis=0), v.max(axis=0)
    cols = np.rint((v[:, 0] - lo[0]) / (hi[0] - lo[0]) * (size - 1)).astype(int)
    rows = np.rint((hi[1] - v[:, 1]) / (hi[1] - lo[1]) * (size - 1)).astype(int)
    order = np.argsort(v[:, 2])  # back to front: the front-most vertex wins
    tag_img = np.full((size, size), -1)
    tag_img[rows[order], cols[order]] = pair.dressed_tags[order]
    hair = np.isin(tag_img, [TAGS.index("hair_cap"), TAGS.index("ponytail")])
    return ndimage.binary_closing(hair, iterations=2)


@pytest.fixture(scope="module")
def pair_and_files(tmp_path_factory):
    pair = make_pair(seed=21, resolution=0.8)
    root = tmp_path_factory.mktemp("regen_mask")
    dressed, body = write_pair(pair, root)
    return pair, dressed, body, root


def _labels_by_tag(pair, result):
    labels = np.load(result.labels_path)
    _, idx = cKDTree(np.asarray(pair.dressed.vertices)).query(labels["positions"])
    return labels, pair.dressed_tags[idx]


def test_ground_truth_mask_keeps_the_hair(pair_and_files):
    pair, dressed, body, root = pair_and_files
    mask = front_view_hair_mask(pair)
    result = decompose(dressed, body, root / "with_mask", hair_mask=HairMask(mask))
    assert result.document["diagnostics"]["hair_source"] == "mask"
    labels, tags = _labels_by_tag(pair, result)
    hair = np.isin(tags, [TAGS.index("hair_cap"), TAGS.index("ponytail")])
    skin = np.isin(tags, [TAGS.index("skin"), TAGS.index("shirt")])
    assert np.mean(labels["labels"][hair] == 1) >= 0.90
    assert np.mean(labels["labels"][skin] == 0) >= 0.95
    assert any(g["name"] == "hair" and g["chains"] for g in result.document["groups"])


def test_empty_mask_overrides_only_what_the_front_view_sees(pair_and_files):
    pair, dressed, body, root = pair_and_files
    empty = np.zeros((160, 160), dtype=bool)
    result = decompose(dressed, body, root / "empty_mask", hair_mask=empty)
    labels, tags = _labels_by_tag(pair, result)
    cap = tags == TAGS.index("hair_cap")
    front_cap = cap & (labels["positions"][:, 2] > 0.06)
    back_cap = cap & (labels["positions"][:, 2] < -0.06)
    pony = tags == TAGS.index("ponytail")
    assert np.mean(labels["labels"][front_cap] == 1) < 0.2  # the mask says: no hair here
    assert np.mean(labels["labels"][back_cap] == 1) > 0.8  # hidden from the front: heuristic kept
    assert np.mean(labels["labels"][pony] == 1) > 0.8


def _cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "regen.cli", *map(str, args)],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=str(BACKEND),
    )


def test_cli_decompose_with_a_mask_image(pair_and_files):
    pair, dressed, body, root = pair_and_files
    png = root / "mask.png"
    Image.fromarray((front_view_hair_mask(pair) * 255).astype(np.uint8)).save(png)
    out = root / "cli_out"
    proc = _cli("decompose", "--dressed", dressed, "--body", body, "--out", out, "--hair-mask", png)
    assert proc.returncode == 0, proc.stderr
    summary = json.loads(proc.stdout)
    assert summary["ok"] is True
    assert (out / "decomposition.json").is_file()
    doc = json.loads((out / "decomposition.json").read_text())
    assert doc["diagnostics"]["hair_source"] == "mask"
    assert {g["name"] for g in summary["groups"]} >= {"hair", "cloth_0"}


def test_cli_reports_decomposition_errors_with_exit_code_2(tmp_path):
    import trimesh

    from regen.mesh_io import save_trimesh_glb

    cube = tmp_path / "cube.glb"
    save_trimesh_glb(cube, [("cube", trimesh.creation.box().subdivide().subdivide())])
    proc = _cli("decompose", "--dressed", cube, "--body", cube, "--out", tmp_path / "out")
    assert proc.returncode == 2
    assert "decomposition failed" in proc.stderr and "T-pose" in proc.stderr
