"""End-to-end AutoRig Regen decomposition on synthetic mannequins.

A: base body + tight shirt flowing into a flared skirt + hair cap + ponytail
(+ an optional cape). B: the base body moved by a random similarity. See
regen/synthetic.py.
"""

import json
import math

import numpy as np
import pytest

pytest.importorskip("scipy")
trimesh = pytest.importorskip("trimesh")

from scipy.spatial import cKDTree  # noqa: E402

from regen import DecompositionError, RegenConfig  # noqa: E402
from regen.decompose import BUILTIN_PRESETS, decompose  # noqa: E402
from regen.synthetic import make_pair, write_pair  # noqa: E402

HEIGHT = 1.0  # synthetic characters are 1 unit tall in dressed space


def _run(tmp_path_factory, name, **pair_kwargs):
    pair = make_pair(**pair_kwargs)
    root = tmp_path_factory.mktemp(name)
    dressed, body = write_pair(pair, root, textured=True)
    result = decompose(dressed, body, root / "out")
    labels = np.load(result.labels_path)
    # map every labels.npz row back to its ground-truth tag by position
    dist, idx = cKDTree(np.asarray(pair.dressed.vertices)).query(labels["positions"])
    assert dist.max() < 1e-5
    tags = pair.dressed_tags[idx]
    return pair, result, labels, tags


@pytest.fixture(scope="module")
def dressed_run(tmp_path_factory):
    return _run(tmp_path_factory, "regen_main", seed=7)


@pytest.fixture(scope="module")
def caped_run(tmp_path_factory):
    return _run(tmp_path_factory, "regen_cape", seed=11, cape=True)


def _share(labels, tags, pair, label, *tag_names):
    mask = np.isin(tags, [pair_tag(pair, n) for n in tag_names])
    assert mask.any()
    return float(np.mean(labels["labels"][mask] == label))


def pair_tag(pair, name):
    from regen.synthetic import TAGS

    return TAGS.index(name)


def _group(doc, name):
    for group in doc["groups"]:
        if group["name"] == name:
            return group
    raise AssertionError(f"no group {name!r} in {[g['name'] for g in doc['groups']]}")


def _cloth_part_by_tag(result, labels, tags, pair, tag):
    """Name of the cloth part holding most vertices of a ground-truth tag."""
    mask = tags == pair_tag(pair, tag)
    parts = labels["part_index"][mask]
    parts = parts[parts >= 0]
    assert len(parts), f"no {tag} vertex belongs to a part"
    index = int(np.bincount(parts).argmax())
    return result.document["parts"][index]["name"]


# --------------------------------------------------------------- alignment


def test_alignment_recovers_the_random_similarity(dressed_run):
    pair, result, _, _ = dressed_run
    sim = result.similarity
    true = pair.body_to_dressed
    true_scale = float(np.cbrt(np.linalg.det(true[:3, :3])))
    assert abs(sim.scale / true_scale - 1.0) < 0.02
    body = np.asarray(pair.body_moved.vertices)
    expected = body @ true[:3, :3].T + true[:3, 3]
    got = sim.apply(body)
    # translation: where the body's centre lands, and every vertex on average
    assert np.linalg.norm(expected.mean(axis=0) - got.mean(axis=0)) < 0.01 * HEIGHT
    assert np.linalg.norm(expected - got, axis=1).mean() < 0.01 * HEIGHT
    matrix = np.asarray(result.document["body_transform"], dtype=float).reshape(4, 4)
    assert np.allclose(matrix, sim.matrix)
    assert result.document["diagnostics"]["align_rms_relative"] < 0.02


def test_landmarks_match_the_mannequin(dressed_run):
    _, result, _, _ = dressed_run
    lm = result.document["landmarks"]
    assert lm["height"] == pytest.approx(1.0, abs=0.02)
    assert lm["ground_y"] == pytest.approx(0.0, abs=0.02)
    assert lm["neck"][1] == pytest.approx(0.8, abs=0.03)
    assert lm["head_center"][1] == pytest.approx(0.9, abs=0.03)
    assert lm["head_radius"] == pytest.approx(0.1, abs=0.02)
    assert 0.42 < lm["hips"][1] < 0.56
    assert lm["shoulder_l"][0] > 0.08 and lm["shoulder_r"][0] < -0.08  # faces +Z: left is +X
    assert lm["front_axis"] == "+Z"


# ------------------------------------------------------------ segmentation


def test_labels_match_ground_truth(dressed_run):
    pair, _, labels, tags = dressed_run
    assert _share(labels, tags, pair, 2, "skirt") >= 0.90
    assert _share(labels, tags, pair, 1, "hair_cap", "ponytail") >= 0.90
    assert _share(labels, tags, pair, 1, "ponytail") >= 0.85
    assert _share(labels, tags, pair, 0, "shirt", "skin") >= 0.95


def test_labels_npz_layout(dressed_run):
    pair, result, labels, _ = dressed_run
    n = len(pair.dressed.vertices)
    assert labels["positions"].dtype == np.float32 and labels["positions"].shape == (n, 3)
    assert labels["labels"].dtype == np.uint8 and labels["labels"].shape == (n,)
    assert labels["part_index"].dtype == np.int16 and labels["part_index"].shape == (n,)
    assert set(np.unique(labels["labels"])) <= {0, 1, 2}
    outer = labels["labels"] == 0
    assert np.all(labels["part_index"][outer] == -1)
    assert np.all(labels["part_index"][~outer] >= 0)
    counts = result.document["diagnostics"]["label_counts"]
    assert sum(counts.values()) == n


# ------------------------------------------------------------------ chains


def test_skirt_is_a_loop_of_equal_chains_on_the_hips(dressed_run):
    pair, result, labels, tags = dressed_run
    doc = result.document
    skirt = _group(doc, _cloth_part_by_tag(result, labels, tags, pair, "skirt"))
    assert skirt["connection"] == "loop"
    assert skirt["attach"] == "hips"
    assert skirt["kind"] == "cloth"
    assert skirt["preset"] == "skirt"
    k = RegenConfig().loop_sectors
    assert len(skirt["chains"]) == k
    joints = {len(c["joints"]) for c in skirt["chains"]}
    assert joints == {RegenConfig().joints_per_chain + 1}
    lengths = np.array([c["length"] for c in skirt["chains"]])
    assert lengths.max() / lengths.min() < 1.15  # a symmetric skirt hangs evenly


def test_hair_is_independent_strands_on_the_head(dressed_run):
    _, result, _, _ = dressed_run
    doc = result.document
    hair = _group(doc, "hair")
    assert hair["connection"] == "none"
    assert hair["attach"] == "head"
    assert hair["kind"] == "hair"
    assert hair["preset"] in ("hair", "hair_stiff")
    assert hair["chains"], "the ponytail must get a chain"
    part = next(p for p in doc["parts"] if p["name"] == "hair")
    assert part["rigid"] is False
    # the ponytail hangs at the back: its chain points away from the face
    root = np.asarray(hair["chains"][0]["joints"][0])
    end = np.asarray(hair["chains"][0]["joints"][-1])
    assert end[1] < root[1] - 0.2
    assert end[2] < -0.1


def _angles(group, facing=1):
    ax, az = group["axis"]
    roots = np.array([c["joints"][0] for c in group["chains"]])
    return np.degrees(np.arctan2(facing * (roots[:, 0] - ax), facing * (roots[:, 2] - az))) % 360.0


def test_chains_are_listed_in_angular_order(dressed_run, caped_run):
    for _, result, _, _ in (dressed_run, caped_run):
        for group in result.document["groups"]:
            if group["connection"] == "none" or len(group["chains"]) < 2:
                continue
            steps = np.diff(_angles(group)) % 360.0
            if group["connection"] == "loop":
                steps = np.append(steps, (_angles(group)[0] - _angles(group)[-1]) % 360.0)
                assert np.all(steps > 0.0) and np.all(steps < 180.0)
                assert np.sum(steps) == pytest.approx(360.0, abs=1e-6)
            else:
                assert np.all(steps > 0.0) and np.sum(steps) < 360.0


def test_joints_lie_inside_or_near_their_part(dressed_run, caped_run):
    for _, result, _, _ in (dressed_run, caped_run):
        scene = trimesh.load_scene(str(result.parts_glb_path))
        for group in result.document["groups"]:
            surface = scene.geometry[group["part"]]
            samples, _ = trimesh.sample.sample_surface(surface, 50_000, seed=1)
            tree = cKDTree(samples)
            for chain in group["chains"]:
                joints = np.asarray(chain["joints"])
                d, _ = tree.query(joints)
                assert d.max() < 0.04 * HEIGHT, (group["name"], d.max())
                assert np.all(np.diff(joints[:, 1]) < 0.0)  # root to tip, downward


def test_cape_is_an_open_sheet_on_the_upper_back(caped_run):
    pair, result, labels, tags = caped_run
    doc = result.document
    assert _share(labels, tags, pair, 2, "cape") >= 0.90
    cape = _group(doc, _cloth_part_by_tag(result, labels, tags, pair, "cape"))
    assert cape["connection"] == "open"
    assert cape["preset"] == "cape"
    assert cape["attach"] in ("chest", "spine")
    cfg = RegenConfig()
    assert cfg.open_sectors_min <= len(cape["chains"]) <= cfg.open_sectors_max
    assert len({len(c["joints"]) for c in cape["chains"]}) == 1
    skirt = _group(doc, _cloth_part_by_tag(result, labels, tags, pair, "skirt"))
    assert skirt["connection"] == "loop" and skirt["attach"] == "hips"
    assert _group(doc, "hair")["attach"] == "head"


# ----------------------------------------------------------------- weights


def test_weights_are_normalised_and_sparse(dressed_run, caped_run):
    for _, result, labels, _ in (dressed_run, caped_run):
        weights = np.load(result.weights_path)
        for group in result.document["groups"]:
            name = group["name"]
            ids = weights[f"{name}__bone_ids"]
            w = weights[f"{name}__weights"]
            verts = weights[f"{name}__vertices"]
            assert ids.dtype == np.int32 and w.dtype == np.float32
            assert ids.shape == w.shape and ids.shape[1] == 4
            assert np.all(w >= 0.0)
            assert np.allclose(w.sum(axis=1), 1.0, atol=1e-5)
            assert np.all((w > 0).sum(axis=1) <= 4)
            n_bones = 1 + sum(len(c["joints"]) - 1 for c in group["chains"])
            assert ids.max() < n_bones
            # every weighted vertex belongs to the group's part
            index = [p["name"] for p in result.document["parts"]].index(group["part"])
            assert np.all(labels["part_index"][verts] == index)


# ----------------------------------------------------------------- outputs


def test_output_files_exist(dressed_run):
    _, result, _, _ = dressed_run
    for path in (result.decomposition_path, result.labels_path, result.weights_path, result.parts_glb_path):
        assert path.is_file() and path.stat().st_size > 0


def test_parts_glb_reloads_with_named_layers(dressed_run):
    _, result, _, _ = dressed_run
    scene = trimesh.load_scene(str(result.parts_glb_path))
    names = set(scene.geometry)
    assert {"outer", "hair", "cloth_0", "body_inner"} <= names
    assert any(n.startswith("chain_") for n in names)
    assert "debug_chains" in scene.graph.nodes
    parents = scene.graph.transforms.parents
    for name in names:
        if name.startswith(("chain_", "joints_")):
            assert parents[name] == "debug_chains"
    # A's texture survives into the parts
    hair = scene.geometry["hair"]
    assert hair.visual.uv is not None and len(hair.visual.uv) == len(hair.vertices)
    assert hair.visual.material.baseColorTexture is not None
    # the parts partition the dressed faces
    dressed_faces = sum(len(scene.geometry[n].faces) for n in names if n in ("outer", "hair") or n.startswith("cloth_"))
    assert dressed_faces == result.document["inputs"]["dressed_faces"]


def test_body_inner_is_the_aligned_body_pulled_inward(dressed_run):
    pair, result, _, _ = dressed_run
    scene = trimesh.load_scene(str(result.parts_glb_path))
    inner = np.asarray(scene.geometry["body_inner"].vertices)
    true_body = pair.body  # B in dressed space, same vertex order as B.glb
    assert inner.shape == true_body.vertices.shape
    # every vertex moved inward along the true surface normal
    along = np.einsum("ij,ij->i", inner - true_body.vertices, true_body.vertex_normals)
    assert np.mean(along < 0.0) > 0.99
    assert np.all(np.linalg.norm(inner - true_body.vertices, axis=1) < 0.04 * HEIGHT)
    # under the tight shirt (1% of the height) the body stays inside the shirt
    shirt = pair.tag_mask("shirt")
    shirt_faces = np.all(shirt[np.asarray(pair.dressed.faces)], axis=1)
    shirt_mesh = trimesh.Trimesh(pair.dressed.vertices, np.asarray(pair.dressed.faces)[shirt_faces], process=False)
    samples, face = trimesh.sample.sample_surface(shirt_mesh, 100_000, seed=2)
    normals = shirt_mesh.face_normals[face]
    torso = (inner[:, 1] > 0.58) & (inner[:, 1] < 0.7) & (np.abs(inner[:, 0]) < 0.1)
    assert torso.sum() > 50
    _, idx = cKDTree(samples).query(inner[torso])
    outside = np.einsum("ij,ij->i", inner[torso] - samples[idx], normals[idx])
    assert np.all(outside < 0.0)


def test_decomposition_json_shape(dressed_run):
    _, result, _, _ = dressed_run
    doc = json.loads(result.decomposition_path.read_text(encoding="utf-8"))
    assert doc["format"] == "autorig.regen.decomposition"
    assert doc["version"] == 1
    assert doc["space"] == "dressed_glb"
    assert isinstance(doc["height"], float) and doc["height"] > 0
    assert len(doc["body_transform"]) == 16 and all(isinstance(v, float) for v in doc["body_transform"])
    assert doc["labels"] == {"values": ["outer", "hair", "cloth"], "file": "labels.npz", "count": doc["labels"]["count"]}
    part_keys = {"name", "kind", "vertex_count", "bbox_min", "bbox_max", "attach", "rigid"}
    for part in doc["parts"]:
        assert part_keys <= set(part)
        assert part["kind"] in ("hair", "cloth")
        assert len(part["bbox_min"]) == 3 and len(part["bbox_max"]) == 3
        assert isinstance(part["rigid"], bool) and part["vertex_count"] > 0
    group_keys = {"name", "kind", "part", "attach", "connection", "preset", "chains"}
    part_names = {p["name"] for p in doc["parts"]}
    for group in doc["groups"]:
        assert group_keys <= set(group)
        assert group["part"] in part_names
        assert group["connection"] in ("none", "open", "loop")
        assert group["preset"] in BUILTIN_PRESETS
        for chain in group["chains"]:
            assert len(chain["joints"]) >= 2 and all(len(j) == 3 for j in chain["joints"])
            assert chain["bones"][-1].endswith("_end")
            assert len(chain["bones"]) == len(chain["joints"])
    lm_keys = {"ground_y", "height", "neck", "head_center", "head_radius", "hips", "shoulder_l", "shoulder_r"}
    assert lm_keys <= set(doc["landmarks"])
    for key in ("neck", "head_center", "hips", "shoulder_l", "shoulder_r"):
        assert len(doc["landmarks"][key]) == 3
    diag = doc["diagnostics"]
    assert {"align_rms", "align_inlier_ratio", "label_counts"} <= set(diag)
    assert set(diag["label_counts"]) == {"outer", "hair", "cloth"}
    assert math.isfinite(diag["align_rms"]) and 0.0 < diag["align_inlier_ratio"] <= 1.0


def test_short_hair_stays_rigid(tmp_path):
    pair = make_pair(seed=5, ponytail=False, resolution=0.7)
    dressed, body = write_pair(pair, tmp_path)
    result = decompose(dressed, body, tmp_path / "out")
    hair = next(p for p in result.document["parts"] if p["name"] == "hair")
    assert hair["rigid"] is True
    assert all(g["part"] != "hair" for g in result.document["groups"])
    assert any(g["connection"] == "loop" for g in result.document["groups"])


def test_decompose_raises_decomposition_error_not_other_exceptions(tmp_path):
    with pytest.raises(DecompositionError):
        decompose(tmp_path / "missing.glb", tmp_path / "also-missing.glb", tmp_path / "out")
