"""regen.mesh_io: flattening scene graphs and re-exporting textured parts."""

import numpy as np
import pytest

trimesh = pytest.importorskip("trimesh")
pytest.importorskip("PIL")

from PIL import Image  # noqa: E402
from trimesh.visual import TextureVisuals  # noqa: E402
from trimesh.visual.material import PBRMaterial  # noqa: E402

from regen import MeshLoadError  # noqa: E402
from regen.mesh_io import DebugPoints, DebugPolyline, export_layered_glb, load_mesh  # noqa: E402


def _textured(mesh, colour, seed):
    rng = np.random.default_rng(seed)
    image = Image.new("RGB", (8, 8), colour)
    material = PBRMaterial(name=f"mat{seed}", baseColorTexture=image)
    uv = rng.random((len(mesh.vertices), 2))
    return trimesh.Trimesh(mesh.vertices, mesh.faces, visual=TextureVisuals(uv=uv, material=material), process=False)


def _scene(tmp_path):
    box = _textured(trimesh.creation.box(extents=(1, 2, 3)), (255, 0, 0), 1)
    ball = _textured(trimesh.creation.icosphere(2), (0, 0, 255), 2)
    rot = trimesh.transformations.rotation_matrix(0.4, [0, 1, 0])
    rot[:3, 3] = [5.0, 1.0, -2.0]
    mirror = np.diag([-1.0, 1.0, 1.0, 1.0])
    mirror[:3, 3] = [0.0, 3.0, 0.0]
    scene = trimesh.Scene()
    scene.add_geometry(box, geom_name="box", node_name="box_node", transform=rot)
    scene.add_geometry(ball, geom_name="ball", node_name="ball_node", transform=mirror)
    path = tmp_path / "scene.glb"
    path.write_bytes(scene.export(file_type="glb"))
    return path, (box, rot), (ball, mirror)


def test_flatten_applies_node_transforms_and_keeps_materials(tmp_path):
    path, (box, rot), (ball, mirror) = _scene(tmp_path)
    mesh = load_mesh(path)
    assert mesh.n_vertices == len(box.vertices) + len(ball.vertices)
    assert mesh.n_faces == len(box.faces) + len(ball.faces)
    assert len(mesh.materials) == 2
    assert set(np.unique(mesh.face_material)) == {0, 1}
    assert mesh.uv is not None and mesh.uv.shape == (mesh.n_vertices, 2)
    expected_box = trimesh.transform_points(box.vertices, rot)
    expected_ball = trimesh.transform_points(ball.vertices, mirror)
    got = mesh.vertices
    for expected in (expected_box, expected_ball):
        d = np.min(np.linalg.norm(got[:, None, :] - expected[None, :8, :], axis=2), axis=0)
        assert d.max() < 1e-5
    # the mirrored instance (the ball, 320 faces) keeps outward-facing triangles
    flat = trimesh.Trimesh(mesh.vertices, mesh.faces, process=False)
    counts = np.bincount(mesh.face_material)
    ball_faces = mesh.face_material == int(np.flatnonzero(counts == len(ball.faces))[0])
    centre = mesh.vertices[mesh.faces[ball_faces]].mean(axis=(0, 1))
    outward = np.einsum(
        "ij,ij->i", flat.face_normals[ball_faces], flat.triangles_center[ball_faces] - centre
    )
    assert np.all(outward > 0)


def test_layered_export_round_trip(tmp_path):
    path, _, _ = _scene(tmp_path)
    mesh = load_mesh(path)
    first = mesh.face_material == 0  # flatten order follows the reloaded scene graph
    first_colour = mesh.materials[0].baseColorTexture.getpixel((0, 0))[:3]
    assert first_colour in ((255, 0, 0), (0, 0, 255))
    out = tmp_path / "layers.glb"
    names = export_layered_glb(
        out,
        [("first", mesh, first), ("second", mesh, ~first), ("both", mesh, np.ones(mesh.n_faces, bool))],
        debug_lines=[DebugPolyline("line", np.array([[0, 0, 0], [0, 1, 0], [0, 2, 1]], dtype=float))],
        debug_points=[DebugPoints("pts", np.eye(3))],
    )
    assert names == ["first", "second", "both", "both__mat1", "line", "pts"]
    scene = trimesh.load_scene(str(out))
    assert {"first", "second", "both", "both__mat1", "line", "pts"} <= set(scene.geometry)
    assert scene.graph.transforms.parents["line"] == "debug_chains"
    assert scene.graph.transforms.parents["both__mat1"] == "both"
    first_mesh = scene.geometry["first"]
    assert len(first_mesh.faces) == int(first.sum())
    assert first_mesh.visual.uv is not None
    assert first_mesh.visual.material.baseColorTexture.getpixel((0, 0))[:3] == first_colour


def test_obj_and_errors(tmp_path):
    obj = tmp_path / "ball.obj"
    obj.write_text(trimesh.exchange.obj.export_obj(trimesh.creation.icosphere(2)))
    mesh = load_mesh(obj)
    assert mesh.n_faces == 320
    with pytest.raises(MeshLoadError, match="not found"):
        load_mesh(tmp_path / "nope.glb")
    bad = tmp_path / "bad.glb"
    bad.write_bytes(b"not a glb at all")
    with pytest.raises(MeshLoadError):
        load_mesh(bad)
