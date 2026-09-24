"""Load and export meshes for AutoRig Regen.

``load_mesh`` reads GLB / glTF / OBJ with trimesh and flattens the scene
graph: every mesh instance is transformed into scene space and concatenated
into one :class:`MeshData`, keeping per-face material ids, per-vertex UVs and
vertex colours so any subset of faces can be re-exported with its texture.

``export_layered_glb`` writes several named meshes (plus optional debug lines
and points under a named group node) into one GLB.

Coordinates are left exactly as the file stores them: glTF is Y-up, and a
Hunyuan3D GLB faces +Z.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .errors import MeshLoadError

SUPPORTED_SUFFIXES = (".glb", ".gltf", ".obj")


@dataclasses.dataclass
class MeshData:
    """One triangle mesh flattened from a scene."""

    vertices: np.ndarray  # (V, 3) float64, scene space
    faces: np.ndarray  # (F, 3) int64
    face_material: np.ndarray  # (F,) int32 index into ``materials``; -1 = none
    materials: List[Any]  # trimesh material objects
    uv: Optional[np.ndarray] = None  # (V, 2) float64; rows without UVs are 0
    vertex_colors: Optional[np.ndarray] = None  # (V, 4) uint8
    material_has_uv: Optional[np.ndarray] = None  # (len(materials),) bool
    primitive_names: List[str] = dataclasses.field(default_factory=list)
    source: str = ""

    @property
    def n_vertices(self) -> int:
        return int(len(self.vertices))

    @property
    def n_faces(self) -> int:
        return int(len(self.faces))

    def bounds(self) -> Tuple[np.ndarray, np.ndarray]:
        return self.vertices.min(axis=0), self.vertices.max(axis=0)

    def with_vertices(self, vertices: np.ndarray, flip_faces: bool = False) -> "MeshData":
        """A copy with new vertex positions (same topology, UVs, materials)."""
        faces = self.faces[:, ::-1].copy() if flip_faces else self.faces
        return dataclasses.replace(self, vertices=np.asarray(vertices, dtype=np.float64), faces=faces)


# ----------------------------------------------------------------------- load


def load_mesh(path) -> MeshData:
    """Load a GLB/glTF/OBJ file into one flattened :class:`MeshData`."""
    p = Path(path)
    if not p.is_file():
        raise MeshLoadError(f"mesh file not found: {p}")
    if p.suffix.lower() not in SUPPORTED_SUFFIXES:
        raise MeshLoadError(f"unsupported mesh format {p.suffix!r} ({p.name}); use GLB, glTF or OBJ")
    try:
        import trimesh

        scene = trimesh.load_scene(str(p), process=False)
    except MeshLoadError:
        raise
    except Exception as exc:  # trimesh raises anything from struct.error to KeyError
        raise MeshLoadError(f"cannot read {p.name}: {exc}") from exc
    mesh = flatten_scene(scene, source=str(p))
    if mesh.n_faces == 0:
        raise MeshLoadError(f"{p.name} contains no triangles")
    if not np.all(np.isfinite(mesh.vertices)):
        raise MeshLoadError(f"{p.name} has non-finite vertex coordinates")
    return mesh


def flatten_scene(scene, source: str = "") -> MeshData:
    """Concatenate every triangle mesh instance of a trimesh Scene into scene space."""
    import trimesh
    from trimesh.visual import ColorVisuals, TextureVisuals

    all_vertices: List[np.ndarray] = []
    all_faces: List[np.ndarray] = []
    all_face_mat: List[np.ndarray] = []
    all_uv: List[Optional[np.ndarray]] = []
    all_colors: List[Optional[np.ndarray]] = []
    materials: List[Any] = []
    material_index: Dict[int, int] = {}
    material_has_uv: List[bool] = []
    names: List[str] = []
    offset = 0

    if isinstance(scene, trimesh.Trimesh):
        scene = trimesh.Scene(scene)

    for node in scene.graph.nodes_geometry:
        matrix, geom_name = scene.graph[node]
        geom = scene.geometry.get(geom_name)
        if not isinstance(geom, trimesh.Trimesh) or len(geom.faces) == 0:
            continue  # lines, points, empty meshes
        vertices = np.asarray(geom.vertices, dtype=np.float64)
        faces = np.asarray(geom.faces, dtype=np.int64)
        matrix = np.asarray(matrix, dtype=np.float64)
        if not np.allclose(matrix, np.eye(4)):
            vertices = vertices @ matrix[:3, :3].T + matrix[:3, 3]
            if np.linalg.det(matrix[:3, :3]) < 0.0:
                faces = faces[:, ::-1]

        visual = geom.visual
        material = None
        uv = None
        colors = None
        if isinstance(visual, TextureVisuals):
            material = getattr(visual, "material", None)
            raw_uv = getattr(visual, "uv", None)
            if raw_uv is not None and len(raw_uv) == len(vertices):
                uv = np.asarray(raw_uv, dtype=np.float64)[:, :2]
        elif isinstance(visual, ColorVisuals) and visual.kind in ("vertex", "face"):
            try:
                colors = np.asarray(visual.vertex_colors, dtype=np.uint8)
                if colors.shape != (len(vertices), 4):
                    colors = None
            except Exception:
                colors = None

        mat_id = -1
        if material is not None:
            key = id(material)
            if key not in material_index:
                material_index[key] = len(materials)
                materials.append(material)
                material_has_uv.append(uv is not None)
            mat_id = material_index[key]
            if uv is None:
                material_has_uv[mat_id] = False

        all_vertices.append(vertices)
        all_faces.append(faces + offset)
        all_face_mat.append(np.full(len(faces), mat_id, dtype=np.int32))
        all_uv.append(uv)
        all_colors.append(colors)
        names.append(str(node))
        offset += len(vertices)

    if not all_vertices:
        return MeshData(
            vertices=np.zeros((0, 3)),
            faces=np.zeros((0, 3), dtype=np.int64),
            face_material=np.zeros(0, dtype=np.int32),
            materials=[],
            source=source,
        )

    vertices = np.concatenate(all_vertices)
    uv_out = None
    if any(u is not None for u in all_uv):
        uv_out = np.concatenate(
            [u if u is not None else np.zeros((len(v), 2)) for u, v in zip(all_uv, all_vertices)]
        )
    colors_out = None
    if any(c is not None for c in all_colors):
        colors_out = np.concatenate(
            [
                c if c is not None else np.full((len(v), 4), 255, dtype=np.uint8)
                for c, v in zip(all_colors, all_vertices)
            ]
        )
    return MeshData(
        vertices=vertices,
        faces=np.concatenate(all_faces),
        face_material=np.concatenate(all_face_mat),
        materials=materials,
        uv=uv_out,
        vertex_colors=colors_out,
        material_has_uv=np.asarray(material_has_uv, dtype=bool),
        primitive_names=names,
        source=source,
    )


# --------------------------------------------------------------------- export


def submeshes(mesh: MeshData, face_mask: np.ndarray, keep_materials: bool = True) -> List[Any]:
    """trimesh.Trimesh objects for the selected faces, one per material,
    keeping UVs, materials and vertex colours (``keep_materials=False``:
    plain geometry, one mesh)."""
    import trimesh
    from trimesh.visual import ColorVisuals, TextureVisuals

    face_ids = np.flatnonzero(np.asarray(face_mask, dtype=bool))
    out: List[Any] = []
    if len(face_ids) == 0:
        return out
    mats = mesh.face_material[face_ids] if keep_materials else np.full(len(face_ids), -1, dtype=np.int32)
    for mat_id in np.unique(mats):
        sel = face_ids[mats == mat_id]
        faces = mesh.faces[sel]
        used, local = np.unique(faces.reshape(-1), return_inverse=True)
        local = local.reshape(-1, 3)
        visual = None
        if mat_id >= 0:
            material = mesh.materials[int(mat_id)]
            has_uv = (
                mesh.uv is not None
                and mesh.material_has_uv is not None
                and bool(mesh.material_has_uv[int(mat_id)])
            )
            visual = TextureVisuals(uv=mesh.uv[used] if has_uv else None, material=material)
        elif mesh.vertex_colors is not None and keep_materials:
            visual = ColorVisuals(vertex_colors=mesh.vertex_colors[used])
        out.append(
            trimesh.Trimesh(
                vertices=mesh.vertices[used], faces=local, visual=visual, process=False
            )
        )
    return out


@dataclasses.dataclass
class DebugPolyline:
    name: str
    points: np.ndarray  # (n, 3)
    color: Tuple[int, int, int, int] = (255, 64, 32, 255)


@dataclasses.dataclass
class DebugPoints:
    name: str
    points: np.ndarray  # (n, 3)
    color: Tuple[int, int, int, int] = (32, 200, 255, 255)


def export_layered_glb(
    path,
    layers: Sequence[Tuple[str, MeshData, np.ndarray]],
    *,
    debug_group: str = "debug_chains",
    debug_lines: Iterable[DebugPolyline] = (),
    debug_points: Iterable[DebugPoints] = (),
    debug_meshes: Iterable[Tuple[str, Any]] = (),
    keep_materials: bool = True,
) -> List[str]:
    """Write ``layers`` (name, mesh, face mask) as separately named meshes.

    A layer spanning several materials is written as ``name`` plus
    ``name__mat1``, ``name__mat2``, ... all under a node called ``name``.
    Debug polylines / points / meshes go under an empty node ``debug_group``.
    Returns the geometry names written.
    """
    import trimesh

    scene = trimesh.Scene()
    written: List[str] = []
    base = scene.graph.base_frame
    for name, mesh, mask in layers:
        parts = submeshes(mesh, mask, keep_materials=keep_materials)
        if not parts:
            continue
        scene.add_geometry(parts[0], geom_name=name, node_name=name)
        written.append(name)
        for k, extra in enumerate(parts[1:], start=1):
            sub = f"{name}__mat{k}"
            scene.add_geometry(extra, geom_name=sub, node_name=sub, parent_node_name=name)
            written.append(sub)

    lines = list(debug_lines)
    points = list(debug_points)
    meshes = list(debug_meshes)
    if lines or points or meshes:
        scene.graph.update(frame_from=base, frame_to=debug_group, matrix=np.eye(4))
        for item in lines:
            pts = np.asarray(item.points, dtype=np.float64).reshape(-1, 3)
            if len(pts) < 2:
                continue
            path3d = trimesh.load_path(pts)
            try:
                path3d.colors = np.tile(np.asarray(item.color, dtype=np.uint8), (len(path3d.entities), 1))
            except Exception:
                pass
            scene.add_geometry(path3d, geom_name=item.name, node_name=item.name, parent_node_name=debug_group)
            written.append(item.name)
        for item in points:
            pts = np.asarray(item.points, dtype=np.float64).reshape(-1, 3)
            if len(pts) == 0:
                continue
            cloud = trimesh.PointCloud(pts, colors=np.tile(np.asarray(item.color, dtype=np.uint8), (len(pts), 1)))
            scene.add_geometry(cloud, geom_name=item.name, node_name=item.name, parent_node_name=debug_group)
            written.append(item.name)
        for name, geom in meshes:
            scene.add_geometry(geom, geom_name=name, node_name=name, parent_node_name=debug_group)
            written.append(name)

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    data = scene.export(file_type="glb")
    Path(path).write_bytes(data)
    return written


def save_trimesh_glb(path, meshes: Sequence[Tuple[str, Any]]) -> None:
    """Write plain trimesh meshes as named nodes (used by the synthetic
    generator and tests)."""
    import trimesh

    scene = trimesh.Scene()
    for name, mesh in meshes:
        scene.add_geometry(mesh, geom_name=name, node_name=name)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(scene.export(file_type="glb"))
