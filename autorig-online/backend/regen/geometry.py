"""Geometry helpers shared by the Regen stages.

Everything here is vectorised numpy / scipy: no per-vertex Python loops, so a
300k-vertex mesh costs fractions of a second per call.
"""

from __future__ import annotations

import dataclasses
from typing import Optional, Tuple

import numpy as np
from scipy import sparse
from scipy.sparse import csgraph
from scipy.spatial import cKDTree


def as_vertices_faces(mesh) -> Tuple[np.ndarray, np.ndarray]:
    """Accept anything with ``vertices``/``faces`` (MeshData, Topology,
    trimesh.Trimesh) or a ``(vertices, faces)`` pair."""
    if isinstance(mesh, (tuple, list)) and len(mesh) == 2:
        vertices, faces = mesh
    else:
        vertices, faces = mesh.vertices, mesh.faces
    vertices = np.asarray(vertices, dtype=np.float64).reshape(-1, 3)
    faces = np.asarray(faces, dtype=np.int64).reshape(-1, 3)
    return vertices, faces


# --------------------------------------------------------------------- welding


@dataclasses.dataclass
class Topology:
    """A mesh with coincident vertices merged.

    glTF duplicates a vertex for every UV island it belongs to, so the raw
    mesh falls apart along its UV seams. All connectivity (label smoothing,
    component splitting, attachment lines) runs on this welded version;
    ``index`` maps each source vertex to its welded vertex so results can be
    carried back.
    """

    vertices: np.ndarray  # (W, 3) float64
    faces: np.ndarray  # (F, 3) int64, same face order as the source mesh
    index: np.ndarray  # (V,) int64 source vertex -> welded vertex
    _edges: Optional[Tuple[np.ndarray, np.ndarray]] = dataclasses.field(default=None, repr=False)
    _adjacency: Optional[sparse.csr_matrix] = dataclasses.field(default=None, repr=False)
    _normals: Optional[np.ndarray] = dataclasses.field(default=None, repr=False)

    @property
    def n_vertices(self) -> int:
        return int(len(self.vertices))

    @property
    def edges(self) -> Tuple[np.ndarray, np.ndarray]:
        """Unique undirected edges ``(u, v)`` with ``u < v``."""
        if self._edges is None:
            self._edges = unique_edges(self.faces, self.n_vertices)
        return self._edges

    @property
    def adjacency(self) -> sparse.csr_matrix:
        """Binary symmetric vertex adjacency (float32, no self loops)."""
        if self._adjacency is None:
            u, v = self.edges
            n = self.n_vertices
            data = np.ones(2 * len(u), dtype=np.float32)
            self._adjacency = sparse.csr_matrix(
                (data, (np.concatenate([u, v]), np.concatenate([v, u]))), shape=(n, n)
            )
        return self._adjacency

    @property
    def vertex_normals(self) -> np.ndarray:
        if self._normals is None:
            self._normals = vertex_normals(self.vertices, self.faces)
        return self._normals


def weld(vertices: np.ndarray, faces: np.ndarray, tolerance: float = 1e-6) -> Topology:
    """Merge vertices closer than ``tolerance`` x bounding-box diagonal."""
    vertices = np.asarray(vertices, dtype=np.float64).reshape(-1, 3)
    faces = np.asarray(faces, dtype=np.int64).reshape(-1, 3)
    if len(vertices) == 0:
        return Topology(vertices, faces, np.zeros(0, dtype=np.int64))
    lo = vertices.min(axis=0)
    diag = float(np.linalg.norm(vertices.max(axis=0) - lo)) or 1.0
    step = diag * max(float(tolerance), 1e-9)
    q = np.floor((vertices - lo) / step + 0.5).astype(np.int64)
    span = q.max(axis=0) + 1
    if float(span[0]) * float(span[1]) * float(span[2]) < 9.0e18:
        key = (q[:, 0] * span[1] + q[:, 1]) * span[2] + q[:, 2]
        _, first, inverse = np.unique(key, return_index=True, return_inverse=True)
    else:  # pragma: no cover - only for absurd tolerances
        _, first, inverse = np.unique(q, axis=0, return_index=True, return_inverse=True)
    inverse = np.asarray(inverse, dtype=np.int64).reshape(-1)
    return Topology(vertices[first], inverse[faces], inverse)


def unique_edges(faces: np.ndarray, n_vertices: int) -> Tuple[np.ndarray, np.ndarray]:
    faces = np.asarray(faces, dtype=np.int64)
    if len(faces) == 0:
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty
    e = faces[:, [0, 1, 1, 2, 2, 0]].reshape(-1, 2)
    e = e[e[:, 0] != e[:, 1]]
    e.sort(axis=1)
    key = np.unique(e[:, 0] * np.int64(n_vertices) + e[:, 1])
    return key // n_vertices, key % n_vertices


# ------------------------------------------------------------ normals / areas


def face_normals_areas(vertices: np.ndarray, faces: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    tri = vertices[faces]
    cross = np.cross(tri[:, 1] - tri[:, 0], tri[:, 2] - tri[:, 0])
    norm = np.linalg.norm(cross, axis=1)
    normals = np.zeros_like(cross)
    ok = norm > 0
    normals[ok] = cross[ok] / norm[ok, None]
    return normals, 0.5 * norm


def vertex_normals(vertices: np.ndarray, faces: np.ndarray) -> np.ndarray:
    """Area-weighted vertex normals (zero-area faces contribute nothing)."""
    tri = vertices[faces]
    cross = np.cross(tri[:, 1] - tri[:, 0], tri[:, 2] - tri[:, 0])  # length = 2 * area
    n = len(vertices)
    acc = np.zeros((n, 3), dtype=np.float64)
    for k in range(3):
        for c in range(3):
            acc[:, c] += np.bincount(faces[:, k], weights=cross[:, c], minlength=n)
    norm = np.linalg.norm(acc, axis=1)
    out = np.zeros_like(acc)
    ok = norm > 0
    out[ok] = acc[ok] / norm[ok, None]
    return out


def sample_surface(
    vertices: np.ndarray,
    faces: np.ndarray,
    count: int,
    rng: np.random.Generator,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Area-weighted uniform samples: ``(points, face normals, face index)``."""
    normals, areas = face_normals_areas(vertices, faces)
    total = float(areas.sum())
    if count <= 0 or total <= 0.0:
        return np.zeros((0, 3)), np.zeros((0, 3)), np.zeros(0, dtype=np.int64)
    cdf = np.cumsum(areas)
    pick = np.searchsorted(cdf, rng.random(count) * cdf[-1], side="right")
    pick = np.minimum(pick, len(faces) - 1)
    r1 = np.sqrt(rng.random(count))
    r2 = rng.random(count)
    tri = vertices[faces[pick]]
    points = (
        (1.0 - r1)[:, None] * tri[:, 0]
        + (r1 * (1.0 - r2))[:, None] * tri[:, 1]
        + (r1 * r2)[:, None] * tri[:, 2]
    )
    return points, normals[pick], pick


# ---------------------------------------------------------- signed distance


def kdtree(points: np.ndarray) -> cKDTree:
    """A KD-tree for surface samples.

    scipy's defaults (median splits, compacted node boxes) degrade badly on
    samples of flat axis-aligned faces: 8k nearest-neighbour queries against
    150k samples of the synthetic mannequin took 130 ms with the defaults and
    20 ms with these flags. On curved surfaces these flags are as fast.
    """
    return cKDTree(np.asarray(points, dtype=np.float64), balanced_tree=False, compact_nodes=False)


class SurfaceField:
    """Signed distance to a surface represented by dense oriented samples.

    Positive outside (along the sample normals), negative inside. The sign is
    a distance-weighted vote of the ``k`` nearest samples, which keeps it
    stable in creases (armpits, crotch) where the single nearest normal can
    point sideways.
    """

    def __init__(self, points: np.ndarray, normals: np.ndarray):
        self.points = np.asarray(points, dtype=np.float64)
        self.normals = np.asarray(normals, dtype=np.float64)
        self.tree = kdtree(self.points)

    def query(self, query: np.ndarray, k: int = 4, chunk: int = 200_000) -> Tuple[np.ndarray, np.ndarray]:
        """Return ``(signed distance, index of the nearest sample)``."""
        query = np.asarray(query, dtype=np.float64).reshape(-1, 3)
        n = len(query)
        dist_out = np.empty(n, dtype=np.float64)
        idx_out = np.empty(n, dtype=np.int64)
        k = max(1, min(int(k), len(self.points)))
        for start in range(0, n, chunk):
            q = query[start : start + chunk]
            d, idx = self.tree.query(q, k=k, workers=-1)
            if k == 1:
                d = d[:, None]
                idx = idx[:, None]
            diff = q[:, None, :] - self.points[idx]
            dots = np.einsum("nkc,nkc->nk", diff, self.normals[idx])
            vote = (np.sign(dots) / (d + 1e-12)).sum(axis=1)
            sign = np.where(vote < 0.0, -1.0, 1.0)
            dist_out[start : start + chunk] = sign * d[:, 0]
            idx_out[start : start + chunk] = idx[:, 0]
        return dist_out, idx_out


# ----------------------------------------------------------------- components


def masked_components(
    edges: Tuple[np.ndarray, np.ndarray], mask: np.ndarray
) -> Tuple[int, np.ndarray]:
    """Connected components of the subgraph induced by ``mask``.

    Returns ``(n_components, component id per vertex)``; vertices outside the
    mask get their own singleton components, so callers index by mask.
    """
    u, v = edges
    n = len(mask)
    keep = mask[u] & mask[v]
    graph = sparse.coo_matrix(
        (np.ones(int(keep.sum()), dtype=np.int8), (u[keep], v[keep])), shape=(n, n)
    )
    return csgraph.connected_components(graph, directed=False)


def same_label_components(
    edges: Tuple[np.ndarray, np.ndarray], labels: np.ndarray
) -> Tuple[int, np.ndarray]:
    u, v = edges
    n = len(labels)
    keep = labels[u] == labels[v]
    graph = sparse.coo_matrix(
        (np.ones(int(keep.sum()), dtype=np.int8), (u[keep], v[keep])), shape=(n, n)
    )
    return csgraph.connected_components(graph, directed=False)


def face_majority(faces: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Per-face value held by at least two of its three vertices (the first
    vertex's when all three differ)."""
    val = values[faces]
    a, b, c = val[:, 0], val[:, 1], val[:, 2]
    return np.where((a == b) | (a == c), a, np.where(b == c, b, a))


# ----------------------------------------------------------------- rotations


def rotation_angle(rotation: np.ndarray) -> float:
    cos = (float(np.trace(rotation)) - 1.0) * 0.5
    return float(np.arccos(np.clip(cos, -1.0, 1.0)))


def axis_angle_matrix(axis: np.ndarray, angle: float) -> np.ndarray:
    axis = np.asarray(axis, dtype=np.float64)
    norm = float(np.linalg.norm(axis))
    if norm == 0.0 or angle == 0.0:
        return np.eye(3)
    x, y, z = axis / norm
    k = np.array([[0.0, -z, y], [z, 0.0, -x], [-y, x, 0.0]])
    return np.eye(3) + np.sin(angle) * k + (1.0 - np.cos(angle)) * (k @ k)


def clamp_rotation(rotation: np.ndarray, max_angle: float) -> Tuple[np.ndarray, bool]:
    """Scale a rotation's angle down to ``max_angle`` (radians), keeping its axis."""
    angle = rotation_angle(rotation)
    if angle <= max_angle:
        return rotation, False
    axis = np.array(
        [
            rotation[2, 1] - rotation[1, 2],
            rotation[0, 2] - rotation[2, 0],
            rotation[1, 0] - rotation[0, 1],
        ]
    )
    if float(np.linalg.norm(axis)) < 1e-12:  # 180 degrees: pick the dominant axis
        axis = np.sqrt(np.clip((np.diag(rotation) + 1.0) * 0.5, 0.0, None))
    return axis_angle_matrix(axis, max_angle), True
