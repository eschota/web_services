"""Label every dressed-mesh vertex as ``outer``, ``hair`` or ``cloth``.

* Distance: signed distance from each dressed vertex to the aligned base
  body, measured against dense area-weighted body samples with normals
  (positive outside).
* ``cloth`` candidates are farther than ``loose_threshold`` outside the body.
  Skin and tight clothing stay within it and are ``outer``.
* ``hair``: vertices farther than ``hair_threshold`` above the neck seed a
  flood fill over the welded mesh graph; it may spread only through vertices
  that are also far from the body, inside the head/back column (a vertical
  prism around the head, wide enough for hair over the shoulders) and above
  ``hair_min_height``. A front-view hair mask, when given, decides every
  front-visible vertex it covers and seeds the same flood fill.
* Labels are smoothed by majority vote over the vertex graph, small islands
  are absorbed by their surroundings, and ``cloth`` is split into connected
  components (``cloth_0`` is the largest); specks are dropped back to outer.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
from scipy import ndimage

from .config import RegenConfig, coerce_config
from .errors import DecompositionError
from .geometry import SurfaceField, Topology, masked_components, same_label_components
from .landmarks import Landmarks

LABEL_VALUES: Tuple[str, ...] = ("outer", "hair", "cloth")
OUTER, HAIR, CLOTH = 0, 1, 2


# ------------------------------------------------------------------ hair mask


@dataclasses.dataclass
class HairMask:
    """A front-view hair mask of the dressed character.

    ``mask`` is a boolean image, row 0 at the top, True = hair.

    The orthographic mapping: the dressed mesh's X bounds map linearly onto
    columns ``bbox_px[0]..bbox_px[2]`` and its Y bounds (top first) onto rows
    ``bbox_px[1]..bbox_px[3]``. ``None`` means the whole image. For image A
    this box is the bounding box of its alpha silhouette.

    ``mirror``: the image's right side shows world -X. ``None`` decides it
    from the measured facing (a character facing +Z shows +X on the right).
    """

    mask: np.ndarray
    bbox_px: Optional[Tuple[float, float, float, float]] = None
    mirror: Optional[bool] = None

    def __post_init__(self) -> None:
        mask = np.asarray(self.mask)
        if mask.ndim == 3:
            mask = mask[..., 0]
        if mask.ndim != 2 or mask.size == 0:
            raise ValueError("hair mask must be a 2D image")
        self.mask = mask.astype(bool)
        if self.bbox_px is not None:
            box = tuple(float(v) for v in self.bbox_px)
            if len(box) != 4 or box[2] <= box[0] or box[3] <= box[1]:
                raise ValueError("bbox_px must be (col_min, row_min, col_max, row_max)")
            self.bbox_px = box

    @classmethod
    def from_image(
        cls,
        path,
        bbox_px: Optional[Sequence[float]] = None,
        mirror: Optional[bool] = None,
        threshold: int = 127,
    ) -> "HairMask":
        """Load a mask image (white = hair). Needs Pillow."""
        from PIL import Image

        with Image.open(Path(path)) as img:
            gray = np.asarray(img.convert("L"))
        return cls(gray > threshold, tuple(bbox_px) if bbox_px is not None else None, mirror)

    def pixel_coords(
        self, points: np.ndarray, x_bounds: Tuple[float, float], y_bounds: Tuple[float, float], facing: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        rows_n, cols_n = self.mask.shape
        c0, r0, c1, r1 = self.bbox_px if self.bbox_px is not None else (0.0, 0.0, cols_n - 1.0, rows_n - 1.0)
        mirror = self.mirror if self.mirror is not None else facing < 0
        u = (points[:, 0] - x_bounds[0]) / max(x_bounds[1] - x_bounds[0], 1e-12)
        if mirror:
            u = 1.0 - u
        v = (y_bounds[1] - points[:, 1]) / max(y_bounds[1] - y_bounds[0], 1e-12)
        return c0 + u * (c1 - c0), r0 + v * (r1 - r0)


def _mask_decisions(
    positions: np.ndarray,
    mask: HairMask,
    landmarks: Landmarks,
    dressed_bounds: Tuple[np.ndarray, np.ndarray],
    cfg: RegenConfig,
    occluders: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """``(decided, is_hair)``: which vertices the mask can see and what it says."""
    lo, hi = dressed_bounds
    xb = (float(lo[0]), float(hi[0]))
    yb = (float(lo[1]), float(hi[1]))
    rows_n, cols_n = mask.mask.shape
    cols, rows = mask.pixel_coords(positions, xb, yb, landmarks.facing)
    ci = np.rint(cols).astype(np.int64)
    ri = np.rint(rows).astype(np.int64)
    inside = (ci >= 0) & (ci < cols_n) & (ri >= 0) & (ri < rows_n)
    is_hair = np.zeros(len(positions), dtype=bool)
    is_hair[inside] = mask.mask[ri[inside], ci[inside]]

    # Visibility from the front: a point z-buffer of the surface, coarse
    # enough that the splatted samples leave no holes.
    splat = positions if occluders is None else np.concatenate([positions, occluders])
    side = int(np.clip(np.sqrt(len(splat) / 3.0), 64, cfg.hair_mask_zbuffer_max))
    gh = gw = side
    s_cols, s_rows = mask.pixel_coords(splat, xb, yb, landmarks.facing)
    c0, r0, c1, r1 = mask.bbox_px if mask.bbox_px is not None else (0.0, 0.0, cols_n - 1.0, rows_n - 1.0)

    def cell(c: np.ndarray, r: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        gi = np.clip(((r - r0) / max(r1 - r0, 1e-9) * (gh - 1)).round().astype(np.int64), 0, gh - 1)
        gj = np.clip(((c - c0) / max(c1 - c0, 1e-9) * (gw - 1)).round().astype(np.int64), 0, gw - 1)
        return gi, gj

    depth_s = landmarks.facing * splat[:, 2]
    si, sj = cell(s_cols, s_rows)
    zbuf = np.full((gh, gw), -np.inf)
    np.maximum.at(zbuf, (si, sj), depth_s)
    zbuf = ndimage.maximum_filter(zbuf, size=3, mode="nearest")
    vi, vj = cell(cols, rows)
    depth_v = landmarks.facing * positions[:, 2]
    visible = depth_v >= zbuf[vi, vj] - cfg.hair_mask_depth_tolerance * landmarks.height
    return visible & inside, is_hair


# --------------------------------------------------------------- segmentation


@dataclasses.dataclass
class Segmentation:
    labels: np.ndarray  # (W,) uint8 per welded vertex, index into LABEL_VALUES
    part_index: np.ndarray  # (W,) int16, -1 for outer
    part_names: List[str]
    part_kinds: List[str]
    signed_distance: np.ndarray  # (W,) float32, dressed units
    hair_source: str
    warnings: List[str]

    def part_vertices(self, index: int) -> np.ndarray:
        return np.flatnonzero(self.part_index == index)


def _grow(edges, seeds: np.ndarray, allowed: np.ndarray) -> np.ndarray:
    """Vertices of ``allowed`` connected (through ``allowed``) to a seed."""
    allowed = allowed | seeds
    if not seeds.any():
        return np.zeros_like(seeds)
    ncomp, comp = masked_components(edges, allowed)
    seeded = np.zeros(ncomp, dtype=bool)
    seeded[comp[seeds]] = True
    return allowed & seeded[comp]


def smooth_labels(labels: np.ndarray, topology: Topology, iterations: int, n_labels: int = 3) -> np.ndarray:
    """Majority vote over each vertex and its neighbours; ties keep the label."""
    labels = labels.astype(np.uint8).copy()
    n = len(labels)
    if n == 0 or iterations <= 0:
        return labels
    adj = topology.adjacency
    rows = np.arange(n)
    for _ in range(int(iterations)):
        onehot = np.zeros((n, n_labels), dtype=np.float32)
        onehot[rows, labels] = 1.0
        votes = adj @ onehot + onehot
        best = votes.argmax(axis=1)
        keep = votes[rows, labels] >= votes.max(axis=1)
        new = np.where(keep, labels, best).astype(np.uint8)
        if np.array_equal(new, labels):
            break
        labels = new
    return labels


def absorb_islands(labels: np.ndarray, topology: Topology, min_size: int, n_labels: int = 3, rounds: int = 3) -> np.ndarray:
    """Relabel connected same-label islands smaller than ``min_size`` with the
    label most common along their border. Islands without a border (separate
    mesh pieces) keep their label."""
    labels = labels.astype(np.uint8).copy()
    u, v = topology.edges
    if len(u) == 0 or min_size <= 1:
        return labels
    for _ in range(rounds):
        ncomp, comp = same_label_components(topology.edges, labels)
        sizes = np.bincount(comp, minlength=ncomp)
        small = sizes < min_size
        if not small.any():
            break
        cross = labels[u] != labels[v]
        a = np.concatenate([u[cross], v[cross]])
        b = np.concatenate([v[cross], u[cross]])
        from_small = small[comp[a]]
        a = a[from_small]
        b = b[from_small]
        if len(a) == 0:
            break
        votes = np.bincount(comp[a] * n_labels + labels[b], minlength=ncomp * n_labels).reshape(ncomp, n_labels)
        has_votes = votes.sum(axis=1) > 0
        target = votes.argmax(axis=1).astype(np.uint8)
        change = small[comp] & has_votes[comp]
        if not change.any():
            break
        new = labels.copy()
        new[change] = target[comp[change]]
        if np.array_equal(new, labels):
            break
        labels = new
    return labels


def segment_dressed(
    topology: Topology,
    body_field: SurfaceField,
    landmarks: Landmarks,
    *,
    config: Optional[RegenConfig] = None,
    hair_mask: Optional[HairMask] = None,
    dressed_bounds: Optional[Tuple[np.ndarray, np.ndarray]] = None,
    occluders: Optional[np.ndarray] = None,
) -> Segmentation:
    """Label the welded dressed vertices.

    ``body_field``: signed-distance field of the aligned base body.
    ``dressed_bounds``: X/Y bounds the hair mask is registered to (defaults
    to the welded vertices' bounds). ``occluders``: extra dressed surface
    samples that make the mask's visibility test hole-free.
    """
    cfg = coerce_config(config)
    lm = landmarks
    height = lm.height
    pos = topology.vertices
    n = len(pos)
    warnings: List[str] = []
    if n == 0:
        raise DecompositionError("dressed mesh has no vertices")

    dist, _ = body_field.query(pos, k=cfg.distance_knn)
    rel = dist / height
    loose = rel > cfg.loose_threshold
    far_for_hair = rel > cfg.hair_threshold
    loose_share = float(loose.mean())
    if loose_share > cfg.max_loose_fraction:
        raise DecompositionError(
            f"{loose_share:.0%} of the dressed surface is far from the aligned body; "
            "the base body does not match the dressed character"
        )

    x = pos[:, 0]
    y = pos[:, 1]
    column_half = max(cfg.hair_column_head_scale * lm.head_radius, cfg.hair_column_shoulder_scale * lm.chest_half_width)
    column = np.abs(x - lm.head_center[0]) <= column_half
    grow_ok = far_for_hair & column & (y >= lm.ground_y + cfg.hair_min_height * height)
    seeds = far_for_hair & (y > lm.neck[1])
    hair = _grow(topology.edges, seeds, grow_ok)
    hair_source = "heuristic"

    if hair_mask is not None:
        bounds = dressed_bounds if dressed_bounds is not None else (pos.min(axis=0), pos.max(axis=0))
        decided, says_hair = _mask_decisions(pos, hair_mask, lm, bounds, cfg, occluders)
        if decided.any():
            mask_seeds = decided & says_hair
            hair = np.where(decided, says_hair, hair)
            grown = _grow(topology.edges, mask_seeds, grow_ok & ~decided)
            hair |= grown & ~decided
            hair_source = "mask"
        else:
            warnings.append("hair mask covers no front-visible vertex; heuristic hair kept")

    labels = np.full(n, OUTER, dtype=np.uint8)
    labels[loose] = CLOTH
    labels[hair] = HAIR

    labels = smooth_labels(labels, topology, cfg.smooth_iterations)
    min_island = max(3, int(round(cfg.island_fraction * n)))
    labels = absorb_islands(labels, topology, min_island)

    min_part = max(int(cfg.min_part_vertices), int(round(cfg.min_part_fraction * n)))
    min_extent = cfg.min_part_extent * height

    part_names: List[str] = []
    part_kinds: List[str] = []
    part_index = np.full(n, -1, dtype=np.int16)

    hair_ids = np.flatnonzero(labels == HAIR)
    if len(hair_ids):
        extent = float(np.linalg.norm(np.ptp(pos[hair_ids], axis=0)))
        if len(hair_ids) < min_part or extent < min_extent:
            labels[hair_ids] = OUTER
            warnings.append(f"hair region too small ({len(hair_ids)} vertices); treated as outer")
        else:
            part_index[hair_ids] = len(part_names)
            part_names.append("hair")
            part_kinds.append("hair")

    cloth = labels == CLOTH
    if cloth.any():
        ncomp, comp = masked_components(topology.edges, cloth)
        ids = comp[cloth]
        sizes = np.bincount(ids, minlength=ncomp)
        order = np.argsort(-sizes, kind="stable")
        cloth_vertices = np.flatnonzero(cloth)
        comp_of = comp[cloth_vertices]
        sort = np.argsort(comp_of, kind="stable")
        starts = np.concatenate([[0], np.cumsum(np.bincount(comp_of, minlength=ncomp))])
        dropped = 0
        n_cloth = 0
        for c in order:
            if sizes[c] == 0:
                break
            members = cloth_vertices[sort[starts[c] : starts[c + 1]]]
            extent = float(np.linalg.norm(np.ptp(pos[members], axis=0)))
            if sizes[c] < min_part or extent < min_extent:
                labels[members] = OUTER
                dropped += 1
                continue
            part_index[members] = len(part_names)
            part_names.append(f"cloth_{n_cloth}")
            part_kinds.append("cloth")
            n_cloth += 1
        if dropped:
            warnings.append(f"{dropped} cloth speck(s) dropped")

    return Segmentation(
        labels=labels,
        part_index=part_index,
        part_names=part_names,
        part_kinds=part_kinds,
        signed_distance=dist.astype(np.float32),
        hair_source=hair_source,
        warnings=warnings,
    )
