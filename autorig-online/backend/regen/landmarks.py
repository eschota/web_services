"""Body landmarks measured on the aligned base body (bald, skin-tight suit, T-pose).

Everything is read off horizontal slices of dense surface samples:

* up axis +Y, ground = lowest surface, height = ground to the head top;
* arm band = the widest horizontal slab (T-pose arms), hand tips = its X
  extremes, body centre X = their midpoint;
* chest half-width = the torso interval of a slice just under the arm band,
  shoulders = torso sides at upper-arm height;
* neck = the narrowest central cross-section between the arm band and the
  widest head slice; head = everything above the neck;
* crotch = lowest slice above the knees where the gap between the legs closes,
  hips = a little above it;
* facing = the direction the feet point (+Z for Hunyuan output). The
  character's left is +X when it faces +Z.
"""

from __future__ import annotations

import dataclasses
from typing import List, Optional, Tuple

import numpy as np
from scipy import ndimage

from .config import RegenConfig, coerce_config
from .errors import DecompositionError
from .geometry import sample_surface


@dataclasses.dataclass
class Landmarks:
    ground_y: float
    top_y: float
    height: float
    center_x: float
    center_z: float
    facing: int  # +1: the character faces +Z
    facing_confident: bool
    arm_band_y: float
    arm_band_lo: float
    arm_band_hi: float
    arm_span: float
    shoulder_y: float
    chest_half_width: float
    shoulder_l: np.ndarray
    shoulder_r: np.ndarray
    neck: np.ndarray
    head_center: np.ndarray
    head_radius: float
    crotch_y: float
    hips: np.ndarray
    legs_separated: bool
    warnings: List[str] = dataclasses.field(default_factory=list)

    def rel(self, y: float) -> float:
        """Height above the ground as a fraction of the character height."""
        return float((y - self.ground_y) / self.height)

    def side(self, x: float) -> str:
        """``"l"`` or ``"r"``: the character's side a world X lies on."""
        return "l" if (x - self.center_x) * self.facing > 0 else "r"

    def to_json(self) -> dict:
        def vec(v) -> list:
            return [float(c) for c in np.asarray(v).reshape(-1)]

        return {
            "ground_y": float(self.ground_y),
            "height": float(self.height),
            "top_y": float(self.top_y),
            "neck": vec(self.neck),
            "head_center": vec(self.head_center),
            "head_radius": float(self.head_radius),
            "hips": vec(self.hips),
            "shoulder_l": vec(self.shoulder_l),
            "shoulder_r": vec(self.shoulder_r),
            "crotch_y": float(self.crotch_y),
            "arm_band_y": float(self.arm_band_y),
            "arm_span": float(self.arm_span),
            "shoulder_y": float(self.shoulder_y),
            "chest_half_width": float(self.chest_half_width),
            "center": [float(self.center_x), float(self.center_z)],
            "up_axis": "+Y",
            "front_axis": "+Z" if self.facing > 0 else "-Z",
            "left_axis": "+X" if self.facing > 0 else "-X",
            "facing_confident": bool(self.facing_confident),
            "legs_separated": bool(self.legs_separated),
            "warnings": list(self.warnings),
        }


class _Slices:
    def __init__(self, points: np.ndarray, y0: float, y1: float, n: int):
        self.points = points
        self.y0 = y0
        self.h = y1 - y0
        self.n = n
        idx = np.clip(((points[:, 1] - y0) / self.h * n).astype(np.int64), 0, n - 1)
        inside = (points[:, 1] >= y0) & (points[:, 1] <= y1)
        idx = np.where(inside, idx, -1)
        keep = np.flatnonzero(idx >= 0)
        order = keep[np.argsort(idx[keep], kind="stable")]
        counts = np.bincount(idx[keep], minlength=n)
        self.order = order
        self.bounds = np.concatenate([[0], np.cumsum(counts)])

    def members(self, i: int) -> np.ndarray:
        i = int(np.clip(i, 0, self.n - 1))
        return self.order[self.bounds[i] : self.bounds[i + 1]]

    def index(self, y: float) -> int:
        return int(np.clip((y - self.y0) / self.h * self.n, 0, self.n - 1))

    def centre(self, i: int) -> float:
        return self.y0 + self.h * (i + 0.5) / self.n

    def bottom(self, i: int) -> float:
        return self.y0 + self.h * i / self.n


def central_interval(xs: np.ndarray, cx: float, gap: float) -> Optional[Tuple[float, float]]:
    """The run of x samples (split at gaps wider than ``gap``) containing
    ``cx``, or the run nearest to it."""
    if len(xs) == 0:
        return None
    s = np.sort(xs)
    breaks = np.flatnonzero(np.diff(s) > gap)
    starts = np.concatenate([[0], breaks + 1])
    ends = np.concatenate([breaks, [len(s) - 1]])
    lo = s[starts]
    hi = s[ends]
    inside = np.flatnonzero((lo <= cx) & (hi >= cx))
    if len(inside):
        k = int(inside[0])
    else:
        k = int(np.argmin(np.minimum(np.abs(lo - cx), np.abs(hi - cx))))
    return float(lo[k]), float(hi[k])


def _central_stats(points: np.ndarray, members: np.ndarray, cx: float, gap: float):
    """(x_lo, x_hi, z_lo, z_hi) of the central cross-section of one slice."""
    if len(members) < 3:
        return None
    xs = points[members, 0]
    iv = central_interval(xs, cx, gap)
    if iv is None:
        return None
    sel = members[(xs >= iv[0]) & (xs <= iv[1])]
    if len(sel) < 3:
        return None
    z_lo, z_hi = np.percentile(points[sel, 2], [2.0, 98.0])
    return iv[0], iv[1], float(z_lo), float(z_hi)


def compute_landmarks(
    vertices: np.ndarray,
    faces: np.ndarray,
    *,
    config: Optional[RegenConfig] = None,
    samples: Optional[np.ndarray] = None,
    rng: Optional[np.random.Generator] = None,
) -> Landmarks:
    """Measure landmarks on the aligned base body (dressed-space coordinates)."""
    cfg = coerce_config(config)
    rng = rng if rng is not None else np.random.default_rng(cfg.seed + 1)
    if samples is None:
        samples, _, _ = sample_surface(
            np.asarray(vertices, dtype=np.float64), np.asarray(faces, dtype=np.int64), cfg.landmark_samples, rng
        )
    pts = np.asarray(samples, dtype=np.float64)
    if len(pts) < 1000:
        raise DecompositionError("base body has too little surface for landmarks")
    warnings: List[str] = []
    y = pts[:, 1]
    ground = float(np.percentile(y, 0.05))
    top = float(np.percentile(y, 99.95))
    height = top - ground
    if not np.isfinite(height) or height <= 0:
        raise DecompositionError("base body has no vertical extent")
    n = int(cfg.landmark_slices)
    sl = _Slices(pts, ground, top, n)
    gap = 0.012 * height

    # --- arm band -------------------------------------------------------
    lo_x = np.full(n, np.nan)
    hi_x = np.full(n, np.nan)
    for i in range(n):
        m = sl.members(i)
        if len(m) >= 5:
            lo_x[i], hi_x[i] = np.percentile(pts[m, 0], [0.5, 99.5])
    widths = hi_x - lo_x
    if not np.any(np.isfinite(widths)):
        raise DecompositionError("no T-pose arm band found: empty body slices")
    imax = int(np.nanargmax(widths))
    wmax = float(widths[imax])
    i0 = imax
    while i0 > 0 and np.isfinite(widths[i0 - 1]) and widths[i0 - 1] >= 0.85 * wmax:
        i0 -= 1
    i1 = imax
    while i1 < n - 1 and np.isfinite(widths[i1 + 1]) and widths[i1 + 1] >= 0.85 * wmax:
        i1 += 1
    band_lo = sl.bottom(i0)
    band_hi = sl.bottom(i1 + 1)
    band_y = 0.5 * (band_lo + band_hi)
    if wmax / height < cfg.tpose_min_span or (band_y - ground) / height < cfg.tpose_min_band_height:
        raise DecompositionError(
            "no T-pose arm band found on the aligned base body "
            f"(span {wmax / height:.2f} x height at {(band_y - ground) / height:.2f} of height)"
        )
    x_lo = float(np.nanmin(lo_x[i0 : i1 + 1]))
    x_hi = float(np.nanmax(hi_x[i0 : i1 + 1]))
    cx = 0.5 * (x_lo + x_hi)
    span = x_hi - x_lo

    # --- torso / chest just below the arm band --------------------------
    chest = None
    for drop in (0.04, 0.07, 0.10, 0.02):
        chest = _central_stats(pts, sl.members(sl.index(band_lo - drop * height)), cx, gap)
        if chest is not None and (chest[1] - chest[0]) < 0.8 * span:
            break
        chest = None
    if chest is None:
        raise DecompositionError("cannot find the torso below the arm band")
    chest_half = 0.5 * (chest[1] - chest[0])
    torso_x = 0.5 * (chest[0] + chest[1])
    cz = 0.5 * (chest[2] + chest[3])
    if abs(torso_x - cx) > 0.1 * height:
        warnings.append("torso is off-centre between the hand tips")

    # --- facing: the feet point forward ---------------------------------
    feet = pts[y < ground + 0.035 * height]
    shins = pts[(y > ground + 0.12 * height) & (y < ground + 0.2 * height)]
    facing = 1
    facing_confident = False
    if len(feet) >= 20 and len(shins) >= 20:
        dz = float(np.median(feet[:, 2]) - np.median(shins[:, 2]))
        facing = 1 if dz >= 0 else -1
        facing_confident = abs(dz) > 0.008 * height
    if not facing_confident:
        warnings.append("facing direction not measurable from the feet; assuming +Z (glTF front)")

    # --- shoulders: upper-arm height next to the torso ------------------
    probe = chest_half + 0.05 * height
    near_band = (y > band_lo - 0.1 * height) & (y < band_hi + 0.1 * height)
    arm_ys = []
    for sgn in (-1.0, 1.0):
        sel = near_band & (np.abs(pts[:, 0] - (cx + sgn * probe)) < 0.01 * height)
        if sel.sum() >= 5:
            arm_ys.append(float(np.median(y[sel])))
    shoulder_y = float(np.mean(arm_ys)) if arm_ys else band_y
    shoulder_l = np.array([cx + facing * chest_half, shoulder_y, cz])
    shoulder_r = np.array([cx - facing * chest_half, shoulder_y, cz])

    # --- neck and head ---------------------------------------------------
    i_start = sl.index(band_hi)
    stats = {}
    for i in range(i_start, n):
        st = _central_stats(pts, sl.members(i), cx, gap)
        if st is not None:
            stats[i] = st
    if not stats:
        raise DecompositionError("no head found above the arm band")
    region_mid = band_hi + 0.4 * (top - band_hi)
    upper = [i for i in stats if sl.centre(i) >= region_mid]
    if not upper:
        raise DecompositionError("no head found above the arm band")
    head_eq = max(upper, key=lambda i: stats[i][1] - stats[i][0])
    head_w = stats[head_eq][1] - stats[head_eq][0]
    lower = [i for i in stats if i <= head_eq]
    area = {i: (stats[i][1] - stats[i][0]) * max(stats[i][3] - stats[i][2], 1e-9) for i in lower}
    neck_i = min(area, key=area.get)
    neck_w = stats[neck_i][1] - stats[neck_i][0]
    if neck_w > 0.9 * head_w:
        warnings.append("neck is not narrower than the head")
    neck_y = sl.centre(neck_i)
    neck = np.array(
        [
            0.5 * (stats[neck_i][0] + stats[neck_i][1]),
            neck_y,
            0.5 * (stats[neck_i][2] + stats[neck_i][3]),
        ]
    )
    head_pts = pts[y > neck_y]
    if len(head_pts) < 50:
        raise DecompositionError("no head found above the neck")
    hx = np.percentile(head_pts[:, 0], [1.0, 99.0])
    hz = np.percentile(head_pts[:, 2], [1.0, 99.0])
    head_center = np.array([0.5 * (hx[0] + hx[1]), 0.5 * (neck_y + top), 0.5 * (hz[0] + hz[1])])
    head_radius = float(max(0.5 * (hx[1] - hx[0]), 0.5 * (hz[1] - hz[0]), 0.45 * (top - neck_y)))
    if not (0.02 * height < head_radius < 0.35 * height):
        raise DecompositionError(f"implausible head size ({head_radius / height:.2f} x height)")

    # --- crotch and hips -------------------------------------------------
    half = 0.01 * height
    lo_i = sl.index(ground + 0.2 * height)
    hi_i = sl.index(min(band_lo, ground + 0.75 * height))
    occupied = np.zeros(n, dtype=bool)
    for i in range(lo_i, hi_i + 1):
        m = sl.members(i)
        occupied[i] = int(np.sum(np.abs(pts[m, 0] - cx) < half)) >= 2

    def solid(i: int) -> bool:
        return bool(occupied[i] and occupied[min(i + 1, n - 1)] and occupied[min(i + 2, n - 1)])

    legs_separated = not solid(lo_i)
    crotch_y = None
    if legs_separated:
        for i in range(lo_i, hi_i + 1):
            if solid(i):
                crotch_y = sl.bottom(i)
                break
    if crotch_y is None:
        crotch_y = ground + 0.47 * height
        warnings.append("legs are not separated; crotch height assumed at 0.47 of the height")
    hips_y = min(crotch_y + 0.05 * height, 0.5 * (crotch_y + shoulder_y))
    hip_stats = _central_stats(pts, sl.members(sl.index(hips_y)), cx, gap)
    if hip_stats is not None:
        hips = np.array([0.5 * (hip_stats[0] + hip_stats[1]), hips_y, 0.5 * (hip_stats[2] + hip_stats[3])])
    else:
        hips = np.array([cx, hips_y, cz])
    if not (ground < crotch_y < shoulder_y < neck_y < top):
        raise DecompositionError("body landmarks are out of order (crotch/shoulders/neck/top)")

    return Landmarks(
        ground_y=ground,
        top_y=top,
        height=height,
        center_x=cx,
        center_z=cz,
        facing=facing,
        facing_confident=facing_confident,
        arm_band_y=band_y,
        arm_band_lo=band_lo,
        arm_band_hi=band_hi,
        arm_span=span,
        shoulder_y=shoulder_y,
        chest_half_width=chest_half,
        shoulder_l=shoulder_l,
        shoulder_r=shoulder_r,
        neck=neck,
        head_center=head_center,
        head_radius=head_radius,
        crotch_y=float(crotch_y),
        hips=hips,
        legs_separated=legs_separated,
        warnings=warnings,
    )


def body_axis_at(
    body_points: np.ndarray, y: float, near_xz: np.ndarray, height: float
) -> np.ndarray:
    """Centre (x, z) of the body cross-section at height ``y`` nearest to
    ``near_xz`` — the torso for a skirt or cape, one leg for a trouser leg.

    The slab's samples are rasterised on a 1%-of-height grid, split into
    8-connected blobs, and the blob closest to ``near_xz`` wins.
    """
    near_xz = np.asarray(near_xz, dtype=np.float64)
    pts = None
    for half in (0.012, 0.025, 0.05):
        sel = np.abs(body_points[:, 1] - y) < half * height
        if sel.sum() >= 30:
            pts = body_points[sel][:, [0, 2]]
            break
    if pts is None:
        return near_xz.copy()
    cell = 0.01 * height
    lo = pts.min(axis=0) - cell
    ij = np.floor((pts - lo) / cell).astype(np.int64)
    shape = tuple(int(v) + 2 for v in ij.max(axis=0))
    grid = np.zeros(shape, dtype=bool)
    grid[ij[:, 0], ij[:, 1]] = True
    grid = ndimage.binary_dilation(grid, iterations=1)
    labels, count = ndimage.label(grid, structure=np.ones((3, 3), dtype=bool))
    if count <= 1:
        return pts.mean(axis=0)
    point_label = labels[ij[:, 0], ij[:, 1]]
    best = None
    best_d = np.inf
    for lab in range(1, count + 1):
        members = pts[point_label == lab]
        if len(members) < 5:
            continue
        d = float(np.min(np.linalg.norm(members - near_xz, axis=1)))
        centre = members.mean(axis=0)
        # a point inside a blob's outline is "at distance 0" from it
        lo_b, hi_b = members.min(axis=0), members.max(axis=0)
        if np.all(near_xz >= lo_b) and np.all(near_xz <= hi_b):
            d = min(d, 0.0) - 1.0 / (1.0 + float(np.linalg.norm(centre - near_xz)))
        if d < best_d:
            best_d = d
            best = centre
    return best if best is not None else pts.mean(axis=0)
