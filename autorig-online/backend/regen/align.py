"""Similarity alignment of the base body (image B) onto the dressed character (image A).

Hunyuan normalises each reconstruction on its own, so the two GLBs differ by
an unknown uniform scale and translation (plus at most a few degrees of
rotation). Clothing and hair only ever *add* material outside the body, so:

1. An initial guess comes from landmarks that clothes and hair barely move:
   feet level (min Y), the T-pose arm band (the widest horizontal slab), the
   hand tips (its X extremes) and the arms' depth.
2. Trimmed similarity ICP (Umeyama) moves body samples onto the dressed
   surface. Each iteration keeps only the best ``align_keep_fraction`` of the
   correspondences — the rest are body points under loose clothes and hair —
   and rejects pairs whose normals disagree. Rotation is clamped to a few
   degrees and the scale may not wander far from the landmark guess.
3. The fit is restarted from a few scale multipliers and the lowest trimmed
   residual wins.
"""

from __future__ import annotations

import dataclasses
import math
from typing import List, Optional, Tuple

import numpy as np
from scipy.spatial import cKDTree

from .config import RegenConfig, coerce_config
from .errors import DecompositionError
from .geometry import as_vertices_faces, clamp_rotation, kdtree, rotation_angle, sample_surface


# ----------------------------------------------------------------- similarity


@dataclasses.dataclass
class Similarity:
    """``p_dressed = scale * R @ p_body + t`` as a 4x4 matrix, plus fit residuals."""

    matrix: np.ndarray
    scale: float
    rms: float = float("nan")
    """Trimmed RMS of the kept correspondences, dressed-space units."""
    inlier_ratio: float = float("nan")
    """Share of body samples within ``align_inlier_distance`` of the dressed surface."""
    iterations: int = 0
    converged: bool = False
    rotation_clamped: bool = False
    initial_matrix: Optional[np.ndarray] = None
    reference_height: float = float("nan")
    """Height (dressed units) the relative thresholds were evaluated against."""
    starts: List[dict] = dataclasses.field(default_factory=list)

    @classmethod
    def from_srt(cls, scale: float, rotation: np.ndarray, translation: np.ndarray, **kw) -> "Similarity":
        m = np.eye(4)
        m[:3, :3] = scale * np.asarray(rotation, dtype=np.float64)
        m[:3, 3] = np.asarray(translation, dtype=np.float64)
        return cls(matrix=m, scale=float(scale), **kw)

    @property
    def rotation(self) -> np.ndarray:
        return self.matrix[:3, :3] / self.scale

    @property
    def translation(self) -> np.ndarray:
        return self.matrix[:3, 3].copy()

    @property
    def rotation_deg(self) -> float:
        return math.degrees(rotation_angle(self.rotation))

    @property
    def rms_relative(self) -> float:
        return float(self.rms / self.reference_height) if self.reference_height > 0 else float("nan")

    def apply(self, points: np.ndarray) -> np.ndarray:
        points = np.asarray(points, dtype=np.float64)
        return points @ self.matrix[:3, :3].T + self.matrix[:3, 3]

    def apply_vectors(self, vectors: np.ndarray) -> np.ndarray:
        """Rotate direction vectors (normals) — no scale, no translation."""
        return np.asarray(vectors, dtype=np.float64) @ self.rotation.T

    def inverse(self) -> "Similarity":
        r_inv = self.rotation.T
        s_inv = 1.0 / self.scale
        return Similarity.from_srt(s_inv, r_inv, -s_inv * (r_inv @ self.translation))

    def as_list(self) -> List[float]:
        """16 floats, row-major."""
        return [float(v) for v in self.matrix.reshape(-1)]


def umeyama(
    src: np.ndarray, dst: np.ndarray, weights: Optional[np.ndarray] = None
) -> Tuple[float, np.ndarray, np.ndarray]:
    """(Weighted) least-squares similarity ``dst ~ s * R @ src + t`` (Umeyama 1991)."""
    if weights is None:
        w = np.full(len(src), 1.0 / len(src))
    else:
        w = np.asarray(weights, dtype=np.float64)
        w = w / w.sum()
    mu_s = w @ src
    mu_d = w @ dst
    xs = src - mu_s
    xd = dst - mu_d
    var_s = float(w @ (xs**2).sum(axis=1))
    cov = (xd * w[:, None]).T @ xs
    u, d, vt = np.linalg.svd(cov)
    sign = np.ones(3)
    if np.linalg.det(u) * np.linalg.det(vt) < 0.0:
        sign[2] = -1.0
    rot = u @ np.diag(sign) @ vt
    scale = float((d * sign).sum() / var_s) if var_s > 0 else 1.0
    trans = mu_d - scale * rot @ mu_s
    return scale, rot, trans


# ------------------------------------------------------------------ pose frame


@dataclasses.dataclass
class PoseFrame:
    """Coarse T-pose frame of a mesh used for the initial alignment guess."""

    ground: float
    top: float
    band_y: float
    band_lo: float
    band_hi: float
    span: float
    x_mid: float
    z_mid: float
    leg_ratio: float

    @property
    def height(self) -> float:
        return self.top - self.ground

    @property
    def anchor(self) -> np.ndarray:
        return np.array([self.x_mid, self.ground, self.z_mid])


def pose_frame(
    points: np.ndarray,
    *,
    n_slices: int = 100,
    min_span: float = 0.6,
    min_band_height: float = 0.55,
    max_leg_ratio: float = 0.6,
    what: str = "mesh",
) -> PoseFrame:
    """Find feet level, the arm band and hand tips of a T-posed character.

    Raises DecompositionError when the widest horizontal slab does not look
    like T-pose arms (too narrow, too low, or no wider than the legs).
    """
    points = np.asarray(points, dtype=np.float64)
    if len(points) < 100:
        raise DecompositionError(f"{what} has too little surface to measure")
    y = points[:, 1]
    ground = float(np.percentile(y, 0.05))
    top = float(np.percentile(y, 99.95))
    h = top - ground
    if not np.isfinite(h) or h <= 0.0:
        raise DecompositionError(f"{what} has no vertical extent")
    idx = np.clip(((y - ground) / h * n_slices).astype(np.int64), 0, n_slices - 1)
    order = np.argsort(idx, kind="stable")
    bounds = np.concatenate([[0], np.cumsum(np.bincount(idx, minlength=n_slices))])
    lo_x = np.full(n_slices, np.nan)
    hi_x = np.full(n_slices, np.nan)
    for i in range(n_slices):
        members = order[bounds[i] : bounds[i + 1]]
        if len(members) >= 5:
            lo_x[i], hi_x[i] = np.percentile(points[members, 0], [0.5, 99.5])
    widths = hi_x - lo_x
    if not np.any(np.isfinite(widths)):
        raise DecompositionError(f"no T-pose arm band found in {what}: empty slices")
    imax = int(np.nanargmax(widths))
    wmax = float(widths[imax])
    i0 = imax
    while i0 > 0 and np.isfinite(widths[i0 - 1]) and widths[i0 - 1] >= 0.85 * wmax:
        i0 -= 1
    i1 = imax
    while i1 < n_slices - 1 and np.isfinite(widths[i1 + 1]) and widths[i1 + 1] >= 0.85 * wmax:
        i1 += 1
    band_lo = ground + h * i0 / n_slices
    band_hi = ground + h * (i1 + 1) / n_slices
    band_y = 0.5 * (band_lo + band_hi)
    t_band = (band_y - ground) / h
    centres = (np.arange(n_slices) + 0.5) / n_slices
    legs = (centres >= 0.25) & (centres <= 0.42) & np.isfinite(widths)
    leg_ratio = float(np.mean(widths[legs]) / wmax) if np.any(legs) else 1.0
    problems = []
    if wmax / h < min_span:
        problems.append(f"widest slab is {wmax / h:.2f} x height (< {min_span:.2f})")
    if t_band < min_band_height:
        problems.append(f"widest slab at {t_band:.2f} of height (< {min_band_height:.2f})")
    if leg_ratio > max_leg_ratio:
        problems.append(f"leg region is {leg_ratio:.2f} of the widest slab (> {max_leg_ratio:.2f})")
    if problems:
        raise DecompositionError(f"no T-pose arm band found in {what}: " + "; ".join(problems))
    x_lo = float(np.nanmin(lo_x[i0 : i1 + 1]))
    x_hi = float(np.nanmax(hi_x[i0 : i1 + 1]))
    x_mid = 0.5 * (x_lo + x_hi)
    in_band = (y >= band_lo) & (y <= band_hi)
    outer_arm = in_band & (np.abs(points[:, 0] - x_mid) > 0.3 * 0.5 * (x_hi - x_lo))
    z_src = points[outer_arm, 2] if outer_arm.sum() >= 10 else points[in_band, 2]
    z_mid = float(np.median(z_src)) if len(z_src) else float(np.median(points[:, 2]))
    return PoseFrame(
        ground=ground,
        top=top,
        band_y=band_y,
        band_lo=band_lo,
        band_hi=band_hi,
        span=x_hi - x_lo,
        x_mid=x_mid,
        z_mid=z_mid,
        leg_ratio=leg_ratio,
    )


# ------------------------------------------------------------------------ ICP


def _trimmed(dist: np.ndarray, candidates: np.ndarray, keep: int) -> np.ndarray:
    if len(candidates) > keep:
        return candidates[np.argpartition(dist[candidates], keep - 1)[:keep]]
    return candidates


def _icp(
    src: np.ndarray,
    src_n: np.ndarray,
    tree: cKDTree,
    tgt: np.ndarray,
    tgt_n: np.ndarray,
    scale: float,
    rot: np.ndarray,
    trans: np.ndarray,
    scale_ref: float,
    h: float,
    cfg: RegenConfig,
) -> dict:
    n = len(src)
    keep = max(10, int(round(cfg.align_keep_fraction * n)))
    max_rot = math.radians(cfg.align_max_rotation_deg)
    s_lo = scale_ref * (1.0 - cfg.align_max_scale_change)
    s_hi = scale_ref * (1.0 + cfg.align_max_scale_change)
    converged = False
    clamped_any = False
    iterations = 0
    for iterations in range(1, cfg.align_max_iterations + 1):
        moved = scale * src @ rot.T + trans
        dist, idx = tree.query(moved, workers=-1)
        ok = np.einsum("ij,ij->i", src_n @ rot.T, tgt_n[idx]) > cfg.align_normal_min_dot
        cand = np.flatnonzero(ok)
        if len(cand) < keep // 4:
            cand = np.arange(n)
        sel = _trimmed(dist, cand, keep)
        matched = tgt[idx[sel]]
        # Clothing only adds material outside the body, so a body point lying
        # *inside* the dressed surface is expected (it is under a garment) and
        # must not pull the body outward; one lying outside is a real error.
        depth = np.einsum("ij,ij->i", matched - moved[sel], tgt_n[idx[sel]])
        w = np.where(depth > cfg.align_inside_tolerance * h, cfg.align_inside_weight, 1.0)
        s_new, r_new, t_new = umeyama(src[sel], matched, w)
        r_new, clamped = clamp_rotation(r_new, max_rot)
        s_clip = float(np.clip(s_new, s_lo, s_hi))
        if clamped or s_clip != s_new:
            wn = w / w.sum()
            t_new = wn @ matched - s_clip * r_new @ (wn @ src[sel])
        clamped_any = clamped_any or clamped
        new_moved = s_clip * src @ r_new.T + t_new
        step = float(np.max(np.linalg.norm(new_moved - moved, axis=1)))
        scale, rot, trans = s_clip, r_new, t_new
        if step < cfg.align_convergence * h:
            converged = True
            break
    moved = scale * src @ rot.T + trans
    dist, idx = tree.query(moved, workers=-1)
    ok = np.einsum("ij,ij->i", src_n @ rot.T, tgt_n[idx]) > cfg.align_normal_min_dot
    cand = np.flatnonzero(ok)
    if len(cand) < keep // 4:
        cand = np.arange(n)
    sel = _trimmed(dist, cand, keep)
    rms = float(np.sqrt(np.mean(dist[sel] ** 2)))
    inliers = float(np.mean(dist < cfg.align_inlier_distance * h))
    return dict(
        scale=scale,
        rot=rot,
        trans=trans,
        rms=rms,
        inlier_ratio=inliers,
        iterations=iterations,
        converged=converged,
        clamped=clamped_any,
    )


def align_body_to_dressed(
    body,
    dressed,
    *,
    config: Optional[RegenConfig] = None,
    dressed_samples: Optional[Tuple[np.ndarray, np.ndarray]] = None,
    rng: Optional[np.random.Generator] = None,
) -> Similarity:
    """Similarity mapping base-body coordinates into dressed-mesh coordinates.

    ``body`` / ``dressed``: anything with ``vertices`` and ``faces`` (or a
    ``(vertices, faces)`` pair). ``dressed_samples`` may pass precomputed
    ``(points, normals)`` of the dressed surface to avoid resampling.
    """
    cfg = coerce_config(config)
    rng = rng if rng is not None else np.random.default_rng(cfg.seed)
    bv, bf = as_vertices_faces(body)
    dv, df = as_vertices_faces(dressed)
    if len(bf) < cfg.min_faces or len(df) < cfg.min_faces:
        raise DecompositionError("mesh too small to align (fewer than %d triangles)" % cfg.min_faces)

    b_frame_pts, _, _ = sample_surface(bv, bf, cfg.align_frame_samples, rng)
    d_frame_pts, _, _ = sample_surface(dv, df, cfg.align_frame_samples, rng)
    fb = pose_frame(
        b_frame_pts,
        min_span=cfg.tpose_min_span,
        min_band_height=cfg.tpose_min_band_height,
        max_leg_ratio=cfg.tpose_max_leg_ratio,
        what="base body",
    )
    fd = pose_frame(
        d_frame_pts,
        min_span=cfg.tpose_min_span * 0.8,
        min_band_height=cfg.tpose_min_band_height * 0.85,
        max_leg_ratio=cfg.tpose_max_leg_ratio_dressed,
        what="dressed mesh",
    )
    ratios = [fd.span / fb.span, (fd.band_y - fd.ground) / (fb.band_y - fb.ground)]
    if not all(np.isfinite(ratios)) or min(ratios) <= 0:
        raise DecompositionError("cannot estimate the body/dressed scale from landmarks")
    s0 = float(math.sqrt(ratios[0] * ratios[1]))
    h = fd.height

    src, src_n, _ = sample_surface(bv, bf, cfg.align_source_samples, rng)
    if dressed_samples is None:
        tgt, tgt_n, _ = sample_surface(dv, df, cfg.align_target_samples, rng)
    else:
        tgt, tgt_n = (np.asarray(a, dtype=np.float64) for a in dressed_samples)
    tree = kdtree(tgt)

    best = None
    starts = []
    initial = None
    for mult in tuple(cfg.align_scale_starts) or (1.0,):
        s_init = s0 * float(mult)
        t_init = fd.anchor - s_init * fb.anchor
        if initial is None:
            initial = Similarity.from_srt(s_init, np.eye(3), t_init).matrix
        res = _icp(src, src_n, tree, tgt, tgt_n, s_init, np.eye(3), t_init, s0, h, cfg)
        starts.append(
            {
                "scale_start": s_init,
                "scale": res["scale"],
                "rms": res["rms"],
                "inlier_ratio": res["inlier_ratio"],
                "iterations": res["iterations"],
            }
        )
        agrees = best is not None and abs(res["scale"] / best["scale"] - 1.0) < 0.002
        if best is None or res["rms"] < best["rms"]:
            best = res
        if agrees:
            break  # two starts converged to the same fit; a third will too
        if res["rms"] < 0.002 * h and res["converged"]:
            break  # an essentially exact fit; more starts cannot improve it

    if best is None:  # pragma: no cover - the loop always runs at least once
        raise DecompositionError("alignment did not run")
    return Similarity.from_srt(
        best["scale"],
        best["rot"],
        best["trans"],
        rms=best["rms"],
        inlier_ratio=best["inlier_ratio"],
        iterations=int(sum(s["iterations"] for s in starts)),
        converged=bool(best["converged"]),
        rotation_clamped=bool(best["clamped"]),
        initial_matrix=initial,
        reference_height=h,
        starts=starts,
    )
