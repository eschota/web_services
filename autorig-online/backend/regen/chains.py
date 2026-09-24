"""Bone chains for the moving parts (hair and loose cloth components).

For each part:

1. **Attachment line**: part vertices sharing an edge with ``outer``. A part
   that does not touch the body through the mesh (a floating shell) falls
   back to the part vertices closest to ``outer``.
2. **Attach bone** (semantic): ``head`` for hair; for cloth, by the
   attachment centroid's height and side: ``hips`` / ``spine`` / ``chest``,
   ``upper_leg_l|r`` below the crotch, ``upper_arm_l|r`` beside the torso,
   ``head`` above the neck.
3. **Axis**: a vertical line through the head centre (hair) or through the
   body cross-section at the attachment height (cloth).
4. **Coverage**: occupied angle around the axis. >= ``loop_min_coverage_deg``
   is a ``loop`` (skirt), less is an ``open`` sheet (cape); hair is ``none``.
5. **Sectors**: K equal angular sectors (loop: ``loop_sectors``; open: one
   per ``open_sector_deg`` of arc, 5-7; hair: circumference /
   ``hair_chain_spacing``, 6-12, even so the back centre is a sector centre).
6. **Chain per sector**: from the attachment height in that sector down to
   the part's lowest point there; ``joints_per_chain`` joints plus an end
   joint at evenly spaced heights, each at the median radius/angle of the
   part's vertices at that height, pushed toward the axis by
   ``joint_push_fraction`` of the local thickness. Sectors that hang less
   than ``min_hang_*`` get no chain; a part with no chain is ``rigid``.

Angles are measured around the axis from the character's front (0 deg)
toward its left (90 deg). Chains are listed in increasing angle; an open
group runs from one end of its arc to the other.
"""

from __future__ import annotations

import dataclasses
import math
from typing import Dict, List, Optional, Tuple

import numpy as np

from .config import RegenConfig, coerce_config
from .geometry import Topology, face_normals_areas, kdtree, sample_surface
from .landmarks import Landmarks, body_axis_at
from .segment import OUTER


@dataclasses.dataclass
class Chain:
    joints: np.ndarray  # (joints_per_chain + 1, 3), root first, end joint last
    angle_deg: float
    sector: int
    hang: float

    @property
    def length(self) -> float:
        return float(np.linalg.norm(np.diff(self.joints, axis=0), axis=1).sum())


@dataclasses.dataclass
class PartChains:
    name: str
    kind: str  # "hair" | "cloth"
    attach: str
    connection: str  # "none" | "open" | "loop"
    preset: str
    chains: List[Chain]
    axis: np.ndarray  # (x, z)
    attach_origin: np.ndarray  # (3,) point the chains hang from: head centre / body axis at attach height
    attach_centroid: np.ndarray  # (3,) centroid of the attachment line
    attach_vertices: np.ndarray  # welded vertex ids of the attachment line
    attach_mode: str  # "adjacent" | "proximity"
    coverage_deg: float
    arc_start_deg: float
    arc_deg: float
    sectors: int
    hang: float
    rigid: bool
    notes: List[str] = dataclasses.field(default_factory=list)


# ------------------------------------------------------------------- helpers


def angles_around(points: np.ndarray, axis_xz: np.ndarray, facing: int) -> np.ndarray:
    """Degrees in [0, 360): 0 = the character's front, 90 = its left.

    A character facing -Z is the +Z case turned 180 degrees about Y, so both
    X and Z flip with ``facing``."""
    dx = facing * (points[:, 0] - axis_xz[0])
    dz = facing * (points[:, 2] - axis_xz[1])
    return np.degrees(np.arctan2(dx, dz)) % 360.0


def angular_coverage(theta: np.ndarray, bin_deg: float, min_count: int) -> Tuple[float, float, float]:
    """``(coverage, arc_start, arc_length)`` in degrees. The arc is the
    complement of the widest empty gap."""
    nb = max(8, int(round(360.0 / bin_deg)))
    width = 360.0 / nb
    counts = np.bincount((np.floor(theta / width).astype(np.int64)) % nb, minlength=nb)
    occ = counts >= min_count
    coverage = float(occ.sum() * width)
    if occ.all():
        return 360.0, 0.0, 360.0
    if not occ.any():
        return 0.0, 0.0, 0.0
    # longest circular run of empty bins
    empty = ~occ
    best_len = 0
    best_end = 0
    run = 0
    for i in range(2 * nb):
        if empty[i % nb]:
            run += 1
            if run > best_len:
                best_len = min(run, nb)
                best_end = i % nb
        else:
            run = 0
    arc_start = ((best_end + 1) % nb) * width
    return coverage, arc_start, 360.0 - best_len * width


def _circular_mean_deg(theta: np.ndarray) -> float:
    rad = np.radians(theta)
    return float(np.degrees(np.arctan2(np.sin(rad).mean(), np.cos(rad).mean())) % 360.0)


def attachment_line(
    part_mask: np.ndarray,
    labels: np.ndarray,
    topology: Topology,
    height: float,
    cfg: RegenConfig,
) -> Tuple[np.ndarray, str]:
    """Welded ids of the part's attachment vertices and how they were found."""
    u, v = topology.edges
    outer = labels == OUTER
    a = u[part_mask[u] & outer[v]]
    b = v[part_mask[v] & outer[u]]
    touching = np.unique(np.concatenate([a, b]))
    if len(touching) >= cfg.attach_min_vertices:
        return touching, "adjacent"
    part_ids = np.flatnonzero(part_mask)
    outer_ids = np.flatnonzero(outer)
    if len(outer_ids) == 0 or len(part_ids) == 0:
        return touching, "adjacent"
    pos = topology.vertices
    d, _ = kdtree(pos[outer_ids]).query(pos[part_ids], workers=-1)
    near = part_ids[d <= d.min() + cfg.attach_proximity_band * height]
    return np.unique(np.concatenate([touching, near])), "proximity"


def attach_semantic(kind: str, origin: np.ndarray, lm: Landmarks) -> str:
    if kind == "hair":
        return "head"
    x, y = float(origin[0]), float(origin[1])
    h = lm.height
    dx = x - lm.center_x
    if y > lm.neck[1]:
        return "head"
    if abs(dx) > lm.chest_half_width + 0.02 * h and y > lm.arm_band_lo - 0.05 * h:
        return f"upper_arm_{lm.side(x)}"  # T-pose arms: beside the torso at arm-band height
    if y < lm.crotch_y - 0.02 * h and abs(dx) > 0.03 * h:
        return f"upper_leg_{lm.side(x)}"
    t = (y - lm.hips[1]) / max(lm.shoulder_y - lm.hips[1], 1e-9)
    if t < 0.4:
        return "hips"
    if t < 0.72:
        return "spine"
    return "chest"


def _longest_run(sectors: List[int], k: int, circular: bool) -> List[int]:
    """Longest run of consecutive sector ids (wrapping around when circular)."""
    present = set(sectors)
    if not present:
        return []
    if circular and len(present) == k:
        return list(range(k))
    best: List[int] = []
    for s in sorted(present):
        prev = (s - 1) % k if circular else s - 1
        if prev in present and (circular or prev >= 0):
            continue  # not the start of a run
        run = [s]
        nxt = (s + 1) % k if circular else s + 1
        while nxt in present and nxt not in run:
            run.append(nxt)
            nxt = (nxt + 1) % k if circular else nxt + 1
        if len(run) > len(best):
            best = run
    return best


# -------------------------------------------------------------------- chains


def build_part_chains(
    name: str,
    kind: str,
    part_ids: np.ndarray,
    labels: np.ndarray,
    topology: Topology,
    landmarks: Landmarks,
    body_points: np.ndarray,
    *,
    part_faces: Optional[np.ndarray] = None,
    config: Optional[RegenConfig] = None,
    rng: Optional[np.random.Generator] = None,
) -> PartChains:
    """Chains for one part.

    ``part_ids``: welded vertex ids of the part; ``part_faces``: its faces
    (welded ids). Chain geometry is measured on area-weighted samples of those
    faces, so it does not depend on how the surface is tessellated (long thin
    triangles leave whole bands without a single vertex).
    """
    cfg = coerce_config(config)
    rng = rng if rng is not None else np.random.default_rng(cfg.seed + 7)
    lm = landmarks
    h = lm.height
    pos_all = topology.vertices
    part_mask = np.zeros(len(pos_all), dtype=bool)
    part_mask[part_ids] = True
    pos = pos_all[part_ids]
    notes: List[str] = []

    stat = pos
    if part_faces is not None and len(part_faces):
        _, areas = face_normals_areas(pos_all, part_faces)
        n_samples = int(np.clip(float(areas.sum()) / (cfg.chain_sample_spacing * h) ** 2, 2000, 60000))
        samples, _, _ = sample_surface(pos_all, part_faces, n_samples, rng)
        if len(samples):
            stat = samples

    attach_ids, attach_mode = attachment_line(part_mask, labels, topology, h, cfg)
    if len(attach_ids) == 0:
        # no outer vertex at all: hang from the part's top rim
        top = pos[:, 1] >= np.percentile(pos[:, 1], 97.0)
        attach_ids = part_ids[top]
        attach_mode = "top"
        notes.append("part does not touch the body; attached at its top rim")
    attach_pos = pos_all[attach_ids]
    centroid = attach_pos.mean(axis=0)
    # Where a part touches the body along its length (hair lying on the back,
    # a skirt fused to the thighs) the attachment "line" is a band; the part
    # hangs from its top, so heights come from a high percentile.
    top_q = cfg.attach_top_percentile
    attach_y = float(np.percentile(attach_pos[:, 1], top_q))
    attach = attach_semantic(kind, np.array([centroid[0], attach_y, centroid[2]]), lm)

    if kind == "hair":
        axis = np.array([lm.head_center[0], lm.head_center[2]])
        origin = np.asarray(lm.head_center, dtype=np.float64).copy()
    else:
        axis = body_axis_at(body_points, attach_y, centroid[[0, 2]], h)
        origin = np.array([axis[0], attach_y, axis[1]])
    theta = angles_around(stat, axis, lm.facing)
    theta_v = angles_around(pos, axis, lm.facing)
    attach_theta = angles_around(attach_pos, axis, lm.facing)
    min_count = max(2, int(0.002 * len(stat)))
    coverage, arc_start, arc_len = angular_coverage(theta, cfg.coverage_bin_deg, min_count)

    # --- sectors ---------------------------------------------------------
    if kind == "hair":
        radius = float(np.median(np.hypot(stat[:, 0] - axis[0], stat[:, 2] - axis[1])))
        circ = 2.0 * math.pi * radius * max(coverage, 1.0) / 360.0
        k = int(np.clip(round(circ / (cfg.hair_chain_spacing * h)), cfg.hair_sectors_min, cfg.hair_sectors_max))
        if k % 2:
            k = k + 1 if k + 1 <= cfg.hair_sectors_max else k - 1
        mode = "hair"
    elif coverage >= cfg.loop_min_coverage_deg:
        k = int(cfg.loop_sectors)
        mode = "loop"
    else:
        if arc_len < cfg.narrow_arc_deg:
            k = int(np.clip(round(arc_len / 30.0), 1, 3))
        else:
            k = int(np.clip(round(arc_len / cfg.open_sector_deg), cfg.open_sectors_min, cfg.open_sectors_max))
        mode = "open"
    k = max(1, k)

    def sector_index(angles: np.ndarray) -> np.ndarray:
        if mode == "open":
            rel = (angles - arc_start) % 360.0
            idx = np.where(rel < arc_len, np.floor(rel / (arc_len / k)), -1).astype(np.int64)
            return np.minimum(idx, k - 1)
        width = 360.0 / k
        return (np.floor(((angles + 0.5 * width) % 360.0) / width).astype(np.int64)) % k

    sector_of = sector_index(theta)
    sector_of_v = sector_index(theta_v)
    attach_sector = sector_index(attach_theta)

    # Shoes and boots are wider than the base body's bare feet, so they come
    # out as loose "cloth" around one leg reaching the floor. They must not
    # swing (a floor-length gown goes around both legs and is not affected).
    footwear = (
        kind == "cloth"
        and attach.startswith("upper_leg")
        and float(pos[:, 1].min()) < lm.ground_y + cfg.ground_contact_margin * h
    )
    if footwear:
        notes.append("reaches the floor around one leg (footwear / trouser leg): kept rigid")

    # --- one chain per sector ----------------------------------------------
    min_hang = (cfg.min_hang_hair if kind == "hair" else cfg.min_hang_cloth) * h
    min_points = max(int(cfg.min_sector_vertices), int(0.003 * len(stat)))
    n_joints = max(1, int(cfg.joints_per_chain))
    chains: Dict[int, Chain] = {}
    best_hang = 0.0
    for s in range(0 if footwear else k):
        sel = np.flatnonzero(sector_of == s)
        if len(sel) < min_points:
            continue
        ys = stat[sel, 1]
        sel_v = sector_of_v == s
        top_y = max(float(ys.max()), float(pos[sel_v, 1].max()) if sel_v.any() else -np.inf)
        att_y = attach_pos[attach_sector == s, 1]
        root_y = float(np.percentile(att_y, top_q)) if len(att_y) >= 2 else attach_y
        root_y = min(root_y, top_y)
        below = sel[ys <= root_y + 0.01 * h]
        if len(below) < min_points:
            continue
        bottom_y = float(np.percentile(stat[below, 1], 0.5))
        below_v = sel_v & (pos[:, 1] <= root_y + 0.01 * h)
        if below_v.any():
            bottom_y = min(bottom_y, float(np.percentile(pos[below_v, 1], 0.5)))
        hang = root_y - bottom_y
        best_hang = max(best_hang, hang)
        if hang < min_hang:
            continue
        heights = root_y - np.arange(n_joints + 1) / n_joints * hang
        half = max(0.5 * hang / n_joints, 0.01 * h)
        b_pos = stat[below]
        b_r = np.hypot(b_pos[:, 0] - axis[0], b_pos[:, 2] - axis[1])
        b_t = theta[below]
        joints = np.empty((n_joints + 1, 3))
        root_angle = 0.0
        for j, yk in enumerate(heights):
            band = np.flatnonzero(np.abs(b_pos[:, 1] - yk) <= half)
            if len(band) < 3:
                band = np.argsort(np.abs(b_pos[:, 1] - yk))[:8]
            r = b_r[band]
            r_med = float(np.median(r))
            thick = float(np.percentile(r, 90.0) - np.percentile(r, 10.0))
            r_j = max(r_med - cfg.joint_push_fraction * thick, 0.0)
            t_j = _circular_mean_deg(b_t[band])
            if j == 0:
                root_angle = t_j
            rad = math.radians(t_j)
            joints[j] = (
                axis[0] + lm.facing * r_j * math.sin(rad),
                yk,
                axis[1] + lm.facing * r_j * math.cos(rad),
            )
        chains[s] = Chain(joints=joints, angle_deg=root_angle, sector=s, hang=hang)

    # --- group topology ------------------------------------------------------
    if mode == "hair":
        order = sorted(chains)
        connection = "none"
    elif mode == "loop":
        order = _longest_run(list(chains), k, circular=True)
        if len(order) == k:
            connection = "loop"
        else:
            connection = "open" if len(order) >= 2 else "none"
            if chains:
                notes.append(f"loop part: only {len(chains)}/{k} sectors hang long enough")
    else:
        order = _longest_run(list(chains), k, circular=False)
        connection = "open" if len(order) >= 2 else "none"
    if len(order) < len(chains):
        notes.append(f"{len(chains) - len(order)} chain(s) dropped to keep the group contiguous")
    chain_list = [chains[s] for s in order]
    rigid = not chain_list

    if kind == "hair":
        longest = max((c.hang for c in chain_list), default=0.0)
        preset = "hair_stiff" if longest < cfg.hair_stiff_hang * h else "hair"
    elif mode == "loop":
        preset = "skirt"
    elif connection == "open" and arc_len >= cfg.narrow_arc_deg:
        preset = "cape"
    else:
        preset = "ribbon"

    return PartChains(
        name=name,
        kind=kind,
        attach=attach,
        connection=connection if not rigid else "none",
        preset=preset,
        chains=chain_list,
        axis=axis,
        attach_origin=origin,
        attach_centroid=centroid,
        attach_vertices=attach_ids,
        attach_mode=attach_mode,
        coverage_deg=coverage,
        arc_start_deg=arc_start,
        arc_deg=arc_len,
        sectors=k,
        hang=best_hang,
        rigid=rigid,
        notes=notes,
    )
