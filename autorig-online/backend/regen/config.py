"""Tunable thresholds for AutoRig Regen.

Every length is a fraction of the character height (ground to the top of the
bald base-body head), so the same numbers work whatever scale Hunyuan
normalised a model to. Angles are in degrees. Counts are absolute.

The README in this package says which ones are worth touching and why.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Mapping, Tuple


@dataclasses.dataclass(frozen=True)
class RegenConfig:
    # ------------------------------------------------------------------ general
    seed: int = 20260924
    """Seed of every random draw (surface sampling); results are deterministic."""
    weld_tolerance: float = 1e-6
    """Vertices closer than this fraction of the bounding-box diagonal are the
    same vertex for topology purposes (glTF splits vertices at UV seams)."""
    min_faces: int = 100
    """Fewer triangles than this is not a character."""

    # ---------------------------------------------------------------- alignment
    align_source_samples: int = 8000
    """Body surface samples moved by ICP."""
    align_target_samples: int = 150_000
    """Dressed surface samples ICP matches against (their spacing bounds the
    point-to-point accuracy)."""
    align_frame_samples: int = 40_000
    """Samples used to find feet level / arm band / hand tips for the initial guess."""
    align_max_iterations: int = 60
    align_keep_fraction: float = 0.6
    """Trimmed ICP keeps the best 60% of correspondences each iteration: the
    40% it rejects are the body points under loose clothes and hair."""
    align_normal_min_dot: float = 0.0
    """Correspondences whose normals disagree by more than 90 degrees are rejected."""
    align_inside_tolerance: float = 0.003
    align_inside_weight: float = 0.25
    """A body sample lying more than ``align_inside_tolerance`` inside the
    dressed surface is under clothing: its correspondence gets this weight, so
    tight clothes do not inflate the scale."""
    align_max_rotation_deg: float = 10.0
    """Hunyuan normalises both reconstructions the same way; they differ by at
    most a few degrees, so a larger rotation means ICP went wrong."""
    align_max_scale_change: float = 0.3
    """ICP may not move the scale further than this from the landmark guess."""
    align_scale_starts: Tuple[float, ...] = (1.0, 0.95, 1.05)
    """Multipliers of the landmark scale guess ICP is started from; the start
    with the lowest trimmed residual wins."""
    align_convergence: float = 5e-5
    """Stop when no source point moved more than this (x height) in an iteration."""
    align_inlier_distance: float = 0.015
    """A body sample within this distance of the dressed surface is an inlier."""
    align_max_rms: float = 0.02
    """Trimmed RMS above this fails the decomposition ("alignment residual too high")."""
    align_min_inlier_ratio: float = 0.25
    """Fewer inliers than this fails the decomposition."""

    # ---------------------------------------------------------------- landmarks
    landmark_samples: int = 60_000
    landmark_slices: int = 160
    tpose_min_span: float = 0.6
    """Arm span must be at least this x height (a T-pose span is about 1.0)."""
    tpose_min_band_height: float = 0.55
    """The arm band must be above this fraction of the height (an A-pose's
    widest slab is at hand height, around 0.5)."""
    tpose_max_leg_ratio: float = 0.6
    """Width of the knee/thigh region over the arm span, base body."""
    tpose_max_leg_ratio_dressed: float = 0.85
    """Same for the dressed mesh, which may wear a wide skirt."""

    # ------------------------------------------------------------- segmentation
    body_distance_samples: int = 200_000
    """Area-weighted body surface samples the signed distance is measured against."""
    distance_knn: int = 4
    """Nearest body samples that vote on the inside/outside sign."""
    loose_threshold: float = 0.035
    """Dressed vertices farther than this outside the body are loose (cloth
    candidates). Everything closer, including tight clothing, is ``outer``."""
    hair_threshold: float = 0.02
    """Hair candidates must be at least this far outside the (bald) body."""
    hair_column_head_scale: float = 1.6
    """Half-width of the head/back column hair may grow down: this x head radius ..."""
    hair_column_shoulder_scale: float = 0.9
    """... or this x chest half-width, whichever is larger."""
    hair_min_height: float = 0.25
    """Hair does not grow below this height above the ground."""
    hair_mask_depth_tolerance: float = 0.02
    """A vertex is front-visible (so a hair mask decides it) when it is within
    this distance of the front-most surface at its pixel."""
    hair_mask_zbuffer_max: int = 512
    """Longest side of the visibility z-buffer used with a hair mask."""
    smooth_iterations: int = 2
    """Majority-vote passes over the vertex adjacency graph."""
    island_fraction: float = 0.002
    """Connected label islands with fewer vertices than this fraction are
    absorbed into their surroundings."""
    min_part_fraction: float = 0.002
    """Cloth components smaller than this fraction of the vertices are dropped ..."""
    min_part_vertices: int = 40
    """... as are components with fewer vertices than this ..."""
    min_part_extent: float = 0.04
    """... or whose bounding-box diagonal is below this (x height)."""
    max_loose_fraction: float = 0.8
    """If more of the dressed surface than this is loose, the pair does not
    match (wrong body, failed alignment) and the decomposition fails."""

    # ------------------------------------------------------------------- chains
    joints_per_chain: int = 4
    """Deforming joints per chain; an end joint is added after them."""
    loop_sectors: int = 8
    """Chains around a closed (loop) garment such as a skirt."""
    open_sectors_min: int = 5
    open_sectors_max: int = 7
    open_sector_deg: float = 30.0
    """An open sheet gets one chain per this many degrees of arc, clamped to
    [open_sectors_min, open_sectors_max]."""
    narrow_arc_deg: float = 90.0
    """Open parts narrower than this are ribbons: 1-3 chains."""
    hair_sectors_min: int = 6
    hair_sectors_max: int = 12
    hair_chain_spacing: float = 0.06
    """Hair sectors = circumference / spacing, clamped and made even."""
    loop_min_coverage_deg: float = 300.0
    """Angular coverage around the attach axis from which a part is a loop."""
    coverage_bin_deg: float = 5.0
    min_hang_cloth: float = 0.06
    """A cloth sector needs this much length below its attachment for a chain."""
    min_hang_hair: float = 0.08
    """Same for hair; shorter hair stays rigid on the head."""
    ground_contact_margin: float = 0.03
    """A cloth part around one leg whose lowest point is this close to the
    ground is footwear or a trouser leg and stays rigid."""
    hair_stiff_hang: float = 0.15
    """Hair whose longest chain is shorter than this uses the hair_stiff preset."""
    joint_push_fraction: float = 0.25
    chain_sample_spacing: float = 0.004
    """Chains are measured on area-weighted samples of the part's faces about
    this far apart (2k-60k samples), not on its vertices."""
    """Joints are moved toward the axis by this fraction of the local thickness."""
    min_sector_vertices: int = 6
    attach_min_vertices: int = 6
    """Fewer part vertices than this touching ``outer`` switches the attachment
    to the proximity fallback (the part floats next to the body)."""
    attach_proximity_band: float = 0.012
    """Proximity fallback: part vertices within this much of the closest one."""
    attach_top_percentile: float = 85.0
    """Chain roots and the attach height sit at this percentile of the
    attachment vertices' heights (the top of a contact band, not its middle)."""

    # ------------------------------------------------------------------ weights
    attach_blend: float = 0.25
    max_influences: int = 4

    # ------------------------------------------------------------------ outputs
    body_inner_shrink: float = 0.004
    """body_inner is moved inward along its normals by this much ..."""
    body_inner_margin: float = 0.002
    """... plus however far it pokes out of the dressed surface, plus this ..."""
    body_inner_max_shrink: float = 0.03
    """... but never more than this."""
    debug_joint_radius: float = 0.006
    """Size of the joint markers in parts.glb."""

    def replace(self, **changes: Any) -> "RegenConfig":
        return dataclasses.replace(self, **changes)

    def to_dict(self) -> dict:
        out = dataclasses.asdict(self)
        out["align_scale_starts"] = list(self.align_scale_starts)
        return out

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "RegenConfig":
        """Build a config from a mapping; unknown keys are an error (typos
        must not silently fall back to defaults)."""
        if not values:
            return cls()
        known = {f.name for f in dataclasses.fields(cls)}
        unknown = sorted(set(values) - known)
        if unknown:
            raise ValueError(f"unknown RegenConfig keys: {', '.join(unknown)}")
        clean = dict(values)
        if "align_scale_starts" in clean:
            clean["align_scale_starts"] = tuple(float(v) for v in clean["align_scale_starts"])
        return cls(**clean)


def coerce_config(config: Any) -> RegenConfig:
    if config is None:
        return RegenConfig()
    if isinstance(config, RegenConfig):
        return config
    if isinstance(config, Mapping):
        return RegenConfig.from_mapping(config)
    raise TypeError(f"config must be a RegenConfig or a mapping, not {type(config).__name__}")
