"""Numpy-only core of the AutoRig Regen cloth rig step.

Everything here is independent of Blender so it can be unit-tested with plain
numpy: the decomposition loader, the glTF -> Blender axis conversion, the
similarity ICP, humanoid bone mapping (names first, skeleton geometry as a
fallback), chain/bone naming, angular ordering of chains, collider radii, and
the adapter around ``backend/regen/weights.py``.

``blender_cloth_rig.py`` loads this file BY PATH and registers it in
``sys.modules`` before executing it (see the renderfin gotcha "A Blender script
must not import the server's package"), so it must never import the backend.
"""

from __future__ import annotations

import importlib.util
import json
import math
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np

DECOMPOSITION_FORMAT = "autorig.regen.decomposition"
DECOMPOSITION_VERSION = 1
WEIGHTS_API_VERSION = 1
MAX_BONE_NAME = 60

# glTF is Y-up right-handed; Blender is Z-up right-handed. Blender's glTF
# importer maps a glTF (x, y, z) to Blender (x, -z, y)
# (io_import_scene_gltf2 ``convert_swizzle_location``).
GLTF_TO_BLENDER = np.array([[1.0, 0.0, 0.0], [0.0, 0.0, -1.0], [0.0, 1.0, 0.0]])

NearestFn = Callable[[np.ndarray], "tuple[np.ndarray, np.ndarray]"]


class ClothRigFailure(RuntimeError):
    """A failure the Blender script reports in error.json.

    ``stage`` names the step that failed; ``details`` is JSON-serialisable.
    """

    def __init__(self, stage: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.stage = stage
        self.details = dict(details or {})


class DecompositionError(ClothRigFailure):
    def __init__(self, message: str, details: Mapping[str, Any] | None = None):
        super().__init__("decomposition", message, details)


class AlignmentError(ClothRigFailure):
    def __init__(self, message: str, details: Mapping[str, Any] | None = None):
        super().__init__("alignment", message, details)


# --------------------------------------------------------------------------- loading


def load_module_by_path(module_name: str, path: str | Path):
    """Import a single file as ``module_name`` without touching any package.

    The module is registered in ``sys.modules`` BEFORE ``exec_module`` so that
    dataclasses (which resolve ``sys.modules[cls.__module__]``) work.
    """
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"module file not found: {path}")
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module


@dataclass
class Decomposition:
    directory: Path
    meta: dict
    positions: np.ndarray  # (N, 3) float64, glTF space
    labels: np.ndarray  # (N,) int
    part_index: np.ndarray  # (N,) int, -1 = not a moving part
    values: list[str]
    parts: list[dict]
    groups: list[dict]
    landmarks: dict
    height: float
    warnings: list[str] = field(default_factory=list)

    def part_by_name(self, name: str) -> tuple[int, dict] | None:
        for index, part in enumerate(self.parts):
            if part.get("name") == name:
                return index, part
        return None


def _finite_vec3(value: Any) -> bool:
    try:
        arr = np.asarray(value, dtype=float)
    except (TypeError, ValueError):
        return False
    return arr.shape == (3,) and bool(np.all(np.isfinite(arr)))


def load_decomposition(directory: str | Path) -> Decomposition:
    """Read and validate ``decomposition.json`` + its labels file."""
    directory = Path(directory)
    meta_path = directory / "decomposition.json"
    if not meta_path.is_file():
        raise DecompositionError(f"decomposition.json not found in {directory}")
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DecompositionError(f"decomposition.json is not valid JSON: {exc}") from exc
    if not isinstance(meta, dict):
        raise DecompositionError("decomposition.json must hold an object")
    if meta.get("format") != DECOMPOSITION_FORMAT:
        raise DecompositionError(
            f"decomposition format is {meta.get('format')!r}, expected {DECOMPOSITION_FORMAT!r}"
        )
    version = meta.get("version")
    if not isinstance(version, int) or version != DECOMPOSITION_VERSION:
        raise DecompositionError(f"unsupported decomposition version {version!r}")
    warnings: list[str] = []
    if meta.get("space") != "dressed_glb":
        warnings.append(f"decomposition space is {meta.get('space')!r}; treating it as glTF Y-up")

    labels_meta = meta.get("labels")
    if not isinstance(labels_meta, dict):
        raise DecompositionError("decomposition.labels must be an object")
    values = labels_meta.get("values")
    if not isinstance(values, list) or not values or not all(isinstance(v, str) for v in values):
        raise DecompositionError("decomposition.labels.values must be a non-empty list of strings")
    labels_file = labels_meta.get("file") or "labels.npz"
    labels_path = (directory / str(labels_file)).resolve()
    if directory.resolve() not in labels_path.parents:
        raise DecompositionError(f"labels file {labels_file!r} escapes the decomposition directory")
    if not labels_path.is_file():
        raise DecompositionError(f"labels file not found: {labels_path}")
    try:
        with np.load(labels_path, allow_pickle=False) as npz:
            positions = np.asarray(npz["positions"], dtype=np.float64)
            labels = np.asarray(npz["labels"]).astype(np.int64)
            part_index = np.asarray(npz["part_index"]).astype(np.int64)
    except KeyError as exc:
        raise DecompositionError(f"labels file lacks array {exc}") from exc
    except (OSError, ValueError) as exc:
        raise DecompositionError(f"cannot read labels file: {exc}") from exc
    if positions.ndim != 2 or positions.shape[1] != 3 or len(positions) < 16:
        raise DecompositionError(f"labels positions must be (N>=16, 3), got {positions.shape}")
    if labels.shape != (len(positions),) or part_index.shape != (len(positions),):
        raise DecompositionError("labels/part_index must have one entry per position")
    if not np.all(np.isfinite(positions)):
        raise DecompositionError("labels positions contain non-finite values")
    if labels.min() < 0 or labels.max() >= len(values):
        raise DecompositionError("labels index outside labels.values")

    parts = meta.get("parts") or []
    groups = meta.get("groups") or []
    if not isinstance(parts, list) or not all(isinstance(p, dict) for p in parts):
        raise DecompositionError("decomposition.parts must be a list of objects")
    if not isinstance(groups, list) or not all(isinstance(g, dict) for g in groups):
        raise DecompositionError("decomposition.groups must be a list of objects")
    if part_index.min() < -1 or part_index.max() >= len(parts):
        raise DecompositionError("part_index outside decomposition.parts")
    for g_index, group in enumerate(groups):
        chains = group.get("chains") or []
        if not isinstance(chains, list):
            raise DecompositionError(f"groups[{g_index}].chains must be a list")
        for c_index, chain in enumerate(chains):
            joints = chain.get("joints") if isinstance(chain, dict) else None
            if not isinstance(joints, list) or not all(_finite_vec3(j) for j in joints):
                raise DecompositionError(
                    f"groups[{g_index}].chains[{c_index}].joints must be a list of finite [x, y, z]"
                )

    height = meta.get("height")
    extent = float(np.ptp(positions[:, 1]))
    if not isinstance(height, (int, float)) or not height > 0:
        height = extent
    elif extent > 0 and abs(extent - float(height)) > 0.15 * extent:
        warnings.append(
            f"decomposition height {height} differs from the labels' Y extent {extent:.4f}"
        )
    landmarks = meta.get("landmarks") if isinstance(meta.get("landmarks"), dict) else {}
    return Decomposition(
        directory=directory,
        meta=meta,
        positions=positions,
        labels=labels,
        part_index=part_index,
        values=[str(v) for v in values],
        parts=parts,
        groups=groups,
        landmarks=landmarks,
        height=float(height),
        warnings=warnings,
    )


# --------------------------------------------------------------------------- geometry


def gltf_to_blender(points: Any) -> np.ndarray:
    """glTF Y-up -> Blender Z-up, exactly as Blender's glTF importer does."""
    arr = np.asarray(points, dtype=np.float64)
    return arr @ GLTF_TO_BLENDER.T


def blender_to_gltf(points: Any) -> np.ndarray:
    arr = np.asarray(points, dtype=np.float64)
    return arr @ GLTF_TO_BLENDER


def rotation_z(degrees: float) -> np.ndarray:
    a = math.radians(degrees)
    c, s = math.cos(a), math.sin(a)
    return np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])


def similarity_matrix(scale: float, rotation: np.ndarray, translation: np.ndarray) -> np.ndarray:
    m = np.eye(4)
    m[:3, :3] = scale * np.asarray(rotation, dtype=float)
    m[:3, 3] = np.asarray(translation, dtype=float)
    return m


def apply_similarity(scale: float, rotation: np.ndarray, translation: np.ndarray, points: Any) -> np.ndarray:
    pts = np.asarray(points, dtype=np.float64)
    return scale * pts @ np.asarray(rotation).T + np.asarray(translation)


def umeyama(src: np.ndarray, dst: np.ndarray, with_scale: bool = True) -> tuple[float, np.ndarray, np.ndarray]:
    """Least-squares similarity (s, R, t) with dst ~= s * R @ src + t (Umeyama 1991)."""
    src = np.asarray(src, dtype=np.float64)
    dst = np.asarray(dst, dtype=np.float64)
    if src.shape != dst.shape or src.ndim != 2 or src.shape[1] != 3 or len(src) < 3:
        raise ValueError("umeyama needs two (N>=3, 3) arrays of equal shape")
    mu_s = src.mean(axis=0)
    mu_d = dst.mean(axis=0)
    xs = src - mu_s
    xd = dst - mu_d
    cov = xd.T @ xs / len(src)
    u, d, vt = np.linalg.svd(cov)
    sign = np.ones(3)
    if np.linalg.det(u) * np.linalg.det(vt) < 0:
        sign[2] = -1.0
    rotation = u @ np.diag(sign) @ vt
    var_s = float((xs ** 2).sum() / len(src))
    scale = float((d * sign).sum() / var_s) if (with_scale and var_s > 0) else 1.0
    translation = mu_d - scale * rotation @ mu_s
    return scale, rotation, translation


def brute_force_nearest(points: Any, block: int = 256) -> NearestFn:
    """Exact nearest neighbour by blocked brute force (tests, small inputs)."""
    ref = np.asarray(points, dtype=np.float64)
    ref_sq = (ref ** 2).sum(axis=1)

    def query(q: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        q = np.asarray(q, dtype=np.float64)
        idx = np.empty(len(q), dtype=np.int64)
        for start in range(0, len(q), block):
            chunk = q[start : start + block]
            d2 = (chunk ** 2).sum(axis=1)[:, None] + ref_sq[None, :] - 2.0 * chunk @ ref.T
            idx[start : start + block] = np.argmin(d2, axis=1)
        nearest = ref[idx]
        return nearest, np.linalg.norm(nearest - q, axis=1)

    return query


def subsample(points: np.ndarray, count: int, rng: np.random.Generator) -> np.ndarray:
    if len(points) <= count:
        return np.asarray(points, dtype=np.float64)
    pick = rng.choice(len(points), size=count, replace=False)
    return np.asarray(points[np.sort(pick)], dtype=np.float64)


@dataclass
class IcpResult:
    scale: float
    rotation: np.ndarray
    translation: np.ndarray
    forward_rms: float
    reverse_rms: float
    forward_median: float
    reverse_median: float
    iterations: int
    converged: bool
    yaw_init_deg: float

    @property
    def matrix(self) -> np.ndarray:
        return similarity_matrix(self.scale, self.rotation, self.translation)

    def rotation_angle_deg(self) -> float:
        cos = (np.trace(self.rotation) - 1.0) / 2.0
        return float(math.degrees(math.acos(max(-1.0, min(1.0, cos)))))


def _trim(dist: np.ndarray, fraction: float) -> np.ndarray:
    keep = max(3, int(round(len(dist) * fraction)))
    if keep >= len(dist):
        return np.arange(len(dist))
    return np.argpartition(dist, keep - 1)[:keep]


def _robust_bounds(points: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Bounding box ignoring the outermost 0.2% per axis (stray fragments)."""
    return np.percentile(points, 0.2, axis=0), np.percentile(points, 99.8, axis=0)


def bbox_init(src: np.ndarray, dst: np.ndarray, yaw_deg: float = 0.0) -> tuple[float, np.ndarray, np.ndarray]:
    """Initial similarity: optional yaw, heights (Z extents) matched, bbox centres matched."""
    rotation = rotation_z(yaw_deg)
    rotated = src @ rotation.T
    lo_s, hi_s = _robust_bounds(rotated)
    lo_d, hi_d = _robust_bounds(dst)
    h_src = float(hi_s[2] - lo_s[2])
    h_dst = float(hi_d[2] - lo_d[2])
    if h_src <= 0 or h_dst <= 0:
        raise AlignmentError("cannot initialise alignment: a point set has zero height")
    scale = h_dst / h_src
    c_src = (lo_s + hi_s) / 2.0
    c_dst = (lo_d + hi_d) / 2.0
    return scale, rotation, c_dst - scale * c_src


def similarity_icp(
    src_forward: np.ndarray,
    dst_reverse: np.ndarray,
    nearest_dst: NearestFn,
    nearest_src: NearestFn,
    init: tuple[float, np.ndarray, np.ndarray],
    *,
    trim: float = 0.85,
    max_iter: int = 60,
    tol: float = 1e-7,
    yaw_init_deg: float = 0.0,
    abort_turn_deg: float | None = None,
) -> IcpResult:
    """Symmetric trimmed similarity ICP.

    Forward pairs: each source point -> nearest target point. Reverse pairs:
    each target sample -> nearest source point (queried in the source frame,
    so the source tree never has to be rebuilt). Using both directions stops
    the uniform scale from collapsing onto a patch of the target, which a
    one-sided trimmed ICP with scale is prone to. ``abort_turn_deg`` stops
    early once the rotation has left the initial orientation by that much.
    """
    scale, rotation, translation = init
    base_scale = scale
    base_rotation = np.asarray(rotation)
    prev_err = math.inf
    converged = False
    iterations = 0
    for iterations in range(1, max_iter + 1):
        moved = apply_similarity(scale, rotation, translation, src_forward)
        near_dst, dist_f = nearest_dst(moved)
        keep_f = _trim(dist_f, trim)
        back = ((dst_reverse - translation) @ rotation) / scale
        near_src, dist_r = nearest_src(back)
        dist_r = dist_r * scale
        keep_r = _trim(dist_r, trim)
        pair_src = np.concatenate([src_forward[keep_f], near_src[keep_r]])
        pair_dst = np.concatenate([near_dst[keep_f], dst_reverse[keep_r]])
        err = float(np.mean(np.concatenate([dist_f[keep_f], dist_r[keep_r]]) ** 2))
        scale, rotation, translation = umeyama(pair_src, pair_dst)
        if not (0.2 * base_scale < scale < 5.0 * base_scale):
            break  # diverged; the residual check below reports it
        if abort_turn_deg is not None and _angle_deg(rotation @ base_rotation.T) > abort_turn_deg:
            break  # turning away from the expected orientation; the caller rejects it
        if prev_err < math.inf and abs(prev_err - err) <= tol * max(prev_err, 1e-30):
            converged = True
            break
        prev_err = err

    moved = apply_similarity(scale, rotation, translation, src_forward)
    _, dist_f = nearest_dst(moved)
    back = ((dst_reverse - translation) @ rotation) / scale
    _, dist_r = nearest_src(back)
    dist_r = dist_r * scale
    keep_f = _trim(dist_f, trim)
    keep_r = _trim(dist_r, trim)
    return IcpResult(
        scale=float(scale),
        rotation=rotation,
        translation=translation,
        forward_rms=float(np.sqrt(np.mean(dist_f[keep_f] ** 2))),
        reverse_rms=float(np.sqrt(np.mean(dist_r[keep_r] ** 2))),
        forward_median=float(np.median(dist_f)),
        reverse_median=float(np.median(dist_r)),
        iterations=iterations,
        converged=converged,
        yaw_init_deg=yaw_init_deg,
    )


@dataclass
class AlignmentResult:
    icp: IcpResult
    height: float
    threshold_rel: float
    residual_rel: float
    candidates: list[dict]
    warnings: list[str]
    max_rotation_deg: float = 20.0
    left_check_applied: bool = False

    def to_report(self) -> dict:
        icp = self.icp
        return {
            "method": "bbox-initialised symmetric trimmed similarity ICP (Umeyama, uniform scale)",
            "maps": "Blender world = matrix @ gltf_to_blender(decomposition point)",
            "scale": icp.scale,
            "rotation_deg": icp.rotation_angle_deg(),
            "translation": [float(v) for v in icp.translation],
            "matrix_row_major": [float(v) for v in icp.matrix.reshape(-1)],
            "yaw_init_deg": icp.yaw_init_deg,
            "residual_rel": self.residual_rel,
            "threshold_rel": self.threshold_rel,
            "max_rotation_deg": self.max_rotation_deg,
            "left_right_check": self.left_check_applied,
            "forward_rms": icp.forward_rms,
            "reverse_rms": icp.reverse_rms,
            "forward_median": icp.forward_median,
            "reverse_median": icp.reverse_median,
            "height": self.height,
            "iterations": icp.iterations,
            "converged": icp.converged,
            "candidates": self.candidates,
        }


def _angle_deg(rotation: np.ndarray) -> float:
    cos = (np.trace(rotation) - 1.0) / 2.0
    return float(math.degrees(math.acos(max(-1.0, min(1.0, cos)))))


def align_similarity(
    src_points: np.ndarray,
    dst_points: np.ndarray,
    *,
    nearest_factory: Callable[[np.ndarray], NearestFn],
    height: float | None = None,
    max_residual_rel: float = 0.02,
    max_rotation_deg: float = 20.0,
    trim: float = 0.85,
    n_forward: int = 6000,
    n_reverse: int = 6000,
    n_source_tree: int = 40000,
    seed: int = 0,
    yaw_candidates: Sequence[float] = (0.0, 180.0, 90.0, -90.0),
    max_iter: int = 60,
    left_check: tuple[Any, Any] | None = None,
) -> AlignmentResult:
    """Fit dst ~= s R src + t; both sets are in Blender Z-up.

    The rigged model is a re-scaled, re-centred copy of the dressed mesh, so
    a candidate is accepted only if its residual is under
    ``max_residual_rel`` of the character height AND the ICP rotated it by at
    most ``max_rotation_deg`` away from its initial yaw: a fit that needs a
    large rotation means the decomposition is in another space (for example
    Z-up data labelled Y-up), and that must fail loudly, not be absorbed.
    The identity orientation is tried first; the other yaw candidates are
    only tried when it fails, and a result from them carries a warning.

    A character is nearly front/back symmetric, so a 180 degree turn can fit
    almost as well as the truth. ``left_check = (source_left, target_left)``
    (the decomposition's left direction from its shoulder landmarks, and the
    rig's from its named L/R bones) makes a candidate acceptable only when it
    maps one onto the other. Raises ``AlignmentError`` when no candidate is
    accepted.
    """
    check_src = check_dst = None
    if left_check is not None:
        up = np.array([0.0, 0.0, 1.0])
        check_src = _unit(_horizontal(np.asarray(left_check[0], dtype=np.float64), up))
        check_dst = _unit(_horizontal(np.asarray(left_check[1], dtype=np.float64), up))
        if check_src is None or check_dst is None:
            check_src = check_dst = None
    src_points = np.asarray(src_points, dtype=np.float64)
    dst_points = np.asarray(dst_points, dtype=np.float64)
    if height is None or not height > 0:
        height = float(np.ptp(dst_points[:, 2]))
    if not height > 0:
        raise AlignmentError("rigged model has zero height")
    rng = np.random.default_rng(seed)
    src_forward = subsample(src_points, n_forward, rng)
    src_tree = subsample(src_points, n_source_tree, rng)
    dst_reverse = subsample(dst_points, n_reverse, rng)
    coarse_forward = subsample(src_forward, min(1500, n_forward), rng)
    coarse_reverse = subsample(dst_reverse, min(1500, n_reverse), rng)
    nearest_dst = nearest_factory(dst_points)
    nearest_src = nearest_factory(src_tree)

    candidates: list[dict] = []
    accepted: list[tuple[float, IcpResult]] = []
    for yaw in yaw_candidates:
        init = bbox_init(src_points, dst_points, yaw)
        # Coarse pass on few points screens the candidate; only a promising one
        # is refined on the full samples, starting where the coarse pass ended.
        result = similarity_icp(
            coarse_forward, coarse_reverse, nearest_dst, nearest_src, init, trim=trim,
            max_iter=max_iter, tol=1e-5, yaw_init_deg=float(yaw), abort_turn_deg=max_rotation_deg + 40.0,
        )
        stage = "coarse"
        rel = max(result.forward_rms, result.reverse_rms) / height
        turned = _angle_deg(result.rotation @ init[1].T)
        facing = None if check_src is None else float((result.rotation @ check_src) @ check_dst)
        if (rel <= 3.0 * max_residual_rel and turned <= max_rotation_deg + 10.0
                and (facing is None or facing > 0.5)):
            result = similarity_icp(
                src_forward, dst_reverse, nearest_dst, nearest_src,
                (result.scale, result.rotation, result.translation),
                trim=trim, max_iter=max_iter, tol=1e-6, yaw_init_deg=float(yaw),
            )
            stage = "fine"
            rel = max(result.forward_rms, result.reverse_rms) / height
            turned = _angle_deg(result.rotation @ init[1].T)
            facing = None if check_src is None else float((result.rotation @ check_src) @ check_dst)
        ok = (stage == "fine" and rel <= max_residual_rel and turned <= max_rotation_deg
              and (facing is None or facing > 0.5))
        candidates.append({
            "yaw_init_deg": float(yaw),
            "residual_rel": rel,
            "rotation_from_init_deg": turned,
            "left_agreement": facing,
            "scale": result.scale,
            "rotation_deg": result.rotation_angle_deg(),
            "iterations": result.iterations,
            "stage": stage,
            "accepted": ok,
        })
        if ok:
            accepted.append((rel, result))
            if yaw == yaw_candidates[0]:
                break
    if not accepted:
        best = min(candidates, key=lambda c: c["residual_rel"])
        tried = ", ".join(
            f"yaw {c['yaw_init_deg']:+.0f}deg -> residual {100 * c['residual_rel']:.2f}%, "
            f"rotation {c['rotation_from_init_deg']:.1f}deg" for c in candidates
        )
        geometric = [c for c in candidates
                     if c["residual_rel"] <= max_residual_rel and c["rotation_from_init_deg"] <= max_rotation_deg]
        if geometric:
            why = ("every fit that matches the geometry puts the decomposition's left side (shoulder "
                   "landmarks) on the rig's right side (named L/R bones); the rig or the decomposition "
                   f"is mirrored or turned around (closest residual {100 * best['residual_rel']:.2f}% of "
                   "character height)")
        elif all(c["rotation_from_init_deg"] > max_rotation_deg for c in candidates):
            why = (f"every orientation tried had to rotate by more than the {max_rotation_deg:.0f} degree limit "
                   "for a re-scaled copy of the same model (closest residual "
                   f"{100 * best['residual_rel']:.2f}% of character height)")
        elif best["residual_rel"] <= max_residual_rel:
            why = (f"the closest fit (residual {100 * best['residual_rel']:.2f}% of character height) needs a "
                   f"{best['rotation_from_init_deg']:.1f} degree rotation, above the {max_rotation_deg:.0f} degree "
                   "limit for a re-scaled copy of the same model")
        else:
            why = (f"best residual {100 * best['residual_rel']:.2f}% of character height exceeds the "
                   f"{100 * max_residual_rel:.2f}% limit")
        raise AlignmentError(
            f"cannot align the decomposition to the rigged model: {why} ({tried}). The decomposition "
            "probably does not belong to this model or is not in glTF Y-up dressed-GLB space.",
            {"candidates": candidates, "height": height, "threshold_rel": max_residual_rel,
             "max_rotation_deg": max_rotation_deg},
        )
    rel, best = min(accepted, key=lambda item: item[0])
    warnings: list[str] = []
    if best.yaw_init_deg != yaw_candidates[0]:
        warnings.append(
            f"identity orientation failed; decomposition aligned after a {best.yaw_init_deg:+.0f} degree "
            "turn about the vertical axis"
        )
    return AlignmentResult(
        icp=best, height=height, threshold_rel=max_residual_rel, residual_rel=rel,
        candidates=candidates, warnings=warnings, max_rotation_deg=max_rotation_deg,
        left_check_applied=check_src is not None,
    )


def point_segment_distance(points: np.ndarray, a: Any, b: Any) -> tuple[np.ndarray, np.ndarray]:
    """Distance of every point to segment [a, b] and the clamped parameter t."""
    pts = np.asarray(points, dtype=np.float64)
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    ab = b - a
    denom = float(ab @ ab)
    if denom <= 1e-18:
        t = np.zeros(len(pts))
    else:
        t = np.clip((pts - a) @ ab / denom, 0.0, 1.0)
    closest = a + t[:, None] * ab
    return np.linalg.norm(pts - closest, axis=1), t


def robust_radius(distances: np.ndarray, percentile: float = 60.0) -> float:
    if len(distances) == 0:
        return 0.0
    return float(np.percentile(np.asarray(distances, dtype=np.float64), percentile))


def capsule_radii(
    points: np.ndarray, a: Any, b: Any, *, percentile: float = 60.0, min_count: int = 8
) -> tuple[float, float, int]:
    """Radius near ``a`` and near ``b`` of a capsule fitted to ``points``.

    Each end uses the points whose projection falls on its half of the
    segment; a half with too few points uses the whole set.
    """
    if len(points) == 0:
        return 0.0, 0.0, 0
    dist, t = point_segment_distance(points, a, b)
    overall = robust_radius(dist, percentile)
    near_a = dist[t <= 0.5]
    near_b = dist[t >= 0.5]
    r_a = robust_radius(near_a, percentile) if len(near_a) >= min_count else overall
    r_b = robust_radius(near_b, percentile) if len(near_b) >= min_count else overall
    return r_a, r_b, len(points)


def angular_order(
    points: np.ndarray, centre: Any, axis: Any, reference: Any
) -> tuple[np.ndarray, np.ndarray]:
    """Order of points by angle around ``axis`` through ``centre``.

    Angle 0 is ``reference`` (projected); angles grow counter-clockwise seen
    from the tip of ``axis``. Returns (order, angles in [0, 2pi)).
    """
    axis = np.asarray(axis, dtype=np.float64)
    axis = axis / (np.linalg.norm(axis) or 1.0)
    ref = np.asarray(reference, dtype=np.float64)
    ref = ref - axis * (ref @ axis)
    if np.linalg.norm(ref) < 1e-9:
        helper = np.array([1.0, 0.0, 0.0]) if abs(axis[0]) < 0.9 else np.array([0.0, 1.0, 0.0])
        ref = helper - axis * (helper @ axis)
    ref = ref / np.linalg.norm(ref)
    ortho = np.cross(axis, ref)
    rel = np.asarray(points, dtype=np.float64) - np.asarray(centre, dtype=np.float64)
    angles = np.mod(np.arctan2(rel @ ortho, rel @ ref), 2.0 * math.pi)
    return np.argsort(angles, kind="stable"), angles


def order_chains_angular(
    roots: np.ndarray, connection: str, centre: Any, axis: Any, reference: Any
) -> list[int]:
    """Permutation putting ``open``/``loop`` chains in angular order.

    ``loop`` starts at the chain nearest ``reference`` (the character's
    front); ``open`` starts after the largest angular gap so the open side of
    a cape is between the last and the first chain. ``none`` keeps the order.
    """
    count = len(roots)
    if connection not in ("open", "loop") or count < 2:
        return list(range(count))
    order, angles = angular_order(roots, centre, axis, reference)
    order = [int(i) for i in order]
    sorted_angles = angles[order]
    if connection == "open":
        gaps = np.diff(np.concatenate([sorted_angles, [sorted_angles[0] + 2.0 * math.pi]]))
        start = (int(np.argmax(gaps)) + 1) % count
    else:  # loop: begin with the chain nearest the reference, wherever the 0/2pi seam falls
        start = int(np.argmin(np.minimum(sorted_angles, 2.0 * math.pi - sorted_angles)))
    return order[start:] + order[:start]


# --------------------------------------------------------------------------- naming


_IDENT_BAD = re.compile(r"[^A-Za-z0-9_]+")


def sanitize_identifier(text: Any, fallback: str = "group") -> str:
    """ASCII ``[A-Za-z0-9_]`` identifier (accents folded, runs collapsed)."""
    folded = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"_+", "_", _IDENT_BAD.sub("_", folded)).strip("_")
    return cleaned or fallback


def chain_bone_names(group: str, chain_index: int, deform_count: int, chain_width: int = 2) -> list[str]:
    """``<group>_<cc>_<k>`` for the deforming bones plus ``<group>_<cc>_end``."""
    prefix = f"{group}_{chain_index:0{chain_width}d}"
    return [f"{prefix}_{k}" for k in range(deform_count)] + [f"{prefix}_end"]


def _name_keys(name: str) -> set[str]:
    stripped = re.split(r"[:|]", name)[-1]
    return {name.lower(), stripped.lower()}


def plan_group_names(
    requested: Sequence[str],
    chain_joint_counts: Sequence[Sequence[int]],
    existing_bones: Iterable[str],
    *,
    max_len: int = MAX_BONE_NAME,
) -> list[dict]:
    """Unique ASCII group names whose bone names fit ``max_len`` and clash with nothing.

    ``chain_joint_counts[g]`` lists the joint count of every chain of group g.
    Returns per group ``{"name", "chain_width", "bones": [[names...], ...]}``.
    """
    taken: set[str] = set()
    for bone in existing_bones:
        taken |= _name_keys(bone)
    used_groups: set[str] = set()
    plans: list[dict] = []
    for raw, joint_counts in zip(requested, chain_joint_counts):
        chain_width = max(2, len(str(max(len(joint_counts) - 1, 0))))
        max_deform = max([j - 1 for j in joint_counts] or [1])
        tail_len = 1 + chain_width + 1 + max(3, len(str(max(max_deform - 1, 0))))
        base = sanitize_identifier(raw)
        base = base[: max(1, max_len - tail_len - 4)].rstrip("_") or "g"
        candidate = base
        serial = 2
        while True:
            names = [
                chain_bone_names(candidate, c, max(1, j - 1), chain_width)
                for c, j in enumerate(joint_counts)
            ]
            flat = [n for chain in names for n in chain]
            clash = candidate.lower() in used_groups or any(n.lower() in taken for n in flat)
            if not clash:
                break
            candidate = f"{base}_{serial}"
            serial += 1
        used_groups.add(candidate.lower())
        for n in flat:
            taken.add(n.lower())
        plans.append({"name": candidate, "chain_width": chain_width, "bones": names})
    return plans


# --------------------------------------------------------------------------- bone mapping


SLOTS = (
    "hips", "spine", "chest", "upper_chest", "neck", "head",
    "shoulder_l", "upper_arm_l", "lower_arm_l", "hand_l",
    "shoulder_r", "upper_arm_r", "lower_arm_r", "hand_r",
    "upper_leg_l", "lower_leg_l", "foot_l", "toe_l",
    "upper_leg_r", "lower_leg_r", "foot_r", "toe_r",
)
_CENTER_SLOTS = ("hips", "spine", "chest", "upper_chest", "neck", "head")

# normalised base name -> slot kind. Ordered: earlier synonyms win ties.
_CENTER_SYNONYMS: dict[str, tuple[str, ...]] = {
    "hips": ("hips", "hip", "pelvis"),
    "chest": ("chest",),
    "upper_chest": ("upperchest",),
    "neck": ("neck", "neck1", "neck01", "neck0", "necktwist01", "necktwist1"),
    "head": ("head",),
}
_SIDED_SYNONYMS: dict[str, tuple[str, ...]] = {
    "shoulder": ("shoulder", "clavicle", "collar", "collarbone"),
    "upper_arm": ("upperarm", "arm", "uparm", "armstretch", "armupper"),
    "lower_arm": ("forearm", "lowerarm", "forearmstretch", "elbow", "armlower"),
    "hand": ("hand", "wrist"),
    "upper_leg": ("upleg", "upperleg", "thigh", "thighstretch", "legupper"),
    "lower_leg": ("leg", "lowerleg", "calf", "shin", "knee", "legstretch", "leglower"),
    "foot": ("foot", "ankle"),
    "toe": ("toebase", "toe", "toes", "toes01", "toe01", "toe0", "ball"),
}
# Rigify DEF chain (flavour detected from the DEF-spine.00N bones).
_RIGIFY_CENTER = {
    ("spine", 0): "hips", ("spine", 1): "spine", ("spine", 2): "chest",
    ("spine", 3): "upper_chest", ("spine", 4): "neck",
}
_RIGIFY_SIDED = {
    "shoulder": "shoulder", "upperarm": "upper_arm", "forearm": "lower_arm", "hand": "hand",
    "thigh": "upper_leg", "shin": "lower_leg", "foot": "foot", "toe": "toe",
}
_RIG_PREFIX_TOKENS = {"cc", "base", "rig", "armature", "skeleton", "b", "bn", "jnt", "j"}
_CONTROL_PREFIXES = {"org", "mch", "ctrl", "ctl", "drv", "c"}
_CAMEL = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+")


@dataclass(frozen=True)
class ParsedName:
    raw: str
    base: str
    side: str  # "l", "r", "c" (explicit centre, e.g. Auto-Rig Pro ".x"), or ""
    prefix: str  # "def", "org", "mch", "c", ... or ""
    dup: int  # Blender duplicate suffix ".001" -> 1


def parse_bone_name(name: str) -> ParsedName:
    """Split a bone name into (base, side, prefix, dup), all normalised.

    Handles ``mixamorig:LeftUpLeg``, ``Armature|Hips``, ``thigh_l``,
    ``DEF-thigh.L.001``, ``thigh_stretch.l``/``root.x`` (Auto-Rig Pro),
    ``Bip01 L Thigh``, ``CC_Base_L_Thigh``, ``UpperLeg.L``.
    """
    text = re.split(r"[:|]", name)[-1].strip()
    prefix = ""
    match = re.match(r"^(def|org|mch|ctrl|ctl|drv|c)[-_.](?=.)", text, re.IGNORECASE)
    if match:
        prefix = match.group(1).lower()
        text = text[match.end():]
    dup = 0
    match = re.search(r"\.(\d{3})$", text)
    if match:
        dup = int(match.group(1))
        text = text[: match.start()]
    side = ""
    match = re.search(r"(left|right)", text, re.IGNORECASE)
    if match:
        side = "l" if match.group(1).lower() == "left" else "r"
        text = text[: match.start()] + " " + text[match.end():]
    else:
        match = re.search(r"[._\- ]([lLrRxX])(?:[._]\d+)?$", text)
        if match:
            letter = match.group(1).lower()
            side = "c" if letter == "x" else letter
            text = text[: match.start()]
        else:
            # A side token between separators: "Bip01 L Thigh", "CC_Base_L_Thigh",
            # VRoid "J_Bip_L_UpperLeg" / "J_Bip_C_Hips" (C = centre).
            match = re.search(r"(?:^|[._\- ])([lLrRcC])(?:[._\- ])", text)
            if match:
                letter = match.group(1).lower()
                side = "c" if letter == "c" else letter
                text = text[: match.start()] + " " + text[match.end():]
    parts = [part for part in re.split(r"[._\-\s]+", text) if part]
    while parts and (
        parts[0].lower() in _RIG_PREFIX_TOKENS
        or parts[0].lower().startswith("mixamorig")
        or re.fullmatch(r"bip\d*", parts[0].lower())
    ):
        parts.pop(0)
    tokens = [tok.lower() for part in parts for tok in _CAMEL.findall(part)]
    return ParsedName(raw=name, base="".join(tokens), side=side, prefix=prefix, dup=dup)


@dataclass
class BoneInfo:
    name: str
    parent: str | None
    head: np.ndarray
    tail: np.ndarray
    use_deform: bool = True
    weight_count: int = 0


@dataclass
class BoneMapping:
    slots: dict[str, str]
    methods: dict[str, str]
    left_axis: np.ndarray
    left_axis_source: str
    flavour: str
    warnings: list[str]

    def get(self, slot: str) -> str | None:
        return self.slots.get(slot)

    def to_report(self) -> dict:
        return {
            "flavour": self.flavour,
            "left_axis": [float(v) for v in self.left_axis],
            "left_axis_source": self.left_axis_source,
            "slots": {slot: {"bone": self.slots[slot], "method": self.methods[slot]}
                      for slot in SLOTS if slot in self.slots},
            "missing": [slot for slot in SLOTS if slot not in self.slots],
        }


class _Skeleton:
    def __init__(self, bones: Sequence[BoneInfo]):
        self.bones = {b.name: b for b in bones}
        self.children: dict[str | None, list[str]] = {}
        for b in bones:
            parent = b.parent if b.parent in self.bones else None
            self.children.setdefault(parent, []).append(b.name)
        self.depth: dict[str, int] = {}
        for b in bones:
            depth, cursor, guard = 0, b.parent, 0
            while cursor in self.bones and guard < 10000:
                depth += 1
                cursor = self.bones[cursor].parent
                guard += 1
            self.depth[b.name] = depth

    def ancestors(self, name: str) -> list[str]:
        out, cursor = [], self.bones[name].parent
        while cursor in self.bones and len(out) < 10000:
            out.append(cursor)
            cursor = self.bones[cursor].parent
        return out

    def is_ancestor(self, ancestor: str, name: str) -> bool:
        return ancestor in self.ancestors(name)

    def path(self, top: str, bottom: str) -> list[str] | None:
        """Bones strictly between ``top`` and descendant ``bottom``, top-down."""
        chain = self.ancestors(bottom)
        if top not in chain:
            return None
        return list(reversed(chain[: chain.index(top)]))

    def descendants(self, name: str) -> list[str]:
        out, stack = [], list(self.children.get(name, []))
        while stack:
            current = stack.pop()
            out.append(current)
            stack.extend(self.children.get(current, []))
        return out

    def lca(self, names: Sequence[str]) -> str | None:
        paths = [[n] + self.ancestors(n) for n in names]
        common = set(paths[0]).intersection(*map(set, paths[1:]))
        for candidate in paths[0]:
            if candidate in common:
                return candidate
        return None


def _horizontal(vec: np.ndarray, up: np.ndarray) -> np.ndarray:
    return vec - up * float(vec @ up)


def _unit(vec: np.ndarray) -> np.ndarray | None:
    norm = float(np.linalg.norm(vec))
    return vec / norm if norm > 1e-9 else None


def map_humanoid(
    bones: Sequence[BoneInfo],
    *,
    up: Any = (0.0, 0.0, 1.0),
    left_hint: Any = None,
) -> BoneMapping:
    """Map semantic humanoid slots to bone names.

    Names first (Mixamo with or without namespace, Unreal/Unity, Auto-Rig
    Pro, Rigify DEF, Biped, CC, generic ``UpperLeg.L``); the spine between
    hips and neck from the hierarchy; whatever is still missing from the
    skeleton's geometry (legs descend from the hips, arms extend sideways,
    the head tops the spine). ``left_hint`` (a vector pointing to the
    character's left, e.g. from landmarks) only matters when no named pair of
    left/right limbs exists.
    """
    up = np.asarray(up, dtype=np.float64)
    up = up / np.linalg.norm(up)
    skel = _Skeleton(bones)
    parsed = {b.name: parse_bone_name(b.name) for b in bones}
    warnings: list[str] = []
    slots: dict[str, str] = {}
    methods: dict[str, str] = {}

    rigify = any(p.prefix == "def" and p.base == "spine" for p in parsed.values())
    flavour = "rigify" if rigify else "generic"
    if any(p.side == "c" for p in parsed.values()) and any(
        p.base in ("thighstretch", "armstretch") for p in parsed.values()
    ):
        flavour = "auto-rig-pro"
    elif any(b.name.lower().startswith("mixamorig") for b in bones):
        flavour = "mixamo"

    def rank(name: str, synonym_index: int) -> tuple:
        info, p = skel.bones[name], parsed[name]
        return (
            0 if info.use_deform else 1,
            0 if info.weight_count > 0 else 1,
            1 if p.prefix in _CONTROL_PREFIXES else 0,
            p.dup,
            synonym_index,
            skel.depth[name],
            name,
        )

    candidates: dict[str, list[tuple[tuple, str]]] = {slot: [] for slot in SLOTS}
    for name, p in parsed.items():
        if rigify:
            if p.prefix != "def":
                continue
            if not p.side and (p.base, p.dup) in _RIGIFY_CENTER:
                slot = _RIGIFY_CENTER[(p.base, p.dup)]
                candidates[slot].append((rank(name, 0), name))
            elif p.side in ("l", "r") and p.dup == 0 and p.base in _RIGIFY_SIDED:
                slot = f"{_RIGIFY_SIDED[p.base]}_{p.side}"
                candidates[slot].append((rank(name, 0), name))
            continue
        if p.side in ("", "c"):
            for slot, synonyms in _CENTER_SYNONYMS.items():
                if p.base in synonyms:
                    candidates[slot].append((rank(name, synonyms.index(p.base)), name))
            if p.base == "root" and p.side == "c":  # Auto-Rig Pro "root.x" is the hips
                candidates["hips"].append((rank(name, 10), name))
        elif p.side in ("l", "r"):
            for kind, synonyms in _SIDED_SYNONYMS.items():
                if p.base in synonyms:
                    candidates[f"{kind}_{p.side}"].append((rank(name, synonyms.index(p.base)), name))
    if rigify:
        heads = [(p.dup, n) for n, p in parsed.items() if p.prefix == "def" and p.base == "spine" and not p.side]
        if heads:
            top_dup, top_name = max(heads)
            if top_dup >= 5:
                candidates["head"].append((rank(top_name, 0), top_name))

    for side in ("l", "r"):
        # "Leg" is the lower leg next to an "UpLeg"/"Thigh"; alone it is the upper leg.
        lower = candidates[f"lower_leg_{side}"]
        if not candidates[f"upper_leg_{side}"] and len(lower) >= 2:
            legs = [c for c in lower if parsed[c[1]].base == "leg"]
            if legs:
                candidates[f"upper_leg_{side}"] = legs
                candidates[f"lower_leg_{side}"] = [c for c in lower if c not in legs]

    for slot in SLOTS:
        if candidates[slot]:
            slots[slot] = min(candidates[slot])[1]
            methods[slot] = "name"

    # Hips: among the named candidates prefer the one that is an ancestor of
    # both legs and the neck/head (CC "Hip" over "Pelvis").
    hips_candidates = [n for _, n in sorted(candidates["hips"])]
    if len(hips_candidates) > 1:
        anchors = [slots[s] for s in ("upper_leg_l", "upper_leg_r", "neck", "head") if s in slots]
        if anchors:
            good = [h for h in hips_candidates if all(skel.is_ancestor(h, a) for a in anchors)]
            if good:
                slots["hips"] = max(good, key=lambda h: skel.depth[h])

    # Hierarchy sanity: every distal slot must descend from its proximal one.
    chains = [("upper_leg", "lower_leg", "foot", "toe"), ("shoulder", "upper_arm", "lower_arm", "hand")]
    for side in ("l", "r"):
        for chain in chains:
            previous = None
            for kind in chain:
                slot = f"{kind}_{side}"
                if slot not in slots:
                    continue
                if previous and not skel.is_ancestor(slots[previous], slots[slot]):
                    warnings.append(
                        f"{slot} {slots[slot]!r} does not descend from {previous} {slots[previous]!r}; ignored"
                    )
                    del slots[slot]
                    methods.pop(slot, None)
                    continue
                previous = slot
    if "hips" in slots and "head" in slots and not skel.is_ancestor(slots["hips"], slots["head"]):
        warnings.append(f"head {slots['head']!r} does not descend from hips {slots['hips']!r}")

    left_axis, left_source = _infer_left_axis(skel, slots, up, left_hint)
    _fill_spine_from_hierarchy(skel, slots, methods, parsed)
    _geometric_fill(skel, slots, methods, up, left_axis, warnings)
    # The left axis may now be measurable from geometry-mapped limbs.
    if left_source in ("convention", "landmarks"):
        axis2, source2 = _infer_left_axis(skel, slots, up, None)
        if source2 == "names" and left_source == "convention":
            left_axis, left_source = axis2, "geometry"
    return BoneMapping(
        slots=slots, methods=methods, left_axis=left_axis, left_axis_source=left_source,
        flavour=flavour, warnings=warnings,
    )


def _infer_left_axis(
    skel: _Skeleton, slots: Mapping[str, str], up: np.ndarray, hint: Any
) -> tuple[np.ndarray, str]:
    for kind in ("upper_leg", "upper_arm", "hand", "foot", "shoulder", "lower_leg", "lower_arm"):
        left, right = slots.get(f"{kind}_l"), slots.get(f"{kind}_r")
        if left and right:
            vec = _unit(_horizontal(skel.bones[left].head - skel.bones[right].head, up))
            if vec is not None:
                return vec, "names"
    if hint is not None:
        vec = _unit(_horizontal(np.asarray(hint, dtype=np.float64), up))
        if vec is not None:
            return vec, "landmarks"
    return np.array([1.0, 0.0, 0.0]), "convention"


def _fill_spine_from_hierarchy(
    skel: _Skeleton, slots: dict[str, str], methods: dict[str, str], parsed: Mapping[str, ParsedName]
) -> None:
    hips = slots.get("hips")
    if not hips:
        return
    neck = slots.get("neck")
    head = slots.get("head")
    if not neck and head:
        path = skel.path(hips, head)
        if path:
            slots["neck"] = path[-1]
            methods["neck"] = "hierarchy"
            neck = path[-1]
    if not neck:
        return
    path = skel.path(hips, neck)
    if not path:
        return
    deform = [n for n in path if skel.bones[n].use_deform]
    if deform:
        path = deform
    named = {slots.get(s) for s in ("chest", "upper_chest") if methods.get(s) == "name"}
    free = [n for n in path if n not in named]
    if not free:
        return
    plan: dict[str, str] = {"spine": free[0]}
    if len(free) == 2:
        plan["chest"] = free[1]
    elif len(free) >= 3:
        plan["chest"] = free[(len(free) - 1) // 2]
        plan["upper_chest"] = free[-1]
    for slot, name in plan.items():
        if slot not in slots:
            slots[slot] = name
            methods[slot] = "hierarchy"


def _geometric_fill(
    skel: _Skeleton,
    slots: dict[str, str],
    methods: dict[str, str],
    up: np.ndarray,
    left_axis: np.ndarray,
    warnings: list[str],
) -> None:
    """Fill missing slots from a T-pose skeleton's shape (see ``map_humanoid``)."""
    if not skel.bones:
        return
    heads = {n: b.head for n, b in skel.bones.items()}
    height_of = lambda n: float(heads[n] @ up)  # noqa: E731
    all_z = [height_of(n) for n in heads] + [float(b.tail @ up) for b in skel.bones.values()]
    span = max(all_z) - min(all_z)
    if span <= 0:
        return
    centre = heads[slots["hips"]] if "hips" in slots else np.median(np.array(list(heads.values())), axis=0)
    lateral = lambda n: float((heads[n] - centre) @ left_axis)  # noqa: E731
    weighted_any = any(b.weight_count > 0 for b in skel.bones.values())

    def put(slot: str, name: str | None) -> None:
        if name and slot not in slots:
            slots[slot] = name
            methods[slot] = "geometry"

    names = list(skel.bones)
    top = max(names, key=height_of)
    lows = {}
    for side, sign in (("l", 1.0), ("r", -1.0)):
        pool = [n for n in names if sign * lateral(n) > 0.02 * span]
        if pool:
            lows[side] = min(pool, key=height_of)
    if "hips" not in slots and len(lows) == 2:
        put("hips", skel.lca([top, lows["l"], lows["r"]]))
        if "hips" in slots:
            centre = heads[slots["hips"]]
    hips = slots.get("hips")
    if not hips:
        warnings.append("could not locate the hips from names or skeleton geometry")
        return
    hips_z = height_of(hips)

    # Head, neck and spine along the path from the hips to the topmost bone.
    if "head" not in slots and skel.is_ancestor(hips, top):
        path = (skel.path(hips, top) or []) + [top]
        head = None
        for name in reversed(path):
            if weighted_any and skel.bones[name].weight_count == 0:
                continue
            if not weighted_any and name == top and re.search(r"end|top|nub|tip", name, re.I):
                continue
            head = name
            break
        put("head", head)
    if "head" in slots and "neck" not in slots:
        parent = skel.bones[slots["head"]].parent
        if parent and parent != hips and parent in skel.bones:
            put("neck", parent)
    if "neck" in slots:
        path = skel.path(hips, slots["neck"]) or []
        if path:
            put("spine", path[0])
            if len(path) == 2:
                put("chest", path[1])
            elif len(path) >= 3:
                put("chest", path[(len(path) - 1) // 2])
                put("upper_chest", path[-1])

    spine_set = {hips} | {slots[s] for s in ("spine", "chest", "upper_chest", "neck", "head") if s in slots}

    # Legs: from the hips down to the lowest bone on each side.
    for side in ("l", "r"):
        low = lows.get(side)
        if not low or not skel.is_ancestor(hips, low):
            continue
        seq = (skel.path(hips, low) or []) + [low]
        if not seq:
            continue
        z_hip = height_of(seq[0])
        z_floor = min(height_of(n) for n in seq)
        if f"foot_{side}" in slots and slots[f"foot_{side}"] in seq:
            foot = slots[f"foot_{side}"]
        else:
            target = z_floor + 0.1 * (z_hip - z_floor)
            foot = min(seq[1:] or seq, key=lambda n: abs(height_of(n) - target))
        put(f"foot_{side}", foot)
        foot = slots.get(f"foot_{side}", foot)
        if foot in seq:
            after = seq[seq.index(foot) + 1 :]
            if after:
                put(f"toe_{side}", after[0])
            before = seq[: seq.index(foot)]
            if before:
                z_foot = height_of(foot)
                target = z_foot + 0.5 * (z_hip - z_foot)
                knee_pool = before[1:] or before
                knee = slots.get(f"lower_leg_{side}") or min(knee_pool, key=lambda n: abs(height_of(n) - target))
                put(f"lower_leg_{side}", knee)
                if knee in before:
                    thigh_pool = before[: before.index(knee)]
                    if thigh_pool:
                        limit = z_hip - 0.1 * (z_hip - z_foot)
                        near_hip = [n for n in thigh_pool if height_of(n) >= limit] or thigh_pool
                        put(f"upper_leg_{side}", max(near_hip, key=lambda n: abs(lateral(n))))

    # Arms: the most lateral bone above the hips on each side, reached from the spine.
    leg_bones = set()
    for side in ("l", "r"):
        start = slots.get(f"upper_leg_{side}")
        if start:
            leg_bones |= {start, *skel.descendants(start)}
    for side, sign in (("l", 1.0), ("r", -1.0)):
        pool = [
            n for n in names
            if n not in leg_bones and n not in spine_set and skel.is_ancestor(hips, n)
            and height_of(n) > hips_z + 0.1 * span and sign * lateral(n) > 0
        ]
        if not pool:
            continue
        tip = max(pool, key=lambda n: sign * lateral(n))
        chain = (skel.path(hips, tip) or []) + [tip]
        branch = max((i for i, n in enumerate(chain) if n in spine_set), default=-1)
        seq = chain[branch + 1 :]
        if not seq:
            continue
        hand = slots.get(f"hand_{side}")
        if not hand or hand not in seq:
            forks = [n for n in seq if len(skel.children.get(n, [])) >= 2]
            if forks:
                hand = forks[-1]
            elif len(seq) >= 2 and (
                (weighted_any and skel.bones[tip].weight_count == 0)
                or re.search(r"end|nub|tip", tip, re.I)
            ):
                hand = seq[-2]
            else:
                hand = seq[-1]
        put(f"hand_{side}", hand)
        before = seq[: seq.index(hand)]
        if not before:
            continue
        s0 = sign * lateral(before[0])
        wrist = sign * lateral(hand)
        elbow_target = s0 + 0.6 * (wrist - s0)
        elbow_pool = before[1:] or before
        elbow = slots.get(f"lower_arm_{side}") or min(elbow_pool, key=lambda n: abs(sign * lateral(n) - elbow_target))
        put(f"lower_arm_{side}", elbow)
        if elbow not in before:
            continue
        upper_pool = before[: before.index(elbow)]
        if not upper_pool:
            continue
        if len(upper_pool) == 1:
            upper = upper_pool[0]
        else:
            shoulder_target = s0 + 0.22 * (wrist - s0)
            upper = min(upper_pool[1:], key=lambda n: abs(sign * lateral(n) - shoulder_target))
        put(f"upper_arm_{side}", upper)
        if upper in upper_pool and upper_pool.index(upper) > 0:
            put(f"shoulder_{side}", upper_pool[upper_pool.index(upper) - 1])


# Where to hang a group whose semantic attach bone is missing.
ATTACH_FALLBACKS: dict[str, tuple[str, ...]] = {
    "head": ("head", "neck", "upper_chest", "chest", "spine", "hips"),
    "neck": ("neck", "upper_chest", "chest", "spine", "hips"),
    "upper_chest": ("upper_chest", "chest", "spine", "hips"),
    "chest": ("chest", "upper_chest", "spine", "hips"),
    "spine": ("spine", "chest", "hips"),
    "hips": ("hips", "spine"),
    "upper_leg_l": ("upper_leg_l", "hips"), "upper_leg_r": ("upper_leg_r", "hips"),
    "lower_leg_l": ("lower_leg_l", "upper_leg_l", "hips"), "lower_leg_r": ("lower_leg_r", "upper_leg_r", "hips"),
    "upper_arm_l": ("upper_arm_l", "shoulder_l", "upper_chest", "chest", "spine"),
    "upper_arm_r": ("upper_arm_r", "shoulder_r", "upper_chest", "chest", "spine"),
    "lower_arm_l": ("lower_arm_l", "upper_arm_l", "shoulder_l", "chest"),
    "lower_arm_r": ("lower_arm_r", "upper_arm_r", "shoulder_r", "chest"),
    "shoulder_l": ("shoulder_l", "upper_chest", "chest"), "shoulder_r": ("shoulder_r", "upper_chest", "chest"),
    "hand_l": ("hand_l", "lower_arm_l"), "hand_r": ("hand_r", "lower_arm_r"),
    "foot_l": ("foot_l", "lower_leg_l"), "foot_r": ("foot_r", "lower_leg_r"),
}


def resolve_attach(semantic: str, mapping: BoneMapping) -> tuple[str | None, str | None]:
    """(bone name, slot used) for a semantic attach point, following fallbacks."""
    key = str(semantic or "").strip().lower()
    for slot in ATTACH_FALLBACKS.get(key, (key,)):
        if slot in mapping.slots:
            return mapping.slots[slot], slot
    return None, None


# --------------------------------------------------------------------------- weights


def load_weights_module(path: str | Path):
    """Load ``backend/regen/weights.py`` by file path and check its API version."""
    module = load_module_by_path("autorig_regen_weights", path)
    version = getattr(module, "WEIGHTS_API_VERSION", None)
    if version != WEIGHTS_API_VERSION:
        raise ClothRigFailure(
            "weights",
            f"weights module {path} has WEIGHTS_API_VERSION={version!r}, expected {WEIGHTS_API_VERSION}",
        )
    if not callable(getattr(module, "compute_chain_weights", None)):
        raise ClothRigFailure("weights", f"weights module {path} has no compute_chain_weights()")
    return module


def chain_id_layout(joint_counts: Sequence[int]) -> list[tuple[int, int]]:
    """Bone id -> (chain, joint); id 0 is the attach bone ``(-1, -1)``.

    Chain c joint j (j < J_c - 1) is ``1 + offset(c) + j`` with
    ``offset(c) = sum(J_k - 1 for k < c)``; end joints never get an id.
    """
    layout = [(-1, -1)]
    for chain, count in enumerate(joint_counts):
        layout.extend((chain, joint) for joint in range(count - 1))
    return layout


def compute_part_weights(
    module: Any,
    points: np.ndarray,
    chains: Sequence[np.ndarray],
    *,
    attach_origin: np.ndarray | None,
    attach_blend: float = 0.25,
    max_influences: int = 4,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Call ``compute_chain_weights`` and return validated, renormalised output.

    Returns ``(bone_ids (N, K) int, weights (N, K) float64, stats)`` where
    unused slots have weight 0 and rows sum to 1.
    """
    points = np.ascontiguousarray(points, dtype=np.float64)
    chains = [np.ascontiguousarray(c, dtype=np.float64) for c in chains]
    bone_count = 1 + sum(len(c) - 1 for c in chains)
    result = module.compute_chain_weights(
        points, chains,
        attach_origin=None if attach_origin is None else np.asarray(attach_origin, dtype=np.float64),
        attach_blend=attach_blend, max_influences=max_influences,
    )
    try:
        bone_ids, weights = result
    except (TypeError, ValueError) as exc:
        raise ClothRigFailure("weights", "compute_chain_weights must return (bone_ids, weights)") from exc
    bone_ids = np.asarray(bone_ids)
    weights = np.asarray(weights, dtype=np.float64)
    if bone_ids.ndim == 1:
        bone_ids = bone_ids[:, None]
        weights = weights[:, None]
    if bone_ids.shape != weights.shape or bone_ids.shape[0] != len(points):
        raise ClothRigFailure(
            "weights",
            f"compute_chain_weights returned shapes {bone_ids.shape}/{weights.shape} for {len(points)} points",
        )
    if bone_ids.shape[1] > max_influences:
        order = np.argsort(-weights, axis=1)[:, :max_influences]
        bone_ids = np.take_along_axis(bone_ids, order, axis=1)
        weights = np.take_along_axis(weights, order, axis=1)
    bone_ids = bone_ids.astype(np.int64)
    if bone_ids.size and (bone_ids.min() < 0 or bone_ids.max() >= bone_count):
        raise ClothRigFailure(
            "weights", f"compute_chain_weights returned bone ids outside [0, {bone_count})"
        )
    if not np.all(np.isfinite(weights)):
        raise ClothRigFailure("weights", "compute_chain_weights returned non-finite weights")
    weights = np.clip(weights, 0.0, None)
    sums = weights.sum(axis=1)
    empty = sums <= 1e-12
    row_error = float(np.max(np.abs(sums[~empty] - 1.0))) if np.any(~empty) else 0.0
    if np.any(empty):
        bone_ids[empty] = 0
        weights[empty] = 0.0
        weights[empty, 0] = 1.0
        sums = weights.sum(axis=1)
    weights = weights / sums[:, None]
    stats = {
        "points": int(len(points)),
        "bones": int(bone_count),
        "rows_without_weight": int(np.count_nonzero(empty)),
        "max_row_sum_error_before_normalise": row_error,
    }
    return bone_ids, weights, stats
