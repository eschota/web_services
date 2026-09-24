"""End-to-end AutoRig Regen decomposition.

``decompose(dressed_glb, body_glb, out_dir)`` loads the dressed character
(image A's reconstruction, the one that gets rigged) and the base body
(image B's: same character, skin-tight plain suit, bald), aligns B onto A,
measures landmarks, labels A's vertices, builds bone chains for the moving
parts, and writes:

* ``decomposition.json`` — parts, chain groups, landmarks, transform, diagnostics;
* ``labels.npz`` — per dressed vertex: positions, label, part index;
* ``weights.npz`` — per chain group: part vertex ids, bone ids, weights;
* ``parts.glb`` — layered preview: outer / hair / cloth_k (A's materials),
  body_inner (aligned B, shrunk inside A), debug chains.

Any failure raises :class:`DecompositionError`; the orchestrator then falls
back to plain rigging without cloth.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import numpy as np

from . import __version__
from .align import Similarity, align_body_to_dressed
from .chains import PartChains, build_part_chains
from .config import RegenConfig, coerce_config
from .errors import DecompositionError
from .geometry import SurfaceField, Topology, face_majority, sample_surface, vertex_normals, weld
from .landmarks import Landmarks, compute_landmarks
from .mesh_io import DebugPoints, DebugPolyline, MeshData, export_layered_glb, load_mesh
from .segment import LABEL_VALUES, HairMask, Segmentation, segment_dressed
from .weights import WEIGHTS_API_VERSION, compute_chain_weights

FORMAT = "autorig.regen.decomposition"
FORMAT_VERSION = 1
GENERATOR = f"autorig-regen/{__version__}"
BUILTIN_PRESETS = ("hair", "hair_stiff", "skirt", "cape", "ribbon", "tail", "accessory")

DECOMPOSITION_FILE = "decomposition.json"
LABELS_FILE = "labels.npz"
WEIGHTS_FILE = "weights.npz"
PARTS_FILE = "parts.glb"


@dataclasses.dataclass
class DecompositionResult:
    out_dir: Path
    decomposition_path: Path
    labels_path: Path
    weights_path: Path
    parts_glb_path: Path
    document: Dict[str, Any]
    similarity: Similarity
    landmarks: Landmarks
    segmentation: Segmentation
    groups: List[PartChains]
    timings: Dict[str, float]

    @property
    def parts(self) -> List[dict]:
        return self.document["parts"]


class _Stages:
    def __init__(self) -> None:
        self.timings: Dict[str, float] = {}

    @contextlib.contextmanager
    def __call__(self, name: str) -> Iterator[None]:
        t0 = time.perf_counter()
        try:
            yield
        except DecompositionError:
            raise
        except Exception as exc:  # anything unexpected still means "fall back"
            raise DecompositionError(f"{name} failed: {type(exc).__name__}: {exc}") from exc
        finally:
            self.timings[name] = round(time.perf_counter() - t0, 3)


def _coerce_hair_mask(hair_mask: Any) -> Optional[HairMask]:
    if hair_mask is None:
        return None
    if isinstance(hair_mask, HairMask):
        return hair_mask
    if isinstance(hair_mask, (str, os.PathLike)):
        return HairMask.from_image(hair_mask)
    return HairMask(np.asarray(hair_mask))


def _vec(v) -> List[float]:
    return [float(c) for c in np.asarray(v, dtype=np.float64).reshape(-1)]


def _clean(obj: Any) -> Any:
    """JSON-safe copy: numpy scalars to Python, non-finite floats to None."""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return _clean(obj.tolist())
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        value = float(obj)
        return value if math.isfinite(value) else None
    return obj


def bone_names(group: str, chain_index: int, n_joints: int) -> List[str]:
    """Manifest-style names: ``<group>_<cc>_<j>`` ... ``<group>_<cc>_end``."""
    return [f"{group}_{chain_index:02d}_{j}" for j in range(n_joints - 1)] + [f"{group}_{chain_index:02d}_end"]


def _body_inner_vertices(
    body_topo: Topology, similarity: Similarity, dressed_field: SurfaceField, height: float, cfg: RegenConfig
) -> np.ndarray:
    """Aligned body vertices pulled inward so the body never pokes through A."""
    aligned = similarity.apply(body_topo.vertices)
    normals = vertex_normals(aligned, body_topo.faces)
    outside, _ = dressed_field.query(aligned, k=cfg.distance_knn)
    shrink = cfg.body_inner_shrink * height + np.maximum(outside + cfg.body_inner_margin * height, 0.0)
    shrink = np.minimum(shrink, cfg.body_inner_max_shrink * height)
    moved = aligned - normals * shrink[:, None]
    return moved[body_topo.index]


def decompose(
    dressed_glb,
    body_glb,
    out_dir,
    *,
    hair_mask: Any = None,
    config: Any = None,
) -> DecompositionResult:
    """Decompose a dressed character into outer / hair / cloth parts with chains.

    ``hair_mask``: optional :class:`HairMask`, a boolean front-view image
    (the dressed mesh's X/Y bounds map onto the whole image), or a path to a
    mask image. ``config``: :class:`RegenConfig` or a mapping of overrides.
    """
    stage = _Stages()
    with stage("config"):
        cfg = coerce_config(config)
        mask = _coerce_hair_mask(hair_mask)
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        # A previous run's outputs must not read as this run's result if this
        # one fails half way (decomposition.json is written last).
        for stale in (DECOMPOSITION_FILE, LABELS_FILE, WEIGHTS_FILE, PARTS_FILE):
            (out / stale).unlink(missing_ok=True)
        rng = np.random.default_rng(cfg.seed)

    with stage("load"):
        dressed = load_mesh(dressed_glb)
        body = load_mesh(body_glb)
        for what, mesh in (("dressed mesh", dressed), ("base body", body)):
            if mesh.n_faces < cfg.min_faces:
                raise DecompositionError(f"{what} has only {mesh.n_faces} triangles")
            extent = np.ptp(mesh.vertices, axis=0)
            if not np.all(np.isfinite(extent)) or float(extent.max()) <= 0.0:
                raise DecompositionError(f"{what} is degenerate (zero extent)")

    with stage("topology"):
        d_topo = weld(dressed.vertices, dressed.faces, cfg.weld_tolerance)
        b_topo = weld(body.vertices, body.faces, cfg.weld_tolerance)
        d_samples, d_normals, _ = sample_surface(d_topo.vertices, d_topo.faces, cfg.align_target_samples, rng)
        if len(d_samples) == 0:
            raise DecompositionError("dressed mesh has no surface area")

    with stage("align"):
        similarity = align_body_to_dressed(
            b_topo, d_topo, config=cfg, dressed_samples=(d_samples, d_normals), rng=rng
        )

    with stage("landmarks"):
        aligned = similarity.apply(b_topo.vertices)
        b_samples, b_sample_normals, _ = sample_surface(aligned, b_topo.faces, cfg.body_distance_samples, rng)
        landmarks = compute_landmarks(
            aligned, b_topo.faces, config=cfg, samples=b_samples[: cfg.landmark_samples], rng=rng
        )
        height = landmarks.height
        similarity.reference_height = height
        rms_rel = similarity.rms / height
        if not math.isfinite(rms_rel) or rms_rel > cfg.align_max_rms:
            raise DecompositionError(
                f"alignment residual too high: trimmed RMS {rms_rel:.2%} of height (limit {cfg.align_max_rms:.2%})"
            )
        if similarity.inlier_ratio < cfg.align_min_inlier_ratio:
            raise DecompositionError(
                f"alignment residual too high: only {similarity.inlier_ratio:.0%} of the body lies on the "
                f"dressed surface (need {cfg.align_min_inlier_ratio:.0%})"
            )

    with stage("segment"):
        b_vertex_normals = vertex_normals(aligned, b_topo.faces)
        body_field = SurfaceField(
            np.concatenate([b_samples, aligned]), np.concatenate([b_sample_normals, b_vertex_normals])
        )
        segmentation = segment_dressed(
            d_topo,
            body_field,
            landmarks,
            config=cfg,
            hair_mask=mask,
            dressed_bounds=dressed.bounds(),
            occluders=d_samples,
        )

    with stage("chains"):
        groups: List[PartChains] = []
        welded_face_part = face_majority(d_topo.faces, segmentation.part_index)
        for index, (name, kind) in enumerate(zip(segmentation.part_names, segmentation.part_kinds)):
            groups.append(
                build_part_chains(
                    name,
                    kind,
                    segmentation.part_vertices(index),
                    segmentation.labels,
                    d_topo,
                    landmarks,
                    b_samples,
                    part_faces=d_topo.faces[welded_face_part == index],
                    config=cfg,
                    rng=rng,
                )
            )

    with stage("weights"):
        part_of_vertex = segmentation.part_index[d_topo.index]
        weight_arrays: Dict[str, np.ndarray] = {}
        weight_stats: Dict[str, dict] = {}
        lookup = np.full(d_topo.n_vertices, -1, dtype=np.int64)
        for index, group in enumerate(groups):
            if group.rigid:
                continue
            welded_ids = segmentation.part_vertices(index)
            lookup[welded_ids] = np.arange(len(welded_ids))
            ids_w, w_w = compute_chain_weights(
                d_topo.vertices[welded_ids],
                [c.joints for c in group.chains],
                attach_origin=group.attach_origin,
                attach_blend=cfg.attach_blend,
                max_influences=cfg.max_influences,
            )
            source_ids = np.flatnonzero(part_of_vertex == index)
            rows = lookup[d_topo.index[source_ids]]
            weight_arrays[f"{group.name}__vertices"] = source_ids.astype(np.int32)
            weight_arrays[f"{group.name}__bone_ids"] = ids_w[rows]
            weight_arrays[f"{group.name}__weights"] = w_w[rows]
            sums = w_w.sum(axis=1, dtype=np.float64)
            weight_stats[group.name] = {
                "vertices": int(len(source_ids)),
                "bones": 1 + sum(len(c.joints) - 1 for c in group.chains),
                "max_row_sum_error": float(np.max(np.abs(sums - 1.0))) if len(sums) else 0.0,
                "attach_share": float(np.mean(np.where(ids_w == 0, w_w, 0.0).sum(axis=1))) if len(sums) else 0.0,
            }

    with stage("write"):
        labels_source = segmentation.labels[d_topo.index]
        counts = {name: int(np.sum(labels_source == i)) for i, name in enumerate(LABEL_VALUES)}
        labels_path = out / LABELS_FILE
        np.savez_compressed(
            labels_path,
            positions=dressed.vertices.astype(np.float32),
            labels=labels_source.astype(np.uint8),
            part_index=part_of_vertex.astype(np.int16),
            signed_distance=segmentation.signed_distance[d_topo.index].astype(np.float32),
        )
        weights_path = out / WEIGHTS_FILE
        if not weight_arrays:
            weight_arrays["empty"] = np.zeros(0, dtype=np.int32)
        np.savez_compressed(weights_path, **weight_arrays)

        face_part = face_majority(dressed.faces, part_of_vertex)
        layers = [("outer", dressed, face_part == -1)]
        for index, name in enumerate(segmentation.part_names):
            layers.append((name, dressed, face_part == index))
        d_field = SurfaceField(d_samples, d_normals)
        inner = body.with_vertices(_body_inner_vertices(b_topo, similarity, d_field, height, cfg))
        layers.append(("body_inner", inner, np.ones(inner.n_faces, dtype=bool)))
        lines = []
        points = []
        for group in groups:
            for c, chain in enumerate(group.chains):
                lines.append(DebugPolyline(f"chain_{group.name}_{c:02d}", chain.joints))
            if group.chains:
                points.append(DebugPoints(f"joints_{group.name}", np.concatenate([c.joints for c in group.chains])))
        parts_path = out / PARTS_FILE
        export_warnings: List[str] = []
        try:
            export_layered_glb(parts_path, layers, debug_lines=lines, debug_points=points)
        except Exception as exc:  # the preview must not sink the decomposition
            export_warnings.append(f"parts.glb written without materials: {type(exc).__name__}: {exc}")
            try:
                export_layered_glb(
                    parts_path, layers, debug_lines=lines, debug_points=points, keep_materials=False
                )
            except Exception as exc2:
                export_warnings.append(f"parts.glb not written: {type(exc2).__name__}: {exc2}")
                parts_path.unlink(missing_ok=True)

        document = _document(
            dressed,
            body,
            d_topo,
            similarity,
            landmarks,
            segmentation,
            groups,
            part_of_vertex,
            face_part,
            counts,
            weight_stats,
            stage.timings,
            cfg,
        )
        document["diagnostics"]["warnings"].extend(export_warnings)
        if not parts_path.is_file():
            document["files"].pop("parts_glb", None)
        json_path = out / DECOMPOSITION_FILE
        tmp = json_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(_clean(document), indent=1, allow_nan=False), encoding="utf-8")
        os.replace(tmp, json_path)

    document["diagnostics"]["timings"] = dict(stage.timings)
    return DecompositionResult(
        out_dir=out,
        decomposition_path=json_path,
        labels_path=labels_path,
        weights_path=weights_path,
        parts_glb_path=parts_path,
        document=document,
        similarity=similarity,
        landmarks=landmarks,
        segmentation=segmentation,
        groups=groups,
        timings=dict(stage.timings),
    )


def _document(
    dressed: MeshData,
    body: MeshData,
    d_topo: Topology,
    similarity: Similarity,
    landmarks: Landmarks,
    segmentation: Segmentation,
    groups: List[PartChains],
    part_of_vertex: np.ndarray,
    face_part: np.ndarray,
    counts: Dict[str, int],
    weight_stats: Dict[str, dict],
    timings: Dict[str, float],
    cfg: RegenConfig,
) -> Dict[str, Any]:
    height = landmarks.height
    parts = []
    group_docs = []
    for index, group in enumerate(groups):
        members = dressed.vertices[part_of_vertex == index]
        parts.append(
            {
                "name": group.name,
                "kind": group.kind,
                "vertex_count": int(len(members)),
                "face_count": int(np.sum(face_part == index)),
                "bbox_min": _vec(members.min(axis=0)) if len(members) else [0.0, 0.0, 0.0],
                "bbox_max": _vec(members.max(axis=0)) if len(members) else [0.0, 0.0, 0.0],
                "attach": group.attach,
                "rigid": bool(group.rigid),
                "connection": group.connection,
                "hang_length": float(group.hang),
                "coverage_deg": float(group.coverage_deg),
                "attach_mode": group.attach_mode,
                "attach_origin": _vec(group.attach_origin),
                "attach_centroid": _vec(group.attach_centroid),
                "notes": list(group.notes),
            }
        )
        if group.rigid:
            continue
        chains = []
        for c, chain in enumerate(group.chains):
            chains.append(
                {
                    "joints": [_vec(j) for j in chain.joints],
                    "bones": bone_names(group.name, c, len(chain.joints)),
                    "angle_deg": float(chain.angle_deg),
                    "length": float(chain.length),
                    "sector": int(chain.sector),
                }
            )
        group_docs.append(
            {
                "name": group.name,
                "kind": group.kind,
                "part": group.name,
                "attach": group.attach,
                "connection": group.connection,
                "preset": group.preset,
                "chains": chains,
                "joints_per_chain": int(cfg.joints_per_chain),
                "attach_origin": _vec(group.attach_origin),
                "attach_centroid": _vec(group.attach_centroid),
                "axis": [float(group.axis[0]), float(group.axis[1])],
                "coverage_deg": float(group.coverage_deg),
                "arc_start_deg": float(group.arc_start_deg),
                "arc_deg": float(group.arc_deg),
                "sectors": int(group.sectors),
                "weights": {"file": WEIGHTS_FILE, "prefix": group.name, "api_version": WEIGHTS_API_VERSION},
            }
        )
    lo, hi = dressed.bounds()
    return {
        "format": FORMAT,
        "version": FORMAT_VERSION,
        "generator": GENERATOR,
        "space": "dressed_glb",
        "height": float(height),
        "body_transform": similarity.as_list(),
        "body_scale": float(similarity.scale),
        "labels": {"values": list(LABEL_VALUES), "file": LABELS_FILE, "count": int(len(part_of_vertex))},
        "parts": parts,
        "groups": group_docs,
        "landmarks": landmarks.to_json(),
        "diagnostics": {
            "align_rms": float(similarity.rms),
            "align_rms_relative": float(similarity.rms / height),
            "align_inlier_ratio": float(similarity.inlier_ratio),
            "align_scale": float(similarity.scale),
            "align_rotation_deg": float(similarity.rotation_deg),
            "align_rotation_clamped": bool(similarity.rotation_clamped),
            "align_iterations": int(similarity.iterations),
            "align_converged": bool(similarity.converged),
            "align_starts": similarity.starts,
            "label_counts": counts,
            "hair_source": segmentation.hair_source,
            "weights": weight_stats,
            "warnings": list(landmarks.warnings) + list(segmentation.warnings),
            "timings": dict(timings),
        },
        "files": {"labels": LABELS_FILE, "weights": WEIGHTS_FILE, "parts_glb": PARTS_FILE},
        "inputs": {
            "dressed": Path(dressed.source).name,
            "body": Path(body.source).name,
            "dressed_vertices": dressed.n_vertices,
            "dressed_faces": dressed.n_faces,
            "dressed_welded_vertices": d_topo.n_vertices,
            "body_vertices": body.n_vertices,
            "dressed_bbox_min": _vec(lo),
            "dressed_bbox_max": _vec(hi),
        },
        "config": cfg.to_dict(),
    }
