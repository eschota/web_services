#!/usr/bin/env python3
"""Export pinned farm RAW tracker cache as non-accepted 2D measurements.

The output is deliberately not ``autorig-fitting-observations.v1``.  It carries
the original per-frame tracker coordinates, visibility and confidence only;
camera-Z calibration and animation QA remain false.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np


SCHEMA = "autorig.equine-raw-2d-measurements.v1"
RAW_META_SCHEMA = "autorig.equine-raw-measurements/1"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class RawObservationError(ValueError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _array_sha(array: np.ndarray) -> str:
    value = np.ascontiguousarray(array)
    digest = hashlib.sha256()
    digest.update(str(value.dtype).encode())
    digest.update(str(value.shape).encode())
    digest.update(value.tobytes())
    return digest.hexdigest()


def _required_sha(value: Any, field: str) -> str:
    text = str(value or "").lower()
    if not SHA_RE.fullmatch(text):
        raise RawObservationError(f"{field} must be exact lowercase SHA-256")
    return text


def _pinned(path: str | Path, expected: str, field: str) -> Path:
    result = Path(path).resolve()
    expected = _required_sha(expected, field + "_sha256")
    if not result.is_file() or _sha(result) != expected:
        raise RawObservationError(f"{field} SHA-256 mismatch or file missing")
    return result


def _json(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RawObservationError(f"invalid {field} JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise RawObservationError(f"{field} must be an object")
    return value


def convert_raw_measurements(*, raw_meta_path: str | Path, raw_meta_sha256: str,
                             raw_npz_path: str | Path, raw_npz_sha256: str,
                             source_video_path: str | Path, source_video_sha256: str,
                             source_scope_sha256: str, fps: float,
                             anchor_map_path: str | Path, anchor_map_sha256: str,
                             surface_anchors_path: str | Path, surface_anchors_sha256: str,
                             fitting_bundle_path: str | Path, fitting_bundle_sha256: str,
                             immutable_manifest_path: str | Path,
                             immutable_manifest_sha256: str,
                             video_metadata_receipt: Mapping[str, Any] | None = None,
                             source_frame_indices: list[int] | None = None,
                             expected_array_sha256: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Convert exact raw cache bytes; no point is generated, mirrored or filled."""
    paths = {
        "meta": _pinned(raw_meta_path, raw_meta_sha256, "raw_meta"),
        "npz": _pinned(raw_npz_path, raw_npz_sha256, "raw_npz"),
        "video": _pinned(source_video_path, source_video_sha256, "source_video"),
        "anchor": _pinned(anchor_map_path, anchor_map_sha256, "anchor_map"),
        "surface_anchors": _pinned(surface_anchors_path, surface_anchors_sha256, "surface_anchors"),
        "bundle": _pinned(fitting_bundle_path, fitting_bundle_sha256, "fitting_bundle"),
        "manifest": _pinned(immutable_manifest_path, immutable_manifest_sha256, "immutable_manifest"),
    }
    source_scope_sha256 = _required_sha(source_scope_sha256, "source_scope_sha256")
    if isinstance(fps, bool) or not isinstance(fps, (int, float)) or not math.isfinite(float(fps)) or fps <= 0:
        raise RawObservationError("fps must be a finite positive source-video measurement")
    meta, anchor, surface_anchors, bundle, manifest = (
        _json(paths[name], name) for name in ("meta", "anchor", "surface_anchors", "bundle", "manifest"))
    if (meta.get("schema") != RAW_META_SCHEMA or meta.get("stage") != "tracks" or
            meta.get("calibration_accepted") is not False or meta.get("animation_quality_approved") is not False):
        raise RawObservationError("raw stage identity/acceptance flags are invalid")
    if meta.get("data_sha256") != raw_npz_sha256:
        raise RawObservationError("raw metadata does not bind the NPZ bytes")
    binding = meta.get("binding") if isinstance(meta.get("binding"), Mapping) else {}
    if binding.get("scope_sha256") != source_scope_sha256:
        raise RawObservationError("raw stage source scope mismatch")
    if binding.get("video_sha256") != source_video_sha256:
        raise RawObservationError("raw stage source video mismatch")
    frame_count, width, height = binding.get("frame_count"), binding.get("width"), binding.get("height")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (frame_count, width, height)):
        raise RawObservationError("raw binding dimensions/frame count are invalid")
    timing_pending = []
    binding_fps = binding.get("fps")
    timing_source = "raw_binding"
    if binding_fps is not None:
        if (isinstance(binding_fps, bool) or not isinstance(binding_fps, (int, float)) or
                not math.isfinite(float(binding_fps)) or float(binding_fps) <= 0 or
                abs(float(binding_fps) - float(fps)) > 1e-9):
            raise RawObservationError("raw binding fps is invalid or differs from requested fps")
        timing_verified = True
    elif video_metadata_receipt is not None:
        receipt = video_metadata_receipt
        if (receipt.get("schema") != "autorig.video-timing-evidence.v1" or
                receipt.get("source_video_sha256") != source_video_sha256 or
                receipt.get("frame_count") != frame_count or
                isinstance(receipt.get("fps"), bool) or not isinstance(receipt.get("fps"), (int, float)) or
                not math.isfinite(float(receipt["fps"])) or float(receipt["fps"]) <= 0 or
                abs(float(receipt["fps"]) - float(fps)) > 1e-9 or
                not isinstance(receipt.get("receipt_id"), str) or not receipt.get("receipt_id")):
            raise RawObservationError("video timing receipt is invalid or source-mismatched")
        timing_verified, timing_source = True, "pinned_video_metadata_receipt"
    else:
        timing_verified, timing_source = False, "caller_measured_unverified"
        timing_pending.append("unverified_source_fps")
    if source_frame_indices is None:
        source_indices = list(range(frame_count)); full_source = True
    else:
        source_indices = list(source_frame_indices); full_source = False
        if (len(source_indices) != frame_count or any(isinstance(value, bool) or not isinstance(value, int) or value < 0
                                                      for value in source_indices) or
                any(b <= a for a, b in zip(source_indices, source_indices[1:]))):
            raise RawObservationError("cropped source_frame_indices must be a strictly increasing frame mapping")
    if anchor.get("schema") != "autorig.equine-fitting-anchor-map/1":
        raise RawObservationError("unsupported anchor-map schema")
    if bundle.get("schema") != "autorig-actionless-fitting-bundle.v1" or manifest.get("schema") != "autorig-fitting-immutable-bundle.v1":
        raise RawObservationError("unsupported immutable bundle schema")
    if anchor.get("bundle_manifest_sha256") != immutable_manifest_sha256:
        raise RawObservationError("anchor map/immutable manifest mismatch")
    if manifest.get("bundle_manifest", {}).get("sha256") != fitting_bundle_sha256:
        raise RawObservationError("immutable manifest/fitting bundle mismatch")
    surface_artifact = bundle.get("artifacts", {}).get("surface_anchors", {})
    if surface_artifact.get("sha256") != surface_anchors_sha256:
        raise RawObservationError("fitting bundle does not bind surface anchors")
    manifest_files = manifest.get("files")
    manifest_surface = next((row for row in manifest_files or []
                             if isinstance(row, Mapping) and row.get("filename") == "surface_anchors.json"), None)
    if not isinstance(manifest_surface, Mapping) or manifest_surface.get("sha256") != surface_anchors_sha256:
        raise RawObservationError("immutable manifest does not bind surface anchors")
    if anchor.get("source_sha256") != bundle.get("source", {}).get("sha256"):
        raise RawObservationError("anchor map/source model mismatch")
    anchor_rows = anchor.get("tracks")
    if not isinstance(anchor_rows, list):
        raise RawObservationError("anchor-map tracks missing")
    known_anchor_ids = [row.get("anchor_id") for row in anchor_rows if isinstance(row, Mapping)]
    if len(known_anchor_ids) != len(set(known_anchor_ids)) or any(not isinstance(value, str) or not value for value in known_anchor_ids):
        raise RawObservationError("anchor-map identities must be unique strings")
    source_bones = surface_anchors.get("bones")
    if not isinstance(source_bones, list):
        raise RawObservationError("surface anchors bones are missing")
    all_source_anchor_ids = set()
    for bone_row in source_bones:
        if not isinstance(bone_row, Mapping) or not isinstance(bone_row.get("bone"), str):
            raise RawObservationError("invalid surface-anchor bone row")
        source_points = bone_row.get("points")
        if not isinstance(source_points, list):
            raise RawObservationError("invalid surface-anchor point list")
        for point in source_points:
            vertex_id = point.get("vertex_id") if isinstance(point, Mapping) else None
            if isinstance(vertex_id, bool) or not isinstance(vertex_id, int) or vertex_id < 0:
                raise RawObservationError("invalid surface-anchor vertex identity")
            exact_id = f"{bone_row['bone']}:{vertex_id}"
            if exact_id in all_source_anchor_ids:
                raise RawObservationError("duplicate exact identity in surface anchors")
            all_source_anchor_ids.add(exact_id)
    if any(value not in all_source_anchor_ids for value in known_anchor_ids):
        raise RawObservationError("priority anchor map is not a subset of immutable surface anchors")
    try:
        with np.load(paths["npz"], allow_pickle=False) as archive:
            arrays = {name: np.asarray(archive[name]) for name in archive.files}
    except Exception as exc:
        raise RawObservationError(f"invalid raw tracks NPZ: {exc}") from exc
    required = {"points_xy", "visible", "confidence", "anchor_ids", "seed_points_xy"}
    if set(arrays) != required:
        raise RawObservationError(f"raw tracks arrays must be exactly {sorted(required)}")
    points, visible, confidence = arrays["points_xy"], arrays["visible"], arrays["confidence"]
    raw_anchor_ids = [str(value) for value in arrays["anchor_ids"].tolist()]
    track_count = len(raw_anchor_ids)
    if (len(set(raw_anchor_ids)) != track_count or points.shape != (frame_count, track_count, 2) or
            visible.shape != (frame_count, track_count) or confidence.shape != (frame_count, track_count) or
            arrays["seed_points_xy"].shape != (track_count, 2)):
        raise RawObservationError("raw track identity/count/array shapes are inconsistent")
    if visible.dtype != np.bool_:
        raise RawObservationError("raw visibility must remain boolean")
    if not np.all(np.isfinite(points)) or not np.all(np.isfinite(confidence)) or not np.all(np.isfinite(arrays["seed_points_xy"])):
        raise RawObservationError("raw points/confidence contain non-finite values")
    if np.any(confidence < 0):
        raise RawObservationError("raw confidence must be non-negative")
    if any(value not in all_source_anchor_ids for value in raw_anchor_ids):
        raise RawObservationError("raw stage contains an anchor outside immutable surface anchors")
    bound_ids = binding.get("anchor_ids")
    if not isinstance(bound_ids, list) or [str(value) for value in bound_ids] != raw_anchor_ids:
        raise RawObservationError("raw binding and NPZ anchor order differ")
    expected_seed_sha = binding.get("seed_points_sha256")
    if expected_seed_sha != _array_sha(arrays["seed_points_xy"]):
        raise RawObservationError("raw binding seed-points array hash mismatch")
    array_hashes = {name: _array_sha(value) for name, value in sorted(arrays.items())}
    if expected_array_sha256 is not None:
        expected = {str(key): _required_sha(value, f"expected_array_sha256.{key}")
                    for key, value in expected_array_sha256.items()}
        if expected != array_hashes:
            raise RawObservationError("raw stage array hashes differ from pinned expectations")
    semantic_by_anchor = {row["anchor_id"]: row.get("track_id") for row in anchor_rows}
    tracks = []
    for index, anchor_id in enumerate(raw_anchor_ids):
        tracks.append({
            "id": f"raw-track-{index:03d}", "anchor_id": anchor_id, "query_frame": 0,
            "priority_semantic_id": semantic_by_anchor.get(anchor_id),
            "points": [{"frame": frame, "x": float(points[frame, index, 0]),
                        "y": float(points[frame, index, 1]),
                        "visible": bool(visible[frame, index]),
                        "confidence": float(confidence[frame, index])}
                       for frame in range(frame_count)],
        })
    return {
        "schema": SCHEMA, "measurement_state": "raw_2d_measurements",
        "frame_count": frame_count, "width": width, "height": height, "fps": float(fps),
        "tracks": tracks, "timing": {"frame_count": frame_count, "fps": float(fps),
                                      "source_frame_indices": source_indices, "full_source": full_source,
                                      "source_video_sha256": source_video_sha256,
                                      "verified": timing_verified, "source": timing_source,
                                      "retimed": False, "loop_declared": False},
        "pending": timing_pending,
        "calibration_accepted": False, "accepted_observations": False,
        "animation_quality_approved": False, "camera_z_accepted": False,
        "provenance": {
            "producer": "farm_raw_capture.tracks", "raw_meta_sha256": raw_meta_sha256,
            "raw_npz_sha256": raw_npz_sha256, "raw_array_sha256": array_hashes,
            "source_scope_sha256": source_scope_sha256, "source_video_sha256": source_video_sha256,
            "anchor_map_sha256": anchor_map_sha256, "bundle_sha256": fitting_bundle_sha256,
            "immutable_manifest_sha256": immutable_manifest_sha256,
            "surface_anchors_sha256": surface_anchors_sha256,
            "source_model_sha256": bundle.get("source", {}).get("sha256"),
            "track_selection": {"mode": "preserve_all_exact_immutable_source_anchors",
                                "raw_track_count": track_count,
                                "priority_anchor_ids": known_anchor_ids,
                                "priority_present_count": sum(value in set(raw_anchor_ids) for value in known_anchor_ids)},
            "metric_depth_used": False, "points_generated": False,
            "points_mirrored": False, "hidden_points_interpolated": False,
        },
    }


def write_raw_measurements(document: Mapping[str, Any], destination: str | Path) -> Path:
    """Write one fresh derived document; existing paths are never replaced."""
    path = Path(destination).resolve()
    if path.exists():
        raise RawObservationError("destination already exists; reference output is immutable")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + f".{os.getpid()}.tmp")
    data = json.dumps(dict(document), indent=2, sort_keys=True, allow_nan=False) + "\n"
    try:
        temporary.write_text(data, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists(): temporary.unlink()
    return path


__all__ = ["RawObservationError", "SCHEMA", "convert_raw_measurements", "write_raw_measurements"]
