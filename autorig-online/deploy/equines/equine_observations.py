#!/usr/bin/env python3
"""Source-bound runtime observations -> equine motion-contract sample.

This adapter only unprojects tracker points whose identity, visibility,
confidence and calibrated camera-Z are present in the official observation
bundle.  It does not interpolate occlusions, mirror a far leg, infer joint
angles or approve an animation.  Incomplete evidence is returned as
``pending`` and the corresponding motion-contract track is omitted.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np


SCHEMA = "autorig.equine-source-observations.v1"
OBS_SCHEMA = "autorig-fitting-observations.v1"
ANCHOR_SCHEMA = "autorig.equine-fitting-anchor-map/1"
MANIFEST_SCHEMA = "autorig-fitting-immutable-bundle.v1"
BUNDLE_SCHEMA = "autorig-actionless-fitting-bundle.v1"
REQUIRED_TRACKS = {
    "fore_left.hoof": "LF", "fore_right.hoof": "RF",
    "hind_left.hoof": "LH", "hind_right.hoof": "RH",
    "head": "head", "tail": "tail",
}
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class ObservationContractError(ValueError):
    pass


def options_get(options: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    specs = {
        "min_frame_confidence": (0.50, 0.01, 1.0, "Minimum tracker confidence for every accepted frame."),
        "depth_patch_radius_px": (1, 0, 8, "Radius of the finite camera-Z median patch around the tracker point."),
        "min_depth_patch_samples": (2, 1, 81, "Minimum finite positive calibrated camera-Z pixels per point."),
        "min_track_visible_fraction": (1.0, 1.0, 1.0, "Required visible-frame fraction is fixed at 1; partial tracks are diagnostics only."),
        "min_calibration_coverage": (0.90, 0.50, 1.0, "Minimum declared first-frame calibrated camera-Z coverage."),
        "min_ground_confidence": (0.95, 0.50, 1.0, "Minimum source-bound ground-plane confidence."),
    }
    supplied = dict(options or {})
    unknown = sorted(set(supplied) - set(specs))
    if unknown:
        raise ObservationContractError("unknown options: " + ", ".join(unknown))
    result = {}
    for name, (default, low, high, doc) in specs.items():
        value = supplied.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ObservationContractError(f"{name} must be finite")
        value = int(value) if name in {"depth_patch_radius_px", "min_depth_patch_samples"} else float(value)
        if not low <= value <= high:
            raise ObservationContractError(f"{name} must be in [{low}, {high}]")
        result[name] = {"value": value, "default": default, "min": low, "max": high, "doc": doc}
    return result


def _values(options: Mapping[str, Any] | None) -> dict[str, float]:
    return {key: row["value"] for key, row in options_get(options).items()}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _expected_sha(value: Any, field: str) -> str:
    text = str(value or "").lower()
    if not SHA_RE.fullmatch(text):
        raise ObservationContractError(f"{field} must be an exact lowercase SHA-256")
    return text


def _pinned(path: str | Path, expected: str, field: str) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise ObservationContractError(f"missing {field}: {resolved}")
    expected = _expected_sha(expected, field + "_sha256")
    actual = _sha(resolved)
    if actual != expected:
        raise ObservationContractError(f"{field} SHA-256 mismatch: expected {expected}, got {actual}")
    return resolved


def _json(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ObservationContractError(f"invalid {field} JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ObservationContractError(f"{field} must be an object")
    return value


def _finite_vector(value: Any, size: int, field: str) -> np.ndarray:
    vector = np.asarray(value, dtype=np.float64)
    if vector.shape != (size,) or not np.all(np.isfinite(vector)):
        raise ObservationContractError(f"{field} must be a finite {size}-vector")
    return vector


def _matrix(value: Any, field: str) -> np.ndarray:
    matrix = np.asarray(value, dtype=np.float64)
    if matrix.size != 16:
        raise ObservationContractError(f"{field} must contain 16 values")
    matrix = matrix.reshape(4, 4)
    if not np.all(np.isfinite(matrix)) or abs(float(np.linalg.det(matrix))) < 1e-10:
        raise ObservationContractError(f"{field} must be finite and invertible")
    return matrix


def _normalize(vector: np.ndarray, field: str) -> np.ndarray:
    length = float(np.linalg.norm(vector))
    if not math.isfinite(length) or length < 1e-9:
        raise ObservationContractError(f"{field} has zero length")
    return vector / length


def _blender_to_gltf(vector: np.ndarray, *, point: bool = True) -> np.ndarray:
    """Apply Blender Z-up -> glTF Y-up exactly once: (x,y,z)->(x,z,-y)."""
    converted = np.asarray([vector[0], vector[2], -vector[1]], dtype=np.float64)
    return converted


def _unproject(x: float, y: float, camera_z: float, intrinsics: Mapping[str, Any],
               camera_to_world: np.ndarray) -> np.ndarray:
    fx, fy, cx, cy = (float(intrinsics[key]) for key in ("fx", "fy", "cx", "cy"))
    if not all(math.isfinite(value) for value in (x, y, camera_z, fx, fy, cx, cy)) or fx <= 0 or fy <= 0 or camera_z <= 0:
        raise ObservationContractError("invalid camera unprojection value")
    camera = np.asarray([(x - cx) * camera_z / fx, (cy - y) * camera_z / fy, -camera_z, 1.0])
    world = camera_to_world @ camera
    if not np.all(np.isfinite(world)) or abs(float(world[3])) < 1e-9:
        raise ObservationContractError("camera unprojection produced invalid world point")
    return world[:3] / world[3]


def _sample_depth(frame: np.ndarray, x: float, y: float, radius: int, minimum: int) -> float | None:
    if frame.ndim != 2:
        return None
    ix, iy = int(round(x)), int(round(y))
    x0, x1 = max(0, ix - radius), min(frame.shape[1], ix + radius + 1)
    y0, y1 = max(0, iy - radius), min(frame.shape[0], iy + radius + 1)
    values = np.asarray(frame[y0:y1, x0:x1], dtype=np.float64)
    values = values[np.isfinite(values) & (values > 0)]
    if values.size < minimum:
        return None
    return float(np.median(values))


def _anchor_contract(anchor_map: Mapping[str, Any], bundle: Mapping[str, Any],
                     manifest_sha: str) -> tuple[dict[str, Mapping[str, Any]], dict[str, Any]]:
    if anchor_map.get("schema") != ANCHOR_SCHEMA:
        raise ObservationContractError(f"anchor map schema must be {ANCHOR_SCHEMA}")
    if anchor_map.get("bundle_manifest_sha256") != manifest_sha:
        raise ObservationContractError("anchor map is not bound to the immutable bundle manifest")
    source = bundle.get("source") if isinstance(bundle.get("source"), Mapping) else {}
    if anchor_map.get("source_sha256") != source.get("sha256"):
        raise ObservationContractError("anchor map source model does not match fitting bundle")
    rows = anchor_map.get("tracks")
    if not isinstance(rows, list):
        raise ObservationContractError("anchor map tracks must be an array")
    by_id = {}
    anchors = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ObservationContractError("anchor-map track must be an object")
        track_id, anchor_id = row.get("track_id"), row.get("anchor_id")
        if track_id in by_id or not isinstance(track_id, str) or track_id not in REQUIRED_TRACKS:
            raise ObservationContractError(f"unexpected or duplicate tracker identity: {track_id!r}")
        if not isinstance(anchor_id, str) or not anchor_id or anchor_id in anchors:
            raise ObservationContractError("anchor IDs must be unique non-empty strings")
        expected_bone = {"fore_left.hoof": "LeftFrontToe", "fore_right.hoof": "RightFrontToe",
                         "hind_left.hoof": "LeftHindToe", "hind_right.hoof": "RightHindToe",
                         "head": "Head", "tail": "Tail3"}[track_id]
        if row.get("bone") != expected_bone or not anchor_id.startswith(expected_bone + ":"):
            raise ObservationContractError(f"{track_id} is not bound to its exact semantic anchor")
        by_id[track_id] = row
        anchors.add(anchor_id)
    if set(by_id) != set(REQUIRED_TRACKS):
        raise ObservationContractError("anchor map must contain exactly four hooves, head and tail")
    return by_id, dict(source)


def _ground_camera_receipt(value: Mapping[str, Any] | None, *, source_video_sha256: str,
                           observations_sha256: str) -> tuple[float, dict[str, Any] | None]:
    """Validate measured temporal ground/camera evidence without inventing limits."""
    if not isinstance(value, Mapping):
        return 0.0, None
    if value.get("schema") != "autorig.equine-video-ground-evidence.v1":
        raise ObservationContractError("unsupported ground/camera evidence schema")
    if value.get("source_video_sha256") != source_video_sha256:
        raise ObservationContractError("ground/camera evidence source-video hash mismatch")
    if value.get("observations_sha256") != observations_sha256:
        raise ObservationContractError("ground/camera evidence observations hash mismatch")
    receipt_id = value.get("receipt_id")
    if not isinstance(receipt_id, str) or not receipt_id:
        raise ObservationContractError("ground/camera evidence receipt_id is required")
    checks = value.get("checks")
    required = {"temporal_ground_plane", "camera_continuity", "ground_visible_fraction"}
    if not isinstance(checks, Mapping) or set(checks) != required:
        raise ObservationContractError("ground/camera evidence requires the three measured checks")
    normalized = {}
    passed = True
    for name in sorted(required):
        row = checks.get(name)
        if not isinstance(row, Mapping):
            raise ObservationContractError(f"ground/camera check {name} must be an object")
        measured, threshold, operator = row.get("value"), row.get("threshold"), row.get("operator")
        if (isinstance(measured, bool) or not isinstance(measured, (int, float)) or not math.isfinite(float(measured)) or
                isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not math.isfinite(float(threshold)) or
                operator not in {"<=", ">="}):
            raise ObservationContractError(f"ground/camera check {name} lacks finite value/operator/threshold")
        check_pass = float(measured) <= float(threshold) if operator == "<=" else float(measured) >= float(threshold)
        normalized[name] = {"value": float(measured), "operator": operator,
                            "threshold": float(threshold), "pass": check_pass}
        passed = passed and check_pass
    confidence = value.get("confidence")
    if (isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or
            not math.isfinite(float(confidence)) or not 0.0 <= float(confidence) <= 1.0):
        raise ObservationContractError("ground/camera evidence confidence must be inside [0,1]")
    return (float(confidence) if passed else 0.0,
            {"schema": value["schema"], "receipt_id": receipt_id, "checks": normalized,
             "confidence": float(confidence), "pass": passed})


def adapt_observations(*, observations_path: str | Path, observations_sha256: str,
                       npz_path: str | Path, npz_sha256: str,
                       anchor_map_path: str | Path, anchor_map_sha256: str,
                       fitting_bundle_path: str | Path, fitting_bundle_sha256: str,
                       immutable_manifest_path: str | Path, immutable_manifest_sha256: str,
                       source_video_sha256: str,
                       action: str, loop: bool, clip_type: str,
                       ground_camera_evidence: Mapping[str, Any] | None = None,
                       options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Build a motion-contract sample from pinned official runtime outputs."""
    opt = _values(options)
    paths = {
        "observations": _pinned(observations_path, observations_sha256, "observations"),
        "npz": _pinned(npz_path, npz_sha256, "observations_npz"),
        "anchor_map": _pinned(anchor_map_path, anchor_map_sha256, "anchor_map"),
        "fitting_bundle": _pinned(fitting_bundle_path, fitting_bundle_sha256, "fitting_bundle"),
        "immutable_manifest": _pinned(immutable_manifest_path, immutable_manifest_sha256, "immutable_manifest"),
    }
    observations = _json(paths["observations"], "observations")
    anchor_map = _json(paths["anchor_map"], "anchor_map")
    bundle = _json(paths["fitting_bundle"], "fitting_bundle")
    manifest = _json(paths["immutable_manifest"], "immutable_manifest")
    if observations.get("schema") != OBS_SCHEMA:
        raise ObservationContractError(f"observations schema must be {OBS_SCHEMA}")
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ObservationContractError(f"immutable manifest schema must be {MANIFEST_SCHEMA}")
    if bundle.get("schema") != BUNDLE_SCHEMA:
        raise ObservationContractError(f"fitting bundle schema must be {BUNDLE_SCHEMA}")
    bundle_manifest = manifest.get("bundle_manifest") if isinstance(manifest.get("bundle_manifest"), Mapping) else {}
    if bundle_manifest.get("sha256") != fitting_bundle_sha256:
        raise ObservationContractError("immutable manifest does not bind the fitting bundle hash")
    anchor_by_track, source_model = _anchor_contract(anchor_map, bundle, immutable_manifest_sha256)
    provenance = observations.get("provenance") if isinstance(observations.get("provenance"), Mapping) else {}
    if provenance.get("runtime") != "autorig-official-animal-tracking.v1":
        raise ObservationContractError("observations are not from the official animal tracking runtime")
    if provenance.get("bundle_sha256") != fitting_bundle_sha256:
        raise ObservationContractError("observations fitting-bundle hash mismatch")
    if provenance.get("immutable_manifest_sha256") != immutable_manifest_sha256:
        raise ObservationContractError("observations immutable-manifest hash mismatch")
    expected_video_sha = _expected_sha(source_video_sha256, "source_video_sha256")
    if provenance.get("source_video_sha256") != expected_video_sha:
        raise ObservationContractError("observations source-video hash mismatch")
    if provenance.get("relative_depth_contract") != "calibrated_to_camera_z_from_immutable_actionless_reference":
        raise ObservationContractError("relative VDA depth is not calibrated camera-Z evidence")
    calibration = provenance.get("camera_z_calibration") if isinstance(provenance.get("camera_z_calibration"), Mapping) else {}
    coverage = calibration.get("first_frame_output_coverage")
    if not isinstance(coverage, (int, float)) or isinstance(coverage, bool) or float(coverage) < opt["min_calibration_coverage"]:
        raise ObservationContractError("camera-Z calibration coverage is missing or weak")

    frame_count = observations.get("frame_count")
    width, height, fps = observations.get("width"), observations.get("height"), observations.get("fps")
    if (isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 2 or
            isinstance(width, bool) or not isinstance(width, int) or width < 1 or
            isinstance(height, bool) or not isinstance(height, int) or height < 1 or
            isinstance(fps, bool) or not isinstance(fps, (int, float)) or not math.isfinite(float(fps)) or fps <= 0):
        raise ObservationContractError("invalid observation dimensions/fps")
    try:
        archive = np.load(paths["npz"], allow_pickle=False)
        arrays = {key: np.asarray(archive[key]) for key in archive.files}
    except Exception as exc:
        raise ObservationContractError(f"invalid observations NPZ: {exc}") from exc
    required_arrays = {"tracks_xy", "visible", "confidence", "track_ids", "anchor_ids", "camera_z", "fps"}
    if not required_arrays.issubset(arrays):
        raise ObservationContractError("NPZ lacks calibrated tracks/visibility/confidence/camera_z arrays")
    track_ids = [str(value) for value in arrays["track_ids"].tolist()]
    anchor_ids = [str(value) for value in arrays["anchor_ids"].tolist()]
    if len(track_ids) != len(set(track_ids)) or len(track_ids) != len(anchor_ids):
        raise ObservationContractError("NPZ track IDs must be unique and align with anchor IDs")
    if len(anchor_ids) != len(set(anchor_ids)):
        raise ObservationContractError("NPZ anchor IDs must be unique")
    selected_indices = []
    for semantic_id in REQUIRED_TRACKS:
        expected_anchor = str(anchor_by_track[semantic_id]["anchor_id"])
        if expected_anchor not in anchor_ids:
            raise ObservationContractError(f"NPZ lacks exact source anchor for {semantic_id}: {expected_anchor}")
        selected_indices.append(anchor_ids.index(expected_anchor))
    points, visible, confidence, camera_z = (arrays[key] for key in ("tracks_xy", "visible", "confidence", "camera_z"))
    track_count = len(track_ids)
    if (points.shape != (frame_count, track_count, 2) or visible.shape != (frame_count, track_count) or
            confidence.shape != (frame_count, track_count) or camera_z.shape != (frame_count, height, width)):
        raise ObservationContractError("NPZ observation array shapes do not match observations.json")
    if visible.dtype != np.bool_:
        raise ObservationContractError("visibility mask must be boolean; thresholds are not guessed")
    if not np.all(np.isfinite(points)) or not np.all(np.isfinite(confidence)):
        raise ObservationContractError("tracker points/confidence contain non-finite values")
    if abs(float(np.asarray(arrays["fps"]).reshape(())) - float(fps)) > 1e-9:
        raise ObservationContractError("NPZ and JSON fps differ")

    json_tracks = observations.get("tracks")
    if not isinstance(json_tracks, list) or len(json_tracks) != track_count:
        raise ObservationContractError("observations JSON/NPZ track counts differ")
    json_by_id = {row.get("id"): row for row in json_tracks if isinstance(row, Mapping)}
    if set(json_by_id) != set(track_ids):
        raise ObservationContractError("JSON and NPZ track identities differ")
    for index, track_id in enumerate(track_ids):
        row = json_by_id[track_id]
        if row.get("anchor_id") != anchor_ids[index]:
            raise ObservationContractError(f"JSON anchor mismatch for {track_id}")
        rows = row.get("points")
        if not isinstance(rows, list) or len(rows) != frame_count:
            raise ObservationContractError(f"JSON frame rows missing for {track_id}")
        for frame, point in enumerate(rows):
            if (not isinstance(point, Mapping) or point.get("frame") != frame or
                    point.get("visible") is not bool(visible[frame, index])):
                raise ObservationContractError(f"JSON/NPZ visibility mismatch for {track_id} frame {frame}")
            try:
                json_x, json_y = float(point.get("x")), float(point.get("y"))
                json_confidence = float(point.get("confidence"))
            except (TypeError, ValueError) as exc:
                raise ObservationContractError(f"invalid JSON point for {track_id} frame {frame}") from exc
            if (not math.isfinite(json_x) or not math.isfinite(json_y) or not math.isfinite(json_confidence) or
                    abs(json_x - float(points[frame, index, 0])) > 1e-5 or
                    abs(json_y - float(points[frame, index, 1])) > 1e-5 or
                    abs(json_confidence - float(confidence[frame, index])) > 1e-5):
                raise ObservationContractError(f"JSON/NPZ point-confidence mismatch for {track_id} frame {frame}")

    camera = bundle.get("camera") if isinstance(bundle.get("camera"), Mapping) else {}
    intrinsics = camera.get("intrinsics") if isinstance(camera.get("intrinsics"), Mapping) else {}
    resolution = camera.get("resolution")
    if not isinstance(resolution, list) or len(resolution) != 2:
        raise ObservationContractError("bundle camera resolution missing")
    sx, sy = width / float(resolution[0]), height / float(resolution[1])
    if abs(sx - sy) > 1e-6:
        raise ObservationContractError("observation camera aspect differs from immutable bundle")
    scaled_intrinsics = {"fx": float(intrinsics["fx"]) * sx, "fy": float(intrinsics["fy"]) * sy,
                         "cx": float(intrinsics["cx"]) * sx, "cy": float(intrinsics["cy"]) * sy}
    camera_to_world = _matrix(camera.get("camera_to_world"), "camera.camera_to_world")

    pending = []
    tracks_3d: dict[str, dict[str, Any]] = {}
    visibility_metrics = {}
    for semantic_id, index in zip(REQUIRED_TRACKS, selected_indices):
        runtime_track_id = track_ids[index]
        frame_visible = np.asarray(visible[:, index], dtype=bool)
        frame_confident = np.asarray(confidence[:, index], dtype=np.float64) >= opt["min_frame_confidence"]
        accepted_mask = frame_visible & frame_confident
        visibility_fraction = float(np.mean(accepted_mask))
        positions = []
        depth_valid = np.zeros(frame_count, dtype=bool)
        for frame in range(frame_count):
            if not accepted_mask[frame]:
                positions.append(None)
                continue
            x, y = (float(value) for value in points[frame, index])
            depth = _sample_depth(camera_z[frame], x, y, int(opt["depth_patch_radius_px"]),
                                  int(opt["min_depth_patch_samples"]))
            if depth is None:
                positions.append(None)
                continue
            world = _unproject(x, y, depth, scaled_intrinsics, camera_to_world)
            positions.append(_blender_to_gltf(world).tolist())
            depth_valid[frame] = True
        complete = (visibility_fraction >= opt["min_track_visible_fraction"] and all(row is not None for row in positions))
        visibility_metrics[semantic_id] = {
            "runtime_track_id": runtime_track_id,
            "source_anchor_id": anchor_ids[index],
            "visible_confident_fraction": visibility_fraction,
            "depth_valid_fraction": float(np.mean(depth_valid)),
            "visible_mask": [bool(value) for value in frame_visible],
            "confidence_mask": [bool(value) for value in frame_confident],
            "depth_valid_mask": [bool(value) for value in depth_valid],
        }
        if not complete:
            pending.append(f"incomplete_observed_track:{semantic_id}")
            continue
        semantic = REQUIRED_TRACKS[semantic_id]
        tracks_3d[semantic] = {
            "positions": positions, "source_track": runtime_track_id,
            "source_semantic_id": semantic_id,
            "source_anchor_id": anchor_ids[index], "source_observed": True,
            "identity_observed": True, "generated": False, "mirrored": False,
            "visibility_mask": [bool(value) for value in accepted_mask],
        }

    ground = bundle.get("ground_plane") if isinstance(bundle.get("ground_plane"), Mapping) else {}
    ground_normal_blender = _normalize(_finite_vector(ground.get("normal"), 3, "ground.normal"), "ground.normal")
    ground_height = ground.get("height")
    geometry = bundle.get("renderer", {}).get("geometry_contract", {}) if isinstance(bundle.get("renderer"), Mapping) else {}
    reference_plane_identity = geometry.get("raw_evaluated_vertex_identity") is True
    ground_confidence, ground_receipt = _ground_camera_receipt(
        ground_camera_evidence, source_video_sha256=expected_video_sha,
        observations_sha256=observations_sha256)
    if not isinstance(ground_height, (int, float)) or not math.isfinite(float(ground_height)):
        raise ObservationContractError("source ground height missing")
    if ground_confidence < opt["min_ground_confidence"]:
        pending.append("weak_source_ground_plane")
    normal_gltf = _normalize(_blender_to_gltf(ground_normal_blender, point=False), "gltf ground normal")
    # A Blender plane dot(n,p)=height becomes the same source plane after the
    # one declared basis conversion.  Offset follows motion-contract n.p+d=0.
    ground_point_gltf = _blender_to_gltf(ground_normal_blender * float(ground_height))
    ground_offset = -float(np.dot(normal_gltf, ground_point_gltf))

    anchors = {track_id: _blender_to_gltf(_finite_vector(anchor_by_track[track_id].get("world"), 3,
                                                         f"{track_id}.world")) for track_id in REQUIRED_TRACKS}
    left_mid = (anchors["fore_left.hoof"] + anchors["hind_left.hoof"]) * 0.5
    right_mid = (anchors["fore_right.hoof"] + anchors["hind_right.hoof"]) * 0.5
    front_mid = (anchors["fore_left.hoof"] + anchors["fore_right.hoof"]) * 0.5
    hind_mid = (anchors["hind_left.hoof"] + anchors["hind_right.hoof"]) * 0.5
    physical_right = _normalize(right_mid - left_mid, "anatomical right")
    physical_forward = _normalize(front_mid - hind_mid, "anatomical forward")
    bundle_up = _normalize(_finite_vector(anchor_map.get("coordinates", {}).get("bundle_up"), 3,
                                          "coordinates.bundle_up"), "bundle up")
    up_gltf = _normalize(_blender_to_gltf(bundle_up, point=False), "gltf up")
    sample = {
        "action": str(action), "times": [frame / float(fps) for frame in range(frame_count)],
        "model_height": float(anchor_map.get("coordinates", {}).get("bundle_height")),
        "loop": bool(loop), "clip_type": str(clip_type),
        "ground": {"normal": normal_gltf.tolist(), "offset": ground_offset,
                   "source": "immutable_actionless_bundle.ground_plane", "confidence": ground_confidence,
                   "reference_plane_identity": reference_plane_identity,
                   "temporal_video_evidence_receipt": None if ground_receipt is None else ground_receipt["receipt_id"]},
        "axes": {"up": up_gltf.tolist(), "forward": physical_forward.tolist(),
                 "right": physical_right.tolist(), "source_supported": True,
                 "source": "six_exact_surface_anchors"},
        "hooves": {leg: tracks_3d[leg] for leg in ("LF", "RF", "LH", "RH") if leg in tracks_3d},
    }
    if "head" in tracks_3d: sample["head_position"] = tracks_3d["head"]
    if "tail" in tracks_3d: sample["tail"] = tracks_3d["tail"]
    # Root position/quaternion and head rotation are intentionally absent:
    # tracker points cannot prove them.  The motion contract will keep the
    # corresponding action pending until source-bound post-retarget tracks exist.
    pending.extend(["post_retarget_root_position_track", "post_retarget_root_rotation_track"])
    if action in {"idle_look_around", "look"}: pending.append("post_retarget_head_rotation_track")
    return {
        "schema": SCHEMA, "sample": sample, "pending": sorted(set(pending)),
        "metrics": {"tracks": visibility_metrics, "ground_confidence": ground_confidence,
                    "reference_plane_identity": reference_plane_identity,
                    "ground_camera_evidence": ground_receipt,
                    "frame_count": frame_count, "fps": float(fps)},
        "provenance": {
            "observations_sha256": observations_sha256, "npz_sha256": npz_sha256,
            "anchor_map_sha256": anchor_map_sha256, "fitting_bundle_sha256": fitting_bundle_sha256,
            "immutable_manifest_sha256": immutable_manifest_sha256,
            "source_video_sha256": expected_video_sha,
            "source_model_sha256": source_model.get("sha256"),
            "coordinate_conversion": "calibrated_camera_z_to_blender_world_then_blender_zup_to_gltf_yup_once",
            "animation_quality_approved": False,
        },
    }


__all__ = ["ObservationContractError", "REQUIRED_TRACKS", "SCHEMA", "adapt_observations", "options_get"]
