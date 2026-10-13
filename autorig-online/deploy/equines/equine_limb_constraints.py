#!/usr/bin/env python3
"""Source-bound sagittal limb constraints for linear equine gait fitting.

This is a candidate-building adapter, not metric 3D observation or animation
QA.  Each visible 2D hoof point is intersected with that hoof's immutable
source sagittal plane.  The fixed-camera/planar-limb premise is explicitly
recorded as an assumption and must later be validated on the real retargeted
3D result.  Occluded points are never interpolated or mirrored.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np


SCHEMA = "autorig.equine-sagittal-limb-constraints.v1"
ALLOWED_ACTIONS = frozenset({
    "walk", "walk_forward", "trot", "trot_jog", "canter",
    "gallop", "run", "sprint",
})
HOOFS = {
    "fore_left.hoof": ("LF", "LeftFrontToe"),
    "fore_right.hoof": ("RF", "RightFrontToe"),
    "hind_left.hoof": ("LH", "LeftHindToe"),
    "hind_right.hoof": ("RH", "RightHindToe"),
}
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class ConstraintContractError(ValueError):
    pass


def options_get(options: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    specs = {
        "min_frame_confidence": (0.50, 0.01, 1.0, "Minimum source tracker confidence for an emitted constraint."),
        "sagittal_plane_offset_ratio": (0.0, -0.05, 0.05, "Signed offset from each immutable source-hoof plane / source height; default is the exact source plane."),
        "max_plane_deviation_ratio": (0.02, 0.0, 0.10, "Maximum numerical ray/plane residual / source height."),
        "min_ray_plane_dot": (0.02, 0.001, 1.0, "Minimum absolute ray dot plane-normal; near-parallel rays stay pending."),
        "max_constraint_distance_ratio": (10.0, 0.1, 100.0, "Maximum camera-to-constraint distance / source height."),
    }
    supplied = dict(options or {})
    unknown = sorted(set(supplied) - set(specs))
    if unknown:
        raise ConstraintContractError("unknown options: " + ", ".join(unknown))
    result = {}
    for name, (default, low, high, doc) in specs.items():
        value = supplied.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ConstraintContractError(f"{name} must be finite")
        value = float(value)
        if not low <= value <= high:
            raise ConstraintContractError(f"{name} must be in [{low}, {high}]")
        result[name] = {"value": value, "default": default, "min": low, "max": high, "doc": doc}
    return result


def _values(options):
    return {key: row["value"] for key, row in options_get(options).items()}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _pin(path: str | Path, expected: str, field: str) -> Path:
    resolved = Path(path).resolve()
    expected = str(expected or "").lower()
    if not SHA_RE.fullmatch(expected):
        raise ConstraintContractError(f"{field}_sha256 must be exact lowercase SHA-256")
    if not resolved.is_file() or _sha(resolved) != expected:
        raise ConstraintContractError(f"{field} SHA-256 mismatch or file missing")
    return resolved


def _json(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ConstraintContractError(f"invalid {field}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConstraintContractError(f"{field} must be an object")
    return value


def _vector(value: Any, size: int, field: str) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.shape != (size,) or not np.all(np.isfinite(result)):
        raise ConstraintContractError(f"{field} must be a finite {size}-vector")
    return result


def _unit(value: np.ndarray, field: str) -> np.ndarray:
    length = float(np.linalg.norm(value))
    if not math.isfinite(length) or length < 1e-9:
        raise ConstraintContractError(f"{field} is degenerate")
    return value / length


def _matrix(value: Any, field: str) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.size != 16:
        raise ConstraintContractError(f"{field} must contain 16 values")
    result = result.reshape(4, 4)
    if not np.all(np.isfinite(result)) or abs(float(np.linalg.det(result))) < 1e-10:
        raise ConstraintContractError(f"{field} must be finite/invertible")
    return result


def _gltf(value: np.ndarray) -> np.ndarray:
    return np.asarray([value[0], value[2], -value[1]], dtype=np.float64)


def _skeleton_chains(skeleton: Mapping[str, Any], hoof_rows: Mapping[str, Mapping[str, Any]]) -> dict[str, list[str]]:
    armatures = skeleton.get("armatures")
    if not isinstance(armatures, list) or len(armatures) != 1 or not isinstance(armatures[0], Mapping):
        raise ConstraintContractError("skeleton must contain exactly one source armature")
    bones = armatures[0].get("bones")
    if not isinstance(bones, list):
        raise ConstraintContractError("source skeleton bones missing")
    parents = {}
    for bone in bones:
        if not isinstance(bone, Mapping) or not isinstance(bone.get("name"), str):
            raise ConstraintContractError("invalid source bone row")
        parents[bone["name"]] = bone.get("parent")
    chains = {}
    for semantic_id, (_, expected_toe) in HOOFS.items():
        if hoof_rows[semantic_id].get("bone") != expected_toe:
            raise ConstraintContractError(f"{semantic_id} exact toe identity mismatch")
        chain, current, seen = [], expected_toe, set()
        while current is not None and current not in seen:
            if current not in parents:
                raise ConstraintContractError(f"source bone {current} missing")
            chain.append(current); seen.add(current); current = parents[current]
            if len(chain) >= 4 and (current in {"Hips", "Spine2"} or current is None):
                if current is not None: chain.append(current)
                break
        side = "Left" if "left" in semantic_id else "Right"
        if len(chain) < 4 or any(not name.startswith(side) for name in chain[:4]):
            raise ConstraintContractError(f"{semantic_id} does not have an actual same-side hoof chain")
        chains[semantic_id] = chain
    return chains


def build_constraints(*, observations_path: str | Path, observations_sha256: str,
                      anchor_map_path: str | Path, anchor_map_sha256: str,
                      fitting_bundle_path: str | Path, fitting_bundle_sha256: str,
                      immutable_manifest_path: str | Path, immutable_manifest_sha256: str,
                      skeleton_path: str | Path, skeleton_sha256: str,
                      source_video_sha256: str, action: str,
                      options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return source-plane IK candidate constraints; never an acceptance verdict."""
    if action not in ALLOWED_ACTIONS:
        raise ConstraintContractError("sagittal constraints are restricted to linear walk/trot/canter/gallop")
    opt = _values(options)
    paths = {
        "observations": _pin(observations_path, observations_sha256, "observations"),
        "anchor_map": _pin(anchor_map_path, anchor_map_sha256, "anchor_map"),
        "bundle": _pin(fitting_bundle_path, fitting_bundle_sha256, "fitting_bundle"),
        "manifest": _pin(immutable_manifest_path, immutable_manifest_sha256, "immutable_manifest"),
        "skeleton": _pin(skeleton_path, skeleton_sha256, "skeleton"),
    }
    observations, anchor_map = _json(paths["observations"], "observations"), _json(paths["anchor_map"], "anchor_map")
    bundle, manifest, skeleton = (_json(paths[name], name) for name in ("bundle", "manifest", "skeleton"))
    if observations.get("schema") != "autorig-fitting-observations.v1":
        raise ConstraintContractError("unsupported observations schema")
    if anchor_map.get("schema") != "autorig.equine-fitting-anchor-map/1":
        raise ConstraintContractError("unsupported anchor-map schema")
    if bundle.get("schema") != "autorig-actionless-fitting-bundle.v1" or manifest.get("schema") != "autorig-fitting-immutable-bundle.v1":
        raise ConstraintContractError("unsupported immutable source bundle schema")
    if anchor_map.get("bundle_manifest_sha256") != immutable_manifest_sha256:
        raise ConstraintContractError("anchor map is not bound to immutable manifest")
    if manifest.get("bundle_manifest", {}).get("sha256") != fitting_bundle_sha256:
        raise ConstraintContractError("immutable manifest does not bind fitting bundle")
    artifact = bundle.get("artifacts", {}).get("skeleton", {})
    if artifact.get("sha256") != skeleton_sha256:
        raise ConstraintContractError("fitting bundle does not bind source skeleton")
    source = bundle.get("source", {})
    if anchor_map.get("source_sha256") != source.get("sha256"):
        raise ConstraintContractError("anchor map/source model mismatch")
    provenance = observations.get("provenance") if isinstance(observations.get("provenance"), Mapping) else {}
    if provenance.get("runtime") != "autorig-official-animal-tracking.v1":
        raise ConstraintContractError("observations are not from the official tracking runtime")
    expected_video = str(source_video_sha256 or "").lower()
    if not SHA_RE.fullmatch(expected_video) or provenance.get("source_video_sha256") != expected_video:
        raise ConstraintContractError("observations source-video hash mismatch")
    if provenance.get("bundle_sha256") != fitting_bundle_sha256 or provenance.get("immutable_manifest_sha256") != immutable_manifest_sha256:
        raise ConstraintContractError("observations source bundle hashes mismatch")

    rows = anchor_map.get("tracks")
    if not isinstance(rows, list): raise ConstraintContractError("anchor-map tracks missing")
    by_semantic = {row.get("track_id"): row for row in rows if isinstance(row, Mapping)}
    if not set(HOOFS).issubset(by_semantic): raise ConstraintContractError("four exact hoof anchors are required")
    for semantic_id, (_, toe) in HOOFS.items():
        row = by_semantic[semantic_id]
        if row.get("bone") != toe or not str(row.get("anchor_id") or "").startswith(toe + ":"):
            raise ConstraintContractError(f"{semantic_id} exact anchor identity mismatch")
    chains = _skeleton_chains(skeleton, by_semantic)

    height = float(anchor_map.get("coordinates", {}).get("bundle_height"))
    if not math.isfinite(height) or height <= 0: raise ConstraintContractError("source height missing")
    anchors = {name: _vector(by_semantic[name].get("world"), 3, name + ".world") for name in HOOFS}
    left_mid = (anchors["fore_left.hoof"] + anchors["hind_left.hoof"]) * .5
    right_mid = (anchors["fore_right.hoof"] + anchors["hind_right.hoof"]) * .5
    front_mid = (anchors["fore_left.hoof"] + anchors["fore_right.hoof"]) * .5
    hind_mid = (anchors["hind_left.hoof"] + anchors["hind_right.hoof"]) * .5
    physical_right = _unit(right_mid - left_mid, "physical right")
    physical_forward = _unit(front_mid - hind_mid, "physical forward")
    physical_up = _unit(_vector(anchor_map.get("coordinates", {}).get("bundle_up"), 3, "bundle_up"), "physical up")
    if max(abs(float(np.dot(physical_right, physical_forward))), abs(float(np.dot(physical_right, physical_up)))) > .1:
        raise ConstraintContractError("source anatomical axes are not a sagittal basis")

    camera = bundle.get("camera") if isinstance(bundle.get("camera"), Mapping) else {}
    matrix = _matrix(camera.get("camera_to_world"), "camera_to_world")
    origin = (matrix @ np.asarray([0, 0, 0, 1.0]))[:3]
    intrinsics = camera.get("intrinsics") if isinstance(camera.get("intrinsics"), Mapping) else {}
    resolution = camera.get("resolution")
    width, image_height = int(observations.get("width", 0)), int(observations.get("height", 0))
    if not isinstance(resolution, list) or len(resolution) != 2 or width < 1 or image_height < 1:
        raise ConstraintContractError("camera/observation dimensions missing")
    sx, sy = width / float(resolution[0]), image_height / float(resolution[1])
    if abs(sx - sy) > 1e-6: raise ConstraintContractError("observation aspect differs from immutable camera")
    fx, fy = float(intrinsics["fx"]) * sx, float(intrinsics["fy"]) * sy
    cx, cy = float(intrinsics["cx"]) * sx, float(intrinsics["cy"]) * sy
    tracks = observations.get("tracks")
    if not isinstance(tracks, list): raise ConstraintContractError("observation tracks missing")
    by_anchor = {}
    for track in tracks:
        if isinstance(track, Mapping) and isinstance(track.get("anchor_id"), str):
            if track["anchor_id"] in by_anchor: raise ConstraintContractError("duplicate observed anchor")
            by_anchor[track["anchor_id"]] = track

    constraints, pending = {}, []
    for semantic_id, (leg, _) in HOOFS.items():
        source_row = by_semantic[semantic_id]
        anchor_id = source_row["anchor_id"]
        track = by_anchor.get(anchor_id)
        if not isinstance(track, Mapping):
            pending.append(f"missing_observed_hoof:{semantic_id}"); continue
        points = track.get("points")
        if not isinstance(points, list):
            pending.append(f"missing_observed_hoof:{semantic_id}"); continue
        plane_point = anchors[semantic_id] + physical_right * opt["sagittal_plane_offset_ratio"] * height
        emitted, masks = [], {"visible": [], "confident": [], "intersected": []}
        for frame, point in enumerate(points):
            visible = isinstance(point, Mapping) and point.get("frame") == frame and point.get("visible") is True
            confidence = float(point.get("confidence", 0.0)) if isinstance(point, Mapping) else 0.0
            confident = math.isfinite(confidence) and confidence >= opt["min_frame_confidence"]
            masks["visible"].append(visible); masks["confident"].append(confident)
            intersected = False
            if visible and confident:
                x, y = float(point["x"]), float(point["y"])
                local = np.asarray([(x - cx) / fx, (cy - y) / fy, -1.0, 0.0])
                direction = _unit((matrix @ local)[:3], "camera ray")
                denominator = float(np.dot(physical_right, direction))
                if abs(denominator) >= opt["min_ray_plane_dot"]:
                    distance = float(np.dot(physical_right, plane_point - origin) / denominator)
                    candidate = origin + distance * direction
                    residual = abs(float(np.dot(physical_right, candidate - plane_point))) / height
                    if (distance > 0 and distance / height <= opt["max_constraint_distance_ratio"] and
                            residual <= opt["max_plane_deviation_ratio"]):
                        emitted.append({"frame": frame, "position": _gltf(candidate).tolist(),
                                        "confidence": confidence, "plane_residual_ratio": residual})
                        intersected = True
            masks["intersected"].append(intersected)
            if visible and not intersected: pending.append(f"unresolved_visible_point:{semantic_id}:{frame}")
        if len(emitted) != len(points): pending.append(f"partial_constraint_track:{semantic_id}")
        constraints[leg] = {
            "semantic_id": semantic_id, "source_anchor_id": anchor_id,
            "runtime_track_id": track.get("id"), "source_bone_chain": chains[semantic_id],
            "derivation": "derived_from_source_plane_constraint", "generated": False, "mirrored": False,
            "plane": {"normal": _gltf(physical_right).tolist(), "point": _gltf(plane_point).tolist(),
                      "offset_ratio": opt["sagittal_plane_offset_ratio"]},
            "constraints": emitted, "masks": masks,
        }
    return {
        "schema": SCHEMA, "state": "fit_candidate_constraints", "accepted_observations": False,
        "animation_qa_pass": False, "action": action, "constraints": constraints,
        "pending": sorted(set(pending)), "assumptions": {
            "fixed_camera": True, "sagittal_limb_motion": True,
            "requires_real_post_retarget_3d_validation": True,
            "metric_depth_used": False, "ground_confidence_claimed": False,
        },
        "axes": {"up": _gltf(physical_up).tolist(), "forward": _gltf(physical_forward).tolist(),
                 "right": _gltf(physical_right).tolist(), "source": "exact_source_hoof_anchors"},
        "options": opt, "provenance": {
            "observations_sha256": observations_sha256, "source_video_sha256": expected_video,
            "anchor_map_sha256": anchor_map_sha256, "fitting_bundle_sha256": fitting_bundle_sha256,
            "immutable_manifest_sha256": immutable_manifest_sha256, "skeleton_sha256": skeleton_sha256,
            "source_model_sha256": source.get("sha256"),
        },
    }


__all__ = ["ALLOWED_ACTIONS", "ConstraintContractError", "SCHEMA", "build_constraints", "options_get"]
