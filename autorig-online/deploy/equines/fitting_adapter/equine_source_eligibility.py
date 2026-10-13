#!/usr/bin/env python3
"""Generic source-LBS chain eligibility for measured equine targets.

The kernel does not alter target positions.  It computes conservative physical
reach bounds from the immutable rig and marks each observed target eligible or
pending before a fitter/contact objective consumes it.
"""
from __future__ import annotations

import math
import re
from typing import Any, Mapping

import numpy as np


SCHEMA = "autorig.equine-source-chain-eligibility.v1"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class EligibilityError(ValueError):
    pass


def options_get(options: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    specs = {
        "weight_sum_tolerance": (1e-4, 1e-8, 0.05, "Maximum source LBS influence-weight sum error before fail-closed rejection."),
        "rigid_matrix_tolerance": (1e-5, 1e-9, 1e-2, "Maximum rest-transform orthonormal/affine error."),
        "numeric_reach_tolerance_ratio": (0.005, 0.0, 0.10, "Numeric allowance added to source reach / source height."),
        "temporal_jump_multiplier": (2.0, 1.01, 10.0, "Maximum consecutive measured pair-distance multiple before downstream target becomes pending."),
        "temporal_jump_tolerance_ratio": (0.005, 0.0, 0.10, "Numeric allowance for the consecutive pair jump / source height."),
    }
    supplied = dict(options or {})
    unknown = sorted(set(supplied) - set(specs))
    if unknown: raise EligibilityError("unknown options: " + ", ".join(unknown))
    result = {}
    for name, (default, low, high, doc) in specs.items():
        value = supplied.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise EligibilityError(f"{name} must be finite")
        value = float(value)
        if not low <= value <= high: raise EligibilityError(f"{name} must be in [{low}, {high}]")
        result[name] = {"value": value, "default": default, "min": low, "max": high, "doc": doc}
    return result


def _values(options): return {key: row["value"] for key, row in options_get(options).items()}


def _rigid(matrix: Any, tolerance: float, field: str) -> np.ndarray:
    value = np.asarray(matrix, dtype=np.float64)
    if value.shape != (4, 4) or not np.all(np.isfinite(value)):
        raise EligibilityError(f"{field} must be a finite 4x4 rest transform")
    if (np.max(np.abs(value[3] - np.asarray([0, 0, 0, 1], dtype=np.float64))) > tolerance or
            np.max(np.abs(value[:3, :3].T @ value[:3, :3] - np.eye(3))) > tolerance or
            abs(float(np.linalg.det(value[:3, :3])) - 1.0) > tolerance):
        raise EligibilityError(f"{field} contains scale/shear/reflection; source reach is uncertain")
    return value


def _influences(rig: Any, anchor_id: str, weight_tolerance: float):
    anchor = getattr(rig, "anchors", {}).get(anchor_id)
    if anchor is None: raise EligibilityError(f"unknown immutable source anchor: {anchor_id}")
    influences = tuple(getattr(anchor, "influences", ()))
    if not influences: raise EligibilityError(f"source anchor has no LBS influences: {anchor_id}")
    rows, total = [], 0.0
    for influence in influences:
        weight = getattr(influence, "weight", None)
        local = np.asarray(getattr(influence, "bone_local", None), dtype=np.float64)
        bone = getattr(influence, "bone", None)
        if (isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(float(weight)) or
                float(weight) < 0 or local.shape != (3,) or not np.all(np.isfinite(local)) or bone not in rig.bones):
            raise EligibilityError(f"invalid source LBS influence: {anchor_id}")
        if float(weight) > 0:
            rows.append((str(bone), float(weight), local)); total += float(weight)
    if not rows or abs(total - 1.0) > weight_tolerance:
        raise EligibilityError(f"source LBS weights are not normalized: {anchor_id} sum={total}")
    return [(bone, weight / total, local) for bone, weight, local in rows]


def _ancestor_path(rig: Any, bone: str) -> list[str]:
    result, seen, current = [], set(), bone
    while current is not None:
        if current in seen or current not in rig.bones: raise EligibilityError("source bone hierarchy is cyclic/incomplete")
        result.append(current); seen.add(current); current = rig.bones[current].parent
    return result


def _pair_bound(rig: Any, anchor_a: str, anchor_b: str, opt: Mapping[str, float]) -> dict[str, Any]:
    ia = _influences(rig, anchor_a, opt["weight_sum_tolerance"])
    ib = _influences(rig, anchor_b, opt["weight_sum_tolerance"])
    contributions, bound = [], 0.0
    for bone_a, weight_a, local_a in ia:
        for bone_b, weight_b, local_b in ib:
            weight = weight_a * weight_b
            if bone_a == bone_b:
                pair_bound = float(np.linalg.norm(local_a - local_b))
                lca = bone_a
            else:
                path_a, path_b = _ancestor_path(rig, bone_a), _ancestor_path(rig, bone_b)
                common = next((name for name in path_a if name in set(path_b)), None)
                if common is None: raise EligibilityError(f"influence bones have no common ancestor: {bone_a}, {bone_b}")
                path_length = 0.0
                for path, start in ((path_a, bone_a), (path_b, bone_b)):
                    current = start
                    while current != common:
                        path_length += float(np.linalg.norm(rig.bones[current].rest_local[:3, 3]))
                        current = rig.bones[current].parent
                pair_bound = float(np.linalg.norm(local_a)) + path_length + float(np.linalg.norm(local_b))
                lca = common
            bound += weight * pair_bound
            contributions.append({"bone_a": bone_a, "bone_b": bone_b, "lca": lca,
                                  "weight": weight, "bound": pair_bound})
    return {"anchor_a": anchor_a, "anchor_b": anchor_b, "weighted_bound": bound,
            "contributions": contributions}


def evaluate_source_eligibility(rig: Any, constraints: Mapping[str, Any], *,
                                constraints_sha256: str,
                                options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return per-target eligibility; input constraints remain untouched."""
    opt = _values(options)
    if not isinstance(constraints_sha256, str) or SHA_RE.fullmatch(constraints_sha256) is None:
        raise EligibilityError("constraints_sha256 must be exact lowercase SHA-256")
    if (constraints.get("schema") != "autorig.equine-sagittal-limb-constraints.v1" or
            constraints.get("state") != "fit_candidate_constraints"):
        raise EligibilityError("unsupported source constraints document")
    height = constraints.get("source_height")
    if isinstance(height, bool) or not isinstance(height, (int, float)) or not math.isfinite(float(height)) or height <= 0:
        raise EligibilityError("positive source_height is required")
    height = float(height)
    timing = constraints.get("timing") if isinstance(constraints.get("timing"), Mapping) else {}
    frame_count, source_frames = timing.get("frame_count"), timing.get("source_frame_indices")
    if (isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 2 or
            not isinstance(source_frames, list) or len(source_frames) != frame_count or
            any(isinstance(value, bool) or not isinstance(value, int) for value in source_frames)):
        raise EligibilityError("constraint timing is invalid")
    for name, bone in rig.bones.items(): _rigid(bone.rest_local, opt["rigid_matrix_tolerance"], f"bones.{name}.rest_local")
    legs = constraints.get("constraints")
    if not isinstance(legs, Mapping) or set(legs) != {"LF", "RF", "LH", "RH"}:
        raise EligibilityError("exact four leg constraint rows are required")

    output_targets, output_pairs, reach_bounds, pending = {}, {}, {}, []
    for leg in ("LF", "RF", "LH", "RH"):
        leg_row = legs[leg]
        if not isinstance(leg_row, Mapping): raise EligibilityError(f"invalid leg row: {leg}")
        chain_names = list(leg_row.get("source_bone_chain") or [])[:4]
        if len(chain_names) != 4: raise EligibilityError(f"{leg} source chain must contain Toe/Foot/Leg/UpLeg")
        track_by_bone = {chain_names[0]: leg_row}
        joints = leg_row.get("joint_constraints") if isinstance(leg_row.get("joint_constraints"), Mapping) else {}
        for bone in chain_names[1:]:
            if bone in joints: track_by_bone[bone] = joints[bone]
        observations = {}
        targets = {}
        for bone in chain_names:
            track = track_by_bone.get(bone)
            anchor_id = track.get("source_anchor_id") if isinstance(track, Mapping) else None
            frame_rows = {}
            if isinstance(track, Mapping):
                for row in track.get("constraints", []):
                    frame = row.get("frame") if isinstance(row, Mapping) else None
                    position = np.asarray(row.get("position"), dtype=np.float64) if isinstance(row, Mapping) else np.asarray([])
                    if (isinstance(frame, bool) or not isinstance(frame, int) or not 0 <= frame < frame_count or
                            frame in frame_rows or position.shape != (3,) or not np.all(np.isfinite(position)) or
                            row.get("source_frame") != source_frames[frame]):
                        raise EligibilityError(f"{leg}.{bone} invalid measured constraint row")
                    frame_rows[frame] = row
            observations[bone] = (anchor_id, frame_rows)
            if anchor_id is not None:
                targets[anchor_id] = {"bone": bone, "frames": [
                    {"frame": frame, "source_frame": source_frames[frame], "observed": frame in frame_rows,
                     "eligible": frame in frame_rows,
                     "confidence": None if frame not in frame_rows else frame_rows[frame].get("confidence"),
                     "reasons": [] if frame in frame_rows else ["MISSING_OBSERVATION"]}
                    for frame in range(frame_count)]}
        pair_results = {}
        pair_validity = {}
        for distal, proximal in zip(chain_names, chain_names[1:]):
            anchor_a, rows_a = observations[distal]; anchor_b, rows_b = observations[proximal]
            pair_key = f"{distal}|{proximal}"
            if anchor_a is None or anchor_b is None:
                pair_results[pair_key] = {"bound": None, "frames": []}
                pair_validity[pair_key] = {}
                continue
            bound = _pair_bound(rig, anchor_a, anchor_b, opt)
            reach_bounds[f"{anchor_a}|{anchor_b}"] = bound
            tolerance = opt["numeric_reach_tolerance_ratio"] * height
            jump_tolerance = opt["temporal_jump_tolerance_ratio"] * height
            rows, validity, previous = [], {}, None
            for frame in range(frame_count):
                if frame not in rows_a or frame not in rows_b:
                    rows.append({"frame": frame, "source_frame": source_frames[frame], "eligible": False,
                                 "distance": None, "bound": bound["weighted_bound"],
                                 "reasons": ["ADJACENT_SUPPORT_MISSING"]})
                    validity[frame] = (False, ("ADJACENT_SUPPORT_MISSING",))
                    continue
                distance = float(np.linalg.norm(np.asarray(rows_a[frame]["position"]) -
                                                np.asarray(rows_b[frame]["position"])))
                reasons = []
                if distance > bound["weighted_bound"] + tolerance:
                    reasons.append("SOURCE_REACH_EXCEEDED")
                if previous is not None and source_frames[frame] == previous[0] + 1 and \
                        distance > opt["temporal_jump_multiplier"] * previous[1] + jump_tolerance:
                    reasons.append("TEMPORAL_PAIR_JUMP")
                eligible = not reasons
                rows.append({"frame": frame, "source_frame": source_frames[frame], "eligible": eligible,
                             "distance": distance, "bound": bound["weighted_bound"], "reasons": reasons})
                validity[frame] = (eligible, tuple(reasons))
                previous = (source_frames[frame], distance)
            pair_results[pair_key] = {"anchor_a": anchor_a, "anchor_b": anchor_b,
                                      "bound": bound["weighted_bound"], "frames": rows}
            pair_validity[pair_key] = validity

        for index, bone in enumerate(chain_names):
            anchor_id, measured = observations[bone]
            if anchor_id is None: continue
            for frame_row in targets[anchor_id]["frames"]:
                frame = frame_row["frame"]
                if not frame_row["observed"]: continue
                support = []
                if index > 0: support.append(pair_validity.get(f"{chain_names[index-1]}|{bone}", {}).get(frame))
                if index < 3: support.append(pair_validity.get(f"{bone}|{chain_names[index+1]}", {}).get(frame))
                valid_support = [row for row in support if row is not None]
                if not valid_support:
                    frame_row["eligible"] = False; frame_row["reasons"].append("ADJACENT_SUPPORT_MISSING")
                elif not any(row[0] for row in valid_support):
                    frame_row["eligible"] = False
                    combined = {reason for row in valid_support for reason in row[1]}
                    frame_row["reasons"].extend(
                        reason for reason in ("SOURCE_REACH_EXCEEDED", "TEMPORAL_PAIR_JUMP",
                                              "ADJACENT_SUPPORT_MISSING", "BOUND_UNCERTAIN")
                        if reason in combined)
                if not frame_row["eligible"]:
                    pending.append(f"ineligible_target:{leg}:{anchor_id}:{frame}")
        output_targets[leg], output_pairs[leg] = targets, pair_results
    provenance = constraints.get("provenance") if isinstance(constraints.get("provenance"), Mapping) else {}
    return {"schema": SCHEMA, "state": "source_bound_target_eligibility",
            "constraints_sha256": constraints_sha256, "options": opt,
            "reach_bounds": reach_bounds, "pairs": output_pairs, "targets": output_targets,
            "pending": sorted(set(pending)),
            "contact_semantics": {"toe_contact_requires_eligible_target": True,
                                  "ineligible_toe_contact_action": "suppress_to_pending",
                                  "opposite_leg_inference": False},
            "provenance": {"rig_metadata_sha256": getattr(rig, "metadata_sha256", None),
                           "rig_manifest_sha256": getattr(rig, "immutable_manifest_sha256", None),
                           "source_model_sha256": provenance.get("source_model_sha256"),
                           "constraints_sha256": constraints_sha256,
                           "input_targets_mutated": False,
                           "bound_method": "normalized_source_lbs_weighted_pair_upper_reach_to_lca"}}


__all__ = ["EligibilityError", "SCHEMA", "evaluate_source_eligibility", "options_get"]
