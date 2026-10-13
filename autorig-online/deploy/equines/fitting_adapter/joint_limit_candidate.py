"""Validate an explicitly authored, run-scoped joint-limit candidate.

This tool does not derive anatomical limits and cannot promote category defaults.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path


SCHEMA = "autorig.equine-joint-limit-profile-candidate/1"
RECEIPT = "autorig.equine-joint-limit-candidate-receipt/1"
FIT_DEFAULTS = {"reprojection_weight": 1.0, "depth_weight": 1.0, "silhouette_weight": 0.25,
                "joint_rest_weight": 0.0025, "joint_velocity_weight": 0.02, "joint_acceleration_weight": 0.05,
                "root_velocity_weight": 0.01, "root_acceleration_weight": 0.025,
                "contact_height_weight": 2.0, "contact_slide_weight": 2.0,
                "loop_pose_weight": 2.0, "loop_velocity_weight": 1.0, "robust_loss": "soft_l1",
                "f_scale": 3.0, "max_nfev": 300, "max_variables": 12000,
                "allow_unbounded_joints": False, "active_bones": None}
FIT_FIELDS = set(FIT_DEFAULTS)


def options_get() -> dict:
    return {"schema": "autorig.equine-joint-limit-options/1", "defaults": FIT_DEFAULTS,
            "ranges": {"*_weight": [0.0, 100.0], "f_scale": [0.001, 100.0],
                       "max_nfev": [1, 5000], "max_variables": [1, 1000000]},
            "robust_loss_values": ["linear", "soft_l1", "huber", "cauchy", "arctan"],
            "fixed": {"allow_unbounded_joints": False},
            "note": "Overrides use exact animation_fitting.optimizer.FittingConfig field names."}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def bounded(value, low, high, label):
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number) or not low <= number <= high:
        raise ValueError(f"{label} outside {low}..{high}")
    return number


def build_receipt(args) -> dict:
    source, rig_path, profile_path = args.source.resolve(), args.rig_json.resolve(), args.profile.resolve()
    rig, profile = read(rig_path), read(profile_path)
    if profile.get("schema") != SCHEMA or profile.get("state") != "candidate":
        raise ValueError("profile must be an explicit candidate schema/state")
    if profile.get("scope") != "run" or profile.get("run_id") != args.run_id:
        raise ValueError("profile must be scoped to this exact run")
    if profile.get("approved") is not False or profile.get("category_default") is not False:
        raise ValueError("candidate cannot be approved or a category default")
    source_sha, rig_sha = sha(source), sha(rig_path)
    if profile.get("source_sha256") != source_sha or profile.get("rig_json_sha256") != rig_sha:
        raise ValueError("profile source or rig SHA does not match")
    known = {row.get("name") for row in rig.get("bones") or [] if isinstance(row, dict)}
    limits = profile.get("bones")
    if not isinstance(limits, dict) or not limits:
        raise ValueError("profile has no explicitly authored bone limits")
    normalized = {}
    for bone, axes in limits.items():
        if bone not in known or not isinstance(axes, dict) or set(axes) != {"x", "y", "z"}:
            raise ValueError(f"invalid exact bone/axis contract: {bone}")
        normalized[bone] = {}
        for axis, pair in axes.items():
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError(f"{bone}.{axis} must be [min,max] radians")
            lo = bounded(pair[0], -math.pi, math.pi, f"{bone}.{axis}.min")
            hi = bounded(pair[1], -math.pi, math.pi, f"{bone}.{axis}.max")
            if lo > hi:
                raise ValueError(f"{bone}.{axis} min exceeds max")
            normalized[bone][axis] = [lo, hi]
    if "regularization" in profile:
        raise ValueError("legacy regularization fields are unsupported; use exact fitting_config_overrides")
    opts = profile.get("fitting_config_overrides") or {}
    if not isinstance(opts, dict) or set(opts) - FIT_FIELDS:
        raise ValueError(f"unknown FittingConfig overrides: {sorted(set(opts or {}) - FIT_FIELDS)}")
    overrides = {}
    for name, value in opts.items():
        if name.endswith("_weight"):
            overrides[name] = bounded(value, 0.0, 100.0, name)
        elif name == "f_scale":
            overrides[name] = bounded(value, 0.001, 100.0, name)
        elif name == "max_nfev":
            overrides[name] = int(bounded(value, 1, 5000, name))
        elif name == "max_variables":
            overrides[name] = int(bounded(value, 1, 1000000, name))
        elif name == "robust_loss":
            if value not in options_get()["robust_loss_values"]: raise ValueError("unsupported robust_loss")
            overrides[name] = value
        elif name == "allow_unbounded_joints":
            if value is not False: raise ValueError("allow_unbounded_joints must remain false")
            overrides[name] = False
        elif name == "active_bones":
            if value is not None and (not isinstance(value, list) or any(x not in known for x in value) or len(value) != len(set(value))):
                raise ValueError("active_bones must be unique exact rig bone names or null")
            overrides[name] = value
    receipt = {"schema": RECEIPT, "state": "candidate_ready_for_bounded_fit_only", "run_id": args.run_id,
               "source_sha256": source_sha, "rig_json_sha256": rig_sha, "profile_sha256": sha(profile_path),
               "bones": normalized, "fitting_config_overrides": overrides, "promotion_allowed": False,
               "category_default_allowed": False,
               "acceptance": "owner approval across all five models and the full 34-clip set is still required"}
    return receipt


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options-get", action="store_true")
    ap.add_argument("--source", type=Path)
    ap.add_argument("--rig-json", type=Path)
    ap.add_argument("--profile", type=Path)
    ap.add_argument("--run-id")
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()
    if args.options_get:
        print(json.dumps(options_get(), indent=2)); return 0
    if any(getattr(args, name) is None for name in ("source", "rig_json", "profile", "run_id", "output")):
        ap.error("--source --rig-json --profile --run-id --output are required unless --options-get")
    receipt = build_receipt(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
