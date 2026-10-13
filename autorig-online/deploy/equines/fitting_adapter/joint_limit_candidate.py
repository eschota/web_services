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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, type=Path)
    ap.add_argument("--rig-json", required=True, type=Path)
    ap.add_argument("--profile", required=True, type=Path)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--output", required=True, type=Path)
    args = ap.parse_args()
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
    opts = profile.get("regularization") or {}
    exact = {"joint_limit_weight", "temporal_weight", "jerk_weight", "max_nfev"}
    if set(opts) != exact:
        raise ValueError(f"regularization options must be exactly {sorted(exact)}")
    regularization = {
        "joint_limit_weight": bounded(opts["joint_limit_weight"], 0.0, 100.0, "joint_limit_weight"),
        "temporal_weight": bounded(opts["temporal_weight"], 0.0, 100.0, "temporal_weight"),
        "jerk_weight": bounded(opts["jerk_weight"], 0.0, 100.0, "jerk_weight"),
        "max_nfev": int(bounded(opts["max_nfev"], 1, 5000, "max_nfev")),
    }
    receipt = {"schema": RECEIPT, "state": "candidate_ready_for_bounded_fit_only", "run_id": args.run_id,
               "source_sha256": source_sha, "rig_json_sha256": rig_sha, "profile_sha256": sha(profile_path),
               "bones": normalized, "regularization": regularization, "promotion_allowed": False,
               "category_default_allowed": False,
               "acceptance": "owner approval across all five models and the full 34-clip set is still required"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
