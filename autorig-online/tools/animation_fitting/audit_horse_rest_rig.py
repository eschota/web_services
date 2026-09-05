"""Audit paired horse chains in an immutable actionless bundle before fitting.

This is a morphology gate, not a visual-quality verdict. Chain lengths are
measured in armature-local space so anisotropic object scale cannot change
the result. The explicit ARP joints work with both base and segmented rigs.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import numpy as np
from .rig import load_rig_bundle

CHAINS = {
    "fore": ("c_thigh_b_dupli_001.{side}", "thigh_twist_dupli_001.{side}",
             "leg_stretch_dupli_001.{side}", "foot_dupli_001.{side}", "toes_01_dupli_001.{side}"),
    "hind": ("c_thigh_b.{side}", "thigh_twist.{side}",
             "leg_stretch.{side}", "foot.{side}", "toes_01.{side}"),
}


def audit_skeleton(armature: dict, max_pair_difference=0.20) -> dict:
    if not math.isfinite(max_pair_difference) or not 0 < max_pair_difference < 1:
        raise ValueError("max_pair_difference must be in (0,1)")
    bones = {b["name"]: b for b in armature["bones"]}
    pairs, reasons = {}, []
    for region, templates in CHAINS.items():
        lengths, segments = {}, {}
        for side in ("l", "r"):
            names = [s.format(side=side) for s in templates]
            missing = [s for s in names if s not in bones]
            if missing:
                raise ValueError(f"missing explicit {region} chain bones: {missing}")
            points = [bones[n]["head_local"] for n in names]
            points += [bones[names[-1]]["tail_local"]]
            points = np.asarray(points, dtype=float)
            if points.shape != (6, 3) or not np.isfinite(points).all():
                raise ValueError("chain positions must be finite 3D points")
            seg = np.linalg.norm(np.diff(points, axis=0), axis=1)
            if np.any(seg <= 1e-8):
                raise ValueError(f"degenerate {region}.{side} segment")
            segments[side] = seg.tolist()
            lengths[side] = float(seg.sum())
        relative = abs(lengths["l"] - lengths["r"]) / max(lengths.values())
        segment_relative = [abs(a-b) / max(a,b) for a,b in zip(segments['l'],segments['r'])]
        ok = relative <= max_pair_difference and max(segment_relative) <= max_pair_difference
        pairs[region] = {"lengths": lengths, "segments": segments,
                         "relative_length_difference": relative,
                         "relative_segment_differences": segment_relative, "passed": ok}
        if not ok:
            reasons.append(f"{region}_paired_chain_asymmetry")
    return {"schema": "autorig-horse-rest-rig-audit.v1", "passed": not reasons,
            "max_pair_difference": max_pair_difference, "pairs": pairs,
            "blocking_reasons": reasons, "production_quality_approved": False}


def audit_bundle(path) -> dict:
    rig = load_rig_bundle(path)
    if str(rig.metadata.get('source', {}).get('species', '')).lower() != 'horse':
        raise ValueError('This morphology profile is calibrated only for horses')
    payload = json.loads(rig.artifacts["skeleton"].read_text(encoding="utf-8"))
    if len(payload["armatures"]) != 1:
        raise ValueError("expected exactly one armature")
    report = audit_skeleton(payload["armatures"][0])
    report.update(bundle_sha256=rig.metadata_sha256,
                  immutable_manifest_sha256=rig.immutable_manifest_sha256)
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bundle',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    report=audit_bundle(args.bundle)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as f:
        json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps(report))
    return 0 if report['passed'] else 2


if __name__=='__main__':
    raise SystemExit(main())
