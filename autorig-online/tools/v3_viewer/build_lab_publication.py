#!/usr/bin/env python3
"""Build reviewable V3 viewer directories from the bounded F5 evidence summary.

This does not deploy. It accepts only the known lab evidence schema, verifies
every payload hash/provenance/count, and writes a fresh publication tree that
the web API can validate again before serving.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_hash(value: object) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def stage(name: str, status: str, receipt: str | None, duration_ms: int | None, counts=None, message="") -> dict:
    return {
        "name": name,
        "status": status,
        "receipt_sha256": receipt,
        "duration_ms": duration_ms,
        "counts": counts or {},
        "message": message,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    summary_bytes = args.summary.read_bytes()
    summary = json.loads(summary_bytes)
    if summary.get("schema") != "autorig.v3.lab-evidence/1":
        raise ValueError("unsupported evidence summary")
    summary_sha = sha256_bytes(summary_bytes)
    if args.output_root.exists():
        raise ValueError("output root must be fresh")
    args.output_root.mkdir(parents=True)

    receipt = {"schema": "autorig.v3.publication-receipt/1", "evidence_summary_sha256": summary_sha, "tasks": []}
    for case in summary.get("cases") or []:
        label = str(case["label"])
        task_id = str(case["task_id"])
        source_sha = str(case["source_sha256"])
        case_root = args.results_root / label
        viewer_root = case_root / "viewer"
        core_path = case_root / "core-g64" / "report.json"
        graph_path = case_root / "graph-g64" / "graph_result.json"
        core = read_json(core_path)
        graph = read_json(graph_path)
        if sha256_bytes(core_path.read_bytes()) != case["core_receipt_sha256"]:
            raise ValueError(f"core receipt mismatch: {label}")
        if sha256_bytes(graph_path.read_bytes()) != case["graph_receipt_sha256"]:
            raise ValueError(f"graph receipt mismatch: {label}")

        out = args.output_root / task_id
        out.mkdir()
        artifacts = []
        stage_files = (("c1_surface", "surface", "surface_voxels"), ("c2_solid", "solid", "solid_voxels"), ("c3_thin", "thin", "thin_voxels"))
        for stage_name, summary_name, public_name in stage_files:
            src = viewer_root / f"{summary_name}.json"
            payload_bytes = src.read_bytes()
            payload = json.loads(payload_bytes)
            evidence = case["voxel_layers"][summary_name]
            if sha256_bytes(payload_bytes) != evidence["sha256"]:
                raise ValueError(f"payload hash mismatch: {label}/{summary_name}")
            if payload.get("task_id") != task_id or payload.get("source_sha256") != source_sha:
                raise ValueError(f"payload provenance mismatch: {label}/{summary_name}")
            if payload.get("stage") != stage_name or payload.get("stage_receipt_sha256") != case["core_receipt_sha256"]:
                raise ValueError(f"payload stage mismatch: {label}/{summary_name}")
            if payload.get("point_count") != evidence["count"] or len(payload.get("positions") or []) != evidence["count"] * 3:
                raise ValueError(f"payload point count mismatch: {label}/{summary_name}")
            filename = f"{stage_name}.json"
            shutil.copyfile(src, out / filename)
            artifacts.append({
                "name": public_name, "type": "voxel_points", "stage": stage_name,
                "file": filename, "sha256": evidence["sha256"], "bytes": len(payload_bytes),
                "points": evidence["count"], "vertices": 0,
            })

        if case.get("graph_success"):
            src = viewer_root / "skeleton.json"
            payload_bytes = src.read_bytes()
            payload = json.loads(payload_bytes)
            if sha256_bytes(payload_bytes) != case["skeleton_overlay_sha256"]:
                raise ValueError(f"skeleton hash mismatch: {label}")
            if payload.get("task_id") != task_id or payload.get("source_sha256") != source_sha:
                raise ValueError(f"skeleton provenance mismatch: {label}")
            if payload.get("stage") != "c4_graph" or payload.get("stage_receipt_sha256") != case["graph_receipt_sha256"]:
                raise ValueError(f"skeleton stage mismatch: {label}")
            if payload.get("joint_count") != len(payload.get("joints") or []) or payload.get("edge_count") != len(payload.get("edges") or []):
                raise ValueError(f"skeleton count mismatch: {label}")
            shutil.copyfile(src, out / "c4_graph.json")
            artifacts.append({
                "name": "c4_skeleton_graph", "type": "skeleton_graph", "stage": "c4_graph",
                "file": "c4_graph.json", "sha256": case["skeleton_overlay_sha256"], "bytes": len(payload_bytes),
                "points": payload["joint_count"], "vertices": 0,
            })

        core_stages = core["stages"]
        stages = [
            stage("source", "complete", case["coordinate_proof_sha256"], None, {"vertices": case["vertices"], "triangles": case["triangles"]}),
            stage("c1_surface", "complete", case["core_receipt_sha256"], round(core_stages["surface"]["timings_ms"]["wall_total_inclusive"]), {"voxels": case["surface_voxels"]}),
            stage("c2_solid", "complete", case["core_receipt_sha256"], round(core_stages["solid"]["timings_ms"]["wall_total_inclusive"]), {"voxels": case["solid_voxels"]}),
            stage("c3_thin", "complete", case["core_receipt_sha256"], round(core_stages["thinning"]["timings_ms"]["wall_total_inclusive"]), {"voxels": case["thin_voxels"]}),
        ]
        graph_ms = round((graph.get("timings_ms") or {}).get("wall_total_inclusive", 0))
        if case.get("graph_success"):
            stages.append(stage("c4_graph", "complete", case["graph_receipt_sha256"], graph_ms, {
                "joints": next(item["points"] for item in artifacts if item["type"] == "skeleton_graph")
            }, "Geometry graph; not anatomical bones"))
        else:
            stages.append(stage("c4_graph", "failed", case["graph_receipt_sha256"], graph_ms, {}, str(case.get("graph_status") or "failed")))
        stages.extend(stage(name, "unavailable", None, None, {}, "Not produced by this bounded run") for name in (
            "r1_bones", "s1_owner", "s2_weights", "s3_skin", "final_deformation"
        ))

        manifest_input = {
            "task_id": task_id,
            "source_sha256": source_sha,
            "mesh_sha256": case["mesh_sha256"],
            "core_receipt_sha256": case["core_receipt_sha256"],
            "graph_receipt_sha256": case["graph_receipt_sha256"],
            "coordinate_proof_sha256": case["coordinate_proof_sha256"],
            "evidence_summary_sha256": summary_sha,
        }
        manifest = {
            "schema": "autorig.v3.viewer-manifest/1",
            "task_id": task_id,
            "build": {
                "version": f"animal-gpu-lab-{summary['release_commit'][:12]}",
                "source_sha256": source_sha,
                "manifest_input_sha256": canonical_hash(manifest_input),
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            "model": {
                "input_type": "animal", "coordinate_space": "model_local_gltf",
                "vertices": case["vertices"], "triangles": case["triangles"],
            },
            "stages": stages,
            "artifacts": artifacts,
        }
        manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        (out / "manifest.json").write_bytes(manifest_bytes)
        receipt["tasks"].append({"task_id": task_id, "manifest_sha256": sha256_bytes(manifest_bytes), "artifacts": len(artifacts)})

    receipt_bytes = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    (args.output_root / "publication-receipt.json").write_bytes(receipt_bytes)
    print(json.dumps({"output_root": str(args.output_root), "receipt_sha256": sha256_bytes(receipt_bytes), "tasks": len(receipt["tasks"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
