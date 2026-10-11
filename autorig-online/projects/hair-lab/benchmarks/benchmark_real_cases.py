#!/usr/bin/env python3
"""Run Hair Lab geometry kernels on real case files from a private manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
import time
from pathlib import Path

import numpy as np
import torch


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gpu_snapshot():
    text = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,driver_version,memory.total,memory.used,memory.free,utilization.gpu", "--format=csv,noheader,nounits"],
        text=True,
    ).strip()
    name, driver, total, used, free, utilization = [part.strip() for part in text.split(",")]
    return {"name": name, "driver": driver, "memory_total_mib": int(total), "memory_used_mib": int(used), "memory_free_mib": int(free), "utilization_percent": int(utilization)}


def percentile(values, q):
    values = sorted(values)
    p = (len(values) - 1) * q
    lo, hi = math.floor(p), math.ceil(p)
    return values[lo] if lo == hi else values[lo] * (hi - p) + values[hi] * (p - lo)


def stats(values):
    return {"n": len(values), "p50_ms": statistics.median(values), "p90_ms": percentile(values, 0.9), "min_ms": min(values), "max_ms": max(values)}


def sync(device):
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def timed(fn, device):
    sync(device)
    start = time.perf_counter_ns()
    value = fn()
    sync(device)
    return (time.perf_counter_ns() - start) / 1e6, value


def actual_hull(masks):
    front, back, left, right, top, bottom = masks
    xy = (front & back).T       # x,y
    yz = left & right           # y,z
    xz = (top & bottom).T       # x,z
    occupied = xy[:, :, None] & yz[None, :, :] & xz[:, None, :]
    interior = occupied.clone()
    interior[1:-1, 1:-1, 1:-1] &= (
        occupied[:-2, 1:-1, 1:-1] & occupied[2:, 1:-1, 1:-1]
        & occupied[1:-1, :-2, 1:-1] & occupied[1:-1, 2:, 1:-1]
        & occupied[1:-1, 1:-1, :-2] & occupied[1:-1, 1:-1, 2:]
    )
    return occupied, occupied & ~interior


def ribbons_from_actual_surface(vertices, normals, guide_count, samples=16):
    if vertices.shape[0] == 0:
        raise ValueError("no authoritative/name-hinted hair vertices")
    selection = torch.linspace(0, vertices.shape[0] - 1, guide_count, device=vertices.device).long()
    roots = vertices[selection]
    directions = torch.nn.functional.normalize(normals[selection], dim=-1)
    t = torch.linspace(0.0, 1.0, samples, device=vertices.device)[None, :, None]
    scale = torch.linalg.vector_norm(vertices.max(dim=0).values - vertices.min(dim=0).values) * 0.08
    guides = roots[:, None, :] + directions[:, None, :] * t * scale
    tangent = torch.empty_like(guides)
    tangent[:, 1:-1] = guides[:, 2:] - guides[:, :-2]
    tangent[:, 0] = guides[:, 1] - guides[:, 0]
    tangent[:, -1] = guides[:, -1] - guides[:, -2]
    tangent = torch.nn.functional.normalize(tangent, dim=-1)
    view = torch.tensor([0.0, 0.0, 1.0], device=vertices.device).expand_as(tangent)
    side = torch.cross(tangent, view, dim=-1)
    fallback = torch.cross(tangent, torch.tensor([0.0, 1.0, 0.0], device=vertices.device).expand_as(tangent), dim=-1)
    side = torch.nn.functional.normalize(torch.where(torch.linalg.vector_norm(side, dim=-1, keepdim=True) < 1e-5, fallback, side), dim=-1)
    width = scale * 0.04
    ribbon_vertices = torch.stack((guides - side * width, guides + side * width), dim=2).reshape(-1, 3)
    base = (torch.arange(guide_count, device=vertices.device)[:, None] * samples * 2 + torch.arange(samples - 1, device=vertices.device)[None, :] * 2)
    triangles = torch.stack((base, base + 2, base + 1, base + 1, base + 2, base + 3), dim=-1).reshape(-1, 3)
    return ribbon_vertices, triangles


def case_path(row):
    rig_local = row.get("rig", {}).get("local", {}).get("path")
    if rig_local:
        return Path(rig_local)
    source_local = row.get("source", {}).get("local", {}).get("path")
    if source_local:
        return Path(source_local)
    for key in ("local_path", "asset_path", "source_path", "path"):
        if row.get(key):
            return Path(row[key])
    raise KeyError("case has no local asset path")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--extract-python", type=Path, required=True)
    parser.add_argument("--blender", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=25)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    cases = manifest.get("cases", manifest) if isinstance(manifest, dict) else manifest
    args.work_dir.mkdir(parents=True, exist_ok=True)
    extractor = Path(__file__).with_name("extract_real_head_views.py")
    blender_extractor = Path(__file__).with_name("extract_rigged_head_blender.py")
    output = {
        "scope": "real V3 current-rig head-weight ROI projection and geometry-kernel benchmark; not hair reconstruction",
        "manifest_sha256": sha256(args.manifest),
        "repeats": args.repeats,
        "runtime": {"torch": torch.__version__, "cuda": torch.version.cuda, "gpu_before": gpu_snapshot()},
        "source_receipts": {"benchmark_sha256": sha256(Path(__file__)), "projector_sha256": sha256(extractor), "blender_extractor_sha256": sha256(blender_extractor)},
        "cases": [],
    }
    for index, row in enumerate(cases):
        case_id = str(row.get("case_id") or row.get("caseId") or f"case-{index+1}")
        source = case_path(row)
        case_dir = args.work_dir / case_id / "benchmark-output"
        case_dir.mkdir(parents=True, exist_ok=True)
        blender_npz = case_dir / "rig_head_roi.npz"
        blender_receipt = case_dir / "rig_head_receipt.json"
        load_pipeline_start = time.perf_counter()
        blender_start = time.perf_counter()
        subprocess.run(
            [str(args.blender), "--background", "--python", str(blender_extractor), "--",
             "--source", str(source), "--output", str(blender_npz), "--receipt", str(blender_receipt)],
            text=True, capture_output=True, check=True,
        )
        blender_wall = time.perf_counter() - blender_start
        projection_start = time.perf_counter()
        completed = subprocess.run(
            [str(args.extract_python), str(extractor), "--source", str(source), "--output-dir", str(case_dir),
             "--resolution", "256", "--preselected-npz", str(blender_npz), "--preselection-receipt", str(blender_receipt),
             "--skip-head-payload"],
            text=True, capture_output=True, check=True,
        )
        projection_wall = time.perf_counter() - projection_start
        extraction_wall = time.perf_counter() - load_pipeline_start
        receipt = json.loads((case_dir / "receipt.json").read_text(encoding="utf-8"))
        bundle = np.load(case_dir / "head_roi.npz")
        masks_cpu = torch.from_numpy(bundle["masks"])
        case_result = {
            "case_id": case_id,
            "task_id": row.get("task_id"),
            "terminal_status": row.get("terminal_status"),
            "vision_hair_text": row.get("classification", {}).get("hair"),
            "source_sha256": receipt["source_sha256"],
            "source_bytes": receipt["source_bytes"],
            "classification": receipt["classification"],
            "selection_method": receipt["selection_method"],
            "roi_vertices": receipt["roi_vertices"], "roi_triangles": receipt["roi_triangles"],
            "hair_hint_triangles": receipt["hair_hint_triangles"],
            "extractor_timing_seconds": receipt["timing_seconds"],
            "extractor_process_wall_seconds": extraction_wall,
            "blender_head_extract_wall_seconds": blender_wall,
            "projection_process_wall_seconds": projection_wall,
            "devices": {},
        }
        hull_outputs = {}
        for device in (torch.device("cpu"), torch.device("cuda:0")):
            masks = masks_cpu.to(device)
            elapsed = []
            cold_ms, hull = timed(lambda: actual_hull(masks), device)
            for _ in range(args.repeats):
                ms, hull = timed(lambda: actual_hull(masks), device)
                elapsed.append(ms)
            counts = torch.stack((hull[0].sum(), hull[1].sum())).cpu()
            case_result["devices"][str(device)] = {"hull_cold_ms": cold_ms, "hull_warm": stats(elapsed), "occupied_voxels": int(counts[0]), "surface_voxels": int(counts[1])}
            hull_outputs[str(device)] = (hull[0].cpu(), hull[1].cpu())
        case_result["hull_cpu_cuda_equal"] = bool(torch.equal(hull_outputs["cpu"][0], hull_outputs["cuda:0"][0]) and torch.equal(hull_outputs["cpu"][1], hull_outputs["cuda:0"][1]))
        # Names are weak evidence. Only benchmark geometry-derived pseudo-guides when a named layer exists.
        hair_faces = bundle["hair_face_hint"]
        if hair_faces.any():
            roi_faces = bundle["faces"]
            hair_vertex_ids = np.unique(roi_faces[hair_faces].reshape(-1))
            guide_count = min(2000, max(100, int(hair_vertex_ids.shape[0])))
            guide_result = {}
            guide_outputs = {}
            for device in (torch.device("cpu"), torch.device("cuda:0")):
                vertices = torch.from_numpy(bundle["vertices"][hair_vertex_ids]).to(device)
                normals = torch.from_numpy(bundle["normals"][hair_vertex_ids]).to(device)
                times = []
                cold_ms, result = timed(lambda: ribbons_from_actual_surface(vertices, normals, guide_count), device)
                for _ in range(args.repeats):
                    ms, result = timed(lambda: ribbons_from_actual_surface(vertices, normals, guide_count), device)
                    times.append(ms)
                guide_result[str(device)] = {"cold_ms": cold_ms, "warm": stats(times), "guides": guide_count, "samples_per_guide": 16, "segments_per_guide": 15, "vertices": result[0].shape[0], "triangles": result[1].shape[0]}
                guide_outputs[str(device)] = (result[0].cpu(), result[1].cpu())
            guide_result["cpu_cuda_vertices_allclose"] = bool(torch.allclose(guide_outputs["cpu"][0], guide_outputs["cuda:0"][0], rtol=1e-5, atol=1e-6))
            guide_result["cpu_cuda_indices_equal"] = bool(torch.equal(guide_outputs["cpu"][1], guide_outputs["cuda:0"][1]))
            guide_result["semantic_limit"] = "mesh/node/material hair name hint only; pseudo-guides follow vertex normals and are not reconstructed strand guides"
            case_result["geometry_derived_pseudo_guides"] = guide_result
        else:
            case_result["geometry_derived_pseudo_guides"] = {"skipped": True, "reason": "no named hair layer; head ROI is not hair evidence"}
        output["cases"].append(case_result)
    output["all_hulls_cpu_cuda_equal"] = all(case["hull_cpu_cuda_equal"] for case in output["cases"])
    output["runtime"]["gpu_after"] = gpu_snapshot()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps({"cases": len(output["cases"]), "output": str(args.output), "all_equal": output["all_hulls_cpu_cuda_equal"]}))


if __name__ == "__main__":
    main()
