#!/usr/bin/env python3
"""Bounded synthetic benchmark for Hair Lab geometry kernels.

This measures only data-parallel building blocks: a six-view orthographic
visual hull and deterministic guide-to-ribbon extrusion.  It is not a hair
reconstruction or a quality result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import time
from pathlib import Path

import torch


SEED = 20261012


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def summary_ms(values: list[float]) -> dict[str, float | int]:
    return {
        "n": len(values),
        "min_ms": min(values),
        "median_ms": statistics.median(values),
        "p90_ms": percentile(values, 0.9),
        "max_ms": max(values),
        "mean_ms": statistics.fmean(values),
    }


def sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def timed(fn, device: torch.device) -> tuple[float, object]:
    sync(device)
    start = time.perf_counter_ns()
    result = fn()
    sync(device)
    return (time.perf_counter_ns() - start) / 1_000_000.0, result


def make_six_masks(resolution: int, device: torch.device) -> torch.Tensor:
    # Synthetic asymmetric head + hair envelope in three orthographic axes.
    axis = torch.linspace(-1.0, 1.0, resolution, device=device)
    u, v = torch.meshgrid(axis, axis, indexing="ij")
    front = ((u / 0.78) ** 2 + ((v + 0.04) / 0.92) ** 2) <= 1.0
    side = (((u - 0.04) / 0.68) ** 2 + ((v + 0.04) / 0.92) ** 2) <= 1.0
    top = ((u / 0.78) ** 2 + ((v - 0.04) / 0.68) ** 2) <= 1.0
    # Six distinct buffers mimic front/back/left/right/top/bottom analyzer masks.
    return torch.stack((front, front.flip(0), side, side.flip(0), top, top.flip(1)))


def visual_hull(masks: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    # Index convention: volume[x, y, z]. Pair opposing masks conservatively.
    xy = masks[0] & masks[1].flip(0)
    yz = masks[2] & masks[3].flip(0)
    xz = masks[4] & masks[5].flip(1)
    occupied = xy[:, :, None] & yz[None, :, :] & xz[:, None, :]
    # One-voxel surface band is a useful downstream field seed.
    interior = occupied.clone()
    interior[1:-1, 1:-1, 1:-1] &= (
        occupied[:-2, 1:-1, 1:-1]
        & occupied[2:, 1:-1, 1:-1]
        & occupied[1:-1, :-2, 1:-1]
        & occupied[1:-1, 2:, 1:-1]
        & occupied[1:-1, 1:-1, :-2]
        & occupied[1:-1, 1:-1, 2:]
    )
    surface = occupied & ~interior
    return occupied, surface


def make_guides(count: int, segments: int, device: torch.device) -> torch.Tensor:
    idx = torch.arange(count, device=device, dtype=torch.float32)
    phi = idx * (math.pi * (3.0 - math.sqrt(5.0)))
    z = 1.0 - 2.0 * (idx + 0.5) / count
    radius = torch.sqrt(torch.clamp(1.0 - z * z, min=0.0))
    roots = torch.stack((radius * torch.cos(phi), z, radius * torch.sin(phi)), dim=-1)
    t = torch.linspace(0.0, 1.0, segments, device=device)[None, :, None]
    normal = torch.nn.functional.normalize(roots, dim=-1)[:, None, :]
    gravity = torch.tensor([0.0, -1.0, 0.0], device=device)[None, None, :]
    swirl = torch.stack((-roots[:, 2], torch.zeros_like(z), roots[:, 0]), dim=-1)[:, None, :]
    return roots[:, None, :] + normal * (0.22 * t) + gravity * (0.32 * t * t) + swirl * (0.06 * torch.sin(t * math.pi))


def guides_to_ribbons(guides: torch.Tensor, width: float = 0.012) -> tuple[torch.Tensor, torch.Tensor]:
    tangent = torch.empty_like(guides)
    tangent[:, 1:-1] = guides[:, 2:] - guides[:, :-2]
    tangent[:, 0] = guides[:, 1] - guides[:, 0]
    tangent[:, -1] = guides[:, -1] - guides[:, -2]
    tangent = torch.nn.functional.normalize(tangent, dim=-1)
    view = torch.tensor([0.0, 0.0, 1.0], device=guides.device).expand_as(tangent)
    side = torch.cross(tangent, view, dim=-1)
    degenerate = torch.linalg.vector_norm(side, dim=-1, keepdim=True) < 1e-5
    fallback = torch.cross(tangent, torch.tensor([0.0, 1.0, 0.0], device=guides.device).expand_as(tangent), dim=-1)
    side = torch.nn.functional.normalize(torch.where(degenerate, fallback, side), dim=-1)
    vertices = torch.stack((guides - side * width, guides + side * width), dim=2).reshape(-1, 3)
    c, s, _ = guides.shape
    base = (torch.arange(c, device=guides.device)[:, None] * s * 2 + torch.arange(s - 1, device=guides.device)[None, :] * 2)
    triangles = torch.stack((base, base + 2, base + 1, base + 1, base + 2, base + 3), dim=-1).reshape(-1, 3)
    return vertices, triangles


def bench_hull(device: torch.device, resolution: int, repeats: int) -> dict:
    masks = make_six_masks(resolution, torch.device("cpu"))
    transfer_ms = 0.0
    if device.type == "cuda":
        transfer_ms, masks = timed(lambda: masks.to(device), device)
    cold_ms, output = timed(lambda: visual_hull(masks), device)
    times = []
    for _ in range(repeats):
        elapsed, output = timed(lambda: visual_hull(masks), device)
        times.append(elapsed)
    occupied, surface = output
    scalar_ms, counts = timed(
        lambda: torch.stack((occupied.sum(), surface.sum())).to("cpu"), device
    )
    return {
        "resolution": resolution,
        "voxels": resolution ** 3,
        "mask_bytes": masks.numel() * masks.element_size(),
        "volume_bytes": (occupied.numel() * occupied.element_size()) * 2,
        "h2d_ms": transfer_ms,
        "cold_ms": cold_ms,
        "warm": summary_ms(times),
        "d2h_scalar_ms": scalar_ms,
        "occupied_voxels": int(counts[0]),
        "surface_voxels": int(counts[1]),
    }


def bench_ribbons(device: torch.device, guide_count: int, samples: int, repeats: int) -> dict:
    cold_ms, output = timed(lambda: guides_to_ribbons(make_guides(guide_count, samples, device)), device)
    times = []
    for _ in range(repeats):
        elapsed, output = timed(lambda: guides_to_ribbons(make_guides(guide_count, samples, device)), device)
        times.append(elapsed)
    vertices, triangles = output
    scalar_ms, checksum = timed(lambda: (vertices.sum() + triangles.sum()).to("cpu"), device)
    return {
        "guides": guide_count,
        "samples_per_guide": samples,
        "segments_per_guide": samples - 1,
        "vertices": vertices.shape[0],
        "triangles": triangles.shape[0],
        "output_bytes": vertices.numel() * vertices.element_size() + triangles.numel() * triangles.element_size(),
        "cold_ms": cold_ms,
        "warm": summary_ms(times),
        "d2h_scalar_ms": scalar_ms,
        "checksum": float(checksum),
    }


def nvidia_snapshot() -> dict:
    query = "name,driver_version,memory.total,memory.used,memory.free,utilization.gpu"
    text = subprocess.check_output(
        ["nvidia-smi", f"--query-gpu={query}", "--format=csv,noheader,nounits"],
        text=True,
    ).strip()
    name, driver, total, used, free, util = [part.strip() for part in text.split(",")]
    return {
        "name": name,
        "driver": driver,
        "memory_total_mib": int(total),
        "memory_used_mib": int(used),
        "memory_free_mib": int(free),
        "utilization_percent": int(util),
    }


def git_snapshot() -> dict:
    repo = Path(__file__).resolve().parents[4]
    def run(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()
    return {"repo": str(repo), "branch": run("branch", "--show-current"), "commit": run("rev-parse", "HEAD")}


def validate_cpu_cuda() -> dict:
    if not torch.cuda.is_available():
        return {"performed": False, "reason": "CUDA unavailable"}
    cpu = torch.device("cpu")
    cuda = torch.device("cuda:0")
    masks_cpu = make_six_masks(96, cpu)
    cpu_hull, cpu_surface = visual_hull(masks_cpu)
    cuda_hull, cuda_surface = visual_hull(masks_cpu.to(cuda))
    guides_cpu = make_guides(500, 16, cpu)
    cpu_vertices, cpu_triangles = guides_to_ribbons(guides_cpu)
    cuda_vertices, cuda_triangles = guides_to_ribbons(guides_cpu.to(cuda))
    cuda_vertices = cuda_vertices.cpu()
    cuda_triangles = cuda_triangles.cpu()
    result = {
        "performed": True,
        "visual_hull_equal": bool(torch.equal(cpu_hull, cuda_hull.cpu())),
        "surface_equal": bool(torch.equal(cpu_surface, cuda_surface.cpu())),
        "ribbon_vertices_allclose": bool(torch.allclose(cpu_vertices, cuda_vertices, rtol=1e-5, atol=1e-6)),
        "ribbon_vertex_max_abs_error": float((cpu_vertices - cuda_vertices).abs().max()),
        "triangle_indices_equal": bool(torch.equal(cpu_triangles, cuda_triangles)),
        "vertices_finite": bool(torch.isfinite(cpu_vertices).all() and torch.isfinite(cuda_vertices).all()),
        "triangle_indices_in_range": bool(
            cpu_triangles.min() >= 0
            and cpu_triangles.max() < cpu_vertices.shape[0]
            and cuda_triangles.min() >= 0
            and cuda_triangles.max() < cuda_vertices.shape[0]
        ),
    }
    result["passed"] = all(value for key, value in result.items() if key not in {"performed", "ribbon_vertex_max_abs_error"})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=25)
    args = parser.parse_args()
    torch.manual_seed(SEED)
    start_gpu = nvidia_snapshot()
    result = {
        "scope": "synthetic geometry kernel benchmark; not end-to-end hair reconstruction",
        "seed": SEED,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_model": os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor() or "unknown",
            "logical_cpu_count": os.cpu_count(),
            "torch_num_threads": torch.get_num_threads(),
            "pid": os.getpid(),
        },
        "torch": {"version": torch.__version__, "cuda_version": torch.version.cuda, "cuda_available": torch.cuda.is_available()},
        "git": git_snapshot(),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "gpu_before": start_gpu,
        "repeats": args.repeats,
        "devices": {},
    }
    devices = [torch.device("cpu")]
    if torch.cuda.is_available():
        devices.append(torch.device("cuda:0"))
    for device in devices:
        key = str(device)
        result["devices"][key] = {
            "visual_hull": [bench_hull(device, r, args.repeats) for r in (128, 192, 256)],
            "guide_ribbons": [bench_ribbons(device, n, 16, args.repeats) for n in (100, 500, 2000)],
        }
    if torch.cuda.is_available():
        result["cuda_peak_allocated_bytes"] = torch.cuda.max_memory_allocated()
        result["cuda_peak_reserved_bytes"] = torch.cuda.max_memory_reserved()
    result["cross_device_validation"] = validate_cpu_cuda()
    result["gpu_after"] = nvidia_snapshot()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
