#!/usr/bin/env python3
"""Benchmark small motion-analysis frames and make lossless categorical derivatives.

The tool is deliberately independent of the Motion Transfer service.  It reads an
existing 2x2 synchronized clip plus projection metadata, benchmarks the CPU work
used by tracking, and optionally writes low-resolution mask / triangle-ID PNGs.
Categorical images are resized with nearest-neighbour only; video codecs are never
used for labels.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import time

import cv2
import numpy as np


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as src:
        for block in iter(lambda: src.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _fraction(value: str) -> float:
    n, d = value.split("/", 1)
    return float(n) / float(d) if float(d) else 0.0


def video_metadata(path: pathlib.Path) -> dict:
    cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
           "stream=codec_name,width,height,avg_frame_rate,r_frame_rate,time_base,nb_frames,duration",
           "-of", "json", str(path)]
    doc = json.loads(subprocess.run(cmd, check=True, capture_output=True, text=True).stdout)
    stream = doc["streams"][0]
    return {
        "codec": stream.get("codec_name"),
        "width": int(stream["width"]),
        "height": int(stream["height"]),
        "avg_frame_rate": stream.get("avg_frame_rate"),
        "r_frame_rate": stream.get("r_frame_rate"),
        "time_base": stream.get("time_base"),
        "fps": _fraction(stream["avg_frame_rate"]),
        "declared_frames": int(stream["nb_frames"]) if stream.get("nb_frames", "N/A").isdigit() else None,
        "duration_seconds": float(stream["duration"]) if stream.get("duration") not in (None, "N/A") else None,
    }


def decode_tri_ids(image: np.ndarray) -> np.ndarray:
    if image.ndim != 3 or image.shape[2] != 3:
        raise ValueError("triangle-ID PNG must be 3-channel")
    # cv2 is BGR; the producer encodes RGB bytes as id+1.
    b, g, r = (image[..., i].astype(np.uint32) for i in range(3))
    return (r << 16) | (g << 8) | b


def categorical_values(image: np.ndarray, kind: str) -> np.ndarray:
    if kind == "tri":
        return np.unique(decode_tri_ids(image))
    if image.ndim == 3:
        image = image.reshape(-1, image.shape[2])
        return np.unique(image, axis=0)
    return np.unique(image)


def resize_categorical(src: pathlib.Path, dst: pathlib.Path, width: int, height: int, kind: str) -> dict:
    image = cv2.imread(str(src), cv2.IMREAD_UNCHANGED)
    if image is None:
        raise ValueError(f"cannot decode {src}")
    before = categorical_values(image, kind)
    out = cv2.resize(image, (width, height), interpolation=cv2.INTER_NEAREST)
    after = categorical_values(out, kind)
    if kind == "tri" or before.ndim == 1:
        invented = np.setdiff1d(after, before)
    else:
        before_set = {tuple(x.tolist()) for x in before}
        invented = [x for x in after if tuple(x.tolist()) not in before_set]
    if len(invented):
        raise AssertionError(f"nearest-neighbour resize invented {len(invented)} labels")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(dst), out):
        raise OSError(f"cannot write {dst}")
    source_bytes, output_bytes = src.stat().st_size, dst.stat().st_size
    return {"source": str(src), "source_sha256": sha256(src), "source_bytes": source_bytes,
            "output": str(dst), "output_sha256": sha256(dst), "output_bytes": output_bytes,
            "byte_ratio": float(output_bytes / max(1, source_bytes)), "source_shape": list(image.shape),
            "output_shape": list(out.shape), "source_label_count": int(len(before)),
            "output_label_count": int(len(after)),
            "retained_label_share": float(len(after) / max(1, len(before))),
            "source_nonzero_pixels": int(np.count_nonzero(np.any(image != 0, axis=-1) if image.ndim == 3 else image)),
            "output_nonzero_pixels": int(np.count_nonzero(np.any(out != 0, axis=-1) if out.ndim == 3 else out)),
            "invented_label_count": 0}


def scaled_cameras(doc: dict, source_tile_px: int, target_tile_px: int) -> dict:
    result = json.loads(json.dumps(doc))
    scale = source_tile_px / target_tile_px
    for camera in result.get("views", {}).values():
        if "size" in camera:
            camera["size"] = [target_tile_px, target_tile_px]
        if "px_world" in camera:
            camera["px_world"] = float(camera["px_world"]) * scale
    sheet = result.get("sheet", {})
    sheet["tile_px"] = target_tile_px
    for tile in sheet.get("tiles", {}).values():
        tile["x"] = int(round(float(tile["x"]) / source_tile_px * target_tile_px))
        tile["y"] = int(round(float(tile["y"]) / source_tile_px * target_tile_px))
    return result


def sample_flow(flow: np.ndarray, points: np.ndarray) -> np.ndarray:
    """Bilinear flow vectors at x/y points, clamped inside the valid cell."""
    h, w = flow.shape[:2]
    x = np.clip(points[:, 0], 0, w - 1.001)
    y = np.clip(points[:, 1], 0, h - 1.001)
    x0, y0 = np.floor(x).astype(np.int32), np.floor(y).astype(np.int32)
    fx, fy = (x - x0)[:, None], (y - y0)[:, None]
    return (flow[y0, x0] * (1 - fx) * (1 - fy)
            + flow[y0, x0 + 1] * fx * (1 - fy)
            + flow[y0 + 1, x0] * (1 - fx) * fy
            + flow[y0 + 1, x0 + 1] * fx * fy)


def benchmark_video(video: pathlib.Path, tile_px: int, max_frames: int) -> dict:
    wall_start = time.perf_counter()
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        raise ValueError(f"cannot open {video}")
    dis = [cv2.DISOpticalFlow_create(cv2.DISOPTICAL_FLOW_PRESET_MEDIUM) for _ in range(4)]
    previous = None
    axis = np.linspace(0.1, 0.9, 8, dtype=np.float32) * tile_px
    grid = np.stack(np.meshgrid(axis, axis), -1).reshape(-1, 2)
    points = [grid.copy() for _ in range(4)]
    path_lengths = np.zeros((4, len(grid)), np.float64)
    decoded = tracked = 0
    decode_s = resize_s = gray_s = flow_s = 0.0
    while decoded < max_frames:
        t = time.perf_counter()
        ok, frame = cap.read()
        decode_s += time.perf_counter() - t
        if not ok:
            break
        decoded += 1
        t = time.perf_counter()
        frame = cv2.resize(frame, (tile_px * 2, tile_px * 2), interpolation=cv2.INTER_AREA)
        resize_s += time.perf_counter() - t
        tiles = (frame[:tile_px, :tile_px], frame[:tile_px, tile_px:],
                 frame[tile_px:, :tile_px], frame[tile_px:, tile_px:])
        t = time.perf_counter()
        gray = [cv2.cvtColor(x, cv2.COLOR_BGR2GRAY) for x in tiles]
        gray_s += time.perf_counter() - t
        if previous is not None:
            t = time.perf_counter()
            for i, (engine, old, cur) in enumerate(zip(dis, previous, gray)):
                flow = engine.calc(old, cur, None)
                delta = sample_flow(flow, points[i])
                points[i] = np.clip(points[i] + delta, 0, tile_px - 1)
                path_lengths[i] += np.linalg.norm(delta, axis=1)
            flow_s += time.perf_counter() - t
            tracked += 1
        previous = gray
    cap.release()
    total = decode_s + resize_s + gray_s + flow_s
    return {"tile_px": tile_px, "decoded_frames": decoded, "tracked_transitions": tracked,
            "timings_ms": {"decode": decode_s * 1000, "resize": resize_s * 1000,
                           "grayscale": gray_s * 1000, "dis_flow": flow_s * 1000,
                           "measured_total": total * 1000},
            "milliseconds_per_transition": (total * 1000 / tracked) if tracked else None,
            "wall_total_ms": (time.perf_counter() - wall_start) * 1000,
            "probe_grid": "8x8 normalized points per tile; DIS-integrated; diagnostic, not semantic joints",
            "probe_endpoints_normalized": [(p / tile_px).round(7).tolist() for p in points],
            "probe_path_lengths_normalized": (path_lengths / tile_px).round(7).tolist()}


def add_flow_agreement(benchmarks: list[dict], native_tile_px: int) -> None:
    reference = next((x for x in benchmarks if x["tile_px"] == native_tile_px), None)
    if reference is None:
        return
    ref = np.asarray(reference["probe_endpoints_normalized"], np.float64)
    ref_len = np.asarray(reference["probe_path_lengths_normalized"], np.float64)
    for case in benchmarks:
        endpoints = np.asarray(case["probe_endpoints_normalized"], np.float64)
        lengths = np.asarray(case["probe_path_lengths_normalized"], np.float64)
        drift = np.linalg.norm(endpoints - ref, axis=-1) * native_tile_px
        ratio = lengths / np.maximum(ref_len, 1e-9)
        case["agreement_to_native"] = {
            "native_tile_px": native_tile_px,
            "endpoint_drift_native_px_mean": float(drift.mean()),
            "endpoint_drift_native_px_p95": float(np.percentile(drift, 95)),
            "path_length_ratio_median": float(np.median(ratio)),
            "path_length_ratio_p05": float(np.percentile(ratio, 5)),
            "path_length_ratio_p95": float(np.percentile(ratio, 95)),
        }


def aggregate_benchmarks(samples: list[dict]) -> list[dict]:
    result = []
    for size in sorted({x["tile_px"] for x in samples}):
        cases = [x for x in samples if x["tile_px"] == size]
        representative = json.loads(json.dumps(cases[0]))
        representative["repeats"] = len(cases)
        representative["timings_ms"] = {
            key + "_median": float(np.median([x["timings_ms"][key] for x in cases]))
            for key in cases[0]["timings_ms"]
        }
        representative["milliseconds_per_transition"] = float(np.median(
            [x["milliseconds_per_transition"] for x in cases]))
        representative["wall_total_ms"] = float(np.median([x["wall_total_ms"] for x in cases]))
        representative["repeat_wall_total_ms"] = [x["wall_total_ms"] for x in cases]
        result.append(representative)
    return result


def run(args: argparse.Namespace) -> dict:
    cv2.setNumThreads(2)
    video = pathlib.Path(args.video).resolve()
    proj = pathlib.Path(args.projection_dir).resolve()
    output = pathlib.Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    cameras_path = proj / "cameras.json"
    cameras = json.loads(cameras_path.read_text(encoding="utf-8"))
    source_tile_px = int(cameras["sheet"]["tile_px"])
    sizes = [int(x) for x in args.sizes.split(",")]
    if any(x <= 0 or x > 1024 for x in sizes) or len(set(sizes)) != len(sizes):
        raise ValueError("sizes must be unique integers in 1..1024")
    if not 2 <= args.max_frames <= 1000:
        raise ValueError("max-frames must be in 2..1000")
    if not 1 <= args.repeats <= 5:
        raise ValueError("repeats must be in 1..5")
    metadata = video_metadata(video)
    report = {"schema": "autorig.v3.lowres-motion-benchmark/1", "video": str(video),
              "video_sha256": sha256(video), "video_metadata": metadata,
              "projection_dir": str(proj), "cameras_sha256": sha256(cameras_path),
              "source_tile_px": source_tile_px, "max_frames": args.max_frames,
              "opencv_threads": 2, "warmup_frames": min(args.warmup_frames, args.max_frames),
              "repeats": args.repeats,
              "categorical_policy": "PNG only; INTER_NEAREST; output labels must be a subset of source labels",
              "benchmarks": [], "derivatives": []}
    for size in sizes:
        benchmark_video(video, size, min(args.warmup_frames, args.max_frames))
    samples = []
    for repeat in range(args.repeats):
        order = sizes[repeat % len(sizes):] + sizes[:repeat % len(sizes)]
        for size in order:
            samples.append(benchmark_video(video, size, args.max_frames))
    report["benchmarks"] = aggregate_benchmarks(samples)
    for size in sizes:
        case = output / str(size)
        case.mkdir()
        scaled = scaled_cameras(cameras, source_tile_px, size)
        camera_out = case / "cameras.json"
        camera_out.write_text(json.dumps(scaled, indent=2), encoding="utf-8")
        for source in sorted(proj.glob("*_mask.png")):
            sheet = source.name.startswith("sheet_")
            rows = len(cameras["sheet"]["layout"])
            cols = len(cameras["sheet"]["layout"][0])
            report["derivatives"].append(resize_categorical(
                source, case / source.name, size * cols if sheet else size, size * rows if sheet else size, "mask"))
        for source in sorted(proj.glob("*_tri.png")):
            report["derivatives"].append(resize_categorical(source, case / source.name, size, size, "tri"))
    add_flow_agreement(report["benchmarks"], metadata["width"] // 2)
    report["completed_at_utc"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    report_path = output / "benchmark.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--video", required=True)
    parser.add_argument("--projection-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--sizes", default="256,384,512")
    parser.add_argument("--max-frames", type=int, default=32)
    parser.add_argument("--warmup-frames", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    report = run(args)
    print(json.dumps({"schema": report["schema"], "video_sha256": report["video_sha256"],
                      "sizes": [x["tile_px"] for x in report["benchmarks"]],
                      "repeats": report["repeats"], "derivatives": len(report["derivatives"]),
                      "invented_labels": sum(x["invented_label_count"] for x in report["derivatives"])}, indent=2))


if __name__ == "__main__":
    main()
