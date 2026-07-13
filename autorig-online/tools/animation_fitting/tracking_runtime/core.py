from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Any

import numpy as np

from ..contact_profile import load_contact_profile, validate_contact_profile_bundle
from ..errors import ContractError, DependencyUnavailableError
from ..rig import RigBundle, load_rig_bundle
from .contact_integration import (
    calibrate_bundle_camera_z,
    infer_contact_runtime,
)
from .models import (
    DepthBackend,
    DepthResult,
    MaskBackend,
    MaskResult,
    SeedSet,
    TrackerBackend,
    TrackResult,
    VideoFrames,
)
from .runtime_lock import sha256_file


OBSERVATIONS_SCHEMA = "autorig-fitting-observations.v1"
OUTPUT_MANIFEST_SCHEMA = "autorig-tracking-observation-bundle.v1"


@dataclass(frozen=True)
class ObservationRuntimeConfig:
    min_frame_count: int = 2
    max_frame_count: int = 513
    min_track_count: int = 12
    max_track_count: int = 64
    min_alignment_correlation: float = 0.65
    min_seed_inside_mask_ratio: float = 0.85
    min_visible_ratio: float = 0.35
    min_visible_tracks_per_frame: int = 6
    min_visible_confidence: float = 0.05
    min_median_visible_confidence: float = 0.50
    min_mask_fraction: float = 0.005
    max_mask_fraction: float = 0.80
    max_mask_area_step_ratio: float = 2.75
    max_track_step_diagonal: float = 0.28
    min_visible_track_inside_mask_ratio: float = 0.55
    loop_max_endpoint_diagonal: float = 0.16
    loop: bool = False

    def validate(self) -> None:
        if self.min_frame_count < 2 or self.max_frame_count < self.min_frame_count:
            raise ContractError("Invalid frame-count QA bounds")
        if self.min_track_count < 1 or self.max_track_count < self.min_track_count:
            raise ContractError("Invalid track-count QA bounds")
        if (
            isinstance(self.min_visible_tracks_per_frame, bool)
            or not isinstance(self.min_visible_tracks_per_frame, int)
            or self.min_visible_tracks_per_frame < 1
            or self.min_visible_tracks_per_frame > self.max_track_count
        ):
            raise ContractError(
                "min_visible_tracks_per_frame must be a positive bounded integer"
            )
        for name in (
            "min_alignment_correlation",
            "min_seed_inside_mask_ratio",
            "min_visible_ratio",
            "min_mask_fraction",
            "max_mask_fraction",
            "max_track_step_diagonal",
            "min_visible_track_inside_mask_ratio",
            "loop_max_endpoint_diagonal",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0 or value > 1.0:
                raise ContractError(f"{name} must be inside [0, 1]")
        for name in ("min_visible_confidence", "min_median_visible_confidence"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0 or value > 1.0:
                raise ContractError(f"{name} must be inside (0, 1]")
        if self.min_median_visible_confidence < self.min_visible_confidence:
            raise ContractError(
                "min_median_visible_confidence must be at least min_visible_confidence"
            )
        if self.max_mask_area_step_ratio < 1.0:
            raise ContractError("max_mask_area_step_ratio must be at least 1")


def _write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _load_rgb(path: Path) -> np.ndarray:
    try:
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Pillow is required by the tracking runtime"
        ) from exc
    try:
        with Image.open(path) as image:
            return np.asarray(image.convert("RGB"), dtype=np.uint8)
    except Exception as exc:
        raise ContractError(f"Cannot read image {path}: {exc}") from exc


def _load_mask(path: Path) -> np.ndarray:
    try:
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Pillow is required by the tracking runtime"
        ) from exc
    try:
        with Image.open(path) as image:
            mask = np.asarray(image.convert("L"), dtype=np.uint8) > 0
    except Exception as exc:
        raise ContractError(f"Cannot read mask {path}: {exc}") from exc
    if not np.any(mask):
        raise ContractError(f"Canonical reference mask is empty: {path}")
    return mask


def _artifact_path(rig: RigBundle, key: str) -> Path:
    path = rig.artifacts.get(key)
    if path is None:
        raise ContractError(f"Fitting bundle has no {key} artifact")
    return path


def _safe_track_name(index: int, bone: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", bone.lower()).strip("_") or "anchor"
    return f"semantic_{index:03d}_{slug}"


def select_anchor_seeds(
    bundle: str | Path,
    *,
    max_tracks: int = 64,
    minimum_pixel_separation: float = 2.0,
    priority_anchor_ids: tuple[str, ...] = (),
) -> SeedSet:
    """Select one visible, high-weight surface anchor per deform region.

    Selection uses only the immutable actionless bundle. It never infers a
    skeleton from RGB and therefore preserves an explicit anchor-to-bone map.
    """

    if max_tracks < 1 or minimum_pixel_separation < 0:
        raise ContractError("Invalid seed selection bounds")
    if (
        any(not isinstance(value, str) or not value for value in priority_anchor_ids)
        or len(set(priority_anchor_ids)) != len(priority_anchor_ids)
        or len(priority_anchor_ids) > max_tracks
    ):
        raise ContractError("Priority anchor IDs must be unique and fit max_tracks")
    rig = load_rig_bundle(bundle)
    reference_rgb = _load_rgb(_artifact_path(rig, "rgb"))
    reference_mask = _load_mask(_artifact_path(rig, "mask"))
    if reference_rgb.shape[:2] != reference_mask.shape:
        raise ContractError("Canonical RGB and mask dimensions do not match")
    if (rig.camera.height, rig.camera.width) != reference_mask.shape:
        raise ContractError(
            "Canonical camera dimensions do not match reference artifacts"
        )

    grouped: dict[str, list[tuple[str, np.ndarray, float, int]]] = {}
    visible_by_anchor: dict[str, tuple[str, str, np.ndarray]] = {}
    for anchor_id, anchor in rig.anchors.items():
        xy, depth = rig.camera.project(anchor.rest_world)
        if depth <= 0.0 or not np.all(np.isfinite(xy)):
            continue
        x, y = int(round(float(xy[0]))), int(round(float(xy[1])))
        if x < 0 or x >= rig.camera.width or y < 0 or y >= rig.camera.height:
            continue
        if not reference_mask[y, x]:
            continue
        point = np.asarray(xy, dtype=np.float32)
        grouped.setdefault(anchor.bone, []).append(
            (anchor_id, point, float(anchor.skin_weight), anchor.vertex_id)
        )
        visible_by_anchor[anchor_id] = (anchor.bone, anchor_id, point)
    missing_priority = sorted(set(priority_anchor_ids).difference(visible_by_anchor))
    if missing_priority:
        raise ContractError(
            "Priority contact anchors are not visible in the canonical mask: "
            + ", ".join(missing_priority)
        )
    selected: list[tuple[str, str, np.ndarray]] = [
        visible_by_anchor[anchor_id] for anchor_id in priority_anchor_ids
    ]
    priority_bones = {row[0] for row in selected}
    for bone in rig.bone_order:
        if len(selected) >= max_tracks:
            break
        if bone in priority_bones:
            continue
        candidates = grouped.get(bone)
        if not candidates:
            continue
        candidates.sort(key=lambda item: (-item[2], item[3], item[0]))
        for anchor_id, xy, _, _ in candidates:
            if all(
                float(np.linalg.norm(xy - row[2])) >= minimum_pixel_separation
                for row in selected
            ):
                selected.append((bone, anchor_id, xy))
                break
    if not selected:
        raise ContractError(
            "No visible surface anchors could be projected into the canonical mask"
        )
    points = np.stack([row[2] for row in selected]).astype(np.float32, copy=False)
    track_ids = tuple(
        _safe_track_name(index, row[0]) for index, row in enumerate(selected)
    )
    anchor_ids = tuple(row[1] for row in selected)
    return SeedSet(
        track_ids=track_ids,
        anchor_ids=anchor_ids,
        points_xy=points,
        canonical_mask=reference_mask,
        reference_rgb=reference_rgb,
        bundle_sha256=rig.metadata_sha256,
        immutable_manifest_sha256=rig.immutable_manifest_sha256,
    )


def _resolve_ffprobe(explicit: str | None) -> Path:
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    from_path = shutil.which("ffprobe")
    if from_path:
        candidates.append(Path(from_path))
    candidates.extend(
        (
            Path(r"C:\API\ffmpeg\bin\ffprobe.exe"),
            Path(r"C:\Users\escho\AppData\Local\Freestock\tools\bin\ffprobe.exe"),
        )
    )
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    raise DependencyUnavailableError("ffprobe was not found; pass --ffprobe explicitly")


def _fraction(raw: str, field: str) -> float:
    try:
        numerator, denominator = raw.split("/", 1)
        value = float(numerator) / float(denominator)
    except (AttributeError, ValueError, ZeroDivisionError) as exc:
        raise ContractError(f"ffprobe returned invalid {field}: {raw!r}") from exc
    if not math.isfinite(value) or value <= 0.0:
        raise ContractError(f"ffprobe returned invalid {field}: {raw!r}")
    return value


def load_video(video: str | Path, *, ffprobe: str | None = None) -> VideoFrames:
    source = Path(video).resolve()
    if not source.is_file():
        raise ContractError(f"Video does not exist: {source}")
    probe_exe = _resolve_ffprobe(ffprobe)
    command = [
        str(probe_exe),
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name,width,height,pix_fmt,avg_frame_rate,r_frame_rate,nb_frames,duration",
        "-show_entries",
        "format=duration,size,format_name",
        "-of",
        "json",
        str(source),
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise ContractError(
            f"ffprobe failed: {(completed.stderr or completed.stdout).strip()}"
        )
    try:
        probe = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ContractError(f"ffprobe returned invalid JSON: {exc}") from exc
    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list) or len(streams) != 1:
        raise ContractError("Video must expose one selected video stream")
    stream = streams[0]
    try:
        width, height = int(stream["width"]), int(stream["height"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ContractError("ffprobe did not return valid video dimensions") from exc
    fps = _fraction(
        stream.get("avg_frame_rate") or stream.get("r_frame_rate"), "frame rate"
    )
    nominal_fps = _fraction(
        stream.get("r_frame_rate") or stream.get("avg_frame_rate"), "nominal frame rate"
    )
    if abs(fps - nominal_fps) / max(fps, nominal_fps) > 0.01:
        raise ContractError(
            "Variable-rate input is not accepted by deterministic fitting"
        )
    try:
        import cv2
    except ImportError as exc:
        raise DependencyUnavailableError(
            "opencv-python-headless is required to decode video"
        ) from exc
    capture = cv2.VideoCapture(str(source))
    if not capture.isOpened():
        raise ContractError(f"OpenCV cannot open video: {source}")
    rows: list[np.ndarray] = []
    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                break
            if frame.dtype != np.uint8 or frame.shape != (height, width, 3):
                raise ContractError(
                    f"Decoded frame has {frame.shape}/{frame.dtype}, expected {(height, width, 3)}/uint8"
                )
            rows.append(np.ascontiguousarray(frame))
    finally:
        capture.release()
    if not rows:
        raise ContractError("Video decoder produced no frames")
    if stream.get("nb_frames") not in (None, "N/A") and int(stream["nb_frames"]) != len(
        rows
    ):
        raise ContractError(
            f"Frame count mismatch: ffprobe reports {stream['nb_frames']}, decoder produced {len(rows)}"
        )
    return VideoFrames(
        source=source,
        source_sha256=sha256_file(source),
        frames_bgr=np.stack(rows),
        fps=fps,
        ffprobe={"executable": str(probe_exe), "command": command, "result": probe},
    )


def _pearson(left: np.ndarray, right: np.ndarray) -> float:
    a = np.asarray(left, dtype=np.float64).reshape(-1)
    b = np.asarray(right, dtype=np.float64).reshape(-1)
    a -= np.mean(a)
    b -= np.mean(b)
    denominator = float(np.linalg.norm(a) * np.linalg.norm(b))
    return 0.0 if denominator <= 1e-12 else float(np.dot(a, b) / denominator)


def _align_seeds(
    seeds: SeedSet, frame_bgr: np.ndarray
) -> tuple[SeedSet, dict[str, float]]:
    try:
        import cv2
    except ImportError as exc:
        raise DependencyUnavailableError(
            "opencv-python-headless is required for alignment"
        ) from exc
    height, width = frame_bgr.shape[:2]
    source_height, source_width = seeds.reference_rgb.shape[:2]
    source_aspect, target_aspect = source_width / source_height, width / height
    if abs(source_aspect - target_aspect) / source_aspect > 0.01:
        raise ContractError(
            f"Video aspect ratio {width}x{height} does not match canonical {source_width}x{source_height}"
        )
    reference = cv2.resize(
        seeds.reference_rgb, (width, height), interpolation=cv2.INTER_AREA
    )
    mask = cv2.resize(
        seeds.canonical_mask.astype(np.uint8),
        (width, height),
        interpolation=cv2.INTER_NEAREST,
    ).astype(bool)
    target_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    ys, xs = np.nonzero(mask)
    margin = max(4, int(round(0.04 * max(width, height))))
    x0, x1 = max(0, int(xs.min()) - margin), min(width, int(xs.max()) + margin + 1)
    y0, y1 = max(0, int(ys.min()) - margin), min(height, int(ys.max()) + margin + 1)
    reference_gray = cv2.cvtColor(reference, cv2.COLOR_RGB2GRAY)[y0:y1, x0:x1]
    target_gray = cv2.cvtColor(target_rgb, cv2.COLOR_RGB2GRAY)[y0:y1, x0:x1]
    intensity = max(0.0, _pearson(reference_gray, target_gray))
    reference_edges = cv2.Canny(reference_gray, 40, 120)
    target_edges = cv2.Canny(target_gray, 40, 120)
    edges = max(0.0, _pearson(reference_edges, target_edges))
    correlation = 0.55 * intensity + 0.45 * edges
    scale = np.asarray((width / source_width, height / source_height), dtype=np.float32)
    aligned = replace(
        seeds,
        points_xy=np.asarray(seeds.points_xy * scale, dtype=np.float32),
        canonical_mask=mask,
        reference_rgb=reference,
    )
    return aligned, {
        "combined_correlation": correlation,
        "intensity_correlation": intensity,
        "edge_correlation": edges,
    }


def _validate_results(
    video: VideoFrames,
    seeds: SeedSet,
    tracks: TrackResult,
    masks: MaskResult,
    depth: DepthResult | None,
    config: ObservationRuntimeConfig,
    alignment: dict[str, float],
) -> tuple[dict[str, Any], list[str]]:
    frame_count, height, width = video.frame_count, video.height, video.width
    track_count = len(seeds.track_ids)
    points = np.asarray(tracks.points_xy)
    visible = np.asarray(tracks.visible)
    confidence = np.asarray(tracks.confidence)
    mask_array = np.asarray(masks.masks)
    if points.shape != (frame_count, track_count, 2):
        raise ContractError(f"Tracker returned invalid point shape: {points.shape}")
    if visible.shape != (frame_count, track_count) or visible.dtype != np.bool_:
        raise ContractError(
            f"Tracker returned invalid visibility shape/type: {visible.shape}/{visible.dtype}"
        )
    if confidence.shape != (frame_count, track_count):
        raise ContractError(
            f"Tracker returned invalid confidence shape: {confidence.shape}"
        )
    if not np.all(np.isfinite(points)) or not np.all(np.isfinite(confidence)):
        raise ContractError("Tracker returned non-finite values")
    if np.any(confidence < 0.0) or np.any(confidence > 1.0):
        raise ContractError("Tracker confidence must stay inside [0, 1]")
    if mask_array.shape != (frame_count, height, width):
        raise ContractError(
            f"Segmenter returned invalid mask shape: {mask_array.shape}"
        )
    mask_array = mask_array.astype(bool, copy=False)
    if depth is not None:
        depth_array = np.asarray(depth.relative_depth)
        if depth_array.shape != (frame_count, height, width):
            raise ContractError(
                f"Depth backend returned invalid shape: {depth_array.shape}"
            )
        if not np.all(np.isfinite(depth_array)) or float(np.ptp(depth_array)) <= 1e-8:
            raise ContractError("Relative depth must be finite and non-constant")

    diagonal = math.hypot(width, height)
    visible_counts = np.sum(visible, axis=1)
    visible_ratio = float(np.mean(visible))
    visible_confidence = confidence[visible]
    minimum_visible_confidence = (
        float(np.min(visible_confidence)) if visible_confidence.size else 0.0
    )
    median_visible_confidence = (
        float(np.median(visible_confidence)) if visible_confidence.size else 0.0
    )
    areas = np.mean(mask_array, axis=(1, 2))
    area_steps = np.maximum(
        areas[1:] / np.maximum(areas[:-1], 1e-12),
        areas[:-1] / np.maximum(areas[1:], 1e-12),
    )
    steps = np.linalg.norm(points[1:] - points[:-1], axis=2) / diagonal
    both_visible = visible[1:] & visible[:-1]
    max_visible_step = (
        float(np.max(steps[both_visible])) if np.any(both_visible) else float("inf")
    )
    rounded = np.rint(points).astype(np.int64)
    inside_frame = (
        (rounded[:, :, 0] >= 0)
        & (rounded[:, :, 0] < width)
        & (rounded[:, :, 1] >= 0)
        & (rounded[:, :, 1] < height)
    )
    visible_outside = visible & ~inside_frame
    try:
        from scipy.ndimage import binary_dilation
    except ImportError as exc:
        raise DependencyUnavailableError("SciPy is required for tracking QA") from exc
    dilated = binary_dilation(
        mask_array, iterations=max(2, int(round(diagonal * 0.005)))
    )
    point_inside_mask = np.zeros_like(visible)
    for frame in range(frame_count):
        valid = inside_frame[frame]
        point_inside_mask[frame, valid] = dilated[
            frame,
            rounded[frame, valid, 1],
            rounded[frame, valid, 0],
        ]
    visible_denominator = max(1, int(np.sum(visible)))
    visible_inside_ratio = float(
        np.sum(point_inside_mask & visible) / visible_denominator
    )
    seed_rounded = np.rint(seeds.points_xy).astype(np.int64)
    seed_inside = [
        0 <= x < width and 0 <= y < height and bool(seeds.canonical_mask[y, x])
        for x, y in seed_rounded
    ]
    seed_inside_ratio = float(np.mean(seed_inside))
    endpoint = np.linalg.norm(points[-1] - points[0], axis=1) / diagonal
    endpoint_visible = visible[-1] & visible[0]
    loop_endpoint = (
        float(np.median(endpoint[endpoint_visible]))
        if np.any(endpoint_visible)
        else float("inf")
    )

    metrics = {
        "alignment": alignment,
        "frame_count": frame_count,
        "track_count": track_count,
        "visible_ratio": visible_ratio,
        "visible_tracks_per_frame": {
            "minimum": int(np.min(visible_counts)),
            "median": float(np.median(visible_counts)),
            "maximum": int(np.max(visible_counts)),
        },
        "confidence": {
            "minimum": float(np.min(confidence)),
            "median": float(np.median(confidence)),
            "maximum": float(np.max(confidence)),
        },
        "visible_confidence": {
            "minimum": minimum_visible_confidence,
            "median": median_visible_confidence,
            "maximum": float(np.max(visible_confidence))
            if visible_confidence.size
            else 0.0,
        },
        "mask_fraction": {
            "minimum": float(np.min(areas)),
            "median": float(np.median(areas)),
            "maximum": float(np.max(areas)),
            "maximum_adjacent_ratio": float(np.max(area_steps))
            if len(area_steps)
            else 1.0,
        },
        "seed_inside_mask_ratio": seed_inside_ratio,
        "visible_track_inside_mask_ratio": visible_inside_ratio,
        "visible_track_outside_frame_count": int(np.sum(visible_outside)),
        "max_visible_track_step_diagonal": max_visible_step,
        "loop_endpoint_median_diagonal": loop_endpoint,
        "relative_depth": None
        if depth is None
        else {
            "minimum": float(np.min(depth.relative_depth)),
            "median": float(np.median(depth.relative_depth)),
            "maximum": float(np.max(depth.relative_depth)),
            "metric": False,
        },
    }
    failures = []
    if frame_count < config.min_frame_count or frame_count > config.max_frame_count:
        failures.append("frame_count")
    if track_count < config.min_track_count or track_count > config.max_track_count:
        failures.append("track_count")
    if alignment["combined_correlation"] < config.min_alignment_correlation:
        failures.append("canonical_first_frame_alignment")
    if seed_inside_ratio < config.min_seed_inside_mask_ratio:
        failures.append("canonical_seed_mask_membership")
    if visible_ratio < config.min_visible_ratio:
        failures.append("visible_ratio")
    if int(np.min(visible_counts)) < config.min_visible_tracks_per_frame:
        failures.append("visible_tracks_per_frame")
    if minimum_visible_confidence < config.min_visible_confidence:
        failures.append("visible_confidence_minimum")
    if median_visible_confidence < config.min_median_visible_confidence:
        failures.append("visible_confidence_median")
    if (
        float(np.min(areas)) < config.min_mask_fraction
        or float(np.max(areas)) > config.max_mask_fraction
    ):
        failures.append("mask_area")
    if len(area_steps) and float(np.max(area_steps)) > config.max_mask_area_step_ratio:
        failures.append("mask_temporal_area")
    if np.any(visible_outside):
        failures.append("visible_tracks_outside_frame")
    if max_visible_step > config.max_track_step_diagonal:
        failures.append("track_temporal_jump")
    if visible_inside_ratio < config.min_visible_track_inside_mask_ratio:
        failures.append("track_mask_consistency")
    if config.loop and loop_endpoint > config.loop_max_endpoint_diagonal:
        failures.append("loop_endpoint")
    return metrics, failures


def _contact_sheet(
    path: Path,
    frames_bgr: np.ndarray,
    masks: np.ndarray,
    points: np.ndarray,
    visible: np.ndarray,
    track_ids: tuple[str, ...],
) -> None:
    try:
        import cv2
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise DependencyUnavailableError(
            "OpenCV and Pillow are required for diagnostics"
        ) from exc
    count = min(8, len(frames_bgr))
    indices = np.linspace(0, len(frames_bgr) - 1, count, dtype=int)
    tiles = []
    for frame_index in indices:
        rgb = cv2.cvtColor(frames_bgr[frame_index], cv2.COLOR_BGR2RGB)
        overlay = rgb.copy()
        overlay[masks[frame_index]] = (
            0.55 * overlay[masks[frame_index]] + 0.45 * np.asarray((35, 210, 120))
        ).astype(np.uint8)
        image = Image.fromarray(overlay)
        draw = ImageDraw.Draw(image)
        draw.rectangle((0, 0, 92, 20), fill=(0, 0, 0))
        draw.text((5, 4), f"frame {frame_index}", fill=(255, 255, 255))
        for track_index, track_id in enumerate(track_ids):
            if not visible[frame_index, track_index]:
                continue
            digest = hashlib.sha256(track_id.encode("utf-8")).digest()
            color = (64 + digest[0] // 2, 64 + digest[1] // 2, 64 + digest[2] // 2)
            x, y = (float(value) for value in points[frame_index, track_index])
            draw.ellipse(
                (x - 2.5, y - 2.5, x + 2.5, y + 2.5), fill=color, outline=(0, 0, 0)
            )
        tiles.append(image)
    tile_width, tile_height = tiles[0].size
    columns = 4
    rows = int(math.ceil(len(tiles) / columns))
    sheet = Image.new(
        "RGB", (columns * tile_width, rows * tile_height), color=(20, 20, 20)
    )
    for index, tile in enumerate(tiles):
        sheet.paste(
            tile, ((index % columns) * tile_width, (index // columns) * tile_height)
        )
    sheet.save(path, format="JPEG", quality=90, optimize=True)


def _output_manifest(root: Path, provenance: dict[str, Any]) -> dict[str, Any]:
    files = []
    for path in sorted(
        item
        for item in root.rglob("*")
        if item.is_file() and item.name != "observation_bundle_manifest.json"
    ):
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return {"schema": OUTPUT_MANIFEST_SCHEMA, "files": files, "provenance": provenance}


def run_observation_pipeline(
    *,
    video: str | Path,
    bundle: str | Path,
    output_dir: str | Path,
    tracker: TrackerBackend,
    segmenter: MaskBackend,
    depth_backend: DepthBackend | None = None,
    contact_profile: str | Path | None = None,
    config: ObservationRuntimeConfig | None = None,
    ffprobe: str | None = None,
) -> Path:
    """Create an atomic, optimizer-compatible observation bundle or fail.

    A rejected run never leaves ``observations.json`` at the requested output
    path. The caller must choose a fresh output directory for each candidate.
    """

    cfg = config or ObservationRuntimeConfig()
    cfg.validate()
    destination = Path(output_dir).resolve()
    if destination.exists():
        raise ContractError(
            f"Output path already exists; choose a fresh directory: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_video = load_video(video, ffprobe=ffprobe)
    if (
        source_video.frame_count < cfg.min_frame_count
        or source_video.frame_count > cfg.max_frame_count
    ):
        raise ContractError(
            f"Video has {source_video.frame_count} frames; accepted range is "
            f"[{cfg.min_frame_count}, {cfg.max_frame_count}]"
        )
    rig = load_rig_bundle(bundle)
    profile = (
        load_contact_profile(contact_profile) if contact_profile is not None else None
    )
    if profile is not None:
        if not cfg.loop:
            raise ContractError(
                "Animal contact inference requires a loop observation config"
            )
        if depth_backend is None:
            raise ContractError(
                "Animal contact inference requires the calibrated depth backend"
            )
        validate_contact_profile_bundle(
            profile,
            rig_metadata=rig.metadata,
            anchors=rig.anchors,
        )
    seeds = select_anchor_seeds(
        bundle,
        max_tracks=cfg.max_track_count,
        priority_anchor_ids=() if profile is None else profile.priority_anchor_ids,
    )
    aligned_seeds, alignment = _align_seeds(seeds, source_video.frames_bgr[0])
    if alignment["combined_correlation"] < cfg.min_alignment_correlation:
        raise ContractError(
            "First video frame does not match the canonical actionless render: "
            f"correlation={alignment['combined_correlation']:.4f}, "
            f"required={cfg.min_alignment_correlation:.4f}"
        )
    try:
        track_result = tracker.track(source_video, aligned_seeds)
        mask_result = segmenter.segment(source_video, aligned_seeds.canonical_mask)
        depth_result = (
            depth_backend.infer(source_video) if depth_backend is not None else None
        )
    except (ContractError, DependencyUnavailableError):
        raise
    except Exception as exc:
        raise ContractError(
            f"Observation backend failed closed: {type(exc).__name__}: {exc}"
        ) from exc
    metrics, failures = _validate_results(
        source_video,
        aligned_seeds,
        track_result,
        mask_result,
        depth_result,
        cfg,
        alignment,
    )
    if failures:
        raise ContractError(
            "Observation QA rejected the candidate: "
            + ", ".join(failures)
            + "; metrics="
            + json.dumps(metrics, sort_keys=True)
        )

    points = np.asarray(track_result.points_xy, dtype=np.float32)
    visible = np.asarray(track_result.visible, dtype=bool)
    confidence = np.asarray(track_result.confidence, dtype=np.float32)
    mask_array = np.asarray(mask_result.masks, dtype=bool)
    depth_calibration = None
    if depth_result is not None and "camera_z" in rig.artifacts:
        depth_calibration = calibrate_bundle_camera_z(
            rig,
            np.asarray(depth_result.relative_depth),
            aligned_seeds.canonical_mask,
        )
    if profile is not None and depth_calibration is None:
        raise ContractError(
            "Animal contact inference requires a v2+ immutable bundle with camera_z"
        )
    contact_runtime = (
        None
        if profile is None
        else infer_contact_runtime(
            rig=rig,
            profile=profile,
            camera_z=depth_calibration.camera_z,
            points_xy=points,
            visible=visible,
            confidence=confidence,
            masks=mask_array,
            anchor_ids=aligned_seeds.anchor_ids,
            fps=source_video.fps,
        )
    )

    staging = Path(
        tempfile.mkdtemp(prefix=destination.name + ".tmp-", dir=destination.parent)
    )
    try:
        mask_dir = staging / "masks"
        mask_dir.mkdir()
        try:
            from PIL import Image
        except ImportError as exc:
            raise DependencyUnavailableError(
                "Pillow is required to write observation masks"
            ) from exc
        silhouettes = []
        for frame in range(source_video.frame_count):
            path = mask_dir / f"frame_{frame:06d}.png"
            Image.fromarray(mask_array[frame].astype(np.uint8) * 255, mode="L").save(
                path, optimize=True
            )
            silhouettes.append(
                {"frame": frame, "path": path.relative_to(staging).as_posix()}
            )
        tracks_payload = []
        for track_index, (track_id, anchor_id) in enumerate(
            zip(aligned_seeds.track_ids, aligned_seeds.anchor_ids)
        ):
            rows = []
            for frame in range(source_video.frame_count):
                rows.append(
                    {
                        "frame": frame,
                        "x": float(points[frame, track_index, 0]),
                        "y": float(points[frame, track_index, 1]),
                        "visible": bool(visible[frame, track_index]),
                        "confidence": float(confidence[frame, track_index]),
                    }
                )
            tracks_payload.append(
                {
                    "id": track_id,
                    "anchor_id": anchor_id,
                    "query_frame": 0,
                    "points": rows,
                }
            )
        camera_z_rows = []
        if depth_calibration is not None:
            depth_dir = staging / "camera_z"
            depth_dir.mkdir()
            for frame, camera_z_frame in enumerate(depth_calibration.camera_z):
                depth_path = depth_dir / f"frame_{frame:06d}.npy"
                np.save(depth_path, np.asarray(camera_z_frame, dtype=np.float32))
                camera_z_rows.append(
                    {
                        "frame": frame,
                        "path": depth_path.relative_to(staging).as_posix(),
                        "mode": "camera_z",
                    }
                )
        provenance = {
            "runtime": "autorig-official-animal-tracking.v1",
            "source_video": str(source_video.source),
            "source_video_sha256": source_video.source_sha256,
            "bundle": str(Path(bundle).resolve()),
            "bundle_sha256": aligned_seeds.bundle_sha256,
            "immutable_manifest_sha256": aligned_seeds.immutable_manifest_sha256,
            "alignment": alignment,
            "tracker": track_result.provenance,
            "segmenter": mask_result.provenance,
            "depth": None if depth_result is None else depth_result.provenance,
            "relative_depth_contract": (
                None
                if depth_result is None
                else (
                    "relative_unscaled_diagnostics_only_not_camera_z"
                    if depth_calibration is None
                    else "calibrated_to_camera_z_from_immutable_actionless_reference"
                )
            ),
            "camera_z_calibration": (
                None if depth_calibration is None else depth_calibration.provenance
            ),
            "contacts": None if contact_runtime is None else contact_runtime.provenance,
        }
        observations = {
            "schema": OBSERVATIONS_SCHEMA,
            "frame_count": source_video.frame_count,
            "width": source_video.width,
            "height": source_video.height,
            "fps": source_video.fps,
            "tracks": tracks_payload,
            "silhouettes": silhouettes,
            "depth": camera_z_rows,
            "contacts": []
            if contact_runtime is None
            else list(contact_runtime.contacts),
            "provenance": provenance,
        }
        _write_json(staging / "observations.json", observations)
        npz_payload: dict[str, Any] = {
            "tracks_xy": points,
            "visible": visible,
            "confidence": confidence,
            "masks": mask_array,
            "track_ids": np.asarray(aligned_seeds.track_ids),
            "anchor_ids": np.asarray(aligned_seeds.anchor_ids),
            "fps": np.asarray(source_video.fps, dtype=np.float64),
        }
        if depth_result is not None:
            npz_payload["relative_depth"] = np.asarray(
                depth_result.relative_depth, dtype=np.float16
            )
        if depth_calibration is not None:
            npz_payload["camera_z"] = np.asarray(
                depth_calibration.camera_z, dtype=np.float32
            )
        if contact_runtime is not None:
            npz_payload["virtual_ground_increments"] = np.asarray(
                contact_runtime.virtual_ground_increments,
                dtype=np.float64,
            )
            npz_payload["virtual_ground_root_path"] = np.asarray(
                contact_runtime.virtual_ground_root_path,
                dtype=np.float64,
            )
        np.savez_compressed(staging / "observations.npz", **npz_payload)
        diagnostics = {
            "schema": "autorig-tracking-diagnostics.v1",
            "decision": "accepted_observations",
            "animation_quality_approved": False,
            "qa": metrics,
            "contact_qa": None if contact_runtime is None else contact_runtime.qa,
            "thresholds": asdict(cfg),
            "provenance": provenance,
        }
        _write_json(staging / "diagnostics.json", diagnostics)
        _contact_sheet(
            staging / "contact_sheet.jpg",
            source_video.frames_bgr,
            mask_array,
            points,
            visible,
            aligned_seeds.track_ids,
        )
        _write_json(staging / "ffprobe.json", source_video.ffprobe)
        _write_json(
            staging / "observation_bundle_manifest.json",
            _output_manifest(staging, provenance),
        )
        os.replace(staging, destination)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return destination / "observations.json"
