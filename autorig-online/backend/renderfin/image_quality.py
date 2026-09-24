"""Fail-closed quality checks for Renderfin T-pose image bundles.

The Flux/Comfy stage produces two task-owned images: the normal render and an
RGBA foreground cut-out.  Merely being a decodable image is not sufficient:
Comfy can expose the uploaded control pose when its actual output nodes never
ran.  This module keeps that failure out of Hunyuan by validating the bundle
before it can be consumed by the 3D stage.

The validator is intentionally independent of the queue/database layer.  Its
report contains only JSON scalar/container types so callers can persist it in
task metadata and archive it with the rejected bytes.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from PIL import Image, ImageChops, UnidentifiedImageError


REPORT_SCHEMA = "renderfin.tpose_bundle_quality.v1"
ARCHIVE_SCHEMA = "renderfin.rejected_tpose_bundle.v1"

DEFAULT_MIN_DIMENSION = 256
DEFAULT_MAX_IMAGE_PIXELS = 64 * 1024 * 1024
DEFAULT_MAX_ENCODED_BYTES = 128 * 1024 * 1024
DEFAULT_ALPHA_FOREGROUND_THRESHOLD = 8
DEFAULT_ALPHA_OCCUPANCY_MIN = 0.02
DEFAULT_ALPHA_OCCUPANCY_MAX = 0.90
DEFAULT_ECHO_MAE_LEVELS = 2.0
DEFAULT_ECHO_WITHIN_LEVELS = 5
DEFAULT_ECHO_WITHIN_FRACTION = 0.995

# Qwen-Image-Edit re-pose (regen). There is no control mask to compare with -
# the input is the task's own still - so the cut-out itself is read instead.
QWEN_EDIT_REPORT_SCHEMA = "renderfin.qwen_edit_bundle_quality.v1"
# RMBG leaves a soft fringe; only a clearly opaque pixel counts as the subject.
QWEN_EDIT_SOLID_ALPHA_LEVEL = 127
# Solid pixels in the outermost two rows/columns that mean the frame cut the
# figure. A clipped hand or foot reconstructs as a truncated stump, and a
# single matting speck must not fail an otherwise clean render.
QWEN_EDIT_EDGE_BAND_PIXELS = 2
QWEN_EDIT_EDGE_CLIP_MIN_PIXELS = 12
# Trimmed from each end of the subject's mass before measuring its extent, so a
# stray speck cannot stretch the box; fingertips lose only a few pixels.
QWEN_EDIT_EXTENT_TRIM_FRACTION = 0.002
# Arms out: a T-pose spans about its own height, arms down about half of it.
QWEN_EDIT_MIN_SPAN_TO_HEIGHT = 0.7
# Every verdict on the CONTENT of a well-formed render carries this prefix, and
# nothing else does: callers use it to tell a model miss from a broken box.
CONTENT_REJECTION_PREFIX = "qwen_edit_"


class RenderArtifactQualityError(ValueError):
    """A stable, machine-readable rejection from an artifact quality gate."""

    def __init__(
        self,
        machine_code: str,
        message: str,
        report: Mapping[str, Any],
    ) -> None:
        self.machine_code = str(machine_code)
        # ``code`` is a convenient compatibility alias for queue/error code
        # handling while ``machine_code`` is the explicit public contract.
        self.code = self.machine_code
        self.report: Dict[str, Any] = _json_safe_copy(report)
        super().__init__(f"{self.machine_code}: {message}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "machine_code": self.machine_code,
            "message": str(self),
            "report": _json_safe_copy(self.report),
        }


class RenderContentRejected(RenderArtifactQualityError):
    """A well-formed render whose content cannot be used (no clean T-pose).

    Unlike a broken bundle this says nothing about the renderer: the next seed
    on the same box may well succeed, so the box must not be quarantined. It is
    a verdict on this output, so the retry it causes spends an attempt.
    """


def is_content_rejection(text: str) -> bool:
    """Whether an error text carries a content verdict rather than a box fault."""
    return CONTENT_REJECTION_PREFIX in (text or "").lower()


def _json_safe_copy(value: Any) -> Any:
    """Round-trip a value through strict JSON and reject NaN/opaque objects."""

    return json.loads(
        json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    )


def _coerce_bytes(value: Any, role: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(f"{role} must be bytes-like")
    return bytes(value)


def _reject(
    report: Dict[str, Any],
    machine_code: str,
    message: str,
    **details: Any,
) -> None:
    report["passed"] = False
    failure: Dict[str, Any] = {
        "machine_code": machine_code,
        "message": message,
    }
    if details:
        failure["details"] = details
    report["failure"] = failure
    raise RenderArtifactQualityError(machine_code, message, report)


def _decode_image(
    payload: bytes,
    *,
    role: str,
    report: Dict[str, Any],
    min_dimension: int,
    max_image_pixels: int,
    max_encoded_bytes: int,
) -> Tuple[Image.Image, Dict[str, Any]]:
    if not payload:
        _reject(report, f"{role}_missing", f"{role} image bytes are empty")
    if len(payload) > max_encoded_bytes:
        _reject(
            report,
            f"{role}_encoded_size_exceeded",
            f"{role} image exceeds the encoded-size limit",
            encoded_bytes=len(payload),
            maximum=max_encoded_bytes,
        )

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(payload)) as source:
                source.seek(0)
                width, height = source.size
                image_format = str(source.format or "unknown")
                mode = str(source.mode or "unknown")
                if width <= 0 or height <= 0:
                    _reject(
                        report,
                        f"{role}_dimensions_invalid",
                        f"{role} image has invalid dimensions",
                        width=width,
                        height=height,
                    )
                pixels = int(width) * int(height)
                if pixels > max_image_pixels:
                    _reject(
                        report,
                        f"{role}_pixel_count_exceeded",
                        f"{role} image exceeds the decoded-pixel limit",
                        pixels=pixels,
                        maximum=max_image_pixels,
                    )
                source.load()
                decoded = source.copy()
    except RenderArtifactQualityError:
        raise
    except (
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
        UnidentifiedImageError,
        OSError,
        SyntaxError,
        ValueError,
    ) as exc:
        _reject(
            report,
            f"{role}_decode_failed",
            f"{role} image could not be decoded",
            error_type=type(exc).__name__,
        )

    metrics: Dict[str, Any] = {
        "encoded_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "format": image_format,
        "mode": mode,
        "width": int(width),
        "height": int(height),
        "pixels": int(pixels),
    }
    report[role] = metrics
    if width < min_dimension or height < min_dimension:
        _reject(
            report,
            f"{role}_dimensions_too_small",
            f"{role} image is smaller than the minimum dimensions",
            width=width,
            height=height,
            minimum=min_dimension,
        )
    return decoded, metrics


def _control_echo_metrics(
    primary: Image.Image,
    reference: Image.Image,
) -> Dict[str, Any]:
    primary_rgb = primary.convert("RGB")
    reference_rgb = reference.convert("RGB")
    reference_resized = reference_rgb.size != primary_rgb.size
    if reference_resized:
        primary_ratio = primary_rgb.width / primary_rgb.height
        reference_ratio = reference_rgb.width / reference_rgb.height
        ratio_delta = abs(primary_ratio - reference_ratio) / max(
            primary_ratio, reference_ratio
        )
        if ratio_delta > 0.01:
            return {
                "comparable": False,
                "reason": "aspect_ratio_mismatch",
                "primary_size": [primary_rgb.width, primary_rgb.height],
                "reference_size": [reference_rgb.width, reference_rgb.height],
                "aspect_ratio_relative_delta": float(ratio_delta),
            }
        reference_rgb = reference_rgb.resize(
            primary_rgb.size, Image.Resampling.LANCZOS
        )

    difference = ImageChops.difference(primary_rgb, reference_rgb)
    channel_histograms = [channel.histogram() for channel in difference.split()]
    pixel_count = primary_rgb.width * primary_rgb.height
    absolute_level_sum = sum(
        level * count
        for histogram in channel_histograms
        for level, count in enumerate(histogram)
    )
    mae_levels = absolute_level_sum / float(pixel_count * 3)

    within_masks = [
        channel.point(
            lambda level: 255
            if level <= DEFAULT_ECHO_WITHIN_LEVELS
            else 0,
            mode="L",
        )
        for channel in difference.split()
    ]
    within_all_channels = ImageChops.multiply(
        ImageChops.multiply(within_masks[0], within_masks[1]), within_masks[2]
    )
    within_histogram = within_all_channels.histogram()
    pixels_within = within_histogram[255]

    return {
        "comparable": True,
        "reference_resized": bool(reference_resized),
        "primary_size": [primary_rgb.width, primary_rgb.height],
        "reference_size": [reference.width, reference.height],
        "rgb_mae_levels": float(mae_levels),
        "rgb_mae_normalized": float(mae_levels / 255.0),
        "pixels_within_5_levels": int(pixels_within),
        "pixels_within_5_fraction": float(pixels_within / pixel_count),
    }


def validate_tpose_bundle(
    primary_bytes: bytes,
    isolated_bytes: bytes,
    reference_bytes: Optional[bytes] = None,
    *,
    control_mask_bytes: Optional[bytes] = None,
    min_dimension: int = DEFAULT_MIN_DIMENSION,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_encoded_bytes: int = DEFAULT_MAX_ENCODED_BYTES,
    alpha_occupancy_min: float = DEFAULT_ALPHA_OCCUPANCY_MIN,
    alpha_occupancy_max: float = DEFAULT_ALPHA_OCCUPANCY_MAX,
) -> Dict[str, Any]:
    """Validate a Flux T-pose primary/isolated bundle.

    ``reference_bytes`` is the decoded control-pose image supplied to Comfy.
    ``control_mask_bytes`` is an explicit keyword alias useful at call sites.
    If a reference is supplied, a near byte-format-independent visual echo is
    rejected using both RGB MAE and per-pixel tolerances.

    Returns a strict JSON-safe metrics report.  Any malformed or low-quality
    bundle raises :class:`RenderArtifactQualityError`; validation never falls
    back to the primary image when the isolated artifact is missing.
    """

    if reference_bytes is not None and control_mask_bytes is not None:
        if bytes(reference_bytes) != bytes(control_mask_bytes):
            raise ValueError(
                "reference_bytes and control_mask_bytes identify different images"
            )
    if reference_bytes is None:
        reference_bytes = control_mask_bytes

    if min_dimension < 1:
        raise ValueError("min_dimension must be positive")
    if max_image_pixels < 1 or max_encoded_bytes < 1:
        raise ValueError("image safety limits must be positive")
    if not 0.0 <= alpha_occupancy_min < alpha_occupancy_max <= 1.0:
        raise ValueError("alpha occupancy bounds must satisfy 0 <= min < max <= 1")

    report: Dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "passed": False,
        "thresholds": {
            "minimum_dimension_pixels": int(min_dimension),
            "maximum_decoded_pixels": int(max_image_pixels),
            "maximum_encoded_bytes": int(max_encoded_bytes),
            "alpha_foreground_level_exclusive": int(
                DEFAULT_ALPHA_FOREGROUND_THRESHOLD
            ),
            "alpha_foreground_occupancy_min": float(alpha_occupancy_min),
            "alpha_foreground_occupancy_max": float(alpha_occupancy_max),
            "control_echo_rgb_mae_levels_max": float(DEFAULT_ECHO_MAE_LEVELS),
            "control_echo_rgb_mae_normalized_max": float(
                DEFAULT_ECHO_MAE_LEVELS / 255.0
            ),
            "control_echo_pixel_delta_levels_max": int(
                DEFAULT_ECHO_WITHIN_LEVELS
            ),
            "control_echo_pixels_within_fraction_min": float(
                DEFAULT_ECHO_WITHIN_FRACTION
            ),
        },
    }

    try:
        primary_payload = _coerce_bytes(primary_bytes, "primary")
        isolated_payload = _coerce_bytes(isolated_bytes, "isolated")
        reference_payload = (
            _coerce_bytes(reference_bytes, "reference")
            if reference_bytes is not None
            else None
        )
    except TypeError as exc:
        _reject(
            report,
            "tpose_bundle_bytes_invalid",
            "T-pose bundle inputs must be bytes-like",
            error=str(exc),
        )

    primary, primary_metrics = _decode_image(
        primary_payload,
        role="primary",
        report=report,
        min_dimension=min_dimension,
        max_image_pixels=max_image_pixels,
        max_encoded_bytes=max_encoded_bytes,
    )
    isolated, isolated_metrics = _decode_image(
        isolated_payload,
        role="isolated",
        report=report,
        min_dimension=min_dimension,
        max_image_pixels=max_image_pixels,
        max_encoded_bytes=max_encoded_bytes,
    )

    if isolated.mode != "RGBA" or "A" not in isolated.getbands():
        _reject(
            report,
            "isolated_rgba_required",
            "isolated image must decode as RGBA with an alpha channel",
            decoded_mode=isolated.mode,
        )
    if isolated.size != primary.size:
        _reject(
            report,
            "isolated_dimensions_mismatch",
            "isolated image dimensions do not match the primary image",
            primary_size=[primary.width, primary.height],
            isolated_size=[isolated.width, isolated.height],
        )

    alpha = isolated.getchannel("A")
    alpha_histogram = alpha.histogram()
    foreground_pixels = sum(
        alpha_histogram[DEFAULT_ALPHA_FOREGROUND_THRESHOLD + 1 :]
    )
    total_pixels = isolated.width * isolated.height
    foreground_occupancy = foreground_pixels / float(total_pixels)
    alpha_mean = sum(
        level * count for level, count in enumerate(alpha_histogram)
    ) / float(total_pixels * 255)
    isolated_metrics["alpha_foreground_pixels"] = int(foreground_pixels)
    isolated_metrics["alpha_foreground_occupancy"] = float(
        foreground_occupancy
    )
    isolated_metrics["alpha_mean_normalized"] = float(alpha_mean)
    isolated_metrics["alpha_extrema"] = [int(value) for value in alpha.getextrema()]

    if foreground_occupancy < alpha_occupancy_min:
        _reject(
            report,
            "isolated_foreground_occupancy_too_low",
            "isolated image contains too little alpha foreground",
            occupancy=float(foreground_occupancy),
            minimum=float(alpha_occupancy_min),
        )
    if foreground_occupancy > alpha_occupancy_max:
        _reject(
            report,
            "isolated_foreground_occupancy_too_high",
            "isolated image contains too much alpha foreground",
            occupancy=float(foreground_occupancy),
            maximum=float(alpha_occupancy_max),
        )

    if reference_payload is not None:
        # The control image itself need not meet the production output's size;
        # same-aspect inputs are compared after deterministic resampling.
        reference, _ = _decode_image(
            reference_payload,
            role="reference",
            report=report,
            min_dimension=1,
            max_image_pixels=max_image_pixels,
            max_encoded_bytes=max_encoded_bytes,
        )
        echo = _control_echo_metrics(primary, reference)
        report["control_mask_comparison"] = echo
        if not echo["comparable"]:
            _reject(
                report,
                "control_mask_not_comparable",
                "control mask and primary image have incompatible aspect ratios",
                reason=echo.get("reason", "unknown"),
            )
        is_echo = (
            echo["rgb_mae_levels"] <= DEFAULT_ECHO_MAE_LEVELS
            and echo["pixels_within_5_fraction"]
            >= DEFAULT_ECHO_WITHIN_FRACTION
        )
        echo["is_control_mask_echo"] = bool(is_echo)
        if is_echo:
            _reject(
                report,
                "primary_matches_control_mask",
                "primary render is a near-identical echo of the control mask",
                rgb_mae_normalized=echo["rgb_mae_normalized"],
                pixels_within_5_fraction=echo["pixels_within_5_fraction"],
            )
    else:
        report["control_mask_comparison"] = {
            "performed": False,
            "reason": "reference_not_supplied",
        }

    report["passed"] = True
    return _json_safe_copy(report)


def _reject_content(
    report: Dict[str, Any],
    machine_code: str,
    message: str,
    **details: Any,
) -> None:
    report["passed"] = False
    failure: Dict[str, Any] = {
        "machine_code": machine_code,
        "message": message,
        "content": True,
    }
    if details:
        failure["details"] = details
    report["failure"] = failure
    raise RenderContentRejected(machine_code, message, report)


def _solid_line_counts(alpha: Image.Image) -> Tuple[List[float], List[float]]:
    """Clearly-opaque pixel count of every column and of every row.

    A float image box-filtered down to a single row (or column) holds the exact
    per-line mean, which keeps this PIL-only and fast even at 2048 px.
    """
    width, height = alpha.size
    solid = alpha.point(
        lambda level: 255 if level > QWEN_EDIT_SOLID_ALPHA_LEVEL else 0
    ).convert("F")
    columns = solid.resize((width, 1), Image.Resampling.BOX).getdata()
    rows = solid.resize((1, height), Image.Resampling.BOX).getdata()
    return (
        [value / 255.0 * height for value in columns],
        [value / 255.0 * width for value in rows],
    )


def _trimmed_extent(counts: List[float], trim: float) -> Tuple[int, int]:
    """First and last line holding the subject, ignoring `trim` of its mass
    at each end."""
    cut = sum(counts) * trim
    first, last = 0, len(counts) - 1
    running = 0.0
    for index, value in enumerate(counts):
        running += value
        if running > cut:
            first = index
            break
    running = 0.0
    for index in range(len(counts) - 1, -1, -1):
        running += counts[index]
        if running > cut:
            last = index
            break
    return first, last


def validate_qwen_edit_bundle(
    primary_bytes: bytes,
    isolated_bytes: bytes,
    *,
    min_dimension: int = DEFAULT_MIN_DIMENSION,
    max_image_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
    max_encoded_bytes: int = DEFAULT_MAX_ENCODED_BYTES,
    alpha_occupancy_min: float = DEFAULT_ALPHA_OCCUPANCY_MIN,
    alpha_occupancy_max: float = DEFAULT_ALPHA_OCCUPANCY_MAX,
    min_span_to_height: float = QWEN_EDIT_MIN_SPAN_TO_HEIGHT,
) -> Dict[str, Any]:
    """Validate a Qwen-Image-Edit re-pose before the 3D stage can consume it.

    Structure is checked as for a Flux T-pose bundle - both images decode, the
    cut-out is RGBA, the pair shares one size - and a failure there is the
    box's, raised as :class:`RenderArtifactQualityError`. The content is then
    read off the alpha: a subject is present, the frame did not cut it, and it
    spans roughly its own height the way a T-pose does. A miss there raises
    :class:`RenderContentRejected`: the render was fine, the pose was not.
    """

    if not 0.0 <= alpha_occupancy_min < alpha_occupancy_max <= 1.0:
        raise ValueError("alpha occupancy bounds must satisfy 0 <= min < max <= 1")
    if min_span_to_height <= 0:
        raise ValueError("min_span_to_height must be positive")

    report: Dict[str, Any] = {
        "schema": QWEN_EDIT_REPORT_SCHEMA,
        "passed": False,
        "thresholds": {
            "minimum_dimension_pixels": int(min_dimension),
            "maximum_decoded_pixels": int(max_image_pixels),
            "maximum_encoded_bytes": int(max_encoded_bytes),
            "alpha_foreground_level_exclusive": int(
                DEFAULT_ALPHA_FOREGROUND_THRESHOLD
            ),
            "alpha_foreground_occupancy_min": float(alpha_occupancy_min),
            "alpha_foreground_occupancy_max": float(alpha_occupancy_max),
            "solid_alpha_level_exclusive": int(QWEN_EDIT_SOLID_ALPHA_LEVEL),
            "edge_band_pixels": int(QWEN_EDIT_EDGE_BAND_PIXELS),
            "edge_clip_min_solid_pixels": int(QWEN_EDIT_EDGE_CLIP_MIN_PIXELS),
            "extent_trim_fraction": float(QWEN_EDIT_EXTENT_TRIM_FRACTION),
            "min_span_to_height": float(min_span_to_height),
        },
    }

    try:
        primary_payload = _coerce_bytes(primary_bytes, "primary")
        isolated_payload = _coerce_bytes(isolated_bytes, "isolated")
    except TypeError as exc:
        _reject(
            report,
            "edit_bundle_bytes_invalid",
            "edit bundle inputs must be bytes-like",
            error=str(exc),
        )

    primary, _ = _decode_image(
        primary_payload,
        role="primary",
        report=report,
        min_dimension=min_dimension,
        max_image_pixels=max_image_pixels,
        max_encoded_bytes=max_encoded_bytes,
    )
    isolated, isolated_metrics = _decode_image(
        isolated_payload,
        role="isolated",
        report=report,
        min_dimension=min_dimension,
        max_image_pixels=max_image_pixels,
        max_encoded_bytes=max_encoded_bytes,
    )
    if isolated.mode != "RGBA" or "A" not in isolated.getbands():
        _reject(
            report,
            "isolated_rgba_required",
            "isolated image must decode as RGBA with an alpha channel",
            decoded_mode=isolated.mode,
        )
    if isolated.size != primary.size:
        _reject(
            report,
            "isolated_dimensions_mismatch",
            "isolated image dimensions do not match the primary image",
            primary_size=[primary.width, primary.height],
            isolated_size=[isolated.width, isolated.height],
        )

    alpha = isolated.getchannel("A")
    alpha_histogram = alpha.histogram()
    total_pixels = isolated.width * isolated.height
    foreground_occupancy = sum(
        alpha_histogram[DEFAULT_ALPHA_FOREGROUND_THRESHOLD + 1 :]
    ) / float(total_pixels)
    isolated_metrics["alpha_foreground_occupancy"] = float(foreground_occupancy)
    if foreground_occupancy < alpha_occupancy_min:
        _reject_content(
            report,
            "qwen_edit_subject_missing",
            "the cut-out holds too little of a figure",
            occupancy=float(foreground_occupancy),
            minimum=float(alpha_occupancy_min),
        )
    if foreground_occupancy > alpha_occupancy_max:
        _reject_content(
            report,
            "qwen_edit_background_not_removed",
            "the cut-out covers nearly the whole frame",
            occupancy=float(foreground_occupancy),
            maximum=float(alpha_occupancy_max),
        )

    columns, rows = _solid_line_counts(alpha)
    if sum(columns) <= 0:
        _reject_content(
            report,
            "qwen_edit_subject_missing",
            "the cut-out has no clearly opaque pixels",
        )
    band = QWEN_EDIT_EDGE_BAND_PIXELS
    edge_counts = {
        "left": sum(columns[:band]),
        "right": sum(columns[-band:]),
        "top": sum(rows[:band]),
        "bottom": sum(rows[-band:]),
    }
    left, right = _trimmed_extent(columns, QWEN_EDIT_EXTENT_TRIM_FRACTION)
    top, bottom = _trimmed_extent(rows, QWEN_EDIT_EXTENT_TRIM_FRACTION)
    span = right - left + 1
    height = bottom - top + 1
    span_to_height = span / float(height)
    report["subject"] = {
        "extent_px": [int(left), int(top), int(right), int(bottom)],
        "span_px": int(span),
        "height_px": int(height),
        "span_to_height": round(span_to_height, 4),
        "edge_solid_pixels": {
            name: round(count, 2) for name, count in edge_counts.items()
        },
    }
    clipped = sorted(
        name
        for name, count in edge_counts.items()
        if count >= QWEN_EDIT_EDGE_CLIP_MIN_PIXELS
    )
    if clipped:
        _reject_content(
            report,
            "qwen_edit_subject_clipped",
            "the frame cuts the figure at " + ", ".join(clipped),
            edges=clipped,
        )
    if span_to_height < min_span_to_height:
        _reject_content(
            report,
            "qwen_edit_pose_not_spread",
            "the figure is not spread like a T-pose",
            span_to_height=round(span_to_height, 4),
            minimum=float(min_span_to_height),
        )

    report["passed"] = True
    return _json_safe_copy(report)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        with temporary.open("xb") as sink:
            sink.write(payload)
            sink.flush()
            os.fsync(sink.fileno())
        os.replace(str(temporary), str(path))
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _fsync_directory_best_effort(path: Path) -> None:
    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)


def archive_rejected_bundle(
    root: Path | str,
    *,
    primary_bytes: bytes,
    isolated_bytes: Optional[bytes] = None,
    reference_bytes: Optional[bytes] = None,
    report: Mapping[str, Any] | RenderArtifactQualityError,
    label: str = "tpose",
) -> Path:
    """Atomically archive exact rejected bytes and their quality report.

    A unique temporary directory is populated using atomic file writes and is
    then renamed into the caller-provided archive root.  Existing archives are
    never overwritten.  Missing optional artifacts are recorded in the
    manifest rather than synthesized.
    """

    archive_root = Path(root)
    archive_root.mkdir(parents=True, exist_ok=True)
    safe_label = re.sub(r"[^A-Za-z0-9._-]+", "-", str(label)).strip("-._")
    safe_label = (safe_label or "tpose")[:64]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    unique = uuid.uuid4().hex
    directory_name = f"{timestamp}_{safe_label}_{unique}"
    final_directory = archive_root / directory_name
    temporary_directory = archive_root / f".{directory_name}.tmp-{uuid.uuid4().hex}"

    primary_payload = _coerce_bytes(primary_bytes, "primary")
    isolated_payload = (
        _coerce_bytes(isolated_bytes, "isolated")
        if isolated_bytes is not None
        else None
    )
    reference_payload = (
        _coerce_bytes(reference_bytes, "reference")
        if reference_bytes is not None
        else None
    )
    report_value = report.report if isinstance(report, RenderArtifactQualityError) else report
    report_payload = json.dumps(
        _json_safe_copy(report_value),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ).encode("utf-8") + b"\n"

    artifacts = {
        "primary.bin": primary_payload,
        "isolated.bin": isolated_payload,
        "reference.bin": reference_payload,
    }
    manifest: Dict[str, Any] = {
        "schema": ARCHIVE_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
    }
    for filename, payload in artifacts.items():
        manifest["files"][filename] = {
            "present": payload is not None,
            "bytes": len(payload) if payload is not None else 0,
            "sha256": hashlib.sha256(payload).hexdigest()
            if payload is not None
            else None,
        }

    try:
        temporary_directory.mkdir(parents=False, exist_ok=False)
        for filename, payload in artifacts.items():
            if payload is not None:
                _atomic_write_bytes(temporary_directory / filename, payload)
        _atomic_write_bytes(temporary_directory / "report.json", report_payload)
        manifest_payload = json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8") + b"\n"
        _atomic_write_bytes(temporary_directory / "manifest.json", manifest_payload)
        _fsync_directory_best_effort(temporary_directory)
        os.replace(str(temporary_directory), str(final_directory))
        _fsync_directory_best_effort(archive_root)
    except Exception:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise

    return final_directory
