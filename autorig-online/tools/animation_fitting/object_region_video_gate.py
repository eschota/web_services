from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import struct
import sys
import tempfile
from typing import Any, Iterable

import cv2
import numpy as np
from scipy.ndimage import distance_transform_edt

from .errors import ContractError


SCHEMA = "autorig.animation-fitting.object-region-video-gate.v1"
GUIDE_SCHEMA = "autorig-browser-ltx-recovery-guide-bundle.v1"
WIDTH = 768
HEIGHT = 448
FRAME_COUNT = 49
KEY_FRAMES = (0, 6, 12, 18, 24, 30, 36, 42, 48)
RECOVERY_FRAMES = (12, 24, 36)
SWING_NEIGHBORS = {6: (0, 12), 18: (12, 24), 30: (24, 36), 42: (36, 48)}
ENDPOINT_FRAMES = (0, 12, 24, 36, 48)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
SHA256_RE = re.compile(r"[0-9a-f]{64}")
AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS = frozenset(
    {
        (
            "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451",
            "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3",
        )
    }
)

RECOVERY_THRESHOLDS = {
    "normalized_visibility_ratio_min": 0.92,
    "normalized_visibility_ratio_max": 1.08,
    "silhouette_iou_min": 0.90,
    "boundary_p95_px_max": 5.0,
    "object_psnr_db_min": 35.0,
    "object_ssim_gray_min": 0.95,
}
ENDPOINT_THRESHOLDS = {
    "normalized_visibility_ratio_min": 0.97,
    "normalized_visibility_ratio_max": 1.03,
    "silhouette_iou_min": 0.95,
    "boundary_p95_px_max": 3.0,
    "object_psnr_db_min": 38.0,
    "object_ssim_gray_min": 0.98,
    "centroid_shift_px_max": 2.0,
}
DISTAL_PHASE_THRESHOLDS = {
    "silhouette_recall_min": 0.90,
    "silhouette_iou_min": 0.85,
    "boundary_p95_px_max": 5.0,
    "object_psnr_db_min": 30.0,
    "object_ssim_gray_min": 0.94,
}
SWING_THRESHOLDS = {
    "normalized_local_mae_ratio_min": 0.75,
    "normalized_local_mae_ratio_max": 1.75,
    "normalized_changed_fraction_ratio_min": 0.75,
    "normalized_changed_fraction_ratio_max": 1.75,
    "phase_roi_guide_delta_threshold_rgb": 3.0,
    "phase_roi_dilation_radius_px": 7,
    "changed_pixel_delta_threshold_rgb": 5.0,
    "direct_silhouette_recall_min": 0.70,
    "direct_silhouette_iou_min": 0.65,
    "direct_boundary_p95_px_max": 8.0,
    "direct_object_psnr_db_min": 25.0,
    "direct_object_ssim_gray_min": 0.88,
    "signed_motion_correlation_min": 0.75,
}


def _round(value: float) -> float:
    return round(float(value), 6)


def _required_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ContractError(f"{field} must be an exact lowercase SHA-256")
    return value


def _required_bytes(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContractError(f"{field} must be a positive integer")
    return value


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


class _ReadOnceCache:
    """Caches immutable input bytes so a resolved path is never reopened."""

    def __init__(self) -> None:
        self._payloads: dict[Path, bytes] = {}

    def read(self, path: Path, *, field: str) -> tuple[Path, bytes]:
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise ContractError(f"Cannot resolve {field} {path}: {exc}") from exc
        payload = self._payloads.get(resolved)
        if payload is None:
            if not resolved.is_file():
                raise ContractError(f"{field} must be a regular file: {resolved}")
            try:
                payload = resolved.read_bytes()
            except OSError as exc:
                raise ContractError(f"Cannot read {field} {resolved}: {exc}") from exc
            self._payloads[resolved] = payload
        return resolved, payload


def _verify_pin(payload: bytes, *, expected_sha256: str, expected_bytes: int, field: str) -> None:
    if len(payload) != expected_bytes:
        raise ContractError(
            f"{field} byte-size mismatch: expected {expected_bytes}, got {len(payload)}"
        )
    actual = _sha256(payload)
    if actual != expected_sha256:
        raise ContractError(
            f"{field} SHA-256 mismatch: expected {expected_sha256}, got {actual}"
        )


def _decode_png(payload: bytes, *, field: str) -> np.ndarray:
    if not payload.startswith(PNG_SIGNATURE):
        raise ContractError(f"{field} must be an exact PNG")
    decoded = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_UNCHANGED)
    if decoded is None:
        raise ContractError(f"Cannot decode {field} PNG")
    if decoded.ndim != 3 or decoded.shape[2] not in (3, 4):
        raise ContractError(f"{field} PNG must have RGB or opaque RGBA pixels")
    if decoded.shape[2] == 4:
        if not np.all(decoded[:, :, 3] == 255):
            raise ContractError(f"{field} PNG alpha must be fully opaque")
        decoded = cv2.cvtColor(decoded, cv2.COLOR_BGRA2BGR)
    if decoded.shape != (HEIGHT, WIDTH, 3) or decoded.dtype != np.uint8:
        raise ContractError(
            f"{field} must decode to exactly {WIDTH}x{HEIGHT} uint8 RGB"
        )
    return decoded


def frame_set_digest(payloads: Iterable[tuple[str, bytes]]) -> tuple[str, int]:
    """Return the versioned digest used to pin an immutable extracted frame set."""

    digest = hashlib.sha256()
    digest.update(b"autorig.object-region-frame-set.v1\0")
    total_bytes = 0
    for filename, payload in payloads:
        encoded = filename.encode("utf-8")
        digest.update(struct.pack(">I", len(encoded)))
        digest.update(encoded)
        digest.update(struct.pack(">Q", len(payload)))
        digest.update(payload)
        total_bytes += len(payload)
    return digest.hexdigest(), total_bytes


def _safe_bundle_child(root: Path, filename: Any, field: str) -> Path:
    if not isinstance(filename, str) or not filename:
        raise ContractError(f"{field} must be a non-empty relative filename")
    relative = Path(filename)
    if relative.is_absolute() or relative.drive:
        raise ContractError(f"{field} must be relative to the guide bundle")
    candidate = (root / relative).resolve()
    if not _is_within(candidate, root):
        raise ContractError(f"{field} escapes the guide bundle")
    return candidate


def _load_candidate_frames(
    candidate: Path,
    *,
    expected_sha256: str,
    expected_bytes: int,
    reader: _ReadOnceCache,
) -> tuple[list[np.ndarray], dict[str, Any]]:
    resolved = candidate.resolve()
    if resolved.is_dir():
        expected_names = tuple(f"frame_{index:06d}.png" for index in range(FRAME_COUNT))
        observed_names = tuple(sorted(path.name for path in resolved.glob("frame_*.png")))
        if observed_names != expected_names:
            raise ContractError(
                "Extracted frame directory must contain exactly frame_000000.png through "
                "frame_000048.png"
            )
        pinned_payloads: list[tuple[str, bytes]] = []
        frames: list[np.ndarray] = []
        for index, filename in enumerate(expected_names):
            path = (resolved / filename).resolve()
            if not _is_within(path, resolved):
                raise ContractError(f"candidate frame {filename} escapes its directory")
            _, payload = reader.read(path, field=f"candidate frame {index}")
            pinned_payloads.append((filename, payload))
            frames.append(_decode_png(payload, field=f"candidate frame {index}"))
        actual_sha256, actual_bytes = frame_set_digest(pinned_payloads)
        if actual_bytes != expected_bytes:
            raise ContractError(
                f"candidate frame-set byte-size mismatch: expected {expected_bytes}, got {actual_bytes}"
            )
        if actual_sha256 != expected_sha256:
            raise ContractError(
                f"candidate frame-set SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
            )
        return frames, {
            "kind": "immutable_frame_directory",
            "path": str(resolved),
            "sha256": actual_sha256,
            "bytes": actual_bytes,
            "digest_contract": "autorig.object-region-frame-set.v1",
        }

    if not resolved.is_file() or resolved.suffix.lower() != ".mp4":
        raise ContractError("candidate must be an MP4 file or immutable extracted frame directory")
    resolved, payload = reader.read(resolved, field="candidate MP4")
    _verify_pin(
        payload,
        expected_sha256=expected_sha256,
        expected_bytes=expected_bytes,
        field="candidate MP4",
    )
    frames = []
    with tempfile.TemporaryDirectory(prefix="autorig-object-region-") as temporary:
        decode_path = Path(temporary) / "candidate.mp4"
        decode_path.write_bytes(payload)
        capture = cv2.VideoCapture(str(decode_path))
        if not capture.isOpened():
            raise ContractError("Cannot decode pinned candidate MP4")
        try:
            while True:
                ok, frame = capture.read()
                if not ok:
                    break
                if frame.shape != (HEIGHT, WIDTH, 3) or frame.dtype != np.uint8:
                    raise ContractError(
                        f"candidate MP4 frames must be exactly {WIDTH}x{HEIGHT} uint8 RGB"
                    )
                frames.append(frame)
                if len(frames) > FRAME_COUNT:
                    raise ContractError(f"candidate MP4 must contain exactly {FRAME_COUNT} frames")
        finally:
            capture.release()
    if len(frames) != FRAME_COUNT:
        raise ContractError(
            f"candidate MP4 must contain exactly {FRAME_COUNT} frames; got {len(frames)}"
        )
    return frames, {
        "kind": "mp4",
        "path": str(resolved),
        "sha256": expected_sha256,
        "bytes": expected_bytes,
    }


def _load_guide_bundle(
    bundle: Path,
    *,
    manifest_sha256: str,
    endpoint_sha256: str,
    endpoint_bytes: int,
    reader: _ReadOnceCache,
) -> tuple[dict[int, np.ndarray], dict[str, Any]]:
    if (manifest_sha256, endpoint_sha256) not in AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS:
        raise ContractError(
            "guide manifest and endpoint pins are not a code-authorized immutable pair"
        )
    try:
        root = bundle.resolve(strict=True)
    except OSError as exc:
        raise ContractError(f"Cannot resolve guide bundle {bundle}: {exc}") from exc
    if not root.is_dir():
        raise ContractError(f"guide_bundle must be a directory: {root}")
    manifest_path = _safe_bundle_child(root, "immutable_manifest.json", "guide manifest")
    _, manifest_bytes = reader.read(manifest_path, field="guide manifest")
    actual_manifest_sha256 = _sha256(manifest_bytes)
    if actual_manifest_sha256 != manifest_sha256:
        raise ContractError(
            "guide manifest SHA-256 mismatch: "
            f"expected {manifest_sha256}, got {actual_manifest_sha256}"
        )
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid guide manifest JSON: {exc}") from exc
    if not isinstance(manifest, dict) or manifest.get("schema") != GUIDE_SCHEMA:
        raise ContractError(f"guide manifest schema must be {GUIDE_SCHEMA}")
    if manifest.get("resolution") != [WIDTH, HEIGHT]:
        raise ContractError(f"guide manifest resolution must be [{WIDTH}, {HEIGHT}]")
    if manifest.get("cycle_frame_count_int") != FRAME_COUNT:
        raise ContractError(f"guide manifest cycle_frame_count_int must be {FRAME_COUNT}")
    if manifest.get("guide_count_int") != len(KEY_FRAMES):
        raise ContractError(f"guide manifest guide_count_int must be {len(KEY_FRAMES)}")
    if manifest.get("recovery_frame_indices_array") != list(RECOVERY_FRAMES):
        raise ContractError("guide manifest recovery frame indices are not canonical")
    if manifest.get("recovery_guides_byte_identical_endpoint_bool") is not True:
        raise ContractError("guide manifest must pin byte-identical recovery guides")
    if manifest.get("endpoint_guide_sha256_string") != endpoint_sha256:
        raise ContractError("guide manifest endpoint SHA-256 disagrees with endpoint guide pin")
    entries = manifest.get("frames_array")
    if not isinstance(entries, list) or len(entries) != len(KEY_FRAMES):
        raise ContractError("guide manifest frames_array must contain exactly nine entries")
    by_index: dict[int, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ContractError("guide manifest frame entries must be objects")
        index = entry.get("frame_index_int")
        if isinstance(index, bool) or not isinstance(index, int) or index in by_index:
            raise ContractError("guide manifest frame indices must be unique integers")
        by_index[index] = entry
    if tuple(sorted(by_index)) != KEY_FRAMES:
        raise ContractError(f"guide manifest frame indices must be {list(KEY_FRAMES)}")

    images: dict[int, np.ndarray] = {}
    resolved_files: list[str] = []
    for index in KEY_FRAMES:
        entry = by_index[index]
        sha = _required_sha256(entry.get("sha256_string"), f"guide frame {index} SHA-256")
        byte_count = _required_bytes(entry.get("bytes_int"), f"guide frame {index} bytes")
        if index in ENDPOINT_FRAMES and (sha != endpoint_sha256 or byte_count != endpoint_bytes):
            raise ContractError(
                f"guide frame {index} must be byte-identical to the pinned endpoint guide"
            )
        path = _safe_bundle_child(
            root, entry.get("filename_string"), f"guide frame {index} filename"
        )
        resolved, payload = reader.read(path, field=f"guide frame {index}")
        _verify_pin(
            payload,
            expected_sha256=sha,
            expected_bytes=byte_count,
            field=f"guide frame {index}",
        )
        images[index] = _decode_png(payload, field=f"guide frame {index}")
        resolved_files.append(str(resolved))
    return images, {
        "path": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "schema": GUIDE_SCHEMA,
        "files": resolved_files,
    }


def _border_mask() -> np.ndarray:
    border = np.ones((HEIGHT, WIDTH), dtype=bool)
    border[32:-32, 32:-32] = False
    return border


def _endpoint_object_mask(endpoint: np.ndarray) -> np.ndarray:
    background = np.median(endpoint[_border_mask()], axis=0)
    residual = np.linalg.norm(endpoint.astype(np.float32) - background, axis=2)
    foreground = (residual >= 5.0).astype(np.uint8)
    foreground = cv2.morphologyEx(
        foreground, cv2.MORPH_CLOSE, np.ones((3, 3), dtype=np.uint8)
    )
    count, labels, stats, _ = cv2.connectedComponentsWithStats(foreground, 8)
    if count <= 1:
        raise ContractError("endpoint guide has no measurable foreground object")
    selected = 1 + int(np.argmax(stats[1:, cv2.CC_STAT_AREA]))
    mask = labels == selected
    fraction = float(mask.mean())
    if fraction < 0.005 or fraction > 0.50:
        raise ContractError(
            f"endpoint guide foreground fraction {fraction:.6f} is outside [0.005, 0.50]"
        )
    return mask


def _segment_frame(
    frame: np.ndarray,
    *,
    support: np.ndarray,
    endpoint_mask: np.ndarray,
    codec_tolerant: bool = False,
) -> tuple[np.ndarray, dict[str, Any]]:
    background = np.median(frame[~support], axis=0)
    residual = np.linalg.norm(frame.astype(np.float32) - background, axis=2)
    background_residual_p99 = float(np.percentile(residual[~support], 99))
    foreground = (residual >= 5.0) & support
    codec_reference_threshold = None
    codec_boundary_snap_radius = 0
    if codec_tolerant:
        # H.264/yuv420p can quantize low-contrast object-edge pixels below the
        # normal residual threshold and can bleed one or two pixels outside the
        # reference silhouette. Recover only pixels inside the pinned reference
        # object, then discard only that narrow exterior codec halo. Missing
        # reference pixels remain missing, so distal recall is not weakened.
        codec_reference_threshold = max(1.5, background_residual_p99 + 0.5)
        foreground |= endpoint_mask & (residual >= codec_reference_threshold) & support
        codec_boundary_snap_radius = 2
        codec_halo = cv2.dilate(
            endpoint_mask.astype(np.uint8),
            cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5)),
        ).astype(bool) & ~endpoint_mask
        foreground &= ~codec_halo
    foreground = foreground.astype(np.uint8)
    foreground = cv2.morphologyEx(
        foreground, cv2.MORPH_CLOSE, np.ones((3, 3), dtype=np.uint8)
    )
    count, labels, stats, _ = cv2.connectedComponentsWithStats(foreground, 8)
    if count <= 1:
        raise ContractError("candidate key frame has no measurable foreground object")
    scores = []
    for component in range(1, count):
        overlap = int(np.logical_and(labels == component, endpoint_mask).sum())
        area = int(stats[component, cv2.CC_STAT_AREA])
        scores.append((overlap, area, component))
    overlap, _, selected = max(scores)
    if overlap < 1:
        raise ContractError("candidate foreground does not overlap the endpoint guide object")
    return labels == selected, {
        "background_median_bgr": [_round(value) for value in background],
        "background_residual_p99": _round(background_residual_p99),
        "codec_tolerant_reference_segmentation": codec_tolerant,
        "codec_reference_residual_threshold_rgb_l2": (
            None
            if codec_reference_threshold is None
            else _round(codec_reference_threshold)
        ),
        "codec_boundary_snap_radius_px": codec_boundary_snap_radius,
    }


def _psnr(first: np.ndarray, second: np.ndarray, mask: np.ndarray) -> float:
    difference = first.astype(np.float32) - second.astype(np.float32)
    mse = float(np.mean(np.square(difference[mask])))
    if mse == 0.0:
        return 99.0
    return float(10.0 * np.log10(255.0 * 255.0 / mse))


def _ssim_map(first: np.ndarray, second: np.ndarray) -> np.ndarray:
    first_gray = cv2.cvtColor(first, cv2.COLOR_BGR2GRAY).astype(np.float64)
    second_gray = cv2.cvtColor(second, cv2.COLOR_BGR2GRAY).astype(np.float64)
    mu_first = cv2.GaussianBlur(first_gray, (11, 11), 1.5)
    mu_second = cv2.GaussianBlur(second_gray, (11, 11), 1.5)
    mu_first_sq = mu_first * mu_first
    mu_second_sq = mu_second * mu_second
    mu_product = mu_first * mu_second
    sigma_first = cv2.GaussianBlur(first_gray * first_gray, (11, 11), 1.5) - mu_first_sq
    sigma_second = (
        cv2.GaussianBlur(second_gray * second_gray, (11, 11), 1.5) - mu_second_sq
    )
    covariance = (
        cv2.GaussianBlur(first_gray * second_gray, (11, 11), 1.5) - mu_product
    )
    c1 = (0.01 * 255.0) ** 2
    c2 = (0.03 * 255.0) ** 2
    numerator = (2.0 * mu_product + c1) * (2.0 * covariance + c2)
    denominator = (mu_first_sq + mu_second_sq + c1) * (
        sigma_first + sigma_second + c2
    )
    return np.divide(numerator, denominator, out=np.ones_like(numerator), where=denominator != 0)


def _boundary(mask: np.ndarray) -> np.ndarray:
    source = mask.astype(np.uint8)
    return (source - cv2.erode(source, np.ones((3, 3), dtype=np.uint8))).astype(bool)


def _boundary_p95(first: np.ndarray, second: np.ndarray) -> float:
    first_boundary = _boundary(first)
    second_boundary = _boundary(second)
    first_to_second = distance_transform_edt(~second_boundary)[first_boundary]
    second_to_first = distance_transform_edt(~first_boundary)[second_boundary]
    distances = np.concatenate((first_to_second, second_to_first))
    if distances.size == 0:
        raise ContractError("Cannot measure an empty silhouette boundary")
    return float(np.percentile(distances, 95))


def _pair_metrics(
    first_image: np.ndarray,
    second_image: np.ndarray,
    first_mask: np.ndarray,
    second_mask: np.ndarray,
) -> dict[str, float]:
    union = first_mask | second_mask
    intersection = first_mask & second_mask
    first_y, first_x = np.where(first_mask)
    second_y, second_x = np.where(second_mask)
    first_centroid = np.array((first_x.mean(), first_y.mean()))
    second_centroid = np.array((second_x.mean(), second_y.mean()))
    return {
        "silhouette_iou": _round(intersection.sum() / union.sum()),
        "boundary_p95_px": _round(_boundary_p95(first_mask, second_mask)),
        "object_psnr_db": _round(_psnr(first_image, second_image, union)),
        "object_ssim_gray": _round(_ssim_map(first_image, second_image)[union].mean()),
        "centroid_shift_px": _round(np.linalg.norm(second_centroid - first_centroid)),
    }


def _roi_boundary_p95(
    first_mask: np.ndarray, second_mask: np.ndarray, roi: np.ndarray
) -> float:
    first_boundary = _boundary(first_mask)
    second_boundary = _boundary(second_mask)
    first_samples = first_boundary & roi
    second_samples = second_boundary & roi
    if not first_samples.any() or not second_samples.any():
        return float(max(WIDTH, HEIGHT))
    first_to_second = distance_transform_edt(~second_boundary)[first_samples]
    second_to_first = distance_transform_edt(~first_boundary)[second_samples]
    return float(np.percentile(np.concatenate((first_to_second, second_to_first)), 95))


def _roi_pair_metrics(
    first_image: np.ndarray,
    second_image: np.ndarray,
    first_mask: np.ndarray,
    second_mask: np.ndarray,
    roi: np.ndarray,
) -> dict[str, float]:
    first_local = first_mask & roi
    second_local = second_mask & roi
    union = first_local | second_local
    intersection = first_local & second_local
    if not first_local.any():
        raise ContractError("phase ROI has no expected object pixels")
    if not union.any():
        raise ContractError("phase ROI has no measurable object pixels")
    return {
        "silhouette_recall": _round(intersection.sum() / first_local.sum()),
        "silhouette_iou": _round(intersection.sum() / union.sum()),
        "boundary_p95_px": _round(_roi_boundary_p95(first_mask, second_mask, roi)),
        "object_psnr_db": _round(_psnr(first_image, second_image, union)),
        "object_ssim_gray": _round(
            _ssim_map(first_image, second_image)[union].mean()
        ),
    }


def _distal_phase_checks(metrics: dict[str, float]) -> dict[str, bool]:
    return {
        "silhouette_recall": metrics["silhouette_recall"]
        >= DISTAL_PHASE_THRESHOLDS["silhouette_recall_min"],
        "silhouette_iou": metrics["silhouette_iou"]
        >= DISTAL_PHASE_THRESHOLDS["silhouette_iou_min"],
        "boundary_p95": metrics["boundary_p95_px"]
        <= DISTAL_PHASE_THRESHOLDS["boundary_p95_px_max"],
        "object_psnr": metrics["object_psnr_db"]
        >= DISTAL_PHASE_THRESHOLDS["object_psnr_db_min"],
        "object_ssim": metrics["object_ssim_gray"]
        >= DISTAL_PHASE_THRESHOLDS["object_ssim_gray_min"],
    }


def _signed_motion_correlation(
    candidate: np.ndarray, guide: np.ndarray, endpoint: np.ndarray, roi: np.ndarray
) -> float:
    guide_delta = (guide.astype(np.float32) - endpoint.astype(np.float32))[roi].reshape(-1)
    candidate_delta = (
        candidate.astype(np.float32) - endpoint.astype(np.float32)
    )[roi].reshape(-1)
    guide_norm = float(np.linalg.norm(guide_delta))
    candidate_norm = float(np.linalg.norm(candidate_delta))
    if guide_norm <= 0.0:
        raise ContractError("guide swing phase ROI has no signed motion")
    if candidate_norm <= 0.0:
        return 0.0
    return float(np.dot(guide_delta, candidate_delta) / (guide_norm * candidate_norm))


def _recovery_checks(visibility: float, metrics: dict[str, float]) -> dict[str, bool]:
    return {
        "visibility": RECOVERY_THRESHOLDS["normalized_visibility_ratio_min"]
        <= visibility
        <= RECOVERY_THRESHOLDS["normalized_visibility_ratio_max"],
        "silhouette_iou": metrics["silhouette_iou"]
        >= RECOVERY_THRESHOLDS["silhouette_iou_min"],
        "boundary_p95": metrics["boundary_p95_px"]
        <= RECOVERY_THRESHOLDS["boundary_p95_px_max"],
        "object_psnr": metrics["object_psnr_db"]
        >= RECOVERY_THRESHOLDS["object_psnr_db_min"],
        "object_ssim": metrics["object_ssim_gray"]
        >= RECOVERY_THRESHOLDS["object_ssim_gray_min"],
    }


def _endpoint_checks(visibility: float, metrics: dict[str, float]) -> dict[str, bool]:
    return {
        "visibility": ENDPOINT_THRESHOLDS["normalized_visibility_ratio_min"]
        <= visibility
        <= ENDPOINT_THRESHOLDS["normalized_visibility_ratio_max"],
        "silhouette_iou": metrics["silhouette_iou"]
        >= ENDPOINT_THRESHOLDS["silhouette_iou_min"],
        "boundary_p95": metrics["boundary_p95_px"]
        <= ENDPOINT_THRESHOLDS["boundary_p95_px_max"],
        "object_psnr": metrics["object_psnr_db"]
        >= ENDPOINT_THRESHOLDS["object_psnr_db_min"],
        "object_ssim": metrics["object_ssim_gray"]
        >= ENDPOINT_THRESHOLDS["object_ssim_gray_min"],
        "centroid_shift": metrics["centroid_shift_px"]
        <= ENDPOINT_THRESHOLDS["centroid_shift_px_max"],
    }


def _evaluate(
    frames: list[np.ndarray],
    endpoint: np.ndarray,
    guide_images: dict[int, np.ndarray] | None,
    *,
    codec_tolerant: bool,
) -> tuple[dict[str, Any], bytes]:
    endpoint_mask = _endpoint_object_mask(endpoint)
    support = cv2.dilate(
        endpoint_mask.astype(np.uint8),
        cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (91, 91)),
    ).astype(bool)
    if int((~support).sum()) < WIDTH * HEIGHT // 4:
        raise ContractError("endpoint guide leaves too little background for object segmentation")
    masks: dict[int, np.ndarray] = {}
    backgrounds: dict[int, dict[str, Any]] = {}
    for index in KEY_FRAMES:
        masks[index], backgrounds[index] = _segment_frame(
            frames[index],
            support=support,
            endpoint_mask=endpoint_mask,
            codec_tolerant=codec_tolerant,
        )

    phase_rois: dict[int, np.ndarray] = {}
    guide_masks: dict[int, np.ndarray] = {}
    if guide_images is not None:
        endpoint_float = endpoint.astype(np.float32)
        for swing in SWING_NEIGHBORS:
            guide_difference = np.abs(
                guide_images[swing].astype(np.float32) - endpoint_float
            )
            phase_roi = np.max(guide_difference, axis=2) >= SWING_THRESHOLDS[
                "phase_roi_guide_delta_threshold_rgb"
            ]
            phase_roi = cv2.dilate(
                phase_roi.astype(np.uint8),
                cv2.getStructuringElement(
                    cv2.MORPH_ELLIPSE,
                    (
                        SWING_THRESHOLDS["phase_roi_dilation_radius_px"] * 2 + 1,
                        SWING_THRESHOLDS["phase_roi_dilation_radius_px"] * 2 + 1,
                    ),
                ),
            ).astype(bool)
            if int(phase_roi.sum()) < 64:
                raise ContractError(f"guide swing frame {swing} has an empty phase ROI")
            if int((endpoint_mask & phase_roi).sum()) < 32:
                raise ContractError(
                    f"guide swing frame {swing} phase ROI has too little endpoint object support"
                )
            phase_rois[swing] = phase_roi
            guide_masks[swing], _ = _segment_frame(
                guide_images[swing], support=support, endpoint_mask=endpoint_mask
            )
            if codec_tolerant:
                masks[swing], backgrounds[swing] = _segment_frame(
                    frames[swing],
                    support=support,
                    endpoint_mask=guide_masks[swing],
                    codec_tolerant=True,
                )

    baseline_area = float(endpoint_mask.sum())
    frame_results: dict[str, Any] = {}
    recovery_results: dict[str, Any] = {}
    for index in ENDPOINT_FRAMES:
        visibility = float(masks[index].sum() / baseline_area)
        endpoint_metrics = _pair_metrics(
            endpoint, frames[index], endpoint_mask, masks[index]
        )
        global_checks = (
            _recovery_checks(visibility, endpoint_metrics)
            if index in RECOVERY_FRAMES
            else _endpoint_checks(visibility, endpoint_metrics)
        )
        distal_results: dict[str, Any] = {}
        if guide_images is not None:
            for phase, phase_roi in phase_rois.items():
                distal_metrics = _roi_pair_metrics(
                    endpoint,
                    frames[index],
                    endpoint_mask,
                    masks[index],
                    phase_roi,
                )
                distal_checks = _distal_phase_checks(distal_metrics)
                distal_results[str(phase)] = {
                    "metrics": distal_metrics,
                    "checks": distal_checks,
                    "pass": all(distal_checks.values()),
                }
            distal_status = "evaluated"
            distal_pass = all(item["pass"] for item in distal_results.values())
        else:
            distal_status = "not_evaluated_missing_pinned_guide_bundle"
            distal_pass = False
        stance_pass = all(global_checks.values()) and distal_pass
        frame_result: dict[str, Any] = {
            "foreground_pixels": int(masks[index].sum()),
            "normalized_visibility_ratio": _round(visibility),
            "background": backgrounds[index],
            "vs_endpoint_guide": endpoint_metrics,
            "endpoint_guide_checks": global_checks,
            "distal_phase_status": distal_status,
            "distal_phase_results": distal_results,
            "pass": stance_pass,
        }
        if index != 0:
            metrics = _pair_metrics(frames[0], frames[index], masks[0], masks[index])
            frame_result["vs_frame0"] = metrics
            if index in RECOVERY_FRAMES:
                recovery_results[str(index)] = {
                    "checks": global_checks,
                    "vs_endpoint_guide": endpoint_metrics,
                    "distal_phase_results": distal_results,
                    "pass": stance_pass,
                }
        frame_results[str(index)] = frame_result
    baseline_endpoint_result = {
        "checks": frame_results["0"]["endpoint_guide_checks"],
        "distal_phase_results": frame_results["0"]["distal_phase_results"],
        "pass": frame_results["0"]["pass"],
    }
    endpoint_result = {
        "checks": frame_results["48"]["endpoint_guide_checks"],
        "distal_phase_results": frame_results["48"]["distal_phase_results"],
        "pass": frame_results["48"]["pass"],
    }

    swing_results: dict[str, Any] = {}
    if guide_images is not None:
        endpoint_float = endpoint.astype(np.float32)
        for swing, (left, right) in SWING_NEIGHBORS.items():
            guide_difference = np.abs(guide_images[swing].astype(np.float32) - endpoint_float)
            phase_roi = phase_rois[swing]
            guide_mae = float(guide_difference[phase_roi].mean())
            guide_changed = float(
                (
                    np.max(guide_difference, axis=2)[phase_roi]
                    >= SWING_THRESHOLDS["changed_pixel_delta_threshold_rgb"]
                ).mean()
            )
            if guide_mae <= 0.0 or guide_changed <= 0.0:
                raise ContractError(f"guide swing frame {swing} has no measurable local motion")
            neighbors = []
            for neighbor in (left, right):
                difference = np.abs(
                    frames[swing].astype(np.float32) - frames[neighbor].astype(np.float32)
                )
                local_mae = float(difference[phase_roi].mean())
                changed = float(
                    (
                        np.max(difference, axis=2)[phase_roi]
                        >= SWING_THRESHOLDS["changed_pixel_delta_threshold_rgb"]
                    ).mean()
                )
                neighbors.append(
                    {
                        "frame": neighbor,
                        "local_mae_rgb": _round(local_mae),
                        "changed_pixel_fraction": _round(changed),
                        "normalized_mae_ratio": _round(local_mae / guide_mae),
                        "normalized_changed_fraction_ratio": _round(changed / guide_changed),
                    }
                )
            mae_ratios = [item["normalized_mae_ratio"] for item in neighbors]
            changed_ratios = [
                item["normalized_changed_fraction_ratio"] for item in neighbors
            ]
            direct_metrics = _roi_pair_metrics(
                guide_images[swing],
                frames[swing],
                guide_masks[swing],
                masks[swing],
                phase_roi,
            )
            signed_correlation = _round(
                _signed_motion_correlation(
                    frames[swing], guide_images[swing], endpoint, phase_roi
                )
            )
            direct_metrics["signed_motion_correlation"] = signed_correlation
            checks = {
                "mae_ratio": SWING_THRESHOLDS["normalized_local_mae_ratio_min"]
                <= min(mae_ratios)
                and max(mae_ratios)
                <= SWING_THRESHOLDS["normalized_local_mae_ratio_max"],
                "changed_fraction_ratio": SWING_THRESHOLDS[
                    "normalized_changed_fraction_ratio_min"
                ]
                <= min(changed_ratios)
                and max(changed_ratios)
                <= SWING_THRESHOLDS["normalized_changed_fraction_ratio_max"],
                "direct_silhouette_recall": direct_metrics["silhouette_recall"]
                >= SWING_THRESHOLDS["direct_silhouette_recall_min"],
                "direct_silhouette_iou": direct_metrics["silhouette_iou"]
                >= SWING_THRESHOLDS["direct_silhouette_iou_min"],
                "direct_boundary_p95": direct_metrics["boundary_p95_px"]
                <= SWING_THRESHOLDS["direct_boundary_p95_px_max"],
                "direct_object_psnr": direct_metrics["object_psnr_db"]
                >= SWING_THRESHOLDS["direct_object_psnr_db_min"],
                "direct_object_ssim": direct_metrics["object_ssim_gray"]
                >= SWING_THRESHOLDS["direct_object_ssim_gray_min"],
                "signed_motion_correlation": signed_correlation
                >= SWING_THRESHOLDS["signed_motion_correlation_min"],
            }
            swing_results[str(swing)] = {
                "phase_roi_pixels": int(phase_roi.sum()),
                "guide_local_mae_rgb": _round(guide_mae),
                "guide_changed_pixel_fraction": _round(guide_changed),
                "neighbors": neighbors,
                "direct_vs_pinned_guide": direct_metrics,
                "checks": checks,
                "pass": all(checks.values()),
            }
        swing_status = "evaluated"
        swing_pass = all(item["pass"] for item in swing_results.values())
    else:
        swing_status = "not_evaluated_missing_pinned_guide_bundle"
        swing_pass = False

    stances_pass = all(frame_results[str(index)]["pass"] for index in ENDPOINT_FRAMES)
    approved = stances_pass and swing_pass
    evaluation = {
        "approved_for_fitting": approved,
        "verdict": "PASS" if approved else "FAIL",
        "frames": frame_results,
        "baseline_endpoint_result": baseline_endpoint_result,
        "recovery_results": recovery_results,
        "loop_endpoint_result": endpoint_result,
        "swing_status": swing_status,
        "swing_results": swing_results,
        "summary": {
            "recoveries_passed": sum(item["pass"] for item in recovery_results.values()),
            "recoveries_total": len(RECOVERY_FRAMES),
            "loop_endpoint_pass": endpoint_result["pass"],
            "swings_passed": sum(item["pass"] for item in swing_results.values()),
            "swings_total": len(SWING_NEIGHBORS),
        },
    }

    panels = []
    for index in ENDPOINT_FRAMES:
        detail = (
            f"guide IoU={frame_results[str(index)]['vs_endpoint_guide']['silhouette_iou']:.3f} "
            f"P95={frame_results[str(index)]['vs_endpoint_guide']['boundary_p95_px']:.1f}px"
        )
        panels.append(_evidence_panel(frames[index], masks[index], f"frame {index}", detail))
    recovery_row = np.hstack(panels)
    swing_panels = []
    for swing in SWING_NEIGHBORS:
        result = swing_results.get(str(swing))
        if result is None:
            detail = "not evaluated: no pinned guide bundle"
            roi = None
        else:
            detail = (
                f"guide IoU={result['direct_vs_pinned_guide']['silhouette_iou']:.3f} "
                f"corr={result['direct_vs_pinned_guide']['signed_motion_correlation']:.3f}"
            )
            roi = phase_rois[swing]
        swing_panels.append(
            _evidence_panel(frames[swing], masks[swing], f"swing {swing}", detail, roi=roi)
        )
    swing_panels.append(np.zeros_like(swing_panels[0]))
    evidence = np.vstack((recovery_row, np.hstack(swing_panels)))
    ok, encoded = cv2.imencode(".png", evidence)
    if not ok:
        raise ContractError("Cannot encode object-region evidence PNG")
    return evaluation, encoded.tobytes()


def _evidence_panel(
    frame: np.ndarray,
    mask: np.ndarray,
    title: str,
    detail: str,
    *,
    roi: np.ndarray | None = None,
) -> np.ndarray:
    image = frame.copy()
    contours, _ = cv2.findContours(
        mask.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    cv2.drawContours(image, contours, -1, (40, 220, 40), 2)
    if roi is not None:
        roi_contours, _ = cv2.findContours(
            roi.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
        )
        cv2.drawContours(image, roi_contours, -1, (255, 0, 255), 2)
    cv2.rectangle(image, (0, 0), (WIDTH - 1, 58), (18, 18, 18), -1)
    cv2.putText(
        image, title, (10, 23), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (255, 255, 255), 1, cv2.LINE_AA
    )
    cv2.putText(
        image, detail, (10, 48), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (230, 230, 230), 1, cv2.LINE_AA
    )
    return cv2.resize(image, (384, 224), interpolation=cv2.INTER_AREA)


def run_object_region_video_gate(
    *,
    candidate: str | Path,
    candidate_sha256: str,
    candidate_bytes: int,
    endpoint_guide: str | Path,
    endpoint_guide_sha256: str,
    endpoint_guide_bytes: int,
    output_dir: str | Path,
    guide_bundle: str | Path | None = None,
    guide_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    candidate_pin = _required_sha256(candidate_sha256, "candidate_sha256")
    candidate_size = _required_bytes(candidate_bytes, "candidate_bytes")
    endpoint_pin = _required_sha256(endpoint_guide_sha256, "endpoint_guide_sha256")
    endpoint_size = _required_bytes(endpoint_guide_bytes, "endpoint_guide_bytes")
    if (guide_bundle is None) != (guide_manifest_sha256 is None):
        raise ContractError(
            "guide_bundle and guide_manifest_sha256 must be provided together"
        )
    manifest_pin = (
        None
        if guide_manifest_sha256 is None
        else _required_sha256(guide_manifest_sha256, "guide_manifest_sha256")
    )
    output = Path(output_dir).resolve()
    if output.exists():
        raise ContractError(f"output_dir must be a new non-existing directory: {output}")
    candidate_path = Path(candidate).resolve()
    if candidate_path.is_dir() and _is_within(output, candidate_path):
        raise ContractError("output_dir must not be inside the candidate frame directory")
    bundle_path = None if guide_bundle is None else Path(guide_bundle).resolve()
    if bundle_path is not None and _is_within(output, bundle_path):
        raise ContractError("output_dir must not be inside the guide bundle")

    reader = _ReadOnceCache()
    frames, candidate_provenance = _load_candidate_frames(
        candidate_path,
        expected_sha256=candidate_pin,
        expected_bytes=candidate_size,
        reader=reader,
    )
    endpoint_path, endpoint_payload = reader.read(Path(endpoint_guide), field="endpoint guide")
    _verify_pin(
        endpoint_payload,
        expected_sha256=endpoint_pin,
        expected_bytes=endpoint_size,
        field="endpoint guide",
    )
    endpoint = _decode_png(endpoint_payload, field="endpoint guide")
    guide_images = None
    guide_provenance = None
    if bundle_path is not None and manifest_pin is not None:
        guide_images, guide_provenance = _load_guide_bundle(
            bundle_path,
            manifest_sha256=manifest_pin,
            endpoint_sha256=endpoint_pin,
            endpoint_bytes=endpoint_size,
            reader=reader,
        )
    evaluation, evidence = _evaluate(
        frames,
        endpoint,
        guide_images,
        codec_tolerant=candidate_provenance["kind"] == "mp4",
    )
    report = {
        "schema": SCHEMA,
        "inputs": {
            "candidate": candidate_provenance,
            "endpoint_guide": {
                "path": str(endpoint_path),
                "sha256": endpoint_pin,
                "bytes": endpoint_size,
            },
            "guide_bundle": guide_provenance,
        },
        "contract": {
            "width": WIDTH,
            "height": HEIGHT,
            "frame_count": FRAME_COUNT,
            "loop": True,
            "key_frames": list(KEY_FRAMES),
            "recovery_frames": list(RECOVERY_FRAMES),
            "swing_frames": list(SWING_NEIGHBORS),
            "foreground_residual_threshold_rgb_l2": 5.0,
            "mp4_codec_reference_residual_floor_rgb_l2": 1.5,
            "mp4_codec_reference_background_margin_rgb_l2": 0.5,
            "mp4_codec_boundary_snap_radius_px": 2,
            "endpoint_support_dilation_radius_px": 45,
            "recovery_thresholds": RECOVERY_THRESHOLDS,
            "loop_endpoint_thresholds": ENDPOINT_THRESHOLDS,
            "distal_phase_thresholds": DISTAL_PHASE_THRESHOLDS,
            "swing_thresholds": SWING_THRESHOLDS,
        },
        **evaluation,
    }
    output.mkdir(parents=True, exist_ok=False)
    (output / "object_region_video_gate.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (output / "object_region_video_gate.png").write_bytes(evidence)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed horse object-region pre-fit QA for a fixed 49-frame LTX loop."
    )
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--candidate-bytes", required=True, type=int)
    parser.add_argument("--endpoint-guide", required=True)
    parser.add_argument("--endpoint-guide-sha256", required=True)
    parser.add_argument("--endpoint-guide-bytes", required=True, type=int)
    parser.add_argument("--guide-bundle")
    parser.add_argument("--guide-manifest-sha256")
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = run_object_region_video_gate(
            candidate=args.candidate,
            candidate_sha256=args.candidate_sha256,
            candidate_bytes=args.candidate_bytes,
            endpoint_guide=args.endpoint_guide,
            endpoint_guide_sha256=args.endpoint_guide_sha256,
            endpoint_guide_bytes=args.endpoint_guide_bytes,
            guide_bundle=args.guide_bundle,
            guide_manifest_sha256=args.guide_manifest_sha256,
            output_dir=args.output_dir,
        )
    except ContractError as exc:
        print(f"OBJECT_REGION_GATE_CONTRACT_ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"verdict": report["verdict"], "output_dir": args.output_dir}))
    return 0 if report["approved_for_fitting"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
