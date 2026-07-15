from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import math
import os
import re
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .comfy import (
    ComfyAnimationClient,
    ComfyOutputFile,
    ComfySubmission,
    ComfyWorker,
    apply_workflow_bindings,
    deterministic_prompt_id,
    worker_from_environment,
)
from .specs import WorkflowProfile, load_animation_fitting_specs
from .storage import FfmpegFrameExtractor, ImmutableArtifactStore, StoredArtifact, WorkerBusyError


EXPERIMENT_SCHEMA = "autorig.animation-fitting-experiment.v1"
EXPECTED_EXPERIMENT_ID = "horse_walk_prompt_v2_semantic_reference_guide_080_v1"
V5_EXPERIMENT_ID = (
    "horse_walk_prompt_v5_semantic_chronological_av_"
    "seed_3794990487858656905_guide_065_v1"
)
V6_EXPERIMENT_ID = (
    "horse_walk_prompt_v6_semantic_chronological_av_"
    "seed_3794990487858656905_guide_075_v1"
)
V7_EXPERIMENT_ID = (
    "horse_walk_prompt_v7_semantic_chronological_av_"
    "seed_4891025524393280044_guide_065_v1"
)
V8_EXPERIMENT_ID = (
    "horse_walk_prompt_v8_rgb_native_768x448_"
    "seed_4373011867009528156_guide_080_v1"
)
V9_EXPERIMENT_IDS = frozenset({
    "horse_walk_prompt_v9_rgb_four_beat_seed_6550110377254033429_guide_080_v1",
    "horse_walk_prompt_v9_rgb_four_beat_seed_1448959135068762145_guide_080_v1",
    "horse_walk_prompt_v9_rgb_four_beat_seed_6552386848790876755_guide_080_v1",
})
V10_EXPERIMENT_ID = (
    "horse_walk_v10_browser_rgb_swing_guides_"
    "seed_6550110377254033429_v1"
)
V11_EXPERIMENT_ID = (
    "horse_walk_v11_browser_static_scene_guides_"
    "seed_6550110377254033429_v1"
)
V12_EXPERIMENT_ID = (
    "horse_walk_v12_browser_recovery_guides_"
    "seed_6550110377254033429_v1"
)
V12_EXPERIMENT_SPEC_SHA256 = (
    "22ee5269f14e382fbd214f23ede44130bd9409db471c97087564bb82ef7fd1e1"
)
SUPPORTED_EXPERIMENT_IDS = frozenset({
    EXPECTED_EXPERIMENT_ID,
    "horse_walk_prompt_v3_semantic_staggered_beats_guide_065_v1",
    "horse_walk_prompt_v4_semantic_seed_7721404986102443281_guide_055_v1",
    V5_EXPERIMENT_ID,
    V6_EXPERIMENT_ID,
    V7_EXPERIMENT_ID,
    V8_EXPERIMENT_ID,
    V10_EXPERIMENT_ID,
    V11_EXPERIMENT_ID,
    V12_EXPERIMENT_ID,
}) | V9_EXPERIMENT_IDS
RESULT_SCHEMA = "autorig.animation-fitting-controlled-result.v1"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
V10_GUIDE_FRAME_INDICES = (0, 6, 18, 30, 42, 48)
V10_INTERMEDIATE_GUIDE_FRAME_INDICES = (6, 18, 30, 42)
V12_GUIDE_FRAME_INDICES = (0, 6, 12, 18, 24, 30, 36, 42, 48)
V12_SWING_GUIDE_FRAME_INDICES = (6, 18, 30, 42)
V12_RECOVERY_GUIDE_FRAME_INDICES = (12, 24, 36)
V12_GUIDE_STRENGTHS = (0.8, 0.7, 0.85, 0.7, 0.85, 0.7, 0.85, 0.7, 0.8)
V12_GUIDE_CONTRACT = "browser_rendered_recovery_static_scene_rgb_keyframes_v1"
V12_SCENE_CONTRACT = "v12_unified_browser_recovery_guides_v1"
V10_GUIDE_RESOLUTION = (768, 448)
V12_GUIDE_BUNDLE_ID = "horse-walk-v12-browser-recovery-guides-f2"
V12_GUIDE_MANIFEST_SHA256 = (
    "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451"
)
V12_ENDPOINT_GUIDE_SHA256 = (
    "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3"
)
V12_GUIDE_CLI_SHA256 = (
    "13e1da43f47292be01e99bb32c63dd0d8ca46c88149aa58b64705503a361425d"
)
V11_GUIDE_BUNDLE_ID = "horse-walk-v11-browser-static-scene-guides-f2"
V11_GUIDE_MANIFEST_SHA256 = (
    "9290e2c5c95ab0a24175f1ba873f4af6f221ce963a315e933bcc97aa540ec173"
)
V11_ENDPOINT_GUIDE_SHA256 = (
    "520d0ee816de4557ab9e3f38e19b2b44be900961b62a6f05779b7b09a96474bf"
)
V11_STATIC_SCENE_QA_SUMMARY = {
    "schema": "autorig-browser-static-scene-qa.v1",
    "status": "PASS",
    "decoded_rgb_statistics_bool": True,
    "endpoint_byte_identical_bool": True,
    "border_width_int": 32,
    "background_sample_pixels_int": 73728,
    "maximum_background_channel_delta_int": 0,
    "background_mean_luma_range_float": 0,
    "maximum_background_mean_luma_range_float": 0,
    "full_frame_mean_luma_range_float": 0.044062841506246286,
    "maximum_full_frame_mean_luma_range_float": 0.5,
    "near_black_threshold_int": 64,
    "maximum_near_black_pixel_fraction_float": 0,
    "allowed_near_black_pixel_fraction_float": 0.001,
}
V11_STATIC_SCENE_RENDERER_SETTINGS = {
    "contract": "v11_unified_browser_static_scene_v1",
    "cameraSource": "immutable_fitting_bundle",
    "clearColorHex": 7437190,
    "backgroundHex": 7437190,
    "outputColorSpace": "SRGBColorSpace",
    "toneMapping": "ACESFilmicToneMapping",
    "toneMappingExposure": 1.1,
    "shadowsEnabled": False,
    "hemisphere": {
        "skyHex": 15331839,
        "groundHex": 4146768,
        "intensity": 2.1,
    },
    "key": {
        "colorHex": 16777215,
        "intensity": 3.5,
        "position": [4.5, -5.5, 8.5],
    },
    "ground": {
        "colorHex": 12108748,
        "roughness": 0.92,
        "metalness": 0,
        "size": 50,
    },
}
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class ControlledExperimentError(RuntimeError):
    """Raised when a controlled generation would violate its immutable contract."""


@dataclass(frozen=True)
class ControlledGuideFrame:
    frame_index: int
    image: Path
    sha256: str
    size_bytes: int
    strength: float


@dataclass(frozen=True)
class BrowserRecoveryGuidePins:
    """Code-owned immutable pins required before a recovery bundle can load.

    Accepting these values from the experiment JSON would make an arbitrary
    repin launchable, so the allowlisted v12 loader receives only this
    code-owned instance.
    """

    bundle_id: str
    manifest_sha256: str
    endpoint_sha256: str


V12_GUIDE_PINS = BrowserRecoveryGuidePins(
    bundle_id=V12_GUIDE_BUNDLE_ID,
    manifest_sha256=V12_GUIDE_MANIFEST_SHA256,
    endpoint_sha256=V12_ENDPOINT_GUIDE_SHA256,
)


@dataclass(frozen=True)
class ControlledExperimentPlan:
    experiment_id: str
    experiment_path: Path
    experiment_sha256: str
    reference_bundle: Path
    reference_image: Path
    reference_sha256: str
    positive_prompt: str
    negative_prompt: str
    frame_count: int
    input_fps: int
    output_fps: int
    seed: int
    workflow_name: str
    workflow_fingerprint: str
    start_guide_strength: float
    end_guide_strength: float
    artifact_root: Path
    latent_width: Optional[int] = None
    latent_height: Optional[int] = None
    resize_longer: Optional[int] = None
    guide_bundle: Optional[Path] = None
    guide_manifest_sha256: Optional[str] = None
    guide_frames: Sequence[ControlledGuideFrame] = ()


@dataclass(frozen=True)
class ControlledExperimentResult:
    job_id: str
    prompt_id: str
    raw_video: StoredArtifact
    frames: Sequence[StoredArtifact]
    resumed_existing_result: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": RESULT_SCHEMA,
            "job_id_string": self.job_id,
            "prompt_id_string": self.prompt_id,
            "raw_video_path_string": str(self.raw_video.path),
            "raw_video_sha256_string": self.raw_video.sha256,
            "raw_video_bytes_int": self.raw_video.size_bytes,
            "frame_count_int": len(self.frames),
            "frame_paths_array": [str(frame.path) for frame in self.frames],
            "frame_sha256_array": [frame.sha256 for frame in self.frames],
            "resumed_existing_result_bool": self.resumed_existing_result,
            "approval_state_string": "generated_not_approved",
            "send_to_skeletal_fitting_bool": False,
        }


def _read_bytes_and_sha256(path: Path) -> tuple[bytes, str]:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ControlledExperimentError(f"Cannot read JSON contract {path}: {exc}") from exc
    return data, hashlib.sha256(data).hexdigest()


def _parse_json_bytes(path: Path, data: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(data.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlledExperimentError(f"Cannot read JSON contract {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ControlledExperimentError(f"JSON contract must be an object: {path}")
    return parsed


def _read_json_and_sha256(path: Path) -> tuple[dict[str, Any], str]:
    data, digest = _read_bytes_and_sha256(path)
    return _parse_json_bytes(path, data), digest


def _read_json(path: Path) -> dict[str, Any]:
    parsed, _digest = _read_json_and_sha256(path)
    return parsed


def _read_pinned_json(
    path: Path, expected: object, label: str
) -> tuple[dict[str, Any], str]:
    expected_digest = _require_sha(expected, label)
    data, actual_digest = _read_bytes_and_sha256(path)
    if actual_digest != expected_digest:
        raise ControlledExperimentError(
            f"{label} mismatch for {path}: expected {expected_digest}, got {actual_digest}"
        )
    return _parse_json_bytes(path, data), actual_digest


def _sha256(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise ControlledExperimentError(f"Cannot hash {path}: {exc}") from exc


def _require_sha(value: object, label: str) -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise ControlledExperimentError(f"{label} must be a lowercase SHA-256 digest")
    return digest


def _require_exact_sha(path: Path, expected: object, label: str) -> str:
    digest = _require_sha(expected, label)
    actual = _sha256(path)
    if actual != digest:
        raise ControlledExperimentError(
            f"{label} mismatch for {path}: expected {digest}, got {actual}"
        )
    return actual


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ControlledExperimentError(f"{label} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be a positive integer") from exc
    if result <= 0:
        raise ControlledExperimentError(f"{label} must be a positive integer")
    return result


def _guide_strength(value: object, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be numeric") from exc
    if not 0 <= result <= 1:
        raise ControlledExperimentError(f"{label} must be in [0, 1]")
    return result


def _verify_reference_manifest(
    bundle: Path, reference: Mapping[str, Any]
) -> dict[str, Any]:
    manifest_path = bundle / str(reference.get("immutable_manifest_filename_string") or "")
    manifest, _manifest_sha256 = _read_pinned_json(
        manifest_path,
        reference.get("immutable_manifest_sha256_string"),
        "reference immutable manifest SHA-256",
    )
    if manifest.get("schema") != "autorig-ltx-semantic-reference-output.v1":
        raise ControlledExperimentError("reference immutable manifest schema is invalid")
    rows = manifest.get("files")
    if not isinstance(rows, list) or int(manifest.get("file_count", -1)) != len(rows) or len(rows) != 2:
        raise ControlledExperimentError("reference immutable manifest must contain exactly two files")
    expected_names = {
        str(reference.get("reference_png_filename_string") or ""),
        str(reference.get("derivation_manifest_filename_string") or ""),
    }
    if {row.get("filename") for row in rows if isinstance(row, dict)} != expected_names:
        raise ControlledExperimentError("reference immutable manifest file inventory is not exact")
    derivation_filename = str(
        reference.get("derivation_manifest_filename_string") or ""
    )
    derivation: Optional[dict[str, Any]] = None
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ControlledExperimentError(f"reference manifest row {index} is invalid")
        filename = str(row.get("filename") or "")
        if not filename or Path(filename).name != filename:
            raise ControlledExperimentError("reference manifest filenames must be simple names")
        path = bundle / filename
        if filename == derivation_filename:
            derivation, digest = _read_pinned_json(
                path,
                row.get("sha256"),
                f"reference file {filename} SHA-256",
            )
        else:
            digest = _require_exact_sha(
                path, row.get("sha256"), f"reference file {filename} SHA-256"
            )
        if path.stat().st_size != _positive_int(row.get("bytes"), f"reference file {filename} bytes"):
            raise ControlledExperimentError(f"reference file {filename} byte size mismatch")
        if filename == reference.get("reference_png_filename_string") and digest != reference.get(
            "reference_png_sha256_string"
        ):
            raise ControlledExperimentError("reference PNG disagrees with experiment contract")
        if filename == reference.get("derivation_manifest_filename_string") and digest != reference.get(
            "derivation_manifest_sha256_string"
        ):
            raise ControlledExperimentError("reference derivation manifest disagrees with experiment contract")
    if derivation is None:
        raise ControlledExperimentError("reference derivation manifest is missing")
    return derivation


def _verify_actionless_reference_manifest(
    bundle: Path, reference: Mapping[str, Any]
) -> tuple[int, int]:
    manifest_filename = str(reference.get("immutable_manifest_filename_string") or "")
    if not manifest_filename or Path(manifest_filename).name != manifest_filename:
        raise ControlledExperimentError("actionless immutable manifest filename is invalid")
    manifest_path = bundle / manifest_filename
    manifest, _manifest_sha256 = _read_pinned_json(
        manifest_path,
        reference.get("immutable_manifest_sha256_string"),
        "actionless immutable manifest SHA-256",
    )
    if manifest.get("schema") != "autorig-fitting-immutable-copy.v1":
        raise ControlledExperimentError("actionless immutable manifest schema is invalid")
    rows = manifest.get("files")
    expected_count = _positive_int(
        manifest.get("bundle_file_count"), "actionless bundle_file_count"
    )
    if not isinstance(rows, list) or len(rows) != expected_count:
        raise ControlledExperimentError("actionless immutable manifest file inventory is incomplete")
    bundle_manifest_filename = str(
        reference.get("bundle_manifest_filename_string") or ""
    )
    bundle_manifest: Optional[dict[str, Any]] = None
    inventory: dict[str, Mapping[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ControlledExperimentError(f"actionless manifest row {index} is invalid")
        filename = str(row.get("filename") or "")
        if not filename or Path(filename).name != filename or filename in inventory:
            raise ControlledExperimentError("actionless manifest filenames must be unique simple names")
        path = bundle / filename
        if filename == bundle_manifest_filename:
            bundle_manifest, _bundle_manifest_sha256 = _read_pinned_json(
                path,
                row.get("sha256"),
                f"actionless file {filename} SHA-256",
            )
        else:
            _require_exact_sha(
                path, row.get("sha256"), f"actionless file {filename} SHA-256"
            )
        if path.stat().st_size != _positive_int(row.get("bytes"), f"actionless file {filename} bytes"):
            raise ControlledExperimentError(f"actionless file {filename} byte size mismatch")
        inventory[filename] = row

    image_filename = str(reference.get("reference_png_filename_string") or "")
    image_row = inventory.get(image_filename)
    if not image_row or image_row.get("sha256") != reference.get("reference_png_sha256_string"):
        raise ControlledExperimentError("actionless RGB reference disagrees with immutable inventory")
    bundle_manifest_row = inventory.get(bundle_manifest_filename)
    if (
        not bundle_manifest_row
        or bundle_manifest_row.get("sha256")
        != reference.get("bundle_manifest_sha256_string")
    ):
        raise ControlledExperimentError("actionless fitting bundle disagrees with immutable inventory")
    if bundle_manifest is None:
        raise ControlledExperimentError("actionless fitting bundle manifest is missing")
    if bundle_manifest.get("schema") != "autorig-actionless-fitting-bundle.v1":
        raise ControlledExperimentError("actionless fitting bundle schema is invalid")
    actionless = bundle_manifest.get("actionless")
    if not isinstance(actionless, dict) or actionless.get("actionless") is not True:
        raise ControlledExperimentError("reference bundle is not actionless")
    rgb = (bundle_manifest.get("artifacts") or {}).get("rgb")
    if not isinstance(rgb, dict) or (
        rgb.get("filename") != image_filename
        or rgb.get("sha256") != reference.get("reference_png_sha256_string")
    ):
        raise ControlledExperimentError("fitting bundle RGB artifact disagrees with experiment")
    resolution = (bundle_manifest.get("camera") or {}).get("resolution")
    if (
        not isinstance(resolution, list)
        or len(resolution) != 2
        or any(isinstance(value, bool) or not isinstance(value, int) for value in resolution)
    ):
        raise ControlledExperimentError("actionless camera resolution is invalid")
    return _positive_int(resolution[0], "actionless camera width"), _positive_int(
        resolution[1], "actionless camera height"
    )


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ControlledExperimentError(f"{label} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be a non-negative integer") from exc
    if result < 0:
        raise ControlledExperimentError(f"{label} must be a non-negative integer")
    return result


def _decode_png_dimensions(path: Path, label: str) -> tuple[int, int]:
    """Validate a non-interlaced PNG through zlib scanline decoding.

    Browser canvas exports used by the v10 contract are ordinary non-interlaced
    PNGs. Validating the signature/IHDR alone would still accept truncated or
    forged files, so this parser also checks every chunk CRC, decompresses IDAT,
    and reconstructs every filtered scanline without adding a runtime image
    dependency to the backend worker.
    """

    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ControlledExperimentError(f"Cannot read {label} {path}: {exc}") from exc
    if not payload.startswith(PNG_SIGNATURE):
        raise ControlledExperimentError(f"{label} is not a valid decodable PNG: bad signature")

    offset = len(PNG_SIGNATURE)
    width = height = bit_depth = color_type = None
    idat_parts: list[bytes] = []
    saw_iend = False
    while offset < len(payload):
        if offset + 12 > len(payload):
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: truncated chunk header"
            )
        length = struct.unpack(">I", payload[offset : offset + 4])[0]
        chunk_type = payload[offset + 4 : offset + 8]
        chunk_end = offset + 12 + length
        if chunk_end > len(payload):
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: truncated {chunk_type!r} chunk"
            )
        chunk_data = payload[offset + 8 : offset + 8 + length]
        expected_crc = struct.unpack(">I", payload[offset + 8 + length : chunk_end])[0]
        actual_crc = zlib.crc32(chunk_type)
        actual_crc = zlib.crc32(chunk_data, actual_crc) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: {chunk_type!r} CRC mismatch"
            )

        if offset == len(PNG_SIGNATURE) and chunk_type != b"IHDR":
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: IHDR must be first"
            )
        if chunk_type == b"IHDR":
            if width is not None or length != 13:
                raise ControlledExperimentError(
                    f"{label} is not a valid decodable PNG: invalid IHDR"
                )
            width, height, bit_depth, color_type, compression, filtering, interlace = (
                struct.unpack(">IIBBBBB", chunk_data)
            )
            if (
                width <= 0
                or height <= 0
                or bit_depth != 8
                or color_type not in (2, 6)
                or compression != 0
                or filtering != 0
                or interlace != 0
            ):
                raise ControlledExperimentError(
                    f"{label} is not a valid decodable PNG: unsupported IHDR"
                )
        elif chunk_type == b"IDAT":
            if width is None:
                raise ControlledExperimentError(
                    f"{label} is not a valid decodable PNG: IDAT before IHDR"
                )
            idat_parts.append(chunk_data)
        elif chunk_type == b"IEND":
            if length != 0:
                raise ControlledExperimentError(
                    f"{label} is not a valid decodable PNG: invalid IEND"
                )
            saw_iend = True
            offset = chunk_end
            break
        elif chunk_type[:1].isupper() and chunk_type != b"PLTE":
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: unknown critical chunk {chunk_type!r}"
            )
        offset = chunk_end

    if not saw_iend or offset != len(payload) or width is None or not idat_parts:
        raise ControlledExperimentError(
            f"{label} is not a valid decodable PNG: incomplete chunk stream"
        )

    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[int(color_type)]
    bits_per_row = int(width) * channels * int(bit_depth)
    row_bytes = (bits_per_row + 7) // 8
    expected_raw_bytes = (row_bytes + 1) * int(height)
    decoder = zlib.decompressobj()
    try:
        raw = decoder.decompress(b"".join(idat_parts), expected_raw_bytes + 1)
        if len(raw) > expected_raw_bytes or decoder.unconsumed_tail:
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: decoded scanline size mismatch"
            )
        raw += decoder.flush(expected_raw_bytes + 1 - len(raw))
    except zlib.error as exc:
        raise ControlledExperimentError(
            f"{label} is not a valid decodable PNG: IDAT decode failed"
        ) from exc
    if (
        len(raw) != expected_raw_bytes
        or not decoder.eof
        or decoder.unconsumed_tail
        or decoder.unused_data
    ):
        raise ControlledExperimentError(
            f"{label} is not a valid decodable PNG: decoded scanline size mismatch"
        )

    bytes_per_pixel = max(1, (channels * int(bit_depth) + 7) // 8)
    prior = bytearray(row_bytes)
    cursor = 0
    for _row_index in range(int(height)):
        filter_type = raw[cursor]
        cursor += 1
        if filter_type > 4:
            raise ControlledExperimentError(
                f"{label} is not a valid decodable PNG: invalid scanline filter"
            )
        encoded = raw[cursor : cursor + row_bytes]
        cursor += row_bytes
        decoded = bytearray(row_bytes)
        for index, value in enumerate(encoded):
            left = decoded[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
            above = prior[index]
            upper_left = prior[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
            if filter_type == 0:
                predictor = 0
            elif filter_type == 1:
                predictor = left
            elif filter_type == 2:
                predictor = above
            elif filter_type == 3:
                predictor = (left + above) // 2
            else:
                candidate = left + above - upper_left
                left_distance = abs(candidate - left)
                above_distance = abs(candidate - above)
                upper_left_distance = abs(candidate - upper_left)
                predictor = (
                    left
                    if left_distance <= above_distance and left_distance <= upper_left_distance
                    else above
                    if above_distance <= upper_left_distance
                    else upper_left
                )
            decoded[index] = (value + predictor) & 0xFF
        prior = decoded
    return int(width), int(height)


def _verify_planned_guide_frame(
    frame: ControlledGuideFrame, *, decode_png: bool
) -> None:
    _require_exact_sha(
        frame.image,
        frame.sha256,
        f"v10 guide frame {frame.frame_index} SHA-256",
    )
    try:
        actual_bytes = frame.image.stat().st_size
    except OSError as exc:
        raise ControlledExperimentError(
            f"Cannot stat v10 guide frame {frame.frame_index} {frame.image}: {exc}"
        ) from exc
    if actual_bytes != frame.size_bytes:
        raise ControlledExperimentError(
            f"v10 guide frame {frame.frame_index} byte size mismatch: "
            f"expected {frame.size_bytes}, got {actual_bytes}"
        )
    if decode_png:
        dimensions = _decode_png_dimensions(
            frame.image, f"v10 guide frame {frame.frame_index}"
        )
        if dimensions != V10_GUIDE_RESOLUTION:
            raise ControlledExperimentError(
                f"v10 guide frame {frame.frame_index} must be exactly "
                f"{V10_GUIDE_RESOLUTION[0]}x{V10_GUIDE_RESOLUTION[1]}, "
                f"got {dimensions[0]}x{dimensions[1]}"
            )


def _load_browser_guide_sequence(
    experiment: Mapping[str, Any],
    *,
    guide_bundle: Optional[Path],
    reference_sha256: str,
    frame_count: int,
    start_strength: float,
    end_strength: float,
) -> tuple[Path, str, tuple[ControlledGuideFrame, ...]]:
    sequence = experiment.get("guide_sequence_object")
    if not isinstance(sequence, dict) or sequence.get("ready_bool") is not True:
        raise ControlledExperimentError(
            "v10 requires a completed pinned browser-rendered guide bundle before launch"
        )
    if sequence.get("guide_contract_string") != "browser_rendered_rgb_keyframes_v1":
        raise ControlledExperimentError("v10 browser guide contract is invalid")
    bundle_id = str(sequence.get("bundle_id_string") or "")
    bundle = Path(guide_bundle).resolve() if guide_bundle is not None else None
    if bundle is None or not bundle.is_dir() or bundle.name != bundle_id:
        raise ControlledExperimentError(
            f"v10 --guide-bundle must be the existing {bundle_id!r} directory"
        )

    manifest_filename = str(sequence.get("immutable_manifest_filename_string") or "")
    if not manifest_filename or Path(manifest_filename).name != manifest_filename:
        raise ControlledExperimentError("v10 guide manifest filename is invalid")
    manifest_path = bundle / manifest_filename
    manifest, manifest_sha256 = _read_pinned_json(
        manifest_path,
        sequence.get("immutable_manifest_sha256_string"),
        "v10 guide manifest SHA-256",
    )
    if manifest.get("schema") != "autorig-browser-ltx-guide-bundle.v1":
        raise ControlledExperimentError("v10 guide manifest schema is invalid")
    if manifest.get("source_reference_sha256_string") != reference_sha256:
        raise ControlledExperimentError("v10 guide bundle does not pin the actionless reference")
    if _positive_int(manifest.get("cycle_frame_count_int"), "v10 cycle frame count") != frame_count:
        raise ControlledExperimentError("v10 guide bundle frame count disagrees with experiment")
    renderer = manifest.get("renderer_object")
    if not isinstance(renderer, dict) or (
        renderer.get("renderer_string") != "browser_threejs"
        or renderer.get("blender_used_bool") is not False
    ):
        raise ControlledExperimentError(
            "v10 guide bundle must be rendered in browser Three.js without Blender"
        )

    contract_rows = sequence.get("frames_array")
    manifest_rows = manifest.get("frames_array")
    if not isinstance(contract_rows, list) or not isinstance(manifest_rows, list):
        raise ControlledExperimentError("v10 guide frame inventories are required")
    if (
        len(contract_rows) != len(V10_GUIDE_FRAME_INDICES)
        or len(manifest_rows) != len(V10_GUIDE_FRAME_INDICES)
        or _positive_int(manifest.get("guide_count_int"), "v10 guide count")
        != len(V10_GUIDE_FRAME_INDICES)
    ):
        raise ControlledExperimentError("v10 guide bundle must contain exactly six frames")

    manifest_by_index: dict[int, Mapping[str, Any]] = {}
    for row in manifest_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v10 guide manifest frame row is invalid")
        index = _nonnegative_int(row.get("frame_index_int"), "v10 manifest frame index")
        if index in manifest_by_index:
            raise ControlledExperimentError("v10 guide manifest frame indices must be unique")
        manifest_by_index[index] = row

    frames: list[ControlledGuideFrame] = []
    seen: set[int] = set()
    for row in contract_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v10 guide contract frame row is invalid")
        index = _nonnegative_int(row.get("frame_index_int"), "v10 contract frame index")
        if index in seen:
            raise ControlledExperimentError("v10 guide contract frame indices must be unique")
        seen.add(index)
        filename = str(row.get("filename_string") or "")
        if (
            not filename
            or Path(filename).name != filename
            or Path(filename).suffix.lower() != ".png"
        ):
            raise ControlledExperimentError("v10 guide frames must be simple PNG filenames")
        digest = _require_sha(row.get("sha256_string"), f"v10 guide frame {index} SHA-256")
        expected_bytes = _positive_int(row.get("bytes_int"), f"v10 guide frame {index} bytes")
        strength = _guide_strength(row.get("strength_float"), f"v10 guide frame {index} strength")
        manifest_row = manifest_by_index.get(index)
        if not manifest_row or (
            manifest_row.get("filename_string") != filename
            or manifest_row.get("sha256_string") != digest
            or manifest_row.get("bytes_int") != expected_bytes
        ):
            raise ControlledExperimentError(
                f"v10 guide frame {index} disagrees with immutable manifest"
            )
        frame = ControlledGuideFrame(
            frame_index=index,
            image=bundle / filename,
            sha256=digest,
            size_bytes=expected_bytes,
            strength=strength,
        )
        _verify_planned_guide_frame(frame, decode_png=True)
        frames.append(frame)

    if tuple(sorted(seen)) != V10_GUIDE_FRAME_INDICES:
        raise ControlledExperimentError("v10 guide frames must be exactly 0, 6, 18, 30, 42, 48")
    by_index = {frame.frame_index: frame for frame in frames}
    if by_index[0].sha256 != reference_sha256:
        raise ControlledExperimentError(
            "v10 frame 0 guide PNG must be byte-identical to immutable reference_rgb"
        )
    if by_index[0].sha256 != by_index[48].sha256:
        raise ControlledExperimentError("v10 frame 0 and frame 48 guide PNGs must be byte-identical")
    if start_strength != 0.8 or end_strength != 0.8:
        raise ControlledExperimentError("v10 experiment endpoint guide strengths must be exactly 0.8")
    if by_index[0].strength != 0.8 or by_index[48].strength != 0.8:
        raise ControlledExperimentError(
            "v10 endpoint guide strengths must be exactly 0.8"
        )
    intermediate_frames = [by_index[index] for index in V10_INTERMEDIATE_GUIDE_FRAME_INDICES]
    if any(frame.strength != 0.7 for frame in intermediate_frames):
        raise ControlledExperimentError("v10 intermediate guide strengths must be exactly 0.7")
    intermediate_hashes = [frame.sha256 for frame in intermediate_frames]
    if (
        len(set(intermediate_hashes)) != len(intermediate_hashes)
        or by_index[0].sha256 in intermediate_hashes
    ):
        raise ControlledExperimentError(
            "v10 intermediate guide hashes must be pairwise distinct and differ from endpoint"
        )
    return bundle, manifest_sha256, tuple(sorted(frames, key=lambda frame: frame.frame_index))


def _finite_float(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise ControlledExperimentError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ControlledExperimentError(f"{label} must be a finite number") from exc
    if not math.isfinite(result):
        raise ControlledExperimentError(f"{label} must be a finite number")
    return result


def _verify_v11_planned_guide_frame(
    frame: ControlledGuideFrame, *, decode_png: bool
) -> None:
    _require_exact_sha(
        frame.image,
        frame.sha256,
        f"v11 guide frame {frame.frame_index} SHA-256",
    )
    try:
        actual_bytes = frame.image.stat().st_size
    except OSError as exc:
        raise ControlledExperimentError(
            f"Cannot stat v11 guide frame {frame.frame_index} {frame.image}: {exc}"
        ) from exc
    if actual_bytes != frame.size_bytes:
        raise ControlledExperimentError(
            f"v11 guide frame {frame.frame_index} byte size mismatch: "
            f"expected {frame.size_bytes}, got {actual_bytes}"
        )
    if decode_png:
        dimensions = _decode_png_dimensions(
            frame.image, f"v11 guide frame {frame.frame_index}"
        )
        if dimensions != V10_GUIDE_RESOLUTION:
            raise ControlledExperimentError(
                f"v11 guide frame {frame.frame_index} must be exactly "
                f"{V10_GUIDE_RESOLUTION[0]}x{V10_GUIDE_RESOLUTION[1]}, "
                f"got {dimensions[0]}x{dimensions[1]}"
            )


def _verify_v12_planned_guide_frame(
    frame: ControlledGuideFrame, *, decode_png: bool
) -> None:
    _require_exact_sha(
        frame.image,
        frame.sha256,
        f"v12 guide frame {frame.frame_index} SHA-256",
    )
    try:
        actual_bytes = frame.image.stat().st_size
    except OSError as exc:
        raise ControlledExperimentError(
            f"Cannot stat v12 guide frame {frame.frame_index} {frame.image}: {exc}"
        ) from exc
    if actual_bytes != frame.size_bytes:
        raise ControlledExperimentError(
            f"v12 guide frame {frame.frame_index} byte size mismatch: "
            f"expected {frame.size_bytes}, got {actual_bytes}"
        )
    if decode_png:
        dimensions = _decode_png_dimensions(
            frame.image, f"v12 guide frame {frame.frame_index}"
        )
        if dimensions != V10_GUIDE_RESOLUTION:
            raise ControlledExperimentError(
                f"v12 guide frame {frame.frame_index} must be exactly "
                f"{V10_GUIDE_RESOLUTION[0]}x{V10_GUIDE_RESOLUTION[1]}, "
                f"got {dimensions[0]}x{dimensions[1]}"
            )


def _verify_v11_static_scene_contract(
    experiment: Mapping[str, Any], manifest: Mapping[str, Any]
) -> None:
    contract = experiment.get("static_scene_contract_object")
    if not isinstance(contract, dict):
        raise ControlledExperimentError("v11 static-scene contract is required")
    if contract.get("qa_summary_object") != V11_STATIC_SCENE_QA_SUMMARY:
        raise ControlledExperimentError("v11 pinned static-scene QA summary is invalid")
    if contract.get("renderer_settings_object") != V11_STATIC_SCENE_RENDERER_SETTINGS:
        raise ControlledExperimentError("v11 pinned static-scene renderer settings are invalid")

    renderer = manifest.get("staticSceneRenderer")
    if renderer != V11_STATIC_SCENE_RENDERER_SETTINGS:
        raise ControlledExperimentError(
            "v11 immutable manifest static-scene renderer settings changed"
        )
    qa = manifest.get("staticSceneQa")
    if not isinstance(qa, dict):
        raise ControlledExperimentError("v11 immutable manifest staticSceneQa is required")
    qa_summary = {key: value for key, value in qa.items() if key != "guides_array"}
    if qa_summary != V11_STATIC_SCENE_QA_SUMMARY:
        raise ControlledExperimentError(
            "v11 immutable manifest static-scene QA summary changed"
        )
    if (
        qa.get("schema") != "autorig-browser-static-scene-qa.v1"
        or qa.get("status") != "PASS"
        or qa.get("decoded_rgb_statistics_bool") is not True
        or qa.get("endpoint_byte_identical_bool") is not True
    ):
        raise ControlledExperimentError("v11 static-scene QA did not pass")

    _positive_int(qa.get("border_width_int"), "v11 static-scene border width")
    _positive_int(
        qa.get("background_sample_pixels_int"),
        "v11 static-scene background sample pixels",
    )
    background_channel_delta = _nonnegative_int(
        qa.get("maximum_background_channel_delta_int"),
        "v11 maximum background channel delta",
    )
    background_range = _finite_float(
        qa.get("background_mean_luma_range_float"),
        "v11 background mean luma range",
    )
    background_limit = _finite_float(
        qa.get("maximum_background_mean_luma_range_float"),
        "v11 maximum background mean luma range",
    )
    full_frame_range = _finite_float(
        qa.get("full_frame_mean_luma_range_float"),
        "v11 full-frame mean luma range",
    )
    full_frame_limit = _finite_float(
        qa.get("maximum_full_frame_mean_luma_range_float"),
        "v11 maximum full-frame mean luma range",
    )
    maximum_near_black = _finite_float(
        qa.get("maximum_near_black_pixel_fraction_float"),
        "v11 maximum near-black pixel fraction",
    )
    allowed_near_black = _finite_float(
        qa.get("allowed_near_black_pixel_fraction_float"),
        "v11 allowed near-black pixel fraction",
    )
    if (
        background_channel_delta != 0
        or background_range != 0
        or background_limit != 0
        or full_frame_range < 0
        or full_frame_range > full_frame_limit
        or maximum_near_black < 0
        or maximum_near_black > allowed_near_black
    ):
        raise ControlledExperimentError("v11 static-scene background metrics failed")

    guide_rows = qa.get("guides_array")
    if not isinstance(guide_rows, list) or len(guide_rows) != len(
        V10_GUIDE_FRAME_INDICES
    ):
        raise ControlledExperimentError("v11 static-scene per-guide QA is incomplete")
    if [row.get("frame_index_int") for row in guide_rows if isinstance(row, dict)] != list(
        V10_GUIDE_FRAME_INDICES
    ):
        raise ControlledExperimentError("v11 static-scene per-guide QA order is invalid")

    background_luma: list[float] = []
    full_frame_luma: list[float] = []
    near_black_fractions: list[float] = []
    for row in guide_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v11 static-scene per-guide QA row is invalid")
        background_luma.append(
            _finite_float(
                row.get("background_mean_luma_float"),
                "v11 guide background mean luma",
            )
        )
        full_frame_luma.append(
            _finite_float(
                row.get("full_frame_mean_luma_float"),
                "v11 guide full-frame mean luma",
            )
        )
        near_black_fractions.append(
            _finite_float(
                row.get("near_black_pixel_fraction_float"),
                "v11 guide near-black pixel fraction",
            )
        )
    observed_background_range = max(background_luma) - min(background_luma)
    observed_full_frame_range = max(full_frame_luma) - min(full_frame_luma)
    observed_near_black = max(near_black_fractions)
    if (
        not math.isclose(observed_background_range, background_range, abs_tol=1e-9)
        or not math.isclose(observed_full_frame_range, full_frame_range, abs_tol=1e-9)
        or not math.isclose(observed_near_black, maximum_near_black, abs_tol=1e-12)
        or any(value < 0 or value > allowed_near_black for value in near_black_fractions)
    ):
        raise ControlledExperimentError(
            "v11 static-scene per-guide metrics disagree with the pinned summary"
        )


def _load_browser_static_scene_guide_sequence(
    experiment: Mapping[str, Any],
    *,
    guide_bundle: Optional[Path],
    reference_sha256: str,
    frame_count: int,
    start_strength: float,
    end_strength: float,
) -> tuple[Path, str, tuple[ControlledGuideFrame, ...]]:
    sequence = experiment.get("guide_sequence_object")
    if not isinstance(sequence, dict) or sequence.get("ready_bool") is not True:
        raise ControlledExperimentError(
            "v11 requires a completed pinned browser static-scene guide bundle before launch"
        )
    if (
        sequence.get("guide_contract_string")
        != "browser_rendered_static_scene_rgb_keyframes_v1"
    ):
        raise ControlledExperimentError("v11 browser static-scene guide contract is invalid")
    bundle_id = str(sequence.get("bundle_id_string") or "")
    if bundle_id != V11_GUIDE_BUNDLE_ID:
        raise ControlledExperimentError("v11 immutable guide bundle id is invalid")
    bundle = Path(guide_bundle).resolve() if guide_bundle is not None else None
    if bundle is None or not bundle.is_dir() or bundle.name != bundle_id:
        raise ControlledExperimentError(
            f"v11 --guide-bundle must be the existing {bundle_id!r} directory"
        )

    manifest_filename = str(sequence.get("immutable_manifest_filename_string") or "")
    if not manifest_filename or Path(manifest_filename).name != manifest_filename:
        raise ControlledExperimentError("v11 guide manifest filename is invalid")
    if sequence.get("immutable_manifest_sha256_string") != V11_GUIDE_MANIFEST_SHA256:
        raise ControlledExperimentError("v11 immutable guide manifest pin is invalid")
    manifest_path = bundle / manifest_filename
    manifest, manifest_sha256 = _read_pinned_json(
        manifest_path,
        sequence.get("immutable_manifest_sha256_string"),
        "v11 guide manifest SHA-256",
    )
    if manifest.get("schema") != "autorig-browser-ltx-static-scene-guide-bundle.v1":
        raise ControlledExperimentError("v11 guide manifest schema is invalid")
    if (
        manifest.get("status") != "PASS"
        or manifest.get("approvedForAnimationLibrary") is not False
        or manifest.get("browserOnly") is not True
        or manifest.get("blenderUsed") is not False
    ):
        raise ControlledExperimentError("v11 guide bundle is not browser-only PASS/unapproved")
    if manifest.get("source_reference_sha256_string") != reference_sha256:
        raise ControlledExperimentError("v11 guide bundle does not pin the actionless reference")
    if manifest.get("source_reference_is_guide_bool") is not False:
        raise ControlledExperimentError(
            "v11 source reference must remain provenance-only, not a guide frame"
        )
    endpoint_guide_sha256 = _require_sha(
        sequence.get("endpoint_guide_sha256_string"),
        "v11 endpoint guide SHA-256",
    )
    if endpoint_guide_sha256 != V11_ENDPOINT_GUIDE_SHA256:
        raise ControlledExperimentError("v11 immutable endpoint guide pin is invalid")
    if manifest.get("endpoint_guide_sha256_string") != endpoint_guide_sha256:
        raise ControlledExperimentError(
            "v11 endpoint guide SHA-256 disagrees with immutable manifest"
        )
    if manifest.get("resolution") != list(V10_GUIDE_RESOLUTION):
        raise ControlledExperimentError("v11 guide bundle resolution is invalid")
    if _positive_int(manifest.get("cycle_frame_count_int"), "v11 cycle frame count") != frame_count:
        raise ControlledExperimentError("v11 guide bundle frame count disagrees with experiment")
    renderer = manifest.get("renderer_object")
    if not isinstance(renderer, dict) or (
        renderer.get("renderer_string") != "browser_threejs"
        or renderer.get("blender_used_bool") is not False
        or renderer.get("scene_contract_string")
        != "v11_unified_browser_static_scene_v1"
        or renderer.get("all_guide_frames_browser_rendered_bool") is not True
        or renderer.get("shadows_enabled_bool") is not False
    ):
        raise ControlledExperimentError(
            "v11 guide bundle must use one browser Three.js static-scene renderer without Blender"
        )
    _verify_v11_static_scene_contract(experiment, manifest)

    post_bake = manifest.get("postBakeQa")
    if not isinstance(post_bake, dict) or (
        post_bake.get("status") != "PASS"
        or post_bake.get("hierarchyBakeVerified") is not True
        or _positive_int(post_bake.get("minimumStanceHooves"), "v11 minimum stance hooves")
        < 3
        or _finite_float(
            post_bake.get("endpointMaximumErrorPx"),
            "v11 endpoint maximum error",
        )
        != 0
    ):
        raise ControlledExperimentError("v11 guide post-bake hoof QA did not pass")

    contract_rows = sequence.get("frames_array")
    manifest_rows = manifest.get("frames_array")
    if not isinstance(contract_rows, list) or not isinstance(manifest_rows, list):
        raise ControlledExperimentError("v11 guide frame inventories are required")
    if (
        len(contract_rows) != len(V10_GUIDE_FRAME_INDICES)
        or len(manifest_rows) != len(V10_GUIDE_FRAME_INDICES)
        or _positive_int(manifest.get("guide_count_int"), "v11 guide count")
        != len(V10_GUIDE_FRAME_INDICES)
    ):
        raise ControlledExperimentError("v11 guide bundle must contain exactly six frames")

    manifest_by_index: dict[int, Mapping[str, Any]] = {}
    for row in manifest_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v11 guide manifest frame row is invalid")
        index = _nonnegative_int(row.get("frame_index_int"), "v11 manifest frame index")
        if index in manifest_by_index:
            raise ControlledExperimentError("v11 guide manifest frame indices must be unique")
        manifest_by_index[index] = row

    frames: list[ControlledGuideFrame] = []
    seen: set[int] = set()
    for row in contract_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v11 guide contract frame row is invalid")
        index = _nonnegative_int(row.get("frame_index_int"), "v11 contract frame index")
        if index in seen:
            raise ControlledExperimentError("v11 guide contract frame indices must be unique")
        seen.add(index)
        filename = str(row.get("filename_string") or "")
        if (
            not filename
            or Path(filename).name != filename
            or Path(filename).suffix.lower() != ".png"
        ):
            raise ControlledExperimentError("v11 guide frames must be simple PNG filenames")
        digest = _require_sha(row.get("sha256_string"), f"v11 guide frame {index} SHA-256")
        expected_bytes = _positive_int(row.get("bytes_int"), f"v11 guide frame {index} bytes")
        strength = _guide_strength(row.get("strength_float"), f"v11 guide frame {index} strength")
        manifest_row = manifest_by_index.get(index)
        if not manifest_row or (
            manifest_row.get("filename_string") != filename
            or manifest_row.get("sha256_string") != digest
            or manifest_row.get("bytes_int") != expected_bytes
            or manifest_row.get("strength_float") != strength
        ):
            raise ControlledExperimentError(
                f"v11 guide frame {index} disagrees with immutable manifest"
            )
        frame = ControlledGuideFrame(
            frame_index=index,
            image=bundle / filename,
            sha256=digest,
            size_bytes=expected_bytes,
            strength=strength,
        )
        _verify_v11_planned_guide_frame(frame, decode_png=True)
        frames.append(frame)

    if tuple(sorted(seen)) != V10_GUIDE_FRAME_INDICES:
        raise ControlledExperimentError("v11 guide frames must be exactly 0, 6, 18, 30, 42, 48")
    by_index = {frame.frame_index: frame for frame in frames}
    if by_index[0].sha256 != by_index[48].sha256:
        raise ControlledExperimentError("v11 frame 0 and frame 48 guide PNGs must be byte-identical")
    if by_index[0].sha256 != endpoint_guide_sha256:
        raise ControlledExperimentError("v11 endpoint guide PNG SHA-256 is invalid")
    if by_index[0].sha256 == reference_sha256:
        raise ControlledExperimentError(
            "v11 endpoint guides must explicitly differ from the actionless source reference"
        )
    if start_strength != 0.8 or end_strength != 0.8:
        raise ControlledExperimentError("v11 experiment endpoint guide strengths must be exactly 0.8")
    if by_index[0].strength != 0.8 or by_index[48].strength != 0.8:
        raise ControlledExperimentError("v11 endpoint guide strengths must be exactly 0.8")
    intermediate_frames = [
        by_index[index] for index in V10_INTERMEDIATE_GUIDE_FRAME_INDICES
    ]
    if any(frame.strength != 0.7 for frame in intermediate_frames):
        raise ControlledExperimentError("v11 intermediate guide strengths must be exactly 0.7")
    intermediate_hashes = [frame.sha256 for frame in intermediate_frames]
    if (
        len(set(intermediate_hashes)) != len(intermediate_hashes)
        or by_index[0].sha256 in intermediate_hashes
    ):
        raise ControlledExperimentError(
            "v11 intermediate guide hashes must be pairwise distinct and differ from endpoint"
        )
    return bundle, manifest_sha256, tuple(sorted(frames, key=lambda frame: frame.frame_index))


def _verify_v12_recovery_manifest(manifest: Mapping[str, Any]) -> None:
    renderer = manifest.get("renderer_object")
    if not isinstance(renderer, dict) or (
        renderer.get("renderer_string") != "browser_threejs"
        or renderer.get("blender_used_bool") is not False
        or renderer.get("scene_contract_string") != V12_SCENE_CONTRACT
        or renderer.get("all_guide_frames_browser_rendered_bool") is not True
        or renderer.get("shadows_enabled_bool") is not False
        or renderer.get("deterministic_contact_cues_bool") is not True
        or renderer.get("per_guide_contact_cue_visibility_bool") is not True
        or renderer.get("contact_cue_implementation_string")
        != "static_rest_hoof_radial_alpha_planes"
    ):
        raise ControlledExperimentError(
            "v12 guide bundle must use the pinned browser recovery scene and contact cues"
        )

    if manifest.get("recovery_frame_indices_array") != list(
        V12_RECOVERY_GUIDE_FRAME_INDICES
    ) or manifest.get("recovery_guides_byte_identical_endpoint_bool") is not True:
        raise ControlledExperimentError(
            "v12 recovery frame inventory/equality declaration is invalid"
        )

    post_bake = manifest.get("postBakeQa")
    if not isinstance(post_bake, dict) or (
        post_bake.get("status") != "PASS"
        or post_bake.get("hierarchyBakeVerified") is not True
        or _positive_int(
            post_bake.get("minimumStanceHooves"), "v12 minimum stance hooves"
        )
        < 3
        or _positive_int(
            post_bake.get("recoveryGuideCount"), "v12 recovery guide count"
        )
        != len(V12_RECOVERY_GUIDE_FRAME_INDICES)
        or _finite_float(
            post_bake.get("endpointMaximumErrorPx"),
            "v12 endpoint maximum error",
        )
        != 0
    ):
        raise ControlledExperimentError("v12 guide post-bake hoof QA did not pass")

    contact_qa = manifest.get("contactCueQa")
    contact_rows = contact_qa.get("guides") if isinstance(contact_qa, dict) else None
    if not isinstance(contact_qa, dict) or (
        contact_qa.get("schema")
        != "autorig-browser-contact-cue-visibility-qa.v1"
        or contact_qa.get("status") != "PASS"
        or contact_qa.get("perGuideVisibility") is not True
        or contact_qa.get("swingGuidesHideExactlyOneCue") is not True
        or contact_qa.get("stanceGuidesShowAllFourCues") is not True
        or not isinstance(contact_rows, list)
        or [
            row.get("frameIndex") for row in contact_rows if isinstance(row, dict)
        ]
        != list(V12_GUIDE_FRAME_INDICES)
    ):
        raise ControlledExperimentError("v12 contact-cue visibility QA did not pass")
    for row in contact_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v12 contact-cue QA row is invalid")
        frame_index = _nonnegative_int(
            row.get("frameIndex"), "v12 contact-cue frame index"
        )
        swing = frame_index in V12_SWING_GUIDE_FRAME_INDICES
        if (
            row.get("exactlyMatchesStance") is not True
            or _positive_int(
                row.get("visibleCueCount"), "v12 visible contact-cue count"
            )
            != (3 if swing else 4)
            or _nonnegative_int(
                row.get("hiddenCueCount"), "v12 hidden contact-cue count"
            )
            != (1 if swing else 0)
        ):
            raise ControlledExperimentError(
                f"v12 contact-cue visibility is invalid at frame {frame_index}"
            )

    static_qa = manifest.get("staticSceneQa")
    static_rows = static_qa.get("guides_array") if isinstance(static_qa, dict) else None
    if not isinstance(static_qa, dict) or (
        static_qa.get("schema") != "autorig-browser-static-scene-qa.v1"
        or static_qa.get("status") != "PASS"
        or static_qa.get("decoded_rgb_statistics_bool") is not True
        or static_qa.get("endpoint_byte_identical_bool") is not True
        or static_qa.get("expected_frame_indices_array")
        != list(V12_GUIDE_FRAME_INDICES)
        or not isinstance(static_rows, list)
        or [
            row.get("frame_index_int") for row in static_rows if isinstance(row, dict)
        ]
        != list(V12_GUIDE_FRAME_INDICES)
    ):
        raise ControlledExperimentError("v12 static-scene QA did not pass")
    if (
        _nonnegative_int(
            static_qa.get("maximum_background_channel_delta_int"),
            "v12 maximum background channel delta",
        )
        != 0
        or _finite_float(
            static_qa.get("background_mean_luma_range_float"),
            "v12 background mean luma range",
        )
        != 0
        or _finite_float(
            static_qa.get("full_frame_mean_luma_range_float"),
            "v12 full-frame mean luma range",
        )
        > _finite_float(
            static_qa.get("maximum_full_frame_mean_luma_range_float"),
            "v12 maximum full-frame mean luma range",
        )
        or _finite_float(
            static_qa.get("maximum_near_black_pixel_fraction_float"),
            "v12 maximum near-black pixel fraction",
        )
        > _finite_float(
            static_qa.get("allowed_near_black_pixel_fraction_float"),
            "v12 allowed near-black pixel fraction",
        )
    ):
        raise ControlledExperimentError("v12 static-scene background metrics failed")

    static_scene = manifest.get("staticSceneRenderer")
    contact_cues = (
        static_scene.get("contactCues") if isinstance(static_scene, dict) else None
    )
    if not isinstance(static_scene, dict) or (
        static_scene.get("contract") != V12_SCENE_CONTRACT
        or static_scene.get("shadowsEnabled") is not False
        or not isinstance(contact_cues, dict)
        or contact_cues.get("enabled") is not True
        or contact_cues.get("implementation")
        != "static_rest_hoof_radial_alpha_planes"
        or contact_cues.get("perGuideVisibility") is not True
    ):
        raise ControlledExperimentError("v12 static-scene renderer contract is invalid")


def _load_browser_recovery_guide_sequence(
    experiment: Mapping[str, Any],
    *,
    guide_bundle: Optional[Path],
    reference_sha256: str,
    frame_count: int,
    start_strength: float,
    end_strength: float,
    pins: BrowserRecoveryGuidePins,
) -> tuple[Path, str, tuple[ControlledGuideFrame, ...]]:
    """Load a nine-frame recovery bundle against code-owned immutable pins.

    The caller must pass the code-owned pins selected by the separately
    allowlisted experiment id; contract JSON cannot select or replace them.
    """

    bundle_id = str(pins.bundle_id or "")
    if not bundle_id or Path(bundle_id).name != bundle_id:
        raise ControlledExperimentError("v12 immutable guide bundle id is invalid")
    manifest_pin = _require_sha(
        pins.manifest_sha256, "v12 code-owned guide manifest SHA-256"
    )
    endpoint_pin = _require_sha(
        pins.endpoint_sha256, "v12 code-owned endpoint guide SHA-256"
    )
    recovery_contract = experiment.get("recovery_guide_contract_object")
    if not isinstance(recovery_contract, dict) or (
        recovery_contract.get("scene_contract_string") != V12_SCENE_CONTRACT
        or recovery_contract.get("frame_indices_array")
        != list(V12_GUIDE_FRAME_INDICES)
        or recovery_contract.get("swing_frame_indices_array")
        != list(V12_SWING_GUIDE_FRAME_INDICES)
        or recovery_contract.get("recovery_frame_indices_array")
        != list(V12_RECOVERY_GUIDE_FRAME_INDICES)
        or recovery_contract.get("strengths_array") != list(V12_GUIDE_STRENGTHS)
        or recovery_contract.get("recovery_guides_byte_identical_endpoint_bool")
        is not True
        or recovery_contract.get("deterministic_contact_cues_bool") is not True
        or recovery_contract.get("per_guide_contact_cue_visibility_bool") is not True
        or recovery_contract.get("author_cli_sha256_string")
        != V12_GUIDE_CLI_SHA256
    ):
        raise ControlledExperimentError(
            "v12 immutable recovery-guide experiment contract is invalid"
        )
    sequence = experiment.get("guide_sequence_object")
    if not isinstance(sequence, dict) or sequence.get("ready_bool") is not True:
        raise ControlledExperimentError(
            "v12 requires a completed pinned browser recovery guide bundle before launch"
        )
    if sequence.get("guide_contract_string") != V12_GUIDE_CONTRACT:
        raise ControlledExperimentError("v12 browser recovery guide contract is invalid")
    if sequence.get("bundle_id_string") != bundle_id:
        raise ControlledExperimentError("v12 immutable guide bundle id is invalid")
    bundle = Path(guide_bundle).resolve() if guide_bundle is not None else None
    if bundle is None or not bundle.is_dir() or bundle.name != bundle_id:
        raise ControlledExperimentError(
            f"v12 --guide-bundle must be the existing {bundle_id!r} directory"
        )

    manifest_filename = str(sequence.get("immutable_manifest_filename_string") or "")
    if not manifest_filename or Path(manifest_filename).name != manifest_filename:
        raise ControlledExperimentError("v12 guide manifest filename is invalid")
    if sequence.get("immutable_manifest_sha256_string") != manifest_pin:
        raise ControlledExperimentError("v12 immutable guide manifest pin is invalid")
    if sequence.get("endpoint_guide_sha256_string") != endpoint_pin:
        raise ControlledExperimentError("v12 immutable endpoint guide pin is invalid")
    manifest, manifest_sha256 = _read_pinned_json(
        bundle / manifest_filename,
        manifest_pin,
        "v12 guide manifest SHA-256",
    )
    if manifest.get("schema") != "autorig-browser-ltx-recovery-guide-bundle.v1":
        raise ControlledExperimentError("v12 guide manifest schema is invalid")
    if (
        manifest.get("status") != "PASS"
        or manifest.get("approvedForAnimationLibrary") is not False
        or manifest.get("browserOnly") is not True
        or manifest.get("blenderUsed") is not False
    ):
        raise ControlledExperimentError(
            "v12 guide bundle is not browser-only PASS/unapproved"
        )
    renderer_provenance = manifest.get("renderer")
    cli_provenance = (
        renderer_provenance.get("cli")
        if isinstance(renderer_provenance, dict)
        else None
    )
    if not isinstance(cli_provenance, dict) or (
        cli_provenance.get("sha256") != V12_GUIDE_CLI_SHA256
    ):
        raise ControlledExperimentError("v12 immutable browser author CLI pin is invalid")
    if manifest.get("source_reference_sha256_string") != reference_sha256:
        raise ControlledExperimentError(
            "v12 guide bundle does not pin the actionless reference"
        )
    if manifest.get("source_reference_is_guide_bool") is not False:
        raise ControlledExperimentError(
            "v12 source reference must remain provenance-only, not a guide frame"
        )
    if manifest.get("endpoint_guide_sha256_string") != endpoint_pin:
        raise ControlledExperimentError(
            "v12 endpoint guide SHA-256 disagrees with immutable manifest"
        )
    if manifest.get("resolution") != list(V10_GUIDE_RESOLUTION):
        raise ControlledExperimentError("v12 guide bundle resolution is invalid")
    if (
        _positive_int(manifest.get("cycle_frame_count_int"), "v12 cycle frame count")
        != frame_count
    ):
        raise ControlledExperimentError(
            "v12 guide bundle frame count disagrees with experiment"
        )
    _verify_v12_recovery_manifest(manifest)

    contract_rows = sequence.get("frames_array")
    manifest_rows = manifest.get("frames_array")
    if not isinstance(contract_rows, list) or not isinstance(manifest_rows, list):
        raise ControlledExperimentError("v12 guide frame inventories are required")
    if (
        len(contract_rows) != len(V12_GUIDE_FRAME_INDICES)
        or len(manifest_rows) != len(V12_GUIDE_FRAME_INDICES)
        or _positive_int(manifest.get("guide_count_int"), "v12 guide count")
        != len(V12_GUIDE_FRAME_INDICES)
    ):
        raise ControlledExperimentError("v12 guide bundle must contain exactly nine frames")
    if [
        row.get("frame_index_int") for row in contract_rows if isinstance(row, dict)
    ] != list(V12_GUIDE_FRAME_INDICES) or [
        row.get("frame_index_int") for row in manifest_rows if isinstance(row, dict)
    ] != list(V12_GUIDE_FRAME_INDICES):
        raise ControlledExperimentError(
            "v12 guide frames must be ordered exactly 0, 6, 12, 18, 24, 30, 36, 42, 48"
        )

    manifest_by_index = {
        _nonnegative_int(row.get("frame_index_int"), "v12 manifest frame index"): row
        for row in manifest_rows
        if isinstance(row, dict)
    }
    if len(manifest_by_index) != len(V12_GUIDE_FRAME_INDICES):
        raise ControlledExperimentError("v12 guide manifest frame indices must be unique")
    expected_strengths = dict(zip(V12_GUIDE_FRAME_INDICES, V12_GUIDE_STRENGTHS))
    frames: list[ControlledGuideFrame] = []
    seen: set[int] = set()
    for row in contract_rows:
        if not isinstance(row, dict):
            raise ControlledExperimentError("v12 guide contract frame row is invalid")
        index = _nonnegative_int(row.get("frame_index_int"), "v12 contract frame index")
        if index in seen:
            raise ControlledExperimentError("v12 guide contract frame indices must be unique")
        seen.add(index)
        filename = str(row.get("filename_string") or "")
        if (
            not filename
            or Path(filename).name != filename
            or Path(filename).suffix.lower() != ".png"
        ):
            raise ControlledExperimentError("v12 guide frames must be simple PNG filenames")
        digest = _require_sha(
            row.get("sha256_string"), f"v12 guide frame {index} SHA-256"
        )
        expected_bytes = _positive_int(
            row.get("bytes_int"), f"v12 guide frame {index} bytes"
        )
        strength = _guide_strength(
            row.get("strength_float"), f"v12 guide frame {index} strength"
        )
        manifest_row = manifest_by_index.get(index)
        if not manifest_row or (
            manifest_row.get("filename_string") != filename
            or manifest_row.get("sha256_string") != digest
            or manifest_row.get("bytes_int") != expected_bytes
            or manifest_row.get("strength_float") != strength
        ):
            raise ControlledExperimentError(
                f"v12 guide frame {index} disagrees with immutable manifest"
            )
        if strength != expected_strengths[index]:
            raise ControlledExperimentError(
                f"v12 guide frame {index} strength must be exactly "
                f"{expected_strengths[index]}"
            )
        frame = ControlledGuideFrame(
            frame_index=index,
            image=bundle / filename,
            sha256=digest,
            size_bytes=expected_bytes,
            strength=strength,
        )
        _verify_v12_planned_guide_frame(frame, decode_png=True)
        frames.append(frame)

    by_index = {frame.frame_index: frame for frame in frames}
    endpoint_and_recovery = (0, *V12_RECOVERY_GUIDE_FRAME_INDICES, 48)
    if any(by_index[index].sha256 != endpoint_pin for index in endpoint_and_recovery):
        raise ControlledExperimentError(
            "v12 endpoint and recovery guide PNGs must be byte-identical"
        )
    if endpoint_pin == reference_sha256:
        raise ControlledExperimentError(
            "v12 endpoint guides must explicitly differ from the actionless source reference"
        )
    if start_strength != 0.8 or end_strength != 0.8:
        raise ControlledExperimentError(
            "v12 experiment endpoint guide strengths must be exactly 0.8"
        )
    swing_hashes = [
        by_index[index].sha256 for index in V12_SWING_GUIDE_FRAME_INDICES
    ]
    if len(set(swing_hashes)) != len(swing_hashes) or endpoint_pin in swing_hashes:
        raise ControlledExperimentError(
            "v12 swing guide hashes must be pairwise distinct and differ from recovery"
        )
    return bundle, manifest_sha256, tuple(frames)


def load_controlled_plan(
    *,
    experiment_path: Path,
    authorization: str,
    reference_bundle: Path,
    artifact_root: Path,
    guide_bundle: Optional[Path] = None,
) -> ControlledExperimentPlan:
    path = Path(experiment_path).resolve()
    experiment, experiment_sha256 = _read_json_and_sha256(path)
    if experiment.get("schema") != EXPERIMENT_SCHEMA:
        raise ControlledExperimentError(f"experiment schema must be {EXPERIMENT_SCHEMA}")
    experiment_id = str(experiment.get("experiment_id_string") or "")
    if experiment_id not in SUPPORTED_EXPERIMENT_IDS:
        raise ControlledExperimentError(
            "controlled runner does not allow this experiment id: "
            f"{experiment_id!r}; supported={sorted(SUPPORTED_EXPERIMENT_IDS)}"
        )
    if (
        experiment_id == V12_EXPERIMENT_ID
        and experiment_sha256 != V12_EXPERIMENT_SPEC_SHA256
    ):
        raise ControlledExperimentError(
            "v12 experiment spec SHA-256 is not the exact code-owned checked-in pin: "
            f"expected {V12_EXPERIMENT_SPEC_SHA256}, got {experiment_sha256}"
        )
    if authorization != experiment_id:
        raise ControlledExperimentError(
            f"explicit --authorize-experiment {experiment_id} is required"
        )
    authorization_object = experiment.get("generation_authorization_object")
    if not isinstance(authorization_object, dict) or authorization_object.get("authorized_bool") is not False:
        raise ControlledExperimentError(
            "immutable experiment must remain prepared/unapproved; runtime CLI authorization is recorded separately"
        )
    if experiment.get("approved_bool") is not False:
        raise ControlledExperimentError("controlled experiment must not be pre-approved")
    if experiment.get("generation_mode_string") != "loop":
        raise ControlledExperimentError("controlled semantic experiment must use loop generation")

    frame_count = _positive_int(experiment.get("frame_count_int"), "frame_count_int")
    if (frame_count - 1) % 8:
        raise ControlledExperimentError("frame_count_int must satisfy 8n+1")
    input_fps = _positive_int(experiment.get("input_fps_int"), "input_fps_int")
    output_fps = _positive_int(experiment.get("output_fps_int"), "output_fps_int")
    seed = _positive_int(experiment.get("seed_int"), "seed_int")
    positive_prompt = str(experiment.get("positive_prompt_string") or "")
    negative_prompt = str(experiment.get("negative_prompt_string") or "")
    if not positive_prompt or not negative_prompt:
        raise ControlledExperimentError("positive and negative prompts must be non-empty")

    variants = experiment.get("variants_array")
    if not isinstance(variants, list) or len(variants) != 1 or not isinstance(variants[0], dict):
        raise ControlledExperimentError("controlled experiment must contain exactly one variant")
    start_strength = _guide_strength(
        variants[0].get("start_guide_strength_float"), "start guide strength"
    )
    end_strength = _guide_strength(
        variants[0].get("end_guide_strength_float"), "end guide strength"
    )
    if start_strength != end_strength:
        raise ControlledExperimentError(
            "controlled loop experiment must use the same immutable start/end guide strength"
        )

    workflow = experiment.get("workflow_object")
    if not isinstance(workflow, dict):
        raise ControlledExperimentError("workflow_object is required")
    workflow_name = str(workflow.get("workflow_name_string") or "")
    workflow_fingerprint = _require_sha(
        workflow.get("workflow_fingerprint_sha256_string"), "workflow fingerprint"
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    if (
        workflow_name != profile.workflow_name
        or workflow_fingerprint != profile.workflow_fingerprint
        or input_fps != profile.input_fps
        or output_fps != profile.output_fps
    ):
        raise ControlledExperimentError("experiment workflow/FPS does not match the pinned loop profile")

    reference = experiment.get("reference_object")
    if not isinstance(reference, dict):
        raise ControlledExperimentError("reference_object is required")
    bundle = Path(reference_bundle).resolve()
    if bundle.name != reference.get("bundle_id_string") or not bundle.is_dir():
        raise ControlledExperimentError(
            f"reference bundle must be the existing {reference.get('bundle_id_string')} directory"
        )
    reference_contract = str(
        reference.get("reference_contract_string") or "semantic_reference_v1"
    )
    source_resolution: Optional[tuple[int, int]] = None
    derivation: Optional[dict[str, Any]] = None
    if reference_contract == "semantic_reference_v1":
        derivation = _verify_reference_manifest(bundle, reference)
    elif reference_contract == "actionless_bundle_rgb_v1":
        source_resolution = _verify_actionless_reference_manifest(bundle, reference)
    else:
        raise ControlledExperimentError(
            f"unsupported reference contract: {reference_contract!r}"
        )
    image = bundle / str(reference.get("reference_png_filename_string") or "")
    reference_sha = _require_exact_sha(
        image, reference.get("reference_png_sha256_string"), "reference PNG SHA-256"
    )
    if reference_contract == "semantic_reference_v1":
        if derivation is None:
            raise ControlledExperimentError("reference derivation manifest is missing")
        if derivation.get("schema") != "autorig-ltx-semantic-reference-derivation.v1":
            raise ControlledExperimentError("reference derivation schema is invalid")
        semantic_profile = derivation.get("semantic_profile")
        if not isinstance(semantic_profile, dict) or (
            semantic_profile.get("profile_id") != reference.get("semantic_profile_id_string")
            or semantic_profile.get("sha256") != reference.get("semantic_profile_sha256_string")
        ):
            raise ControlledExperimentError(
                "semantic profile provenance disagrees with experiment contract"
            )

    browser_guide_bundle: Optional[Path] = None
    guide_manifest_sha256: Optional[str] = None
    guide_frames: tuple[ControlledGuideFrame, ...] = ()
    if experiment_id == V10_EXPERIMENT_ID:
        browser_guide_bundle, guide_manifest_sha256, guide_frames = (
            _load_browser_guide_sequence(
                experiment,
                guide_bundle=guide_bundle,
                reference_sha256=reference_sha,
                frame_count=frame_count,
                start_strength=start_strength,
                end_strength=end_strength,
            )
        )
    elif experiment_id == V11_EXPERIMENT_ID:
        browser_guide_bundle, guide_manifest_sha256, guide_frames = (
            _load_browser_static_scene_guide_sequence(
                experiment,
                guide_bundle=guide_bundle,
                reference_sha256=reference_sha,
                frame_count=frame_count,
                start_strength=start_strength,
                end_strength=end_strength,
            )
        )
    elif experiment_id == V12_EXPERIMENT_ID:
        browser_guide_bundle, guide_manifest_sha256, guide_frames = (
            _load_browser_recovery_guide_sequence(
                experiment,
                guide_bundle=guide_bundle,
                reference_sha256=reference_sha,
                frame_count=frame_count,
                start_strength=start_strength,
                end_strength=end_strength,
                pins=V12_GUIDE_PINS,
            )
        )

    latent_width: Optional[int] = None
    latent_height: Optional[int] = None
    resize_longer: Optional[int] = None
    resolution = experiment.get("resolution_override_object")
    if resolution is not None:
        if not isinstance(resolution, dict):
            raise ControlledExperimentError("resolution_override_object must be an object")
        latent_width = _positive_int(resolution.get("latent_width_int"), "latent width")
        latent_height = _positive_int(resolution.get("latent_height_int"), "latent height")
        resize_longer = _positive_int(resolution.get("resize_longer_int"), "resize longer")
        if latent_width % 32 or latent_height % 32:
            raise ControlledExperimentError("LTX latent width and height must be divisible by 32")
        if resize_longer != max(latent_width, latent_height):
            raise ControlledExperimentError("resize longer must equal the longest latent dimension")
        if source_resolution is None or source_resolution != (latent_width, latent_height):
            raise ControlledExperimentError(
                "native resolution override must match the immutable actionless camera resolution"
            )

    return ControlledExperimentPlan(
        experiment_id=experiment_id,
        experiment_path=path,
        experiment_sha256=experiment_sha256,
        reference_bundle=bundle,
        reference_image=image,
        reference_sha256=reference_sha,
        positive_prompt=positive_prompt,
        negative_prompt=negative_prompt,
        frame_count=frame_count,
        input_fps=input_fps,
        output_fps=output_fps,
        seed=seed,
        workflow_name=workflow_name,
        workflow_fingerprint=workflow_fingerprint,
        start_guide_strength=start_strength,
        end_guide_strength=end_strength,
        artifact_root=Path(artifact_root).resolve(),
        latent_width=latent_width,
        latent_height=latent_height,
        resize_longer=resize_longer,
        guide_bundle=browser_guide_bundle,
        guide_manifest_sha256=guide_manifest_sha256,
        guide_frames=guide_frames,
    )


def patch_guide_strengths(
    prompt: Mapping[str, Any], *, start_strength: float, end_strength: float
) -> dict[str, Any]:
    # apply_workflow_bindings already deep-copies the pinned prompt, so mutating
    # this owned result cannot change the immutable workflow source.
    result = dict(prompt)
    matches: dict[int, list[dict[str, Any]]] = {0: [], -1: []}
    for node in result.values():
        if not isinstance(node, dict) or node.get("class_type") != "LTXVAddGuide":
            continue
        inputs = node.get("inputs")
        if isinstance(inputs, dict) and inputs.get("frame_idx") in matches:
            matches[int(inputs["frame_idx"])].append(inputs)
    if len(matches[0]) != 1 or len(matches[-1]) != 1:
        raise ControlledExperimentError(
            "pinned loop workflow must have exactly one frame-0 and one frame-N-1 guide"
        )
    matches[0][0]["strength"] = _guide_strength(start_strength, "start guide strength")
    matches[-1][0]["strength"] = _guide_strength(end_strength, "end guide strength")
    return result


def patch_native_resolution(
    prompt: Mapping[str, Any], *, width: int, height: int, resize_longer: int
) -> dict[str, Any]:
    result = copy.deepcopy(dict(prompt))
    latent_nodes = [
        node
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "EmptyLTXVLatentVideo"
    ]
    resize_nodes = [
        node
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "ResizeImageMaskNode"
    ]
    if len(latent_nodes) != 1 or len(resize_nodes) != 1:
        raise ControlledExperimentError(
            "pinned workflow must have exactly one video latent and one image resize node"
        )
    width_value = _positive_int(width, "native latent width")
    height_value = _positive_int(height, "native latent height")
    longer_value = _positive_int(resize_longer, "native resize longer")
    if width_value % 32 or height_value % 32 or longer_value != max(width_value, height_value):
        raise ControlledExperimentError("native resolution must be 32-aligned with exact longer size")
    latent_inputs = latent_nodes[0].get("inputs")
    resize_inputs = resize_nodes[0].get("inputs")
    if not isinstance(latent_inputs, dict) or not isinstance(resize_inputs, dict):
        raise ControlledExperimentError("pinned resolution nodes have invalid inputs")
    if (
        latent_inputs.get("width") != 512
        or latent_inputs.get("height") != 320
        or resize_inputs.get("resize_type") != "scale longer dimension"
        or resize_inputs.get("resize_type.longer_size") != 512
    ):
        raise ControlledExperimentError("pinned workflow base resolution changed unexpectedly")
    latent_inputs["width"] = width_value
    latent_inputs["height"] = height_value
    resize_inputs["resize_type.longer_size"] = longer_value
    return result


def patch_browser_keyframe_guides(
    prompt: Mapping[str, Any],
    *,
    uploaded_images: Mapping[int, str],
    strengths: Mapping[int, float],
) -> dict[str, Any]:
    """Chain an exact immutable browser RGB guide profile into the LTX graph.

    V10/v11 retain their six-node endpoint plus four-swing graph.  The bounded
    v12 profile adds three explicit four-hoof recovery guides, producing nine
    ordered ``LTXVAddGuide`` nodes.  Recovery nodes reuse the endpoint's exact
    preprocessed image instead of creating a second conditioning path.
    """

    image_indices = set(uploaded_images)
    strength_indices = set(strengths)
    if (
        image_indices == set(V10_GUIDE_FRAME_INDICES)
        and strength_indices == set(V10_GUIDE_FRAME_INDICES)
    ):
        label = "v10"
        frame_indices = V10_GUIDE_FRAME_INDICES
        recovery_indices: tuple[int, ...] = ()
        expected_strengths = {
            index: (0.7 if index in V10_INTERMEDIATE_GUIDE_FRAME_INDICES else 0.8)
            for index in frame_indices
        }
    elif (
        image_indices == set(V12_GUIDE_FRAME_INDICES)
        and strength_indices == set(V12_GUIDE_FRAME_INDICES)
    ):
        label = "v12"
        frame_indices = V12_GUIDE_FRAME_INDICES
        recovery_indices = V12_RECOVERY_GUIDE_FRAME_INDICES
        expected_strengths = dict(zip(V12_GUIDE_FRAME_INDICES, V12_GUIDE_STRENGTHS))
    else:
        raise ControlledExperimentError(
            "browser guide patch requires exactly either frames "
            "0, 6, 18, 30, 42, 48 or "
            "0, 6, 12, 18, 24, 30, 36, 42, 48"
        )
    if uploaded_images[48] != uploaded_images[0]:
        raise ControlledExperimentError(
            "browser guide frames 0 and 48 must use the same uploaded endpoint image"
        )
    for index in recovery_indices:
        if uploaded_images[index] != uploaded_images[0]:
            raise ControlledExperimentError(
                f"v12 recovery guide frame {index} must use the uploaded endpoint image"
            )
    for index in frame_indices:
        if not str(uploaded_images[index] or "").strip():
            raise ControlledExperimentError(
                f"{label} uploaded guide frame {index} is empty"
            )
        strength = _guide_strength(
            strengths[index], f"{label} guide frame {index} strength"
        )
        expected_strength = expected_strengths[index]
        if strength != expected_strength:
            raise ControlledExperimentError(
                f"{label} guide frame {index} strength must be exactly {expected_strength}"
            )

    result = copy.deepcopy(dict(prompt))
    titled: dict[str, tuple[str, dict[str, Any]]] = {}
    for node_id, node in result.items():
        if not isinstance(node, dict):
            raise ControlledExperimentError(
                f"{label} workflow node {node_id} is invalid"
            )
        meta = node.get("_meta")
        title = str(meta.get("title") or "") if isinstance(meta, dict) else ""
        if title:
            if title in titled:
                raise ControlledExperimentError(
                    f"{label} workflow title is duplicated: {title}"
                )
            titled[title] = (str(node_id), node)

    required_titles = (
        "AUTORIG_START_FRAME",
        "AUTORIG_START_GUIDE",
        "AUTORIG_END_GUIDE_N_MINUS_1",
    )
    if any(title not in titled for title in required_titles):
        raise ControlledExperimentError(
            f"{label} pinned workflow guide nodes are missing"
        )
    start_load_id, start_load = titled["AUTORIG_START_FRAME"]
    start_guide_id, start_guide = titled["AUTORIG_START_GUIDE"]
    _, end_guide = titled["AUTORIG_END_GUIDE_N_MINUS_1"]
    if start_load.get("class_type") != "LoadImage":
        raise ControlledExperimentError(
            f"{label} start-frame node must be LoadImage"
        )

    resize_matches = [
        (str(node_id), node)
        for node_id, node in result.items()
        if isinstance(node, dict)
        and node.get("class_type") == "ResizeImageMaskNode"
        and node.get("inputs", {}).get("input") == [start_load_id, 0]
    ]
    if len(resize_matches) != 1:
        raise ControlledExperimentError(
            f"{label} start-frame resize path is not exact"
        )
    resize_id, resize_template = resize_matches[0]
    preprocess_matches = [
        (str(node_id), node)
        for node_id, node in result.items()
        if isinstance(node, dict)
        and node.get("class_type") == "LTXVPreprocess"
        and node.get("inputs", {}).get("image") == [resize_id, 0]
    ]
    if len(preprocess_matches) != 1:
        raise ControlledExperimentError(
            f"{label} start-frame preprocess path is not exact"
        )
    preprocess_id, preprocess_template = preprocess_matches[0]
    if (
        start_guide.get("class_type") != "LTXVAddGuide"
        or end_guide.get("class_type") != "LTXVAddGuide"
        or start_guide.get("inputs", {}).get("image") != [preprocess_id, 0]
        or end_guide.get("inputs", {}).get("image") != [preprocess_id, 0]
    ):
        raise ControlledExperimentError(
            f"{label} endpoint guide chain is not exact"
        )

    start_load["inputs"]["image"] = str(uploaded_images[0])
    start_guide["inputs"]["strength"] = float(strengths[0])
    end_guide["inputs"]["strength"] = float(strengths[48])
    previous_guide_id = start_guide_id

    for frame_index in frame_indices[1:-1]:
        suffix = f"{frame_index:03d}"
        load_id = f"91{suffix}"
        guide_resize_id = f"92{suffix}"
        guide_preprocess_id = f"93{suffix}"
        guide_id = f"94{suffix}"
        generated_ids = (
            (guide_id,)
            if frame_index in recovery_indices
            else (load_id, guide_resize_id, guide_preprocess_id, guide_id)
        )
        if any(node_id in result for node_id in generated_ids):
            raise ControlledExperimentError(
                f"{label} deterministic guide node ids collide at frame {frame_index}"
            )

        guide_node = copy.deepcopy(start_guide)
        guide_node["_meta"] = {"title": f"AUTORIG_BROWSER_GUIDE_ADD_{suffix}"}
        if frame_index in recovery_indices:
            guide_image = [preprocess_id, 0]
        else:
            load_node = copy.deepcopy(start_load)
            load_node["_meta"] = {
                "title": f"AUTORIG_BROWSER_GUIDE_FRAME_{suffix}"
            }
            load_node["inputs"]["image"] = str(uploaded_images[frame_index])
            resize_node = copy.deepcopy(resize_template)
            resize_node["_meta"] = {
                "title": f"AUTORIG_BROWSER_GUIDE_RESIZE_{suffix}"
            }
            resize_node["inputs"]["input"] = [load_id, 0]
            preprocess_node = copy.deepcopy(preprocess_template)
            preprocess_node["_meta"] = {
                "title": f"AUTORIG_BROWSER_GUIDE_PREPROCESS_{suffix}"
            }
            preprocess_node["inputs"]["image"] = [guide_resize_id, 0]
            result[load_id] = load_node
            result[guide_resize_id] = resize_node
            result[guide_preprocess_id] = preprocess_node
            guide_image = [guide_preprocess_id, 0]
        guide_node["inputs"].update({
            "positive": [previous_guide_id, 0],
            "negative": [previous_guide_id, 1],
            "latent": [previous_guide_id, 2],
            "image": guide_image,
            "frame_idx": frame_index,
            "strength": float(strengths[frame_index]),
        })
        result[guide_id] = guide_node
        previous_guide_id = guide_id

    end_inputs = end_guide.get("inputs")
    if not isinstance(end_inputs, dict):
        raise ControlledExperimentError(
            f"{label} end guide inputs are invalid"
        )
    end_inputs["positive"] = [previous_guide_id, 0]
    end_inputs["negative"] = [previous_guide_id, 1]
    end_inputs["latent"] = [previous_guide_id, 2]
    return result


def _job_identity(plan: ControlledExperimentPlan, worker: ComfyWorker) -> tuple[dict[str, Any], str, str]:
    identity = {
        "schema": "autorig.animation-fitting-controlled-job-identity.v1",
        "experiment_id_string": plan.experiment_id,
        "experiment_sha256_string": plan.experiment_sha256,
        "runtime_authorization_string": f"explicit_cli:{plan.experiment_id}",
        "reference_sha256_string": plan.reference_sha256,
        "positive_prompt_sha256_string": hashlib.sha256(plan.positive_prompt.encode()).hexdigest(),
        "negative_prompt_sha256_string": hashlib.sha256(plan.negative_prompt.encode()).hexdigest(),
        "seed_int": plan.seed,
        "frame_count_int": plan.frame_count,
        "input_fps_int": plan.input_fps,
        "output_fps_int": plan.output_fps,
        "start_guide_strength_float": plan.start_guide_strength,
        "end_guide_strength_float": plan.end_guide_strength,
        "worker_id_string": worker.worker_id,
        "worker_base_url_string": worker.base_url,
        "workflow_name_string": worker.workflow_name,
        "workflow_fingerprint_string": worker.expected_workflow_fingerprint,
        "approval_state_string": "generated_not_approved",
        "send_to_skeletal_fitting_bool": False,
    }
    if plan.latent_width is not None:
        identity["resolution_override_object"] = {
            "latent_width_int": plan.latent_width,
            "latent_height_int": plan.latent_height,
            "resize_longer_int": plan.resize_longer,
        }
    if plan.guide_frames:
        identity["browser_guide_sequence_object"] = {
            "guide_manifest_sha256_string": plan.guide_manifest_sha256,
            "frames_array": [
                {
                    "frame_index_int": frame.frame_index,
                    "sha256_string": frame.sha256,
                    "strength_float": frame.strength,
                }
                for frame in plan.guide_frames
            ],
        }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    job_id = hashlib.sha256(canonical.encode()).hexdigest()
    idempotency_key = f"autorig-controlled-animation-fitting:{job_id}"
    return identity, job_id, idempotency_key


def _stored(path: Path, digest: str) -> StoredArtifact:
    resolved = Path(path).resolve()
    if not resolved.is_file() or _sha256(resolved) != digest:
        raise ControlledExperimentError(f"stored artifact is missing or corrupt: {resolved}")
    return StoredArtifact(sha256=digest, path=resolved, size_bytes=resolved.stat().st_size)


def _resume_completed(
    store: ImmutableArtifactStore, job_id: str, frame_count: int
) -> Optional[ControlledExperimentResult]:
    state = store.latest_job_state(job_id)
    if not state or state.get("status_string") != "completed":
        return None
    raw = _stored(
        Path(str(state.get("raw_video_path_string") or "")),
        _require_sha(state.get("raw_video_sha256_string"), "stored video SHA-256"),
    )
    frame_paths = state.get("frame_paths_array")
    frame_hashes = state.get("frame_sha256_array")
    if not isinstance(frame_paths, list) or not isinstance(frame_hashes, list) or (
        len(frame_paths) != frame_count or len(frame_hashes) != frame_count
    ):
        raise ControlledExperimentError("completed state frame inventory is incomplete")
    frames = tuple(
        _stored(Path(str(path)), _require_sha(digest, f"stored frame {index} SHA-256"))
        for index, (path, digest) in enumerate(zip(frame_paths, frame_hashes))
    )
    return ControlledExperimentResult(
        job_id=job_id,
        prompt_id=str(state.get("prompt_id_string") or ""),
        raw_video=raw,
        frames=frames,
        resumed_existing_result=True,
    )


async def run_controlled_experiment(
    plan: ControlledExperimentPlan,
    *,
    worker: Optional[ComfyWorker] = None,
    client_factory: Callable[[ComfyWorker], Any] = ComfyAnimationClient,
    frame_extractor: Optional[FfmpegFrameExtractor] = None,
) -> ControlledExperimentResult:
    selected_worker = worker or worker_from_environment("loop")
    if (
        selected_worker.workflow_name != plan.workflow_name
        or selected_worker.expected_workflow_fingerprint != plan.workflow_fingerprint
    ):
        raise ControlledExperimentError("worker does not match the experiment's pinned workflow")
    store = ImmutableArtifactStore(plan.artifact_root)
    store.ensure()
    identity, job_id, idempotency_key = _job_identity(plan, selected_worker)
    resumed = _resume_completed(store, job_id, plan.frame_count)
    if resumed:
        return resumed

    extractor = frame_extractor or FfmpegFrameExtractor(
        os.getenv("AUTORIG_FFMPEG_PATH", "ffmpeg")
    )
    planned_prompt_id = deterministic_prompt_id(idempotency_key)
    client = client_factory(selected_worker)
    try:
        api_prompt, fingerprint = await client.fetch_api_workflow()
        if fingerprint != plan.workflow_fingerprint:
            raise ControlledExperimentError("live workflow fingerprint changed after worker validation")
        with store.worker_lease(selected_worker.worker_id, owner_id=job_id):
            queue_load = await client.queue_load()
            same_prompt_is_active = bool(queue_load) and await client.prompt_exists(
                planned_prompt_id
            )
            if queue_load and not same_prompt_is_active:
                raise WorkerBusyError(
                    f"Comfy worker {selected_worker.worker_id} has {queue_load} queued/running task(s)"
                )
            if same_prompt_is_active and plan.guide_frames:
                # The prompt id is deterministic and already belongs to this exact
                # immutable job identity. Re-uploading its pinned guides cannot
                # change the active graph and only creates redundant Comfy inputs.
                submission = ComfySubmission(
                    prompt_id=planned_prompt_id,
                    client_id="",
                    resumed_existing_bool=True,
                )
            else:
                uploaded_guides: dict[int, str] = {}
                if plan.guide_frames:
                    uploads_by_sha256: dict[str, str] = {}
                    for frame in plan.guide_frames:
                        if plan.experiment_id == V12_EXPERIMENT_ID:
                            _verify_v12_planned_guide_frame(frame, decode_png=False)
                        elif plan.experiment_id == V11_EXPERIMENT_ID:
                            _verify_v11_planned_guide_frame(frame, decode_png=False)
                        else:
                            _verify_planned_guide_frame(frame, decode_png=False)
                        uploaded = uploads_by_sha256.get(frame.sha256)
                        if uploaded is None:
                            uploaded = await client.upload_reference_image(
                                frame.image,
                                expected_sha256=frame.sha256,
                                expected_size_bytes=frame.size_bytes,
                            )
                            uploads_by_sha256[frame.sha256] = uploaded
                        uploaded_guides[frame.frame_index] = uploaded
                    uploaded_start = uploaded_guides[0]
                else:
                    uploaded_start = await client.upload_reference_image(plan.reference_image)
                profile: WorkflowProfile = load_animation_fitting_specs().workflows["loop"]
                prompt = apply_workflow_bindings(
                    api_prompt,
                    profile,
                    uploaded_start_image=uploaded_start,
                    positive_prompt=plan.positive_prompt,
                    negative_prompt=plan.negative_prompt,
                    frame_count=plan.frame_count,
                    seed=plan.seed,
                    output_prefix=(
                        f"animation_fitting/controlled/{plan.experiment_id}/{job_id[:16]}"
                    ),
                )
                if plan.latent_width is not None:
                    prompt = patch_native_resolution(
                        prompt,
                        width=plan.latent_width,
                        height=int(plan.latent_height or 0),
                        resize_longer=int(plan.resize_longer or 0),
                    )
                prompt = patch_guide_strengths(
                    prompt,
                    start_strength=plan.start_guide_strength,
                    end_strength=plan.end_guide_strength,
                )
                if plan.guide_frames:
                    prompt = patch_browser_keyframe_guides(
                        prompt,
                        uploaded_images=uploaded_guides,
                        strengths={
                            frame.frame_index: frame.strength for frame in plan.guide_frames
                        },
                    )
                store.append_job_state(job_id, {
                    **identity,
                    "status_string": "submitting",
                    "prompt_id_string": planned_prompt_id,
                    "positive_prompt_string": plan.positive_prompt,
                    "negative_prompt_string": plan.negative_prompt,
                })
                submission = await client.submit(prompt, idempotency_key)
            store.append_job_state(job_id, {
                **identity,
                "status_string": "rendering",
                "prompt_id_string": submission.prompt_id,
                "resumed_existing_prompt_bool": submission.resumed_existing_bool,
            })
            _, output = await client.wait_for_output(submission.prompt_id)
            if not isinstance(output, ComfyOutputFile):
                raise ControlledExperimentError("Comfy returned an invalid output contract")
            if not output.filename.lower().endswith(".mp4"):
                raise ControlledExperimentError("controlled experiment requires MP4 output")
            video = store.store_raw_video(await client.download_output(output))
            frames = tuple(await asyncio.to_thread(
                extractor.extract_and_store,
                video,
                store,
                expected_frame_count=plan.frame_count,
            ))
            store.append_job_state(job_id, {
                **identity,
                "status_string": "completed",
                "prompt_id_string": submission.prompt_id,
                "raw_video_path_string": str(video.path),
                "raw_video_sha256_string": video.sha256,
                "raw_video_bytes_int": video.size_bytes,
                "frame_count_int": len(frames),
                "frame_paths_array": [str(frame.path) for frame in frames],
                "frame_sha256_array": [frame.sha256 for frame in frames],
                "backend_output_object": {
                    "filename_string": output.filename,
                    "subfolder_string": output.subfolder,
                    "type_string": output.file_type,
                },
            })
            return ControlledExperimentResult(
                job_id=job_id,
                prompt_id=submission.prompt_id,
                raw_video=video,
                frames=frames,
                resumed_existing_result=False,
            )
    except Exception as exc:
        store.append_job_state(job_id, {
            **identity,
            "status_string": "failed",
            "prompt_id_string": planned_prompt_id,
            "error_type_string": type(exc).__name__,
            "error_string": str(exc)[:3000],
        })
        raise
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            await close()


def _default_spec_path() -> Path:
    return Path(__file__).resolve().parent / "specs" / "experiments" / (
        "horse_walk_prompt_v2_semantic_reference_guide_080.v1.json"
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one explicitly authorized, allowlisted immutable semantic Horse Walk LTX experiment."
    )
    parser.add_argument("--authorize-experiment", required=True)
    parser.add_argument("--experiment", type=Path, default=_default_spec_path())
    parser.add_argument("--reference-bundle", type=Path, required=True)
    parser.add_argument(
        "--guide-bundle",
        type=Path,
        help="Required by v10/v11/v12: immutable browser-rendered RGB keyframe bundle.",
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--ffmpeg", default=os.getenv("AUTORIG_FFMPEG_PATH", "ffmpeg"))
    return parser


async def _main_async(arguments: argparse.Namespace) -> dict[str, Any]:
    plan = load_controlled_plan(
        experiment_path=arguments.experiment,
        authorization=arguments.authorize_experiment,
        reference_bundle=arguments.reference_bundle,
        artifact_root=arguments.artifact_root,
        guide_bundle=arguments.guide_bundle,
    )
    result = await run_controlled_experiment(
        plan,
        frame_extractor=FfmpegFrameExtractor(arguments.ffmpeg),
    )
    return result.to_dict()


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    result = asyncio.run(_main_async(arguments))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
