from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import io
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
FIRST_FRAME_REFERENCE_SCHEMA = "autorig-tracking-first-frame-reference.v1"
BROWSER_STATIC_SCENE_GUIDE_MANIFEST_SCHEMA = (
    "autorig-browser-ltx-static-scene-guide-bundle.v1"
)
# Backward-compatible name retained for v11 callers that imported the original
# single-profile constant before v12 was introduced.
BROWSER_GUIDE_MANIFEST_SCHEMA = BROWSER_STATIC_SCENE_GUIDE_MANIFEST_SCHEMA
BROWSER_RECOVERY_GUIDE_MANIFEST_SCHEMA = (
    "autorig-browser-ltx-recovery-guide-bundle.v1"
)
BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA = (
    "autorig-browser-ltx-interval-guide-bundle.v1"
)
BROWSER_STATIC_SCENE_CONTRACT = "v11_unified_browser_static_scene_v1"
BROWSER_RECOVERY_SCENE_CONTRACT = "v12_unified_browser_recovery_guides_v1"
BROWSER_INTERVAL_SCENE_CONTRACT = "v14_unified_browser_interval_guide_v1"
BROWSER_GUIDE_CONTRACTS = {
    BROWSER_STATIC_SCENE_GUIDE_MANIFEST_SCHEMA: BROWSER_STATIC_SCENE_CONTRACT,
    BROWSER_RECOVERY_GUIDE_MANIFEST_SCHEMA: BROWSER_RECOVERY_SCENE_CONTRACT,
    BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA: BROWSER_INTERVAL_SCENE_CONTRACT,
}
V12_BROWSER_GUIDE_MANIFEST_SHA256 = (
    "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451"
)
V14_BROWSER_GUIDE_MANIFEST_SHA256 = (
    "a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb"
)
AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256 = frozenset(
    {
        "9290e2c5c95ab0a24175f1ba873f4af6f221ce963a315e933bcc97aa540ec173",
        V12_BROWSER_GUIDE_MANIFEST_SHA256,
        V14_BROWSER_GUIDE_MANIFEST_SHA256,
    }
)
V12_BROWSER_GUIDE_FRAME_INDICES = (0, 6, 12, 18, 24, 30, 36, 42, 48)
V12_BROWSER_RECOVERY_FRAME_INDICES = (12, 24, 36)
V12_BROWSER_SWING_LIMBS = {
    6: "hind_left",
    18: "fore_left",
    30: "hind_right",
    42: "fore_right",
}
V12_BROWSER_LIMBS = ("hind_left", "fore_left", "hind_right", "fore_right")
V14_BROWSER_FRAME_INDICES = tuple(range(49))
V14_BROWSER_BARRIER_FRAME_INDICES = (0, 12, 24, 36, 48)
REFERENCE_GEOMETRY_ASPECT_STRICT = "aspect_strict_resize"
REFERENCE_GEOMETRY_CENTER_CROP = "center_crop_cover"
REFERENCE_GEOMETRY_MODES = frozenset(
    {
        REFERENCE_GEOMETRY_ASPECT_STRICT,
        REFERENCE_GEOMETRY_CENTER_CROP,
    }
)


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
    reference_geometry_mode: str = REFERENCE_GEOMETRY_ASPECT_STRICT

    def validate(self) -> None:
        if self.reference_geometry_mode not in REFERENCE_GEOMETRY_MODES:
            raise ContractError(
                "reference_geometry_mode must be one of: "
                + ", ".join(sorted(REFERENCE_GEOMETRY_MODES))
            )
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


def _required_lower_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ContractError(f"{field} must be an exact lowercase SHA-256")
    return value


def _normalized_bundle_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ContractError(f"{field} must be an exact SHA-256")
    return value.lower()


def _required_positive_bytes(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContractError(f"{field} must be a positive integer byte count")
    return value


def _read_pinned_bytes(
    path: Path,
    *,
    expected_sha256: str,
    expected_bytes: int | None,
    field: str,
) -> bytes:
    source = path.resolve()
    try:
        payload = source.read_bytes()
    except OSError as exc:
        raise ContractError(f"Cannot read {field} {source}: {exc}") from exc
    actual_bytes = len(payload)
    if expected_bytes is not None and actual_bytes != expected_bytes:
        raise ContractError(
            f"{field} byte-size mismatch: expected {expected_bytes}, got {actual_bytes}"
        )
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ContractError(
            f"{field} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    return payload


def _decode_pinned_png(payload: bytes, path: Path) -> np.ndarray:
    try:
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Pillow is required by the tracking runtime"
        ) from exc
    try:
        with Image.open(io.BytesIO(payload)) as image:
            if image.format != "PNG":
                raise ContractError(
                    f"Browser first-frame reference must be PNG, got {image.format!r}: {path}"
                )
            return np.asarray(image.convert("RGB"), dtype=np.uint8)
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(
            f"Cannot decode browser first-frame reference {path}: {exc}"
        ) from exc


def _manifest_artifact_path(root: Path, filename: Any, field: str) -> Path:
    if not isinstance(filename, str) or not filename:
        raise ContractError(f"{field} must be a non-empty relative filename")
    relative = Path(filename)
    if relative.is_absolute() or relative.drive:
        raise ContractError(f"{field} must be relative to the browser guide bundle")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ContractError(f"{field} escapes the browser guide bundle") from exc
    return candidate


def _browser_reference_arguments(
    *,
    bundle: str | Path | None,
    manifest_sha256: str | None,
) -> tuple[Path, str] | None:
    values = (bundle, manifest_sha256)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ContractError(
            "Browser first-frame override requires both "
            "browser_endpoint_guide_bundle and its manifest SHA-256"
        )
    if not isinstance(bundle, (str, Path)) or not str(bundle):
        raise ContractError("browser_endpoint_guide_bundle must be a non-empty path")
    pinned_sha256 = _required_lower_sha256(
        manifest_sha256, "browser_guide_manifest_sha256"
    )
    if pinned_sha256 not in AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256:
        raise ContractError(
            "Browser endpoint guide manifest SHA-256 is not in the authoritative allowlist"
        )
    return (Path(bundle).resolve(), pinned_sha256)


def _object_field(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{field} must be an object")
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"Browser guide manifest duplicates JSON key {key!r}")
        result[key] = value
    return result


def _exact_list(value: Any, expected: tuple[Any, ...], field: str) -> None:
    if not isinstance(value, list) or value != list(expected):
        raise ContractError(f"{field} must be exactly {list(expected)!r}")


def _v12_guide_rows(
    value: Any, *, field: str, frame_key: str
) -> dict[int, dict[str, Any]]:
    if not isinstance(value, list) or len(value) != len(V12_BROWSER_GUIDE_FRAME_INDICES):
        raise ContractError(
            f"{field} must contain exactly the nine v12 guide-frame records"
        )
    by_index: dict[int, dict[str, Any]] = {}
    for value_index, raw in enumerate(value):
        row = _object_field(raw, f"{field}[{value_index}]")
        frame_index = row.get(frame_key)
        if (
            isinstance(frame_index, bool)
            or not isinstance(frame_index, int)
            or frame_index in by_index
        ):
            raise ContractError(f"{field} contains an invalid or duplicate frame index")
        by_index[frame_index] = row
    if tuple(sorted(by_index)) != V12_BROWSER_GUIDE_FRAME_INDICES:
        raise ContractError(
            f"{field} must cover exactly {list(V12_BROWSER_GUIDE_FRAME_INDICES)!r}"
        )
    return by_index


def _verify_v12_recovery_guide_contract(
    manifest: dict[str, Any],
    *,
    by_index: dict[int, dict[str, Any]],
    frame_payloads: dict[int, bytes],
    endpoint_sha256: str,
) -> None:
    if manifest.get("cycle_frame_count_int") != 49:
        raise ContractError("v12 browser recovery guide cycle must contain 49 frames")
    if tuple(sorted(by_index)) != V12_BROWSER_GUIDE_FRAME_INDICES:
        raise ContractError(
            "v12 browser recovery manifest must contain exactly frames "
            f"{list(V12_BROWSER_GUIDE_FRAME_INDICES)!r}"
        )
    _exact_list(
        manifest.get("recovery_frame_indices_array"),
        V12_BROWSER_RECOVERY_FRAME_INDICES,
        "recovery_frame_indices_array",
    )
    if manifest.get("recovery_guides_byte_identical_endpoint_bool") is not True:
        raise ContractError(
            "v12 recovery guides must assert byte identity with the endpoint"
        )

    endpoint_payload = frame_payloads[0]
    endpoint_and_recovery = (
        0,
        *V12_BROWSER_RECOVERY_FRAME_INDICES,
        V12_BROWSER_GUIDE_FRAME_INDICES[-1],
    )
    for frame_index in endpoint_and_recovery:
        record = by_index[frame_index]
        expected_strength = (
            0.85 if frame_index in V12_BROWSER_RECOVERY_FRAME_INDICES else 0.8
        )
        if (
            record.get("filename_string") != f"guide_{frame_index:03d}.png"
            or record.get("sha256_string") != endpoint_sha256
            or record.get("strength_float") != expected_strength
            or frame_payloads[frame_index] != endpoint_payload
        ):
            raise ContractError(
                "v12 endpoint and recovery PNGs must be exact byte-identical pins"
            )
    swing_sha256: list[str] = []
    for frame_index in V12_BROWSER_SWING_LIMBS:
        record = by_index[frame_index]
        if (
            record.get("filename_string") != f"guide_{frame_index:03d}.png"
            or record.get("sha256_string") == endpoint_sha256
            or record.get("strength_float") != 0.7
        ):
            raise ContractError(
                "v12 swing guides must be distinct pinned PNGs, not recovery frames"
            )
        swing_sha256.append(record["sha256_string"])
    if len(set(swing_sha256)) != len(swing_sha256):
        raise ContractError("v12 swing guide PNG pins must be pairwise distinct")

    static_scene_qa = _object_field(manifest.get("staticSceneQa"), "staticSceneQa")
    _exact_list(
        static_scene_qa.get("expected_frame_indices_array"),
        V12_BROWSER_GUIDE_FRAME_INDICES,
        "staticSceneQa.expected_frame_indices_array",
    )
    scene_renderer = _object_field(
        manifest.get("staticSceneRenderer"), "staticSceneRenderer"
    )
    contact_cues = _object_field(
        scene_renderer.get("contactCues"), "staticSceneRenderer.contactCues"
    )
    if (
        contact_cues.get("enabled") is not True
        or contact_cues.get("implementation")
        != "static_rest_hoof_radial_alpha_planes"
        or contact_cues.get("count") != 4
        or contact_cues.get("shadowMapUsed") is not False
        or contact_cues.get("perGuideVisibility") is not True
    ):
        raise ContractError(
            "v12 static-scene renderer contact-cue contract is invalid"
        )

    cue_qa = _object_field(manifest.get("contactCueQa"), "contactCueQa")
    if (
        cue_qa.get("schema")
        != "autorig-browser-contact-cue-visibility-qa.v1"
        or cue_qa.get("status") != "PASS"
        or cue_qa.get("perGuideVisibility") is not True
        or cue_qa.get("swingGuidesHideExactlyOneCue") is not True
        or cue_qa.get("stanceGuidesShowAllFourCues") is not True
    ):
        raise ContractError("v12 contact-cue visibility QA contract is not PASS")
    cue_rows = _v12_guide_rows(
        cue_qa.get("guides"), field="contactCueQa.guides", frame_key="frameIndex"
    )
    for frame_index in V12_BROWSER_GUIDE_FRAME_INDICES:
        row = cue_rows[frame_index]
        swing_limb = V12_BROWSER_SWING_LIMBS.get(frame_index)
        visible_limbs = tuple(
            limb for limb in V12_BROWSER_LIMBS if limb != swing_limb
        )
        hidden_limbs = () if swing_limb is None else (swing_limb,)
        expected_visible_count = 4 if swing_limb is None else 3
        if (
            row.get("swingLimb") != swing_limb
            or row.get("visibleLimbs") != list(visible_limbs)
            or row.get("hiddenLimbs") != list(hidden_limbs)
            or isinstance(row.get("visibleCueCount"), bool)
            or row.get("visibleCueCount") != expected_visible_count
            or isinstance(row.get("hiddenCueCount"), bool)
            or row.get("hiddenCueCount") != len(hidden_limbs)
            or row.get("exactlyMatchesStance") is not True
        ):
            raise ContractError(
                f"v12 contact-cue QA frame {frame_index} must show "
                f"{expected_visible_count} stance cues and hide only its swing cue"
            )

    post_bake = _object_field(manifest.get("postBakeQa"), "postBakeQa")
    if (
        post_bake.get("status") != "PASS"
        or post_bake.get("hierarchyBakeVerified") is not True
        or post_bake.get("minimumStanceHooves") != 3
        or post_bake.get("recoveryGuideCount") != 3
    ):
        raise ContractError("v12 recovery post-bake QA contract is not PASS")
    post_bake_rows = _v12_guide_rows(
        post_bake.get("guides"), field="postBakeQa.guides", frame_key="frameIndex"
    )
    for frame_index in V12_BROWSER_GUIDE_FRAME_INDICES:
        swing_limb = V12_BROWSER_SWING_LIMBS.get(frame_index)
        expected_stance_count = 4 if swing_limb is None else 3
        row = post_bake_rows[frame_index]
        if (
            row.get("swingLimb") != swing_limb
            or isinstance(row.get("stanceHoofCount"), bool)
            or row.get("stanceHoofCount") != expected_stance_count
        ):
            raise ContractError(
                f"v12 post-bake QA frame {frame_index} must retain "
                f"{expected_stance_count} stance hooves"
            )


def _v14_frame_rows(
    value: Any, *, field: str, frame_key: str
) -> dict[int, dict[str, Any]]:
    if not isinstance(value, list) or len(value) != len(V14_BROWSER_FRAME_INDICES):
        raise ContractError(f"{field} must contain exactly 49 v14 frame records")
    by_index: dict[int, dict[str, Any]] = {}
    for value_index, raw in enumerate(value):
        row = _object_field(raw, f"{field}[{value_index}]")
        frame_index = row.get(frame_key)
        if (
            isinstance(frame_index, bool)
            or not isinstance(frame_index, int)
            or frame_index in by_index
        ):
            raise ContractError(f"{field} contains an invalid or duplicate frame index")
        by_index[frame_index] = row
    if tuple(sorted(by_index)) != V14_BROWSER_FRAME_INDICES:
        raise ContractError(f"{field} must cover every v14 frame from 0 through 48")
    return by_index


def _v14_swing_limb(frame_index: int) -> str | None:
    if 1 <= frame_index <= 11:
        return "hind_left"
    if 13 <= frame_index <= 23:
        return "fore_left"
    if 25 <= frame_index <= 35:
        return "hind_right"
    if 37 <= frame_index <= 47:
        return "fore_right"
    return None


def _verify_v14_interval_guide_contract(
    manifest: dict[str, Any],
    *,
    manifest_root: Path,
    by_index: dict[int, dict[str, Any]],
    frame_payloads: dict[int, bytes],
    endpoint_sha256: str,
) -> None:
    if manifest.get("cycle_frame_count_int") != 49:
        raise ContractError("v14 browser interval guide cycle must contain 49 frames")
    if manifest.get("browser_frame_count_int") != 49:
        raise ContractError("v14 browser interval guide must declare 49 browser frames")
    if manifest.get("guide_count_int") != 1:
        raise ContractError("v14 browser interval bundle must contain exactly one video guide")
    if tuple(sorted(by_index)) != V14_BROWSER_FRAME_INDICES:
        raise ContractError("v14 browser interval manifest must contain every frame 0..48")
    _exact_list(
        manifest.get("recovery_frame_indices_array"),
        V12_BROWSER_RECOVERY_FRAME_INDICES,
        "recovery_frame_indices_array",
    )
    _exact_list(
        manifest.get("source_anchor_frame_indices_array"),
        V12_BROWSER_GUIDE_FRAME_INDICES,
        "source_anchor_frame_indices_array",
    )
    if manifest.get("recovery_guides_byte_identical_endpoint_bool") is not None:
        raise ContractError(
            "v14 interval mode must not claim the legacy multi-guide recovery flag"
        )
    if manifest.get("source_anchors_byte_identical_bool") is not True:
        raise ContractError("v14 source anchor frames must retain their exact source-guide pins")

    endpoint_payload = frame_payloads[0]
    for frame_index in V14_BROWSER_FRAME_INDICES:
        record = by_index[frame_index]
        if record.get("filename_string") != f"guide_{frame_index:03d}.png":
            raise ContractError(f"v14 frame {frame_index} has an invalid filename")
        decoded_sha256 = _required_lower_sha256(
            record.get("decoded_rgb_sha256_string"),
            f"frames_array[{frame_index}].decoded_rgb_sha256_string",
        )
        if not decoded_sha256:
            raise ContractError(f"v14 frame {frame_index} has no decoded RGB pin")
        is_source_anchor = frame_index in V12_BROWSER_GUIDE_FRAME_INDICES
        if record.get("source_anchor_byte_identical_bool") is not is_source_anchor:
            raise ContractError(
                f"v14 frame {frame_index} source-anchor identity flag is invalid"
            )
        if frame_index in V14_BROWSER_BARRIER_FRAME_INDICES and (
            record.get("sha256_string") != endpoint_sha256
            or frame_payloads[frame_index] != endpoint_payload
        ):
            raise ContractError(
                "v14 endpoint and recovery barrier PNGs must be byte-identical pins"
            )

    resolution = manifest.get("resolution")
    if (
        not isinstance(resolution, list)
        or len(resolution) != 2
        or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in resolution)
    ):
        raise ContractError("v14 resolution must contain two positive integers")
    width, height = resolution

    guide_rows = _v14_frame_rows(
        manifest.get("guides"), field="guides", frame_key="frameIndex"
    )
    for frame_index in V14_BROWSER_FRAME_INDICES:
        frame_record = by_index[frame_index]
        guide_record = guide_rows[frame_index]
        swing_limb = _v14_swing_limb(frame_index)
        if (
            guide_record.get("filename") != frame_record.get("filename_string")
            or guide_record.get("bytes") != frame_record.get("bytes_int")
            or guide_record.get("sha256") != frame_record.get("sha256_string")
            or guide_record.get("decodedRgbSha256")
            != frame_record.get("decoded_rgb_sha256_string")
            or guide_record.get("sourceAnchorByteIdentical")
            is not frame_record.get("source_anchor_byte_identical_bool")
            or guide_record.get("swingLimb") != swing_limb
            or guide_record.get("width") != width
            or guide_record.get("height") != height
            or guide_record.get("renderSource") != "browser_threejs"
        ):
            raise ContractError(f"v14 guide record {frame_index} does not match its frame pin")

    source_guide = _object_field(manifest.get("sourceGuideBundle"), "sourceGuideBundle")
    source_manifest = _object_field(
        source_guide.get("immutableManifest"),
        "sourceGuideBundle.immutableManifest",
    )
    if (
        source_guide.get("bundleId") != "horse-walk-v12-browser-recovery-guides-f2"
        or source_manifest.get("filename") != "immutable_manifest.json"
        or source_manifest.get("sha256") != V12_BROWSER_GUIDE_MANIFEST_SHA256
    ):
        raise ContractError("v14 source guide provenance must pin the authorized v12 f2 bundle")
    _required_positive_bytes(
        source_manifest.get("bytes"), "sourceGuideBundle.immutableManifest.bytes"
    )
    source_pose = _object_field(
        source_guide.get("poseContract"), "sourceGuideBundle.poseContract"
    )
    if source_pose.get("filename") != "pose_contract.json":
        raise ContractError("v14 source guide pose-contract filename is invalid")
    _required_positive_bytes(
        source_pose.get("bytes"), "sourceGuideBundle.poseContract.bytes"
    )
    _required_lower_sha256(
        source_pose.get("sha256"), "sourceGuideBundle.poseContract.sha256"
    )
    source_anchor_rows = source_guide.get("anchorFrames")
    if not isinstance(source_anchor_rows, list) or len(source_anchor_rows) != 9:
        raise ContractError("v14 sourceGuideBundle.anchorFrames must contain nine pins")
    source_anchors: dict[int, dict[str, Any]] = {}
    for item_index, raw in enumerate(source_anchor_rows):
        record = _object_field(raw, f"sourceGuideBundle.anchorFrames[{item_index}]")
        frame_index = record.get("frameIndex")
        if (
            isinstance(frame_index, bool)
            or not isinstance(frame_index, int)
            or frame_index in source_anchors
        ):
            raise ContractError("v14 source anchor frame index is invalid or duplicated")
        source_anchors[frame_index] = record
    if tuple(sorted(source_anchors)) != V12_BROWSER_GUIDE_FRAME_INDICES:
        raise ContractError("v14 source anchor pins must cover the exact nine v12 frames")
    for frame_index in V12_BROWSER_GUIDE_FRAME_INDICES:
        source_record = source_anchors[frame_index]
        frame_record = by_index[frame_index]
        if (
            source_record.get("filename") != frame_record.get("filename_string")
            or source_record.get("bytes") != frame_record.get("bytes_int")
            or source_record.get("sha256") != frame_record.get("sha256_string")
        ):
            raise ContractError(f"v14 source anchor pin {frame_index} does not match output")

    scene_qa = _object_field(manifest.get("staticSceneQa"), "staticSceneQa")
    _exact_list(
        scene_qa.get("expected_frame_indices_array"),
        V14_BROWSER_FRAME_INDICES,
        "staticSceneQa.expected_frame_indices_array",
    )
    scene_renderer = _object_field(
        manifest.get("staticSceneRenderer"), "staticSceneRenderer"
    )
    contact_cues = _object_field(
        scene_renderer.get("contactCues"), "staticSceneRenderer.contactCues"
    )
    if (
        contact_cues.get("enabled") is not True
        or contact_cues.get("implementation")
        != "static_rest_hoof_radial_alpha_planes"
        or contact_cues.get("count") != 4
        or contact_cues.get("shadowMapUsed") is not False
        or contact_cues.get("perGuideVisibility") is not True
    ):
        raise ContractError("v14 static-scene contact-cue contract is invalid")

    cue_qa = _object_field(manifest.get("contactCueQa"), "contactCueQa")
    if (
        cue_qa.get("schema") != "autorig-browser-contact-cue-visibility-qa.v1"
        or cue_qa.get("status") != "PASS"
        or cue_qa.get("perFrameVisibility") is not True
        or cue_qa.get("swingFramesHideExactlyOneCue") is not True
        or cue_qa.get("barrierFramesShowAllFourCues") is not True
    ):
        raise ContractError("v14 contact-cue visibility QA contract is not PASS")
    cue_rows = _v14_frame_rows(
        cue_qa.get("frames"), field="contactCueQa.frames", frame_key="frameIndex"
    )
    for frame_index in V14_BROWSER_FRAME_INDICES:
        row = cue_rows[frame_index]
        swing_limb = _v14_swing_limb(frame_index)
        visible_limbs = tuple(limb for limb in V12_BROWSER_LIMBS if limb != swing_limb)
        hidden_limbs = () if swing_limb is None else (swing_limb,)
        if (
            row.get("swingLimb") != swing_limb
            or row.get("visibleLimbs") != list(visible_limbs)
            or row.get("hiddenLimbs") != list(hidden_limbs)
            or row.get("visibleCueCount") != len(visible_limbs)
            or row.get("hiddenCueCount") != len(hidden_limbs)
            or row.get("exactlyMatchesStance") is not True
        ):
            raise ContractError(
                f"v14 contact-cue QA frame {frame_index} does not match its swing interval"
            )

    post_bake = _object_field(manifest.get("postBakeQa"), "postBakeQa")
    if (
        post_bake.get("status") != "PASS"
        or post_bake.get("hierarchyBakeVerified") is not True
        or post_bake.get("frameCount") != 49
        or post_bake.get("minimumStanceHooves") != 3
    ):
        raise ContractError("v14 interval post-bake QA contract is not PASS")
    post_bake_rows = _v14_frame_rows(
        post_bake.get("frames"), field="postBakeQa.frames", frame_key="frameIndex"
    )
    for frame_index in V14_BROWSER_FRAME_INDICES:
        swing_limb = _v14_swing_limb(frame_index)
        expected_stance_count = 4 if swing_limb is None else 3
        row = post_bake_rows[frame_index]
        if (
            row.get("swingLimb") != swing_limb
            or row.get("stanceHoofCount") != expected_stance_count
        ):
            raise ContractError(
                f"v14 post-bake QA frame {frame_index} has an invalid stance contract"
            )

    deterministic_qa = _object_field(
        manifest.get("deterministicRenderQa"), "deterministicRenderQa"
    )
    if (
        deterministic_qa.get("schema") != "autorig-browser-deterministic-rerender-qa.v1"
        or deterministic_qa.get("status") != "PASS"
        or deterministic_qa.get("frameCount") != 49
        or deterministic_qa.get("byteIdenticalFrameCount") != 49
        or deterministic_qa.get("mismatchFrameIndices") != []
    ):
        raise ContractError("v14 deterministic rerender QA contract is not PASS")
    lossless_qa = _object_field(manifest.get("losslessVideoQa"), "losslessVideoQa")
    if (
        lossless_qa.get("schema") != "autorig-browser-lossless-interval-video-qa.v1"
        or lossless_qa.get("status") != "PASS"
        or lossless_qa.get("frameCount") != 49
        or lossless_qa.get("codec") != "png"
        or lossless_qa.get("pixelFormat") != "rgb24"
        or lossless_qa.get("decodedRgbSha256MatchesBrowserFrames") is not True
    ):
        raise ContractError("v14 lossless interval-video QA contract is not PASS")

    interval_video = _object_field(
        manifest.get("interval_guide_video_object"), "interval_guide_video_object"
    )
    video_sha256 = _required_lower_sha256(
        interval_video.get("sha256"), "interval_guide_video_object.sha256"
    )
    video_bytes = _required_positive_bytes(
        interval_video.get("bytes"), "interval_guide_video_object.bytes"
    )
    decoded_pins = interval_video.get("decoded_rgb_sha256_array")
    expected_decoded_pins = [
        by_index[index]["decoded_rgb_sha256_string"] for index in V14_BROWSER_FRAME_INDICES
    ]
    if (
        interval_video.get("filename") != "interval_guide.mkv"
        or interval_video.get("container") != "matroska"
        or interval_video.get("codec") != "png"
        or interval_video.get("pixelFormat") != "rgb24"
        or interval_video.get("width") != width
        or interval_video.get("height") != height
        or interval_video.get("frameCount") != 49
        or interval_video.get("audioStreamCount") != 0
        or interval_video.get("exact_browser_frame_rgb_bool") is not True
        or interval_video.get("load_video_node_compatible_bool") is not True
        or decoded_pins != expected_decoded_pins
    ):
        raise ContractError("v14 interval guide video contract is invalid")
    video_path = _manifest_artifact_path(
        manifest_root,
        interval_video.get("filename"),
        "interval_guide_video_object.filename",
    )
    _read_pinned_bytes(
        video_path,
        expected_sha256=video_sha256,
        expected_bytes=video_bytes,
        field="v14 interval guide video",
    )

    pose_contract = _object_field(manifest.get("poseContract"), "poseContract")
    pose_sha256 = _required_lower_sha256(
        pose_contract.get("sha256"), "poseContract.sha256"
    )
    pose_bytes = _required_positive_bytes(pose_contract.get("bytes"), "poseContract.bytes")
    if pose_contract.get("filename") != "pose_contract.json":
        raise ContractError("v14 pose contract filename is invalid")
    pose_path = _manifest_artifact_path(
        manifest_root, pose_contract.get("filename"), "poseContract.filename"
    )
    _read_pinned_bytes(
        pose_path,
        expected_sha256=pose_sha256,
        expected_bytes=pose_bytes,
        field="v14 pose contract",
    )


def _browser_first_frame_reference(
    rig: RigBundle,
    canonical_rgb: np.ndarray,
    arguments: tuple[Path, str],
) -> tuple[np.ndarray, dict[str, Any]]:
    guide_root, manifest_sha256 = arguments
    if not guide_root.is_dir():
        raise ContractError(
            f"Browser endpoint guide bundle does not exist: {guide_root}"
        )
    manifest_path = (guide_root / "immutable_manifest.json").resolve()
    try:
        manifest_path.relative_to(guide_root)
    except ValueError as exc:
        raise ContractError(
            "Browser guide manifest resolves outside the authorized guide bundle"
        ) from exc
    manifest_payload = _read_pinned_bytes(
        manifest_path,
        expected_sha256=manifest_sha256,
        expected_bytes=None,
        field="browser guide manifest",
    )
    manifest_bytes = len(manifest_payload)
    try:
        manifest = json.loads(
            manifest_payload.decode("utf-8"), object_pairs_hook=_unique_json_object
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"Invalid browser guide manifest {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ContractError("Browser guide manifest must contain a JSON object")
    manifest_schema = manifest.get("schema")
    scene_contract = BROWSER_GUIDE_CONTRACTS.get(manifest_schema)
    if scene_contract is None:
        raise ContractError(
            "Unsupported browser guide manifest schema; expected one of "
            f"{sorted(BROWSER_GUIDE_CONTRACTS)!r}"
        )
    if manifest.get("status") != "PASS":
        raise ContractError("Browser guide manifest status must be PASS")
    if manifest.get("browserOnly") is not True or manifest.get("blenderUsed") is not False:
        raise ContractError(
            "Browser guide manifest must assert browserOnly=true and blenderUsed=false"
        )
    renderer = _object_field(manifest.get("renderer_object"), "renderer_object")
    if (
        renderer.get("renderer_string") != "browser_threejs"
        or renderer.get("scene_contract_string") != scene_contract
        or renderer.get("all_guide_frames_browser_rendered_bool") is not True
        or renderer.get("blender_used_bool") is not False
    ):
        raise ContractError(
            "Browser guide manifest does not satisfy its authorized unified "
            "browser scene contract"
        )
    if manifest_schema == BROWSER_RECOVERY_GUIDE_MANIFEST_SCHEMA and (
        renderer.get("deterministic_contact_cues_bool") is not True
        or renderer.get("per_guide_contact_cue_visibility_bool") is not True
        or renderer.get("shadows_enabled_bool") is not False
        or renderer.get("contact_cue_implementation_string")
        != "static_rest_hoof_radial_alpha_planes"
    ):
        raise ContractError(
            "v12 browser recovery renderer must provide deterministic per-guide "
            "contact cues"
        )
    if manifest_schema == BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA and (
        renderer.get("deterministic_contact_cues_bool") is not True
        or renderer.get("per_guide_contact_cue_visibility_bool") is not False
        or renderer.get("per_frame_contact_cue_visibility_bool") is not True
        or renderer.get("shadows_enabled_bool") is not False
        or renderer.get("contact_cue_implementation_string")
        != "static_rest_hoof_radial_alpha_planes"
    ):
        raise ContractError(
            "v14 browser interval renderer must provide deterministic per-frame "
            "contact cues"
        )
    static_scene_qa = _object_field(manifest.get("staticSceneQa"), "staticSceneQa")
    if (
        static_scene_qa.get("schema") != "autorig-browser-static-scene-qa.v1"
        or static_scene_qa.get("status") != "PASS"
        or static_scene_qa.get("decoded_rgb_statistics_bool") is not True
        or static_scene_qa.get("endpoint_byte_identical_bool") is not True
    ):
        raise ContractError("Browser guide static-scene QA contract is not PASS")
    static_scene_renderer = _object_field(
        manifest.get("staticSceneRenderer"), "staticSceneRenderer"
    )
    if static_scene_renderer.get("contract") != scene_contract:
        raise ContractError("Browser guide static-scene renderer contract is invalid")

    canonical_record = _object_field(
        _object_field(rig.metadata.get("artifacts"), "bundle artifacts").get("rgb"),
        "bundle artifacts.rgb",
    )
    canonical_sha256 = _normalized_bundle_sha256(
        canonical_record.get("sha256"), "bundle artifacts.rgb.sha256"
    )
    canonical_bytes = _required_positive_bytes(
        canonical_record.get("bytes"), "bundle artifacts.rgb.bytes"
    )
    if manifest.get("source_reference_sha256_string") != canonical_sha256:
        raise ContractError(
            "Browser guide manifest source reference does not match the canonical bundle RGB"
        )
    if manifest.get("source_reference_is_guide_bool") is not False:
        raise ContractError(
            "Browser guide manifest must preserve canonical RGB as provenance, not as a guide"
        )
    endpoint_sha256 = _required_lower_sha256(
        manifest.get("endpoint_guide_sha256_string"),
        "endpoint_guide_sha256_string",
    )
    if manifest_schema in {
        BROWSER_RECOVERY_GUIDE_MANIFEST_SCHEMA,
        BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA,
    } and endpoint_sha256 == canonical_sha256:
        raise ContractError(
            "Browser contact-guide endpoint must remain distinct from canonical RGB"
        )

    source = _object_field(manifest.get("source"), "source")
    source_reference = _object_field(source.get("referenceRgb"), "source.referenceRgb")
    if (
        source_reference.get("filename") != canonical_record.get("filename")
        or source_reference.get("sha256") != canonical_sha256
        or source_reference.get("bytes") != canonical_bytes
    ):
        raise ContractError(
            "Browser guide source.referenceRgb does not match the canonical bundle"
        )
    source_manifest = _object_field(
        source.get("immutableManifest"), "source.immutableManifest"
    )
    if (
        source_manifest.get("filename") != rig.immutable_manifest_path.name
        or source_manifest.get("sha256") != rig.immutable_manifest_sha256
        or source_manifest.get("bytes") != rig.immutable_manifest_path.stat().st_size
    ):
        raise ContractError(
            "Browser guide immutable-manifest provenance does not match the canonical bundle"
        )
    source_bundle = _object_field(source.get("fittingBundle"), "source.fittingBundle")
    if (
        source_bundle.get("filename") != rig.metadata_path.name
        or source_bundle.get("sha256") != rig.metadata_sha256
        or source_bundle.get("bytes") != rig.metadata_path.stat().st_size
    ):
        raise ContractError(
            "Browser guide fitting-bundle provenance does not match the canonical bundle"
        )
    bundle_source = _object_field(rig.metadata.get("source"), "bundle source")
    canonical_source_sha256 = _normalized_bundle_sha256(
        bundle_source.get("sha256"), "bundle source.sha256"
    )
    if source.get("sourceModelSha256") != canonical_source_sha256:
        raise ContractError(
            "Browser guide source model provenance does not match the canonical bundle"
        )
    rig_type = bundle_source.get("rig_type")
    if not isinstance(rig_type, str) or not rig_type or manifest.get("rigType") != rig_type:
        raise ContractError("Browser guide rigType does not match the canonical bundle")

    frames = manifest.get("frames_array")
    if not isinstance(frames, list) or not frames:
        raise ContractError("Browser guide frames_array must be a non-empty array")
    cycle_frame_count = manifest.get("cycle_frame_count_int")
    if (
        isinstance(cycle_frame_count, bool)
        or not isinstance(cycle_frame_count, int)
        or cycle_frame_count < 2
    ):
        raise ContractError("Browser guide cycle_frame_count_int must be at least two")
    guide_count = manifest.get("guide_count_int")
    if isinstance(guide_count, bool) or not isinstance(guide_count, int):
        raise ContractError("Browser guide guide_count_int must be an integer")
    if manifest_schema == BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA:
        if guide_count != 1:
            raise ContractError("v14 interval bundle must declare exactly one video guide")
    elif guide_count != len(frames):
        raise ContractError("Browser guide guide_count_int does not match frames_array")
    manifest_root = manifest_path.parent.resolve()
    by_index: dict[int, dict[str, Any]] = {}
    frame_paths: dict[int, Path] = {}
    frame_payloads: dict[int, bytes] = {}
    for row in frames:
        record = _object_field(row, "frames_array item")
        index = record.get("frame_index_int")
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or index < 0
            or index >= cycle_frame_count
        ):
            raise ContractError(
                "Browser guide frame_index_int must lie inside the declared cycle"
            )
        if index in by_index:
            raise ContractError(f"Browser guide frame index is duplicated: {index}")
        by_index[index] = record
        record_sha256 = _required_lower_sha256(
            record.get("sha256_string"), f"frames_array[{index}].sha256_string"
        )
        record_bytes = _required_positive_bytes(
            record.get("bytes_int"), f"frames_array[{index}].bytes_int"
        )
        frame_path = _manifest_artifact_path(
            manifest_root,
            record.get("filename_string"),
            f"frames_array[{index}].filename_string",
        )
        if frame_path.suffix.lower() != ".png":
            raise ContractError(f"Browser guide frame {index} must be a PNG")
        frame_paths[index] = frame_path
        frame_payloads[index] = _read_pinned_bytes(
            frame_path,
            expected_sha256=record_sha256,
            expected_bytes=record_bytes,
            field=f"browser guide frame {index}",
        )
    last_index = cycle_frame_count - 1
    if 0 not in by_index or last_index not in by_index:
        raise ContractError(
            f"Browser guide manifest must contain endpoint frames 0 and {last_index}"
        )
    reference_path: Path | None = None
    reference_sha256: str | None = None
    reference_bytes: int | None = None
    reference_payload: bytes | None = None
    for endpoint_index in (0, last_index):
        record = by_index[endpoint_index]
        record_sha256 = _required_lower_sha256(
            record.get("sha256_string"),
            f"frames_array[{endpoint_index}].sha256_string",
        )
        record_bytes = _required_positive_bytes(
            record.get("bytes_int"), f"frames_array[{endpoint_index}].bytes_int"
        )
        if record_sha256 != endpoint_sha256:
            raise ContractError(
                f"Browser guide frame {endpoint_index} does not match endpoint SHA-256"
            )
        endpoint_path = frame_paths[endpoint_index]
        endpoint_payload = frame_payloads[endpoint_index]
        if endpoint_index == 0:
            reference_path = endpoint_path
            reference_sha256 = record_sha256
            reference_bytes = record_bytes
            reference_payload = endpoint_payload
        elif record_bytes != reference_bytes or endpoint_payload != reference_payload:
            raise ContractError(
                "Browser guide endpoint frames are not byte-identical"
            )
    if (
        reference_path is None
        or reference_sha256 is None
        or reference_bytes is None
        or reference_payload is None
    ):
        raise ContractError("Browser guide first-frame reference was not resolved")
    if manifest_schema == BROWSER_RECOVERY_GUIDE_MANIFEST_SCHEMA:
        _verify_v12_recovery_guide_contract(
            manifest,
            by_index=by_index,
            frame_payloads=frame_payloads,
            endpoint_sha256=endpoint_sha256,
        )
    elif manifest_schema == BROWSER_INTERVAL_GUIDE_MANIFEST_SCHEMA:
        _verify_v14_interval_guide_contract(
            manifest,
            manifest_root=manifest_root,
            by_index=by_index,
            frame_payloads=frame_payloads,
            endpoint_sha256=endpoint_sha256,
        )
    reference_rgb = _decode_pinned_png(reference_payload, reference_path)
    if reference_rgb.shape != canonical_rgb.shape:
        raise ContractError(
            "Browser first-frame reference dimensions do not match canonical bundle RGB: "
            f"{reference_rgb.shape[:2]} vs {canonical_rgb.shape[:2]}"
        )
    return reference_rgb, {
        "schema": FIRST_FRAME_REFERENCE_SCHEMA,
        "mode": "browser_static_scene_override",
        "selected": {
            "path": str(reference_path),
            "sha256": reference_sha256,
            "bytes": reference_bytes,
            "manifest": {
                "path": str(manifest_path),
                "sha256": manifest_sha256,
                "bytes": manifest_bytes,
                "schema": manifest_schema,
                "scene_contract": scene_contract,
                "cycle_frame_count": cycle_frame_count,
            },
        },
        "canonical_bundle": {
            "path": str(rig.root),
            "bundle_sha256": rig.metadata_sha256,
            "immutable_manifest_sha256": rig.immutable_manifest_sha256,
            "source_model_sha256": canonical_source_sha256,
            "rig_type": rig_type,
            "reference_rgb": {
                "path": str(_artifact_path(rig, "rgb")),
                "sha256": canonical_sha256,
                "bytes": canonical_bytes,
            },
        },
    }


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
    browser_endpoint_guide_bundle: str | Path | None = None,
    browser_endpoint_guide_manifest_sha256: str | None = None,
    loop: bool = False,
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
    canonical_rgb_path = _artifact_path(rig, "rgb")
    canonical_rgb = _load_rgb(canonical_rgb_path)
    reference_mask = _load_mask(_artifact_path(rig, "mask"))
    if canonical_rgb.shape[:2] != reference_mask.shape:
        raise ContractError("Canonical RGB and mask dimensions do not match")
    if (rig.camera.height, rig.camera.width) != reference_mask.shape:
        raise ContractError(
            "Canonical camera dimensions do not match reference artifacts"
        )
    browser_reference = _browser_reference_arguments(
        bundle=browser_endpoint_guide_bundle,
        manifest_sha256=browser_endpoint_guide_manifest_sha256,
    )
    if not isinstance(loop, bool):
        raise ContractError("loop must be boolean")
    if browser_reference is not None and not loop:
        raise ContractError(
            "Browser endpoint guide reference override is accepted only for loop seed selection"
        )
    canonical_record = _object_field(
        _object_field(rig.metadata.get("artifacts"), "bundle artifacts").get("rgb"),
        "bundle artifacts.rgb",
    )
    reference_rgb = canonical_rgb
    reference_provenance = {
        "schema": FIRST_FRAME_REFERENCE_SCHEMA,
        "mode": "canonical_bundle_rgb",
        "selected": {
            "path": str(canonical_rgb_path),
            "sha256": _normalized_bundle_sha256(
                canonical_record.get("sha256"), "bundle artifacts.rgb.sha256"
            ),
            "bytes": _required_positive_bytes(
                canonical_record.get("bytes"), "bundle artifacts.rgb.bytes"
            ),
        },
        "canonical_bundle": {
            "path": str(rig.root),
            "bundle_sha256": rig.metadata_sha256,
            "immutable_manifest_sha256": rig.immutable_manifest_sha256,
        },
    }
    if browser_reference is not None:
        reference_rgb, reference_provenance = _browser_first_frame_reference(
            rig, canonical_rgb, browser_reference
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
        reference_provenance=reference_provenance,
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


def _reference_geometry_transform(
    seeds: SeedSet,
    *,
    width: int,
    height: int,
    mode: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    """Map the pinned reference, mask, and semantic seeds into video pixels.

    ``center_crop_cover`` is an explicit opt-in for image-conditioned video
    workflows whose declared preprocessing is a centered cover crop.  The
    default remains fail-closed on aspect-ratio changes.
    """

    try:
        import cv2
    except ImportError as exc:
        raise DependencyUnavailableError(
            "opencv-python-headless is required for alignment"
        ) from exc
    if mode not in REFERENCE_GEOMETRY_MODES:
        raise ContractError(f"Unsupported reference geometry mode: {mode}")
    source_height, source_width = seeds.reference_rgb.shape[:2]
    if width < 1 or height < 1 or source_width < 1 or source_height < 1:
        raise ContractError("Reference/video geometry must be positive")

    source_aspect = source_width / source_height
    target_aspect = width / height
    if mode == REFERENCE_GEOMETRY_ASPECT_STRICT:
        if abs(source_aspect - target_aspect) / source_aspect > 0.01:
            raise ContractError(
                f"Video aspect ratio {width}x{height} does not match canonical "
                f"{source_width}x{source_height}"
            )
        crop_x = 0
        crop_y = 0
        crop_width = source_width
        crop_height = source_height
    else:
        if target_aspect < source_aspect:
            crop_width = max(
                1,
                min(source_width, int(round(source_height * target_aspect))),
            )
            crop_height = source_height
            crop_x = (source_width - crop_width) // 2
            crop_y = 0
        else:
            crop_width = source_width
            crop_height = max(
                1,
                min(source_height, int(round(source_width / target_aspect))),
            )
            crop_x = 0
            crop_y = (source_height - crop_height) // 2

    crop_rgb = seeds.reference_rgb[
        crop_y : crop_y + crop_height,
        crop_x : crop_x + crop_width,
    ]
    crop_mask = seeds.canonical_mask[
        crop_y : crop_y + crop_height,
        crop_x : crop_x + crop_width,
    ]
    rgb_interpolation = (
        cv2.INTER_LINEAR
        if mode == REFERENCE_GEOMETRY_CENTER_CROP
        else cv2.INTER_AREA
    )
    reference = cv2.resize(
        crop_rgb,
        (width, height),
        interpolation=rgb_interpolation,
    )
    mask = cv2.resize(
        crop_mask.astype(np.uint8),
        (width, height),
        interpolation=cv2.INTER_NEAREST,
    ).astype(bool)
    scale = np.asarray(
        (width / crop_width, height / crop_height), dtype=np.float32
    )
    offset = np.asarray((crop_x, crop_y), dtype=np.float32)
    if mode == REFERENCE_GEOMETRY_CENTER_CROP:
        points = np.asarray(
            (seeds.points_xy - offset + 0.5) * scale - 0.5,
            dtype=np.float32,
        )
        coordinate_transform = "half_pixel_centers"
        rgb_interpolation_name = "opencv_bilinear"
    else:
        points = np.asarray((seeds.points_xy - offset) * scale, dtype=np.float32)
        coordinate_transform = "legacy_edge_scale"
        rgb_interpolation_name = "opencv_inter_area"
    geometry = {
        "mode": mode,
        "source_resolution": [source_width, source_height],
        "target_resolution": [width, height],
        "crop_pixels": {
            "x": crop_x,
            "y": crop_y,
            "width": crop_width,
            "height": crop_height,
        },
        "scale_xy": [float(scale[0]), float(scale[1])],
        "coordinate_transform": coordinate_transform,
        "rgb_interpolation": rgb_interpolation_name,
        "mask_interpolation": "opencv_nearest",
    }
    return reference, mask, points, geometry


def _align_seeds(
    seeds: SeedSet,
    frame_bgr: np.ndarray,
    *,
    reference_geometry_mode: str = REFERENCE_GEOMETRY_ASPECT_STRICT,
) -> tuple[SeedSet, dict[str, float]]:
    try:
        import cv2
    except ImportError as exc:
        raise DependencyUnavailableError(
            "opencv-python-headless is required for alignment"
        ) from exc
    height, width = frame_bgr.shape[:2]
    reference, mask, points, geometry = _reference_geometry_transform(
        seeds,
        width=width,
        height=height,
        mode=reference_geometry_mode,
    )
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
    aligned = replace(
        seeds,
        points_xy=points,
        canonical_mask=mask,
        reference_rgb=reference,
        reference_provenance={
            **seeds.reference_provenance,
            "geometry_transform": geometry,
        },
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
        failures.append(
            "browser_first_frame_alignment"
            if seeds.reference_provenance.get("mode")
            == "browser_static_scene_override"
            else "canonical_first_frame_alignment"
        )
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
    browser_endpoint_guide_bundle: str | Path | None = None,
    browser_endpoint_guide_manifest_sha256: str | None = None,
    config: ObservationRuntimeConfig | None = None,
    ffprobe: str | None = None,
) -> Path:
    """Create an atomic, optimizer-compatible observation bundle or fail.

    A rejected run never leaves ``observations.json`` at the requested output
    path. The caller must choose a fresh output directory for each candidate.
    """

    cfg = config or ObservationRuntimeConfig()
    cfg.validate()
    browser_reference = _browser_reference_arguments(
        bundle=browser_endpoint_guide_bundle,
        manifest_sha256=browser_endpoint_guide_manifest_sha256,
    )
    if browser_reference is not None and not cfg.loop:
        raise ContractError(
            "Browser endpoint guide reference override is accepted only for loop observations"
        )
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
        browser_endpoint_guide_bundle=browser_endpoint_guide_bundle,
        browser_endpoint_guide_manifest_sha256=(
            browser_endpoint_guide_manifest_sha256
        ),
        loop=cfg.loop,
    )
    if browser_reference is not None:
        expected_frames = seeds.reference_provenance["selected"]["manifest"][
            "cycle_frame_count"
        ]
        if source_video.frame_count != expected_frames:
            raise ContractError(
                "Browser endpoint guide cycle length does not match the video: "
                f"manifest={expected_frames}, video={source_video.frame_count}"
            )
    aligned_seeds, alignment = _align_seeds(
        seeds,
        source_video.frames_bgr[0],
        reference_geometry_mode=cfg.reference_geometry_mode,
    )
    if alignment["combined_correlation"] < cfg.min_alignment_correlation:
        reference_label = (
            "the pinned browser static-scene endpoint guide"
            if aligned_seeds.reference_provenance.get("mode")
            == "browser_static_scene_override"
            else "the canonical actionless render"
        )
        raise ContractError(
            f"First video frame does not match {reference_label}: "
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
            "first_frame_reference": aligned_seeds.reference_provenance,
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
