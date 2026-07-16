from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
import subprocess

from jsonschema import Draft202012Validator
import numpy as np
from PIL import Image, ImageDraw
import pytest

import animation_fitting.tracking_runtime.core as tracking_core
import animation_fitting.tracking_runtime.cli as tracking_cli
from animation_fitting.errors import (
    ContractError,
    DependencyUnavailableError,
    FittingError,
)
from animation_fitting.observations import load_observations
from animation_fitting.tracking_runtime.core import (
    ObservationRuntimeConfig,
    REFERENCE_GEOMETRY_ASPECT_STRICT,
    REFERENCE_GEOMETRY_CENTER_CROP,
    run_observation_pipeline,
    select_anchor_seeds,
)
from animation_fitting.tracking_runtime.models import (
    DepthResult,
    MaskResult,
    TrackResult,
)
from animation_fitting.tracking_runtime.official_backends import _torch
from animation_fitting.tracking_runtime.runtime_lock import (
    CheckpointPin,
    RepoPin,
    RuntimeLock,
    load_runtime_lock,
)
from animation_fitting.rig import load_rig_bundle


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact_record(path: Path) -> dict:
    return {"filename": path.name, "bytes": path.stat().st_size, "sha256": _sha(path)}


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_immutable_manifest(bundle: Path, artifacts: list[Path]) -> None:
    paths = [bundle / "fitting_bundle.json", *artifacts]
    _write_json(
        bundle / "immutable_manifest.json",
        {
            "schema": "autorig-fitting-immutable-bundle.v1",
            "files": [
                _artifact_record(path)
                for path in sorted(paths, key=lambda item: item.name)
            ],
        },
    )


def _flat(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in matrix.reshape(-1)]


def _bundle(root: Path) -> tuple[Path, np.ndarray]:
    bundle = root / "horse_bundle"
    bundle.mkdir()
    identity = np.eye(4)
    bones = [
        {
            "name": "HorseBody",
            "parent": None,
            "use_deform": True,
            "helper": False,
            "length": 1.0,
            "parent_relative_matrix": _flat(identity),
            "joint_limits": [],
        }
    ]
    vertices = []
    anchor_groups = []
    width, height = 320, 240
    world_to_camera = np.asarray(
        (
            (1.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 1.0, 0.0),
            (0.0, 1.0, 0.0, -8.0),
            (0.0, 0.0, 0.0, 1.0),
        )
    )
    fx = fy = 260.0
    cx, cy = width / 2.0, height / 2.0
    projected = []
    for index in range(12):
        bone = f"HorseSemantic{index:02d}"
        bones.append(
            {
                "name": bone,
                "parent": "HorseBody",
                "use_deform": True,
                "helper": False,
                "length": 0.2,
                "parent_relative_matrix": _flat(identity),
                "joint_limits": [],
            }
        )
        column, row = index % 4, index // 4
        world = ((column - 1.5) * 0.85, 0.0, (1.0 - row) * 0.65)
        vertex_id = index
        vertices.append(
            {
                "vertex_id": vertex_id,
                "world": list(world),
                "local": list(world),
                "weights": [{"bone": bone, "weight": 1.0}],
            }
        )
        anchor_groups.append(
            {
                "bone": bone,
                "points": [
                    {
                        "id": f"semantic_anchor_{index:02d}",
                        "vertex_id": vertex_id,
                        "weight": 1.0,
                        "world": list(world),
                    }
                ],
            }
        )
        camera = world_to_camera @ np.asarray((*world, 1.0))
        depth = -camera[2]
        projected.append((fx * camera[0] / depth + cx, cy - fy * camera[1] / depth))
    skeleton_path = bundle / "skeleton.json"
    _write_json(
        skeleton_path,
        {
            "armatures": [
                {
                    "name": "SyntheticHorse",
                    "matrix_world": _flat(identity),
                    "bones": bones,
                }
            ]
        },
    )
    skin_path = bundle / "skin_weights.json.gz"
    with gzip.open(skin_path, "wt", encoding="utf-8") as stream:
        json.dump({"vertices": vertices}, stream)
    anchors_path = bundle / "surface_anchors.json"
    _write_json(anchors_path, {"bones": anchor_groups})
    mask_image = Image.new("L", (width, height), color=0)
    mask_draw = ImageDraw.Draw(mask_image)
    for x, y in projected:
        mask_draw.ellipse((x - 9, y - 9, x + 9, y + 9), fill=255)
    mask_path = bundle / "reference_mask.png"
    mask_image.save(mask_path)
    reference = Image.new("RGB", (width, height), color=(10, 15, 22))
    reference_draw = ImageDraw.Draw(reference)
    for index, (x, y) in enumerate(projected):
        color = (180 + index * 5, 95 + index * 8, 55 + index * 6)
        reference_draw.ellipse((x - 9, y - 9, x + 9, y + 9), fill=color)
    rgb_path = bundle / "reference_rgb.png"
    reference.save(rgb_path)
    metadata = {
        "schema": "autorig-actionless-fitting-bundle.v1",
        "source": {
            "sha256": "a" * 64,
            "rig_type": "HORSE_2",
        },
        "actionless": {"actionless": True},
        "camera": {
            "resolution": [width, height],
            "intrinsics": {"fx": fx, "fy": fy, "cx": cx, "cy": cy},
            "world_to_camera": _flat(world_to_camera),
        },
        "ground_plane": {"normal": [0.0, 0.0, 1.0], "height": 0.0},
        "artifacts": {
            "skeleton": _artifact_record(skeleton_path),
            "skin_weights": _artifact_record(skin_path),
            "surface_anchors": _artifact_record(anchors_path),
            "rgb": _artifact_record(rgb_path),
            "mask": _artifact_record(mask_path),
        },
    }
    _write_json(bundle / "fitting_bundle.json", metadata)
    _write_immutable_manifest(
        bundle,
        [skeleton_path, skin_path, anchors_path, rgb_path, mask_path],
    )
    return bundle, np.asarray(reference)


def _browser_guide_bundle(
    root: Path, bundle: Path, canonical_rgb: np.ndarray
) -> tuple[Path, np.ndarray, str]:
    guide = root / "browser_guides"
    guide.mkdir()
    browser_rgb = 255 - canonical_rgb
    first = guide / "guide_000.png"
    Image.fromarray(browser_rgb, mode="RGB").save(first)
    last = guide / "guide_002.png"
    last.write_bytes(first.read_bytes())
    metadata = json.loads((bundle / "fitting_bundle.json").read_text(encoding="utf-8"))
    endpoint_sha256 = _sha(first)
    endpoint_bytes = first.stat().st_size
    manifest = {
        "schema": "autorig-browser-ltx-static-scene-guide-bundle.v1",
        "status": "PASS",
        "approvedForAnimationLibrary": False,
        "browserOnly": True,
        "blenderUsed": False,
        "rigType": metadata["source"]["rig_type"],
        "resolution": [browser_rgb.shape[1], browser_rgb.shape[0]],
        "source_reference_sha256_string": metadata["artifacts"]["rgb"]["sha256"],
        "source_reference_is_guide_bool": False,
        "endpoint_guide_sha256_string": endpoint_sha256,
        "cycle_frame_count_int": 3,
        "guide_count_int": 2,
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "scene_contract_string": "v11_unified_browser_static_scene_v1",
            "all_guide_frames_browser_rendered_bool": True,
            "blender_used_bool": False,
            "shadows_enabled_bool": False,
        },
        "frames_array": [
            {
                "frame_index_int": 0,
                "filename_string": first.name,
                "sha256_string": endpoint_sha256,
                "bytes_int": endpoint_bytes,
                "strength_float": 0.8,
            },
            {
                "frame_index_int": 2,
                "filename_string": last.name,
                "sha256_string": endpoint_sha256,
                "bytes_int": endpoint_bytes,
                "strength_float": 0.8,
            },
        ],
        "source": {
            "sourceModelSha256": metadata["source"]["sha256"],
            "immutableManifest": {
                "filename": "immutable_manifest.json",
                "bytes": (bundle / "immutable_manifest.json").stat().st_size,
                "sha256": _sha(bundle / "immutable_manifest.json"),
            },
            "fittingBundle": {
                "filename": "fitting_bundle.json",
                "bytes": (bundle / "fitting_bundle.json").stat().st_size,
                "sha256": _sha(bundle / "fitting_bundle.json"),
            },
            "referenceRgb": {
                "filename": metadata["artifacts"]["rgb"]["filename"],
                "bytes": metadata["artifacts"]["rgb"]["bytes"],
                "sha256": metadata["artifacts"]["rgb"]["sha256"],
            },
        },
        "staticSceneQa": {
            "schema": "autorig-browser-static-scene-qa.v1",
            "status": "PASS",
            "decoded_rgb_statistics_bool": True,
            "endpoint_byte_identical_bool": True,
        },
        "staticSceneRenderer": {
            "contract": "v11_unified_browser_static_scene_v1",
        },
    }
    manifest_path = guide / "immutable_manifest.json"
    _write_json(manifest_path, manifest)
    return guide, browser_rgb, _sha(manifest_path)


def _browser_recovery_guide_bundle(
    root: Path, bundle: Path, canonical_rgb: np.ndarray
) -> tuple[Path, np.ndarray, str]:
    guide = root / "browser_recovery_guides"
    guide.mkdir()
    browser_rgb = 255 - canonical_rgb
    metadata = json.loads((bundle / "fitting_bundle.json").read_text(encoding="utf-8"))
    frame_indices = (0, 6, 12, 18, 24, 30, 36, 42, 48)
    recovery_indices = (12, 24, 36)
    swing_limbs = {
        6: "hind_left",
        18: "fore_left",
        30: "hind_right",
        42: "fore_right",
    }
    limbs = ("hind_left", "fore_left", "hind_right", "fore_right")

    endpoint = guide / "guide_000.png"
    Image.fromarray(browser_rgb, mode="RGB").save(endpoint)
    endpoint_payload = endpoint.read_bytes()
    for frame_index in frame_indices[1:]:
        target = guide / f"guide_{frame_index:03d}.png"
        if frame_index in recovery_indices or frame_index == 48:
            target.write_bytes(endpoint_payload)
            continue
        swing_rgb = browser_rgb.copy()
        swing_rgb[0, frame_index // 6, 0] = (
            int(swing_rgb[0, frame_index // 6, 0]) + frame_index
        ) % 256
        Image.fromarray(swing_rgb, mode="RGB").save(target)

    endpoint_sha256 = _sha(endpoint)
    frames = []
    cue_guides = []
    post_bake_guides = []
    for frame_index in frame_indices:
        path = guide / f"guide_{frame_index:03d}.png"
        swing_limb = swing_limbs.get(frame_index)
        visible_limbs = [limb for limb in limbs if limb != swing_limb]
        hidden_limbs = [] if swing_limb is None else [swing_limb]
        frames.append(
            {
                "frame_index_int": frame_index,
                "filename_string": path.name,
                "sha256_string": _sha(path),
                "bytes_int": path.stat().st_size,
                "strength_float": (
                    0.85
                    if frame_index in recovery_indices
                    else 0.7
                    if swing_limb is not None
                    else 0.8
                ),
            }
        )
        cue_guides.append(
            {
                "frameIndex": frame_index,
                "swingLimb": swing_limb,
                "visibleLimbs": visible_limbs,
                "hiddenLimbs": hidden_limbs,
                "visibleCueCount": len(visible_limbs),
                "hiddenCueCount": len(hidden_limbs),
                "exactlyMatchesStance": True,
            }
        )
        post_bake_guides.append(
            {
                "frameIndex": frame_index,
                "swingLimb": swing_limb,
                "stanceHoofCount": len(visible_limbs),
            }
        )

    manifest = {
        "schema": "autorig-browser-ltx-recovery-guide-bundle.v1",
        "status": "PASS",
        "approvedForAnimationLibrary": False,
        "browserOnly": True,
        "blenderUsed": False,
        "rigType": metadata["source"]["rig_type"],
        "resolution": [browser_rgb.shape[1], browser_rgb.shape[0]],
        "source_reference_sha256_string": metadata["artifacts"]["rgb"]["sha256"],
        "source_reference_is_guide_bool": False,
        "endpoint_guide_sha256_string": endpoint_sha256,
        "cycle_frame_count_int": 49,
        "guide_count_int": len(frame_indices),
        "recovery_frame_indices_array": list(recovery_indices),
        "recovery_guides_byte_identical_endpoint_bool": True,
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "scene_contract_string": "v12_unified_browser_recovery_guides_v1",
            "all_guide_frames_browser_rendered_bool": True,
            "blender_used_bool": False,
            "shadows_enabled_bool": False,
            "deterministic_contact_cues_bool": True,
            "per_guide_contact_cue_visibility_bool": True,
            "contact_cue_implementation_string": (
                "static_rest_hoof_radial_alpha_planes"
            ),
        },
        "frames_array": frames,
        "source": {
            "sourceModelSha256": metadata["source"]["sha256"],
            "immutableManifest": {
                "filename": "immutable_manifest.json",
                "bytes": (bundle / "immutable_manifest.json").stat().st_size,
                "sha256": _sha(bundle / "immutable_manifest.json"),
            },
            "fittingBundle": {
                "filename": "fitting_bundle.json",
                "bytes": (bundle / "fitting_bundle.json").stat().st_size,
                "sha256": _sha(bundle / "fitting_bundle.json"),
            },
            "referenceRgb": {
                "filename": metadata["artifacts"]["rgb"]["filename"],
                "bytes": metadata["artifacts"]["rgb"]["bytes"],
                "sha256": metadata["artifacts"]["rgb"]["sha256"],
            },
        },
        "staticSceneQa": {
            "schema": "autorig-browser-static-scene-qa.v1",
            "status": "PASS",
            "expected_frame_indices_array": list(frame_indices),
            "decoded_rgb_statistics_bool": True,
            "endpoint_byte_identical_bool": True,
        },
        "staticSceneRenderer": {
            "contract": "v12_unified_browser_recovery_guides_v1",
            "contactCues": {
                "enabled": True,
                "implementation": "static_rest_hoof_radial_alpha_planes",
                "count": 4,
                "shadowMapUsed": False,
                "perGuideVisibility": True,
            },
        },
        "contactCueQa": {
            "schema": "autorig-browser-contact-cue-visibility-qa.v1",
            "status": "PASS",
            "perGuideVisibility": True,
            "swingGuidesHideExactlyOneCue": True,
            "stanceGuidesShowAllFourCues": True,
            "guides": cue_guides,
        },
        "postBakeQa": {
            "status": "PASS",
            "hierarchyBakeVerified": True,
            "minimumStanceHooves": 3,
            "recoveryGuideCount": 3,
            "guides": post_bake_guides,
        },
    }
    manifest_path = guide / "immutable_manifest.json"
    _write_json(manifest_path, manifest)
    return guide, browser_rgb, _sha(manifest_path)


def _browser_interval_guide_bundle(
    root: Path, bundle: Path, canonical_rgb: np.ndarray
) -> tuple[Path, np.ndarray, str]:
    guide = root / "browser_interval_guides"
    guide.mkdir()
    browser_rgb = 255 - canonical_rgb
    metadata = json.loads((bundle / "fitting_bundle.json").read_text(encoding="utf-8"))
    frame_indices = tuple(range(49))
    source_anchor_indices = (0, 6, 12, 18, 24, 30, 36, 42, 48)
    barrier_indices = (0, 12, 24, 36, 48)
    recovery_indices = (12, 24, 36)
    limbs = ("hind_left", "fore_left", "hind_right", "fore_right")

    endpoint = guide / "guide_000.png"
    Image.fromarray(browser_rgb, mode="RGB").save(endpoint)
    endpoint_payload = endpoint.read_bytes()
    frame_arrays: dict[int, np.ndarray] = {0: browser_rgb.copy()}
    for frame_index in frame_indices[1:]:
        target = guide / f"guide_{frame_index:03d}.png"
        if frame_index in barrier_indices:
            target.write_bytes(endpoint_payload)
            frame_arrays[frame_index] = browser_rgb.copy()
            continue
        frame = browser_rgb.copy()
        column = 1 + (frame_index % (frame.shape[1] - 1))
        frame[0, column, 0] = (int(frame[0, column, 0]) + frame_index) % 256
        Image.fromarray(frame, mode="RGB").save(target)
        frame_arrays[frame_index] = frame

    def swing_limb(frame_index: int) -> str | None:
        if 1 <= frame_index <= 11:
            return "hind_left"
        if 13 <= frame_index <= 23:
            return "fore_left"
        if 25 <= frame_index <= 35:
            return "hind_right"
        if 37 <= frame_index <= 47:
            return "fore_right"
        return None

    frames = []
    guides = []
    cue_frames = []
    post_bake_frames = []
    decoded_pins = []
    for frame_index in frame_indices:
        path = guide / f"guide_{frame_index:03d}.png"
        decoded_sha256 = hashlib.sha256(
            np.ascontiguousarray(frame_arrays[frame_index]).tobytes()
        ).hexdigest()
        decoded_pins.append(decoded_sha256)
        is_source_anchor = frame_index in source_anchor_indices
        swing = swing_limb(frame_index)
        visible_limbs = [limb for limb in limbs if limb != swing]
        hidden_limbs = [] if swing is None else [swing]
        frames.append(
            {
                "frame_index_int": frame_index,
                "filename_string": path.name,
                "sha256_string": _sha(path),
                "bytes_int": path.stat().st_size,
                "decoded_rgb_sha256_string": decoded_sha256,
                "source_anchor_byte_identical_bool": is_source_anchor,
            }
        )
        guides.append(
            {
                "filename": path.name,
                "bytes": path.stat().st_size,
                "sha256": _sha(path),
                "frameIndex": frame_index,
                "role": "interval_fixture",
                "swingLimb": swing,
                "width": browser_rgb.shape[1],
                "height": browser_rgb.shape[0],
                "decodedRgbSha256": decoded_sha256,
                "sourceAnchorByteIdentical": is_source_anchor,
                "renderSource": "browser_threejs",
                "byteIdenticalReferenceCopy": False,
            }
        )
        cue_frames.append(
            {
                "frameIndex": frame_index,
                "swingLimb": swing,
                "visibleLimbs": visible_limbs,
                "hiddenLimbs": hidden_limbs,
                "visibleCueCount": len(visible_limbs),
                "hiddenCueCount": len(hidden_limbs),
                "exactlyMatchesStance": True,
            }
        )
        post_bake_frames.append(
            {
                "frameIndex": frame_index,
                "swingLimb": swing,
                "stanceHoofCount": len(visible_limbs),
            }
        )

    interval_video = guide / "interval_guide.mkv"
    interval_video.write_bytes(b"synthetic-png-in-matroska-interval-guide")
    pose_contract = guide / "pose_contract.json"
    _write_json(pose_contract, {"schema": "synthetic-v14-pose-contract.v1"})
    endpoint_sha256 = _sha(endpoint)
    source_anchors = [
        {
            "filename": frames[index]["filename_string"],
            "bytes": frames[index]["bytes_int"],
            "sha256": frames[index]["sha256_string"],
            "frameIndex": index,
        }
        for index in source_anchor_indices
    ]
    manifest = {
        "schema": "autorig-browser-ltx-interval-guide-bundle.v1",
        "status": "PASS",
        "approvedForAnimationLibrary": False,
        "browserOnly": True,
        "blenderUsed": False,
        "rigType": metadata["source"]["rig_type"],
        "resolution": [browser_rgb.shape[1], browser_rgb.shape[0]],
        "source_reference_sha256_string": metadata["artifacts"]["rgb"]["sha256"],
        "source_reference_is_guide_bool": False,
        "endpoint_guide_sha256_string": endpoint_sha256,
        "cycle_frame_count_int": 49,
        "guide_count_int": 1,
        "browser_frame_count_int": 49,
        "recovery_frame_indices_array": list(recovery_indices),
        "recovery_guides_byte_identical_endpoint_bool": None,
        "source_anchor_frame_indices_array": list(source_anchor_indices),
        "source_anchors_byte_identical_bool": True,
        "interval_guide_video_object": {
            "filename": interval_video.name,
            "bytes": interval_video.stat().st_size,
            "sha256": _sha(interval_video),
            "container": "matroska",
            "codec": "png",
            "pixelFormat": "rgb24",
            "width": browser_rgb.shape[1],
            "height": browser_rgb.shape[0],
            "frameRate": 30,
            "frameCount": 49,
            "audioStreamCount": 0,
            "decoded_rgb_sha256_array": decoded_pins,
            "exact_browser_frame_rgb_bool": True,
            "load_video_node_compatible_bool": True,
        },
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "scene_contract_string": "v14_unified_browser_interval_guide_v1",
            "all_guide_frames_browser_rendered_bool": True,
            "blender_used_bool": False,
            "shadows_enabled_bool": False,
            "deterministic_contact_cues_bool": True,
            "per_guide_contact_cue_visibility_bool": False,
            "per_frame_contact_cue_visibility_bool": True,
            "contact_cue_implementation_string": (
                "static_rest_hoof_radial_alpha_planes"
            ),
        },
        "frames_array": frames,
        "source": {
            "sourceModelSha256": metadata["source"]["sha256"],
            "immutableManifest": {
                "filename": "immutable_manifest.json",
                "bytes": (bundle / "immutable_manifest.json").stat().st_size,
                "sha256": _sha(bundle / "immutable_manifest.json"),
            },
            "fittingBundle": {
                "filename": "fitting_bundle.json",
                "bytes": (bundle / "fitting_bundle.json").stat().st_size,
                "sha256": _sha(bundle / "fitting_bundle.json"),
            },
            "referenceRgb": {
                "filename": metadata["artifacts"]["rgb"]["filename"],
                "bytes": metadata["artifacts"]["rgb"]["bytes"],
                "sha256": metadata["artifacts"]["rgb"]["sha256"],
            },
        },
        "sourceGuideBundle": {
            "bundleId": "horse-walk-v12-browser-recovery-guides-f2",
            "immutableManifest": {
                "filename": "immutable_manifest.json",
                "bytes": 1,
                "sha256": tracking_core.V12_BROWSER_GUIDE_MANIFEST_SHA256,
            },
            "poseContract": {
                "filename": "pose_contract.json",
                "bytes": 1,
                "sha256": "1" * 64,
            },
            "anchorFrames": source_anchors,
        },
        "staticSceneQa": {
            "schema": "autorig-browser-static-scene-qa.v1",
            "status": "PASS",
            "expected_frame_indices_array": list(frame_indices),
            "decoded_rgb_statistics_bool": True,
            "endpoint_byte_identical_bool": True,
        },
        "staticSceneRenderer": {
            "contract": "v14_unified_browser_interval_guide_v1",
            "contactCues": {
                "enabled": True,
                "implementation": "static_rest_hoof_radial_alpha_planes",
                "count": 4,
                "shadowMapUsed": False,
                "perGuideVisibility": True,
            },
        },
        "contactCueQa": {
            "schema": "autorig-browser-contact-cue-visibility-qa.v1",
            "status": "PASS",
            "perFrameVisibility": True,
            "swingFramesHideExactlyOneCue": True,
            "barrierFramesShowAllFourCues": True,
            "frames": cue_frames,
        },
        "postBakeQa": {
            "status": "PASS",
            "hierarchyBakeVerified": True,
            "frameCount": 49,
            "minimumStanceHooves": 3,
            "frames": post_bake_frames,
        },
        "deterministicRenderQa": {
            "schema": "autorig-browser-deterministic-rerender-qa.v1",
            "status": "PASS",
            "frameCount": 49,
            "byteIdenticalFrameCount": 49,
            "mismatchFrameIndices": [],
        },
        "losslessVideoQa": {
            "schema": "autorig-browser-lossless-interval-video-qa.v1",
            "status": "PASS",
            "frameCount": 49,
            "codec": "png",
            "pixelFormat": "rgb24",
            "decodedRgbSha256MatchesBrowserFrames": True,
        },
        "poseContract": _artifact_record(pose_contract),
        "guides": guides,
    }
    manifest_path = guide / "immutable_manifest.json"
    _write_json(manifest_path, manifest)
    return guide, browser_rgb, _sha(manifest_path)


def _authorize_browser_manifest(
    monkeypatch: pytest.MonkeyPatch, manifest_sha256: str
) -> None:
    monkeypatch.setattr(
        tracking_core,
        "AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256",
        frozenset({manifest_sha256}),
    )


def _upgrade_bundle_to_v2_camera_z(bundle: Path) -> None:
    metadata_path = bundle / "fitting_bundle.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    width, height = metadata["camera"]["resolution"]
    mask_path = bundle / metadata["artifacts"]["mask"]["filename"]
    mask = np.zeros((height, width), dtype=np.uint8)
    mask[40:201, 40:281] = 255
    Image.fromarray(mask, mode="L").save(mask_path)
    yy = np.linspace(0.1, 1.0, height, dtype=np.float32)[:, None]
    relative = np.repeat(yy, width, axis=1)
    camera_z = np.where(mask > 0, 1.2 + 2.3 * relative, np.nan).astype(np.float32)
    camera_z_path = bundle / "reference_camera_z.npy"
    np.save(camera_z_path, camera_z)
    depth_path = bundle / "reference_depth.npy"
    np.save(depth_path, np.where(mask > 0, camera_z, 0.0).astype(np.float32))
    face_id_path = bundle / "reference_face_id.png"
    Image.fromarray((mask > 0).astype(np.uint8), mode="L").save(face_id_path)
    topology_path = bundle / "surface_topology.json.gz"
    with gzip.open(topology_path, "wt", encoding="utf-8") as stream:
        json.dump({"faces": []}, stream)
    finite = camera_z[np.isfinite(camera_z)]
    metadata["revision"] = "autorig_actionless_bundle_v2"
    metadata["counts"] = {"armatures": 1}
    metadata["camera"]["camera_z_contract"] = {
        "mode": "positive_camera_z",
        "dtype": "float32",
        "invalid": "NaN",
        "shape": [height, width],
        "valid_pixels": int(finite.size),
        "minimum": float(np.min(finite)),
        "median": float(np.median(finite)),
        "maximum": float(np.max(finite)),
    }
    metadata["artifacts"]["mask"] = _artifact_record(mask_path)
    metadata["artifacts"].update(
        {
            "camera_z": _artifact_record(camera_z_path),
            "depth": _artifact_record(depth_path),
            "face_id": _artifact_record(face_id_path),
            "surface_topology": _artifact_record(topology_path),
        }
    )
    _write_json(metadata_path, metadata)
    artifact_paths = [
        bundle / record["filename"] for record in metadata["artifacts"].values()
    ]
    _write_immutable_manifest(bundle, artifact_paths)


def _video(path: Path, rgb: np.ndarray) -> None:
    import cv2

    height, width = rgb.shape[:2]
    writer = cv2.VideoWriter(
        str(path), cv2.VideoWriter_fourcc(*"mp4v"), 24.0, (width, height)
    )
    assert writer.isOpened()
    bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
    for _ in range(3):
        writer.write(bgr)
    writer.release()


class _Tracker:
    def track(self, video, seeds):
        points = np.repeat(seeds.points_xy[None, :, :], video.frame_count, axis=0)
        return TrackResult(
            points_xy=points,
            visible=np.ones(points.shape[:2], dtype=bool),
            confidence=np.full(points.shape[:2], 0.95, dtype=np.float32),
            provenance={"backend": "deterministic-test"},
        )


class _ZeroConfidenceTracker(_Tracker):
    def track(self, video, seeds):
        result = super().track(video, seeds)
        return TrackResult(
            points_xy=result.points_xy,
            visible=result.visible,
            confidence=np.zeros_like(result.confidence),
            provenance={"backend": "zero-confidence-test"},
        )


class _Segmenter:
    def segment(self, video, initial_mask):
        return MaskResult(
            masks=np.repeat(initial_mask[None, :, :], video.frame_count, axis=0),
            provenance={"backend": "deterministic-test"},
        )


class _Depth:
    def infer(self, video):
        y = np.linspace(0.1, 1.0, video.height, dtype=np.float32)[:, None]
        depth = np.repeat(y, video.width, axis=1)
        return DepthResult(
            relative_depth=np.repeat(depth[None, :, :], video.frame_count, axis=0),
            provenance={"backend": "deterministic-test", "metric": False},
        )


def test_runtime_lock_pins_official_apache_repositories() -> None:
    lock = load_runtime_lock()
    assert lock.repos["tapnet"].commit == "bb3fd2720260ce383933f9bbd141c73854dfff1f"
    assert lock.repos["sam2"].url == "https://github.com/facebookresearch/sam2.git"
    assert lock.repos["video_depth_anything"].license == "LICENSE"
    assert lock.checkpoints["video_depth_anything_small"].bytes == 116440756
    assert lock.checkpoints["tapnextpp"].license_source_repo == "tapnet"


def test_runtime_lock_rejects_dirty_license_source_repo(tmp_path: Path) -> None:
    repo = tmp_path / "official"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "AutoRig Test"], check=True
    )
    license_path = repo / "LICENSE"
    license_path.write_text("Apache License 2.0 fixture\n", encoding="utf-8")
    (repo / "model.py").write_text("MODEL = 'fixture'\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "LICENSE", "model.py"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"], check=True
    )
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"official release fixture")
    lock = RuntimeLock(
        path=tmp_path / "runtime-lock.json",
        repos={
            "official": RepoPin(
                name="official",
                url="https://example.invalid/official.git",
                commit=head,
                license="LICENSE",
                license_sha256=_sha(license_path),
            )
        },
        checkpoints={
            "model": CheckpointPin(
                name="model",
                url="https://example.invalid/model.pt",
                sha256=_sha(checkpoint),
                bytes=checkpoint.stat().st_size,
                license_source_repo="official",
            )
        },
        python={},
    )
    provenance = lock.verify_checkpoint("model", checkpoint, license_repo=repo)
    assert provenance["license_source_repo"] == "official"
    assert provenance["license_source_repo_provenance"]["commit"] == head
    assert "not_a_separate_weights_license" in provenance["license_claim"]

    (repo / "dirty.txt").write_text("untracked\n", encoding="utf-8")
    with pytest.raises(ContractError, match="worktree is not clean"):
        lock.verify_repo("official", repo)
    with pytest.raises(ContractError, match="worktree is not clean"):
        lock.verify_checkpoint("model", checkpoint, license_repo=repo)


def test_conflicting_cublas_workspace_config_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("CUBLAS_WORKSPACE_CONFIG", ":16:8")
    with pytest.raises(DependencyUnavailableError, match="must be unset or exactly"):
        _torch("cpu", require_cuda=False)


def test_seed_selection_preserves_explicit_anchor_mapping(tmp_path: Path) -> None:
    bundle, _ = _bundle(tmp_path)
    seeds = select_anchor_seeds(bundle)
    assert len(seeds.track_ids) == 12
    assert len(set(seeds.anchor_ids)) == 12
    assert seeds.anchor_ids[0] == "semantic_anchor_00"
    assert np.all(
        seeds.canonical_mask[
            np.rint(seeds.points_xy[:, 1]).astype(int),
            np.rint(seeds.points_xy[:, 0]).astype(int),
        ]
    )


def test_priority_seed_selection_never_exceeds_max_tracks(tmp_path: Path) -> None:
    bundle, _ = _bundle(tmp_path)
    baseline = select_anchor_seeds(bundle)
    priority = baseline.anchor_ids[:4]

    seeds = select_anchor_seeds(
        bundle,
        max_tracks=len(priority),
        priority_anchor_ids=priority,
    )

    assert seeds.anchor_ids == priority
    assert len(seeds.track_ids) == len(priority)


def test_priority_seed_selection_places_requested_anchor_first(tmp_path: Path) -> None:
    bundle, _ = _bundle(tmp_path)
    baseline = select_anchor_seeds(bundle)
    requested = baseline.anchor_ids[-1]

    seeds = select_anchor_seeds(bundle, priority_anchor_ids=(requested,))

    assert seeds.anchor_ids[0] == requested
    assert len(seeds.anchor_ids) == len(baseline.anchor_ids)


def test_browser_endpoint_manifest_must_be_authoritatively_allowlisted(
    tmp_path: Path,
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_guide_bundle(tmp_path, bundle, canonical)

    with pytest.raises(ContractError, match="authoritative allowlist"):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=manifest_sha256,
            loop=True,
        )


def test_default_seed_selection_preserves_uppercase_bundle_sha_compatibility(
    tmp_path: Path,
) -> None:
    bundle, _ = _bundle(tmp_path)
    metadata_path = bundle / "fitting_bundle.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    expected_rgb_sha256 = metadata["artifacts"]["rgb"]["sha256"]
    metadata["artifacts"]["rgb"]["sha256"] = expected_rgb_sha256.upper()
    metadata["source"]["sha256"] = metadata["source"]["sha256"].upper()
    _write_json(metadata_path, metadata)
    _write_immutable_manifest(
        bundle,
        [
            bundle / "skeleton.json",
            bundle / "skin_weights.json.gz",
            bundle / "surface_anchors.json",
            bundle / "reference_rgb.png",
            bundle / "reference_mask.png",
        ],
    )

    load_rig_bundle(bundle)
    seeds = select_anchor_seeds(bundle)
    assert seeds.reference_provenance["mode"] == "canonical_bundle_rgb"
    assert seeds.reference_provenance["selected"]["sha256"] == expected_rgb_sha256


def test_browser_endpoint_reference_is_manifest_pinned_and_bundle_linked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, browser_rgb, manifest_sha256 = _browser_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    video = tmp_path / "browser-endpoint.mp4"
    _video(video, browser_rgb)

    with pytest.raises(ContractError, match="canonical actionless render"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=tmp_path / "canonical-rejected",
            tracker=_Tracker(),
            segmenter=_Segmenter(),
            config=ObservationRuntimeConfig(loop=True),
        )

    output = tmp_path / "browser-accepted"
    observations_path = run_observation_pipeline(
        video=video,
        bundle=bundle,
        output_dir=output,
        tracker=_Tracker(),
        segmenter=_Segmenter(),
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        config=ObservationRuntimeConfig(loop=True),
    )
    payload = json.loads(observations_path.read_text(encoding="utf-8"))
    provenance = payload["provenance"]["first_frame_reference"]
    assert provenance["mode"] == "browser_static_scene_override"
    assert provenance["selected"]["sha256"] == _sha(guide / "guide_000.png")
    assert provenance["selected"]["bytes"] == (guide / "guide_000.png").stat().st_size
    assert provenance["selected"]["manifest"]["sha256"] == manifest_sha256
    assert provenance["canonical_bundle"]["bundle_sha256"] == _sha(
        bundle / "fitting_bundle.json"
    )
    assert provenance["canonical_bundle"]["immutable_manifest_sha256"] == _sha(
        bundle / "immutable_manifest.json"
    )
    assert provenance["canonical_bundle"]["reference_rgb"]["sha256"] == _sha(
        bundle / "reference_rgb.png"
    )


def test_browser_endpoint_override_is_loop_only_and_cli_all_or_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, browser_rgb, manifest_sha256 = _browser_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    video = tmp_path / "browser-endpoint.mp4"
    _video(video, browser_rgb)
    output = tmp_path / "not-loop"
    with pytest.raises(ContractError, match="only for loop seed selection"):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=manifest_sha256,
        )
    with pytest.raises(ContractError, match="only for loop observations"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=output,
            tracker=_Tracker(),
            segmenter=_Segmenter(),
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=manifest_sha256,
        )
    assert not output.exists()

    args = tracking_cli._parser().parse_args(
        [
            "observe",
            "--video",
            str(video),
            "--bundle",
            str(bundle),
            "--output-dir",
            str(tmp_path / "unused"),
            "--browser-endpoint-guide-bundle",
            str(guide),
        ]
    )
    with pytest.raises(FittingError, match="requires both"):
        tracking_cli._browser_reference_cli_kwargs(args)


def test_browser_endpoint_manifest_cannot_be_repinned_to_another_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, _ = _browser_guide_bundle(tmp_path, bundle, canonical)
    manifest_path = guide / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_reference_sha256_string"] = "0" * 64
    _write_json(manifest_path, manifest)
    repinned_sha256 = _sha(manifest_path)
    _authorize_browser_manifest(monkeypatch, repinned_sha256)

    with pytest.raises(ContractError, match="canonical bundle RGB"):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=repinned_sha256,
            loop=True,
        )


def test_browser_endpoint_files_are_verified_from_manifest_pins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_guide_bundle(tmp_path, bundle, canonical)
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    first = guide / "guide_000.png"
    first.write_bytes(first.read_bytes() + b"tampered")

    with pytest.raises(ContractError, match="byte-size mismatch"):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=manifest_sha256,
            loop=True,
        )


def test_browser_endpoint_manifest_rejects_path_escape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, _ = _browser_guide_bundle(tmp_path, bundle, canonical)
    manifest_path = guide / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    escaped = tmp_path / "escaped.png"
    escaped.write_bytes((guide / "guide_000.png").read_bytes())
    manifest["frames_array"][0]["filename_string"] = "../escaped.png"
    _write_json(manifest_path, manifest)
    repinned_sha256 = _sha(manifest_path)
    _authorize_browser_manifest(monkeypatch, repinned_sha256)

    with pytest.raises(ContractError, match="escapes the browser guide bundle"):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=repinned_sha256,
            loop=True,
        )


def test_browser_endpoint_manifest_and_first_frame_are_each_read_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_guide_bundle(tmp_path, bundle, canonical)
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    manifest_path = (guide / "immutable_manifest.json").resolve()
    first_path = (guide / "guide_000.png").resolve()
    original = Path.read_bytes
    reads: dict[Path, int] = {}

    def counted(path: Path) -> bytes:
        resolved = path.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", counted)
    select_anchor_seeds(
        bundle,
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )
    assert reads[manifest_path] == 1
    assert reads[first_path] == 1


def test_v12_browser_recovery_endpoint_is_pinned_and_preserves_v11_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, browser_rgb, manifest_sha256 = _browser_recovery_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)

    seeds = select_anchor_seeds(
        bundle,
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    assert np.array_equal(seeds.reference_rgb, browser_rgb)
    selected = seeds.reference_provenance["selected"]
    assert selected["sha256"] == _sha(guide / "guide_000.png")
    assert selected["manifest"]["schema"] == (
        "autorig-browser-ltx-recovery-guide-bundle.v1"
    )
    assert selected["manifest"]["scene_contract"] == (
        "v12_unified_browser_recovery_guides_v1"
    )
    assert selected["manifest"]["cycle_frame_count"] == 49


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("recovery_indices", "recovery_frame_indices_array"),
        ("swing_cue_count", "must show 3 stance cues"),
        ("recovery_endpoint", "byte-identical pins"),
        ("path_escape", "escapes the browser guide bundle"),
    ),
)
def test_v12_browser_recovery_contract_rejects_repinned_semantic_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    error: str,
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, _ = _browser_recovery_guide_bundle(tmp_path, bundle, canonical)
    manifest_path = guide / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if mutation == "recovery_indices":
        manifest["recovery_frame_indices_array"] = [12, 24, 42]
    elif mutation == "swing_cue_count":
        row = next(
            row
            for row in manifest["contactCueQa"]["guides"]
            if row["frameIndex"] == 6
        )
        row["visibleCueCount"] = 4
    elif mutation == "recovery_endpoint":
        recovery_path = guide / "guide_012.png"
        recovery_path.write_bytes(recovery_path.read_bytes() + b"not-the-endpoint")
        row = next(
            row
            for row in manifest["frames_array"]
            if row["frame_index_int"] == 12
        )
        row["sha256_string"] = _sha(recovery_path)
        row["bytes_int"] = recovery_path.stat().st_size
    elif mutation == "path_escape":
        escaped = tmp_path / "escaped-v12.png"
        escaped.write_bytes((guide / "guide_006.png").read_bytes())
        row = next(
            row
            for row in manifest["frames_array"]
            if row["frame_index_int"] == 6
        )
        row["filename_string"] = "../escaped-v12.png"
    else:  # pragma: no cover - guarded by the parametrization
        raise AssertionError(mutation)
    _write_json(manifest_path, manifest)
    repinned_sha256 = _sha(manifest_path)
    _authorize_browser_manifest(monkeypatch, repinned_sha256)

    with pytest.raises(ContractError, match=error):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=repinned_sha256,
            loop=True,
        )


def test_v12_browser_manifest_and_all_nine_guides_are_read_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_recovery_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    manifest_path = (guide / "immutable_manifest.json").resolve()
    guide_paths = {
        (guide / f"guide_{frame_index:03d}.png").resolve()
        for frame_index in (0, 6, 12, 18, 24, 30, 36, 42, 48)
    }
    original = Path.read_bytes
    reads: dict[Path, int] = {}

    def counted(path: Path) -> bytes:
        resolved = path.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", counted)
    select_anchor_seeds(
        bundle,
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    assert reads[manifest_path] == 1
    assert {path: reads[path] for path in guide_paths} == {
        path: 1 for path in guide_paths
    }


def test_v12_exact_f2_bundle_validates_against_real_canonical_bundle() -> None:
    canonical_bundle = Path(
        r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1"
    )
    guide_bundle = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v12-browser-recovery-guides-f2"
    )
    if not canonical_bundle.is_dir() or not guide_bundle.is_dir():
        pytest.skip("Exact local v12 f2/canonical bundles are not installed")

    manifest_sha256 = (
        "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451"
    )
    assert _sha(guide_bundle / "immutable_manifest.json") == manifest_sha256
    seeds = select_anchor_seeds(
        canonical_bundle,
        browser_endpoint_guide_bundle=guide_bundle,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    selected = seeds.reference_provenance["selected"]
    assert selected["sha256"] == (
        "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3"
    )
    assert selected["manifest"]["schema"] == (
        "autorig-browser-ltx-recovery-guide-bundle.v1"
    )
    assert selected["manifest"]["scene_contract"] == (
        "v12_unified_browser_recovery_guides_v1"
    )


def test_v14_browser_interval_endpoint_validates_all_49_frames_and_video(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, browser_rgb, manifest_sha256 = _browser_interval_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)

    seeds = select_anchor_seeds(
        bundle,
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    assert np.array_equal(seeds.reference_rgb, browser_rgb)
    selected = seeds.reference_provenance["selected"]
    assert selected["sha256"] == _sha(guide / "guide_000.png")
    assert selected["manifest"]["schema"] == (
        "autorig-browser-ltx-interval-guide-bundle.v1"
    )
    assert selected["manifest"]["scene_contract"] == (
        "v14_unified_browser_interval_guide_v1"
    )
    assert selected["manifest"]["cycle_frame_count"] == 49


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("guide_count", "exactly one video guide"),
        ("cue_interval", "does not match its swing interval"),
        ("source_anchor", "does not match output"),
        ("interval_video", "mismatch"),
    ),
)
def test_v14_browser_interval_contract_rejects_semantic_or_file_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    error: str,
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_interval_guide_bundle(
        tmp_path, bundle, canonical
    )
    manifest_path = guide / "immutable_manifest.json"
    if mutation == "interval_video":
        (guide / "interval_guide.mkv").write_bytes(b"tampered-video")
    else:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if mutation == "guide_count":
            manifest["guide_count_int"] = 49
        elif mutation == "cue_interval":
            manifest["contactCueQa"]["frames"][1]["swingLimb"] = "fore_left"
        elif mutation == "source_anchor":
            manifest["sourceGuideBundle"]["anchorFrames"][1]["sha256"] = "2" * 64
        else:  # pragma: no cover - guarded by parametrization
            raise AssertionError(mutation)
        _write_json(manifest_path, manifest)
        manifest_sha256 = _sha(manifest_path)
    _authorize_browser_manifest(monkeypatch, manifest_sha256)

    with pytest.raises(ContractError, match=error):
        select_anchor_seeds(
            bundle,
            browser_endpoint_guide_bundle=guide,
            browser_endpoint_guide_manifest_sha256=manifest_sha256,
            loop=True,
        )


def test_v14_browser_manifest_frames_video_and_pose_are_each_read_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, canonical = _bundle(tmp_path)
    guide, _, manifest_sha256 = _browser_interval_guide_bundle(
        tmp_path, bundle, canonical
    )
    _authorize_browser_manifest(monkeypatch, manifest_sha256)
    expected_paths = {
        (guide / "immutable_manifest.json").resolve(),
        (guide / "interval_guide.mkv").resolve(),
        (guide / "pose_contract.json").resolve(),
        *{
            (guide / f"guide_{frame_index:03d}.png").resolve()
            for frame_index in range(49)
        },
    }
    original = Path.read_bytes
    reads: dict[Path, int] = {}

    def counted(path: Path) -> bytes:
        resolved = path.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", counted)
    select_anchor_seeds(
        bundle,
        browser_endpoint_guide_bundle=guide,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    assert {path: reads[path] for path in expected_paths} == {
        path: 1 for path in expected_paths
    }


def test_v14_exact_f1_bundle_validates_against_real_canonical_bundle() -> None:
    canonical_bundle = Path(r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1")
    guide_bundle = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v14-browser-interval-guide-f1"
    )
    if not canonical_bundle.is_dir() or not guide_bundle.is_dir():
        pytest.skip("Exact local v14 f1/canonical bundles are not installed")

    manifest_sha256 = tracking_core.V14_BROWSER_GUIDE_MANIFEST_SHA256
    assert _sha(guide_bundle / "immutable_manifest.json") == manifest_sha256
    seeds = select_anchor_seeds(
        canonical_bundle,
        browser_endpoint_guide_bundle=guide_bundle,
        browser_endpoint_guide_manifest_sha256=manifest_sha256,
        loop=True,
    )

    selected = seeds.reference_provenance["selected"]
    assert selected["sha256"] == (
        "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3"
    )
    assert selected["manifest"]["schema"] == (
        "autorig-browser-ltx-interval-guide-bundle.v1"
    )
    assert selected["manifest"]["scene_contract"] == (
        "v14_unified_browser_interval_guide_v1"
    )


def test_tracking_cli_help_names_v11_v12_and_v14_browser_guide_profiles() -> None:
    parser = tracking_cli._parser()
    subparsers = next(
        action
        for action in parser._actions
        if isinstance(action, tracking_cli.argparse._SubParsersAction)
    )
    observe_help = subparsers.choices["observe"].format_help()
    assert "v11 static-scene" in observe_help
    assert "v12 recovery-" in observe_help
    assert "v14 lossless interval-" in observe_help
    assert "guide browser bundle" in observe_help
    assert (
        "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451"
        in tracking_core.AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256
    )
    assert (
        "a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb"
        in tracking_core.AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256
    )


def test_immutable_manifest_rejects_tampered_reference_rgb(tmp_path: Path) -> None:
    bundle, _ = _bundle(tmp_path)
    rgb = bundle / "reference_rgb.png"
    payload = bytearray(rgb.read_bytes())
    payload[-12] ^= 1
    rgb.write_bytes(payload)
    with pytest.raises(ContractError, match="Immutable artifact SHA-256 mismatch"):
        load_rig_bundle(bundle)


def test_pipeline_writes_optimizer_compatible_atomic_bundle(tmp_path: Path) -> None:
    bundle, reference = _bundle(tmp_path)
    video = tmp_path / "horse.mp4"
    _video(video, reference)
    output = tmp_path / "observations"
    observations_path = run_observation_pipeline(
        video=video,
        bundle=bundle,
        output_dir=output,
        tracker=_Tracker(),
        segmenter=_Segmenter(),
        depth_backend=_Depth(),
    )
    loaded = load_observations(observations_path)
    assert loaded.frame_count == 3
    assert len(loaded.tracks) == 12
    assert len(loaded.silhouettes) == 3
    payload = json.loads(observations_path.read_text(encoding="utf-8"))
    schema_path = (
        Path(__file__).resolve().parents[1] / "schemas" / "observations.v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(payload)
    assert payload["depth"] == []
    assert payload["provenance"]["relative_depth_contract"] == (
        "relative_unscaled_diagnostics_only_not_camera_z"
    )
    arrays = np.load(output / "observations.npz")
    assert arrays["tracks_xy"].shape == (3, 12, 2)
    assert arrays["masks"].shape == (3, 240, 320)
    assert arrays["relative_depth"].shape == (3, 240, 320)
    diagnostics = json.loads((output / "diagnostics.json").read_text(encoding="utf-8"))
    assert diagnostics["decision"] == "accepted_observations"
    assert diagnostics["animation_quality_approved"] is False
    assert (output / "contact_sheet.jpg").stat().st_size > 0
    manifest = json.loads(
        (output / "observation_bundle_manifest.json").read_text(encoding="utf-8")
    )
    assert any(row["path"] == "observations.json" for row in manifest["files"])


def test_pipeline_calibrates_v2_relative_depth_into_optimizer_camera_z(
    tmp_path: Path,
) -> None:
    bundle, reference = _bundle(tmp_path)
    _upgrade_bundle_to_v2_camera_z(bundle)
    video = tmp_path / "horse-v2.mp4"
    _video(video, reference)
    output = tmp_path / "observations-v2"

    observations_path = run_observation_pipeline(
        video=video,
        bundle=bundle,
        output_dir=output,
        tracker=_Tracker(),
        segmenter=_Segmenter(),
        depth_backend=_Depth(),
    )

    loaded = load_observations(observations_path)
    assert len(loaded.depths) == 3
    payload = json.loads(observations_path.read_text(encoding="utf-8"))
    assert len(payload["depth"]) == 3
    assert {row["mode"] for row in payload["depth"]} == {"camera_z"}
    assert payload["provenance"]["relative_depth_contract"] == (
        "calibrated_to_camera_z_from_immutable_actionless_reference"
    )
    assert payload["provenance"]["camera_z_calibration"]["selected"]["mode"] == (
        "affine"
    )
    arrays = np.load(output / "observations.npz")
    assert arrays["camera_z"].shape == (3, 240, 320)


def test_first_frame_mismatch_fails_without_publishing_output(tmp_path: Path) -> None:
    bundle, reference = _bundle(tmp_path)
    video = tmp_path / "wrong.mp4"
    _video(video, np.full_like(reference, 250))
    output = tmp_path / "rejected"
    with pytest.raises(ContractError, match="does not match"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=output,
            tracker=_Tracker(),
            segmenter=_Segmenter(),
            config=ObservationRuntimeConfig(min_alignment_correlation=0.65),
        )
    assert not output.exists()


def test_center_crop_reference_geometry_is_explicit_and_provenanced(
    tmp_path: Path,
) -> None:
    import cv2

    bundle, reference = _bundle(tmp_path)
    target_width, target_height = 240, 240
    crop_width = 240
    crop_x = (reference.shape[1] - crop_width) // 2
    target = cv2.resize(
        reference[:, crop_x : crop_x + crop_width],
        (target_width, target_height),
        interpolation=cv2.INTER_LINEAR,
    )
    video = tmp_path / "center-cropped.mp4"
    _video(video, target)

    strict_output = tmp_path / "strict-aspect-rejected"
    with pytest.raises(ContractError, match="aspect ratio"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=strict_output,
            tracker=_Tracker(),
            segmenter=_Segmenter(),
        )
    assert not strict_output.exists()

    output = tmp_path / "center-crop-accepted"
    observations_path = run_observation_pipeline(
        video=video,
        bundle=bundle,
        output_dir=output,
        tracker=_Tracker(),
        segmenter=_Segmenter(),
        config=ObservationRuntimeConfig(
            reference_geometry_mode=REFERENCE_GEOMETRY_CENTER_CROP,
        ),
    )
    payload = json.loads(observations_path.read_text(encoding="utf-8"))
    geometry = payload["provenance"]["first_frame_reference"][
        "geometry_transform"
    ]
    assert geometry == {
        "mode": REFERENCE_GEOMETRY_CENTER_CROP,
        "source_resolution": [320, 240],
        "target_resolution": [240, 240],
        "crop_pixels": {"x": 40, "y": 0, "width": 240, "height": 240},
        "scale_xy": [1.0, 1.0],
        "coordinate_transform": "half_pixel_centers",
        "rgb_interpolation": "opencv_bilinear",
        "mask_interpolation": "opencv_nearest",
    }
    assert payload["provenance"]["alignment"]["combined_correlation"] >= 0.65
    assert payload["provenance"]["alignment"]["combined_correlation"] <= 1.0

    seeds = select_anchor_seeds(bundle)
    _, _, mapped_points, mapped_geometry = (
        tracking_core._reference_geometry_transform(
            seeds,
            width=120,
            height=120,
            mode=REFERENCE_GEOMETRY_CENTER_CROP,
        )
    )
    expected_points = (
        (seeds.points_xy - np.asarray((40.0, 0.0)) + 0.5) * 0.5 - 0.5
    )
    np.testing.assert_allclose(mapped_points, expected_points, atol=1e-6)
    assert mapped_geometry["coordinate_transform"] == "half_pixel_centers"


def test_reference_geometry_mode_is_fail_closed_and_cli_defaults_strict() -> None:
    with pytest.raises(ContractError, match="reference_geometry_mode"):
        ObservationRuntimeConfig(
            reference_geometry_mode="stretch_anything"
        ).validate()

    parser = tracking_cli._parser()
    base = [
        "observe",
        "--video",
        "candidate.mp4",
        "--bundle",
        "bundle",
        "--output-dir",
        "observations",
    ]
    assert parser.parse_args(base).reference_geometry_mode == (
        REFERENCE_GEOMETRY_ASPECT_STRICT
    )
    assert parser.parse_args(
        [*base, "--reference-geometry-mode", REFERENCE_GEOMETRY_CENTER_CROP]
    ).reference_geometry_mode == REFERENCE_GEOMETRY_CENTER_CROP
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--reference-geometry-mode", "stretch_anything"])


def test_visible_zero_confidence_tracks_fail_without_publishing_output(
    tmp_path: Path,
) -> None:
    bundle, reference = _bundle(tmp_path)
    video = tmp_path / "horse.mp4"
    _video(video, reference)
    output = tmp_path / "zero-confidence-rejected"
    with pytest.raises(ContractError, match="visible_confidence_minimum"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=output,
            tracker=_ZeroConfidenceTracker(),
            segmenter=_Segmenter(),
        )
    assert not output.exists()


def test_existing_output_directory_is_never_overwritten(tmp_path: Path) -> None:
    bundle, reference = _bundle(tmp_path)
    video = tmp_path / "horse.mp4"
    _video(video, reference)
    output = tmp_path / "existing"
    output.mkdir()
    marker = output / "owned.txt"
    marker.write_text("user data", encoding="utf-8")
    with pytest.raises(ContractError, match="already exists"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=output,
            tracker=_Tracker(),
            segmenter=_Segmenter(),
        )
    assert marker.read_text(encoding="utf-8") == "user data"


def test_late_bundle_failure_removes_staging_without_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, reference = _bundle(tmp_path)
    video = tmp_path / "horse.mp4"
    _video(video, reference)
    output = tmp_path / "late-rejected"

    def fail_contact_sheet(*_args, **_kwargs) -> None:
        raise RuntimeError("late diagnostics failure")

    monkeypatch.setattr(tracking_core, "_contact_sheet", fail_contact_sheet)
    with pytest.raises(RuntimeError, match="late diagnostics failure"):
        run_observation_pipeline(
            video=video,
            bundle=bundle,
            output_dir=output,
            tracker=_Tracker(),
            segmenter=_Segmenter(),
        )

    assert not output.exists()
    assert not list(tmp_path.glob("late-rejected.tmp-*"))
