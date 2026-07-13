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
from animation_fitting.errors import ContractError, DependencyUnavailableError
from animation_fitting.observations import load_observations
from animation_fitting.tracking_runtime.core import (
    ObservationRuntimeConfig,
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
