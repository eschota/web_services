from __future__ import annotations

import json
from pathlib import Path
import struct

import numpy as np
import pytest

import animation_fitting.apply_fitted_motion as applier
from animation_fitting.apply_fitted_motion import promote_staged_bundle, validate_export_fps
from animation_fitting.errors import ContractError
from animation_fitting.math3d import quaternion_xyzw_from_matrix
from animation_fitting.motion_export_contract import (
    load_motion,
    load_target_spec,
    parse_glb_validation,
    validate_action_id,
    validate_target_source,
)


def _flat(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in matrix.reshape(-1)]


def _bone(parent: str | None, matrix: np.ndarray) -> dict:
    return {
        "parent": parent,
        "local_matrix": _flat(matrix),
        "local_translation": [float(value) for value in matrix[:3, 3]],
        "local_rotation_xyzw": quaternion_xyzw_from_matrix(matrix),
    }


def _write_motion(path: Path, *, child_translation: bool = False) -> None:
    identity = np.eye(4)
    child_rest = np.eye(4)
    child_rest[1, 3] = 1.0
    root_second = np.eye(4)
    root_second[0, 3] = 0.25
    angle = 0.3
    rotation = np.asarray(
        (
            (np.cos(angle), -np.sin(angle), 0.0, 0.0),
            (np.sin(angle), np.cos(angle), 0.0, 0.0),
            (0.0, 0.0, 1.0, 0.0),
            (0.0, 0.0, 0.0, 1.0),
        )
    )
    child_second = child_rest @ rotation
    if child_translation:
        child_second[0, 3] += 0.1
    payload = {
        "schema": "autorig-fitted-animation.v1",
        "frame_count": 2,
        "fps": 24.0,
        "loop": False,
        "transform_contract": {
            "schema": "autorig-fitted-transform-contract.v1",
            "source_armature_name": "HorseRig",
            "source_armature_world_matrix": _flat(identity),
            "root_local_matrix_space": "WORLD",
            "child_local_matrix_space": "PARENT_BONE",
            "rotation_channel": "QUATERNION",
            "scale_animation": False,
            "translation_policy": {"mode": "root_only", "bones": ["Root"]},
        },
        "frames": [
            {
                "frame": 0,
                "bones": {
                    "Root": _bone(None, identity),
                    "Child": _bone("Root", child_rest),
                },
            },
            {
                "frame": 1,
                "bones": {
                    "Root": _bone(None, root_second),
                    "Child": _bone("Root", child_second),
                },
            },
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_glb(path: Path, document: dict) -> None:
    chunk = json.dumps(document, separators=(",", ":")).encode("utf-8")
    chunk += b" " * ((4 - len(chunk) % 4) % 4)
    total = 12 + 8 + len(chunk)
    path.write_bytes(
        struct.pack("<4sII", b"glTF", 2, total)
        + struct.pack("<II", len(chunk), 0x4E4F534A)
        + chunk
    )


def _publication_fixture(root: Path, *, existing: bool) -> tuple[Path, dict[str, Path]]:
    root.mkdir()
    staging = root / ".staging"
    staging.mkdir()
    final_paths = {
        "blend": root / "horse_walk_forward.blend",
        "fbx": root / "horse_walk_forward.fbx",
        "glb": root / "horse_walk_forward.glb",
        "manifest": root / "horse_walk_forward.animation-manifest.json",
    }
    for key, path in final_paths.items():
        (staging / path.name).write_text(f"new-{key}", encoding="utf-8")
        if existing:
            path.write_text(f"old-{key}", encoding="utf-8")
    return staging, final_paths


def test_motion_contract_accepts_root_translation_and_parent_local_rotation(tmp_path: Path) -> None:
    path = tmp_path / "motion.json"
    _write_motion(path)
    clip = load_motion(path)
    assert clip.armature_name == "HorseRig"
    assert clip.translation_bones == ("Root",)
    assert clip.parent_by_bone == {"Root": None, "Child": "Root"}
    assert clip.frames[1].bones["Root"].local_matrix[0, 3] == pytest.approx(0.25)


def test_export_fps_must_match_motion_fps_exactly() -> None:
    assert validate_export_fps(24, 24.0) == 24.0
    assert validate_export_fps(24.0000000001, 24.0) == 24.0
    with pytest.raises(ContractError, match="must match motion.fps"):
        validate_export_fps(30, 24.0)


def test_bundle_publication_promotes_manifest_last(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging, final_paths = _publication_fixture(tmp_path / "output", existing=False)
    replacements: list[str] = []
    real_replace = applier.os.replace

    def recording_replace(source: str | Path, destination: str | Path) -> None:
        replacements.append(Path(source).name)
        real_replace(source, destination)

    monkeypatch.setattr(applier.os, "replace", recording_replace)
    promote_staged_bundle(staging, final_paths)
    assert replacements[-1] == final_paths["manifest"].name
    for key, path in final_paths.items():
        assert path.read_text(encoding="utf-8") == f"new-{key}"


def test_bundle_publication_failure_restores_complete_previous_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging, final_paths = _publication_fixture(tmp_path / "output", existing=True)
    real_replace = applier.os.replace
    failed_source = staging / final_paths["fbx"].name

    def failing_replace(source: str | Path, destination: str | Path) -> None:
        if Path(source) == failed_source:
            raise OSError("injected publication failure")
        real_replace(source, destination)

    monkeypatch.setattr(applier.os, "replace", failing_replace)
    with pytest.raises(ContractError, match="was rolled back"):
        promote_staged_bundle(staging, final_paths)
    for key, path in final_paths.items():
        assert path.read_text(encoding="utf-8") == f"old-{key}"


def test_bundle_publication_failure_leaves_no_partial_new_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging, final_paths = _publication_fixture(tmp_path / "output", existing=False)
    real_replace = applier.os.replace
    failed_source = staging / final_paths["fbx"].name

    def failing_replace(source: str | Path, destination: str | Path) -> None:
        if Path(source) == failed_source:
            raise OSError("injected publication failure")
        real_replace(source, destination)

    monkeypatch.setattr(applier.os, "replace", failing_replace)
    with pytest.raises(ContractError, match="was rolled back"):
        promote_staged_bundle(staging, final_paths)
    assert not any(path.exists() for path in final_paths.values())


def test_motion_contract_rejects_unpermitted_child_translation(tmp_path: Path) -> None:
    path = tmp_path / "motion.json"
    _write_motion(path, child_translation=True)
    with pytest.raises(ContractError, match="without translation_policy permission"):
        load_motion(path)


def test_target_manifest_is_source_hash_pinned(tmp_path: Path) -> None:
    manifest = tmp_path / "target.json"
    manifest.write_text(
        json.dumps(
            {
                "schema": "autorig-motion-target.v1",
                "source_sha256": "a" * 64,
                "armature_name": "HorseRig",
                "bone_names": ["Root", "Child"],
                "bone_parents": {"Root": None, "Child": "Root"},
            }
        ),
        encoding="utf-8",
    )
    target = load_target_spec(manifest_path=manifest, armature_name=None)
    validate_target_source(target, "a" * 64)
    with pytest.raises(ContractError, match="source_sha256"):
        validate_target_source(target, "b" * 64)


def test_semantic_action_id_rejects_generic_or_unsafe_names() -> None:
    assert validate_action_id("horse_walk_forward") == "horse_walk_forward"
    for value in ("Animation", "animation", "walk forward", "../walk"):
        with pytest.raises(ContractError):
            validate_action_id(value)


def test_glb_validation_requires_one_named_clip_skin_mesh_and_four_influences(tmp_path: Path) -> None:
    path = tmp_path / "clip.glb"
    document = {
        "asset": {"version": "2.0"},
        "accessors": [{"min": [0.0], "max": [2.0 / 24.0]}],
        "animations": [
            {
                "name": "horse_walk_forward",
                "samplers": [{"input": 0, "output": 0}],
                "channels": [],
            }
        ],
        "meshes": [{"primitives": [{"attributes": {"POSITION": 0, "JOINTS_0": 0, "WEIGHTS_0": 0}}]}],
        "skins": [{"joints": [0, 1]}],
    }
    _write_glb(path, document)
    result = parse_glb_validation(
        path,
        action_id="horse_walk_forward",
        expected_duration=2.0 / 24.0,
        duration_tolerance=1e-8,
    )
    assert result["animation_count"] == 1
    document["meshes"][0]["primitives"][0]["attributes"]["JOINTS_1"] = 1
    _write_glb(path, document)
    with pytest.raises(ContractError, match="more than four"):
        parse_glb_validation(
            path,
            action_id="horse_walk_forward",
            expected_duration=2.0 / 24.0,
            duration_tolerance=1e-8,
        )
