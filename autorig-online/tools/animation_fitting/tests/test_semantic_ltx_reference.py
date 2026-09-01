from __future__ import annotations

import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator
import numpy as np
import pytest

from animation_fitting.semantic_ltx_reference import (
    OUTPUT_LABEL_KEYS,
    SemanticLtxContractError,
    build_semantic_ltx_plan,
    decode_semantic_label_masks,
    load_semantic_ltx_profile,
    validate_semantic_pixel_contract,
    validate_semantic_profile_source,
)


ROOT = Path(__file__).resolve().parents[1]
HORSE_PROFILE = ROOT / "data" / "semantic_ltx_profiles" / "horse_2.v1.json"


def _profile_payload() -> dict:
    return {
        "schema": "autorig-semantic-ltx-profile.v1",
        "profile_id": "test.semantic_limbs.v1",
        "source": {
            "rig_type": "TEST_RIG",
            "filename": "test.blend",
            "sha256": "1" * 64,
            "armature_name": "Rig",
            "mesh_names": ["Mesh"],
        },
        "limb_groups": {
            "fore_left": {"anatomy": "fore", "side": "left", "bones": ["FL"]},
            "fore_right": {"anatomy": "fore", "side": "right", "bones": ["FR"]},
            "hind_left": {"anatomy": "hind", "side": "left", "bones": ["HL"]},
            "hind_right": {"anatomy": "hind", "side": "right", "bones": ["HR"]},
        },
        "palette_linear": {
            "body": [0.45, 0.50, 0.55],
            "fore_near": [0.0, 0.85, 1.0],
            "fore_far": [0.1, 0.2, 1.0],
            "hind_near": [1.0, 0.75, 0.0],
            "hind_far": [1.0, 0.1, 0.55],
        },
        "gates": {
            "minimum_face_limb_weight": 0.35,
            "minimum_face_dominance": 0.75,
            "minimum_faces_per_source_group": 1,
            "minimum_group_weight_mass": 1.0,
            "minimum_near_far_depth_separation": 0.25,
            "minimum_pixels_per_output_label": 4,
            "minimum_pixel_fraction_of_mask": 0.01,
            "maximum_mask_mismatch_pixels": 0,
            "mask_threshold": 0.5,
            "pixel_color_tolerance": 0.08,
            "minimum_palette_distance": 0.5,
        },
    }


def _write_profile(tmp_path: Path, payload: dict | None = None):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(payload or _profile_payload(), indent=2), encoding="utf-8")
    return load_semantic_ltx_profile(path)


def _synthetic_rows(*, ambiguous_fore: bool = False):
    depths = {"FL": 2.0, "FR": 3.0, "HL": 4.0, "HR": 5.0}
    skin = []
    faces = []
    vertex_id = 0
    for face_id, bone in enumerate(("FL", "FR", "HL", "HR"), start=1):
        vertices = []
        for corner in range(3):
            weights = [{"bone": bone, "weight": 1.0}]
            if ambiguous_fore and bone == "FL":
                weights = [
                    {"bone": "FL", "weight": 0.5},
                    {"bone": "FR", "weight": 0.5},
                ]
            skin.append(
                {
                    "vertex_id": vertex_id,
                    "world": [corner * 0.1, face_id * 0.1, -depths[bone]],
                    "weights": weights,
                }
            )
            vertices.append(vertex_id)
            vertex_id += 1
        faces.append({"face_id": face_id, "vertex_ids": vertices})
    return skin, faces


def test_horse_2_profile_is_schema_valid_and_explicit() -> None:
    payload = json.loads(HORSE_PROFILE.read_text(encoding="utf-8"))
    schema = json.loads(
        (ROOT / "schemas" / "semantic-ltx-profile.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator(schema).validate(payload)
    profile = load_semantic_ltx_profile(HORSE_PROFILE)
    assert profile.profile_id == "horse_2.semantic_limbs.v1"
    assert profile.source["sha256"] == (
        "fa75772d83c2613ddd6df6f7a305a407e12abf4a75c9083bb53df4d2619f50a1"
    )
    assert profile.limb_groups["fore_left"] == (
        "clavicle.l",
        "c_thigh_b_dupli_001.l",
        "thigh_twist_dupli_001.l",
        "thigh_stretch_dupli_001.l",
        "leg_stretch_dupli_001.l",
        "leg_twist_dupli_001.l",
        "foot_dupli_001.l",
        "toes_01_dupli_001.l",
    )
    assert profile.limb_groups["hind_right"] == (
        "c_thigh_b.r",
        "thigh_twist.r",
        "thigh_stretch.r",
        "leg_stretch.r",
        "leg_twist.r",
        "foot.r",
        "toes_01.r",
    )


def test_plan_uses_explicit_fore_hind_and_camera_near_far(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    skin, faces = _synthetic_rows()
    plan = build_semantic_ltx_plan(
        profile,
        skin_rows=skin,
        topology_rows=faces,
        available_bones=["FL", "FR", "HL", "HR"],
        world_to_camera=np.eye(4),
    )
    assert plan.face_labels == {
        1: "fore_near",
        2: "fore_far",
        3: "hind_near",
        4: "hind_far",
    }
    assert plan.contract["near_far_assignment"]["fore"] == {
        "near_source_group": "fore_left",
        "far_source_group": "fore_right",
        "near_camera_z": 2.0,
        "far_camera_z": 3.0,
        "separation": 1.0,
        "statistic": "skin_weighted_median_positive_camera_z",
    }
    assert plan.contract["source_group_face_counts"] == {
        "fore_left": 1,
        "fore_right": 1,
        "hind_left": 1,
        "hind_right": 1,
    }


def test_plan_fails_closed_on_ambiguous_face_and_missing_bone(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    skin, faces = _synthetic_rows(ambiguous_fore=True)
    with pytest.raises(SemanticLtxContractError, match="dominance gate"):
        build_semantic_ltx_plan(
            profile,
            skin_rows=skin,
            topology_rows=faces,
            available_bones=["FL", "FR", "HL", "HR"],
            world_to_camera=np.eye(4),
        )
    skin, faces = _synthetic_rows()
    with pytest.raises(SemanticLtxContractError, match="missing deform bones"):
        build_semantic_ltx_plan(
            profile,
            skin_rows=skin,
            topology_rows=faces,
            available_bones=["FL", "FR", "HL"],
            world_to_camera=np.eye(4),
        )


def test_profile_source_identity_is_exact(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    validate_semantic_profile_source(
        profile,
        rig_type="TEST_RIG",
        filename="test.blend",
        source_sha256="1" * 64,
        armature_name="Rig",
        mesh_names=["Mesh"],
    )
    with pytest.raises(SemanticLtxContractError, match="identity mismatch"):
        validate_semantic_profile_source(
            profile,
            rig_type="TEST_RIG",
            filename="test.blend",
            source_sha256="2" * 64,
            armature_name="Rig",
            mesh_names=["Mesh"],
        )


def test_pixel_contract_requires_four_sufficient_exact_mask_regions(tmp_path: Path) -> None:
    profile = _write_profile(tmp_path)
    height, width = 20, 20
    alpha = np.zeros((height, width), dtype=np.float32)
    alpha[2:18, 2:18] = 1.0
    canonical_mask = alpha >= 0.5
    overlay = np.zeros((height, width, 3), dtype=np.float32)
    regions = {
        "fore_near": (slice(3, 8), slice(3, 8)),
        "fore_far": (slice(3, 8), slice(9, 14)),
        "hind_near": (slice(10, 15), slice(3, 8)),
        "hind_far": (slice(10, 15), slice(9, 14)),
    }
    for label, region in regions.items():
        linear = np.asarray(profile.palette[label], dtype=np.float32)
        overlay[region] = np.where(
            linear <= 0.0031308,
            12.92 * linear,
            1.055 * np.power(linear, 1.0 / 2.4) - 0.055,
        )
    label_masks = decode_semantic_label_masks(profile, overlay, alpha)
    contract = validate_semantic_pixel_contract(
        profile,
        overlay_alpha=alpha,
        canonical_mask=canonical_mask,
        label_masks=label_masks,
    )
    assert contract["output_label_pixel_counts"] == {
        label: 25 for label in OUTPUT_LABEL_KEYS
    }

    missing = dict(label_masks)
    missing["hind_far"] = np.zeros_like(canonical_mask)
    with pytest.raises(SemanticLtxContractError, match="insufficient pixels"):
        validate_semantic_pixel_contract(
            profile,
            overlay_alpha=alpha,
            canonical_mask=canonical_mask,
            label_masks=missing,
        )
    mismatched_mask = canonical_mask.copy()
    mismatched_mask[0, 0] = True
    with pytest.raises(SemanticLtxContractError, match="canonical mask"):
        validate_semantic_pixel_contract(
            profile,
            overlay_alpha=alpha,
            canonical_mask=mismatched_mask,
            label_masks=label_masks,
        )


def test_profile_rejects_overlapping_bones_and_weak_palette(tmp_path: Path) -> None:
    payload = _profile_payload()
    payload["limb_groups"]["fore_right"]["bones"] = ["FL"]
    with pytest.raises(SemanticLtxContractError, match="only one group"):
        _write_profile(tmp_path, payload)

    payload = copy.deepcopy(_profile_payload())
    payload["palette_linear"]["fore_far"] = payload["palette_linear"]["fore_near"]
    with pytest.raises(SemanticLtxContractError, match="too similar"):
        _write_profile(tmp_path, payload)
