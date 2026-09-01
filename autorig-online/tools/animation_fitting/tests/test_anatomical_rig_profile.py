from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from animation_fitting.anatomical_profile import load_anatomical_profile


TOOLS_ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = TOOLS_ROOT / "profiles" / "horse_arp_deform_v1.json"
SCHEMA_PATH = TOOLS_ROOT / "schemas" / "anatomical-rig-profile.v1.schema.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _topological_order(parent_map: dict[str, str | None]) -> tuple[str, ...]:
    ordered: list[str] = []
    pending = dict(parent_map)
    while pending:
        ready = sorted(
            name
            for name, parent in pending.items()
            if parent is None or parent in ordered
        )
        if not ready:
            raise AssertionError(f"cyclic or unresolved anatomical parents: {pending}")
        for name in ready:
            ordered.append(name)
            pending.pop(name)
    return tuple(ordered)


def test_horse_profile_matches_schema_and_is_fail_closed() -> None:
    profile = _load(PROFILE_PATH)
    Draft202012Validator(_load(SCHEMA_PATH)).validate(profile)
    assert profile["profile_id"] == "horse_arp_deform_v1"
    assert profile["canonical_source"]["deform_bone_count"] == 51
    assert profile["canonical_source"]["maximum_vertex_influences"] == 4
    assert profile["target"]["translation_policy"] == "master_root_only"
    assert profile["target"]["scale_policy"] == "identity"
    approval = profile["approval_contract"]
    assert approval["joint_limit_profile_required"] is True
    assert approval["joint_limit_profile"] is None
    assert approval["fitting_ready"] is False
    assert "joint_limit_profile_missing" in approval["blocking_reasons"]

    loaded = load_anatomical_profile(PROFILE_PATH)
    assert loaded.profile_id == profile["profile_id"]
    assert loaded.fitting_ready is False
    assert loaded.blocking_reasons == tuple(approval["blocking_reasons"])
    assert len(loaded.sha256) == 64


def test_horse_parent_map_is_complete_acyclic_and_has_two_body_branches() -> None:
    profile = _load(PROFILE_PATH)
    parent_map = profile["target"]["parent_map"]
    assert len(parent_map) == 51
    assert {name for name, parent in parent_map.items() if parent is None} == {
        "root.x",
        "spine_01.x",
    }
    assert all(parent is None or parent in parent_map for parent in parent_map.values())
    order = _topological_order(parent_map)
    assert len(order) == len(parent_map)
    assert not any(name.startswith("c_root") for name in parent_map)
    assert not any("_ik" in name or "_fk" in name for name in parent_map)


def test_horse_profile_has_four_explicit_contiguous_limb_chains() -> None:
    parent_map = _load(PROFILE_PATH)["target"]["parent_map"]
    for side in ("l", "r"):
        for prefix, branch_parent in (
            ("", "root.x"),
            ("_dupli_001", f"clavicle.{side}"),
        ):
            chain = [
                f"c_thigh_b{prefix}.{side}",
                f"thigh_twist{prefix}.{side}",
                f"thigh_stretch{prefix}.{side}",
                f"leg_stretch{prefix}.{side}",
                f"leg_twist{prefix}.{side}",
                f"foot{prefix}.{side}",
                f"toes_01{prefix}.{side}",
            ]
            assert parent_map[chain[0]] == branch_parent
            assert [parent_map[name] for name in chain[1:]] == chain[:-1]


def test_linearization_is_explicit_and_calibrated_not_silent() -> None:
    profile = _load(PROFILE_PATH)
    linearization = profile["linearization"]
    assert linearization["target_deformation_model"] == "normalized_linear_blend_skinning"
    assert set(linearization["bbone_bones"]).issubset(profile["target"]["parent_map"])
    reference = linearization["horse_gallop_reference"]
    assert reference["action"] == "Horse_gallop"
    assert reference["frames"] == [0, 18]
    assert reference["maximum_rmse_m"] < reference["maximum_vertex_error_m"]
    assert (
        reference["maximum_error_without_bbone_deformation_m"]
        < reference["maximum_vertex_error_m"] / 1000.0
    )
