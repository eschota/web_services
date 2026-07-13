from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path

import pytest

from animation_fitting.anatomical_profile import load_anatomical_profile
from animation_fitting.build_anatomical_rig import (
    FITTING_USAGE,
    REFERENCE_USAGE,
    determine_build_policy,
    normalize_vertex_influences,
    validate_blender_version,
    validate_file_provenance,
)
from animation_fitting.errors import ContractError


ROOT = Path(__file__).resolve().parents[1]
HORSE_PROFILE = ROOT / "profiles" / "horse_arp_deform_v1.json"


def test_unapproved_horse_profile_is_reference_only_and_preserves_blockers() -> None:
    profile = load_anatomical_profile(HORSE_PROFILE)

    with pytest.raises(ContractError, match="not fitting-ready"):
        determine_build_policy(profile, reference_only=False)

    policy = determine_build_policy(profile, reference_only=True)
    assert policy.usage == REFERENCE_USAGE
    assert policy.artifact_fitting_ready is False
    assert policy.profile_fitting_ready is False
    assert policy.blocker_state == "blocked"
    assert policy.blocking_reasons == profile.blocking_reasons
    assert FITTING_USAGE != policy.usage


def test_weight_contract_normalizes_and_sorts_at_most_four_influences() -> None:
    normalized = normalize_vertex_influences(
        (("b", 2.0), ("a", 1.0), ("b", 1.0), ("ignored", 0.0)),
        allowed_bones={"a", "b"},
        maximum_influences=4,
    )
    assert tuple(name for name, _ in normalized) == ("a", "b")
    assert tuple(weight for _, weight in normalized) == pytest.approx((0.25, 0.75))
    assert sum(weight for _, weight in normalized) == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("influences", "message"),
    [
        (("a", float("nan")), "non-finite"),
        (("a", -0.1), "negative"),
        (("unknown", 1.0), "unknown bone"),
        (("a", 0.0), "at least one"),
    ],
)
def test_weight_contract_rejects_invalid_values(
    influences: tuple[str, float],
    message: str,
) -> None:
    with pytest.raises(ContractError, match=message):
        normalize_vertex_influences(
            (influences,),
            allowed_bones={"a"},
            maximum_influences=4,
        )


def test_weight_contract_rejects_more_than_four_nonzero_influences() -> None:
    influences = tuple((f"bone_{index}", 1.0) for index in range(5))
    with pytest.raises(ContractError, match="5 nonzero"):
        normalize_vertex_influences(
            influences,
            allowed_bones={name for name, _ in influences},
            maximum_influences=4,
        )


def test_blender_minimum_version_is_fail_closed() -> None:
    assert validate_blender_version((5, 1, 0), (5, 1, 0)) == (5, 1, 0)
    assert validate_blender_version((5, 2, 0), (5, 1, 0)) == (5, 2, 0)
    with pytest.raises(ContractError, match=r"Blender >= 5\.1\.0"):
        validate_blender_version((5, 0, 9), (5, 1, 0))


def test_source_filename_and_sha_are_both_required(tmp_path: Path) -> None:
    source = tmp_path / "Horse_2.blend"
    source.write_bytes(b"immutable-blender-source")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    profile = load_anatomical_profile(HORSE_PROFILE)
    canonical = dict(profile.canonical_source)
    canonical["sha256"] = digest
    synthetic = replace(profile, canonical_source=canonical)
    assert validate_file_provenance(source, synthetic) == digest

    source.write_bytes(b"mutated")
    with pytest.raises(ContractError, match="SHA-256 mismatch"):
        validate_file_provenance(source, synthetic)

    wrong_name = tmp_path / "horse.blend"
    wrong_name.write_bytes(b"immutable-blender-source")
    with pytest.raises(ContractError, match="filename"):
        validate_file_provenance(wrong_name, synthetic)
