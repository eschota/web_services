from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

from jsonschema import Draft202012Validator
import pytest

from animation_fitting.contact_profile import (
    CONTACT_PROFILE_SCHEMA,
    load_contact_profile,
    validate_contact_profile_bundle,
)
from animation_fitting.errors import ContractError
from animation_fitting.rig import Anchor


PROFILE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "contact_profiles"
    / "horse_2.walk_forward.v1.json"
)
SCHEMA = (
    Path(__file__).resolve().parents[1]
    / "schemas"
    / "animal-contact-profile.v1.schema.json"
)


def test_horse_walk_contact_profile_has_exact_ground_hoof_anchors() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    payload = json.loads(PROFILE.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(payload)
    profile = load_contact_profile(PROFILE)
    assert profile.profile_id == "horse_2.walk_forward.contacts.v1"
    assert profile.loop_unique_frames == 48
    assert profile.foot_order == (
        "hind_left",
        "fore_left",
        "hind_right",
        "fore_right",
    )
    assert len(profile.priority_anchor_ids) == 16
    assert len(set(profile.priority_anchor_ids)) == 16
    assert "toes_01.l:37" in profile.priority_anchor_ids
    assert "toes_01_dupli_001.r:168" in profile.priority_anchor_ids


def test_horse_walk_loop64_profile_is_the_v1_anatomy_at_64_unique_frames() -> None:
    loop64_path = PROFILE.with_name("horse_2.walk_forward.loop64.v1.json")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(
        json.loads(loop64_path.read_text(encoding="utf-8"))
    )
    base = load_contact_profile(PROFILE)
    loop64 = load_contact_profile(loop64_path)

    assert loop64.profile_id == "horse_2.walk_forward.contacts.loop64.v1"
    assert loop64.loop_unique_frames == 64
    assert base.loop_unique_frames == 48
    assert loop64.rig_type == base.rig_type
    assert loop64.action_id == base.action_id
    assert loop64.gait == base.gait
    assert loop64.foot_order == base.foot_order
    assert loop64.root_motion_policy == base.root_motion_policy
    assert loop64.priority_anchor_ids == base.priority_anchor_ids
    assert loop64.sha256 != base.sha256


def test_contact_profile_schema_and_loader_reject_extra_or_duplicate_anchors(
    tmp_path: Path,
) -> None:
    payload = json.loads(PROFILE.read_text(encoding="utf-8"))
    assert payload["schema"] == CONTACT_PROFILE_SCHEMA
    payload["feet"]["fore_right"]["vertex_ids"][0] = 37
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ContractError, match="globally unique"):
        load_contact_profile(path)


def test_contact_profile_bundle_validation_is_fail_closed() -> None:
    profile = load_contact_profile(PROFILE)
    anchors = {}
    for foot in profile.feet.values():
        for vertex_id, anchor_id in zip(foot.vertex_ids, foot.anchor_ids):
            anchors[anchor_id] = Anchor(
                id=anchor_id,
                bone=foot.bone,
                vertex_id=vertex_id,
                rest_world=None,
                skin_weight=1.0,
                influences=(),
            )
    validate_contact_profile_bundle(
        profile,
        rig_metadata={"source": {"rig_type": "HORSE_2"}},
        anchors=anchors,
    )
    first, second = profile.feet["hind_left"].anchor_ids[:2]
    original = anchors[first]
    anchors[first] = replace(original, vertex_id=anchors[second].vertex_id)
    with pytest.raises(ContractError, match="does not match declared vertex"):
        validate_contact_profile_bundle(
            profile,
            rig_metadata={"source": {"rig_type": "HORSE_2"}},
            anchors=anchors,
        )
    anchors[first] = original
    anchors.pop(profile.priority_anchor_ids[0])
    with pytest.raises(ContractError, match="absent"):
        validate_contact_profile_bundle(
            profile,
            rig_metadata={"source": {"rig_type": "HORSE_2"}},
            anchors=anchors,
        )
