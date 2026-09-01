from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np

from .errors import ContractError


CONTACT_PROFILE_SCHEMA = "autorig-animal-contact-profile.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class ContactFootProfile:
    foot_id: str
    bone: str
    vertex_ids: tuple[int, int, int, int]

    @property
    def anchor_ids(self) -> tuple[str, str, str, str]:
        return tuple(f"{self.bone}:{vertex_id}" for vertex_id in self.vertex_ids)  # type: ignore[return-value]


@dataclass(frozen=True)
class AnimalContactProfile:
    path: Path
    sha256: str
    profile_id: str
    revision: int
    rig_type: str
    action_id: str
    loop_unique_frames: int
    gait: str
    foot_order: tuple[str, str, str, str]
    forward_axis_world: np.ndarray
    root_motion_policy: str
    feet: Mapping[str, ContactFootProfile]

    @property
    def priority_anchor_ids(self) -> tuple[str, ...]:
        return tuple(
            anchor_id
            for foot_id in self.foot_order
            for anchor_id in self.feet[foot_id].anchor_ids
        )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"Contact profile {key} must be a non-empty string")
    return value.strip()


def load_contact_profile(path: str | Path) -> AnimalContactProfile:
    source = Path(path).resolve()
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid animal contact profile {source}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema") != CONTACT_PROFILE_SCHEMA:
        raise ContractError(f"Contact profile schema must be {CONTACT_PROFILE_SCHEMA}")
    expected_keys = {
        "schema",
        "profile_id",
        "revision",
        "rig_type",
        "action_id",
        "loop_unique_frames",
        "gait",
        "foot_order",
        "forward_axis_world",
        "root_motion_policy",
        "feet",
    }
    if set(payload) != expected_keys:
        raise ContractError(
            f"Contact profile keys must be exactly {sorted(expected_keys)}"
        )
    profile_id = _required_string(payload, "profile_id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]+", profile_id):
        raise ContractError("Contact profile_id must be a stable lowercase identifier")
    revision = payload.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ContractError("Contact profile revision must be a positive integer")
    rig_type = _required_string(payload, "rig_type")
    action_id = _required_string(payload, "action_id")
    gait = _required_string(payload, "gait")
    if gait != "lateral_sequence_walk":
        raise ContractError(
            "Animal contact profile v1 supports lateral_sequence_walk only"
        )
    unique_frames = payload.get("loop_unique_frames")
    if (
        isinstance(unique_frames, bool)
        or not isinstance(unique_frames, int)
        or unique_frames < 8
    ):
        raise ContractError("loop_unique_frames must be an integer of at least eight")
    order = payload.get("foot_order")
    if (
        not isinstance(order, list)
        or len(order) != 4
        or any(not isinstance(value, str) or not value for value in order)
        or len(set(order)) != 4
    ):
        raise ContractError("foot_order must contain four unique foot IDs")
    raw_axis = payload.get("forward_axis_world")
    if not isinstance(raw_axis, list) or len(raw_axis) != 3:
        raise ContractError("forward_axis_world must contain three values")
    try:
        forward_axis = np.asarray(raw_axis, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise ContractError(
            "forward_axis_world must contain three finite values"
        ) from exc
    if (
        not np.all(np.isfinite(forward_axis))
        or float(np.linalg.norm(forward_axis)) <= 1e-12
    ):
        raise ContractError("forward_axis_world must contain a finite non-zero vector")
    root_motion_policy = _required_string(payload, "root_motion_policy")
    if root_motion_policy != "canonical_in_place_optional_derived_root_motion":
        raise ContractError(
            "root_motion_policy must be canonical_in_place_optional_derived_root_motion"
        )
    raw_feet = payload.get("feet")
    if not isinstance(raw_feet, dict) or set(raw_feet) != set(order):
        raise ContractError("feet keys must exactly match foot_order")
    feet: dict[str, ContactFootProfile] = {}
    claimed_bones: set[str] = set()
    claimed_vertices: set[int] = set()
    for foot_id in order:
        raw = raw_feet[foot_id]
        if not isinstance(raw, dict) or set(raw) != {"bone", "vertex_ids"}:
            raise ContractError(f"feet.{foot_id} must contain bone and vertex_ids")
        bone = _required_string(raw, "bone")
        if bone in claimed_bones:
            raise ContractError(f"Contact foot bones must be unique: {bone}")
        vertex_ids = raw.get("vertex_ids")
        if (
            not isinstance(vertex_ids, list)
            or len(vertex_ids) != 4
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in vertex_ids
            )
            or len(set(vertex_ids)) != 4
        ):
            raise ContractError(
                f"feet.{foot_id}.vertex_ids must contain exactly four IDs"
            )
        overlap = claimed_vertices.intersection(vertex_ids)
        if overlap:
            raise ContractError(
                f"Contact vertex IDs must be globally unique: {sorted(overlap)}"
            )
        claimed_bones.add(bone)
        claimed_vertices.update(vertex_ids)
        feet[foot_id] = ContactFootProfile(
            foot_id=foot_id,
            bone=bone,
            vertex_ids=tuple(vertex_ids),  # type: ignore[arg-type]
        )
    return AnimalContactProfile(
        path=source,
        sha256=_sha256(source),
        profile_id=profile_id,
        revision=revision,
        rig_type=rig_type,
        action_id=action_id,
        loop_unique_frames=unique_frames,
        gait=gait,
        foot_order=tuple(order),  # type: ignore[arg-type]
        forward_axis_world=forward_axis / float(np.linalg.norm(forward_axis)),
        root_motion_policy=root_motion_policy,
        feet=feet,
    )


def validate_contact_profile_bundle(
    profile: AnimalContactProfile,
    *,
    rig_metadata: Mapping[str, Any],
    anchors: Mapping[str, Any],
) -> None:
    source = rig_metadata.get("source")
    if not isinstance(source, dict) or source.get("rig_type") != profile.rig_type:
        actual = source.get("rig_type") if isinstance(source, dict) else None
        raise ContractError(
            f"Contact profile rig_type mismatch: expected {profile.rig_type}, got {actual}"
        )
    missing = [
        anchor_id
        for anchor_id in profile.priority_anchor_ids
        if anchor_id not in anchors
    ]
    if missing:
        raise ContractError(
            f"Contact profile anchors are absent from the immutable bundle: {missing}"
        )
    for foot in profile.feet.values():
        for vertex_id, anchor_id in zip(foot.vertex_ids, foot.anchor_ids):
            anchor = anchors[anchor_id]
            if getattr(anchor, "id", None) != anchor_id:
                raise ContractError(
                    f"Contact anchor {anchor_id} has inconsistent immutable ID"
                )
            if getattr(anchor, "bone", None) != foot.bone:
                raise ContractError(
                    f"Contact anchor {anchor_id} does not belong to declared bone {foot.bone}"
                )
            if getattr(anchor, "vertex_id", None) != vertex_id:
                raise ContractError(
                    f"Contact anchor {anchor_id} does not match declared vertex {vertex_id}"
                )
