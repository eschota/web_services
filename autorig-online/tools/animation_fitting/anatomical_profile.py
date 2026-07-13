from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .errors import ContractError


ANATOMICAL_PROFILE_SCHEMA = "autorig-anatomical-rig-profile.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_file(path: str | Path) -> str:
    source = Path(path).resolve()
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class AnatomicalRigProfile:
    path: Path
    sha256: str
    raw: Mapping[str, Any]
    profile_id: str
    species: str
    source_rig_type: str
    minimum_blender_version: tuple[int, int, int]
    canonical_source: Mapping[str, Any]
    master_root: str
    parent_map: Mapping[str, str | None]
    topological_order: tuple[str, ...]
    bbone_bones: tuple[str, ...]
    approval_contract: Mapping[str, Any]

    @property
    def fitting_ready(self) -> bool:
        return bool(self.approval_contract["fitting_ready"])

    @property
    def blocking_reasons(self) -> tuple[str, ...]:
        return tuple(str(value) for value in self.approval_contract["blocking_reasons"])


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"Anatomical profile {key} must be a non-empty string")
    return value.strip()


def _topological_order(parent_map: Mapping[str, str | None]) -> tuple[str, ...]:
    ordered: list[str] = []
    pending = dict(parent_map)
    while pending:
        ready = sorted(
            name
            for name, parent in pending.items()
            if parent is None or parent in ordered
        )
        if not ready:
            raise ContractError(
                "Anatomical profile parent_map is cyclic or references an unresolved parent: "
                + ", ".join(sorted(pending))
            )
        for name in ready:
            ordered.append(name)
            pending.pop(name)
    return tuple(ordered)


def load_anatomical_profile(path: str | Path) -> AnatomicalRigProfile:
    source = Path(path).resolve()
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid anatomical rig profile {source}: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("schema") != ANATOMICAL_PROFILE_SCHEMA:
        raise ContractError(
            f"Anatomical profile schema must be {ANATOMICAL_PROFILE_SCHEMA}"
        )
    profile_id = _required_string(raw, "profile_id")
    species = _required_string(raw, "species")
    source_rig_type = _required_string(raw, "source_rig_type")
    revision = raw.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ContractError("Anatomical profile revision must be a positive integer")

    version = raw.get("minimum_blender_version")
    if (
        not isinstance(version, list)
        or len(version) != 3
        or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in version)
    ):
        raise ContractError("minimum_blender_version must contain three non-negative integers")

    canonical = raw.get("canonical_source")
    if not isinstance(canonical, dict):
        raise ContractError("canonical_source must be an object")
    for key in ("filename", "armature_name"):
        _required_string(canonical, key)
    source_sha = canonical.get("sha256")
    if not isinstance(source_sha, str) or not SHA256_RE.fullmatch(source_sha):
        raise ContractError("canonical_source.sha256 must be lowercase SHA-256")
    mesh_names = canonical.get("mesh_names")
    if (
        not isinstance(mesh_names, list)
        or not mesh_names
        or any(not isinstance(name, str) or not name for name in mesh_names)
        or len(set(mesh_names)) != len(mesh_names)
    ):
        raise ContractError("canonical_source.mesh_names must be unique non-empty strings")
    for key in ("vertex_count", "deform_bone_count", "maximum_vertex_influences"):
        value = canonical.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ContractError(f"canonical_source.{key} must be a positive integer")
    if int(canonical["maximum_vertex_influences"]) > 4:
        raise ContractError("Anatomical fitting supports at most four vertex influences")

    target = raw.get("target")
    if not isinstance(target, dict):
        raise ContractError("target must be an object")
    master_root = _required_string(target, "master_root")
    if target.get("deformation_model") != "normalized_linear_blend_skinning":
        raise ContractError("target.deformation_model must be normalized_linear_blend_skinning")
    if target.get("translation_policy") != "master_root_only":
        raise ContractError("target.translation_policy must be master_root_only")
    if target.get("scale_policy") != "identity":
        raise ContractError("target.scale_policy must be identity")
    raw_parent_map = target.get("parent_map")
    if not isinstance(raw_parent_map, dict) or not raw_parent_map:
        raise ContractError("target.parent_map must be a non-empty object")
    parent_map: dict[str, str | None] = {}
    for raw_name, raw_parent in raw_parent_map.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise ContractError("target.parent_map bone names must be non-empty strings")
        if raw_parent is not None and (not isinstance(raw_parent, str) or not raw_parent):
            raise ContractError(f"target.parent_map[{raw_name!r}] has an invalid parent")
        parent_map[raw_name] = raw_parent
    if master_root in parent_map:
        raise ContractError("target.master_root must be synthetic and absent from parent_map")
    unknown_parents = sorted(
        {parent for parent in parent_map.values() if parent is not None}.difference(parent_map)
    )
    if unknown_parents:
        raise ContractError(f"target.parent_map references unknown parents: {unknown_parents}")
    if len(parent_map) != int(canonical["deform_bone_count"]):
        raise ContractError(
            "target.parent_map size does not match canonical_source.deform_bone_count"
        )
    order = _topological_order(parent_map)

    linearization = raw.get("linearization")
    if not isinstance(linearization, dict):
        raise ContractError("linearization must be an object")
    if linearization.get("target_deformation_model") != target["deformation_model"]:
        raise ContractError("linearization and target deformation models disagree")
    raw_bbone = linearization.get("bbone_bones")
    if (
        not isinstance(raw_bbone, list)
        or any(not isinstance(name, str) or not name for name in raw_bbone)
        or len(set(raw_bbone)) != len(raw_bbone)
    ):
        raise ContractError("linearization.bbone_bones must be unique strings")
    unknown_bbone = sorted(set(raw_bbone).difference(parent_map))
    if unknown_bbone:
        raise ContractError(f"linearization references unknown B-Bones: {unknown_bbone}")

    approval = raw.get("approval_contract")
    if not isinstance(approval, dict):
        raise ContractError("approval_contract must be an object")
    if approval.get("joint_limit_profile_required") is not True:
        raise ContractError("joint_limit_profile_required must be true for fitting v1")
    blockers = approval.get("blocking_reasons")
    if (
        not isinstance(blockers, list)
        or any(not isinstance(value, str) or not value for value in blockers)
        or len(set(blockers)) != len(blockers)
    ):
        raise ContractError("approval_contract.blocking_reasons must be unique strings")
    fitting_ready = approval.get("fitting_ready")
    if not isinstance(fitting_ready, bool):
        raise ContractError("approval_contract.fitting_ready must be boolean")
    joint_limits = approval.get("joint_limit_profile")
    if joint_limits is not None and (not isinstance(joint_limits, str) or not joint_limits):
        raise ContractError("approval_contract.joint_limit_profile must be null or a string")
    if fitting_ready and (joint_limits is None or blockers):
        raise ContractError(
            "A fitting-ready anatomical profile requires joint limits and no blockers"
        )
    if not fitting_ready and not blockers:
        raise ContractError("A non-ready anatomical profile must name at least one blocker")

    return AnatomicalRigProfile(
        path=source,
        sha256=sha256_file(source),
        raw=raw,
        profile_id=profile_id,
        species=species,
        source_rig_type=source_rig_type,
        minimum_blender_version=tuple(version),
        canonical_source=canonical,
        master_root=master_root,
        parent_map=parent_map,
        topological_order=order,
        bbone_bones=tuple(raw_bbone),
        approval_contract=approval,
    )
