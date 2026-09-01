#!/usr/bin/env python3
"""Build a compact, actionless anatomical rig from an approved Blender source.

The script is intentionally fail-closed.  A profile whose approval contract is
not fitting-ready can only be built with ``--reference-only``; that artifact is
labelled for reference rendering and must never enter the fitting pipeline.

Run with the Blender version required by the profile, for example::

    blender --background --factory-startup --python build_anatomical_rig.py -- \
        --input Horse_2.blend \
        --profile profiles/horse_arp_deform_v1.json \
        --output-dir horse_anatomical_reference \
        --reference-only
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile
import traceback
import types
from typing import Any, Iterable, Mapping, Sequence


TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

def _load_profile_contract() -> tuple[Any, Any, Any, Any]:
    """Load the small profile contract without requiring SciPy in Blender.

    Importing the regular package executes its public fitting imports, which
    intentionally depend on SciPy.  Blender's bundled Python does not provide
    SciPy, while this deterministic builder only needs ``errors.py`` and
    ``anatomical_profile.py``.  The fallback keeps that dependency boundary
    explicit and loads the same source files under their real module names.
    """

    try:
        from animation_fitting.anatomical_profile import (
            AnatomicalRigProfile,
            load_anatomical_profile,
            sha256_file,
        )
        from animation_fitting.errors import ContractError

        return AnatomicalRigProfile, load_anatomical_profile, sha256_file, ContractError
    except ModuleNotFoundError as exc:
        if exc.name != "scipy" and not str(exc.name).startswith("animation_fitting."):
            raise

    package_root = Path(__file__).resolve().parent
    for name in tuple(sys.modules):
        if name == "animation_fitting" or name.startswith("animation_fitting."):
            sys.modules.pop(name, None)
    package = types.ModuleType("animation_fitting")
    package.__file__ = str(package_root / "__init__.py")
    package.__package__ = "animation_fitting"
    package.__path__ = [str(package_root)]
    sys.modules["animation_fitting"] = package

    def load_module(name: str, filename: str) -> Any:
        spec = importlib.util.spec_from_file_location(name, package_root / filename)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load anatomical builder module {name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    errors_module = load_module("animation_fitting.errors", "errors.py")
    profile_module = load_module(
        "animation_fitting.anatomical_profile",
        "anatomical_profile.py",
    )
    return (
        profile_module.AnatomicalRigProfile,
        profile_module.load_anatomical_profile,
        profile_module.sha256_file,
        errors_module.ContractError,
    )


(
    AnatomicalRigProfile,
    load_anatomical_profile,
    sha256_file,
    ContractError,
) = _load_profile_contract()


BUILD_REPORT_SCHEMA = "autorig-anatomical-rig-build.v1"
OUTPUT_BLEND_FILENAME = "anatomical_rig.blend"
OUTPUT_REPORT_FILENAME = "build_report.json"
REFERENCE_USAGE = "reference_render_only"
FITTING_USAGE = "animation_fitting"
IDENTITY_MATRIX = (
    1.0,
    0.0,
    0.0,
    0.0,
    0.0,
    1.0,
    0.0,
    0.0,
    0.0,
    0.0,
    1.0,
    0.0,
    0.0,
    0.0,
    0.0,
    1.0,
)


@dataclass(frozen=True)
class BuildPolicy:
    usage: str
    artifact_fitting_ready: bool
    profile_fitting_ready: bool
    blocker_state: str
    blocking_reasons: tuple[str, ...]
    reference_only: bool


@dataclass(frozen=True)
class SourceSummary:
    armature_name: str
    mesh_names: tuple[str, ...]
    vertex_count: int
    deform_bone_names: tuple[str, ...]
    maximum_vertex_influences: int
    discarded_non_armature_modifiers: tuple[str, ...]


def determine_build_policy(
    profile: AnatomicalRigProfile,
    *,
    reference_only: bool,
) -> BuildPolicy:
    """Resolve the only two legal artifact usages for an anatomical profile."""

    blockers = profile.blocking_reasons
    if reference_only:
        return BuildPolicy(
            usage=REFERENCE_USAGE,
            artifact_fitting_ready=False,
            profile_fitting_ready=profile.fitting_ready,
            blocker_state="blocked" if blockers else "reference_only",
            blocking_reasons=blockers,
            reference_only=True,
        )
    if not profile.fitting_ready:
        reasons = ", ".join(blockers)
        raise ContractError(
            f"Anatomical profile {profile.profile_id!r} is not fitting-ready: "
            f"{reasons}. Use --reference-only only for reference rendering."
        )
    return BuildPolicy(
        usage=FITTING_USAGE,
        artifact_fitting_ready=True,
        profile_fitting_ready=True,
        blocker_state="clear",
        blocking_reasons=(),
        reference_only=False,
    )


def normalize_vertex_influences(
    influences: Iterable[tuple[str, float]],
    *,
    allowed_bones: Iterable[str],
    maximum_influences: int,
) -> tuple[tuple[str, float], ...]:
    """Validate and normalize one vertex's linear blend skinning weights."""

    if maximum_influences < 1 or maximum_influences > 4:
        raise ContractError("maximum_influences must be between one and four")
    allowed = frozenset(allowed_bones)
    combined: dict[str, float] = {}
    for raw_name, raw_weight in influences:
        if not isinstance(raw_name, str) or not raw_name:
            raise ContractError("Vertex influence bone names must be non-empty strings")
        weight = float(raw_weight)
        if not math.isfinite(weight) or weight < 0.0:
            raise ContractError(
                f"Vertex influence {raw_name!r} has a non-finite or negative weight"
            )
        if weight == 0.0:
            continue
        if raw_name not in allowed:
            raise ContractError(f"Vertex has a nonzero weight for unknown bone {raw_name!r}")
        combined[raw_name] = combined.get(raw_name, 0.0) + weight

    if not combined:
        raise ContractError("Every source vertex must have at least one nonzero deform weight")
    if len(combined) > maximum_influences:
        raise ContractError(
            f"Vertex has {len(combined)} nonzero deform weights; maximum is "
            f"{maximum_influences}"
        )
    total = math.fsum(combined.values())
    if not math.isfinite(total) or total <= 0.0:
        raise ContractError("Vertex deform weights do not have a finite positive sum")
    return tuple(
        (name, weight / total)
        for name, weight in sorted(combined.items())
    )


def validate_file_provenance(
    source: Path,
    profile: AnatomicalRigProfile,
) -> str:
    source = source.resolve()
    if not source.is_file():
        raise ContractError(f"Canonical anatomical source does not exist: {source}")
    expected_name = str(profile.canonical_source["filename"])
    if source.name != expected_name:
        raise ContractError(
            f"Canonical source filename must be {expected_name!r}, got {source.name!r}"
        )
    actual_sha = sha256_file(source)
    expected_sha = str(profile.canonical_source["sha256"])
    if actual_sha != expected_sha:
        raise ContractError(
            f"Canonical source SHA-256 mismatch: expected {expected_sha}, got {actual_sha}"
        )
    return actual_sha


def validate_blender_version(
    actual: Sequence[int],
    required: Sequence[int],
) -> tuple[int, int, int]:
    actual_version = tuple(int(value) for value in actual[:3])
    required_version = tuple(int(value) for value in required[:3])
    if len(actual_version) != 3 or len(required_version) != 3:
        raise ContractError("Blender versions must contain major, minor, and patch")
    if actual_version < required_version:
        required_text = ".".join(str(value) for value in required_version)
        actual_text = ".".join(str(value) for value in actual_version)
        raise ContractError(
            f"Anatomical rig requires Blender >= {required_text}; running {actual_text}"
        )
    return actual_version


def _matrix_values(matrix: Any) -> tuple[float, ...]:
    values = tuple(float(matrix[row][column]) for row in range(4) for column in range(4))
    if len(values) != 16 or not all(math.isfinite(value) for value in values):
        raise ContractError("Object transform must be a finite 4x4 matrix")
    return values


def _is_identity_matrix(matrix: Any, *, tolerance: float = 1e-8) -> bool:
    return all(
        abs(actual - expected) <= tolerance
        for actual, expected in zip(_matrix_values(matrix), IDENTITY_MATRIX)
    )


def _extract_source(
    bpy: Any,
    profile: AnatomicalRigProfile,
) -> tuple[
    Any,
    tuple[Any, ...],
    dict[str, tuple[tuple[tuple[str, float], ...], ...]],
    SourceSummary,
]:
    armatures = [obj for obj in bpy.context.scene.objects if obj.type == "ARMATURE"]
    expected_armature_name = str(profile.canonical_source["armature_name"])
    if len(armatures) != 1:
        raise ContractError(
            f"Canonical source must contain exactly one armature, found {len(armatures)}"
        )
    source_armature = armatures[0]
    if source_armature.name != expected_armature_name:
        raise ContractError(
            f"Canonical armature must be named {expected_armature_name!r}, "
            f"got {source_armature.name!r}"
        )
    _matrix_values(source_armature.matrix_world)

    expected_mesh_names = tuple(str(name) for name in profile.canonical_source["mesh_names"])
    all_scene_meshes = tuple(obj for obj in bpy.context.scene.objects if obj.type == "MESH")
    meshes = tuple(
        obj
        for obj in all_scene_meshes
        if obj.name in expected_mesh_names
        or (obj.visible_get() and not obj.hide_render)
    )
    actual_mesh_names = tuple(sorted(obj.name for obj in meshes))
    if actual_mesh_names != tuple(sorted(expected_mesh_names)):
        raise ContractError(
            "Canonical mesh set mismatch: expected "
            f"{sorted(expected_mesh_names)}, got {list(actual_mesh_names)}"
        )
    meshes_by_name = {obj.name: obj for obj in meshes}
    ordered_meshes = tuple(meshes_by_name[name] for name in expected_mesh_names)

    deform_bones = tuple(sorted(bone.name for bone in source_armature.data.bones if bone.use_deform))
    expected_deform_bones = tuple(sorted(profile.parent_map))
    if deform_bones != expected_deform_bones:
        missing = sorted(set(expected_deform_bones).difference(deform_bones))
        extra = sorted(set(deform_bones).difference(expected_deform_bones))
        raise ContractError(
            f"Canonical deform-bone set mismatch; missing={missing}, extra={extra}"
        )
    if len(deform_bones) != int(profile.canonical_source["deform_bone_count"]):
        raise ContractError("Canonical deform-bone count does not match the profile")

    vertex_count = sum(len(obj.data.vertices) for obj in ordered_meshes)
    expected_vertex_count = int(profile.canonical_source["vertex_count"])
    if vertex_count != expected_vertex_count:
        raise ContractError(
            f"Canonical source must contain {expected_vertex_count} mesh vertices, "
            f"found {vertex_count}"
        )

    armature_modifiers: list[tuple[Any, Any]] = []
    discarded_modifiers: list[str] = []
    normalized_by_mesh: dict[str, tuple[tuple[tuple[str, float], ...], ...]] = {}
    maximum_seen = 0
    maximum_allowed = int(profile.canonical_source["maximum_vertex_influences"])
    allowed_bones = frozenset(profile.parent_map)
    for mesh in ordered_meshes:
        _matrix_values(mesh.matrix_world)
        if mesh.data.shape_keys is not None:
            raise ContractError(f"Canonical mesh {mesh.name!r} must not have shape keys")
        for modifier in mesh.modifiers:
            if modifier.type == "ARMATURE":
                armature_modifiers.append((mesh, modifier))
            else:
                discarded_modifiers.append(f"{mesh.name}:{modifier.name}:{modifier.type}")

        group_names = {group.index: group.name for group in mesh.vertex_groups}
        vertex_rows: list[tuple[tuple[str, float], ...]] = []
        for vertex in mesh.data.vertices:
            raw = tuple(
                (group_names[item.group], float(item.weight))
                for item in vertex.groups
                if item.group in group_names
            )
            try:
                normalized = normalize_vertex_influences(
                    raw,
                    allowed_bones=allowed_bones,
                    maximum_influences=maximum_allowed,
                )
            except ContractError as exc:
                raise ContractError(
                    f"Canonical mesh {mesh.name!r} vertex {vertex.index}: {exc}"
                ) from exc
            maximum_seen = max(maximum_seen, len(normalized))
            vertex_rows.append(normalized)
        normalized_by_mesh[mesh.name] = tuple(vertex_rows)

    if len(armature_modifiers) != 1:
        raise ContractError(
            "Canonical source must contain exactly one Armature modifier across its "
            f"mesh set, found {len(armature_modifiers)}"
        )
    modifier_mesh, modifier = armature_modifiers[0]
    if modifier.object is not source_armature:
        raise ContractError(
            f"Armature modifier on {modifier_mesh.name!r} does not target "
            f"{source_armature.name!r}"
        )
    if not modifier.use_vertex_groups:
        raise ContractError("Canonical Armature modifier must use vertex groups")

    summary = SourceSummary(
        armature_name=source_armature.name,
        mesh_names=expected_mesh_names,
        vertex_count=vertex_count,
        deform_bone_names=deform_bones,
        maximum_vertex_influences=maximum_seen,
        discarded_non_armature_modifiers=tuple(sorted(discarded_modifiers)),
    )
    return source_armature, ordered_meshes, normalized_by_mesh, summary


def _copy_meshes(
    bpy: Any,
    source_meshes: Sequence[Any],
    normalized_by_mesh: Mapping[str, Sequence[Sequence[tuple[str, float]]]],
    profile: AnatomicalRigProfile,
) -> tuple[Any, ...]:
    from mathutils import Matrix

    output_meshes: list[Any] = []
    for source_mesh in source_meshes:
        copied_data = source_mesh.data.copy()
        copied_data.name = f"{source_mesh.name}_anatomical_data"
        copied_data.transform(source_mesh.matrix_world)
        copied_data.update()
        output_mesh = bpy.data.objects.new(
            f"__anatomical_mesh_{source_mesh.name}",
            copied_data,
        )
        bpy.context.scene.collection.objects.link(output_mesh)
        output_mesh.matrix_world = Matrix.Identity(4)
        output_mesh.parent = None
        output_mesh.animation_data_clear()
        while output_mesh.vertex_groups:
            output_mesh.vertex_groups.remove(output_mesh.vertex_groups[0])

        groups = {
            name: output_mesh.vertex_groups.new(name=name)
            for name in profile.topological_order
        }
        for vertex_index, influences in enumerate(normalized_by_mesh[source_mesh.name]):
            for bone_name, weight in influences:
                groups[bone_name].add([vertex_index], float(weight), "REPLACE")
        output_meshes.append(output_mesh)
    return tuple(output_meshes)


def _copy_armature(
    bpy: Any,
    source_armature: Any,
    profile: AnatomicalRigProfile,
    policy: BuildPolicy,
) -> Any:
    from mathutils import Matrix, Vector

    armature_data = bpy.data.armatures.new("__anatomical_armature_data")
    output_armature = bpy.data.objects.new("__anatomical_armature", armature_data)
    bpy.context.scene.collection.objects.link(output_armature)
    output_armature.matrix_world = Matrix.Identity(4)
    output_armature.parent = None
    bpy.context.view_layer.objects.active = output_armature
    output_armature.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")

    source_world = source_armature.matrix_world.copy()
    source_bones = source_armature.data.bones
    world_points: list[Any] = []
    edit_bones: dict[str, Any] = {}
    for name in profile.topological_order:
        source_bone = source_bones.get(name)
        if source_bone is None:
            raise ContractError(f"Canonical source is missing deform bone {name!r}")
        head = source_world @ source_bone.head_local
        tail = source_world @ source_bone.tail_local
        if not all(math.isfinite(float(value)) for value in (*head, *tail)):
            raise ContractError(f"Bone {name!r} has non-finite rest coordinates")
        if (tail - head).length <= 1e-8:
            raise ContractError(f"Bone {name!r} has zero world-space rest length")
        world_points.extend((head, tail))

        edit_bone = armature_data.edit_bones.new(name)
        edit_bone.head = head
        edit_bone.tail = tail
        source_z = source_world.to_3x3() @ source_bone.z_axis
        if source_z.length > 1e-8:
            edit_bone.align_roll(source_z.normalized())
        edit_bone.use_deform = True
        edit_bone.bbone_segments = 1
        edit_bones[name] = edit_bone

    minimum = Vector(
        tuple(min(float(point[axis]) for point in world_points) for axis in range(3))
    )
    maximum = Vector(
        tuple(max(float(point[axis]) for point in world_points) for axis in range(3))
    )
    diagonal = max((maximum - minimum).length, 1.0)
    root = armature_data.edit_bones.new(profile.master_root)
    root.head = Vector((0.0, 0.0, 0.0))
    root.tail = Vector((0.0, 0.0, diagonal * 0.1))
    root.use_deform = False
    root.bbone_segments = 1

    for name in profile.topological_order:
        parent_name = profile.parent_map[name]
        edit_bones[name].parent = root if parent_name is None else edit_bones[parent_name]
        edit_bones[name].use_connect = False

    bpy.ops.object.mode_set(mode="OBJECT")
    output_armature.data.pose_position = "REST"
    output_armature.animation_data_clear()
    output_armature.data.animation_data_clear()
    output_armature.lock_location = (True, True, True)
    output_armature.lock_rotation = (True, True, True)
    output_armature.lock_scale = (True, True, True)
    for pose_bone in output_armature.pose.bones:
        pose_bone.custom_shape = None
        for constraint in list(pose_bone.constraints):
            pose_bone.constraints.remove(constraint)
        pose_bone.lock_scale = (True, True, True)
        if pose_bone.name == profile.master_root:
            pose_bone.lock_location = (False, False, False)
            pose_bone.lock_rotation = (True, True, True)
        else:
            pose_bone.lock_location = (True, True, True)

    properties = {
        "autorig_anatomical_profile_id": profile.profile_id,
        "autorig_anatomical_profile_sha256": profile.sha256,
        "autorig_source_sha256": str(profile.canonical_source["sha256"]),
        "autorig_usage": policy.usage,
        "autorig_fitting_ready": policy.artifact_fitting_ready,
        "autorig_blocker_state": policy.blocker_state,
        "autorig_blocking_reasons_json": json.dumps(
            policy.blocking_reasons,
            ensure_ascii=True,
            separators=(",", ":"),
        ),
        "autorig_approval_contract_json": json.dumps(
            profile.approval_contract,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "autorig_translation_policy": "master_root_only",
        "autorig_scale_policy": "identity",
        "autorig_deformation_model": "normalized_linear_blend_skinning",
    }
    for name, value in properties.items():
        output_armature[name] = value
    return output_armature


def _remove_source_scene(
    bpy: Any,
    output_armature: Any,
    output_meshes: Sequence[Any],
    profile: AnatomicalRigProfile,
) -> None:
    keep = {output_armature, *output_meshes}
    for obj in list(bpy.data.objects):
        if obj not in keep:
            bpy.data.objects.remove(obj, do_unlink=True)
    for action in list(bpy.data.actions):
        bpy.data.actions.remove(action, do_unlink=True)

    output_armature.name = str(profile.canonical_source["armature_name"])
    output_armature.data.name = f"{output_armature.name}_Data"
    for output_mesh, expected_name in zip(output_meshes, profile.canonical_source["mesh_names"]):
        output_mesh.name = str(expected_name)
        output_mesh.data.name = f"{expected_name}_Data"
        modifier = output_mesh.modifiers.new(name="AnatomicalArmature", type="ARMATURE")
        modifier.object = output_armature
        modifier.use_vertex_groups = True

    bpy.context.view_layer.objects.active = output_armature
    for obj in bpy.context.selected_objects:
        obj.select_set(False)
    output_armature.select_set(True)
    bpy.context.scene.frame_start = 0
    bpy.context.scene.frame_end = 0
    bpy.context.scene.frame_set(0)
    bpy.context.view_layer.update()


def _assert_no_animation(id_block: Any, label: str) -> None:
    animation = getattr(id_block, "animation_data", None)
    if animation is None:
        return
    if animation.action is not None or len(animation.nla_tracks) or len(animation.drivers):
        raise ContractError(f"Output {label} unexpectedly contains animation data")


def _validate_output_scene(
    bpy: Any,
    profile: AnatomicalRigProfile,
    policy: BuildPolicy,
) -> dict[str, Any]:
    objects = tuple(bpy.context.scene.objects)
    armatures = tuple(obj for obj in objects if obj.type == "ARMATURE")
    meshes = tuple(obj for obj in objects if obj.type == "MESH")
    if len(objects) != 1 + len(profile.canonical_source["mesh_names"]):
        raise ContractError("Output scene contains objects outside the compact rig contract")
    if len(armatures) != 1:
        raise ContractError("Output scene must contain exactly one armature")
    armature = armatures[0]
    if armature.name != str(profile.canonical_source["armature_name"]):
        raise ContractError("Output armature name changed unexpectedly")
    if not _is_identity_matrix(armature.matrix_world):
        raise ContractError("Output armature must have an identity world transform")
    if armature.data.pose_position != "REST":
        raise ContractError("Output armature must be stored in REST pose")

    expected_bones = set(profile.parent_map) | {profile.master_root}
    actual_bones = set(armature.data.bones.keys())
    if actual_bones != expected_bones:
        raise ContractError("Output bone set does not match profile plus synthetic root")
    deform_bones = {bone.name for bone in armature.data.bones if bone.use_deform}
    if deform_bones != set(profile.parent_map):
        raise ContractError("Output deform-bone set does not exactly match the profile")
    root = armature.data.bones[profile.master_root]
    if root.parent is not None or root.use_deform:
        raise ContractError("Synthetic master root must be parentless and non-deforming")
    for name, parent_name in profile.parent_map.items():
        expected_parent = profile.master_root if parent_name is None else parent_name
        bone = armature.data.bones[name]
        if bone.parent is None or bone.parent.name != expected_parent:
            raise ContractError(f"Output parent mismatch for bone {name!r}")
        if int(bone.bbone_segments) != 1:
            raise ContractError(f"Output bone {name!r} is not linearized to one B-Bone segment")
    if int(root.bbone_segments) != 1:
        raise ContractError("Synthetic root must have one B-Bone segment")

    _assert_no_animation(armature, "armature object")
    _assert_no_animation(armature.data, "armature data")
    if armature.constraints:
        raise ContractError("Output armature object must not contain constraints")
    for pose_bone in armature.pose.bones:
        if pose_bone.constraints or pose_bone.custom_shape is not None:
            raise ContractError(
                f"Output pose bone {pose_bone.name!r} contains controls or constraints"
            )
    if bpy.data.actions:
        raise ContractError("Output file must not contain Actions")

    expected_mesh_names = tuple(str(name) for name in profile.canonical_source["mesh_names"])
    if tuple(sorted(mesh.name for mesh in meshes)) != tuple(sorted(expected_mesh_names)):
        raise ContractError("Output mesh set does not exactly match the profile")
    total_vertices = 0
    maximum_influences = 0
    for mesh in meshes:
        if not _is_identity_matrix(mesh.matrix_world) or mesh.parent is not None:
            raise ContractError(f"Output mesh {mesh.name!r} must be identity-world and unparented")
        if mesh.data.shape_keys is not None:
            raise ContractError(f"Output mesh {mesh.name!r} unexpectedly has shape keys")
        if len(mesh.modifiers) != 1 or mesh.modifiers[0].type != "ARMATURE":
            raise ContractError(f"Output mesh {mesh.name!r} must have one Armature modifier")
        if mesh.modifiers[0].object is not armature:
            raise ContractError(f"Output mesh {mesh.name!r} modifier targets the wrong armature")
        actual_groups = {group.name for group in mesh.vertex_groups}
        expected_groups = set(profile.parent_map)
        if actual_groups != expected_groups:
            raise ContractError(
                f"Output mesh {mesh.name!r} vertex groups do not match profile; "
                f"missing={sorted(expected_groups - actual_groups)}, "
                f"extra={sorted(actual_groups - expected_groups)}"
            )

        group_names = {group.index: group.name for group in mesh.vertex_groups}
        total_vertices += len(mesh.data.vertices)
        for vertex in mesh.data.vertices:
            normalized = normalize_vertex_influences(
                (
                    (group_names[item.group], float(item.weight))
                    for item in vertex.groups
                    if item.group in group_names
                ),
                allowed_bones=profile.parent_map,
                maximum_influences=int(
                    profile.canonical_source["maximum_vertex_influences"]
                ),
            )
            if abs(math.fsum(weight for _, weight in normalized) - 1.0) > 1e-8:
                raise ContractError("Output vertex weights are not normalized")
            maximum_influences = max(maximum_influences, len(normalized))
        _assert_no_animation(mesh, f"mesh object {mesh.name}")
        _assert_no_animation(mesh.data, f"mesh data {mesh.name}")

    if total_vertices != int(profile.canonical_source["vertex_count"]):
        raise ContractError("Output vertex count does not match the profile")
    if bool(armature["autorig_fitting_ready"]) != policy.artifact_fitting_ready:
        raise ContractError("Output fitting-ready custom property violates build policy")
    if str(armature["autorig_usage"]) != policy.usage:
        raise ContractError("Output usage custom property violates build policy")

    return {
        "objects": len(objects),
        "armatures": 1,
        "meshes": len(meshes),
        "vertices": total_vertices,
        "bones": len(actual_bones),
        "deform_bones": len(deform_bones),
        "maximum_vertex_influences": maximum_influences,
        "actions": 0,
        "constraints": 0,
        "drivers": 0,
        "nla_tracks": 0,
        "shape_keys": 0,
        "custom_shapes": 0,
    }


def _build_report(
    *,
    source: Path,
    source_sha: str,
    profile: AnatomicalRigProfile,
    policy: BuildPolicy,
    blender_version: Sequence[int],
    source_summary: SourceSummary,
    output_sha: str,
    output_bytes: int,
    output_counts: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": BUILD_REPORT_SCHEMA,
        "status": "built_reference_only" if policy.reference_only else "built_fitting_ready",
        "usage": policy.usage,
        "approval": {
            "profile_fitting_ready": policy.profile_fitting_ready,
            "artifact_fitting_ready": policy.artifact_fitting_ready,
            "reference_only": policy.reference_only,
            "blocker_state": policy.blocker_state,
            "blocking_reasons": list(policy.blocking_reasons),
        },
        "source": {
            "path": str(source),
            "filename": source.name,
            "sha256": source_sha,
            "armature_name": source_summary.armature_name,
            "mesh_names": list(source_summary.mesh_names),
            "vertex_count": source_summary.vertex_count,
            "deform_bone_count": len(source_summary.deform_bone_names),
            "maximum_vertex_influences": source_summary.maximum_vertex_influences,
            "discarded_non_armature_modifiers": list(
                source_summary.discarded_non_armature_modifiers
            ),
        },
        "profile": {
            "path": str(profile.path),
            "profile_id": profile.profile_id,
            "sha256": profile.sha256,
        },
        "builder": {
            "blender_version": [int(value) for value in blender_version[:3]],
            "deformation_model": "normalized_linear_blend_skinning",
            "translation_policy": "master_root_only",
            "scale_policy": "identity",
            "atomic_output_directory": True,
        },
        "output": {
            "blend": {
                "filename": OUTPUT_BLEND_FILENAME,
                "sha256": output_sha,
                "bytes": output_bytes,
            },
            "report": {"filename": OUTPUT_REPORT_FILENAME},
            "counts": dict(output_counts),
        },
    }


def build_anatomical_rig(
    *,
    source: Path,
    profile_path: Path,
    output_dir: Path,
    reference_only: bool,
) -> dict[str, Any]:
    try:
        import bpy
    except ImportError as exc:  # pragma: no cover - exercised only outside Blender
        raise ContractError("build_anatomical_rig.py must run inside Blender") from exc

    source = source.resolve()
    output_dir = output_dir.resolve()
    profile = load_anatomical_profile(profile_path)
    policy = determine_build_policy(profile, reference_only=reference_only)
    source_sha = validate_file_provenance(source, profile)
    blender_version = validate_blender_version(
        bpy.app.version,
        profile.minimum_blender_version,
    )

    if output_dir.exists():
        raise ContractError(f"Atomic output directory already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{output_dir.name}.staging-",
            dir=output_dir.parent,
        )
    )
    try:
        result = bpy.ops.wm.open_mainfile(filepath=str(source))
        if "FINISHED" not in result:
            raise ContractError(f"Blender could not open canonical source: {result}")
        source_armature, source_meshes, normalized, source_summary = _extract_source(
            bpy,
            profile,
        )
        output_meshes = _copy_meshes(bpy, source_meshes, normalized, profile)
        output_armature = _copy_armature(bpy, source_armature, profile, policy)
        _remove_source_scene(bpy, output_armature, output_meshes, profile)
        output_counts = _validate_output_scene(bpy, profile, policy)

        staged_blend = staging / OUTPUT_BLEND_FILENAME
        save_result = bpy.ops.wm.save_as_mainfile(
            filepath=str(staged_blend),
            check_existing=False,
            compress=True,
        )
        if "FINISHED" not in save_result:
            raise ContractError(f"Blender could not save anatomical rig: {save_result}")
        reload_result = bpy.ops.wm.open_mainfile(filepath=str(staged_blend))
        if "FINISHED" not in reload_result:
            raise ContractError(f"Blender could not reload anatomical rig: {reload_result}")
        output_counts = _validate_output_scene(bpy, profile, policy)

        output_sha = sha256_file(staged_blend)
        report = _build_report(
            source=source,
            source_sha=source_sha,
            profile=profile,
            policy=policy,
            blender_version=blender_version,
            source_summary=source_summary,
            output_sha=output_sha,
            output_bytes=staged_blend.stat().st_size,
            output_counts=output_counts,
        )
        (staging / OUTPUT_REPORT_FILENAME).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if sha256_file(source) != source_sha:
            raise ContractError("Canonical source changed while building the anatomical rig")
        if set(path.name for path in staging.iterdir()) != {
            OUTPUT_BLEND_FILENAME,
            OUTPUT_REPORT_FILENAME,
        }:
            raise ContractError("Staging directory contains unexpected output files")
        os.replace(staging, output_dir)
        return report
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reference-only", action="store_true")
    return parser.parse_args(argv)


def _args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []


def main() -> None:
    try:
        args = _parse_args(_args_after_separator())
        report = build_anatomical_rig(
            source=args.input,
            profile_path=args.profile,
            output_dir=args.output_dir,
            reference_only=args.reference_only,
        )
        print("AUTORIG_ANATOMICAL_RIG=" + json.dumps(report, sort_keys=True))
    except Exception as exc:
        print(
            "AUTORIG_ANATOMICAL_RIG_ERROR="
            + json.dumps(
                {"error": type(exc).__name__, "message": str(exc)},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        traceback.print_exc()
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
