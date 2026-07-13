"""Bake fitted local transforms into a single named Blender Action and export it.

Blender invocation (arguments after ``--`` are passed to this script):

    blender --background --factory-startup --python apply_fitted_motion.py -- \
      --source horse.blend --motion horse_walk_fitted.json \
      --semantic-action-id walk_forward --output-dir output --fps 24 \
      --target-manifest horse.motion-target.json

The source is read-only by contract. All Blender/FBX/GLB writes first land in a
private staging directory under ``--output-dir`` and the sidecar manifest is
promoted last.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile
import traceback
from typing import Any, Dict, Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
TOOLS_DIR = SCRIPT_DIR.parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from animation_fitting.errors import ContractError  # noqa: E402
from animation_fitting.motion_export_contract import (  # noqa: E402
    ASSET_BUNDLE_SCHEMA,
    artifact_record,
    load_motion,
    load_target_spec,
    parse_glb_validation,
    sha256_file,
    validate_action_id,
    validate_target_source,
)


SUCCESS_MARKER = "AUTORIG_FITTED_MOTION="
ERROR_MARKER = "AUTORIG_FITTED_MOTION_ERROR="
SUPPORTED_SOURCE_EXTENSIONS = {".blend", ".fbx", ".glb", ".gltf"}
PUBLICATION_ORDER = ("blend", "fbx", "glb", "manifest")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--motion", type=Path, required=True)
    parser.add_argument("--semantic-action-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fps", type=float, required=True)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--target-manifest", type=Path)
    target.add_argument("--target-armature-name")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def validate_export_fps(requested_fps: Any, motion_fps: float) -> float:
    try:
        fps = float(requested_fps)
    except (TypeError, ValueError) as exc:
        raise ContractError("--fps must be a finite positive number") from exc
    if not math.isfinite(fps) or fps <= 0.0:
        raise ContractError("--fps must be a finite positive number")
    if not math.isclose(fps, motion_fps, rel_tol=0.0, abs_tol=1e-9):
        raise ContractError(
            f"--fps ({fps:.12g}) must match motion.fps ({motion_fps:.12g}); "
            "speed-changing export overrides are forbidden"
        )
    return motion_fps


def promote_staged_bundle(staging: Path, final_paths: Dict[str, Path]) -> None:
    if set(final_paths) != set(PUBLICATION_ORDER):
        raise ContractError("Bundle publication requires blend, fbx, glb, and manifest paths")
    staged_paths = {key: staging / final_paths[key].name for key in PUBLICATION_ORDER}
    missing = [str(path) for path in staged_paths.values() if not path.is_file()]
    if missing:
        raise ContractError(f"Cannot publish an incomplete staged bundle: {missing}")

    existing = {key for key, path in final_paths.items() if path.exists()}
    if existing and existing != set(PUBLICATION_ORDER):
        raise ContractError(
            "Existing published bundle is incomplete; refusing a non-atomic overwrite: "
            + ", ".join(sorted(existing))
        )
    rollback_dir = staging / ".previous-version"
    rollback_dir.mkdir()
    backups: Dict[str, Path] = {}
    promoted: list[str] = []
    try:
        # Remove the old commit marker first. Consumers must treat the manifest
        # as the only indication that all same-version artifacts are available.
        for key in ("manifest", "blend", "fbx", "glb"):
            final = final_paths[key]
            if final.exists():
                backup = rollback_dir / final.name
                os.replace(final, backup)
                backups[key] = backup
        for key in PUBLICATION_ORDER:
            os.replace(staged_paths[key], final_paths[key])
            promoted.append(key)
    except Exception as exc:
        rollback_errors: list[str] = []
        # Remove a new commit marker before any partial new artifacts.
        for key in reversed(promoted):
            final = final_paths[key]
            try:
                if final.exists() or final.is_symlink():
                    final.unlink()
            except OSError as rollback_exc:
                rollback_errors.append(f"remove {final}: {rollback_exc}")
        # Restore old artifacts first and the old commit marker last.
        for key in ("blend", "fbx", "glb"):
            backup = backups.get(key)
            if backup is None:
                continue
            try:
                os.replace(backup, final_paths[key])
            except OSError as rollback_exc:
                rollback_errors.append(f"restore {final_paths[key]}: {rollback_exc}")
        old_manifest = backups.get("manifest")
        if old_manifest is not None and not rollback_errors:
            try:
                os.replace(old_manifest, final_paths["manifest"])
            except OSError as rollback_exc:
                rollback_errors.append(
                    f"restore {final_paths['manifest']}: {rollback_exc}"
                )
        if rollback_errors:
            marker = staging / ".publication-rollback-incomplete"
            marker.write_text("\n".join(rollback_errors) + "\n", encoding="utf-8")
            raise ContractError(
                f"Bundle publication failed ({exc}); rollback is incomplete: "
                + "; ".join(rollback_errors)
            ) from exc
        raise ContractError(f"Bundle publication failed and was rolled back: {exc}") from exc


def blender_argv() -> list[str]:
    try:
        marker = sys.argv.index("--")
    except ValueError as exc:
        raise ContractError("Blender command must include -- before applier arguments") from exc
    return sys.argv[marker + 1 :]


def ensure_blender_runtime(bpy: Any) -> None:
    if not bpy.app.background:
        raise ContractError("apply_fitted_motion.py must run with Blender --background")
    if tuple(bpy.app.version) < (4, 3, 0) or tuple(bpy.app.version) >= (6, 0, 0):
        raise ContractError(
            f"Unsupported Blender {bpy.app.version_string}; supported runtime line is 4.3 through 5.x"
        )


def load_canonical_source(bpy: Any, source: Path) -> None:
    extension = source.suffix.lower()
    if extension not in SUPPORTED_SOURCE_EXTENSIONS:
        raise ContractError(f"Unsupported canonical source extension: {extension}")
    if extension == ".blend":
        result = bpy.ops.wm.open_mainfile(filepath=str(source), load_ui=False)
    else:
        bpy.ops.wm.read_factory_settings(use_empty=True)
        if extension == ".fbx":
            _operator_call(
                bpy.ops.import_scene.fbx,
                {"filepath": str(source), "use_anim": False},
                required=("filepath",),
                label="FBX import",
            )
        else:
            _operator_call(
                bpy.ops.import_scene.gltf,
                {"filepath": str(source), "import_pack_images": True},
                required=("filepath",),
                label="glTF import",
            )
        result = {"FINISHED"}
    if "FINISHED" not in result:
        raise ContractError(f"Blender failed to load canonical source {source}: {result}")


def _animation_id_blocks(bpy: Any) -> Iterable[Any]:
    collection_names = (
        "objects",
        "armatures",
        "meshes",
        "shape_keys",
        "materials",
        "cameras",
        "lights",
        "worlds",
        "node_groups",
    )
    seen: set[int] = set()
    for name in collection_names:
        collection = getattr(bpy.data, name, None)
        if collection is None:
            continue
        for datablock in collection:
            pointer = int(datablock.as_pointer())
            if pointer in seen:
                continue
            seen.add(pointer)
            yield datablock


def clear_animation_pollution(bpy: Any) -> dict:
    cleared_blocks = 0
    for datablock in _animation_id_blocks(bpy):
        if getattr(datablock, "animation_data", None) is not None:
            datablock.animation_data_clear()
            cleared_blocks += 1
    removed_actions = len(bpy.data.actions)
    for action in list(bpy.data.actions):
        bpy.data.actions.remove(action, do_unlink=True)
    return {"cleared_animation_blocks": cleared_blocks, "removed_actions": removed_actions}


def find_target_armature(bpy: Any, name: str) -> Any:
    matches = [obj for obj in bpy.data.objects if obj.type == "ARMATURE" and obj.name == name]
    if len(matches) != 1:
        raise ContractError(
            f"Expected exactly one target armature named {name!r}, found {len(matches)}"
        )
    armature = matches[0]
    if armature.library is not None or armature.data.library is not None:
        raise ContractError("Target armature must be local/editable, not library-linked")
    return armature


def matrix_from_numpy(Matrix: Any, matrix: Any) -> Any:
    return Matrix([[float(matrix[row, column]) for column in range(4)] for row in range(4)])


def matrix_max_error(first: Any, second: Any) -> float:
    return max(abs(float(first[row][column] - second[row][column])) for row in range(4) for column in range(4))


def validate_target_contract(armature: Any, motion: Any, target: Any, source_sha: str, Matrix: Any) -> None:
    validate_target_source(target, source_sha)
    if target.armature_name != motion.armature_name:
        raise ContractError(
            f"Target armature {target.armature_name!r} differs from motion source armature {motion.armature_name!r}"
        )
    if target.armature_data_name is not None and armature.data.name != target.armature_data_name:
        raise ContractError(
            f"Target armature data is {armature.data.name!r}, expected {target.armature_data_name!r}"
        )
    actual_names = tuple(bone.name for bone in armature.data.bones)
    if set(actual_names) != set(motion.bone_names):
        missing = sorted(set(motion.bone_names).difference(actual_names))
        extra = sorted(set(actual_names).difference(motion.bone_names))
        raise ContractError(f"Target/motion bone sets differ; missing={missing}, extra={extra}")
    if target.bone_names is not None and set(target.bone_names) != set(actual_names):
        raise ContractError("Target manifest bone_names do not match the loaded armature")
    actual_parents = {
        bone.name: bone.parent.name if bone.parent is not None else None
        for bone in armature.data.bones
    }
    if actual_parents != motion.parent_by_bone:
        raise ContractError("Target armature parent hierarchy does not match the motion contract")
    if target.bone_parents is not None and target.bone_parents != actual_parents:
        raise ContractError("Target manifest bone_parents do not match the loaded armature")
    expected_world = matrix_from_numpy(Matrix, motion.armature_world)
    if matrix_max_error(armature.matrix_world, expected_world) > 1e-5:
        raise ContractError(
            "Target armature world matrix differs from the actionless fitting-bundle transform contract"
        )


def topological_bones(armature: Any) -> tuple[str, ...]:
    pending = {bone.name: bone for bone in armature.data.bones}
    result: list[str] = []
    resolved: set[str] = set()
    while pending:
        progressed = False
        for name, bone in list(pending.items()):
            if bone.parent is not None and bone.parent.name not in resolved:
                continue
            result.append(name)
            resolved.add(name)
            del pending[name]
            progressed = True
        if not progressed:
            raise ContractError("Target armature hierarchy contains a cycle")
    return tuple(result)


def mute_constraints(armature: Any) -> dict:
    object_constraints = 0
    pose_constraints = 0
    for constraint in armature.constraints:
        constraint.mute = True
        object_constraints += 1
    for pose_bone in armature.pose.bones:
        for constraint in pose_bone.constraints:
            constraint.mute = True
            pose_constraints += 1
    return {
        "muted_object_constraints": object_constraints,
        "muted_pose_constraints": pose_constraints,
    }


def action_fcurves(action: Any) -> list[Any]:
    """Return legacy (4.3) or layered/slot (5.x) Action F-Curves."""
    legacy = getattr(action, "fcurves", None)
    if legacy is not None:
        return list(legacy)
    result: list[Any] = []
    for layer in getattr(action, "layers", []):
        for strip in getattr(layer, "strips", []):
            for channelbag in getattr(strip, "channelbags", []):
                result.extend(channelbag.fcurves)
    return result


def create_single_action(bpy: Any, armature: Any, action_id: str, motion: Any, fps: float, Matrix: Any) -> dict:
    scene = bpy.context.scene
    fps_integer = max(1, int(round(fps)))
    scene.render.fps = fps_integer
    scene.render.fps_base = fps_integer / fps
    scene.frame_start = 0
    scene.frame_end = motion.frame_count - 1
    scene.frame_preview_start = 0
    scene.frame_preview_end = motion.frame_count - 1
    armature.data.pose_position = "POSE"
    armature.animation_data_create()
    action = bpy.data.actions.new(action_id)
    if action.name != action_id:
        raise ContractError(f"Blender could not create exact Action name {action_id!r}")
    armature.animation_data.action = action
    action.use_frame_range = True
    action.frame_start = 0.0
    action.frame_end = float(motion.frame_count - 1)
    action.use_cyclic = bool(motion.loop)
    bpy.context.preferences.edit.keyframe_new_interpolation_type = "LINEAR"

    order = topological_bones(armature)
    armature_world_inverse = armature.matrix_world.inverted_safe()
    previous_quaternions: Dict[str, Any] = {}
    rotation_keyframes = 0
    translation_keyframes = 0
    for motion_frame in motion.frames:
        scene.frame_set(motion_frame.frame)
        for pose_bone in armature.pose.bones:
            pose_bone.rotation_mode = "QUATERNION"
            pose_bone.matrix_basis.identity()
        bpy.context.view_layer.update()
        desired_pose: Dict[str, Any] = {}
        for name in order:
            local = matrix_from_numpy(Matrix, motion_frame.bones[name].local_matrix)
            parent = motion.parent_by_bone[name]
            desired = armature_world_inverse @ local if parent is None else desired_pose[parent] @ local
            desired_pose[name] = desired
            armature.pose.bones[name].matrix = desired
            # PoseBone.matrix assignment derives matrix_basis from the currently
            # evaluated parent. Commit each parent before assigning its child;
            # otherwise Blender can leak root translation into child location.
            bpy.context.view_layer.update()
        bpy.context.view_layer.update()

        for name in order:
            pose_bone = armature.pose.bones[name]
            error = matrix_max_error(pose_bone.matrix, desired_pose[name])
            if error > 2e-4:
                raise ContractError(
                    f"Pose-space reconstruction error for bone {name} frame {motion_frame.frame}: {error:.9g}"
                )
            scale_error = max(abs(float(value) - 1.0) for value in pose_bone.scale)
            if scale_error > 2e-4:
                raise ContractError(
                    f"Bone {name} frame {motion_frame.frame} would require scale animation ({scale_error:.9g})"
                )
            if name not in motion.translation_bones and pose_bone.location.length > 2e-4:
                raise ContractError(
                    f"Bone {name} frame {motion_frame.frame} requires forbidden child translation "
                    f"{tuple(float(value) for value in pose_bone.location)}"
                )
            quaternion = pose_bone.rotation_quaternion.copy()
            previous = previous_quaternions.get(name)
            if previous is not None and quaternion.dot(previous) < 0.0:
                quaternion.negate()
                pose_bone.rotation_quaternion = quaternion
            previous_quaternions[name] = quaternion.copy()
            if not pose_bone.keyframe_insert(
                data_path="rotation_quaternion", frame=motion_frame.frame, group=name
            ):
                raise ContractError(f"Failed to key quaternion for bone {name}")
            rotation_keyframes += 1
            if name in motion.translation_bones:
                if not pose_bone.keyframe_insert(data_path="location", frame=motion_frame.frame, group=name):
                    raise ContractError(f"Failed to key translation for bone {name}")
                translation_keyframes += 1

    fcurves = action_fcurves(action)
    if not fcurves:
        raise ContractError("Semantic Action contains no F-Curves after key insertion")
    expected_channels = set()
    for name in order:
        pose_bone = armature.pose.bones[name]
        rotation_path = pose_bone.path_from_id("rotation_quaternion")
        expected_channels.update((rotation_path, index) for index in range(4))
        if name in motion.translation_bones:
            location_path = pose_bone.path_from_id("location")
            expected_channels.update((location_path, index) for index in range(3))
    actual_channels = {(fcurve.data_path, int(fcurve.array_index)) for fcurve in fcurves}
    if actual_channels != expected_channels:
        missing = sorted(expected_channels.difference(actual_channels))
        extra = sorted(actual_channels.difference(expected_channels))
        raise ContractError(f"Semantic Action channel contract differs; missing={missing}, extra={extra}")
    for fcurve in fcurves:
        frames = [float(point.co.x) for point in fcurve.keyframe_points]
        expected_frames = [float(frame) for frame in range(motion.frame_count)]
        if frames != expected_frames:
            raise ContractError(
                f"Action channel {fcurve.data_path}[{fcurve.array_index}] keys {frames}, expected {expected_frames}"
            )
        for keyframe in fcurve.keyframe_points:
            keyframe.interpolation = "LINEAR"

    scene.frame_set(0)
    bpy.context.view_layer.update()
    if len(bpy.data.actions) != 1 or bpy.data.actions[0].name != action_id:
        raise ContractError("Derived scene does not contain exactly the requested named Action")
    if armature.animation_data.action != action or len(armature.animation_data.nla_tracks) != 0:
        raise ContractError("Action assignment/NLA contract was polluted during bake")
    return {
        "name": action_id,
        "rotation_bone_frame_keys": rotation_keyframes,
        "translation_bone_frame_keys": translation_keyframes,
        "translation_bones": list(motion.translation_bones),
        "interpolation": "LINEAR",
        "nla_track_count": 0,
        "action_datablock_count": 1,
    }


def bound_meshes(armature: Any) -> list[Any]:
    result = []
    for obj in armature.users_scene[0].objects if armature.users_scene else []:
        if obj.type != "MESH":
            continue
        if any(modifier.type == "ARMATURE" and modifier.object == armature for modifier in obj.modifiers):
            result.append(obj)
    if not result:
        raise ContractError("Target armature has no mesh bound by an Armature modifier")
    return result


def validate_skin_influences(meshes: Iterable[Any], bone_names: Iterable[str]) -> dict:
    bone_set = set(bone_names)
    maximum = 0
    unweighted = 0
    vertex_count = 0
    for mesh_object in meshes:
        group_names = {group.index: group.name for group in mesh_object.vertex_groups}
        for vertex in mesh_object.data.vertices:
            vertex_count += 1
            influences = [
                group
                for group in vertex.groups
                if group.weight > 1e-8 and group_names.get(group.group) in bone_set
            ]
            maximum = max(maximum, len(influences))
            if not influences:
                unweighted += 1
            if len(influences) > 4:
                raise ContractError(
                    f"Mesh {mesh_object.name} vertex {vertex.index} has {len(influences)} bone influences; maximum is 4"
                )
    return {
        "mesh_count": len(list(meshes)) if not isinstance(meshes, list) else len(meshes),
        "vertex_count": vertex_count,
        "max_influences": maximum,
        "unweighted_vertex_count": unweighted,
    }


def select_export_objects(bpy: Any, armature: Any, meshes: Iterable[Any]) -> None:
    if bpy.context.object is not None and bpy.context.object.mode != "OBJECT":
        bpy.ops.object.mode_set(mode="OBJECT")
    for obj in bpy.context.view_layer.objects:
        obj.select_set(False)
    for obj in [armature, *list(meshes)]:
        obj.hide_set(False)
        obj.hide_viewport = False
        obj.select_set(True)
    bpy.context.view_layer.objects.active = armature


def _operator_call(operator: Any, kwargs: dict, required: Iterable[str], label: str) -> None:
    supported = {prop.identifier for prop in operator.get_rna_type().properties}
    missing = set(required).difference(supported)
    if missing:
        raise ContractError(f"Blender {label} operator lacks required options: {sorted(missing)}")
    filtered = {key: value for key, value in kwargs.items() if key in supported}
    result = operator(**filtered)
    if "FINISHED" not in result:
        raise ContractError(f"Blender {label} failed: {result}")


def export_fbx(bpy: Any, path: Path, action_id: str) -> dict:
    kwargs = {
        "filepath": str(path),
        "use_selection": True,
        "object_types": {"ARMATURE", "MESH"},
        "use_mesh_modifiers": True,
        "add_leaf_bones": False,
        "bake_anim": True,
        "bake_anim_use_all_bones": True,
        "bake_anim_use_nla_strips": False,
        # The FBX exporter names a single active-action bake "Scene". With the
        # derived file containing exactly one Action, all-actions mode is still
        # single-clip and preserves the semantic action/take name.
        "bake_anim_use_all_actions": True,
        "bake_anim_force_startend_keying": True,
        "bake_anim_step": 1.0,
        "bake_anim_simplify_factor": 0.0,
        "path_mode": "AUTO",
    }
    _operator_call(
        bpy.ops.export_scene.fbx,
        kwargs,
        required=(
            "filepath",
            "use_selection",
            "bake_anim",
            "bake_anim_use_nla_strips",
            "bake_anim_use_all_actions",
            "bake_anim_simplify_factor",
        ),
        label="FBX export",
    )
    if not path.is_file() or path.stat().st_size < 1024:
        raise ContractError("FBX export did not produce a non-empty standalone file")
    content = path.read_bytes()
    header = content[:23]
    if not header.startswith(b"Kaydara FBX Binary") and not header.startswith(b"; FBX"):
        raise ContractError("FBX export has an invalid header")
    if action_id.encode("utf-8") not in content:
        raise ContractError("FBX export does not contain the semantic action/take name")
    return {
        "semantic_take_name_present": True,
        "binary": header.startswith(b"Kaydara FBX Binary"),
    }


def export_glb(bpy: Any, path: Path) -> None:
    kwargs = {
        "filepath": str(path),
        "export_format": "GLB",
        "use_selection": True,
        "export_animations": True,
        # ACTIONS preserves the semantic Action name. ACTIVE_ACTIONS merges to
        # Blender's generic "Animation" name even when only one action exists.
        "export_animation_mode": "ACTIONS",
        "export_nla_strips": False,
        "export_frame_range": True,
        "export_frame_step": 1,
        "export_force_sampling": True,
        "export_optimize_animation_size": False,
        "export_skins": True,
        "export_all_influences": False,
        "export_influence_nb": 4,
        "export_def_bones": False,
        "export_armature_object_remove": False,
        "export_reset_pose_bones": False,
        "export_morph": False,
        "export_cameras": False,
        "export_lights": False,
        "export_materials": "EXPORT",
    }
    _operator_call(
        bpy.ops.export_scene.gltf,
        kwargs,
        required=(
            "filepath",
            "export_format",
            "use_selection",
            "export_animations",
            "export_animation_mode",
            "export_all_influences",
            "export_influence_nb",
        ),
        label="GLB export",
    )


def run(args: argparse.Namespace) -> dict:
    try:
        import bpy
        from mathutils import Matrix
    except ImportError as exc:
        raise ContractError(
            "apply_fitted_motion.py must run inside Blender: blender --background --python ..."
        ) from exc
    ensure_blender_runtime(bpy)
    source = args.source.resolve()
    motion_path = args.motion.resolve()
    output_dir = args.output_dir.resolve()
    if not source.is_file() or not motion_path.is_file():
        raise ContractError("--source and --motion must exist")
    action_id = validate_action_id(args.semantic_action_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    source_sha = sha256_file(source)
    motion = load_motion(motion_path)
    fps = validate_export_fps(args.fps, motion.fps)
    target = load_target_spec(
        manifest_path=args.target_manifest,
        armature_name=args.target_armature_name,
    )
    validate_target_source(target, source_sha)

    final_paths = {
        "blend": output_dir / f"{action_id}.blend",
        "fbx": output_dir / f"{action_id}.fbx",
        "glb": output_dir / f"{action_id}.glb",
        "manifest": output_dir / f"{action_id}.animation-manifest.json",
    }
    if source in final_paths.values():
        raise ContractError("Canonical source path collides with an output path")
    existing = [str(path) for path in final_paths.values() if path.exists()]
    if existing and not args.overwrite:
        raise ContractError(f"Output artifacts already exist; use --overwrite explicitly: {existing}")
    if existing and len(existing) != len(final_paths):
        raise ContractError(
            "Existing published bundle is incomplete; refusing a non-atomic overwrite: "
            + ", ".join(existing)
        )

    staging = Path(tempfile.mkdtemp(prefix=f".{action_id}.staging-", dir=str(output_dir)))
    try:
        load_canonical_source(bpy, source)
        cleanup = clear_animation_pollution(bpy)
        armature = find_target_armature(bpy, target.armature_name)
        constraints = mute_constraints(armature)
        bpy.context.view_layer.update()
        validate_target_contract(armature, motion, target, source_sha, Matrix)
        action = create_single_action(bpy, armature, action_id, motion, fps, Matrix)
        meshes = bound_meshes(armature)
        skin = validate_skin_influences(meshes, motion.bone_names)
        select_export_objects(bpy, armature, meshes)

        staged_blend = staging / final_paths["blend"].name
        staged_fbx = staging / final_paths["fbx"].name
        staged_glb = staging / final_paths["glb"].name
        result = bpy.ops.wm.save_as_mainfile(filepath=str(staged_blend), check_existing=False, compress=True)
        if "FINISHED" not in result or not staged_blend.is_file():
            raise ContractError(f"Failed to save derived Blender file: {result}")
        if Path(bpy.data.filepath).resolve() != staged_blend.resolve():
            raise ContractError("Blender saved to an unexpected path")
        fbx_validation = export_fbx(bpy, staged_fbx, action_id)
        select_export_objects(bpy, armature, meshes)
        export_glb(bpy, staged_glb)

        duration = (motion.frame_count - 1) / fps
        glb_validation = parse_glb_validation(
            staged_glb,
            action_id=action_id,
            expected_duration=duration,
            duration_tolerance=max(1e-6, 0.25 / fps),
        )
        if sha256_file(source) != source_sha:
            raise ContractError("Canonical source file changed during fitting export")
        manifest = {
            "schema": ASSET_BUNDLE_SCHEMA,
            "semantic_action_id": action_id,
            "source": {"path": str(source), "sha256": source_sha},
            "motion": {
                "path": str(motion.path),
                "sha256": motion.sha256,
                "schema": motion.raw["schema"],
                "input_fps": motion.fps,
            },
            "target": {
                "manifest": str(target.path) if target.path else None,
                "manifest_sha256": target.sha256,
                "armature_name": armature.name,
                "armature_data_name": armature.data.name,
                "bone_count": len(motion.bone_names),
                "mesh_names": sorted(mesh.name for mesh in meshes),
            },
            "blender": {
                "version": bpy.app.version_string,
                "version_tuple": list(bpy.app.version),
                "background": bool(bpy.app.background),
            },
            "timing": {
                "frame_count": motion.frame_count,
                "frame_start": 0,
                "frame_end": motion.frame_count - 1,
                "fps": fps,
                "duration_seconds": duration,
                "loop": motion.loop,
            },
            "transform_contract": motion.raw["transform_contract"],
            "action": action,
            "skin": skin,
            "cleanup": {**cleanup, **constraints},
            "fbx_validation": fbx_validation,
            "glb_validation": glb_validation,
            "artifacts": {
                "blend": artifact_record(staged_blend),
                "fbx": artifact_record(staged_fbx),
                "glb": artifact_record(staged_glb),
            },
        }
        staged_manifest = staging / final_paths["manifest"].name
        staged_manifest.write_text(
            json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        promote_staged_bundle(staging, final_paths)
        return {
            "manifest": str(final_paths["manifest"]),
            "blend": str(final_paths["blend"]),
            "fbx": str(final_paths["fbx"]),
            "glb": str(final_paths["glb"]),
            "semantic_action_id": action_id,
        }
    finally:
        if not (staging / ".publication-rollback-incomplete").exists():
            shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    try:
        payload = run(parse_args(blender_argv()))
    except Exception as exc:
        traceback.print_exc()
        print(
            ERROR_MARKER
            + json.dumps(
                {"error_type": type(exc).__name__, "message": str(exc)},
                sort_keys=True,
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 1
    print(SUCCESS_MARKER + json.dumps(payload, sort_keys=True, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
