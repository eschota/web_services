#!/usr/bin/env python3
"""Render a deterministic, actionless fitting bundle from a Blender/FBX/GLB rig.

Run with Blender, for example:

    blender --background --python render_actionless_bundle.py -- \
        --input horse.fbx --output-dir fitting_bundle --species horse

The source file is never saved. Actions/NLA are detached only in Blender's
temporary process before RGB/depth/mask/face-id renders are produced.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import traceback
from typing import Any, Iterable

import bpy
from mathutils import Matrix, Vector
import numpy as np

TOOLS_ROOT = Path(__file__).resolve().parents[1]
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from animation_fitting.semantic_ltx_reference import (  # noqa: E402
    OUTPUT_LABEL_KEYS,
    SemanticLtxProfile,
    build_semantic_ltx_plan,
    decode_semantic_label_masks,
    load_semantic_ltx_profile,
    validate_semantic_pixel_contract,
    validate_semantic_profile_source,
)


REVISION = "autorig_actionless_bundle_v2"
SEMANTIC_REVISION = "autorig_actionless_bundle_v3"
IMMUTABLE_MANIFEST_SCHEMA = "autorig-fitting-immutable-bundle.v1"
COLOR_ATTRIBUTE = "autorig_face_id"
REST_FRAME = 0
DEFAULT_HORSE_2_SEMANTIC_PROFILE = (
    Path(__file__).with_name("data")
    / "semantic_ltx_profiles"
    / "horse_2.v1.json"
)


def parse_args() -> argparse.Namespace:
    argv = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--species", default="unknown")
    parser.add_argument("--rig-type", default="unknown")
    parser.add_argument("--orientation", default="unknown")
    parser.add_argument("--source-task-id", default="")
    parser.add_argument("--width", type=int, default=768)
    parser.add_argument("--height", type=int, default=448)
    parser.add_argument("--samples", type=int, default=32)
    parser.add_argument(
        "--semantic-profile",
        type=Path,
        help=(
            "Explicit versioned semantic-limb profile. HORSE_2 automatically uses "
            "the bundled horse_2.v1 profile when this option is omitted."
        ),
    )
    return parser.parse_args(argv)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_semantic_profile(args: argparse.Namespace) -> SemanticLtxProfile | None:
    path = args.semantic_profile
    if path is None and str(args.rig_type).upper() == "HORSE_2":
        path = DEFAULT_HORSE_2_SEMANTIC_PROFILE
    if path is None:
        return None
    return load_semantic_ltx_profile(path)


def matrix_values(matrix: Matrix) -> list[float]:
    return [float(value) for row in matrix for value in row]


def vector_values(vector: Vector) -> list[float]:
    return [float(value) for value in vector]


def load_source(path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix == ".blend":
        if Path(bpy.data.filepath).resolve() != path.resolve():
            bpy.ops.wm.open_mainfile(filepath=str(path))
        return

    bpy.ops.wm.read_factory_settings(use_empty=True)
    if suffix == ".fbx":
        bpy.ops.import_scene.fbx(filepath=str(path), use_anim=True, automatic_bone_orientation=False)
    elif suffix in {".glb", ".gltf"}:
        bpy.ops.import_scene.gltf(filepath=str(path))
    else:
        raise RuntimeError(f"unsupported fitting source: {suffix}")


def visible_meshes() -> list[bpy.types.Object]:
    return [
        obj
        for obj in bpy.context.scene.objects
        if obj.type == "MESH"
        and obj.visible_get()
        and not obj.hide_render
        and not obj.get("autorig_ground")
    ]


def armatures() -> list[bpy.types.Object]:
    # Rig templates commonly hide the armature object from final renders while
    # the skinned meshes still depend on it. Hidden armatures remain part of
    # the fitting contract and must not disappear from skeleton metadata.
    return [obj for obj in bpy.context.scene.objects if obj.type == "ARMATURE"]


def require_single_armature() -> bpy.types.Object:
    arms = armatures()
    if len(arms) != 1:
        raise RuntimeError(
            f"fitting v1 requires exactly one armature in the source scene; found {len(arms)}"
        )
    return arms[0]


def animation_owners() -> list[tuple[str, object]]:
    owners: list[tuple[str, object]] = []
    seen: set[int] = set()

    def add(label: str, owner: object | None) -> None:
        if owner is None or not hasattr(owner, "animation_data"):
            return
        identity = int(owner.as_pointer()) if hasattr(owner, "as_pointer") else id(owner)
        if identity in seen:
            return
        seen.add(identity)
        owners.append((label, owner))

    for obj in bpy.context.scene.objects:
        add(f"object:{obj.name}", obj)
        add(f"data:{obj.name}", getattr(obj, "data", None))
        if obj.type == "MESH":
            add(f"shape_keys:{obj.name}", getattr(obj.data, "shape_keys", None))
    return owners


def animation_curves(animation_data: object) -> list[object]:
    """Return every curve that can leave an evaluated value on an ID owner."""

    curves: list[object] = []
    action = getattr(animation_data, "action", None)
    curves.extend(getattr(action, "fcurves", ()) if action else ())
    for track in getattr(animation_data, "nla_tracks", ()):
        for strip in track.strips:
            strip_action = getattr(strip, "action", None)
            curves.extend(getattr(strip_action, "fcurves", ()) if strip_action else ())
    curves.extend(getattr(animation_data, "drivers", ()))
    return curves


def reset_animated_rna_value(owner: object, data_path: str, array_index: int) -> bool:
    """Restore one animated RNA channel to its declared default value.

    Detaching an Action or muting a driver does *not* restore the property that
    Blender last evaluated.  That can silently bake the final animated object
    transform into an otherwise actionless bundle.  We therefore reset only
    channels that actually had an Action/NLA/driver curve, preserving every
    untouched authored channel on imported rigs.

    Invalid or custom-property paths cannot have a reliable RNA default.  Such
    paths are reported to the caller, but they are safe after all Actions,
    drivers and constraints have been disabled.
    """

    parent_path, separator, property_name = data_path.rpartition(".")
    if not separator:
        parent = owner
        property_name = data_path
    else:
        try:
            parent = owner.path_resolve(parent_path)
        except (AttributeError, ValueError):
            return False
    if not property_name or property_name.startswith("["):
        return False
    properties = getattr(getattr(parent, "bl_rna", None), "properties", None)
    rna_property = properties.get(property_name) if properties is not None else None
    if rna_property is None or getattr(rna_property, "is_readonly", False):
        return False
    try:
        if bool(getattr(rna_property, "is_array", False)):
            value = getattr(parent, property_name)
            defaults = tuple(rna_property.default_array)
            if array_index < 0 or array_index >= len(value) or array_index >= len(defaults):
                return False
            value[array_index] = defaults[array_index]
        else:
            setattr(parent, property_name, rna_property.default)
    except (AttributeError, IndexError, TypeError, ValueError):
        return False
    return True


def make_actionless(armature: bpy.types.Object) -> dict:
    active_actions: list[str] = []
    muted_tracks = 0
    muted_drivers = 0
    muted_object_constraints = 0
    muted_pose_constraints = 0
    reset_shape_keys = 0
    reset_bones = 0
    reset_animated_channels: list[str] = []
    unresolved_animated_channels: list[str] = []
    for label, owner in animation_owners():
        animation_data = owner.animation_data
        if animation_data:
            curve_channels = sorted(
                {
                    (str(curve.data_path), int(curve.array_index))
                    for curve in animation_curves(animation_data)
                    if getattr(curve, "data_path", "")
                }
            )
            if animation_data.action:
                active_actions.append(f"{label}:{animation_data.action.name}")
                animation_data.action = None
            for track in animation_data.nla_tracks:
                if not track.mute:
                    track.mute = True
                    muted_tracks += 1
            for driver in animation_data.drivers:
                if not driver.mute:
                    driver.mute = True
                    muted_drivers += 1
            for data_path, array_index in curve_channels:
                channel = f"{label}:{data_path}[{array_index}]"
                if reset_animated_rna_value(owner, data_path, array_index):
                    reset_animated_channels.append(channel)
                else:
                    unresolved_animated_channels.append(channel)
    for obj in bpy.context.scene.objects:
        for constraint in obj.constraints:
            if not constraint.mute:
                constraint.mute = True
                muted_object_constraints += 1
        if obj.type == "MESH" and obj.data.shape_keys:
            for key_block in obj.data.shape_keys.key_blocks[1:]:
                key_block.value = 0.0
                if hasattr(key_block, "mute"):
                    key_block.mute = True
                reset_shape_keys += 1
    for pose_bone in armature.pose.bones:
        for constraint in pose_bone.constraints:
            if not constraint.mute:
                constraint.mute = True
                muted_pose_constraints += 1
    bpy.context.scene.frame_set(REST_FRAME)
    armature.data.pose_position = "REST"
    for pose_bone in armature.pose.bones:
        pose_bone.matrix_basis.identity()
        reset_bones += 1
    bpy.context.view_layer.update()
    state = {
        "detached_actions": active_actions,
        "muted_nla_tracks": muted_tracks,
        "muted_drivers": muted_drivers,
        "muted_object_constraints": muted_object_constraints,
        "muted_pose_constraints": muted_pose_constraints,
        "reset_shape_keys": reset_shape_keys,
        "reset_pose_bones": reset_bones,
        "reset_animated_rna_channels": reset_animated_channels,
        "unresolved_animated_rna_channels": unresolved_animated_channels,
        "frame": REST_FRAME,
        "armature_pose_position": armature.data.pose_position,
        "actionless": True,
    }
    assert_actionless_rest_state(armature)
    return state


def assert_actionless_rest_state(armature: bpy.types.Object) -> None:
    if bpy.context.scene.frame_current != REST_FRAME:
        raise RuntimeError(f"actionless render frame drifted from {REST_FRAME}")
    if armature.data.pose_position != "REST":
        raise RuntimeError("armature pose_position must remain REST for fitting renders")
    for label, owner in animation_owners():
        animation_data = owner.animation_data
        if not animation_data:
            continue
        if animation_data.action is not None:
            raise RuntimeError(f"active action survived actionless reset on {label}")
        if any(not track.mute for track in animation_data.nla_tracks):
            raise RuntimeError(f"active NLA track survived actionless reset on {label}")
        if any(not driver.mute for driver in animation_data.drivers):
            raise RuntimeError(f"active driver survived actionless reset on {label}")
    if any(not constraint.mute for obj in bpy.context.scene.objects for constraint in obj.constraints):
        raise RuntimeError("active object constraint survived actionless reset")
    if any(
        not constraint.mute
        for pose_bone in armature.pose.bones
        for constraint in pose_bone.constraints
    ):
        raise RuntimeError("active pose constraint survived actionless reset")
    for obj in bpy.context.scene.objects:
        if obj.type != "MESH" or not obj.data.shape_keys:
            continue
        for key_block in obj.data.shape_keys.key_blocks[1:]:
            if abs(float(key_block.value)) > 1e-8:
                raise RuntimeError(f"shape key survived actionless reset: {obj.name}/{key_block.name}")


def evaluated_vertices(obj: bpy.types.Object) -> Iterable[Vector]:
    depsgraph = bpy.context.evaluated_depsgraph_get()
    evaluated = obj.evaluated_get(depsgraph)
    mesh = evaluated.to_mesh()
    try:
        for vertex in mesh.vertices:
            yield evaluated.matrix_world @ vertex.co
    finally:
        evaluated.to_mesh_clear()


def validate_rest_geometry_contract(
    meshes: list[bpy.types.Object],
    armature: bpy.types.Object,
) -> dict:
    if not meshes:
        raise RuntimeError("fitting source contains no visible render meshes")
    depsgraph = bpy.context.evaluated_depsgraph_get()
    maximum_world_delta = 0.0
    for obj in meshes:
        enabled = [
            modifier
            for modifier in obj.modifiers
            if bool(modifier.show_viewport) or bool(modifier.show_render)
        ]
        unsupported = [modifier.name for modifier in enabled if modifier.type != "ARMATURE"]
        if unsupported:
            raise RuntimeError(
                f"mesh {obj.name} has geometry-changing modifiers that invalidate raw vertex maps: "
                + ", ".join(unsupported)
            )
        armature_modifiers = [modifier for modifier in enabled if modifier.type == "ARMATURE"]
        if len(armature_modifiers) != 1 or armature_modifiers[0].object != armature:
            raise RuntimeError(
                f"mesh {obj.name} must have exactly one enabled ARMATURE modifier "
                f"targeting {armature.name}"
            )
        evaluated = obj.evaluated_get(depsgraph)
        evaluated_mesh = evaluated.to_mesh()
        try:
            if len(evaluated_mesh.vertices) != len(obj.data.vertices):
                raise RuntimeError(
                    f"mesh {obj.name} evaluated vertex count differs from raw topology"
                )
            for raw_vertex, evaluated_vertex in zip(obj.data.vertices, evaluated_mesh.vertices):
                raw_world = obj.matrix_world @ raw_vertex.co
                evaluated_world = evaluated.matrix_world @ evaluated_vertex.co
                maximum_world_delta = max(
                    maximum_world_delta,
                    float((raw_world - evaluated_world).length),
                )
        finally:
            evaluated.to_mesh_clear()
    if maximum_world_delta > 1e-5:
        raise RuntimeError(
            "evaluated REST geometry does not match raw vertex IDs used by skin/topology maps: "
            f"max_world_delta={maximum_world_delta:.9g}"
        )
    return {
        "allowed_modifier": "single_armature_only",
        "raw_evaluated_vertex_identity": True,
        "maximum_world_delta": maximum_world_delta,
    }


def model_bounds(meshes: list[bpy.types.Object]) -> tuple[Vector, Vector]:
    minimum = Vector((math.inf, math.inf, math.inf))
    maximum = Vector((-math.inf, -math.inf, -math.inf))
    count = 0
    for obj in meshes:
        for point in evaluated_vertices(obj):
            count += 1
            for axis in range(3):
                minimum[axis] = min(minimum[axis], point[axis])
                maximum[axis] = max(maximum[axis], point[axis])
    if not count:
        raise RuntimeError("fitting source contains no evaluated mesh vertices")
    return minimum, maximum


def sampled_model_points(meshes: list[bpy.types.Object], limit: int = 12000) -> list[Vector]:
    per_mesh = max(64, limit // max(1, len(meshes)))
    sampled: list[Vector] = []
    for obj in meshes:
        points = list(evaluated_vertices(obj))
        step = max(1, math.ceil(len(points) / per_mesh))
        sampled.extend(points[::step])
    return sampled


def find_bone(armature: bpy.types.Object, patterns: tuple[str, ...]) -> bpy.types.Bone | None:
    candidates = []
    for bone in armature.data.bones:
        key = re.sub(r"[^a-z0-9]+", "_", bone.name.lower())
        score = next((index for index, pattern in enumerate(patterns) if re.search(pattern, key)), None)
        if score is not None:
            candidates.append((score, len(key), bone.name, bone))
    return min(candidates, default=(None, None, None, None))[-1]


def animal_forward_vector(arms: list[bpy.types.Object]) -> Vector:
    if not arms:
        return Vector((0.0, -1.0, 0.0))
    armature = max(arms, key=lambda item: len(item.data.bones))
    head = find_bone(armature, (r"(^|_)head($|_)", r"neck", r"jaw"))
    body = find_bone(armature, (r"pelvis", r"hips?", r"spine", r"root"))
    if not head or not body:
        return Vector((0.0, -1.0, 0.0))
    head_world = armature.matrix_world @ head.head_local
    body_world = armature.matrix_world @ body.head_local
    direction = head_world - body_world
    direction.z = 0.0
    return direction.normalized() if direction.length > 1e-5 else Vector((0.0, -1.0, 0.0))


def look_at(obj: bpy.types.Object, target: Vector) -> None:
    obj.rotation_euler = (target - obj.location).to_track_quat("-Z", "Y").to_euler()


def create_camera(
    minimum: Vector,
    maximum: Vector,
    forward: Vector,
    width: int,
    height: int,
    fit_points: list[Vector],
) -> bpy.types.Object:
    center = (minimum + maximum) * 0.5
    extent = maximum - minimum
    target = Vector((center.x, center.y, minimum.z + extent.z * 0.47))
    side = Vector((-forward.y, forward.x, 0.0)).normalized()
    span = max(extent.length, extent.z, extent.x, extent.y, 0.1)
    direction = (forward * 1.45 + side * 1.0 + Vector((0.0, 0.0, 0.68))).normalized()

    camera_data = bpy.data.cameras.new("AutoRig_Fitting_Camera")
    camera_data.lens = 58.0
    camera_data.sensor_width = 36.0
    camera_data.dof.use_dof = False
    camera = bpy.data.objects.new("AutoRig_Fitting_Camera", camera_data)
    bpy.context.scene.collection.objects.link(camera)
    bpy.context.scene.camera = camera

    distance = span * 2.3
    camera.location = target + direction * distance
    look_at(camera, target)
    bpy.context.view_layer.update()

    from bpy_extras.object_utils import world_to_camera_view

    points = fit_points or [
        Vector((x, y, z))
        for x in (minimum.x, maximum.x)
        for y in (minimum.y, maximum.y)
        for z in (minimum.z, maximum.z)
    ]
    for _ in range(24):
        projected = [world_to_camera_view(bpy.context.scene, camera, point) for point in points]
        min_x = min(point.x for point in projected)
        max_x = max(point.x for point in projected)
        min_y = min(point.y for point in projected)
        max_y = max(point.y for point in projected)
        occupancy = max(max_x - min_x, max_y - min_y)
        inside = min_x >= 0.065 and max_x <= 0.935 and min_y >= 0.065 and max_y <= 0.935
        if inside and 0.68 <= occupancy <= 0.84 and all(point.z > 0 for point in projected):
            break
        distance *= 0.90 if inside and occupancy < 0.68 else 1.10
        camera.location = target + direction * distance
        look_at(camera, target)
        bpy.context.view_layer.update()
    return camera


def make_principled_material(name: str, color: tuple[float, float, float, float], roughness: float) -> bpy.types.Material:
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    bsdf = material.node_tree.nodes.get("Principled BSDF")
    bsdf.inputs["Base Color"].default_value = color
    bsdf.inputs["Roughness"].default_value = roughness
    return material


def create_ground(minimum: Vector, maximum: Vector) -> bpy.types.Object:
    extent = maximum - minimum
    radius = max(extent.x, extent.y, extent.z) * 4.0
    bpy.ops.mesh.primitive_plane_add(size=max(radius, 1.0), location=((minimum.x + maximum.x) / 2, (minimum.y + maximum.y) / 2, minimum.z - 0.006))
    ground = bpy.context.object
    ground.name = "AutoRig_Fitting_Ground"
    ground["autorig_ground"] = True
    ground.pass_index = 0
    ground.data.materials.append(make_principled_material("AutoRig_Ground", (0.16, 0.18, 0.20, 1.0), 0.82))
    return ground


def add_area_light(name: str, location: Vector, target: Vector, energy: float, size: float, color: tuple[float, float, float]) -> None:
    data = bpy.data.lights.new(name, "AREA")
    data.energy = energy
    data.shape = "DISK"
    data.size = size
    data.color = color
    light = bpy.data.objects.new(name, data)
    bpy.context.scene.collection.objects.link(light)
    light.location = location
    look_at(light, target)


def configure_lighting(minimum: Vector, maximum: Vector, camera: bpy.types.Object) -> None:
    center = (minimum + maximum) * 0.5
    span = max((maximum - minimum).length, 0.5)
    world = bpy.context.scene.world or bpy.data.worlds.new("AutoRig_Fitting_World")
    bpy.context.scene.world = world
    world.use_nodes = True
    background = world.node_tree.nodes.get("Background")
    background.inputs["Color"].default_value = (0.19, 0.22, 0.26, 1.0)
    background.inputs["Strength"].default_value = 0.72

    add_area_light("AutoRig_Key", camera.location + Vector((span * 0.15, -span * 0.1, span * 0.55)), center, 1100.0, span * 1.7, (1.0, 0.92, 0.82))
    add_area_light("AutoRig_Fill", center + Vector((-span, span * 0.4, span * 0.35)), center, 720.0, span * 1.4, (0.70, 0.82, 1.0))
    add_area_light("AutoRig_Rim", center + Vector((0.0, span, span)), center, 900.0, span, (0.78, 0.88, 1.0))


def configure_render(scene: bpy.types.Scene, width: int, height: int, samples: int) -> None:
    selected_engine = None
    for engine in ("BLENDER_EEVEE_NEXT", "BLENDER_EEVEE"):
        try:
            scene.render.engine = engine
            selected_engine = engine
            break
        except TypeError:
            continue
    if selected_engine is None:
        raise RuntimeError("This Blender build does not expose an Eevee render engine")
    scene.render.resolution_x = width
    scene.render.resolution_y = height
    scene.render.resolution_percentage = 100
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGB"
    scene.render.image_settings.color_depth = "8"
    scene.render.film_transparent = False
    scene.render.use_file_extension = True
    scene.render.resolution_percentage = 100
    if hasattr(scene, "eevee") and hasattr(scene.eevee, "taa_render_samples"):
        scene.eevee.taa_render_samples = samples


def compositor_tree(scene: bpy.types.Scene):
    """Return ``(tree, uses_group_output)`` on Blender 4.x and 5.x."""
    scene.use_nodes = True
    tree = getattr(scene, "node_tree", None)
    if tree is not None:
        return tree, False
    tree = getattr(scene, "compositing_node_group", None)
    if tree is None:
        tree = bpy.data.node_groups.new(
            "AutoRig_Fitting_Compositor",
            "CompositorNodeTree",
        )
        scene.compositing_node_group = tree
    return tree, True


def render_rgb_depth(
    output_dir: Path,
    meshes: list[bpy.types.Object],
    armature: bpy.types.Object,
) -> dict[str, Path]:
    scene = bpy.context.scene
    scene.view_layers[0].use_pass_z = True
    for mesh in meshes:
        mesh.pass_index = 1

    tree, uses_group_output = compositor_tree(scene)
    tree.nodes.clear()
    render_layers = tree.nodes.new("CompositorNodeRLayers")
    if uses_group_output:
        has_image_output = any(
            getattr(item, "item_type", "") == "SOCKET"
            and getattr(item, "in_out", "") == "OUTPUT"
            and getattr(item, "name", "") == "Image"
            for item in tree.interface.items_tree
        )
        if not has_image_output:
            tree.interface.new_socket(
                name="Image",
                in_out="OUTPUT",
                socket_type="NodeSocketColor",
            )
        composite = tree.nodes.new("NodeGroupOutput")
    else:
        composite = tree.nodes.new("CompositorNodeComposite")
    tree.links.new(render_layers.outputs["Image"], composite.inputs["Image"])

    depth_out = tree.nodes.new("CompositorNodeOutputFile")
    if uses_group_output:
        depth_out.directory = str(output_dir)
        depth_out.file_name = "reference_depth_"
        depth_item = next(
            (item for item in depth_out.file_output_items if getattr(item, "name", "") == "Depth"),
            None,
        )
        if depth_item is None:
            depth_item = depth_out.file_output_items.new("FLOAT", "Depth")
        depth_item.override_node_format = True
        depth_item.format.file_format = "OPEN_EXR"
        depth_item.format.color_mode = "BW"
        depth_item.format.color_depth = "32"
        depth_input = depth_out.inputs["Depth"]
    else:
        depth_out.base_path = str(output_dir)
        depth_out.file_slots[0].path = "reference_depth_"
        depth_out.format.file_format = "OPEN_EXR"
        depth_out.format.color_mode = "BW"
        depth_out.format.color_depth = "32"
        depth_input = depth_out.inputs[0]
    tree.links.new(render_layers.outputs["Depth"], depth_input)

    rgb = output_dir / "reference_rgb.png"
    scene.render.filepath = str(rgb)
    assert_actionless_rest_state(armature)
    bpy.ops.render.render(write_still=True)

    if uses_group_output:
        # Blender 5.x keeps evaluating the assigned compositor group even when
        # the deprecated use_nodes flag is cleared. Detach it before mask and
        # face-ID renders or the depth File Output node writes duplicates.
        scene.compositing_node_group = None

    depth = next(output_dir.glob("reference_depth_*.*"))
    final_depth = output_dir / "reference_depth.exr"
    depth.replace(final_depth)
    return {"rgb": rgb, "depth": final_depth}


def make_emission_material(name: str, color: tuple[float, float, float, float]) -> bpy.types.Material:
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    nodes = material.node_tree.nodes
    nodes.clear()
    output = nodes.new("ShaderNodeOutputMaterial")
    emission = nodes.new("ShaderNodeEmission")
    emission.inputs["Color"].default_value = color
    material.node_tree.links.new(emission.outputs["Emission"], output.inputs["Surface"])
    return material


def render_silhouette_mask(
    output_dir: Path,
    meshes: list[bpy.types.Object],
    ground: bpy.types.Object,
    armature: bpy.types.Object,
) -> Path:
    white = make_emission_material("AutoRig_Silhouette", (1.0, 1.0, 1.0, 1.0))
    for obj in meshes:
        obj.data.materials.clear()
        obj.data.materials.append(white)
        for polygon in obj.data.polygons:
            polygon.material_index = 0
    ground.hide_render = True
    world = bpy.context.scene.world
    if world and world.use_nodes:
        background = world.node_tree.nodes.get("Background")
        background.inputs["Color"].default_value = (0.0, 0.0, 0.0, 1.0)
        background.inputs["Strength"].default_value = 0.0
    scene = bpy.context.scene
    scene.use_nodes = False
    scene.view_settings.view_transform = "Standard"
    scene.view_settings.look = "None"
    scene.view_settings.exposure = 0.0
    scene.view_settings.gamma = 1.0
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "BW"
    scene.render.image_settings.color_depth = "8"
    mask = output_dir / "reference_mask.png"
    scene.render.filepath = str(mask)
    assert_actionless_rest_state(armature)
    bpy.ops.render.render(write_still=True)
    return mask


def analyze_mask_framing(mask_path: Path, threshold: float = 0.5) -> dict:
    image = bpy.data.images.load(str(mask_path), check_existing=False)
    try:
        width, height = (int(image.size[0]), int(image.size[1]))
        pixels = image.pixels[:]
    finally:
        bpy.data.images.remove(image)
    foreground = []
    for pixel_index in range(0, len(pixels), 4):
        if float(pixels[pixel_index]) >= threshold:
            scalar = pixel_index // 4
            foreground.append((scalar % width, scalar // width))
    if not foreground:
        raise RuntimeError("rendered silhouette mask has no foreground pixels")
    min_x = min(point[0] for point in foreground)
    max_x = max(point[0] for point in foreground)
    min_y = min(point[1] for point in foreground)
    max_y = max(point[1] for point in foreground)
    box_width = max_x - min_x + 1
    box_height = max_y - min_y + 1
    width_occupancy = box_width / width
    height_occupancy = box_height / height
    max_occupancy = max(width_occupancy, height_occupancy)
    center_x = ((min_x + max_x + 1) * 0.5) / width
    center_y = ((min_y + max_y + 1) * 0.5) / height
    border_clear = min_x > 0 and min_y > 0 and max_x < width - 1 and max_y < height - 1
    accepted = (
        border_clear
        and 0.65 <= max_occupancy <= 0.90
        and 0.30 <= center_x <= 0.70
        and 0.30 <= center_y <= 0.70
    )
    framing = {
        "threshold": threshold,
        "bbox_pixels": [min_x, min_y, max_x, max_y],
        "width_occupancy": width_occupancy,
        "height_occupancy": height_occupancy,
        "foreground_fraction": len(foreground) / (width * height),
        "center_normalized": [center_x, center_y],
        "border_clear": border_clear,
        "accepted": accepted,
    }
    if not accepted:
        raise RuntimeError(
            "silhouette framing failed the LTX reference gate: "
            + json.dumps(framing, ensure_ascii=False, sort_keys=True)
        )
    return framing


def load_image_rgba(path: Path) -> np.ndarray:
    image = bpy.data.images.load(str(path), check_existing=False)
    try:
        width, height = int(image.size[0]), int(image.size[1])
        channels = int(image.channels)
        pixels = np.asarray(image.pixels[:], dtype=np.float32)
    finally:
        bpy.data.images.remove(image)
    expected = width * height * channels
    if channels >= 1 and width > 0 and height > 0 and pixels.size == expected:
        # Blender exposes regular Image pixels bottom-to-top; fitting
        # observations use conventional top-to-bottom image row coordinates.
        decoded = np.flipud(pixels.reshape(height, width, channels)).copy()
    else:
        # Blender 5.x writes compositor File Output EXRs as a valid
        # single-channel multipart container. bpy.data.images.load() exposes
        # those as a 0x0 image, while Blender's bundled OpenImageIO reader
        # decodes them deterministically in top-to-bottom row order.
        try:
            import OpenImageIO as oiio

            buffer = oiio.ImageBuf(str(path))
            specification = buffer.spec()
            decoded = np.asarray(buffer.get_pixels(oiio.FLOAT), dtype=np.float32)
        except Exception as exc:
            raise RuntimeError(f"cannot decode rendered image from {path}: {exc}") from exc
        if (
            decoded.ndim != 3
            or decoded.shape[0] != specification.height
            or decoded.shape[1] != specification.width
            or decoded.shape[2] != specification.nchannels
            or decoded.shape[2] < 1
        ):
            raise RuntimeError(f"cannot decode rendered image from {path}")
        height, width, channels = decoded.shape
        decoded = decoded.copy()
    if channels == 1:
        return np.concatenate(
            (np.repeat(decoded, 3, axis=2), np.ones((height, width, 1), dtype=np.float32)),
            axis=2,
        )
    if channels == 2:
        return np.concatenate(
            (np.repeat(decoded[:, :, :1], 3, axis=2), decoded[:, :, 1:2]),
            axis=2,
        )
    if channels == 3:
        return np.concatenate(
            (decoded, np.ones((height, width, 1), dtype=np.float32)),
            axis=2,
        )
    return decoded[:, :, :4]


def load_image_channel(path: Path) -> np.ndarray:
    return load_image_rgba(path)[:, :, 0]


def json_contract_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def srgb_to_linear_rgb(rgb: np.ndarray) -> np.ndarray:
    values = np.clip(np.asarray(rgb, dtype=np.float32), 0.0, 1.0)
    return np.where(
        values <= 0.04045,
        values / 12.92,
        np.power((values + 0.055) / 1.055, 2.4),
    ).astype(np.float32, copy=False)


def write_linear_rgb_png(path: Path, rgb: np.ndarray) -> None:
    pixels = np.asarray(rgb, dtype=np.float32)
    if pixels.ndim != 3 or pixels.shape[2] != 3 or not np.all(np.isfinite(pixels)):
        raise RuntimeError("semantic LTX composite must be finite HxWx3 RGB")
    height, width = pixels.shape[:2]
    pixels = np.clip(pixels, 0.0, 1.0)
    rgba = np.concatenate(
        (pixels, np.ones((height, width, 1), dtype=np.float32)),
        axis=2,
    )
    image = bpy.data.images.new(
        "AutoRig_LTX_Semantic_Composite",
        width=width,
        height=height,
        alpha=True,
        float_buffer=True,
    )
    scene = bpy.context.scene
    render = scene.render
    state = {
        "file_format": render.image_settings.file_format,
        "color_mode": render.image_settings.color_mode,
        "color_depth": render.image_settings.color_depth,
        "view_transform": scene.view_settings.view_transform,
        "look": scene.view_settings.look,
        "exposure": scene.view_settings.exposure,
        "gamma": scene.view_settings.gamma,
    }
    try:
        image.pixels.foreach_set(np.flipud(rgba).reshape(-1))
        render.image_settings.file_format = "PNG"
        render.image_settings.color_mode = "RGB"
        render.image_settings.color_depth = "8"
        scene.view_settings.view_transform = "Standard"
        scene.view_settings.look = "None"
        scene.view_settings.exposure = 0.0
        scene.view_settings.gamma = 1.0
        image.save_render(filepath=str(path), scene=scene)
    finally:
        render.image_settings.file_format = state["file_format"]
        render.image_settings.color_mode = state["color_mode"]
        render.image_settings.color_depth = state["color_depth"]
        scene.view_settings.view_transform = state["view_transform"]
        scene.view_settings.look = state["look"]
        scene.view_settings.exposure = state["exposure"]
        scene.view_settings.gamma = state["gamma"]
        bpy.data.images.remove(image)


def render_semantic_ltx_reference(
    output_dir: Path,
    meshes: list[bpy.types.Object],
    ground: bpy.types.Object,
    armature: bpy.types.Object,
    canonical_rgb_path: Path,
    profile: SemanticLtxProfile,
    plan,
    camera_contract: dict,
) -> tuple[Path, dict[str, Any], np.ndarray, dict[str, np.ndarray]]:
    mesh_data_ids = [int(obj.data.as_pointer()) for obj in meshes]
    if len(set(mesh_data_ids)) != len(mesh_data_ids):
        raise RuntimeError(
            "semantic LTX render does not support linked visible mesh datablocks"
        )
    canonical_sha = sha256_file(canonical_rgb_path)
    material_snapshots = [
        (
            obj,
            tuple(obj.data.materials),
            tuple(int(polygon.material_index) for polygon in obj.data.polygons),
            bool(obj.hide_render),
        )
        for obj in meshes
    ]
    palette_order = ("body", *OUTPUT_LABEL_KEYS)
    semantic_materials = {
        label: make_emission_material(
            f"AutoRig_LTX_{label}",
            (*profile.palette[label], 1.0),
        )
        for label in palette_order
    }
    scene = bpy.context.scene
    render = scene.render
    compositor_group_supported = hasattr(scene, "compositing_node_group")
    render_state = {
        "use_nodes": bool(scene.use_nodes),
        "compositing_node_group": (
            scene.compositing_node_group if compositor_group_supported else None
        ),
        "filepath": render.filepath,
        "file_format": render.image_settings.file_format,
        "color_mode": render.image_settings.color_mode,
        "color_depth": render.image_settings.color_depth,
        "film_transparent": bool(render.film_transparent),
        "view_transform": scene.view_settings.view_transform,
        "look": scene.view_settings.look,
        "exposure": scene.view_settings.exposure,
        "gamma": scene.view_settings.gamma,
        "ground_hide_render": bool(ground.hide_render),
    }
    overlay_path = output_dir / ".reference_ltx_semantic_overlay.png"
    face_offset = 0
    overlay_rgba: np.ndarray | None = None
    try:
        for obj in meshes:
            obj.data.materials.clear()
            for label in palette_order:
                obj.data.materials.append(semantic_materials[label])
            for polygon in obj.data.polygons:
                face_id = face_offset + polygon.index + 1
                label = plan.face_labels.get(face_id, "body")
                polygon.material_index = palette_order.index(label)
            face_offset += len(obj.data.polygons)
        if face_offset != int(plan.contract["total_faces"]):
            raise RuntimeError(
                "semantic face plan does not cover the rendered mesh topology"
            )
        ground.hide_render = True
        scene.use_nodes = False
        if compositor_group_supported:
            scene.compositing_node_group = None
        render.film_transparent = True
        render.image_settings.file_format = "PNG"
        render.image_settings.color_mode = "RGBA"
        render.image_settings.color_depth = "8"
        scene.view_settings.view_transform = "Standard"
        scene.view_settings.look = "None"
        scene.view_settings.exposure = 0.0
        scene.view_settings.gamma = 1.0
        render.filepath = str(overlay_path)
        assert_actionless_rest_state(armature)
        bpy.ops.render.render(write_still=True)
        overlay_rgba = load_image_rgba(overlay_path)
    finally:
        for obj, materials, material_indices, hide_render in material_snapshots:
            obj.data.materials.clear()
            for material in materials:
                obj.data.materials.append(material)
            for polygon, material_index in zip(obj.data.polygons, material_indices):
                polygon.material_index = material_index
            obj.hide_render = hide_render
        ground.hide_render = render_state["ground_hide_render"]
        scene.use_nodes = render_state["use_nodes"]
        if compositor_group_supported:
            scene.compositing_node_group = render_state["compositing_node_group"]
        render.filepath = render_state["filepath"]
        render.image_settings.file_format = render_state["file_format"]
        render.image_settings.color_mode = render_state["color_mode"]
        render.image_settings.color_depth = render_state["color_depth"]
        render.film_transparent = render_state["film_transparent"]
        scene.view_settings.view_transform = render_state["view_transform"]
        scene.view_settings.look = render_state["look"]
        scene.view_settings.exposure = render_state["exposure"]
        scene.view_settings.gamma = render_state["gamma"]
        for material in semantic_materials.values():
            bpy.data.materials.remove(material)
    if overlay_rgba is None:
        raise RuntimeError("semantic LTX overlay render did not produce pixels")
    if overlay_path.is_file():
        overlay_path.unlink()
    for obj, materials, material_indices, hide_render in material_snapshots:
        if tuple(obj.data.materials) != materials:
            raise RuntimeError(f"semantic material slots leaked onto {obj.name}")
        if tuple(int(polygon.material_index) for polygon in obj.data.polygons) != material_indices:
            raise RuntimeError(f"semantic polygon material indices leaked onto {obj.name}")
        if bool(obj.hide_render) != hide_render:
            raise RuntimeError(f"semantic visibility state leaked onto {obj.name}")
    assert_actionless_rest_state(armature)
    if sha256_file(canonical_rgb_path) != canonical_sha:
        raise RuntimeError("semantic render mutated canonical reference_rgb.png")

    canonical_rgba = load_image_rgba(canonical_rgb_path)
    if canonical_rgba.shape != overlay_rgba.shape:
        raise RuntimeError(
            f"semantic/canonical image dimensions differ: "
            f"{overlay_rgba.shape} vs {canonical_rgba.shape}"
        )
    alpha = np.clip(overlay_rgba[:, :, 3], 0.0, 1.0)
    overlay_linear = srgb_to_linear_rgb(overlay_rgba[:, :, :3])
    canonical_linear = srgb_to_linear_rgb(canonical_rgba[:, :, :3])
    composite = (
        overlay_linear * alpha[:, :, None]
        + canonical_linear * (1.0 - alpha[:, :, None])
    )
    semantic_path = output_dir / "reference_ltx_semantic.png"
    write_linear_rgb_png(semantic_path, composite)
    final_rgba = load_image_rgba(semantic_path)
    if final_rgba.shape[:2] != overlay_rgba.shape[:2]:
        raise RuntimeError("semantic LTX PNG resolution changed while saving")
    label_masks = decode_semantic_label_masks(
        profile,
        overlay_rgba[:, :, :3],
        alpha,
    )
    base_contract = {
        "schema": "autorig-ltx-semantic-reference.v1",
        "profile": {
            "profile_id": profile.profile_id,
            "filename": profile.path.name,
            "sha256": profile.sha256,
        },
        "resolution": [int(overlay_rgba.shape[1]), int(overlay_rgba.shape[0])],
        "camera_contract_sha256": json_contract_sha256(camera_contract),
        "canonical_rgb": {
            "filename": canonical_rgb_path.name,
            "sha256": canonical_sha,
            "bytes": canonical_rgb_path.stat().st_size,
        },
        "limb_groups": {
            key: list(value) for key, value in profile.limb_groups.items()
        },
        "palette_linear": {
            key: list(value) for key, value in profile.palette.items()
        },
        "gates": dict(profile.gates),
        "classification": plan.contract,
        "composition": "semantic_animal_over_unchanged_canonical_rgb",
        "render_order": "after_reference_rgb_before_face_id_override",
        "restoration_verified": True,
    }
    return semantic_path, base_contract, alpha, label_masks


def finalize_semantic_ltx_contract(
    profile: SemanticLtxProfile,
    base_contract: dict[str, Any],
    overlay_alpha: np.ndarray,
    label_masks: dict[str, np.ndarray],
    mask_path: Path,
) -> dict[str, Any]:
    mask = load_image_channel(mask_path) >= float(profile.gates["mask_threshold"])
    pixel_contract = validate_semantic_pixel_contract(
        profile,
        overlay_alpha=overlay_alpha,
        canonical_mask=mask,
        label_masks=label_masks,
    )
    return {
        **base_contract,
        "canonical_mask": {
            "filename": mask_path.name,
            "sha256": sha256_file(mask_path),
            "bytes": mask_path.stat().st_size,
        },
        "pixels": pixel_contract,
    }


def write_camera_z_artifact(
    output_dir: Path,
    radial_depth_path: Path,
    mask_path: Path,
    camera: dict,
) -> tuple[Path, dict]:
    radial = load_image_channel(radial_depth_path)
    mask = load_image_channel(mask_path) >= 0.5
    width, height = (int(value) for value in camera["resolution"])
    if radial.shape != (height, width) or mask.shape != (height, width):
        raise RuntimeError(
            f"camera-Z source dimensions differ from camera metadata: "
            f"radial={radial.shape}, mask={mask.shape}, expected={(height, width)}"
        )
    intrinsics = camera["intrinsics"]
    fx, fy = float(intrinsics["fx"]), float(intrinsics["fy"])
    cx, cy = float(intrinsics["cx"]), float(intrinsics["cy"])
    x = (np.arange(width, dtype=np.float32) + 0.5 - cx) / fx
    y = (np.arange(height, dtype=np.float32) + 0.5 - cy) / fy
    ray_factor = np.sqrt(1.0 + y[:, None] ** 2 + x[None, :] ** 2).astype(
        np.float32,
        copy=False,
    )
    valid = mask & np.isfinite(radial) & (radial > 0.0) & (radial < 1.0e19)
    camera_z = np.full((height, width), np.nan, dtype=np.float32)
    camera_z[valid] = radial[valid] / ray_factor[valid]
    finite = camera_z[valid]
    if finite.size == 0 or not np.all(np.isfinite(finite)) or np.any(finite <= 0.0):
        raise RuntimeError("camera-Z conversion produced no finite positive foreground depth")
    if np.any(np.isfinite(camera_z[~mask])):
        raise RuntimeError("camera-Z invalid/background pixels must remain NaN")
    path = output_dir / "reference_camera_z.npy"
    np.save(path, camera_z, allow_pickle=False)
    return path, {
        "mode": "positive_camera_z",
        "dtype": "float32",
        "shape": [height, width],
        "invalid": "NaN",
        "source": "blender_z_pass_radial_distance",
        "conversion": "radial_distance_divided_by_camera_ray_factor",
        "valid_pixels": int(finite.size),
        "minimum": float(np.min(finite)),
        "median": float(np.median(finite)),
        "maximum": float(np.max(finite)),
    }


def encode_face_id(value: int) -> tuple[float, float, float, float]:
    return (
        float(value & 255) / 255.0,
        float((value >> 8) & 255) / 255.0,
        float((value >> 16) & 255) / 255.0,
        1.0,
    )


def render_face_ids(
    output_dir: Path,
    meshes: list[bpy.types.Object],
    ground: bpy.types.Object,
    armature: bpy.types.Object,
) -> tuple[Path, int]:
    material = bpy.data.materials.new("AutoRig_Face_ID")
    material.use_nodes = True
    nodes = material.node_tree.nodes
    nodes.clear()
    output = nodes.new("ShaderNodeOutputMaterial")
    attribute = nodes.new("ShaderNodeVertexColor")
    attribute.layer_name = COLOR_ATTRIBUTE
    emission = nodes.new("ShaderNodeEmission")
    material.node_tree.links.new(attribute.outputs["Color"], emission.inputs["Color"])
    material.node_tree.links.new(emission.outputs["Emission"], output.inputs["Surface"])

    face_offset = 0
    for obj in meshes:
        mesh = obj.data
        existing = mesh.color_attributes.get(COLOR_ATTRIBUTE)
        if existing:
            mesh.color_attributes.remove(existing)
        colors = mesh.color_attributes.new(name=COLOR_ATTRIBUTE, type="FLOAT_COLOR", domain="CORNER")
        for polygon in mesh.polygons:
            color = encode_face_id(face_offset + polygon.index + 1)
            for loop_index in polygon.loop_indices:
                colors.data[loop_index].color = color
        face_offset += len(mesh.polygons)
        mesh.materials.clear()
        mesh.materials.append(material)
        for polygon in mesh.polygons:
            polygon.material_index = 0

    ground.hide_render = True
    world = bpy.context.scene.world
    if world and world.use_nodes:
        background = world.node_tree.nodes.get("Background")
        background.inputs["Color"].default_value = (0.0, 0.0, 0.0, 1.0)
        background.inputs["Strength"].default_value = 0.0

    scene = bpy.context.scene
    scene.use_nodes = False
    scene.render.image_settings.file_format = "OPEN_EXR"
    scene.render.image_settings.color_mode = "RGB"
    scene.render.image_settings.color_depth = "32"
    face_ids = output_dir / "reference_face_id.exr"
    scene.render.filepath = str(face_ids)
    assert_actionless_rest_state(armature)
    bpy.ops.render.render(write_still=True)
    return face_ids, face_offset


def bone_constraints(pose_bone: bpy.types.PoseBone) -> list[dict]:
    result = []
    for constraint in pose_bone.constraints:
        if constraint.type != "LIMIT_ROTATION":
            continue
        result.append(
            {
                "type": constraint.type,
                "space": constraint.owner_space,
                "use_limit_x": bool(constraint.use_limit_x),
                "use_limit_y": bool(constraint.use_limit_y),
                "use_limit_z": bool(constraint.use_limit_z),
                "min": [float(constraint.min_x), float(constraint.min_y), float(constraint.min_z)],
                "max": [float(constraint.max_x), float(constraint.max_y), float(constraint.max_z)],
            }
        )
    return result


def export_skeletons(arms: list[bpy.types.Object]) -> list[dict]:
    result = []
    for armature in arms:
        bones = []
        for bone in armature.data.bones:
            pose_bone = armature.pose.bones.get(bone.name)
            local_matrix = bone.matrix_local.copy()
            parent_local = bone.parent.matrix_local.inverted() @ local_matrix if bone.parent else local_matrix
            bones.append(
                {
                    "name": bone.name,
                    "parent": bone.parent.name if bone.parent else None,
                    "use_deform": bool(bone.use_deform),
                    "helper": not bool(bone.use_deform),
                    "head_local": vector_values(bone.head_local),
                    "tail_local": vector_values(bone.tail_local),
                    "matrix_local": matrix_values(local_matrix),
                    "parent_relative_matrix": matrix_values(parent_local),
                    "length": float(bone.length),
                    "rotation_mode": pose_bone.rotation_mode if pose_bone else None,
                    "joint_limits": bone_constraints(pose_bone) if pose_bone else [],
                }
            )
        result.append(
            {
                "name": armature.name,
                "matrix_world": matrix_values(armature.matrix_world),
                "bones": bones,
            }
        )
    return result


def vertex_groups_for(
    obj: bpy.types.Object,
    vertex: bpy.types.MeshVertex,
    deform_bones: set[str],
) -> list[dict]:
    weights = []
    for element in vertex.groups:
        if element.group >= len(obj.vertex_groups):
            continue
        bone = obj.vertex_groups[element.group].name
        weight = float(element.weight)
        if bone not in deform_bones or weight <= 0.0:
            continue
        if not math.isfinite(weight):
            raise RuntimeError(
                f"mesh {obj.name} vertex {vertex.index} has non-finite deform weight"
            )
        weights.append({"bone": bone, "weight": weight})
    if len(weights) > 4:
        raise RuntimeError(
            f"mesh {obj.name} vertex {vertex.index} has {len(weights)} nonzero deform weights; "
            "fitting v1 supports at most four and never truncates influences"
        )
    total = sum(item["weight"] for item in weights)
    if total <= 1e-12:
        raise RuntimeError(
            f"mesh {obj.name} vertex {vertex.index} has zero-sum deform weights"
        )
    for item in weights:
        item["weight"] /= total
    weights.sort(key=lambda item: (-item["weight"], item["bone"]))
    return weights


def export_mesh_data(
    output_dir: Path,
    meshes: list[bpy.types.Object],
    armature: bpy.types.Object,
) -> tuple[Path, Path, Path, int, int]:
    skin_path = output_dir / "skin_weights.json.gz"
    topology_path = output_dir / "surface_topology.json.gz"
    anchors_path = output_dir / "surface_anchors.json"
    skin_rows = []
    topology_rows = []
    anchors_by_bone: dict[str, list[dict]] = {}
    vertex_offset = 0
    face_offset = 0
    deform_bones = {bone.name for bone in armature.data.bones if bone.use_deform}
    if not deform_bones:
        raise RuntimeError("armature contains no deform bones")

    for obj in meshes:
        world = obj.matrix_world
        for vertex in obj.data.vertices:
            weights = vertex_groups_for(obj, vertex, deform_bones)
            global_vertex = vertex_offset + vertex.index
            row = {
                "vertex_id": global_vertex,
                "object": obj.name,
                "vertex_index": vertex.index,
                "local": vector_values(vertex.co),
                "world": vector_values(world @ vertex.co),
                "weights": weights,
            }
            skin_rows.append(row)
            for weight in weights:
                if weight["weight"] >= 0.08:
                    anchors_by_bone.setdefault(weight["bone"], []).append(
                        {
                            "vertex_id": global_vertex,
                            "object": obj.name,
                            "weight": weight["weight"],
                            "world": row["world"],
                        }
                    )
        for polygon in obj.data.polygons:
            topology_rows.append(
                {
                    "face_id": face_offset + polygon.index + 1,
                    "object": obj.name,
                    "polygon_index": polygon.index,
                    "vertex_ids": [vertex_offset + index for index in polygon.vertices],
                }
            )
        vertex_offset += len(obj.data.vertices)
        face_offset += len(obj.data.polygons)

    with gzip.open(skin_path, "wt", encoding="utf-8") as handle:
        json.dump({"vertices": skin_rows}, handle, separators=(",", ":"))
    with gzip.open(topology_path, "wt", encoding="utf-8") as handle:
        json.dump({"faces": topology_rows}, handle, separators=(",", ":"))

    anchors = []
    for bone_name, candidates in sorted(anchors_by_bone.items()):
        candidates.sort(key=lambda item: (-item["weight"], item["vertex_id"]))
        limit = 24 if re.search(r"hoof|foot|ankle|paw", bone_name, re.I) else 12
        pool = candidates[: max(limit * 8, limit)]
        if len(pool) <= limit:
            selected = pool
        else:
            selected = [pool[round(index * (len(pool) - 1) / (limit - 1))] for index in range(limit)]
        anchors.append({"bone": bone_name, "points": selected})
    anchors_path.write_text(json.dumps({"bones": anchors}, ensure_ascii=False, indent=2), encoding="utf-8")
    return skin_path, topology_path, anchors_path, vertex_offset, face_offset


def camera_metadata(camera: bpy.types.Object, width: int, height: int) -> dict:
    angle_x = float(camera.data.angle_x)
    angle_y = float(camera.data.angle_y)
    return {
        "name": camera.name,
        "resolution": [width, height],
        "lens_mm": float(camera.data.lens),
        "sensor_width_mm": float(camera.data.sensor_width),
        "intrinsics": {
            "fx": width / (2.0 * math.tan(angle_x / 2.0)),
            "fy": height / (2.0 * math.tan(angle_y / 2.0)),
            "cx": width * 0.5,
            "cy": height * 0.5,
        },
        "camera_to_world": matrix_values(camera.matrix_world),
        "world_to_camera": matrix_values(camera.matrix_world.inverted()),
    }


def write_immutable_manifest(
    output_dir: Path,
    files: Iterable[Path],
    *,
    revision: str,
) -> Path:
    root = output_dir.resolve()
    rows = []
    seen: set[str] = set()
    for path in sorted((item.resolve() for item in files), key=lambda item: item.name):
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise RuntimeError(f"immutable artifact escapes output directory: {path}") from exc
        if relative.casefold() in seen:
            raise RuntimeError(f"duplicate immutable artifact filename: {relative}")
        seen.add(relative.casefold())
        if not path.is_file():
            raise RuntimeError(f"immutable artifact is missing: {path}")
        rows.append(
            {
                "filename": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    if not any(row["filename"] == "fitting_bundle.json" for row in rows):
        raise RuntimeError("immutable manifest must include fitting_bundle.json")
    manifest = output_dir / "immutable_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema": IMMUTABLE_MANIFEST_SCHEMA,
                "revision": revision,
                "bundle_file_count": len(rows),
                "bundle_total_bytes": sum(int(row["bytes"]) for row in rows),
                "bundle_manifest": next(
                    {
                        "filename": row["filename"],
                        "bytes": row["bytes"],
                        "sha256": row["sha256"],
                    }
                    for row in rows
                    if row["filename"] == "fitting_bundle.json"
                ),
                "files": rows,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    args = parse_args()
    source = args.input.resolve()
    source_sha256 = sha256_file(source)
    semantic_profile = resolve_semantic_profile(args)
    revision = SEMANTIC_REVISION if semantic_profile is not None else REVISION
    output_dir = args.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise RuntimeError(f"output directory must be fresh and empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.width <= 0 or args.height <= 0:
        raise RuntimeError("resolution must be positive")

    load_source(source)
    armature = require_single_armature()
    actionless = make_actionless(armature)
    meshes = visible_meshes()
    if semantic_profile is not None:
        validate_semantic_profile_source(
            semantic_profile,
            rig_type=args.rig_type,
            filename=source.name,
            source_sha256=source_sha256,
            armature_name=armature.name,
            mesh_names=[obj.name for obj in meshes],
        )
    geometry_contract = validate_rest_geometry_contract(meshes, armature)
    arms = [armature]
    minimum, maximum = model_bounds(meshes)
    fit_points = sampled_model_points(meshes)
    forward = animal_forward_vector(arms)
    # world_to_camera_view() uses the scene render aspect ratio while fitting
    # the camera.  Apply the requested dimensions first so arbitrary source
    # .blend render settings cannot change (or invalidate) canonical framing.
    configure_render(bpy.context.scene, args.width, args.height, args.samples)
    camera = create_camera(minimum, maximum, forward, args.width, args.height, fit_points)
    ground = create_ground(minimum, maximum)
    configure_lighting(minimum, maximum, camera)
    camera_contract = camera_metadata(camera, args.width, args.height)

    skeleton_path = output_dir / "skeleton.json"
    skeleton_path.write_text(
        json.dumps({"armatures": export_skeletons(arms)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    skin_path, topology_path, anchors_path, vertex_count, topology_face_count = export_mesh_data(
        output_dir,
        meshes,
        armature,
    )
    semantic_plan = None
    if semantic_profile is not None:
        with gzip.open(skin_path, "rt", encoding="utf-8") as handle:
            skin_rows = json.load(handle)["vertices"]
        with gzip.open(topology_path, "rt", encoding="utf-8") as handle:
            topology_rows = json.load(handle)["faces"]
        semantic_plan = build_semantic_ltx_plan(
            semantic_profile,
            skin_rows=skin_rows,
            topology_rows=topology_rows,
            available_bones=[bone.name for bone in armature.data.bones if bone.use_deform],
            world_to_camera=camera_contract["world_to_camera"],
        )
    rendered = render_rgb_depth(output_dir, meshes, armature)
    semantic_contract = None
    semantic_pending = None
    if semantic_profile is not None and semantic_plan is not None:
        semantic_path, semantic_base, semantic_alpha, semantic_label_masks = (
            render_semantic_ltx_reference(
                output_dir,
                meshes,
                ground,
                armature,
                rendered["rgb"],
                semantic_profile,
                semantic_plan,
                camera_contract,
            )
        )
        semantic_pending = (
            semantic_path,
            semantic_base,
            semantic_alpha,
            semantic_label_masks,
        )
    rendered["mask"] = render_silhouette_mask(output_dir, meshes, ground, armature)
    mask_framing = analyze_mask_framing(rendered["mask"])
    if semantic_profile is not None and semantic_pending is not None:
        semantic_path, semantic_base, semantic_alpha, semantic_label_masks = semantic_pending
        semantic_contract = finalize_semantic_ltx_contract(
            semantic_profile,
            semantic_base,
            semantic_alpha,
            semantic_label_masks,
            rendered["mask"],
        )
        rendered["ltx_semantic"] = semantic_path
    camera_z_path, camera_z_contract = write_camera_z_artifact(
        output_dir,
        rendered["depth"],
        rendered["mask"],
        camera_contract,
    )
    rendered["camera_z"] = camera_z_path
    face_id_path, rendered_face_count = render_face_ids(output_dir, meshes, ground, armature)
    if rendered_face_count != topology_face_count:
        raise RuntimeError("face-ID render and topology face counts differ")

    metadata_path = output_dir / "fitting_bundle.json"
    artifact_paths = {
        **rendered,
        "face_id": face_id_path,
        "skeleton": skeleton_path,
        "skin_weights": skin_path,
        "surface_topology": topology_path,
        "surface_anchors": anchors_path,
    }
    metadata = {
        "schema": "autorig-actionless-fitting-bundle.v1",
        "revision": revision,
        "source": {
            "filename": source.name,
            "sha256": source_sha256,
            "task_id": args.source_task_id or None,
            "species": args.species,
            "rig_type": args.rig_type,
            "orientation": args.orientation,
        },
        "actionless": actionless,
        "renderer": {
            "blender_version": bpy.app.version_string,
            "engine": bpy.context.scene.render.engine,
            "samples": args.samples,
            "geometry_contract": geometry_contract,
        },
        "camera": {
            **camera_contract,
            "mask_framing": mask_framing,
            "camera_z_contract": camera_z_contract,
        },
        "ground_plane": {"normal": [0.0, 0.0, 1.0], "height": float(minimum.z)},
        "bounds": {"minimum": vector_values(minimum), "maximum": vector_values(maximum)},
        "counts": {
            "meshes": len(meshes),
            "armatures": len(arms),
            "vertices": vertex_count,
            "faces": rendered_face_count,
        },
        "artifacts": {
            name: {
                "filename": path.name,
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
            for name, path in artifact_paths.items()
        },
    }
    if semantic_contract is not None:
        metadata["semantic_ltx_reference"] = semantic_contract
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = write_immutable_manifest(
        output_dir,
        [metadata_path, *artifact_paths.values()],
        revision=revision,
    )
    assert_actionless_rest_state(armature)
    print(
        "AUTORIG_FITTING_BUNDLE="
        + json.dumps(
            {
                "metadata": str(metadata_path),
                "immutable_manifest": str(manifest_path),
                "rgb": str(rendered["rgb"]),
                "camera_z": str(camera_z_path),
                "ltx_semantic": (
                    str(rendered["ltx_semantic"])
                    if "ltx_semantic" in rendered
                    else None
                ),
                "vertices": vertex_count,
                "faces": rendered_face_count,
                "actions_removed": actionless["detached_actions"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException as exc:
        traceback.print_exc()
        print(
            "AUTORIG_FITTING_BUNDLE_ERROR="
            + json.dumps({"type": type(exc).__name__, "message": str(exc)}, ensure_ascii=False),
            file=sys.stderr,
            flush=True,
        )
        # Blender may otherwise turn an exception raised by --python into exit
        # code 0. Force a failing process so launchers cannot publish a partial
        # bundle when the success marker and metadata were never produced.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)
