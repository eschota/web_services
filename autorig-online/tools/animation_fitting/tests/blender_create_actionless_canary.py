from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import bpy


def args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :]


def save(path: Path) -> None:
    result = bpy.ops.wm.save_as_mainfile(
        filepath=str(path),
        check_existing=False,
        compress=True,
    )
    if "FINISHED" not in result:
        raise RuntimeError(f"Cannot save actionless canary source: {result}")


def action_with_curve(name: str, data_path: str, index: int | None, values: tuple[float, float]):
    action = bpy.data.actions.new(name)
    curve = action.fcurves.new(data_path=data_path, index=index) if index is not None else action.fcurves.new(data_path=data_path)
    curve.keyframe_points.insert(0.0, values[0])
    curve.keyframe_points.insert(5.0, values[1])
    return action


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(args_after_separator())
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bpy.ops.wm.read_factory_settings(use_empty=True)

    armature_data = bpy.data.armatures.new("CanaryRigData")
    armature = bpy.data.objects.new("CanaryRig", armature_data)
    bpy.context.scene.collection.objects.link(armature)
    bpy.context.view_layer.objects.active = armature
    armature.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")
    specifications = (
        ("Root", (0.0, 0.0, 0.0), (0.0, 0.0, 0.8), None),
        ("Spine", (0.0, 0.0, 0.8), (0.0, 0.0, 1.4), "Root"),
        ("Head", (0.0, 0.35, 1.35), (0.0, 0.75, 1.65), "Spine"),
        ("LegA", (-0.3, 0.0, 0.6), (-0.3, 0.0, 0.0), "Root"),
        ("LegB", (0.3, 0.0, 0.6), (0.3, 0.0, 0.0), "Root"),
    )
    edit_bones = {}
    for name, head, tail, parent in specifications:
        bone = armature_data.edit_bones.new(name)
        bone.head = head
        bone.tail = tail
        if parent:
            bone.parent = edit_bones[parent]
        edit_bones[name] = bone
    bpy.ops.object.mode_set(mode="OBJECT")

    # A deliberately simple but horse-like landscape volume.  Keeping the
    # canary longer than it is tall exercises the same 16:9 framing path as a
    # quadruped reference instead of accidentally testing a portrait biped.
    vertices = [
        (-0.45, -1.40, 0.05),
        (0.45, -1.40, 0.05),
        (0.45, 1.40, 0.05),
        (-0.45, 1.40, 0.05),
        (-0.45, -1.40, 1.15),
        (0.45, -1.40, 1.15),
        (0.45, 1.40, 1.15),
        (-0.45, 1.40, 1.15),
    ]
    faces = [
        (0, 1, 2, 3),
        (4, 7, 6, 5),
        (0, 4, 5, 1),
        (1, 5, 6, 2),
        (2, 6, 7, 3),
        (4, 0, 3, 7),
    ]
    mesh_data = bpy.data.meshes.new("CanaryMeshData")
    mesh_data.from_pydata(vertices, [], faces)
    mesh_data.update()
    mesh = bpy.data.objects.new("CanaryMesh", mesh_data)
    bpy.context.scene.collection.objects.link(mesh)
    groups = {name: mesh.vertex_groups.new(name=name) for name, *_ in specifications}
    for vertex in range(len(vertices)):
        groups["Root"].add([vertex], 0.2, "REPLACE")
        groups["Head"].add([vertex], 0.3, "REPLACE")
    modifier = mesh.modifiers.new(name="CanaryArmature", type="ARMATURE")
    modifier.object = armature

    mesh.shape_key_add(name="Basis")
    poison = mesh.shape_key_add(name="PoisonShape")
    for point in poison.data:
        point.co.x += 3.0
    poison.value = 0.0
    poison_driver = poison.driver_add("value")
    poison_driver.driver.expression = "1.0"

    target = bpy.data.objects.new("PoisonTarget", None)
    target.location = (4.0, 0.0, 3.0)
    bpy.context.scene.collection.objects.link(target)
    object_constraint = mesh.constraints.new("COPY_LOCATION")
    object_constraint.name = "PoisonObjectConstraint"
    object_constraint.target = target
    pose_constraint = armature.pose.bones["Head"].constraints.new("COPY_LOCATION")
    pose_constraint.name = "PoisonPoseConstraint"
    pose_constraint.target = target
    armature.pose.bones["Head"].rotation_mode = "XYZ"
    armature.pose.bones["Head"].rotation_euler.x = 0.7

    armature.animation_data_create().action = action_with_curve(
        "PoisonObjectAction",
        "location",
        0,
        (0.0, 2.0),
    )
    armature_data.animation_data_create().action = action_with_curve(
        "PoisonDataAction",
        "axes_position",
        None,
        (0.0, 1.0),
    )
    mesh.animation_data_create()
    nla_action = action_with_curve("PoisonNLAAction", "location", 1, (0.0, 2.0))
    nla_track = mesh.animation_data.nla_tracks.new()
    nla_track.name = "PoisonNLA"
    nla_track.strips.new("PoisonStrip", 0, nla_action)
    scale_driver = armature.driver_add("scale", 0)
    scale_driver.driver.expression = "2.0"
    bpy.context.scene.frame_set(5)
    bpy.context.view_layer.update()

    valid = output / "actionless_valid_source.blend"
    save(valid)

    geometry_modifier = mesh.modifiers.new(name="PoisonSubdivision", type="SUBSURF")
    geometry_modifier.levels = 1
    geometry_modifier.render_levels = 1
    modifier_source = output / "actionless_geometry_modifier_source.blend"
    save(modifier_source)
    mesh.modifiers.remove(geometry_modifier)

    second_data = bpy.data.armatures.new("SecondRigData")
    second = bpy.data.objects.new("SecondRig", second_data)
    bpy.context.scene.collection.objects.link(second)
    two_armatures = output / "actionless_two_armatures_source.blend"
    save(two_armatures)
    bpy.data.objects.remove(second, do_unlink=True)
    bpy.data.armatures.remove(second_data)

    for group in groups.values():
        group.add([0], 0.2, "REPLACE")
    five_weights = output / "actionless_five_weights_source.blend"
    save(five_weights)

    print(
        "AUTORIG_ACTIONLESS_CANARY="
        + json.dumps(
            {
                "valid": str(valid),
                "geometry_modifier": str(modifier_source),
                "two_armatures": str(two_armatures),
                "five_weights": str(five_weights),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
