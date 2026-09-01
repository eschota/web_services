from __future__ import annotations

import argparse
from pathlib import Path
import sys
import traceback

import bpy


def action_fcurves(action):
    legacy = getattr(action, "fcurves", None)
    if legacy is not None:
        return list(legacy)
    result = []
    for layer in action.layers:
        for strip in layer.strips:
            for channelbag in strip.channelbags:
                result.extend(channelbag.fcurves)
    return result


def args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--action-id", required=True)
    parser.add_argument("--mode", choices=("blend", "fbx"), required=True)
    args = parser.parse_args(args_after_separator())
    if args.mode == "blend":
        result = bpy.ops.wm.open_mainfile(filepath=str(args.source.resolve()), load_ui=False)
    else:
        bpy.ops.wm.read_factory_settings(use_empty=True)
        result = bpy.ops.import_scene.fbx(filepath=str(args.source.resolve()), use_anim=True)
    if "FINISHED" not in result:
        raise RuntimeError(f"Cannot load canary artifact: {result}")
    armatures = [obj for obj in bpy.data.objects if obj.type == "ARMATURE"]
    meshes = [obj for obj in bpy.data.objects if obj.type == "MESH"]
    if len(armatures) != 1 or not meshes:
        raise RuntimeError("Canary artifact lacks exactly one armature and at least one mesh")
    matching = [action for action in bpy.data.actions if args.action_id in action.name]
    if len(matching) != 1:
        raise RuntimeError(
            f"Expected one action containing {args.action_id!r}, got {[action.name for action in bpy.data.actions]}"
        )
    frame_range = matching[0].frame_range
    if abs(float(frame_range[1] - frame_range[0]) - 2.0) > 1e-4:
        raise RuntimeError(f"Canary animation duration is not two source frames: {tuple(frame_range)}")
    if args.mode == "blend":
        if len(bpy.data.actions) != 1 or matching[0].name != args.action_id:
            raise RuntimeError("Derived blend does not contain exactly the semantic Action")
        armature = armatures[0]
        if armature.animation_data is None or armature.animation_data.action != matching[0]:
            raise RuntimeError("Derived blend semantic Action is not active")
        if armature.animation_data.nla_tracks:
            raise RuntimeError("Derived blend contains NLA tracks")
        fcurves = action_fcurves(matching[0])
        child_locations = [
            curve for curve in fcurves
            if curve.data_path == 'pose.bones["Child"].location'
        ]
        root_locations = [
            curve for curve in fcurves
            if curve.data_path == 'pose.bones["Root"].location'
        ]
        rotations = [curve for curve in fcurves if curve.data_path.endswith("rotation_quaternion")]
        if child_locations or len(root_locations) != 3 or len(rotations) != 8:
            raise RuntimeError("Quaternion/root-only translation channel contract is invalid")
        if any(len(curve.keyframe_points) != 3 for curve in fcurves):
            raise RuntimeError("Canary Action does not key every source frame")
        if any(point.interpolation != "LINEAR" for curve in fcurves for point in curve.keyframe_points):
            raise RuntimeError("Canary Action interpolation is not LINEAR")
        bpy.context.scene.frame_set(0)
        bpy.context.view_layer.update()
        world_zero = (armature.matrix_world @ armature.pose.bones["Root"].matrix).translation.copy()
        bpy.context.scene.frame_set(1)
        bpy.context.view_layer.update()
        world_one = (armature.matrix_world @ armature.pose.bones["Root"].matrix).translation.copy()
        delta = world_one - world_zero
        if abs(float(delta.x) - 0.2) > 1e-4 or abs(float(delta.y)) > 1e-4 or abs(float(delta.z)) > 1e-4:
            raise RuntimeError(f"Canary world-space root translation did not survive bake: {tuple(delta)}")
    print(f"AUTORIG_APPLIED_CANARY_OK={args.mode}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
