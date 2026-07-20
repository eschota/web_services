#!/usr/bin/env python3
"""Validate AutoRig artifacts from Blender's background mode.

Usage:
  blender -b --python validate_blender_artifacts.py -- --output report.json FILE...
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import traceback
from pathlib import Path

import bpy


def _matrix_values(matrix):
    return tuple(value for row in matrix for value in row)


def _action_frame_range(action):
    try:
        start, end = action.frame_range
        return [float(start), float(end)]
    except Exception:
        return None


def _action_curve_count(action):
    try:
        return len(action.fcurves)
    except Exception:
        pass
    try:
        return sum(len(strip.channelbag(action.slot).fcurves) for strip in action.layers[0].strips)
    except Exception:
        return None


def _import_artifact(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".blend":
        bpy.ops.wm.open_mainfile(filepath=str(path))
    elif suffix == ".fbx":
        if bpy.app.version >= (4, 4, 0):
            bpy.ops.wm.fbx_import(filepath=str(path))
        else:
            bpy.ops.import_scene.fbx(filepath=str(path))
    elif suffix in {".glb", ".gltf"}:
        bpy.ops.import_scene.gltf(filepath=str(path))
    else:
        raise ValueError(f"Unsupported Blender artifact: {suffix}")


def _detect_motion(armatures, actions):
    if not armatures or not actions:
        return False
    scene = bpy.context.scene
    armature = armatures[0]
    if armature.animation_data is None:
        armature.animation_data_create()

    for action in actions:
        frame_range = _action_frame_range(action)
        if not frame_range or math.isclose(frame_range[0], frame_range[1]):
            continue
        try:
            armature.animation_data.action = action
        except Exception:
            continue
        samples = [frame_range[0], (frame_range[0] + frame_range[1]) / 2.0, frame_range[1]]
        poses = []
        for frame in samples:
            scene.frame_set(round(frame))
            bpy.context.view_layer.update()
            poses.append([_matrix_values(bone.matrix.copy()) for bone in armature.pose.bones])
        for before, after in zip(poses, poses[1:]):
            if any(
                any(abs(a - b) > 1e-5 for a, b in zip(left, right))
                for left, right in zip(before, after)
            ):
                return True
    return False


def validate(path: Path):
    bpy.ops.wm.read_factory_settings(use_empty=True)
    result = {
        "path": str(path),
        "bytes": path.stat().st_size if path.exists() else 0,
        "ok": False,
    }
    if not path.exists():
        result["error"] = "file does not exist"
        return result

    try:
        _import_artifact(path)
        meshes = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
        armatures = [obj for obj in bpy.context.scene.objects if obj.type == "ARMATURE"]
        actions = list(bpy.data.actions)
        images = list(bpy.data.images)
        external_images = [image for image in images if image.source == "FILE" and not image.packed_file]
        missing_images = [image for image in external_images if not bpy.path.abspath(image.filepath) or not Path(bpy.path.abspath(image.filepath)).exists()]
        result.update(
            {
                "ok": True,
                "objects": len(bpy.context.scene.objects),
                "meshes": len(meshes),
                "vertices": sum(len(obj.data.vertices) for obj in meshes),
                "materials": len(bpy.data.materials),
                "armatures": len(armatures),
                "bones": sum(len(obj.data.bones) for obj in armatures),
                "bone_names": sorted({bone.name for obj in armatures for bone in obj.data.bones}),
                "actions": [
                    {
                        "name": action.name,
                        "frame_range": _action_frame_range(action),
                        "fcurves": _action_curve_count(action),
                    }
                    for action in actions
                ],
                "animated_bone_matrices": _detect_motion(armatures, actions),
                "images": len(images),
                "packed_images": sum(1 for image in images if image.packed_file),
                "external_images": len(external_images),
                "missing_external_images": [image.filepath for image in missing_images],
            }
        )
    except Exception as exc:
        result["error"] = str(exc)
        result["traceback"] = traceback.format_exc()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("files", nargs="+")
    args = parser.parse_args(sys.argv[sys.argv.index("--") + 1 :])

    report = {
        "blender_version": bpy.app.version_string,
        "artifacts": [validate(Path(raw).resolve()) for raw in args.files],
    }
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=True))
    return 0 if all(item["ok"] for item in report["artifacts"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
