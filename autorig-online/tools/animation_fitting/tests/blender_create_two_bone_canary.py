from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import bpy
from mathutils import Matrix


def args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :]


def flat(matrix: Matrix) -> list[float]:
    return [float(matrix[row][column]) for row in range(4) for column in range(4)]


def quaternion_xyzw(matrix: Matrix) -> list[float]:
    quaternion = matrix.to_quaternion().normalized()
    return [float(quaternion.x), float(quaternion.y), float(quaternion.z), float(quaternion.w)]


def bone_payload(parent: str | None, matrix: Matrix) -> dict:
    return {
        "parent": parent,
        "local_matrix": flat(matrix),
        "local_translation": [float(value) for value in matrix.to_translation()],
        "local_rotation_xyzw": quaternion_xyzw(matrix),
    }


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(args_after_separator())
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bpy.ops.wm.read_factory_settings(use_empty=True)

    armature_data = bpy.data.armatures.new("HorseRigData")
    armature = bpy.data.objects.new("HorseRig", armature_data)
    bpy.context.scene.collection.objects.link(armature)
    armature.matrix_world = Matrix.Translation((1.0, 2.0, 0.25)) @ Matrix.Rotation(0.15, 4, "Z")
    bpy.context.view_layer.objects.active = armature
    armature.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")
    root = armature_data.edit_bones.new("Root")
    root.head = (0.0, 0.0, 0.0)
    root.tail = (0.0, 0.0, 1.0)
    child = armature_data.edit_bones.new("Child")
    child.head = (0.0, 0.0, 1.0)
    child.tail = (0.0, 0.0, 2.0)
    child.parent = root
    child.use_connect = True
    bpy.ops.object.mode_set(mode="OBJECT")

    mesh_data = bpy.data.meshes.new("HorseMeshData")
    mesh_data.from_pydata(
        [(-0.2, 0.0, 0.1), (0.2, 0.0, 0.1), (-0.2, 0.0, 1.9), (0.2, 0.0, 1.9)],
        [],
        [(0, 1, 2), (1, 3, 2)],
    )
    mesh_data.update()
    mesh = bpy.data.objects.new("HorseMesh", mesh_data)
    bpy.context.scene.collection.objects.link(mesh)
    root_group = mesh.vertex_groups.new(name="Root")
    root_group.add([0, 1], 1.0, "REPLACE")
    child_group = mesh.vertex_groups.new(name="Child")
    child_group.add([2, 3], 1.0, "REPLACE")
    modifier = mesh.modifiers.new(name="HorseArmature", type="ARMATURE")
    modifier.object = armature
    mesh.parent = armature

    source = output / "horse_source.blend"
    result = bpy.ops.wm.save_as_mainfile(filepath=str(source), check_existing=False, compress=True)
    if "FINISHED" not in result:
        raise RuntimeError(f"Cannot save canary source: {result}")
    source_sha = sha256(source)

    root_rest = armature.data.bones["Root"].matrix_local.copy()
    child_rest = root_rest.inverted() @ armature.data.bones["Child"].matrix_local
    root_world_rest = armature.matrix_world @ root_rest
    frames = []
    for frame, (translation, angle) in enumerate(((0.0, 0.0), (0.2, 0.3), (0.4, -0.2))):
        root_local = Matrix.Translation((translation, 0.0, 0.0)) @ root_world_rest
        child_local = child_rest @ Matrix.Rotation(angle, 4, "X")
        frames.append(
            {
                "frame": frame,
                "time_seconds": frame / 24.0,
                "root_translation": [translation, 0.0, 0.0],
                "root_rotation_rotvec": [0.0, 0.0, 0.0],
                "bones": {
                    "Root": bone_payload(None, root_local),
                    "Child": bone_payload("Root", child_local),
                },
            }
        )
    motion = {
        "schema": "autorig-fitted-animation.v1",
        "rig_bundle": {"path": "synthetic", "sha256": "0" * 64},
        "observations": {"path": "synthetic", "sha256": "1" * 64, "provenance": {}},
        "frame_count": 3,
        "fps": 24.0,
        "duration_seconds": 2.0 / 24.0,
        "loop": False,
        "transform_contract": {
            "schema": "autorig-fitted-transform-contract.v1",
            "source_armature_name": "HorseRig",
            "source_armature_world_matrix": flat(armature.matrix_world),
            "root_local_matrix_space": "WORLD",
            "child_local_matrix_space": "PARENT_BONE",
            "rotation_channel": "QUATERNION",
            "scale_animation": False,
            "translation_policy": {"mode": "root_only", "bones": ["Root"]},
        },
        "active_bones": ["Child"],
        "degrees_of_freedom": ["Child.x"],
        "config": {},
        "optimizer": {"success": True},
        "qa": {"decision": None},
        "frames": frames,
    }
    motion_path = output / "horse_motion.json"
    motion_path.write_text(json.dumps(motion, indent=2) + "\n", encoding="utf-8")
    target = {
        "schema": "autorig-motion-target.v1",
        "source_sha256": source_sha,
        "armature_name": "HorseRig",
        "armature_data_name": "HorseRigData",
        "bone_names": ["Root", "Child"],
        "bone_parents": {"Root": None, "Child": "Root"},
    }
    target_path = output / "horse_target.json"
    target_path.write_text(json.dumps(target, indent=2) + "\n", encoding="utf-8")
    print(
        "AUTORIG_TWO_BONE_CANARY="
        + json.dumps(
            {
                "source": str(source),
                "source_sha256": source_sha,
                "motion": str(motion_path),
                "target": str(target_path),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
