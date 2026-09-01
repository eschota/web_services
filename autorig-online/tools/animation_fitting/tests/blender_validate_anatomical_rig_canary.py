from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

import bpy
from mathutils import Matrix


def _args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _assert_identity(matrix: Matrix) -> None:
    identity = Matrix.Identity(4)
    assert all(
        abs(float(matrix[row][column] - identity[row][column])) <= 1e-7
        for row in range(4)
        for column in range(4)
    )


def _assert_vector_close(actual, expected, *, tolerance: float = 1e-6) -> None:
    assert all(
        abs(float(actual[index] - expected[index])) <= tolerance
        for index in range(3)
    )


def _assert_no_animation(id_block) -> None:
    animation = getattr(id_block, "animation_data", None)
    if animation is None:
        return
    assert animation.action is None
    assert len(animation.nla_tracks) == 0
    assert len(animation.drivers) == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(_args_after_separator())
    source = args.source.resolve()
    output_dir = args.output_dir.resolve()
    output_blend = output_dir / "anatomical_rig.blend"
    report_path = output_dir / "build_report.json"
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    canonical = profile["canonical_source"]
    parent_map = profile["target"]["parent_map"]
    master_root = profile["target"]["master_root"]

    bpy.ops.wm.open_mainfile(filepath=str(source))
    source_mesh = bpy.data.objects[canonical["mesh_names"][0]]
    source_armature = bpy.data.objects[canonical["armature_name"]]
    source_vertices = tuple(source_mesh.matrix_world @ vertex.co for vertex in source_mesh.data.vertices)
    source_bones = {
        name: (
            source_armature.matrix_world @ source_armature.data.bones[name].head_local,
            source_armature.matrix_world @ source_armature.data.bones[name].tail_local,
        )
        for name in parent_map
    }

    result = bpy.ops.wm.open_mainfile(filepath=str(output_blend))
    assert "FINISHED" in result
    assert tuple(int(value) for value in bpy.app.version[:3]) >= tuple(
        profile["minimum_blender_version"]
    )
    objects = tuple(bpy.context.scene.objects)
    assert len(objects) == 2
    armature = bpy.data.objects[canonical["armature_name"]]
    mesh = bpy.data.objects[canonical["mesh_names"][0]]
    assert armature.type == "ARMATURE"
    assert mesh.type == "MESH"
    _assert_identity(armature.matrix_world)
    _assert_identity(mesh.matrix_world)
    assert mesh.parent is None
    assert armature.data.pose_position == "REST"

    assert {bone.name for bone in armature.data.bones} == set(parent_map) | {master_root}
    assert {bone.name for bone in armature.data.bones if bone.use_deform} == set(parent_map)
    root = armature.data.bones[master_root]
    assert root.parent is None
    assert root.use_deform is False
    assert root.bbone_segments == 1
    for name, source_points in source_bones.items():
        bone = armature.data.bones[name]
        expected_parent = master_root if parent_map[name] is None else parent_map[name]
        assert bone.parent.name == expected_parent
        assert bone.use_connect is False
        assert bone.use_deform is True
        assert bone.bbone_segments == 1
        _assert_vector_close(bone.head_local, source_points[0])
        _assert_vector_close(bone.tail_local, source_points[1])

    assert len(mesh.data.vertices) == canonical["vertex_count"] == len(source_vertices)
    for source_point, output_vertex in zip(source_vertices, mesh.data.vertices):
        _assert_vector_close(output_vertex.co, source_point)
    assert mesh.data.shape_keys is None
    assert set(group.name for group in mesh.vertex_groups) == set(parent_map)
    assert len(mesh.modifiers) == 1
    assert mesh.modifiers[0].type == "ARMATURE"
    assert mesh.modifiers[0].object is armature
    group_names = {group.index: group.name for group in mesh.vertex_groups}
    for vertex in mesh.data.vertices:
        weights = [
            float(item.weight)
            for item in vertex.groups
            if item.group in group_names and item.weight > 0.0
        ]
        assert 1 <= len(weights) <= canonical["maximum_vertex_influences"]
        assert all(math.isfinite(weight) and weight > 0.0 for weight in weights)
        assert abs(math.fsum(weights) - 1.0) <= 1e-6

    assert armature["autorig_usage"] == "reference_render_only"
    assert armature["autorig_fitting_ready"] is False
    assert armature["autorig_blocker_state"] == "blocked"
    assert json.loads(armature["autorig_blocking_reasons_json"]) == profile[
        "approval_contract"
    ]["blocking_reasons"]
    assert len(bpy.data.actions) == 0
    assert len(armature.constraints) == 0
    for pose_bone in armature.pose.bones:
        assert len(pose_bone.constraints) == 0
        assert pose_bone.custom_shape is None
    for id_block in (armature, armature.data, mesh, mesh.data):
        _assert_no_animation(id_block)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["schema"] == "autorig-anatomical-rig-build.v1"
    assert report["status"] == "built_reference_only"
    assert report["usage"] == "reference_render_only"
    assert report["approval"]["artifact_fitting_ready"] is False
    assert report["approval"]["blocking_reasons"] == profile["approval_contract"][
        "blocking_reasons"
    ]
    assert report["source"]["sha256"] == _sha256(source)
    assert report["profile"]["sha256"] == _sha256(args.profile)
    assert report["output"]["blend"]["sha256"] == _sha256(output_blend)
    assert report["output"]["counts"]["deform_bones"] == 51
    assert set(path.name for path in output_dir.iterdir()) == {
        "anatomical_rig.blend",
        "build_report.json",
    }
    print("AUTORIG_ANATOMICAL_RIG_CANARY=OK")


if __name__ == "__main__":
    main()
