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


def _topological_order(parent_map: dict[str, str | None]) -> tuple[str, ...]:
    result: list[str] = []
    pending = dict(parent_map)
    while pending:
        ready = sorted(
            name
            for name, parent in pending.items()
            if parent is None or parent in result
        )
        if not ready:
            raise RuntimeError("Canary parent map is cyclic")
        for name in ready:
            result.append(name)
            pending.pop(name)
    return tuple(result)


def _save(path: Path) -> None:
    result = bpy.ops.wm.save_as_mainfile(
        filepath=str(path),
        check_existing=False,
        compress=True,
    )
    if "FINISHED" not in result:
        raise RuntimeError(f"Cannot save anatomical source canary: {result}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile-template", type=Path, required=True)
    args = parser.parse_args(_args_after_separator())
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    profile = json.loads(args.profile_template.read_text(encoding="utf-8"))
    canonical = profile["canonical_source"]
    parent_map = profile["target"]["parent_map"]
    bone_names = _topological_order(parent_map)
    if len(bone_names) != 51:
        raise RuntimeError("The horse canary requires the exact 51-bone profile")

    bpy.ops.wm.read_factory_settings(use_empty=True)
    armature_data = bpy.data.armatures.new(f"{canonical['armature_name']}_Data")
    armature = bpy.data.objects.new(canonical["armature_name"], armature_data)
    bpy.context.scene.collection.objects.link(armature)
    bpy.context.view_layer.objects.active = armature
    armature.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")
    created = {}
    bbone_names = set(profile["linearization"]["bbone_bones"])
    for index, name in enumerate(bone_names):
        row = index // 9
        column = index % 9
        head = (
            -1.2 + column * 0.3,
            -0.9 + row * 0.34,
            0.45 + (index % 4) * 0.08,
        )
        tail = (head[0] + 0.03, head[1] + 0.02, head[2] + 0.22)
        bone = armature_data.edit_bones.new(name)
        bone.head = head
        bone.tail = tail
        bone.use_deform = True
        bone.bbone_segments = 4 if name in bbone_names else 1
        parent = parent_map[name]
        if parent is not None:
            bone.parent = created[parent]
        created[name] = bone
    bpy.ops.object.mode_set(mode="OBJECT")

    ring_count = 43
    ring_size = 8
    vertices = []
    for ring in range(ring_count):
        y = -2.1 + 4.2 * ring / (ring_count - 1)
        for step in range(ring_size):
            angle = 2.0 * math.pi * step / ring_size
            vertices.append((0.48 * math.cos(angle), y, 0.9 + 0.32 * math.sin(angle)))
    faces = []
    for ring in range(ring_count - 1):
        for step in range(ring_size):
            following = (step + 1) % ring_size
            faces.append(
                (
                    ring * ring_size + step,
                    ring * ring_size + following,
                    (ring + 1) * ring_size + following,
                    (ring + 1) * ring_size + step,
                )
            )
    if len(vertices) != 344:
        raise RuntimeError("Canary must contain exactly 344 vertices")

    mesh_data = bpy.data.meshes.new(f"{canonical['mesh_names'][0]}_Data")
    mesh_data.from_pydata(vertices, [], faces)
    mesh_data.update()
    mesh = bpy.data.objects.new(canonical["mesh_names"][0], mesh_data)
    bpy.context.scene.collection.objects.link(mesh)
    groups = {name: mesh.vertex_groups.new(name=name) for name in bone_names}
    for index in range(len(vertices)):
        first = bone_names[index % len(bone_names)]
        second = bone_names[(index + 7) % len(bone_names)]
        groups[first].add([index], 0.25, "REPLACE")
        groups[second].add([index], 0.75, "REPLACE")
    modifier = mesh.modifiers.new(name="HorseArmature", type="ARMATURE")
    modifier.object = armature
    modifier.use_vertex_groups = True

    world = Matrix.Translation((0.7, -0.4, 1.1)) @ Matrix.Rotation(0.17, 4, "Z")
    armature.matrix_world = world
    mesh.matrix_world = world
    bpy.context.view_layer.update()

    source = output_dir / canonical["filename"]
    _save(source)
    profile["canonical_source"]["sha256"] = _sha256(source)
    generated_profile = output_dir / "canary_profile.json"
    generated_profile.write_text(
        json.dumps(profile, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "AUTORIG_ANATOMICAL_SOURCE_CANARY="
        + json.dumps(
            {
                "source": str(source),
                "source_sha256": profile["canonical_source"]["sha256"],
                "profile": str(generated_profile),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
