from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import bpy


def args_after_separator() -> list[str]:
    return sys.argv[sys.argv.index("--") + 1 :]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def append_box(
    vertices: list[tuple[float, float, float]],
    faces: list[tuple[int, int, int, int]],
    minimum: tuple[float, float, float],
    maximum: tuple[float, float, float],
) -> list[int]:
    offset = len(vertices)
    min_x, min_y, min_z = minimum
    max_x, max_y, max_z = maximum
    vertices.extend(
        (
            (min_x, min_y, min_z),
            (max_x, min_y, min_z),
            (max_x, max_y, min_z),
            (min_x, max_y, min_z),
            (min_x, min_y, max_z),
            (max_x, min_y, max_z),
            (max_x, max_y, max_z),
            (min_x, max_y, max_z),
        )
    )
    faces.extend(
        tuple(offset + index for index in face)
        for face in (
            (0, 1, 2, 3),
            (4, 7, 6, 5),
            (0, 4, 5, 1),
            (1, 5, 6, 2),
            (2, 6, 7, 3),
            (4, 0, 3, 7),
        )
    )
    return list(range(offset, offset + 8))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(args_after_separator())
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bpy.ops.wm.read_factory_settings(use_empty=True)

    armature_data = bpy.data.armatures.new("SemanticRigData")
    armature = bpy.data.objects.new("SemanticRig", armature_data)
    bpy.context.scene.collection.objects.link(armature)
    bpy.context.view_layer.objects.active = armature
    armature.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")
    bones = {
        "Root": ((0.0, 0.0, 0.0), (0.0, 0.0, 0.8)),
        "ForeLeft": ((0.30, -0.90, 0.2), (0.30, -0.90, -0.8)),
        "ForeRight": ((-0.30, -0.90, 0.2), (-0.30, -0.90, -0.8)),
        "HindLeft": ((0.30, 0.90, 0.2), (0.30, 0.90, -0.8)),
        "HindRight": ((-0.30, 0.90, 0.2), (-0.30, 0.90, -0.8)),
    }
    for name, (head, tail) in bones.items():
        bone = armature_data.edit_bones.new(name)
        bone.head = head
        bone.tail = tail
    bpy.ops.object.mode_set(mode="OBJECT")

    vertices: list[tuple[float, float, float]] = []
    faces: list[tuple[int, int, int, int]] = []
    body_vertices = append_box(vertices, faces, (-0.50, -1.40, 0.00), (0.50, 1.40, 1.00))
    limb_vertices = {
        "ForeLeft": append_box(
            vertices, faces, (0.18, -1.10, -0.90), (0.42, -0.75, 0.20)
        ),
        "ForeRight": append_box(
            vertices, faces, (-0.42, -1.10, -0.90), (-0.18, -0.75, 0.20)
        ),
        "HindLeft": append_box(
            vertices, faces, (0.18, 0.75, -0.90), (0.42, 1.10, 0.20)
        ),
        "HindRight": append_box(
            vertices, faces, (-0.42, 0.75, -0.90), (-0.18, 1.10, 0.20)
        ),
    }
    mesh_data = bpy.data.meshes.new("SemanticHorseData")
    mesh_data.from_pydata(vertices, [], faces)
    mesh_data.update()
    mesh = bpy.data.objects.new("SemanticHorse", mesh_data)
    bpy.context.scene.collection.objects.link(mesh)
    groups = {name: mesh.vertex_groups.new(name=name) for name in bones}
    groups["Root"].add(body_vertices, 1.0, "REPLACE")
    for bone, indices in limb_vertices.items():
        groups["Root"].add(indices, 0.25, "REPLACE")
        groups[bone].add(indices, 0.75, "REPLACE")
    modifier = mesh.modifiers.new(name="SemanticArmature", type="ARMATURE")
    modifier.object = armature

    material = bpy.data.materials.new("CanonicalHorseBrown")
    material.use_nodes = True
    bsdf = material.node_tree.nodes.get("Principled BSDF")
    bsdf.inputs["Base Color"].default_value = (0.18, 0.055, 0.018, 1.0)
    bsdf.inputs["Roughness"].default_value = 0.72
    mesh.data.materials.append(material)

    source = output / "semantic_horse_source.blend"
    result = bpy.ops.wm.save_as_mainfile(
        filepath=str(source),
        check_existing=False,
        compress=True,
    )
    if "FINISHED" not in result:
        raise RuntimeError(f"Cannot save semantic canary source: {result}")
    profile = output / "semantic_horse_profile.v1.json"
    profile.write_text(
        json.dumps(
            {
                "schema": "autorig-semantic-ltx-profile.v1",
                "profile_id": "semantic_horse_canary.v1",
                "source": {
                    "rig_type": "SEMANTIC_CANARY",
                    "filename": source.name,
                    "sha256": sha256_file(source),
                    "armature_name": armature.name,
                    "mesh_names": [mesh.name],
                },
                "limb_groups": {
                    "fore_left": {
                        "anatomy": "fore",
                        "side": "left",
                        "bones": ["ForeLeft"],
                    },
                    "fore_right": {
                        "anatomy": "fore",
                        "side": "right",
                        "bones": ["ForeRight"],
                    },
                    "hind_left": {
                        "anatomy": "hind",
                        "side": "left",
                        "bones": ["HindLeft"],
                    },
                    "hind_right": {
                        "anatomy": "hind",
                        "side": "right",
                        "bones": ["HindRight"],
                    },
                },
                "palette_linear": {
                    "body": [0.46, 0.50, 0.56],
                    "fore_near": [0.00, 0.85, 1.00],
                    "fore_far": [0.12, 0.22, 1.00],
                    "hind_near": [1.00, 0.72, 0.02],
                    "hind_far": [1.00, 0.08, 0.55],
                },
                "gates": {
                    "minimum_face_limb_weight": 0.60,
                    "minimum_face_dominance": 0.95,
                    "minimum_faces_per_source_group": 4,
                    "minimum_group_weight_mass": 4.0,
                    "minimum_near_far_depth_separation": 0.05,
                    "minimum_pixels_per_output_label": 50,
                    "minimum_pixel_fraction_of_mask": 0.003,
                    "maximum_mask_mismatch_pixels": 0,
                    "mask_threshold": 0.50,
                    "pixel_color_tolerance": 0.08,
                    "minimum_palette_distance": 0.55,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        "AUTORIG_SEMANTIC_CANARY="
        + json.dumps(
            {
                "source": str(source),
                "profile": str(profile),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
