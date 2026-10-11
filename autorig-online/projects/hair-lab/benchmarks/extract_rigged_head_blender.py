"""Blender-side extraction of current rig head-weight ROI.

Invoke with Blender: blender --background --python this.py -- --source x.glb
--output x.npz --receipt x.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import bpy
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(sys.argv[sys.argv.index("--") + 1 :])


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    args = parse_args()
    started = time.perf_counter()
    bpy.ops.wm.read_factory_settings(use_empty=True)
    import_start = time.perf_counter()
    bpy.ops.import_scene.gltf(filepath=str(args.source))
    import_seconds = time.perf_counter() - import_start
    all_vertices = []
    all_normals = []
    all_faces = []
    all_source_faces = []
    inventories = []
    offset = 0
    source_face_offset = 0
    matched_groups = set()
    for obj in [item for item in bpy.context.scene.objects if item.type == "MESH"]:
        obj.data.calc_loop_triangles()
        group_names = {group.index: group.name for group in obj.vertex_groups}
        head_group_ids = {
            index for index, name in group_names.items()
            if "head" in name.lower() and not any(word in name.lower() for word in ("end", "top", "nub"))
        }
        matched_groups.update(group_names[index] for index in head_group_ids)
        weights = np.zeros(len(obj.data.vertices), dtype=np.float32)
        for vertex in obj.data.vertices:
            for membership in vertex.groups:
                if membership.group in head_group_ids:
                    weights[vertex.index] = max(weights[vertex.index], membership.weight)
        world3 = obj.matrix_world.to_3x3()
        vertices_world = np.asarray([obj.matrix_world @ vertex.co for vertex in obj.data.vertices], dtype=np.float32)
        normals_world = np.asarray([(world3 @ vertex.normal).normalized() for vertex in obj.data.vertices], dtype=np.float32)
        # Blender is Z-up. Convert to a right-handed canonical Y-up basis.
        vertices = np.column_stack((vertices_world[:, 0], vertices_world[:, 2], -vertices_world[:, 1])).astype(np.float32)
        normals = np.column_stack((normals_world[:, 0], normals_world[:, 2], -normals_world[:, 1])).astype(np.float32)
        selected_faces = []
        selected_source = []
        for local_face_id, tri in enumerate(obj.data.loop_triangles):
            ids = np.asarray(tri.vertices, dtype=np.int64)
            if weights[ids].max(initial=0.0) >= 0.20:
                selected_faces.append(ids)
                selected_source.append(source_face_offset + local_face_id)
        inventories.append({
            "object": obj.name,
            "vertices": len(obj.data.vertices),
            "triangles": len(obj.data.loop_triangles),
            "head_groups": sorted(group_names[index] for index in head_group_ids),
            "selected_triangles": len(selected_faces),
        })
        if selected_faces:
            selected_faces = np.asarray(selected_faces, dtype=np.int64)
            used, inverse = np.unique(selected_faces.reshape(-1), return_inverse=True)
            all_vertices.append(vertices[used])
            all_normals.append(normals[used])
            all_faces.append(inverse.reshape(-1, 3).astype(np.uint32) + offset)
            all_source_faces.append(np.asarray(selected_source, dtype=np.uint32))
            offset += used.shape[0]
        source_face_offset += len(obj.data.loop_triangles)
    if not all_faces:
        raise RuntimeError("no triangles influenced by a head-named vertex group")
    vertices = np.concatenate(all_vertices)
    normals = np.concatenate(all_normals)
    faces = np.concatenate(all_faces)
    source_faces = np.concatenate(all_source_faces)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(args.output, vertices=vertices, normals=normals, faces=faces, source_face_ids=source_faces)
    receipt = {
        "source_sha256": sha256(args.source),
        "source_bytes": args.source.stat().st_size,
        "classification": "current_rig_head_weight_roi_not_hair",
        "selection_method": "Blender glTF world transform + vertex groups containing head; face admitted at max weight >= 0.20",
        "canonical_axis": "right-handed Y-up; Blender world (x,z,-y)",
        "matched_head_groups": sorted(matched_groups),
        "roi_vertices": int(vertices.shape[0]), "roi_triangles": int(faces.shape[0]),
        "mesh_inventory": inventories,
        "timing_seconds": {"blender_import": import_seconds, "total": time.perf_counter() - started},
    }
    args.receipt.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
