#!/usr/bin/env python3
"""Extract conservative head ROI and six registered projections from a real mesh.

Runs in the system Python that has trimesh and OpenCV. Mesh/name classification
is recorded explicitly and is never promoted to authoritative hair evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import cv2
import numpy as np
import trimesh


HAIR_WORDS = ("hair", "cheveux", "coiff", "bang", "fringe", "ponytail", "braid")
HEAD_WORDS = ("head", "face", "skull", "cranium")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scene_meshes(source: Path):
    loaded = trimesh.load(source, force="scene", process=False)
    scene = loaded if isinstance(loaded, trimesh.Scene) else trimesh.Scene(loaded)
    for node_name in scene.graph.nodes_geometry:
        transform, geometry_name = scene.graph[node_name]
        mesh = scene.geometry[geometry_name].copy()
        mesh.apply_transform(transform)
        material_name = ""
        visual = getattr(mesh, "visual", None)
        material = getattr(visual, "material", None)
        if material is not None:
            material_name = str(getattr(material, "name", "") or "")
        identity = f"{node_name} {geometry_name} {material_name}".lower()
        yield str(node_name), str(geometry_name), material_name, identity, mesh


def concatenate_tagged(source: Path):
    records = list(scene_meshes(source))
    if not records:
        raise RuntimeError("source contains no triangle mesh")
    vertices = []
    faces = []
    face_mesh = []
    vertex_normals = []
    offset = 0
    metadata = []
    for mesh_id, (node, geom, material, identity, mesh) in enumerate(records):
        tri = np.asarray(mesh.triangles, dtype=np.float32)
        if tri.size == 0:
            continue
        verts = np.asarray(mesh.vertices, dtype=np.float32)
        mesh_faces = np.asarray(mesh.faces, dtype=np.int64)
        normals = np.asarray(mesh.vertex_normals, dtype=np.float32)
        vertices.append(verts)
        faces.append(mesh_faces + offset)
        vertex_normals.append(normals)
        face_mesh.append(np.full(mesh_faces.shape[0], mesh_id, dtype=np.int32))
        metadata.append({
            "mesh_id": mesh_id,
            "node": node,
            "geometry": geom,
            "material": material,
            "hair_name_hint": any(word in identity for word in HAIR_WORDS),
            "head_name_hint": any(word in identity for word in HEAD_WORDS),
            "vertices": int(verts.shape[0]),
            "triangles": int(mesh_faces.shape[0]),
        })
        offset += verts.shape[0]
    return (
        np.concatenate(vertices),
        np.concatenate(vertex_normals),
        np.concatenate(faces),
        np.concatenate(face_mesh),
        metadata,
    )


def canonicalize_axes(vertices, normals):
    extents = vertices.max(axis=0) - vertices.min(axis=0)
    vertical = int(np.argmax(extents))
    remaining = [axis for axis in range(3) if axis != vertical]
    lateral = remaining[int(np.argmax(extents[remaining]))]
    depth = next(axis for axis in range(3) if axis not in (lateral, vertical))
    transform3 = np.zeros((3, 3), dtype=np.float32)
    transform3[0, lateral] = 1.0
    transform3[1, vertical] = 1.0
    transform3[2, depth] = 1.0
    if np.linalg.det(transform3) < 0:
        transform3[2, depth] = -1.0
    return vertices @ transform3.T, normals @ transform3.T, transform3, {
        "method": "largest_extent_vertical_second_largest_lateral",
        "source_extents": extents.tolist(),
        "source_lateral_axis": lateral,
        "source_vertical_axis": vertical,
        "source_depth_axis": depth,
    }


def select_roi(vertices, normals, faces, face_mesh, metadata):
    named_head = {m["mesh_id"] for m in metadata if m["head_name_hint"]}
    named_hair = {m["mesh_id"] for m in metadata if m["hair_name_hint"]}
    if named_head:
        selected = np.isin(face_mesh, list(named_head | named_hair))
        method = "node_material_name_head_hint"
    else:
        bounds_min = vertices.min(axis=0)
        bounds_max = vertices.max(axis=0)
        height = max(float(bounds_max[1] - bounds_min[1]), 1e-8)
        cutoff = bounds_max[1] - 0.28 * height
        centroids = vertices[faces].mean(axis=1)
        center_x = 0.5 * (bounds_min[0] + bounds_max[0])
        width = max(float(bounds_max[0] - bounds_min[0]), 1e-8)
        selected = (centroids[:, 1] >= cutoff) & (np.abs(centroids[:, 0] - center_x) <= 0.32 * width)
        method = "geometric_top_head_roi_y_up"
    if not selected.any():
        raise RuntimeError("head ROI selection is empty")
    selected_faces = faces[selected]
    used, inverse = np.unique(selected_faces.reshape(-1), return_inverse=True)
    roi_vertices = vertices[used]
    roi_faces = inverse.reshape(-1, 3).astype(np.uint32)
    roi_face_source = np.flatnonzero(selected).astype(np.uint32)
    hair_selected = np.isin(face_mesh[selected], list(named_hair)) if named_hair else np.zeros(selected_faces.shape[0], dtype=bool)
    return roi_vertices, normals[used], roi_faces, roi_face_source, hair_selected, method, named_hair


def rasterize(vertices, faces, source_ids, resolution, output_dir):
    center = 0.5 * (vertices.min(axis=0) + vertices.max(axis=0))
    extent = float(np.max(vertices.max(axis=0) - vertices.min(axis=0))) * 1.08
    extent = max(extent, 1e-6)
    views = {
        "front": (0, 1, 2, 1.0), "back": (0, 1, 2, -1.0),
        "left": (2, 1, 0, 1.0), "right": (2, 1, 0, -1.0),
        "top": (0, 2, 1, 1.0), "bottom": (0, 2, 1, -1.0),
    }
    masks = []
    tri_maps = []
    preview_tiles = []
    for name, (u_axis, v_axis, depth_axis, depth_sign) in views.items():
        uv = (vertices[:, [u_axis, v_axis]] - center[[u_axis, v_axis]]) / extent + 0.5
        px = np.empty_like(uv)
        px[:, 0] = uv[:, 0] * (resolution - 1)
        px[:, 1] = (1.0 - uv[:, 1]) * (resolution - 1)
        depth = (vertices[:, depth_axis] - center[depth_axis]) * depth_sign
        face_depth = depth[faces].mean(axis=1)
        order = np.argsort(face_depth)  # far to near painter order
        mask = np.zeros((resolution, resolution), dtype=np.uint8)
        tri_map = np.zeros((resolution, resolution), dtype=np.int32)
        for face_index in order:
            polygon = np.rint(px[faces[face_index]]).astype(np.int32)
            cv2.fillConvexPoly(mask, polygon, 255, lineType=cv2.LINE_8)
            cv2.fillConvexPoly(tri_map, polygon, int(source_ids[face_index]) + 1, lineType=cv2.LINE_8)
        cv2.imwrite(str(output_dir / f"{name}_mask.png"), mask)
        np.save(output_dir / f"{name}_tri.npy", tri_map)
        masks.append(mask > 0)
        tri_maps.append(tri_map)
        tile = cv2.cvtColor(mask, cv2.COLOR_GRAY2BGR)
        cv2.putText(tile, name, (8, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 180, 255), 2, cv2.LINE_AA)
        preview_tiles.append(tile)
    sheet = cv2.vconcat((cv2.hconcat(preview_tiles[:3]), cv2.hconcat(preview_tiles[3:])))
    cv2.imwrite(str(output_dir / "six_view_head_roi.png"), sheet)
    return np.stack(masks), np.stack(tri_maps), center, extent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--resolution", type=int, default=256)
    parser.add_argument("--preselected-npz", type=Path)
    parser.add_argument("--preselection-receipt", type=Path)
    parser.add_argument("--skip-head-payload", action="store_true")
    args = parser.parse_args()
    started = time.perf_counter()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    source_hash = sha256(args.source)
    load_start = time.perf_counter()
    if args.preselected_npz:
        selected = np.load(args.preselected_npz)
        roi_vertices = selected["vertices"].astype(np.float32)
        roi_normals = selected["normals"].astype(np.float32)
        roi_faces = selected["faces"].astype(np.uint32)
        source_ids = selected["source_face_ids"].astype(np.uint32)
        pre = json.loads(args.preselection_receipt.read_text(encoding="utf-8")) if args.preselection_receipt else {}
        metadata = pre.get("mesh_inventory", [])
        method = pre.get("selection_method", "preselected_roi")
        named_hair = set()
        hair_selected = np.zeros(roi_faces.shape[0], dtype=bool)
        axis_transform3 = np.eye(3, dtype=np.float32)
        axis_receipt = {"method": "already canonical", "source": pre.get("canonical_axis")}
        load_seconds = time.perf_counter() - load_start
        extract_seconds = 0.0
        classification = pre.get("classification", "preselected_head_roi_not_hair")
        source_vertices_count = sum(int(item.get("vertices", 0)) for item in metadata)
        source_triangles_count = sum(int(item.get("triangles", 0)) for item in metadata)
    else:
        vertices, normals, faces, face_mesh, metadata = concatenate_tagged(args.source)
        vertices, normals, axis_transform3, axis_receipt = canonicalize_axes(vertices, normals)
        load_seconds = time.perf_counter() - load_start
        extract_start = time.perf_counter()
        roi_vertices, roi_normals, roi_faces, source_ids, hair_selected, method, named_hair = select_roi(vertices, normals, faces, face_mesh, metadata)
        extract_seconds = time.perf_counter() - extract_start
        classification = "name_hint_only" if named_hair else "head_roi_only_not_hair"
        source_vertices_count = int(vertices.shape[0])
        source_triangles_count = int(faces.shape[0])
    project_start = time.perf_counter()
    masks, tri_maps, center, extent = rasterize(roi_vertices, roi_faces, source_ids, args.resolution, args.output_dir)
    project_seconds = time.perf_counter() - project_start
    np.savez_compressed(
        args.output_dir / "head_roi.npz",
        vertices=roi_vertices.astype(np.float32), normals=roi_normals.astype(np.float32), faces=roi_faces, source_face_ids=source_ids,
        masks=masks, hair_face_hint=hair_selected, center=center.astype(np.float32), extent=np.float32(extent),
    )
    payload = {
        "caseId": args.output_dir.parent.name,
        "sourceHash": source_hash,
        "positions": roi_vertices.astype(np.float32).reshape(-1).tolist(),
        "normals": roi_normals.astype(np.float32).reshape(-1).tolist(),
        "indices": roi_faces.astype(np.uint32).reshape(-1).tolist(),
        "rootTransform": [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0],
        "bounds": {"min": roi_vertices.min(axis=0).tolist(), "max": roi_vertices.max(axis=0).tolist()},
        "axis": "right-handed Y-up after recorded source-to-canonical transform",
        "sourceToCanonical3x3RowMajor": axis_transform3.reshape(-1).tolist(),
        "units": "source units; meters not proven",
        "selection": {"method": method, "classification": classification},
        "originalHair": {
            "present": None,
            "separateMesh": None,
            "meshNames": [m.get("geometry", m.get("object", "")) for m in metadata if m.get("hair_name_hint", False)],
            "evidence": "unavailable" if not named_hair else "node/material name hint only; not authoritative",
        },
    }
    if not args.skip_head_payload:
        (args.output_dir / "head_payload.json").write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    receipt = {
        "source_sha256": source_hash,
        "source_bytes": args.source.stat().st_size,
        "axis_units": "right-handed Y-up canonical coordinates; source units",
        "axis_normalization": axis_receipt,
        "source_to_canonical_3x3_row_major": axis_transform3.reshape(-1).tolist(),
        "selection_method": method,
        "classification": classification,
        "named_hair_mesh_ids": sorted(named_hair),
        "source_vertices": source_vertices_count, "source_triangles": source_triangles_count,
        "roi_vertices": int(roi_vertices.shape[0]), "roi_triangles": int(roi_faces.shape[0]),
        "hair_hint_triangles": int(hair_selected.sum()), "resolution": args.resolution,
        "mesh_inventory": metadata,
        "timing_seconds": {"load": load_seconds, "extract": extract_seconds, "project": project_seconds, "total": time.perf_counter() - started},
        "triangle_id_identity": (
            "diagnostic Blender-imported ROI-local triangle IDs; mapping to original GLB primitives is not established; "
            "zero is background; painter depth uses mean triangle depth and is not an authoritative z-buffer"
            if args.preselected_npz else
            "diagnostic trimesh-imported triangle IDs; mapping to original GLB primitives is not established; "
            "zero is background; painter depth uses mean triangle depth and is not an authoritative z-buffer"
        ),
    }
    (args.output_dir / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
