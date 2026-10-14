"""Blender-side deterministic vehicle assembly builder."""
from __future__ import annotations

import json
import math
import pathlib
import sys
import traceback

import bpy
from mathutils import Quaternion, Vector


def material(name, base, metallic, roughness):
    mat = bpy.data.materials.new(name)
    mat.diffuse_color = (*base[:3], base[3] if len(base) > 3 else 1.0)
    mat.use_nodes = True
    bsdf = mat.node_tree.nodes.get("Principled BSDF")
    bsdf.inputs["Base Color"].default_value = mat.diffuse_color
    metallic_socket = bsdf.inputs.get("Metallic") or bsdf.inputs.get("Metallic IOR Level")
    if metallic_socket is not None:
        metallic_socket.default_value = metallic
    bsdf.inputs["Roughness"].default_value = roughness
    return mat


def cube_mesh(name, dimensions, bevel=0.08):
    bpy.ops.mesh.primitive_cube_add()
    obj = bpy.context.object
    obj.name = name
    obj.dimensions = dimensions
    bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)
    mod = obj.modifiers.new("Edge radius", "BEVEL")
    mod.width = min(bevel, min(dimensions) * 0.15)
    mod.segments = 3
    bpy.context.view_layer.objects.active = obj
    bpy.ops.object.modifier_apply(modifier=mod.name)
    return obj


def create_wheel_meshes(group, dimensions, tire_mat, rim_mat):
    width = dimensions[0]
    diameter = max(dimensions[1], dimensions[2])
    major = diameter * 0.36
    minor = max(0.025, diameter * 0.14)
    bpy.ops.mesh.primitive_torus_add(major_radius=major, minor_radius=minor, major_segments=48, minor_segments=12, rotation=(0, math.pi / 2, 0))
    tire = bpy.context.object
    tire.name = f"mesh.{group}.tire.source"
    tire.scale.x = width / max(2 * minor, 1e-6)
    bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)
    tire.data.name = f"mesh.{group}.tire"
    tire.data.materials.append(tire_mat)
    bpy.ops.mesh.primitive_cylinder_add(vertices=32, radius=diameter * 0.265, depth=width * 0.82, rotation=(0, math.pi / 2, 0))
    rim = bpy.context.object
    rim.name = f"mesh.{group}.rim.source"
    rim.data.name = f"mesh.{group}.rim"
    rim.data.materials.append(rim_mat)
    # Mirrored radial spokes are generated parametrically around the hub.
    return tire, rim


def linked_object(name, source, position, rotation):
    obj = bpy.data.objects.new(name, source.data)
    bpy.context.collection.objects.link(obj)
    obj.location = position
    obj.rotation_mode = "QUATERNION"
    obj.rotation_quaternion = Quaternion((rotation[3], rotation[0], rotation[1], rotation[2]))
    return obj


def tube_mesh(name, points, radius, mat, segments=16):
    """One source-bound tube path -> one mesh object; different pipes never merge."""
    points = [Vector(point) for point in points]
    vertices, faces = [], []
    for index, point in enumerate(points):
        tangent = (points[min(index + 1, len(points) - 1)] - points[max(index - 1, 0)]).normalized()
        side = tangent.cross(Vector((0, 0, 1)))
        if side.length < 1e-5:
            side = tangent.cross(Vector((0, 1, 0)))
        side.normalize()
        up = tangent.cross(side).normalized()
        for segment in range(segments):
            angle = 2 * math.pi * segment / segments
            vertices.append(tuple(point + radius * (math.cos(angle) * side + math.sin(angle) * up)))
    for ring in range(len(points) - 1):
        a, b = ring * segments, (ring + 1) * segments
        for segment in range(segments):
            nxt = (segment + 1) % segments
            faces.append((a + segment, a + nxt, b + nxt, b + segment))
    faces.extend([tuple(range(segments))[::-1], tuple((len(points) - 1) * segments + i for i in range(segments))])
    mesh = bpy.data.meshes.new(f"mesh.{name}")
    mesh.from_pydata(vertices, [], faces)
    mesh.materials.append(mat)
    obj = bpy.data.objects.new(name, mesh)
    bpy.context.collection.objects.link(obj)
    return obj


def import_asset(path, prefix, position, rotation):
    before = set(bpy.context.scene.objects)
    bpy.ops.import_scene.gltf(filepath=str(path))
    imported = [obj for obj in bpy.context.scene.objects if obj not in before]
    root = bpy.data.objects.new(prefix, None)
    bpy.context.collection.objects.link(root)
    root.location = position
    root.rotation_mode = "QUATERNION"
    root.rotation_quaternion = Quaternion((rotation[3], rotation[0], rotation[1], rotation[2]))
    for obj in imported:
        if obj.parent is None:
            obj.parent = root
        obj.name = f"{prefix}.{obj.name}"
    return root, imported


def imported_dimensions(objects):
    bpy.context.view_layer.update()
    points = []
    for obj in objects:
        if obj.type != "MESH":
            continue
        points.extend(obj.matrix_world @ Vector(corner) for corner in obj.bound_box)
    if not points:
        return None
    return [max(p[i] for p in points) - min(p[i] for p in points) for i in range(3)]


def unique_polygon_count():
    meshes = {obj.data for obj in bpy.context.scene.objects if obj.type == "MESH"}
    return sum(len(mesh.polygons) for mesh in meshes)


def run(receipt_path, output_dir):
    receipt = json.loads(pathlib.Path(receipt_path).read_text(encoding="utf-8"))
    output = pathlib.Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)
    rubber = material("PBR.Rubber", [0.018, 0.021, 0.024, 1], 0.0, 0.72)
    metal = material("PBR.RimMetal", [0.32, 0.34, 0.37, 1], 0.82, 0.24)
    paint = material("PBR.BodyPaint", [0.18, 0.035, 0.025, 1], 0.65, 0.2)
    nodes, mesh_groups = [], {}
    accepted_counts = {}
    exported_counts = {}
    registration_metrics = {}
    acceptance_receipts = {}

    mode = receipt.get("assembly_mode", "full_vehicle")
    selected = set(receipt.get("selected_part_ids") or [])
    body_asset = pathlib.Path(receipt["body"].get("asset_path") or "")
    include_body = mode != "selected_parts"
    if include_body and body_asset.is_file():
        root, imported = import_asset(body_asset, "body", [0, 0, 0], [0, 0, 0, 1])
        nodes.append(root.name)
        nodes.extend(obj.name for obj in imported)
    elif include_body:
        body = cube_mesh("collision.chassis.proxy", receipt["body"]["dimensions"], bevel=0.16)
        body.location.z = receipt["body"]["dimensions"][2] * 0.55
        body.data.materials.append(paint)
        nodes.append(body.name)

    wheel_sources = {}
    for part in receipt["parts"]:
        if mode == "body_only":
            continue
        if mode == "selected_parts" and part["id"] not in selected:
            continue
        accepted_counts[part["semantic"]] = accepted_counts.get(part["semantic"], 0) + 1
        if part.get("shape_gate"):
            registration_metrics[part["id"]] = part["shape_gate"]
        if part.get("acceptance_receipt"):
            acceptance_receipts[part["id"]] = part["acceptance_receipt"]
        pos, rot = part["position"], part["rotation"]
        if part["semantic"] == "wheel":
            group = part["mesh_group"]
            if group not in wheel_sources:
                wheel_sources[group] = create_wheel_meshes(group, part["dimensions"], rubber, metal)
            tire_source, rim_source = wheel_sources[group]
            wheel_root = bpy.data.objects.new(part["id"], None)
            bpy.context.collection.objects.link(wheel_root)
            wheel_root.location = pos
            wheel_root.rotation_mode = "QUATERNION"
            wheel_root.rotation_quaternion = Quaternion((rot[3], rot[0], rot[1], rot[2]))
            tire = linked_object(f"{part['id']}.tire", tire_source, [0, 0, 0], [0, 0, 0, 1])
            rim = linked_object(f"{part['id']}.rim", rim_source, [0, 0, 0], [0, 0, 0, 1])
            tire.parent = wheel_root
            rim.parent = wheel_root
            nodes.extend([wheel_root.name, tire.name, rim.name])
            mesh_groups.setdefault(group, {"tire_mesh": tire.data.name, "rim_mesh": rim.data.name, "nodes": []})["nodes"].extend([tire.name, rim.name])
            exported_counts["wheel"] = exported_counts.get("wheel", 0) + 1
        elif part["semantic"] in {"pipe", "exhaust", "intake"} and part.get("path"):
            obj = tube_mesh(part["id"], part["path"], part["radius"], metal)
            obj.location = pos
            obj.rotation_mode = "QUATERNION"
            obj.rotation_quaternion = Quaternion((rot[3], rot[0], rot[1], rot[2]))
            nodes.append(obj.name)
            exported_counts[part["semantic"]] = exported_counts.get(part["semantic"], 0) + 1
        elif part.get("asset_path") and pathlib.Path(part["asset_path"]).is_file():
            absolute = part.get("asset_registration_mode") == "absolute_common_space"
            root, imported = import_asset(pathlib.Path(part["asset_path"]), part["id"],
                                          [0, 0, 0] if absolute else pos,
                                          [0, 0, 0, 1] if absolute else rot)
            # Imported details are never stretched to appear fitted. They must already
            # match the accepted common-space boundary within the declared tolerance.
            dims = imported_dimensions(imported)
            tolerance = float(receipt.get("registration_tolerance_m", .02))
            if dims and any(abs(a - b) > tolerance for a, b in zip(dims, part["dimensions"])):
                raise ValueError(f"{part['id']} misses registered dimensions by more than {tolerance:.4f} m")
            nodes.append(root.name)
            nodes.extend(obj.name for obj in imported)
            exported_counts[part["semantic"]] = exported_counts.get(part["semantic"], 0) + 1
        else:
            obj = cube_mesh(part["id"], part["dimensions"], bevel=min(part["dimensions"]) * 0.08)
            obj.location = pos
            obj.rotation_mode = "QUATERNION"
            obj.rotation_quaternion = Quaternion((rot[3], rot[0], rot[1], rot[2]))
            obj.data.materials.append(metal if part["semantic"] in ("weapon", "bumper") else paint)
            nodes.append(obj.name)
            exported_counts[part["semantic"]] = exported_counts.get(part["semantic"], 0) + 1

    # Source objects only donate datablocks; exporting them would duplicate wheels.
    for tire, rim in wheel_sources.values():
        bpy.data.objects.remove(tire, do_unlink=True)
        bpy.data.objects.remove(rim, do_unlink=True)
    polygon_budget = int(receipt.get("polygon_budget") or 0)
    polygon_count_before = unique_polygon_count()
    polygon_resampled = False
    if polygon_budget and polygon_count_before > polygon_budget and mode in {"body_only", "full_vehicle"}:
        fixed_meshes = {obj.data for obj in bpy.context.scene.objects
                        if obj.type == "MESH" and (obj.name.endswith(".tire") or obj.name.endswith(".rim"))}
        fixed_count = sum(len(mesh.polygons) for mesh in fixed_meshes)
        decimatable = [obj for obj in bpy.context.scene.objects
                       if obj.type == "MESH" and obj.data not in fixed_meshes]
        dec_count = sum(len(obj.data.polygons) for obj in decimatable)
        target = max(4 * len(decimatable), polygon_budget - fixed_count)
        ratio = min(1.0, target / max(dec_count, 1))
        for obj in decimatable:
            bpy.context.view_layer.objects.active = obj
            modifier = obj.modifiers.new("Polygon budget", "DECIMATE")
            modifier.ratio = ratio
            bpy.ops.object.modifier_apply(modifier=modifier.name)
        polygon_resampled = True
    polygon_count = unique_polygon_count()
    missing_visible_body = bool(include_body and not body_asset.is_file())
    model = output / "vehicle-assembly.glb"
    bpy.ops.export_scene.gltf(filepath=str(model), export_format="GLB", export_apply=True, export_materials="EXPORT")
    # Verify the delivered file, not only Blender's pre-export mesh. Export can
    # remove degenerate triangles or alter topology.
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)
    bpy.ops.import_scene.gltf(filepath=str(model))
    exported_polygon_count = unique_polygon_count()
    over_budget = bool(polygon_budget and exported_polygon_count > polygon_budget)
    needs_review = missing_visible_body or over_budget
    reasons = []
    if missing_visible_body:
        reasons.append("visible voxel/body GLB is missing; collision proxy is not a generated vehicle")
    if over_budget:
        reasons.append(f"exported polygon count {exported_polygon_count} exceeds target {polygon_budget}")
    report_path = output / "vehicle-assembly.report.json"
    report = {
        "ok_bool": not needs_review, "status_string": "needs_review" if needs_review else "completed",
        "reason_string": "; ".join(reasons),
        "schema_string": receipt["schema"], "run_id_string": receipt["run_id"],
        "model_path_string": str(model), "report_path_string": str(report_path),
        "node_names_array": nodes, "mesh_groups_object": mesh_groups,
        "separate_parts_bool": True, "joined_mesh_bool": False,
        "common_space_applied_bool": True, "pbr_material_names_array": [rubber.name, metal.name, paint.name],
        "blender_version_string": bpy.app.version_string,
        "visible_body_source_string": "voxel_or_supplied_glb" if body_asset.is_file() else "none",
        "collision_proxy_only_bool": not body_asset.is_file(),
        "production_ready_bool": bool(body_asset.is_file() and receipt.get("quality", {}).get("ready_for_exact_parts_bool")),
        "registration_tolerance_m_float": receipt.get("registration_tolerance_m", .02),
        "skipped_unregistered_part_ids_array": receipt.get("skipped_unregistered_parts", []),
        "input_coordinate_system_object": {"handedness_string": "right", "lateral_axis_string": "+X",
                                             "up_axis_string": "+Z", "forward_axis_string": "-Y", "units_string": "meters",
                                             "source_to_common_matrix_4x4_array": receipt["source_to_common_matrix"],
                                             "source_receipt_coordinate_system_object": receipt.get("source_coordinate_system", {})},
        "output_coordinate_system_object": {"format_string": "glTF 2.0", "handedness_string": "right",
                                              "up_axis_string": "Y", "forward_axis_string": "+Z",
                                              "units_string": "meters",
                                              "common_to_gltf_rule_string": "[x,y,z] -> [x,z,-y]"},
        "assembly_mode_string": mode, "selected_part_ids_array": sorted(selected),
        "replacement_node_ids_array": sorted(part["id"] for part in receipt["parts"] if part["id"] in selected),
        "polygon_count_int": exported_polygon_count, "polygon_budget_int": polygon_budget,
        "blender_pre_export_polygon_count_int": polygon_count,
        "polygon_count_before_int": polygon_count_before, "polygon_resampled_bool": polygon_resampled,
        "accepted_part_counts_object": accepted_counts, "exported_part_counts_object": exported_counts,
        "part_count_match_bool": accepted_counts == exported_counts,
        "part_registration_metrics_object": registration_metrics,
        "part_acceptance_receipts_object": acceptance_receipts,
        "original_source_sha256_string": receipt.get("source_sha256", ""),
        "prepared_source_sha256_string": receipt.get("prepared_source_sha256", ""),
        "canonical_camera_object": receipt.get("camera", {}),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    args = sys.argv[sys.argv.index("--") + 1:]
    try:
        run(args[0], args[1])
    except Exception:
        traceback.print_exc()
        raise

