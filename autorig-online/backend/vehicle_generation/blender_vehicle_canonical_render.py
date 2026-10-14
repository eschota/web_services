"""Render an actual body/wheel proxy at exact ortho 45/45 with geometry passes."""
from __future__ import annotations

import hashlib
import json
import math
import pathlib
import sys

import bpy
from mathutils import Vector


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def camera_45(target, distance, scale):
    yaw = elevation = math.radians(45)
    offset = Vector((math.cos(elevation) * math.cos(yaw),
                     math.cos(elevation) * math.sin(yaw), math.sin(elevation))) * distance
    data = bpy.data.cameras.new("Canonical.Ortho45")
    data.type, data.ortho_scale = "ORTHO", scale
    camera = bpy.data.objects.new("Canonical.Ortho45", data)
    bpy.context.collection.objects.link(camera)
    camera.location = Vector(target) + offset
    camera.rotation_euler = (Vector(target) - camera.location).to_track_quat("-Z", "Y").to_euler()
    bpy.context.scene.camera = camera
    return camera


def wheel_mesh(name, center_gltf, radius, width, material):
    x, y, z = center_gltf
    center_blender = (x, -z, y)
    bpy.ops.mesh.primitive_torus_add(major_radius=radius * .72, minor_radius=radius * .28,
                                     major_segments=48, minor_segments=12,
                                     location=center_blender, rotation=(0, math.pi / 2, 0))
    wheel = bpy.context.object
    wheel.name = name
    wheel.scale.x = width / max(radius * .56, 1e-6)
    bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)
    wheel.data.materials.append(material)
    return wheel


def emission_material(name, color):
    material = bpy.data.materials.new(name)
    material.use_nodes = True
    nodes, links = material.node_tree.nodes, material.node_tree.links
    nodes.clear()
    out = nodes.new("ShaderNodeOutputMaterial")
    emission = nodes.new("ShaderNodeEmission")
    emission.inputs["Color"].default_value = (*color, 1)
    emission.inputs["Strength"].default_value = 1
    links.new(emission.outputs["Emission"], out.inputs["Surface"])
    return material


def worldspace_material(bounds_min, bounds_max):
    material = bpy.data.materials.new("Pass.WorldSpace")
    material.use_nodes = True
    nodes, links = material.node_tree.nodes, material.node_tree.links
    nodes.clear()
    out = nodes.new("ShaderNodeOutputMaterial")
    geometry = nodes.new("ShaderNodeNewGeometry")
    separate = nodes.new("ShaderNodeSeparateXYZ")
    combine = nodes.new("ShaderNodeCombineColor")
    emission = nodes.new("ShaderNodeEmission")
    links.new(geometry.outputs["Position"], separate.inputs["Vector"])
    for index, channel in enumerate("XYZ"):
        mapper = nodes.new("ShaderNodeMapRange")
        mapper.inputs["From Min"].default_value = bounds_min[index]
        mapper.inputs["From Max"].default_value = bounds_max[index]
        mapper.inputs["To Min"].default_value = 0
        mapper.inputs["To Max"].default_value = 1
        mapper.clamp = True
        links.new(separate.outputs[channel], mapper.inputs["Value"])
        links.new(mapper.outputs["Result"], combine.inputs[index])
    links.new(combine.outputs["Color"], emission.inputs["Color"])
    links.new(emission.outputs["Emission"], out.inputs["Surface"])
    return material


def depth_material(far_distance):
    material = bpy.data.materials.new("Pass.CameraDepth")
    material.use_nodes = True
    nodes, links = material.node_tree.nodes, material.node_tree.links
    nodes.clear()
    out = nodes.new("ShaderNodeOutputMaterial")
    geometry = nodes.new("ShaderNodeNewGeometry")
    transform = nodes.new("ShaderNodeVectorTransform")
    transform.vector_type, transform.convert_from, transform.convert_to = "POINT", "WORLD", "CAMERA"
    separate = nodes.new("ShaderNodeSeparateXYZ")
    mapper = nodes.new("ShaderNodeMapRange")
    mapper.inputs["From Min"].default_value = -far_distance
    mapper.inputs["From Max"].default_value = 0
    mapper.inputs["To Min"].default_value = 0
    mapper.inputs["To Max"].default_value = 1
    mapper.clamp = True
    combine = nodes.new("ShaderNodeCombineColor")
    emission = nodes.new("ShaderNodeEmission")
    links.new(geometry.outputs["Position"], transform.inputs["Vector"])
    links.new(transform.outputs["Vector"], separate.inputs["Vector"])
    links.new(separate.outputs["Z"], mapper.inputs["Value"])
    for channel in ("Red", "Green", "Blue"):
        links.new(mapper.outputs["Result"], combine.inputs[channel])
    links.new(combine.outputs["Color"], emission.inputs["Color"])
    links.new(emission.outputs["Emission"], out.inputs["Surface"])
    return material


def render(body_path, wheel_rig_path, out_dir, original_sha, prepared_sha):
    body_path, wheel_rig_path, out = pathlib.Path(body_path), pathlib.Path(wheel_rig_path), pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rig = json.loads(wheel_rig_path.read_text(encoding="utf-8"))
    if rig.get("coordinate_space_string") != "gltf_right_handed_y_up_forward_plus_z":
        raise ValueError("wheel rig must use locked glTF common space")
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)
    bpy.ops.import_scene.gltf(filepath=str(body_path))
    body_objects = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
    if not body_objects:
        raise ValueError("body GLB contains no mesh")
    tire = emission_material("Proxy.Tire", (.025, .028, .032))
    wheel_objects = []
    for wheel in rig.get("wheels_array") or []:
        wheel_objects.append(wheel_mesh(str(wheel["part_id_string"]), wheel["center_xyz_array"],
                                        float(wheel["radius_m_float"]), float(wheel["width_m_float"]), tire))
    if len(wheel_objects) != int((rig.get("qa_object") or {}).get("expected_wheel_count_int") or 0):
        raise ValueError("wheel proxy count does not match receipt")
    scene = bpy.context.scene
    scene.render.engine = "BLENDER_EEVEE"
    scene.render.resolution_x = scene.render.resolution_y = 768
    scene.render.resolution_percentage = 100
    scene.render.image_settings.file_format = "PNG"
    scene.render.image_settings.color_mode = "RGBA"
    scene.view_settings.look = "AgX - Medium High Contrast"
    scene.world.color = (.28, .28, .28)
    points = [obj.matrix_world @ Vector(corner) for obj in body_objects + wheel_objects for corner in obj.bound_box]
    bounds_min = [min(point[index] for point in points) for index in range(3)]
    bounds_max = [max(point[index] for point in points) for index in range(3)]
    target = [(bounds_min[i] + bounds_max[i]) * .5 for i in range(3)]
    extents = [bounds_max[i] - bounds_min[i] for i in range(3)]
    camera = camera_45(target, max(extents) * 2.5, max(extents) * 1.35)
    light_data = bpy.data.lights.new("Canonical.Key", "AREA")
    light_data.energy, light_data.size = 1200, 5
    light = bpy.data.objects.new("Canonical.Key", light_data)
    bpy.context.collection.objects.link(light)
    light.location = (4, -5, 8)

    # Diffuse, then normalized camera depth using the exact same camera.
    diffuse = out / "renderer-derived-ortho45-diffuse.png"
    scene.render.filepath = str(diffuse)
    bpy.ops.render.render(write_still=True)
    depth = out / "renderer-derived-ortho45-depth.png"
    scene.view_layers[0].material_override = depth_material(max(extents) * 5)
    scene.render.image_settings.color_mode = "BW"
    scene.render.image_settings.color_depth = "16"
    scene.render.filepath = str(depth)
    bpy.ops.render.render(write_still=True)

    # World-position RGB, normalized only for storage; bounds make it reversible.
    scene.render.image_settings.color_mode = "RGBA"
    scene.render.image_settings.color_depth = "8"
    scene.view_layers[0].material_override = worldspace_material(bounds_min, bounds_max)
    world = out / "renderer-derived-ortho45-worldspace.png"
    scene.render.filepath = str(world)
    bpy.ops.render.render(write_still=True)

    # One stable colour per object. Preserve the mapping in the receipt.
    object_ids = {}
    for index, obj in enumerate(body_objects + wheel_objects, 1):
        color = (((index * 73) % 251) / 255, ((index * 151) % 251) / 255, ((index * 199) % 251) / 255)
        obj.data.materials.clear()
        obj.data.materials.append(emission_material(f"Pass.Object.{index}", color))
        object_ids[obj.name] = {"id_int": index, "rgb_array": list(color)}
    scene.view_layers[0].material_override = None
    object_id = out / "renderer-derived-ortho45-object-id.png"
    scene.render.filepath = str(object_id)
    bpy.ops.render.render(write_still=True)

    receipt = {"schema_string": "autorig.vehicle-renderer-derived-canonical/1",
        "renderer_derived_bool": True, "customer_result_bool": False,
        "source_chain_object": {"original_source_sha256_string": original_sha,
                                "prepared_source_sha256_string": prepared_sha,
                                "body_glb_sha256_string": sha(body_path), "wheel_rig_sha256_string": sha(wheel_rig_path)},
        "camera_object": {"projection_string": "orthographic", "yaw_deg_float": 45.0, "pitch_deg_float": 45.0,
            "target_xyz_m_array": target, "location_xyz_m_array": list(camera.location),
            "rotation_euler_rad_array": list(camera.rotation_euler),
            "matrix_world_4x4_array": [list(row) for row in camera.matrix_world],
            "orthographic_scale_m_float": camera.data.ortho_scale, "resolution_px_array": [768, 768]},
        "passes_object": {"diffuse_path_string": diffuse.name, "depth_path_string": depth.name,
            "worldspace_path_string": world.name, "object_id_path_string": object_id.name,
            "worldspace_bounds_min_xyz_array": bounds_min, "worldspace_bounds_max_xyz_array": bounds_max,
            "object_ids_object": object_ids},
        "wheel_object_ids_array": [obj.name for obj in wheel_objects]}
    (out / "renderer-derived-ortho45-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")


if __name__ == "__main__":
    args = sys.argv[sys.argv.index("--") + 1:]
    render(*args)
