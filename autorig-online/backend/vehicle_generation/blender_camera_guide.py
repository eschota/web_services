"""Render an exact orthographic 45-degree yaw/elevation vehicle camera guide."""
from __future__ import annotations

import json
import math
import pathlib
import sys

import bpy
from mathutils import Vector


def mat(name, color, metallic=0.0, roughness=.55):
    value = bpy.data.materials.new(name)
    value.diffuse_color = (*color, 1)
    value.use_nodes = True
    bsdf = value.node_tree.nodes.get("Principled BSDF")
    bsdf.inputs["Base Color"].default_value = (*color, 1)
    bsdf.inputs["Metallic"].default_value = metallic
    bsdf.inputs["Roughness"].default_value = roughness
    return value


def cube(name, location, scale, material, bevel=.08):
    bpy.ops.mesh.primitive_cube_add(location=location)
    obj = bpy.context.object
    obj.name = name
    obj.scale = [value / 2 for value in scale]
    bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)
    modifier = obj.modifiers.new("guide bevel", "BEVEL")
    modifier.width, modifier.segments = bevel, 3
    bpy.context.view_layer.objects.active = obj
    bpy.ops.object.modifier_apply(modifier=modifier.name)
    obj.data.materials.append(material)
    return obj


def camera_45(target, distance, ortho_scale):
    yaw, elevation = math.radians(45), math.radians(45)
    offset = Vector((math.cos(elevation) * math.cos(yaw),
                     math.cos(elevation) * math.sin(yaw), math.sin(elevation))) * distance
    data = bpy.data.cameras.new("Camera.Ortho45")
    data.type = "ORTHO"
    data.ortho_scale = ortho_scale
    camera = bpy.data.objects.new("Camera.Ortho45", data)
    bpy.context.collection.objects.link(camera)
    camera.location = Vector(target) + offset
    camera.rotation_euler = (Vector(target) - camera.location).to_track_quat("-Z", "Y").to_euler()
    bpy.context.scene.camera = camera
    return camera


def render(out_dir, length, width, height):
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    bpy.ops.object.select_all(action="SELECT")
    bpy.ops.object.delete(use_global=False)
    body_mat = mat("Guide.Body", (.28, .32, .38), .25, .42)
    tire_mat = mat("Guide.Tire", (.015, .018, .022), 0, .78)
    rim_mat = mat("Guide.Rim", (.45, .47, .5), .8, .22)
    cube("guide.body.lower", (0, 0, height * .42), (width, length, height * .48), body_mat, .14)
    cube("guide.body.cabin", (0, .25, height * .75), (width * .72, length * .48, height * .48), body_mat, .12)
    wheel_diameter, wheel_width = min(height * .34, length * .18), width * .11
    axle_y = (-length * .31, length * .31)
    wheel_nodes = []
    for axle, y in enumerate(axle_y):
        for side, x in (("left", -width * .5), ("right", width * .5)):
            bpy.ops.mesh.primitive_torus_add(major_radius=wheel_diameter * .36, minor_radius=wheel_diameter * .14,
                                             major_segments=40, minor_segments=12,
                                             location=(x, y, wheel_diameter * .5), rotation=(0, math.pi / 2, 0))
            wheel = bpy.context.object
            wheel.name = f"guide.wheel.axle_{axle}.{side}"
            wheel.scale.x = wheel_width / max(wheel_diameter * .28, 1e-6)
            bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)
            wheel.data.materials.append(tire_mat)
            wheel_nodes.append(wheel.name)
            bpy.ops.mesh.primitive_cylinder_add(vertices=32, radius=wheel_diameter * .24, depth=wheel_width * .82,
                                                location=(x, y, wheel_diameter * .5), rotation=(0, math.pi / 2, 0))
            bpy.context.object.name = f"guide.rim.axle_{axle}.{side}"
            bpy.context.object.data.materials.append(rim_mat)
    ground = cube("guide.ground", (0, 0, -.035), (12, 12, .06), mat("Guide.Ground", (.34, .34, .34), 0, .88), .01)
    target = (0, 0, height * .48)
    camera = camera_45(target, max(length, width, height) * 2.4, max(length, width * 1.6, height * 1.6) * 1.25)
    light_data = bpy.data.lights.new("Guide.Key", "AREA")
    light_data.energy, light_data.shape, light_data.size = 1100, "DISK", 5
    light = bpy.data.objects.new("Guide.Key", light_data)
    bpy.context.collection.objects.link(light)
    light.location = (4, -5, 8)
    scene = bpy.context.scene
    scene.render.engine = "BLENDER_EEVEE"
    scene.render.resolution_x = scene.render.resolution_y = 768
    scene.render.resolution_percentage = 100
    scene.render.image_settings.file_format = "PNG"
    scene.world.color = (.18, .18, .18)
    scene.render.filepath = str(out / "vehicle-ortho45-guide.png")
    bpy.ops.render.render(write_still=True)
    receipt = {
        "schema_string": "autorig.vehicle-camera-guide/1", "diagnostic_guide_bool": True,
        "customer_result_bool": False, "projection_string": "orthographic",
        "yaw_deg_float": 45.0, "pitch_deg_float": 45.0, "target_xyz_m_array": list(target),
        "camera_location_xyz_m_array": list(camera.location), "camera_rotation_euler_rad_array": list(camera.rotation_euler),
        "camera_matrix_world_4x4_array": [list(row) for row in camera.matrix_world],
        "orthographic_scale_m_float": camera.data.ortho_scale, "resolution_px_array": [768, 768],
        "guide_dimensions_m_object": {"length_float": length, "width_float": width, "height_float": height,
                                      "provenance_string": "vision_inferred_not_measured"},
        "coordinate_system_object": {"handedness_string": "right", "up_axis_string": "+Z",
                                     "forward_axis_string": "-Y", "units_string": "meters"},
        "wheel_object_ids_array": wheel_nodes,
    }
    (out / "vehicle-ortho45-camera.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")


if __name__ == "__main__":
    args = sys.argv[sys.argv.index("--") + 1:]
    render(args[0], float(args[1]), float(args[2]), float(args[3]))
