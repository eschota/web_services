"""Downloads · V3: one rigged V3 GLB -> the FBX / GLB / .blend downloads, inside Blender (run by an export worker).

    blender -b --factory-startup -P v3_export_blender.py -- rigged.glb out_dir [spec.json | '{"clips": ["Idle"]}']

Writes into out_dir:
    out.<fbx|glb|blend>   armature (+ skinned meshes unless spec.mesh is false) with the chosen clips, every clip
                          its own take / action; spec.mixamo prefixes the bones «mixamorig:» (humanoids)
    export.json           clips, bones, meshes, the file with its size and seconds, warnings, the re-import check

Prints «PROGRESS <0..1> <stage>» lines; the worker relays them to the task page's progress bar.
The internal «rig_check» clip of the V3 conveyor is never exported.
Textures are embedded (path_mode COPY + embed_textures). Deform bones only; the «_End» leaves only point.
"""
import json
import os
import sys
import time

import bpy

INTERNAL_CLIPS = {"rig_check"}


def progress(value, stage):
    print(f"PROGRESS {max(0.0, min(1.0, value)):.3f} {stage}", flush=True)


def size(path):
    return os.path.getsize(path) if os.path.exists(path) else 0


def direct_base_color(materials):
    """FBX follows only an image wired straight into Base Color; the glTF importer may put a mix node between."""
    changed = 0
    for mat in materials:
        if not mat or not mat.use_nodes:
            continue
        nt = mat.node_tree
        bsdf = next((n for n in nt.nodes if n.type == "BSDF_PRINCIPLED"), None)
        if bsdf is None or not bsdf.inputs["Base Color"].is_linked:
            continue
        start = bsdf.inputs["Base Color"].links[0].from_node
        if start.type == "TEX_IMAGE":
            continue
        seen, queue, img = set(), [start], None
        while queue and img is None:
            node = queue.pop(0)
            if node.name in seen:
                continue
            seen.add(node.name)
            for sock in node.inputs:
                for link in sock.links:
                    if link.from_node.type == "TEX_IMAGE" and link.from_node.image is not None:
                        img = link.from_node
                    queue.append(link.from_node)
        if img is not None:
            nt.links.new(img.outputs["Color"], bsdf.inputs["Base Color"])
            changed += 1
    return changed


def clip_name(action, arm_name):
    name = action.name
    for suffix in (f"_{arm_name}", "_Armature"):
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def assign(arm, action):
    ad = arm.animation_data or arm.animation_data_create()
    ad.action = action
    slots = getattr(action, "slots", None)       # Blender 4.4+: slotted actions
    if slots is not None and len(slots) and hasattr(ad, "action_slot"):
        try:
            if ad.action_slot is None:
                ad.action_slot = slots[0]
        except (AttributeError, TypeError, RuntimeError):
            pass
    f0, f1 = (int(round(x)) for x in action.frame_range)
    bpy.context.scene.frame_start, bpy.context.scene.frame_end = f0, max(f1, f0 + 1)
    return f0, f1


def action_fcurves(action):
    """Every F-curve of an action: the legacy list, or the channel bags of a slotted action (Blender 4.4+)."""
    legacy = getattr(action, "fcurves", None)
    if legacy is not None:
        try:
            return list(legacy)
        except (AttributeError, RuntimeError, TypeError):
            pass
    curves = []
    for layer in getattr(action, "layers", []) or []:
        for strip in layer.strips:
            for bag in getattr(strip, "channelbags", []) or []:
                curves.extend(bag.fcurves)
    return curves


def rename_bones(arm, actions):
    """Prefix every bone with «mixamorig:». Blender fixes the vertex groups and only the assigned action, so the
    action is unassigned first and every clip's paths and groups are rewritten here, the same way for all."""
    if arm.animation_data:
        arm.animation_data.action = None
    mapping = {}
    for b in arm.data.bones:
        if not b.name.startswith("mixamorig:"):
            new = "mixamorig:" + b.name
            mapping[b.name] = new
            b.name = new
    for act in actions:
        for fc in action_fcurves(act):
            path = fc.data_path
            if path.startswith('pose.bones["'):
                old = path[len('pose.bones["'):].split('"]', 1)[0]
                if old in mapping:
                    fc.data_path = path.replace(f'pose.bones["{old}"]', f'pose.bones["{mapping[old]}"]', 1)
            group = getattr(fc, "group", None)
            if group is not None and group.name in mapping:
                group.name = mapping[group.name]
    return mapping


def export_fbx(path, *, all_actions):
    bpy.ops.export_scene.fbx(
        filepath=path, use_selection=True, object_types={"ARMATURE", "MESH"}, use_mesh_modifiers=False,
        add_leaf_bones=False, primary_bone_axis="Y", secondary_bone_axis="X", use_armature_deform_only=True,
        armature_nodetype="NULL", bake_anim=True, bake_anim_use_all_bones=True, bake_anim_use_nla_strips=False,
        bake_anim_use_all_actions=all_actions, bake_anim_force_startend_keying=True, bake_anim_step=1.0,
        bake_anim_simplify_factor=0.0, path_mode="COPY", embed_textures=True,
        axis_forward="-Z", axis_up="Y",
        apply_scale_options="FBX_SCALE_ALL", mesh_smooth_type="FACE")


def verify(path, fmt, takes, mesh):
    """Re-open what was written: one armature, the skinned meshes when the model was asked for, every take back.
    A failed check fails the export."""
    bpy.ops.wm.read_factory_settings(use_empty=True)
    if fmt == "fbx":
        bpy.ops.import_scene.fbx(filepath=path)
    elif fmt == "glb":
        bpy.ops.import_scene.gltf(filepath=path)
    else:
        bpy.ops.wm.open_mainfile(filepath=path)
    objs = bpy.context.scene.objects
    arms = [o for o in objs if o.type == "ARMATURE"]
    skinned = [o for o in objs if o.type == "MESH" and any(m.type == "ARMATURE" for m in o.modifiers)]
    doc = {"format": fmt, "armatures": len(arms), "skinned_meshes": len(skinned), "actions": len(bpy.data.actions),
           "bones": len(arms[0].data.bones) if arms else 0}
    doc["ok"] = bool(len(arms) == 1 and doc["bones"] > 1 and (bool(skinned) == bool(mesh))
                     and (takes == 0 or doc["actions"] >= takes))
    if not doc["ok"]:
        raise SystemExit("re-import check failed: " + json.dumps(doc))
    return doc


def main():
    argv = sys.argv[sys.argv.index("--") + 1:]
    src, out = os.path.abspath(argv[0]), os.path.abspath(argv[1])
    spec = {}
    if len(argv) > 2 and argv[2].strip():                # a spec file (the worker writes one) or inline JSON
        raw = argv[2].strip()
        spec = json.loads(open(raw, encoding="utf-8").read() if os.path.isfile(raw) else raw)
    # spec: {"clip": name} (one clip, older jobs) or {"clips": [names] | null (all), "mesh": bool, "mixamo": bool,
    #        "format": "fbx" | "glb" | "blend", "preset": "unity" | "unreal" | "blender" | "glb"}
    keep = spec.get("clips")
    if spec.get("clip"):
        keep = [spec["clip"]]
    mesh_wanted = bool(spec.get("mesh", True))
    mixamo = bool(spec.get("mixamo", False))
    fmt = str(spec.get("format") or "fbx")
    if fmt not in ("fbx", "glb", "blend"):
        raise SystemExit(f"unknown format {fmt!r}")
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    rep = {"schema": "autorig.v3-export/1", "source": os.path.basename(src), "files": {}, "clips": [],
           "warnings": [], "blender": bpy.app.version_string, "spec": spec}
    progress(0.02, "import")
    bpy.ops.wm.read_factory_settings(use_empty=True)
    scene = bpy.context.scene
    scene.render.fps = 30
    bpy.ops.import_scene.gltf(filepath=src)
    progress(0.25, "import")
    arms = [o for o in scene.objects if o.type == "ARMATURE"]
    if not arms:
        raise SystemExit("no armature in " + src)
    arm = arms[0]
    meshes = [o for o in scene.objects if o.type == "MESH" and any(m.type == "ARMATURE" for m in o.modifiers)]
    for o in [o for o in scene.objects if o.type == "MESH" and o not in meshes]:
        bpy.data.objects.remove(o, do_unlink=True)      # the importer's bone-display shapes
    for pb in arm.pose.bones:
        pb.custom_shape = None
    for b in arm.data.bones:
        b.use_deform = not b.name.endswith("_End")
    if arm.animation_data:
        for track in list(arm.animation_data.nla_tracks):
            arm.animation_data.nla_tracks.remove(track)
    for o in meshes:                                     # shape-key actions are not part of the downloads
        if o.data.shape_keys and o.data.shape_keys.animation_data:
            o.data.shape_keys.animation_data_clear()

    actions = []
    for act in list(bpy.data.actions):
        name = clip_name(act, arm.name)
        if name in INTERNAL_CLIPS:
            bpy.data.actions.remove(act)
            continue
        act.name = name
        act.use_fake_user = True
        actions.append(act)
    rep["clips"] = [a.name for a in actions]
    if keep is not None:
        missing = [c for c in keep if c not in {a.name for a in actions}]
        if missing:
            raise SystemExit(f"no clip(s) {missing!r} in {src}")
        wanted = set(keep)
        kept = [a for a in actions if a.name in wanted]
        for other in [a for a in actions if a.name not in wanted]:
            bpy.data.actions.remove(other)
        actions = kept
    rep["exported_clips"] = [a.name for a in actions]
    if not mesh_wanted:                                  # animation only: the skeleton and its clips
        for o in meshes:
            bpy.data.objects.remove(o, do_unlink=True)
        meshes = []
    if mixamo:                                           # Mixamo / Unreal retarget names; clips and groups follow
        rename_bones(arm, actions)
    rep["bones"] = len(arm.data.bones)
    rep["deform_bones"] = sum(b.use_deform for b in arm.data.bones)
    rep["meshes"] = len(meshes)
    rep["materials_rewired"] = direct_base_color({s.material for m in meshes for s in m.material_slots})
    for o in scene.objects:
        o.select_set(o in meshes or o is arm)
    bpy.context.view_layer.objects.active = arm

    t = time.time()
    progress(0.35, fmt)
    if actions:
        f0, f1 = assign(arm, actions[0])
        rep["frames"] = [f0, f1]
    elif arm.animation_data:
        arm.animation_data.action = None
    path = os.path.join(out, f"out.{fmt}")
    if fmt == "fbx":
        export_fbx(path, all_actions=len(actions) > 1)
    elif fmt == "glb":
        bpy.ops.export_scene.gltf(filepath=path, export_format="GLB", use_selection=True, export_skins=True,
                                  export_animations=bool(actions), export_animation_mode="ACTIONS",
                                  export_force_sampling=True)
    else:
        if arm.animation_data:
            arm.animation_data.action = actions[0] if actions else None
        bpy.ops.file.pack_all()
        bpy.ops.wm.save_as_mainfile(filepath=path, compress=True, copy=True)
    rep["files"][f"out.{fmt}"] = {"bytes": size(path), "seconds": round(time.time() - t, 2), "takes": len(actions)}
    progress(0.96, "verify")
    rep["verify"] = verify(path, fmt, len(actions), bool(meshes))
    rep["seconds"] = round(time.time() - t0, 2)
    with open(os.path.join(out, "export.json"), "w", encoding="utf-8") as fh:
        json.dump(rep, fh, indent=1)
    progress(1.0, "done")
    print("EXPORT", json.dumps(rep))


main()
