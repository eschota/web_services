"""Verify optimized skin against its exact full-weight clip baseline in Blender.

This checks compression, contact and export fidelity, not anatomical ownership.
The single-mesh authored asset and every manifest input must match their hashes.
No final Blend/GLB/FBX is saved unless all quarter-frame checks pass.
"""
import argparse
import bpy
import hashlib
import json
import numpy as np
from pathlib import Path
import re
import sys

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source',type=Path,required=True,help='Authored Blend beside export-report.json')
parser.add_argument('--reduction',type=Path,required=True,help='animal-skin-weights-report.json')
parser.add_argument('--output',type=Path,required=True,help='Fresh directory for scratch and verified exports')
parser.add_argument('--preflight-only',action='store_true')
args=parser.parse_args(sys.argv[sys.argv.index('--')+1:])
source=args.source.resolve()
out=args.output.resolve()
if out.exists():raise ValueError('Output must be a fresh directory')
reduction=json.loads(args.reduction.resolve().read_text())
candidate_path=Path(reduction['candidate']['path']).resolve()
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(candidate_path) == reduction['candidate']['sha256']
full_path = Path(reduction['inputs']['weights']['path']).resolve()
assert sha(full_path) == reduction['input_hashes']['weights']
rig_path = Path(reduction['inputs']['rig']['path'])
assert sha(rig_path) == reduction['input_hashes']['rig']
blueprint = json.loads(rig_path.read_text())
clips = {}
for row in reduction['inputs']['clips']:
    path = Path(row['path'])
    assert sha(path) == row['sha256']
    clip = json.loads(path.read_text())
    name = clip['action']
    if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',name):
        raise ValueError('Clip action must be a safe gameplay identifier')
    if name in clips:raise ValueError('Duplicate action in reduction manifest: '+name)
    clips[name] = clip
assert clips, 'Reduction manifest must contain clips'
source_report = json.loads((source.parent / 'export-report.json').read_text())
assert sha(source) == source_report['files'][source.name]['sha256']
bpy.ops.wm.open_mainfile(filepath=str(source))
arm = bpy.data.objects[blueprint['armature']]
assert len(blueprint['meshes']) == 1, 'Expected a single-mesh authored asset'
obj = bpy.data.objects[blueprint['meshes'][0]['name']]
scene = bpy.context.scene
arm.data.pose_position = 'POSE'
full = np.load(full_path, allow_pickle=False)
candidate = np.load(candidate_path, allow_pickle=False)
metadata = json.loads(str(candidate['metadata_json']))
assert metadata['input_hashes'] == reduction['input_hashes']
assert metadata['clip_hash_set'] == reduction['clip_hash_set']
vertex_count = len(obj.data.vertices)
assert vertex_count == len(blueprint['meshes'][0]['vertices']) and vertex_count > 0
assert len([m for m in obj.modifiers if m.type == 'ARMATURE']) == 1
modifier = next(m for m in obj.modifiers if m.type == 'ARMATURE')
assert modifier.object == arm and modifier.show_viewport and modifier.show_render
assert np.allclose(np.array(arm.matrix_world), np.eye(4), atol=1e-7, rtol=0)
assert len(blueprint['meshes']) == 1
rig_mesh = blueprint['meshes'][0]
assert rig_mesh['name'] == obj.name
transform = arm.matrix_world.inverted() @ obj.matrix_world
np.testing.assert_allclose([list(transform @ v.co) for v in obj.data.vertices],
                           [v['point'] for v in rig_mesh['vertices']], atol=1e-7, rtol=0)
assert [list(p.vertices) for p in obj.data.polygons] == rig_mesh['faces']
assert source_report['source_sha256'] == blueprint['source_sha256']
assert set(clips) == {c['action'] for c in source_report['clips']} == {a.name for a in bpy.data.actions}
for name, clip in clips.items():
    assert clip['rig_blueprint_sha256'] == reduction['input_hashes']['rig']
    assert clip['timing']['fps'] == 30
    np.testing.assert_allclose([f['time']*30 for f in clip['frames']], np.arange(len(clip['frames'])), atol=1e-6, rtol=0)
    np.testing.assert_allclose(bpy.data.actions[name].frame_range, [0,len(clip['frames'])-1], atol=1e-6, rtol=0)
    assert set(clip['contacts']) == set(clip['surface_anchors']) == set(clip['hoof_targets']) == {'fore_left','fore_right','hind_left','hind_right'}
    for leg, contacts in clip['contacts'].items():
        assert len(contacts) == len(clip['frames']) and all(type(v) is bool for v in contacts) and any(contacts)
        ids = clip['surface_anchors'][leg]['sole_vertices']
        assert ids and len(ids) == len(set(ids)) and all(type(i) is int and 0 <= i < vertex_count for i in ids)
        targets = np.array(clip['hoof_targets'][leg])
        assert targets.shape == (len(contacts),3) and np.isfinite(targets).all()
        assert np.max(np.abs(targets[np.array(contacts),2])) < 1e-7

def geometry_hash():
    def field(collection, prop, width, dtype):
        values = np.empty(len(collection)*width, dtype=dtype)
        collection.foreach_get(prop, values)
        return hashlib.sha256(values.tobytes()).hexdigest()
    mesh = obj.data
    fingerprint = {'positions':field(mesh.vertices,'co',3,np.float32),
        'normals':field(mesh.vertices,'normal',3,np.float32),'corner_normals':field(mesh.corner_normals,'vector',3,np.float32),
        'loops':field(mesh.loops,'vertex_index',1,np.int32),'faces':[list(p.vertices) for p in mesh.polygons],
        'material_indices':field(mesh.polygons,'material_index',1,np.int32),'materials':[m.name for m in mesh.materials],
        'uv':{layer.name:field(layer.data,'uv',2,np.float32) for layer in mesh.uv_layers},
        'colors':{layer.name:field(layer.data,'color',4,np.float32) for layer in mesh.color_attributes}}
    return hashlib.sha256(json.dumps(fingerprint,sort_keys=True).encode()).hexdigest()

def textures():
    result = {}
    for mat in obj.data.materials:
        assert mat and mat.node_tree
        result[mat.name] = {'links':sorted((l.from_node.name,l.from_socket.identifier,l.to_node.name,l.to_socket.identifier) for l in mat.node_tree.links), 'images':{}}
        for node in mat.node_tree.nodes:
            if node.type == 'TEX_IMAGE' and node.image:
                image = node.image
                assert image.packed_file, 'Expected packed source material images'
                result[mat.name]['images'][node.name] = {'image':image.name,'sha256':hashlib.sha256(image.packed_file.data).hexdigest(),
                    'colorspace':image.colorspace_settings.name,'filepath':image.filepath}
    return result

original_geometry = geometry_hash()
original_textures = textures()
if args.preflight_only:
    print('BLENDER_PREFLIGHT_ONLY_OK', json.dumps({'geometry_sha256':original_geometry,'textures':original_textures,'clips':sorted(clips),'quality_approved':False}), flush=True)
    raise SystemExit(0)
assert reduction['holdout_gate_passed_bool']
out.mkdir(parents=True,exist_ok=False)

def replace_weights(data):
    indices, weights, names = data['joint_indices'], data['weights'], data['bone_names'].tolist()
    assert len(indices) == vertex_count and weights.shape == indices.shape
    assert np.isfinite(weights).all() and (weights >= 0).all()
    assert np.max(np.abs(weights.sum(axis=1) - 1)) < 1e-6
    deform = {bone.name for bone in arm.data.bones if bone.use_deform}
    assert all(names[int(i)] in deform for i in np.unique(indices[weights > 0]))
    for group in list(obj.vertex_groups): obj.vertex_groups.remove(group)
    groups = [obj.vertex_groups.new(name=name) for name in names]
    for vi, (js, ws) in enumerate(zip(indices, weights)):
        for ji, weight in zip(js, ws):
            if weight > 0: groups[int(ji)].add([vi], float(weight), 'REPLACE')

def action(name):
    arm.animation_data.action = bpy.data.actions[name]
    arm.animation_data.action_slot = arm.animation_data.action.slots[0]

def points(frame):
    index = int(frame)
    scene.frame_set(index, subframe=float(frame-index))
    evaluated = obj.evaluated_get(bpy.context.evaluated_depsgraph_get())
    mesh = evaluated.to_mesh()
    try:
        values = np.empty(vertex_count * 3, dtype=np.float32)
        mesh.vertices.foreach_get('co', values)
        matrix = np.array(evaluated.matrix_world, dtype=np.float64)
        return (values.reshape(-1,3) @ matrix[:3,:3].T + matrix[:3,3]).astype(np.float32)
    finally: evaluated.to_mesh_clear()

sample_times = {name: np.arange((len(clip['frames'])-1)*4+1)/4 for name, clip in clips.items()}
replace_weights(full)
baseline_motion_max = 0.
for name, times in sample_times.items():
    action(name)
    cache = np.lib.format.open_memmap(out / (name + '-baseline.npy'), mode='w+', dtype=np.float32, shape=(len(times),vertex_count,3))
    for i, frame in enumerate(times):
        cache[i] = points(frame)
        if i: baseline_motion_max = max(baseline_motion_max,float(np.linalg.norm(cache[i]-cache[0],axis=1).max()))
    cache.flush()
    del cache
    print('BASELINE_CAPTURED', name, len(times), flush=True)
assert baseline_motion_max > .001, 'Full-weight baseline has no meaningful motion'
replace_weights(candidate)
per_clip = {}
for name, times in sample_times.items():
    action(name)
    clip = clips[name]
    cache_path = out / (name + '-baseline.npy')
    cache = np.load(cache_path, mmap_mode='r')
    maximum = 0.; squared = 0.; over3 = 0; worst = None
    hoof = {'max_hoof_target_error':0., 'max_stance_height':0., 'minimum_mesh_height':float('inf')}
    contact_counts = {leg:0 for leg in clip['contacts']}
    for i, frame in enumerate(times):
        current = points(frame)
        errors = np.linalg.norm(current - cache[i], axis=1)
        error = float(errors.max())
        if error > maximum: maximum = error; worst = {'frame':float(frame),'vertex':int(errors.argmax())}
        squared += float(np.sum(errors.astype(float)**2)); over3 += int((errors > .003).sum())
        hoof['minimum_mesh_height'] = min(hoof['minimum_mesh_height'], float(current[:,2].min()))
        key = int(frame); nxt = min(key+1,len(clip['frames'])-1); fraction = frame-key
        for leg, anchor in clip['surface_anchors'].items():
            if clip['contacts'][leg][key] and (fraction == 0 or clip['contacts'][leg][nxt]):
                contact_counts[leg] += 1
                sole = current[anchor['sole_vertices']].mean(axis=0)
                target = np.array(clip['hoof_targets'][leg][key])*(1-fraction) + np.array(clip['hoof_targets'][leg][nxt])*fraction
                hoof['max_hoof_target_error'] = max(hoof['max_hoof_target_error'],float(np.linalg.norm(sole-target)))
                hoof['max_stance_height'] = max(hoof['max_stance_height'],abs(float(sole[2])))
    del cache
    assert all(count > 0 for count in contact_counts.values())
    cache_path.unlink()  # Only the exact scratch file created above; no recursive removal.
    per_clip[name] = {'sample_count':len(times),'max_surface_error_m':maximum,'rmse_m':(squared/(len(times)*vertex_count))**.5,
                      'sample_vertex_pairs_over_3mm':over3,'worst':worst,'contacts':hoof,'contact_sample_counts':contact_counts,
                      'passed':maximum <= .003 and hoof['max_hoof_target_error'] <= .006 and hoof['max_stance_height'] <= .006 and hoof['minimum_mesh_height'] >= -.006}
    print('ACTUAL_BLENDER_QA', name, json.dumps(per_clip[name]), flush=True)
assert geometry_hash() == original_geometry and textures() == original_textures
maximum_influences = max(sum(g.weight>0 for g in v.groups) for v in obj.data.vertices)
assert maximum_influences <= 4
validation = {'schema':'autorig-blender-multiclip-weight-validation.v1','source_blend_sha256':sha(source),
              'full_weights_sha256':sha(full_path),'candidate_sha256':sha(candidate_path),'clip_hash_set':reduction['clip_hash_set'],
              'blender_version':list(bpy.app.version),'interpolation':'actual Blender quarter-frame evaluation',
              'per_clip':per_clip,'geometry_sha256':original_geometry,'geometry_unchanged':True,'baseline_motion_max_m':baseline_motion_max,
              'texture_sha256':original_textures,'textures_unchanged':True,'maximum_influences':maximum_influences,
              'quality_approved':False,'passed':all(row['passed'] for row in per_clip.values())}
(out/'weight-validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8')
assert validation['passed'], 'Actual Blender multi-clip QA failed; diagnostic report saved'
arm['weight_optimization_status'] = 'validated_exact_manifest_candidate'
arm['weight_optimization_clip_hash_set'] = json.dumps(reduction['clip_hash_set'])
arm['autorig_quality_approved'] = False
bpy.ops.object.select_all(action='DESELECT'); arm.select_set(True); obj.select_set(True)
bpy.context.view_layer.objects.active = arm
scene.render.fps=30; scene.render.fps_base=1; scene.frame_start=0
scene.frame_end=max(len(c['frames'])-1 for c in clips.values()); scene.frame_set(0)
bpy.ops.wm.save_as_mainfile(filepath=str(out/'authored-candidates.blend'),compress=True,check_existing=False)
bpy.ops.export_scene.gltf(filepath=str(out/'authored-candidates.glb'),export_format='GLB',use_selection=True,
    export_animations=True,export_def_bones=False,export_animation_mode='ACTIONS',export_frame_range=False,
    export_force_sampling=True,export_frame_step=1,export_extras=True)
for name, clip in clips.items():
    action(name); scene.frame_end=len(clip['frames'])-1; scene.frame_set(0)
    bpy.ops.export_scene.fbx(filepath=str(out/(name+'.fbx')),use_selection=True,object_types={'ARMATURE','MESH'},
        add_leaf_bones=False,use_armature_deform_only=False,bake_anim=True,bake_anim_use_all_actions=False,
        bake_anim_use_nla_strips=False,bake_anim_step=1,bake_anim_simplify_factor=0,axis_forward='-Z',axis_up='Y')
report = {'schema':'autorig-quadruped-export-candidate.v1','source_sha256':sha(source),'source_kind':'reweighted_authored_clip_set',
          'blender_version':list(bpy.app.version),'clips':source_report['clips'],
          'evaluated_surface_qa':{name:row['contacts'] for name,row in per_clip.items()},'weight_optimization':validation,
          'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha(p)} for p in out.iterdir() if p.is_file()},'quality_approved':False}
(out/'export-report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('OPTIMIZED_QUADRUPED_EXPORT_COMPLETE',flush=True)
