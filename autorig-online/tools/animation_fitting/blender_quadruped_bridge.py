"""Blender bridge for measured quadruped authoring candidates (not approvals)."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import math
import os
import subprocess

import bpy
from mathutils import Matrix, Quaternion, Vector


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def export_blueprint(source, output):
    source=source.resolve();output=output.resolve()
    bpy.ops.wm.open_mainfile(filepath=str(source))
    arms=[o for o in bpy.data.objects if o.type=='ARMATURE']
    if len(arms)!=1:
        raise ValueError('Expected exactly one armature')
    arm=arms[0]
    if '__animal_export_root' not in arm.data.bones:
        raise ValueError('Expected a compact anatomical rig with an explicit motion root')
    if any(b.use_deform and b.bbone_segments!=1 for b in arm.data.bones):
        raise ValueError('Authoring blueprint requires linearized deform bones')
    if any(p.constraints for p in arm.pose.bones):
        raise ValueError('Authoring blueprint cannot contain active control constraints')
    if arm.animation_data and (arm.animation_data.action or len(arm.animation_data.nla_tracks)):
        raise ValueError('Blueprint source must be actionless')
    bones=[]
    for bone in arm.data.bones:
        parent=bone.parent
        local=parent.matrix_local.inverted() @ bone.matrix_local if parent else bone.matrix_local
        bones.append({'name':bone.name,'parent':parent.name if parent else None,
            'deform':bone.use_deform,'rest_world':[v for row in bone.matrix_local for v in row],
            'rest_local':[v for row in local for v in row],
            'head':list(bone.head_local),'tail':list(bone.tail_local)})
    meshes=[]
    for obj in bpy.data.objects:
        if obj.type!='MESH':continue
        transform=arm.matrix_world.inverted() @ obj.matrix_world
        groups={g.index:g.name for g in obj.vertex_groups}
        vertices=[]
        for v in obj.data.vertices:
            weights=[{'bone':groups[g.group],'weight':g.weight} for g in v.groups
                     if g.weight>0 and groups[g.group] in arm.data.bones]
            if not 1<=len(weights)<=4:
                raise ValueError('Expected one to four influences per vertex')
            total=sum(w['weight'] for w in weights)
            for w in weights:w['weight']/=total
            vertices.append({'point':list(transform @ v.co),'weights':weights})
        meshes.append({'name':obj.name,'vertices':vertices,
                       'faces':[list(p.vertices) for p in obj.data.polygons]})
    payload={'schema':'autorig-quadruped-authoring-rig.v1','source_sha256':sha256(source),
             'source_path':str(source.resolve()),'armature':arm.name,
             'coordinates':{'up':'+Z','forward':'-Y','right':'+X','units':'source_meters'},
             'bones':bones,'meshes':meshes,'quality_approved':False}
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x',encoding='utf-8') as f:json.dump(payload,f,indent=2,allow_nan=False)
    print(json.dumps({'blueprint':str(output),'bones':len(bones),
                      'vertices':sum(len(m['vertices']) for m in meshes)}))


def object_mode(arm):
    bpy.context.view_layer.objects.active=arm
    if arm.mode!='OBJECT':bpy.ops.object.mode_set(mode='OBJECT')


def evaluated_points(arm, mesh_names):
    graph=bpy.context.evaluated_depsgraph_get()
    points=[]
    for name in mesh_names:
        obj=bpy.data.objects[name];evaluated=obj.evaluated_get(graph)
        mesh=evaluated.to_mesh()
        matrix=arm.matrix_world.inverted() @ obj.matrix_world
        points.extend(matrix @ v.co for v in mesh.vertices)
        evaluated.to_mesh_clear()
    return points


def action_curves(action):
    for layer in action.layers:
        for strip in layer.strips:
            for bag in strip.channelbags:
                yield from bag.fcurves


def apply_clips(source, clips_dir, blueprint_path, output):
    source=source.resolve()
    output=output.resolve();clips_dir=clips_dir.resolve();blueprint_path=blueprint_path.resolve()
    blueprint=json.loads(blueprint_path.read_text(encoding='utf-8'))
    blueprint_hash=sha256(blueprint_path)
    if sha256(source)!=blueprint['source_sha256']:raise ValueError('Source blend changed')
    clips=[json.loads(p.read_text(encoding='utf-8')) for p in sorted(clips_dir.glob('*.json'))]
    if not clips:raise ValueError('No authored clips')
    for clip in clips:
        if clip.get('rig_blueprint_sha256')!=blueprint_hash or clip['rig_source_sha256']!=blueprint['source_sha256']:
            raise ValueError('Clip and skeleton source pins disagree')
    output.mkdir(parents=True,exist_ok=False)
    bpy.ops.wm.open_mainfile(filepath=str(source))
    arm=bpy.data.objects[blueprint['armature']];object_mode(arm)
    arm.data.pose_position='POSE'
    bpy.context.scene.render.fps=30;bpy.context.scene.render.fps_base=1
    arm.animation_data_clear();arm.animation_data_create()
    rest={row['name']:Matrix([row['rest_local'][i:i+4] for i in range(0,16,4)]) for row in blueprint['bones']}
    mesh_names=[m['name'] for m in blueprint['meshes']]
    for name in mesh_names:
        obj=bpy.data.objects[name];world=obj.matrix_world.copy()
        obj.parent=arm;obj.matrix_world=world
    actions={};validation={}
    for clip in clips:
        semantic=clip['action']
        action=bpy.data.actions.new(semantic)
        slot=action.slots.new(id_type='OBJECT',name=arm.name)
        arm.animation_data.action=action;arm.animation_data.action_slot=slot
        action.use_fake_user=True
        action['loop']=clip['timing']['loop'];action['semantic_id']=semantic
        action['reference_speed']=clip['reference_speed'];action['quality_approved']=False
        for index,frame in enumerate(clip['frames']):
            bpy.context.scene.frame_set(index)
            for name,trs in frame['bones'].items():
                q=trs['rotation'];local=Quaternion((q[3],q[0],q[1],q[2])).to_matrix().to_4x4()
                local.translation=Vector(trs['translation'])
                basis=rest[name].inverted() @ local
                pose=arm.pose.bones[name]
                pose.rotation_mode='QUATERNION';pose.matrix_basis=basis
                if max(abs(s-1) for s in pose.scale)>1e-4:raise ValueError('Unexpected bone scale')
                pose.keyframe_insert('location',frame=index,group=name)
                pose.keyframe_insert('rotation_quaternion',frame=index,group=name)
        for curve in action_curves(action):
            for key in curve.keyframe_points:key.interpolation='LINEAR'
        actions[semantic]=action
        # Independently measure Blender's evaluated skinned surface, including
        # halfway samples between keys that can reveal interpolation footslide.
        maxima={'max_hoof_target_error':0.,'max_stance_height':0.,'minimum_mesh_height':float('inf')}
        for step in range((len(clip['frames'])-1)*2+1):
            time=step/2;frame=int(time);fraction=time-frame
            bpy.context.scene.frame_set(frame,subframe=fraction)
            points=evaluated_points(arm,mesh_names)
            lowest=min(range(len(points)),key=lambda v:points[v].z)
            if points[lowest].z<maxima['minimum_mesh_height']:
                maxima.update(minimum_mesh_height=points[lowest].z,lowest_frame=time,
                              lowest_vertex=lowest,lowest_point=list(points[lowest]))
            next_frame=min(frame+1,len(clip['frames'])-1)
            for leg,anchor in clip['surface_anchors'].items():
                ids=anchor['sole_vertices'];sole=sum((points[i] for i in ids),Vector())/len(ids)
                if clip['contacts'][leg][frame] and clip['contacts'][leg][next_frame]:
                    a=Vector(clip['hoof_targets'][leg][frame]);b=Vector(clip['hoof_targets'][leg][next_frame])
                    target=a.lerp(b,fraction)
                    maxima['max_hoof_target_error']=max(maxima['max_hoof_target_error'],(sole-target).length)
                    maxima['max_stance_height']=max(maxima['max_stance_height'],abs(sole.z))
        if maxima['max_hoof_target_error']>.006 or maxima['minimum_mesh_height']<-.006:
            raise ValueError('Blender deformation failed '+semantic+': '+json.dumps(maxima))
        validation[semantic]=maxima
    arm['authoring_status']='candidate_pending_visual_and_export_qa'
    bpy.ops.object.select_all(action='DESELECT')
    arm.select_set(True)
    for name in mesh_names:bpy.data.objects[name].select_set(True)
    bpy.context.view_layer.objects.active=arm
    bpy.ops.wm.save_as_mainfile(filepath=str(output/'authored-candidates.blend'))
    bpy.ops.export_scene.gltf(filepath=str(output/'authored-candidates.glb'),export_format='GLB',
        use_selection=True,export_animations=True,export_def_bones=False,
        export_animation_mode='ACTIONS',export_frame_range=False,
        export_force_sampling=True,export_frame_step=1,export_extras=True)
    for clip in clips:
        action=actions[clip['action']];arm.animation_data.action=action
        arm.animation_data.action_slot=action.slots[0]
        scene=bpy.context.scene;scene.frame_start=0;scene.frame_end=len(clip['frames'])-1;scene.frame_set(0)
        bpy.ops.export_scene.fbx(filepath=str(output/(clip['action']+'.fbx')),use_selection=True,
            object_types={'ARMATURE','MESH'},add_leaf_bones=False,use_armature_deform_only=False,
            bake_anim=True,bake_anim_use_all_actions=False,bake_anim_use_nla_strips=False,
            bake_anim_step=1,bake_anim_simplify_factor=0,axis_forward='-Z',axis_up='Y')
    report={'schema':'autorig-quadruped-export-candidate.v1','source_sha256':blueprint['source_sha256'],
        'blender_version':list(bpy.app.version),'evaluated_surface_qa':validation,
        'clips':[{'action':c['action'],'timing':c['timing'],'reference_speed':c['reference_speed'],
                  'root_motion':c['root_motion'],'root_delta':c['root_delta'],
                  'contacts':c['contacts'],'quality_approved':False} for c in clips],
        'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha256(p)} for p in output.iterdir() if p.is_file()},
        'quality_approved':False}
    (output/'export-report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print('QUADRUPED_EXPORT='+json.dumps(report))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',choices=('export-rig','apply'))
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--clips-dir',type=Path)
    parser.add_argument('--blueprint',type=Path)
    args=parser.parse_args(sys.argv[sys.argv.index('--')+1:])
    if args.stage=='export-rig':export_blueprint(args.source,args.output)
    else:
        if not args.clips_dir or not args.blueprint:parser.error('apply needs --clips-dir and --blueprint')
        apply_clips(args.source,args.clips_dir,args.blueprint,args.output)


if __name__=='__main__':main()
