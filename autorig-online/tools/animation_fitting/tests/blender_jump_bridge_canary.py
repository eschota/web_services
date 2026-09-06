"""Create or inspect a tiny original mesh for the v2 bridge tests."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import bpy
from mathutils import Quaternion

TOOLS=Path(__file__).resolve().parents[1];sys.path.insert(0,str(TOOLS))
from blender_quadruped_bridge import export_blueprint
from quadruped_clip_semantics import FEET

p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
p.add_argument('--mesh-z',type=float,default=0.);p.add_argument('--verify',type=Path)
p.add_argument('--target-offset-x',type=float,default=0.)
a=p.parse_args(sys.argv[sys.argv.index('--')+1:]);root=a.root.resolve()
if a.verify:
    bpy.ops.wm.open_mainfile(filepath=str(a.verify.resolve()))
    arm=bpy.data.objects['CanaryRig'];act=bpy.data.actions['jump_full']
    arm.animation_data.action=act;arm.animation_data.action_slot=act.slots[0]
    rows=[]
    for i in range(3):
        bpy.context.scene.frame_set(i);bpy.context.view_layer.update()
        rows.append({'frame':i,'actor_location':list(arm.location),
                     'diagnostic_only':bool(arm['diagnostic_only']),
                     'surface_qa_passed':bool(arm['surface_qa_passed']),
                     'root_translation':list(arm.pose.bones['__animal_export_root'].matrix.translation)})
    (root/'pose-check.json').write_text(json.dumps(rows))
else:
    bpy.ops.object.select_all(action='SELECT');bpy.ops.object.delete(use_global=False)
    bpy.ops.object.armature_add();arm=bpy.context.object;arm.name='CanaryRig'
    bpy.ops.object.mode_set(mode='EDIT')
    bone=arm.data.edit_bones[0];bone.name='__animal_export_root';bone.head=(0,0,0);bone.tail=(0,.5,0)
    child=arm.data.edit_bones.new('spine');child.parent=bone;child.head=(0,.5,0);child.tail=(0,1,0)
    bpy.ops.object.mode_set(mode='OBJECT')
    coords=[(-.2,-.3,a.mesh_z),(.2,-.3,a.mesh_z),(-.2,.3,a.mesh_z),(.2,.3,a.mesh_z)]
    mesh=bpy.data.meshes.new('CanaryMeshData');mesh.from_pydata(coords,[],[(0,1,2),(1,3,2)])
    obj=bpy.data.objects.new('CanaryMesh',mesh);bpy.context.scene.collection.objects.link(obj)
    mod=obj.modifiers.new('Skin','ARMATURE');mod.object=arm
    group=obj.vertex_groups.new(name='__animal_export_root');group.add(list(range(4)),1.,'REPLACE')
    source=root/'source.blend';bpy.ops.wm.save_as_mainfile(filepath=str(source))
    blueprint_path=root/'rig.json';export_blueprint(source,blueprint_path)
    blueprint=json.loads(blueprint_path.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    transforms={}
    for bone in bpy.data.objects['CanaryRig'].data.bones:
        matrix=bone.parent.matrix_local.inverted()@bone.matrix_local if bone.parent else bone.matrix_local
        q=Quaternion((1,0,0),.05) if bone.name=='spine' else matrix.to_quaternion()
        transforms[bone.name]={'translation':list(matrix.translation),'rotation':[q.x,q.y,q.z,q.w],'scale':[1,1,1]}
    clip={'schema':'autorig-authored-quadruped-clip.v2','action':'jump_full',
        'timing':{'fps':30,'sample_count':3,'interval_count':2},
        'playback':{'mode':'one_shot','seam_policy':'end_pose'},
        'motion':{'world_owner':'controller','pose_root':'__animal_export_root','pose_space':'actor_local',
                  'baked_actor_translation':False,'pose_root_offsets':[[0,0,0]]*3},
        'reference_actor_motion':{'mode':'one_shot','translations':[[0,0,0],[0,0,.2],[0,0,0]]},
        'ground':{'space':'reference_world','height':0.,'tolerance':.001},
        'frames':[{'time':i/30,'bones':transforms} for i in range(3)],
        'contacts':{f:[False]*3 for f in FEET},'entry_contacts':{f:False for f in FEET},
        'phases':[{'kind':'flight','start':0,'end':3}],'events':[],
        'hoof_targets':{f:[[coords[i][0]+a.target_offset_x,coords[i][1],0.]]*3 for i,f in enumerate(FEET)},
        'surface_anchors':{f:{'sole_vertices':[i],'foot_vertices':[i]} for i,f in enumerate(FEET)},
        'source_sha256':sha(source),'rig_source_sha256':sha(source),'rig_blueprint_sha256':sha(blueprint_path)}
    clips=root/'clips';clips.mkdir();(clips/'jump_full.json').write_text(json.dumps(clip))
