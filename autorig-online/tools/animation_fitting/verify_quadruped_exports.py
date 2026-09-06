"""Re-import actual GLB/FBX files and compare every frame to Blender source."""
import argparse
import json
from pathlib import Path
import sys
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from quadruped_clip_semantics import require_v1_export_report

import bpy
from mathutils import Vector
from mathutils.kdtree import KDTree


def points():
    graph=bpy.context.evaluated_depsgraph_get();result=[]
    for obj in bpy.data.objects:
        if obj.type!='MESH':continue
        # The glTF importer creates a hidden Icosphere custom bone shape.
        # It is UI state, not an exported/skinned animal surface.
        if not any(m.type=='ARMATURE' for m in obj.modifiers):continue
        evaluated=obj.evaluated_get(graph);mesh=evaluated.to_mesh()
        try:
            values=np.empty(len(mesh.vertices)*3,dtype=np.float32)
            mesh.vertices.foreach_get('co',values)
            world=np.asarray(obj.matrix_world,dtype=np.float64)
            result.append(values.reshape(-1,3) @ world[:3,:3].T+world[:3,3])
        finally:evaluated.to_mesh_clear()
    if not result:raise ValueError('Missing exported surface')
    # Bidirectional nearest-point distance is unchanged by exact duplicates.
    # Keep numerical arrays instead of millions of Python coordinate tuples;
    # never round, weld by a tolerance, or average distinct surface positions.
    return np.unique(np.concatenate(result),axis=0)


def select_action(semantic, single_file=False):
    arm=next(o for o in bpy.data.objects if o.type=='ARMATURE')
    matches=[a for a in bpy.data.actions if semantic in a.name]
    if not matches and single_file and len(bpy.data.actions)==1:
        matches=list(bpy.data.actions)
    if len(matches)!=1:
        raise ValueError(f'Expected one action for {semantic}: {[a.name for a in bpy.data.actions]}')
    action=matches[0];arm.data.pose_position='POSE';arm.animation_data_create()
    arm.animation_data.action=action;arm.animation_data.action_slot=action.slots[0]
    return arm,action


def distance(a,b):
    if len(a)==0 or len(b)==0:raise ValueError('Missing exported surface')
    tree=KDTree(len(b))
    for i,p in enumerate(b):tree.insert(Vector(p),i)
    tree.balance()
    return max(tree.find(Vector(p))[2] for p in a)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--directory',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args(sys.argv[sys.argv.index('--')+1:])
    root=args.directory.resolve();report=json.loads((root/'export-report.json').read_text())
    require_v1_export_report(report)
    bpy.ops.wm.open_mainfile(filepath=str(root/'authored-candidates.blend'))
    references={}
    for clip in report['clips']:
        semantic=clip['action'];select_action(semantic)
        frames=[]
        for i in range((clip['timing']['sample_count']-1)*2+1):
            bpy.context.scene.frame_set(i//2,subframe=(i%2)/2)
            frames.append(points())
        references[semantic]=frames
    checks=[]
    for kind in ('glb','fbx'):
        for clip in report['clips']:
            bpy.ops.wm.read_factory_settings(use_empty=True)
            bpy.context.scene.render.fps=30;bpy.context.scene.render.fps_base=1
            semantic=clip['action']
            path=root/('authored-candidates.glb' if kind=='glb' else semantic+'.fbx')
            if kind=='glb':bpy.ops.import_scene.gltf(filepath=str(path))
            else:bpy.ops.import_scene.fbx(filepath=str(path),use_anim=True,anim_offset=0,automatic_bone_orientation=False)
            arm,action=select_action(semantic,single_file=kind=='fbx')
            start,end=action.frame_range
            duration=(end-start)/30
            if abs(duration-clip['timing']['duration_seconds'])>1e-5:
                raise ValueError(f'{kind} {semantic} duration changed: {duration}')
            worst=0.;worst_frame=0
            for i,reference in enumerate(references[semantic]):
                time=float(start)+i/2
                bpy.context.scene.frame_set(int(time),subframe=time-int(time))
                actual=points()
                error=max(distance(actual,reference),distance(reference,actual))
                if error>worst:worst=error;worst_frame=i/2
            check={'format':kind,'action':semantic,'duration':duration,'sampled_times':len(references[semantic]),
                   'bones':len(arm.data.bones),'max_surface_error':worst,'worst_frame':worst_frame,
                   'passed':worst<=.003}
            checks.append(check)
            print('EXPORT_CHECK='+json.dumps(check),flush=True)
    result={'schema':'autorig-quadruped-reimport-qa.v1','checks':checks,'passed':all(c['passed'] for c in checks),
            'reference_storage':'numerical arrays; exact duplicate surface points removed without tolerance',
            'reference_storage_bytes':sum(frame.nbytes for frames in references.values() for frame in frames),
            'maximum_surface_error_gate':.003,'quality_approved':False,
            'scope':'source versus reimport, every key and half-frame; not gait aesthetics or controller transitions'}
    with args.output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
    if not result['passed']:raise ValueError('Export deformation mismatch')


if __name__=='__main__':main()
