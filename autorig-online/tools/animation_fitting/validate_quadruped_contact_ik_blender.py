"""Evaluate nominal contact-IK correction on the full native Blender mesh."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import bpy
from mathutils import Matrix
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
from quadruped_clip_semantics import validate_v2_clip,verify_profile_sources
from quadruped_surface_blender import actor_local_points


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--skin-proof',type=Path,required=True);p.add_argument('--envelope',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args(sys.argv[sys.argv.index('--')+1:])
    envelope=json.loads(a.envelope.read_text());pin=envelope['source_report_pin']
    if envelope.get('schema')!='autorig-quadruped-contact-ik-envelope.v1' or not envelope['passed']:
        raise ValueError('A passed explicit contact envelope is required')
    if sha(pin['path'])!=pin['sha256']:raise ValueError('Source report changed')
    report=json.loads(Path(pin['path']).read_text());rp=envelope['rig_blueprint_pin']
    if report.get('schema')!='autorig-quadruped-export-candidate.v2':raise ValueError('V2 source report required')
    if rp!=report['rig_blueprint_pin'] or sha(rp['path'])!=rp['sha256']:raise ValueError('Blueprint pin changed')
    blueprint=json.loads(Path(rp['path']).read_text());clip=next(c for c in report['clips'] if c['action']==envelope['action'])
    verify_profile_sources(clip);context=validate_v2_clip(clip,blueprint)
    if (report['source_sha256']!=blueprint['source_sha256'] or clip['source_sha256']!=report['source_sha256'] or
            clip['rig_source_sha256']!=report['source_sha256'] or clip['rig_blueprint_sha256']!=rp['sha256']):
        raise ValueError('Source provenance pins disagree')
    gameplay=json.loads(Path(clip['profile_sources']['gameplay_profile']).read_text())
    expected_frames=np.arange((context.sample_count-1)*4+1)/4
    if envelope['sample_rate_hz']!=120 or len(envelope['samples'])!=len(expected_frames):
        raise ValueError('Envelope must cover the complete quarter-frame grid')
    bone_names={b['name'] for b in blueprint['bones']}
    measured_correction=0.;contact_counts={leg:0 for leg in context.contacts}
    for expected,sample in zip(expected_frames,envelope['samples']):
        if sample['frame']!=expected or sample['correction_passed'] is not True:raise ValueError('Invalid envelope sample')
        i=int(expected);f=expected-i;j=min(i+1,context.sample_count-1)
        actor=context.actor_translation[i]*(1-f)+context.actor_translation[j]*f
        np.testing.assert_allclose(sample['actor_translation'],actor,rtol=0,atol=1e-12)
        active={leg:bool(context.contacts[leg][i]) for leg in context.contacts}
        if sample['active_contacts']!=active:raise ValueError('Runtime event contact state was altered')
        for leg,state in active.items():contact_counts[leg]+=int(state)
        permitted=set()
        for leg in active:
            target=context.targets[leg][i]*(1-f)+context.targets[leg][j]*f+actor
            np.testing.assert_allclose(sample['world_targets'][leg],target,rtol=0,atol=1e-12)
            if active[leg]:
                if abs(target[2]-context.ground_height)>context.ground_tolerance:raise ValueError('Active target left ground')
                permitted.update(gameplay['limbs'][leg]['chain'])
                permitted.update(r['bone'] for r in gameplay['limbs'][leg]['intermediate'])
        if set(sample['original_local'])!=bone_names or set(sample['corrected_local'])!=bone_names:
            raise ValueError('Envelope bone coverage mismatch')
        for name in bone_names:
            before=np.asarray(sample['original_local'][name],float).reshape(4,4)
            after=np.asarray(sample['corrected_local'][name],float).reshape(4,4)
            for matrix in (before,after):
                if (not np.isfinite(matrix).all() or not np.allclose(matrix[3],[0,0,0,1],atol=1e-7,rtol=0) or
                        not np.allclose(matrix[:3,:3].T@matrix[:3,:3],np.eye(3),atol=1e-5,rtol=0) or
                        abs(np.linalg.det(matrix[:3,:3])-1)>1e-5):raise ValueError('Envelope matrix is not rigid affine')
            if name not in permitted and not np.array_equal(before,after):raise ValueError('Non-contact bone was changed')
            u,_,v=np.linalg.svd(before[:3,:3]);rb=u@v
            u,_,v=np.linalg.svd(after[:3,:3]);ra=u@v
            angle=np.degrees(np.arccos(np.clip((np.trace(rb.T@ra)-1)/2,-1,1)))
            measured_correction=max(measured_correction,float(angle))
            if angle>8+1e-4:raise ValueError('Envelope exceeds per-bone correction cap')
    proof=json.loads(a.skin_proof.read_text());source_hash=sha(a.source)
    if proof.get('schema','autorig-full-native-skin-proof.v1')!='autorig-full-native-skin-proof.v1':raise ValueError('Unsupported skin proof')
    if proof['output_sha256']!=source_hash or proof['source_sha256']!=report['files']['authored-candidates.blend']['sha256']:
        raise ValueError('Native proof does not match')
    bpy.ops.wm.open_mainfile(filepath=str(a.source.resolve()))
    arm=bpy.data.objects[blueprint['armature']];arm.data.pose_position='POSE'
    names=[m['name'] for m in blueprint['meshes']];action=bpy.data.actions[envelope['action']]
    source_scales={bone.name:bone.scale.copy() for bone in arm.pose.bones}
    rest={r['name']:Matrix([r['rest_local'][i:i+4] for i in range(0,16,4)]) for r in blueprint['bones']}
    stats={kind:{'minimum_ground_clearance_m':float('inf'),'max_active_target_error_m':0.,
                 'max_active_stance_height_m':0.,'max_all_target_error_m':0.} for kind in ('original','corrected')}
    parity=0.;parity_worst=None;applied_error=0.;joint_violation=0.;rows=[]
    for sample in envelope['samples']:
        frame=sample['frame'];actor=np.asarray(sample['actor_translation']);targets=sample['world_targets'];active=sample['active_contacts']
        arm.animation_data.action=action;arm.animation_data.action_slot=action.slots[0]
        # Scale is not keyed by the source bridge. Manual matrix assignment
        # during the preceding correction must not contaminate this baseline.
        for name,scale in source_scales.items():arm.pose.bones[name].scale=scale
        bpy.context.scene.frame_set(int(frame),subframe=frame-int(frame));bpy.context.view_layer.update()
        for name,values in sample['original_local'].items():
            bone=arm.pose.bones[name];local=bone.parent.matrix.inverted()@bone.matrix if bone.parent else bone.matrix
            error=float(np.max(np.abs(np.asarray(local)-np.asarray(values).reshape(4,4))))
            if error>parity:parity=error;parity_worst={'frame':frame,'bone':name,'source_scale':list(source_scales[name])}
        before=actor_local_points(arm,names)+actor
        arm.animation_data.action=None
        for name,values in sample['corrected_local'].items():
            local=Matrix([values[i:i+4] for i in range(0,16,4)])
            arm.pose.bones[name].matrix_basis=rest[name].inverted()@local
            arm.pose.bones[name].scale=(1,1,1)
        bpy.context.view_layer.update();after=actor_local_points(arm,names)+actor
        for name,values in sample['corrected_local'].items():
            bone=arm.pose.bones[name];local=bone.parent.matrix.inverted()@bone.matrix if bone.parent else bone.matrix
            applied_error=max(applied_error,float(np.max(np.abs(np.asarray(local)-np.asarray(values).reshape(4,4)))))
        for leg,declaration in gameplay['limbs'].items():
            chain=declaration['chain'];parent=arm.pose.bones[chain[0]].parent
            base=np.asarray(parent.matrix)[:3,:3]@np.asarray(parent.bone.matrix_local)[:3,:3].T
            heads=np.asarray([list(arm.pose.bones[n].matrix.translation) for n in chain[:4]])
            vectors=np.diff(heads,axis=0)@base
            angles=np.arctan2(vectors[:,1],-vectors[:,2])
            rest_heads=np.asarray([list(arm.data.bones[n].head_local) for n in chain[:4]])
            rest_first=rest_heads[1]-rest_heads[0]
            q=np.asarray([angles[0]-np.arctan2(rest_first[1],-rest_first[2]),angles[1]-angles[0],angles[2]-angles[1]])
            q=(q+np.pi)%(2*np.pi)-np.pi
            low=np.radians(declaration['joint_lower_degrees']);high=np.radians(declaration['joint_upper_degrees'])
            joint_violation=max(joint_violation,float(np.maximum(low-q,q-high).max()))
        row={'frame':frame}
        for kind,points in (('original',before),('corrected',after)):
            result=stats[kind];clearance=float(points[:,2].min()-context.ground_height)
            if clearance<result['minimum_ground_clearance_m']:
                result.update(minimum_ground_clearance_m=clearance,worst_ground_frame=frame,worst_ground_vertex=int(points[:,2].argmin()))
            row[kind+'_active_error_m']=0.
            for leg,anchors in context.sole_anchors.items():
                sole=points[list(anchors['sole_vertices'])].mean(axis=0);error=float(np.linalg.norm(sole-np.asarray(targets[leg])))
                result['max_all_target_error_m']=max(result['max_all_target_error_m'],error)
                if active[leg]:
                    result['max_active_target_error_m']=max(result['max_active_target_error_m'],error)
                    result['max_active_stance_height_m']=max(result['max_active_stance_height_m'],abs(float(sole[2]-context.ground_height)))
                    row[kind+'_active_error_m']=max(row[kind+'_active_error_m'],error)
        rows.append(row)
    result={'schema':'autorig-native-contact-ik-validation.v1','source_native_sha256':source_hash,
        'native_weights_sha256':proof['full_weights_sha256'],'envelope_sha256':sha(a.envelope),
        'sample_count':len(rows),'blender_nlerp_matrix_max_error':parity,'parity_worst':parity_worst,
        'applied_matrix_max_error':applied_error,'joint_bound_max_violation_radians':joint_violation,
        'source_unkeyed_scales_restored_for_baseline':True,'corrected_pose_scales_normalized_to_one':True,'stats':stats,
        'strict_contact_tolerance_m':context.ground_tolerance,'maximum_local_correction_degrees':measured_correction,
        'active_contact_sample_counts':contact_counts,
        'passed':sum(contact_counts.values())>0 and parity<=1e-5 and applied_error<=1e-5 and joint_violation<=1e-4 and stats['corrected']['max_active_target_error_m']<=context.ground_tolerance and
                  stats['corrected']['max_active_stance_height_m']<=context.ground_tolerance and
                  stats['corrected']['minimum_ground_clearance_m']>=-context.ground_tolerance,
        'scope':'nominal contact replay only; no early/late/absent collision or engine approval','quality_approved':False,'per_sample':rows}
    output=a.output.resolve();output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x') as stream:json.dump(result,stream,indent=2)
    if sha(a.source)!=source_hash:raise ValueError('Source Blend changed')
    print(json.dumps({k:v for k,v in result.items() if k!='per_sample'}),flush=True)
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
