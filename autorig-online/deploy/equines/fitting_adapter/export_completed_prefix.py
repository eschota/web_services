"""Export a verified completed fit prefix as a diagnostic partial reference, never a library clip."""
from __future__ import annotations
import argparse,hashlib,json,math,os,pathlib,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import equine_constraint_fit as FIT
SCHEMA='autorig.equine-prefix-export-job/1'
SPEC={'name':'equine-prefix-export','kind':'cli','farm_safe':True,
 'summary':'Replay a SHA-pinned completed fit prefix as an explicit partial diagnostic; never promotes a library.',
 'usage':'equine-prefix-export --describe | --job PROJECT_JOB_JSON (managed Windows farm only)',
 'contract':{'in':{'job':'exact source/fit-job/checkpoint/code pins; fresh project output'},
             'out':{'state':'DIAGNOSTIC_PARTIAL_REFERENCE_NOT_LIBRARY','metrics':'not recomputed; real export/retarget QA still required'},
             'scope':'partial source frames only; no optimizer, customer writes, library promotion or owner acceptance'}}
def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def rd(p):
 v=json.loads(pathlib.Path(p).read_text(encoding='utf-8'))
 if not isinstance(v,dict): raise ValueError(f'{p} must contain object')
 return v
def pin(row,label):
 p=pathlib.Path(row['path']).resolve()
 if not p.is_file() or sha(p)!=row['sha256']: raise ValueError(f'{label} missing/SHA mismatch')
 return p
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--job',type=pathlib.Path); ap.add_argument('--describe',action='store_true'); a=ap.parse_args()
 if a.describe: print(json.dumps(SPEC,indent=2)); return 0
 if a.job is None: ap.error('--job or --describe is required')
 try:
  job=rd(a.job)
  if job.get('schema')!=SCHEMA or job.get('accepted') is not False or job.get('library_activation') is not False: raise ValueError('prefix export job contract invalid')
  checkpoint_path=pin(job['checkpoint'],'checkpoint'); fitjob_path=pin(job['original_fit_job'],'original_fit_job'); checkpoint=rd(checkpoint_path); fitjob=rd(fitjob_path)
  code_pins={name:sha(pin(row,f'code:{name}')) for name,row in (job.get('code_pins') or {}).items()}
  if not code_pins or code_pins.get('equine_constraint_fit.py')!=sha(pathlib.Path(FIT.__file__).resolve()) or code_pins.get('export_completed_prefix.py')!=sha(pathlib.Path(__file__).resolve()): raise ValueError('exact fitter/exporter code pins required')
  paths,c,_,action,opt=FIT.preflight(fitjob); FIT.farm_execute_guard()
  import numpy as np
  runtime=pathlib.Path(fitjob['animation_fitting_code']).resolve(); sys.path.insert(0,str(runtime.parent))
  from animation_fitting.rig import load_rig_bundle
  from animation_fitting.math3d import flatten_matrix,quaternion_xyzw_from_matrix
  rig=load_rig_bundle(paths['bundle_manifest'].parent); variables=[]; axes={}
  for leg,row in (c.get('constraints') or {}).items():
   chain=[x for x in row.get('source_bone_chain') or [] if x in rig.bones and rig.bones[x].parent is not None][:4]
   normal=np.asarray(FIT.gltf_to_blender((row.get('plane') or {}).get('normal')),float); normal/=np.linalg.norm(normal)
   for bone in chain:
    axis=int(np.argmax(np.abs(rig.bones[bone].rest_world[:3,:3].T@normal)))
    if bone not in variables: variables.append(bone); axes[bone]=axis
  torso=c.get('torso_constraints') or {}; root_enabled=any((row.get('constraints') or []) for row in torso.values()); n_joint=len(variables); n_total=n_joint+(2 if root_enabled else 0)
  order=[f'{bone}.{"xyz"[axes[bone]]}' for bone in variables]+(['root_translation.forward','root_translation.up'] if root_enabled else [])
  pins={'bundle_manifest_sha256':sha(paths['bundle_manifest']),'action_contract_sha256':sha(paths['action_contract']),'constraints_sha256':sha(paths['constraints']),'profile_sha256':sha(paths['profile']),'contact_evidence_sha256':sha(paths['contact_evidence']),'source_eligibility_sha256':sha(paths['source_eligibility']),'surface_anchors_sha256':sha(paths['surface_anchors']),'source_model_sha256':c.get('provenance',{}).get('source_model_sha256')}
  end,solved,_=FIT.validate_resume(checkpoint,order,pins,opt)
  original_count=int(action['frame_count']); fps=float(action['fps'])
  if end<1 or end>=original_count-1: raise ValueError('checkpoint must be a strict nonempty partial prefix')
  physical=np.asarray(FIT.gltf_to_blender((c.get('axes') or {}).get('up')),float); physical/=np.linalg.norm(physical); forward=np.asarray(FIT.gltf_to_blender((c.get('axes') or {}).get('forward')),float); forward-=physical*np.dot(forward,physical); forward/=np.linalg.norm(forward)
  frames=[]
  for frame in range(end+1):
   x=np.asarray(solved[str(frame)],float)
   if x.shape!=(n_total,) or not np.isfinite(x).all(): raise ValueError(f'invalid solved vector at frame {frame}')
   e={bone:np.eye(3)[axes[bone]]*x[i] for i,bone in enumerate(variables)}; root=np.zeros(3) if not root_enabled else forward*x[n_joint]+physical*x[n_joint+1]; _,local=rig.forward_kinematics(root,np.zeros(3),e)
   bones={name:{'parent':rig.bones[name].parent,'local_matrix':flatten_matrix(local[name]),'local_translation':[float(v) for v in local[name][:3,3]],'local_rotation_xyzw':quaternion_xyzw_from_matrix(local[name])} for name in rig.bone_order}
   frames.append({'frame':frame,'time_seconds':frame/fps,'original_frame':frame,'root_translation':root.tolist(),'root_rotation_rotvec':[0,0,0],'bones':bones})
  output={'schema':'autorig-fitted-animation.v1','state':'DIAGNOSTIC_PARTIAL_REFERENCE_NOT_LIBRARY','rig_bundle':{'path':str(rig.metadata_path),'sha256':rig.metadata_sha256},'observations':{'path':str(paths['constraints']),'sha256':sha(paths['constraints']),'provenance':c.get('provenance')},'frame_count':len(frames),'fps':fps,'duration_seconds':end/fps,'loop':False,'transform_contract':{'schema':'autorig-fitted-transform-contract.v1','source_armature_name':rig.armature_name,'source_armature_world_matrix':flatten_matrix(rig.armature_world),'root_local_matrix_space':'WORLD','child_local_matrix_space':'PARENT_BONE','rotation_channel':'QUATERNION','scale_animation':False,'translation_policy':{'mode':'root_only','bones':[name for name in rig.bone_order if rig.bones[name].parent is None]}},'active_bones':variables,'degrees_of_freedom':order,'config':{'original_action_frame_count':original_count,'original_action_contract_sha256':sha(paths['action_contract']),'checkpoint_sha256':sha(checkpoint_path),'original_fit_job_sha256':sha(fitjob_path),'code_pins':code_pins,'prefix_completed_through_frame':end,'source_frame_mapping':list(range(end+1)),'cropped_failure_hidden':False,'metrics_recomputed':False},'optimizer':{'kind':'none_fk_replay_of_verified_prefix','optimization_performed':False},'qa':{'decision':None,'animation_quality_approved':False,'partial_reference':True,'pending':['remaining_original_frames_under_investigation','real_post_retarget_3d_QA','owner_acceptance'],'library_eligible':False},'frames':frames}
  destination=pathlib.Path(job['output']); destination.parent.mkdir(parents=True,exist_ok=True)
  if destination.exists(): raise ValueError('fresh output required')
  tmp=destination.with_name(f'.{destination.name}.{os.getpid()}.tmp'); tmp.write_text(json.dumps(output,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(tmp,destination); return 0
 except Exception as e: print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
