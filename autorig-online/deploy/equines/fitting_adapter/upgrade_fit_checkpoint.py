"""Strictly attest missing pins on a legacy diagnostic checkpoint; never resume or accept it."""
from __future__ import annotations
import argparse,copy,hashlib,json,os,pathlib,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import equine_constraint_fit as FIT
SCHEMA='autorig.equine-checkpoint-upgrade-request/1'
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
 ap=argparse.ArgumentParser(); ap.add_argument('--request',required=True,type=pathlib.Path); ap.add_argument('--output',required=True,type=pathlib.Path); a=ap.parse_args()
 try:
  req=rd(a.request)
  if req.get('schema')!=SCHEMA or req.get('accepted') is not False or req.get('automatic_resume') is not False: raise ValueError('upgrade request contract invalid')
  legacy_path=pin(req['legacy_checkpoint'],'legacy_checkpoint'); fitjob_path=pin(req['original_fit_job'],'original_fit_job'); legacy=rd(legacy_path); job=rd(fitjob_path)
  code={}
  for name,row in (req.get('code_pins') or {}).items(): code[name]={'path':str(pin(row,f'code:{name}')),'sha256':row['sha256']}
  if not code: raise ValueError('exact original code pins required')
  paths,c,_,_,opt=FIT.preflight(job)
  full={'bundle_manifest_sha256':sha(paths['bundle_manifest']),'action_contract_sha256':sha(paths['action_contract']),'constraints_sha256':sha(paths['constraints']),'profile_sha256':sha(paths['profile']),'contact_evidence_sha256':sha(paths['contact_evidence']),'surface_anchors_sha256':sha(paths['surface_anchors']),'source_model_sha256':c.get('provenance',{}).get('source_model_sha256')}
  if legacy.get('schema')!='autorig.equine-constraint-fit-progress/1' or legacy.get('objective_schema')!=FIT.OBJECTIVE_SCHEMA or not isinstance(legacy.get('variable_order'),list) or not legacy['variable_order']: raise ValueError('legacy checkpoint schema/order invalid')
  for key,value in (legacy.get('pins') or {}).items():
   if key not in full or full[key]!=value: raise ValueError(f'legacy checkpoint source pin mismatch: {key}')
  legacy_options=legacy.get('options') or {}; objective_keys=(set(legacy_options)|set(opt))-FIT.SOLVER_ONLY_OPTIONS
  mismatch=sorted(k for k in objective_keys if legacy_options.get(k)!=opt.get(k))
  if mismatch: raise ValueError(f'legacy checkpoint objective options differ from original fit job: {mismatch}')
  solver_mismatch=sorted(k for k in FIT.SOLVER_ONLY_OPTIONS if k in legacy_options and legacy_options[k]!=opt.get(k))
  if solver_mismatch: raise ValueError(f'legacy checkpoint explicit solver options differ from original fit job: {solver_mismatch}')
  solved=legacy.get('solved_parameters') or {}; width=len(legacy['variable_order'])
  if not solved or any(not isinstance(v,list) or len(v)!=width for v in solved.values()): raise ValueError('legacy solved prefix/order width mismatch')
  upgraded=copy.deepcopy(legacy); upgraded['pins']=full; upgraded['options']={**opt,**legacy_options}; upgraded['accepted']=False; upgraded['automatic_resume']=False; upgraded['resume_requires_explicit_new_job']=True
  upgraded['upgrade_attestation']={'schema':'autorig.equine-checkpoint-pin-upgrade/1','legacy_checkpoint_sha256':sha(legacy_path),'original_fit_job_sha256':sha(fitjob_path),'request_sha256':sha(a.request),'code_pins':code,'added_pin_keys':sorted(set(full)-set(legacy.get('pins') or {})),'added_solver_defaults':sorted(set(opt)-set(legacy_options)),'prefix_recomputed':False,'objective_changed':False,'clip_created':False}
  a.output.parent.mkdir(parents=True,exist_ok=True)
  if a.output.exists(): raise ValueError('fresh output required')
  q=a.output.with_name(f'.{a.output.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(upgraded,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,a.output); return 0
 except Exception as e: print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
