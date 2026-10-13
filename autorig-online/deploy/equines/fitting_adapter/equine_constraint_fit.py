"""Bounded CPU fitting of measured sagittal hoof constraints onto an immutable rig.

This first slice fits one linear Walk. Search bounds are engineering options,
not claimed animal anatomy. Missing measurements are marked inferred/pending and
are never mirrored from another leg.
"""
from __future__ import annotations
import argparse, hashlib, json, math, os, pathlib, sys, time

SCHEMA='autorig.equine-constraint-fit-job/1'; CONSTRAINTS='autorig.equine-sagittal-limb-constraints.v1'
SPEC={'name':'equine-constraint-fit','kind':'cli','farm_safe':True,
 'summary':'Bounded CPU Walk fitting from pinned measured source-plane constraints; engineering candidate only, preserves the immutable source.',
 'usage':'equine-constraint-fit --describe | --options-get | --job PROJECT_JOB_JSON [--execute on managed Windows farm only]',
 'contract':{'in':{'job':'source/constraints/profile/action SHA-pinned JSON','scope':'run only; no category defaults'},
             'out':{'metrics':'per observed hoof/frame rest-to-target before and fitted-to-target after, world and source-height units',
                    'state':'candidate_not_accepted; hidden frames remain inferred/pending'},
             'scope':'CPU farm execution only; no GPU, customer mounting, library promotion or owner acceptance'}}
OPTIONS={
 'max_rotation_delta_rad':(.75,.05,1.5,'Symmetric engineering search bound around source rest per selected hinge.'),
 'rest_weight':(.01,0.0,10.0,'L2 regularization toward source rest.'),
 'temporal_weight':(.05,0.0,10.0,'L2 regularization toward the previous observed-frame solution.'),
 'max_nfev':(250,10,5000,'Maximum scipy least_squares evaluations per observed frame.'),
 'target_scale':(1.0,.1,10.0,'Multiplier on confidence-weighted measured hoof residuals.'),
 'max_root_translation_height_ratio':(.5,.01,2.0,'Engineering bound for measured torso-driven root travel along source forward/up.'),
 'max_fit_seconds':(300,10,3600,'Wall-time ceiling for the complete CPU fit.'),
 'solver_ftol':(1e-8,1e-12,1e-3,'SciPy TRF engineering convergence tolerance; not an animation QA threshold.'),
 'solver_xtol':(1e-8,1e-12,1e-3,'SciPy TRF engineering step tolerance; not an animation QA threshold.'),
 'solver_gtol':(1e-8,1e-12,1e-3,'SciPy TRF engineering gradient tolerance; not an animation QA threshold.'),
 'solver_x_scale':(1.0,1e-3,100.0,'SciPy variable scale; numeric only unless a separately reviewed diagnostic adds jac mode.'),
 'window_frames':(5,2,9,'Bounded temporal solve window; never a global sequence solve.'),
 'sole_band_height_ratio':(.015,.001,.10,'Bottom strip above each actual Foot/Toe anchor minimum / source height.'),
 'sole_min_count':(3,1,64,'Minimum immutable sole anchors required per leg.'),
 'penetration_tolerance_height_ratio':(.002,0.0,.05,'Allowed source-geometry-plane penetration / source height.'),
 'ground_nonpenetration_weight':(4.0,0.0,100.0,'One-sided sole nonpenetration residual weight.'),
 'contact_height_weight':(2.0,0.0,100.0,'Observed-contact sole height residual weight.'),
 'planted_velocity_weight':(3.0,0.0,100.0,'Observed consecutive-contact tangential sole velocity residual weight.'),
 'joint_target_weight':(1.0,0.0,10.0,'Confidence-weighted actual Foot/Leg/UpLeg source-plane target weight.'),
 'joint_huber_delta_height_ratio':(.05,.001,1.0,'Pseudo-Huber delta for joint targets / immutable source height; targets are never clamped.'),
 'hoof_huber_delta_height_ratio':(.05,.001,1.0,'Stable pseudo-Huber delta for measured Toe/hoof targets / immutable source height; targets are never clamped.'),
 'limb_torso_vertical_weight':(1.0,0.0,10.0,'Same-frame source-backed limb-to-torso up-axis displacement residual weight.'),
}
LEGS={'LF','RF','LH','RH'}
ALLOWED_FARM_BOXES={'f1','f2','f7','f11','f12','f13','f15','raptor'}
def options_get(given=None):
 out={}; given=dict(given or {}); unknown=set(given)-set(OPTIONS)
 if unknown: raise ValueError(f'unknown options: {sorted(unknown)}')
 for k,(d,lo,hi,doc) in OPTIONS.items():
  v=given.get(k,d)
  if isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(float(v)) or not lo<=float(v)<=hi: raise ValueError(f'{k} outside [{lo},{hi}]')
  if k in ('max_nfev','window_frames','sole_min_count') and (isinstance(v,bool) or not isinstance(v,int)): raise ValueError(f'{k} must be an integer')
  if k=='max_fit_seconds' and (isinstance(v,bool) or not isinstance(v,(int,float))): raise ValueError('max_fit_seconds must be numeric')
  out[k]={'value':int(v) if k in ('max_nfev','window_frames','sole_min_count') else float(v),'default':d,'min':lo,'max':hi,'doc':doc}
 return out
def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def rd(p):
 v=json.loads(pathlib.Path(p).read_text(encoding='utf-8'))
 if not isinstance(v,dict): raise ValueError(f'{p} must be JSON object')
 return v
def pin(row,label):
 p=pathlib.Path(row['path']).resolve()
 if not p.is_file() or sha(p)!=row['sha256']: raise ValueError(f'{label} missing/SHA mismatch')
 return p
def atomic(p,v):
 p=pathlib.Path(p); p.parent.mkdir(parents=True,exist_ok=True)
 if p.exists(): raise ValueError(f'fresh output required: {p}')
 q=p.with_name(f'.{p.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,p)
def checkpoint(path,value):
 p=pathlib.Path(path); p.parent.mkdir(parents=True,exist_ok=True); q=p.with_name(f'.{p.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,p)
OBJECTIVE_SCHEMA='autorig.equine-window-objective/4-source-relative-vertical-eligibility-physics'
SOLVER_ONLY_OPTIONS={'max_nfev','max_fit_seconds','solver_ftol','solver_xtol','solver_gtol','solver_x_scale','resume_failed_window_best_seed'}
def validate_resume(document,variable_order,pins,options):
 if document.get('schema')!='autorig.equine-constraint-fit-progress/1' or document.get('objective_schema')!=OBJECTIVE_SCHEMA: raise ValueError('resume objective schema mismatch')
 if document.get('variable_order')!=variable_order: raise ValueError('resume variable order mismatch')
 if document.get('pins')!=pins: raise ValueError('resume source pins mismatch')
 old=document.get('options') or {}
 objective_keys=(set(old)|set(options))-SOLVER_ONLY_OPTIONS
 mismatch=sorted(k for k in objective_keys if old.get(k)!=options.get(k))
 if mismatch: raise ValueError(f'resume objective options mismatch: {mismatch}')
 solved=document.get('solved_parameters')
 if not isinstance(solved,dict) or not solved: raise ValueError('resume solved prefix missing')
 end=document.get('completed_through_frame')
 if not isinstance(end,int) or sorted(int(k) for k in solved)!=list(range(end+1)): raise ValueError('resume prefix is not contiguous from frame 0')
 return end,solved,{'changed_solver_options':{k:{'old':old.get(k),'new':options.get(k)} for k in SOLVER_ONLY_OPTIONS if old.get(k)!=options.get(k)},'objective_options_identical':True}
def sparsity_from_dependencies(dependencies,n_total,frame_blocks):
 from scipy.sparse import lil_matrix
 matrix=lil_matrix((len(dependencies),n_total*frame_blocks),dtype='int8')
 for row,columns in enumerate(dependencies):
  if not columns: raise ValueError('residual dependency columns cannot be empty')
  for item in columns:
   if isinstance(item,int):
    if item<0 or item>=frame_blocks: raise ValueError('invalid residual dependency block')
    matrix[row,item*n_total:(item+1)*n_total]=1
   else:
    block,index=item
    if not isinstance(block,int) or not isinstance(index,int) or block<0 or block>=frame_blocks or index<0 or index>=n_total: raise ValueError('invalid residual dependency column')
    matrix[row,block*n_total+index]=1
 return matrix.tocsr()
def sole_geometry_stats(anchors,height):
 import numpy as np
 vertex_ids=sorted({int(anchor.vertex_id) for anchor in anchors})
 epsilon=max(float(np.finfo(np.float64).eps)*max(float(height),1.0)*128.0,float(height)*1e-12)
 distinct=[]
 for anchor in anchors:
  point=np.asarray(anchor.rest_world,dtype=np.float64)
  if not any(float(np.linalg.norm(point-other))<=epsilon for other in distinct): distinct.append(point)
 return {'source_vertex_ids':vertex_ids,'distinct_source_vertex_count':len(vertex_ids),'distinct_world_point_count':len(distinct),'world_point_epsilon':epsilon}
def influence_variable_indices(anchor,bones,variable_index,root_indices=()):
 found=set(root_indices)
 for influence in anchor.influences:
  if float(influence.weight)==0: continue
  name=influence.bone
  while name is not None:
   if name in variable_index: found.add(variable_index[name])
   name=bones[name].parent
 return sorted(found)
def joint_constraint_rows(row,chain,anchor_bones,frame_count):
 raw=row.get('joint_constraints') or {}; out={}
 if not isinstance(raw,dict): raise ValueError('joint_constraints must be an object')
 for key,value in raw.items():
  if not isinstance(value,dict) or value.get('source_bone')!=key or key not in chain: raise ValueError(f'joint constraint source bone/chain mismatch: {key}')
  anchor=value.get('source_anchor_id')
  if anchor_bones.get(anchor)!=key: raise ValueError(f'joint constraint anchor/bone mismatch: {key}')
  rows=value.get('constraints') or []
  if any(not isinstance(x,dict) or not isinstance(x.get('frame'),int) or x['frame']<0 or x['frame']>=frame_count for x in rows): raise ValueError(f'joint constraint frame invalid: {key}')
  if len({x['frame'] for x in rows})!=len(rows): raise ValueError(f'joint constraint duplicate frame: {key}')
  out[key]={'anchor':anchor,'rows':rows,'runtime_track_id':value.get('runtime_track_id'),'derivation':value.get('derivation'),'generated':value.get('generated'),'mirrored':value.get('mirrored')}
  if out[key]['derivation']!='derived_from_source_plane_constraint' or out[key]['generated'] is not False or out[key]['mirrored'] is not False: raise ValueError(f'joint constraint provenance invalid: {key}')
 return out
def farm_execute_guard(*,platform_name=None,environ=None):
 platform_name=os.name if platform_name is None else platform_name; environ=os.environ if environ is None else environ
 if platform_name!='nt': raise ValueError('CPU execute is allowed only on a managed Windows farm box')
 home=environ.get('FARM_RUNNER_HOME')
 if home: root=pathlib.Path(home).resolve()
 else:
  local=environ.get('LOCALAPPDATA')
  if not local: raise ValueError('LOCALAPPDATA/FARM_RUNNER_HOME is required for managed farm identity')
  root=(pathlib.Path(local)/'AutoRig'/'farm-runner').resolve()
 identity=root/'box.txt'
 if not identity.is_file(): raise ValueError('managed farm box.txt is missing')
 value=identity.read_text(encoding='utf-8').strip().lower()
 if value not in ALLOWED_FARM_BOXES: raise ValueError(f'farm box is not allowed for heavy fitting: {value or "empty"}')
 declared=environ.get('FARM_BOX')
 if declared is not None and declared.strip().lower()!=value: raise ValueError('FARM_BOX disagrees with managed box.txt')
 return {'box':value,'identity_path':str(identity),'source':'managed_farm_runner_box_file'}
def pseudo_huber_residual(delta,huber):
 import numpy as np
 value=np.asarray(delta,dtype=np.float64); scale=float(huber)
 if not math.isfinite(scale) or scale<=0: raise ValueError('pseudo-Huber delta must be finite positive')
 # Stable equivalent of sign(x)*sqrt(2*d²*(sqrt(1+(x/d)²)-1)); avoids cancellation near zero.
 return math.sqrt(2.0)*value/np.sqrt(np.sqrt(1.0+(value/scale)**2)+1.0)
def source_relative_vertical_residual(source_anchor,source_torso,actual_anchor,actual_torso,up,*,admitted):
 import numpy as np
 if not admitted or source_torso is None or actual_torso is None: return None
 axis=np.asarray(up,dtype=np.float64); axis/=np.linalg.norm(axis)
 return float(np.dot(np.asarray(actual_anchor)-np.asarray(actual_torso),axis)-np.dot(np.asarray(source_anchor)-np.asarray(source_torso),axis))
def gltf_to_blender(v): return [float(v[0]),-float(v[2]),float(v[1])]
def preflight(job):
 if job.get('schema')!=SCHEMA: raise ValueError('unsupported job schema')
 paths={k:pin(job[k],k) for k in ('bundle_manifest','surface_anchors','constraints','contact_evidence','source_eligibility','profile','action_contract')}
 c,p,a,ce,elig=rd(paths['constraints']),rd(paths['profile']),rd(paths['action_contract']),rd(paths['contact_evidence']),rd(paths['source_eligibility'])
 bundle_root=paths['bundle_manifest'].parent; bundle=rd(bundle_root/'fitting_bundle.json'); manifest=rd(paths['bundle_manifest']); surface_sha=sha(paths['surface_anchors'])
 if (bundle.get('artifacts') or {}).get('surface_anchors',{}).get('sha256')!=surface_sha: raise ValueError('surface_anchors SHA differs from RigBundle artifact')
 surface_row=next((x for x in manifest.get('files') or [] if x.get('filename')=='surface_anchors.json'),None)
 if not isinstance(surface_row,dict) or surface_row.get('sha256')!=surface_sha or surface_row.get('bytes')!=paths['surface_anchors'].stat().st_size: raise ValueError('surface_anchors is not the manifest-bound immutable artifact')
 if c.get('schema')!=CONSTRAINTS or c.get('action') not in ('walk','walk_forward'): raise ValueError('first fitter slice accepts measured Walk constraints only')
 if c.get('provenance',{}).get('immutable_manifest_sha256')!=sha(paths['bundle_manifest']): raise ValueError('constraints/bundle manifest mismatch')
 if p.get('source_sha256')!=c.get('provenance',{}).get('source_model_sha256') or p.get('constraints_sha256')!=sha(paths['constraints']): raise ValueError('profile is not bound to source and constraints')
 if p.get('scope')!='run' or p.get('accepted') is not False or p.get('category_default') is not False: raise ValueError('profile must be unaccepted run-scoped candidate')
 if a.get('action_id')!=job.get('action_id') or a.get('taxonomy')!='full34' or a.get('action_id') not in ('walk','walk_forward'): raise ValueError('Walk action contract mismatch')
 if int(a.get('frame_count',0))<2 or float(a.get('fps',0))<=0: raise ValueError('invalid action timing')
 timing=c.get('timing')
 if not isinstance(timing,dict) or timing.get('frame_count')!=a.get('frame_count') or float(timing.get('fps',0))!=float(a.get('fps')): raise ValueError('constraint timing frame_count/fps differs from action contract')
 if set(c.get('constraints') or {})!=LEGS: raise ValueError('constraints must contain exactly LF RF LH RH')
 if ce.get('schema')!='autorig.equine-measured-contact-evidence.v1' or ce.get('state')!='measured_source_contact_evidence' or ce.get('accepted_observations') is not False or ce.get('animation_qa_pass') is not False or set(ce.get('hooves') or {})!=LEGS: raise ValueError('contact evidence must be an unaccepted exact measured LF RF LH RH document')
 if ce.get('timing')!=timing or ce.get('provenance',{}).get('constraints_sha256')!=sha(paths['constraints']): raise ValueError('contact evidence timing/constraints identity mismatch')
 source_indices=timing.get('source_frame_indices')
 if not isinstance(source_indices,list) or len(source_indices)!=int(a['frame_count']): raise ValueError('contact evidence source-frame mapping missing')
 flags=ce.get('provenance') or {}
 if any(flags.get(k) is not False for k in ('opposite_leg_inference','mirroring','hidden_point_interpolation','gait_phase_prior')): raise ValueError('contact evidence inference flags must all be false')
 for leg,row in ce['hooves'].items():
  if row.get('source_anchor_id')!=(c.get('constraints') or {}).get(leg,{}).get('source_anchor_id'): raise ValueError(f'contact evidence source anchor mismatch: {leg}')
  frames=row.get('frames')
  if not isinstance(frames,list) or len(frames)!=int(a['frame_count']): raise ValueError(f'contact evidence frames incomplete: {leg}')
  for frame,item in enumerate(frames):
   if item.get('frame')!=frame or item.get('source_frame')!=source_indices[frame] or item.get('state') not in ('OBSERVED_CONTACT','OBSERVED_SWING','PENDING'): raise ValueError(f'contact evidence frame contract invalid: {leg}:{frame}')
   if item['state']=='PENDING':
    if not isinstance(item.get('reason'),str) or not item['reason']: raise ValueError(f'pending contact lacks reason: {leg}:{frame}')
   elif isinstance(item.get('confidence'),bool) or not isinstance(item.get('confidence'),(int,float)) or not math.isfinite(float(item['confidence'])): raise ValueError(f'observed contact confidence invalid: {leg}:{frame}')
 if elig.get('schema')!='autorig.equine-source-chain-eligibility.v1' or elig.get('state')!='source_bound_target_eligibility' or elig.get('constraints_sha256')!=sha(paths['constraints']) or set(elig.get('targets') or {})!=LEGS: raise ValueError('source eligibility contract/constraints identity mismatch')
 for leg,anchors in elig['targets'].items():
  if not isinstance(anchors,dict): raise ValueError(f'eligibility targets invalid: {leg}')
  constraint_row=(c.get('constraints') or {}).get(leg) or {}; expected={constraint_row.get('source_anchor_id')}|{row.get('source_anchor_id') for row in (constraint_row.get('joint_constraints') or {}).values() if isinstance(row,dict)}; expected.discard(None)
  if not expected.issubset(anchors): raise ValueError(f'eligibility missing original constraint targets: {leg}')
  for anchor_id,row in anchors.items():
   frames=row.get('frames') if isinstance(row,dict) else None
   if not isinstance(frames,list) or len(frames)!=int(a['frame_count']): raise ValueError(f'eligibility frames incomplete: {leg}:{anchor_id}')
   for frame,item in enumerate(frames):
    if item.get('frame')!=frame or item.get('source_frame')!=source_indices[frame] or not isinstance(item.get('observed'),bool) or not isinstance(item.get('eligible'),bool) or not isinstance(item.get('reasons'),list): raise ValueError(f'eligibility frame invalid: {leg}:{anchor_id}:{frame}')
 profile_options=p.get('fit_options')
 if not isinstance(profile_options,dict): raise ValueError('profile must declare exact fit_options')
 opt={k:v['value'] for k,v in options_get(profile_options).items()}
 if job.get('options') is not None:
  requested={k:v['value'] for k,v in options_get(job['options']).items()}
  if requested!=opt: raise ValueError('job options differ from the source/constraints-bound profile')
 if job.get('solver_control_profile') is not None:
  solver_path=pin(job['solver_control_profile'],'solver_control_profile'); solver=rd(solver_path)
  allowed={'schema','scope','run_id','accepted','category_default','base_profile_sha256','solver_options'}
  if set(solver)!=allowed or solver.get('schema')!='autorig.equine-solver-control-profile/1' or solver.get('scope')!='run' or solver.get('run_id')!=p.get('run_id') or solver.get('accepted') is not False or solver.get('category_default') is not False or solver.get('base_profile_sha256')!=sha(paths['profile']): raise ValueError('solver control profile contract/base profile mismatch')
  values=solver.get('solver_options')
  if not isinstance(values,dict) or not values or set(values)-SOLVER_ONLY_OPTIONS: raise ValueError('solver control profile may contain solver-only options')
  seed_mode=values.pop('resume_failed_window_best_seed',False)
  if not isinstance(seed_mode,bool): raise ValueError('resume_failed_window_best_seed must be boolean')
  checked={k:v['value'] for k,v in options_get({**opt,**values}).items()}
  if any(checked[k]!=opt[k] for k in set(opt)-SOLVER_ONLY_OPTIONS): raise ValueError('solver control profile changed objective options')
  opt.update({k:checked[k] for k in values}); opt['resume_failed_window_best_seed']=seed_mode; paths['solver_control_profile']=solver_path
 return paths,c,p,a,opt
def execute(job,paths,c,p,a,opt):
 farm_identity=farm_execute_guard()
 import numpy as np
 from scipy.optimize import least_squares
 code=pathlib.Path(job['animation_fitting_code']).resolve(); sys.path.insert(0,str(code.parent))
 from animation_fitting.rig import load_rig_bundle
 from animation_fitting.math3d import flatten_matrix,quaternion_xyzw_from_matrix
 rig=load_rig_bundle(paths['bundle_manifest'].parent); cons=c.get('constraints') or {}; contact=rd(paths['contact_evidence']); eligibility=rd(paths['source_eligibility']); frame_count=int(a['frame_count']); fps=float(a['fps'])
 eligibility_rows={leg:{anchor_id:{int(x['frame']):x for x in row['frames']} for anchor_id,row in anchors.items()} for leg,anchors in eligibility['targets'].items()}
 def eligibility_for(leg,anchor_id,frame):
  row=(eligibility_rows.get(leg,{}).get(anchor_id) or {}).get(frame)
  return row if row is not None else {'frame':frame,'observed':False,'eligible':False,'reasons':['ELIGIBILITY_TARGET_MISSING']}
 # Each chain supplies its own actual toe anchor and measured source-plane target.
 chain_docs={}; variables=[]; joint_docs={}; began=time.monotonic()
 for leg,row in cons.items():
  chain=[x for x in row.get('source_bone_chain') or [] if x in rig.bones and rig.bones[x].parent is not None][:4]
  if len(chain)<3: raise ValueError(f'{leg} lacks a usable actual source chain')
  anchor=row.get('source_anchor_id')
  if anchor not in rig.anchors: raise ValueError(f'{leg} anchor absent from RigBundle')
  normal=np.asarray(gltf_to_blender((row.get('plane') or {}).get('normal')),float); normal/=np.linalg.norm(normal)
  axes={}
  for bone in chain:
   local=rig.bones[bone].rest_world[:3,:3].T@normal; axis=int(np.argmax(np.abs(local))); axes[bone]=axis
   if bone not in variables: variables.append(bone)
  measured={int(x['frame']):(np.asarray(gltf_to_blender(x['position']),float),float(x['confidence'])) for x in row.get('constraints') or []}
  if any(frame<0 or frame>=frame_count for frame in measured): raise ValueError(f'{leg} constraint frame outside action contract')
  chain_docs[leg]={'chain':chain,'anchor':anchor,'axes':axes,'measured':measured}
  parsed=joint_constraint_rows(row,chain,{k:v.bone for k,v in rig.anchors.items()},frame_count)
  joint_docs[leg]={bone:{**value,'measured':{int(x['frame']):(np.asarray(gltf_to_blender(x['position']),float),float(x['confidence'])) for x in value['rows']}} for bone,value in parsed.items()}
 if not variables: raise ValueError('no measured limb variables')
 var_axis={bone:next(doc['axes'][bone] for doc in chain_docs.values() if bone in doc['axes']) for bone in variables}
 bounds=rig.metadata.get('bounds') or {}; bmin=np.asarray(bounds.get('minimum'),float); bmax=np.asarray(bounds.get('maximum'),float); up=np.asarray(rig.ground_normal,float); height=float(np.max([np.dot(up,[x,y,z]) for x in (bmin[0],bmax[0]) for y in (bmin[1],bmax[1]) for z in (bmin[2],bmax[2])])-np.min([np.dot(up,[x,y,z]) for x in (bmin[0],bmax[0]) for y in (bmin[1],bmax[1]) for z in (bmin[2],bmax[2])]))
 if not math.isfinite(height) or height<=0: raise ValueError('invalid immutable source height')
 rest_world,_=rig.forward_kinematics(np.zeros(3),np.zeros(3),{}); rest_pos=rig.anchor_positions(rest_world)
 sole_sets={}
 for leg,doc in chain_docs.items():
  candidates=[x for x in rig.anchors.values() if x.bone in set(doc['chain'][:2])]
  if not candidates: raise ValueError(f'PENDING sole_geometry:{leg}: no immutable Foot/Toe anchors')
  heights={x.id:float(np.dot(up,x.rest_world)-rig.ground_height) for x in candidates}; floor=min(heights.values()); selected=[x.id for x in candidates if heights[x.id]<=floor+opt['sole_band_height_ratio']*height]
  selected_anchors=[rig.anchors[k] for k in selected]; geometry=sole_geometry_stats(selected_anchors,height)
  if geometry['distinct_source_vertex_count']<int(opt['sole_min_count']): raise ValueError(f'PENDING sole_geometry:{leg}: distinct source vertices {geometry["distinct_source_vertex_count"]} < {int(opt["sole_min_count"])}')
  if len(selected_anchors)>1 and geometry['distinct_world_point_count']<2: raise ValueError(f'PENDING sole_geometry:{leg}: all immutable sole anchors are coincident')
  sole_sets[leg]={'anchor_ids':selected,'bind_heights':{k:heights[k] for k in selected},'bone_names':sorted({rig.anchors[k].bone for k in selected}),**geometry}
 contact_state={leg:{} for leg in LEGS}
 for leg in LEGS:
  toe_anchor=chain_docs[leg]['anchor']
  for row in contact['hooves'][leg]['frames']:
   item=dict(row); eligible=eligibility_for(leg,toe_anchor,int(row['frame']))
   if not (eligible.get('observed') is True and eligible.get('eligible') is True): item={'frame':row['frame'],'source_frame':row['source_frame'],'state':'PENDING','reason':'source_toe_target_ineligible','eligibility_reasons':eligible.get('reasons') or []}
   contact_state[leg][int(row['frame'])]=item
 axes=c.get('axes') or {}; up_axis=np.asarray(gltf_to_blender(axes.get('up')),float); forward_axis=np.asarray(gltf_to_blender(axes.get('forward')),float); up_axis/=np.linalg.norm(up_axis); forward_axis-=up_axis*np.dot(forward_axis,up_axis); forward_axis/=np.linalg.norm(forward_axis)
 torso_docs={}
 raw_torso=c.get('torso_constraints')
 if raw_torso is not None:
  if not isinstance(raw_torso,dict) or not raw_torso: raise ValueError('torso_constraints must be a nonempty object when present')
  for role,row in raw_torso.items():
   anchor=row.get('source_anchor_id') if isinstance(row,dict) else None
   if anchor not in rig.anchors: raise ValueError(f'torso constraint {role} anchor absent from RigBundle')
   measured={int(x['frame']):(np.asarray(gltf_to_blender(x['position']),float),float(x['confidence'])) for x in row.get('constraints') or []}
   if any(frame<0 or frame>=frame_count for frame in measured): raise ValueError(f'torso {role} frame outside action contract')
   torso_docs[role]={'anchor':anchor,'measured':measured}
 torso_order=sorted(torso_docs,key=lambda role:({'Hips':0,'Spine':1,'Spine1':2,'Spine2':3}.get(rig.anchors[torso_docs[role]['anchor']].bone,10),role))
 def torso_reference(frame):
  return next(((role,torso_docs[role],torso_docs[role]['measured'][frame]) for role in torso_order if frame in torso_docs[role]['measured']),None)
 root_enabled=any(doc['measured'] for doc in torso_docs.values()); n_joint=len(variables); n_total=n_joint+(2 if root_enabled else 0)
 variable_index={bone:i for i,bone in enumerate(variables)}; root_indices=list(range(n_joint,n_total))
 def anchor_indices(anchor_ids):
  found=set(root_indices)
  for anchor_id in anchor_ids:
   found.update(influence_variable_indices(rig.anchors[anchor_id],rig.bones,variable_index,root_indices))
  return sorted(found)
 anchor_dependency={anchor_id:anchor_indices([anchor_id]) for anchor_id in rig.anchors}
 lo=np.full(n_total,-opt['max_rotation_delta_rad']); hi=-lo
 if root_enabled:
  lo[n_joint:]=-opt['max_root_translation_height_ratio']*height; hi[n_joint:]=-lo[n_joint:]
 variable_order=[f'{bone}.{"xyz"[var_axis[bone]]}' for bone in variables]+(['root_translation.forward','root_translation.up'] if root_enabled else [])
 current_pins={'bundle_manifest_sha256':sha(paths['bundle_manifest']),'action_contract_sha256':sha(paths['action_contract']),'constraints_sha256':sha(paths['constraints']),'profile_sha256':sha(paths['profile']),'contact_evidence_sha256':sha(paths['contact_evidence']),'source_eligibility_sha256':sha(paths['source_eligibility']),'surface_anchors_sha256':sha(paths['surface_anchors']),'source_model_sha256':c.get('provenance',{}).get('source_model_sha256')}; solver_profile_sha=sha(paths['solver_control_profile']) if 'solver_control_profile' in paths else None
 progress_path=pathlib.Path(job.get('progress_file') or (str(job['output'])+'.progress.json')); observed={}; previous=np.zeros(n_total); metrics={}; W=int(opt['window_frames']); tol=opt['penetration_tolerance_height_ratio']*height; resume_meta=None; resume_document=None; first_frame=0
 if job.get('resume_from') is not None:
  resume_path=pin(job['resume_from'],'resume_from'); resume=rd(resume_path); resume_document=resume; end,solved,resume_meta=validate_resume(resume,variable_order,current_pins,opt)
  if end>=frame_count-1: raise ValueError('resume prefix already covers the full action')
  observed={int(k):np.asarray(v,float) for k,v in solved.items()}
  if any(v.shape!=(n_total,) or not np.isfinite(v).all() for v in observed.values()): raise ValueError('resume parameter vector invalid')
  previous=observed[end].copy(); first_frame=end+1
  if first_frame%W: raise ValueError('resume prefix must end on a completed window boundary')
 for start in range(first_frame,frame_count,W):
  stop=min(frame_count,start+W); count=stop-start
  if time.monotonic()-began>opt['max_fit_seconds']: raise TimeoutError(f'fit exceeded max_fit_seconds={opt["max_fit_seconds"]}')
  print(json.dumps({'stage':'fit','frame':start,'frames':frame_count,'window_stop':stop,'elapsed_seconds':round(time.monotonic()-began,2)}),flush=True)
  x0=np.tile(previous,count); blo=np.tile(lo,count); bhi=np.tile(hi,count)
  if start==first_frame and opt.get('resume_failed_window_best_seed'):
   failed=(resume_document or {}).get('window') or {}; best=failed.get('best_finite_parameters')
   if failed.get('start')!=start or failed.get('stop')!=stop or not isinstance(best,list) or len(best)!=len(x0): raise ValueError('resume checkpoint has no exact failed-window best seed')
   x0=np.asarray(best,float)
   if not np.isfinite(x0).all(): raise ValueError('failed-window best seed is non-finite')
  def state(x,wi):
   xx=x[wi*n_total:(wi+1)*n_total]; e={bone:np.eye(3)[var_axis[bone]]*xx[i] for i,bone in enumerate(variables)}; root=np.zeros(3) if not root_enabled else forward_axis*xx[n_joint]+up_axis*xx[n_joint+1]; world,_=rig.forward_kinematics(root,np.zeros(3),e); return xx,root,rig.anchor_positions(world)
  boundary_pos=None
  if start:
   pe={bone:np.eye(3)[var_axis[bone]]*previous[i] for i,bone in enumerate(variables)}; proot=np.zeros(3) if not root_enabled else forward_axis*previous[n_joint]+up_axis*previous[n_joint+1]; pworld,_=rig.forward_kinematics(proot,np.zeros(3),pe); boundary_pos=rig.anchor_positions(pworld)
  def evaluate(x,tag=False):
   if time.monotonic()-began>opt['max_fit_seconds']: raise TimeoutError(f'fit exceeded max_fit_seconds={opt["max_fit_seconds"]}')
   rows=[]; deps=[]; kinds=[]; states=[]
   def add(values,columns,kind):
    vals=np.asarray(values,float).reshape(-1).tolist(); rows.extend(vals)
    if tag: deps.extend([tuple(columns)]*len(vals)); kinds.extend([kind]*len(vals))
   def cols(wi,indices): return [(wi,index) for index in indices]
   for wi,frame in enumerate(range(start,stop)):
    xx,root,pos=state(x,wi); states.append((xx,pos)); targets=[(leg,doc,t) for leg,doc in chain_docs.items() if (t:=doc['measured'].get(frame)) is not None and eligibility_for(leg,doc['anchor'],frame).get('observed') is True and eligibility_for(leg,doc['anchor'],frame).get('eligible') is True]; joint_targets=[(leg,bone,doc,t) for leg,docs in joint_docs.items() for bone,doc in docs.items() if (t:=doc['measured'].get(frame)) is not None and eligibility_for(leg,doc['anchor'],frame).get('observed') is True and eligibility_for(leg,doc['anchor'],frame).get('eligible') is True]; torso_targets=[(role,doc,t) for role,doc in torso_docs.items() if (t:=doc['measured'].get(frame)) is not None]
    for _,doc,(target,confidence) in targets:
     delta=pos[doc['anchor']]-target; robust=pseudo_huber_residual(delta,opt['hoof_huber_delta_height_ratio']*height); add(robust*math.sqrt(max(confidence,1e-6))*opt['target_scale'],cols(wi,anchor_dependency[doc['anchor']]),'hoof_target')
    for _,_,doc,(target,confidence) in joint_targets:
     delta=(pos[doc['anchor']]-target); robust=pseudo_huber_residual(delta,opt['joint_huber_delta_height_ratio']*height); add(robust*math.sqrt(max(confidence,1e-6))*opt['joint_target_weight'],cols(wi,anchor_dependency[doc['anchor']]),'joint_target')
    for _,doc,(target,confidence) in torso_targets: add((pos[doc['anchor']]-target)*math.sqrt(max(confidence,1e-6))*opt['target_scale'],cols(wi,anchor_dependency[doc['anchor']]),'torso_target')
    torso_ref=torso_reference(frame)
    if torso_ref is not None:
     _,tdoc,(ttarget,tconfidence)=torso_ref
     for leg,doc,(target,confidence) in targets:
      relative=source_relative_vertical_residual(target,ttarget,pos[doc['anchor']],pos[tdoc['anchor']],up,admitted=True); add([relative*math.sqrt(max(min(confidence,tconfidence),1e-6))*opt['limb_torso_vertical_weight']],cols(wi,sorted(set(anchor_dependency[doc['anchor']])|set(anchor_dependency[tdoc['anchor']]))),'limb_torso_vertical')
     for leg,bone,doc,(target,confidence) in joint_targets:
      relative=source_relative_vertical_residual(target,ttarget,pos[doc['anchor']],pos[tdoc['anchor']],up,admitted=True); add([relative*math.sqrt(max(min(confidence,tconfidence),1e-6))*opt['limb_torso_vertical_weight']],cols(wi,sorted(set(anchor_dependency[doc['anchor']])|set(anchor_dependency[tdoc['anchor']]))),'limb_torso_vertical')
    for index,value in enumerate(xx*opt['rest_weight']): add([value],[(wi,index)],'rest')
    prev=previous if wi==0 else states[wi-1][0]
    for index,value in enumerate((xx-prev)*opt['temporal_weight']): add([value],[(wi,index)] if wi==0 else [(wi-1,index),(wi,index)],'temporal')
    for leg,sole in sole_sets.items():
     pts=np.asarray([pos[k] for k in sole['anchor_ids']]); signed=pts@up-rig.ground_height
     for anchor_id,value in zip(sole['anchor_ids'],np.minimum(0,signed+tol)*opt['ground_nonpenetration_weight']): add([value],cols(wi,anchor_dependency[anchor_id]),'sole_nonpenetration')
     sole_indices=anchor_indices(sole['anchor_ids'])
     if contact_state[leg][frame]['state']=='OBSERVED_CONTACT': add([float(np.mean(signed))*opt['contact_height_weight']],cols(wi,sole_indices),'contact_height')
     if frame and contact_state[leg][frame]['state']=='OBSERVED_CONTACT' and contact_state[leg][frame-1]['state']=='OBSERVED_CONTACT':
      prior=states[wi-1][1] if wi else boundary_pos
      if prior is not None:
       prevpts=np.asarray([prior[k] for k in sole['anchor_ids']]); delta=np.mean(pts,0)-np.mean(prevpts,0); tangent=delta-up*np.dot(delta,up); add(tangent*fps*opt['planted_velocity_weight'],cols(wi,sole_indices) if wi==0 else cols(wi-1,sole_indices)+cols(wi,sole_indices),'planted_velocity')
   return (rows,deps,kinds) if tag else rows
  seed_rows,dependencies,row_kinds=evaluate(x0,True)
  sparsity=sparsity_from_dependencies(dependencies,n_total,count)
  if sparsity.shape!=(len(seed_rows),len(x0)) or len(dependencies)!=len(seed_rows): raise RuntimeError('internal residual/jacobian row contract mismatch')
  if job.get('diagnostic_audit') is not None:
   audit=job['diagnostic_audit']
   if audit.get('fitter_code_sha256')!=sha(pathlib.Path(__file__).resolve()): raise ValueError('diagnostic audit fitter code SHA mismatch')
   if resume_document is None or start!=audit.get('window_start') or (resume_document.get('window') or {}).get('start')!=start: raise ValueError('diagnostic audit must target the pinned failed resume window')
   point=(resume_document.get('window') or {}).get('best_finite_parameters')
   if not isinstance(point,list) or len(point)!=len(x0): raise ValueError('failed checkpoint has no compatible best finite window parameters')
   point=np.asarray(point,float); step=float(audit.get('finite_difference_step',1e-6)); tolerance=float(audit.get('unexpected_derivative_tolerance',1e-7))
   if not math.isfinite(step) or step<=0 or not math.isfinite(tolerance) or tolerance<0: raise ValueError('diagnostic finite-difference controls invalid')
   base=np.asarray(evaluate(point),float); declared=sparsity.toarray().astype(bool); max_unexpected=0.0; unexpected=[]
   for column in range(len(point)):
    shifted=point.copy(); shifted[column]+=step; derivative=(np.asarray(evaluate(shifted),float)-base)/step; bad=np.where((~declared[:,column]) & (np.abs(derivative)>tolerance))[0]
    if len(bad):
     value=float(np.max(np.abs(derivative[bad]))); max_unexpected=max(max_unexpected,value); unexpected.append({'column':column,'variable':variable_order[column%n_total],'frame_block':column//n_total,'rows':bad[:50].tolist(),'max_abs_derivative':value})
   costs={kind:float(.5*np.sum(base[np.asarray(row_kinds)==kind]**2)) for kind in sorted(set(row_kinds))}
   report={'schema':'autorig.equine-window-jacobian-audit/1','state':'diagnostic_only_not_clip','accepted':False,'window':{'start':start,'stop':stop},'pins':current_pins,'fitter_code_sha256':audit['fitter_code_sha256'],'objective_schema':OBJECTIVE_SCHEMA,'variable_order':variable_order,'point_source':{'resume_checkpoint_sha256':sha(resume_path),'kind':'best_finite_failed_window'},'finite_difference':{'step':step,'unexpected_tolerance':tolerance,'max_unexpected_derivative':max_unexpected,'unexpected':unexpected,'declared_nonzero_count':int(sparsity.nnz),'rows':len(base),'columns':len(point)},'cost_by_residual_kind':costs,'total_cost':float(.5*np.sum(base**2)),'optimizer_started':False,'output_clip_created':False}
   atomic(pathlib.Path(audit['output']),report); print(json.dumps({'stage':'jacobian_audit','output':audit['output'],'max_unexpected_derivative':max_unexpected,'cost_by_kind':costs}),flush=True); return
  def residual_checked(x):
   rows=evaluate(x)
   if len(rows)!=len(seed_rows): raise RuntimeError('residual row count changed after jacobian contract construction')
   return rows
  result=least_squares(residual_checked,x0,bounds=(blo,bhi),max_nfev=int(opt['max_nfev']),method='trf',jac_sparsity=sparsity,ftol=opt['solver_ftol'],xtol=opt['solver_xtol'],gtol=opt['solver_gtol'],x_scale=opt['solver_x_scale'])
  if not result.success or not np.isfinite(result.x).all():
   checkpoint(progress_path,{'schema':'autorig.equine-constraint-fit-progress/1','state':'diagnostic_failed_window_not_clip','accepted':False,'automatic_resume':False,'resume_requires_explicit_new_job':True,'completed_through_frame':start-1,'objective_schema':OBJECTIVE_SCHEMA,'variable_order':variable_order,'solver_control_profile_sha256':solver_profile_sha,'window':{'start':start,'stop':stop,'success':bool(result.success),'status':int(result.status),'message':str(result.message),'nfev':int(result.nfev),'cost':float(result.cost) if math.isfinite(float(result.cost)) else None,'optimality':float(result.optimality) if math.isfinite(float(result.optimality)) else None,'best_finite_parameters':result.x.tolist() if np.isfinite(result.x).all() else None},'pins':current_pins,'options':opt,'resume_compatibility':resume_meta,'elapsed_seconds':time.monotonic()-began,'solved_parameters':{str(frame):observed[frame].tolist() for frame in sorted(observed)}})
   raise RuntimeError(f'window {start}:{stop} bounded solve failed: {result.message}')
  for wi,frame in enumerate(range(start,stop)):
   observed[frame]=result.x[wi*n_total:(wi+1)*n_total].copy()
  previous=observed[stop-1].copy()
  checkpoint(progress_path,{'schema':'autorig.equine-constraint-fit-progress/1','state':'diagnostic_partial_not_clip','accepted':False,'automatic_resume':False,'resume_requires_explicit_new_job':True,'completed_through_frame':stop-1,'objective_schema':OBJECTIVE_SCHEMA,'variable_order':variable_order,'solver_control_profile_sha256':solver_profile_sha,'window':{'start':start,'stop':stop,'success':True,'status':int(result.status),'message':str(result.message),'nfev':int(result.nfev),'cost':float(result.cost),'optimality':float(result.optimality)},'pins':current_pins,'options':opt,'resume_compatibility':resume_meta,'elapsed_seconds':time.monotonic()-began,'solved_parameters':{str(frame):observed[frame].tolist() for frame in sorted(observed)}})
 if not observed: raise ValueError('constraints contain no observed target frame')
 # Missing frames use bounded time interpolation only; provenance stays INFERRED/PENDING.
 keys=sorted(observed); values=[]; provenance=[]
 for frame in range(frame_count):
  if frame in observed: values.append(observed[frame]); provenance.append('SOURCE_PLANE_OBSERVED')
  else:
   left=max([k for k in keys if k<frame],default=keys[0]); right=min([k for k in keys if k>frame],default=keys[-1]); w=0 if right==left else (frame-left)/(right-left); values.append(observed[left]*(1-w)+observed[right]*w); provenance.append('INFERRED_PENDING')
 def measure_frame(frame,x,previous_centroids):
  e={bone:np.eye(3)[var_axis[bone]]*x[i] for i,bone in enumerate(variables)}; root=np.zeros(3) if not root_enabled else forward_axis*x[n_joint]+up_axis*x[n_joint+1]; world,local=rig.forward_kinematics(root,np.zeros(3),e); fitted_pos=rig.anchor_positions(world); hits={bone:bool(abs(abs(x[i])-opt['max_rotation_delta_rad'])<=1e-5) for i,bone in enumerate(variables)}; result={}
  torso_ref=torso_reference(frame)
  for leg,doc in chain_docs.items():
   target=doc['measured'].get(frame)
   if target is not None:
    point,confidence=target; before=float(np.linalg.norm(rest_pos[doc['anchor']]-point)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-point)); eligible=eligibility_for(leg,doc['anchor'],frame); result[leg]={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'bound_hit':any(hits[b] for b in doc['chain']),'robust_delta_height_ratio':opt['hoof_huber_delta_height_ratio'],'target_clamped':False,'admitted_to_objective':bool(eligible.get('observed') and eligible.get('eligible')),'eligibility_reasons':eligible.get('reasons') or []}
    if torso_ref is not None:
     _,tdoc,(ttarget,_)=torso_ref; result[leg].update(source_torso_relative_up=float(np.dot(point-ttarget,up)),rest_torso_relative_up=float(np.dot(rest_pos[doc['anchor']]-rest_pos[tdoc['anchor']],up)),fitted_torso_relative_up=float(np.dot(fitted_pos[doc['anchor']]-fitted_pos[tdoc['anchor']],up)))
  for role,doc in torso_docs.items():
   target=doc['measured'].get(frame)
   if target is not None:
    point,confidence=target; before=float(np.linalg.norm(rest_pos[doc['anchor']]-point)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-point)); result[f'torso:{role}']={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'root_forward_bound_hit':bool(root_enabled and abs(abs(x[n_joint])-hi[n_joint])<=1e-5),'root_up_bound_hit':bool(root_enabled and abs(abs(x[n_joint+1])-hi[n_joint+1])<=1e-5)}
  for leg,docs in joint_docs.items():
   for bone,doc in docs.items():
    target=doc['measured'].get(frame)
    if target is not None:
     point,confidence=target; before=float(np.linalg.norm(rest_pos[doc['anchor']]-point)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-point)); eligible=eligibility_for(leg,doc['anchor'],frame); key=f'joint:{leg}:{bone}'; result[key]={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'source_anchor_id':doc['anchor'],'runtime_track_id':doc['runtime_track_id'],'robust_delta_height_ratio':opt['joint_huber_delta_height_ratio'],'target_clamped':False,'admitted_to_objective':bool(eligible.get('observed') and eligible.get('eligible')),'eligibility_reasons':eligible.get('reasons') or []}
     if torso_ref is not None:
      _,tdoc,(ttarget,_)=torso_ref; result[key].update(source_torso_relative_up=float(np.dot(point-ttarget,up)),rest_torso_relative_up=float(np.dot(rest_pos[doc['anchor']]-rest_pos[tdoc['anchor']],up)),fitted_torso_relative_up=float(np.dot(fitted_pos[doc['anchor']]-fitted_pos[tdoc['anchor']],up)))
  centroids={}
  for leg,sole in sole_sets.items():
   pts=np.asarray([fitted_pos[k] for k in sole['anchor_ids']]); signed=pts@up-rig.ground_height; centroid=np.mean(pts,0); centroids[leg]=centroid; speed=None
   if frame and previous_centroids.get(leg) is not None and contact_state[leg][frame]['state']=='OBSERVED_CONTACT' and contact_state[leg][frame-1]['state']=='OBSERVED_CONTACT':
    delta=centroid-previous_centroids[leg]; speed=float(np.linalg.norm(delta-up*np.dot(delta,up))*fps/height)
   result[f'physics:{leg}']={'contact_state':contact_state[leg][frame]['state'],'min_height_ratio':float(np.min(signed)/height),'mean_height_ratio':float(np.mean(signed)/height),'penetrating_vertices':int(np.sum(signed < -tol)),'planted_tangential_speed_height_per_second':speed}
  return root,local,result,centroids
 frames=[]; previous_centroids={}
 for frame,x in enumerate(values):
  root,local,metrics[frame],previous_centroids=measure_frame(frame,x,previous_centroids)
  bones={name:{'parent':rig.bones[name].parent,'local_matrix':flatten_matrix(local[name]),'local_translation':[float(v) for v in local[name][:3,3]],'local_rotation_xyzw':quaternion_xyzw_from_matrix(local[name])} for name in rig.bone_order}
  by_leg={leg:('SOURCE_PLANE_OBSERVED_ADMITTED' if frame in doc['measured'] and eligibility_for(leg,doc['anchor'],frame).get('eligible') else 'SOURCE_PLANE_INELIGIBLE_DIAGNOSTIC' if frame in doc['measured'] else 'INFERRED_PENDING') for leg,doc in chain_docs.items()}
  whole='SOURCE_PLANE_OBSERVED_ADMITTED' if all(v=='SOURCE_PLANE_OBSERVED_ADMITTED' for v in by_leg.values()) else 'INFERRED_PENDING'
  torso_by_role={role:('SOURCE_PLANE_OBSERVED' if frame in doc['measured'] else 'INFERRED_PENDING') for role,doc in torso_docs.items()}
  joint_evidence={f'{leg}:{bone}':('SOURCE_PLANE_OBSERVED_ADMITTED' if frame in doc['measured'] and eligibility_for(leg,doc['anchor'],frame).get('eligible') else 'SOURCE_PLANE_INELIGIBLE_DIAGNOSTIC' if frame in doc['measured'] else 'INFERRED_PENDING') for leg,docs in joint_docs.items() for bone,doc in docs.items()}
  frames.append({'frame':frame,'time_seconds':frame/fps,'root_translation':root.tolist(),'root_rotation_rotvec':[0,0,0],'evidence':whole,'evidence_by_leg':by_leg,'evidence_by_joint':joint_evidence,'root_translation_evidence':('SOURCE_TORSO_OBSERVED' if torso_by_role and all(v=='SOURCE_PLANE_OBSERVED' for v in torso_by_role.values()) else 'INFERRED_PENDING' if root_enabled else 'ROOT_FIXED_NO_TORSO_EVIDENCE'),'evidence_by_torso':torso_by_role,'target_residuals':metrics.get(frame,{}),'bones':bones})
 dofs=[f'{b}.{"xyz"[var_axis[b]]}' for b in variables]+(['root_translation.forward','root_translation.up'] if root_enabled else [])
 coverage={'toe_targets':{leg:{'measured':len(doc['measured']),'admitted':sum(1 for frame in doc['measured'] if eligibility_for(leg,doc['anchor'],frame).get('eligible'))} for leg,doc in chain_docs.items()},'joint_targets':{leg:{bone:{'measured':len(doc['measured']),'admitted':sum(1 for frame in doc['measured'] if eligibility_for(leg,doc['anchor'],frame).get('eligible'))} for bone,doc in docs.items()} for leg,docs in joint_docs.items()},'contact_states':{leg:{state:sum(1 for row in contact_state[leg].values() if row['state']==state) for state in ('OBSERVED_CONTACT','OBSERVED_SWING','PENDING')} for leg in LEGS},'four_leg_toe_observed_frames':sum(1 for frame in range(frame_count) if all(frame in chain_docs[leg]['measured'] and eligibility_for(leg,chain_docs[leg]['anchor'],frame).get('eligible') for leg in LEGS))}
 output={'schema':'autorig-fitted-animation.v1','state':'walk_engineering_candidate_not_accepted','rig_bundle':{'path':str(rig.metadata_path),'sha256':rig.metadata_sha256},'observations':{'path':str(paths['constraints']),'sha256':sha(paths['constraints']),'provenance':c.get('provenance')},'frame_count':frame_count,'fps':fps,'duration_seconds':(frame_count-1)/fps,'loop':bool(a.get('loop')),'transform_contract':{'schema':'autorig-fitted-transform-contract.v1','source_armature_name':rig.armature_name,'source_armature_world_matrix':flatten_matrix(rig.armature_world),'root_local_matrix_space':'WORLD','child_local_matrix_space':'PARENT_BONE','rotation_channel':'QUATERNION','scale_animation':False,'translation_policy':{'mode':'root_only','bones':[n for n in rig.bone_order if rig.bones[n].parent is None]}},'active_bones':variables,'degrees_of_freedom':dofs,'config':{'engineering_search_options':opt,'profile_sha256':sha(paths['profile']),'solver_control_profile_sha256':solver_profile_sha,'constraints_sha256':sha(paths['constraints']),'contact_evidence_sha256':sha(paths['contact_evidence']),'source_eligibility_sha256':sha(paths['source_eligibility']),'surface_anchors_sha256':sha(paths['surface_anchors']),'sole_sets':sole_sets,'ground_plane':{'normal':up.tolist(),'height':rig.ground_height,'provenance':'SOURCE_GEOMETRY_PLANE_CANDIDATE','video_ground_observed':False},'anatomical_limits_claimed':False},'optimizer':{'kind':'bounded_window_scipy_least_squares','farm_identity':farm_identity,'window_frames':W,'observed_frames':keys,'inferred_frames':[i for i,x in enumerate(provenance) if x!='SOURCE_PLANE_OBSERVED'],'source_height':height,'residual_metric':'admitted hoof/joint + torso/sole physics in immutable bundle world and source-height units','root_translation_enabled':root_enabled,'root_rotation_enabled':False,'elapsed_seconds':time.monotonic()-began,'measurement_coverage':coverage},'qa':{'decision':None,'animation_quality_approved':False,'assumptions':c.get('assumptions'),'pending':sorted(set(c.get('pending') or [])|set(contact.get('pending') or [])|set(eligibility.get('pending') or [])|({'four_leg_measured_gait_not_demonstrated'} if coverage['four_leg_toe_observed_frames']==0 else set())|{'SOURCE_GEOMETRY_PLANE_CANDIDATE','real_post_retarget_3d_QA'})},'frames':frames}
 atomic(pathlib.Path(job['output']),output)
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--job',type=pathlib.Path); ap.add_argument('--execute',action='store_true'); ap.add_argument('--options-get',action='store_true'); ap.add_argument('--describe',action='store_true'); args=ap.parse_args()
 if args.describe: print(json.dumps(SPEC,indent=2)); return 0
 if args.options_get: print(json.dumps(options_get(),indent=2)); return 0
 if args.job is None: ap.error('--job required unless --options-get')
 try:
  job=rd(args.job); paths,c,p,a,opt=preflight(job)
  if not args.execute: print(json.dumps({'state':'preflight_pass','next':'execute_cpu_farm','options':opt},indent=2)); return 0
  if os.name!='nt': raise ValueError('CPU execute requires the managed Windows farm runtime; VPS only supports preflight/options')
  execute(job,paths,c,p,a,opt); return 0
 except Exception as e: print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
