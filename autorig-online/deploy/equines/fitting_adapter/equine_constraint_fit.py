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
def gltf_to_blender(v): return [float(v[0]),-float(v[2]),float(v[1])]
def preflight(job):
 if job.get('schema')!=SCHEMA: raise ValueError('unsupported job schema')
 paths={k:pin(job[k],k) for k in ('bundle_manifest','surface_anchors','constraints','contact_evidence','profile','action_contract')}
 c,p,a,ce=rd(paths['constraints']),rd(paths['profile']),rd(paths['action_contract']),rd(paths['contact_evidence'])
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
 profile_options=p.get('fit_options')
 if not isinstance(profile_options,dict): raise ValueError('profile must declare exact fit_options')
 opt={k:v['value'] for k,v in options_get(profile_options).items()}
 if job.get('options') is not None:
  requested={k:v['value'] for k,v in options_get(job['options']).items()}
  if requested!=opt: raise ValueError('job options differ from the source/constraints-bound profile')
 return paths,c,p,a,opt
def execute(job,paths,c,p,a,opt):
 farm_identity=farm_execute_guard()
 import numpy as np
 from scipy.optimize import least_squares
 code=pathlib.Path(job['animation_fitting_code']).resolve(); sys.path.insert(0,str(code.parent))
 from animation_fitting.rig import load_rig_bundle
 from animation_fitting.math3d import flatten_matrix,quaternion_xyzw_from_matrix
 rig=load_rig_bundle(paths['bundle_manifest'].parent); cons=c.get('constraints') or {}; contact=rd(paths['contact_evidence']); frame_count=int(a['frame_count']); fps=float(a['fps'])
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
 contact_state={leg:{int(x['frame']):x for x in contact['hooves'][leg]['frames']} for leg in LEGS}
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
 observed={}; previous=np.zeros(n_total); metrics={}; W=int(opt['window_frames']); tol=opt['penetration_tolerance_height_ratio']*height
 for start in range(0,frame_count,W):
  stop=min(frame_count,start+W); count=stop-start
  if time.monotonic()-began>opt['max_fit_seconds']: raise TimeoutError(f'fit exceeded max_fit_seconds={opt["max_fit_seconds"]}')
  print(json.dumps({'stage':'fit','frame':start,'frames':frame_count,'window_stop':stop,'elapsed_seconds':round(time.monotonic()-began,2)}),flush=True)
  x0=np.tile(previous,count); blo=np.tile(lo,count); bhi=np.tile(hi,count)
  def state(x,wi):
   xx=x[wi*n_total:(wi+1)*n_total]; e={bone:np.eye(3)[var_axis[bone]]*xx[i] for i,bone in enumerate(variables)}; root=np.zeros(3) if not root_enabled else forward_axis*xx[n_joint]+up_axis*xx[n_joint+1]; world,_=rig.forward_kinematics(root,np.zeros(3),e); return xx,root,rig.anchor_positions(world)
  boundary_pos=None
  if start:
   pe={bone:np.eye(3)[var_axis[bone]]*previous[i] for i,bone in enumerate(variables)}; proot=np.zeros(3) if not root_enabled else forward_axis*previous[n_joint]+up_axis*previous[n_joint+1]; pworld,_=rig.forward_kinematics(proot,np.zeros(3),pe); boundary_pos=rig.anchor_positions(pworld)
  def evaluate(x,tag=False):
   if time.monotonic()-began>opt['max_fit_seconds']: raise TimeoutError(f'fit exceeded max_fit_seconds={opt["max_fit_seconds"]}')
   rows=[]; deps=[]; states=[]
   def add(values,columns):
    vals=np.asarray(values,float).reshape(-1).tolist(); rows.extend(vals)
    if tag: deps.extend([tuple(columns)]*len(vals))
   def cols(wi,indices): return [(wi,index) for index in indices]
   for wi,frame in enumerate(range(start,stop)):
    xx,root,pos=state(x,wi); states.append((xx,pos)); targets=[(doc,t) for doc in chain_docs.values() if (t:=doc['measured'].get(frame)) is not None]; joint_targets=[(leg,bone,doc,t) for leg,docs in joint_docs.items() for bone,doc in docs.items() if (t:=doc['measured'].get(frame)) is not None]; torso_targets=[(role,doc,t) for role,doc in torso_docs.items() if (t:=doc['measured'].get(frame)) is not None]
    for doc,(target,confidence) in targets:
     delta=pos[doc['anchor']]-target; robust=pseudo_huber_residual(delta,opt['hoof_huber_delta_height_ratio']*height); add(robust*math.sqrt(max(confidence,1e-6))*opt['target_scale'],cols(wi,anchor_dependency[doc['anchor']]))
    for _,_,doc,(target,confidence) in joint_targets:
     delta=(pos[doc['anchor']]-target); robust=pseudo_huber_residual(delta,opt['joint_huber_delta_height_ratio']*height); add(robust*math.sqrt(max(confidence,1e-6))*opt['joint_target_weight'],cols(wi,anchor_dependency[doc['anchor']]))
    for _,doc,(target,confidence) in torso_targets: add((pos[doc['anchor']]-target)*math.sqrt(max(confidence,1e-6))*opt['target_scale'],cols(wi,anchor_dependency[doc['anchor']]))
    for index,value in enumerate(xx*opt['rest_weight']): add([value],[(wi,index)])
    prev=previous if wi==0 else states[wi-1][0]
    for index,value in enumerate((xx-prev)*opt['temporal_weight']): add([value],[(wi,index)] if wi==0 else [(wi-1,index),(wi,index)])
    for leg,sole in sole_sets.items():
     pts=np.asarray([pos[k] for k in sole['anchor_ids']]); signed=pts@up-rig.ground_height
     for anchor_id,value in zip(sole['anchor_ids'],np.minimum(0,signed+tol)*opt['ground_nonpenetration_weight']): add([value],cols(wi,anchor_dependency[anchor_id]))
     sole_indices=anchor_indices(sole['anchor_ids'])
     if contact_state[leg][frame]['state']=='OBSERVED_CONTACT': add([float(np.mean(signed))*opt['contact_height_weight']],cols(wi,sole_indices))
     if frame and contact_state[leg][frame]['state']=='OBSERVED_CONTACT' and contact_state[leg][frame-1]['state']=='OBSERVED_CONTACT':
      prior=states[wi-1][1] if wi else boundary_pos
      if prior is not None:
       prevpts=np.asarray([prior[k] for k in sole['anchor_ids']]); delta=np.mean(pts,0)-np.mean(prevpts,0); tangent=delta-up*np.dot(delta,up); add(tangent*fps*opt['planted_velocity_weight'],cols(wi,sole_indices) if wi==0 else cols(wi-1,sole_indices)+cols(wi,sole_indices))
   return (rows,deps) if tag else rows
  seed_rows,dependencies=evaluate(x0,True)
  sparsity=sparsity_from_dependencies(dependencies,n_total,count)
  if sparsity.shape!=(len(seed_rows),len(x0)) or len(dependencies)!=len(seed_rows): raise RuntimeError('internal residual/jacobian row contract mismatch')
  def residual_checked(x):
   rows=evaluate(x)
   if len(rows)!=len(seed_rows): raise RuntimeError('residual row count changed after jacobian contract construction')
   return rows
  result=least_squares(residual_checked,x0,bounds=(blo,bhi),max_nfev=int(opt['max_nfev']),method='trf',jac_sparsity=sparsity)
  if not result.success or not np.isfinite(result.x).all(): raise RuntimeError(f'window {start}:{stop} bounded solve failed: {result.message}')
  for wi,frame in enumerate(range(start,stop)):
   xx=result.x[wi*n_total:(wi+1)*n_total]; _,root,fitted_pos=state(result.x,wi); hits={bone:bool(abs(abs(xx[i])-opt['max_rotation_delta_rad'])<=1e-5) for i,bone in enumerate(variables)}; targets=[(doc,t) for doc in chain_docs.values() if (t:=doc['measured'].get(frame)) is not None]; joint_targets=[(leg,bone,doc,t) for leg,docs in joint_docs.items() for bone,doc in docs.items() if (t:=doc['measured'].get(frame)) is not None]; torso_targets=[(role,doc,t) for role,doc in torso_docs.items() if (t:=doc['measured'].get(frame)) is not None]
   metrics[frame]={}
   for doc,(target,confidence) in targets:
    before=float(np.linalg.norm(rest_pos[doc['anchor']]-target)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-target)); leg=next(k for k,v in chain_docs.items() if v is doc)
    metrics[frame][leg]={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'bound_hit':any(hits[b] for b in doc['chain']),'robust_delta_height_ratio':opt['hoof_huber_delta_height_ratio'],'target_clamped':False}
   for role,doc,(target,confidence) in torso_targets:
    before=float(np.linalg.norm(rest_pos[doc['anchor']]-target)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-target)); metrics[frame][f'torso:{role}']={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'root_forward_bound_hit':bool(root_enabled and abs(abs(xx[n_joint])-hi[n_joint])<=1e-5),'root_up_bound_hit':bool(root_enabled and abs(abs(xx[n_joint+1])-hi[n_joint+1])<=1e-5)}
   for leg,bone,doc,(target,confidence) in joint_targets:
    before=float(np.linalg.norm(rest_pos[doc['anchor']]-target)); after=float(np.linalg.norm(fitted_pos[doc['anchor']]-target)); metrics[frame][f'joint:{leg}:{bone}']={'confidence':confidence,'before_world':before,'after_world':after,'before_height_ratio':before/height,'after_height_ratio':after/height,'source_anchor_id':doc['anchor'],'runtime_track_id':doc['runtime_track_id'],'robust_delta_height_ratio':opt['joint_huber_delta_height_ratio'],'target_clamped':False}
   for leg,sole in sole_sets.items():
    pts=np.asarray([fitted_pos[k] for k in sole['anchor_ids']]); signed=pts@up-rig.ground_height; metrics[frame][f'physics:{leg}']={'contact_state':contact_state[leg][frame]['state'],'min_height_ratio':float(np.min(signed)/height),'mean_height_ratio':float(np.mean(signed)/height),'penetrating_vertices':int(np.sum(signed < -tol))}
   observed[frame]=xx.copy()
  previous=observed[stop-1].copy()
  progress_path=pathlib.Path(job.get('progress_file') or (str(job['output'])+'.progress.json'))
  variable_order=[f'{bone}.{"xyz"[var_axis[bone]]}' for bone in variables]+(['root_translation.forward','root_translation.up'] if root_enabled else [])
  checkpoint(progress_path,{'schema':'autorig.equine-constraint-fit-progress/1','state':'diagnostic_partial_not_clip','accepted':False,'automatic_resume':False,'resume_requires_explicit_new_job':True,'completed_through_frame':stop-1,'objective_schema':'autorig.equine-window-objective/2-exact-lbs-ancestor-sparsity','variable_order':variable_order,'window':{'start':start,'stop':stop,'nfev':int(result.nfev),'cost':float(result.cost)},'pins':{'constraints_sha256':sha(paths['constraints']),'profile_sha256':sha(paths['profile']),'contact_evidence_sha256':sha(paths['contact_evidence']),'surface_anchors_sha256':sha(paths['surface_anchors']),'source_model_sha256':c.get('provenance',{}).get('source_model_sha256')},'options':opt,'elapsed_seconds':time.monotonic()-began,'solved_parameters':{str(frame):observed[frame].tolist() for frame in sorted(observed)}})
 if not observed: raise ValueError('constraints contain no observed target frame')
 # Missing frames use bounded time interpolation only; provenance stays INFERRED/PENDING.
 keys=sorted(observed); values=[]; provenance=[]
 for frame in range(frame_count):
  if frame in observed: values.append(observed[frame]); provenance.append('SOURCE_PLANE_OBSERVED')
  else:
   left=max([k for k in keys if k<frame],default=keys[0]); right=min([k for k in keys if k>frame],default=keys[-1]); w=0 if right==left else (frame-left)/(right-left); values.append(observed[left]*(1-w)+observed[right]*w); provenance.append('INFERRED_PENDING')
 frames=[]
 for frame,x in enumerate(values):
  e={bone:np.eye(3)[var_axis[bone]]*x[i] for i,bone in enumerate(variables)}; root=np.zeros(3) if not root_enabled else forward_axis*x[n_joint]+up_axis*x[n_joint+1]; _,local=rig.forward_kinematics(root,np.zeros(3),e)
  bones={name:{'parent':rig.bones[name].parent,'local_matrix':flatten_matrix(local[name]),'local_translation':[float(v) for v in local[name][:3,3]],'local_rotation_xyzw':quaternion_xyzw_from_matrix(local[name])} for name in rig.bone_order}
  by_leg={leg:('SOURCE_PLANE_OBSERVED' if frame in doc['measured'] else 'INFERRED_PENDING') for leg,doc in chain_docs.items()}
  whole='SOURCE_PLANE_OBSERVED' if all(v=='SOURCE_PLANE_OBSERVED' for v in by_leg.values()) else 'INFERRED_PENDING'
  torso_by_role={role:('SOURCE_PLANE_OBSERVED' if frame in doc['measured'] else 'INFERRED_PENDING') for role,doc in torso_docs.items()}
  joint_evidence={f'{leg}:{bone}':('SOURCE_PLANE_OBSERVED' if frame in doc['measured'] else 'INFERRED_PENDING') for leg,docs in joint_docs.items() for bone,doc in docs.items()}
  frames.append({'frame':frame,'time_seconds':frame/fps,'root_translation':root.tolist(),'root_rotation_rotvec':[0,0,0],'evidence':whole,'evidence_by_leg':by_leg,'evidence_by_joint':joint_evidence,'root_translation_evidence':('SOURCE_TORSO_OBSERVED' if torso_by_role and all(v=='SOURCE_PLANE_OBSERVED' for v in torso_by_role.values()) else 'INFERRED_PENDING' if root_enabled else 'ROOT_FIXED_NO_TORSO_EVIDENCE'),'evidence_by_torso':torso_by_role,'target_residuals':metrics.get(frame,{}),'bones':bones})
 dofs=[f'{b}.{"xyz"[var_axis[b]]}' for b in variables]+(['root_translation.forward','root_translation.up'] if root_enabled else [])
 coverage={'toe_targets':{leg:len(doc['measured']) for leg,doc in chain_docs.items()},'joint_targets':{leg:{bone:len(doc['measured']) for bone,doc in docs.items()} for leg,docs in joint_docs.items()},'contact_states':{leg:{state:sum(1 for row in contact_state[leg].values() if row['state']==state) for state in ('OBSERVED_CONTACT','OBSERVED_SWING','PENDING')} for leg in LEGS},'four_leg_toe_observed_frames':sum(1 for frame in range(frame_count) if all(frame in chain_docs[leg]['measured'] for leg in LEGS))}
 output={'schema':'autorig-fitted-animation.v1','state':'walk_engineering_candidate_not_accepted','rig_bundle':{'path':str(rig.metadata_path),'sha256':rig.metadata_sha256},'observations':{'path':str(paths['constraints']),'sha256':sha(paths['constraints']),'provenance':c.get('provenance')},'frame_count':frame_count,'fps':fps,'duration_seconds':(frame_count-1)/fps,'loop':bool(a.get('loop')),'transform_contract':{'schema':'autorig-fitted-transform-contract.v1','source_armature_name':rig.armature_name,'source_armature_world_matrix':flatten_matrix(rig.armature_world),'root_local_matrix_space':'WORLD','child_local_matrix_space':'PARENT_BONE','rotation_channel':'QUATERNION','scale_animation':False,'translation_policy':{'mode':'root_only','bones':[n for n in rig.bone_order if rig.bones[n].parent is None]}},'active_bones':variables,'degrees_of_freedom':dofs,'config':{'engineering_search_options':opt,'profile_sha256':sha(paths['profile']),'constraints_sha256':sha(paths['constraints']),'contact_evidence_sha256':sha(paths['contact_evidence']),'surface_anchors_sha256':sha(paths['surface_anchors']),'sole_sets':sole_sets,'ground_plane':{'normal':up.tolist(),'height':rig.ground_height,'provenance':'SOURCE_GEOMETRY_PLANE_CANDIDATE','video_ground_observed':False},'anatomical_limits_claimed':False},'optimizer':{'kind':'bounded_window_scipy_least_squares','farm_identity':farm_identity,'window_frames':W,'observed_frames':keys,'inferred_frames':[i for i,x in enumerate(provenance) if x!='SOURCE_PLANE_OBSERVED'],'source_height':height,'residual_metric':'hoof/joint/torso/sole physics in immutable bundle world and source-height units','root_translation_enabled':root_enabled,'root_rotation_enabled':False,'elapsed_seconds':time.monotonic()-began,'measurement_coverage':coverage},'qa':{'decision':None,'animation_quality_approved':False,'assumptions':c.get('assumptions'),'pending':sorted(set(c.get('pending') or [])|set(contact.get('pending') or [])|({'four_leg_measured_gait_not_demonstrated'} if coverage['four_leg_toe_observed_frames']==0 else set())|{'SOURCE_GEOMETRY_PLANE_CANDIDATE','real_post_retarget_3d_QA'})},'frames':frames}
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
