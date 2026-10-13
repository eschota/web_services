"""Preflight-first farm orchestration for one immutable reference-video fit.

Default mode only reports blockers. `--execute` runs official observation and
fit CLIs. `--finalize` requires a separate hash-bound PASS QA receipt before it
writes a registry-34 source-motion candidate. It never promotes a library.
"""
from __future__ import annotations
import argparse, hashlib, json, math, os, subprocess, sys, time
from pathlib import Path

JOB='autorig.equine-farm-fit-job/1'; OUT='autorig.registry34.source-motion-candidate/1'
FIT_FIELDS={'reprojection_weight','depth_weight','silhouette_weight','joint_rest_weight','joint_velocity_weight','joint_acceleration_weight','root_velocity_weight','root_acceleration_weight','contact_height_weight','contact_slide_weight','loop_pose_weight','loop_velocity_weight','robust_loss','f_scale','max_nfev','max_variables','allow_unbounded_joints','active_bones'}
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rd(p):
    v=json.loads(Path(p).read_text(encoding='utf-8'))
    if not isinstance(v,dict): raise ValueError(f'{p} must contain an object')
    return v
def pin(row,label):
    p=Path(row['path']).resolve()
    if not p.is_file() or sha(p)!=row['sha256']: raise ValueError(f'{label} missing or SHA mismatch')
    return p
def repo_pin(row,label):
    p=Path(row['path']).resolve()
    if not p.is_dir(): raise ValueError(f'{label} repository missing')
    got=subprocess.run(['git','-C',str(p),'rev-parse','HEAD'],capture_output=True,text=True).stdout.strip().lower()
    if got!=str(row.get('commit') or '').lower(): raise ValueError(f'{label} commit mismatch')
    license_path=(p/row['license']).resolve()
    if p not in license_path.parents or not license_path.is_file() or sha(license_path)!=row['license_sha256']:
        raise ValueError(f'{label} license proof mismatch')
    return p
def atomic(p,v):
    p=Path(p); p.parent.mkdir(parents=True,exist_ok=True); q=p.with_name(f'.{p.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,p)
def progress(work,stage,state,detail=''):
    p=work/'progress.json'; d=rd(p) if p.exists() else {'schema':'autorig.equine-farm-fit-progress/1','stages':[]}
    d['stages'].append({'at':time.time(),'stage':stage,'state':state,'detail':detail}); atomic(p,d)
def run(cmd,work,stage):
    progress(work,stage,'running',' '.join(map(str,cmd))); log=work/f'{stage}.log'
    with log.open('wb') as f: r=subprocess.run(list(map(str,cmd)),stdout=f,stderr=subprocess.STDOUT)
    if r.returncode: progress(work,stage,'failed',f'exit {r.returncode}; {log}'); raise RuntimeError(f'{stage} failed: {r.returncode}')
    progress(work,stage,'done',str(log))
def preflight(job):
    if job.get('schema')!=JOB: raise ValueError('unsupported job schema')
    paths={k:pin(job[k],k) for k in ('bundle_manifest','anchor_map','video','joint_profile','fit_config','action_contract','runtime_lock')}
    bundle=paths['bundle_manifest'].parent; meta=rd(bundle/'fitting_bundle.json'); anchors=rd(paths['anchor_map']); profile=rd(paths['joint_profile']); action=rd(paths['action_contract'])
    if action.get('action_id')!=job.get('action_id') or action.get('taxonomy')!='full34': raise ValueError('action is not the requested full34 contract')
    for k in ('frame_count','fps','loop'):
        if k not in action: raise ValueError(f'action contract lacks {k}')
    if profile.get('source_sha256')!=(meta.get('source') or {}).get('sha256'): raise ValueError('joint profile/source bundle mismatch')
    if profile.get('state') not in ('candidate','candidate_ready_for_bounded_fit_only'): raise ValueError('joint profile is not run-scoped candidate evidence')
    ids={x.get('track_id') for x in anchors.get('tracks') or []}; need={'fore_left.hoof','fore_right.hoof','hind_left.hoof','hind_right.hoof','head','tail'}
    if not need<=ids: raise ValueError(f'anchor map missing {sorted(need-ids)}')
    cam=(meta.get('camera') or {}).get('camera_z_contract') or {}
    if cam.get('mode')!='positive_camera_z': raise ValueError('bundle has no calibrated camera-Z contract')
    repos=job.get('runtime_repos') or {}; models=job.get('model_checkpoints') or {}
    if not repos or not models: raise ValueError('runtime repo/checkpoint proofs are required')
    for name,row in repos.items(): repo_pin(row,f'runtime_repo:{name}')
    for name,row in models.items(): pin(row,f'checkpoint:{name}')
    return paths,bundle,anchors,profile,action
def existing_observations(job,paths,bundle,anchors):
    row=job.get('existing_observations')
    if row is None: return None
    obs=pin(row,'existing_observations'); manifest=pin(job['existing_observation_manifest'],'existing_observation_manifest'); root=manifest.parent; md=rd(manifest)
    if md.get('schema')!='autorig-tracking-observation-bundle.v1': raise ValueError('unsupported observation manifest schema')
    covered=set()
    for item in md.get('files') or []:
        rel=Path(item.get('path',''))
        if rel.is_absolute() or '..' in rel.parts: raise ValueError('observation manifest path escapes bundle')
        p=(root/rel).resolve()
        if root.resolve() not in p.parents or not p.is_file() or p.stat().st_size!=item.get('bytes') or sha(p)!=item.get('sha256'): raise ValueError(f'observation artifact mismatch: {rel}')
        covered.add(rel.as_posix())
    if obs.resolve().parent!=root.resolve() or obs.name not in covered or 'diagnostics.json' not in covered: raise ValueError('observation manifest does not cover observations/diagnostics')
    payload=rd(obs); diagnostics=rd(root/'diagnostics.json'); prov=payload.get('provenance') or {}; bundle_meta=rd(bundle/'fitting_bundle.json')
    if diagnostics.get('decision')!='accepted_observations' or diagnostics.get('animation_quality_approved') is not False: raise ValueError('observations diagnostics are not accepted non-animation-QA evidence')
    if prov.get('source_video_sha256')!=sha(paths['video']): raise ValueError('observations video SHA mismatch')
    if prov.get('bundle_sha256')!=sha(bundle/'fitting_bundle.json') or prov.get('immutable_manifest_sha256')!=sha(bundle/'immutable_manifest.json'): raise ValueError('observations original bundle identity mismatch')
    if prov.get('relative_depth_contract')!='calibrated_to_camera_z_from_immutable_actionless_reference' or not isinstance(prov.get('camera_z_calibration'),dict): raise ValueError('observations lack valid camera-Z calibration evidence')
    depth=payload.get('depth') or []
    if not depth or any(x.get('mode')!='camera_z' for x in depth): raise ValueError('observations do not expose calibrated camera_z frames')
    required={x['anchor_id'] for x in anchors['tracks']}; observed={x.get('anchor_id') for x in payload.get('tracks') or []}
    if not required<=observed: raise ValueError(f'observations miss required anchors: {sorted(required-observed)}')
    return obs
def merged_fit_config(paths,profile,work):
    base=rd(paths['fit_config'])
    if base.get('schema')!='autorig-fitting-config.v1': raise ValueError('fit config schema mismatch')
    unknown=set(base)-({'schema'}|FIT_FIELDS)
    if unknown: raise ValueError(f'unsupported fit config fields: {sorted(unknown)}')
    overrides=profile.get('fitting_config_overrides')
    if overrides is None:
        if profile.get('regularization'): raise ValueError('legacy joint_profile.regularization names do not map to FittingConfig; provide exact fitting_config_overrides')
        overrides={}
    if not isinstance(overrides,dict) or set(overrides)-FIT_FIELDS: raise ValueError(f'unknown joint profile FittingConfig fields: {sorted(set(overrides or {})-FIT_FIELDS)}')
    merged={**base,**overrides,'schema':'autorig-fitting-config.v1'}
    if merged.get('allow_unbounded_joints') is not False: raise ValueError('allow_unbounded_joints must remain false')
    for name,value in merged.items():
        if name.endswith('_weight') and (isinstance(value,bool) or not math.isfinite(float(value)) or float(value)<0): raise ValueError(f'{name} must be finite and non-negative')
    if merged.get('robust_loss') not in {'linear','soft_l1','huber','cauchy','arctan'}: raise ValueError('unsupported robust_loss')
    if not math.isfinite(float(merged.get('f_scale',0))) or float(merged.get('f_scale',0))<=0: raise ValueError('f_scale must be finite positive')
    for name in ('max_nfev','max_variables'):
        if isinstance(merged.get(name),bool) or not isinstance(merged.get(name),int) or merged[name]<=0: raise ValueError(f'{name} must be positive integer')
    active=merged.get('active_bones')
    if active is not None and (not isinstance(active,list) or any(not isinstance(x,str) or not x for x in active) or len(active)!=len(set(active))): raise ValueError('active_bones must be unique strings or null')
    atomic(work/'effective-fit-config.json',merged); return work/'effective-fit-config.json'
def materialize_bundle(source,profile,work):
    # Exact supported path: old optimizer reads LIMIT_ROTATION only from skeleton.json.
    import shutil
    out=work/'bounded-bundle'; shutil.copytree(source,out)
    sk=rd(out/'skeleton.json'); limits=profile.get('bones') or {}; seen=set()
    for arm in sk.get('armatures') or []:
        for bone in arm.get('bones') or []:
            if bone.get('name') in limits:
                axes=limits[bone['name']]; bone['joint_limits']=[{'type':'LIMIT_ROTATION','space':'LOCAL','use_limit_x':True,'use_limit_y':True,'use_limit_z':True,'min':[axes[x][0] for x in 'xyz'],'max':[axes[x][1] for x in 'xyz']}]; seen.add(bone['name'])
    if seen!=set(limits): raise ValueError(f'joint profile bones absent from skeleton: {sorted(set(limits)-seen)}')
    atomic(out/'skeleton.json',sk)
    meta=rd(out/'fitting_bundle.json'); data=(out/'skeleton.json').read_bytes(); meta['artifacts']['skeleton'].update(sha256=hashlib.sha256(data).hexdigest(),bytes=len(data)); atomic(out/'fitting_bundle.json',meta)
    manifest=rd(out/'immutable_manifest.json')
    for row in manifest['files']:
        p=out/row['filename']; row.update(sha256=sha(p),bytes=p.stat().st_size)
    manifest['bundle_manifest']={'filename':'fitting_bundle.json','sha256':sha(out/'fitting_bundle.json'),'bytes':(out/'fitting_bundle.json').stat().st_size}
    manifest['bundle_file_count']=len(manifest['files'])
    manifest['bundle_total_bytes']=sum(row['bytes'] for row in manifest['files'])
    atomic(out/'immutable_manifest.json',manifest); return out
def execute(job,paths,bundle,anchors,profile,action,work):
    python=Path(job['python']).resolve(); runtime=Path(job['runtime_root']).resolve()
    observations=existing_observations(job,paths,bundle,anchors)
    bounded=materialize_bundle(bundle,profile,work); fitted=work/'fitted-animation.json'; config=merged_fit_config(paths,profile,work)
    if observations is None:
        contact=pin(job['contact_profile'],'contact_profile')
        obsdir=work/'observations'; cmd=[python,'-m','animation_fitting.tracking_runtime','--runtime-root',runtime,'--runtime-lock',paths['runtime_lock'],'observe','--video',paths['video'],'--bundle',bundle,'--output-dir',obsdir,'--device','cuda','--with-depth','--contact-profile',contact]
        for row in anchors['tracks']: cmd += ['--priority-anchor-id',row['anchor_id']]
        if action['loop']: cmd+=['--loop']
        run(cmd,work,'observe'); observations=obsdir/'observations.json'
        if not observations.is_file(): raise RuntimeError('runtime produced no observations.json')
    else:
        atomic(work/'observations-binding.json',{'schema':'autorig.equine-existing-observations-binding/1','observations_sha256':sha(observations),'original_bundle_sha256':sha(bundle/'fitting_bundle.json'),'original_manifest_sha256':sha(bundle/'immutable_manifest.json'),'bounded_bundle_sha256':sha(bounded/'fitting_bundle.json'),'calibration_artifacts_unchanged':True,'change':'joint limits added to private skeleton only'})
        progress(work,'observe','reused',str(observations))
    fit=[python,'-m','animation_fitting','fit','--bundle',bounded,'--observations',observations,'--config',config,'--output',fitted,'--loop' if action['loop'] else '--one-shot']
    run(fit,work,'fit'); progress(work,'qa','pending','external real QA receipt required before finalize'); return fitted
def normalize_quaternion(q):
    n=math.sqrt(sum(float(x)*float(x) for x in q))
    if n<1e-12: raise ValueError('zero quaternion')
    q=[float(x)/n for x in q]; return [-x for x in q] if q[3]<0 else q
def resample_frames(frames,count,fps):
    if len(frames)<2 or count<2: raise ValueError('resampling requires at least two source and target frames')
    src_end=float(frames[-1]['time_seconds']); target=[]
    for i in range(count):
        t=src_end*i/(count-1); hi=next((k for k,x in enumerate(frames) if float(x['time_seconds'])>=t),len(frames)-1); lo=max(0,hi-1)
        a,b=frames[lo],frames[hi]; span=float(b['time_seconds'])-float(a['time_seconds']); w=0.0 if span<=0 else (t-float(a['time_seconds']))/span
        bones={}
        if set(a['bones'])!=set(b['bones']): raise ValueError('bone inventory changes between fitted frames')
        for name in a['bones']:
            aa,bb=a['bones'][name],b['bones'][name]; qa,qb=normalize_quaternion(aa['local_rotation_xyzw']),normalize_quaternion(bb['local_rotation_xyzw'])
            if sum(qa[j]*qb[j] for j in range(4))<0: qb=[-x for x in qb]
            bones[name]={'parent':aa['parent'],'local_translation':[(1-w)*float(aa['local_translation'][j])+w*float(bb['local_translation'][j]) for j in range(3)],'local_rotation_xyzw':normalize_quaternion([(1-w)*qa[j]+w*qb[j] for j in range(4)])}
        target.append({'frame':i,'time_seconds':i/float(fps),'root_translation':[(1-w)*float(a['root_translation'][j])+w*float(b['root_translation'][j]) for j in range(3)],'bones':bones})
    return target
def finalize(job,paths,action,work):
    fitted=work/'fitted-animation.json'; qa=pin(job['qa_receipt'],'qa_receipt'); q=rd(qa)
    if q.get('passed') is not True or q.get('fitted_sha256')!=sha(fitted): raise ValueError('QA receipt is not a hash-bound PASS')
    # Known boundary: fitted channels and skeleton are in Blender world/bone
    # frames. They cannot be labelled registry34/glTF +Y by rotating local
    # quaternions; Blender import may change each bone's local basis. A real
    # apply_fitted_motion -> GLB export and re-read is required first.
    pending={'schema':'autorig.equine-registry-export-pending/1','state':'export_required','action_id':job['action_id'],
             'fitted_sha256':sha(fitted),'qa_receipt_sha256':sha(qa),'source_video_sha256':sha(paths['video']),
             'required_stage':'apply_fitted_motion.py in Blender 4.3, export derived GLB, then derive reference skeleton and absolute local glTF +Y channels from that GLB',
             'reason':'Blender Z-up/root and per-bone local bases are not registry34 glTF +Y bases',
             'source_preservation':'original GLB remains unchanged; export must use a derived output path',
             'promotion_allowed':False}
    atomic(work/'registry-export-pending.json',pending); progress(work,'registry_export','pending',pending['required_stage'])
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--job',required=True,type=Path); ap.add_argument('--execute',action='store_true'); ap.add_argument('--finalize',action='store_true'); a=ap.parse_args(); job=rd(a.job); work=Path(job['work_root']).resolve(); work.mkdir(parents=True,exist_ok=True)
    try:
        paths,bundle,anchors,profile,action=preflight(job)
        if job.get('existing_observations') is not None: existing_observations(job,paths,bundle,anchors)
        merged_fit_config(paths,profile,work)
        progress(work,'preflight','done')
        if not a.execute and not a.finalize: print(json.dumps({'state':'preflight_pass','next':'execute_opt_in'},indent=2)); return 0
        if a.execute: execute(job,paths,bundle,anchors,profile,action,work)
        if a.finalize: finalize(job,paths,action,work)
        return 0
    except Exception as e:
        progress(work,'run','failed',f'{type(e).__name__}: {e}'); print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
