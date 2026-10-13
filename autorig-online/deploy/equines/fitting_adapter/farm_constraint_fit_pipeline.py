"""Pinned CPU-only RAW2D -> constraints -> single-Walk IK orchestration."""
from __future__ import annotations
import argparse,hashlib,json,math,os,pathlib,subprocess,sys
SCHEMA='autorig.equine-constraint-fit-pipeline-job/1'
def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def rd(p):
 v=json.loads(pathlib.Path(p).read_text(encoding='utf-8'))
 if not isinstance(v,dict): raise ValueError(f'{p} must contain object')
 return v
def pin(row,label):
 p=pathlib.Path(row['path']).resolve()
 if not p.is_file() or sha(p)!=row['sha256']: raise ValueError(f'{label} missing/SHA mismatch')
 return p
def atomic(p,v):
 p=pathlib.Path(p); p.parent.mkdir(parents=True,exist_ok=True)
 if p.exists(): raise ValueError(f'fresh output required: {p}')
 q=p.with_name(f'.{p.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,p)
def timing(ffprobe,video,video_sha):
 cmd=[str(ffprobe),'-v','error','-count_frames','-select_streams','v:0','-show_entries','stream=avg_frame_rate,nb_frames,nb_read_frames,width,height,duration','-of','json',str(video)]
 r=subprocess.run(cmd,capture_output=True,text=True)
 if r.returncode: raise RuntimeError(f'ffprobe failed: {r.stderr[-300:]}')
 streams=json.loads(r.stdout).get('streams') or []
 if len(streams)!=1: raise ValueError('reference video must have exactly one video stream')
 s=streams[0]; count=s.get('nb_read_frames') or s.get('nb_frames')
 if not str(count).isdigit() or int(count)<2: raise ValueError('ffprobe frame count unavailable')
 parts=str(s.get('avg_frame_rate','')).split('/',1)
 try: fps=float(parts[0])/float(parts[1] if len(parts)==2 else 1)
 except Exception as e: raise ValueError('ffprobe fps unavailable') from e
 if not math.isfinite(fps) or fps<=0: raise ValueError('ffprobe fps invalid')
 receipt={'schema':'autorig.video-timing-evidence.v1','source_video_sha256':video_sha,'frame_count':int(count),'fps':fps,'width':int(s['width']),'height':int(s['height']),'duration_seconds':float(s.get('duration') or ((int(count)-1)/fps)),'ffprobe_command':cmd,'retimed':False}
 canonical=json.dumps(receipt,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8'); receipt['receipt_id']=hashlib.sha256(canonical).hexdigest(); return receipt
def preflight(job):
 if job.get('schema')!=SCHEMA: raise ValueError('unsupported pipeline job schema')
 paths={k:pin(job[k],k) for k in ('raw_tracks_meta','raw_tracks_npz','source_video','anchor_map','surface_anchors','fitting_bundle','bundle_manifest','skeleton','ffprobe','raw_observations_module','limb_constraints_module','contact_evidence_module','source_eligibility_module','constraint_fit_module')}
 ffprobe=paths['ffprobe']; modules=pathlib.Path(job['equines_code']).resolve(); out=pathlib.Path(job['output_root']).resolve()
 if paths['raw_observations_module']!=(modules/'equine_raw_observations.py').resolve() or paths['limb_constraints_module']!=(modules/'equine_limb_constraints.py').resolve() or paths['contact_evidence_module']!=(modules/'equine_contact_evidence.py').resolve() or paths['source_eligibility_module']!=pathlib.Path(__file__).resolve().with_name('equine_source_eligibility.py') or paths['constraint_fit_module']!=pathlib.Path(__file__).resolve().with_name('equine_constraint_fit.py'): raise ValueError('pinned executable module paths do not match the staged code layout')
 if out.exists(): raise ValueError('fresh output_root required')
 if job.get('action_id')!='walk_forward' or job.get('loop') is not False: raise ValueError('first pipeline slice is exact non-loop walk_forward candidate')
 if not isinstance(job.get('run_id'),str) or not job['run_id']: raise ValueError('run_id required')
 return paths,ffprobe,modules,out
def execute(job,paths,ffprobe,modules,out):
 sys.path.insert(0,str(modules)); import equine_raw_observations as RAW; import equine_limb_constraints as LC; import equine_contact_evidence as CE
 sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent)); import equine_constraint_fit as FIT; import equine_source_eligibility as ELIG
 out.mkdir(parents=True); receipt=timing(ffprobe,paths['source_video'],sha(paths['source_video'])); atomic(out/'video-timing.json',receipt)
 raw=RAW.convert_raw_measurements(raw_meta_path=paths['raw_tracks_meta'],raw_meta_sha256=sha(paths['raw_tracks_meta']),raw_npz_path=paths['raw_tracks_npz'],raw_npz_sha256=sha(paths['raw_tracks_npz']),source_video_path=paths['source_video'],source_video_sha256=sha(paths['source_video']),source_scope_sha256=job['source_scope_sha256'],fps=receipt['fps'],anchor_map_path=paths['anchor_map'],anchor_map_sha256=sha(paths['anchor_map']),surface_anchors_path=paths['surface_anchors'],surface_anchors_sha256=sha(paths['surface_anchors']),fitting_bundle_path=paths['fitting_bundle'],fitting_bundle_sha256=sha(paths['fitting_bundle']),immutable_manifest_path=paths['bundle_manifest'],immutable_manifest_sha256=sha(paths['bundle_manifest']),video_metadata_receipt=receipt,source_frame_indices=job.get('source_frame_indices'),expected_array_sha256=job.get('expected_array_sha256'))
 RAW.write_raw_measurements(raw,out/'raw-2d.json')
 constraints=LC.build_constraints(observations_path=out/'raw-2d.json',observations_sha256=sha(out/'raw-2d.json'),anchor_map_path=paths['anchor_map'],anchor_map_sha256=sha(paths['anchor_map']),fitting_bundle_path=paths['fitting_bundle'],fitting_bundle_sha256=sha(paths['fitting_bundle']),immutable_manifest_path=paths['bundle_manifest'],immutable_manifest_sha256=sha(paths['bundle_manifest']),skeleton_path=paths['skeleton'],skeleton_sha256=sha(paths['skeleton']),source_video_sha256=sha(paths['source_video']),action='walk_forward',options=job.get('constraint_options'),surface_anchors_path=paths['surface_anchors'],surface_anchors_sha256=sha(paths['surface_anchors']))
 atomic(out/'source-plane-constraints.json',constraints); source_sha=(rd(paths['fitting_bundle']).get('source') or {}).get('sha256')
 contact=CE.build_contact_evidence(constraints_path=out/'source-plane-constraints.json',constraints_sha256=sha(out/'source-plane-constraints.json'),options=job.get('contact_options')); atomic(out/'contact-evidence.json',contact)
 code=pathlib.Path(job['animation_fitting_code']).resolve(); sys.path.insert(0,str(code.parent)); from animation_fitting.rig import load_rig_bundle
 eligibility=ELIG.evaluate_source_eligibility(load_rig_bundle(paths['bundle_manifest'].parent),constraints,constraints_sha256=sha(out/'source-plane-constraints.json'),options=job.get('eligibility_options')); atomic(out/'source-eligibility.json',eligibility)
 profile={'schema':'autorig.equine-constraint-fit-profile/1','scope':'run','run_id':job['run_id'],'accepted':False,'category_default':False,'source_sha256':source_sha,'constraints_sha256':sha(out/'source-plane-constraints.json'),'fit_options':{k:v['value'] for k,v in FIT.options_get(job.get('fit_options')).items()},'claims':{'anatomical_limits':False,'metric_depth':False,'animation_qa':False}}
 atomic(out/'fit-profile.json',profile)
 action={'schema':'autorig.hoof-action-contract-candidate/1','taxonomy':'full34','action_id':'walk_forward','frame_count':receipt['frame_count'],'fps':receipt['fps'],'loop':False,'source_video_sha256':sha(paths['source_video']),'accepted':False,'retimed':False}; atomic(out/'walk-action-contract.json',action)
 fitjob={'schema':FIT.SCHEMA,'action_id':'walk_forward','bundle_manifest':{'path':str(paths['bundle_manifest']),'sha256':sha(paths['bundle_manifest'])},'surface_anchors':{'path':str(paths['surface_anchors']),'sha256':sha(paths['surface_anchors'])},'constraints':{'path':str(out/'source-plane-constraints.json'),'sha256':sha(out/'source-plane-constraints.json')},'contact_evidence':{'path':str(out/'contact-evidence.json'),'sha256':sha(out/'contact-evidence.json')},'source_eligibility':{'path':str(out/'source-eligibility.json'),'sha256':sha(out/'source-eligibility.json')},'profile':{'path':str(out/'fit-profile.json'),'sha256':sha(out/'fit-profile.json')},'action_contract':{'path':str(out/'walk-action-contract.json'),'sha256':sha(out/'walk-action-contract.json')},'animation_fitting_code':job['animation_fitting_code'],'output':str(out/'walk-fitted-animation.json'),'options':profile['fit_options']}; atomic(out/'fit-job.json',fitjob)
 p,c,pr,a,opt=FIT.preflight(fitjob); FIT.execute(fitjob,p,c,pr,a,opt)
 atomic(out/'pipeline-receipt.json',{'schema':'autorig.equine-constraint-fit-pipeline-receipt/1','state':'candidate_not_accepted','source_video_sha256':sha(paths['source_video']),'raw_meta_sha256':sha(paths['raw_tracks_meta']),'raw_npz_sha256':sha(paths['raw_tracks_npz']),'surface_anchors_sha256':sha(paths['surface_anchors']),'timing_sha256':sha(out/'video-timing.json'),'raw_2d_sha256':sha(out/'raw-2d.json'),'constraints_sha256':sha(out/'source-plane-constraints.json'),'contact_evidence_sha256':sha(out/'contact-evidence.json'),'source_eligibility_sha256':sha(out/'source-eligibility.json'),'profile_sha256':sha(out/'fit-profile.json'),'fitted_sha256':sha(out/'walk-fitted-animation.json'),'pending':['SOURCE_GEOMETRY_PLANE_CANDIDATE','real_post_retarget_3d_QA','owner_acceptance'],'customer_mounted':False,'published':False})
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--job',required=True,type=pathlib.Path); ap.add_argument('--execute',action='store_true'); a=ap.parse_args()
 try:
  job=rd(a.job); values=preflight(job)
  if not a.execute: print(json.dumps({'state':'preflight_pass','next':'execute_cpu_farm'},indent=2)); return 0
  execute(job,*values); return 0
 except Exception as e: print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
