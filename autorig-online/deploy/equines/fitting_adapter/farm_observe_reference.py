"""Observe an accepted immutable video on a managed farm runtime, without fitting.

Original inputs stay intact. Full masks/depth stay in the farm project for the
next fitting stage; the queue receives diagnostics and an artifact inventory.
This stage does not certify hoof identity, gait quality, or a library release.
"""
from __future__ import annotations
import argparse, hashlib, json, os, pathlib, shutil, subprocess, zipfile

def digest(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()

def main():
    p = argparse.ArgumentParser(); p.add_argument('--inputs', required=True); p.add_argument('--output', required=True)
    a = p.parse_args(); inputs = pathlib.Path(a.inputs); out = pathlib.Path(a.output)
    job = json.loads((inputs/'observe-job.json').read_text(encoding='utf-8'))
    if job['schema'] != 'autorig.equine-observe-job/1': raise ValueError('unsupported job')
    for name, sha in job['input_sha256'].items():
        if digest(inputs/name) != sha: raise ValueError('input changed: '+name)
    receipt = json.loads((inputs/'owner-reference-verdict.json').read_text(encoding='utf-8'))
    verdicts = [x for x in receipt['items'] if x.get('inbox_id') == job['reference_verdict_inbox_id']]
    if len(verdicts) != 1: raise ValueError('exact reference verdict missing')
    v = verdicts[0]
    if v.get('author_kind') != 'user' or v.get('user_id') != 223960353 or v.get('kind') != 'verdict' or 'Принять' not in v.get('text','') or (v.get('about') or {}).get('uid') != job['reference_media_uid']:
        raise ValueError('reference acceptance mismatch')
    managed = pathlib.Path(job['managed_root']).resolve(); work = managed/'jobs'/job['candidate_id']
    work.mkdir(parents=True, exist_ok=False)
    bundle = work/'actionless-bundle'
    with zipfile.ZipFile(inputs/'actionless-bundle.zip') as z:
        for n in z.namelist():
            target = (bundle/n).resolve()
            if bundle not in target.parents: raise ValueError('unsafe archive path')
        z.extractall(bundle)
    if digest(bundle/'immutable_manifest.json') != job['bundle_manifest_sha256']: raise ValueError('bundle changed')
    anchor = json.loads((inputs/'tracker-anchor-map.json').read_text())
    if anchor['bundle_manifest_sha256'] != job['bundle_manifest_sha256']: raise ValueError('anchors changed')
    code_manifest = json.loads((managed/'code/runtime-code-manifest.json').read_text())
    for row in code_manifest['files']:
        if digest(managed/'code'/row['path']) != row['sha256']: raise ValueError('runtime code changed: '+row['path'])
    video = work/'accepted-reference.mp4'; shutil.copyfile(inputs/'reference.mp4',video)
    env = os.environ.copy(); env.update(PYTHONPATH=str(managed/'code'), PYTHONUNBUFFERED='1', PYTHONDONTWRITEBYTECODE='1',
        AUTORIG_FITTING_VDA_ENCODER='vits', AUTORIG_FITTING_VDA_ENCODER_FRAME_CHUNK='1', HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
        HF_HOME=str(managed/'cache/hf'), TORCH_HOME=str(managed/'cache/torch'), TEMP=str(managed/'tmp'), TMP=str(managed/'tmp'))
    (managed/'tmp').mkdir(exist_ok=True)
    python = managed/'venv-py310-cu128/Scripts/python.exe'; obs = work/'observations'
    cmd = [str(python),str(inputs/'farm_tracking_runtime.py'),'--minimum-free-mib',str(job.get('minimum_free_mib',14000)),'--runtime-root',str(managed/'runtime'),
        '--runtime-lock',str(managed/'repos/runtime-lock.v1.json'),'--gpu-lock',str(managed/'equine-tracking.lock'),
        'observe','--video',str(video),'--bundle',str(bundle),'--output-dir',str(obs),'--device','cuda','--with-depth',
        '--ffprobe',str(managed/'ffprobe.exe')]
    if job.get('free_idle_comfy_cache') is True:
        cmd[2:2]=['--free-idle-comfy-cache','--customer-wait-seconds',str(job.get('customer_wait_seconds',0))]
    for row in anchor['tracks']: cmd.extend(['--priority-anchor-id',row['anchor_id']])
    # A generated reference is not assumed cyclic merely because its action is Walk.
    shutil.copyfile(inputs/'observe-job.json',work/'observe-job.json')
    with (work/'observe.log').open('wb') as log:
        result = subprocess.run(cmd,cwd=work,env=env,stdout=log,stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    shutil.copyfile(work/'observe.log',out/'observe.log')
    inventory = [{'path':str(f.relative_to(work)), 'bytes':f.stat().st_size,'sha256':digest(f)} for f in sorted(work.rglob('*')) if f.is_file()]
    report = {'schema':'autorig.equine-observe-receipt/1','candidate_id':job['candidate_id'],'farm_work_root':str(work),
        'exit_code':result.returncode,'source_video_sha256':digest(video),'source_bundle_manifest_sha256':job['bundle_manifest_sha256'],
        'source_reference_media_uid':job['reference_media_uid'],'source_reference_verdict':job['reference_verdict_inbox_id'],
        'artifacts':inventory,'animation_acceptance':False,'promotion_allowed':False}
    (out/'observe-receipt.json').write_text(json.dumps(report,indent=2)+'\n')
    for name in ('observations.json','observations.npz','diagnostics.json','observation_bundle_manifest.json'):
        f=obs/name
        if f.is_file(): shutil.copyfile(f,out/name)
    print(json.dumps(report,ensure_ascii=False))
    return result.returncode

if __name__ == '__main__': raise SystemExit(main())
