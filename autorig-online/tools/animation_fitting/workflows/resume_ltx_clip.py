"""Collect an existing LTX job after a client timeout without resubmitting it."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from animation_fitting.gpu_lease import DEFAULT_GPU_LOCK, gpu_lease
from animation_fitting.workflows.run_ltx_clip import validate_video, write_json_atomic


def resume_run(directory, *, base=None, timeout_seconds=1800,
               ffprobe='ffprobe', lock_path=DEFAULT_GPU_LOCK):
    directory=Path(directory)
    with gpu_lease(lock_path, 'collect-existing-ltx'):
        record=json.loads((directory/'run.json').read_text(encoding='utf-8'))
        if record.get('state') == 'rejected_by_gait_qa':
            # Collecting the render again must not reset a downstream verdict.
            return record
        base=(base or record.get('comfy_url') or 'http://127.0.0.1:8188').rstrip('/')
        graph=json.loads((directory/'workflow.json').read_text(encoding='utf-8'))
        response=record.get('submission_response',{})
        pid=record.get('prompt_id') or (response.get('prompt_id') if not response.get('node_errors') else None)
        if not isinstance(pid,str) or not pid:
            raise ValueError('No recorded prompt ID: recovery never submits a new job')
        record['prompt_id']=pid
        name=graph['save_video']['inputs']['filename_prefix']
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',name):
            raise ValueError('invalid recorded output name')
        def persist():
            write_json_atomic(directory/'run.json',record)
        deadline=time.monotonic()+timeout_seconds
        while time.monotonic()<deadline:
            # Only GET requests. Never retry /prompt after an uncertain submit.
            with urllib.request.urlopen(base+'/history/'+urllib.parse.quote(pid,safe=''),timeout=30) as response:
                history=json.load(response)
            item=history.get(pid)
            if item:
                status=item.get('status',{})
                if status.get('status_str')=='error':
                    record.update(state='failed',error=str(status.get('messages',[]))[-2000:])
                    persist()
                    return record
                if status.get('completed'):
                    outputs=[v for out in item.get('outputs',{}).values()
                             for key in ('images','videos','gifs') for v in out.get(key,[])
                             if str(v.get('filename','')).lower().endswith('.mp4')]
                    if len(outputs)!=1:
                        raise ValueError('expected exactly one MP4 in recorded job')
                    dst=directory/(name+'.mp4')
                    if not dst.exists():
                        query=urllib.parse.urlencode({k:outputs[0][k] for k in ('filename','subfolder','type') if k in outputs[0]})
                        partial=directory/(name+'.mp4.part')
                        with urllib.request.urlopen(base+'/view?'+query,timeout=120) as response, partial.open('xb') as output:
                            shutil.copyfileobj(response,output)
                        validate_video(partial,record['requested_generation_samples'],ffprobe)
                        partial.rename(dst)
                    record['video_probe']=validate_video(dst,record['requested_generation_samples'],ffprobe)
                    record.update(state='rendered_pending_gait_qa',video_sha256=hashlib.sha256(dst.read_bytes()).hexdigest())
                    persist()
                    return record
            time.sleep(5)
        record['state']='pending_after_timeout'
        persist()
        return record


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir',type=Path,required=True)
    p.add_argument('--comfy-url',help='defaults to the worker URL saved with the job')
    p.add_argument('--timeout-min',type=float,default=30)
    p.add_argument('--ffprobe',default='ffprobe')
    p.add_argument('--gpu-lock',type=Path,default=DEFAULT_GPU_LOCK)
    args=p.parse_args()
    if args.timeout_min<=0:
        p.error('timeout must be positive')
    result=resume_run(args.run_dir,base=args.comfy_url,timeout_seconds=args.timeout_min*60,
                      ffprobe=args.ffprobe,lock_path=args.gpu_lock)
    print(json.dumps(result))
    return 0 if result['state']=='rendered_pending_gait_qa' else 2


if __name__=='__main__':
    raise SystemExit(main())
