"""Explicit small-model farm resource adapter; original CLI defaults stay intact."""
from __future__ import annotations
import argparse, json, os, sys, time, urllib.request
from pathlib import Path

def main():
    outer=argparse.ArgumentParser(add_help=False)
    outer.add_argument('--minimum-free-mib',type=int,default=14000)
    outer.add_argument('--free-idle-comfy-cache',action='store_true')
    outer.add_argument('--customer-wait-seconds',type=int,default=0)
    outer.add_argument('--raw-cache-root')
    outer.add_argument('--raw-scope-sha256')
    outer.add_argument('--reuse-raw-only',action='store_true')
    outer.add_argument('--inference-marker')
    limits, argv=outer.parse_known_args()
    if not 4096 <= limits.minimum_free_mib <= 24000:
        outer.error('minimum-free-mib must be in [4096,24000]')
    if not 0 <= limits.customer_wait_seconds <= 900:
        outer.error('customer-wait-seconds must be in [0,900]')
    print(json.dumps({'resource_options':{'minimum_free_mib':limits.minimum_free_mib,
        'default_minimum_free_mib':14000,'range':[4096,24000]},
        'scope':'run only; no customer or category defaults'},ensure_ascii=False),flush=True)
    from animation_fitting.tracking_runtime import cli
    _parser, _execute=cli._parser,cli._execute
    from animation_fitting.gpu_lease import gpu_lease, require_free_cuda_memory
    args=_parser().parse_args(argv)
    if args.command != 'observe' or args.device != 'cuda': raise ValueError('adapter supports CUDA observe only')
    if not args.gpu_lock: raise ValueError('explicit project GPU lease required')
    if limits.raw_cache_root:
        import re
        if not re.fullmatch('[0-9a-f]{64}',limits.raw_scope_sha256 or ''): raise ValueError('exact raw scope SHA required')
        from farm_raw_capture import install
        install(cli,limits.raw_cache_root,limits.raw_scope_sha256,limits.reuse_raw_only,args.priority_anchor_id)
    elif limits.reuse_raw_only: raise ValueError('reuse-only requires pinned raw cache')
    if limits.reuse_raw_only:
        # All three delegates reject a cache miss; no model inference/CUDA API.
        return _execute(args)
    # Backends independently verify pinned weights/repos and release each model
    # before the next one; TAP uses fp16/256 and VDA uses the explicit small model.
    with gpu_lease(args.gpu_lock,args.command):
        if limits.free_idle_comfy_cache:
            # F15's existing render service. Never cancel/delete a prompt or
            # unload a cache while its executor has pending/running work.
            base='http://127.0.0.1:8588'
            def idle():
                with urllib.request.urlopen(base+'/queue',timeout=10) as r: q=json.load(r)
                return not q.get('queue_running') and not q.get('queue_pending')
            deadline=time.monotonic()+limits.customer_wait_seconds
            while not idle():
                if time.monotonic() >= deadline: raise RuntimeError('customer ComfyUI work has priority; idle cache release refused')
                print('Waiting for customer ComfyUI work; GPU inference has not started',flush=True)
                time.sleep(5)
            req=urllib.request.Request(base+'/free',data=b'{"unload_models":true,"free_memory":true}',headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=10) as r: r.read()
            print(json.dumps({'idle_comfy_cache_release_requested':True,'queue_cancelled':False}),flush=True)
            for _ in range(10):
                time.sleep(1)
                if not idle(): raise RuntimeError('customer render arrived; tracking refused')
                try: require_free_cuda_memory(limits.minimum_free_mib); break
                except RuntimeError:
                    if _==9: raise
        require_free_cuda_memory(limits.minimum_free_mib)
        import torch
        torch.cuda.reset_peak_memory_stats()
        marker=Path(limits.inference_marker) if limits.inference_marker else None
        if marker: marker.write_text(json.dumps({'pid':os.getpid(),'state':'gpu_admitted'}))
        try:
            return _execute(args)
        finally:
            if marker and marker.exists(): marker.unlink()
            print(json.dumps({'cuda_peak_allocated_mib':torch.cuda.max_memory_allocated()/1048576,
                'cuda_peak_reserved_mib':torch.cuda.max_memory_reserved()/1048576}),flush=True)

if __name__=='__main__':
    try: raise SystemExit(main())
    except (ValueError,OSError,RuntimeError) as e:
        print(f'ERROR: {e}',file=sys.stderr); raise SystemExit(2)
