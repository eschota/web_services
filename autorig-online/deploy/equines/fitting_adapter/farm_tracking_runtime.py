"""Explicit small-model farm resource adapter; original CLI defaults stay intact."""
from __future__ import annotations
import argparse, json, sys, time, urllib.request

def main():
    outer=argparse.ArgumentParser(add_help=False)
    outer.add_argument('--minimum-free-mib',type=int,default=14000)
    outer.add_argument('--free-idle-comfy-cache',action='store_true')
    outer.add_argument('--customer-wait-seconds',type=int,default=0)
    limits, argv=outer.parse_known_args()
    if not 4096 <= limits.minimum_free_mib <= 24000:
        outer.error('minimum-free-mib must be in [4096,24000]')
    if not 0 <= limits.customer_wait_seconds <= 900:
        outer.error('customer-wait-seconds must be in [0,900]')
    print(json.dumps({'resource_options':{'minimum_free_mib':limits.minimum_free_mib,
        'default_minimum_free_mib':14000,'range':[4096,24000]},
        'scope':'run only; no customer or category defaults'},ensure_ascii=False),flush=True)
    from animation_fitting.tracking_runtime.cli import _parser, _execute
    from animation_fitting.gpu_lease import gpu_lease, require_free_cuda_memory
    args=_parser().parse_args(argv)
    if args.command != 'observe' or args.device != 'cuda': raise ValueError('adapter supports CUDA observe only')
    if not args.gpu_lock: raise ValueError('explicit project GPU lease required')
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
        return _execute(args)

if __name__=='__main__':
    try: raise SystemExit(main())
    except (ValueError,OSError,RuntimeError) as e:
        print(f'ERROR: {e}',file=sys.stderr); raise SystemExit(2)
