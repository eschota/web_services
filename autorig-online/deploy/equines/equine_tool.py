#!/srv/autorig/venv/bin/python3
"""Read-only hoof-motion diagnostics for the DEV tool registry."""
import argparse, json, os
from pathlib import Path
import equine_motion_contract as qa
import sample_equine_task as sampler

SPEC={'name':'equine-motion','kind':'cli','farm_safe':False,
    'summary':'Source-bound GLB hoof sampling and full34 motion QA. Missing observed identities/ground remain pending; never promotes an animation library.',
    'usage':'equine-motion --describe | options | sample --run-dir PATH --task-id ID --clip Walk --output PATH | qa --input PATH --output PATH',
    'contract':{'in':{'command':'options|sample|qa','input':'project JSON or exact MT run directory'},
                'out':{'pass':'measured boolean, never owner acceptance','pending':'missing evidence','metrics':'numeric diagnostics'},
                'scope':'diagnostics only; use farm for heavy meshes; original source preserved'}}

def path(value):
    p=Path(value).resolve(); root=Path('/srv/autorig/data/motion_transfer').resolve()
    if root not in p.parents: raise ValueError('paths must remain inside the MT project')
    return p

def main():
    p=argparse.ArgumentParser(description=SPEC['summary']); p.add_argument('--describe',action='store_true')
    sub=p.add_subparsers(dest='command'); sub.add_parser('options')
    s=sub.add_parser('sample'); s.add_argument('--run-dir',required=True,type=path); s.add_argument('--task-id',required=True)
    s.add_argument('--clip',required=True); s.add_argument('--output',required=True,type=path)
    s.add_argument('--fps',type=int,default=60); s.add_argument('--frame-cap',type=int,default=600)
    s.add_argument('--min-chain-weight',type=float,default=.7); s.add_argument('--sole-band-height-ratio',type=float,default=.01)
    q=sub.add_parser('qa'); q.add_argument('--input',required=True,type=path); q.add_argument('--output',required=True,type=path)
    a=p.parse_args()
    if a.describe: print(json.dumps(SPEC,ensure_ascii=False)); return 0
    if a.command=='options': print(json.dumps({'sample':sampler.options_get(),'qa':qa.options_get()},ensure_ascii=False)); return 0
    if a.command=='sample':
        result=sampler.sample(a.run_dir,a.task_id,a.clip,fps=a.fps,frame_cap=a.frame_cap,
            min_chain_weight=a.min_chain_weight,sole_band_height_ratio=a.sole_band_height_ratio)
    elif a.command=='qa':
        value=json.loads(a.input.read_text()); result=qa.gate_report(qa.evaluate(value.get('sample',value)))
    else: p.error('command required')
    if a.output.exists(): raise ValueError('choose a fresh output; existing evidence is preserved')
    a.output.parent.mkdir(parents=True,exist_ok=True)
    temp=a.output.with_name(f'.{a.output.name}.{os.getpid()}.tmp')
    temp.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n'); os.replace(temp,a.output)
    print(json.dumps({'output':str(a.output),'owner_acceptance':False,'promotion_allowed':False}))
    return 0

if __name__=='__main__': raise SystemExit(main())
