"""Build the code-only full animation_fitting runtime archive."""
from __future__ import annotations
import argparse, hashlib, json, zipfile
from pathlib import Path

def h(data): return hashlib.sha256(data).hexdigest()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--animation-fitting',required=True,type=Path); ap.add_argument('--output',required=True,type=Path)
    a=ap.parse_args(); root=a.animation_fitting.resolve(); files={}
    for p in root.rglob('*.py'):
        rel=p.relative_to(root)
        if 'tests' not in rel.parts and '__pycache__' not in rel.parts: files[f'animation_fitting/{rel.as_posix()}']=p
    for sub in ('schemas','data/semantic_ltx_profiles'):
        for p in (root/sub).rglob('*.json'): files[f'animation_fitting/{p.relative_to(root).as_posix()}']=p
    lock=root/'tracking_runtime/runtime-lock.v1.json'; files['animation_fitting/tracking_runtime/runtime-lock.v1.json']=lock
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(a.output,'x',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        rows=[]
        for name,p in sorted(files.items()):
            data=p.read_bytes(); z.writestr(name,data); rows.append({'path':name,'sha256':h(data),'bytes':len(data)})
        z.writestr('runtime-code-manifest.json',json.dumps({'schema':'autorig.equine-fitting-runtime-code/1','role':'full_fitting_runtime_code_only','contains_models':False,'contains_customer_data':False,'files':rows},indent=2)+'\n')
    print(json.dumps({'path':str(a.output),'sha256':h(a.output.read_bytes()),'bytes':a.output.stat().st_size},indent=2))
if __name__=='__main__': main()
