"""Build the minimal Blender apply_fitted_motion code-only archive."""
from __future__ import annotations
import argparse, ast, hashlib, json, zipfile
from pathlib import Path

def h(b): return hashlib.sha256(b).hexdigest()
def closure(root,seeds):
    seen=set(); todo=list(seeds)
    while todo:
        p=todo.pop()
        if p in seen: continue
        seen.add(p); tree=ast.parse(p.read_text(encoding='utf-8'),str(p))
        for n in ast.walk(tree):
            if isinstance(n,ast.ImportFrom) and n.level==1:
                for name in ([n.module] if n.module else [x.name for x in n.names]):
                    q=root.joinpath(*name.split('.')).with_suffix('.py') if name else None
                    if q and q.is_file() and q not in seen: todo.append(q)
    return seen
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--animation-fitting',required=True,type=Path); ap.add_argument('--output',required=True,type=Path); a=ap.parse_args(); root=a.animation_fitting.resolve()
    init=b'''"""Blender motion-export subset; not the full fitting API."""\nfrom .errors import ContractError, DependencyUnavailableError, OptimizationError\n'''
    files=closure(root,[root/'errors.py',root/'math3d.py',root/'motion_export_contract.py'])
    files.add(root/'apply_fitted_motion.py'); a.output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(a.output,'x',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        z.writestr('animation_fitting/__init__.py',init); rows=[{'path':'animation_fitting/__init__.py','sha256':h(init),'bytes':len(init),'role':'blender_export_subset'}]
        for p in sorted(files):
            b=p.read_bytes(); name=f'animation_fitting/{p.relative_to(root).as_posix()}'; z.writestr(name,b); rows.append({'path':name,'sha256':h(b),'bytes':len(b)})
        m={'schema':'autorig.equine-motion-export-code/1','role':'apply_fitted_motion_only_no_scipy','canonical_init_sha256':h((root/'__init__.py').read_bytes()),'files':rows}; z.writestr('code-manifest.json',json.dumps(m,indent=2)+'\n')
    print(json.dumps({'path':str(a.output),'sha256':h(a.output.read_bytes()),'bytes':a.output.stat().st_size},indent=2))
if __name__=='__main__': main()
