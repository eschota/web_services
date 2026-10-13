"""Apply fitted source motion in Blender, export derived GLB, and re-read glTF channels."""
from __future__ import annotations
import argparse, hashlib, json, math, os, pathlib, struct, subprocess, sys

COMP={5120:('b',1),5121:('B',1),5122:('h',2),5123:('H',2),5125:('I',4),5126:('f',4)}
WIDTH={'SCALAR':1,'VEC2':2,'VEC3':3,'VEC4':4,'MAT4':16}
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
    p=pathlib.Path(p); p.parent.mkdir(parents=True,exist_ok=True); q=p.with_name(f'.{p.name}.{os.getpid()}.tmp'); q.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n',encoding='utf-8'); os.replace(q,p)
def glb(path):
    raw=path.read_bytes()
    if len(raw)<20 or len(raw)>512*1024*1024 or raw[:4]!=b'glTF' or struct.unpack_from('<I',raw,4)[0]!=2 or struct.unpack_from('<I',raw,8)[0]!=len(raw): raise ValueError('invalid GLB header/size')
    off=12; doc=None; binary=b''
    while off<len(raw):
        if off+8>len(raw): raise ValueError('truncated GLB chunk header')
        n,t=struct.unpack_from('<II',raw,off); off+=8
        if n<0 or off+n>len(raw): raise ValueError('GLB chunk exceeds file bounds')
        data=raw[off:off+n]; off+=n
        if t==0x4E4F534A:
            if doc is not None: raise ValueError('duplicate GLB JSON chunk')
            doc=json.loads(data.rstrip(b' \0'))
        elif t==0x004E4942:
            if binary: raise ValueError('multiple GLB BIN chunks unsupported')
            binary=data
    if not isinstance(doc,dict): raise ValueError('GLB lacks JSON')
    buffers=doc.get('buffers') or []
    if len(buffers)!=1 or buffers[0].get('uri') is not None: raise ValueError('external/multiple glTF buffers unsupported')
    if int(buffers[0].get('byteLength',-1))>len(binary): raise ValueError('declared buffer exceeds BIN chunk')
    return doc,binary
def accessor(doc,buf,index,expected_type):
    if not isinstance(index,int) or index<0 or index>=len(doc.get('accessors') or []): raise ValueError('animation accessor index out of range')
    a=doc['accessors'][index]
    if a.get('sparse'): raise ValueError('sparse animation accessor unsupported')
    if a.get('componentType')!=5126 or a.get('type')!=expected_type or a.get('normalized') is True: raise ValueError(f'animation accessor must be FLOAT {expected_type}')
    views=doc.get('bufferViews') or []; vi=a.get('bufferView')
    if not isinstance(vi,int) or vi<0 or vi>=len(views): raise ValueError('animation bufferView index out of range')
    bv=views[vi]
    if bv.get('buffer',0)!=0: raise ValueError('animation accessor uses unsupported buffer')
    fmt,size=COMP[a['componentType']]; width=WIDTH[a['type']]; packed=size*width; stride=int(bv.get('byteStride',packed)); start=int(bv.get('byteOffset',0))+int(a.get('byteOffset',0)); count=int(a.get('count',-1))
    if count<1 or count>1000000 or stride<packed or stride>4096 or start<0: raise ValueError('unsafe animation accessor shape/stride')
    view_end=int(bv.get('byteOffset',0))+int(bv.get('byteLength',-1)); end=start+(count-1)*stride+packed
    if view_end<start or end>view_end or end>len(buf): raise ValueError('animation accessor exceeds bufferView/BIN bounds')
    out=[]
    for i in range(count):
        row=list(struct.unpack_from('<'+fmt*width,buf,start+i*stride))
        if not all(math.isfinite(float(x)) for x in row): raise ValueError('animation accessor contains non-finite value')
        out.append(row[0] if width==1 else row)
    return out
def reread(path,action):
    doc,buf=glb(path); animations=doc.get('animations') or []
    if len(animations)!=1: raise ValueError(f'derived GLB must contain exactly one animation, got {len(animations)}')
    anim=animations[0]
    if anim.get('name')!=action: raise ValueError(f'animation name {anim.get("name")} != {action}')
    nodes=doc.get('nodes') or []
    if not nodes or len(nodes)>100000: raise ValueError('invalid node inventory')
    parent={}
    for i,n in enumerate(nodes):
        for c in n.get('children',[]):
            if not isinstance(c,int) or c<0 or c>=len(nodes) or c in parent: raise ValueError('invalid/multiple node parent')
            parent[c]=i
    skins=doc.get('skins') or []; joints={j for s in skins for j in s.get('joints',[])}
    if not joints or any(not isinstance(j,int) or j<0 or j>=len(nodes) for j in joints): raise ValueError('invalid/empty skin joints')
    animated=set()
    for ch in anim.get('channels') or []:
        target=ch.get('target') or {}; node=target.get('node')
        if not isinstance(node,int) or node<0 or node>=len(nodes): raise ValueError('invalid animation target node')
        animated.add(node)
    included=set(joints)|animated
    for j in tuple(included):
        seen=set()
        while j in parent:
            j=parent[j]
            if j in seen: raise ValueError('node ancestry cycle')
            seen.add(j); included.add(j)
    skeleton=[]
    for i in sorted(included):
        n=nodes[i]
        if n.get('matrix') is not None: raise ValueError(f'matrix node {i} requires explicit decomposition; rejected')
        tr=n.get('translation',[0,0,0]); rot=n.get('rotation',[0,0,0,1]); scale=n.get('scale',[1,1,1])
        if len(tr)!=3 or len(rot)!=4 or len(scale)!=3 or not all(math.isfinite(float(x)) for x in [*tr,*rot,*scale]): raise ValueError('invalid node TRS')
        skeleton.append({'index':i,'name':n.get('name'),'parent':parent.get(i),'is_skin_joint':i in joints,'is_animated':i in animated,'translation':tr,'rotation_xyzw':rot,'scale':scale})
    channels=[]; seen_channels=set()
    for ch in anim.get('channels') or []:
        target=ch['target']; sampler=anim['samplers'][ch['sampler']]
        path=target.get('path')
        if path not in ('rotation','translation','scale'): raise ValueError(f'unsupported animation target path {path}; morph weights rejected')
        node=target.get('node')
        if not isinstance(node,int) or node not in included: raise ValueError('animation target is outside preserved skeleton ancestry')
        if (node,path) in seen_channels: raise ValueError('duplicate animation channel target/path')
        seen_channels.add((node,path))
        if sampler.get('interpolation','LINEAR') not in ('LINEAR','STEP'): raise ValueError('unsupported glTF interpolation')
        times=accessor(doc,buf,sampler['input'],'SCALAR'); values=accessor(doc,buf,sampler['output'],'VEC4' if path=='rotation' else 'VEC3')
        if len(times)!=len(values) or any(float(times[i])>float(times[i+1]) for i in range(len(times)-1)): raise ValueError('animation timing/value count mismatch or unsorted times')
        if path=='rotation' and any(abs(math.sqrt(sum(float(x)*float(x) for x in q))-1)>1e-3 for q in values): raise ValueError('non-normalized glTF quaternion')
        channels.append({'node':node,'node_name':nodes[node].get('name'),'path':path,'interpolation':sampler.get('interpolation','LINEAR'),'times':times,'values':values})
    if not channels: raise ValueError('derived GLB animation has no channels')
    return {'coordinate_system':'glTF 2.0 right-handed +Y up; node-local TRS','reference_skeleton':skeleton,'channels':channels}
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--job',required=True,type=pathlib.Path); ap.add_argument('--execute',action='store_true'); a=ap.parse_args(); job=rd(a.job)
    try:
        source=pin(job['source_glb'],'source_glb'); fitted=pin(job['fitted_motion'],'fitted_motion'); action=rd(pin(job['action_contract'],'action_contract')); code=pin(job['export_code_zip'],'export_code_zip')
        if action.get('action_id')!=job.get('action_id') or action.get('taxonomy')!='full34' or not isinstance(action.get('loop'),bool): raise ValueError('action contract/timing scope mismatch')
        blender=pathlib.Path(job['blender']).resolve(); work=pathlib.Path(job['work_root']).resolve()
        if not blender.is_file(): raise ValueError('Blender missing')
        result={'schema':'autorig.equine-motion-export-preflight/1','state':'preflight_pass','source_sha256':sha(source),'fitted_sha256':sha(fitted),'code_sha256':sha(code),'action_id':job['action_id'],'next':'execute_opt_in'}
        if not a.execute: print(json.dumps(result,indent=2)); return 0
        if work.exists(): raise ValueError('work_root already exists')
        work.mkdir(parents=True); import zipfile; code_dir=work/'code'; zipfile.ZipFile(code).extractall(code_dir); out=work/'derived'
        before=sha(source); cmd=[str(blender),'--background','--factory-startup','--python',str(code_dir/'animation_fitting/apply_fitted_motion.py'),'--','--source',str(source),'--motion',str(fitted),'--semantic-action-id',job['action_id'],'--output-dir',str(out),'--fps',str(action['fps']),'--target-armature-name',rd(fitted)['transform_contract']['source_armature_name']]
        log=work/'blender-export.log'
        with log.open('wb') as f: r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
        if r.returncode or before!=sha(source): raise RuntimeError(f'Blender export failed or source changed; exit={r.returncode}')
        derived=out/f"{job['action_id']}.glb"; manifest=out/f"{job['action_id']}.animation-manifest.json"
        if not derived.is_file() or not manifest.is_file(): raise RuntimeError('derived GLB/manifest missing')
        channels=reread(derived,job['action_id'])
        candidate={'schema':'autorig.registry34.gltf-source-motion/1','state':'candidate_pending_real_qa','action_id':job['action_id'],'taxonomy':'full34','loop':action['loop'],'source_glb_sha256':before,'fitted_motion_sha256':sha(fitted),'derived_glb':{'path':str(derived),'sha256':sha(derived),'bytes':derived.stat().st_size},'export_manifest_sha256':sha(manifest),'coordinate_contract':channels['coordinate_system'],'reference_skeleton':channels['reference_skeleton'],'channels':channels['channels'],'qa_approved':False,'promotion_allowed':False}
        atomic(work/'gltf-source-motion.json',candidate); print(json.dumps({'state':candidate['state'],'output':str(work/'gltf-source-motion.json')},indent=2)); return 0
    except Exception as e: print(f'ERROR: {e}',file=sys.stderr); return 2
if __name__=='__main__': raise SystemExit(main())
