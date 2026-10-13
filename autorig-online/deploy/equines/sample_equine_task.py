#!/usr/bin/env python3
"""Fail-closed sampler of the actual animation and skinned hoof geometry in a GLB."""
from __future__ import annotations

import argparse, hashlib, json, math, struct, sys
from pathlib import Path
import numpy as np

import equine_motion_contract as contract

DT={5120:np.int8,5121:np.uint8,5122:np.int16,5123:np.uint16,5125:np.uint32,5126:np.float32}
NW={"SCALAR":1,"VEC2":2,"VEC3":3,"VEC4":4,"MAT4":16}
LEGS={"LF":("LeftFrontFoot","LeftFrontToe","LeftFrontToe_End"),
      "RF":("RightFrontFoot","RightFrontToe","RightFrontToe_End"),
      "LH":("LeftHindFoot","LeftHindToe","LeftHindToe_End"),
      "RH":("RightHindFoot","RightHindToe","RightHindToe_End")}
OPTION_SPEC={
    "min_chain_weight":(.7,.5,1.0,"Minimum summed weight to the exact Foot/Toe chain for a source sole vertex."),
    "sole_band_height_ratio":(.01,.001,.05,"Lowest bind-world surface band thickness divided by bind model height."),
}

def options_get(options=None):
    supplied=dict(options or {}); unknown=set(supplied)-set(OPTION_SPEC)
    if unknown: raise ValueError("unknown options: "+", ".join(sorted(unknown)))
    out={}
    for name,(default,low,high,doc) in OPTION_SPEC.items():
        value=supplied.get(name,default)
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(float(value)) or not low<=float(value)<=high:
            raise ValueError(f"{name} must be finite in [{low}, {high}]")
        out[name]={"value":float(value),"default":default,"min":low,"max":high,"doc":doc}
    return out

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def glb(path):
    data=Path(path).read_bytes()
    if len(data)>512*1024*1024 or len(data)<20: raise ValueError("invalid/bounded GLB")
    magic,ver,total=struct.unpack_from("<4sII",data)
    if magic!=b"glTF" or ver!=2 or total!=len(data): raise ValueError("invalid GLB2 header")
    off=12; doc=blob=None
    while off<len(data):
        n,k=struct.unpack_from("<II",data,off); off+=8; chunk=data[off:off+n]; off+=n
        if len(chunk)!=n or n%4: raise ValueError("bad GLB chunk")
        if k==0x4e4f534a: doc=json.loads(chunk.rstrip(b" \0\r\n\t"))
        elif k==0x004e4942: blob=chunk
    if doc is None or blob is None or doc.get("extensionsRequired"): raise ValueError("unsupported GLB extensions")
    if len(doc.get("buffers",[]))!=1 or doc["buffers"][0].get("uri") is not None: raise ValueError("embedded buffer required")
    return doc,blob[:int(doc["buffers"][0]["byteLength"])]

def acc(d,b,i):
    a=d["accessors"][i]
    if "sparse" in a or "bufferView" not in a: raise ValueError("sparse/implicit accessor unsupported")
    v=d["bufferViews"][a["bufferView"]]
    if v.get("extensions") or int(v.get("buffer",0))!=0: raise ValueError("compressed/external view unsupported")
    typ=DT.get(a.get("componentType")); width=NW.get(a.get("type"))
    if typ is None or width is None: raise ValueError("unsupported accessor")
    count=int(a["count"]); item=np.dtype(typ).itemsize*width; stride=int(v.get("byteStride",item))
    start=int(v.get("byteOffset",0))+int(a.get("byteOffset",0)); end=start+(count-1)*stride+item if count else start
    if stride<item or end>int(v.get("byteOffset",0))+int(v["byteLength"]) or end>len(b): raise ValueError("accessor bounds")
    out=(np.frombuffer(b,typ,count=count*width,offset=start).reshape(count,width).copy() if stride==item else
         np.ndarray((count,width),typ,buffer=b,offset=start,strides=(stride,np.dtype(typ).itemsize)).copy())
    if a.get("normalized") and not np.issubdtype(out.dtype,np.floating):
        info=np.iinfo(out.dtype); out=out.astype(float); out=np.maximum(out/info.max,-1) if info.min<0 else out/info.max
    if not np.isfinite(out).all(): raise ValueError("nonfinite accessor")
    return out

def qnorm(q):
    q=np.asarray(q,float); n=np.linalg.norm(q)
    if q.shape!=(4,) or n<1e-10: raise ValueError("bad quaternion")
    return q/n

def qmat(q):
    x,y,z,w=qnorm(q); return np.array([[1-2*(y*y+z*z),2*(x*y-z*w),2*(x*z+y*w)],
        [2*(x*y+z*w),1-2*(x*x+z*z),2*(y*z-x*w)],[2*(x*z-y*w),2*(y*z+x*w),1-2*(x*x+y*y)]])

def trs(t,q,s):
    m=np.eye(4); m[:3,:3]=qmat(q)@np.diag(s); m[:3,3]=t; return m

def slerp(a,b,t):
    a,b=qnorm(a),qnorm(b); dot=float(a@b)
    if dot<0: b=-b; dot=-dot
    if dot>.9995: return qnorm(a+t*(b-a))
    th=math.acos(max(-1,min(1,dot))); return (math.sin((1-t)*th)*a+math.sin(t*th)*b)/math.sin(th)

def interp(times,vals,t,rotation=False):
    if t<=times[0]: return vals[0].copy()
    if t>=times[-1]: return vals[-1].copy()
    j=int(np.searchsorted(times,t)); f=(t-times[j-1])/(times[j]-times[j-1])
    return slerp(vals[j-1],vals[j],f) if rotation else vals[j-1]*(1-f)+vals[j]*f

def frame_count(duration,fps,cap):
    """Count endpoint-inclusive frames; absorb only Float32-near-integer interval noise."""
    intervals=float(duration)*int(fps); nearest=round(intervals)
    steps=int(nearest if abs(intervals-nearest)<=1e-5 else math.ceil(intervals))
    return min(int(cap),max(2,steps+1))

def parents(d):
    out={}
    for p,n in enumerate(d.get("nodes",[])):
        for c in n.get("children",[]):
            if c in out: raise ValueError("multiple node parents")
            out[c]=p
    return out

def worlds(d,local,par):
    cache={}
    def one(i,stack=()):
        if i in cache:return cache[i]
        if i in stack:raise ValueError("node cycle")
        cache[i]=local[i] if i not in par else one(par[i],stack+(i,))@local[i]
        return cache[i]
    return [one(i) for i in range(len(local))]

def node_id(d,name):
    ids=[i for i,n in enumerate(d.get("nodes",[])) if n.get("name")==name]
    if len(ids)!=1: raise ValueError(f"required unique node missing: {name}")
    return ids[0]

def first_node_id(d,names):
    for name in names:
        ids=[i for i,n in enumerate(d.get("nodes",[])) if n.get("name")==name]
        if len(ids)>1: raise ValueError(f"required unique node duplicated: {name}")
        if ids: return ids[0],name
    raise ValueError("required node missing: "+" or ".join(names))

def sample(run_dir,task_id,clip,fps=60,frame_cap=600,min_chain_weight=.7,sole_band_height_ratio=.01):
    configured=options_get({"min_chain_weight":min_chain_weight,"sole_band_height_ratio":sole_band_height_ratio})
    min_chain_weight=configured["min_chain_weight"]["value"]; sole_band_height_ratio=configured["sole_band_height_ratio"]["value"]
    run_dir=Path(run_dir); source=run_dir/"rig"/"rigged.glb"; rig_path=run_dir/"rig"/"rig.json"; d,b=glb(source)
    rig_doc=json.loads(rig_path.read_text(encoding="utf-8")) if rig_path.is_file() else {}
    if any(m.get("weights") for m in d.get("meshes",[])): raise ValueError("mesh weights/morphs unsupported")
    nodes=d.get("nodes",[]); par=parents(d); base=[]; base_trs=[]
    for n in nodes:
        if "matrix" in n: raise ValueError("matrix nodes unsupported: explicit TRS required")
        t=np.asarray(n.get("translation",[0,0,0]),float); q=qnorm(n.get("rotation",[0,0,0,1])); s=np.asarray(n.get("scale",[1,1,1]),float)
        if t.shape!=(3,) or s.shape!=(3,) or not np.isfinite(t).all() or not np.isfinite(s).all(): raise ValueError("bad node TRS")
        base_trs.append((t,q,s)); base.append(trs(t,q,s))
    animations=[a for a in d.get("animations",[]) if a.get("name")==clip]
    if len(animations)!=1: raise ValueError("clip must match exactly one animation")
    extras=animations[0].get("extras") or {}
    if not isinstance(extras,dict): raise ValueError("animation extras must be an object")
    loop_extra=extras.get("loop")
    if loop_extra is not None and not isinstance(loop_extra,bool): raise ValueError("animation extras.loop must be boolean")
    loop_value=loop_extra if isinstance(loop_extra,bool) else clip.lower() in contract.LOOPS
    loop_source="animation.extras.loop" if isinstance(loop_extra,bool) else "legacy_action_policy_pending"
    channels={}; amin=math.inf; amax=-math.inf
    for ch in animations[0].get("channels",[]):
        target=ch.get("target",{}); path=target.get("path"); ni=target.get("node")
        if path=="weights": raise ValueError("morph animation unsupported")
        if path not in ("translation","rotation","scale") or not isinstance(ni,int): raise ValueError("unsupported animation target")
        key=(ni,path)
        if key in channels: raise ValueError("duplicate animation channel")
        sm=animations[0]["samplers"][ch["sampler"]]
        if sm.get("interpolation","LINEAR")!="LINEAR": raise ValueError("only LINEAR animation supported")
        ti=acc(d,b,sm["input"]).reshape(-1).astype(float); va=acc(d,b,sm["output"]).astype(float)
        if len(ti)!=len(va) or len(ti)<1 or np.any(np.diff(ti)<=0): raise ValueError("bad animation sampler")
        if va.shape[1] != (4 if path=="rotation" else 3): raise ValueError("animation output width mismatch")
        channels[key]=(ti,va); amin=min(amin,float(ti[0])); amax=max(amax,float(ti[-1]))
    if not channels or not amax>amin: raise ValueError("empty/zero-duration clip")
    count=frame_count(amax-amin,fps,frame_cap); times=np.linspace(amin,amax,count)
    leg_slots={}; required=set()
    for leg,names in LEGS.items():
        ids={node_id(d,n) for n in names}; leg_slots[leg]=ids; required|=ids
    hips=node_id(d,"Hips"); head=node_id(d,"Head"); tail,tail_name=first_node_id(d,("Tail_End","Tail3"))
    records=[]; bind_world=worlds(d,base,par); ymin=math.inf; ymax=-math.inf
    for ni,n in enumerate(nodes):
        if "mesh" not in n: continue
        if "skin" not in n: continue
        skin=d["skins"][n["skin"]]; joints=list(skin.get("joints",[])); ibm=acc(d,b,skin["inverseBindMatrices"]).reshape(-1,4,4).transpose(0,2,1)
        if len(joints)!=len(ibm): raise ValueError("skin/IBM mismatch")
        slot={j:i for i,j in enumerate(joints)}
        if not required.issubset(slot): continue
        for primitive_id,prim in enumerate(d["meshes"][n["mesh"]].get("primitives",[])):
            if prim.get("targets") or prim.get("extensions") or prim.get("mode",4)!=4: raise ValueError("morph/compressed/nontriangle primitive")
            at=prim.get("attributes",{})
            if not {"POSITION","JOINTS_0","WEIGHTS_0"}<=set(at) or "JOINTS_1" in at or "WEIGHTS_1" in at: raise ValueError("unsupported skin attributes")
            ja=d["accessors"][at["JOINTS_0"]]; wa=d["accessors"][at["WEIGHTS_0"]]
            if ja.get("type")!="VEC4" or ja.get("componentType") not in (5121,5123) or ja.get("normalized"): raise ValueError("JOINTS_0 format")
            if wa.get("type")!="VEC4" or wa.get("componentType") not in (5121,5123,5126) or (wa.get("componentType")!=5126 and not wa.get("normalized")): raise ValueError("WEIGHTS_0 format")
            pos=acc(d,b,at["POSITION"]).astype(float); js=acc(d,b,at["JOINTS_0"]).astype(int); ws=acc(d,b,at["WEIGHTS_0"]).astype(float)
            if pos.shape[1]!=3 or js.shape!=ws.shape or len(pos)!=len(js) or js.min()<0 or js.max()>=len(joints) or np.any(ws<0): raise ValueError("bad skin arrays")
            if "indices" in prim:
                ia=d["accessors"][prim["indices"]]
                if ia.get("type")!="SCALAR" or ia.get("componentType") not in (5121,5123,5125) or ia.get("normalized"): raise ValueError("index format")
                indices=acc(d,b,prim["indices"]).reshape(-1).astype(int)
            else: indices=np.arange(len(pos),dtype=int)
            if len(indices)%3 or not len(indices) or indices.min()<0 or indices.max()>=len(pos): raise ValueError("triangle indices")
            surface=np.zeros(len(pos),bool); surface[np.unique(indices)]=True
            sums=ws.sum(1)
            if np.any(sums<=0): raise ValueError("zero skin weights")
            ws=ws/sums[:,None]; world_bind=(bind_world[ni]@np.c_[pos,np.ones(len(pos))].T).T[:,:3]
            ymin=min(ymin,float(world_bind[:,1].min())); ymax=max(ymax,float(world_bind[:,1].max()))
            records.append({"node":ni,"mesh":int(n["mesh"]),"primitive":primitive_id,"joints":joints,"ibm":ibm,
                            "pos":pos,"js":js,"ws":ws,"slot":slot,"bind":world_bind,"surface":surface})
    if not records or not ymax>ymin: raise ValueError("no measurable skinned geometry")
    # One immutable source vertex per hoof.  The sole band is defined in bind
    # geometry, never by animation.  Within its lowest 1% height band choose
    # the vertex nearest the band's XZ centre, then stable source ids.
    anchors={}
    band=max((ymax-ymin)*sole_band_height_ratio,1e-8)
    for leg,ids in leg_slots.items():
        candidates=[]
        for ri,r in enumerate(records):
            slots={r["slot"][x] for x in ids}; dom=np.where(np.isin(r["js"],list(slots)),r["ws"],0).sum(1)
            for vi in np.flatnonzero((dom>=min_chain_weight)&r["surface"]): candidates.append((ri,int(vi),float(dom[vi]),r["bind"][vi]))
        if not candidates: raise ValueError(f"no >={min_chain_weight} dominant anchor vertices: {leg}")
        low=min(float(x[3][1]) for x in candidates); sole=[x for x in candidates if float(x[3][1])<=low+band]
        centre=np.mean(np.asarray([x[3][[0,2]] for x in sole]),axis=0)
        chosen=min(sole,key=lambda x:(float(np.sum((x[3][[0,2]]-centre)**2)),float(x[3][1]),x[0],x[1]))
        ri,vi,dominance,point=chosen; r=records[ri]
        anchors[leg]={"record":ri,"vertex":vi,"weight":dominance,"bind":point,
                      "mesh_node":r["node"],"mesh":r["mesh"],"primitive":r["primitive"]}
    tracks={leg:[] for leg in LEGS}; roots=[]; rootq=[]; heads=[]; tails=[]
    for t in times:
        local=[]
        for i,(bt,bq,bs) in enumerate(base_trs):
            tt=interp(*channels[(i,"translation")],t) if (i,"translation") in channels else bt
            qq=interp(*channels[(i,"rotation")],t,True) if (i,"rotation") in channels else bq
            ss=interp(*channels[(i,"scale")],t) if (i,"scale") in channels else bs
            local.append(trs(tt,qq,ss))
        w=worlds(d,local,par); roots.append(w[hips][:3,3]); rootq.append(matrix_quat(w[hips][:3,:3])); heads.append(matrix_quat(w[head][:3,:3])); tails.append(w[tail][:3,3])
        posed=[]
        for r in records:
            mats=np.stack([np.linalg.inv(w[r["node"]])@w[j]@r["ibm"][k] for k,j in enumerate(r["joints"])])
            hp=np.c_[r["pos"],np.ones(len(r["pos"]))]; sk=np.einsum("vjik,vk->vji",mats[r["js"]],hp)
            p=np.einsum("vj,vji->vi",r["ws"],sk)[:,:3]
            posed.append((w[r["node"]]@np.c_[p,np.ones(len(p))].T).T[:,:3])
        for leg,a in anchors.items(): tracks[leg].append(posed[a["record"]][a["vertex"]])
    ground=min(float(a["bind"][1]) for a in anchors.values())
    axis_values={"+x":[1,0,0],"-x":[-1,0,0],"+y":[0,1,0],"-y":[0,-1,0],"+z":[0,0,1],"-z":[0,0,-1]}
    forward=np.asarray(axis_values.get(str(rig_doc.get("forward_axis") or "").lower(),[0,0,0]),float); up=np.asarray([0.,1.,0.])
    forward=forward-up*float(forward@up); fn=float(np.linalg.norm(forward)); forward=forward/fn if fn>1e-8 else forward
    left=(anchors["LF"]["bind"]+anchors["LH"]["bind"])/2; rightp=(anchors["RF"]["bind"]+anchors["RH"]["bind"])/2
    right=rightp-left; right=right-up*float(right@up)-(forward*float(right@forward) if fn>1e-8 else 0); rn=float(np.linalg.norm(right))
    right=right/rn if rn>1e-8 else right; axes_ok=bool(fn>1e-8 and rn>1e-8 and abs(float(right@forward))<.05)
    norm=lambda rows:[[round(float(x),8),round(float(y-ground),8),round(float(z),8)] for x,y,z in rows]
    provenance={"source_observed":False,"identity_observed":False,"generated":False,"mirrored":None,
                "source_video_mirrored":"unknown","motion_source_identity":"pending",
                "measurement":"actual_glb_lbs_source_bound_sole_vertex"}
    doc={"action":clip.lower(),"times":[round(float(t-amin),8) for t in times],"model_height":ymax-ymin,
         "loop":loop_value,"clip_type":"loop" if loop_value else "one_shot",
         "loop_metadata":{"source":loop_source,"explicit":isinstance(loop_extra,bool)},
         "ground":{"normal":[0,1,0],"offset":0,"inference":"minimum selected bind/rest sole vertex Y",
                   "confidence":"pending_no_explicit_plane"},
         "axes":{"up":[0,1,0],"forward":forward.tolist(),"right":right.tolist(),"source_supported":axes_ok,
                 "forward_source":"rig.json.forward_axis" if fn>1e-8 else "pending",
                 "right_source":"mean(RF,RH)-mean(LF,LH) bind sole anchors" if rn>1e-8 else "pending"},
         "root":{"positions":norm(roots),"quaternions":[list(map(float,q)) for q in rootq],"source_track":"Hips"},
         "hooves":{leg:{"positions":norm(rows),"source_track":"+".join(LEGS[leg]),
                         "anchor":{"mesh_node":anchors[leg]["mesh_node"],"mesh":anchors[leg]["mesh"],
                                   "primitive":anchors[leg]["primitive"],"vertex":anchors[leg]["vertex"],
                                   "chain_weight":anchors[leg]["weight"],
                                   "bind_point":norm([anchors[leg]["bind"]])[0]},**provenance} for leg,rows in tracks.items()},
         "head":{"quaternions":[list(map(float,q)) for q in heads],"source_track":"Head","source_observed":False},
         "tail":{"positions":norm(tails),"source_track":tail_name,"source_observed":False}}
    report=contract.evaluate(doc)
    if not isinstance(loop_extra,bool):
        if "loop_metadata_source" not in report["pending"]: report["pending"].append("loop_metadata_source")
        if "missing_evidence:loop_metadata_source" not in report["failures"]: report["failures"].append("missing_evidence:loop_metadata_source")
    if "ground_plane_confidence" not in report["pending"]: report["pending"].append("ground_plane_confidence")
    if "missing_evidence:ground_plane_confidence" not in report["failures"]: report["failures"].append("missing_evidence:ground_plane_confidence")
    report["pass"]=False
    return {"schema":"autorig.equine-task-sample/1","task_id":task_id,"run_dir":str(run_dir),"clip":clip,
            "source":{"path":str(source),"sha256":sha(source),"rig_json_path":str(rig_path) if rig_path.is_file() else None,
                      "rig_json_sha256":sha(rig_path) if rig_path.is_file() else None},
            "sampling":{"fps":fps,"frame_cap":frame_cap,"options":configured},
            "geometry":{"model_height_bind_world":ymax-ymin,"bind_world_y_min":ymin,"bind_world_y_max":ymax,
                        "ground_bind_world_y":ground,"ground_confidence":"pending_no_explicit_plane"},
            "provenance":{"rendered_rig_geometry_measured":True,"source_video_identity":"pending","observed_motion":False},
            "sample":doc,"evaluation":report}

def matrix_quat(m):
    u,_,vh=np.linalg.svd(m); r=u@vh
    if np.linalg.det(r)<0: u[:,-1]*=-1; r=u@vh
    tr=np.trace(r)
    if tr>0: s=math.sqrt(tr+1)*2; q=[(r[2,1]-r[1,2])/s,(r[0,2]-r[2,0])/s,(r[1,0]-r[0,1])/s,s/4]
    else:
        i=int(np.argmax(np.diag(r))); j=(i+1)%3; k=(i+2)%3; s=math.sqrt(1+r[i,i]-r[j,j]-r[k,k])*2
        q=[0.,0.,0.,0.]; q[i]=s/4; q[3]=(r[k,j]-r[j,k])/s; q[j]=(r[j,i]+r[i,j])/s; q[k]=(r[k,i]+r[i,k])/s
    return qnorm(q)

def main():
    p=argparse.ArgumentParser(); p.add_argument("--run-dir",type=Path,required=True); p.add_argument("--task-id",required=True)
    p.add_argument("--clip",required=True); p.add_argument("--output",type=Path,required=True); p.add_argument("--fps",type=int,default=60)
    p.add_argument("--frame-cap",type=int,default=600)
    p.add_argument("--min-chain-weight",type=float,default=.7); p.add_argument("--sole-band-height-ratio",type=float,default=.01); a=p.parse_args()
    if not 1<=a.fps<=240 or not 12<=a.frame_cap<=10000: p.error("fps 1..240; frame-cap 12..10000")
    try: out=sample(a.run_dir,a.task_id,a.clip,a.fps,a.frame_cap,a.min_chain_weight,a.sole_band_height_ratio)
    except ValueError as exc: p.error(str(exc))
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(out,indent=1),encoding="utf-8"); print(json.dumps({"output":str(a.output),"source":out["source"],"evaluation":out["evaluation"]}))

if __name__=="__main__": main()
