"""Pin, derive and execute the actual zebra Walk source-glTF -> target-GLB retarget.

This driver never reads video/depth or invents camera calibration.  Bone
head/tail and forward/height/ground come from the exact V3 target rig.json in
the same glTF coordinate system; the actionless bundle is used only as an
immutable source identity pin.
"""
from __future__ import annotations
import argparse, hashlib, json, math, os, pathlib, struct, tempfile
from typing import Any, Mapping
import equine_gltf_retarget as GR

SCHEMA="autorig.hoof-actual-retarget-job.v1"
RESULT="autorig.hoof-actual-retarget-result.v1"

def sha(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
def read(path):
    value=json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    if not isinstance(value,dict): raise ValueError(f"{path} must contain an object")
    return value
def pin(row,label):
    path=pathlib.Path(row["path"]).resolve()
    if not path.is_file() or sha(path)!=row["sha256"]: raise ValueError(f"{label} missing or SHA mismatch")
    return path
def atomic(path,value):
    path=pathlib.Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists(): raise ValueError(f"refusing to overwrite {path}")
    fd,tmp=tempfile.mkstemp(prefix=f".{path.name}.",suffix=".tmp",dir=str(path.parent))
    try:
        with os.fdopen(fd,"w",encoding="utf-8",newline="\n") as stream:
            json.dump(value,stream,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False); stream.write("\n")
            stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp,path)
    finally:
        try: os.unlink(tmp)
        except FileNotFoundError: pass

def derive_calibration(source_motion:Mapping[str,Any],rig:Mapping[str,Any],bundle:Mapping[str,Any]):
    if source_motion.get("action_id")!="walk_forward": raise ValueError("actual source motion is not walk_forward")
    source_sha=(bundle.get("source") or {}).get("sha256")
    if not source_sha or source_motion.get("source_glb_sha256")!=source_sha:
        raise ValueError("source-motion/actionless-bundle source identity mismatch")
    loop=source_motion.get("loop")
    if not isinstance(loop,bool): raise ValueError("source-motion loop must be an explicit boolean from the exact action contract")
    rig_bones={row.get("name"):row for row in rig.get("bones") or [] if isinstance(row,dict) and row.get("name")}
    refs=source_motion.get("reference_skeleton") or []; animated={row.get("node") for row in source_motion.get("channels") or []}
    mapped={}
    for node in refs:
        index,name=node.get("index"),node.get("name")
        if index in animated and name not in rig_bones:
            raise ValueError(f"animated source node absent from exact target rig and cannot be ignored: {name}")
        if name in rig_bones:
            bone=rig_bones[name]
            mapped[str(index)]={"target":name,"head":bone.get("head"),"tail":bone.get("tail")}
    if not mapped or not animated <= {int(key) for key in mapped}: raise ValueError("not every actual animated source node is calibrated")
    points=[point for bone in rig_bones.values() for point in (bone.get("head"),bone.get("tail")) if isinstance(point,list) and len(point)==3]
    if not points: raise ValueError("target rig has no measured head/tail points")
    forward={"+z":[0,0,1],"-z":[0,0,-1],"+x":[1,0,0],"-x":[-1,0,0]}.get(rig.get("forward_axis"))
    if forward is None: raise ValueError("target rig forward axis is not explicit")
    height=float(rig.get("model_height_units") or 0)
    if not math.isfinite(height) or height<=0: raise ValueError("target rig height missing")
    return {"schema":"autorig.hoof-source-calibration.v1","source":"exact_target_rig_json_same_source_glb",
            "height":height,"ground_baseline":min(float(p[1]) for p in points),"forward_axis":forward,
            "up_axis":[0,1,0],
            "loop":loop,"bones":mapped}

def _accessor(doc,binary,index,width):
    accessors=doc.get("accessors") or []; views=doc.get("bufferViews") or []
    if not isinstance(index,int) or not 0<=index<len(accessors): raise ValueError("invalid output accessor")
    acc=accessors[index]
    if acc.get("sparse") or acc.get("componentType")!=5126 or acc.get("type")!=("SCALAR" if width==1 else f"VEC{width}"):
        raise ValueError("output sampler accessor contract mismatch")
    view=views[acc["bufferView"]]; stride=int(view.get("byteStride",4*width)); count=int(acc.get("count",0))
    start=int(view.get("byteOffset",0))+int(acc.get("byteOffset",0)); end_view=int(view.get("byteOffset",0))+int(view.get("byteLength",0))
    if count<2 or stride<4*width or start+(count-1)*stride+4*width>end_view: raise ValueError("output accessor bounds invalid")
    rows=[]
    for i in range(count):
        row=list(struct.unpack_from("<"+"f"*width,binary,start+i*stride))
        if any(not math.isfinite(v) for v in row): raise ValueError("output sampler has non-finite value")
        rows.append(row[0] if width==1 else row)
    return rows

def inspect_actual_output(path,action="walk_forward"):
    doc,binary=GR._read_glb(pathlib.Path(path).read_bytes()); animations=doc.get("animations") or []
    if len(animations)!=1 or animations[0].get("name")!=action: raise ValueError("derived GLB needs exactly actual Walk animation")
    animation=animations[0]; nodes=doc.get("nodes") or []; seen=set(); common_times=None; rotations=translations=0; root_travel=0.0
    for channel in animation.get("channels") or []:
        target=channel.get("target") or {}; node,path_value=target.get("node"),target.get("path")
        if not isinstance(node,int) or not 0<=node<len(nodes) or path_value not in {"rotation","translation"}: raise ValueError("invalid output channel")
        if (node,path_value) in seen: raise ValueError("duplicate output channel")
        seen.add((node,path_value))
        sampler=animation["samplers"][channel["sampler"]]
        times=_accessor(doc,binary,sampler["input"],1); values=_accessor(doc,binary,sampler["output"],4 if path_value=="rotation" else 3)
        if common_times is None: common_times=times
        elif times!=common_times: raise ValueError("output channels disagree on actual sample times")
        if path_value=="rotation":
            rotations+=1
            if any(abs(math.sqrt(sum(v*v for v in q))-1)>2e-4 for q in values): raise ValueError("output quaternion not normalized")
        else:
            translations+=1
            travel=math.sqrt(sum((a-b)**2 for a,b in zip(values[-1],values[0]))); root_travel=max(root_travel,travel)
    if rotations<1 or translations<1: raise ValueError("derived Walk lacks numeric rotation/translation samplers")
    return {"frames":len(common_times),"duration_s":common_times[-1]-common_times[0],
            "rotation_channels":rotations,"translation_channels":translations,"maximum_translation_travel":root_travel}

def execute(job):
    if job.get("schema")!=SCHEMA or job.get("state")!="prepared_waiting_actual_source_motion": raise ValueError("unsupported/unprepared job")
    paths={key:pin(job[key],key) for key in ("source_motion","actionless_bundle","target_rig","target_glb")}
    source_motion=read(paths["source_motion"]); bundle=read(paths["actionless_bundle"]); rig=read(paths["target_rig"])
    if sha(paths["target_glb"])!=(bundle.get("source") or {}).get("sha256"): raise ValueError("target GLB is not exact actionless source GLB")
    calibration=derive_calibration(source_motion,rig,bundle); entry,bone_map=GR.adapt_source_motion(source_motion,calibration)
    if job.get("existing_animation_policy")!="replace": raise ValueError("actual Walk candidate must explicitly replace old animations")
    output=pathlib.Path(job["output_glb"]).resolve(); receipt=GR.write_retargeted_glb(
        paths["target_glb"],output,entry,rig,bone_map,existing_animation_policy="replace")
    numeric=inspect_actual_output(output)
    result={"schema":RESULT,"state":"candidate_not_release","job_sha256":hashlib.sha256(json.dumps(job,sort_keys=True,separators=(",",":")).encode()).hexdigest(),
            "calibration":calibration,"retarget_receipt":receipt,"numeric_sampler_validation":numeric,
            "source_adapter":entry.get("source_motion"),
            "source_motion_sha256":sha(paths["source_motion"]),"actionless_bundle_sha256":sha(paths["actionless_bundle"]),
            "target_rig_sha256":sha(paths["target_rig"]),"accepted_camera_z_claim":False,"procedural_override_used":False,
            "activation_allowed":False,"release_complete":False}
    atomic(job["result_json"],result); return result

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--job",required=True,type=pathlib.Path); args=parser.parse_args()
    try: print(json.dumps(execute(read(args.job)),ensure_ascii=False,sort_keys=True,indent=2)); return 0
    except Exception as exc: print(json.dumps({"ok":False,"error":str(exc)},ensure_ascii=False)); return 2
if __name__=="__main__": raise SystemExit(main())
