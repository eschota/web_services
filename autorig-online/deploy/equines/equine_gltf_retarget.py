"""Adapt actual glTF source motion and write a derived retargeted target GLB.

No source or target file is edited.  The writer preserves the target BIN bytes
as an exact prefix and changes only animations/accessors/bufferViews plus the
single buffer byteLength in a newly-created output path.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import pathlib
import struct
import tempfile
from typing import Any, Mapping, Sequence

import equine_retarget as ER


SOURCE_SCHEMA = "autorig.registry34.gltf-source-motion/1"
RECEIPT_SCHEMA = "autorig.hoof-retargeted-glb.v1"
STATIC_REST_SCALE_TOLERANCE = 1.0e-6
JSON_CHUNK = 0x4E4F534A
BIN_CHUNK = 0x004E4942


class GltfRetargetError(ValueError):
    pass


def _sha(data: bytes) -> str: return hashlib.sha256(data).hexdigest()


def _read_glb(data: bytes) -> tuple[dict[str, Any], bytes]:
    if len(data) < 20 or data[:4] != b"glTF" or struct.unpack_from("<I", data, 4)[0] != 2:
        raise GltfRetargetError("invalid GLB 2.0 header")
    if struct.unpack_from("<I", data, 8)[0] != len(data): raise GltfRetargetError("GLB length mismatch")
    offset, doc, binary = 12, None, None
    while offset < len(data):
        if offset + 8 > len(data): raise GltfRetargetError("truncated GLB chunk")
        size, kind = struct.unpack_from("<II", data, offset); offset += 8
        if offset + size > len(data): raise GltfRetargetError("GLB chunk exceeds bounds")
        chunk = data[offset:offset+size]; offset += size
        if kind == JSON_CHUNK:
            if doc is not None: raise GltfRetargetError("duplicate JSON chunk")
            try: doc = json.loads(chunk.rstrip(b" \0").decode("utf-8"))
            except Exception as exc: raise GltfRetargetError(f"invalid GLB JSON: {exc}") from exc
        elif kind == BIN_CHUNK:
            if binary is not None: raise GltfRetargetError("multiple BIN chunks unsupported")
            binary = chunk
        else:
            raise GltfRetargetError("unknown GLB chunk unsupported")
    if not isinstance(doc, dict) or binary is None: raise GltfRetargetError("GLB requires JSON and BIN chunks")
    buffers = doc.get("buffers") or []
    if len(buffers) != 1 or buffers[0].get("uri") is not None: raise GltfRetargetError("exactly one embedded buffer required")
    if int(buffers[0].get("byteLength", -1)) > len(binary): raise GltfRetargetError("buffer exceeds BIN")
    return doc, binary


def _write_glb(doc: Mapping[str, Any], binary: bytes) -> bytes:
    js = json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    js += b" " * (-len(js) % 4); binary += b"\0" * (-len(binary) % 4)
    total = 12 + 8 + len(js) + 8 + len(binary)
    return (b"glTF" + struct.pack("<II", 2, total) + struct.pack("<II", len(js), JSON_CHUNK) + js
            + struct.pack("<II", len(binary), BIN_CHUNK) + binary)


def _finite_vector(value: Any, width: int, label: str) -> list[float]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != width:
        raise GltfRetargetError(f"{label} must be a {width}-vector")
    out = [float(v) for v in value]
    if any(not math.isfinite(v) for v in out): raise GltfRetargetError(f"{label} contains non-finite values")
    return out


def _qmatrix(q):
    x,y,z,w=_finite_vector(q,4,"quaternion"); n=math.sqrt(x*x+y*y+z*z+w*w)
    if n < 1e-8: raise GltfRetargetError("singular quaternion")
    x,y,z,w=x/n,y/n,z/n,w/n
    return [[1-2*(y*y+z*z),2*(x*y-z*w),2*(x*z+y*w)],
            [2*(x*y+z*w),1-2*(x*x+z*z),2*(y*z-x*w)],
            [2*(x*z-y*w),2*(y*z+x*w),1-2*(x*x+y*y)]]


def _trs_matrix(t,q,s):
    t=_finite_vector(t,3,"translation"); s=_finite_vector(s,3,"scale"); r=_qmatrix(q)
    return [[r[i][j]*s[j] for j in range(3)]+[t[i]] for i in range(3)]+[[0.,0.,0.,1.]]


def _mmul(a,b):
    return [[sum(a[i][k]*b[k][j] for k in range(4)) for j in range(4)] for i in range(4)]


def _decompose_trs(m,label):
    t=[m[i][3] for i in range(3)]; cols=[[m[i][j] for i in range(3)] for j in range(3)]
    scales=[math.sqrt(sum(v*v for v in col)) for col in cols]
    if any(v < 1e-8 or not math.isfinite(v) for v in scales): raise GltfRetargetError(f"{label}: singular composed scale")
    cols=[[v/scales[j] for v in cols[j]] for j in range(3)]
    if any(abs(sum(cols[a][i]*cols[b][i] for i in range(3)))>1e-5 for a,b in ((0,1),(0,2),(1,2))):
        raise GltfRetargetError(f"{label}: omitted ancestor composition creates shear")
    det=(cols[0][0]*(cols[1][1]*cols[2][2]-cols[1][2]*cols[2][1])
         -cols[1][0]*(cols[0][1]*cols[2][2]-cols[0][2]*cols[2][1])
         +cols[2][0]*(cols[0][1]*cols[1][2]-cols[0][2]*cols[1][1]))
    if det < 0: raise GltfRetargetError(f"{label}: reflected composed transform unsupported")
    r=[[cols[j][i] for j in range(3)] for i in range(3)]
    tr=r[0][0]+r[1][1]+r[2][2]
    if tr>0:
        s=math.sqrt(tr+1)*2; q=[(r[2][1]-r[1][2])/s,(r[0][2]-r[2][0])/s,(r[1][0]-r[0][1])/s,.25*s]
    elif r[0][0]>r[1][1] and r[0][0]>r[2][2]:
        s=math.sqrt(1+r[0][0]-r[1][1]-r[2][2])*2; q=[.25*s,(r[0][1]+r[1][0])/s,(r[0][2]+r[2][0])/s,(r[2][1]-r[1][2])/s]
    elif r[1][1]>r[2][2]:
        s=math.sqrt(1+r[1][1]-r[0][0]-r[2][2])*2; q=[(r[0][1]+r[1][0])/s,.25*s,(r[1][2]+r[2][1])/s,(r[0][2]-r[2][0])/s]
    else:
        s=math.sqrt(1+r[2][2]-r[0][0]-r[1][1])*2; q=[(r[0][2]+r[2][0])/s,(r[1][2]+r[2][1])/s,.25*s,(r[1][0]-r[0][1])/s]
    n=math.sqrt(sum(v*v for v in q)); q=[v/n for v in q]
    return t,q,scales


def adapt_source_motion(candidate: Mapping[str, Any], calibration: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convert farm_export_fitted output into one immutable retarget entry and map.

    Calibration is reviewed data, not inference: every retained source node has
    explicit model-space head/tail and a target bone.  Ancestor-only glTF nodes
    can be omitted; each retained parent becomes its nearest retained ancestor.
    """
    if candidate.get("schema") != SOURCE_SCHEMA or candidate.get("taxonomy") != "full34":
        raise GltfRetargetError("unsupported source-motion schema/taxonomy")
    action = str(candidate.get("action_id") or "")
    if not action: raise GltfRetargetError("source action_id missing")
    reference = candidate.get("reference_skeleton")
    channels = candidate.get("channels")
    if not isinstance(reference, list) or not reference or not isinstance(channels, list) or not channels:
        raise GltfRetargetError("actual reference skeleton/channels missing")
    if calibration.get("schema") != "autorig.hoof-source-calibration.v1":
        raise GltfRetargetError("reviewed source calibration schema required")
    rows = calibration.get("bones")
    if not isinstance(rows, Mapping) or not rows: raise GltfRetargetError("source calibration bone map missing")
    forward_axis=_finite_vector(calibration.get("forward_axis"),3,"source calibration forward_axis")
    up_axis=_finite_vector(calibration.get("up_axis"),3,"source calibration up_axis")
    if math.sqrt(sum(v*v for v in forward_axis))<1e-8 or math.sqrt(sum(v*v for v in up_axis))<1e-8:
        raise GltfRetargetError("source calibration axes are singular")
    by_index = {}
    for raw in reference:
        if not isinstance(raw, Mapping) or not isinstance(raw.get("index"), int): raise GltfRetargetError("invalid reference node")
        index = raw["index"]
        if index in by_index: raise GltfRetargetError("duplicate reference node index")
        if raw.get("matrix") is not None: raise GltfRetargetError("matrix fallback forbidden")
        scale = _finite_vector(raw.get("scale"), 3, f"node {index} scale")
        if any(abs(v) < 1e-8 for v in scale): raise GltfRetargetError("singular node scale")
        by_index[index] = raw
    selected = {int(key): value for key, value in rows.items()}
    if set(selected) - set(by_index): raise GltfRetargetError("calibration references absent source node")
    names, bones, bone_map, chains = set(), [], {}, {}
    def nearest_selected_parent(index):
        seen = set(); parent = by_index[index].get("parent")
        while parent is not None and parent not in selected:
            if parent in seen or parent not in by_index: raise GltfRetargetError("invalid/cyclic source ancestry")
            seen.add(parent); parent = by_index[parent].get("parent")
        return parent
    for index in sorted(selected):
        raw, spec = by_index[index], selected[index]
        if not isinstance(spec, Mapping): raise GltfRetargetError("invalid source calibration row")
        name = raw.get("name")
        if not isinstance(name, str) or not name or name in names: raise GltfRetargetError("mapped source node names must be unique")
        names.add(name)
        parent_index = nearest_selected_parent(index)
        parent_name = by_index[parent_index].get("name") if parent_index is not None else None
        chain=[]; cursor=index
        while cursor is not None and cursor!=parent_index:
            chain.append(cursor); cursor=by_index[cursor].get("parent")
        chain.reverse(); chains[index]=chain
        composed=[[1.,0.,0.,0.],[0.,1.,0.,0.],[0.,0.,1.,0.],[0.,0.,0.,1.]]
        for chain_index in chain:
            node=by_index[chain_index]
            composed=_mmul(composed,_trs_matrix(node.get("translation"),node.get("rotation_xyzw"),node.get("scale")))
        rest_translation,rest_rotation,rest_scale=_decompose_trs(composed,f"node {index} collapsed rest")
        target = spec.get("target")
        optional = spec.get("optional_decorative") is True
        if not isinstance(target, str) or not target: raise GltfRetargetError(f"target bone missing for source node {index}")
        bones.append({"name": name, "parent": parent_name,
                      "optional_decorative": optional,
                      "rest_translation": rest_translation,
                      "rest_rotation": rest_rotation,
                      "rest_scale": rest_scale,
                      "head": _finite_vector(spec.get("head"), 3, f"{name}.head"),
                      "tail": _finite_vector(spec.get("tail"), 3, f"{name}.tail")})
        bone_map[name] = {"target": target, "optional_decorative": optional}
    common_times, tracks, seen, channel_data = None, {}, set(), {}
    ignored_static_scale_channels=0
    for number, channel in enumerate(channels):
        if not isinstance(channel, Mapping): raise GltfRetargetError("invalid source channel")
        node, path = channel.get("node"), channel.get("path")
        if node not in selected: raise GltfRetargetError("animated source node lacks exact calibration/mapping")
        if path not in {"rotation", "translation", "scale"}: raise GltfRetargetError("morph/unknown source channel forbidden")
        if channel.get("interpolation") != "LINEAR": raise GltfRetargetError("only LINEAR source channels supported")
        if (node, path) in seen: raise GltfRetargetError("duplicate source node/property channel")
        seen.add((node, path))
        times = channel.get("times")
        if not isinstance(times, list) or len(times) < 2: raise GltfRetargetError("source channel needs at least two samples")
        times = [float(v) for v in times]
        if any(not math.isfinite(v) for v in times) or any(b <= a for a, b in zip(times, times[1:])):
            raise GltfRetargetError("source times must be finite and strictly increasing")
        if common_times is None: common_times = times
        elif times != common_times: raise GltfRetargetError("all source channels must share exact sample times")
        step = times[1] - times[0]
        if any(abs((b-a)-step) > max(1e-7, step*1e-5) for a, b in zip(times, times[1:])):
            raise GltfRetargetError("nonuniform source sampling unsupported")
        values = channel.get("values")
        width = 4 if path == "rotation" else 3
        if not isinstance(values, list) or len(values) != len(times): raise GltfRetargetError("source channel sample mismatch")
        values = [_finite_vector(v, width, f"channel {number}") for v in values]
        if path=="scale":
            rest_scale=_finite_vector(by_index[node].get("scale"),3,f"node {node} rest scale")
            if any(any(abs(actual-expected)>STATIC_REST_SCALE_TOLERANCE for actual,expected in zip(sample,rest_scale)) for sample in values):
                raise GltfRetargetError(f"node {node}: animated or unexpected scale channel forbidden")
            ignored_static_scale_channels+=1
            continue
        channel_data.setdefault(node,{})[path]=values
    # Collapse every omitted static ancestor into every animated sample.  The
    # result is still local to the nearest retained parent, never model/world.
    for node,animated in channel_data.items():
        name=by_index[node]["name"]; rotations=[]; translations=[]; scales=[]
        for frame in range(len(common_times)):
            composed=[[1.,0.,0.,0.],[0.,1.,0.,0.],[0.,0.,1.,0.],[0.,0.,0.,1.]]
            for chain_index in chains[node]:
                raw=by_index[chain_index]
                tr=animated.get("translation",[])[frame] if chain_index==node and "translation" in animated else raw.get("translation")
                rot=animated.get("rotation",[])[frame] if chain_index==node and "rotation" in animated else raw.get("rotation_xyzw")
                composed=_mmul(composed,_trs_matrix(tr,rot,raw.get("scale")))
            tr,rot,scale=_decompose_trs(composed,f"node {node} frame {frame}")
            translations.append(tr); rotations.append(rot); scales.append(scale)
        rest_scale=next(b["rest_scale"] for b in bones if b["name"]==name)
        if any(any(abs(a-b)>1e-5 for a,b in zip(scale,rest_scale)) for scale in scales):
            raise GltfRetargetError(f"node {node}: animated/composed scale drift unsupported")
        tracks[f"{node}.rotation"]={"bone":name,"property":"rotation","type":"quaternion","values":rotations}
        tracks[f"{node}.translation"]={"bone":name,"property":"translation","type":"translation","values":translations}
    fps = 1.0 / (common_times[1] - common_times[0])
    entry = {"clip": action, "loop": bool(calibration.get("loop")),
             "action_metadata": {"action": action, "frame_count": len(common_times), "fps": fps,
                                 "duration_s": common_times[-1]-common_times[0],
                                 "coordinate_system": "right_handed_y_up", "track_space": "local"},
             "reference_skeleton": {"height": calibration.get("height"),
                                    "ground_baseline": calibration.get("ground_baseline"),
                                    "forward_axis": forward_axis,
                                    "up_axis":up_axis, "bones": bones},
             "tracks": tracks,
             "source_motion": {"schema": SOURCE_SCHEMA, "derived_glb": copy.deepcopy(candidate.get("derived_glb")),
                               "source_glb_sha256": candidate.get("source_glb_sha256"),
                               "ignored_static_rest_scale_channels": ignored_static_scale_channels,
                               "static_rest_scale_tolerance": STATIC_REST_SCALE_TOLERANCE,
                               "candidate_only": True, "release_complete": False}}
    return entry, bone_map


def adapt_source_library(candidates: Sequence[Mapping[str, Any]], calibrations: Mapping[str, Mapping[str, Any]],
                         required_ids: Sequence[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Adapt a release-sized source set; reject partial, duplicate or calibration drift."""
    required=tuple(required_ids); by_id={str(row.get("action_id") or ""):row for row in candidates}
    if len(by_id)!=len(candidates) or set(by_id)!=set(required) or set(calibrations)!=set(required):
        raise GltfRetargetError("source library must exactly match the full required action set and calibrations")
    entries=[]; common_map=None; common_skeleton=None
    for action in required:
        entry,bone_map=adapt_source_motion(by_id[action],calibrations[action])
        skeleton=json.dumps(entry["reference_skeleton"],sort_keys=True,separators=(",",":"),allow_nan=False)
        mapping=json.dumps(bone_map,sort_keys=True,separators=(",",":"),allow_nan=False)
        if common_skeleton is None: common_skeleton,common_map=skeleton,mapping
        elif skeleton!=common_skeleton or mapping!=common_map:
            raise GltfRetargetError("source skeleton or exact bone calibration drifted across actions")
        entry["source_motion"]["candidate_only"]=False; entry["source_motion"]["release_complete"]=True
        entries.append(entry)
    return entries,json.loads(common_map)


def target_from_glb(target_bytes: bytes, rig_doc: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    doc, _binary = _read_glb(target_bytes)
    nodes = doc.get("nodes") or []
    parents = {}
    for parent, node in enumerate(nodes):
        if node.get("matrix") is not None: raise GltfRetargetError("matrix nodes forbidden; explicit TRS required")
        for child in node.get("children") or []:
            if not isinstance(child, int) or child < 0 or child >= len(nodes) or child in parents:
                raise GltfRetargetError("invalid/multiple target parent")
            parents[child] = parent
    name_to_index = {}
    for index, node in enumerate(nodes):
        name = node.get("name")
        if isinstance(name, str) and name:
            if name in name_to_index: raise GltfRetargetError("duplicate target node name")
            name_to_index[name] = index
    bones = []
    for raw in rig_doc.get("bones") or []:
        name = raw.get("name") if isinstance(raw, Mapping) else None
        if name not in name_to_index: raise GltfRetargetError(f"target GLB rest node missing: {name}")
        index, node = name_to_index[name], nodes[name_to_index[name]]
        scale = _finite_vector(node.get("scale", [1,1,1]), 3, f"target {name}.scale")
        if any(abs(v) < 1e-8 for v in scale): raise GltfRetargetError(f"target {name} singular scale")
        expected_parent = raw.get("parent")
        actual_parent = nodes[parents[index]].get("name") if index in parents else None
        # Allow non-bone armature ancestors only for roots; bone-to-bone hierarchy must be exact.
        if expected_parent is not None and actual_parent != expected_parent:
            raise GltfRetargetError(f"target hierarchy mismatch for {name}")
        bones.append({"name": name, "parent": expected_parent,
                      "rest_translation": _finite_vector(node.get("translation", [0,0,0]), 3, f"target {name}.translation"),
                      "rest_rotation": _finite_vector(node.get("rotation", [0,0,0,1]), 4, f"target {name}.rotation"),
                      "rest_scale": scale, "head": _finite_vector(raw.get("head"), 3, f"target {name}.head"),
                      "tail": _finite_vector(raw.get("tail"), 3, f"target {name}.tail")})
    if not bones: raise GltfRetargetError("target rig has no bones")
    points = [v for bone in bones for v in (bone["head"],bone["tail"])]
    height = float(rig_doc.get("model_height_units") or (max(v[1] for v in points)-min(v[1] for v in points)))
    forward = rig_doc.get("forward_axis", "+z")
    forward = {"+z":[0,0,1],"-z":[0,0,-1],"+x":[1,0,0],"-x":[-1,0,0]}.get(forward, forward)
    return {"height": height, "ground_baseline": min(v[1] for v in points),
            "forward_axis": forward, "up_axis":[0,1,0], "bones": bones}, name_to_index


def append_clip_bytes(target_bytes: bytes, clip: Mapping[str, Any], *, existing_animation_policy: str = "reject") -> bytes:
    doc, original_binary = _read_glb(target_bytes)
    if existing_animation_policy not in {"reject","replace","preserve"}:
        raise GltfRetargetError("invalid existing animation policy")
    if doc.get("animations") and existing_animation_policy == "reject":
        raise GltfRetargetError("target already has animations; explicit policy required")
    output = copy.deepcopy(doc); binary = bytearray(original_binary)
    if existing_animation_policy == "replace": output["animations"]=[]
    output.setdefault("bufferViews", []); output.setdefault("accessors", []); output.setdefault("animations", [])
    by_name = {node.get("name"): index for index,node in enumerate(output.get("nodes") or []) if node.get("name")}
    times = clip.get("times")
    if not isinstance(times, list) or len(times) < 2: raise GltfRetargetError("retarget clip times missing")
    def add(values, width, target=None):
        nonlocal binary
        binary.extend(b"\0" * (-len(binary)%4)); offset=len(binary)
        flat=[]
        for value in values:
            row=[float(value)] if width==1 else _finite_vector(value,width,"output track")
            flat.extend(row)
        packed=struct.pack("<"+"f"*len(flat),*flat); binary.extend(packed)
        view={"buffer":0,"byteOffset":offset,"byteLength":len(packed)}
        if target is not None: view["target"]=target
        output["bufferViews"].append(view)
        accessor={"bufferView":len(output["bufferViews"])-1,"componentType":5126,
                  "count":len(values),"type":"SCALAR" if width==1 else f"VEC{width}"}
        if width==1: accessor.update(min=[min(flat)],max=[max(flat)])
        output["accessors"].append(accessor); return len(output["accessors"])-1
    time_accessor=add(times,1); samplers=[]; channels=[]
    for path,key,width in (("rotation","rotations",4),("translation","translations",3)):
        for bone,values in (clip.get(key) or {}).items():
            if bone not in by_name: raise GltfRetargetError(f"output target node missing: {bone}")
            if len(values)!=len(times): raise GltfRetargetError("output channel/time count mismatch")
            samplers.append({"input":time_accessor,"output":add(values,width),"interpolation":"LINEAR"})
            channels.append({"sampler":len(samplers)-1,"target":{"node":by_name[bone],"path":path}})
    if not channels: raise GltfRetargetError("no output channels")
    output["animations"].append({"name":str(clip.get("name") or clip.get("id") or ""),
                                 "samplers":samplers,"channels":channels,
                                 "extras":{"source":clip.get("source"),"loop":clip.get("loop") is True}})
    output["buffers"][0]["byteLength"]=len(binary)
    return _write_glb(output,bytes(binary))


def write_retargeted_glb(target_path: os.PathLike[str] | str, output_path: os.PathLike[str] | str,
                         entry: Mapping[str, Any], rig_doc: Mapping[str, Any], bone_map: Mapping[str, Any],
                         options: Mapping[str, Any] | None = None, *, candidate_only: bool = True,
                         existing_animation_policy: str = "reject") -> dict[str, Any]:
    source_path=pathlib.Path(target_path).resolve(); destination=pathlib.Path(output_path).resolve()
    receipt_path=destination.with_suffix(destination.suffix+".receipt.json")
    if source_path == destination or not source_path.is_file(): raise GltfRetargetError("source target GLB missing or output aliases it")
    if destination.exists() or receipt_path.exists(): raise GltfRetargetError("derived output or receipt already exists")
    target_bytes=source_path.read_bytes(); target_doc,node_map=target_from_glb(target_bytes,rig_doc)
    clip=ER.retarget_entry(entry,target_doc,bone_map,options)
    derived=append_clip_bytes(target_bytes,clip,existing_animation_policy=existing_animation_policy)
    destination.parent.mkdir(parents=True,exist_ok=True)
    fd,temp_name=tempfile.mkstemp(prefix=f".{destination.name}.",suffix=".tmp",dir=str(destination.parent))
    try:
        with os.fdopen(fd,"wb") as stream:
            stream.write(derived); stream.flush(); os.fsync(stream.fileno())
        if source_path.read_bytes()!=target_bytes: raise GltfRetargetError("target GLB changed during materialization")
        os.replace(temp_name,destination)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass
    before_doc,before_bin=_read_glb(target_bytes); after_doc,after_bin=_read_glb(derived)
    protected=("nodes","meshes","materials","textures","images","skins","scenes","scene","samplers","cameras")
    preservation={key:_sha(json.dumps(before_doc.get(key),sort_keys=True,separators=(",",":"),ensure_ascii=False).encode())
                  for key in protected if key in before_doc}
    if any(before_doc.get(key)!=after_doc.get(key) for key in protected):
        raise GltfRetargetError("protected target JSON changed")
    if after_bin[:len(before_bin)]!=before_bin: raise GltfRetargetError("target BIN prefix changed")
    receipt={"schema":RECEIPT_SCHEMA,"state":"candidate_not_release" if candidate_only else "derived",
            "action_id":entry.get("clip"),"target_before_sha256":_sha(target_bytes),
            "target_after_sha256":_sha(derived),"output_path":str(destination),"output_bytes":len(derived),
            "receipt_path":str(receipt_path),"original_binary_sha256":_sha(before_bin),
            "original_binary_prefix_sha256":_sha(after_bin[:len(before_bin)]),
            "protected_json_sha256":preservation,
            "node_count":len(node_map),"retarget_metadata":clip["retarget_metadata"],
            "existing_animation_policy":existing_animation_policy,
            "activation_allowed":False,"release_complete":False}
    encoded=(json.dumps(receipt,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+"\n").encode("utf-8")
    fd,temp_name=tempfile.mkstemp(prefix=f".{receipt_path.name}.",suffix=".tmp",dir=str(receipt_path.parent))
    try:
        with os.fdopen(fd,"wb") as stream:
            stream.write(encoded); stream.flush(); os.fsync(stream.fileno())
        os.replace(temp_name,receipt_path)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass
    return receipt
