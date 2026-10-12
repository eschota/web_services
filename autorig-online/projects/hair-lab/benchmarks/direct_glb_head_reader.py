#!/usr/bin/env python3
"""Strict direct GLB reader for current-rig Head-weight geometry.

The reader supports embedded GLB 2.0 triangle primitives with ordinary
accessors. It preserves mesh/primitive/local-triangle provenance and fails on
compression, sparse accessors, morph targets and unsupported topology.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
import time
from pathlib import Path

import numpy as np


COMPONENTS = {5120: np.int8, 5121: np.uint8, 5122: np.int16, 5123: np.uint16, 5125: np.uint32, 5126: np.float32}
WIDTHS = {"SCALAR": 1, "VEC2": 2, "VEC3": 3, "VEC4": 4, "MAT2": 4, "MAT3": 9, "MAT4": 16}


def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return digest.hexdigest()


def parse_glb(path, max_bytes=512*1024*1024):
    if path.stat().st_size>max_bytes: raise ValueError("GLB exceeds bounded input limit")
    data=path.read_bytes()
    if len(data)<20 or len(data)%4: raise ValueError("truncated or unaligned GLB")
    magic,version,total=struct.unpack_from("<4sII",data,0)
    if magic!=b"glTF" or version!=2 or total!=len(data): raise ValueError("invalid GLB 2.0 header")
    offset=12; document=None; binary=None
    while offset<len(data):
        if offset+8>len(data): raise ValueError("truncated GLB chunk header")
        length,kind=struct.unpack_from("<II",data,offset); offset+=8
        if length%4: raise ValueError("unaligned GLB chunk")
        chunk=data[offset:offset+length]; offset+=length
        if len(chunk)!=length: raise ValueError("truncated GLB chunk")
        if kind==0x4E4F534A:
            if document is not None: raise ValueError("duplicate JSON chunk")
            document=json.loads(chunk.rstrip(b" \t\r\n\0"))
        elif kind==0x004E4942:
            if binary is not None: raise ValueError("duplicate BIN chunk")
            binary=chunk
    if document is None or binary is None: raise ValueError("GLB needs JSON and BIN chunks")
    required=set(document.get("extensionsRequired",[]))
    if required: raise ValueError(f"unsupported required extensions: {sorted(required)}")
    buffers=document.get("buffers",[])
    if len(buffers)!=1 or buffers[0].get("uri") is not None: raise ValueError("only one embedded GLB buffer is supported")
    declared=int(buffers[0].get("byteLength",-1))
    if declared<0 or declared>len(binary) or len(binary)-declared>3: raise ValueError("declared buffer/BIN length mismatch")
    return document,binary[:declared]


def accessor(document,binary,index):
    if not isinstance(index,int) or index<0 or index>=len(document.get("accessors",[])): raise ValueError("accessor index out of range")
    spec=document["accessors"][index]
    if "sparse" in spec: raise ValueError("sparse accessors unsupported")
    if "bufferView" not in spec: raise ValueError("accessor without bufferView unsupported")
    dtype=COMPONENTS.get(spec["componentType"]); width=WIDTHS.get(spec["type"])
    if dtype is None or width is None: raise ValueError("unsupported accessor type")
    view=document["bufferViews"][spec["bufferView"]]
    if int(view.get("buffer",0))!=0: raise ValueError("only embedded buffer 0 supported")
    if "extensions" in view: raise ValueError("compressed/extended bufferView unsupported")
    count=int(spec["count"]); item_bytes=np.dtype(dtype).itemsize*width
    if count<0: raise ValueError("negative accessor count")
    stride=int(view.get("byteStride",item_bytes))
    view_offset=int(view.get("byteOffset",0)); accessor_offset=int(spec.get("byteOffset",0)); view_length=int(view.get("byteLength",-1))
    if stride<item_bytes or stride<0 or view_offset<0 or accessor_offset<0 or view_length<0: raise ValueError("invalid accessor/view offset, length or stride")
    start=view_offset+accessor_offset
    end=start+(count-1)*stride+item_bytes if count else start
    view_end=view_offset+view_length
    if start<0 or end>len(binary) or end>view_end: raise ValueError("accessor out of bounds")
    if stride==item_bytes:
        result=np.frombuffer(binary,dtype=dtype,count=count*width,offset=start).reshape(count,width).copy()
    else:
        result=np.ndarray((count,width),dtype=dtype,buffer=binary,offset=start,strides=(stride,np.dtype(dtype).itemsize)).copy()
    if spec.get("normalized") and not np.issubdtype(result.dtype,np.floating):
        info=np.iinfo(result.dtype); result=result.astype(np.float32)
        result=np.maximum(result/info.max,-1.0) if info.min<0 else result/info.max
    return result


def local_matrix(node):
    if "matrix" in node:
        matrix=np.asarray(node["matrix"],np.float64)
        if matrix.shape!=(16,) or not np.isfinite(matrix).all(): raise ValueError("invalid node matrix")
        matrix=matrix.reshape(4,4,order="F")
        if not np.allclose(matrix[3],[0,0,0,1],rtol=0,atol=1e-10): raise ValueError("non-affine node matrix")
        return matrix
    translation=np.asarray(node.get("translation",[0,0,0]),np.float64)
    scale=np.asarray(node.get("scale",[1,1,1]),np.float64)
    if translation.shape!=(3,) or scale.shape!=(3,) or not np.isfinite(translation).all() or not np.isfinite(scale).all(): raise ValueError("invalid node TRS")
    rotation_value=np.asarray(node.get("rotation",[0,0,0,1]),np.float64)
    if rotation_value.shape!=(4,) or not np.isfinite(rotation_value).all(): raise ValueError("invalid node rotation")
    x,y,z,w=map(float,rotation_value); n=math.sqrt(x*x+y*y+z*z+w*w)
    if n==0: raise ValueError("zero quaternion")
    x,y,z,w=x/n,y/n,z/n,w/n
    rotation=np.array([[1-2*(y*y+z*z),2*(x*y-z*w),2*(x*z+y*w)],[2*(x*y+z*w),1-2*(x*x+z*z),2*(y*z-x*w)],[2*(x*z-y*w),2*(y*z+x*w),1-2*(x*x+y*y)]],np.float64)
    matrix=np.eye(4); matrix[:3,:3]=rotation@np.diag(scale); matrix[:3,3]=translation
    return matrix


def world_matrices(document):
    nodes=document.get("nodes",[]); parents={}
    for parent,node in enumerate(nodes):
        for child in node.get("children",[]):
            if child in parents: raise ValueError("node has multiple parents")
            parents[child]=parent
    cache={}
    def world(index,stack=()):
        if index in cache: return cache[index]
        if index in stack: raise ValueError("node cycle")
        local=local_matrix(nodes[index]); parent=parents.get(index)
        result=local if parent is None else world(parent,stack+(index,))@local
        if not np.isfinite(result).all() or not np.allclose(result[3],[0,0,0,1],rtol=0,atol=1e-10): raise ValueError("invalid accumulated affine transform")
        cache[index]=result; return result
    return [world(i) for i in range(len(nodes))]


def read_head(path, threshold=.20, max_vertices=2_000_000, max_triangles=4_000_000):
    if not math.isfinite(threshold) or threshold<0 or threshold>1: raise ValueError("threshold must be finite and within [0,1]")
    started=time.perf_counter(); document,binary=parse_glb(path); worlds=world_matrices(document)
    vertices_out=[]; normals_out=[]; faces_out=[]; node_ids=[]; mesh_ids=[]; primitive_ids=[]; triangle_ids=[]; source_vertex_ids=[]
    offset=0; selected_triangle_total=0; inventory=[]; matched=set()
    for node_id,node in enumerate(document.get("nodes",[])):
        if "mesh" not in node: continue
        mesh_id=int(node["mesh"]); mesh=document["meshes"][mesh_id]; skin_id=node.get("skin")
        if skin_id is None: continue
        skin=document["skins"][skin_id]; joints=skin.get("joints",[])
        head_slots={slot for slot,joint_node in enumerate(joints) if document["nodes"][joint_node].get("name","").lower()=="head"}
        if not head_slots: continue
        matched.update(document["nodes"][joints[slot]].get("name","") for slot in head_slots)
        world=worlds[node_id]; normal_matrix=np.linalg.inv(world[:3,:3]).T
        for primitive_id,primitive in enumerate(mesh.get("primitives",[])):
            if primitive.get("mode",4)!=4: raise ValueError("only TRIANGLES mode supported")
            if primitive.get("targets"): raise ValueError("morph targets unsupported")
            if primitive.get("extensions"): raise ValueError("primitive extensions unsupported")
            attrs=primitive.get("attributes",{})
            required={"POSITION","NORMAL","JOINTS_0","WEIGHTS_0"}
            if not required.issubset(attrs): continue
            if "JOINTS_1" in attrs or "WEIGHTS_1" in attrs: raise ValueError("secondary skin weight sets unsupported")
            position_spec=document["accessors"][attrs["POSITION"]]; normal_spec=document["accessors"][attrs["NORMAL"]]
            joint_spec=document["accessors"][attrs["JOINTS_0"]]; weight_spec=document["accessors"][attrs["WEIGHTS_0"]]
            if position_spec.get("type")!="VEC3" or position_spec.get("componentType")!=5126 or normal_spec.get("type")!="VEC3" or normal_spec.get("componentType")!=5126: raise ValueError("POSITION/NORMAL must be float VEC3")
            if joint_spec.get("type")!="VEC4" or joint_spec.get("componentType") not in (5121,5123) or joint_spec.get("normalized",False): raise ValueError("JOINTS_0 must be unsigned non-normalized VEC4")
            if weight_spec.get("type")!="VEC4" or weight_spec.get("componentType") not in (5121,5123,5126) or (weight_spec.get("componentType")!=5126 and not weight_spec.get("normalized",False)): raise ValueError("WEIGHTS_0 must be float or normalized unsigned VEC4")
            vertex_count=int(position_spec.get("count",-1)); triangle_index_count=int(document["accessors"][primitive["indices"]].get("count",-1)) if "indices" in primitive else vertex_count
            if vertex_count<0 or vertex_count>max_vertices or triangle_index_count<0 or triangle_index_count//3>max_triangles: raise ValueError("primitive exceeds bounded limits")
            positions=accessor(document,binary,attrs["POSITION"]).astype(np.float64)
            normals=accessor(document,binary,attrs["NORMAL"]).astype(np.float64)
            joints0=accessor(document,binary,attrs["JOINTS_0"]).astype(np.int64)
            weights0=accessor(document,binary,attrs["WEIGHTS_0"]).astype(np.float64)
            if positions.shape[1]!=3 or normals.shape!=positions.shape or joints0.shape!=weights0.shape or len(joints0)!=len(positions): raise ValueError("attribute shape/count mismatch")
            if not np.isfinite(positions).all() or not np.isfinite(normals).all() or not np.isfinite(weights0).all() or (weights0<0).any(): raise ValueError("non-finite/negative geometry or weights")
            if joints0.size and (joints0.min()<0 or joints0.max()>=len(joints)): raise ValueError("joint index out of skin range")
            if "indices" in primitive:
                index_spec=document["accessors"][primitive["indices"]]
                if index_spec.get("type")!="SCALAR" or index_spec.get("componentType") not in (5121,5123,5125) or index_spec.get("normalized",False): raise ValueError("indices must be unsigned non-normalized SCALAR")
                indices=accessor(document,binary,primitive["indices"]).reshape(-1).astype(np.int64)
            else: indices=np.arange(positions.shape[0],dtype=np.int64)
            if indices.size%3 or indices.size==0 or indices.min()<0 or indices.max()>=positions.shape[0]: raise ValueError("invalid triangle indices")
            faces=indices.reshape(-1,3)
            weight_sum=weights0.sum(axis=1,keepdims=True)
            if not np.isfinite(weight_sum).all() or np.any(weight_sum<=0): raise ValueError("invalid skin-weight sum")
            # glTF clients normalize weights before skinning; Blender's importer
            # exposes those normalized vertex-group values.
            weights0=weights0/weight_sum
            head_weight=np.where(np.isin(joints0,list(head_slots)),weights0,0).max(axis=1)
            selected=np.max(head_weight[faces],axis=1)>=threshold
            selected_faces=faces[selected]
            inventory.append({"node_id":node_id,"mesh_id":mesh_id,"primitive_id":primitive_id,"vertices":int(positions.shape[0]),"triangles":int(faces.shape[0]),"selected_triangles":int(selected.sum())})
            if not selected.any(): continue
            used,inverse=np.unique(selected_faces.reshape(-1),return_inverse=True)
            if offset+used.shape[0]>max_vertices or selected_triangle_total+selected_faces.shape[0]>max_triangles: raise ValueError("selected geometry exceeds bounded limits")
            homogeneous=np.column_stack((positions[used],np.ones(used.shape[0])))
            transformed=(world@homogeneous.T).T[:,:3].astype(np.float32)
            transformed_normals=(normal_matrix@normals[used].T).T
            transformed_normals=(transformed_normals/np.maximum(np.linalg.norm(transformed_normals,axis=1,keepdims=True),1e-20)).astype(np.float32)
            selected_ids=np.flatnonzero(selected).astype(np.uint32)
            vertices_out.append(transformed); normals_out.append(transformed_normals); faces_out.append(inverse.reshape(-1,3).astype(np.uint32)+offset)
            node_ids.append(np.full(selected_ids.shape,node_id,np.uint32)); mesh_ids.append(np.full(selected_ids.shape,mesh_id,np.uint32)); primitive_ids.append(np.full(selected_ids.shape,primitive_id,np.uint32)); triangle_ids.append(selected_ids); source_vertex_ids.append(used.astype(np.uint32))
            offset+=used.shape[0]; selected_triangle_total+=selected_faces.shape[0]
    if not faces_out: raise ValueError("no Head-weight triangles")
    vertices=np.concatenate(vertices_out); faces=np.concatenate(faces_out)
    if vertices.shape[0]>max_vertices or faces.shape[0]>max_triangles: raise ValueError("selected geometry exceeds bounded limits")
    if not np.isfinite(vertices).all(): raise ValueError("non-finite transformed positions")
    return {"vertices":vertices,"normals":np.concatenate(normals_out),"faces":faces,"node_ids":np.concatenate(node_ids),"mesh_ids":np.concatenate(mesh_ids),"primitive_ids":np.concatenate(primitive_ids),"triangle_ids":np.concatenate(triangle_ids),"source_vertex_ids":np.concatenate(source_vertex_ids)}, {"source_sha256":sha256(path),"geometry_state":"bind-mesh geometry; armature animation/skin deformation not evaluated","classification":"direct_current_rig_head_weight_roi_not_hair","threshold":threshold,"matched_head_joints":sorted(matched),"vertices":int(vertices.shape[0]),"triangles":int(faces.shape[0]),"primitive_inventory":inventory,"timing_seconds":time.perf_counter()-started}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--source",type=Path,required=True); parser.add_argument("--output",type=Path,required=True); parser.add_argument("--receipt",type=Path,required=True)
    args=parser.parse_args(); arrays,receipt=read_head(args.source); args.output.parent.mkdir(parents=True,exist_ok=True); np.savez_compressed(args.output,**arrays); args.receipt.write_text(json.dumps(receipt,indent=2),encoding="utf-8"); print(json.dumps(receipt))


if __name__=="__main__": main()
