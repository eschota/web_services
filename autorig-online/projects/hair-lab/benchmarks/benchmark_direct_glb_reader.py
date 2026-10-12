#!/usr/bin/env python3
"""Benchmark direct GLB Head-weight extraction against retained Blender refs."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import statistics
import subprocess
import time
from pathlib import Path

import numpy as np
from scipy.spatial import cKDTree


def load_reader(path):
    spec=importlib.util.spec_from_file_location("direct_glb_head_reader",path); module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module


def p90(values):
    return float(np.percentile(np.asarray(values),90))


def triangle_multiset_hash(vertices,faces,tolerance=1e-5):
    q=np.rint(vertices[faces]/tolerance).astype(np.int64)
    keys=q[:,:,0]*np.int64(73856093)^q[:,:,1]*np.int64(19349663)^q[:,:,2]*np.int64(83492791)
    order=np.argsort(keys,axis=1); sorted_q=np.take_along_axis(q,order[:,:,None],axis=1).reshape(faces.shape[0],9)
    h=np.full(faces.shape[0],np.uint64(1469598103934665603),dtype=np.uint64)
    for column in range(9):
        h=(h^sorted_q[:,column].view(np.uint64))*np.uint64(1099511628211)
    h.sort(); return hashlib.sha256(h.tobytes()).hexdigest()


def compare(reference,direct):
    rv=reference["vertices"].astype(np.float64); dv=direct["vertices"].astype(np.float64)
    d_to_r=cKDTree(rv).query(dv,k=1)[0]; r_to_d=cKDTree(dv).query(rv,k=1)[0]
    rf=reference["faces"]; df=direct["faces"]
    def descriptors(vertices,faces):
        triangles=vertices[faces]
        centroid=triangles.mean(axis=1)
        edges=np.stack((np.sum((triangles[:,1]-triangles[:,0])**2,axis=1),np.sum((triangles[:,2]-triangles[:,1])**2,axis=1),np.sum((triangles[:,0]-triangles[:,2])**2,axis=1)),axis=1)
        return np.column_stack((centroid,np.sort(edges,axis=1)))
    rd=descriptors(rv,rf); dd=descriptors(dv,df)
    direct_triangle_distance=cKDTree(rd).query(dd,k=1)[0] if len(rd) else np.array([np.inf])
    reference_triangle_distance=cKDTree(dd).query(rd,k=1)[0] if len(dd) else np.array([np.inf])
    result={
        "reference_vertices":int(rv.shape[0]),"direct_vertices":int(dv.shape[0]),
        "reference_triangles":int(rf.shape[0]),"direct_triangles":int(df.shape[0]),
        "direct_to_reference_position_max":float(d_to_r.max(initial=0)),"reference_to_direct_position_max":float(r_to_d.max(initial=0)),
        "positions_bijective_within_1e_6":bool(dv.shape==rv.shape and d_to_r.max(initial=0)<=1e-6 and r_to_d.max(initial=0)<=1e-6),
        "faces_array_equal":bool(np.array_equal(df,rf)),
        "direct_to_reference_triangle_descriptor_max":float(direct_triangle_distance.max(initial=0)),
        "reference_to_direct_triangle_descriptor_max":float(reference_triangle_distance.max(initial=0)),
        "triangle_geometry_bijective_within_1e_5":bool(len(df)==len(rf) and direct_triangle_distance.max(initial=0)<=1e-5 and reference_triangle_distance.max(initial=0)<=1e-5),
        "direct_triangle_multiset_sha256_1e_5":triangle_multiset_hash(dv,df),
        "reference_triangle_multiset_sha256_1e_5":triangle_multiset_hash(rv,rf),
    }
    if len(df)!=len(rf):
        def canonical_rows(vertices,faces):
            q=np.rint(vertices[faces]/1e-6).astype(np.int64); keys=q[:,:,0]*73856093^q[:,:,1]*19349663^q[:,:,2]*83492791; order=np.argsort(keys,axis=1)
            return np.take_along_axis(q,order[:,:,None],axis=1).reshape(len(faces),9)
        direct_rows=canonical_rows(dv,df); reference_rows=canonical_rows(rv,rf)
        _,direct_counts=np.unique(direct_rows,axis=0,return_counts=True); _,reference_counts=np.unique(reference_rows,axis=0,return_counts=True)
        result["triangle_count_discrepancy"]={"direct_minus_reference":int(len(df)-len(rf)),"direct_duplicate_excess_1e_6":int(np.maximum(direct_counts-1,0).sum()),"reference_duplicate_excess_1e_6":int(np.maximum(reference_counts-1,0).sum()),"interpretation":"direct reader preserves exact primitive triangles; Blender reference may merge duplicate geometric faces"}
    return result


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--manifest",type=Path,required=True); parser.add_argument("--work-dir",type=Path,required=True); parser.add_argument("--output",type=Path,required=True); parser.add_argument("--python",type=Path,required=True); parser.add_argument("--repeats",type=int,default=5)
    args=parser.parse_args(); reader_path=Path(__file__).with_name("direct_glb_head_reader.py"); reader=load_reader(reader_path)
    cases=json.loads(args.manifest.read_text(encoding="utf-8"))["cases"]; report={"scope":"direct current-rig GLB Head-weight extraction compared to retained Blender reference; not hair isolation","repeats":args.repeats,"cases":[]}
    for row in cases:
        case_id=row["case_id"]; source=Path(row["rig"]["local"]["path"]); output_dir=args.work_dir/"direct-reader"/case_id; output_dir.mkdir(parents=True,exist_ok=True)
        fresh_start=time.perf_counter_ns()
        completed=subprocess.run([str(args.python),str(reader_path),"--source",str(source),"--output",str(output_dir/"head_roi.npz"),"--receipt",str(output_dir/"receipt.json")],capture_output=True,text=True,check=True)
        fresh_ms=(time.perf_counter_ns()-fresh_start)/1e6
        first_start=time.perf_counter_ns(); arrays,receipt=reader.read_head(source); first_ms=(time.perf_counter_ns()-first_start)/1e6
        repeats=[]
        for _ in range(args.repeats):
            start=time.perf_counter_ns(); reader.read_head(source); repeats.append((time.perf_counter_ns()-start)/1e6)
        reference=np.load(args.work_dir/case_id/"benchmark-output"/"rig_head_roi.npz")
        comparison=compare(reference,arrays); comparison["triangle_multiset_equal"]=comparison["direct_triangle_multiset_sha256_1e_5"]==comparison["reference_triangle_multiset_sha256_1e_5"]
        report["cases"].append({"case_id":case_id,"source_sha256":receipt["source_sha256"],"source_bytes":source.stat().st_size,"fresh_process_wall_ms_os_cache_state_uncontrolled":fresh_ms,"in_process_first_ms":first_ms,"in_process_warm":{"n":len(repeats),"p50_ms":statistics.median(repeats),"p90_ms":p90(repeats),"min_ms":min(repeats),"max_ms":max(repeats)},"receipt":receipt,"blender_comparison":comparison})
    report["all_position_and_triangle_parity"]=all(c["blender_comparison"]["positions_bijective_within_1e_6"] and c["blender_comparison"]["triangle_geometry_bijective_within_1e_5"] for c in report["cases"])
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(report,indent=2),encoding="utf-8"); print(json.dumps({"cases":len(report["cases"]),"all_parity":report["all_position_and_triangle_parity"]}))


if __name__=="__main__": main()
