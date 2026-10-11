#!/usr/bin/env python3
"""CUDA raster benchmark for real current-rig head-weight ROI meshes.

Requires the existing Hunyuan portable CuPy runtime. IDs are diagnostic
Blender-imported triangle IDs and are not an authoritative source back-map.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import time
from pathlib import Path


def enable_portable_cuda():
    root = Path(r"C:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone\Lib\site-packages")
    dirs = [root / "nvidia/cuda_nvrtc/bin", root / "nvidia/cuda_runtime/bin", root / "torch/lib"]
    os.environ["PATH"] = ";".join(map(str, dirs)) + ";" + os.environ.get("PATH", "")
    if hasattr(os, "add_dll_directory"):
        for directory in dirs:
            os.add_dll_directory(str(directory))


enable_portable_cuda()
import cupy as cp
import cv2
import numpy as np


CUDA_SOURCE = r'''
extern "C" __global__
void raster_triangles(const float* vertices, const unsigned int* faces,
                      unsigned long long* pixels, int triangle_count,
                      int resolution, int view, float cx, float cy, float cz,
                      float extent) {
    int triangle = blockDim.x * blockIdx.x + threadIdx.x;
    if (triangle >= triangle_count) return;
    float3 p[3];
    #pragma unroll
    for (int k=0; k<3; ++k) {
        unsigned int vi = faces[triangle*3+k];
        p[k] = make_float3(vertices[vi*3], vertices[vi*3+1], vertices[vi*3+2]);
    }
    float2 q[3]; float d[3];
    #pragma unroll
    for (int k=0; k<3; ++k) {
        float u, v, depth;
        if (view==0) { u=p[k].x-cx; v=p[k].y-cy; depth=p[k].z-cz; }
        else if (view==1) { u=-(p[k].x-cx); v=p[k].y-cy; depth=-(p[k].z-cz); }
        else if (view==2) { u=-(p[k].z-cz); v=p[k].y-cy; depth=p[k].x-cx; }
        else if (view==3) { u=p[k].z-cz; v=p[k].y-cy; depth=-(p[k].x-cx); }
        else if (view==4) { u=p[k].x-cx; v=-(p[k].z-cz); depth=p[k].y-cy; }
        else { u=p[k].x-cx; v=p[k].z-cz; depth=-(p[k].y-cy); }
        q[k].x = (u/extent + 0.5f) * resolution;
        q[k].y = (1.0f - (v/extent + 0.5f)) * resolution;
        d[k] = fminf(1.0f, fmaxf(0.0f, depth/extent + 0.5f));
    }
    int minx=max(0,(int)floorf(fminf(q[0].x,fminf(q[1].x,q[2].x))));
    int maxx=min(resolution-1,(int)ceilf(fmaxf(q[0].x,fmaxf(q[1].x,q[2].x)))-1);
    int miny=max(0,(int)floorf(fminf(q[0].y,fminf(q[1].y,q[2].y))));
    int maxy=min(resolution-1,(int)ceilf(fmaxf(q[0].y,fmaxf(q[1].y,q[2].y)))-1);
    float denom=(q[1].y-q[2].y)*(q[0].x-q[2].x)+(q[2].x-q[1].x)*(q[0].y-q[2].y);
    if (fabsf(denom)<1e-12f) return;
    for (int y=miny; y<=maxy; ++y) for (int x=minx; x<=maxx; ++x) {
        float px=x+0.5f, py=y+0.5f;
        float a=((q[1].y-q[2].y)*(px-q[2].x)+(q[2].x-q[1].x)*(py-q[2].y))/denom;
        float b=((q[2].y-q[0].y)*(px-q[2].x)+(q[0].x-q[2].x)*(py-q[2].y))/denom;
        float c=1.0f-a-b;
        if (a>=-1e-6f && b>=-1e-6f && c>=-1e-6f) {
            float depth=a*d[0]+b*d[1]+c*d[2];
            unsigned long long packed=((unsigned long long)__float_as_uint(depth)<<32) | (unsigned int)(triangle+1);
            atomicMax(&pixels[view*resolution*resolution+y*resolution+x],packed);
        }
    }
}
'''


def percentile(values, q):
    values=sorted(values); p=(len(values)-1)*q; lo=math.floor(p); hi=math.ceil(p)
    return values[lo] if lo==hi else values[lo]*(hi-p)+values[hi]*(p-lo)


def stats(values):
    return {"n":len(values),"p50_ms":statistics.median(values),"p90_ms":percentile(values,.9),"min_ms":min(values),"max_ms":max(values)}


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def elapsed(fn):
    cp.cuda.Stream.null.synchronize(); start=time.perf_counter_ns(); value=fn(); cp.cuda.Stream.null.synchronize()
    return (time.perf_counter_ns()-start)/1e6,value


def project(vertices, view, center, extent, resolution):
    p=vertices-center
    if view==0: u,v,d=p[:,0],p[:,1],p[:,2]
    elif view==1: u,v,d=-p[:,0],p[:,1],-p[:,2]
    elif view==2: u,v,d=-p[:,2],p[:,1],p[:,0]
    elif view==3: u,v,d=p[:,2],p[:,1],-p[:,0]
    elif view==4: u,v,d=p[:,0],-p[:,2],p[:,1]
    else: u,v,d=p[:,0],p[:,2],-p[:,1]
    q=np.column_stack(((u/extent+.5)*resolution,(1-(v/extent+.5))*resolution)).astype(np.float32)
    return q,np.clip(d/extent+.5,0,1).astype(np.float32)


def cpu_opencv_core(vertices,faces,center,extent,resolution):
    masks=np.zeros((6,resolution,resolution),dtype=np.uint8)
    for view in range(6):
        q,d=project(vertices,view,center,extent,resolution)
        order=np.argsort(d[faces].mean(axis=1))
        for face_id in order:
            polygon=np.rint(q[faces[face_id]]-.5).astype(np.int32)
            cv2.fillConvexPoly(masks[view],polygon,255,lineType=cv2.LINE_8)
    return masks


def scalar_reference(vertices,faces,center,extent,resolution):
    packed=np.zeros((6,resolution,resolution),dtype=np.uint64)
    for view in range(6):
        q,d=project(vertices,view,center,extent,resolution)
        for face_id,ids in enumerate(faces):
            t=q[ids]; td=d[ids]
            minx=max(0,int(np.floor(t[:,0].min()))); maxx=min(resolution-1,int(np.ceil(t[:,0].max()))-1)
            miny=max(0,int(np.floor(t[:,1].min()))); maxy=min(resolution-1,int(np.ceil(t[:,1].max()))-1)
            denom=(t[1,1]-t[2,1])*(t[0,0]-t[2,0])+(t[2,0]-t[1,0])*(t[0,1]-t[2,1])
            if abs(float(denom))<1e-12: continue
            for y in range(miny,maxy+1):
                for x in range(minx,maxx+1):
                    px=np.float32(x+.5); py=np.float32(y+.5)
                    a=((t[1,1]-t[2,1])*(px-t[2,0])+(t[2,0]-t[1,0])*(py-t[2,1]))/denom
                    b=((t[2,1]-t[0,1])*(px-t[2,0])+(t[0,0]-t[2,0])*(py-t[2,1]))/denom
                    c=np.float32(1)-a-b
                    if a>=-1e-6 and b>=-1e-6 and c>=-1e-6:
                        depth=np.float32(a*td[0]+b*td[1]+c*td[2])
                        bits=np.asarray(depth,dtype=np.float32).view(np.uint32).item()
                        value=(int(bits)<<32)|(face_id+1)
                        if value>int(packed[view,y,x]): packed[view,y,x]=value
    return packed


def gpu_run(kernel,vertices,faces,center,extent,resolution):
    gv=cp.asarray(np.ascontiguousarray(vertices,dtype=np.float32)); gf=cp.asarray(np.ascontiguousarray(faces,dtype=np.uint32))
    packed=cp.zeros(6*resolution*resolution,dtype=cp.uint64); blocks=(faces.shape[0]+255)//256
    packed.fill(0)
    for view in range(6):
        kernel((blocks,),(256,),(gv,gf,packed,np.int32(faces.shape[0]),np.int32(resolution),np.int32(view),np.float32(center[0]),np.float32(center[1]),np.float32(center[2]),np.float32(extent)))
    cp.cuda.Stream.null.synchronize()
    return packed.get().reshape(6,resolution,resolution)


def correctness_tests(kernel):
    tests=[]
    # Full-frame quad, overlaps/depth ties/reversed winding, offscreen and degenerate.
    suites=[
        ("full_frame_quad",np.array([[-1,-1,0],[1,-1,0],[1,1,0],[-1,1,0]],np.float32),np.array([[0,1,2],[0,2,3]],np.uint32),32),
        ("overlap_reverse_winding",np.array([[-.8,-.8,-.2],[.8,-.8,-.2],[0,.8,-.2],[-.7,-.7,.3],[0,.7,.3],[.7,-.7,.3]],np.float32),np.array([[2,1,0],[3,4,5]],np.uint32),32),
        ("exact_depth_tie_higher_id",np.array([[-.7,-.7,.2],[.7,-.7,.2],[0,.7,.2]],np.float32),np.array([[0,1,2],[0,1,2]],np.uint32),32),
        ("partly_offscreen_and_degenerate",np.array([[-1.1,-1.1,.1],[.4,-1.1,.1],[-1.1,.4,.1],[0,0,0]],np.float32),np.array([[0,1,2],[3,3,3]],np.uint32),32),
    ]
    for name,vertices,faces,res in suites:
        center=np.zeros(3,np.float32); extent=np.float32(2.0)
        ref=scalar_reference(vertices,faces,center,extent,res); gpu=gpu_run(kernel,vertices,faces,center,extent,res)
        comparison=compare_packed(ref,gpu)
        comparison.update({"name":name,"covered_pixels":int((gpu!=0).sum())})
        tests.append(comparison)
    return tests


def compare_packed(reference,gpu):
    mask_equal=bool(np.array_equal(reference!=0,gpu!=0))
    ref_ids=(reference & np.uint64(0xffffffff)).astype(np.uint32)
    gpu_ids=(gpu & np.uint64(0xffffffff)).astype(np.uint32)
    id_equal=bool(np.array_equal(ref_ids,gpu_ids))
    active=(reference!=0)|(gpu!=0)
    ref_depth=(reference>>np.uint64(32)).astype(np.uint32).view(np.float32)
    gpu_depth=(gpu>>np.uint64(32)).astype(np.uint32).view(np.float32)
    max_error=float(np.max(np.abs(ref_depth[active]-gpu_depth[active]))) if active.any() else 0.0
    return {"packed_bit_equal":bool(np.array_equal(reference,gpu)),"mask_equal":mask_equal,"triangle_id_equal":id_equal,"depth_max_abs_error":max_error,"passed":mask_equal and id_equal and max_error<=1e-6}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--manifest",type=Path,required=True)
    parser.add_argument("--work-dir",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--repeats",type=int,default=25)
    args=parser.parse_args()
    cases=json.loads(args.manifest.read_text(encoding="utf-8"))["cases"]
    compile_start=time.perf_counter_ns()
    module=cp.RawModule(code=CUDA_SOURCE,options=("--std=c++11",),name_expressions=("raster_triangles",))
    kernel=module.get_function("raster_triangles")
    cp.cuda.Stream.null.synchronize()
    compile_ms=(time.perf_counter_ns()-compile_start)/1e6
    props=cp.cuda.runtime.getDeviceProperties(0)
    report={
        "scope":"real current-rig head-weight ROI CUDA raster; diagnostic IDs only",
        "repeats":args.repeats,
        "manifest_sha256":sha256(args.manifest),
        "source_sha256":sha256(Path(__file__)),
        "runtime":{"cupy":cp.__version__,"cuda_runtime":cp.cuda.runtime.runtimeGetVersion(),"gpu":props["name"].decode(),"module_compile_ms":compile_ms},
        "correctness_tests":correctness_tests(kernel),
        "depth_tie_rule":"packed atomic max: greater float32 depth wins; exact depth tie chooses greater diagnostic triangle ID",
        "camera_bases":"front(+X,+Y,+Zdepth), back(-X,+Y,-Z), left(-Z,+Y,+X), right(+Z,+Y,-X), top(+X,-Z,+Y), bottom(+X,+Z,-Y)",
        "cases":[],
    }
    for row in cases:
        case_id=row["case_id"]; folder=args.work_dir/case_id/"benchmark-output"
        roi=np.load(folder/"rig_head_roi.npz"); projected=np.load(folder/"head_roi.npz")
        vertices=np.ascontiguousarray(roi["vertices"],dtype=np.float32); faces=np.ascontiguousarray(roi["faces"],dtype=np.uint32)
        cpu_masks=projected["masks"].astype(bool); center=projected["center"].astype(np.float32); extent=float(projected["extent"]); resolution=cpu_masks.shape[1]
        if not np.isfinite(vertices).all() or not np.isfinite(extent) or extent<=0:
            raise ValueError(f"{case_id}: non-finite vertices/extent")
        if faces.size==0 or faces.min()<0 or faces.max()>=vertices.shape[0]:
            raise ValueError(f"{case_id}: empty or out-of-range faces")
        cpu_start=time.perf_counter_ns(); cpu_core_masks=cpu_opencv_core(vertices,faces,center,extent,resolution); cpu_core_ms=(time.perf_counter_ns()-cpu_start)/1e6
        transfer_ms,(gpu_vertices,gpu_faces)=elapsed(lambda:(cp.asarray(vertices),cp.asarray(faces)))
        allocation_ms,packed=elapsed(lambda:cp.zeros(6*resolution*resolution,dtype=cp.uint64))
        blocks=(faces.shape[0]+255)//256
        def run():
            packed.fill(0)
            for view in range(6):
                kernel((blocks,),(256,),(gpu_vertices,gpu_faces,packed,np.int32(faces.shape[0]),np.int32(resolution),np.int32(view),np.float32(center[0]),np.float32(center[1]),np.float32(center[2]),np.float32(extent)))
            return packed
        first_raster_ms,_=elapsed(run); warm=[]
        for _ in range(args.repeats): ms,_=elapsed(run); warm.append(ms)
        download_ms,gpu_packed=elapsed(lambda:packed.get().reshape(6,resolution,resolution))
        gpu_masks=gpu_packed!=0
        ious=[]
        for view in range(6):
            union=np.logical_or(cpu_core_masks[view]!=0,gpu_masks[view]).sum(); intersection=np.logical_and(cpu_core_masks[view]!=0,gpu_masks[view]).sum()
            ious.append(float(intersection/union) if union else 1.0)
            import cv2
            cv2.imwrite(str(folder/f"gpu_{view}_mask.png"),gpu_masks[view].astype(np.uint8)*255)
        subset_count=min(128,faces.shape[0]); subset_faces=faces[np.linspace(0,faces.shape[0]-1,subset_count,dtype=np.int64)]
        scalar=scalar_reference(vertices,subset_faces,center,extent,64); gpu_subset=gpu_run(kernel,vertices,subset_faces,center,extent,64)
        subset_validation=compare_packed(scalar,gpu_subset); subset_validation.update({"triangles":subset_count,"resolution":64})
        report["cases"].append({"case_id":case_id,"triangles":int(faces.shape[0]),"vertices":int(vertices.shape[0]),"resolution":resolution,"cpu_opencv_core_six_views_ms":cpu_core_ms,"h2d_ms":transfer_ms,"allocation_ms":allocation_ms,"first_raster_ms":first_raster_ms,"warm_six_views":stats(warm),"d2h_ms":download_ms,"cold_same_work_excluding_module_compile_and_file_io_ms":transfer_ms+allocation_ms+first_raster_ms+download_ms,"silhouette_iou_per_view":ious,"silhouette_iou_mean":statistics.fmean(ious),"actual_subset_scalar_validation":subset_validation,"semantic_limit":"Head-bone weight ROI, not hair; CUDA interpolated z-buffer compared with same-process OpenCV integer-fill silhouette baseline."})
    args.output.write_text(json.dumps(report,indent=2),encoding="utf-8")
    print(json.dumps({"cases":len(report["cases"]),"output":str(args.output)}))


if __name__=="__main__": main()
