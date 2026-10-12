#!/usr/bin/env python3
"""Unsigned distance proxy from the largest exact Face surface component."""

from __future__ import annotations

import argparse, hashlib, json, time
from pathlib import Path

import cv2
import numpy as np
import trimesh
from scipy.ndimage import distance_transform_edt


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def weld_and_components(vertices,faces,tolerance=1e-6):
    quantized=np.rint(vertices/tolerance).astype(np.int64); welded,inverse=np.unique(quantized,axis=0,return_inverse=True); welded=welded.astype(np.float64)*tolerance; welded_faces=inverse[faces]
    vertex_faces={}
    for face_id,face in enumerate(welded_faces):
        for vertex in face: vertex_faces.setdefault(int(vertex),[]).append(face_id)
    unseen=set(range(len(welded_faces))); components=[]
    while unseen:
        seed=unseen.pop(); ids=[seed]; stack=[seed]
        while stack:
            for vertex in welded_faces[stack.pop()]:
                for neighbor in vertex_faces[int(vertex)]:
                    if neighbor in unseen: unseen.remove(neighbor); stack.append(neighbor); ids.append(neighbor)
        components.append(np.asarray(ids,np.int64))
    components.sort(key=len,reverse=True); ids=components[0]; selected=welded_faces[ids]; used,remap=np.unique(selected.reshape(-1),return_inverse=True)
    return welded[used],remap.reshape(-1,3),[len(x) for x in components]


def sample_field(distance,bounds_min,pitch,points):
    coords=np.floor((points-bounds_min)/pitch).astype(int); valid=np.all((coords>=0)&(coords<distance.shape[0]),axis=1); clipped=np.clip(coords,0,distance.shape[0]-1); result=np.full(len(points),np.inf); result[valid]=distance[tuple(clipped[valid].T)]; return result


def raster_mask(vertices,faces,resolution,center,extent,u,v):
    q=np.column_stack((((vertices[:,u]-center[u])/extent+.5),1-((vertices[:,v]-center[v])/extent+.5)))*resolution; image=np.zeros((resolution,resolution),np.uint8)
    for face in faces: cv2.fillConvexPoly(image,np.rint(q[face]-.5).astype(np.int32),255,lineType=cv2.LINE_8)
    return image>0


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--head",type=Path,required=True); parser.add_argument("--guides",type=Path,required=True); parser.add_argument("--shell",type=Path,required=True); parser.add_argument("--gates",type=Path,required=True); parser.add_argument("--output-dir",type=Path,required=True); parser.add_argument("--resolutions",default="96,128")
    args=parser.parse_args(); args.output_dir.mkdir(parents=True,exist_ok=True); head=json.loads(args.head.read_text()); guides=json.loads(args.guides.read_text()); shell=json.loads(args.shell.read_text()); names={m["id"]:m["name"] for m in head["materials"]}; primitive=next(p for p in head["primitives"] if names[p["materialId"]]=="Face"); vertices=np.asarray(primitive["positions"],float).reshape(-1,3); faces=np.asarray(primitive["indices"],int).reshape(-1,3); main_vertices,main_faces,component_sizes=weld_and_components(vertices,faces); mesh=trimesh.Trimesh(main_vertices,main_faces,process=False); center=(main_vertices.min(0)+main_vertices.max(0))/2; extent=max((main_vertices.max(0)-main_vertices.min(0)).max()*1.16,1e-9); bounds_min=center-extent/2
    roots=np.asarray([g["root"] for g in guides["guides"]],float); nonroots=np.asarray([s["position"] for g in guides["guides"] for s in g["samples"][1:]],float); shell_vertices=np.asarray(shell["mesh"]["positions"],float).reshape(-1,3); shell_faces=np.asarray(shell["mesh"]["indices"],int).reshape(-1,3); gate=json.loads(args.gates.read_text()); case=next(x for x in gate["gates"] if x["case_id"]==head["source"]["caseId"]); selector=next(x for x in case["source_primitives"] if x["material_name"]=="Face")
    outputs=[]
    for resolution in map(int,args.resolutions.split(',')):
        pitch=extent/resolution; started=time.perf_counter(); voxelized=mesh.voxelized(pitch,method="subdivide"); surface=np.zeros((resolution,resolution,resolution),bool); coords=np.floor((voxelized.points-bounds_min)/pitch).astype(int); valid=np.all((coords>=0)&(coords<resolution),axis=1); coords=coords[valid]; surface[tuple(coords.T)]=True; distance=distance_transform_edt(~surface)*pitch; build_ms=(time.perf_counter()-started)*1000
        root_distance=sample_field(distance,bounds_min,pitch,roots); nonroot_distance=sample_field(distance,bounds_min,pitch,nonroots); shell_distance=sample_field(distance,bounds_min,pitch,shell_vertices); centroid_distance=sample_field(distance,bounds_min,pitch,shell_vertices[shell_faces].mean(1)); radius=1.5*pitch
        path=args.output_dir/f"face-surface-distance-{resolution}.npz"; np.savez_compressed(path,distance=distance.astype(np.float32),surface=surface,bounds_min=bounds_min.astype(np.float32),pitch=np.float32(pitch))
        finite_face=sample_field(distance,bounds_min,pitch,main_vertices)
        def distance_summary(values):
            finite=values[np.isfinite(values)]; return {"inGridFraction":float(np.mean(np.isfinite(values))),"p50InGrid":float(np.median(finite)) if len(finite) else None,"p90InGrid":float(np.percentile(finite,90)) if len(finite) else None,"withinRadiusFractionOfAll":float(np.mean(values<=radius))}
        outputs.append({"resolution":resolution,"pitchNormalized":pitch,"buildMilliseconds":build_ms,"surfaceVoxels":int(surface.sum()),"artifactSha256":sha256(path),"collisionRadiusNormalized":radius,"faceVertexDistance":{"p50":float(np.median(finite_face)),"p90":float(np.percentile(finite_face,90)),"max":float(finite_face.max())},"guides":{"roots":len(roots),"nonRootSamples":len(nonroots),"rootDistance":distance_summary(root_distance),"nonRootDistance":distance_summary(nonroot_distance)},"hairShell":{"vertices":distance_summary(shell_distance),"triangleCentroids":distance_summary(centroid_distance)}})
    exact_path=args.output_dir/"largest-face-component.glb"; mesh.export(exact_path); resolution=512; exact_masks=[raster_mask(main_vertices,main_faces,resolution,center,extent,*axes) for axes in ((0,1),(2,1),(0,2))]; finest=np.load(args.output_dir/f"face-surface-distance-{max(map(int,args.resolutions.split(',')))}.npz"); surface=finest["surface"]; proxy_masks=[surface.max(axis=2).T[::-1],surface.max(axis=0)[::-1],surface.max(axis=1).T[::-1]]; tiles=[]
    for exact,proxy,label in zip(exact_masks,proxy_masks,("XY","ZY","XZ")):
        proxy=cv2.resize(proxy.astype(np.uint8),(resolution,resolution),interpolation=cv2.INTER_NEAREST).astype(bool); tile=np.zeros((resolution,resolution,3),np.uint8); tile[exact,1]=255; tile[proxy,0]=255; tile[proxy,2]=255; cv2.putText(tile,f"{label}: Face green surface voxels magenta",(8,26),cv2.FONT_HERSHEY_SIMPLEX,.55,(0,180,255),2,cv2.LINE_AA); tiles.append(tile)
    cv2.imwrite(str(args.output_dir/"face-surface-distance-overlay.png"),cv2.hconcat(tiles)); output={"schema":"autorig.hair.unsigned-face-distance/1","source":{"caseId":head["source"]["caseId"],"taskId":head["source"]["taskId"],"sha256":head["source"]["sha256"],"headNeutralSha256":sha256(args.head),"guidesSha256":sha256(args.guides),"hairShellSha256":sha256(args.shell),"gateSha256":sha256(args.gates),"selector":{"meshId":selector["mesh_id"],"primitiveId":selector["primitive_id"],"globalTriangleStart":selector["global_triangle_start"],"globalTriangleEndExclusive":selector["global_triangle_end_exclusive"],"materialName":"Face"}},"coordinates":head["coordinates"],"componentSelection":{"method":"largest vertex-connected component after exact-position weld 1e-6","allComponentTriangleCounts":component_sizes,"selectedTriangles":len(main_faces),"selectedVertices":len(main_vertices),"anatomicalTruth":False},"field":{"kind":"unsigned distance to voxelized exact surface","signedInsideAvailable":False,"collisionResponse":"nearest-surface proximity only; cannot detect deep inside/outside state","selfIntersectionsChecked":False},"resolutions":outputs}; (args.output_dir/"report.json").write_text(json.dumps(output,indent=2),encoding="utf-8"); print(json.dumps({"resolutions":len(outputs),"output":str(args.output_dir/"report.json")}))


if __name__=="__main__": main()
