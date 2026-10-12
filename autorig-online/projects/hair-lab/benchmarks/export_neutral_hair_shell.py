#!/usr/bin/env python3
"""Export QEM hair shell in the frozen normalized Head coordinate contract."""

from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path

import numpy as np
import trimesh


def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return digest.hexdigest()


def topology_analysis(head,tolerance=1e-6):
    positions=[]; faces=[]; offset=0
    for primitive in head["primitives"]:
        vertices=np.asarray(primitive["positions"],np.float64).reshape(-1,3); indices=np.asarray(primitive["indices"],np.int64).reshape(-1,3)
        positions.append(vertices); faces.append(indices+offset); offset+=len(vertices)
    positions=np.concatenate(positions); faces=np.concatenate(faces)
    quantized=np.rint(positions/tolerance).astype(np.int64); unique,inverse=np.unique(quantized,axis=0,return_inverse=True); welded=unique.astype(np.float64)*tolerance; welded_faces=inverse[faces]
    triangles=welded[welded_faces]; area2=np.linalg.norm(np.cross(triangles[:,1]-triangles[:,0],triangles[:,2]-triangles[:,0]),axis=1); degenerate=area2<=1e-12
    edges=np.concatenate((welded_faces[:,[0,1]],welded_faces[:,[1,2]],welded_faces[:,[2,0]])); canonical=np.sort(edges,axis=1); unique_edges,counts=np.unique(canonical,axis=0,return_counts=True)
    boundary=unique_edges[counts==1]; nonmanifold=unique_edges[counts>2]
    paired=unique_edges[counts==2]; orientation_conflicts=0
    if len(paired):
        edge_map={tuple(edge):[] for edge in paired.tolist()}
        for a,b in edges:
            key=(min(int(a),int(b)),max(int(a),int(b)))
            if key in edge_map: edge_map[key].append((int(a),int(b)))
        orientation_conflicts=sum(1 for values in edge_map.values() if len(values)==2 and values[0]==values[1])
    adjacency={}
    for a,b in boundary:
        adjacency.setdefault(int(a),set()).add(int(b)); adjacency.setdefault(int(b),set()).add(int(a))
    boundary_components=0; seen=set()
    for start in adjacency:
        if start in seen: continue
        boundary_components+=1; stack=[start]; seen.add(start)
        while stack:
            for nxt in adjacency[stack.pop()]:
                if nxt not in seen: seen.add(nxt); stack.append(nxt)
    signed_volume=float(np.einsum("ij,ij->i",triangles[:,0],np.cross(triangles[:,1],triangles[:,2])).sum()/6.0)
    closed=bool(len(boundary)==0 and len(nonmanifold)==0 and int(degenerate.sum())==0 and orientation_conflicts==0)
    vertex_faces={}
    for face_id,face in enumerate(welded_faces):
        for vertex in face: vertex_faces.setdefault(int(vertex),[]).append(face_id)
    components=[]; unseen=set(range(len(welded_faces)))
    while unseen:
        seed=unseen.pop(); stack=[seed]; ids=[seed]
        while stack:
            face_id=stack.pop()
            for vertex in welded_faces[face_id]:
                for neighbor in vertex_faces[int(vertex)]:
                    if neighbor in unseen: unseen.remove(neighbor); stack.append(neighbor); ids.append(neighbor)
        component_faces=welded_faces[np.asarray(ids)]; component_triangles=welded[component_faces]; component_edges=np.concatenate((component_faces[:,[0,1]],component_faces[:,[1,2]],component_faces[:,[2,0]])); component_canonical=np.sort(component_edges,axis=1); component_unique,component_counts=np.unique(component_canonical,axis=0,return_counts=True); component_boundary=component_unique[component_counts==1]; component_nonmanifold=component_unique[component_counts>2]
        used=np.unique(component_faces); comp_conflicts=0
        paired_keys={tuple(edge):[] for edge in component_unique[component_counts==2].tolist()}
        for a,b in component_edges:
            key=(min(int(a),int(b)),max(int(a),int(b)))
            if key in paired_keys: paired_keys[key].append((int(a),int(b)))
        comp_conflicts=sum(1 for values in paired_keys.values() if len(values)==2 and values[0]==values[1]); comp_degenerate=int((np.linalg.norm(np.cross(component_triangles[:,1]-component_triangles[:,0],component_triangles[:,2]-component_triangles[:,0]),axis=1)<=1e-12).sum()); comp_closed=bool(len(component_boundary)==0 and len(component_nonmanifold)==0 and comp_conflicts==0 and comp_degenerate==0)
        components.append({"triangles":len(ids),"vertices":int(len(used)),"bounds":{"min":welded[used].min(0).tolist(),"max":welded[used].max(0).tolist()},"boundary_edges":int(len(component_boundary)),"nonmanifold_edges":int(len(component_nonmanifold)),"orientation_conflicts":comp_conflicts,"degenerate_triangles":comp_degenerate,"closed_orientable_manifold":comp_closed})
    components.sort(key=lambda item:item["triangles"],reverse=True)
    return {"input_vertices":int(len(positions)),"triangles":int(len(faces)),"position_weld_tolerance":tolerance,"welded_vertices":int(len(welded)),"face_connected_components":len(components),"components":components,"degenerate_triangles":int(degenerate.sum()),"unique_edges":int(len(unique_edges)),"boundary_edges":int(len(boundary)),"boundary_components_or_hole_chains":boundary_components,"nonmanifold_edges":int(len(nonmanifold)),"paired_edge_orientation_conflicts":orientation_conflicts,"signed_volume":signed_volume,"closed_orientable_manifold":closed,"signed_inside_queries_safe":closed,"self_intersections_checked":False,"verdict":"mesh passes topology preconditions for signed queries, but self-intersections remain unchecked" if closed else "reject signed inside/outside queries; use only unsigned nearest-surface tests or separately validated collision primitives"}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--shell",type=Path,required=True); parser.add_argument("--source-shell",type=Path,required=True); parser.add_argument("--mapping",type=Path,required=True); parser.add_argument("--guides",type=Path,required=True); parser.add_argument("--head",type=Path,required=True); parser.add_argument("--gates",type=Path,required=True); parser.add_argument("--output",type=Path,required=True); parser.add_argument("--receipt",type=Path,required=True); parser.add_argument("--collider-report",type=Path,required=True)
    args=parser.parse_args(); guides=json.loads(args.guides.read_text()); head=json.loads(args.head.read_text()); mesh=trimesh.load(args.shell,force="mesh",process=False)
    coordinates=guides["coordinates"]; matrix=np.asarray(coordinates["sourceToNormalized"],np.float64).reshape(4,4,order="F"); vertices=np.asarray(mesh.vertices,np.float64); homogeneous=np.column_stack((vertices,np.ones(len(vertices)))); normalized=(matrix@homogeneous.T).T[:,:3]
    normal_matrix=np.linalg.inv(matrix[:3,:3]).T; normals=(normal_matrix@np.asarray(mesh.vertex_normals,np.float64).T).T; normals/=np.maximum(np.linalg.norm(normals,axis=1,keepdims=True),1e-20)
    uv=getattr(mesh.visual,"uv",None)
    if uv is None or len(uv)!=len(vertices) or not np.isfinite(uv).all(): raise ValueError("finite per-vertex UV attribute required")
    faces=np.asarray(mesh.faces,np.uint32); approximate=np.load(args.mapping).astype(np.uint32)
    if len(approximate)!=len(faces): raise ValueError("source correspondence count mismatch")
    material=guides["material"]
    output={"schema":"autorig.hair.shell/1","source":{"caseId":guides["source"]["caseId"],"taskId":guides["source"]["taskId"],"sha256":guides["source"]["sha256"],"primitiveIndex":guides["source"]["primitiveIndex"],"material":guides["source"]["material"],"derivation":"position weld at bbox diagonal * 1e-7, Blender 5.2 collapse QEM target 2500","sourceMeshSha256":sha256(args.source_shell),"reducedArtifactSha256":sha256(args.shell)},"coordinates":coordinates,"material":material,"mesh":{"positions":normalized.astype(np.float32).reshape(-1).tolist(),"normals":normals.astype(np.float32).reshape(-1).tolist(),"uv":np.asarray(uv,np.float32).reshape(-1).tolist(),"indices":faces.reshape(-1).tolist()},"sourceCorrespondence":{"kind":"approximate_nearest_source_triangle_centroid","sourcePrimitiveLocalTriangleIds":approximate.tolist(),"authoritative":False,"warning":"QEM creates new triangles; these IDs are approximate against the Blender-isolated source shell and are not source mutation/backprojection authority"},"semanticGate":{"replacementAllowed":False,"originalHairMustRemainVisible":True}}
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(output,separators=(",",":")),encoding="utf-8")
    gates=json.loads(args.gates.read_text()); gate=next(item for item in gates["gates"] if item["case_id"]==head["source"]["caseId"]); source_by_material={item["material_name"]:item for item in gate["source_primitives"]}; material_by_id={item["id"]:item["name"] for item in head["materials"]}; per_primitive=[]
    for primitive in head["primitives"]:
        name=material_by_id[primitive["materialId"]]; selector=source_by_material.get(name); per_primitive.append({"headNeutralMaterialId":primitive["materialId"],"materialName":name,"sourceSelector":({"meshId":selector["mesh_id"],"primitiveId":selector["primitive_id"],"globalTriangleStart":selector["global_triangle_start"],"globalTriangleEndExclusive":selector["global_triangle_end_exclusive"]} if selector else None),"topology":topology_analysis({"primitives":[primitive]})})
    collider={"schema":"autorig.hair.head-collider-analysis/2","source":{"caseId":head["source"]["caseId"],"taskId":head["source"]["taskId"],"sha256":head["source"]["sha256"],"headNeutralSha256":sha256(args.head),"gateSha256":sha256(args.gates)},"coordinates":head["coordinates"],"combinedTopology":topology_analysis(head),"perPrimitive":per_primitive,"selectionWarning":"Mask, Face and Eyes are exact material primitives from the non-hair head bundle; material names do not prove anatomical scalp/head collision membership.","collisionPrimitives":{"spheres":[],"reason":"no measured collision spheres are exported; fitted ellipsoids/spheres are estimates, not actual mesh collision evidence"}}
    args.collider_report.write_text(json.dumps(collider,indent=2),encoding="utf-8"); receipt={"neutralShellSha256":sha256(args.output),"colliderReportSha256":sha256(args.collider_report),"reducedArtifactSha256":sha256(args.shell),"sourceShellSha256":sha256(args.source_shell),"vertices":len(vertices),"triangles":len(faces),"uvCount":len(uv),"coordinateSchema":coordinates["convention"]}; args.receipt.write_text(json.dumps(receipt,indent=2),encoding="utf-8"); print(json.dumps(receipt))


if __name__=="__main__": main()
