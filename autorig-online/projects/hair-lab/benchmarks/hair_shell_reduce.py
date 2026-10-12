#!/usr/bin/env python3
"""Geometry-preserving reduction study for one exact Hair primitive.

Normal Python orchestrates Blender. Blender mode isolates a material primitive,
applies QEM Decimate at bounded targets, exports GLBs and renders comparisons.
Reduced triangles receive only approximate nearest-source mappings; exact
source IDs are retained for the untouched source shell receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path


def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return digest.hexdigest()


def blender_main(argv):
    import bpy, bmesh
    from mathutils import Vector
    parser=argparse.ArgumentParser(); parser.add_argument("--source",required=True); parser.add_argument("--output-dir",required=True); parser.add_argument("--material",default="Hair"); parser.add_argument("--targets",default="2500,1500,1000")
    args=parser.parse_args(argv); output=Path(args.output_dir); output.mkdir(parents=True,exist_ok=True); targets=[int(x) for x in args.targets.split(",")]
    started=time.perf_counter(); bpy.ops.wm.read_factory_settings(use_empty=True); import_start=time.perf_counter(); bpy.ops.import_scene.gltf(filepath=args.source); import_seconds=time.perf_counter()-import_start
    candidates=[]
    for obj in bpy.context.scene.objects:
        if obj.type!="MESH": continue
        for slot_index,slot in enumerate(obj.material_slots):
            if slot.material and slot.material.name.lower()==args.material.lower(): candidates.append((obj,slot_index))
    if len(candidates)!=1: raise RuntimeError(f"expected exactly one {args.material} material object/slot, got {len(candidates)}")
    original,material_index=candidates[0]; source=original.copy(); source.data=original.data.copy(); bpy.context.collection.objects.link(source); source.name="Hair_Source_Primitive"
    source.modifiers.clear()
    bm=bmesh.new(); bm.from_mesh(source.data); remove=[face for face in bm.faces if face.material_index!=material_index]; bmesh.ops.delete(bm,geom=remove,context="FACES"); loose=[v for v in bm.verts if not v.link_faces]; bmesh.ops.delete(bm,geom=loose,context="VERTS"); bm.to_mesh(source.data); bm.free(); source.data.update()
    for obj in list(bpy.context.scene.objects): obj.hide_render=True
    def export_object(obj,path):
        bpy.ops.object.select_all(action="DESELECT"); obj.select_set(True); bpy.context.view_layer.objects.active=obj
        bpy.ops.export_scene.gltf(filepath=str(path),export_format="GLB",use_selection=True,export_materials="EXPORT",export_texcoords=True,export_normals=True,export_yup=True)
    source.data.calc_loop_triangles(); source_path=output/"hair-source-primitive.glb"; export_object(source,source_path)
    variants=[]; source_triangles=len(source.data.loop_triangles)
    for target in targets:
        duplicate=source.copy(); duplicate.data=source.data.copy(); bpy.context.collection.objects.link(duplicate); duplicate.name=f"Hair_QEM_{target}"
        before=len(duplicate.data.loop_triangles); vertices_before=len(duplicate.data.vertices)
        weld=bmesh.new(); weld.from_mesh(duplicate.data); bounds=[Vector(c) for c in duplicate.bound_box]; diagonal=max((a-b).length for a in bounds for b in bounds); bmesh.ops.remove_doubles(weld,verts=list(weld.verts),dist=max(diagonal*1e-7,1e-9)); weld.to_mesh(duplicate.data); weld.free(); duplicate.data.update(); vertices_after_weld=len(duplicate.data.vertices)
        modifier=duplicate.modifiers.new("HairShellQEM","DECIMATE"); modifier.decimate_type="COLLAPSE"; modifier.ratio=min(1.0,target/max(before,1)); modifier.use_collapse_triangulate=True
        reduce_start=time.perf_counter(); bpy.context.view_layer.objects.active=duplicate; bpy.ops.object.modifier_apply(modifier=modifier.name); reduce_seconds=time.perf_counter()-reduce_start
        clean=bmesh.new(); clean.from_mesh(duplicate.data); loose=[v for v in clean.verts if not v.link_faces]; bmesh.ops.delete(clean,geom=loose,context="VERTS"); clean.to_mesh(duplicate.data); clean.free(); duplicate.data.update()
        duplicate.data.calc_loop_triangles(); path=output/f"hair-qem-{target}.glb"; export_object(duplicate,path)
        variants.append({"target_triangles":target,"actual_triangles":len(duplicate.data.loop_triangles),"vertices_before_weld":vertices_before,"vertices_after_position_weld":vertices_after_weld,"vertices":len(duplicate.data.vertices),"weld_distance":max(diagonal*1e-7,1e-9),"reduction_seconds":reduce_seconds,"path":str(path)})
    # Render source plus variants from front and side, using existing material.
    display=[source]+[bpy.data.objects[f"Hair_QEM_{target}"] for target in targets]
    widths=[]
    for obj in display:
        corners=[obj.matrix_world@Vector(corner) for corner in obj.bound_box]; widths.append(max(c.x for c in corners)-min(c.x for c in corners))
    spacing=max(widths)*1.35; center_index=(len(display)-1)/2
    original_locations={obj.name:obj.location.copy() for obj in display}
    for i,obj in enumerate(display): obj.hide_render=False; obj.location.x+=(i-center_index)*spacing
    bpy.ops.object.camera_add(); camera=bpy.context.object; bpy.context.scene.camera=camera
    all_corners=[obj.matrix_world@Vector(c) for obj in display for c in obj.bound_box]; center=sum(all_corners,Vector())/len(all_corners); extent=max(max(c.x for c in all_corners)-min(c.x for c in all_corners),max(c.z for c in all_corners)-min(c.z for c in all_corners))
    camera.data.type="ORTHO"; camera.data.ortho_scale=extent*1.12
    scene=bpy.context.scene; scene.world=scene.world or bpy.data.worlds.new("HairShellWorld"); scene.world.use_nodes=True; background=scene.world.node_tree.nodes.get("Background"); background.inputs["Color"].default_value=(0.06,0.06,0.06,1); background.inputs["Strength"].default_value=0.65; scene.render.engine="BLENDER_EEVEE"; scene.render.resolution_x=1400; scene.render.resolution_y=500; scene.render.resolution_percentage=100; scene.render.image_settings.file_format="PNG"; scene.render.film_transparent=False
    bpy.ops.object.light_add(type="AREA"); light=bpy.context.object; light.data.energy=1200; light.data.shape="DISK"; light.data.size=5; light.location=(center.x-2,center.y-4,center.z+4)
    bpy.ops.object.light_add(type="AREA"); fill=bpy.context.object; fill.data.energy=900; fill.data.size=4; fill.location=(center.x+3,center.y-2,center.z+1)
    camera.location=(center.x,center.y-6,center.z); direction=center-camera.location; camera.rotation_euler=direction.to_track_quat("-Z","Y").to_euler(); scene.render.filepath=str(output/"comparison-front.png"); bpy.ops.render.render(write_still=True)
    for i,obj in enumerate(display): obj.location=original_locations[obj.name]+Vector((0,(i-center_index)*spacing,0))
    all_corners=[obj.matrix_world@Vector(c) for obj in display for c in obj.bound_box]; center=sum(all_corners,Vector())/len(all_corners); camera.data.ortho_scale=max(max(c.y for c in all_corners)-min(c.y for c in all_corners),max(c.z for c in all_corners)-min(c.z for c in all_corners))*1.12; camera.location=(center.x+6,center.y,center.z); direction=center-camera.location; camera.rotation_euler=direction.to_track_quat("-Z","Y").to_euler(); scene.render.filepath=str(output/"comparison-side.png"); bpy.ops.render.render(write_still=True)
    receipt={"source_sha256":sha256(Path(args.source)),"source_material":args.material,"selection":"exact Blender material slot, source primitive candidate only","semantic_acceptance":False,"source_triangles":source_triangles,"source_export":str(source_path),"variants":variants,"timing_seconds":{"import":import_seconds,"total":time.perf_counter()-started}}
    (output/"blender-receipt.json").write_text(json.dumps(receipt,indent=2),encoding="utf-8"); print(json.dumps(receipt))


def masks(mesh,resolution=512,center=None,extent=None):
    import cv2, numpy as np
    vertices=np.asarray(mesh.vertices,float); faces=np.asarray(mesh.faces,int); center=np.asarray(center if center is not None else (vertices.min(0)+vertices.max(0))/2,float); extent=float(extent if extent is not None else max((vertices.max(0)-vertices.min(0)).max()*1.05,1e-9)); result=[]
    bases=((0,1),(0,1),(2,1),(2,1),(0,2),(0,2))
    for u,v in bases:
        q=np.column_stack(((vertices[:,u]-center[u])/extent+.5,1-((vertices[:,v]-center[v])/extent+.5)))*resolution; image=np.zeros((resolution,resolution),np.uint8)
        for face in faces: cv2.fillConvexPoly(image,np.rint(q[face]-.5).astype(np.int32),255,lineType=cv2.LINE_8)
        result.append(image>0)
    return np.stack(result)


def normal_main(argv):
    import cv2, numpy as np, trimesh
    from scipy.spatial import cKDTree
    parser=argparse.ArgumentParser(); parser.add_argument("--source",type=Path,required=True); parser.add_argument("--output-dir",type=Path,required=True); parser.add_argument("--blender",type=Path,required=True); parser.add_argument("--targets",default="2500,1500,1000")
    args=parser.parse_args(argv); args.output_dir.mkdir(parents=True,exist_ok=True); script=Path(__file__).resolve(); start=time.perf_counter()
    completed=subprocess.run([str(args.blender),"--background","--python",str(script),"--","--source",str(args.source),"--output-dir",str(args.output_dir),"--material","Hair","--targets",args.targets],capture_output=True,text=True,check=True)
    receipt=json.loads((args.output_dir/"blender-receipt.json").read_text()); source=trimesh.load(args.output_dir/"hair-source-primitive.glb",force="mesh",process=False); source_center=(source.bounds[0]+source.bounds[1])/2; source_extent=max(source.extents.max()*1.05,1e-9); source_masks=masks(source,center=source_center,extent=source_extent); source_diag=float(np.linalg.norm(source.extents)); source_centroids=source.triangles_center; results=[]
    source_uv=getattr(source.visual,"uv",None); all_masks=[source_masks]; source_uv_finite=bool(source_uv is not None and len(source_uv)==len(source.vertices) and np.isfinite(source_uv).all())
    for variant in receipt["variants"]:
        path=Path(variant["path"]); mesh=trimesh.load(path,force="mesh",process=False); variant_masks=masks(mesh,center=source_center,extent=source_extent); all_masks.append(variant_masks); ious=[]
        for a,b in zip(source_masks,variant_masks):
            union=np.logical_or(a,b).sum(); ious.append(float(np.logical_and(a,b).sum()/union) if union else 1.0)
        forward=cKDTree(source.vertices).query(mesh.vertices,k=1)[0]; backward=cKDTree(mesh.vertices).query(source.vertices,k=1)[0]; tri_distance=cKDTree(source_centroids).query(mesh.triangles_center,k=1)[0]
        mapping=np.asarray(cKDTree(source_centroids).query(mesh.triangles_center,k=1)[1],np.uint32); np.save(args.output_dir/f"hair-qem-{variant['target_triangles']}-nearest-source-triangle.npy",mapping)
        uv=getattr(mesh.visual,"uv",None)
        uv_finite=bool(uv is not None and len(uv)==len(mesh.vertices) and np.isfinite(uv).all())
        results.append({**variant,"artifact_sha256":sha256(path),"axis_pair_order":["XY_a","XY_b","ZY_a","ZY_b","XZ_a","XZ_b"],"silhouette_iou_per_axis_pair":ious,"silhouette_iou_mean":float(np.mean(ious)),"vertex_to_source_normalized_p50":float(np.median(forward)/source_diag),"vertex_to_source_normalized_max":float(forward.max(initial=0)/source_diag),"source_to_vertex_normalized_p50":float(np.median(backward)/source_diag),"source_to_vertex_normalized_max":float(backward.max(initial=0)/source_diag),"nearest_source_triangle_centroid_normalized_p50":float(np.median(tri_distance)/source_diag),"nearest_source_triangle_centroid_normalized_max":float(tri_distance.max(initial=0)/source_diag),"uv_attribute_present_and_finite":uv_finite,"uv_bounds":([float(x) for x in np.r_[uv.min(0),uv.max(0)]] if uv_finite else None),"exported_material_count":len(mesh.visual.material.materials) if hasattr(mesh.visual.material,"materials") else 1,"source_triangle_mapping":"approximate nearest centroid; not exact QEM provenance"})
    labels=["source"]+[f"qem {target}" for target in map(int,args.targets.split(','))]
    for view,name in ((0,"xy"),(2,"zy")):
        tiles=[]
        for label_text,view_masks in zip(labels,all_masks):
            tile=cv2.cvtColor(view_masks[view].astype(np.uint8)*255,cv2.COLOR_GRAY2BGR); cv2.putText(tile,label_text,(10,28),cv2.FONT_HERSHEY_SIMPLEX,.75,(0,170,255),2,cv2.LINE_AA); tiles.append(tile)
        cv2.imwrite(str(args.output_dir/f"comparison-{name}-masks.png"),cv2.hconcat(tiles))
    overlay_tiles=[]
    candidate=all_masks[1]
    for view,label_text in ((0,"XY source=green qem2500=magenta"),(2,"ZY source=green qem2500=magenta"),(4,"XZ source=green qem2500=magenta")):
        tile=np.zeros((source_masks.shape[1],source_masks.shape[2],3),np.uint8); tile[source_masks[view],1]=255; tile[candidate[view],0]=255; tile[candidate[view],2]=255; cv2.putText(tile,label_text,(10,28),cv2.FONT_HERSHEY_SIMPLEX,.62,(0,180,255),2,cv2.LINE_AA); overlay_tiles.append(tile)
    cv2.imwrite(str(args.output_dir/"comparison-source-vs-2500-overlay.png"),cv2.hconcat(overlay_tiles))
    report={"scope":"exact cd073 source mesh0 primitive4 Hair material candidate; deterministic QEM shell option, not accepted replacement","projection_contract":"three unsigned axis-pair silhouette tests (XY, ZY, XZ) in one source-derived center/extent; not six registered analyzer cameras","source_glb_sha256":sha256(args.source),"source_export_sha256":sha256(args.output_dir/"hair-source-primitive.glb"),"source_triangles":len(source.faces),"source_vertices":len(source.vertices),"source_uv_attribute_present_and_finite":source_uv_finite,"source_uv_bounds":([float(x) for x in np.r_[source_uv.min(0),source_uv.max(0)]] if source_uv_finite else None),"variants":results,"timing_seconds":{"orchestration_total":time.perf_counter()-start},"semantic_gate":{"replacement_allowed":False,"reason":"exact material candidate exists but source-bound semantic isolation acceptance is closed; original hair remains visible"}}
    (args.output_dir/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8"); print(json.dumps({"variants":len(results),"output":str(args.output_dir/"report.json")}))


if __name__=="__main__":
    if "--" in sys.argv and __import__("importlib").util.find_spec("bpy") is not None: blender_main(sys.argv[sys.argv.index("--")+1:])
    else: normal_main(sys.argv[1:])
