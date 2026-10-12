#!/usr/bin/env python3
"""Build a conservative closed visual-hull proxy from the exact Face primitive."""

from __future__ import annotations

import argparse, hashlib, json, math, subprocess, sys, time
from pathlib import Path


def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return digest.hexdigest()


def blender_overlay(argv):
    import bpy
    from mathutils import Vector
    parser=argparse.ArgumentParser(); parser.add_argument("--face",required=True); parser.add_argument("--proxy",required=True); parser.add_argument("--output-dir",required=True); args=parser.parse_args(argv); out=Path(args.output_dir)
    bpy.ops.wm.read_factory_settings(use_empty=True); bpy.ops.import_scene.gltf(filepath=args.face); face=[o for o in bpy.context.scene.objects if o.type=="MESH"][0]; face.name="Exact_Face_Primitive"
    bpy.ops.import_scene.gltf(filepath=args.proxy); meshes=[o for o in bpy.context.scene.objects if o.type=="MESH" and o!=face]; proxy=meshes[-1]; proxy.name="Inferred_VisualHull_Proxy"
    def material(name,color,alpha=1):
        mat=bpy.data.materials.new(name); mat.diffuse_color=(*color,alpha); mat.use_nodes=True; bsdf=mat.node_tree.nodes.get("Principled BSDF"); bsdf.inputs["Base Color"].default_value=(*color,1); bsdf.inputs["Alpha"].default_value=alpha; bsdf.inputs["Roughness"].default_value=.6
        if alpha<1: mat.surface_render_method="DITHERED"
        return mat
    face.data.materials.clear(); face.data.materials.append(material("ExactFace",(.62,.33,.18),1)); proxy.data.materials.clear(); proxy.data.materials.append(material("Proxy",(.05,.4,1),.28))
    corners=[o.matrix_world@Vector(c) for o in (face,proxy) for c in o.bound_box]; center=sum(corners,Vector())/len(corners); extent=max(max(c.x for c in corners)-min(c.x for c in corners),max(c.y for c in corners)-min(c.y for c in corners),max(c.z for c in corners)-min(c.z for c in corners))
    bpy.ops.object.camera_add(); camera=bpy.context.object; camera.data.type="ORTHO"; camera.data.ortho_scale=extent*1.25; bpy.context.scene.camera=camera
    scene=bpy.context.scene; scene.world=scene.world or bpy.data.worlds.new("ProxyWorld"); scene.world.use_nodes=True; scene.world.node_tree.nodes["Background"].inputs["Color"].default_value=(.025,.025,.025,1); scene.world.node_tree.nodes["Background"].inputs["Strength"].default_value=.6; scene.render.engine="BLENDER_EEVEE"; scene.render.resolution_x=700; scene.render.resolution_y=700; scene.render.resolution_percentage=100; scene.render.image_settings.file_format="PNG"
    bpy.ops.object.light_add(type="AREA",location=(center.x-2,center.y-4,center.z+4)); bpy.context.object.data.energy=1000; bpy.context.object.data.size=5
    for name,location in (("front",(center.x,center.y-extent*2,center.z)),("side",(center.x+extent*2,center.y,center.z))):
        camera.location=location; camera.rotation_euler=(center-camera.location).to_track_quat("-Z","Y").to_euler(); scene.render.filepath=str(out/f"actual-face-proxy-{name}.png"); bpy.ops.render.render(write_still=True)


def fill_holes(mask):
    import cv2, numpy as np
    flood=mask.copy().astype(np.uint8)*255; padded=cv2.copyMakeBorder(flood,1,1,1,1,cv2.BORDER_CONSTANT,value=0); filled=padded.copy(); cv2.floodFill(filled,None,(0,0),255); holes=(filled[1:-1,1:-1]==0); return mask|holes, int(holes.sum())


def raster(vertices,faces,resolution,center,extent):
    import cv2, numpy as np
    result=[]; additions=[]
    for u,v in ((0,1),(2,1),(0,2)):
        q=np.column_stack((((vertices[:,u]-center[u])/extent+.5),1-((vertices[:,v]-center[v])/extent+.5)))*resolution; mask=np.zeros((resolution,resolution),np.uint8)
        for face in faces: cv2.fillConvexPoly(mask,np.rint(q[face]-.5).astype(np.int32),1,lineType=cv2.LINE_8)
        closed,hole_pixels=fill_holes(mask.astype(bool)); dilated=cv2.dilate(closed.astype(np.uint8),np.ones((3,3),np.uint8),iterations=1).astype(bool); additions.append({"internal_hole_pixels_filled":hole_pixels,"one_pixel_margin_added":int((dilated&~closed).sum()),"original_coverage":int(mask.sum()),"final_coverage":int(dilated.sum())}); result.append(dilated)
    return np.stack(result),additions


def surface_mesh(occupancy,bounds_min,bounds_max):
    import numpy as np
    resolution=occupancy.shape[0]; step=(bounds_max-bounds_min)/resolution; vertices=[]; faces=[]
    directions=[((-1,0,0),[(0,0,0),(0,0,1),(0,1,1),(0,1,0)]),((1,0,0),[(1,0,0),(1,1,0),(1,1,1),(1,0,1)]),((0,-1,0),[(0,0,0),(1,0,0),(1,0,1),(0,0,1)]),((0,1,0),[(0,1,0),(0,1,1),(1,1,1),(1,1,0)]),((0,0,-1),[(0,0,0),(0,1,0),(1,1,0),(1,0,0)]),((0,0,1),[(0,0,1),(1,0,1),(1,1,1),(0,1,1)])]
    for x,y,z in np.argwhere(occupancy):
        for (dx,dy,dz),corners in directions:
            nx,ny,nz=x+dx,y+dy,z+dz
            if 0<=nx<resolution and 0<=ny<resolution and 0<=nz<resolution and occupancy[nx,ny,nz]: continue
            base=len(vertices)
            for cx,cy,cz in corners: vertices.append((bounds_min+np.array([x+cx,y+cy,z+cz])*step).tolist())
            faces.extend(((base,base+1,base+2),(base,base+2,base+3)))
    return np.asarray(vertices,np.float32),np.asarray(faces,np.uint32)


def main(argv):
    import cv2, numpy as np, trimesh
    from scipy.ndimage import distance_transform_edt
    parser=argparse.ArgumentParser(); parser.add_argument("--head",type=Path,required=True); parser.add_argument("--shell",type=Path,required=True); parser.add_argument("--gates",type=Path,required=True); parser.add_argument("--output-dir",type=Path,required=True); parser.add_argument("--blender",type=Path,required=True); parser.add_argument("--resolutions",default="64,96")
    args=parser.parse_args(argv); args.output_dir.mkdir(parents=True,exist_ok=True); head=json.loads(args.head.read_text()); material_names={m["id"]:m["name"] for m in head["materials"]}; face_primitive=next(p for p in head["primitives"] if material_names[p["materialId"]]=="Face"); vertices=np.asarray(face_primitive["positions"],np.float64).reshape(-1,3); faces=np.asarray(face_primitive["indices"],np.int64).reshape(-1,3); center=(vertices.min(0)+vertices.max(0))/2; extent=max((vertices.max(0)-vertices.min(0)).max()*1.12,1e-9); bounds_min=center-extent/2; bounds_max=center+extent/2; hair=json.loads(args.shell.read_text()); hair_vertices=np.asarray(hair["mesh"]["positions"],np.float64).reshape(-1,3); hair_faces=np.asarray(hair["mesh"]["indices"],np.int64).reshape(-1,3); gate=json.loads(args.gates.read_text()); case_gate=next(g for g in gate["gates"] if g["case_id"]==head["source"]["caseId"]); selector=next(p for p in case_gate["source_primitives"] if p["material_name"]=="Face")
    reports=[]
    for resolution in map(int,args.resolutions.split(',')):
        started=time.perf_counter(); masks,policy=raster(vertices,faces,resolution,center,extent); xy=masks[0][::-1,:].T; zy=masks[1][::-1,:]; xz=masks[2][::-1,:].T; occupancy=xy[:,:,None]&zy[None,:,:]&xz[:,None,:]; mask_hull_ms=(time.perf_counter()-started)*1000; surface_started=time.perf_counter(); proxy_vertices,proxy_faces=surface_mesh(occupancy,bounds_min,bounds_max); surface_ms=(time.perf_counter()-surface_started)*1000; proxy=trimesh.Trimesh(proxy_vertices,proxy_faces,process=False); proxy_path=args.output_dir/f"face-visual-hull-{resolution}.glb"; proxy.export(proxy_path)
        cell=extent/resolution; outside=distance_transform_edt(~occupancy)*cell; inside=distance_transform_edt(occupancy)*cell
        def sample(points):
            coords=np.floor((points-bounds_min)/cell).astype(int); valid=np.all((coords>=0)&(coords<resolution),axis=1); clipped=np.clip(coords,0,resolution-1); values=np.where(valid,np.where(occupancy[tuple(clipped.T)],inside[tuple(clipped.T)],-outside[tuple(clipped.T)]),-np.inf); return values
        face_signed=sample(vertices); hair_signed=sample(hair_vertices); hair_centroid_signed=sample(hair_vertices[hair_faces].mean(1)); report={"resolution":resolution,"voxelSizeNormalized":cell,"occupancyVoxels":int(occupancy.sum()),"occupancyFraction":float(occupancy.mean()),"timingMilliseconds":{"maskRasterAndHull":mask_hull_ms,"blockSurfaceExtraction":surface_ms,"totalGeometry":mask_hull_ms+surface_ms},"maskPolicy":policy,"proxy":{"vertices":len(proxy_vertices),"triangles":len(proxy_faces),"sha256":sha256(proxy_path)},"faceToHullSignedDistance":{"definition":"positive inside, negative outside; nearest voxel EDT approximation","p50":float(np.median(face_signed[np.isfinite(face_signed)])),"p90":float(np.percentile(face_signed[np.isfinite(face_signed)],90)),"maxAbs":float(np.max(np.abs(face_signed[np.isfinite(face_signed)]))),"outsideFraction":float(np.mean(face_signed<0))},"hairOverlap":{"vertexInsideFraction":float(np.mean(hair_signed>0)),"triangleCentroidInsideFraction":float(np.mean(hair_centroid_signed>0)),"warning":"overlap is expected near roots but can also reveal conservative over-fill; not collision acceptance"}}; np.savez_compressed(args.output_dir/f"face-visual-hull-{resolution}.npz",occupancy=occupancy,bounds_min=bounds_min,bounds_max=bounds_max); reports.append(report)
    face_mesh=trimesh.Trimesh(vertices,faces,process=False); face_path=args.output_dir/"exact-face-primitive.glb"; face_mesh.export(face_path)
    best=trimesh.load(args.output_dir/"face-visual-hull-96.glb",force="mesh",process=False); face_masks,_=raster(vertices,faces,512,center,extent); proxy_masks,_=raster(best.vertices,best.faces,512,center,extent); tiles=[]
    for index,label in enumerate(("XY","ZY","XZ")):
        tile=np.zeros((512,512,3),np.uint8); tile[face_masks[index],1]=255; tile[proxy_masks[index],0]=255; tile[proxy_masks[index],2]=255; cv2.putText(tile,f"{label}: exact Face green, proxy magenta",(8,26),cv2.FONT_HERSHEY_SIMPLEX,.58,(0,180,255),2,cv2.LINE_AA); tiles.append(tile)
    cv2.imwrite(str(args.output_dir/"face-proxy-axis-overlay.png"),cv2.hconcat(tiles)); subprocess.run([str(args.blender),"--background","--python",str(Path(__file__).resolve()),"--","overlay","--face",str(face_path),"--proxy",str(args.output_dir/"face-visual-hull-96.glb"),"--output-dir",str(args.output_dir)],check=True,capture_output=True,text=True)
    output={"schema":"autorig.hair.inferred-head-proxy/1","source":{"caseId":head["source"]["caseId"],"taskId":head["source"]["taskId"],"sha256":head["source"]["sha256"],"headNeutralSha256":sha256(args.head),"hairShellSha256":sha256(args.shell),"gateSha256":sha256(args.gates),"selector":{"meshId":selector["mesh_id"],"primitiveId":selector["primitive_id"],"globalTriangleStart":selector["global_triangle_start"],"globalTriangleEndExclusive":selector["global_triangle_end_exclusive"],"materialName":"Face"}},"coordinates":head["coordinates"],"method":{"kind":"three-axis conservative visual hull","views":"XY, ZY, XZ unsigned silhouettes in normalized Head frame","internalHolePolicy":"fill all enclosed 2D silhouette holes; deliberately over-fills eye/mouth/nose cavities","margin":"one pixel binary dilation per silhouette","truthStatus":"inferred conservative proxy, not signed truth of original open mesh and not accepted collision input"},"resolutions":reports,"semanticWarnings":["Face material selector is exact but material name does not prove complete anatomical head/scalp membership","visual hull cannot preserve concavities and intentionally fills internal projection holes","self-intersections of source Face were not tested"]}; (args.output_dir/"report.json").write_text(json.dumps(output,indent=2),encoding="utf-8"); print(json.dumps({"resolutions":len(reports),"output":str(args.output_dir/"report.json")}))


if __name__=="__main__":
    if "--" in sys.argv and sys.argv[sys.argv.index("--")+1:sys.argv.index("--")+2]==["overlay"]: blender_overlay(sys.argv[sys.argv.index("--")+2:])
    else: main(sys.argv[1:])
