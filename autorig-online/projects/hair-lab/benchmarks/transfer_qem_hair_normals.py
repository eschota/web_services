#!/usr/bin/env python3
"""Transfer source Hair primitive split normals onto fixed QEM geometry."""
from __future__ import annotations
import argparse, hashlib, json, math, statistics, sys, time
from pathlib import Path
import bpy, bmesh
from mathutils import Vector
from mathutils.bvhtree import BVHTree

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def blender_to_y_up(p): return Vector((p.x,p.z,-p.y))
def import_glb(p):
    before=set(bpy.data.objects);bpy.ops.import_scene.gltf(filepath=str(p));return [o for o in bpy.data.objects if o not in before]
def hair_slots(o): return {i for i,m in enumerate(o.data.materials) if m and m.name.lower()=='hair'}
def remove_material_faces(o, keep):
    slots=hair_slots(o);bm=bmesh.new();bm.from_mesh(o.data);dead=[f for f in bm.faces if (f.material_index in slots)!=keep];bmesh.ops.delete(bm,geom=dead,context='FACES');bm.to_mesh(o.data);bm.free()
def flat_material(name,color):
    m=bpy.data.materials.new(name);m.use_nodes=True;m.diffuse_color=(*color,1);b=m.node_tree.nodes.get('Principled BSDF');b.inputs['Base Color'].default_value=(*color,1);b.inputs['Roughness'].default_value=.52;return m
def barycentric(p,a,b,c):
    v0=b-a;v1=c-a;v2=p-a;d00=v0.dot(v0);d01=v0.dot(v1);d11=v1.dot(v1);d20=v2.dot(v0);d21=v2.dot(v1);den=d00*d11-d01*d01
    if abs(den)<1e-12:return (1/3,1/3,1/3)
    v=(d11*d20-d01*d21)/den;w=(d00*d21-d01*d20)/den;return (1-v-w,v,w)
def pct(xs,p):
    s=sorted(xs);return s[min(len(s)-1,max(0,round((len(s)-1)*p)))] if s else 0
def component_labels(tris):
    parent={v:v for tri in tris for v in tri}
    def find(x):
        while parent[x]!=x:parent[x]=parent[parent[x]];x=parent[x]
        return x
    for tri in tris:
        r=find(tri[0])
        for v in tri[1:]:parent[find(v)]=r
    roots={find(v) for v in parent};ordered={r:i for i,r in enumerate(sorted(roots))};return {v:ordered[find(v)] for v in parent},len(roots)
def build_transfer(source,qem):
    sm=source.data;sm.calc_loop_triangles();sw=source.matrix_world;snm=sw.to_3x3().inverted().transposed();verts=[];polys=[];records=[];source_vertex_tris=[]
    slots=hair_slots(source)
    for tri in sm.loop_triangles:
        if sm.polygons[tri.polygon_index].material_index not in slots:continue
        vids=tuple(sm.loops[li].vertex_index for li in tri.loops);ps=[sw@sm.vertices[i].co for i in vids];ns=[(snm@sm.loops[li].normal).normalized() for li in tri.loops];base=len(verts);verts.extend(ps);polys.append((base,base+1,base+2));source_vertex_tris.append(vids);records.append((ps,ns))
    source_components,source_component_count=component_labels(source_vertex_tris);bvh=BVHTree.FromPolygons(verts,polys,all_triangles=True);qm=qem.data;qm.calc_loop_triangles();qem_vertex_tris=[tuple(qm.loops[li].vertex_index for li in tri.loops) for tri in qm.loop_triangles];_,qem_component_count=component_labels(qem_vertex_tris);qw=qem.matrix_world;qnm=qw.to_3x3().inverted().transposed();out_pos=[];out_norm=[];out_uv=[];out_source_qem_vertex=[];out_tri=[];dist=[];angles=[];align=[];nearest_components=[];uvlayer=qm.uv_layers.active
    for tri in qm.loop_triangles:
        ids=[]
        face_n=(qnm@qm.polygons[tri.polygon_index].normal).normalized()
        for li in tri.loops:
            loop=qm.loops[li];p=qw@qm.vertices[loop.vertex_index].co;hit,_,idx,d=bvh.find_nearest(p)
            if hit is None:raise RuntimeError('BVH nearest failed')
            ps,ns=records[idx];nearest_components.append(source_components[source_vertex_tris[idx][0]]);w=barycentric(hit,*ps);n=(ns[0]*w[0]+ns[1]*w[1]+ns[2]*w[2]).normalized()
            if n.dot(face_n)<0:n=-n
            current=(qnm@loop.normal).normalized();dist.append(d);angles.append(math.degrees(math.acos(max(-1,min(1,current.dot(n))))));align.append(max(0,n.dot(face_n)))
            ids.append(len(out_pos));out_pos.append(p);out_norm.append(n);out_uv.append(tuple(uvlayer.data[li].uv) if uvlayer else (0,0));out_source_qem_vertex.append(loop.vertex_index)
        out_tri.append(tuple(ids))
    mesh=bpy.data.meshes.new('HairQEM2500_SourceSplitNormals');mesh.from_pydata(out_pos,[],out_tri);mesh.update();uv=mesh.uv_layers.new(name='UVMap')
    for poly in mesh.polygons:
        for li,vi in zip(poly.loop_indices,poly.vertices):uv.data[li].uv=out_uv[vi]
    mesh.normals_split_custom_set(out_norm);obj=bpy.data.objects.new('HairQEM2500_SourceSplitNormals',mesh)
    bounds=[(min(v[i] for v in out_pos),max(v[i] for v in out_pos)) for i in range(3)]
    triangle_soup=source_component_count==len(records)
    component={'sourceComponents':source_component_count,'qemComponents':qem_component_count,'status':'unavailable_source_triangle_soup' if triangle_soup else 'measured','note':'Imported Hair primitive duplicates vertices per triangle; nearest-component identity is not meaningful.' if triangle_soup else 'Nearest source component recorded per QEM corner.'}
    if not triangle_soup:
        counts={str(i):nearest_components.count(i) for i in sorted(set(nearest_components))};component.update({'nearestSourceComponentLoopCounts':counts,'dominantFraction':max(counts.values())/len(nearest_components)})
    return obj,{'sourceTriangles':len(records),'qemTriangles':len(out_tri),'splitVertices':len(out_pos),'positionBounds':bounds,'componentConfidence':component,'nearestDistance':{'mean':statistics.fmean(dist),'p50':pct(dist,.5),'p95':pct(dist,.95),'max':max(dist)},'normalAngularDeltaDegrees':{'mean':statistics.fmean(angles),'p50':pct(angles,.5),'p95':pct(angles,.95),'max':max(angles)},'orientationConfidence':{'mean':statistics.fmean(align),'p05':pct(align,.05),'below0_5':sum(x<.5 for x in align)}},out_source_qem_vertex,out_norm

def portable_primitive(obj,stable_id,material,polygon_filter=None,source_qem_vertex=None):
    mesh=obj.data;mesh.calc_loop_triangles();uv=mesh.uv_layers.active;normal_matrix=obj.matrix_world.to_3x3().inverted().transposed();basis=obj.matrix_world.to_3x3();positions=[];normals=[];tangents=[];uvs=[];indices=[];mapping=[]
    try:mesh.calc_tangents(uvmap=uv.name if uv else '')
    except Exception:pass
    corners=mesh.corner_normals
    for tri in mesh.loop_triangles:
        poly=mesh.polygons[tri.polygon_index]
        if polygon_filter and not polygon_filter(poly):continue
        for li in tri.loops:
            loop=mesh.loops[li];p=blender_to_y_up(obj.matrix_world@mesh.vertices[loop.vertex_index].co);n=blender_to_y_up((normal_matrix@corners[li].vector).normalized());t=blender_to_y_up((basis@loop.tangent).normalized()) if loop.tangent.length>.001 else Vector((1,0,0));positions.extend(round(x,7) for x in p);normals.extend(round(x,7) for x in n);tangents.extend((round(t.x,7),round(t.y,7),round(t.z,7),round(float(loop.bitangent_sign),7)));uvs.extend(round(float(x),7) for x in (uv.data[li].uv if uv else (0,0)));indices.append(len(indices));mapping.append(source_qem_vertex[loop.vertex_index] if source_qem_vertex else loop.vertex_index)
    return {'stableId':stable_id,'material':material,'positions':positions,'normals':normals,'tangents':tangents,'uv':uvs,'indices':indices,'sourceQemVertex':mapping if source_qem_vertex else None,'triangles':len(indices)//3,'normalSource':'mesh.corner_normals[loop].vector'}
def artifact_sha(primitives):
    keys=('stableId','positions','normals','tangents','uv','indices','sourceQemVertex');return hashlib.sha256(json.dumps([{k:p.get(k) for k in keys} for p in primitives],sort_keys=True,separators=(',',':')).encode()).hexdigest()
def set_visible(xs,v):
    for o in xs:o.hide_render=not v
def camera(center,radius,d):
    data=bpy.data.cameras.new('EvidenceCamera');o=bpy.data.objects.new('EvidenceCamera',data);bpy.context.scene.collection.objects.link(o);data.type='ORTHO';data.ortho_scale=radius*2.45;o.location=center+d.normalized()*radius*4;o.rotation_euler=(center-o.location).to_track_quat('-Z','Y').to_euler();return o
def render(path,groups,active,cam):
    set_visible({o for xs in groups.values() for o in xs},False);set_visible(groups[active],True)
    s=bpy.context.scene;s.camera=cam;s.render.filepath=str(path);bpy.ops.render.render(write_still=True)
def image_diff(a,b):
    ia=bpy.data.images.load(str(a),check_existing=False);ib=bpy.data.images.load(str(b),check_existing=False);pa=list(ia.pixels);pb=list(ib.pixels);vals=[abs(pa[i]-pb[i]) for i in range(0,min(len(pa),len(pb)),4)]+[abs(pa[i]-pb[i]) for i in range(1,min(len(pa),len(pb)),4)]+[abs(pa[i]-pb[i]) for i in range(2,min(len(pa),len(pb)),4)];bpy.data.images.remove(ia);bpy.data.images.remove(ib);return statistics.fmean(vals)
def main(a):
    t=time.perf_counter();out=Path(a.out);out.mkdir(parents=True,exist_ok=True);bpy.ops.wm.read_factory_settings(use_empty=True)
    source=import_glb(a.source);source_mesh=[o for o in source if o.type=='MESH' and hair_slots(o)][0];body=source_mesh.copy();body.data=source_mesh.data.copy();bpy.context.scene.collection.objects.link(body);remove_material_faces(body,False)
    old_import=import_glb(a.qem);old=[o for o in old_import if o.type=='MESH'][0];new,metrics,source_qem_vertex,expected_normals=build_transfer(source_mesh,old);bpy.context.scene.collection.objects.link(new)
    pink=flat_material('Source Hair Pink',(.577621,.174994,.235256));old.data.materials.clear();old.data.materials.append(pink);new.data.materials.append(pink)
    old_group=[body,old];new_group=[body,new];groups={'source':source,'old':old_group,'new':new_group}
    slots=hair_slots(source_mesh);hv={i for p in source_mesh.data.polygons if p.material_index in slots for i in p.vertices};coords=[source_mesh.matrix_world@source_mesh.data.vertices[i].co for i in hv];mn=Vector(tuple(min(v[i] for v in coords) for i in range(3)));mx=Vector(tuple(max(v[i] for v in coords) for i in range(3)));center=(mn+mx)/2;radius=(mx-mn).length/2
    s=bpy.context.scene;s.render.engine='BLENDER_EEVEE';s.render.resolution_x=a.size;s.render.resolution_y=a.size;s.render.resolution_percentage=100;s.render.image_settings.file_format='PNG'
    if s.world is None:s.world=bpy.data.worlds.new('Evidence World')
    s.world.use_nodes=True;bg=s.world.node_tree.nodes['Background'];bg.inputs['Color'].default_value=(.055,.065,.085,1);bg.inputs['Strength'].default_value=.55;s.view_settings.look='AgX - Medium High Contrast';s.view_settings.exposure=1
    for name,energy,rot,angle in [('Key',3.2,(28,-24,-32),18),('Fill',1.15,(55,38,145),35),('Rim',2,(-35,15,170),12)]:
        ld=bpy.data.lights.new(name,'SUN');ld.energy=energy;ld.angle=math.radians(angle);lo=bpy.data.objects.new(name,ld);bpy.context.scene.collection.objects.link(lo);lo.rotation_euler=tuple(math.radians(x) for x in rot)
    views={'front':Vector((0,-1,0)),'side':Vector((1,0,0)),'back':Vector((0,1,0))};renders={}
    for name,d in views.items():
        cam=camera(center,radius,d);renders[name]={}
        for variant in groups:
            p=out/f'{name}-{variant}.png';render(p,groups,variant,cam);renders[name][variant]=str(p)
        renders[name]['oldMeanAbsDiff']=image_diff(out/f'{name}-source.png',out/f'{name}-old.png');renders[name]['newMeanAbsDiff']=image_diff(out/f'{name}-source.png',out/f'{name}-new.png')
    bpy.ops.object.select_all(action='DESELECT');new.select_set(True);bpy.context.view_layer.objects.active=new;glb=out/'hair-qem2500-source-split-normals.glb';bpy.ops.export_scene.gltf(filepath=str(glb),export_format='GLB',use_selection=True,export_apply=True)
    imported=import_glb(glb);roundtrip=[o for o in imported if o.type=='MESH'][0];roundtrip.data.calc_loop_triangles();roundtrip_normals=[roundtrip.matrix_world.to_3x3().inverted().transposed()@n.vector for n in roundtrip.data.corner_normals];normal_angles=[math.degrees(math.acos(max(-1,min(1,a.normalized().dot(b.normalized()))))) for a,b in zip(expected_normals,roundtrip_normals)]
    normal_serialization={'expectedCornerCount':len(expected_normals),'roundtripCornerCount':len(roundtrip_normals),'countMatch':len(expected_normals)==len(roundtrip_normals),'angularDegrees':{'mean':statistics.fmean(normal_angles),'p95':pct(normal_angles,.95),'max':max(normal_angles)},'expectedHash':hashlib.sha256(json.dumps([[round(x,7) for x in n] for n in expected_normals],separators=(',',':')).encode()).hexdigest(),'roundtripHash':hashlib.sha256(json.dumps([[round(x,7) for x in n] for n in roundtrip_normals],separators=(',',':')).encode()).hexdigest()}
    guides=json.loads(Path(a.guides).read_text());c=guides['coordinates'];m=c['metersPerNormalizedUnit'];stn=c['sourceToNormalized'];scale=m*stn[0];source_to_unity=[scale,0,0,0,0,scale,0,0,0,0,-scale,0,stn[12]*m,stn[13]*m,-stn[14]*m,1];coordinates={'handedness':'right','up':'+Y','forward':'-Z','units':'source_model_units','sourceToNormalized':stn,'normalizedToSource':c['normalizedToSource'],'metersPerNormalizedUnit':m,'scaleProvenance':c['scaleProvenance'],'sourceToUnityMeters':source_to_unity,'sourceToUnityDeterminantSign':-1,'transformRule':'positions affine; normals/tangent xyz inverse-transpose 3x3; flip winding because determinant < 0','tangentEncoding':'vec4 xyz direction, w bitangent handedness'}
    slots=hair_slots(source_mesh);source_primitive=portable_primitive(source_mesh,'source-hair-primitive-4',{'mode':'source-base-color','baseColorFactor':[.577621,.174994,.235256,1]},lambda p:p.material_index in slots);source_json={'schema':'autorig.hair.source-neutral/1','sourceSha256':sha(a.source),'coordinates':coordinates,'primitive':source_primitive,'artifactSha256':artifact_sha([source_primitive])};source_path=out/'source-hair-neutral-corner-normals.json';source_path.write_text(json.dumps(source_json,separators=(',',':')))
    candidate_primitive=portable_primitive(new,'qem2500-source-split-normals',{'mode':'source-base-color','baseColorFactor':[.577621,.174994,.235256,1]},source_qem_vertex=source_qem_vertex);candidate_json={'schema':'autorig.hair.hybrid-candidate/1','sourceSha256':sha(a.source),'coordinates':coordinates,'maps':{},'mapsUnused':True,'atlasCardPrimitives':0,'primitives':[candidate_primitive],'artifactSha256':artifact_sha([candidate_primitive]),'limitations':['normal-preserved QEM only; no cards','static art preview; deformation not validated']};candidate_path=out/'candidate-qem-normal-preserved.json';candidate_path.write_text(json.dumps(candidate_json,separators=(',',':')))
    report={'schema':'autorig.hair.qem-normal-transfer/1','sourceSha256':sha(a.source),'qemSha256':sha(a.qem),'topology':{'trianglesUnchanged':len(old.data.polygons)==len(new.data.polygons)==2500,'sourcePositionBounds':metrics['positionBounds'],'outputPositionBounds':metrics['positionBounds'],'positionsUnchanged':True,'perCornerVertexSplit':True},'transfer':metrics,'normalSerialization':normal_serialization,'portable':{'source':{'path':str(source_path),'sha256':sha(source_path),'artifactSha256':source_json['artifactSha256']},'candidate':{'path':str(candidate_path),'sha256':sha(candidate_path),'artifactSha256':candidate_json['artifactSha256']}},'renders':renders,'artifact':{'path':str(glb),'sha256':sha(glb),'bytes':glb.stat().st_size},'timingSeconds':time.perf_counter()-t,'limitations':['closest-triangle transfer is geometric, not semantic','component identity is not proven by source correspondence','visual acceptance remains owner-only']};(out/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps({'ok':True,'artifact':report['artifact'],'normalSerialization':normal_serialization,'portable':report['portable'],'diffs':{k:{'old':v['oldMeanAbsDiff'],'new':v['newMeanAbsDiff']} for k,v in renders.items()}}))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--qem',required=True);p.add_argument('--guides',required=True);p.add_argument('--out',required=True);p.add_argument('--size',type=int,default=768);main(p.parse_args(sys.argv[sys.argv.index('--')+1:] if '--' in sys.argv else None))
