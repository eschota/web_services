#!/usr/bin/env python3
"""Private Blender evidence builder for cd073 source-guided lock cards. No customer bytes are shipped."""
from __future__ import annotations
import argparse, json, math, hashlib, sys, time
from pathlib import Path
import bpy, bmesh
from mathutils import Vector
from mathutils.kdtree import KDTree
from mathutils.bvhtree import BVHTree
from bpy_extras.object_utils import world_to_camera_view

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def vec(values): return Vector(tuple(float(x) for x in values))
def transform(m,p): return Vector((m[0]*p.x+m[4]*p.y+m[8]*p.z+m[12],m[1]*p.x+m[5]*p.y+m[9]*p.z+m[13],m[2]*p.x+m[6]*p.y+m[10]*p.z+m[14]))
def y_up_to_blender(p): return Vector((p.x,-p.z,p.y))
def blender_to_y_up(p): return Vector((p.x,p.z,-p.y))

def select_guides(bundle, center, count=22):
    if count<=0:return []
    gs=[]
    for g in bundle['guides']:
        root=y_up_to_blender(transform(bundle['coordinates']['normalizedToSource'],vec(g['root'])));tip=y_up_to_blender(transform(bundle['coordinates']['normalizedToSource'],vec(g['tip'])));d=(tip-center);length=d.length
        if length<.25: continue
        gs.append((g,root,tip,d.normalized(),length))
    chosen=[]
    def take(items,limit,angle):
        cos=math.cos(math.radians(angle))
        for item in sorted(items,key=lambda x:x[4],reverse=True):
            if any(item[3].dot(o[3])>cos for o in chosen):continue
            chosen.append(item)
            if len(chosen)>=limit:return
    take([x for x in gs if x[0].get('kind')=='hairline'],6,12)
    take([x for x in gs if x[0].get('kind')!='hairline'],count,15)
    return chosen[:count]

def atlas_material(name, albedo, normal, mask, tint, use_normal=True, normalize_albedo=False):
    m=bpy.data.materials.new(name);m.use_nodes=True;m.diffuse_color=(*tint,1);m.surface_render_method='DITHERED'
    n=m.node_tree.nodes;n.clear();out=n.new('ShaderNodeOutputMaterial');bs=n.new('ShaderNodeBsdfPrincipled');bs.inputs['Base Color'].default_value=(*tint,1);bs.inputs['Roughness'].default_value=.48;bs.inputs['Metallic'].default_value=0
    tex=n.new('ShaderNodeTexImage');tex.image=bpy.data.images.load(str(albedo),check_existing=True);tex.image.colorspace_settings.name='sRGB';mix=n.new('ShaderNodeMixRGB');mix.blend_type='MULTIPLY';mix.inputs[0].default_value=1;mix.inputs[2].default_value=(*tint,1)
    color_output=tex.outputs['Color']
    if normalize_albedo:
        div=n.new('ShaderNodeVectorMath');div.operation='DIVIDE';div.inputs[1].default_value=(.62631,.569373,.518633);m.node_tree.links.new(tex.outputs['Color'],div.inputs[0]);color_output=div.outputs['Vector']
    clip=n.new('ShaderNodeMath');clip.operation='GREATER_THAN';clip.inputs[1].default_value=.36;m.node_tree.links.new(tex.outputs['Alpha'],clip.inputs[0]);m.node_tree.links.new(color_output,mix.inputs[1]);m.node_tree.links.new(mix.outputs['Color'],bs.inputs['Base Color']);m.node_tree.links.new(clip.outputs[0],bs.inputs['Alpha'])
    if use_normal:
        nt=n.new('ShaderNodeTexImage');nt.image=bpy.data.images.load(str(normal),check_existing=True);nt.image.colorspace_settings.name='Non-Color';nm=n.new('ShaderNodeNormalMap');nm.inputs['Strength'].default_value=.65;m.node_tree.links.new(nt.outputs['Color'],nm.inputs['Color']);m.node_tree.links.new(nm.outputs['Normal'],bs.inputs['Normal'])
    mt=n.new('ShaderNodeTexImage');mt.image=bpy.data.images.load(str(mask),check_existing=True);mt.image.colorspace_settings.name='Non-Color';sep=n.new('ShaderNodeSeparateColor');inv=n.new('ShaderNodeMath');inv.operation='SUBTRACT';inv.inputs[0].default_value=1;m.node_tree.links.new(mt.outputs['Color'],sep.inputs['Color']);m.node_tree.links.new(sep.outputs['Red'],bs.inputs['Metallic']);m.node_tree.links.new(mt.outputs['Alpha'],inv.inputs[1]);m.node_tree.links.new(inv.outputs[0],bs.inputs['Roughness']);m.node_tree.links.new(bs.outputs[0],out.inputs[0]);return m

def flat_material(name,color,rough=.55):
    m=bpy.data.materials.new(name);m.diffuse_color=(*color,1);m.use_nodes=True;bs=m.node_tree.nodes.get('Principled BSDF');bs.inputs['Base Color'].default_value=(*color,1);bs.inputs['Roughness'].default_value=rough;return m

def make_card(record, root, tip, samples, width, rect, kd, shell_obj, material, surface_bvh=None, surface_offset=0):
    points=[transform(record['_matrix'],vec(s['position'])) for s in samples]
    nearest=kd.find(root)[1];fallback=(shell_obj.matrix_world.to_3x3()@shell_obj.data.vertices[nearest].normal).normalized();surface_normals=[]
    if surface_bvh:
        projected=[]
        for p in points:
            hit,n,_,_=surface_bvh.find_nearest(p);n=n.normalized() if n and n.length>.001 else fallback;projected.append(hit+n*surface_offset);surface_normals.append(n)
        points=projected
    else:surface_normals=[fallback for _ in points]
    verts=[];vertex_normals=[];uv=[];tris=[]
    for i,p in enumerate(points):
        if i==0:t=(points[1]-p).normalized()
        elif i==len(points)-1:t=(p-points[i-1]).normalized()
        else:t=(points[i+1]-points[i-1]).normalized()
        desired=surface_normals[i];right=t.cross(desired);right=right.normalized() if right.length>.001 else Vector((1,0,0));w=width*float(samples[i]['widthScale']);verts.extend((p+right*w,p-right*w));vertex_normals.extend((desired,desired));v=i/(len(points)-1);uv.extend(((rect['u1'],v),(rect['u0'],v)))
        if i<len(points)-1:b=i*2;tris.extend(((b,b+1,b+2),(b+1,b+3,b+2)))
    mean=Vector((0,0,0));
    for a,b,c in tris:mean+=(verts[b]-verts[a]).cross(verts[c]-verts[a])
    if mean.dot(sum(surface_normals,Vector((0,0,0))))<0:tris=[(a,c,b) for a,b,c in tris]
    mesh=bpy.data.meshes.new(record['id']);mesh.from_pydata(verts,[],tris);mesh.update();layer=mesh.uv_layers.new(name='UVMap')
    for poly in mesh.polygons:
        for li,vi in zip(poly.loop_indices,poly.vertices):layer.data[li].uv=uv[vi]
    mesh.normals_split_custom_set([vertex_normals[mesh.loops[li].vertex_index] for li in range(len(mesh.loops))]);mesh.calc_tangents(uvmap='UVMap');obj=bpy.data.objects.new(record['id'],mesh);obj['source_primitive']=4;obj['source_vertex']=record['sourceCompactVertex'];obj['stable_id']=record['id'];obj.data.materials.append(material);return obj

def merge_card_objects(cards,material):
    verts=[];normals=[];uvs=[];tris=[];ranges=[]
    for obj in cards:
        mesh=obj.data;mesh.calc_loop_triangles();uv=mesh.uv_layers.active;corners=mesh.corner_normals;start_v=len(verts);start_t=len(tris)
        normal_matrix=obj.matrix_world.to_3x3().inverted().transposed()
        for tri in mesh.loop_triangles:
            ids=[]
            for li in tri.loops:
                loop=mesh.loops[li];ids.append(len(verts));verts.append(obj.matrix_world@mesh.vertices[loop.vertex_index].co);normals.append((normal_matrix@corners[li].vector).normalized());uvs.append(tuple(uv.data[li].uv))
            tris.append(tuple(ids))
        ranges.append({'stableId':obj['stable_id'],'sourceVertex':int(obj['source_vertex']),'vertexStart':start_v,'vertexCount':len(verts)-start_v,'triangleStart':start_t,'triangleCount':len(tris)-start_t})
    mesh=bpy.data.meshes.new('hair-primitive-merged-wisps');mesh.from_pydata(verts,[],tris);mesh.update();layer=mesh.uv_layers.new(name='UVMap')
    for poly in mesh.polygons:
        for li,vi in zip(poly.loop_indices,poly.vertices):layer.data[li].uv=uvs[vi]
    mesh.normals_split_custom_set([normals[mesh.loops[li].vertex_index] for li in range(len(mesh.loops))]);mesh.calc_tangents(uvmap='UVMap');obj=bpy.data.objects.new('hair-primitive-merged-wisps',mesh);obj.data.materials.append(material);obj['stable_id']='merged-wisps';obj['card_ranges_json']=json.dumps(ranges,separators=(',',':'));return obj,ranges

def import_glb(path):
    before=set(bpy.data.objects);bpy.ops.import_scene.gltf(filepath=str(path));return [o for o in bpy.data.objects if o not in before]

def remove_hair_faces(obj):
    hair={i for i,m in enumerate(obj.data.materials) if m and m.name.lower()=='hair'}
    if not hair:raise RuntimeError('Hair material slot not found')
    bm=bmesh.new();bm.from_mesh(obj.data);dead=[f for f in bm.faces if f.material_index in hair];bmesh.ops.delete(bm,geom=dead,context='FACES');bm.to_mesh(obj.data);bm.free();return len(dead)

def keep_hair_faces(obj):
    hair={i for i,m in enumerate(obj.data.materials) if m and m.name.lower()=='hair'}
    if not hair:raise RuntimeError('Hair material slot not found')
    bm=bmesh.new();bm.from_mesh(obj.data);dead=[f for f in bm.faces if f.material_index not in hair];bmesh.ops.delete(bm,geom=dead,context='FACES');bm.to_mesh(obj.data);bm.free();return len(dead)

def set_visible(objects,visible):
    for o in objects:o.hide_render=not visible

def setup_camera(center, radius, direction):
    cam_data=bpy.data.cameras.new('Evidence Camera');cam=bpy.data.objects.new('Evidence Camera',cam_data);bpy.context.scene.collection.objects.link(cam);bpy.context.scene.camera=cam;cam_data.type='ORTHO';cam_data.ortho_scale=radius*2.45;cam.location=center+direction.normalized()*radius*4;cam.rotation_euler=(center-cam.location).to_track_quat('-Z','Y').to_euler();return cam

def render(path, source, candidate, show_source, camera):
    set_visible(source,show_source);set_visible(candidate,not show_source);bpy.context.scene.camera=camera;bpy.context.scene.render.filepath=str(path);bpy.ops.render.render(write_still=True)

def render_mask(path, all_objects, visible, camera, material):
    set_visible(all_objects,False);set_visible(visible,True);scene=bpy.context.scene;scene.camera=camera;scene.view_layers[0].material_override=material;scene.render.filepath=str(path);bpy.ops.render.render(write_still=True);scene.view_layers[0].material_override=None

def binary_mask(path):
    image=bpy.data.images.load(str(path),check_existing=False);pixels=list(image.pixels);mask=[max(pixels[i],pixels[i+1],pixels[i+2])>.5 for i in range(0,len(pixels),4)];bpy.data.images.remove(image);return mask

def image_exposure_stats(path):
    image=bpy.data.images.load(str(path),check_existing=False);pixels=list(image.pixels);rgb=[pixels[i:i+3] for i in range(0,len(pixels),4)];luma=[.2126*p[0]+.7152*p[1]+.0722*p[2] for p in rgb];result={'meanLuminance':round(sum(luma)/len(luma),6),'darkFraction':round(sum(x<.03 for x in luma)/len(luma),6),'clippedFraction':round(sum(max(p)>.995 for p in rgb)/len(rgb),6)};bpy.data.images.remove(image);return result

def geometry_artifact_sha(primitives):
    payload=[{'stableId':p['stableId'],'positions':p['positions'],'normals':p['normals'],'tangents':p['tangents'],'uv':p['uv'],'indices':p['indices']} for p in primitives]
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def projected_area(scene,cam,objects,w,h,alpha_coverage):
    total=0
    for o in objects:
        if o.type!='MESH' or not o.name.startswith('hair-primitive'):continue
        world=o.matrix_world
        for p in o.data.polygons:
            q=[world_to_camera_view(scene,cam,world@o.data.vertices[i].co) for i in p.vertices];pts=[(x.x*w,(1-x.y)*h) for x in q]
            total+=abs(sum(pts[i][0]*pts[(i+1)%len(pts)][1]-pts[(i+1)%len(pts)][0]*pts[i][1] for i in range(len(pts)))/2)*alpha_coverage
    return total

def portable_primitive(obj, stable_id, material, polygon_filter=None):
    mesh=obj.data;mesh.calc_loop_triangles();uv_layer=mesh.uv_layers.active
    try:mesh.calc_tangents(uvmap=uv_layer.name if uv_layer else '')
    except Exception:pass
    normal_matrix=obj.matrix_world.to_3x3().inverted().transposed();basis=obj.matrix_world.to_3x3();corners=mesh.corner_normals;positions=[];normals=[];tangents=[];uvs=[];indices=[]
    for tri in mesh.loop_triangles:
        poly=mesh.polygons[tri.polygon_index]
        if polygon_filter and not polygon_filter(poly):continue
        for li in tri.loops:
            loop=mesh.loops[li];p=blender_to_y_up(obj.matrix_world@mesh.vertices[loop.vertex_index].co);n=blender_to_y_up((normal_matrix@corners[li].vector).normalized());t=blender_to_y_up((basis@loop.tangent).normalized()) if loop.tangent.length>.001 else Vector((1,0,0));positions.extend(round(x,7) for x in p);normals.extend(round(x,7) for x in n);tangents.extend((round(t.x,7),round(t.y,7),round(t.z,7),round(float(loop.bitangent_sign),7)));uvs.extend(round(float(x),7) for x in (uv_layer.data[li].uv if uv_layer else (0,0)));indices.append(len(indices))
    return {'stableId':stable_id,'material':material,'positions':positions,'normals':normals,'tangents':tangents,'uv':uvs,'indices':indices,'triangles':len(indices)//3,'normalSource':'mesh.corner_normals[loop].vector'}

def main(a):
    t0=time.perf_counter();out=Path(a.out);out.mkdir(parents=True,exist_ok=True);bpy.ops.wm.read_factory_settings(use_empty=True)
    guides=json.loads(Path(a.guides).read_text());manifest=json.loads(Path(a.manifest).read_text());center=y_up_to_blender(Vector((guides['coordinates']['normalizedToSource'][12],guides['coordinates']['normalizedToSource'][13],guides['coordinates']['normalizedToSource'][14])));selected=select_guides(guides,center,a.locks)
    source=import_glb(a.source);source_mesh=[o for o in source if o.type=='MESH' and any(m and m.name.lower()=='hair' for m in o.data.materials)][0]
    source_hair=source_mesh.copy();source_hair.data=source_mesh.data.copy();source_hair.name='Source Hair Primitive 4 Mask';bpy.context.scene.collection.objects.link(source_hair);keep_hair_faces(source_hair)
    candidate_base=source_mesh.copy();candidate_base.data=source_mesh.data.copy();bpy.context.scene.collection.objects.link(candidate_base);candidate=[candidate_base];deleted=remove_hair_faces(candidate_base)
    shell=import_glb(a.shell);shell_obj=[o for o in shell if o.type=='MESH'][0];candidate.extend(shell);shell_mat=flat_material('Source Hair Shell',(0.58,0.175,0.235),.52);shell_obj.data.materials.clear();shell_obj.data.materials.append(shell_mat)
    kd=KDTree(len(shell_obj.data.vertices));
    for i,v in enumerate(shell_obj.data.vertices):kd.insert(shell_obj.matrix_world@v.co,i)
    kd.balance();atlas={r['id']:{'u0':r['uvRect'][0],'v0':r['uvRect'][1],'u1':r['uvRect'][2],'v1':r['uvRect'][3]} for r in manifest['atlas']['rects']}
    if a.card_mode=='flat':mat=flat_material('source pink flat cards',(0.58,0.175,0.235),.52)
    else:mat=atlas_material('hair.strands.retro candidate',a.albedo,a.normal,a.mask,(0.58,0.175,0.235),use_normal=a.card_mode=='atlas',normalize_albedo=a.normalize_atlas)
    cards=[]
    shell_world=[shell_obj.matrix_world@v.co for v in shell_obj.data.vertices];surface_bvh=BVHTree.FromPolygons(shell_world,[tuple(p.vertices) for p in shell_obj.data.polygons],all_triangles=True) if a.project_cards else None
    matrix=guides['coordinates']['normalizedToSource']
    for rec,root,tip,direction,length in selected:
        rec=dict(rec);rec['_matrix']=matrix;converted=[dict(s,position=y_up_to_blender(transform(matrix,vec(s['position'])))) for s in rec['samples']];rec['_matrix']=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1];obj=make_card(rec,root,tip,converted,float(rec['width'])*18.722475*a.card_width_scale,atlas[rec['atlasRectId']],kd,shell_obj,mat,surface_bvh,a.surface_offset);bpy.context.scene.collection.objects.link(obj);cards.append(obj);candidate.append(obj)
    merge_ranges=[]
    if a.merge_cards and cards:
        original_cards=list(cards);merged,merge_ranges=merge_card_objects(original_cards,mat);bpy.context.scene.collection.objects.link(merged)
        for obj in original_cards:bpy.data.objects.remove(obj,do_unlink=True)
        cards=[merged];candidate=[candidate_base]+shell+cards
    # source includes original source object; imported shell objects must not leak into source view.
    source_only=list(source);candidate_only=list(candidate);set_visible(source_only,True);set_visible(candidate_only,False)
    # Frame the verified Hair primitive, not the whole single-mesh character.
    # The source GLB stores body/face/hair as material partitions on one mesh.
    source_hair_slots={i for i,m in enumerate(source_mesh.data.materials) if m and m.name.lower()=='hair'}
    source_hair_vertices={vi for p in source_mesh.data.polygons if p.material_index in source_hair_slots for vi in p.vertices}
    hair_coords=[source_mesh.matrix_world@source_mesh.data.vertices[i].co for i in source_hair_vertices]
    mn=Vector((min(v.x for v in hair_coords),min(v.y for v in hair_coords),min(v.z for v in hair_coords)));mx=Vector((max(v.x for v in hair_coords),max(v.y for v in hair_coords),max(v.z for v in hair_coords)));c=(mn+mx)/2;radius=(mx-mn).length/2
    scene=bpy.context.scene;scene.render.engine='BLENDER_EEVEE';scene.render.resolution_x=a.size;scene.render.resolution_y=a.size;scene.render.resolution_percentage=100;scene.render.image_settings.file_format='PNG';scene.render.film_transparent=False
    if scene.world is None:scene.world=bpy.data.worlds.new('Evidence World')
    scene.world.use_nodes=True;bg=scene.world.node_tree.nodes.get('Background');bg.inputs['Color'].default_value=(.055,.065,.085,1);bg.inputs['Strength'].default_value=.55
    scene.view_settings.look='AgX - Medium High Contrast';scene.view_settings.exposure=1.0
    # Directional lights are scale-independent; this source is ~172 model units tall.
    key_data=bpy.data.lights.new('Key','SUN');key_data.energy=3.2;key_data.angle=math.radians(18);key=bpy.data.objects.new('Key',key_data);scene.collection.objects.link(key);key.rotation_euler=(math.radians(28),math.radians(-24),math.radians(-32))
    fill_data=bpy.data.lights.new('Fill','SUN');fill_data.energy=1.15;fill_data.angle=math.radians(35);fill=bpy.data.objects.new('Fill',fill_data);scene.collection.objects.link(fill);fill.rotation_euler=(math.radians(55),math.radians(38),math.radians(145))
    rim_data=bpy.data.lights.new('Rim','SUN');rim_data.energy=2.0;rim_data.angle=math.radians(12);rim=bpy.data.objects.new('Rim',rim_data);scene.collection.objects.link(rim);rim.rotation_euler=(math.radians(-35),math.radians(15),math.radians(170))
    # Empirically verified against the source face: Blender -Y is its front.
    views={'front':Vector((0,-1,0)),'side':Vector((1,0,0)),'back':Vector((0,1,0))};rendered={};atlas_image=bpy.data.images.load(str(a.albedo),check_existing=True);atlas_pixels=list(atlas_image.pixels);alpha_pixels=atlas_pixels[3::4];alpha_cov=sum(x>=.36 for x in alpha_pixels)/len(alpha_pixels)
    opaque_rgb=[atlas_pixels[i:i+3] for i in range(0,len(atlas_pixels),4) if atlas_pixels[i+3]>=.36];mean_linear=[sum(p[c] for p in opaque_rgb)/max(1,len(opaque_rgb)) for c in range(3)];source_base=[.577621,.174994,.235256];effective=[mean_linear[i]*source_base[i] for i in range(3)]
    mask_mat=bpy.data.materials.new('Mask White');mask_mat.use_nodes=True;mask_bs=mask_mat.node_tree.nodes.get('Principled BSDF');mask_bs.inputs['Base Color'].default_value=(1,1,1,1);mask_bs.inputs['Emission Color'].default_value=(1,1,1,1);mask_bs.inputs['Emission Strength'].default_value=1
    all_renderables=source_only+candidate_only+[source_hair]
    for name,d in views.items():
        cam=setup_camera(c,radius,d);src=out/f'{name}-source.png';cand=out/f'{name}-candidate.png';render(src,source_only,candidate_only+[source_hair],True,cam);render(cand,source_only+[source_hair],candidate_only,False,cam)
        src_mask=out/f'{name}-source-mask.png';cand_mask=out/f'{name}-candidate-mask.png';render_mask(src_mask,all_renderables,[source_hair],cam,mask_mat);render_mask(cand_mask,all_renderables,shell+cards,cam,mask_mat);sm=binary_mask(src_mask);cm=binary_mask(cand_mask);intersection=sum(x and y for x,y in zip(sm,cm));union=sum(x or y for x,y in zip(sm,cm));outside=sum(y and not x for x,y in zip(sm,cm));candidate_pixels=sum(cm)
        area=projected_area(scene,cam,cards,a.size,a.size,alpha_cov);rendered[name]={'source':str(src),'candidate':str(cand),'sourceMask':str(src_mask),'candidateMask':str(cand_mask),'sourceExposure':image_exposure_stats(src),'candidateExposure':image_exposure_stats(cand),'silhouetteIoU':round(intersection/max(1,union),6),'outsideSourceFraction':round(outside/max(1,candidate_pixels),6),'projectedOpaqueCardPixels':round(area,2)}
    # Export additive hair only: QEM shell + stable lock-card nodes.
    bpy.ops.object.select_all(action='DESELECT')
    for o in shell+cards:o.select_set(True)
    bpy.context.view_layer.objects.active=shell_obj;glb=out/'hair-source-locks.glb';bpy.ops.export_scene.gltf(filepath=str(glb),export_format='GLB',use_selection=True,export_apply=True,export_tangents=True,export_extras=True)
    hair_slots={i for i,m in enumerate(source_mesh.data.materials) if m and m.name.lower()=='hair'}
    maps={'albedo':{'kind':'albedo-alpha','path':str(Path(a.albedo).resolve()),'sha256':sha(a.albedo),'format':'PNG sRGB RGBA alpha coverage'},'normal':{'kind':'tangent-normal','path':str(Path(a.normal).resolve()),'sha256':sha(a.normal),'format':'PNG linear tangent OpenGL +Y'},'mask':{'kind':'urp-mask','path':str(Path(a.mask).resolve()),'sha256':sha(a.mask),'format':'PNG linear R metallic/G AO/B 1/A smoothness'},'direction':{'kind':'strand-direction','path':str((Path(a.albedo).parent/'hair.strands.retro_direction.png').resolve()),'sha256':sha(Path(a.albedo).parent/'hair.strands.retro_direction.png'),'format':'PNG linear RG du/dv,+V; B confidence; A coverage'}}
    atlas_semantics={'size':[1024,1024],'orientation':'root v0 PNG bottom; tip v1 PNG top','rects':manifest['atlas']['rects'],'cutoff':.36}
    source_to_normalized=guides['coordinates']['sourceToNormalized'];meters_per_normalized=guides['coordinates']['metersPerNormalizedUnit'];meters_per_source=meters_per_normalized*source_to_normalized[0]
    # Column-major composition: source -> normalized head space -> metres,
    # then flip normalized +Z to Unity-facing -Z. Translation is essential.
    source_to_unity=[meters_per_source,0,0,0,0,meters_per_source,0,0,0,0,-meters_per_source,0,source_to_normalized[12]*meters_per_normalized,source_to_normalized[13]*meters_per_normalized,-source_to_normalized[14]*meters_per_normalized,1];coordinates={'handedness':'right','up':'+Y','forward':'-Z','units':'source_model_units','sourceToNormalized':source_to_normalized,'normalizedToSource':guides['coordinates']['normalizedToSource'],'metersPerNormalizedUnit':meters_per_normalized,'scaleProvenance':guides['coordinates']['scaleProvenance'],'sourceToUnityMeters':source_to_unity,'sourceToUnityDeterminantSign':-1,'transformRule':'positions affine; normals/tangent xyz inverse-transpose 3x3; flip winding because determinant < 0','tangentEncoding':'vec4 xyz direction, w bitangent handedness'}
    source_json={'schema':'autorig.hair.source-neutral/1','sourceSha256':sha(a.source),'coordinates':coordinates,'primitive':portable_primitive(source_mesh,'source-hair-primitive-4',{'mode':'source-base-color','baseColorFactor':[.577621,.174994,.235256,1]},lambda p:p.material_index in hair_slots)};source_json['artifactSha256']=geometry_artifact_sha([source_json['primitive']])
    source_path=out/'source-hair-neutral.json';source_path.write_text(json.dumps(source_json,separators=(',',':')))
    candidate_primitives=[portable_primitive(shell_obj,'qem-shell-2500',{'mode':'source-base-color','baseColorFactor':[.577621,.174994,.235256,1]})]+[portable_primitive(o,o.name,{'mode':'atlas-cards','baseColorFactor':[.577621,.174994,.235256,1],'atlasKey':'hair.strands.retro'}) for o in cards]
    candidate_json={'schema':'autorig.hair.hybrid-candidate/1','sourceSha256':sha(a.source),'coordinates':coordinates,'materials':maps,'atlas':atlas_semantics,'cardMergeRanges':merge_ranges,'primitives':candidate_primitives,'limitations':['additive WIP; semantic replacement gate closed','static art preview; deformation not validated here']};candidate_json['artifactSha256']=geometry_artifact_sha(candidate_primitives)
    candidate_path=out/'candidate-geometry.json';candidate_path.write_text(json.dumps(candidate_json,separators=(',',':')))
    finite_uv=all(math.isfinite(x) and 0<=x<=1 for o in cards for uv in o.data.uv_layers.active.data for x in uv.uv);finite_tangent=all(math.isfinite(x) for o in cards for loop in o.data.loops for x in loop.tangent)
    exposure_gate=all(v['sourceExposure']['meanLuminance']>=.08 and v['candidateExposure']['meanLuminance']>=.08 and v['sourceExposure']['clippedFraction']<=.02 and v['candidateExposure']['clippedFraction']<=.02 for v in rendered.values())
    report={'schema':'autorig.hair.source-lock-candidate/1','status':'wip_visual_candidate','variant':{'cardMode':a.card_mode,'locks':a.locks,'surfaceProjected':a.project_cards,'surfaceOffset':a.surface_offset,'cardWidthScale':a.card_width_scale,'atlasNormalized':a.normalize_atlas,'cardsMerged':a.merge_cards},'source':{'sha256':sha(a.source),'hairPrimitive':4,'deletedHairFacesInCandidateBody':deleted},'inputs':{'guidesSha256':sha(a.guides),'shellSha256':sha(a.shell),'atlasAlbedoSha256':sha(a.albedo),'atlasNormalSha256':sha(a.normal),'atlasMaskSha256':sha(a.mask)},'atlasPhotometry':{'opaqueTexelCount':len(opaque_rgb),'meanOpaqueAlbedoLinear':[round(x,6) for x in mean_linear],'sourceBaseColorFactor':source_base,'effectiveMultipliedLinear':[round(x,6) for x in effective],'channelRetention':[round(mean_linear[i],6) for i in range(3)]},'selection':{'inputGuides':len(guides['guides']),'selectedLocks':len(selected),'stableIds':[x[0]['id'] for x in selected],'mergeRanges':merge_ranges},'geometry':{'shellTriangles':sum(len(o.data.polygons) for o in shell if o.type=='MESH'),'cardTriangles':sum(len(o.data.polygons) for o in cards),'cardObjects':len(cards),'finiteUv':finite_uv,'finiteTangents':finite_tangent,'alphaCoverageAtCutoff':round(alpha_cov,6)},'captureGate':{'meanLuminanceMin':.08,'clippedFractionMax':.02,'pass':exposure_gate},'views':rendered,'glb':{'path':str(glb),'sha256':sha(glb),'bytes':glb.stat().st_size},'portable':{'sourceNeutral':{'path':str(source_path),'sha256':sha(source_path),'artifactSha256':source_json['artifactSha256']},'candidate':{'path':str(candidate_path),'sha256':sha(candidate_path),'artifactSha256':candidate_json['artifactSha256']}},'timingSeconds':round(time.perf_counter()-t0,4),'limitations':['QEM shell is additive WIP and semantic replacement gate remains closed','cards use source-tip NMS and approximate roots','no animation/deformation in this art comparison']};(out/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps({'ok':True,'captureGate':exposure_gate,'locks':len(selected),'triangles':report['geometry'],'glb':str(glb),'seconds':report['timingSeconds']}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--shell',required=True);p.add_argument('--guides',required=True);p.add_argument('--manifest',required=True);p.add_argument('--albedo',required=True);p.add_argument('--normal',required=True);p.add_argument('--mask',required=True);p.add_argument('--out',required=True);p.add_argument('--locks',type=int,default=22);p.add_argument('--card-mode',choices=('flat','atlas-no-normal','atlas'),default='atlas');p.add_argument('--project-cards',action='store_true');p.add_argument('--surface-offset',type=float,default=.12);p.add_argument('--card-width-scale',type=float,default=.28);p.add_argument('--normalize-atlas',action='store_true');p.add_argument('--merge-cards',action='store_true');p.add_argument('--size',type=int,default=768);main(p.parse_args(sys.argv[sys.argv.index('--')+1:] if '--' in sys.argv else None))
