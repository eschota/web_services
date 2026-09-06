"""Render continuous gameplay-motion evidence from the actual authored .blend."""
import argparse
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from quadruped_clip_semantics import require_v1_export_report

import bpy
from mathutils import Vector


def material(name,color):
    m=bpy.data.materials.new(name);m.diffuse_color=(*color,1);m.use_nodes=True
    bsdf=m.node_tree.nodes.get('Principled BSDF')
    bsdf.inputs['Base Color'].default_value=(*color,1)
    bsdf.inputs['Roughness'].default_value=.65
    return m


def aim(obj,target):
    obj.rotation_euler=(Vector(target)-obj.location).to_track_quat('-Z','Y').to_euler()


def configure_scene(arm,profile_path,material_mode='semantic',view_mode='three-quarter'):
    scene=bpy.context.scene
    scene.render.engine='CYCLES'
    scene.cycles.device='CPU';scene.cycles.samples=12;scene.cycles.use_denoising=True
    scene.render.threads_mode='FIXED';scene.render.threads=8
    scene.render.resolution_x=960;scene.render.resolution_y=540;scene.render.resolution_percentage=100
    scene.render.image_settings.file_format='PNG'
    scene.render.film_transparent=False
    scene.render.fps=30;scene.render.fps_base=1
    scene.world=bpy.data.worlds.new('Review world');scene.world.use_nodes=True
    scene.world.node_tree.nodes['Background'].inputs[0].default_value=(.12,.16,.20,1)
    scene.world.node_tree.nodes['Background'].inputs[1].default_value=.6
    if material_mode=='semantic':
        profile=json.loads(profile_path.read_text())
        labels=list(profile['limb_groups'])
        colours={'fore_left':(.015,.68,.82),'fore_right':(.08,.17,.8),
                 'hind_left':(.95,.63,.015),'hind_right':(.82,.04,.35)}
        mats=[material('Body',(.62,.68,.72))]+[material(n,colours[n]) for n in labels]
        bone_groups={bone:label for label,row in profile['limb_groups'].items() for bone in row['bones']}
        for obj in list(bpy.data.objects):
            if obj.type!='MESH':continue
            obj.data.materials.clear()
            for mat in mats:obj.data.materials.append(mat)
            groups={g.index:g.name for g in obj.vertex_groups}
            for poly in obj.data.polygons:
                scores={label:0. for label in labels}
                for idx in poly.vertices:
                    for w in obj.data.vertices[idx].groups:
                        label=bone_groups.get(groups[w.group])
                        if label:scores[label]+=w.weight/len(poly.vertices)
                dominant=max(scores,key=scores.get)
                poly.material_index=labels.index(dominant)+1 if scores[dominant]>=.35 else 0
    scene.frame_set(0)
    graph=bpy.context.evaluated_depsgraph_get()
    render_meshes=[obj for obj in bpy.data.objects if obj.type=='MESH' and not obj.hide_render
                   and any(mod.type=='ARMATURE' and mod.object==arm for mod in obj.modifiers)]
    if not render_meshes:raise ValueError('No render mesh bound to the review armature')
    corners=[obj.matrix_world @ Vector(corner) for obj in render_meshes
             for corner in obj.evaluated_get(graph).bound_box]
    minimum=Vector(tuple(min(point[i] for point in corners) for i in range(3)))
    maximum=Vector(tuple(max(point[i] for point in corners) for i in range(3)))
    target=(minimum+maximum)*.5
    radius=max((maximum-minimum).length*.5,.1)
    bpy.ops.mesh.primitive_plane_add(size=100,location=(0,0,0))
    floor=bpy.context.object;floor.name='Half metre floor'
    mat=material('Ground',(.10,.14,.16));floor.data.materials.append(mat)
    tree=mat.node_tree;checker=tree.nodes.new('ShaderNodeTexChecker')
    checker.inputs['Color1'].default_value=(.075,.11,.13,1)
    checker.inputs['Color2'].default_value=(.13,.18,.19,1)
    checker.inputs['Scale'].default_value=2
    coord=tree.nodes.new('ShaderNodeTexCoord')
    tree.links.new(coord.outputs['Object'],checker.inputs['Vector'])
    tree.links.new(checker.outputs['Color'],tree.nodes['Principled BSDF'].inputs['Base Color'])
    lights=[]
    for name,position,power,size in [('Key',(3,-4,6),1300,5),('Fill',(-4,-1,4),700,4),('Rim',(2,4,6),1500,3)]:
        data=bpy.data.lights.new(name,'AREA');data.energy=power;data.shape='DISK';data.size=size
        obj=bpy.data.objects.new(name,data);scene.collection.objects.link(obj)
        obj.location=position;aim(obj,target);lights.append((obj,Vector(position)))
    data=bpy.data.cameras.new('Review camera');data.type='ORTHO';data.ortho_scale=6
    cam=bpy.data.objects.new('Review camera',data);scene.collection.objects.link(cam)
    direction={'three-quarter':(6,-3.3,3.2),'side':(1,0,.12),'front':(0,-1,.12)}[view_mode]
    cam.location=target+Vector(direction).normalized()*radius*4
    aim(cam,target);scene.camera=cam
    bpy.context.view_layer.update()
    view=[cam.matrix_world.inverted() @ point for point in corners]
    width=max(point.x for point in view)-min(point.x for point in view)
    height=max(point.y for point in view)-min(point.y for point in view)
    # Reserve a separate top band for the caption, including modest head/ear
    # motion above the first pose. A restored tall neck must stay visible.
    data.ortho_scale=max(width,height*scene.render.resolution_x/scene.render.resolution_y)*1.5
    font=bpy.data.curves.new('Review label','FONT');font.size=data.ortho_scale*.022; font.body=''
    text=bpy.data.objects.new('Review label',font);scene.collection.objects.link(text)
    text.parent=cam;text.location=(-.46*data.ortho_scale,.245*data.ortho_scale,-1)
    white=bpy.data.materials.new('Label');white.use_nodes=True
    nodes=white.node_tree.nodes;nodes.clear();e=nodes.new('ShaderNodeEmission');e.inputs[0].default_value=(.85,.95,1,1)
    out=nodes.new('ShaderNodeOutputMaterial');white.node_tree.links.new(e.outputs[0],out.inputs['Surface']);font.materials.append(white)
    plate_mesh=bpy.data.meshes.new('Review label backing')
    scale=data.ortho_scale
    plate_mesh.from_pydata([(-.48*scale,.215*scale,-1.02),(.48*scale,.215*scale,-1.02),
                           (.48*scale,.28*scale,-1.02),(-.48*scale,.28*scale,-1.02)],[],[(0,1,2,3)])
    plate=bpy.data.objects.new('Review label backing',plate_mesh);scene.collection.objects.link(plate);plate.parent=cam
    backing=bpy.data.materials.new('Label backing');backing.use_nodes=True
    nodes=backing.node_tree.nodes;nodes.clear();emission=nodes.new('ShaderNodeEmission')
    emission.inputs[0].default_value=(.008,.012,.02,1)
    output=nodes.new('ShaderNodeOutputMaterial');backing.node_tree.links.new(emission.outputs[0],output.inputs['Surface'])
    plate.data.materials.append(backing)
    for visibility in ('visible_diffuse','visible_glossy','visible_transmission','visible_volume_scatter','visible_shadow'):
        if hasattr(plate,visibility):setattr(plate,visibility,False)
    return cam,lights,font,cam.location.copy(),target


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--report',type=Path,required=True)
    p.add_argument('--action',required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--repetitions',type=int,default=4)
    p.add_argument('--materials',choices=('semantic','original'),default='semantic')
    p.add_argument('--view',choices=('three-quarter','side','front'),default='three-quarter')
    p.add_argument('--profile',type=Path,default=Path(__file__).parent/'data/semantic_ltx_profiles/horse_2.v1.json')
    p.add_argument('--ffmpeg',default='ffmpeg')
    args=p.parse_args(sys.argv[sys.argv.index('--')+1:])
    # Blender's render path resolver is not pathlib's cwd resolver. Pin all
    # paths before opening a .blend so frames cannot escape the chosen folder.
    args.output=args.output.resolve();args.report=args.report.resolve();args.source=args.source.resolve()
    report=json.loads(args.report.read_text());require_v1_export_report(report)
    clip=next(c for c in report['clips'] if c['action']==args.action)
    args.output.mkdir(parents=True,exist_ok=False)
    bpy.ops.wm.open_mainfile(filepath=str(args.source.resolve()))
    arm=next(o for o in bpy.data.objects if o.type=='ARMATURE')
    arm.data.pose_position='POSE';arm.animation_data.action=bpy.data.actions[args.action]
    arm.animation_data.action_slot=arm.animation_data.action.slots[0]
    bpy.context.view_layer.objects.active=arm
    if arm.mode!='OBJECT':bpy.ops.object.mode_set(mode='OBJECT')
    cam,lights,font,camera_start,camera_target=configure_scene(arm,args.profile,args.materials,args.view)
    interval=clip['timing']['interval_count'];total=interval*args.repetitions
    font.body=f"{args.action.upper()}   {clip['reference_speed']:.2f} m/s   30 FPS\n{args.view.upper()} / AUTHORED CANDIDATE / 0.5 m GRID"
    for i in range(total):
        sample=i%interval;cycle=i//interval
        bpy.context.scene.frame_set(sample)
        travel=Vector((0,-clip['reference_speed']*(cycle*interval/30 if clip['root_motion'] else i/30),0))
        arm.location=travel
        camera_travel=Vector((0,-clip['reference_speed']*i/30,0))
        cam.location=camera_start+camera_travel
        aim(cam,camera_target+camera_travel)
        for obj,position in lights:obj.location=position+camera_travel
        bpy.context.scene.render.filepath=str(args.output/f'frame_{i:05d}.png')
        bpy.ops.render.render(write_still=True)
    output=args.output/(args.action+'-continuous.mp4')
    subprocess.run([args.ffmpeg,'-n','-v','error','-framerate','30','-i',str(args.output/'frame_%05d.png'),
        '-c:v','libx264','-crf','17','-pix_fmt','yuv420p',str(output)],check=True)
    print('CONTINUOUS_PREVIEW='+json.dumps({'video':str(output),'frames':total,'fps':30,'cycles':args.repetitions}))


if __name__=='__main__':main()
