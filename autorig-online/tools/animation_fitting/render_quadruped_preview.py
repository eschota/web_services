"""Render continuous gameplay-motion evidence from the actual authored .blend."""
import argparse
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys

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


def configure_scene(arm,profile_path):
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
        obj.location=position;aim(obj,(0,-.5,1.2));lights.append((obj,Vector(position)))
    data=bpy.data.cameras.new('Review camera');data.type='ORTHO';data.ortho_scale=6
    cam=bpy.data.objects.new('Review camera',data);scene.collection.objects.link(cam)
    cam.location=(6,-3.3,3.2);aim(cam,(0,-.5,1.2));scene.camera=cam
    font=bpy.data.curves.new('Review label','FONT');font.size=.13; font.body=''
    text=bpy.data.objects.new('Review label',font);scene.collection.objects.link(text)
    text.parent=cam;text.location=(-2.78,1.45,-4)
    white=bpy.data.materials.new('Label');white.use_nodes=True
    nodes=white.node_tree.nodes;nodes.clear();e=nodes.new('ShaderNodeEmission');e.inputs[0].default_value=(.85,.95,1,1)
    out=nodes.new('ShaderNodeOutputMaterial');white.node_tree.links.new(e.outputs[0],out.inputs['Surface']);font.materials.append(white)
    return cam,lights,font


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--report',type=Path,required=True)
    p.add_argument('--action',required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--repetitions',type=int,default=4)
    p.add_argument('--profile',type=Path,default=Path(__file__).parent/'data/semantic_ltx_profiles/horse_2.v1.json')
    p.add_argument('--ffmpeg',default='ffmpeg')
    args=p.parse_args(sys.argv[sys.argv.index('--')+1:])
    # Blender's render path resolver is not pathlib's cwd resolver. Pin all
    # paths before opening a .blend so frames cannot escape the chosen folder.
    args.output=args.output.resolve();args.report=args.report.resolve();args.source=args.source.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    report=json.loads(args.report.read_text());clip=next(c for c in report['clips'] if c['action']==args.action)
    bpy.ops.wm.open_mainfile(filepath=str(args.source.resolve()))
    arm=next(o for o in bpy.data.objects if o.type=='ARMATURE')
    arm.data.pose_position='POSE';arm.animation_data.action=bpy.data.actions[args.action]
    arm.animation_data.action_slot=arm.animation_data.action.slots[0]
    bpy.context.view_layer.objects.active=arm
    if arm.mode!='OBJECT':bpy.ops.object.mode_set(mode='OBJECT')
    cam,lights,font=configure_scene(arm,args.profile)
    interval=clip['timing']['interval_count'];total=interval*args.repetitions
    font.body=f"{args.action.upper()}   {clip['reference_speed']:.2f} m/s   30 FPS\nAUTHORED CANDIDATE / 0.5 m FLOOR GRID"
    for i in range(total):
        sample=i%interval;cycle=i//interval
        bpy.context.scene.frame_set(sample)
        travel=Vector((0,-clip['reference_speed']*(cycle*interval/30 if clip['root_motion'] else i/30),0))
        arm.location=travel
        camera_travel=Vector((0,-clip['reference_speed']*i/30,0))
        cam.location=Vector((6,-3.3,3.2))+camera_travel
        aim(cam,Vector((0,-.5,1.2))+camera_travel)
        for obj,position in lights:obj.location=position+camera_travel
        bpy.context.scene.render.filepath=str(args.output/f'frame_{i:05d}.png')
        bpy.ops.render.render(write_still=True)
    output=args.output/(args.action+'-continuous.mp4')
    subprocess.run([args.ffmpeg,'-n','-v','error','-framerate','30','-i',str(args.output/'frame_%05d.png'),
        '-c:v','libx264','-crf','17','-pix_fmt','yuv420p',str(output)],check=True)
    print('CONTINUOUS_PREVIEW='+json.dumps({'video':str(output),'frames':total,'fps':30,'cycles':args.repetitions}))


if __name__=='__main__':main()
