"""Render a native forefoot close-up before/after nominal contact IK, at 4x slow motion."""
import argparse,hashlib,json,shutil,subprocess,sys
from pathlib import Path
import bpy
from mathutils import Matrix,Vector
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
from render_quadruped_preview import configure_scene,aim


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--envelope',type=Path,required=True);p.add_argument('--qa',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args(sys.argv[sys.argv.index('--')+1:])
    source=a.source.resolve();out=a.output.resolve();envelope=json.loads(a.envelope.read_text());qa=json.loads(a.qa.read_text())
    source_hash=sha(source)
    if not qa['passed'] or qa['source_native_sha256']!=source_hash or qa['envelope_sha256']!=sha(a.envelope):
        raise ValueError('Passed native QA must bind the exact source and envelope')
    pin=envelope['rig_blueprint_pin']
    if sha(pin['path'])!=pin['sha256']:raise ValueError('Blueprint changed')
    blueprint=json.loads(Path(pin['path']).read_text())
    if out.exists():raise ValueError('Fresh output required')
    out.mkdir(parents=True)
    bpy.ops.wm.open_mainfile(filepath=str(source))
    arm=bpy.data.objects[blueprint['armature']];arm.data.pose_position='POSE';action=bpy.data.actions[envelope['action']]
    source_scales={b.name:b.scale.copy() for b in arm.pose.bones}
    rest={r['name']:Matrix([r['rest_local'][i:i+4] for i in range(0,16,4)]) for r in blueprint['bones']}
    samples=[s for s in envelope['samples'] if 38<=s['frame']<=44]
    if len(samples)!=25:raise ValueError('Expected all quarter poses over the contact window')
    touchdown=next(s for s in samples if s['frame']==40)
    target=Vector(np.mean([touchdown['world_targets'][f] for f in ('fore_left','fore_right')],axis=0))+Vector((0,0,.035))
    scene=bpy.context.scene;scene.frame_set(40)
    cam,_,font,_,_=configure_scene(arm,Path(__file__).parent/'data/semantic_ltx_profiles/horse_2.v1.json','original','three-quarter')
    old_scale=cam.data.ortho_scale;cam.data.ortho_scale=.28
    cam.location=target+Vector((.35,-1,.28)).normalized()*1.2;aim(cam,target)
    ratio=cam.data.ortho_scale/old_scale
    label=bpy.data.objects['Review label'];label.location.x*=ratio;label.location.y*=ratio;font.size*=ratio
    plate=bpy.data.objects['Review label backing'];plate.scale.x*=ratio;plate.scale.y*=ratio
    checker=next(n for n in bpy.data.objects['Half metre floor'].data.materials[0].node_tree.nodes if n.type=='TEX_CHECKER')
    checker.inputs['Scale'].default_value=20
    videos={}
    for kind in ('before','after'):
        folder=out/kind;folder.mkdir()
        font.body=('P8 ANIMATION' if kind=='before' else 'NOMINAL CONTACT IK')+' / 4x SLOW\nNATIVE FOREFOOT CLOSE-UP / 5 cm FLOOR'
        for index,sample in enumerate(samples):
            frame=sample['frame'];arm.location=Vector(sample['actor_translation'])
            if kind=='before':
                arm.animation_data.action=action;arm.animation_data.action_slot=action.slots[0]
                for name,scale in source_scales.items():arm.pose.bones[name].scale=scale
                scene.frame_set(int(frame),subframe=frame-int(frame))
            else:
                arm.animation_data.action=None
                scene.frame_set(int(frame),subframe=frame-int(frame))
                for name,values in sample['corrected_local'].items():
                    local=Matrix([values[i:i+4] for i in range(0,16,4)])
                    arm.pose.bones[name].matrix_basis=rest[name].inverted()@local;arm.pose.bones[name].scale=(1,1,1)
            bpy.context.view_layer.update()
            scene.render.filepath=str(folder/f'pose_{index:03d}.png');bpy.ops.render.render(write_still=True)
        sequence=([0]*10+list(range(25))+[24]*10)*3
        for i,j in enumerate(sequence):shutil.copyfile(folder/f'pose_{j:03d}.png',folder/f'frame_{i:04d}.png')
        video=folder/(kind+'.mp4')
        subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-n','-framerate','30','-i',str(folder/'frame_%04d.png'),
            '-c:v','libx264','-crf','18','-pix_fmt','yuv420p',str(video)],check=True)
        videos[kind]=video
    result=out/'contact-ik-comparison.mp4'
    subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-n','-i',str(videos['before']),'-i',str(videos['after']),
        '-filter_complex','hstack=inputs=2','-c:v','libx264','-crf','18','-pix_fmt','yuv420p','-movflags','+faststart',str(result)],check=True)
    if sha(source)!=source_hash:raise ValueError('Source modified')
    (out/'capture.json').write_text(json.dumps({'video_sha256':sha(result),'source_native_sha256':source_hash,
        'envelope_sha256':sha(a.envelope),'native_qa_sha256':sha(a.qa),'frames':135,'fps':30,
        'reference_speed_factor':.25,'source_frame_window':[38,44],'unique_poses_per_side':25,
        'repeated_frames_copied':True,'camera_matrix_world':[list(r) for r in cam.matrix_world],
        'ortho_scale':cam.data.ortho_scale,'quality_approved':False,
        'scope':'nominal reference contact only; no collision/engine/optimized-skin approval'},indent=2))
    print('CONTACT_IK_DIAGNOSTIC_COMPLETE',str(result),flush=True)


if __name__=='__main__':main()
