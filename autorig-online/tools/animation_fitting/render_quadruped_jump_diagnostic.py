"""Render a finite v2 jump reference on the verified native mesh.

The source Blend remains untouched. Actor translation is applied once to the
armature object for display; it is never baked into source skeletal actions.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import bpy
import numpy as np
from mathutils import Vector

sys.path.insert(0, str(Path(__file__).resolve().parent))
from quadruped_clip_semantics import validate_v2_clip, apply_reference_actor_translation
from blender_quadruped_bridge import evaluated_points
from render_quadruped_preview import configure_scene, aim


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--report', type=Path, required=True)
    p.add_argument('--skin-proof', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--action', default='jump_full')
    p.add_argument('--qa-only', action='store_true', help='Check native geometry and actor placement without rerendering media')
    p.add_argument('--preflight-only', action='store_true', help='Verify input provenance without opening or rendering the Blend')
    args = p.parse_args(sys.argv[sys.argv.index('--')+1:])
    source, report_path, proof_path, out = (v.resolve() for v in (args.source, args.report, args.skin_proof, args.output))
    report = json.loads(report_path.read_text()); proof = json.loads(proof_path.read_text())
    if report.get('schema') != 'autorig-quadruped-export-candidate.v2':
        raise ValueError('Explicit v2 export report required')
    if proof.get('schema', 'autorig-full-native-skin-proof.v1') != 'autorig-full-native-skin-proof.v1':
        raise ValueError('Unsupported native skin proof schema')
    source_hash = sha(source)
    if proof['output_sha256'] != source_hash or proof['source_sha256'] != report['files']['authored-candidates.blend']['sha256']:
        raise ValueError('Native skin proof does not match source and authored report')
    rig_pin = report['rig_blueprint_pin']; rig_path = Path(rig_pin['path'])
    if sha(rig_path) != rig_pin['sha256']: raise ValueError('Blueprint pin changed')
    blueprint = json.loads(rig_path.read_text())
    if report.get('source_sha256') != blueprint['source_sha256']:
        raise ValueError('Report and authoring blueprint source pins disagree')
    matching = [c for c in report['clips'] if c['action'] == args.action]
    if len(matching) != 1: raise ValueError('Expected exactly one requested action')
    clip = matching[0]; context = validate_v2_clip(clip, blueprint)
    if (clip.get('source_sha256') != report['source_sha256'] or
            clip.get('rig_source_sha256') != report['source_sha256'] or
            clip.get('rig_blueprint_sha256') != rig_pin['sha256']):
        raise ValueError('Selected clip and report provenance pins disagree')
    if context.playback_mode != 'one_shot': raise ValueError('Diagnostic requires one complete one-shot')
    if args.preflight_only:
        print('JUMP_DIAGNOSTIC_PREFLIGHT_OK',json.dumps({'native_blend_sha256':source_hash,
            'compiled_posture_blend_sha256':proof['source_sha256'], 'authoring_source_sha256':report['source_sha256'],
            'report_sha256':sha(report_path), 'action':clip['action'], 'sample_count':context.sample_count}),flush=True)
        return
    if out.exists(): raise ValueError('Output directory must be fresh')
    out.mkdir(parents=True, exist_ok=False)
    bpy.ops.wm.open_mainfile(filepath=str(source))
    arm = bpy.data.objects[blueprint['armature']]
    arm.data.pose_position = 'POSE'; action = bpy.data.actions[args.action]
    arm.animation_data.action = action; arm.animation_data.action_slot = action.slots[0]
    np.testing.assert_allclose(np.asarray(arm.matrix_world), np.eye(4), rtol=0, atol=1e-7)
    scene = bpy.context.scene; names = [m['name'] for m in blueprint['meshes']]
    minimum = np.full(3, np.inf); maximum = np.full(3, -np.inf)
    qa = {'sample_count':(context.sample_count-1)*4+1, 'max_target_error_m':0.,
          'minimum_ground_clearance_m':float('inf'), 'max_stance_height_m':None,
          'semantic_target_tolerance_m':context.ground_tolerance,
          'actual_surface_tolerance_m':.006, 'quality_approved':False}
    for step in range(qa['sample_count']):
        t=step/4; i=int(t); f=t-i; nxt=min(i+1, context.sample_count-1)
        scene.frame_set(i, subframe=f)
        actor = context.actor_translation[i]*(1-f)+context.actor_translation[nxt]*f
        points, _ = apply_reference_actor_translation(np.asarray([list(v) for v in evaluated_points(arm,names)],dtype=float), actor, sample_space='actor_local')
        minimum=np.minimum(minimum, points.min(axis=0)); maximum=np.maximum(maximum,points.max(axis=0))
        clearance=float(points[:,2].min()-context.ground_height)
        if clearance < qa['minimum_ground_clearance_m']:
            qa.update(minimum_ground_clearance_m=clearance, worst_ground_sample=t, worst_ground_vertex=int(points[:,2].argmin()))
        for leg, anchors in context.sole_anchors.items():
            sole=points[list(anchors['sole_vertices'])].mean(axis=0)
            target=context.targets[leg][i]*(1-f)+context.targets[leg][nxt]*f+actor
            qa['max_target_error_m']=max(qa['max_target_error_m'],float(np.linalg.norm(sole-target)))
            if context.contacts[leg][i] and (f==0 or context.contacts[leg][nxt]):
                qa['max_stance_height_m']=max(qa['max_stance_height_m'] or 0.,abs(float(sole[2]-context.ground_height)))
    qa['within_target_band'] = qa['max_target_error_m'] <= context.ground_tolerance and qa['minimum_ground_clearance_m'] >= -context.ground_tolerance and (qa['max_stance_height_m'] is None or qa['max_stance_height_m']<=context.ground_tolerance)
    qa['within_surface_gate'] = qa['minimum_ground_clearance_m'] >= -.006 and qa['max_target_error_m'] <= .006 and (qa['max_stance_height_m'] is None or qa['max_stance_height_m']<=.006)
    actor_errors = []
    for sample in (0, int(context.actor_translation[:,2].argmax()), context.sample_count-1):
        arm.location = (0,0,0); scene.frame_set(sample); bpy.context.view_layer.update()
        expected = np.asarray([list(v) for v in evaluated_points(arm,names)]) + context.actor_translation[sample]
        arm.location = Vector(context.actor_translation[sample]); bpy.context.view_layer.update()
        actual = []
        graph = bpy.context.evaluated_depsgraph_get()
        for name in names:
            obj = bpy.data.objects[name].evaluated_get(graph)
            mesh = obj.to_mesh()
            try: actual.extend(list(obj.matrix_world @ v.co) for v in mesh.vertices)
            finally: obj.to_mesh_clear()
        error = float(np.linalg.norm(np.asarray(actual)-expected,axis=1).max())
        if error > 1e-6: raise ValueError('Display actor transform does not match once-only numeric reference')
        actor_errors.append({'sample':sample,'max_world_position_error_m':error})
    arm.location=(0,0,0); bpy.context.view_layer.update()
    qa['actor_application_checks'] = actor_errors
    (out/'native-surface-qa.json').write_text(json.dumps(qa,indent=2))
    if args.qa_only:
        print('JUMP_NATIVE_QA_COMPLETE',json.dumps(qa),flush=True)
        return
    # Diagnostic media records failures too; it must not become a game-asset
    # approval simply because a renderer can display the source.
    scene.frame_set(0); arm.location=(0,0,0)
    cam, lights, font, _, _ = configure_scene(arm, Path(__file__).parent/'data/semantic_ltx_profiles/horse_2.v1.json', 'original', 'three-quarter')
    old_scale=cam.data.ortho_scale
    target=Vector((minimum+maximum)/2); radius=float(np.linalg.norm(maximum-minimum)/2)
    cam.location=target+Vector((6,-3.3,3.2)).normalized()*radius*4; aim(cam,target)
    bpy.context.view_layer.update()
    corners=[Vector((x,y,z)) for x in (minimum[0],maximum[0]) for y in (minimum[1],maximum[1]) for z in (minimum[2],maximum[2])]
    view=[cam.matrix_world.inverted()@v for v in corners]
    width=max(v.x for v in view)-min(v.x for v in view)
    height=max(v.y for v in view)-min(v.y for v in view)
    cam.data.ortho_scale=max(width,height*960/540)*1.22
    ratio=cam.data.ortho_scale/old_scale
    label=bpy.data.objects['Review label']; label.location.x*=ratio; label.location.y*=ratio; font.size*=ratio
    plate=bpy.data.objects['Review label backing']; plate.scale.x*=ratio; plate.scale.y*=ratio
    font.body='STANDING JUMP / 30 FPS\nNATIVE SKIN / EXTERNAL ACTOR TRAJECTORY'
    samples=[0]*10+list(range(context.sample_count))+[context.sample_count-1]*15
    # Two complete attempts with explicit neutral pauses, not modulo playback
    # of each takeoff/air/landing phase.
    samples=samples*2
    for frame,sample in enumerate(samples):
        scene.frame_set(sample); arm.location=Vector(context.actor_translation[sample])
        scene.render.filepath=str(out/f'frame_{frame:05d}.png')
        bpy.ops.render.render(write_still=True)
    video=out/'jump-full-native-diagnostic.mp4'
    subprocess.run(['ffmpeg','-hide_banner','-loglevel','warning','-n','-framerate','30','-i',str(out/'frame_%05d.png'),
        '-c:v','libx264','-crf','18','-pix_fmt','yuv420p','-movflags','+faststart',str(video)],check=True)
    if sha(source)!=source_hash: raise ValueError('Renderer changed source Blend')
    (out/'capture.json').write_text(json.dumps({'video':str(video),'video_sha256':sha(video),'source_sha256':source_hash,
        'native_weights_sha256':proof['full_weights_sha256'],'action':args.action,'frames':len(samples),'fps':30,
        'complete_attempts':2,'actor_applied_once':True,'native_qa':qa,'quality_approved':False},indent=2))
    print('JUMP_NATIVE_DIAGNOSTIC_COMPLETE',json.dumps({'video':str(video),'frames':len(samples),'qa':qa}),flush=True)


if __name__=='__main__': main()
