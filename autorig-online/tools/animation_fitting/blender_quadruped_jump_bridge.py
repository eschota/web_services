"""Apply explicit v2 jump clips with external actor motion used only for QA.

This dedicated consumer leaves legacy v1 readers unchanged. Exported bones
contain local posture; the reference actor trajectory remains in the sidecar.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

import bpy
from mathutils import Matrix, Quaternion, Vector
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from blender_quadruped_bridge import object_mode, evaluated_points, action_curves
from quadruped_clip_semantics import validate_v2_clip, apply_reference_actor_translation

# Existing P6 evaluated-surface/contact policy, distinct from the much
# tighter semantic target-plane declaration validated above.
ACTUAL_SURFACE_TOLERANCE_M = .006


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--rig', type=Path, required=True)
    parser.add_argument('--clips', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--diagnostic-only', action='store_true',
                        help='Save a clearly labelled Blend for review, including failed QA; no game exports')
    args = parser.parse_args(sys.argv[sys.argv.index('--')+1:])
    source, rig_path, clips_path, output = (p.resolve() for p in (args.source, args.rig, args.clips, args.output))
    blueprint = json.loads(rig_path.read_text())
    source_hash, rig_hash = sha(source), sha(rig_path)
    if source_hash != blueprint['source_sha256']:
        raise ValueError('Source Blend and blueprint pin disagree')
    clips = [json.loads(p.read_text()) for p in sorted(clips_path.glob('*.json'))]
    if not clips or len({c['action'] for c in clips}) != len(clips):
        raise ValueError('Unique nonempty v2 clip set required')
    contexts = {}
    for clip in clips:
        context = validate_v2_clip(clip, blueprint)
        if (clip.get('source_sha256') != source_hash or clip.get('rig_source_sha256') != source_hash or
                clip.get('rig_blueprint_sha256') != rig_hash):
            raise ValueError('Clip source pins disagree')
        contexts[clip['action']] = context
    if output.exists():
        raise ValueError('Output directory must be fresh')
    bpy.ops.wm.open_mainfile(filepath=str(source))
    arm = bpy.data.objects[blueprint['armature']]
    object_mode(arm); arm.data.pose_position = 'POSE'
    if not np.allclose(np.asarray(arm.matrix_world), np.eye(4), rtol=0, atol=1e-7):
        raise ValueError('V2 reference actor requires normalized identity armature transform')
    mesh_names = [m['name'] for m in blueprint['meshes']]
    obj = bpy.data.objects[mesh_names[0]]
    # Preserve and verify the single source mesh before assigning any actions.
    relative = arm.matrix_world.inverted() @ obj.matrix_world
    np.testing.assert_allclose([list(relative @ v.co) for v in obj.data.vertices],
        [v['point'] for v in blueprint['meshes'][0]['vertices']], rtol=0, atol=1e-7)
    if [list(p.vertices) for p in obj.data.polygons] != blueprint['meshes'][0]['faces']:
        raise ValueError('Source mesh topology differs from blueprint')
    world = obj.matrix_world.copy(); obj.parent = arm; obj.matrix_world = world
    arm.animation_data_clear(); arm.animation_data_create()
    rest = {b['name']: Matrix([b['rest_local'][i:i+4] for i in range(0,16,4)]) for b in blueprint['bones']}
    scene = bpy.context.scene; scene.render.fps = 30; scene.render.fps_base = 1
    validation = {}
    for clip in clips:
        name = clip['action']; context = contexts[name]
        action = bpy.data.actions.new(name)
        slot = action.slots.new(id_type='OBJECT', name=arm.name)
        arm.animation_data.action = action; arm.animation_data.action_slot = slot
        action.use_fake_user = True
        action['semantic_id'] = name; action['playback_mode'] = context.playback_mode
        action['loop'] = context.playback_mode == 'loop'; action['world_motion_owner'] = 'controller'
        action['quality_approved'] = False
        for index, frame in enumerate(clip['frames']):
            scene.frame_set(index)
            for bone, trs in frame['bones'].items():
                q = trs['rotation']
                local = Quaternion((q[3], q[0], q[1], q[2])).to_matrix().to_4x4()
                local.translation = Vector(trs['translation'])
                pose = arm.pose.bones[bone]; pose.rotation_mode = 'QUATERNION'
                pose.matrix_basis = rest[bone].inverted() @ local
                if max(abs(s-1) for s in pose.scale) > 1e-4:
                    raise ValueError('Unexpected pose scaling')
                pose.keyframe_insert('location', frame=index, group=bone)
                pose.keyframe_insert('rotation_quaternion', frame=index, group=bone)
        for curve in action_curves(action):
            for key in curve.keyframe_points: key.interpolation = 'LINEAR'
        qa = {'sample_count':(context.sample_count-1)*2+1, 'max_hoof_target_error_m':0.,
              'minimum_ground_clearance_m':float('inf'), 'max_stance_height_m':None,
              'semantic_target_ground_tolerance_m':context.ground_tolerance,
              'actual_surface_tolerance_m':ACTUAL_SURFACE_TOLERANCE_M,
              'contact_sample_counts':{leg:0 for leg in context.contacts}}
        for step in range(qa['sample_count']):
            t = step / 2; index = int(t); fraction = t-index
            nxt = min(index+1, context.sample_count-1)
            scene.frame_set(index, subframe=fraction)
            actor = context.actor_translation[index]*(1-fraction) + context.actor_translation[nxt]*fraction
            points, _ = apply_reference_actor_translation(
                np.asarray([list(p) for p in evaluated_points(arm, mesh_names)], dtype=float), actor, sample_space='actor_local')
            clearance = float(points[:,2].min()-context.ground_height)
            if clearance < qa['minimum_ground_clearance_m']:
                qa['minimum_ground_clearance_m'] = clearance
                qa['lowest_sample'] = t; qa['lowest_vertex'] = int(points[:,2].argmin())
            for leg, anchors in context.sole_anchors.items():
                sole = points[list(anchors['sole_vertices'])].mean(axis=0)
                target = context.targets[leg][index]*(1-fraction) + context.targets[leg][nxt]*fraction + actor
                qa['max_hoof_target_error_m'] = max(qa['max_hoof_target_error_m'], float(np.linalg.norm(sole-target)))
                contact = context.contacts[leg]
                if contact[index] and (fraction == 0 or contact[nxt]):
                    qa['contact_sample_counts'][leg] += 1
                    height = abs(float(sole[2]-context.ground_height))
                    qa['max_stance_height_m'] = max(qa['max_stance_height_m'] or 0., height)
        qa['realized_surface_within_target_band'] = (
            qa['max_hoof_target_error_m'] <= context.ground_tolerance and
            qa['minimum_ground_clearance_m'] >= -context.ground_tolerance and
            (qa['max_stance_height_m'] is None or qa['max_stance_height_m'] <= context.ground_tolerance))
        qa['semantic_targets_passed'] = True
        qa['passed'] = (qa['max_hoof_target_error_m'] <= ACTUAL_SURFACE_TOLERANCE_M and
                        qa['minimum_ground_clearance_m'] >= -ACTUAL_SURFACE_TOLERANCE_M and
                        (qa['max_stance_height_m'] is None or qa['max_stance_height_m'] <= ACTUAL_SURFACE_TOLERANCE_M))
        qa['quality_approved'] = False
        validation[name] = qa
        print('V2_BLENDER_QA', name, json.dumps(qa), flush=True)
    output.mkdir(parents=True, exist_ok=False)
    (output/'bridge-qa.json').write_text(json.dumps(validation, indent=2))
    passed = all(q['passed'] for q in validation.values())
    if not passed and not args.diagnostic_only:
        raise ValueError('V2 world-space deformation gate failed; no exports created')
    # The actor path was applied only to temporary numeric arrays. Assert the
    # exported armature has never acquired that world displacement.
    np.testing.assert_allclose(np.asarray(arm.matrix_world), np.eye(4), rtol=0, atol=1e-7)
    arm['world_motion_owner'] = 'controller'; arm['quality_approved'] = False
    arm['diagnostic_only'] = bool(args.diagnostic_only)
    arm['surface_qa_passed'] = bool(passed)
    arm['actual_surface_tolerance_m'] = ACTUAL_SURFACE_TOLERANCE_M
    arm['semantic_target_tolerances_m'] = json.dumps({name:c.ground_tolerance for name,c in contexts.items()})
    bpy.ops.object.select_all(action='DESELECT'); arm.select_set(True); obj.select_set(True)
    bpy.context.view_layer.objects.active = arm
    scene.frame_start = 0; scene.frame_end = max(len(c['frames'])-1 for c in clips); scene.frame_set(0)
    bpy.ops.wm.save_as_mainfile(filepath=str(output/'authored-candidates.blend'), compress=True)
    if not args.diagnostic_only:
        bpy.ops.export_scene.gltf(filepath=str(output/'authored-candidates.glb'), export_format='GLB',
            use_selection=True, export_animations=True, export_def_bones=False,
            export_animation_mode='ACTIONS', export_frame_range=False, export_force_sampling=True,
            export_frame_step=1, export_extras=True, export_yup=True)
        for clip in clips:
            action = bpy.data.actions[clip['action']]
            arm.animation_data.action = action; arm.animation_data.action_slot = action.slots[0]
            scene.frame_end = len(clip['frames'])-1; scene.frame_set(0)
            bpy.ops.export_scene.fbx(filepath=str(output/(clip['action']+'.fbx')), use_selection=True,
                object_types={'ARMATURE','MESH'}, add_leaf_bones=False, use_armature_deform_only=False,
                bake_anim=True, bake_anim_use_all_actions=False, bake_anim_use_nla_strips=False,
                bake_anim_step=1, bake_anim_simplify_factor=0, axis_forward='-Z', axis_up='Y')
    report = {'schema':'autorig-quadruped-export-candidate.v2', 'source_sha256':source_hash,
        'blender_version':list(bpy.app.version), 'source_coordinates':blueprint['coordinates'],
        'export_settings':{'glb_export_yup':True,'fbx_axis_forward':'-Z','fbx_axis_up':'Y',
                           'reference_actor_translation_coordinates':'source_authoring_basis',
                           'reference_actor_result_space':'reference_world',
                           'reference_actor_application':'actor_transform_once'},
        'rig_blueprint_pin':{'path':str(rig_path), 'sha256':rig_hash}, 'clips':clips,
        'evaluated_surface_qa':validation, 'reference_actor_baked':False,
        'diagnostic_only':args.diagnostic_only, 'surface_qa_passed':passed,
        'weight_scope':'authoring input; native restoration and fresh game reduction still required',
        'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha(p)} for p in output.iterdir() if p.is_file()},
        'quality_approved':False}
    (output/'export-report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    if sha(source) != source_hash or sha(rig_path) != rig_hash:
        raise ValueError('Input changed during authoring')
    print('V2_DIAGNOSTIC_COMPLETE' if args.diagnostic_only else 'V2_EXPORT_COMPLETE', flush=True)


if __name__ == '__main__': main()
