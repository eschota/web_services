import copy
import json
import math
import numpy as np
import pytest
from animation_fitting.author_quadruped_motion import AuthoringRig, author_clip, hoof_trajectory, spinal_angles, DEFAULT_PROFILE


def synthetic_rig(scale=1.0,near_straight_fore=False):
    """Original test geometry; contains no ARP mesh or extracted source data."""
    rows={}
    def add(name,parent,head,tail):
        head=np.array(head,float)*scale;tail=np.array(tail,float)*scale
        y=tail-head;y/=np.linalg.norm(y);x=np.array([1.,0,0]);x-=y*(x@y);x/=np.linalg.norm(x);z=np.cross(x,y)
        m=np.eye(4);m[:3,:3]=np.column_stack([x,y,z]);m[:3,3]=head
        rows[name]={'name':name,'parent':parent,'deform':parent is not None,
                    'rest_world':m.ravel().tolist(),'head':head.tolist(),'tail':tail.tolist()}
    root='__animal_export_root'
    add(root,None,(0,0,0),(0,0,.3))
    add('root.x',root,(0,0,1.5),(0,.5,1.5))
    add('spine_01.x',root,(0,0,1.5),(0,-.4,1.5))
    add('spine_02.x','spine_01.x',(0,-.4,1.5),(0,-.8,1.65))
    add('spine_03.x','spine_02.x',(0,-.8,1.65),(0,-1.1,1.8))
    add('neck.x','spine_03.x',(0,-1.1,1.8),(0,-1.4,2.1))
    add('head.x','neck.x',(0,-1.4,2.1),(0,-1.8,1.9))
    for i in range(7):
        add(f'c_tail_{i:02}.x','root.x' if i==0 else f'c_tail_{i-1:02}.x',
            (0,.5+i*.15,1.5-i*.05),(0,.65+i*.15,1.45-i*.05))
    for side,x in [('l',.2),('r',-.2)]:
        add(f'c_ear_01.{side}','head.x',(x*.3,-1.4,2.1),(x*.3,-1.4,2.22))
        for fore in (True,False):
            suffix='_dupli_001' if fore else ''
            names=[f'{base}{suffix}.{side}' for base in ('c_thigh_b','thigh_twist','thigh_stretch','leg_stretch','leg_twist','foot','toes_01')]
            primary=[(x,-.8,1.5),(x,-.6,1.1),(x,-.58,.65),(x,-.51,.19),(x,-.56,.09),(x,-.65,0)] if fore else [
                (x,.6,1.55),(x,.45,.95),(x,.74,.63),(x,.72,.19),(x,.68,.09),(x,.59,0)]
            if fore and near_straight_fore:primary[1]=(x,-.7,1.1)
            a,b,c,d,e,f=map(np.array,primary)
            if fore:
                add(f'clavicle.{side}','spine_03.x',(x*.4,-.65,1.82),a)
            nodes=[a,b,(b+c)/2,c,(c+d)/2,d,e,f]
            for i,n in enumerate(names):
                add(n,(f'clavicle.{side}' if fore else 'root.x') if i==0 else names[i-1],nodes[i],nodes[i+1])
    for row in rows.values():
        world=np.array(row['rest_world']).reshape(4,4)
        parent=np.array(rows[row['parent']]['rest_world']).reshape(4,4) if row['parent'] else np.eye(4)
        row['rest_local']=(np.linalg.inv(parent) @ world).ravel().tolist()
    vertices=[]
    for side,x in [('l',.2),('r',-.2)]:
        for fore in (True,False):
            bone=f"foot{'_dupli_001' if fore else ''}.{side}"
            y=-.6 if fore else .65
            for dx,dy in [(-.05,-.08),(.05,-.08),(0,.08)]:
                vertices.append({'point':((np.array([x+dx,y+dy,0]))*scale).tolist(),
                                 'weights':[{'bone':bone,'weight':1.0}]})
    vertices.append({'point':[0,0,1.5*scale],'weights':[{'bone':'root.x','weight':1.0}]})
    return {'schema':'autorig-quadruped-authoring-rig.v1','source_sha256':'a'*64,
             'bones':list(rows.values()),'meshes':[{'name':'Synthetic','vertices':vertices,'faces':[]}]}


def test_trajectory_has_continuous_stance_velocity_at_both_joins():
    duty=.625;stride=.8;center=np.zeros(3);eps=1e-6
    for direction in (-1,1):
        for p in (duty,1.0):
            a=hoof_trajectory(p-eps,duty,stride,.2,center,direction)[0]
            b=hoof_trajectory(p,duty,stride,.2,center,direction)[0]
            c=hoof_trajectory(p+eps,duty,stride,.2,center,direction)[0]
            np.testing.assert_allclose((b-a)/eps,(c-b)/eps,atol=2e-4)
            assert (c-b)[1]/eps==pytest.approx(direction*stride/duty,abs=2e-4)


@pytest.mark.parametrize('duty,ease',[(.25,.1),(.2,.075)])
def test_bounded_swing_preserves_contact_velocity_and_horizontal_c2_joins(duty,ease):
    eps=1e-5;stride=.7;center=np.zeros(3)
    for direction in (-1,1):
        def y(phase):return hoof_trajectory(phase,duty,stride,.2,center,direction,
            swing_profile='bounded_c2',swing_ease_fraction=ease)[0][1]
        limit=stride*(.5+(1-duty)/duty*ease/2)
        assert max(abs(y(p)) for p in np.linspace(0,1,2001))<=limit+1e-12
        for phase in (duty,duty+(1-duty)*ease,1-(1-duty)*ease,1.):
            left_velocity=(y(phase)-y(phase-eps))/eps
            right_velocity=(y(phase+eps)-y(phase))/eps
            assert left_velocity==pytest.approx(right_velocity,abs=2e-3)
            left_acc=(y(phase)-2*y(phase-eps)+y(phase-2*eps))/eps**2
            right_acc=(y(phase+2*eps)-2*y(phase+eps)+y(phase))/eps**2
            assert left_acc==pytest.approx(right_acc,abs=.15)
        for phase in (duty,1.):
            assert (y(phase+eps)-y(phase-eps))/(2*eps)==pytest.approx(direction*stride/duty,abs=2e-3)


def test_fast_action_requires_explicit_recipe_and_valid_swing_contract():
    with pytest.raises(ValueError,match='No gait recipe'):
        author_clip(AuthoringRig(synthetic_rig()),'run')
    for kwargs in ({'swing_profile':'unknown'},{'swing_ease_fraction':0},{'swing_ease_fraction':float('nan')}):
        with pytest.raises(ValueError):hoof_trajectory(.5,.25,.5,.1,np.zeros(3),**kwargs)


@pytest.mark.parametrize('root_motion',[False,True])
def test_contact_body_pitch_preserves_torso_pivot_and_planted_feet(root_motion):
    from scipy.spatial.transform import Rotation
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile['gaits']['run']={'phases':[.15,.5,0,.35],'duty':.25,'direction':1,
        'stride_height':.2,'lift_height':.1,'body_drop':.05,'bob':.004,
        'swing_profile':'bounded_c2','swing_ease_fraction':.075,
        'body_dynamics':{'model':'contact_impulses','gravity_height_per_second_squared':1.,
            'vertical_impulse_fractions':[.25]*4,'pitch_load_gain_radians':.02}}
    rig=AuthoringRig(synthetic_rig(),profile);clip=author_clip(rig,'run',root_motion)
    pivots=[]
    for frame in clip['frames']:
        root=frame['bones'][rig.root];matrix=np.eye(4)
        matrix[:3,:3]=Rotation.from_quat(root['rotation']).as_matrix()
        matrix[:3,3]=root['translation']
        pivots.append((matrix@rig.inverse[rig.root]@np.r_[rig.body_pivot,1])[:3])
    pivots=np.asarray(pivots)
    np.testing.assert_allclose(pivots[:,0],rig.body_pivot[0],atol=1e-12)
    expected_y=rig.body_pivot[1]-(clip['reference_speed']*np.arange(len(pivots))/30 if root_motion else 0)
    np.testing.assert_allclose(pivots[:,1],expected_y,atol=1e-12)
    assert np.ptp(pivots[:,2])>0.001
    assert clip['qa']['body_dynamics']['flight_max_load_body_weights']==0
    assert clip['qa']['mesh_pose_seam']<1e-6
    for foot in clip['qa']['feet'].values():
        assert foot['max_hoof_target_error']<1e-5
        assert foot['max_stance_slide_per_frame']<1e-6


@pytest.mark.parametrize('invalid_root',['parented','translated'])
def test_contact_body_motion_requires_supported_export_root(invalid_root):
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile['gaits']['walk_forward']['body_dynamics']={'model':'contact_impulses',
        'gravity_height_per_second_squared':1.,'vertical_impulse_fractions':[.25]*4}
    payload=synthetic_rig()
    if invalid_root=='parented':profile['root']='root.x'
    else:
        root=next(b for b in payload['bones'] if b['name']==profile['root'])
        root['rest_local'][3]=.1
    with pytest.raises(ValueError,match='zero-origin export root'):AuthoringRig(payload,profile)


def spine_profile():
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile['gaits']['run']={'phases':[.15,.5,0,.35],'duty':.25,'direction':1,
        'stride_height':.2,'lift_height':.1,'body_drop':.05,'bob':.004,
        'swing_profile':'bounded_c2','swing_ease_fraction':.075,
        'spine_motion':{'model':'hind_protraction_sagittal','pelvis_bone':'root.x',
            'pelvis_amplitude_degrees':-2.5,'pelvis_phase_delay':0.,
            'spine_bones':['spine_01.x','spine_02.x','spine_03.x'],
            'spine_amplitudes_degrees':[1.5,1.5,.5],'spine_phase_delays':[.015,.03,.045]}}
    return profile


def test_articulated_spine_moves_locally_while_feet_and_limits_are_preserved():
    payload=synthetic_rig();original=copy.deepcopy(payload);profile=spine_profile()
    rig=AuthoringRig(payload,profile);clip=author_clip(rig,'run')
    baseline=copy.deepcopy(profile);del baseline['gaits']['run']['spine_motion']
    old=author_clip(AuthoringRig(payload,baseline),'run')
    assert payload==original
    assert clip['hoof_targets']==old['hoof_targets'] and clip['contacts']==old['contacts']
    assert spinal_angles(rig,'run',0)==pytest.approx(spinal_angles(rig,'run',1),abs=1e-12)
    for bone in rig.spinal_configs['run']['bones']:
        rotations=np.array([f['bones'][bone]['rotation'] for f in clip['frames']])
        assert np.max(np.abs(rotations-rotations[0]))>.001
        assert clip['qa']['spinal_articulation']['local_rotation_ranges_degrees'][bone]>.1
    for leg,foot in clip['qa']['feet'].items():
        assert foot['joint_bounds']==old['qa']['feet'][leg]['joint_bounds']
        assert foot['max_hoof_target_error']<1e-5 and foot['max_stance_slide_per_frame']<1e-6
    assert clip['qa']['mesh_pose_seam']<1e-6


@pytest.mark.parametrize('bad',['duplicate','amplitude','phase','hierarchy'])
def test_invalid_spine_profile_rejected(bad):
    payload=synthetic_rig();profile=spine_profile();config=profile['gaits']['run']['spine_motion']
    if bad=='duplicate':config['spine_bones'][1]=config['spine_bones'][0]
    elif bad=='amplitude':config['spine_amplitudes_degrees'][1]=20
    elif bad=='phase':config['spine_phase_delays'][1]=float('nan')
    else:next(b for b in payload['bones'] if b['name']=='spine_02.x')['parent']='root.x'
    with pytest.raises(ValueError):AuthoringRig(payload,profile)


@pytest.mark.parametrize('action,count,duty,ease',[('run',21,.25,.1),('sprint',17,.2,.075)])
def test_four_beat_gait_has_all_limb_contacts_flight_and_closed_mesh_loop(action,count,duty,ease):
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile['gaits'][action]={'phases':[.15,.50,0.,.35],'duty':duty,'direction':1,
        'stride_height':.2,'lift_height':.1,'body_drop':.05,'bob':.004,
        'swing_profile':'bounded_c2','swing_ease_fraction':ease}
    result=author_clip(AuthoringRig(synthetic_rig(),profile),action)
    assert result['timing']['sample_count']==count
    assert result['qa']['mesh_pose_seam']<1e-6
    order=sorted(result['contacts'],key=lambda leg:result['contacts'][leg][:-1].index(True))
    assert order==['hind_right','hind_left','fore_right','fore_left']
    assert any(not any(result['contacts'][leg][i] for leg in result['contacts']) for i in range(count-1))
    for leg in result['qa']['feet'].values():
        assert leg['max_hoof_target_error']<1e-5
        assert leg['max_stance_slide_per_frame']<1e-6
        assert np.all(np.array(leg['joint_min'])>=np.array(leg['joint_bounds'][0])-1e-6)
        assert np.all(np.array(leg['joint_max'])<=np.array(leg['joint_bounds'][1])+1e-6)


@pytest.mark.parametrize('action',['idle_neutral','walk_forward','walk_backward','trot_jog'])
def test_real_skin_equations_plant_all_stance_feet_and_close_the_loop(action):
    result=author_clip(AuthoringRig(synthetic_rig()),action)
    assert result['qa']['mesh_pose_seam']<1e-6
    for leg in result['qa']['feet'].values():
        assert leg['max_stance_slide_per_frame']<1e-6
        assert leg['max_stance_height']<1e-6
        assert leg['min_foot_surface_height']>=-1e-6
        assert np.all(np.array(leg['joint_min'])>=np.array(leg['joint_bounds'][0])-1e-6)
        assert np.all(np.array(leg['joint_max'])<=np.array(leg['joint_bounds'][1])+1e-6)


def test_root_motion_uses_world_forward_even_when_root_rest_axes_are_rotated():
    result=author_clip(AuthoringRig(synthetic_rig()),'walk_forward',root_motion=True)
    root='__animal_export_root'
    delta=np.array(result['frames'][-1]['bones'][root]['translation'])-result['frames'][0]['bones'][root]['translation']
    np.testing.assert_allclose(delta,result['root_delta'],atol=1e-8)
    assert delta[1]<0 and abs(delta[2])<1e-8
    assert max(v['max_stance_slide_per_frame'] for v in result['qa']['feet'].values())<1e-6


def test_geometry_scale_changes_travel_speed_without_changing_joint_motion():
    a=author_clip(AuthoringRig(synthetic_rig()),'walk_forward')
    b=author_clip(AuthoringRig(synthetic_rig(1.7)),'walk_forward')
    assert b['reference_speed']==pytest.approx(a['reference_speed']*1.7)
    for f,g in zip(a['frames'],b['frames']):
        for name in f['bones']:
            qa=np.array(f['bones'][name]['rotation']);qb=np.array(g['bones'][name]['rotation'])
            assert abs(qa@qb)==pytest.approx(1,abs=1e-7)


def test_backward_uses_diagonal_pairs_without_an_aerial_phase():
    result=author_clip(AuthoringRig(synthetic_rig()),'walk_backward')
    c=result['contacts']
    assert c['hind_left']==c['fore_right']
    assert c['fore_left']==c['hind_right']
    assert min(sum(c[n][i] for n in c) for i in range(len(c['hind_left'])))>=2


def test_profile_must_name_existing_limbs_and_finite_bounds():
    p=json.loads(DEFAULT_PROFILE.read_text());p['limbs']['hind_left']['chain'][0]='missing'
    with pytest.raises(ValueError,match='Missing explicit'):AuthoringRig(synthetic_rig(),p)
    p=json.loads(DEFAULT_PROFILE.read_text());p['limbs']['hind_left']['joint_lower_degrees'][0]=float('-inf')
    with pytest.raises(ValueError,match='finite joint bounds'):AuthoringRig(synthetic_rig(),p)


def test_bounded_rest_projection_uses_declared_forelimb_role_and_keeps_pose_limits():
    source=synthetic_rig(near_straight_fore=True)
    with pytest.raises(ValueError,match='Rest chain outside'):AuthoringRig(source)
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile.update(rest_pose_policy='project_within_limits',max_rest_projection_degrees=10.)
    rig=AuthoringRig(source,profile)
    assert rig.limbs['fore_left']['neutral'][1]>0
    assert rig.limbs['fore_left']['bend_sign']==-1
    result=author_clip(rig,'walk_forward')
    for name in ('fore_left','fore_right'):
        qa=result['qa']['feet'][name]
        assert max(qa['rest_projection_degrees'])<=10
        assert np.all(np.array(qa['joint_min'])>=np.array(qa['joint_bounds'][0])-1e-8)
        assert np.all(np.array(qa['joint_max'])<=np.array(qa['joint_bounds'][1])+1e-8)
        assert qa['max_stance_slide_per_frame']<1e-6


def test_rest_projection_rejects_large_correction_and_nonfinite_cap():
    profile=json.loads(DEFAULT_PROFILE.read_text())
    profile.update(rest_pose_policy='project_within_limits',max_rest_projection_degrees=5.)
    with pytest.raises(ValueError,match='Rest chain outside'):
        AuthoringRig(synthetic_rig(near_straight_fore=True),profile)
    profile['max_rest_projection_degrees']=float('nan')
    with pytest.raises(ValueError,match='projection cap'):AuthoringRig(synthetic_rig(),profile)


@pytest.mark.parametrize('action',['idle_neutral','walk_forward'])
def test_explicit_anatomical_support_stance_keeps_contacts_and_limits(action):
    profile=json.loads(DEFAULT_PROFILE.read_text())
    for name,row in profile['limbs'].items():row['stance_center_joint']=1 if name.startswith('fore_') else 0
    rig=AuthoringRig(synthetic_rig(),profile)
    result=author_clip(rig,action)
    for name,leg in result['qa']['feet'].items():
        assert np.linalg.norm(leg['stance_center_offset'])>0
        assert leg['max_stance_slide_per_frame']<1e-6
        assert leg['max_stance_height']<1e-6
        assert np.all(np.array(leg['joint_min'])>=np.array(leg['joint_bounds'][0])-1e-8)
        assert np.all(np.array(leg['joint_max'])<=np.array(leg['joint_bounds'][1])+1e-8)


def test_stance_projection_requires_bounded_adjustment_and_valid_joint():
    profile=json.loads(DEFAULT_PROFILE.read_text());profile['limbs']['hind_left']['stance_center_joint']=0
    profile['max_stance_center_adjustment_height_fraction']=.001
    with pytest.raises(ValueError,match='Stance center exceeds'):AuthoringRig(synthetic_rig(),profile)
    profile['max_stance_center_adjustment_height_fraction']=.35
    profile['limbs']['hind_left']['stance_center_joint']=True
    with pytest.raises(ValueError,match='Stance center joint'):AuthoringRig(synthetic_rig(),profile)
