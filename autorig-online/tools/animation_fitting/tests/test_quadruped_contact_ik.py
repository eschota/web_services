import copy
import numpy as np
import pytest
import animation_fitting.quadruped_contact_ik as contact_ik

from animation_fitting.author_quadruped_motion import ReachError
from animation_fitting.author_quadruped_motion import rx
from animation_fitting.quadruped_contact_ik import correct_contact_pose, sample_local_pose
from animation_fitting.tests.test_quadruped_jump_authoring import authored


def p8_sample():
    rig,clips=authored();clip=clips["jump_full"]
    pose=sample_local_pose(clip,40.5)
    targets={name:(np.asarray(clip["hoof_targets"][name][40])+np.asarray(clip["hoof_targets"][name][41]))/2 for name in rig.limbs}
    active={name:name.startswith("fore") for name in rig.limbs}
    return rig,clip,pose,targets,active


def sole_errors(rig,world,targets,names):
    skin=rig.skin(world);return {name:np.linalg.norm(skin[rig.limbs[name]["sole_indices"]].mean(axis=0)-targets[name]) for name in names}


def test_nlerp_is_shortest_hemisphere_and_unit():
    _,clip,_,_,_=p8_sample();clip=copy.deepcopy(clip)
    bone=next(iter(clip["frames"][0]["bones"]));q=np.asarray(clip["frames"][0]["bones"][bone]["rotation"])
    clip["frames"][1]["bones"][bone]["rotation"]=(-q).tolist()
    clip["frames"][1]["bones"][bone]["translation"]=[2,0,0]
    pose=sample_local_pose(clip,.5);rotation=pose[bone][:3,:3]
    assert np.linalg.det(rotation)==pytest.approx(1,abs=1e-10)
    np.testing.assert_allclose(pose[bone][:3,3],[1,0,0])


def test_contact_correction_reduces_interpolated_stance_error_and_preserves_nonactive_pose():
    rig,_,pose,targets,active=p8_sample();before=copy.deepcopy(pose)
    baseline_world={}
    for name in rig.order:
        parent=rig.rows[name]["parent"];baseline_world[name]=(baseline_world[parent] if parent else np.eye(4))@pose[name]
    baseline=sole_errors(rig,baseline_world,targets,[n for n in active if active[n]])
    world,local,metrics=correct_contact_pose(rig,pose,[0,0,0],targets,active)
    corrected=sole_errors(rig,world,targets,[n for n in active if active[n]])
    assert max(baseline.values())>1e-5 and max(corrected.values())<1e-5
    for name in rig.order:
        if not any(name in rig.limbs[leg]["bones"] or name in [x["bone"] for x in rig.limbs[leg]["extra"]] for leg in active if active[leg]):
            np.testing.assert_array_equal(local[name],before[name])
    assert set(metrics["legs"])=={"fore_left","fore_right"}
    assert pose.keys()==before.keys() and all(np.array_equal(pose[k],before[k]) for k in pose)


def test_actor_translation_is_subtracted_once():
    rig,_,pose,targets,active=p8_sample();actor=np.array([.2,-.1,.3]);world_targets={k:v+actor for k,v in targets.items()}
    world,_,_=correct_contact_pose(rig,pose,actor,world_targets,active)
    corrected=sole_errors(rig,world,targets,[n for n in active if active[n]])
    assert max(corrected.values())<1e-5


def test_no_contacts_returns_equal_pose_and_world():
    rig,_,pose,targets,_=p8_sample();active={name:False for name in rig.limbs}
    world,local,metrics=correct_contact_pose(rig,pose,[0,0,0],targets,active)
    assert metrics=={"legs":{}}
    for name in rig.order:np.testing.assert_array_equal(local[name],pose[name])


@pytest.mark.parametrize("kind",["missing","nan","unreachable","cap"])
def test_invalid_or_unreachable_input_fails_without_mutation(kind):
    rig,_,pose,targets,active=p8_sample();before={k:v.copy() for k,v in pose.items()}
    if kind=="missing":targets.pop("hind_left")
    elif kind=="nan":targets["fore_left"][0]=float("nan")
    elif kind=="unreachable":targets["fore_left"]+=100
    else:pass
    with pytest.raises((ValueError,ReachError)):
        correct_contact_pose(rig,pose,[0,0,0],targets,active,max_pose_correction_degrees=.0001 if kind=="cap" else 8)
    for name in pose:np.testing.assert_array_equal(pose[name],before[name])


def test_all_four_track_ids_are_required():
    rig,_,pose,targets,active=p8_sample();active["extra"]=False
    with pytest.raises(ValueError):correct_contact_pose(rig,pose,[0,0,0],targets,active)


@pytest.mark.parametrize("kind",["foot","extra"])
def test_cap_covers_every_changed_local_bone(kind,monkeypatch):
    rig,_,pose,targets,active=p8_sample();bone=("foot_dupli_001.l" if kind=="foot" else "thigh_stretch_dupli_001.l")
    pose[bone][:3,:3]=pose[bone][:3,:3]@rx(np.radians(20))
    if kind=="extra":
        def unchanged_q(limb,hip,base,target,pitch,height):
            q=limb["posture_prior"].copy();neutral=limb["neutral"]
            angles=np.array([q[0],q[0]+q[1]-neutral[1],q[0]+q[1]-neutral[1]+q[2]-neutral[2]])
            return angles,q,0.
        monkeypatch.setattr(contact_ik,"solve_leg",unchanged_q)
    before={name:value.copy() for name,value in pose.items()}
    with pytest.raises(ReachError,match="local bone correction"):
        correct_contact_pose(rig,pose,[0,0,0],targets,active)
    for name in pose:np.testing.assert_array_equal(pose[name],before[name])


def test_rigid_affine_bool_scale_and_hard_cap_validation():
    rig,clip,pose,targets,active=p8_sample()
    bad=copy.deepcopy(pose);bad[rig.root][0,0]=2
    with pytest.raises(ValueError,match="rigid affine"):correct_contact_pose(rig,bad,[0,0,0],targets,active)
    bad=copy.deepcopy(pose);bad[rig.root]=bad[rig.root].tolist();bad[rig.root][0][0]=True
    with pytest.raises(ValueError,match="booleans"):correct_contact_pose(rig,bad,[0,0,0],targets,active)
    malformed=copy.deepcopy(clip);malformed["frames"][40]["bones"][rig.root]["scale"]=[1,2,1]
    with pytest.raises(ValueError,match="scale"):sample_local_pose(malformed,40.5)
    with pytest.raises(ValueError,match="cap"):correct_contact_pose(rig,pose,[0,0,0],targets,active,max_pose_correction_degrees=9)


def test_out_of_sagittal_target_fails_tight_endpoint_cap():
    rig,_,pose,targets,active=p8_sample();targets["fore_left"]=targets["fore_left"].copy()
    targets["fore_left"][0]+=.001*rig.height
    with pytest.raises(ReachError,match="endpoint error"):
        correct_contact_pose(rig,pose,[0,0,0],targets,active)


def test_active_output_rotations_are_projected_to_so3_with_rest_rounding():
    rig,_,pose,targets,active=p8_sample()
    # A float-export-sized residual remains inside input tolerance.
    bone="thigh_twist_dupli_001.l";pose[bone][:3,:3]*=1+2e-6
    _,local,_=correct_contact_pose(rig,pose,[0,0,0],targets,active)
    changed=set()
    for leg,is_active in active.items():
        if is_active:
            changed.update(rig.limbs[leg]["bones"])
            changed.update(row["bone"] for row in rig.limbs[leg]["extra"])
    for name in changed:
        rotation=local[name][:3,:3]
        np.testing.assert_allclose(rotation.T@rotation,np.eye(3),rtol=0,atol=1e-12)
        assert np.linalg.det(rotation)==pytest.approx(1,abs=1e-12)
