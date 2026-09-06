import json
from pathlib import Path

import numpy as np
import pytest

from animation_fitting.author_quadruped_jump import _foot_pitch, _root_offset, author_jump_clips
from animation_fitting.author_quadruped_motion import AuthoringRig, author_clip
from animation_fitting.tests.test_quadruped_authoring import synthetic_rig


PROFILE_PATH = Path(__file__).parents[1] / "profiles" / "horse_jump.experimental.v1.json"


def authored():
    payload = synthetic_rig()
    gameplay = {
        "schema": "autorig-quadruped-gameplay-profile.v1", "profile_id": "synthetic-jump", "root": "__animal_export_root",
        "rest_pose_policy": "require_within_limits", "max_body_drop_adjustment_height_fraction": .03,
        "body": {"neck": "neck.x", "head": "head.x", "tail": [f"c_tail_{i:02}.x" for i in range(7)], "ears": ["c_ear_01.l", "c_ear_01.r"]},
        "gaits": {}, "limbs": {},
    }
    for side in ("left", "right"):
        suffix = ".l" if side == "left" else ".r"
        gameplay["limbs"][f"hind_{side}"] = {"chain": [f"c_thigh_b{suffix}", f"thigh_twist{suffix}", f"leg_stretch{suffix}", f"foot{suffix}", f"toes_01{suffix}"],
            "intermediate": [{"bone": f"thigh_stretch{suffix}", "segment": 1}, {"bone": f"leg_twist{suffix}", "segment": 2}],
            "joint_lower_degrees": [-55, 3, -145], "joint_upper_degrees": [55, 145, -3], "stance_center_joint": 0}
        gameplay["limbs"][f"fore_{side}"] = {"chain": [f"c_thigh_b_dupli_001{suffix}", f"thigh_twist_dupli_001{suffix}", f"leg_stretch_dupli_001{suffix}", f"foot_dupli_001{suffix}", f"toes_01_dupli_001{suffix}"],
            "intermediate": [{"bone": f"thigh_stretch_dupli_001{suffix}", "segment": 1}, {"bone": f"leg_twist_dupli_001{suffix}", "segment": 2}],
            "joint_lower_degrees": [-55, -145, 3], "joint_upper_degrees": [55, -3, 145], "stance_center_joint": 0}
    rig = AuthoringRig(payload, gameplay)
    profile = json.loads(PROFILE_PATH.read_text())
    return rig, author_jump_clips(rig, profile, source_sha256="a"*64, rig_blueprint_sha256="b"*64,
        gameplay_profile_sha256="c"*64, jump_profile_sha256="d"*64)


def test_four_clips_have_exact_ranges_contacts_and_static_air_pose():
    rig, clips = authored()
    assert {name: len(clip["frames"]) for name, clip in clips.items()} == {"jump_start": 25, "jump_air": 9, "jump_land": 33, "jump_full": 65}
    full = clips["jump_full"]
    for foot in ("fore_left", "fore_right"):
        assert full["contacts"][foot][11] and not full["contacts"][foot][12] and full["contacts"][foot][40]
    for foot in ("hind_left", "hind_right"):
        assert full["contacts"][foot][15] and not full["contacts"][foot][16] and full["contacts"][foot][42]
    air = clips["jump_air"]
    assert all(not any(air["contacts"][foot]) for foot in air["contacts"])
    for bone in rig.order:
        first = air["frames"][0]["bones"][bone]
        for frame in air["frames"][1:]:
            np.testing.assert_allclose(frame["bones"][bone]["translation"], first["translation"], atol=1e-10)
            assert abs(np.dot(frame["bones"][bone]["rotation"], first["rotation"])) > 1-1e-10


def test_ballistic_reference_and_local_root_are_separate_with_c1_joins():
    rig, clips = authored();full=clips["jump_full"]
    actor=np.asarray(full["reference_actor_motion"]["translations"])[:,2]
    offsets=np.asarray(full["motion"]["pose_root_offsets"])[:,2]
    assert np.all(actor[:16] == 0) and np.all(actor[41:] == 0) and actor[28] == actor.max()
    assert actor.max() > 0 and np.max(np.abs(offsets)) > 0
    assert not np.array_equal(actor, offsets)
    world = actor + offsets
    dt=1/30
    velocity=np.diff(world)/dt
    # Sampled finite differences straddle an accelerated analytic C1 join.
    assert abs(velocity[15]-velocity[16]) < .6*rig.height
    assert abs(velocity[39]-velocity[40]) < .6*rig.height
    physics=full["qa"]["physics"]
    assert physics["apex_equation_error_m"] < 1e-10
    assert physics["takeoff_velocity_m_per_second"] == physics["gravity_m_per_second_squared"]*.8/2
    epsilon=1e-5;crouch=.1*rig.height;v0=physics["takeoff_velocity_m_per_second"]
    local_launch=(_root_offset(16,crouch,v0)-_root_offset(16-epsilon,crouch,v0))/(epsilon/30)
    local_land=(_root_offset(40+epsilon,crouch,v0)-_root_offset(40,crouch,v0))/(epsilon/30)
    g=physics["gravity_m_per_second_squared"]
    actor_launch=lambda t:.5*g*t*(.8-t)
    actor_land=lambda t:.5*g*t*(.8-t)
    assert local_launch==pytest.approx(v0,rel=2e-5)
    assert local_land==pytest.approx(-v0,rel=2e-5)
    assert (actor_launch(epsilon/30)-actor_launch(0))/(epsilon/30)==pytest.approx(v0,rel=2e-5)
    assert (actor_land(.8)-actor_land(.8-epsilon/30))/(epsilon/30)==pytest.approx(-v0,rel=2e-5)


def test_shared_cuts_and_final_pose_match_idle_endpoint():
    rig, clips = authored();full=clips["jump_full"]
    for action,index in (("jump_start",24),("jump_air",0),("jump_land",0)):
        source = 24 if action in ("jump_start","jump_air") else 32
        for bone in rig.order:
            candidate=clips[action]["frames"][index if action=="jump_start" else 0]["bones"][bone]
            reference=full["frames"][source]["bones"][bone]
            np.testing.assert_allclose(candidate["translation"],reference["translation"])
            assert abs(np.dot(candidate["rotation"],reference["rotation"]))>1-1e-10
    idle=author_clip(rig,"idle_neutral")["frames"][0]
    for bone in rig.order:
        for endpoint in (full["frames"][0],full["frames"][-1]):
            np.testing.assert_allclose(endpoint["bones"][bone]["translation"],idle["bones"][bone]["translation"],atol=1e-10)
            assert abs(np.dot(endpoint["bones"][bone]["rotation"],idle["bones"][bone]["rotation"]))>1-1e-10


def test_joint_samples_remain_inside_declared_bounds():
    _, clips = authored()
    for row in clips["jump_full"]["qa"]["joint_bounds"].values():
        assert np.all(np.asarray(row["min"]) >= np.asarray(row["bounds"])[0]-1e-8)
        assert np.all(np.asarray(row["max"]) <= np.asarray(row["bounds"])[1]+1e-8)


def test_each_foot_pitch_is_zero_on_contacts_and_first_lift_key():
    profile=json.loads(PROFILE_PATH.read_text())
    boundaries={"fore_left":(12,40),"fore_right":(12,40),"hind_left":(16,42),"hind_right":(16,42)}
    for foot,(lift,touch) in boundaries.items():
        assert _foot_pitch(foot,lift,profile)==0
        assert _foot_pitch(foot,touch,profile)==0
        for sample in list(range(lift))+list(range(touch,65)):
            assert _foot_pitch(foot,sample,profile)==0
        assert _foot_pitch(foot,24,profile)==profile["tuck_pitch_radians"]


def test_air_slice_qa_is_local_to_its_nine_static_samples():
    _,clips=authored();qa=clips["jump_air"]["qa"]
    assert qa["canonical_sample_range"]==[24,33]
    assert "physics" not in qa and qa["canonical_jump_physics"]["scope"].startswith("canonical jump_full")
    for foot in qa["feet"].values():
        assert foot["air_samples_evaluated"]==9
        assert foot["stance_samples_evaluated"]==0
        assert foot["max_planted_height_m"] is None
        assert foot["max_planted_slide_per_frame_m"] is None
    assert list(qa["spinal_articulation"]["local_rotation_ranges_degrees"])==["root.x","spine_01.x","spine_02.x","spine_03.x"]
    assert max(qa["spinal_articulation"]["local_rotation_ranges_degrees"].values())<1e-8


@pytest.mark.parametrize("mutation",[
    lambda p:p["spine_amplitudes_degrees"].__setitem__(0,float("nan")),
    lambda p:p["spine_amplitudes_degrees"].__setitem__(0,True),
    lambda p:p.__setitem__("fore_lift_height_fraction",-.1),
    lambda p:p.__setitem__("hind_lift_height_fraction",.9),
    lambda p:p.__setitem__("tuck_pitch_radians",-2),
    lambda p:p.__setitem__("sample_count",65.0),
])
def test_malformed_profile_is_rejected(mutation):
    rig,_=authored();profile=json.loads(PROFILE_PATH.read_text());mutation(profile)
    with pytest.raises(ValueError):author_jump_clips(rig,profile,source_sha256="a"*64,rig_blueprint_sha256="b"*64,
        gameplay_profile_sha256="c"*64,jump_profile_sha256="d"*64)


def test_wrong_spine_hierarchy_and_source_provenance_are_rejected():
    rig,_=authored();profile=json.loads(PROFILE_PATH.read_text())
    rig.rows["spine_02.x"]["parent"]=rig.root
    with pytest.raises(ValueError,match="hierarchy"):author_jump_clips(rig,profile,source_sha256="a"*64,rig_blueprint_sha256="b"*64,
        gameplay_profile_sha256="c"*64,jump_profile_sha256="d"*64)
    rig,_=authored()
    with pytest.raises(ValueError,match="source_sha256"):author_jump_clips(rig,profile,source_sha256="f"*64,rig_blueprint_sha256="b"*64,
        gameplay_profile_sha256="c"*64,jump_profile_sha256="d"*64)
