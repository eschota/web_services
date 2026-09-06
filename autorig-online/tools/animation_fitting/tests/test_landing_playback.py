import pytest
from animation_fitting.landing_playback import LandingPlayback


def attempt():
    return LandingPlayback(sample_count=25, precontact_frame=7, touchdown_frame=8,
                           recovery_blend_seconds=.2)


def test_absent_collision_never_starts_touchdown_or_recovery():
    player = attempt()
    step = player.advance(10, grounded=False, ground_near=True)
    assert step.state == 'precontact_hold' and step.pose_frame == 7
    for _ in range(20):
        assert player.advance(1, grounded=False, ground_near=True) == step
    assert step.recovery_blend_weight is None and not step.contact_solver_required


def test_early_collision_blends_from_current_pose_without_a_hard_cut():
    player = attempt()
    player.advance(1/30, grounded=False, ground_near=True)
    first = player.advance(1/30, grounded=True, ground_near=True)
    assert first.state == 'recovery' and first.capture_current_pose
    assert first.recovery_blend_weight == 0 and first.contact_solver_required
    following = player.advance(.05, grounded=True, ground_near=True)
    assert not following.capture_current_pose
    assert 0 < following.recovery_blend_weight < 1
    assert following.pose_frame > first.pose_frame


def test_late_collision_releases_hold_and_finishes_at_final_pose():
    player = attempt()
    player.advance(3, grounded=False, ground_near=True)
    contact = player.advance(0, grounded=True, ground_near=True)
    assert contact.capture_current_pose and contact.recovery_blend_weight == 0
    end = player.advance(2, grounded=True, ground_near=True)
    assert end.state == 'complete' and end.pose_frame == 24
    assert end.recovery_blend_weight == 1
    assert player.advance(2, grounded=True, ground_near=True) == end


def test_proximity_loss_returns_to_air_without_timer_grounding():
    player = attempt()
    player.advance(.1, grounded=False, ground_near=True)
    step = player.advance(.1, grounded=False, ground_near=False)
    assert step.state == 'air_blend' and step.capture_current_pose
    assert step.air_blend_weight == 0
    middle = player.advance(.06, grounded=False, ground_near=True)
    assert middle.state == 'air_blend' and middle.air_blend_weight == pytest.approx(.5)
    assert not middle.capture_current_pose
    assert player.advance(20, grounded=False, ground_near=True).state == 'air'


def test_lost_ground_during_recovery_interrupts_to_current_air_pose():
    player = attempt()
    player.advance(0, grounded=True, ground_near=True)
    recovering = player.advance(.1, grounded=True, ground_near=True)
    step = player.advance(.1, grounded=False, ground_near=False)
    assert step.state == 'air_blend' and step.capture_current_pose
    assert step.air_blend_weight == 0
    assert step.pose_frame == recovering.pose_frame
    assert player.advance(.12, grounded=False, ground_near=False).state == 'air'


@pytest.mark.parametrize('seconds', [-1, float('nan'), float('inf'), True])
def test_invalid_controller_time_cannot_advance_state(seconds):
    player = attempt()
    with pytest.raises(ValueError): player.advance(seconds, grounded=False, ground_near=True)
    assert player.state == 'approach' and player.frame == 0


def test_invalid_clip_boundary_or_collision_input_is_rejected():
    with pytest.raises(ValueError): LandingPlayback(25, 8, 8)
    with pytest.raises(ValueError): LandingPlayback(25, 7, 25)
    with pytest.raises(ValueError): LandingPlayback(25, 7, 8, 0)
    with pytest.raises(ValueError): attempt().advance(0, grounded=1, ground_near=True)
