import copy
import numpy as np
import pytest
from animation_fitting.contact_body_motion import ContactBodyMotion


def recipe():
    return {'phases':[.15,.5,0.,.35],'duty':.25,
        'body_dynamics':{'model':'contact_impulses','gravity_height_per_second_squared':7.5,
            'vertical_impulse_fractions':[.25]*4,'pitch_load_gain_radians':.02}}


@pytest.mark.parametrize('duty,phases',[(.25,[.15,.5,0.,.35]),(.2,[.15,.5,0.,.35]),(.625,[0,.25,.5,.75])])
def test_impulses_only_load_contact_limbs_and_integrate_to_weight(duty,phases):
    gait=recipe();gait.update(duty=duty,phases=phases);motion=ContactBodyMotion(gait,2/3)
    p=np.linspace(0,1,20001);sample=motion.sample(p);forces=sample['vertical_load_body_weights']
    assert np.all(forces>=0)
    for leg,offset in enumerate(phases):
        stance=(p-offset)%1<duty
        assert np.max(np.abs(forces[~stance,leg]))<1e-12
        assert np.trapz(forces[:,leg],p)==pytest.approx(.25,abs=1e-9)
    assert np.trapz(sample['acceleration_height_per_second_squared'],p)==pytest.approx(0,abs=1e-8)


def test_body_is_periodic_c2_and_ballistic_in_flight():
    motion=ContactBodyMotion(recipe(),2/3);eps=1e-5
    a=motion.sample(-eps);b=motion.sample(0);c=motion.sample(eps)
    for key in ('height_fraction','velocity_height_per_second','acceleration_height_per_second_squared'):
        assert a[key]==pytest.approx(c[key],abs=2e-4)
        assert motion.sample(1)[key]==pytest.approx(b[key],abs=1e-12)
    p=np.linspace(.751,.999,201);flight=motion.sample(p)
    assert np.max(flight['vertical_load_body_weights'])==0
    assert np.max(np.abs(flight['acceleration_height_per_second_squared']+7.5))<1e-12
    # Finite differences of the position agree with independent analytic velocity/acceleration.
    for phase in (.1,.4,.8,.9):
        left=motion.sample(phase-eps)['height_fraction'];right=motion.sample(phase+eps)['height_fraction']
        center=motion.sample(phase)
        assert (right-left)/(2*eps*motion.duration)==pytest.approx(center['velocity_height_per_second'],abs=1e-7)
        # A larger second-difference step avoids subtractive cancellation.
        h=1e-4
        left=motion.sample(phase-h)['height_fraction'];right=motion.sample(phase+h)['height_fraction']
        assert (right-2*center['height_fraction']+left)/(h*motion.duration)**2==pytest.approx(center['acceleration_height_per_second_squared'],abs=2e-5)


def test_invalid_impulse_contract_rejected():
    for field,value in [('vertical_impulse_fractions',[.25,.25,.25,.3]),('vertical_impulse_fractions',[-.1,.3,.4,.4]),
                        ('gravity_height_per_second_squared',float('nan')),('pitch_load_gain_radians',float('inf'))]:
        gait=copy.deepcopy(recipe());gait['body_dynamics'][field]=value
        with pytest.raises(ValueError):ContactBodyMotion(gait,2/3)
