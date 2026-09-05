import copy
import pytest
from animation_fitting.audit_horse_rest_rig import CHAINS, audit_skeleton


def armature():
    bones=[]
    for region,templates in CHAINS.items():
        for side in ('l','r'):
            for i,template in enumerate(templates):
                bones.append({'name':template.format(side=side),
                    'head_local':[1 if side=='l' else -1, 0 if region=='fore' else 4, 5-i],
                    'tail_local':[1 if side=='l' else -1, 0 if region=='fore' else 4, 4-i]})
    return {'bones':bones}


def test_symmetric_rig_passes_but_not_production_approval():
    result=audit_skeleton(armature())
    assert result['passed']
    assert not result['production_quality_approved']


def test_half_length_hind_leg_fails_before_tracking():
    source=armature()
    hind_left={x.format(side='l') for x in CHAINS['hind']}
    for b in source['bones']:
        if b['name'] in hind_left:
            b['head_local'][2]*=.5
            b['tail_local'][2]*=.5
    report=audit_skeleton(source)
    assert report['blocking_reasons']==['hind_paired_chain_asymmetry']
    assert report['pairs']['hind']['relative_length_difference']==.5


def test_missing_joint_and_nonfinite_geometry_fail():
    source=armature();source['bones'].pop()
    with pytest.raises(ValueError, match='missing'):audit_skeleton(source)
    source=armature();source['bones'][0]['head_local'][0]=float('nan')
    with pytest.raises(ValueError, match='finite'):audit_skeleton(source)
