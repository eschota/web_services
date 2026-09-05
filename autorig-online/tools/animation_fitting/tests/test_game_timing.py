import math
import pytest
from animation_fitting.game_timing import FRAME_BUDGET, timing, retime_clip
from animation_fitting.gpu_lease import gpu_lease
from animation_fitting.workflows.run_ltx_clip import validate_generation, validate_submission, compose_prompt


def clip(action='run'):
    return {'name': action, 'duration': 2.0, 'tracks': [
        {'name':'root.position','type':'vector','times':[0,1,2],
         'values':[0,0,0, 1,0,0, 0,0,0]},
        {'name':'leg.quaternion','type':'quaternion','times':[0,1,2],
         'values':[0,0,0,1, 0,1,0,0, 0,0,0,-1]}]}


def test_skeletal_cadence_is_independent_of_ltx_latents():
    assert timing('run')['sample_count'] == 21
    assert timing('run')['duration_seconds'] == 20/30
    assert timing('walk_forward')['duration_seconds'] == 32/30
    assert timing('death')['loop'] is False
    assert timing('walk_forward', 34)['sample_count'] == 34
    for gen, _, _ in FRAME_BUDGET.values():
        assert (gen - 1) % 8 == 0


def test_retime_slerp_preserves_pose_and_endpoint_without_antipodal_spin():
    output, report = retime_clip(clip(), 'run')
    assert output['duration'] == 20/30
    assert len(output['tracks'][0]['times']) == 21
    assert output['tracks'][0]['values'][30:33] == [1,0,0]
    values=output['tracks'][1]['values']
    for i in range(0,len(values),4):
        assert sum(v*v for v in values[i:i+4]) == pytest.approx(1)
    assert max(row['endpoint_error'] for row in report['endpoint_errors']) == 0
    assert report['quality_approved'] is False


def test_one_shot_does_not_close_or_hide_motion_seams():
    source=clip('death');source['tracks'][0]['values'][-3:]=[0,-1,0]
    output, report=retime_clip(source,'death')
    assert output['tracks'][0]['values'][-3:]==[0,-1,0]
    assert report['endpoint_errors'][0]['endpoint_error']==1


def test_retiming_invalidates_source_approval_and_has_a_packagable_timeline(tmp_path):
    from animation_fitting.package_browser_animation_glb import ApprovedClipInput, _snapshot, _validate_clip
    import json
    source=clip(); source['name']='Browser_run'; source['userData']={'approved':True}
    source['uuid']='old-reviewed-clip'
    output,_=retime_clip(source,'run')
    assert 'uuid' not in output and 'userData' not in output
    path=tmp_path/'run.json';path.write_text(json.dumps(output))
    # Synthetic approval identifiers exercise the existing packager in this
    # unit test only. No such approval is emitted by retime_clip or its CLI.
    record=ApprovedClipInput('run',_snapshot(path,'test'),'test','a'*64,'b'*64)
    validated=_validate_clip(record,{'root':0,'leg':1})
    assert len(validated.tracks[0].times)==21
    assert validated.duration==pytest.approx(20/30)


@pytest.mark.parametrize('mutation', [
    lambda c:c.update(duration=float('nan')),
    lambda c:c['tracks'][0].update(times=[0,0,2]),
    lambda c:c['tracks'][0].update(interpolation=2300),
    lambda c:c['tracks'][1]['values'].__setitem__(3,0),
    lambda c:c['tracks'].append(c['tracks'][0]),
    lambda c:c.update(name='walk_forward'),
])
def test_malformed_source_is_rejected(mutation):
    source=clip();mutation(source)
    with pytest.raises(ValueError):retime_clip(source,'run')


def test_every_action_prompt_loads_in_a_relocated_checkout():
    for action in FRAME_BUDGET:
        positive,negative,_,mode=compose_prompt(action)
        assert '{{species}}' not in positive
        assert positive and negative and mode in ('loop','one_shot')


def test_generation_rejects_game_frame_counts_and_node_errors():
    with pytest.raises(ValueError):validate_generation(21,0,False,'gait')
    assert validate_generation(41,.85,False,'gait')==33
    with pytest.raises(ValueError):validate_generation(9,.85,False,'gait')
    with pytest.raises(ValueError):validate_generation(49,0,True,'one_shot')
    with pytest.raises(ValueError):validate_submission({'prompt_id':'a','node_errors':{'x':{}}})
    assert validate_submission({'prompt_id':'a','node_errors':{}})=='a'


def test_gpu_lease_excludes_other_stage_and_releases_on_error(tmp_path):
    path=tmp_path/'gpu.lock'
    with pytest.raises(RuntimeError):
        with gpu_lease(path,'render'):
            with pytest.raises(FileExistsError):
                with gpu_lease(path,'tracking'):pass
            raise RuntimeError('test')
    assert not path.exists()


def test_tracking_is_blocked_before_backend_loading_while_ltx_holds_lease(tmp_path, monkeypatch):
    from animation_fitting.tracking_runtime import cli
    path=tmp_path/'shared.lock'
    def should_not_run(_):
        pytest.fail('tracking backends must not be constructed during LTX')
    monkeypatch.setattr(cli, '_execute', should_not_run)
    with gpu_lease(path, 'ltx'):
        assert cli.main(['--gpu-lock',str(path),'observe','--video','unused',
                         '--bundle','unused','--output-dir','unused'])==2


def test_cuda_memory_guard_rejects_low_vram_before_model_loading(monkeypatch):
    import types
    import animation_fitting.gpu_lease as lease
    monkeypatch.setattr(lease.subprocess,'run',lambda *a,**k:types.SimpleNamespace(stdout='8000\n'))
    with pytest.raises(RuntimeError,match='14000'):lease.require_free_cuda_memory()
    monkeypatch.setattr(lease.subprocess,'run',lambda *a,**k:types.SimpleNamespace(stdout='20000\n'))
    assert lease.require_free_cuda_memory()==20000
