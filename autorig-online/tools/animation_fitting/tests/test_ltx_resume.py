import io
import json
from animation_fitting.workflows import resume_ltx_clip as recovery


def test_recovery_collects_same_id_with_get_only_and_does_not_resubmit(tmp_path,monkeypatch):
    record={'state':'pending_after_timeout','prompt_id':'existing-job',
            'requested_generation_samples':65,'quality_approved':False}
    (tmp_path/'run.json').write_text(json.dumps(record))
    (tmp_path/'workflow.json').write_text(json.dumps({'save_video':{'inputs':{'filename_prefix':'horse_walk'}}}))
    history={'existing-job':{'status':{'completed':True},'outputs':{
        'save_video':{'videos':[{'filename':'horse_walk_00001.mp4','subfolder':'','type':'output'}]}}}}
    calls=[]
    def fetch(url,**kwargs):
        calls.append(url)
        return io.BytesIO(json.dumps(history).encode() if '/history/' in url else b'video-bytes')
    monkeypatch.setattr(recovery.urllib.request,'urlopen',fetch)
    monkeypatch.setattr(recovery,'validate_video',lambda *args:{'nb_read_frames':'65','avg_frame_rate':'30/1'})
    result=recovery.resume_run(tmp_path,lock_path=tmp_path/'gpu.lock')
    assert result['prompt_id']=='existing-job'
    assert result['state']=='rendered_pending_gait_qa'
    assert not result['quality_approved']
    assert (tmp_path/'horse_walk.mp4').read_bytes()==b'video-bytes'
    assert len(calls)==2 and all('/history/' in u or '/view?' in u for u in calls)
    assert not (tmp_path/'gpu.lock').exists()


def test_recorded_comfy_failure_is_not_retried_as_a_new_job(tmp_path,monkeypatch):
    record={'state':'submitted','prompt_id':'failed-job','requested_generation_samples':65}
    (tmp_path/'run.json').write_text(json.dumps(record))
    (tmp_path/'workflow.json').write_text(json.dumps({'save_video':{'inputs':{'filename_prefix':'horse_walk'}}}))
    history={'failed-job':{'status':{'status_str':'error','messages':['sampler failed']}}}
    monkeypatch.setattr(recovery.urllib.request,'urlopen',lambda *a,**k:io.BytesIO(json.dumps(history).encode()))
    result=recovery.resume_run(tmp_path,lock_path=tmp_path/'gpu.lock')
    assert result['state']=='failed'
    assert result['prompt_id']=='failed-job'
