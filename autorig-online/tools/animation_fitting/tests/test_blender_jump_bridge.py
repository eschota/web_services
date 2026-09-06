import json
import os
from pathlib import Path
import subprocess
import pytest

HERE=Path(__file__).resolve().parent;TOOLS=HERE.parent
BLENDER=Path(os.environ.get('AUTORIG_BLENDER_52',r'C:\Program Files\Blender Foundation\Blender 5.2\blender.exe'))


def run(script,args,tmp_path):
    return subprocess.run([str(BLENDER),'--background','--factory-startup','--python-exit-code','1',
        '--python',str(script),'--',*map(str,args)],capture_output=True,text=True,timeout=90,
        env=dict(os.environ,TEMP=str(tmp_path),TMP=str(tmp_path)))


@pytest.mark.skipif(not BLENDER.is_file(),reason='Local Blender 5.2 not installed')
@pytest.mark.parametrize('mesh_z,diagnostic,target_offset',[(0.,False,0.),(-.01,False,0.),(-.01,True,0.),(0.,False,.002)])
def test_v2_bridge_keeps_actor_external_and_failed_diagnostics_unapproved(tmp_path,mesh_z,diagnostic,target_offset):
    helper=HERE/'blender_jump_bridge_canary.py'
    created=run(helper,['--root',tmp_path,'--mesh-z',mesh_z,'--target-offset-x',target_offset],tmp_path)
    assert created.returncode==0,created.stdout+created.stderr
    out=tmp_path/'result'
    args=['--source',tmp_path/'source.blend','--rig',tmp_path/'rig.json','--clips',tmp_path/'clips','--output',out]
    if diagnostic:args.append('--diagnostic-only')
    result=run(TOOLS/'blender_quadruped_jump_bridge.py',args,tmp_path)
    if mesh_z<0 and not diagnostic:
        assert result.returncode==1,result.stdout+result.stderr
        assert not (out/'authored-candidates.blend').exists()
        assert not (out/'authored-candidates.glb').exists()
        assert not json.loads((out/'bridge-qa.json').read_text())['jump_full']['passed']
        return
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads((out/'export-report.json').read_text())
    assert report['source_coordinates']['up']=='+Z'
    assert report['export_settings']['glb_export_yup'] is True
    assert report['export_settings']['reference_actor_translation_coordinates']=='source_authoring_basis'
    assert report['export_settings']['reference_actor_result_space']=='reference_world'
    assert report['export_settings']['reference_actor_application']=='actor_transform_once'
    assert report['reference_actor_baked'] is False and report['quality_approved'] is False
    assert report['diagnostic_only'] is diagnostic
    assert report['surface_qa_passed'] is (mesh_z==0)
    assert (out/'authored-candidates.glb').exists() is (not diagnostic)
    assert (out/'jump_full.fbx').exists() is (not diagnostic)
    qa=report['evaluated_surface_qa']['jump_full']
    assert qa['realized_surface_within_target_band'] is (mesh_z==0 and target_offset==0)
    assert qa['max_stance_height_m'] is None and all(v==0 for v in qa['contact_sample_counts'].values())
    checked=run(helper,['--root',tmp_path,'--verify',out/'authored-candidates.blend'],tmp_path)
    assert checked.returncode==0,checked.stdout+checked.stderr
    for row in json.loads((tmp_path/'pose-check.json').read_text()):
        assert row['actor_location']==[0,0,0] and row['root_translation']==[0,0,0]
        assert row['diagnostic_only'] is diagnostic
        assert row['surface_qa_passed'] is (mesh_z==0)
    assert report['clips'][0]['reference_actor_motion']['translations'][1][2]==.2
