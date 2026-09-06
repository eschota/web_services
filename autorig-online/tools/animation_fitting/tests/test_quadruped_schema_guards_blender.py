"""Unsupported semantics must fail before any asset open/export/render."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

TOOLS = Path(__file__).resolve().parents[1]
BLENDER = Path(os.environ.get('AUTORIG_BLENDER_52',
    r'C:\Program Files\Blender Foundation\Blender 5.2\blender.exe'))


def write_json(path, value):
    path.write_text(json.dumps(value), encoding='utf-8')
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.skipif(not BLENDER.is_file(), reason='Local Blender 5.2 not installed')
@pytest.mark.parametrize('consumer', ['bridge', 'skin', 'preview', 'reimport'])
def test_v2_rejected_before_legacy_asset_side_effects(tmp_path, consumer):
    # Invalid .blend/.npz bytes are intentional: a functioning early guard
    # must reject the schema before Blender/NumPy ever tries to open them.
    source = tmp_path/'source.blend'
    source.write_bytes(b'synthetic unavailable asset')
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    blueprint = tmp_path/'blueprint.json'
    blueprint_hash = write_json(blueprint, {'source_sha256': source_hash})
    clips = tmp_path/'clips'; clips.mkdir()
    clip = clips/'jump_air.json'
    clip_hash = write_json(clip, {'schema':'autorig-authored-quadruped-clip.v2'})
    report = tmp_path/'export-report.json'
    write_json(report, {'schema':'autorig-quadruped-export-candidate.v2'})
    output = tmp_path/'must-not-exist'
    if consumer == 'bridge':
        script = TOOLS/'blender_quadruped_bridge.py'
        args = ['apply','--source',source,'--blueprint',blueprint,'--clips-dir',clips,'--output',output]
    elif consumer == 'skin':
        weights = tmp_path/'weights.npz'; weights.write_bytes(b'not numpy')
        weight_hash = hashlib.sha256(weights.read_bytes()).hexdigest()
        reduction = tmp_path/'reduction.json'
        write_json(reduction, {
            'candidate':{'path':str(weights),'sha256':weight_hash},
            'inputs':{'weights':{'path':str(weights)},'rig':{'path':str(blueprint)},
                      'clips':[{'path':str(clip),'sha256':clip_hash}]},
            'input_hashes':{'weights':weight_hash,'rig':blueprint_hash},
        })
        script = TOOLS/'validate_quadruped_skin_blender.py'
        args = ['--source',source,'--reduction',reduction,'--output',output,'--preflight-only']
    elif consumer == 'preview':
        script = TOOLS/'render_quadruped_preview.py'
        args = ['--source',source,'--report',report,'--action','jump_air','--output',output]
    else:
        script = TOOLS/'verify_quadruped_exports.py'
        args = ['--directory',tmp_path,'--output',output]
    env = dict(os.environ, TEMP=str(tmp_path), TMP=str(tmp_path))
    result = subprocess.run([str(BLENDER),'--background','--factory-startup',
        '--python-exit-code','1','--python',str(script),'--',*map(str,args)],
        text=True, capture_output=True, timeout=60, env=env)
    message = result.stdout + result.stderr
    assert result.returncode == 1, message
    expected = 'clip must use the exact quadruped v1 schema' if consumer in ('bridge','skin') else 'report must use the exact quadruped export candidate v1 schema'
    assert expected in message, message
    assert not output.exists()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
