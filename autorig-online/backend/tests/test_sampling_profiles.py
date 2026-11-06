"""Catalogue Auto settings must describe the sampler that actually executes."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import ai_model_defaults
from renderfin.runtime_settings import apply_runtime_settings
from renderfin.templating import render_workflow_text

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / 'backend' / 'renderfin' / 'assets' / 'workflows'
CATALOGUE = json.loads((ROOT / 'deploy' / 'ai-models' / 'model_catalogue.json').read_text(encoding='utf-8'))
BASES = [entry for entry in CATALOGUE if entry['kind'] == 'checkpoint' and entry.get('usable')]


def rendered(name, settings, **extra):
    graph = render_workflow_text(
        (WORKFLOWS / name).read_text(encoding='utf-8'), width=960, height=540,
        prompt='A red bicycle beside a cafe', negative_prompt='',
        image_filename='reference.png', output_prefix='sampling-test', frames=97)
    apply_runtime_settings(graph, SimpleNamespace(**settings, **extra), 960, 540)
    return list(graph.values())


@pytest.mark.parametrize('base', BASES, ids=lambda entry: entry['file'])
def test_auto_profile_matches_actual_primary_sampler(base):
    settings = ai_model_defaults.resolve(base, None, {})
    nodes = rendered(settings['work_flow'], settings)
    schedulers = [node for node in nodes if node['class_type'] in
                  {'BasicScheduler', 'Flux2Scheduler', 'KSampler', 'ManualSigmas'}]
    assert schedulers
    for node in schedulers:
        inputs = node['inputs']
        actual_steps = (len(inputs['sigmas'].split(',')) - 1
                        if node['class_type'] == 'ManualSigmas' else inputs['steps'])
        assert actual_steps == settings['steps']
        if 'scheduler' in inputs:
            assert inputs['scheduler'] == settings['scheduler']
    for node in nodes:
        if node['class_type'] in {'KSamplerSelect', 'KSampler'}:
            assert node['inputs']['sampler_name'] == settings['sampler']
        if node['class_type'] in {'KSampler', 'CFGGuider'}:
            assert node['inputs']['cfg'] == settings['cfg']
        if node['class_type'] == 'BasicGuider':
            assert base['sampling_policy']['cfg_mode'] == 'fixed'
            assert settings['cfg'] == 1


@pytest.mark.parametrize('suffix', ['', '_edit', '_control_pose', '_control_depth', '_control_canny'])
def test_pony_auto_and_explicit_steps_reach_every_image_variant(suffix):
    base = next(entry for entry in BASES if entry['family'] == 'pony')
    for explicit, expected in [({}, 50), ({'steps': 37}, 37)]:
        settings = ai_model_defaults.resolve(base, None, explicit)
        nodes = rendered('gen_image_sdxl' + suffix + '.json', settings)
        sampler = next(node['inputs'] for node in nodes if node['class_type'] == 'KSampler')
        assert (sampler['steps'], sampler['cfg'], sampler['sampler_name'], sampler['scheduler']) == (
            expected, 5, 'dpmpp_2m_sde', 'karras')
        clip = next(node['inputs'] for node in nodes if node['class_type'] == 'CLIPSetLastLayer')
        assert clip['stop_at_clip_layer'] == -2
        deliveries = [node['inputs'] for node in nodes if node['class_type'] == 'ImageScale'
                      and node['inputs'].get('height') == 540]
        assert deliveries


def test_t_pose_refinement_keeps_its_independent_schedule():
    base = next(entry for entry in BASES if entry['file'] == 'flux1-schnell.safetensors')
    nodes = rendered('t_pose.json', ai_model_defaults.resolve(base, None, {}), type='t_pose')
    refinement = next(node['inputs'] for node in nodes if node['class_type'] == 'KSamplerAdvanced')
    assert (refinement['steps'], refinement['sampler_name'], refinement['scheduler']) == (
        6, 'dpmpp_2m', 'karras')
