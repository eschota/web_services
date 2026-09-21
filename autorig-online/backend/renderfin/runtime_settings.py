"""Apply render controls to the graph, including exact delivery dimensions."""
from __future__ import annotations

import math


def apply_runtime_settings(workflow, prompt, width, height):
    """Pad model dimensions to /32, then deliver the requested pixel dimensions.

    Output resizing is inserted before the image/video encoder, so metadata,
    previews and the downloaded artifact all refer to the same size.
    """
    # A control map must retain the source framing. Cropping a portrait pose
    # map to the product's landscape render default would cut off both bodies.
    if getattr(prompt, 'type', '') in {'control_pose', 'control_depth', 'control_canny'}:
        return workflow
    internal_width = math.ceil(width / 32) * 32
    internal_height = math.ceil(height / 32) * 32
    lora = getattr(prompt, 'lora', '')
    if lora and not any(n.get('class_type') in {'LoraLoaderModelOnly', 'LoraLoader', 'Power Lora Loader (rgthree)'} for n in workflow.values()):
        loader = next((nid for nid,n in workflow.items() if n.get('class_type') in {'UNETLoader', 'CheckpointLoaderSimple'}), None)
        if loader is None:
            raise ValueError('This workflow has no model input for the chosen LoRA')
        for node in workflow.values():
            if node.get('inputs', {}).get('model') == [loader, 0]:
                node['inputs']['model'] = ['selected_lora', 0]
        strength = getattr(prompt, 'lora_strength', None)
        workflow['selected_lora'] = {'class_type': 'LoraLoaderModelOnly', 'inputs': {
            'model': [loader, 0], 'lora_name': lora,
            'strength_model': 1.0 if strength is None else float(strength)}}
    for node in list(workflow.values()):
        inputs = node.get('inputs', {})
        kind = node.get('class_type', '')
        if kind == 'ControlNetApplyAdvanced':
            inputs['strength'] = float(getattr(prompt, 'control_strength', 0.8))
            inputs['start_percent'] = float(getattr(prompt, 'control_start', 0.0))
            inputs['end_percent'] = float(getattr(prompt, 'control_end', 1.0))
        if kind in {'EmptyLatentImage', 'EmptySD3LatentImage', 'EmptyFlux2LatentImage', 'Flux2Scheduler', 'EmptyLTXVLatentVideo',
                    'LTXVBaseSampler', 'HelperNodes_WidthHeight'}:
            inputs.update(width=internal_width, height=internal_height)
        if kind == 'ImageScale' and node.get('_meta', {}).get('title') != 'delivery':
            inputs.update(width=internal_width, height=internal_height)
        frames = max(1, round((getattr(prompt, 'frame_count', 97) - 1) / 8)) * 8 + 1
        if kind == 'EmptyLTXVLatentVideo':
            inputs['length'] = frames
        elif kind == 'LTXVEmptyLatentAudio':
            inputs['frames_number'] = frames
        elif kind == 'LTXVBaseSampler':
            inputs['num_frames'] = frames
        steps = getattr(prompt, 'steps', 0)
        if steps and kind in {'KSampler', 'KSamplerAdvanced', 'BasicScheduler', 'LTXVScheduler', 'Flux2Scheduler'}:
            inputs['steps'] = int(steps)
        cfg = getattr(prompt, 'cfg', None)
        if cfg is not None:
            if kind in {'KSampler', 'KSamplerAdvanced', 'CFGGuider'}:
                inputs['cfg'] = float(cfg)
            elif kind == 'STGGuiderAdvanced':
                inputs['cfg_values'] = ','.join([str(cfg)] * len(inputs['cfg_values'].split(',')))
        sampler = getattr(prompt, 'sampler', '')
        if sampler and kind in {'KSamplerSelect', 'KSampler', 'KSamplerAdvanced'}:
            inputs['sampler_name'] = sampler
        scheduler = getattr(prompt, 'scheduler', '')
        if scheduler and kind in {'BasicScheduler', 'KSampler', 'KSamplerAdvanced'}:
            inputs['scheduler'] = scheduler
        if kind == 'CLIPSetLastLayer' and getattr(prompt, 'clip_skip', None):
            inputs['stop_at_clip_layer'] = -abs(int(prompt.clip_skip))
        # Distilled models use a trained, non-linear sigma schedule. Do not
        # replace it with an invented schedule when somebody changes steps.
        if steps and kind in {'ManualSigmas', 'StringToFloatList'}:
            key = 'sigmas' if kind == 'ManualSigmas' else 'string'
            schedule = inputs.get(key, '')
            try:
                count = len([float(x.strip()) for x in schedule.split(',')]) - 1
            except (ValueError, AttributeError):
                continue
            if count > 0 and count != steps:
                raise ValueError(f'This distilled workflow requires {count} steps; received {steps}')
    for node_id, node in list(workflow.items()):
        kind = node.get('class_type', '')
        if kind not in {'SaveImage', 'PreviewImage', 'VHS_VideoCombine', 'CreateVideo'}:
            continue
        inputs = node.get('inputs', {})
        source = inputs.get('images')
        if not isinstance(source, list):
            continue
        resize_id = 'delivery_size_' + node_id
        workflow[resize_id] = {
            'class_type': 'ImageScale',
            'inputs': {'image': source, 'upscale_method': 'lanczos',
                       'width': width, 'height': height, 'crop': 'center'},
        }
        inputs['images'] = [resize_id, 0]
    return workflow
