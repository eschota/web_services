"""Apply render controls to the graph, including exact delivery dimensions."""
from __future__ import annotations

import math

# Video VAE decode nodes and the input that carries their latent.
_VIDEO_DECODER_LATENT_INPUT = {
    'LTXVTiledVAEDecode': 'latents',
    'LTXVSpatioTemporalTiledVAEDecode': 'latents',
    'VAEDecodeTiled': 'samples',
    'VAEDecode': 'samples',
}
# Nodes that change a latent's spatial size between sampling and decode.
_LATENT_UPSCALERS = {'LTXVLatentUpsampler', 'LatentUpscale', 'LatentUpscaleBy'}


def apply_runtime_settings(workflow, prompt, width, height):
    """Pad model dimensions to /32, then deliver the requested pixel dimensions.

    Output resizing is inserted before the image/video encoder, so metadata,
    previews and the downloaded artifact all refer to the same size.
    """
    # A control map must retain the source framing. Cropping a portrait pose
    # map to the product's landscape render default would cut off both bodies.
    if getattr(prompt, 'type', '') in {'control_pose', 'control_depth', 'control_canny', 'control_normal'}:
        # The preprocessors scale the shorter edge to `resolution`. Asking for
        # the requested size's own shorter edge keeps the map at the source
        # picture's exact pixel size, so nodes that follow the map's size
        # land on the same dimensions as the picture it came from.
        short_side = int(max(64, min(int(width or 0), int(height or 0)) or 960))
        for node in workflow.values():
            inputs = node.get('inputs', {})
            if 'resolution' in inputs and str(node.get('class_type', '')).endswith('Preprocessor'):
                inputs['resolution'] = short_side
        return workflow
    has_video_control = any(n.get('class_type') in {'LTXAddVideoICLoRAGuide', 'LTXAddVideoICLoRAGuideAdvanced'} for n in workflow.values())
    grid = 64 if has_video_control else 32
    internal_width = math.ceil(width / grid) * grid
    internal_height = math.ceil(height / grid) * grid
    lora = getattr(prompt, 'lora', '')
    # The single LoRA counts as placed only when a loader names it: a LoRA
    # stack's loaders must not stand in for it.
    if lora and not any(
            n.get('class_type') == 'Power Lora Loader (rgthree)'
            or (n.get('class_type') in {'LoraLoaderModelOnly', 'LoraLoader'}
                and n.get('inputs', {}).get('lora_name') == lora)
            for n in workflow.values()):
        loader = next((nid for nid,n in workflow.items() if n.get('class_type') in {'UNETLoader', 'CheckpointLoaderSimple'}), None)
        if loader is None:
            raise ValueError('This workflow has no model input for the chosen LoRA')
        for node in workflow.values():
            if node.get('inputs', {}).get('model') == [loader, 0]:
                node['inputs']['model'] = ['selected_lora', 0]
        strength = getattr(prompt, 'lora_strength', None)
        workflow['selected_lora'] = {'class_type': 'LoraLoaderModelOnly', 'inputs': {
            'model': [loader, 0], 'lora_name': lora,
            'strength_model': float(strength) if strength else 1.0}}
    frames = max(1, round((getattr(prompt, 'frame_count', 97) - 1) / 8)) * 8 + 1
    for node in list(workflow.values()):
        inputs = node.get('inputs', {})
        kind = node.get('class_type', '')
        if kind in {'LTXAddVideoICLoRAGuide', 'LTXAddVideoICLoRAGuideAdvanced'}:
            inputs['strength'] = float(getattr(prompt, 'control_strength', 0.8))
        if kind == 'ControlNetApplyAdvanced':
            inputs['strength'] = float(getattr(prompt, 'control_strength', 0.8))
            inputs['start_percent'] = float(getattr(prompt, 'control_start', 0.0))
            inputs['end_percent'] = float(getattr(prompt, 'control_end', 1.0))
        # Z-Image Fun ControlNet patches the model rather than the conditioning,
        # so it has one strength and no start/end window. A template that tuned
        # its own value for a fixed input (the T-pose skeleton, the inpaint
        # hole) marks it preserve_control and keeps it.
        if kind == 'ZImageFunControlnet' and node.get('_meta', {}).get('preserve_control') is not True:
            inputs['strength'] = float(getattr(prompt, 'control_strength', 0.8))
        if kind == 'WanAnimate2ToVideo':
            inputs['pose_strength'] = float(getattr(prompt, 'control_strength', 1.0))
        if kind in {'EmptyLatentImage', 'EmptySD3LatentImage', 'EmptyFlux2LatentImage', 'Flux2Scheduler', 'EmptyLTXVLatentVideo',
                    'LTXVBaseSampler', 'HelperNodes_WidthHeight', 'WanAnimate2ToVideo'}:
            inputs.update(width=internal_width, height=internal_height)
        if kind == 'ImageScale' and node.get('_meta', {}).get('title') != 'delivery':
            inputs.update(width=internal_width, height=internal_height)
        if kind in {'EmptyLTXVLatentVideo', 'WanAnimate2ToVideo'}:
            inputs['length'] = frames
        elif kind == 'LTXVEmptyLatentAudio':
            inputs['frames_number'] = frames
        elif kind == 'LTXVBaseSampler':
            inputs['num_frames'] = frames
        steps = getattr(prompt, 'steps', 0)
        creativity = getattr(prompt, 'creativity', 0)
        # A multi-stage workflow can have an independently authored refinement
        # sampler. Public sampling controls target its primary generation pass.
        preserve_sampling = node.get('_meta', {}).get('preserve_sampling') is True
        if creativity and kind in {'KSampler', 'BasicScheduler'} and 'denoise' in inputs:
            inputs['denoise'] = float(creativity)
        if steps and not preserve_sampling and kind in {'KSampler', 'KSamplerAdvanced', 'BasicScheduler', 'LTXVScheduler', 'Flux2Scheduler'}:
            inputs['steps'] = int(steps)
        cfg = getattr(prompt, 'cfg', None)
        if cfg is not None and not preserve_sampling:
            if kind in {'KSampler', 'KSamplerAdvanced', 'SamplerCustom', 'CFGGuider'}:
                inputs['cfg'] = float(cfg)
            elif kind == 'STGGuiderAdvanced':
                inputs['cfg_values'] = ','.join([str(cfg)] * len(inputs['cfg_values'].split(',')))
        sampler = getattr(prompt, 'sampler', '')
        if sampler and not preserve_sampling and kind in {'KSamplerSelect', 'KSampler', 'KSamplerAdvanced'}:
            inputs['sampler_name'] = sampler
        scheduler = getattr(prompt, 'scheduler', '')
        if scheduler and not preserve_sampling and kind in {'BasicScheduler', 'KSampler', 'KSamplerAdvanced'}:
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
    # LTXVCropGuides counts unique guide time coordinates. An identity frame
    # and an IC-LoRA video both beginning at frame zero therefore leave one
    # appended identity latent behind. Slice to the requested generated latent
    # count before temporal VAE decode; trimming decoded pixels is too late and
    # lets the trailing frame-zero latent bleed into the last valid frames.
    overlapping_first_frame = has_video_control and any(
        node.get('class_type') == 'LTXVAddGuide'
        and node.get('inputs', {}).get('frame_idx', 0) == 0
        for node in workflow.values()
    )
    if overlapping_first_frame:
        for node_id, node in list(workflow.items()):
            latent_key = _VIDEO_DECODER_LATENT_INPUT.get(node.get('class_type'))
            if latent_key is None:
                continue
            source = node.get('inputs', {}).get(latent_key)
            source_node = workflow.get(source[0]) if isinstance(source, list) and source else None
            if not source_node or source_node.get('class_type') != 'LTXVCropGuides':
                continue
            select_id = 'delivery_latents_' + node_id
            workflow[select_id] = {
                'class_type': 'LTXVSelectLatents',
                'inputs': {'samples': source, 'start_index': 0,
                           'end_index': (frames - 1) // 8},
            }
            node['inputs'][latent_key] = [select_id, 0]
    # An LTX graph without a latent upscaler decodes frames at exactly the
    # padded model size. When that already is the requested size, a delivery
    # resize is an identity lanczos pass that costs a full extra float copy of
    # the clip in RAM (5.5 GB for 193 frames at 1152x2048) and ~13 s of CPU.
    decoded_at_model_size = (
        any(n.get('class_type') == 'EmptyLTXVLatentVideo' for n in workflow.values())
        and not any(n.get('class_type') in _LATENT_UPSCALERS for n in workflow.values())
    )
    identity_video_resize = decoded_at_model_size and (internal_width, internal_height) == (width, height)
    for node_id, node in list(workflow.items()):
        kind = node.get('class_type', '')
        if kind not in {'SaveImage', 'PreviewImage', 'VHS_VideoCombine', 'CreateVideo'}:
            continue
        inputs = node.get('inputs', {})
        source = inputs.get('images')
        if not isinstance(source, list):
            continue
        if has_video_control and kind in {'CreateVideo', 'VHS_VideoCombine'}:
            # Native IC guide cropping may leave extra tail frames. Keep the
            # requested timeline from frame zero, never the appended guide tail.
            trim_id = 'delivery_frames_' + node_id
            workflow[trim_id] = {'class_type': 'ImageFromBatch', 'inputs': {
                'image': source, 'batch_index': 0, 'length': frames}}
            source = [trim_id, 0]
        if identity_video_resize and kind in {'CreateVideo', 'VHS_VideoCombine'}:
            inputs['images'] = source
            continue
        resize_id = 'delivery_size_' + node_id
        workflow[resize_id] = {
            'class_type': 'ImageScale',
            'inputs': {'image': source, 'upscale_method': 'lanczos',
                       'width': width, 'height': height, 'crop': 'center'},
        }
        inputs['images'] = [resize_id, 0]
    return workflow
