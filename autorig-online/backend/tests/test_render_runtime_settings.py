import copy
import unittest
from types import SimpleNamespace

from renderfin.runtime_settings import apply_runtime_settings


class RuntimeSettingsTests(unittest.TestCase):
    def test_video_union_control_uses_half_grid_and_exact_delivery(self):
        graph = {'latent': {'class_type': 'EmptyLTXVLatentVideo', 'inputs': {}},
                 'first': {'class_type': 'LTXVAddGuide', 'inputs': {'frame_idx': 0}},
                 'control': {'class_type': 'LTXAddVideoICLoRAGuide', 'inputs': {'strength': 1}},
                 'crop': {'class_type': 'LTXVCropGuides', 'inputs': {}},
                 'decode': {'class_type': 'LTXVTiledVAEDecode', 'inputs': {'latents': ['crop', 2]}},
                 'video': {'class_type': 'CreateVideo', 'inputs': {'images': ['decode', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(frame_count=97, control_strength=0.6), 960, 540)
        self.assertEqual(graph['latent']['inputs']['height'], 576)
        self.assertEqual(graph['control']['inputs']['strength'], 0.6)
        self.assertEqual(graph['delivery_size_video']['inputs']['height'], 540)
        self.assertEqual(graph['delivery_frames_video']['inputs'],
                         {'image': ['decode', 0], 'batch_index': 0, 'length': 97})
        self.assertEqual(graph['delivery_latents_decode']['inputs'],
                         {'samples': ['crop', 2], 'start_index': 0, 'end_index': 12})
        self.assertEqual(graph['decode']['inputs']['latents'], ['delivery_latents_decode', 0])

    def test_video_union_control_25_frames_crops_before_temporal_decode(self):
        graph = {
            'first': {'class_type': 'LTXVAddGuide', 'inputs': {'frame_idx': 0}},
            'control': {'class_type': 'LTXAddVideoICLoRAGuide', 'inputs': {}},
            'crop': {'class_type': 'LTXVCropGuides', 'inputs': {}},
            'decode': {'class_type': 'LTXVTiledVAEDecode', 'inputs': {'latents': ['crop', 2]}},
        }
        apply_runtime_settings(graph, SimpleNamespace(frame_count=25), 960, 540)
        self.assertEqual(graph['delivery_latents_decode']['inputs'],
                         {'samples': ['crop', 2], 'start_index': 0, 'end_index': 3})
        self.assertEqual(graph['decode']['inputs']['latents'], ['delivery_latents_decode', 0])

    def test_union_control_crops_latents_for_core_tiled_decode(self):
        graph = {
            'first': {'class_type': 'LTXVAddGuide', 'inputs': {'frame_idx': 0}},
            'control': {'class_type': 'LTXAddVideoICLoRAGuide', 'inputs': {}},
            'crop': {'class_type': 'LTXVCropGuides', 'inputs': {}},
            'decode': {'class_type': 'VAEDecodeTiled', 'inputs': {'samples': ['crop', 2]}},
        }
        apply_runtime_settings(graph, SimpleNamespace(frame_count=97), 960, 540)
        self.assertEqual(graph['delivery_latents_decode']['inputs'],
                         {'samples': ['crop', 2], 'start_index': 0, 'end_index': 12})
        self.assertEqual(graph['decode']['inputs']['samples'], ['delivery_latents_decode', 0])

    def test_video_at_model_size_skips_identity_delivery_resize(self):
        graph = {'latent': {'class_type': 'EmptyLTXVLatentVideo', 'inputs': {}},
                 'decode': {'class_type': 'VAEDecodeTiled', 'inputs': {'samples': ['latent', 0]}},
                 'video': {'class_type': 'CreateVideo', 'inputs': {'images': ['decode', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(frame_count=193), 1152, 2048)
        self.assertEqual(graph['latent']['inputs']['width'], 1152)
        self.assertEqual(graph['video']['inputs']['images'], ['decode', 0])
        self.assertNotIn('delivery_size_video', graph)

    def test_latent_upscaled_video_keeps_delivery_resize(self):
        graph = {'latent': {'class_type': 'EmptyLTXVLatentVideo', 'inputs': {}},
                 'upscale': {'class_type': 'LTXVLatentUpsampler', 'inputs': {}},
                 'video': {'class_type': 'CreateVideo', 'inputs': {'images': ['decode', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(frame_count=97), 1152, 2048)
        self.assertEqual(graph['video']['inputs']['images'], ['delivery_size_video', 0])

    def test_images_keep_delivery_resize_at_model_size(self):
        graph = {'latent': {'class_type': 'EmptyLatentImage', 'inputs': {}},
                 'save': {'class_type': 'SaveImage', 'inputs': {'images': ['decode', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(frame_count=1), 1024, 1024)
        self.assertEqual(graph['save']['inputs']['images'], ['delivery_size_save', 0])

    def test_plain_image_to_video_does_not_insert_union_latent_crop(self):
        graph = {
            'first': {'class_type': 'LTXVAddGuide', 'inputs': {'frame_idx': 0}},
            'crop': {'class_type': 'LTXVCropGuides', 'inputs': {}},
            'decode': {'class_type': 'LTXVTiledVAEDecode', 'inputs': {'latents': ['crop', 2]}},
        }
        apply_runtime_settings(graph, SimpleNamespace(frame_count=97), 960, 540)
        self.assertNotIn('delivery_latents_decode', graph)
        self.assertEqual(graph['decode']['inputs']['latents'], ['crop', 2])

    def test_half_hd_is_padded_for_model_and_exact_for_saved_video(self):
        graph = {'latent': {'class_type': 'EmptyLTXVLatentVideo', 'inputs': {'width': 1024, 'height': 1024}},
                 'video': {'class_type': 'CreateVideo', 'inputs': {'images': ['decode', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(frame_count=25), 960, 540)
        self.assertEqual(graph['latent']['inputs'], {'width': 960, 'height': 544, 'length': 25})
        self.assertEqual(graph['video']['inputs']['images'], ['delivery_size_video', 0])
        self.assertEqual(graph['delivery_size_video']['inputs']['height'], 540)
        self.assertEqual(graph['delivery_size_video']['inputs']['image'], ['decode', 0])

    def test_steps_sampler_cfg_scheduler_reach_sampler(self):
        graph = {'sample': {'class_type': 'KSampler', 'inputs': {'steps': 4, 'cfg': 1, 'sampler_name': 'euler', 'scheduler': 'normal'}}}
        apply_runtime_settings(graph, SimpleNamespace(steps=30, cfg=5, sampler='dpmpp_2m', scheduler='karras'), 960, 540)
        self.assertEqual(graph['sample']['inputs'], {'steps': 30, 'cfg': 5.0, 'sampler_name': 'dpmpp_2m', 'scheduler': 'karras'})

    def test_primary_defaults_do_not_overwrite_independent_refinement_schedule(self):
        graph = {
            'primary': {'class_type': 'BasicScheduler', 'inputs': {'steps': 20, 'scheduler': 'normal'}},
            'refine': {'class_type': 'KSamplerAdvanced', '_meta': {'preserve_sampling': True},
                       'inputs': {'steps': 6, 'cfg': 1, 'sampler_name': 'dpmpp_2m',
                                  'scheduler': 'karras', 'start_at_step': 1, 'end_at_step': 6}},
        }
        refine = copy.deepcopy(graph['refine'])
        apply_runtime_settings(graph, SimpleNamespace(steps=4, cfg=1, sampler='euler', scheduler='simple'), 960, 540)
        self.assertEqual(graph['primary']['inputs'], {'steps': 4, 'scheduler': 'simple'})
        self.assertEqual(graph['refine'], refine)

    def test_selected_lora_is_connected_when_template_has_no_loader(self):
        graph = {'model': {'class_type': 'UNETLoader', 'inputs': {'unet_name': 'base'}},
                 'guide': {'class_type': 'BasicGuider', 'inputs': {'model': ['model', 0]}}}
        apply_runtime_settings(graph, SimpleNamespace(lora='style.safetensors', lora_strength=0.8), 960, 540)
        self.assertEqual(graph['guide']['inputs']['model'], ['selected_lora', 0])
        self.assertEqual(graph['selected_lora']['inputs']['lora_name'], 'style.safetensors')
        self.assertEqual(graph['selected_lora']['inputs']['strength_model'], 0.8)

    def test_distilled_schedule_is_not_silently_changed(self):
        graph = {'schedule': {'class_type': 'ManualSigmas', 'inputs': {'sigmas': '1,0.9,0.8,0'}}}
        original = copy.deepcopy(graph)
        apply_runtime_settings(graph, SimpleNamespace(steps=3), 960, 540)
        self.assertEqual(graph, original)
        with self.assertRaisesRegex(ValueError, 'requires 3 steps'):
            apply_runtime_settings(graph, SimpleNamespace(steps=12), 960, 540)
