# -*- coding: utf-8 -*-
"""Builds LTX-2 19B distilled I2V single-stage API-format workflow for ComfyUI."""
import json, sys, copy

NEGATIVE = ("camera movement, zoom, scene cut, extra limb, missing limb, "
            "deformed anatomy, foot sliding, text, watermark")
SIGMAS = "1., 0.99375, 0.9875, 0.98125, 0.975, 0.909375, 0.725, 0.421875, 0.0"
CKPT = "ltx-2-19b-distilled-fp8.safetensors"
LORA = "ltx-2-19b-lora-camera-control-static.safetensors"
GEMMA = "gemma_3_12B_it_fp4_mixed.safetensors"


def build(prompt, image, output_prefix, num_frames, width=768, height=448,
          frame_rate=25, fps=25.0, seed=42):
    g = {
        "checkpoint": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": CKPT},
        },
        "lora": {
            "class_type": "LoraLoaderModelOnly",
            "inputs": {
                "model": ["checkpoint", 0],
                "lora_name": LORA,
                "strength_model": 1.0,
            },
        },
        # NOTE: LTXVGemmaCLIPModelLoader (custom node) requires the full HF
        # gemma folder (tokenizer.model + model-*.safetensors shards + config)
        # and cannot load the single-file gemma_3_12B_it_fp4_mixed.safetensors.
        # Core LTXAVTextEncoderLoader is the loader designed for that file.
        "text_encoder": {
            "class_type": "LTXAVTextEncoderLoader",
            "inputs": {
                "text_encoder": GEMMA,
                "ckpt_name": CKPT,
                "device": "default",
            },
        },
        "positive_prompt": {
            "class_type": "CLIPTextEncode",
            "inputs": {"clip": ["text_encoder", 0], "text": prompt},
        },
        "negative_prompt": {
            "class_type": "CLIPTextEncode",
            "inputs": {"clip": ["text_encoder", 0], "text": NEGATIVE},
        },
        "conditioning": {
            "class_type": "LTXVConditioning",
            "inputs": {
                "positive": ["positive_prompt", 0],
                "negative": ["negative_prompt", 0],
                "frame_rate": float(frame_rate),
            },
        },
        "ref_image": {
            "class_type": "LoadImage",
            "inputs": {"image": image},
        },
        "preprocess": {
            "class_type": "LTXVPreprocess",
            "inputs": {"image": ["ref_image", 0], "img_compression": 33},
        },
        "empty_video_latent": {
            "class_type": "EmptyLTXVLatentVideo",
            "inputs": {
                "width": width,
                "height": height,
                "length": num_frames,
                "batch_size": 1,
            },
        },
        "img_to_video": {
            "class_type": "LTXVImgToVideoInplace",
            "inputs": {
                "vae": ["checkpoint", 2],
                "image": ["preprocess", 0],
                "latent": ["empty_video_latent", 0],
                "strength": 1.0,
                "bypass": False,
            },
        },
        "audio_vae": {
            "class_type": "LTXVAudioVAELoader",
            "inputs": {"ckpt_name": CKPT},
        },
        "empty_audio_latent": {
            "class_type": "LTXVEmptyLatentAudio",
            "inputs": {
                "frames_number": num_frames,
                "frame_rate": int(frame_rate),
                "batch_size": 1,
                "audio_vae": ["audio_vae", 0],
            },
        },
        "concat_av_latent": {
            "class_type": "LTXVConcatAVLatent",
            "inputs": {
                "video_latent": ["img_to_video", 0],
                "audio_latent": ["empty_audio_latent", 0],
            },
        },
        "noise": {
            "class_type": "RandomNoise",
            "inputs": {"noise_seed": seed},
        },
        "sampler_select": {
            "class_type": "KSamplerSelect",
            "inputs": {"sampler_name": "euler"},
        },
        "guider": {
            "class_type": "CFGGuider",
            "inputs": {
                "model": ["lora", 0],
                "positive": ["conditioning", 0],
                "negative": ["conditioning", 1],
                "cfg": 1.0,
            },
        },
        "sigmas": {
            "class_type": "ManualSigmas",
            "inputs": {"sigmas": SIGMAS},
        },
        "sampler": {
            "class_type": "SamplerCustomAdvanced",
            "inputs": {
                "noise": ["noise", 0],
                "guider": ["guider", 0],
                "sampler": ["sampler_select", 0],
                "sigmas": ["sigmas", 0],
                "latent_image": ["concat_av_latent", 0],
            },
        },
        "separate_av_latent": {
            "class_type": "LTXVSeparateAVLatent",
            "inputs": {"av_latent": ["sampler", 1]},
        },
        "vae_decode": {
            "class_type": "LTXVSpatioTemporalTiledVAEDecode",
            "inputs": {
                "vae": ["checkpoint", 2],
                "latents": ["separate_av_latent", 0],
                "spatial_tiles": 4,
                "spatial_overlap": 4,
                "temporal_tile_length": 16,
                "temporal_overlap": 4,
                "last_frame_fix": False,
                "working_device": "auto",
                "working_dtype": "auto",
            },
        },
        "create_video": {
            "class_type": "CreateVideo",
            "inputs": {"images": ["vae_decode", 0], "fps": float(fps)},
        },
        "save_video": {
            "class_type": "SaveVideo",
            "inputs": {
                "video": ["create_video", 0],
                "filename_prefix": output_prefix,
                "format": "mp4",
                "codec": "h264",
            },
        },
    }
    return g


if __name__ == "__main__":
    out_dir = sys.argv[1]
    prompt = ("The horse performs a clear anatomically correct forward walk "
              "cycle in place at a steady controller-friendly cadence. Static "
              "locked camera. No net translation.")
    image = "horse_ref_semantic.png"
    prefix = "horse_pilot_19b"

    # Template with placeholders (49 frames, production shape)
    tmpl = build("$prompt", "$image", "$output_prefix", 49)
    with open(out_dir + "/gen_animation_ltx2_19b_static_lora_api.json", "w",
              encoding="utf-8") as f:
        json.dump(tmpl, f, indent=2, ensure_ascii=False)

    # Filled (49 frames)
    filled = build(prompt, image, prefix, 49)
    with open(out_dir + "/gen_animation_ltx2_19b_static_lora_api_filled.json",
              "w", encoding="utf-8") as f:
        json.dump(filled, f, indent=2, ensure_ascii=False)

    # Smoke (9 frames)
    smoke = build(prompt, image, prefix + "_smoke", 9)
    with open(out_dir + "/smoke_9f.json", "w", encoding="utf-8") as f:
        json.dump(smoke, f, indent=2, ensure_ascii=False)
    print("written")
